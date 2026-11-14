//! What the application hears: its messages through a [`Handler`], and
//! [`Event`]s about the connection.

use std::future::Future;
use std::time::Duration;

use crate::error::Error;
use crate::message::{Message, Position};

/// What the application does with what the server sends.
///
/// The client calls it from the connection's own task, in the order the
/// messages came, with each message read in place from its frame. It holds
/// the connection while it runs: hand long work to a task of its own.
///
/// A closure taking `&Message` is a handler too.
pub trait Handler: Send + 'static {
    /// A message for the application: everything the server sends but its
    /// own protocol messages, duplicates dropped and topic messages in
    /// order.
    fn message(&mut self, message: &Message<'_>);

    /// Something happened to the connection.
    fn event(&mut self, event: Event) {
        let _ = event;
    }

    /// Bytes the server application sent as they are (`send_bytes`).
    fn binary(&mut self, data: &[u8]) {
        let _ = data;
    }

    /// Called before each frame is read. Waiting here holds the server back
    /// while the application catches up; the heartbeat and sends go on
    /// meanwhile, and the wait may be dropped for them and begun again: it
    /// must lose nothing when dropped.
    fn ready(&mut self) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

impl<F> Handler for F
where
    F: FnMut(&Message<'_>) + Send + 'static,
{
    fn message(&mut self, message: &Message<'_>) {
        self(message)
    }
}

/// Something that happened to the connection.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    /// The server took the connection (`server_ready`). Topics are
    /// subscribed again from where the client left off.
    Connected(Connected),
    /// The connection was lost, or an attempt to make it again failed; the
    /// next attempt comes after `retry_in`.
    Disconnected(Disconnected),
    /// The server's answer to a subscription request.
    Subscribed(Subscribed),
    /// Messages of a topic that will not come: reload what depends on them.
    Gap(Gap),
    /// An `error` message from the server.
    ServerError(ServerError),
    /// The client stopped for good: closed by the application (`None`), or
    /// the reason.
    Closed(Option<Error>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Connected {
    pub connection_id: String,
    pub user_id: Option<String>,
    /// A connection made again after one was lost.
    pub reconnected: bool,
    /// The server keeps topic histories: missed messages are replayed.
    pub recovery: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Disconnected {
    /// The close code; `None` when the connection was cut or never made.
    pub code: Option<u16>,
    pub reason: String,
    /// When the next attempt is made.
    pub retry_in: Duration,
    /// Attempts made since the last connection the server took.
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Subscribed {
    pub topics: Vec<String>,
    /// Refused: by the topic rights, an invalid name, or over a limit.
    pub failed: Vec<String>,
    /// Per topic asked for with a position: what was replayed.
    pub recovery: Vec<Recovery>,
}

/// What a subscription replayed of a topic.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Recovery {
    pub topic: String,
    /// Every missed message was replayed.
    pub recovered: bool,
    /// Messages replayed.
    pub count: u64,
    /// The topic's position on the server now.
    pub position: Option<Position>,
}

/// Messages of a topic that will not come.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Gap {
    pub topic: String,
    pub epoch: u32,
    /// The first and last offsets missing, when known.
    pub first: Option<u64>,
    pub last: Option<u64>,
    pub reason: GapReason,
}

impl Gap {
    /// How many messages are missing, when known.
    pub fn missed(&self) -> Option<u64> {
        Some(self.last?.checked_sub(self.first?)? + 1)
    }
}

/// Why a gap was not filled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum GapReason {
    /// The replay did not come within `recovery_timeout`.
    Timeout,
    /// Too many messages came past the gap while waiting.
    Overflow,
    /// The server no longer has the messages.
    NotRecovered,
    /// The server keeps no history, or does not answer recovery requests.
    Unavailable,
    /// The replay stopped short.
    Incomplete,
    /// The server restarted: the topic's history began again.
    EpochChanged,
    /// The client stopped while waiting.
    Closed,
}

/// An `error` message from the server.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ServerError {
    /// `RATE_LIMITED`, `SIGNATURE_REFUSED`, the application's own codes...
    pub code: String,
    pub message: String,
    /// The whole payload.
    pub details: serde_json::Value,
}
