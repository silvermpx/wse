//! What the application hears: its messages through a [`Handler`], and
//! [`Event`]s about the connection.

use std::future::Future;
use std::time::Duration;

use crate::delivery::{Continuity, StreamPosition};
use crate::error::Error;
use crate::message::{Message, Position};

/// `Send`, except in the browser: a handler there may hold what the page
/// has, and runs on its one thread.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> MaybeSend for T {}

/// `Send`, except in the browser: a handler there may hold what the page
/// has, and runs on its one thread.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T> MaybeSend for T {}

/// What the application does with what the server sends.
///
/// The client calls it from the connection's own task, in the order the
/// messages came, with each message read in place from its frame. It holds
/// the connection while it runs: hand long work to a task of its own.
///
/// A closure taking `&Message` is a handler too.
pub trait Handler: MaybeSend + 'static {
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

    /// Called before each frame is read once the server took the
    /// connection. Waiting here holds the server back while the application
    /// catches up; the heartbeat and sends go on meanwhile, and the wait may
    /// be dropped for them and begun again: it must lose nothing when
    /// dropped. Not called in the browser, which cannot hold a WebSocket
    /// back: there the frames wait in memory.
    fn ready(&mut self) -> impl Future<Output = ()> + MaybeSend {
        std::future::ready(())
    }
}

impl<F> Handler for F
where
    F: FnMut(&Message<'_>) + MaybeSend + 'static,
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
    /// The application paused the client ([`Client::pause`]): the
    /// connection ended and no attempt is made until the last reason is
    /// cleared; topics and positions stay.
    ///
    /// [`Client::pause`]: crate::Client::pause
    Paused(Paused),
    /// The server's answer to a subscription request.
    Subscribed(Subscribed),
    /// Messages of a topic that will not come: reload what depends on them.
    Gap(Gap),
    /// An `error` message from the server.
    ServerError(ServerError),
    /// recover-or-resync ([`Config::delivery`]): the topics wait for the
    /// application to bring its state of them up to date;
    /// [`Client::resynced`] with the round then hands on what they held.
    ///
    /// [`Config::delivery`]: crate::Config::delivery
    /// [`Client::resynced`]: crate::Client::resynced
    Resync(Resync),
    /// A topic's continuity changed: its messages run without a break, or
    /// they broke off and the application's state of it is to be brought up
    /// to date.
    Continuity(TopicContinuity),
    /// The checkpoint the delivery was given is not this client's (its
    /// realm, user, consumer or schema differs), or not one at all: recovery
    /// starts without it.
    CheckpointRejected(CheckpointRejected),
    /// The file of the offline queue ([`Config::offline`]) could not be
    /// written: what is kept stays in memory, and may not outlive the
    /// application.
    ///
    /// [`Config::offline`]: crate::Config::offline
    StorageFailed(StorageFailed),
    /// The client stopped for good: closed by the application (`None`), or
    /// the reason.
    Closed(Option<Error>),
}

/// Topics waiting for the application to bring its state of them up to date.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Resync {
    /// What to give [`Client::resynced`](crate::Client::resynced) once the
    /// application's state of the topics is up to date.
    pub round: u64,
    pub topics: Vec<String>,
    /// Why, for each topic in order: `cold_start`, `reconnected`,
    /// `requested`, `overflow`, or the reason of the gap.
    pub reasons: Vec<String>,
    /// Where the consumer stands in each stream of the topics: what it
    /// committed it applied.
    pub committed: Vec<StreamPosition>,
}

/// A topic's continuity, as it changed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicContinuity {
    pub topic: String,
    pub continuity: Continuity,
    /// Why it is not continuous.
    pub reason: Option<String>,
}

/// What failed in the offline queue's file (`operation`: `write`), and why.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageFailed {
    pub operation: String,
    pub message: String,
}

/// Why the delivery's checkpoint was not taken: `malformed`, `version`,
/// `realm`, `user`, `consumer` or `schema`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckpointRejected {
    pub reason: String,
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
pub struct Paused {
    /// The reasons the client is paused for.
    pub reasons: Vec<String>,
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

impl GapReason {
    /// The reason as the other clients name it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Overflow => "overflow",
            Self::NotRecovered => "not_recovered",
            Self::Unavailable => "unavailable",
            Self::Incomplete => "incomplete",
            Self::EpochChanged => "epoch_changed",
            Self::Closed => "closed",
        }
    }
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
