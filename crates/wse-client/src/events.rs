//! The client as a stream: messages and events in one queue, for an
//! application that reads them in a loop.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::mpsc;

use crate::client::Client;
use crate::config::Config;
use crate::delivery;
use crate::error::Error;
use crate::handler::{Event, Handler};
use crate::message::{Message, OwnedMessage};

/// What the [`Events`] stream yields.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Incoming {
    Message(OwnedMessage),
    Event(Event),
    /// Bytes the server application sent as they are.
    Binary(bytes::Bytes),
}

/// What the server sends, in order. While the application does not read,
/// the client stops reading too and the server holds back; in the browser,
/// which cannot hold a WebSocket back, what comes waits in memory.
///
/// The delivery ([`Config::delivery`]) sees what is read in the order it is
/// read: a message read before the events show another user is the last
/// user's, and a topic not continuous holds its messages here until
/// [`Client::resynced`].
pub struct Events {
    rx: Receiver,
    gate: delivery::Shared,
    /// Says that the gate made something (held messages handed on).
    wake: mpsc::UnboundedReceiver<()>,
}

impl std::fmt::Debug for Events {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Events").finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
type Sender = mpsc::Sender<Incoming>;
#[cfg(not(target_arch = "wasm32"))]
type Receiver = mpsc::Receiver<Incoming>;
#[cfg(target_arch = "wasm32")]
type Sender = mpsc::UnboundedSender<Incoming>;
#[cfg(target_arch = "wasm32")]
type Receiver = mpsc::UnboundedReceiver<Incoming>;

impl Events {
    /// The next message or event; `None` once the client stopped and
    /// everything was read.
    pub async fn next(&mut self) -> Option<Incoming> {
        futures_util::future::poll_fn(|cx| self.poll(cx)).await
    }

    /// What came already, without waiting: None when nothing is waiting
    /// now (or the stream ended).
    pub fn try_next(&mut self) -> Option<Incoming> {
        while self.wake.try_recv().is_ok() {}
        loop {
            if let Some(item) = delivery::lock(&self.gate).take_out() {
                return Some(item);
            }
            let incoming = self.rx.try_recv().ok()?;
            if let Some(item) = self.pass(incoming) {
                return Some(item);
            }
        }
    }

    /// One item read, through the delivery's gate: None when it holds it.
    /// Taken and passed without a wait, so a read given up loses nothing.
    fn pass(&mut self, incoming: Incoming) -> Option<Incoming> {
        let mut gate = delivery::lock(&self.gate);
        match incoming {
            Incoming::Message(message) => gate.owned(message).map(Incoming::Message),
            Incoming::Event(event) => {
                gate.event(&event);
                Some(Incoming::Event(event))
            }
            binary => Some(binary),
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Incoming>> {
        loop {
            // What the gate made comes first: it is older than what waits.
            if let Some(item) = delivery::lock(&self.gate).take_out() {
                return Poll::Ready(Some(item));
            }
            // A wake only says to look again; its waker stays registered.
            while let Poll::Ready(Some(())) = self.wake.poll_recv(cx) {}
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(incoming)) => {
                    if let Some(item) = self.pass(incoming) {
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(delivery::lock(&self.gate).take_out()),
                Poll::Pending => {
                    // Made while the queue was looked at.
                    return match delivery::lock(&self.gate).take_out() {
                        Some(item) => Poll::Ready(Some(item)),
                        None => Poll::Pending,
                    };
                }
            }
        }
    }
}

impl futures_util::Stream for Events {
    type Item = Incoming;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Incoming>> {
        self.poll(cx)
    }
}

/// How many the stream holds before the client stops reading.
#[cfg(not(target_arch = "wasm32"))]
const QUEUE: usize = 1024;

/// Connects, and returns the client with the stream of what the server
/// sends. See [`Client::connect`].
pub async fn connect(config: Config) -> Result<(Client, Events), Error> {
    with_events(config, true).await
}

/// Starts, and returns at once the client with the stream of what the
/// server sends: the connection is made in the background. See
/// [`Client::start`].
pub async fn start(config: Config) -> Result<(Client, Events), Error> {
    with_events(config, false).await
}

async fn with_events(config: Config, wait: bool) -> Result<(Client, Events), Error> {
    #[cfg(not(target_arch = "wasm32"))]
    let (tx, rx) = mpsc::channel(QUEUE);
    #[cfg(target_arch = "wasm32")]
    let (tx, rx) = mpsc::unbounded_channel();
    let queue = Queue {
        tx,
        waiting: VecDeque::new(),
    };
    let (wake_tx, wake) = mpsc::unbounded_channel();
    let (client, gate) = Client::launch(config, |_| queue, Some(wake_tx), wait).await?;
    Ok((client, Events { rx, gate, wake }))
}

/// A handler that puts everything in the stream's queue.
struct Queue {
    tx: Sender,
    /// What did not fit; read is held until it does.
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    waiting: VecDeque<Incoming>,
}

impl Queue {
    #[cfg(not(target_arch = "wasm32"))]
    fn push(&mut self, item: Incoming) {
        if !self.waiting.is_empty() {
            self.waiting.push_back(item);
            return;
        }
        match self.tx.try_send(item) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(item)) => self.waiting.push_back(item),
            // Nobody reads the stream any more.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn push(&mut self, item: Incoming) {
        // Nobody reads the stream any more: dropped.
        let _ = self.tx.send(item);
    }
}

impl Handler for Queue {
    fn message(&mut self, message: &Message<'_>) {
        self.push(Incoming::Message(message.to_owned()));
    }

    fn event(&mut self, event: Event) {
        self.push(Incoming::Event(event));
    }

    fn binary(&mut self, data: &[u8]) {
        self.push(Incoming::Binary(bytes::Bytes::copy_from_slice(data)));
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn ready(&mut self) {
        // Room first, then the item: dropped while it waits, nothing is
        // lost.
        while !self.waiting.is_empty() {
            let Ok(permit) = self.tx.reserve().await else {
                self.waiting.clear();
                return;
            };
            if let Some(item) = self.waiting.pop_front() {
                permit.send(item);
            }
        }
    }
}
