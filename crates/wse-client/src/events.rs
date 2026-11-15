//! The client as a stream: messages and events in one queue, for an
//! application that reads them in a loop.

use std::collections::VecDeque;

use tokio::sync::mpsc;

use crate::client::Client;
use crate::config::Config;
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
#[derive(Debug)]
pub struct Events {
    rx: Receiver,
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
        self.rx.recv().await
    }

    /// What came already, without waiting: None when nothing is waiting
    /// now (or the stream ended).
    pub fn try_next(&mut self) -> Option<Incoming> {
        self.rx.try_recv().ok()
    }
}

impl futures_util::Stream for Events {
    type Item = Incoming;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Incoming>> {
        self.rx.poll_recv(cx)
    }
}

/// How many the stream holds before the client stops reading.
#[cfg(not(target_arch = "wasm32"))]
const QUEUE: usize = 1024;

/// Connects, and returns the client with the stream of what the server
/// sends. See [`Client::connect`].
pub async fn connect(config: Config) -> Result<(Client, Events), Error> {
    #[cfg(not(target_arch = "wasm32"))]
    let (tx, rx) = mpsc::channel(QUEUE);
    #[cfg(target_arch = "wasm32")]
    let (tx, rx) = mpsc::unbounded_channel();
    let queue = Queue {
        tx,
        waiting: VecDeque::new(),
    };
    let client = Client::connect(config, queue).await?;
    Ok((client, Events { rx }))
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
