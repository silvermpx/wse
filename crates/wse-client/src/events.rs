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
/// the client stops reading too and the server holds back.
#[derive(Debug)]
pub struct Events {
    rx: mpsc::Receiver<Incoming>,
}

impl Events {
    /// The next message or event; `None` once the client stopped and
    /// everything was read.
    pub async fn next(&mut self) -> Option<Incoming> {
        self.rx.recv().await
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
const QUEUE: usize = 1024;

/// Connects, and returns the client with the stream of what the server
/// sends. See [`Client::connect`].
pub async fn connect(config: Config) -> Result<(Client, Events), Error> {
    let (tx, rx) = mpsc::channel(QUEUE);
    let queue = Queue {
        tx,
        waiting: VecDeque::new(),
    };
    let client = Client::connect(config, queue).await?;
    Ok((client, Events { rx }))
}

/// A handler that puts everything in the stream's queue.
struct Queue {
    tx: mpsc::Sender<Incoming>,
    /// What did not fit; read is held until it does.
    waiting: VecDeque<Incoming>,
}

impl Queue {
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

    async fn ready(&mut self) {
        while let Some(item) = self.waiting.pop_front() {
            if self.tx.send(item).await.is_err() {
                self.waiting.clear();
                return;
            }
        }
    }
}
