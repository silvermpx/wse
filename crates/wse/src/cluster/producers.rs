//! Asking a peer what it took of a client's producer. A client that comes to
//! this node from another names that node, and the server asks it once
//! (`server::acks`): the peer hands the producer over and answers what it
//! took, so a message the client sends again is not handed on twice.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use super::protocol::encode_producer_query;

/// The links to the peers that answer for producers, and the questions out.
#[derive(Default)]
pub(crate) struct ProducerLinks {
    /// Each peer's link by its instance id, with the link's session: a
    /// link's end removes only its own entry.
    routes: DashMap<String, (mpsc::Sender<Bytes>, u64)>,
    next: AtomicU64,
    waiting: DashMap<u64, oneshot::Sender<Option<u64>>>,
}

impl ProducerLinks {
    /// The link of session `session` to `peer` answers from now on.
    pub(crate) fn register(&self, peer: &str, tx: mpsc::Sender<Bytes>, session: u64) {
        self.routes.insert(peer.to_owned(), (tx, session));
    }

    /// That link ended.
    pub(crate) fn release(&self, peer: &str, session: u64) {
        self.routes.remove_if(peer, |_, (_, s)| *s == session);
    }

    /// A link to `peer` answers.
    pub(crate) fn reaches(&self, peer: &str) -> bool {
        self.routes.contains_key(peer)
    }

    /// What `peer` took of `user`'s `producer`, handing it over; None when
    /// it knows nothing of it, has no link, or does not answer in `within`.
    pub(crate) async fn ask(
        &self,
        peer: &str,
        user: Option<&str>,
        producer: &str,
        within: Duration,
    ) -> Option<u64> {
        let tx = self.routes.get(peer).map(|route| route.0.clone())?;
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        self.waiting.insert(id, answer);
        // The link's writer frames what it sends.
        let mut frame = BytesMut::new();
        encode_producer_query(&mut frame, id, user, producer);
        let taken = if tx.send(frame.freeze()).await.is_ok() {
            tokio::time::timeout(within, answered)
                .await
                .ok()
                .and_then(Result::ok)
                .flatten()
        } else {
            None
        };
        self.waiting.remove(&id);
        taken
    }

    /// A peer's answer to question `id`.
    pub(crate) fn answer(&self, id: u64, taken: Option<u64>) {
        if let Some((_, waiting)) = self.waiting.remove(&id) {
            let _ = waiting.send(taken);
        }
    }
}
