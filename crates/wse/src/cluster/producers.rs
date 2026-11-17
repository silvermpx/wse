//! Asking a peer what it decided of a client's producer. A client that comes
//! to this node from another names that node, and the server asks it once
//! (`server::acks`): the peer hands the producer over and answers what it
//! decided and the refusals behind that, so a message the client sends again
//! is not handed on twice, and a refused one is not told taken.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};

use super::protocol::encode_producer_query;
use crate::server::Hole;

/// What a peer decided of a producer, and the refusals behind it.
type Decided = Option<(u64, Vec<Hole>)>;

/// The links to the peers that answer for producers, and the questions out.
#[derive(Default)]
pub(crate) struct ProducerLinks {
    /// Each peer's link by its instance id, with the link's session: a
    /// link's end removes only its own entry.
    routes: DashMap<String, (mpsc::Sender<Bytes>, u64)>,
    next: AtomicU64,
    waiting: DashMap<u64, oneshot::Sender<Decided>>,
}

/// A question out, taken off the waiting ones when it ends.
struct Question<'a> {
    links: &'a ProducerLinks,
    id: u64,
}

impl Drop for Question<'_> {
    fn drop(&mut self) {
        self.links.waiting.remove(&self.id);
    }
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

    /// What `peer` decided of `user`'s `producer`, handing it over; None
    /// when it knows nothing of it, has no link, or does not answer in
    /// `within`.
    pub(crate) async fn ask(
        &self,
        peer: &str,
        user: Option<&str>,
        producer: &str,
        within: Duration,
    ) -> Decided {
        let tx = self.routes.get(peer).map(|route| route.0.clone())?;
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        self.waiting.insert(id, answer);
        // Let go however this ends: answered, too late, or dropped with the
        // future that asked.
        let _question = Question { links: self, id };
        // The link's writer frames what it sends.
        let mut frame = BytesMut::new();
        encode_producer_query(&mut frame, id, user, producer);
        // One deadline for the asking and the answer: a peer whose queue is
        // full is not waited for longer than one that does not answer.
        tokio::time::timeout(within, async {
            tx.send(frame.freeze()).await.ok()?;
            answered.await.ok().flatten()
        })
        .await
        .ok()
        .flatten()
    }

    /// A peer's answer to question `id`.
    pub(crate) fn answer(&self, id: u64, decided: Decided) {
        if let Some((_, waiting)) = self.waiting.remove(&id) {
            let _ = waiting.send(decided);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_peer_whose_queue_is_full_is_not_waited_for_past_the_deadline() {
        let links = ProducerLinks::default();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"full")).unwrap();
        links.register("peer", tx, 1);
        let asked = links.ask("peer", None, "p", Duration::from_millis(5));
        let decided = tokio::time::timeout(Duration::from_secs(2), asked)
            .await
            .expect("not waited for past its deadline");
        assert!(decided.is_none());
        assert!(links.waiting.is_empty(), "the question is let go");
    }

    #[tokio::test]
    async fn a_question_dropped_while_it_waits_is_let_go() {
        let links = ProducerLinks::default();
        let (tx, _rx) = mpsc::channel(4);
        links.register("peer", tx, 1);
        let asked = links.ask("peer", None, "p", Duration::from_secs(60));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), asked)
                .await
                .is_err()
        );
        assert!(links.waiting.is_empty(), "the question is let go");
    }
}
