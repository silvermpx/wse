//! The clients' outboxes, as the server sees them. A client numbers what it
//! sends (`s`) per producer, one producer per outbox, and keeps each message
//! until the server answers: `ack` (taken, up to and with a number) or
//! `nack` (refused, and why). The server remembers, per user and producer,
//! the highest message its application took: a repeat of one taken already
//! is confirmed again and not handed on, and what it cannot tell (its state
//! is gone) reaches the application marked as a possible repeat. A client
//! that comes from another node names it, and that node is asked once.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use tokio_tungstenite::tungstenite::Message;

use super::state::{ConnectionHandle, SharedState};

/// A producer's mark once another node took it over: what still reads for
/// it here stops.
const MOVED: u64 = 1 << 63;

/// How long a producer's mark is kept after its last message: a client
/// that comes back later is told nothing is known of it.
pub(crate) const KEPT_FOR_MS: u64 = 24 * 60 * 60 * 1000;

/// How long a node waits for the node a client was on to say what it took.
const ASK_WITHIN: std::time::Duration = std::time::Duration::from_secs(2);

/// Longest producer id or node name taken: a client's random id, not a
/// document.
const MAX_ID_LEN: usize = 64;

/// One producer's highest message taken on this node.
pub(crate) struct Slot {
    mark: AtomicU64,
    /// Its last activity, in epoch milliseconds: the sweep drops it a day
    /// after.
    seen: AtomicU64,
}

/// Whether a message is handed on.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    /// The first time it came: handed on.
    First,
    /// Taken already: confirmed again, not handed on.
    Repeat,
    /// Another node took the producer over.
    Moved,
}

impl Slot {
    fn new(now: u64) -> Self {
        Self {
            mark: AtomicU64::new(0),
            seen: AtomicU64::new(now),
        }
    }

    /// Claims message `s`: its first claim hands it on, any later one is a
    /// repeat. One compare-and-swap, no lock: two connections of the same
    /// producer never hand on the same message.
    pub(crate) fn claim(&self, s: u64) -> Claim {
        let mut mark = self.mark.load(Ordering::Acquire);
        loop {
            if mark & MOVED != 0 {
                return Claim::Moved;
            }
            if mark >= s {
                return Claim::Repeat;
            }
            match self
                .mark
                .compare_exchange_weak(mark, s, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Claim::First,
                Err(now) => mark = now,
            }
        }
    }

    /// The highest message taken here.
    pub(crate) fn taken(&self) -> u64 {
        self.mark.load(Ordering::Acquire) & !MOVED
    }

    /// Another node takes the producer over: what was taken here; from now
    /// on nothing more is.
    fn hand_over(&self) -> u64 {
        self.mark.fetch_or(MOVED, Ordering::AcqRel) & !MOVED
    }

    fn moved(&self) -> bool {
        self.mark.load(Ordering::Acquire) & MOVED != 0
    }

    /// What another node took counts as taken here too.
    fn advance(&self, to: u64) {
        let to = to & !MOVED;
        let mut mark = self.mark.load(Ordering::Acquire);
        while mark & MOVED == 0 && mark < to {
            match self
                .mark
                .compare_exchange_weak(mark, to, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(now) => mark = now,
            }
        }
    }

    pub(crate) fn touch(&self, now: u64) {
        self.seen.store(now, Ordering::Relaxed);
    }
}

type Key = (Option<Box<str>>, Box<str>);

/// Every producer this node took messages of, by user and producer: a
/// user's producer cannot be reached by another user's connection.
#[derive(Default)]
pub(crate) struct Producers {
    slots: DashMap<Key, Arc<Slot>>,
}

impl Producers {
    /// The slot of `user`'s `producer` here, and whether it is known; one
    /// that is not (never seen, or moved to another node) starts empty.
    fn attach(&self, user: Option<&str>, producer: &str, now: u64) -> (Arc<Slot>, bool) {
        use dashmap::mapref::entry::Entry;
        let key = (user.map(Box::from), Box::from(producer));
        match self.slots.entry(key) {
            Entry::Occupied(entry) if !entry.get().moved() => {
                entry.get().touch(now);
                (Arc::clone(entry.get()), true)
            }
            Entry::Occupied(mut entry) => {
                let slot = Arc::new(Slot::new(now));
                entry.insert(Arc::clone(&slot));
                (slot, false)
            }
            Entry::Vacant(entry) => {
                let slot = Arc::new(Slot::new(now));
                entry.insert(Arc::clone(&slot));
                (slot, false)
            }
        }
    }

    /// A peer asks for `user`'s `producer`, which went on there: what was
    /// taken here, and nothing more is. None: nothing is known of it here.
    pub(crate) fn hand_over(&self, user: Option<&str>, producer: &str) -> Option<u64> {
        let key = (user.map(Box::from), Box::from(producer));
        self.slots.get(&key).map(|slot| slot.hand_over())
    }

    /// The link of a connection whose hello is in: known here (the client
    /// was here last), or not, and then unsure up to what it wrote.
    fn link(&self, user: Option<&str>, hello: &Hello, here: bool, now: u64) -> Link {
        let (slot, known) = self.attach(user, &hello.producer, now);
        if known && here {
            let start = slot.taken();
            Link {
                slot,
                start,
                unsure_until: 0,
            }
        } else {
            Link {
                slot,
                start: 0,
                unsure_until: hello.written,
            }
        }
    }

    /// The link of a client that came from another node, with what that
    /// node took of it: None when it knew nothing or did not answer.
    fn taken_over(&self, user: Option<&str>, hello: &Hello, taken: Option<u64>, now: u64) -> Link {
        let (slot, _) = self.attach(user, &hello.producer, now);
        match taken {
            Some(taken) => {
                slot.advance(taken);
                let start = slot.taken();
                Link {
                    slot,
                    start,
                    unsure_until: 0,
                }
            }
            None => Link {
                slot,
                start: 0,
                unsure_until: hello.written,
            },
        }
    }

    /// Drops the producers silent for longer than [`KEPT_FOR_MS`].
    pub(crate) fn sweep(&self, now: u64) {
        self.slots
            .retain(|_, slot| now.saturating_sub(slot.seen.load(Ordering::Relaxed)) < KEPT_FOR_MS);
    }
}

/// What a client's `client_hello` says of its outbox (`p.outbox`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Hello {
    pub(crate) producer: Box<str>,
    /// The node it was connected to last, as that node named itself.
    pub(crate) node: Option<Box<str>>,
    /// The highest message it wrote on an earlier connection: one the
    /// server cannot tell about is up to it.
    pub(crate) written: u64,
}

impl Hello {
    pub(crate) fn parse(p: Option<&serde_json::Value>) -> Option<Self> {
        let outbox = p?.get("outbox")?;
        let producer = outbox.get("producer")?.as_str()?;
        if producer.is_empty() || producer.len() > MAX_ID_LEN {
            return None;
        }
        Some(Self {
            producer: producer.into(),
            node: outbox
                .get("node")
                .and_then(serde_json::Value::as_str)
                .filter(|n| !n.is_empty() && n.len() <= MAX_ID_LEN)
                .map(Box::from),
            written: outbox
                .get("written")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        })
    }
}

/// A connection's producer, once known.
pub(crate) struct Link {
    pub(crate) slot: Arc<Slot>,
    /// What the client was told was taken: where its messages on this
    /// connection go on from.
    pub(crate) start: u64,
    /// Up to this message the server cannot tell whether one was taken (on
    /// a node that is gone): handed on marked as a possible repeat.
    pub(crate) unsure_until: u64,
}

/// A connection's outbox: its hello, and its producer once the connection
/// is admitted.
#[derive(Default)]
pub(crate) struct Holder {
    pub(crate) hello: OnceLock<Hello>,
    pub(crate) link: OnceLock<Link>,
    started: AtomicBool,
}

impl Holder {
    /// Takes the client's hello: the first one counts.
    pub(crate) fn hello(&self, p: Option<&serde_json::Value>) {
        if self.hello.get().is_none()
            && let Some(hello) = Hello::parse(p)
        {
            let _ = self.hello.set(hello);
        }
    }

    /// The hello, for the one caller that starts the connection's producer:
    /// both the hello and the admission are in, and whoever comes second
    /// starts it, once.
    fn begin(&self) -> Option<&Hello> {
        let hello = self.hello.get()?;
        (!self.started.swap(true, Ordering::AcqRel)).then_some(hello)
    }
}

/// Milliseconds since the Unix epoch: a producer's last activity.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Starts the connection's producer once its hello and its admission are
/// both in, and tells the client what was taken of it. A client that comes
/// from another node this one links to: that node is asked first, once.
pub(crate) fn start(state: &Arc<SharedState>, handle: &ConnectionHandle, user: Option<&str>) {
    let Some(hello) = handle.out.acks.begin() else {
        return;
    };
    let last = hello
        .node
        .as_deref()
        .filter(|last| *last != &*state.node_id);
    if let Some(last) = last
        && hello.written > 0
        && state.producer_links.reaches(last)
    {
        let (state, handle) = (Arc::clone(state), handle.clone());
        let (user, last) = (user.map(str::to_owned), last.to_owned());
        tokio::spawn(async move {
            let Some(hello) = handle.out.acks.hello.get() else {
                return;
            };
            let taken = state
                .producer_links
                .ask(&last, user.as_deref(), &hello.producer, ASK_WITHIN)
                .await;
            let link = state
                .producers
                .taken_over(user.as_deref(), hello, taken, now_ms());
            finish(&state, &handle, link);
        });
        return;
    }
    let link = state.producers.link(user, hello, last.is_none(), now_ms());
    finish(state, handle, link);
}

/// The connection's producer is known: the client hears what was taken.
fn finish(state: &SharedState, handle: &ConnectionHandle, link: Link) {
    let reply = outbox_reply(link.start, &state.node_id);
    let _ = handle.out.acks.link.set(link);
    handle.send_control(Message::Text(reply.into()));
}

/// The server's answer to a client's outbox hello: what was taken of it,
/// and the name this node goes by.
fn outbox_reply(acked: u64, node: &str) -> String {
    serde_json::json!({"c": "WSE", "t": "outbox", "p": {"acked": acked, "node": node}, "v": 1})
        .to_string()
}

/// A message's number in its producer's outbox, when it has one.
pub(crate) fn seq_of(val: &serde_json::Value) -> Option<u64> {
    val.get("s")
        .and_then(serde_json::Value::as_u64)
        .filter(|s| *s > 0 && *s & MOVED == 0)
}

/// What a connection has to answer since its last answer went.
#[derive(Default)]
pub(crate) struct Pending {
    /// The last message read on this connection; 0 before the first.
    last: u64,
    /// The highest message taken since the last answer; 0: none.
    ack: u64,
    /// Messages refused since the last answer: first, last, why.
    nacks: Vec<(u64, u64, &'static str)>,
    /// Frames the rate limit dropped since the last message.
    dropped: bool,
}

impl Pending {
    /// Something to answer.
    pub(crate) fn due(&self) -> bool {
        self.ack > 0 || !self.nacks.is_empty()
    }

    /// The rate limit dropped a frame before it was read.
    pub(crate) fn dropped(&mut self) {
        self.dropped = true;
    }

    /// Message `s` came; what came before it on this connection and was not
    /// read (dropped by the rate limit, or never sent) is refused.
    pub(crate) fn came(&mut self, s: u64, start: u64) {
        let last = if self.last == 0 { start } else { self.last };
        if s > last + 1 {
            let why = if std::mem::take(&mut self.dropped) {
                "rate_limited"
            } else {
                "missing"
            };
            self.refuse_range(last + 1, s - 1, why);
        }
        self.last = self.last.max(s);
    }

    /// Message `s` was taken (or was already).
    pub(crate) fn taken(&mut self, s: u64) {
        self.ack = self.ack.max(s);
    }

    /// Message `s` is refused for `why`.
    pub(crate) fn refuse(&mut self, s: u64, why: &'static str) {
        self.refuse_range(s, s, why);
    }

    fn refuse_range(&mut self, from: u64, to: u64, why: &'static str) {
        if let Some(last) = self.nacks.last_mut()
            && last.2 == why
            && last.1 + 1 == from
        {
            last.1 = to;
            return;
        }
        self.nacks.push((from, to, why));
    }

    /// The answers, refusals first: an `ack` covers every message up to its
    /// number, and the client must hear of a refused one before.
    pub(crate) fn answers(&mut self) -> impl Iterator<Item = String> + '_ {
        let ack = std::mem::take(&mut self.ack);
        self.nacks
            .drain(..)
            .map(|(from, to, why)| nack(from, to, why))
            .chain((ack > 0).then(|| format!(r#"{{"c":"WSE","t":"ack","p":{{"s":{ack}}},"v":1}}"#)))
    }
}

fn nack(from: u64, to: u64, why: &str) -> String {
    if from == to {
        format!(r#"{{"c":"WSE","t":"nack","p":{{"s":{to},"code":"{why}"}},"v":1}}"#)
    } else {
        format!(r#"{{"c":"WSE","t":"nack","p":{{"from":{from},"s":{to},"code":"{why}"}},"v":1}}"#)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_is_handed_on_once() {
        let slot = Slot::new(0);
        assert_eq!(slot.claim(1), Claim::First);
        assert_eq!(slot.claim(1), Claim::Repeat);
        assert_eq!(slot.claim(3), Claim::First);
        assert_eq!(slot.claim(2), Claim::Repeat, "behind the mark");
        assert_eq!(slot.hand_over(), 3);
        assert_eq!(slot.claim(4), Claim::Moved);
        assert_eq!(slot.taken(), 3);
    }

    #[test]
    fn a_producer_is_one_users() {
        let producers = Producers::default();
        let (ann, known) = producers.attach(Some("ann"), "p1", 0);
        assert!(!known);
        ann.claim(5);
        let (again, known) = producers.attach(Some("ann"), "p1", 0);
        assert!(known);
        assert_eq!(again.taken(), 5);
        let (bob, known) = producers.attach(Some("bob"), "p1", 0);
        assert!(!known, "another user's producer of the same name");
        assert_eq!(bob.taken(), 0);
        // Handed over to another node: what comes back here starts over.
        assert_eq!(producers.hand_over(Some("ann"), "p1"), Some(5));
        assert_eq!(producers.hand_over(Some("cy"), "p1"), None);
        let (back, known) = producers.attach(Some("ann"), "p1", 0);
        assert!(!known);
        assert_eq!(back.taken(), 0);
    }

    #[test]
    fn a_silent_producer_is_forgotten_after_a_day() {
        let producers = Producers::default();
        producers.attach(None, "p", 0);
        producers.sweep(KEPT_FOR_MS - 1);
        assert_eq!(producers.slots.len(), 1);
        producers.sweep(KEPT_FOR_MS);
        assert_eq!(producers.slots.len(), 0);
    }

    #[test]
    fn the_hello_names_the_producer_and_what_was_written() {
        let p = serde_json::json!({"outbox": {"producer": "abc", "node": "n1", "written": 7}});
        assert_eq!(
            Hello::parse(Some(&p)),
            Some(Hello {
                producer: "abc".into(),
                node: Some("n1".into()),
                written: 7
            })
        );
        let long = "x".repeat(MAX_ID_LEN + 1);
        let p = serde_json::json!({"outbox": {"producer": long}});
        assert_eq!(Hello::parse(Some(&p)), None);
        assert_eq!(Hello::parse(Some(&serde_json::json!({}))), None);
    }

    fn hello(node: &str, written: u64) -> Hello {
        Hello {
            producer: "p".into(),
            node: Some(node.into()),
            written,
        }
    }

    #[test]
    fn a_client_from_another_node_is_unsure_up_to_what_it_wrote() {
        let producers = Producers::default();
        let link = producers.link(Some("ann"), &hello("other", 4), false, 0);
        assert_eq!((link.start, link.unsure_until), (0, 4));

        // Back on this node, which took messages of it: they are known.
        link.slot.claim(9);
        let link = producers.link(Some("ann"), &hello("here", 11), true, 0);
        assert_eq!((link.start, link.unsure_until), (9, 0));
    }

    #[test]
    fn what_the_last_node_took_counts_here() {
        let producers = Producers::default();
        let link = producers.taken_over(Some("ann"), &hello("other", 12), Some(10), 0);
        assert_eq!((link.start, link.unsure_until), (10, 0));
        assert_eq!(link.slot.claim(10), Claim::Repeat);
        assert_eq!(link.slot.claim(11), Claim::First);
        // The last node did not answer: unsure up to what was written.
        let link = producers.taken_over(Some("bob"), &hello("other", 12), None, 0);
        assert_eq!((link.start, link.unsure_until), (0, 12));
    }

    #[test]
    fn the_hello_starts_the_producer_once() {
        let holder = Holder::default();
        assert!(holder.begin().is_none(), "no hello yet");
        holder.hello(Some(&serde_json::json!({"outbox": {"producer": "p"}})));
        assert!(holder.begin().is_some());
        assert!(holder.begin().is_none(), "once");
    }

    #[test]
    fn answers_put_refusals_before_the_ack() {
        let mut pending = Pending::default();
        pending.came(1, 0);
        pending.taken(1);
        pending.dropped();
        pending.came(4, 0);
        pending.taken(4);
        pending.came(5, 0);
        pending.refuse(5, "busy");
        let answers: Vec<String> = pending.answers().collect();
        assert_eq!(
            answers,
            [
                r#"{"c":"WSE","t":"nack","p":{"from":2,"s":3,"code":"rate_limited"},"v":1}"#,
                r#"{"c":"WSE","t":"nack","p":{"s":5,"code":"busy"},"v":1}"#,
                r#"{"c":"WSE","t":"ack","p":{"s":4},"v":1}"#,
            ]
        );
        assert!(!pending.due());
    }
}
