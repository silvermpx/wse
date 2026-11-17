//! Delivery of one message to many connections (broadcast, topics, queue
//! groups), topic patterns and recovery stamping.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use ahash::AHashSet;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::e2e::{e2e_frame, e2e_message};
use super::frames::{encode_ws_frame, ws_frame_size};
use super::state::Seal;
use super::{ConnectionHandle, QueueGroup, Tally, WsFrame};
use crate::cluster::ClusterMetrics;
use crate::msgpack::serde_json_to_rmpv;

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pb = pattern.as_bytes();
    let tb = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);

    while ti < tb.len() {
        if pi < pb.len() && (pb[pi] == b'?' || pb[pi] == tb[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pb.len() && pb[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pb.len() && pb[pi] == b'*' {
        pi += 1;
    }
    pi == pb.len()
}

// ---------------------------------------------------------------------------
// Topic ACL
// ---------------------------------------------------------------------------

/// A connection's topic access control list, from the JWT `wse_topics`
/// claim or `set_topic_acl()`.
///
/// Format in JWT: `{"wse_topics": {"allow": ["chat.*"], "deny": ["admin.*"]}}`
///
/// Rules:
/// - Deny takes precedence over allow
/// - Empty allow list = allow all (backward compatible)
/// - Patterns use glob matching (*, ?)
#[derive(Debug, Clone)]
pub(crate) struct TopicAcl {
    pub(crate) allow: Vec<String>,
    pub(crate) deny: Vec<String>,
}

impl TopicAcl {
    /// An ACL that allows no topic at all.
    pub(crate) fn deny_all() -> Self {
        Self {
            allow: Vec::new(),
            deny: vec!["*".to_string()],
        }
    }

    /// Check if a topic is allowed by this ACL.
    ///
    /// Deny rules take precedence. Empty allow = allow all.
    pub(crate) fn is_allowed(&self, topic: &str) -> bool {
        // Check deny first (deny takes precedence)
        for pattern in &self.deny {
            if glob_match(pattern, topic) {
                return false;
            }
        }

        // Empty allow = allow all
        if self.allow.is_empty() {
            return true;
        }

        // Check allow patterns
        for pattern in &self.allow {
            if glob_match(pattern, topic) {
                return true;
            }
        }

        false
    }
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

/// A message going to many connections: what the application wrote, and its
/// frame, built once and shared by every connection that gets it as is.
pub(crate) struct Outgoing {
    payload: Bytes,
    frame: Bytes,
    /// JSON text, which the server may send as msgpack.
    text: bool,
    /// The message as `M:` msgpack and its frame, for the connections that
    /// asked for msgpack: made on first use, then shared by all of them.
    /// None when the text is not JSON: they get it as is then.
    packed: OnceLock<Option<(Bytes, Bytes)>>,
}

impl Outgoing {
    pub(crate) fn text(payload: Bytes) -> Self {
        let frame = encode_ws_frame(0x01, &payload);
        Self {
            payload,
            frame,
            text: true,
            packed: OnceLock::new(),
        }
    }

    pub(crate) fn binary(payload: Bytes) -> Self {
        let frame = encode_ws_frame(0x02, &payload);
        Self {
            payload,
            frame,
            text: false,
            packed: OnceLock::new(),
        }
    }

    /// The payload and frame `h` gets: msgpack when it asked for msgpack.
    fn for_handle(&self, h: &ConnectionHandle) -> (&Bytes, &Bytes) {
        if h.msgpack
            && self.text
            && let Some((payload, frame)) = self.packed.get_or_init(|| {
                let payload = to_msgpack(&self.payload)?;
                let frame = encode_ws_frame(0x02, &payload);
                Some((payload, frame))
            })
        {
            return (payload, frame);
        }
        (&self.payload, &self.frame)
    }
}

/// JSON text as an `M:` msgpack message, what a client that asked for msgpack
/// reads; None when the text is not JSON.
pub(crate) fn to_msgpack(json: &[u8]) -> Option<Bytes> {
    let value: serde_json::Value = serde_json::from_slice(json).ok()?;
    let mut buf = Vec::with_capacity(json.len());
    buf.extend_from_slice(b"M:");
    rmpv::encode::write_value(&mut buf, &serde_json_to_rmpv(&value)).ok()?;
    Some(Bytes::from(buf))
}

impl ConnectionHandle {
    /// Deliver `msg` to this connection: sealed with its E2E key when it has
    /// one (an `E:` frame of its own), else the shared frame. Every path that
    /// sends one message to many connections comes through here, so none
    /// skips a connection's encryption. A connection over its outbound budget
    /// drops it (counted in `slow_drops`). True when queued.
    pub(crate) fn deliver(
        &self,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> bool {
        if !self.has_room(max_pending, slow_drops) {
            return false;
        }
        let (payload, frame) = msg.for_handle(self);
        match self.out.cipher.get() {
            None => self.push_frame(frame),
            Some(Seal::Legacy(cipher)) => match e2e_frame(cipher, payload) {
                Some(frame) => self.push_frame(&frame),
                None => return false,
            },
            Some(Seal::Session) => return self.queue(WsFrame::Seal(payload.clone())),
        }
        true
    }

    /// Whether a delivery to this connection would be queued: it is open and
    /// within its outbound budget (over it, counted in `slow_drops`).
    fn has_room(&self, max_pending: usize, slow_drops: &AtomicU64) -> bool {
        if self.out.closed.load(Ordering::Relaxed) {
            return false;
        }
        if max_pending > 0 && self.out.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Queue a frame on the control queue, counted in the outbound budget.
    fn queue(&self, frame: WsFrame) -> bool {
        let bytes = ws_frame_size(&frame);
        self.out.pending.fetch_add(bytes, Ordering::Relaxed);
        if self.tx.send(frame).is_err() {
            self.out.pending.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Queue a message the server makes on its own (a ping, a reply, an
    /// error): sealed in an E2E session, where the client accepts nothing
    /// else, as is otherwise (older E2E clients take these in plaintext).
    pub(crate) fn send_control(&self, msg: Message) -> bool {
        match (self.out.cipher.get(), msg) {
            (Some(Seal::Session), Message::Text(text)) => {
                self.queue(WsFrame::Seal(Bytes::from(text)))
            }
            (Some(Seal::Session), Message::Binary(data)) => self.queue(WsFrame::Seal(data)),
            (_, msg) => self.queue(WsFrame::Msg(msg)),
        }
    }

    /// Send one message to this connection alone, in order with its other
    /// direct sends: sealed with its E2E key when it has one. A connection
    /// over its outbound budget drops it (counted in `slow_drops`). True
    /// when queued.
    pub(crate) fn send_one(
        &self,
        msg: Message,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> bool {
        if max_pending > 0 && self.out.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        self.send_owed(msg)
    }

    /// [`send_one`](Self::send_one) of a message the client waits for (the
    /// answer to its request): never dropped for the outbound budget. One
    /// for each request it made; a connection that stays over its budget is
    /// closed as too slow to read.
    pub(crate) fn send_owed(&self, msg: Message) -> bool {
        // JSON text in msgpack, for a connection that asked for msgpack.
        let msg = match msg {
            Message::Text(text) if self.msgpack => match to_msgpack(text.as_bytes()) {
                Some(packed) => Message::Binary(packed),
                None => Message::Text(text),
            },
            msg => msg,
        };
        let frame = match (self.out.cipher.get(), msg) {
            (Some(Seal::Legacy(cipher)), Message::Text(text)) => {
                match e2e_message(cipher, text.as_bytes()) {
                    Some(sealed) => WsFrame::Msg(sealed),
                    None => return false,
                }
            }
            (Some(Seal::Legacy(cipher)), Message::Binary(data)) => match e2e_message(cipher, &data)
            {
                Some(sealed) => WsFrame::Msg(sealed),
                None => return false,
            },
            (Some(Seal::Session), Message::Text(text)) => WsFrame::Seal(Bytes::from(text)),
            (Some(Seal::Session), Message::Binary(data)) => WsFrame::Seal(data),
            (_, msg) => WsFrame::Msg(msg),
        };
        self.queue(frame)
    }

    /// Append a frame to the broadcast buffer; the write task is woken when
    /// the buffer was empty.
    fn push_frame(&self, frame: &Bytes) {
        let out = &*self.out;
        out.pending.fetch_add(frame.len(), Ordering::Relaxed);
        let was_empty = {
            let mut queue = out.frames.lock();
            let empty = queue.is_empty();
            queue.push(frame);
            empty
        };
        if was_empty {
            out.notify.notify_one();
        }
    }
}

// ---------------------------------------------------------------------------
// Lanes: delivery on every core
// ---------------------------------------------------------------------------

/// A lane's connections by id.
type ConnMap = HashMap<String, ConnectionHandle, ahash::RandomState>;

/// At most this many lanes: a topic's lanes are the bits of a `u64`.
const MAX_LANES: usize = 64;

/// How many jobs a lane's task takes off its queue at once.
const LANE_BATCH: usize = 64;

/// A connection with this many bytes still to write is behind.
pub(crate) const BEHIND_BYTES: usize = 64 * 1024;

/// When most connections a delivery reached were behind, the lane waits
/// until one of its connections has written down to `BEHIND_BYTES` (its
/// write task wakes the lane), at most `LANE_PACE`: the lane goes on at the
/// pace the sockets write, not further ahead. Its queue fills meanwhile, and
/// then the senders wait in turn. Only a delivery that reaches at least
/// `PACE_MIN` connections counts, so a slow client or two never hold back
/// their lane. A connection that is closed or over its own budget misses the
/// message: it counts among those reached, never among those behind, so
/// clients that read nothing hold their lane back only until their budget is
/// full.
const LANE_PACE: std::time::Duration = std::time::Duration::from_millis(1);
const PACE_MIN: usize = 32;

/// What a lane's job did.
#[derive(Default, Clone, Copy)]
struct Served {
    delivered: u64,
    dropped: u64,
    /// The connections the message was queued for that were behind before.
    behind: usize,
}

impl Served {
    fn deliver(
        &mut self,
        h: &ConnectionHandle,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) {
        let behind = h.out.pending.load(Ordering::Relaxed) >= BEHIND_BYTES;
        if h.deliver(msg, max_pending, slow_drops) {
            self.delivered += 1;
            self.behind += usize::from(behind);
        } else {
            self.dropped += 1;
        }
    }

    /// The message reached many connections, and most of those it was
    /// queued for were behind.
    fn lagging(&self) -> bool {
        let reached = (self.delivered + self.dropped) as usize;
        reached >= PACE_MIN && self.behind * 2 > self.delivered as usize
    }
}

#[cfg(test)]
mod pacing_tests {
    use super::*;

    #[test]
    fn a_closed_or_full_connection_never_holds_the_lane_back() {
        let drops = AtomicU64::new(0);
        let msg = Outgoing::text(bytes::Bytes::from_static(b"{}"));
        let lagging = |max_pending: usize, closed: bool| {
            let mut served = Served::default();
            for _ in 0..PACE_MIN {
                let (h, _rx) = super::super::tests::test_handle();
                h.out.pending.store(BEHIND_BYTES, Ordering::Relaxed);
                h.out.closed.store(closed, Ordering::Relaxed);
                served.deliver(&h, &msg, max_pending, &drops);
            }
            served.lagging()
        };
        // Behind and still taking messages: the lane waits for them.
        assert!(lagging(0, false));
        // Over their budget: the message is dropped for them, nothing to wait for.
        assert!(!lagging(BEHIND_BYTES, false));
        // Gone: nothing to wait for either.
        assert!(!lagging(0, true));
    }

    #[test]
    fn many_reached_and_most_takers_behind_hold_the_lane_back() {
        let drops = AtomicU64::new(0);
        let msg = Outgoing::text(bytes::Bytes::from_static(b"{}"));
        let mut served = Served::default();
        // Most of the lane's clients are over their budget, the rest behind:
        // too few take the message to count alone, all of them reached do.
        for i in 0..PACE_MIN {
            let (h, _rx) = super::super::tests::test_handle();
            let full = i % 3 != 0;
            h.out.pending.store(
                if full { 2 * BEHIND_BYTES } else { BEHIND_BYTES },
                Ordering::Relaxed,
            );
            served.deliver(&h, &msg, 2 * BEHIND_BYTES, &drops);
        }
        assert!((served.delivered as usize) < PACE_MIN);
        assert!(served.lagging());
    }
}

/// How many jobs wait for a lane at most. A sender that finds the queue full
/// waits for room: a publisher faster than the delivery then backs up in its
/// own queue, a message each, not in a job for every lane.
pub(super) const LANE_QUEUE: usize = 1024;

/// How many lanes the server delivers on: one per core, at most `MAX_LANES`.
pub(crate) fn lane_count() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(MAX_LANES)
}

/// The lanes whose bits are set in `mask`, lowest first.
fn lanes_of(mut mask: u64) -> impl Iterator<Item = usize> {
    std::iter::from_fn(move || {
        (mask != 0).then(|| {
            let lane = mask.trailing_zeros() as usize;
            mask &= mask - 1;
            lane
        })
    })
}

/// A topic's subscribers, by lane: each lane's task delivers to its own.
#[derive(Default)]
pub(crate) struct Subscribers {
    /// One map for each lane that has subscribers, in lane order.
    parts: Vec<ConnMap>,
    /// Those lanes, one bit each: a lane's map is at the count of the set
    /// bits below its own.
    pub(super) mask: u64,
    len: usize,
}

impl Subscribers {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    fn slot(&self, lane: u8) -> usize {
        (self.mask & ((1u64 << lane) - 1)).count_ones() as usize
    }

    fn part(&self, lane: u8) -> Option<&ConnMap> {
        (self.mask & (1u64 << lane) != 0).then(|| &self.parts[self.slot(lane)])
    }

    /// Add a connection, on its lane; true when it was not there yet.
    fn insert(&mut self, conn_id: &str, handle: &ConnectionHandle) -> bool {
        let lane = handle.lane;
        let slot = self.slot(lane);
        if self.mask & (1u64 << lane) == 0 {
            self.parts.insert(slot, ConnMap::default());
            self.mask |= 1u64 << lane;
        }
        let added = self.parts[slot]
            .insert(conn_id.to_owned(), handle.clone())
            .is_none();
        self.len += usize::from(added);
        added
    }

    /// Remove a connection; true when it was there.
    fn remove(&mut self, conn_id: &str) -> bool {
        let Some(slot) = self
            .parts
            .iter_mut()
            .position(|part| part.remove(conn_id).is_some())
        else {
            return false;
        };
        self.len -= 1;
        if self.parts[slot].is_empty() {
            self.parts.remove(slot);
            // The slot-th set bit is that lane's.
            let mut rest = self.mask;
            for _ in 0..slot {
                rest &= rest - 1;
            }
            self.mask &= !(rest & rest.wrapping_neg());
        }
        true
    }
}

/// Who gets a topic's messages: its subscribers, and the subscribers of the
/// patterns (names with `*` or `?`) matching it; and who gets a broadcast:
/// every connection. Kept by lane. Shared by the server and the cluster's
/// inbound links.
#[derive(Clone)]
pub(crate) struct TopicIndex {
    pub(crate) subscribers: Arc<DashMap<String, Subscribers>>,
    /// The subscribed names that are patterns, by bucket (`pattern_bucket`):
    /// a message looks only into the buckets its topic can match. Changed
    /// only under the shard lock of the pattern's `subscribers` entry, so the
    /// two never disagree.
    patterns: Arc<DashMap<String, Vec<String>>>,
    /// How many patterns the buckets hold: none, no lookup.
    pattern_count: Arc<AtomicUsize>,
    /// Per-connection topic ACLs: a pattern subscription gets only the topics
    /// its connection's ACL allows.
    pub(crate) acls: Arc<DashMap<String, TopicAcl>>,
    /// Every connection, by lane.
    everyone: Arc<[parking_lot::RwLock<ConnMap>]>,
    /// The lanes that have connections, one bit each: what a broadcast
    /// goes to. Changed under the lane's write lock.
    pub(super) occupied: Arc<AtomicU64>,
    /// Wakes a waiting lane: one of its connections wrote down to
    /// `BEHIND_BYTES`.
    drained: Arc<[Arc<tokio::sync::Notify>]>,
    /// Counts the connections that joined, to give each the next lane.
    joined: Arc<AtomicUsize>,
}

pub(crate) fn is_pattern(topic: &str) -> bool {
    topic.contains(['*', '?'])
}

/// The characters that part a topic name (`user:42:orders`, `prices.btc`).
fn is_separator(b: u8) -> bool {
    matches!(b, b':' | b'.' | b'/')
}

/// A pattern's bucket: its literal start (up to the first `*` or `?`), cut
/// after its last separator. A topic it matches starts with that literal, so
/// the bucket is the topic's start up to one of its separators, or empty:
/// a message looks into those buckets only, not at every pattern.
fn pattern_bucket(pattern: &str) -> &str {
    let literal = &pattern[..pattern.find(['*', '?']).unwrap_or(pattern.len())];
    match literal.bytes().rposition(is_separator) {
        Some(i) => &literal[..=i],
        None => "",
    }
}

impl TopicIndex {
    /// An index over `lanes` lanes (1 to `MAX_LANES`).
    pub(crate) fn new(lanes: usize) -> Self {
        Self {
            subscribers: Arc::default(),
            patterns: Arc::default(),
            pattern_count: Arc::default(),
            acls: Arc::default(),
            everyone: (0..lanes.clamp(1, MAX_LANES))
                .map(|_| parking_lot::RwLock::new(ConnMap::default()))
                .collect(),
            occupied: Arc::default(),
            drained: (0..lanes.clamp(1, MAX_LANES))
                .map(|_| Arc::new(tokio::sync::Notify::new()))
                .collect(),
            joined: Arc::default(),
        }
    }

    pub(crate) fn lanes(&self) -> usize {
        self.everyone.len()
    }

    /// How many distinct patterns are subscribed.
    #[cfg(test)]
    pub(crate) fn pattern_count(&self) -> usize {
        self.pattern_count.load(Ordering::Relaxed)
    }

    /// What a connection's write task wakes when it has written down to
    /// `BEHIND_BYTES`: its lane may be waiting for it.
    pub(crate) fn drained(&self, lane: u8) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.drained[usize::from(lane)])
    }

    /// The lane for a new connection: each in turn.
    pub(crate) fn next_lane(&self) -> u8 {
        (self.joined.fetch_add(1, Ordering::Relaxed) % self.lanes()) as u8
    }

    /// A connection joins its lane: broadcasts reach it from now on.
    pub(crate) fn join(&self, conn_id: &str, handle: &ConnectionHandle) {
        let mut conns = self.everyone[usize::from(handle.lane)].write();
        conns.insert(conn_id.to_owned(), handle.clone());
        self.occupied
            .fetch_or(1u64 << handle.lane, Ordering::Release);
    }

    /// A connection leaves its lane.
    pub(crate) fn leave(&self, conn_id: &str, lane: u8) {
        let mut conns = self.everyone[usize::from(lane)].write();
        conns.remove(conn_id);
        if conns.is_empty() {
            self.occupied.fetch_and(!(1u64 << lane), Ordering::Release);
        }
    }

    /// Add `conn_id` to `topic`; true when it was not subscribed yet.
    pub(crate) fn subscribe(&self, topic: &str, conn_id: &str, handle: &ConnectionHandle) -> bool {
        let mut subs = self.subscribers.entry(topic.to_owned()).or_default();
        let added = subs.insert(conn_id, handle);
        if added && subs.len == 1 && is_pattern(topic) {
            self.patterns
                .entry(pattern_bucket(topic).to_owned())
                .or_default()
                .push(topic.to_owned());
            self.pattern_count.fetch_add(1, Ordering::Relaxed);
        }
        added
    }

    /// Remove `conn_id` from `topic`; true when the topic has no subscriber
    /// left.
    pub(crate) fn unsubscribe(&self, topic: &str, conn_id: &str) -> bool {
        if let dashmap::Entry::Occupied(mut subs) = self.subscribers.entry(topic.to_owned()) {
            subs.get_mut().remove(conn_id);
            if subs.get().len == 0 {
                subs.remove();
                if is_pattern(topic) {
                    let bucket = pattern_bucket(topic);
                    if let Some(mut patterns) = self.patterns.get_mut(bucket) {
                        patterns.retain(|p| p != topic);
                    }
                    self.patterns
                        .remove_if(bucket, |_, patterns| patterns.is_empty());
                    self.pattern_count.fetch_sub(1, Ordering::Relaxed);
                }
                return true;
            }
        }
        false
    }

    /// Where a message on `topic` goes: the subscribed patterns matching it,
    /// and the lanes with a subscriber of the topic or of those patterns.
    fn reach(&self, topic: &str) -> (Vec<String>, u64) {
        let mut mask = self.subscribers.get(topic).map_or(0, |subs| subs.mask);
        if self.pattern_count.load(Ordering::Relaxed) == 0 {
            return (Vec::new(), mask);
        }
        // Collected first: no lock of `patterns` is held while `subscribers`
        // is read (subscribe takes them the other way round).
        let mut matching: Vec<String> = Vec::new();
        let mut look = |bucket: &str| {
            if let Some(patterns) = self.patterns.get(bucket) {
                matching.extend(
                    patterns
                        .iter()
                        .filter(|pattern| pattern.as_str() != topic && glob_match(pattern, topic))
                        .cloned(),
                );
            }
        };
        look("");
        for (i, b) in topic.bytes().enumerate() {
            if is_separator(b) {
                look(&topic[..=i]);
            }
        }
        for pattern in &matching {
            mask |= self.subscribers.get(pattern).map_or(0, |subs| subs.mask);
        }
        (matching, mask)
    }

    /// Deliver `msg` to every connection on `lane`.
    fn deliver_all(
        &self,
        lane: u8,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> Served {
        let mut served = Served::default();
        for h in self.everyone[usize::from(lane)].read().values() {
            served.deliver(h, msg, max_pending, slow_drops);
        }
        served
    }

    /// Deliver `msg` on `topic` to the subscribers on `lane`: the topic's,
    /// and those of `patterns` (the patterns matching it) their ACL allows.
    /// A connection subscribed several ways gets it once; `seen` is scratch
    /// space for that.
    #[allow(clippy::too_many_arguments)]
    fn deliver_topic(
        &self,
        lane: u8,
        topic: &str,
        patterns: &[String],
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
        seen: &mut AHashSet<usize>,
    ) -> Served {
        let mut served = Served::default();
        if patterns.is_empty() {
            // Exact topics only: the common case, no bookkeeping.
            if let Some(subs) = self.subscribers.get(topic)
                && let Some(part) = subs.part(lane)
            {
                for h in part.values() {
                    served.deliver(h, msg, max_pending, slow_drops);
                }
            }
            return served;
        }
        // Connections already served, by the identity of their queue.
        seen.clear();
        let id = |h: &ConnectionHandle| Arc::as_ptr(&h.out) as usize;
        if let Some(subs) = self.subscribers.get(topic)
            && let Some(part) = subs.part(lane)
        {
            for h in part.values() {
                if seen.insert(id(h)) {
                    served.deliver(h, msg, max_pending, slow_drops);
                }
            }
        }
        for pattern in patterns {
            let Some(subs) = self.subscribers.get(pattern) else {
                continue;
            };
            let Some(part) = subs.part(lane) else {
                continue;
            };
            for (conn_id, h) in part {
                let allowed = self
                    .acls
                    .get(conn_id)
                    .is_none_or(|acl| acl.is_allowed(topic));
                if allowed && seen.insert(id(h)) {
                    served.deliver(h, msg, max_pending, slow_drops);
                }
            }
        }
        served
    }

    /// Deliver `msg` on `topic` here and now, every lane in turn: what the
    /// lanes' tasks do between them. Returns (delivered, dropped).
    #[cfg(test)]
    pub(crate) fn fanout(
        &self,
        topic: &str,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> (u64, u64) {
        let (patterns, mask) = self.reach(topic);
        let mut seen = AHashSet::new();
        lanes_of(mask).fold((0, 0), |(delivered, dropped), lane| {
            let served = self.deliver_topic(
                lane as u8,
                topic,
                &patterns,
                msg,
                max_pending,
                slow_drops,
                &mut seen,
            );
            (delivered + served.delivered, dropped + served.dropped)
        })
    }
}

/// A delivery for one lane's task.
enum Job {
    /// To every connection on the lane.
    All(Arc<Outgoing>),
    /// To the lane's subscribers of a topic.
    Topic(Arc<TopicMessage>),
    /// To one connection on the lane: a queue group's member.
    One(ConnectionHandle, Arc<Outgoing>),
}

/// A message on a topic, shared by the lanes it goes to.
struct TopicMessage {
    topic: String,
    /// The subscribed patterns matching the topic.
    patterns: Vec<String>,
    msg: Outgoing,
    /// Where its deliveries are counted, for a message from a cluster peer.
    tally: Option<Arc<ClusterMetrics>>,
}

/// Delivery on every core. Each connection belongs to one lane, and each lane
/// has a task of its own delivering to its connections. A message goes to
/// the lanes that have someone to get it, in the order it is handed over
/// here, so a connection gets the messages from one sender in order.
#[derive(Clone)]
pub(crate) struct Fanout {
    index: TopicIndex,
    lanes: Arc<[mpsc::Sender<Job>]>,
}

/// Queue `job` for `lane`, waiting for room when the lane is behind.
async fn hand(lane: &mpsc::Sender<Job>, job: Job) {
    if let Err(mpsc::error::TrySendError::Full(job)) = lane.try_send(job) {
        let _ = lane.send(job).await;
    }
}

impl Fanout {
    /// Start a task for each lane of `index` on the current runtime.
    pub(crate) fn start(index: TopicIndex, max_pending: usize, slow_drops: Arc<Tally>) -> Self {
        let lanes = (0..index.lanes())
            .map(|lane| {
                let (tx, rx) = mpsc::channel(LANE_QUEUE);
                tokio::spawn(run_lane(
                    lane as u8,
                    rx,
                    index.clone(),
                    max_pending,
                    Arc::clone(&slow_drops),
                ));
                tx
            })
            .collect();
        Self { index, lanes }
    }

    /// How many more jobs `lane`'s queue takes.
    #[cfg(test)]
    pub(super) fn room(&self, lane: usize) -> usize {
        self.lanes[lane].capacity()
    }

    /// Deliver `msg` to every connection.
    pub(crate) async fn broadcast(&self, msg: Outgoing) {
        let occupied = self.index.occupied.load(Ordering::Acquire);
        if occupied == 0 {
            return;
        }
        let msg = Arc::new(msg);
        for lane in lanes_of(occupied) {
            hand(&self.lanes[lane], Job::All(Arc::clone(&msg))).await;
        }
    }

    /// Deliver a message on `topic` to its subscribers and to those of the
    /// patterns matching it (see `TopicIndex::deliver_topic`); `msg` makes
    /// it, only when someone is subscribed. The deliveries are counted in
    /// `tally` when given.
    pub(crate) async fn publish(
        &self,
        topic: String,
        msg: impl FnOnce() -> Outgoing,
        tally: Option<&Arc<ClusterMetrics>>,
    ) {
        let (patterns, mask) = self.index.reach(&topic);
        if mask == 0 {
            return;
        }
        let message = Arc::new(TopicMessage {
            topic,
            patterns,
            msg: msg(),
            tally: tally.cloned(),
        });
        for lane in lanes_of(mask) {
            hand(&self.lanes[lane], Job::Topic(Arc::clone(&message))).await;
        }
    }

    /// Deliver a message on `topic` to one member of each of its queue
    /// groups, round-robin; a member that is gone or over its outbound budget
    /// is passed over. `msg` makes the message, only when there is a group.
    /// Returns (delivered, dropped).
    pub(crate) async fn queue_groups(
        &self,
        queue_groups: &DashMap<String, DashMap<String, QueueGroup>>,
        topic: &str,
        msg: impl FnOnce() -> Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> (u64, u64) {
        // The members are chosen under the map's lock, and handed to their
        // lanes after it is let go: no lock is held while waiting for room.
        let mut chosen = Vec::new();
        let mut dropped = 0u64;
        {
            let Some(topic_groups) = queue_groups.get(topic) else {
                return (0, 0);
            };
            for group in topic_groups.iter() {
                let len = group.members.len();
                if len == 0 {
                    continue;
                }
                let base = group.next.fetch_add(1, Ordering::Relaxed);
                let member = (0..len)
                    .map(|attempt| &group.members[(base + attempt) % len].1)
                    .find(|h| h.has_room(max_pending, slow_drops));
                match member {
                    Some(h) => chosen.push(h.clone()),
                    None => dropped += 1,
                }
            }
        }
        if !chosen.is_empty() {
            let msg = Arc::new(msg());
            for h in &chosen {
                hand(
                    &self.lanes[usize::from(h.lane)],
                    Job::One(h.clone(), Arc::clone(&msg)),
                )
                .await;
            }
        }
        (chosen.len() as u64, dropped)
    }
}

/// A lane's task: the deliveries handed to the lane, in order.
async fn run_lane(
    lane: u8,
    mut jobs: mpsc::Receiver<Job>,
    index: TopicIndex,
    max_pending: usize,
    slow_drops: Arc<Tally>,
) {
    let mut batch = Vec::with_capacity(LANE_BATCH);
    let mut seen = AHashSet::new();
    let drained = index.drained(lane);
    // The lane's own count of what it dropped: no other lane writes it.
    let slow_drops = slow_drops.shard(usize::from(lane));
    while jobs.recv_many(&mut batch, LANE_BATCH).await > 0 {
        // Whether the lane served anyone since it last waited.
        let mut served_any = false;
        for job in batch.drain(..) {
            let served = match job {
                Job::All(msg) => index.deliver_all(lane, &msg, max_pending, slow_drops),
                Job::Topic(message) => {
                    let served = index.deliver_topic(
                        lane,
                        &message.topic,
                        &message.patterns,
                        &message.msg,
                        max_pending,
                        slow_drops,
                        &mut seen,
                    );
                    if let Some(metrics) = &message.tally {
                        metrics.add_delivered(served.delivered);
                        if served.dropped > 0 {
                            metrics.add_dropped(served.dropped);
                        }
                    }
                    served
                }
                Job::One(h, msg) => {
                    let mut served = Served::default();
                    served.deliver(&h, &msg, max_pending, slow_drops);
                    served
                }
            };
            served_any = true;
            if served.lagging() {
                // The sockets are behind: they write before more is queued.
                tokio::select! {
                    () = drained.notified() => {}
                    () = tokio::time::sleep(LANE_PACE) => {}
                }
                served_any = false;
            }
        }
        // The write tasks the batch woke wait in this thread's run queue:
        // they run, and the runtime reads the network, before the next
        // batch. Giving way more often, every few hundred connections, cost
        // a sixth of the throughput at 10000 subscribers: after a yield the
        // lane waits for its thread's whole run queue.
        if served_any {
            tokio::task::yield_now().await;
        }
    }
}

/// Count a message on `topic` (the Prometheus top-N topics); a topic seen
/// before is counted without making a key.
pub(crate) fn count_message(counts: &DashMap<String, AtomicU64>, topic: &str) {
    match counts.get(topic) {
        Some(count) => count.fetch_add(1, Ordering::Relaxed),
        None => counts
            .entry(topic.to_owned())
            .or_default()
            .fetch_add(1, Ordering::Relaxed),
    };
}

/// A recovery-stamped message without its stamp: what queue-group members
/// get, since each sees only some of the topic's offsets and would take the
/// rest for gaps. Messages without a stamp come back unchanged.
pub(crate) fn unstamped(payload: &Bytes) -> Bytes {
    let Ok(text) = std::str::from_utf8(payload) else {
        return payload.clone();
    };
    let Some(rest) = text.strip_prefix("{\"tp\":") else {
        return payload.clone();
    };
    // The topic is a JSON string: skip it, escapes included.
    let mut chars = rest.char_indices();
    if !matches!(chars.next(), Some((_, '"'))) {
        return payload.clone();
    }
    let mut after_topic = None;
    let mut escaped = false;
    for (i, c) in chars {
        match (escaped, c) {
            (true, _) => escaped = false,
            (false, '\\') => escaped = true,
            (false, '"') => {
                after_topic = Some(i + 1);
                break;
            }
            _ => {}
        }
    }
    let Some(after_topic) = after_topic else {
        return payload.clone();
    };
    // ,"e":"xxxxxxxx","o":<digits> then `,` or `}`
    let rest = &rest[after_topic..];
    let Some(rest) = rest.strip_prefix(",\"e\":\"") else {
        return payload.clone();
    };
    if rest.len() < 8 || !rest.as_bytes()[..8].iter().all(u8::is_ascii_hexdigit) {
        return payload.clone();
    }
    let Some(rest) = rest[8..].strip_prefix("\",\"o\":") else {
        return payload.clone();
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return payload.clone();
    }
    let rest = &rest[digits..];
    let body = rest.strip_prefix(',').unwrap_or(rest);
    let mut out = String::with_capacity(1 + body.len());
    out.push('{');
    out.push_str(body);
    Bytes::from(out)
}

/// What a message of `topic` stamped by [`stamp_recovery_fields`] begins
/// with, up to its epoch's hex digits.
pub(crate) fn stamp_prefix(topic: &str) -> Vec<u8> {
    let topic_json = serde_json::to_string(topic).unwrap_or_else(|_| "\"\"".to_string());
    format!("{{\"tp\":{topic_json},\"e\":\"").into_bytes()
}

/// The (epoch, offset) a message stamped by [`stamp_recovery_fields`] carries,
/// `prefix` its topic's [`stamp_prefix`].
pub(crate) fn stamped_position(message: &[u8], prefix: &[u8]) -> Option<(u32, u64)> {
    let rest = message.strip_prefix(prefix)?;
    let hex = std::str::from_utf8(rest.get(..8)?).ok()?;
    let epoch = u32::from_str_radix(hex, 16).ok()?;
    let rest = rest.get(8..)?.strip_prefix(b"\",\"o\":")?;
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    let offset = std::str::from_utf8(&rest[..digits]).ok()?.parse().ok()?;
    Some((epoch, offset))
}

/// Weave this message's recovery coordinates -- topic (`tp`), epoch (`e`, as an
/// 8-hex string) and offset (`o`) -- in as the first members of the JSON object,
/// right after the opening brace (the same position inject_category writes `c`).
/// Topic broadcasts are JSON objects produced by inject_category, so they always
/// start with `{`; anything else is returned unchanged. These fields let the
/// client dedup idempotently across reconnects within an epoch, detect gaps
/// (offset jumps), and request recovery from its last-seen (epoch, offset).
pub(crate) fn stamp_recovery_fields(json: &str, topic: &str, epoch: u32, offset: u64) -> String {
    let trimmed = json.trim_start();
    match trimmed.strip_prefix('{') {
        Some(rest) => {
            let sep = if rest.trim_start().starts_with('}') {
                ""
            } else {
                ","
            };
            let topic_json = serde_json::to_string(topic).unwrap_or_else(|_| "\"\"".to_string());
            format!("{{\"tp\":{topic_json},\"e\":\"{epoch:08x}\",\"o\":{offset}{sep}{rest}")
        }
        None => json.to_owned(),
    }
}
