//! Delivery of one message to many connections (broadcast, topics, queue
//! groups), topic patterns and recovery stamping.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use ahash::AHashSet;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::e2e::{e2e_frame, e2e_message};
use super::frames::{encode_ws_frame, ws_frame_size};
use super::state::Seal;
use super::{ConnectionHandle, QueueGroup, WsFrame};
use crate::wse::cluster::ClusterMetrics;
use crate::wse::msgpack::serde_json_to_rmpv;

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

/// How many connections a lane serves before it gives way to the runtime:
/// some tens of microseconds. tokio looks at the network only between task
/// polls, so a lane that kept its thread longer, with every thread running a
/// lane under a steady stream, left new connections waiting until their
/// handshakes timed out.
const LANE_SLICE: usize = 512;

/// A connection with this many bytes still to write is behind.
const BEHIND_BYTES: usize = 64 * 1024;

/// How long a lane waits when most connections a delivery reached were
/// behind: it does not run further ahead of the sockets than they can write.
/// Its queue fills meanwhile, and then the senders wait in turn. Only a
/// delivery to at least `PACE_MIN` connections counts, so one slow client
/// never holds back its lane: over its own budget it just misses messages.
const LANE_PACE: std::time::Duration = std::time::Duration::from_millis(1);
const PACE_MIN: usize = 32;

/// What a lane's job did.
#[derive(Default, Clone, Copy)]
struct Served {
    delivered: u64,
    dropped: u64,
    /// The connections that were behind before this delivery.
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
        if h.out.pending.load(Ordering::Relaxed) >= BEHIND_BYTES {
            self.behind += 1;
        }
        if h.deliver(msg, max_pending, slow_drops) {
            self.delivered += 1;
        } else {
            self.dropped += 1;
        }
    }

    fn connections(&self) -> usize {
        (self.delivered + self.dropped) as usize
    }

    /// Most of the many connections served were behind.
    fn lagging(&self) -> bool {
        self.connections() >= PACE_MIN && self.behind * 2 > self.connections()
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
    /// The subscribed names that are patterns. Changed only under the shard
    /// lock of the pattern's `subscribers` entry, so the two never disagree.
    pub(crate) patterns: Arc<DashSet<String>>,
    /// Per-connection topic ACLs: a pattern subscription gets only the topics
    /// its connection's ACL allows.
    pub(crate) acls: Arc<DashMap<String, TopicAcl>>,
    /// Every connection, by lane.
    everyone: Arc<[parking_lot::RwLock<ConnMap>]>,
    /// The lanes that have connections, one bit each: what a broadcast
    /// goes to. Changed under the lane's write lock.
    pub(super) occupied: Arc<AtomicU64>,
    /// Counts the connections that joined, to give each the next lane.
    joined: Arc<AtomicUsize>,
}

pub(crate) fn is_pattern(topic: &str) -> bool {
    topic.contains(['*', '?'])
}

impl TopicIndex {
    /// An index over `lanes` lanes (1 to `MAX_LANES`).
    pub(crate) fn new(lanes: usize) -> Self {
        Self {
            subscribers: Arc::default(),
            patterns: Arc::default(),
            acls: Arc::default(),
            everyone: (0..lanes.clamp(1, MAX_LANES))
                .map(|_| parking_lot::RwLock::new(ConnMap::default()))
                .collect(),
            occupied: Arc::default(),
            joined: Arc::default(),
        }
    }

    pub(crate) fn lanes(&self) -> usize {
        self.everyone.len()
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
            self.patterns.insert(topic.to_owned());
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
                    self.patterns.remove(topic);
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
        if self.patterns.is_empty() {
            return (Vec::new(), mask);
        }
        // Collected first: no lock of `patterns` is held while `subscribers`
        // is read (subscribe takes them the other way round).
        let matching: Vec<String> = self
            .patterns
            .iter()
            .filter(|pattern| pattern.as_str() != topic && glob_match(pattern, topic))
            .map(|pattern| pattern.key().clone())
            .collect();
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
    pub(crate) fn start(index: TopicIndex, max_pending: usize, slow_drops: Arc<AtomicU64>) -> Self {
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
    slow_drops: Arc<AtomicU64>,
) {
    let mut batch = Vec::with_capacity(LANE_BATCH);
    let mut seen = AHashSet::new();
    while jobs.recv_many(&mut batch, LANE_BATCH).await > 0 {
        // Connections served since the lane last gave way.
        let mut since = 0;
        for job in batch.drain(..) {
            let served = match job {
                Job::All(msg) => index.deliver_all(lane, &msg, max_pending, &slow_drops),
                Job::Topic(message) => {
                    let served = index.deliver_topic(
                        lane,
                        &message.topic,
                        &message.patterns,
                        &message.msg,
                        max_pending,
                        &slow_drops,
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
                    served.deliver(&h, &msg, max_pending, &slow_drops);
                    served
                }
            };
            since += served.connections();
            if served.lagging() {
                // The sockets are behind: they write before more is queued.
                tokio::time::sleep(LANE_PACE).await;
                since = 0;
            } else if since >= LANE_SLICE {
                // The write tasks it woke wait in this thread's run queue:
                // they run, and the runtime reads the network, before the
                // lane goes on. A lane with work would not give way by
                // itself until tokio's budget ran out, after thousands of
                // deliveries.
                tokio::task::yield_now().await;
                since = 0;
            }
        }
        if since > 0 {
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
