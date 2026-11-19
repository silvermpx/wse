//! The clients' outboxes, as the server sees them. A client numbers what it
//! sends (`s`) per producer, one producer per outbox, and keeps each message
//! until the server answers: `ack` (taken, up to and with a number) or
//! `nack` (refused, and why). The server remembers, per user and producer,
//! the highest message it decided and the refusals behind it its client may
//! not have heard of: a repeat of one taken already is confirmed again and
//! not handed on, a repeat of one refused is refused again, a client that
//! comes back is told both, and what the server cannot tell (its state is
//! gone) reaches the application marked as a possible repeat. A client that
//! comes from another node names it, and that node is asked once.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ahash::AHashMap;
use parking_lot::{Mutex, MutexGuard};
use tokio_tungstenite::tungstenite::Message;

use super::state::{ConnectionHandle, SharedState};

/// How long a disconnected producer can be remembered.
const KEPT_FOR: Duration = Duration::from_secs(24 * 60 * 60);

/// The fewest disconnected outboxes a node remembers, across all users.
const MAX_IDLE: usize = 1 << 18;

/// How many disconnected outboxes a node remembers: [`MAX_IDLE`], or twice
/// its connection limit when that is more, so that every client of a mass
/// disconnect (a network blip, a drain) is still known when it comes back.
pub(super) fn idle_limit(max_connections: usize) -> usize {
    MAX_IDLE.max(max_connections.saturating_mul(2))
}

/// How long a node waits for the node a client was on to say what it took.
const ASK_WITHIN: std::time::Duration = std::time::Duration::from_secs(2);

/// Longest producer id or node name taken: a client's random id, not a
/// document.
const MAX_ID_LEN: usize = 64;

/// Most runs of refusals a producer keeps until its client heard of them.
/// A client hears of each refusal at once and says what it still waits for
/// at each connection, so only a client that makes numbers up reaches it.
pub(crate) const MAX_HOLES: usize = 1024;

/// Why the server did not take a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// The application had no room for it.
    Busy,
    /// The rate limit dropped it before it was read.
    RateLimited,
    /// It never came, or came unreadable.
    Missing,
    /// It came before the application took the connection.
    NotReady,
}

impl Refusal {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::RateLimited => "rate_limited",
            Self::Missing => "missing",
            Self::NotReady => "not_ready",
        }
    }

    /// Its code in a cluster frame.
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::Busy => 1,
            Self::RateLimited => 2,
            Self::Missing => 3,
            Self::NotReady => 4,
        }
    }

    pub(crate) fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Busy),
            2 => Some(Self::RateLimited),
            3 => Some(Self::Missing),
            4 => Some(Self::NotReady),
            _ => None,
        }
    }
}

/// Messages `from` to `to` of a producer, behind its mark, were refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Hole {
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) why: Refusal,
}

/// What a node decided of a producer, for the node that takes it over:
/// what it knows, and what it cannot tell.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Decided {
    /// The highest message decided.
    pub(crate) mark: u64,
    /// The refusals behind it its client may not have heard of.
    pub(crate) holes: Vec<Hole>,
    /// Up to here refusals were let go for the bound: one of these is not
    /// known as taken.
    pub(crate) forgot: u64,
    /// A message whose handler never ended: it may have done its work.
    pub(crate) undecided: Option<u64>,
    /// Up to here a message may have been taken on a node that could not be
    /// asked: the first time one comes, it is marked a possible repeat.
    pub(crate) unsure: u64,
}

/// One producer on this node. Its lock is held while a message of it is
/// decided, the application's handler included: a connection that comes
/// back meanwhile is answered once the decision is in, never halfway.
pub(crate) struct Slot {
    state: Mutex<SlotState>,
}

/// What a producer's slot holds.
pub(crate) struct SlotState {
    /// The highest message decided: taken, or refused.
    mark: u64,
    /// Another node took the producer over: nothing more is taken here.
    moved: bool,
    /// The refusals behind the mark, in order, until the client heard of
    /// them.
    holes: VecDeque<Hole>,
    /// Up to here refusals were let go for the bound before the client
    /// heard of them: one of these that comes again is not known as taken.
    forgot: u64,
    /// A message being decided: its handler runs. One whose handler never
    /// ended (it panicked) stays undecided.
    deciding: Option<u64>,
    /// Up to here a message may have been taken on a node that could not be
    /// asked (gone, or it knew nothing of the producer): the first time one
    /// comes, whatever connection brings it, it is handed on marked.
    unsure: u64,
    /// The connection whose hello took the producer last: what an older one
    /// still brings is not taken, so what that hello was told holds.
    owner: u64,
}

/// Whether a message is handed on.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    /// The first time it came: handed on, marked a possible repeat when a
    /// node that could not be asked may have taken it (`unsure`). Those it
    /// skipped are refused (`gap`, first and last).
    First {
        gap: Option<(u64, u64)>,
        unsure: bool,
    },
    /// Taken already: confirmed again, not handed on.
    Repeat,
    /// Refused already: refused again.
    Refused(Refusal),
    /// Behind the mark, and the server cannot tell whether it was taken (a
    /// refusal let go for the bound, or a decision whose handler never
    /// ended): handed on marked as a possible repeat.
    Unsure,
    /// Another node took the producer over.
    Moved,
    /// The client is back on another connection of this node: this one is
    /// stale, and what it still brings is not taken.
    Left,
}

impl SlotState {
    /// Claims message `s`: the first claim hands it on, any later one is
    /// answered as the first was. What it skipped is refused for `skipped`.
    pub(crate) fn claim(&mut self, s: u64, skipped: Refusal) -> Claim {
        if self.moved {
            return Claim::Moved;
        }
        if s <= self.mark {
            if let Some(why) = self.refusal_of(s) {
                return Claim::Refused(why);
            }
            if self.deciding == Some(s) || s <= self.forgot {
                return Claim::Unsure;
            }
            return Claim::Repeat;
        }
        let gap = (s > self.mark + 1).then(|| (self.mark + 1, s - 1));
        if let Some((from, to)) = gap {
            self.refuse(from, to, skipped);
        }
        self.mark = s;
        Claim::First {
            gap,
            unsure: s <= self.unsure,
        }
    }

    /// [`claim`](Self::claim), for a message on the connection that holds
    /// the producer as `owner`: one on a connection the client left is not
    /// taken.
    pub(crate) fn claim_by(&mut self, owner: u64, s: u64, skipped: Refusal) -> Claim {
        if owner != self.owner {
            return Claim::Left;
        }
        self.claim(s, skipped)
    }

    /// A connection's hello takes the producer: what older ones still bring
    /// is not taken from now on. Its owner number.
    fn take(&mut self) -> u64 {
        self.owner += 1;
        self.owner
    }

    /// A message up to `s` may have been taken on a node that could not be
    /// asked.
    fn unsure_up_to(&mut self, s: u64) {
        self.unsure = self.unsure.max(s);
    }

    /// Messages `from` to `to`, behind the mark, were not taken.
    pub(crate) fn refuse(&mut self, from: u64, to: u64, why: Refusal) {
        if let Some(last) = self.holes.back_mut()
            && last.why == why
            && last.to + 1 == from
        {
            last.to = to;
            return;
        }
        if self.holes.len() == MAX_HOLES
            && let Some(gone) = self.holes.pop_front()
        {
            self.forgot = self.forgot.max(gone.to);
        }
        // In order: one decided again behind the mark (it was unsure) goes
        // in its place.
        let at = self.holes.partition_point(|h| h.to < from);
        self.holes.insert(at, Hole { from, to, why });
    }

    /// Message `s` is being decided: its handler runs.
    pub(crate) fn deciding(&mut self, s: u64) {
        self.deciding = Some(s);
    }

    /// Message `s` is decided: its handler ended.
    pub(crate) fn decided(&mut self, s: u64) {
        if self.deciding == Some(s) {
            self.deciding = None;
        }
    }

    /// What a client that comes back is told was decided: up to the mark,
    /// but not a message whose handler never ended.
    pub(crate) fn acked(&self) -> u64 {
        self.deciding
            .map_or(self.mark, |s| self.mark.min(s.saturating_sub(1)))
    }

    /// What a client that comes back waiting from `from` on is told was
    /// decided: [`acked`](Self::acked), but short of the first one it waits
    /// for whose refusal may have been let go for the bound: that one and
    /// those after it come again, and are answered as they were (or handed
    /// on marked, when the server cannot tell).
    fn decided_for(&self, from: u64) -> u64 {
        let from = from.max(1);
        if from <= self.forgot {
            self.acked().min(from - 1)
        } else {
            self.acked()
        }
    }

    fn refusal_of(&self, s: u64) -> Option<Refusal> {
        let at = self.holes.partition_point(|h| h.to < s);
        self.holes.get(at).filter(|h| h.from <= s).map(|h| h.why)
    }

    /// The highest message a client back waiting from `from` on may have
    /// had taken without the server telling (0: none): taken on a node that
    /// could not be asked, one whose refusal was let go, or one whose
    /// handler never ended. Past it, one not decided was not taken.
    fn cannot_tell(&self, from: u64) -> u64 {
        let forgot = if from.max(1) <= self.forgot {
            self.forgot
        } else {
            0
        };
        self.unsure.max(forgot).max(self.deciding.unwrap_or(0))
    }

    /// The refusals from `from` on, for a client that still waits for
    /// those: the ones before it it heard of, and they go.
    fn told_from(&mut self, from: u64) -> Vec<Hole> {
        let heard = self.holes.partition_point(|h| h.to < from);
        self.holes.drain(..heard);
        self.holes.iter().copied().collect()
    }

    /// What another node decided of the producer counts here too, and so
    /// does what it could not tell.
    fn advance(&mut self, decided: &Decided) {
        let to = decided.mark;
        if self.moved {
            return;
        }
        self.unsure_up_to(decided.unsure);
        if to <= self.mark {
            return;
        }
        let mark = self.mark;
        for hole in decided.holes.iter().filter(|h| h.from > mark && h.to <= to) {
            self.refuse(hole.from, hole.to, hole.why);
        }
        self.forgot = self.forgot.max(decided.forgot.min(to));
        if let Some(s) = decided.undecided.filter(|s| *s > mark && *s <= to) {
            self.deciding = Some(s);
        }
        self.mark = to;
    }

    /// Another node takes the producer over: what was decided here, and
    /// what could not be told; from now on nothing more is.
    fn hand_over(&mut self) -> Decided {
        self.moved = true;
        Decided {
            mark: self.mark,
            holes: self.holes.iter().copied().collect(),
            forgot: self.forgot,
            undecided: self.deciding,
            unsure: self.unsure,
        }
    }

    /// The highest message decided here.
    #[cfg(test)]
    fn mark(&self) -> u64 {
        self.mark
    }
}

impl Slot {
    fn new() -> Self {
        Self {
            state: Mutex::new(SlotState {
                mark: 0,
                moved: false,
                holes: VecDeque::new(),
                forgot: 0,
                deciding: None,
                unsure: 0,
                owner: 0,
            }),
        }
    }

    /// The producer's state, while a message of it is decided.
    pub(crate) fn lock(&self) -> MutexGuard<'_, SlotState> {
        self.state.lock()
    }
}

type Key = (Option<Box<str>>, Box<str>);

/// A producer can have overlapping connections. Only the last one to leave
/// puts its slot in the order of disconnected producers.
struct Entry {
    slot: Arc<Slot>,
    connections: usize,
    idle: Option<u64>,
    handed_over: bool,
}

#[derive(Default)]
struct Table {
    slots: AHashMap<Arc<Key>, Entry>,
    /// Disconnect order, with each outbox's expiry deadline.
    idle: BTreeMap<u64, (Instant, Arc<Key>)>,
    next: u64,
}

impl Table {
    /// A reconnect or peer query can arrive between the deadline and the
    /// next periodic cleanup. Check this producer's own deadline then.
    fn expire_if_due(&mut self, key: &Key) {
        if let Some(order) = self.slots.get(key).and_then(|entry| entry.idle)
            && let Some((until, _)) = self.idle.get(&order)
            && Instant::now() >= *until
        {
            self.idle.remove(&order);
            self.slots.remove(key);
        }
    }

    fn forget_oldest(&mut self) {
        if let Some((_, (_, key))) = self.idle.pop_first() {
            self.slots.remove(&key);
        }
    }
}

/// Independent parts of the table, by a hash of the producer: a hello or
/// a connection's end locks only its producer's part, so connections on
/// many cores do not wait on one lock. Fixed, so the bound is the same on
/// every machine.
const SHARDS: usize = 256;

/// One part of the table, with its share of the bound. A connection's
/// hold keeps its own shard, so that connections on many cores share no
/// counter.
struct Shard {
    table: Mutex<Table>,
    /// Disconnected outboxes it keeps.
    limit: usize,
}

/// The outboxes this node knows, by user and producer. A shard's lock only
/// protects membership. An application's handler runs with its slot lock.
pub(crate) struct Producers {
    shards: Box<[Arc<Shard>]>,
    hasher: ahash::RandomState,
}

impl Default for Producers {
    fn default() -> Self {
        Self::with_limit(MAX_IDLE)
    }
}

/// One connection's hold. A previous incarnation's last connection can
/// finish after the outbox has come back from another node.
pub(crate) struct Attached {
    shard: Arc<Shard>,
    key: Arc<Key>,
    slot: Arc<Slot>,
}

impl std::ops::Deref for Attached {
    type Target = Slot;

    fn deref(&self) -> &Slot {
        &self.slot
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let mut table = self.shard.table.lock();
        let order = table.next;
        let Some(entry) = table.slots.get_mut(&self.key) else {
            return;
        };
        if !Arc::ptr_eq(&entry.slot, &self.slot) {
            return;
        }
        entry.connections -= 1;
        if entry.connections == 0 {
            entry.idle = Some(order);
            table.next += 1;
            table
                .idle
                .insert(order, (Instant::now() + KEPT_FOR, Arc::clone(&self.key)));
            if table.idle.len() > self.shard.limit {
                table.forget_oldest();
            }
        }
    }
}

impl Producers {
    /// At most `limit` disconnected outboxes, each shard its share: a shard
    /// that has more forgets its longest-disconnected.
    pub(super) fn with_limit(limit: usize) -> Self {
        Self::sharded(limit, SHARDS)
    }

    fn sharded(limit: usize, shards: usize) -> Self {
        let limit = limit.div_ceil(shards).max(1);
        Self {
            shards: (0..shards)
                .map(|_| {
                    Arc::new(Shard {
                        table: Mutex::default(),
                        limit,
                    })
                })
                .collect(),
            hasher: ahash::RandomState::new(),
        }
    }

    /// One shard: the longest-disconnected of all goes first, exactly.
    #[cfg(test)]
    pub(super) fn exact(limit: usize) -> Self {
        Self::sharded(limit, 1)
    }

    /// The single shard of an [`exact`](Self::exact) table.
    #[cfg(test)]
    fn table(&self) -> MutexGuard<'_, Table> {
        assert_eq!(self.shards.len(), 1);
        self.shards[0].table.lock()
    }

    fn shard(&self, key: &Key) -> &Arc<Shard> {
        // The remainder is below the shard count, which fits.
        &self.shards[(self.hasher.hash_one(key) % self.shards.len() as u64) as usize]
    }

    /// A live connection takes the slot out of the idle order. After a
    /// handover, or past its day, it starts a fresh slot with a fresh count
    /// of connections. One lookup: a hello is a connection's start.
    fn attach(&self, user: Option<&str>, producer: &str) -> (Attached, bool) {
        use std::collections::hash_map::Entry as MapEntry;
        let key = Arc::new((user.map(Box::from), Box::from(producer)));
        let shard = self.shard(&key);
        let mut table = shard.table.lock();
        let Table { slots, idle, .. } = &mut *table;
        let record = slots.entry(key);
        let key = Arc::clone(record.key());
        let (entry, known) = match record {
            MapEntry::Occupied(record) => {
                let entry = record.into_mut();
                // Past its day it is forgotten, as the sweep would have,
                // though the sweep has not come by yet.
                let expired = entry.idle.take().is_some_and(|order| {
                    idle.remove(&order)
                        .is_some_and(|(until, _)| Instant::now() >= until)
                });
                let known = !entry.handed_over && !expired;
                if !known {
                    entry.slot = Arc::new(Slot::new());
                    entry.connections = 0;
                    entry.handed_over = false;
                }
                (entry, known)
            }
            MapEntry::Vacant(record) => (
                record.insert(Entry {
                    slot: Arc::new(Slot::new()),
                    connections: 0,
                    idle: None,
                    handed_over: false,
                }),
                false,
            ),
        };
        entry.connections += 1;
        let slot = Arc::clone(&entry.slot);
        (
            Attached {
                shard: Arc::clone(shard),
                key,
                slot,
            },
            known,
        )
    }

    /// A peer asks what was decided. Wait for the application's decision
    /// outside the table lock. Only the slot we found has moved.
    pub(crate) fn hand_over(&self, user: Option<&str>, producer: &str) -> Option<Decided> {
        let key = (user.map(Box::from), Box::from(producer));
        let shard = self.shard(&key);
        loop {
            let slot = Arc::clone(&shard.table.lock().slots.get(&key)?.slot);
            let mut state = slot.lock();
            let mut table = shard.table.lock();
            table.expire_if_due(&key);
            let entry = table.slots.get_mut(&key)?;
            if !Arc::ptr_eq(&entry.slot, &slot) {
                continue;
            }
            entry.handed_over = true;
            drop(table);
            return Some(state.hand_over());
        }
    }

    /// Attach after any previous node has answered. A peer may take the
    /// slot while this hello waits for a running handler. In that case
    /// start again on the replacement before answering the client.
    fn link(&self, user: Option<&str>, hello: &Hello, last: LastNode<'_>) -> (Link, Vec<Hole>) {
        loop {
            let (slot, known) = self.attach(user, &hello.producer);
            let mut state = slot.lock();
            if state.moved {
                continue;
            }
            let remembered = match last {
                LastNode::Here => known,
                LastNode::Peer(Some(decided)) => {
                    state.advance(decided);
                    true
                }
                LastNode::Peer(None) => false,
            };
            let (start, unsure, told) = if remembered {
                (
                    state.decided_for(hello.from),
                    state.cannot_tell(hello.from),
                    state.told_from(hello.from),
                )
            } else {
                state.unsure_up_to(hello.written);
                (0, hello.written, Vec::new())
            };
            let owner = state.take();
            drop(state);
            return (
                Link {
                    slot,
                    start,
                    unsure,
                    owner,
                },
                told,
            );
        }
    }

    /// Expire disconnected producers in order, shard by shard, giving other
    /// connections and the runtime a turn when many outboxes reach their
    /// deadline.
    pub(crate) async fn sweep(&self, now: Instant) {
        for shard in &self.shards {
            loop {
                let done = {
                    let mut table = shard.table.lock();
                    (0..256).all(|_| {
                        let due = table
                            .idle
                            .first_key_value()
                            .is_some_and(|(_, (until, _))| now >= *until);
                        if due {
                            table.forget_oldest();
                        }
                        due
                    })
                };
                if !done {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Where to look for this outbox's decisions. An unreachable peer and one
/// that already forgot the outbox both leave its written messages unsure.
enum LastNode<'a> {
    Here,
    Peer(Option<&'a Decided>),
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
    /// The first message it still waits to hear of: refusals before it it
    /// heard of. 0: it does not say.
    pub(crate) from: u64,
}

impl Hello {
    pub(crate) fn parse(p: Option<&serde_json::Value>) -> Option<Self> {
        let outbox = p?.get("outbox")?;
        let producer = outbox.get("producer")?.as_str()?;
        if producer.is_empty() || producer.len() > MAX_ID_LEN {
            return None;
        }
        let number = |name: &str| {
            outbox
                .get(name)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        Some(Self {
            producer: producer.into(),
            node: outbox
                .get("node")
                .and_then(serde_json::Value::as_str)
                .filter(|n| !n.is_empty() && n.len() <= MAX_ID_LEN)
                .map(Box::from),
            written: number("written"),
            from: number("from"),
        })
    }
}

/// A connection's producer, once known.
pub(crate) struct Link {
    pub(crate) slot: Attached,
    /// What the client was told was decided: where its messages on this
    /// connection go on from.
    pub(crate) start: u64,
    /// The highest message the server cannot tell whether it took (0:
    /// none): past it and `start`, one the client wrote was not taken.
    pub(crate) unsure: u64,
    /// Its hello's hold on the producer: a later hello of the client's takes
    /// it, and this connection's messages are not taken from then on.
    pub(crate) owner: u64,
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

/// Starts the connection's producer once its hello and its admission are
/// both in, and tells the client what was decided of it. A client that
/// comes from another node this one links to: that node is asked first,
/// once.
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
            let decided = state
                .producer_links
                .ask(&last, user.as_deref(), &hello.producer, ASK_WITHIN)
                .await;
            let (link, told) =
                state
                    .producers
                    .link(user.as_deref(), hello, LastNode::Peer(decided.as_ref()));
            finish(&state, &handle, link, &told);
        });
        return;
    }
    let last = if last.is_none() {
        LastNode::Here
    } else {
        LastNode::Peer(None)
    };
    let (link, told) = state.producers.link(user, hello, last);
    finish(state, handle, link, &told);
}

/// The connection's producer is known: the client hears what was decided.
fn finish(state: &SharedState, handle: &ConnectionHandle, link: Link, told: &[Hole]) {
    let reply = outbox_reply(link.start, &state.node_id, told, link.unsure);
    let _ = handle.out.acks.link.set(link);
    handle.send_control(Message::Text(reply.into()));
}

/// The server's answer to a client's outbox hello: what was decided of it,
/// the refusals among that it may not have heard of, the name this node
/// goes by, and how far past what was decided it cannot tell whether it took
/// a message (`unsure`, left out when it can tell all).
fn outbox_reply(acked: u64, node: &str, refused: &[Hole], unsure: u64) -> String {
    let mut p = serde_json::json!({"acked": acked, "node": node});
    if unsure > acked {
        p["unsure"] = unsure.into();
    }
    if !refused.is_empty() {
        p["refused"] = refused
            .iter()
            .map(|hole| {
                let mut refusal = serde_json::json!({"s": hole.to, "code": hole.why.as_str()});
                if hole.from != hole.to {
                    refusal["from"] = hole.from.into();
                }
                refusal
            })
            .collect();
    }
    serde_json::json!({"c": "WSE", "t": "outbox", "p": p, "v": 1}).to_string()
}

/// A message's number in its producer's outbox, when it has one.
pub(crate) fn seq_of(val: &serde_json::Value) -> Option<u64> {
    val.get("s")
        .and_then(serde_json::Value::as_u64)
        .filter(|s| *s > 0 && *s < u64::MAX)
}

/// The number of an outbox message, read from its first bytes without
/// parsing it: a client puts the number first (`{"s":N,`). For a frame the
/// rate limit drops, which is not parsed.
pub(crate) fn number_in(frame: &[u8]) -> Option<u64> {
    if let Some(packed) = frame.strip_prefix(b"M:") {
        return number_in_msgpack(packed);
    }
    let frame = frame
        .strip_prefix(b"WSE")
        .or_else(|| frame.strip_prefix(b"S"))
        .or_else(|| frame.strip_prefix(b"U"))
        .unwrap_or(frame);
    let digits = frame.strip_prefix(b"{\"s\":")?;
    let end = digits.iter().position(|b| !b.is_ascii_digit())?;
    if end == 0 || end > 19 || !matches!(digits[end], b',' | b'}') {
        return None;
    }
    let s: u64 = std::str::from_utf8(&digits[..end]).ok()?.parse().ok()?;
    (s > 0).then_some(s)
}

/// The number of a msgpack message of a client's outbox, read off its first
/// bytes as a client writes them: a map whose first key is `s`. None for
/// any other, which is not decoded: a frame the rate limit dropped costs
/// nothing more.
fn number_in_msgpack(map: &[u8]) -> Option<u64> {
    let (&head, rest) = map.split_first()?;
    let rest = match head {
        0x81..=0x8f => rest,
        0xde => rest.get(2..)?,
        0xdf => rest.get(4..)?,
        _ => return None,
    };
    let rest = rest.strip_prefix(&[0xa1, b's'])?;
    let (&tag, value) = rest.split_first()?;
    let s = match tag {
        0x00..=0x7f => u64::from(tag),
        0xcc => u64::from(*value.first()?),
        0xcd => u64::from(u16::from_be_bytes(value.get(..2)?.try_into().ok()?)),
        0xce => u64::from(u32::from_be_bytes(value.get(..4)?.try_into().ok()?)),
        0xcf => u64::from_be_bytes(value.get(..8)?.try_into().ok()?),
        _ => return None,
    };
    (s > 0).then_some(s)
}

/// What a connection has to answer since its last answer went.
#[derive(Default)]
pub(crate) struct Pending {
    /// The highest message taken since the last answer; 0: none.
    ack: u64,
    /// Messages refused since the last answer: first, last, why.
    nacks: Vec<(u64, u64, Refusal)>,
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

    /// Why the messages a numbered one skipped did not come: the rate limit
    /// dropped frames since the last, or they never came.
    pub(crate) fn skipped(&mut self) -> Refusal {
        if std::mem::take(&mut self.dropped) {
            Refusal::RateLimited
        } else {
            Refusal::Missing
        }
    }

    /// Message `s` was taken (or was already).
    pub(crate) fn taken(&mut self, s: u64) {
        self.ack = self.ack.max(s);
    }

    /// Messages `from` to `to` are refused for `why`.
    pub(crate) fn refuse(&mut self, from: u64, to: u64, why: Refusal) {
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
            .map(|(from, to, why)| nack(from, to, why.as_str()))
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

    fn claim(slot: &Slot, s: u64) -> Claim {
        slot.lock().claim(s, Refusal::Missing)
    }

    #[test]
    fn a_message_is_handed_on_once() {
        let slot = Slot::new();
        assert_eq!(
            claim(&slot, 1),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        assert_eq!(claim(&slot, 1), Claim::Repeat);
        assert_eq!(
            claim(&slot, 3),
            Claim::First {
                gap: Some((2, 2)),
                unsure: false
            }
        );
        assert_eq!(
            claim(&slot, 2),
            Claim::Refused(Refusal::Missing),
            "skipped: refused again"
        );
        assert_eq!(
            slot.lock().hand_over(),
            Decided {
                mark: 3,
                holes: vec![Hole {
                    from: 2,
                    to: 2,
                    why: Refusal::Missing
                }],
                ..Decided::default()
            }
        );
        assert_eq!(claim(&slot, 4), Claim::Moved);
        assert_eq!(slot.lock().mark(), 3);
    }

    #[test]
    fn a_refused_message_stays_refused_until_the_client_heard() {
        let slot = Slot::new();
        let mut state = slot.lock();
        assert_eq!(
            state.claim(1, Refusal::Missing),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        state.refuse(1, 1, Refusal::Busy);
        assert_eq!(
            state.claim(2, Refusal::Missing),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        state.refuse(2, 2, Refusal::Busy);
        assert_eq!(
            state.claim(3, Refusal::Missing),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        assert_eq!(
            state.claim(1, Refusal::Missing),
            Claim::Refused(Refusal::Busy)
        );
        assert_eq!(state.claim(3, Refusal::Missing), Claim::Repeat);
        assert_eq!(
            state.told_from(0),
            [Hole {
                from: 1,
                to: 2,
                why: Refusal::Busy
            }],
            "one run"
        );
        // The client waits for nothing before 3: it heard of them.
        assert!(state.told_from(3).is_empty());
        assert_eq!(state.claim(1, Refusal::Missing), Claim::Repeat);
    }

    #[test]
    fn a_refusal_forgotten_past_the_bound_is_never_taken_for_a_take() {
        let slot = Slot::new();
        let mut state = slot.lock();
        // Taken and refused in turn: one run of refusals each.
        for n in 0..(MAX_HOLES as u64 + 1) {
            let refused = 2 * n + 1;
            state.claim(refused, Refusal::Missing);
            state.refuse(refused, refused, Refusal::Busy);
            state.claim(refused + 1, Refusal::Missing);
        }
        assert_eq!(state.holes.len(), MAX_HOLES);
        // The first refusal is gone: the server cannot tell it from a take.
        assert_eq!(state.claim(1, Refusal::Missing), Claim::Unsure);
        // Those it still knows are answered as before.
        assert_eq!(
            state.claim(3, Refusal::Missing),
            Claim::Refused(Refusal::Busy)
        );
        // Taken after what was let go: known as taken.
        assert_eq!(state.claim(2, Refusal::Missing), Claim::Repeat);
        let last = 2 * MAX_HOLES as u64 + 2;
        assert_eq!(state.claim(last, Refusal::Missing), Claim::Repeat);
    }

    #[test]
    fn a_decision_whose_handler_never_ended_is_not_said_taken() {
        let slot = Slot::new();
        let mut state = slot.lock();
        assert_eq!(
            state.claim(1, Refusal::Missing),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        state.deciding(1);
        state.decided(1);
        assert_eq!(
            state.claim(2, Refusal::Missing),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        // Its handler panicked: 2 was never decided.
        state.deciding(2);
        assert_eq!(state.acked(), 1, "a client that comes back is told 1");
        assert_eq!(state.claim(2, Refusal::Missing), Claim::Unsure);
        state.deciding(2);
        state.decided(2);
        assert_eq!(state.acked(), 2);
        assert_eq!(state.claim(2, Refusal::Missing), Claim::Repeat);
    }

    #[test]
    fn a_refusal_of_one_the_server_could_not_tell_keeps_its_place() {
        let slot = Slot::new();
        let mut state = slot.lock();
        for n in 0..(MAX_HOLES as u64 + 1) {
            let refused = 2 * n + 1;
            state.claim(refused, Refusal::Missing);
            state.refuse(refused, refused, Refusal::Busy);
            state.claim(refused + 1, Refusal::Missing);
        }
        assert_eq!(state.claim(1, Refusal::Missing), Claim::Unsure);
        // Handed on again, and refused this time: said again as refused.
        state.refuse(1, 1, Refusal::NotReady);
        assert_eq!(
            state.claim(1, Refusal::Missing),
            Claim::Refused(Refusal::NotReady)
        );
        // The ones after it are found as before.
        assert_eq!(
            state.claim(5, Refusal::Missing),
            Claim::Refused(Refusal::Busy)
        );
        let last = 2 * MAX_HOLES as u64 + 1;
        assert_eq!(
            state.claim(last, Refusal::Missing),
            Claim::Refused(Refusal::Busy)
        );
    }

    #[test]
    fn a_client_that_makes_numbers_up_is_kept_within_bounds() {
        let slot = Slot::new();
        let mut state = slot.lock();
        for n in 0..(MAX_HOLES as u64 + 10) {
            state.claim(2 * n + 2, Refusal::Missing);
        }
        assert_eq!(state.holes.len(), MAX_HOLES);
    }

    #[test]
    fn a_producer_is_one_users() {
        let producers = Arc::new(Producers::default());
        let (ann, known) = producers.attach(Some("ann"), "p1");
        assert!(!known);
        claim(&ann, 5);
        let (again, known) = producers.attach(Some("ann"), "p1");
        assert!(known);
        assert_eq!(again.lock().mark(), 5);
        let (bob, known) = producers.attach(Some("bob"), "p1");
        assert!(!known, "another user's producer of the same name");
        assert_eq!(bob.lock().mark(), 0);
        // Handed over to another node: what comes back here starts over.
        let handed = producers.hand_over(Some("ann"), "p1").unwrap();
        assert_eq!(handed.mark, 5);
        assert_eq!(
            handed.holes,
            [Hole {
                from: 1,
                to: 4,
                why: Refusal::Missing
            }]
        );
        assert_eq!(producers.hand_over(Some("cy"), "p1"), None);
        let (back, known) = producers.attach(Some("ann"), "p1");
        assert!(!known);
        assert_eq!(back.lock().mark(), 0);
    }

    /// The connection whose hello came last holds the producer: a message
    /// an older one brings late is not taken, whatever its number.
    #[test]
    fn a_message_on_a_connection_the_client_left_is_not_taken() {
        let producers = Arc::new(Producers::default());
        let hello = |written| Hello {
            producer: Box::from("p"),
            node: None,
            written,
            from: 1,
        };
        let (old, _) = producers.link(None, &hello(0), LastNode::Here);
        assert!(matches!(
            old.slot.lock().claim_by(old.owner, 1, Refusal::Missing),
            Claim::First { .. }
        ));
        let (new, _) = producers.link(None, &hello(2), LastNode::Here);
        assert_eq!(new.start, 1);
        assert_eq!(
            old.slot.lock().claim_by(old.owner, 2, Refusal::Missing),
            Claim::Left
        );
        assert!(matches!(
            new.slot.lock().claim_by(new.owner, 2, Refusal::Missing),
            Claim::First { .. }
        ));
    }

    #[tokio::test]
    async fn a_producer_whose_connection_is_open_is_kept_however_long_it_is_silent() {
        let producers = Arc::new(Producers::default());
        let (link, _) = producers.link(None, &hello("here", 0), LastNode::Here);
        producers.sweep(Instant::now() + 3 * KEPT_FOR).await;
        let (again, known) = producers.attach(None, "p");
        assert!(known);
        assert!(Arc::ptr_eq(&link.slot.slot, &again.slot));
        drop(again);
        let before = Instant::now();
        drop(link);
        producers
            .sweep(before + KEPT_FOR - Duration::from_millis(1))
            .await;
        assert!(producers.hand_over(None, "p").is_some());
    }

    #[tokio::test]
    async fn a_silent_producer_is_forgotten_after_a_day() {
        let producers = Arc::new(Producers::exact(MAX_IDLE));
        let before = Instant::now();
        drop(producers.attach(None, "p"));
        producers
            .sweep(before + KEPT_FOR - Duration::from_millis(1))
            .await;
        assert_eq!(producers.table().slots.len(), 1);
        producers.sweep(Instant::now() + KEPT_FOR).await;
        assert_eq!(producers.table().slots.len(), 0);
    }

    /// Each shard keeps its share of the bound: however the outboxes fall,
    /// the node keeps no more than the bound, and never forgets one a
    /// connection holds.
    #[test]
    fn the_bound_holds_across_the_shards() {
        let producers = Arc::new(Producers::with_limit(SHARDS * 2));
        let (held, _) = producers.attach(None, "held");
        for n in 0..10_000 {
            drop(producers.attach(None, &format!("p{n}")).0);
        }
        let idle: usize = producers
            .shards
            .iter()
            .map(|shard| shard.table.lock().idle.len())
            .sum();
        assert!(idle <= SHARDS * 2, "{idle} kept");
        assert!(idle >= SHARDS, "{idle} kept");
        assert!(producers.hand_over(None, "held").is_some());
        drop(held);
    }

    /// Every outbox of a mass disconnect (a network blip, a drain) is still
    /// known when its client comes back: a node keeps twice its connections.
    #[test]
    fn a_node_keeps_twice_its_connections_when_that_is_more() {
        assert_eq!(idle_limit(1_000), MAX_IDLE);
        assert_eq!(idle_limit(500_000), 1_000_000);
        assert_eq!(idle_limit(usize::MAX), usize::MAX);
    }

    #[test]
    fn only_the_two_most_recently_disconnected_producers_are_remembered() {
        let producers = Arc::new(Producers::exact(2));
        let connect = |user| {
            producers
                .link(Some(user), &hello("here", 0), LastNode::Here)
                .0
        };
        let active = connect("active");
        for user in ["ann", "bob", "ann", "cy"] {
            drop(connect(user));
        }
        assert!(producers.hand_over(Some("bob"), "p").is_none());
        for user in ["active", "ann", "cy"] {
            assert!(producers.hand_over(Some(user), "p").is_some(), "{user}");
        }
        drop(active);
    }

    #[test]
    fn the_last_connection_of_the_current_producer_puts_it_in_the_idle_table() {
        let producers = Arc::new(Producers::exact(1));
        let connect = |user| {
            producers
                .link(Some(user), &hello("here", 0), LastNode::Here)
                .0
        };
        let first = connect("ann");
        let second = connect("ann");
        drop(first);
        drop(connect("bob"));
        assert!(producers.hand_over(Some("ann"), "p").is_some());
        let replacement = connect("ann");
        drop(second);
        drop(connect("cy"));
        assert!(producers.hand_over(Some("bob"), "p").is_none());
        assert_eq!(
            claim(&replacement.slot, 1),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        drop(replacement);
        assert!(producers.hand_over(Some("cy"), "p").is_none());
        assert_eq!(producers.hand_over(Some("ann"), "p").unwrap().mark, 1);
    }

    #[test]
    fn a_query_waiting_on_a_forgotten_slot_hands_over_the_current_one() {
        let producers = Arc::new(Producers::exact(1));
        let (first, _) = producers.attach(None, "p");
        let old = Arc::clone(&first.slot);
        drop(first);
        let deciding = old.lock();
        std::thread::scope(|scope| {
            let query = scope.spawn(|| producers.hand_over(None, "p").unwrap());
            // The table and this test hold two references. The third is
            // the query, waiting for the decision on the original slot.
            let deadline = Instant::now() + Duration::from_secs(5);
            while Arc::strong_count(&old) < 3 {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            drop(producers.attach(None, "other"));
            let (current, known) = producers.attach(None, "p");
            assert!(!known);
            claim(&current, 2);
            drop(deciding);
            assert_eq!(query.join().unwrap().mark, 2);
            assert_eq!(claim(&current, 3), Claim::Moved);
        });
    }

    #[test]
    fn a_reconnect_waiting_on_a_handover_takes_the_replacement_slot() {
        let producers = Arc::new(Producers::exact(MAX_IDLE));
        let (old, _) = producers.attach(None, "p");
        let mut decision = old.lock();
        std::thread::scope(|scope| {
            let back = scope.spawn(|| producers.link(None, &hello("here", 1), LastNode::Here).0);
            let deadline = Instant::now() + Duration::from_secs(5);
            while producers.table().slots[&old.key].connections != 2 {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            // The query holds the decision lock before the hello can read
            // the mark. Complete the handover in that order.
            producers
                .table()
                .slots
                .get_mut(&old.key)
                .unwrap()
                .handed_over = true;
            decision.hand_over();
            drop(decision);
            let back = back.join().unwrap();
            assert_eq!(
                claim(&back.slot, 1),
                Claim::First {
                    gap: None,
                    unsure: true
                }
            );
        });
    }

    #[test]
    fn an_expired_outbox_cannot_be_queried_or_resumed_before_the_next_sweep() {
        let producers = Arc::new(Producers::exact(MAX_IDLE));
        for name in ["query", "resume"] {
            drop(producers.attach(None, name));
        }
        for (until, _) in producers.table().idle.values_mut() {
            *until = Instant::now();
        }
        assert!(producers.hand_over(None, "query").is_none());
        assert!(!producers.attach(None, "resume").1);
    }

    #[test]
    fn the_hello_names_the_producer_and_what_was_written() {
        let p = serde_json::json!({"outbox": {"producer": "abc", "node": "n1", "written": 7, "from": 5}});
        assert_eq!(
            Hello::parse(Some(&p)),
            Some(Hello {
                producer: "abc".into(),
                node: Some("n1".into()),
                written: 7,
                from: 5,
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
            from: 0,
        }
    }

    #[test]
    fn a_client_from_another_node_is_unsure_up_to_what_it_wrote() {
        let producers = Arc::new(Producers::default());
        let (link, _) = producers.link(Some("ann"), &hello("other", 4), LastNode::Peer(None));
        assert_eq!((link.start, link.slot.lock().unsure), (0, 4));

        // Back on this node, which decided messages of it: they are known,
        // and so is what it refused.
        {
            let mut state = link.slot.lock();
            state.claim(9, Refusal::Missing);
            state.refuse(9, 9, Refusal::Busy);
        }
        let (link, told) = producers.link(Some("ann"), &hello("here", 11), LastNode::Here);
        // What it could not tell stays: none of those comes unmarked.
        assert_eq!((link.start, link.slot.lock().unsure), (9, 4));
        assert_eq!(
            told,
            [
                Hole {
                    from: 1,
                    to: 8,
                    why: Refusal::Missing
                },
                Hole {
                    from: 9,
                    to: 9,
                    why: Refusal::Busy
                },
            ]
        );
    }

    #[test]
    fn what_the_last_node_decided_counts_here() {
        let producers = Arc::new(Producers::default());
        let holes = vec![Hole {
            from: 7,
            to: 7,
            why: Refusal::Busy,
        }];
        let decided = Decided {
            mark: 10,
            holes: holes.clone(),
            ..Decided::default()
        };
        let (link, told) = producers.link(
            Some("ann"),
            &hello("other", 12),
            LastNode::Peer(Some(&decided)),
        );
        assert_eq!((link.start, link.slot.lock().unsure), (10, 0));
        assert_eq!(told, holes);
        assert_eq!(claim(&link.slot, 10), Claim::Repeat);
        assert_eq!(claim(&link.slot, 7), Claim::Refused(Refusal::Busy));
        assert_eq!(
            claim(&link.slot, 11),
            Claim::First {
                gap: None,
                unsure: false
            }
        );
        // The last node did not answer: unsure up to what was written.
        let (link, told) = producers.link(Some("bob"), &hello("other", 12), LastNode::Peer(None));
        assert_eq!((link.start, link.slot.lock().unsure), (0, 12));
        assert!(told.is_empty());
    }

    #[test]
    fn a_client_back_is_told_how_far_the_server_cannot_tell() {
        let producers = Arc::new(Producers::default());
        let (link, _) = producers.link(Some("ann"), &hello("here", 0), LastNode::Here);
        link.slot.lock().claim(1, Refusal::Missing);
        // Back on this node, having written up to 3: 2 and 3 were not taken.
        let (link, _) = producers.link(Some("ann"), &hello("here", 3), LastNode::Here);
        assert_eq!((link.start, link.unsure), (1, 0));
        let reply: serde_json::Value =
            serde_json::from_str(&outbox_reply(link.start, "n", &[], link.unsure)).unwrap();
        assert_eq!(reply["p"], serde_json::json!({"acked": 1, "node": "n"}));
        // From a node that cannot be asked: up to what it wrote.
        let (link, _) = producers.link(Some("bob"), &hello("other", 3), LastNode::Peer(None));
        assert_eq!((link.start, link.unsure), (0, 3));
        let reply: serde_json::Value =
            serde_json::from_str(&outbox_reply(link.start, "n", &[], link.unsure)).unwrap();
        assert_eq!(reply["p"]["unsure"], 3);
        // A message whose handler never ended.
        let (link, _) = producers.link(Some("cid"), &hello("here", 0), LastNode::Here);
        {
            let mut state = link.slot.lock();
            state.claim(1, Refusal::Missing);
            state.deciding(1);
        }
        let (link, _) = producers.link(Some("cid"), &hello("here", 2), LastNode::Here);
        assert_eq!((link.start, link.unsure), (0, 1));
    }

    /// Refused and taken in turn, past the bound: the first refusal is let
    /// go.
    fn past_the_bound(state: &mut SlotState) -> u64 {
        for n in 0..(MAX_HOLES as u64 + 1) {
            let refused = 2 * n + 1;
            state.claim(refused, Refusal::Missing);
            state.refuse(refused, refused, Refusal::Busy);
            state.claim(refused + 1, Refusal::Missing);
        }
        2 * MAX_HOLES as u64 + 2
    }

    #[test]
    fn a_client_waiting_for_a_refusal_let_go_is_not_told_it_was_taken() {
        let producers = Arc::new(Producers::default());
        let (link, _) = producers.link(Some("ann"), &hello("here", 0), LastNode::Here);
        let last = past_the_bound(&mut link.slot.lock());
        let comes_back = |from| Hello {
            from,
            ..hello("here", last)
        };
        // Waiting from 1 on: 1 may have been refused, and comes again.
        let (link, told) = producers.link(Some("ann"), &comes_back(1), LastNode::Here);
        assert_eq!(link.start, 0);
        assert_eq!(told.len(), MAX_HOLES);
        assert_eq!(claim(&link.slot, 1), Claim::Unsure);
        // A client that heard of 1 is told everything as it stands.
        let (link, _) = producers.link(Some("ann"), &comes_back(2), LastNode::Here);
        assert_eq!(link.start, last);
    }

    #[test]
    fn what_the_last_node_could_not_tell_it_cannot_tell_here_either() {
        let a = Arc::new(Producers::default());
        let (link, _) = a.link(Some("ann"), &hello("a", 0), LastNode::Here);
        let last = {
            let mut state = link.slot.lock();
            let last = past_the_bound(&mut state);
            // The handler of the next one never ended.
            state.claim(last + 1, Refusal::Missing);
            state.deciding(last + 1);
            last
        };
        let decided = a.hand_over(Some("ann"), "p").unwrap();
        assert_eq!((decided.forgot, decided.undecided), (1, Some(last + 1)));

        let b = Arc::new(Producers::default());
        let comes = Hello {
            from: 1,
            ..hello("a", last + 1)
        };
        let (link, told) = b.link(Some("ann"), &comes, LastNode::Peer(Some(&decided)));
        assert_eq!(link.start, 0, "1 may have been refused");
        assert_eq!(told.len(), MAX_HOLES);
        assert_eq!(claim(&link.slot, 1), Claim::Unsure);
        assert_eq!(claim(&link.slot, 2), Claim::Repeat);
        assert_eq!(
            claim(&link.slot, last + 1),
            Claim::Unsure,
            "may have done its work"
        );

        // A client that heard of all but the last is told up to before it.
        let c = Arc::new(Producers::default());
        let comes = Hello {
            from: last + 1,
            ..hello("a", last + 1)
        };
        let (link, _) = c.link(Some("ann"), &comes, LastNode::Peer(Some(&decided)));
        assert_eq!(link.start, last);
        assert_eq!(claim(&link.slot, last + 1), Claim::Unsure);
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
    fn the_answer_tells_the_refusals_the_client_may_not_have_heard_of() {
        let reply: serde_json::Value = serde_json::from_str(&outbox_reply(5, "n", &[], 0)).unwrap();
        assert_eq!(
            reply,
            serde_json::json!({"c": "WSE", "t": "outbox", "p": {"acked": 5, "node": "n"}, "v": 1})
        );
        let refused = [
            Hole {
                from: 2,
                to: 3,
                why: Refusal::RateLimited,
            },
            Hole {
                from: 5,
                to: 5,
                why: Refusal::Busy,
            },
        ];
        let reply: serde_json::Value =
            serde_json::from_str(&outbox_reply(5, "n", &refused, 0)).unwrap();
        assert_eq!(
            reply["p"]["refused"],
            serde_json::json!([
                {"from": 2, "s": 3, "code": "rate_limited"},
                {"s": 5, "code": "busy"},
            ])
        );
    }

    #[test]
    fn the_number_is_read_from_the_first_bytes() {
        assert_eq!(number_in(br#"{"s":17,"t":"m"}"#), Some(17));
        assert_eq!(number_in(br#"{"s":3}"#), Some(3));
        assert_eq!(number_in(br#"WSE{"s":4,"t":"m"}"#), Some(4));
        assert_eq!(number_in(br#"{"t":"m","s":17}"#), None, "not first");
        assert_eq!(number_in(br#"{"s":0,"t":"m"}"#), None);
        assert_eq!(number_in(br#"{"s":17"#), None, "cut short");
        assert_eq!(number_in(br#"{"s":99999999999999999999,"t":"m"}"#), None);
        assert_eq!(number_in(b""), None);
        // In msgpack: `M:` and a map whose first key is `s`, its number in
        // each width.
        assert_eq!(number_in(b"M:\x82\xa1s\x11\xa1t\xa1m"), Some(17));
        assert_eq!(number_in(b"M:\x81\xa1s\xcc\xc8"), Some(200));
        assert_eq!(number_in(b"M:\x81\xa1s\xcd\x01\x00"), Some(256));
        assert_eq!(number_in(b"M:\x81\xa1s\xce\x00\x01\x00\x00"), Some(65_536));
        let mut wide = b"M:\x81\xa1s\xcf".to_vec();
        wide.extend_from_slice(&(1u64 << 40).to_be_bytes());
        assert_eq!(number_in(&wide), Some(1 << 40));
        assert_eq!(number_in(b"M:\xde\x00\x01\xa1s\x05"), Some(5));
        assert_eq!(number_in(b"M:\x82\xa1t\xa1m\xa1s\x11"), None, "not first");
        assert_eq!(number_in(b"M:\x81\xa1s\x00"), None);
        assert_eq!(number_in(b"M:\x81\xa1s\xce\x00\x01"), None, "cut short");
        assert_eq!(number_in(b"M:\x81\xa1s\xa1x"), None, "not a number");
        assert_eq!(number_in(b"M:\x80"), None);
    }

    #[test]
    fn answers_put_refusals_before_the_ack() {
        let mut pending = Pending::default();
        pending.taken(1);
        pending.dropped();
        let why = pending.skipped();
        pending.refuse(2, 3, why);
        pending.taken(4);
        pending.refuse(5, 5, Refusal::Busy);
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
        assert_eq!(pending.skipped(), Refusal::Missing);
    }
}
