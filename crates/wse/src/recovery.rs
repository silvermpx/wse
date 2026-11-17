use bytes::Bytes;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Global counter to guarantee unique epochs even for rapid buffer re-creation.
static EPOCH_COUNTER: AtomicU32 = AtomicU32::new(0);

// ---------------------------------------------------------------------------
// RecoveryResult
// ---------------------------------------------------------------------------

pub(crate) enum RecoveryResult {
    /// Successfully recovered messages after the requested offset.
    Recovered {
        publications: Vec<Bytes>,
        epoch: u32,
        offset: u64,
    },
    /// Epoch mismatch or offset too old -- client must re-subscribe.
    NotRecovered { epoch: u32, offset: u64 },
    /// No history buffer exists for the requested topic.
    NoHistory,
}

// ---------------------------------------------------------------------------
// RecoveryConfig
// ---------------------------------------------------------------------------

pub(crate) struct RecoveryConfig {
    /// Power-of-two exponent for the per-topic message limit (default 7 = 128
    /// msgs). Clamped to `MAX_BUFFER_SIZE_BITS`.
    pub(crate) buffer_size_bits: u32,
    /// How long (seconds) an idle topic buffer lives before cleanup evicts it.
    pub(crate) history_ttl_secs: u64,
    /// Maximum number of messages returned in a single recovery response.
    pub(crate) max_recovery_messages: usize,
    /// Global memory budget across all topic buffers (bytes).
    pub(crate) global_memory_budget: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            buffer_size_bits: 7,   // 128 slots
            history_ttl_secs: 300, // 5 minutes
            max_recovery_messages: 500,
            global_memory_budget: 256 * 1024 * 1024, // 256 MB
        }
    }
}

/// How often expired and over-budget buffers are removed.
pub(crate) const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// How often the arrival order is marked, and how many marks are kept at
/// most (a day of them).
pub(crate) const MARK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_MARKS: usize = 86_400;

/// How much before a peer's interest began the messages it is sent go back:
/// a mark's interval, and the time its request took to come.
const LATELY_MARGIN: Duration = Duration::from_secs(3);

/// Largest per-topic message limit: 2^16 = 65536 messages.
pub(crate) const MAX_BUFFER_SIZE_BITS: u32 = 16;

/// Bytes charged to the memory budget for a buffer's struct, its map entry and
/// bookkeeping, on top of its topic key and its message slots.
const BUFFER_OVERHEAD: usize = 128;

// ---------------------------------------------------------------------------
// RecoveryEntry -- single message stored in the foreign ring buffer
// ---------------------------------------------------------------------------

struct RecoveryEntry {
    /// Pre-framed WebSocket bytes (Arc-shared with broadcast path -- zero-copy).
    data: Bytes,
    /// Monotonic offset within this topic buffer.
    offset: u64,
    /// Its place in this node's arrival order (`RecoveryManager::arrival`).
    seq: u64,
}

/// A stored message and its place in this node's arrival order.
type Arrived = (u64, Bytes);

/// The streams' messages in one list: each stream in its own order, the
/// streams interleaved by when their messages arrived here.
fn interleave(streams: Vec<Vec<Arrived>>) -> Vec<Bytes> {
    let mut out = Vec::with_capacity(streams.iter().map(Vec::len).sum());
    let mut streams: Vec<_> = streams
        .into_iter()
        .map(|stream| stream.into_iter().peekable())
        .collect();
    while let Some((_, i)) = streams
        .iter_mut()
        .enumerate()
        .filter_map(|(i, stream)| stream.peek().map(|(seq, _)| (*seq, i)))
        .min()
    {
        if let Some((_, data)) = streams[i].next() {
            out.push(data);
        }
    }
    out
}

/// The topic of a foreign buffer's key (`topic\0epoch`).
fn foreign_topic(key: &str) -> &str {
    key.split_once('\0').map_or(key, |(topic, _)| topic)
}

// ---------------------------------------------------------------------------
// TopicRecoveryBuffer -- per-topic ring buffer
// ---------------------------------------------------------------------------

struct TopicRecoveryBuffer {
    /// Stored messages in offset order; the front holds `tail_offset()`. The
    /// queue grows as messages arrive, so a topic that has seen a handful of
    /// messages does not pay for `capacity` slots.
    entries: VecDeque<(Bytes, u64)>,
    /// Maximum number of messages kept; the oldest is evicted beyond it.
    capacity: usize,
    /// The latest arrival number evicted (0: none): every message that
    /// arrived after it is still here.
    evicted_through: u64,
    /// Next offset to be written (one past the newest entry).
    head_offset: u64,
    /// Random epoch generated on creation -- changes on server restart.
    epoch: u32,
    /// Total bytes of all stored entries' data.
    total_bytes: usize,
    /// Length of the topic key this buffer is stored under.
    key_len: usize,
    /// Timestamp of the most recent push.
    last_write: Instant,
}

impl TopicRecoveryBuffer {
    /// Create an empty buffer that keeps at most `1 << capacity_bits` messages.
    fn new(capacity_bits: u32, key_len: usize) -> Self {
        // Generate a unique epoch: time + PID + counter (no external rand dependency).
        // Counter guarantees uniqueness even for rapid buffer re-creation within same process.
        let epoch = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u32)
            .wrapping_add(std::process::id())
            .wrapping_add(EPOCH_COUNTER.fetch_add(1, Ordering::Relaxed));

        Self {
            entries: VecDeque::new(),
            capacity: 1usize << capacity_bits.min(MAX_BUFFER_SIZE_BITS),
            evicted_through: 0,
            head_offset: 0,
            epoch,
            total_bytes: 0,
            key_len,
            last_write: Instant::now(),
        }
    }

    /// Oldest offset still in the buffer (equals `head_offset` when empty).
    fn tail_offset(&self) -> u64 {
        self.head_offset - self.entries.len() as u64
    }

    /// Bytes this buffer costs: payloads, allocated queue slots, the key and
    /// fixed overhead. This, not just the payload size, is what the memory
    /// budget is checked against.
    fn footprint(&self) -> usize {
        self.total_bytes
            + self.entries.capacity() * size_of::<(Bytes, u64)>()
            + self.key_len
            + BUFFER_OVERHEAD
    }

    /// Push a message into the buffer. Returns the assigned offset.
    ///
    /// When the buffer is full, the oldest entry is evicted.
    fn push(&mut self, data: Bytes, seq: u64) -> u64 {
        if self.entries.len() >= self.capacity
            && let Some((old, old_seq)) = self.entries.pop_front()
        {
            self.total_bytes -= old.len();
            self.evicted_through = self.evicted_through.max(old_seq);
        }

        let offset = self.head_offset;
        self.total_bytes += data.len();
        self.entries.push_back((data, seq));
        self.head_offset += 1;
        self.last_write = Instant::now();

        offset
    }

    /// Recover messages with offset strictly greater than `after_offset`.
    ///
    /// Returns `None` if `after_offset` is older than the tail (gap too large).
    /// Results are capped at `max_messages`.
    fn recover_since(&self, after_offset: u64, max_messages: usize) -> Option<Vec<Arrived>> {
        // If the first message we need (after_offset + 1) is before the tail,
        // those messages have been evicted -- gap too large.
        let start = match after_offset.checked_add(1) {
            Some(s) => s,
            None => return Some(Vec::new()), // u64::MAX means fully up-to-date
        };

        let tail = self.tail_offset();
        if self.head_offset > 0 && start < tail {
            return None;
        }
        if start >= self.head_offset {
            // Nothing to recover -- client is up to date.
            return Some(Vec::new());
        }

        let count = ((self.head_offset - start) as usize).min(max_messages);
        let first = (start - tail) as usize;
        Some(
            self.entries
                .iter()
                .skip(first)
                .take(count)
                .map(|(data, seq)| (*seq, data.clone()))
                .collect(),
        )
    }

    /// The arrival number of the message at `offset`, if it is still here.
    fn seq_at(&self, offset: u64) -> Option<u64> {
        let tail = self.tail_offset();
        (offset >= tail && offset < self.head_offset)
            .then(|| self.entries[(offset - tail) as usize].1)
    }

    /// Where the client's last message (`offset`) stands in this node's
    /// arrival order: its own number, the last evicted one's when it was
    /// that, or the newest's when the client has everything here.
    fn anchor_seq(&self, offset: u64) -> Option<u64> {
        self.seq_at(offset)
            .or_else(|| {
                (offset.checked_add(1) == Some(self.tail_offset()) && self.evicted_through > 0)
                    .then_some(self.evicted_through)
            })
            .or_else(|| {
                (offset.saturating_add(1) >= self.head_offset)
                    .then(|| self.entries.back().map(|(_, seq)| *seq))
                    .flatten()
            })
    }

    /// The whole stream, when this node holds all of it (nothing evicted)
    /// and every message arrived after `seq`.
    fn whole_after(&self, seq: u64) -> Option<Vec<Arrived>> {
        (self.evicted_through == 0 && self.entries.iter().all(|(_, s)| *s > seq)).then(|| {
            self.entries
                .iter()
                .map(|(data, s)| (*s, data.clone()))
                .collect()
        })
    }

    /// The latest arrival number among what it holds or evicted (0: none).
    fn last_seq(&self) -> u64 {
        self.entries
            .back()
            .map_or(self.evicted_through, |(_, seq)| *seq)
    }

    /// The messages that arrived after `seq`; None when one of them was
    /// evicted already.
    fn arrived_after(&self, seq: u64) -> Option<Vec<Arrived>> {
        if self.evicted_through > seq {
            return None;
        }
        Some(
            self.entries
                .iter()
                .filter(|(_, s)| *s > seq)
                .map(|(data, s)| (*s, data.clone()))
                .collect(),
        )
    }
}

// ---------------------------------------------------------------------------
// ForeignRecoveryBuffer -- stores messages from other cluster nodes
// ---------------------------------------------------------------------------
//
// When a cluster peer publishes a message, it attaches its local (epoch, offset).
// This buffer stores those messages keyed by the origin node's epoch so that a
// client reconnecting to *this* node can recover using the original epoch/offset
// even though the messages originated elsewhere.

struct ForeignRecoveryBuffer {
    entries: Box<[Option<RecoveryEntry>]>,
    mask: u64,
    head_offset: u64,
    tail_offset: u64,
    /// The originating node's epoch (not ours).
    epoch: u32,
    total_bytes: usize,
    /// Length of the "topic\0epoch" key this buffer is stored under.
    key_len: usize,
    last_write: Instant,
    initialized: bool,
    /// The latest arrival number evicted (0: none).
    evicted_through: u64,
}

impl ForeignRecoveryBuffer {
    fn new(capacity_bits: u32, epoch: u32, key_len: usize) -> Self {
        let capacity = 1u64 << capacity_bits.min(MAX_BUFFER_SIZE_BITS);
        let mut entries = Vec::with_capacity(capacity as usize);
        entries.resize_with(capacity as usize, || None);
        Self {
            entries: entries.into_boxed_slice(),
            mask: capacity - 1,
            head_offset: 0,
            tail_offset: 0,
            epoch,
            total_bytes: 0,
            key_len,
            last_write: Instant::now(),
            initialized: false,
            evicted_through: 0,
        }
    }

    /// Bytes this buffer costs (the ring's slots are allocated up front,
    /// because peer offsets can arrive with gaps and out of order).
    fn footprint(&self) -> usize {
        self.total_bytes
            + self.entries.len() * size_of::<Option<RecoveryEntry>>()
            + self.key_len
            + BUFFER_OVERHEAD
    }

    /// Insert at the origin node's offset. Handles gaps and out-of-order
    /// arrival. False when it is here already, or older than what is kept:
    /// nothing new for this node.
    fn push_at(&mut self, data: Bytes, origin_offset: u64, seq: u64) -> bool {
        let capacity = self.mask + 1;

        // First message sets the baseline (handles origin_offset = 0 correctly).
        if !self.initialized {
            self.tail_offset = origin_offset;
            self.head_offset = origin_offset;
            self.initialized = true;
        }

        // Skip if already stored (duplicate or behind tail).
        if origin_offset < self.tail_offset || self.seq_at(origin_offset).is_some() {
            return false;
        }

        // Advance head if this is a new high-water mark.
        if origin_offset >= self.head_offset {
            let new_tail = origin_offset.saturating_sub(capacity - 1);
            if new_tail.saturating_sub(self.tail_offset) >= capacity {
                // The jump leaves the whole ring behind: drop everything in one
                // pass. Walking offset by offset would spin for as long as the
                // jump is wide - up to 2^64 steps for a hostile peer's offset.
                for slot in self.entries.iter_mut() {
                    if let Some(old) = slot.take() {
                        self.total_bytes -= old.data.len();
                        self.evicted_through = self.evicted_through.max(old.seq);
                    }
                }
                self.tail_offset = new_tail;
            } else {
                while self.tail_offset < new_tail {
                    let evict_idx = (self.tail_offset & self.mask) as usize;
                    if let Some(old) = self.entries[evict_idx].take() {
                        self.total_bytes -= old.data.len();
                        self.evicted_through = self.evicted_through.max(old.seq);
                    }
                    self.tail_offset += 1;
                }
            }
            self.head_offset = origin_offset.saturating_add(1);
        }

        let idx = (origin_offset & self.mask) as usize;
        let data_len = data.len();

        // Evict any existing entry at this slot (shouldn't happen, but defensive).
        if let Some(old) = self.entries[idx].take() {
            self.total_bytes -= old.data.len();
            self.evicted_through = self.evicted_through.max(old.seq);
        }

        self.entries[idx] = Some(RecoveryEntry {
            data,
            offset: origin_offset,
            seq,
        });
        self.total_bytes += data_len;
        self.last_write = Instant::now();
        true
    }

    /// The last offset held with nothing missing between it and the oldest
    /// kept: what a peer is asked to send after, once its link is back.
    /// None for a buffer that holds nothing to anchor on.
    fn contiguous_through(&self) -> Option<u64> {
        if !self.initialized || self.head_offset <= self.tail_offset {
            return None;
        }
        let mut offset = self.tail_offset;
        while offset < self.head_offset && self.seq_at(offset).is_some() {
            offset += 1;
        }
        offset.checked_sub(1)
    }

    /// Recover messages after `after_offset`, same logic as TopicRecoveryBuffer.
    fn recover_since(&self, after_offset: u64, max_messages: usize) -> Option<Vec<Arrived>> {
        let start = match after_offset.checked_add(1) {
            Some(s) => s,
            None => return Some(Vec::new()),
        };

        if self.head_offset > self.tail_offset && start < self.tail_offset {
            return None;
        }
        if start >= self.head_offset {
            return Some(Vec::new());
        }

        let count = ((self.head_offset - start) as usize).min(max_messages);
        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let offset = start + i as u64;
            let idx = (offset & self.mask) as usize;
            if let Some(entry) = &self.entries[idx] {
                if entry.offset == offset {
                    result.push((entry.seq, entry.data.clone()));
                } else {
                    // Gap in foreign buffer (missed cluster message) -- can't recover.
                    return None;
                }
            } else {
                return None;
            }
        }

        Some(result)
    }

    /// The arrival number of the message at `offset`, if it is here.
    fn seq_at(&self, offset: u64) -> Option<u64> {
        self.entries[(offset & self.mask) as usize]
            .as_ref()
            .filter(|entry| entry.offset == offset)
            .map(|entry| entry.seq)
    }

    /// Where the client's last message (`offset`) stands in this node's
    /// arrival order: its own number; the last evicted one's when it was
    /// that; just before the next one here when it never came here; the
    /// newest's when the client has everything here.
    fn anchor_seq(&self, offset: u64) -> Option<u64> {
        if let Some(seq) = self.seq_at(offset) {
            return Some(seq);
        }
        let next = offset.saturating_add(1);
        if next == self.tail_offset && self.evicted_through > 0 {
            return Some(self.evicted_through);
        }
        if next >= self.head_offset {
            return self.entries.iter().flatten().map(|entry| entry.seq).max();
        }
        self.entries
            .iter()
            .flatten()
            .filter(|entry| entry.offset > offset)
            .min_by_key(|entry| entry.offset)
            .map(|entry| entry.seq.saturating_sub(1))
    }

    /// The whole stream, when this node holds all of it from its first
    /// offset without a hole (nothing evicted) and every message arrived
    /// after `seq`.
    fn whole_after(&self, seq: u64) -> Option<Vec<Arrived>> {
        if !self.initialized || self.tail_offset != 0 || self.evicted_through != 0 {
            return None;
        }
        if self.head_offset > 0 && self.contiguous_through() != Some(self.head_offset - 1) {
            return None;
        }
        let mut held: Vec<&RecoveryEntry> = self.entries.iter().flatten().collect();
        if held.iter().any(|entry| entry.seq <= seq) {
            return None;
        }
        held.sort_unstable_by_key(|entry| entry.offset);
        Some(
            held.into_iter()
                .map(|entry| (entry.seq, entry.data.clone()))
                .collect(),
        )
    }

    /// The latest arrival number among what it holds or evicted (0: none).
    fn last_seq(&self) -> u64 {
        self.entries
            .iter()
            .flatten()
            .map(|entry| entry.seq)
            .max()
            .unwrap_or(0)
            .max(self.evicted_through)
    }

    /// The messages that arrived after `seq`; None when one of them was
    /// evicted already, or one never came (shed on the way, or relayed while
    /// this node had no interest): a hole among them.
    fn arrived_after(&self, seq: u64) -> Option<Vec<Arrived>> {
        if self.evicted_through > seq {
            return None;
        }
        let mut after: Vec<&RecoveryEntry> = self
            .entries
            .iter()
            .flatten()
            .filter(|entry| entry.seq > seq)
            .collect();
        after.sort_unstable_by_key(|entry| entry.offset);
        if let Some(first) = after.first() {
            // What came before them is here (or evicted, or the stream's
            // start): nothing is missing in between.
            let before = first.offset.checked_sub(1);
            let held = before
                .is_none_or(|before| before < self.tail_offset || self.seq_at(before).is_some());
            let contiguous = after.windows(2).all(|w| w[1].offset == w[0].offset + 1);
            if !held || !contiguous {
                return None;
            }
        }
        Some(
            after
                .into_iter()
                .map(|entry| (entry.seq, entry.data.clone()))
                .collect(),
        )
    }
}

// ---------------------------------------------------------------------------
// RecoveryManager -- thread-safe manager for all topic buffers
// ---------------------------------------------------------------------------

pub(crate) struct RecoveryManager {
    buffers: DashMap<String, TopicRecoveryBuffer>,
    /// Foreign buffers: keyed by "topic:epoch" to support multiple origin epochs.
    foreign_buffers: DashMap<String, ForeignRecoveryBuffer>,
    /// The origin epochs each topic has a foreign buffer for: a topic
    /// published on several nodes recovers from all of them.
    foreign_epochs: DashMap<String, Vec<u32>>,
    /// The order messages arrive in on this node, whatever node published
    /// them: a recovery replays every stream of a topic after the client's
    /// last message, in this order.
    arrival: AtomicU64,
    /// Where in that order this node's current, unbroken interest in each
    /// topic began, and when: other nodes relay a topic only while this node
    /// has it, so the relayed streams are whole from there on only.
    interest_since: DashMap<String, (u64, Instant)>,
    /// The arrival order once a second, back to the oldest message this
    /// node holds of its own streams: what it published since a moment is
    /// found by it.
    marks: parking_lot::Mutex<VecDeque<(Instant, u64)>>,
    /// Per topic with buffers left, the latest arrival among the messages of
    /// its buffers removed (expired, or over the budget): a client whose last
    /// message came before it may have missed them, in a stream it never
    /// heard from.
    removed_through: DashMap<String, u64>,
    config: RecoveryConfig,
    total_bytes: AtomicUsize,
}

impl RecoveryManager {
    pub(crate) fn new(config: RecoveryConfig) -> Self {
        Self {
            buffers: DashMap::new(),
            foreign_buffers: DashMap::new(),
            foreign_epochs: DashMap::new(),
            arrival: AtomicU64::new(1),
            interest_since: DashMap::new(),
            marks: parking_lot::Mutex::new(VecDeque::new()),
            removed_through: DashMap::new(),
            config,
            total_bytes: AtomicUsize::new(0),
        }
    }

    /// Reserve this message's `(epoch, offset)`, build the stored bytes from them
    /// via `build`, push, and return the stored bytes.
    ///
    /// The epoch/offset read and the push happen under the SAME DashMap shard
    /// write lock, so the offset handed to `build` is exactly the offset the
    /// message is stored at -- race-free even with concurrent pushers. This lets
    /// the caller weave the recovery coordinates INTO the payload before it is
    /// framed/encrypted, so the live copy and any replayed copy are byte-identical
    /// and the client can dedup / detect gaps from the message itself.
    ///
    /// None when the topic has no buffer and the memory budget is used up: a
    /// new buffer allocates its slots up front, and an application may
    /// publish to any number of new topics between two cleanups. The message
    /// then goes unstamped; its topic is recoverable once there is room.
    pub(crate) fn push_stamped<F>(&self, topic: &str, build: F) -> Option<Bytes>
    where
        F: FnOnce(u32, u64) -> Bytes,
    {
        let mut created = false;
        // A topic that has a buffer is found without making a key.
        let mut entry = match self.buffers.get_mut(topic) {
            Some(entry) => entry,
            None if self.total_bytes.load(Ordering::Relaxed)
                >= self.config.global_memory_budget =>
            {
                // Not kept: a client whose last message of the topic came
                // before it cannot be told it missed nothing.
                self.note_removed(topic, self.arrival.fetch_add(1, Ordering::Relaxed));
                return None;
            }
            None => self.buffers.entry(topic.to_owned()).or_insert_with(|| {
                created = true;
                TopicRecoveryBuffer::new(self.config.buffer_size_bits, topic.len())
            }),
        };

        let buf = entry.value_mut();
        let epoch = buf.epoch;
        let offset = buf.head_offset; // the offset buf.push() is about to assign
        let data = build(epoch, offset);

        let old_bytes = if created { 0 } else { buf.footprint() };
        buf.push(data.clone(), self.arrival.fetch_add(1, Ordering::Relaxed));
        let new_bytes = buf.footprint();

        // Update global byte counter with the delta (accounts for evictions).
        // DashMap entry API holds the shard write lock for this entire block,
        // so cleanup() cannot remove this buffer between the read and update.
        if new_bytes > old_bytes {
            self.total_bytes
                .fetch_add(new_bytes - old_bytes, Ordering::AcqRel);
        } else if old_bytes > new_bytes {
            let _ = self
                .total_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                    Some(cur.saturating_sub(old_bytes - new_bytes))
                });
        }
        Some(data)
    }

    /// Push a pre-built message (recovery coordinates already woven in, or none).
    /// Test-only convenience over push_stamped; production stamps via push_stamped.
    #[cfg(test)]
    pub(crate) fn push(&self, topic: &str, data: Bytes) {
        self.push_stamped(topic, move |_epoch, _offset| data);
    }

    /// Store a message received from a cluster peer in a foreign recovery buffer.
    ///
    /// The epoch and offset belong to the originating node. This lets clients
    /// recover using the original epoch/offset from any node in the cluster.
    /// False when this node has it already: a peer sends what a lost link
    /// missed again, and only what is new here is delivered.
    pub(crate) fn push_foreign(
        &self,
        topic: &str,
        data: Bytes,
        origin_epoch: u32,
        origin_offset: u64,
    ) -> bool {
        // Key: "topic\0epoch" -- null byte separator is safe (never in topic
        // names). Written into this thread's buffer: a buffer that exists is
        // found without allocating a key.
        thread_local! {
            static KEY: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
        }
        KEY.with_borrow_mut(|key| {
            use std::fmt::Write;
            key.clear();
            let _ = write!(key, "{topic}\0{origin_epoch}");
            self.push_foreign_at(key, data, origin_epoch, origin_offset)
        })
    }

    fn push_foreign_at(
        &self,
        key: &str,
        data: Bytes,
        origin_epoch: u32,
        origin_offset: u64,
    ) -> bool {
        let mut created = false;
        let mut entry = match self.foreign_buffers.get_mut(key) {
            Some(entry) => entry,
            // Over the memory budget no new buffer is made: each one
            // allocates its slots up front, and a peer can name any number of
            // fresh (topic, epoch) pairs between two cleanups. The message is
            // still delivered live; only its recovery copy is skipped, and a
            // client whose last message came before it cannot be told it
            // missed nothing.
            None if self.total_bytes.load(Ordering::Relaxed)
                >= self.config.global_memory_budget =>
            {
                self.note_removed(
                    foreign_topic(key),
                    self.arrival.fetch_add(1, Ordering::Relaxed),
                );
                return true;
            }
            None => self
                .foreign_buffers
                .entry(key.to_owned())
                .or_insert_with(|| {
                    created = true;
                    ForeignRecoveryBuffer::new(
                        self.config.buffer_size_bits,
                        origin_epoch,
                        key.len(),
                    )
                }),
        };

        if created && let Some((topic, _)) = key.split_once('\0') {
            let mut epochs = self.foreign_epochs.entry(topic.to_owned()).or_default();
            if !epochs.contains(&origin_epoch) {
                epochs.push(origin_epoch);
            }
        }
        let buf = entry.value_mut();
        let old_bytes = if created { 0 } else { buf.footprint() };
        let new = buf.push_at(
            data,
            origin_offset,
            self.arrival.fetch_add(1, Ordering::Relaxed),
        );
        let new_bytes = buf.footprint();

        if new_bytes > old_bytes {
            self.total_bytes
                .fetch_add(new_bytes - old_bytes, Ordering::AcqRel);
        } else if old_bytes > new_bytes {
            let _ = self
                .total_bytes
                .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                    Some(cur.saturating_sub(old_bytes - new_bytes))
                });
        }
        new
    }

    /// Where this node stands in each stream relayed to it: (topic, origin
    /// epoch, the last offset held with nothing missing before it). A peer
    /// whose link was lost sends what came after it once the link is back.
    pub(crate) fn foreign_positions(&self) -> Vec<(String, u32, u64)> {
        self.foreign_buffers
            .iter()
            .filter_map(|entry| {
                let (topic, _) = entry.key().split_once('\0')?;
                let through = entry.value().contiguous_through()?;
                Some((topic.to_owned(), entry.value().epoch, through))
            })
            .collect()
    }

    /// What this node published to `topic` in its stream `epoch` after
    /// `after`, oldest first, for a peer that missed it: from the oldest kept
    /// when some of it is gone, nothing when the stream is not this node's.
    pub(crate) fn published_after(&self, topic: &str, epoch: u32, after: u64) -> Vec<(u64, Bytes)> {
        let Some(buf) = self.buffers.get(topic) else {
            return Vec::new();
        };
        if buf.epoch != epoch {
            return Vec::new();
        }
        let tail = buf.tail_offset();
        let from = after.saturating_add(1).max(tail);
        buf.entries
            .iter()
            .skip((from - tail) as usize)
            .zip(from..)
            .map(|((data, _), offset)| (offset, data.clone()))
            .collect()
    }

    /// Attempt to recover messages for a topic after a given offset.
    ///
    /// First checks the local buffer. If the epoch matches, recovers from local.
    /// If the epoch doesn't match local, checks foreign buffers (cluster-replicated).
    /// This allows clients to recover from any node, even after reconnecting to a
    /// different node than the one that originally published the messages.
    pub(crate) fn recover(&self, topic: &str, epoch: u32, after_offset: u64) -> RecoveryResult {
        let max = self.config.max_recovery_messages;
        // The stream of the client's last message: the local one, or one
        // relayed from another node, by its epoch. What it missed there, and
        // where that last message stands in this node's arrival order.
        let anchored = if let Some(buf) = self.buffers.get(topic)
            && buf.epoch == epoch
        {
            let missed = buf
                .head_offset
                .saturating_sub(after_offset.saturating_add(1)) as usize;
            Some((
                if missed <= max {
                    buf.recover_since(after_offset, max)
                } else {
                    None
                },
                buf.anchor_seq(after_offset),
                buf.head_offset,
            ))
        } else if let Some(fbuf) = self.foreign_buffers.get(&format!("{topic}\0{epoch}"))
            && fbuf.epoch == epoch
        {
            let missed = fbuf
                .head_offset
                .saturating_sub(after_offset.saturating_add(1)) as usize;
            Some((
                if missed <= max {
                    fbuf.recover_since(after_offset, max)
                } else {
                    None
                },
                fbuf.anchor_seq(after_offset),
                fbuf.head_offset,
            ))
        } else {
            None
        };
        let Some((own, anchor_seq, head)) = anchored else {
            // No matching buffer found at all.
            return match self.buffers.get(topic) {
                // Local buffer exists but with different epoch -- tell client to re-subscribe.
                Some(buf) => RecoveryResult::NotRecovered {
                    epoch: buf.epoch,
                    offset: buf.head_offset,
                },
                None => RecoveryResult::NoHistory,
            };
        };
        let not_recovered = RecoveryResult::NotRecovered {
            epoch,
            offset: head,
        };
        // A history of the topic removed, or a message not kept, from after
        // the client's last message: what it missed there is gone.
        if let Some(removed) = self.removed_through.get(topic).map(|r| *r)
            && anchor_seq.is_none_or(|seq| removed > seq)
        {
            return not_recovered;
        }
        let Some(mut publications) = own else {
            return not_recovered;
        };
        // Relayed streams are whole only within this node's unbroken
        // interest in the topic: one that began after the client's last
        // message may have missed what other nodes published in between.
        let relayed_whole = |seq: Option<u64>| {
            let since = self
                .interest_since
                .get(topic)
                .map_or(u64::MAX, |since| since.0);
            seq.is_some_and(|seq| seq >= since)
        };
        let anchored_relayed = !self
            .buffers
            .get(topic)
            .is_some_and(|buf| buf.epoch == epoch);
        let relays = self
            .foreign_epochs
            .get(topic)
            .is_some_and(|epochs| !epochs.is_empty());
        if (anchored_relayed || relays) && !relayed_whole(anchor_seq) {
            return not_recovered;
        }
        // The topic's other streams (published here, or on other nodes):
        // what arrived after the client's last message, which each of them
        // must still hold whole. Without that message here, nothing places
        // them: a stream with anything in it then cannot be recovered.
        let mut streams: Vec<Option<Vec<Arrived>>> = Vec::new();
        if let Some(buf) = self.buffers.get(topic)
            && buf.epoch != epoch
        {
            streams.push(match anchor_seq {
                Some(seq) => buf.arrived_after(seq),
                None => buf.entries.is_empty().then(Vec::new),
            });
        }
        let others: Vec<u32> = self
            .foreign_epochs
            .get(topic)
            .map(|epochs| epochs.iter().copied().filter(|e| *e != epoch).collect())
            .unwrap_or_default();
        for other in others {
            if let Some(fbuf) = self.foreign_buffers.get(&format!("{topic}\0{other}")) {
                streams.push(match anchor_seq {
                    Some(seq) => fbuf.arrived_after(seq),
                    None => fbuf.arrived_after(u64::MAX),
                });
            }
        }
        let mut merged = false;
        for stream in streams {
            let Some(mut more) = stream else {
                return not_recovered;
            };
            merged |= !more.is_empty();
            publications.append(&mut more);
        }
        if publications.len() > max {
            return not_recovered;
        }
        if merged {
            // In the order they arrived here.
            publications.sort_unstable_by_key(|(seq, _)| *seq);
        }
        RecoveryResult::Recovered {
            publications: publications.into_iter().map(|(_, data)| data).collect(),
            epoch,
            offset: head.saturating_sub(1),
        }
    }

    /// Recover for a client that names the last offset it holds of each
    /// stream of `topic` (`named`: epoch, offset), and its last message
    /// (`anchor`).
    ///
    /// Each named stream comes from its own offset, so the order the streams
    /// arrived in on the client's last node does not matter. A stream it does
    /// not name comes only whole: held here from its first message, every
    /// one of which arrived after the anchor. Anything else is not
    /// recovered, because nothing here proves what the client missed: a
    /// named stream this node does not hold, or holds with a hole or only
    /// since its interest in the topic began again; an unnamed one with a
    /// message from before the anchor; a buffer of the topic removed with
    /// messages from after it; more than `max_recovery_messages`.
    pub(crate) fn recover_streams(
        &self,
        topic: &str,
        anchor: (u32, u64),
        named: &[(u32, u64)],
    ) -> RecoveryResult {
        let max = self.config.max_recovery_messages;
        let not_recovered = RecoveryResult::NotRecovered {
            epoch: anchor.0,
            offset: anchor.1,
        };
        let position = |epoch: u32| named.iter().find(|(e, _)| *e == epoch).map(|(_, o)| *o);
        let since = self.interest_since.get(topic).map(|since| since.0);
        let foreign_epochs: Vec<u32> = self
            .foreign_epochs
            .get(topic)
            .map(|epochs| epochs.clone())
            .unwrap_or_default();

        // Where the client's last message stands in this node's arrival
        // order, if it is here.
        let local_anchor = self
            .buffers
            .get(topic)
            .filter(|buf| buf.epoch == anchor.0)
            .map(|buf| buf.seq_at(anchor.1));
        let anchor_seq = match local_anchor {
            Some(seq) => seq,
            None => self
                .foreign_buffers
                .get(&format!("{topic}\0{}", anchor.0))
                .and_then(|fbuf| fbuf.seq_at(anchor.1)),
        };
        if let Some(removed) = self.removed_through.get(topic).map(|r| *r)
            && anchor_seq.is_none_or(|seq| removed > seq)
        {
            return not_recovered;
        }

        let mut streams: Vec<Vec<Arrived>> = Vec::with_capacity(foreign_epochs.len() + 1);
        let mut missed = 0usize;
        let mut head = anchor.1.saturating_add(1);
        let mut seen: Vec<u32> = Vec::with_capacity(foreign_epochs.len() + 1);

        if let Some(buf) = self.buffers.get(topic) {
            seen.push(buf.epoch);
            if buf.epoch == anchor.0 {
                head = head.max(buf.head_offset);
            }
            let more = match position(buf.epoch) {
                Some(offset) => {
                    missed += buf.head_offset.saturating_sub(offset.saturating_add(1)) as usize;
                    if missed > max {
                        return not_recovered;
                    }
                    buf.recover_since(offset, max)
                }
                None => anchor_seq.and_then(|seq| buf.whole_after(seq)),
            };
            let Some(more) = more else {
                return not_recovered;
            };
            streams.push(more);
        }

        for epoch in foreign_epochs {
            let Some(fbuf) = self.foreign_buffers.get(&format!("{topic}\0{epoch}")) else {
                continue;
            };
            seen.push(epoch);
            if epoch == anchor.0 {
                head = head.max(fbuf.head_offset);
            }
            // Relayed here only while this node has the topic: whole only
            // from where its current interest began.
            let more = match position(epoch) {
                Some(offset) => {
                    // The first message it needs, or with none, the newest
                    // here: everything after it came while this node had
                    // the topic.
                    let first = offset.saturating_add(1);
                    let from = if first >= fbuf.head_offset {
                        fbuf.seq_at(fbuf.head_offset.saturating_sub(1))
                    } else {
                        fbuf.seq_at(first)
                    };
                    let covered = from.is_some_and(|seq| since.is_some_and(|since| seq >= since));
                    missed += fbuf.head_offset.saturating_sub(first) as usize;
                    if !covered || missed > max {
                        return not_recovered;
                    }
                    fbuf.recover_since(offset, max)
                }
                None => anchor_seq
                    .filter(|_| {
                        fbuf.entries
                            .iter()
                            .flatten()
                            .map(|entry| entry.seq)
                            .min()
                            .is_none_or(|first| since.is_some_and(|since| first >= since))
                    })
                    .and_then(|seq| fbuf.whole_after(seq)),
            };
            let Some(more) = more else {
                return not_recovered;
            };
            streams.push(more);
        }

        // A stream the client names that this node does not hold.
        if named.iter().any(|(epoch, _)| !seen.contains(epoch)) {
            return not_recovered;
        }
        if streams.iter().map(Vec::len).sum::<usize>() > max {
            return not_recovered;
        }
        RecoveryResult::Recovered {
            publications: interleave(streams),
            epoch: anchor.0,
            offset: head.saturating_sub(1),
        }
    }

    /// What the streams of `topic` hold beyond `before` (each one's last
    /// offset at some moment; a stream not in it, from its first message),
    /// in arrival order: what was published since. None past
    /// `max_recovery_messages`, or when a stream no longer holds all of it.
    pub(crate) fn published_since(&self, topic: &str, before: &[(u32, u64)]) -> Option<Vec<Bytes>> {
        let max = self.config.max_recovery_messages;
        let last = |epoch: u32| before.iter().find(|(e, _)| *e == epoch).map(|(_, o)| *o);
        let mut streams: Vec<Vec<Arrived>> = Vec::new();
        let mut total = 0u64;
        if let Some(buf) = self.buffers.get(topic) {
            let more = match last(buf.epoch) {
                Some(offset) => {
                    total += buf.head_offset.saturating_sub(offset.saturating_add(1));
                    buf.recover_since(offset, max)?
                }
                None => {
                    total += buf.head_offset;
                    buf.whole_after(0)?
                }
            };
            streams.push(more);
        }
        let foreign_epochs: Vec<u32> = self
            .foreign_epochs
            .get(topic)
            .map(|epochs| epochs.clone())
            .unwrap_or_default();
        for epoch in foreign_epochs {
            let Some(fbuf) = self.foreign_buffers.get(&format!("{topic}\0{epoch}")) else {
                continue;
            };
            let more = match last(epoch) {
                Some(offset) => {
                    total += fbuf.head_offset.saturating_sub(offset.saturating_add(1));
                    fbuf.recover_since(offset, max)?
                }
                None => {
                    total += fbuf.head_offset;
                    fbuf.whole_after(0)?
                }
            };
            streams.push(more);
        }
        let held = streams.iter().map(Vec::len).sum::<usize>();
        if total > max as u64 || held as u64 != total {
            return None;
        }
        Some(interleave(streams))
    }

    /// The streams this node holds of `topic`: each one's epoch and the last
    /// offset here. Read once a connection subscribed, it is where the
    /// connection starts in each: what comes after reaches it live.
    pub(crate) fn streams(&self, topic: &str) -> Vec<(u32, u64)> {
        let mut streams = Vec::new();
        if let Some(buf) = self.buffers.get(topic)
            && buf.head_offset > 0
        {
            streams.push((buf.epoch, buf.head_offset - 1));
        }
        let foreign_epochs: Vec<u32> = self
            .foreign_epochs
            .get(topic)
            .map(|epochs| epochs.clone())
            .unwrap_or_default();
        for epoch in foreign_epochs {
            if let Some(fbuf) = self.foreign_buffers.get(&format!("{topic}\0{epoch}"))
                && fbuf.initialized
                && fbuf.head_offset > 0
            {
                streams.push((epoch, fbuf.head_offset - 1));
            }
        }
        streams
    }

    /// Get the current (epoch, head_offset) for a topic, if a buffer exists.
    pub(crate) fn get_position(&self, topic: &str) -> Option<(u32, u64)> {
        self.buffers
            .get(topic)
            .map(|buf| (buf.epoch, buf.head_offset))
    }

    /// Evict stale topic buffers based on TTL and global memory budget.
    ///
    /// First pass: remove any buffer whose `last_write` is older than the TTL.
    /// Second pass (if still over budget): collect remaining buffers sorted by
    /// `last_write` (oldest first) and remove them until under budget.
    /// Both local and foreign buffers are cleaned up.
    pub(crate) fn cleanup(&self) {
        // Compare ages, not `now - ttl`: that subtraction panics when the TTL is
        // longer than the machine's uptime (e.g. a service started at boot on
        // Windows), and a panic here would stop all eviction for good.
        let ttl = Duration::from_secs(self.config.history_ttl_secs);

        // First pass: remove TTL-expired local buffers.
        let mut to_remove = Vec::new();
        for entry in self.buffers.iter() {
            if entry.value().last_write.elapsed() > ttl {
                to_remove.push(entry.key().clone());
            }
        }
        for key in &to_remove {
            if let Some((_, removed)) = self.buffers.remove(key) {
                self.note_removed(key, removed.last_seq());
                let _ = self
                    .total_bytes
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        Some(cur.saturating_sub(removed.footprint()))
                    });
            }
        }

        // First pass: remove TTL-expired foreign buffers.
        let mut foreign_to_remove = Vec::new();
        for entry in self.foreign_buffers.iter() {
            if entry.value().last_write.elapsed() > ttl {
                foreign_to_remove.push(entry.key().clone());
            }
        }
        for key in &foreign_to_remove {
            if let Some(removed) = self.remove_foreign(key) {
                self.note_removed(foreign_topic(key), removed.last_seq());
                let _ = self
                    .total_bytes
                    .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        Some(cur.saturating_sub(removed.footprint()))
                    });
            }
        }

        // Second pass: if still over budget, evict LRU across both local and foreign.
        if self.total_bytes.load(Ordering::Relaxed) > self.config.global_memory_budget {
            // Collect candidates from both maps. Tag with 'L' (local) or 'F' (foreign).
            let mut candidates: Vec<(String, Instant, usize, bool)> = Vec::new();

            for e in self.buffers.iter() {
                candidates.push((
                    e.key().clone(),
                    e.value().last_write,
                    e.value().footprint(),
                    true, // is_local
                ));
            }
            for e in self.foreign_buffers.iter() {
                candidates.push((
                    e.key().clone(),
                    e.value().last_write,
                    e.value().footprint(),
                    false, // is_foreign
                ));
            }

            // Sort by last_write ascending (oldest first).
            candidates.sort_by_key(|(_, lw, _, _)| *lw);

            for (key, _, _, is_local) in candidates {
                if self.total_bytes.load(Ordering::Relaxed) <= self.config.global_memory_budget {
                    break;
                }
                if is_local {
                    if let Some((_, removed)) = self.buffers.remove(&key) {
                        self.note_removed(&key, removed.last_seq());
                        let _ = self.total_bytes.try_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |cur| Some(cur.saturating_sub(removed.footprint())),
                        );
                    }
                } else if let Some(removed) = self.remove_foreign(&key) {
                    self.note_removed(foreign_topic(&key), removed.last_seq());
                    let _ =
                        self.total_bytes
                            .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                                Some(cur.saturating_sub(removed.footprint()))
                            });
                }
            }
        }

        // A topic with no buffer left recovers nothing: what was removed of
        // it no longer matters.
        let gone: Vec<String> = self
            .removed_through
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|topic| {
                !self.buffers.contains_key(topic) && !self.foreign_epochs.contains_key(topic)
            })
            .collect();
        for topic in gone {
            self.removed_through.remove(&topic);
        }
        self.trim_marks();
    }

    /// Messages of `topic` up to arrival `seq` are not held here (a buffer
    /// removed, or a message with no room to keep it).
    fn note_removed(&self, topic: &str, seq: u64) {
        if seq == 0 {
            return;
        }
        if let Some(mut through) = self.removed_through.get_mut(topic) {
            *through = (*through).max(seq);
            return;
        }
        let mut through = self.removed_through.entry(topic.to_owned()).or_insert(0);
        *through = (*through).max(seq);
    }

    /// This node took an interest in `topic` (its first local subscriber,
    /// none lingering): other nodes relay it from now on.
    pub(crate) fn interest_began(&self, topic: &str) {
        self.interest_since.insert(
            topic.to_owned(),
            (self.arrival.load(Ordering::Relaxed), Instant::now()),
        );
    }

    /// The topics this node has, each with how long ago its current interest
    /// began (milliseconds): a node it links to again sends what it
    /// published to them since, the streams it did not name included.
    pub(crate) fn interests(&self) -> Vec<(String, u64)> {
        self.interest_since
            .iter()
            .map(|entry| {
                let age = entry.value().1.elapsed().as_millis();
                (entry.key().clone(), u64::try_from(age).unwrap_or(u64::MAX))
            })
            .collect()
    }

    /// Marks where the arrival order stands now, unless nothing arrived
    /// since the last mark; called once a second.
    pub(crate) fn mark(&self) {
        let next = self.arrival.load(Ordering::Relaxed);
        let mut marks = self.marks.lock();
        if marks.back().is_some_and(|(_, last)| *last == next) {
            return;
        }
        if marks.len() == MAX_MARKS {
            marks.pop_front();
        }
        marks.push_back((Instant::now(), next));
    }

    /// Lets go the marks from before the oldest message this node holds of
    /// its own streams, but the last of them.
    fn trim_marks(&self) {
        let oldest = self
            .buffers
            .iter()
            .filter_map(|buf| buf.entries.front().map(|(_, seq)| *seq))
            .min()
            .unwrap_or(u64::MAX);
        let mut marks = self.marks.lock();
        while marks.len() > 1 && marks[1].1 <= oldest {
            marks.pop_front();
        }
    }

    /// The first arrival number given out after `at`, to a mark: 0 before
    /// the first.
    fn arrival_at(&self, at: Instant) -> u64 {
        let marks = self.marks.lock();
        let after = marks.partition_point(|(mark, _)| *mark <= at);
        after.checked_sub(1).map_or(0, |i| marks[i].1)
    }

    /// What this node published to `topic` in the last `age` (and a little
    /// before), with its stream's epoch and each message's offset, for a peer
    /// that has the topic but never heard of the stream: None when this node
    /// publishes none to it, or the peer named it (`named`).
    pub(crate) fn published_lately(
        &self,
        topic: &str,
        age: Duration,
        named: impl Fn(u32) -> bool,
    ) -> Option<(u32, Vec<(u64, Bytes)>)> {
        let since = Instant::now()
            .checked_sub(age.saturating_add(LATELY_MARGIN))
            .map_or(0, |at| self.arrival_at(at));
        let buf = self.buffers.get(topic)?;
        if named(buf.epoch) {
            return None;
        }
        let tail = buf.tail_offset();
        let messages = buf
            .entries
            .iter()
            .zip(tail..)
            .filter(|((_, seq), _)| *seq >= since)
            .map(|((data, _), offset)| (offset, data.clone()))
            .collect();
        Some((buf.epoch, messages))
    }

    /// This node no longer has `topic`: its relayed streams stop.
    pub(crate) fn interest_ended(&self, topic: &str) {
        self.interest_since.remove(topic);
    }

    /// How long a topic stays of interest after its last local subscriber
    /// left: what other nodes publish meanwhile is still relayed here, for
    /// the client that comes back within the history's lifetime.
    pub(crate) fn interest_linger(&self) -> Duration {
        // A history kept "forever" lingers ten years: an instant that far
        // out is still one the clock can name.
        const MAX_LINGER_SECS: u64 = 10 * 365 * 86_400;
        Duration::from_secs(self.config.history_ttl_secs.min(MAX_LINGER_SECS))
    }

    /// Removes the foreign buffer `key` (`topic\0epoch`) and its epoch from
    /// its topic's streams at once: a relay that makes the buffer anew
    /// meanwhile waits for both, then names the stream again. The buffer's
    /// lock is taken before the streams', as a relay takes them.
    fn remove_foreign(&self, key: &str) -> Option<ForeignRecoveryBuffer> {
        let (_, removed) = self.foreign_buffers.remove_if(key, |key, removed| {
            #[cfg(test)]
            REMOVING.with_borrow(|hook| hook.as_ref().map(|hook| hook()));
            self.forget_foreign(key, removed.epoch);
            true
        })?;
        Some(removed)
    }

    /// A foreign buffer (`topic\0epoch`) is gone: its topic no longer
    /// recovers from that epoch.
    fn forget_foreign(&self, key: &str, epoch: u32) {
        if let Some((topic, _)) = key.split_once('\0') {
            if let Some(mut epochs) = self.foreign_epochs.get_mut(topic) {
                epochs.retain(|e| *e != epoch);
            }
            self.foreign_epochs
                .remove_if(topic, |_, epochs| epochs.is_empty());
        }
    }

    /// How long, in seconds, a stream that receives nothing is still kept
    /// here (its history's lifetime, until the next cleanup): a client that
    /// heard nothing of a stream for longer can forget it.
    pub(crate) fn history_secs(&self) -> u64 {
        self.config
            .history_ttl_secs
            .saturating_add(CLEANUP_INTERVAL.as_secs())
    }

    /// Return the number of topic buffers currently held.
    pub(crate) fn topic_count(&self) -> usize {
        self.buffers.len()
    }

    /// Return the number of buffers holding messages relayed by cluster peers
    /// (one per origin topic and epoch).
    #[cfg(test)]
    pub(crate) fn foreign_buffer_count(&self) -> usize {
        self.foreign_buffers.len()
    }

    /// Return the memory charged to recovery: payloads plus buffer overhead,
    /// for local and foreign buffers.
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    /// Runs as a foreign buffer is removed: a test does there what another
    /// worker may do at that point.
    static REMOVING: std::cell::RefCell<Option<Box<dyn Fn()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_foreign_stream_made_again_as_cleanup_removes_it_is_still_recovered() {
        let mgr = std::sync::Arc::new(RecoveryManager::new(RecoveryConfig {
            history_ttl_secs: 0, // immediate expiry
            ..RecoveryConfig::default()
        }));
        mgr.interest_began("topic");
        mgr.push_foreign("topic", make_data("old"), 77, 0);
        std::thread::sleep(std::time::Duration::from_millis(5));
        // As cleanup removes the stream's buffer, a relay of the stream comes
        // on another worker.
        let pushing = std::sync::Arc::new(std::sync::Mutex::new(None));
        let (relay, slot) = (std::sync::Arc::clone(&mgr), std::sync::Arc::clone(&pushing));
        REMOVING.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                let relay = std::sync::Arc::clone(&relay);
                let pushed = std::thread::spawn(move || {
                    relay.push_foreign("topic", make_data("new"), 77, 1);
                    relay.push_foreign("topic", make_data("later"), 77, 2);
                });
                std::thread::sleep(std::time::Duration::from_millis(50));
                *slot.lock().unwrap() = Some(pushed);
            }));
        });
        mgr.cleanup();
        REMOVING.with_borrow_mut(|hook| *hook = None);
        let pushed = pushing
            .lock()
            .unwrap()
            .take()
            .expect("cleanup removed the buffer");
        pushed.join().unwrap();
        // The new buffer is one of the topic's streams: a client that asks
        // from before it is told what it holds, never that nothing came.
        assert_eq!(mgr.streams("topic"), vec![(77, 2)]);
    }

    fn make_data(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    // --- TopicRecoveryBuffer tests ---

    #[test]
    fn test_push_recover_roundtrip() {
        let mut buf = TopicRecoveryBuffer::new(4, 0); // 16 slots
        for i in 0..5 {
            buf.push(make_data(&format!("msg{i}")), i + 1);
        }

        // Recover from offset 2 (should get messages at offsets 3 and 4).
        let recovered = buf.recover_since(2, 500).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(&recovered[0].1[..], b"msg3");
        assert_eq!(&recovered[1].1[..], b"msg4");
    }

    #[test]
    fn test_buffer_overflow() {
        let mut buf = TopicRecoveryBuffer::new(2, 0); // 4 slots
        for i in 0..10 {
            buf.push(make_data(&format!("msg{i}")), i + 1);
        }

        // Tail should have advanced. head=10, tail=6.
        assert_eq!(buf.head_offset, 10);
        assert_eq!(buf.tail_offset(), 6);

        // Trying to recover from offset 3 (well before tail=6) should return None.
        assert!(buf.recover_since(3, 500).is_none());

        // Boundary: after_offset=5, start=6 which IS the tail -- should succeed.
        let boundary = buf.recover_since(5, 500).unwrap();
        assert_eq!(boundary.len(), 4); // offsets 6, 7, 8, 9
        assert_eq!(&boundary[0].1[..], b"msg6");

        // Recover from offset 7 should return msgs 8 and 9.
        let recovered = buf.recover_since(7, 500).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(&recovered[0].1[..], b"msg8");
        assert_eq!(&recovered[1].1[..], b"msg9");
    }

    #[test]
    fn test_epoch_mismatch() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);

        mgr.push("topic1", make_data("hello"));

        let (real_epoch, _) = mgr.get_position("topic1").unwrap();
        let wrong_epoch = real_epoch.wrapping_add(1);

        match mgr.recover("topic1", wrong_epoch, 0) {
            RecoveryResult::NotRecovered { epoch, offset } => {
                assert_eq!(epoch, real_epoch);
                assert_eq!(offset, 1); // head_offset after one push
            }
            _ => panic!("expected NotRecovered for epoch mismatch"),
        }
    }

    #[test]
    fn test_empty_buffer() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);

        match mgr.recover("nonexistent", 0, 0) {
            RecoveryResult::NoHistory => {}
            _ => panic!("expected NoHistory for missing topic"),
        }
    }

    #[test]
    fn test_ttl_cleanup() {
        let config = RecoveryConfig {
            history_ttl_secs: 0, // immediate expiry
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);

        mgr.push("topic1", make_data("data"));
        assert_eq!(mgr.topic_count(), 1);

        // Sleep a tiny bit so last_write is definitely older than TTL=0.
        std::thread::sleep(std::time::Duration::from_millis(5));

        mgr.cleanup();
        assert_eq!(mgr.topic_count(), 0);
        assert_eq!(mgr.total_bytes(), 0);
    }

    #[test]
    fn test_global_budget_eviction() {
        let config = RecoveryConfig {
            global_memory_budget: 10, // tiny budget: 10 bytes
            history_ttl_secs: 3600,   // don't TTL-expire
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);

        // Push data that exceeds budget across multiple topics.
        mgr.push("t1", make_data("aaaaaaa")); // 7 bytes
        std::thread::sleep(std::time::Duration::from_millis(2));
        mgr.push("t2", make_data("bbbbbbb")); // 7 bytes
        std::thread::sleep(std::time::Duration::from_millis(2));
        mgr.push("t3", make_data("ccccccc")); // 7 bytes

        // Total is 21 bytes, well over the 10-byte budget.
        assert!(mgr.total_bytes() > 10);

        mgr.cleanup();

        // After cleanup, should be at or under budget. At least one topic evicted.
        assert!(mgr.total_bytes() <= 10);
        assert!(mgr.topic_count() < 3);
    }

    #[test]
    fn test_get_position() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);

        assert!(mgr.get_position("topic1").is_none());

        mgr.push("topic1", make_data("msg0"));
        mgr.push("topic1", make_data("msg1"));
        mgr.push("topic1", make_data("msg2"));

        let (_epoch, offset) = mgr.get_position("topic1").unwrap();
        assert_eq!(offset, 3); // head_offset after 3 pushes
    }

    #[test]
    fn over_the_budget_a_new_topic_goes_unstamped() {
        let mgr = RecoveryManager::new(RecoveryConfig {
            global_memory_budget: 1,
            ..RecoveryConfig::default()
        });
        assert!(mgr.push_stamped("first", |_, _| make_data("a")).is_some());
        // The budget is used up: a topic with a buffer keeps it, a new one
        // gets none.
        assert!(mgr.push_stamped("first", |_, _| make_data("b")).is_some());
        assert!(mgr.push_stamped("second", |_, _| make_data("c")).is_none());
    }

    #[test]
    fn push_stamped_offset_matches_storage_and_recover() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        let mut reserved = Vec::new();
        for _ in 0..3 {
            let stored = mgr
                .push_stamped("topic1", |epoch, offset| {
                    reserved.push((epoch, offset));
                    make_data(&format!("o={offset}"))
                })
                .unwrap();
            // The returned bytes are exactly what was built for the reserved offset.
            let (_, off) = *reserved.last().unwrap();
            assert_eq!(&stored[..], format!("o={off}").as_bytes());
        }
        // Offsets are assigned 0,1,2 and the epoch is stable across pushes -- i.e.
        // the offset handed to the builder is exactly the storage offset.
        assert_eq!(
            reserved.iter().map(|(_, o)| *o).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(reserved.iter().all(|(e, _)| *e == reserved[0].0));

        // recover() returns the stamped bytes at the offsets they were stored at.
        let (epoch, _) = mgr.get_position("topic1").unwrap();
        match mgr.recover("topic1", epoch, 0) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(&publications[0][..], b"o=1");
                assert_eq!(&publications[1][..], b"o=2");
            }
            _ => panic!("expected Recovered"),
        }
    }

    #[test]
    fn test_recover_cap() {
        let config = RecoveryConfig {
            max_recovery_messages: 3,
            buffer_size_bits: 5, // 32 slots
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);

        for i in 0..20 {
            mgr.push("topic1", make_data(&format!("msg{i}")));
        }

        let (epoch, _) = mgr.get_position("topic1").unwrap();

        // 14 messages after offset 5, but cap is 3 -- should return NotRecovered
        // (matches Centrifugo: truncated recovery = incomplete = must re-fetch)
        match mgr.recover("topic1", epoch, 5) {
            RecoveryResult::NotRecovered {
                epoch: ep,
                offset: off,
            } => {
                assert_eq!(ep, epoch);
                assert_eq!(off, 20); // head_offset
            }
            _ => panic!("expected NotRecovered when cap truncates"),
        }

        // But if the gap is within the cap (e.g., after_offset=17, 2 messages to recover)
        match mgr.recover("topic1", epoch, 17) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(publications.len(), 2); // offsets 18, 19
                assert_eq!(&publications[0][..], b"msg18");
                assert_eq!(&publications[1][..], b"msg19");
            }
            _ => panic!("expected Recovered when within cap"),
        }
    }

    // --- ForeignRecoveryBuffer tests ---

    #[test]
    fn test_foreign_push_recover() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("topic1");

        let foreign_epoch: u32 = 42;

        // Simulate receiving 5 messages from a cluster peer.
        for i in 0u64..5 {
            mgr.push_foreign("topic1", make_data(&format!("fmsg{i}")), foreign_epoch, i);
        }

        // Recover using the foreign epoch -- should succeed.
        match mgr.recover("topic1", foreign_epoch, 2) {
            RecoveryResult::Recovered {
                publications,
                epoch,
                offset,
            } => {
                assert_eq!(epoch, foreign_epoch);
                assert_eq!(offset, 4); // head_offset - 1 = 5 - 1
                assert_eq!(publications.len(), 2);
                assert_eq!(&publications[0][..], b"fmsg3");
                assert_eq!(&publications[1][..], b"fmsg4");
            }
            other => panic!(
                "expected Recovered, got {:?}",
                match other {
                    RecoveryResult::NotRecovered { .. } => "NotRecovered",
                    RecoveryResult::NoHistory => "NoHistory",
                    _ => "unknown",
                }
            ),
        }
    }

    #[test]
    fn a_relayed_message_held_already_is_not_new() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        assert!(mgr.push_foreign("t", make_data("m0"), 7, 0));
        assert!(mgr.push_foreign("t", make_data("m1"), 7, 1));
        // Sent again by a peer catching this node up: held, so not delivered.
        assert!(!mgr.push_foreign("t", make_data("m1"), 7, 1));
        assert!(!mgr.push_foreign("t", make_data("m0"), 7, 0));
        // Past a hole, then the hole itself: both new.
        assert!(mgr.push_foreign("t", make_data("m3"), 7, 3));
        assert!(mgr.push_foreign("t", make_data("m2"), 7, 2));
        assert!(!mgr.push_foreign("t", make_data("m2"), 7, 2));
    }

    #[test]
    fn a_node_asks_from_its_first_hole_in_each_relayed_stream() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        for offset in [0, 1, 2, 4, 5] {
            mgr.push_foreign("a", make_data("x"), 7, offset);
        }
        for offset in 10..13 {
            mgr.push_foreign("b", make_data("y"), 9, offset);
        }
        let mut points = mgr.foreign_positions();
        points.sort();
        assert_eq!(
            points,
            vec![("a".to_owned(), 7, 2), ("b".to_owned(), 9, 12)]
        );
        // The hole filled: from the newest on.
        mgr.push_foreign("a", make_data("x"), 7, 3);
        assert!(mgr.foreign_positions().contains(&("a".to_owned(), 7, 5)));
    }

    #[test]
    fn a_peer_gets_what_this_node_published_after_its_position() {
        let mgr = RecoveryManager::new(RecoveryConfig {
            buffer_size_bits: 2, // 4 kept
            ..RecoveryConfig::default()
        });
        for i in 0..6 {
            mgr.push("t", make_data(&format!("m{i}")));
        }
        let (epoch, head) = mgr.get_position("t").unwrap();
        assert_eq!(head, 6);
        let sent = |after| -> Vec<(u64, String)> {
            mgr.published_after("t", epoch, after)
                .into_iter()
                .map(|(offset, data)| (offset, String::from_utf8(data.to_vec()).unwrap()))
                .collect()
        };
        assert_eq!(sent(3), vec![(4, "m4".to_owned()), (5, "m5".to_owned())]);
        // What is gone is skipped: from the oldest kept (offset 2).
        assert_eq!(sent(0).first(), Some(&(2, "m2".to_owned())));
        assert_eq!(sent(0).len(), 4);
        assert!(sent(5).is_empty());
        assert!(sent(u64::MAX).is_empty());
        // Another node's stream, or another epoch: nothing.
        assert!(
            mgr.published_after("t", epoch.wrapping_add(1), 0)
                .is_empty()
        );
        assert!(mgr.published_after("other", epoch, 0).is_empty());
    }

    #[test]
    fn test_foreign_epoch_mismatch() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);

        let foreign_epoch: u32 = 42;
        mgr.push_foreign("topic1", make_data("data"), foreign_epoch, 0);

        // Try to recover with wrong epoch -- no matching buffer.
        let wrong_epoch = 99;
        match mgr.recover("topic1", wrong_epoch, 0) {
            RecoveryResult::NoHistory => {}
            _ => panic!("expected NoHistory for unmatched foreign epoch"),
        }
    }

    #[test]
    fn test_foreign_buffer_eviction() {
        let config = RecoveryConfig {
            global_memory_budget: 20,
            history_ttl_secs: 3600,
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);

        // Push local data (7 bytes).
        mgr.push("t_local", make_data("aaaaaaa"));
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Push foreign data (7 bytes).
        mgr.push_foreign("t_foreign", make_data("bbbbbbb"), 42, 0);
        std::thread::sleep(std::time::Duration::from_millis(2));

        // Push more foreign data (7 bytes) -- total now 21, over budget of 20.
        mgr.push_foreign("t_foreign2", make_data("ccccccc"), 43, 0);

        assert!(mgr.total_bytes() > 20);

        mgr.cleanup();

        // After cleanup, should be at or under budget.
        assert!(mgr.total_bytes() <= 20);
    }

    #[test]
    fn fresh_foreign_epochs_stop_at_the_budget() {
        let config = RecoveryConfig {
            global_memory_budget: 64 * 1024,
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("t");
        // A peer naming a new epoch with every message.
        for epoch in 0..10_000u32 {
            mgr.push_foreign("t", make_data("x"), epoch, 0);
        }
        let one_buffer = ForeignRecoveryBuffer::new(mgr.config.buffer_size_bits, 0, 8).footprint();
        assert!(
            mgr.total_bytes() <= 64 * 1024 + one_buffer,
            "{}",
            mgr.total_bytes()
        );
        assert!(mgr.foreign_buffers.len() < 10_000);
        // An existing buffer still takes messages.
        mgr.push_foreign("t", make_data("y"), 0, 1);
        assert!(mgr.streams("t").contains(&(0, 1)));
        // What came past the budget is not kept: a client from before it is
        // not told it missed nothing.
        assert!(not_recovered(&mgr.recover("t", 0, 0)));
    }

    #[test]
    fn test_foreign_ttl_cleanup() {
        let config = RecoveryConfig {
            history_ttl_secs: 0, // immediate expiry
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);

        mgr.push_foreign("topic1", make_data("data"), 42, 0);

        std::thread::sleep(std::time::Duration::from_millis(5));

        mgr.cleanup();
        assert_eq!(mgr.total_bytes(), 0);
        assert_eq!(mgr.foreign_buffers.len(), 0);
    }

    #[test]
    fn test_foreign_and_local_coexist() {
        let config = RecoveryConfig::default();
        let mgr = RecoveryManager::new(config);
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("topic1");

        // Local messages on topic1.
        mgr.push("topic1", make_data("local0"));
        mgr.push("topic1", make_data("local1"));
        let (local_epoch, _) = mgr.get_position("topic1").unwrap();

        // Foreign messages on same topic, different epoch.
        let foreign_epoch: u32 = 77;
        mgr.push_foreign("topic1", make_data("foreign0"), foreign_epoch, 0);
        mgr.push_foreign("topic1", make_data("foreign1"), foreign_epoch, 1);

        // The client last had local0: everything after it, from both
        // streams, in the order it arrived here.
        match mgr.recover("topic1", local_epoch, 0) {
            RecoveryResult::Recovered { publications, .. } => {
                let got: Vec<&[u8]> = publications.iter().map(|p| &p[..]).collect();
                assert_eq!(got, [&b"local1"[..], b"foreign0", b"foreign1"]);
            }
            _ => panic!("expected Recovered from both streams"),
        }

        // The client last had foreign0: only foreign1 came after it.
        match mgr.recover("topic1", foreign_epoch, 0) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(publications.len(), 1);
                assert_eq!(&publications[0][..], b"foreign1");
            }
            _ => panic!("expected Recovered from foreign buffer"),
        }
    }

    #[test]
    fn two_nodes_interleaved_recover_in_arrival_order() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("room");
        let other: u32 = 0xa;
        mgr.push_foreign("room", make_data("a1"), other, 0);
        mgr.push("room", make_data("b1"));
        mgr.push_foreign("room", make_data("a2"), other, 1);
        mgr.push("room", make_data("b2"));
        let (epoch, _) = mgr.get_position("room").unwrap();
        mgr.push_foreign("room", make_data("a3"), other, 2);
        mgr.push("room", make_data("b3"));
        mgr.push_foreign("room", make_data("a4"), other, 3);
        mgr.push("room", make_data("b4"));
        match mgr.recover("room", epoch, 1) {
            RecoveryResult::Recovered { publications, .. } => {
                let got: Vec<&[u8]> = publications.iter().map(|p| &p[..]).collect();
                assert_eq!(got, [&b"a3"[..], b"b3", b"a4", b"b4"]);
            }
            _ => panic!("expected Recovered from both streams"),
        }
    }

    #[test]
    fn a_relayed_stream_with_a_hole_after_the_anchor_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("b0"), 7, 0);
        mgr.push("room", make_data("a0"));
        let (epoch, _) = mgr.get_position("room").unwrap();
        mgr.push_foreign("room", make_data("b1"), 7, 1);
        // b2 was shed on the way here.
        mgr.push_foreign("room", make_data("b3"), 7, 3);
        assert!(matches!(
            mgr.recover("room", epoch, 0),
            RecoveryResult::NotRecovered { .. }
        ));
    }

    #[test]
    fn a_stream_that_lost_what_came_after_the_anchor_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig {
            buffer_size_bits: 1, // 2 slots
            ..RecoveryConfig::default()
        });
        mgr.push_foreign("room", make_data("b0"), 7, 0);
        mgr.push("room", make_data("a0"));
        let (epoch, _) = mgr.get_position("room").unwrap();
        // Three more from the other node: one of them is evicted already.
        for (i, text) in ["b1", "b2", "b3"].iter().enumerate() {
            mgr.push_foreign("room", make_data(text), 7, i as u64 + 1);
        }
        assert!(matches!(
            mgr.recover("room", epoch, 0),
            RecoveryResult::NotRecovered { .. }
        ));
    }

    /// The texts of a recovery, or None when it did not recover.
    fn replayed(result: RecoveryResult) -> Option<Vec<String>> {
        match result {
            RecoveryResult::Recovered { publications, .. } => Some(
                publications
                    .iter()
                    .map(|p| String::from_utf8(p.to_vec()).unwrap())
                    .collect(),
            ),
            _ => None,
        }
    }

    #[test]
    fn a_client_from_a_node_that_saw_the_streams_in_another_order_loses_nothing() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        let (a, b) = (0xa, 0xb);
        mgr.push_foreign("room", make_data("a0"), a, 0);
        mgr.push_foreign("room", make_data("b0"), b, 0);
        // This node had b1 before a1; the client's node had a1 first, and
        // the client got a1 only.
        mgr.push_foreign("room", make_data("b1"), b, 1);
        mgr.push_foreign("room", make_data("a1"), a, 1);
        assert_eq!(
            replayed(mgr.recover_streams("room", (a, 1), &[(a, 1), (b, 0)])),
            Some(vec!["b1".to_owned()])
        );
    }

    #[test]
    fn every_named_stream_comes_from_its_own_offset() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push("room", make_data("l0"));
        mgr.push_foreign("room", make_data("f0"), 7, 0);
        mgr.push("room", make_data("l1"));
        mgr.push_foreign("room", make_data("f1"), 7, 1);
        mgr.push("room", make_data("l2"));
        let (local, _) = mgr.get_position("room").unwrap();
        // In this node's arrival order.
        assert_eq!(
            replayed(mgr.recover_streams("room", (local, 0), &[(local, 0), (7, 0)])),
            Some(vec!["l1".to_owned(), "f1".to_owned(), "l2".to_owned()])
        );
        // Up to date in both: nothing.
        assert_eq!(
            replayed(mgr.recover_streams("room", (local, 2), &[(local, 2), (7, 1)])),
            Some(vec![])
        );
    }

    #[test]
    fn a_named_stream_this_node_does_not_hold_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0), (0xc, 4)])),
            None
        );
    }

    #[test]
    fn a_named_stream_with_a_hole_after_the_position_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        // a1 was shed on the way here.
        mgr.push_foreign("room", make_data("a2"), 0xa, 2);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            None
        );
    }

    #[test]
    fn an_unnamed_stream_that_came_before_the_last_message_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        // c0 came here before a0: whether the client should have had it
        // nothing here says.
        mgr.push_foreign("room", make_data("c0"), 0xc, 0);
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            None
        );
    }

    #[test]
    fn an_unnamed_stream_that_came_after_the_last_message_is_replayed() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        mgr.push_foreign("room", make_data("c0"), 0xc, 0);
        mgr.push_foreign("room", make_data("c1"), 0xc, 1);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            Some(vec!["c0".to_owned(), "c1".to_owned()])
        );
    }

    #[test]
    fn an_unnamed_stream_held_from_its_middle_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        // What came of c before c5 this node never had.
        mgr.push_foreign("room", make_data("c5"), 0xc, 5);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            None
        );
    }

    #[test]
    fn a_stream_removed_after_the_last_message_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        mgr.push_foreign("room", make_data("c0"), 0xc, 0);
        std::thread::sleep(Duration::from_millis(2));
        mgr.push_foreign("room", make_data("a1"), 0xa, 1);
        // Over the memory budget the oldest buffer goes: c, with what the
        // client missed.
        let budget = mgr.total_bytes() - 1;
        let mgr = RecoveryManager {
            config: RecoveryConfig {
                global_memory_budget: budget,
                ..RecoveryConfig::default()
            },
            ..mgr
        };
        mgr.cleanup();
        assert!(!mgr.streams("room").iter().any(|(epoch, _)| *epoch == 0xc));
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            None
        );
    }

    /// The same manager with its memory budget at `budget`.
    fn with_budget(mgr: RecoveryManager, budget: usize) -> RecoveryManager {
        RecoveryManager {
            config: RecoveryConfig {
                global_memory_budget: budget,
                ..RecoveryConfig::default()
            },
            ..mgr
        }
    }

    fn not_recovered(result: &RecoveryResult) -> bool {
        matches!(result, RecoveryResult::NotRecovered { .. })
    }

    #[test]
    fn a_client_with_one_position_is_not_told_a_removed_stream_was_replayed() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        // The client's last message, this node's own; then another node's,
        // then this node's again.
        mgr.push("room", make_data("l0"));
        let (local, _) = mgr.get_position("room").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        mgr.push_foreign("room", make_data("f0"), 0xf, 0);
        std::thread::sleep(Duration::from_millis(2));
        mgr.push("room", make_data("l1"));
        // Over the budget the oldest buffer goes: the other node's.
        let budget = mgr.total_bytes() - 1;
        let mgr = with_budget(mgr, budget);
        mgr.cleanup();
        assert!(mgr.streams("room").iter().all(|(epoch, _)| *epoch == local));
        // A client before 3.1 names its last message only: f0 is gone.
        assert!(not_recovered(&mgr.recover("room", local, 0)));
    }

    #[test]
    fn a_relayed_message_with_no_room_to_keep_it_is_never_recovered_as_nothing() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push("room", make_data("l0"));
        let (local, _) = mgr.get_position("room").unwrap();
        // Over the budget: another node's new stream gets no buffer, and its
        // message goes out live only.
        let budget = mgr.total_bytes();
        let mgr = with_budget(mgr, budget);
        assert!(mgr.push_foreign("room", make_data("f0"), 0xf, 0));
        assert!(mgr.streams("room").iter().all(|(epoch, _)| *epoch == local));
        // A client whose last message came before it may have missed it.
        assert!(not_recovered(&mgr.recover("room", local, 0)));
        assert_eq!(
            replayed(mgr.recover_streams("room", (local, 0), &[(local, 0)])),
            None
        );
    }

    #[test]
    fn a_message_published_with_no_room_to_keep_it_is_never_recovered_as_nothing() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("f0"), 0xf, 0);
        let budget = mgr.total_bytes();
        let mgr = with_budget(mgr, budget);
        // This node's new stream gets no buffer: the message goes unstamped.
        assert!(mgr.push_stamped("room", |_, _| make_data("l0")).is_none());
        assert!(not_recovered(&mgr.recover("room", 0xf, 0)));
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xf, 0), &[(0xf, 0)])),
            None
        );
    }

    #[test]
    fn a_peer_gets_what_this_node_published_since_its_interest_began() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.push("news", make_data("old"));
        let (epoch, _) = mgr.get_position("news").unwrap();
        // A minute ago the arrival order stood after "old".
        let minute_ago = Instant::now().checked_sub(Duration::from_secs(60)).unwrap();
        mgr.marks
            .lock()
            .push_back((minute_ago, mgr.arrival.load(Ordering::Relaxed)));
        mgr.push("news", make_data("new"));
        // An interest of 30 s: what came after the mark before it.
        let (sent_epoch, sent) = mgr
            .published_lately("news", Duration::from_secs(30), |_| false)
            .unwrap();
        assert_eq!(sent_epoch, epoch);
        assert_eq!(sent, vec![(1, make_data("new"))]);
        // One older than every mark: all that is kept.
        let (_, all) = mgr
            .published_lately("news", Duration::from_secs(120), |_| false)
            .unwrap();
        assert_eq!(all.len(), 2);
        // A stream the peer named, or none here: nothing this way.
        assert!(
            mgr.published_lately("news", Duration::from_secs(30), |e| e == epoch)
                .is_none()
        );
        assert!(
            mgr.published_lately("other", Duration::from_secs(30), |_| false)
                .is_none()
        );
    }

    #[test]
    fn the_marks_go_back_as_far_as_this_node_holds_its_messages() {
        // Two slots a topic.
        let mgr = RecoveryManager::new(RecoveryConfig {
            buffer_size_bits: 1,
            ..RecoveryConfig::default()
        });
        for text in ["a", "b", "c"] {
            mgr.mark();
            mgr.push("news", make_data(text));
        }
        mgr.mark();
        mgr.mark();
        let marks = |mgr: &RecoveryManager| -> Vec<u64> {
            mgr.marks.lock().iter().map(|(_, next)| *next).collect()
        };
        assert_eq!(marks(&mgr), [1, 2, 3, 4], "none while nothing came");
        // "a" is gone: of the marks before "b", the last stays.
        mgr.cleanup();
        assert_eq!(marks(&mgr), [2, 3, 4]);
    }

    #[test]
    fn more_than_the_recovery_limit_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig {
            max_recovery_messages: 2,
            ..RecoveryConfig::default()
        });
        mgr.interest_began("room");
        for i in 0..2 {
            mgr.push_foreign("room", make_data("a"), 0xa, i);
            mgr.push_foreign("room", make_data("b"), 0xb, i);
        }
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0), (0xb, 0)])),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
        mgr.push_foreign("room", make_data("b"), 0xb, 2);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0), (0xb, 0)])),
            None
        );
    }

    #[test]
    fn a_named_stream_relayed_before_the_topic_came_back_here_is_not_recovered() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        mgr.push_foreign("room", make_data("a1"), 0xa, 1);
        assert!(replayed(mgr.recover_streams("room", (0xa, 1), &[(0xa, 1)])).is_some());
        // The topic left this node and came back: what was published in
        // between was not relayed here.
        mgr.interest_ended("room");
        mgr.interest_began("room");
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 1), &[(0xa, 1)])),
            None
        );
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0)])),
            None
        );
    }

    #[test]
    fn a_stream_relayed_out_of_order_is_replayed_in_its_own_order() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        mgr.push_foreign("room", make_data("a0"), 0xa, 0);
        mgr.push_foreign("room", make_data("b0"), 0xb, 0);
        mgr.push_foreign("room", make_data("b2"), 0xb, 2);
        mgr.push_foreign("room", make_data("a1"), 0xa, 1);
        mgr.push_foreign("room", make_data("b1"), 0xb, 1);
        assert_eq!(
            replayed(mgr.recover_streams("room", (0xa, 0), &[(0xa, 0), (0xb, 0)])),
            Some(vec!["a1".to_owned(), "b1".to_owned(), "b2".to_owned()])
        );
    }

    #[test]
    fn the_streams_of_a_topic_are_what_this_node_holds_at_their_last_offsets() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        mgr.interest_began("room");
        assert!(mgr.streams("room").is_empty());
        mgr.push("room", make_data("l0"));
        mgr.push("room", make_data("l1"));
        mgr.push_foreign("room", make_data("f3"), 7, 3);
        let (local, _) = mgr.get_position("room").unwrap();
        let mut streams = mgr.streams("room");
        streams.sort_unstable();
        let mut expected = vec![(local, 1), (7, 3)];
        expected.sort_unstable();
        assert_eq!(streams, expected);
    }

    #[test]
    fn test_foreign_buffer_overflow() {
        let config = RecoveryConfig {
            buffer_size_bits: 2, // 4 slots
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(config);
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("topic1");

        let epoch: u32 = 50;
        // Push 10 messages -- only last 4 should survive (offsets 6-9).
        for i in 0u64..10 {
            mgr.push_foreign("topic1", make_data(&format!("fm{i}")), epoch, i);
        }

        // Recovery from offset 4 (start=5, before tail=6) should fail.
        match mgr.recover("topic1", epoch, 4) {
            RecoveryResult::NotRecovered { .. } => {}
            _ => panic!("expected NotRecovered for gap too large"),
        }

        // Recovery from offset 5 (start=6 = tail) should succeed with 4 messages.
        match mgr.recover("topic1", epoch, 5) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(publications.len(), 4);
                assert_eq!(&publications[0][..], b"fm6");
            }
            _ => panic!("expected Recovered at boundary"),
        }

        // Recovery from offset 7 should get messages 8, 9.
        match mgr.recover("topic1", epoch, 7) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(publications.len(), 2);
                assert_eq!(&publications[0][..], b"fm8");
                assert_eq!(&publications[1][..], b"fm9");
            }
            _ => panic!("expected Recovered"),
        }
    }

    #[test]
    fn foreign_offset_jump_is_bounded() {
        let mgr = RecoveryManager::new(RecoveryConfig::default());
        // This node has the topic: other nodes relay it here.
        mgr.interest_began("t");
        mgr.push_foreign("t", make_data("a"), 7, 0);
        let started = Instant::now();
        mgr.push_foreign("t", make_data("b"), 7, 1u64 << 62);
        assert!(started.elapsed() < Duration::from_secs(1));
        // Only the newest message is left; anything older is a gap.
        match mgr.recover("t", 7, (1u64 << 62) - 1) {
            RecoveryResult::Recovered { publications, .. } => {
                assert_eq!(publications.len(), 1);
                assert_eq!(&publications[0][..], b"b");
            }
            _ => panic!("expected Recovered"),
        }
        assert!(matches!(
            mgr.recover("t", 7, 0),
            RecoveryResult::NotRecovered { .. }
        ));
    }

    #[test]
    fn local_buffer_memory_grows_with_use() {
        let cfg = RecoveryConfig {
            buffer_size_bits: 16, // allows 65536 messages per topic
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(cfg);
        mgr.push("room:1", make_data("hello"));
        // One small message must not cost the 65536-slot maximum (~2 MiB).
        assert!(
            mgr.total_bytes() < 4096,
            "charged {} bytes",
            mgr.total_bytes()
        );
    }

    #[test]
    fn budget_accounting_returns_to_zero() {
        let cfg = RecoveryConfig {
            history_ttl_secs: 0,
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(cfg);
        for i in 0..200 {
            mgr.push(&format!("topic-{}", i % 7), make_data("payload"));
            mgr.push_foreign(&format!("f-{}", i % 5), make_data("payload"), 9, i);
        }
        assert!(mgr.total_bytes() > 0);
        std::thread::sleep(Duration::from_millis(5));
        mgr.cleanup();
        assert_eq!(mgr.topic_count(), 0);
        assert_eq!(mgr.foreign_buffer_count(), 0);
        assert_eq!(mgr.total_bytes(), 0);
    }

    #[test]
    fn ttl_longer_than_uptime_does_not_panic() {
        let cfg = RecoveryConfig {
            history_ttl_secs: u64::MAX / 4,
            ..RecoveryConfig::default()
        };
        let mgr = RecoveryManager::new(cfg);
        mgr.push("t", make_data("x"));
        mgr.cleanup();
        assert_eq!(mgr.topic_count(), 1);
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(1000))]

            /// Whatever the history and the client's position, recovery returns
            /// exactly the missing suffix in order, or says it cannot.
            #[test]
            fn recovery_returns_exactly_the_missing_suffix(
                bits in 1u32..6,
                pushed in 0usize..200,
                after in 0u64..210,
                max_messages in 1usize..300,
            ) {
                let cfg = RecoveryConfig {
                    buffer_size_bits: bits,
                    max_recovery_messages: max_messages,
                    ..RecoveryConfig::default()
                };
                let mgr = RecoveryManager::new(cfg);
                for i in 0..pushed {
                    mgr.push("t", make_data(&format!("m{i}")));
                }
                let Some((epoch, head)) = mgr.get_position("t") else {
                    prop_assert_eq!(pushed, 0);
                    return Ok(());
                };
                prop_assert_eq!(head, pushed as u64);
                let capacity = 1u64 << bits;
                let tail = head.saturating_sub(capacity);
                match mgr.recover("t", epoch, after) {
                    RecoveryResult::Recovered { publications, offset, .. } => {
                        let start = after + 1;
                        prop_assert!(start >= tail);
                        prop_assert_eq!(offset, head - 1);
                        let expected: Vec<String> =
                            (start..head).map(|o| format!("m{o}")).collect();
                        let got: Vec<String> = publications
                            .iter()
                            .map(|b| String::from_utf8(b.to_vec()).unwrap())
                            .collect();
                        prop_assert_eq!(got, expected);
                    }
                    RecoveryResult::NotRecovered { .. } => {
                        let missing = head.saturating_sub(after + 1);
                        prop_assert!(after + 1 < tail || missing > max_messages as u64);
                    }
                    RecoveryResult::NoHistory => prop_assert!(false, "history exists"),
                }
            }
        }
    }
}
