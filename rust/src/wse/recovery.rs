use bytes::Bytes;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    /// Power-of-two exponent for ring buffer capacity (default 7 = 128 msgs).
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

// ---------------------------------------------------------------------------
// RecoveryEntry -- single message stored in the ring buffer
// ---------------------------------------------------------------------------

struct RecoveryEntry {
    /// Pre-framed WebSocket bytes (Arc-shared with broadcast path -- zero-copy).
    data: Bytes,
    /// Monotonic offset within this topic buffer.
    offset: u64,
}

// ---------------------------------------------------------------------------
// TopicRecoveryBuffer -- per-topic ring buffer
// ---------------------------------------------------------------------------

struct TopicRecoveryBuffer {
    entries: Box<[Option<RecoveryEntry>]>,
    /// Bitmask for power-of-two indexing: capacity - 1.
    mask: u64,
    /// Next offset to be written (one past the newest entry).
    head_offset: u64,
    /// Oldest valid offset still in the buffer.
    tail_offset: u64,
    /// Random epoch generated on creation -- changes on server restart.
    epoch: u32,
    /// Total bytes of all stored entries' data.
    total_bytes: usize,
    /// Timestamp of the most recent push.
    last_write: Instant,
}

impl TopicRecoveryBuffer {
    /// Create a new ring buffer with `1 << capacity_bits` slots.
    fn new(capacity_bits: u32) -> Self {
        let capacity = 1u64 << capacity_bits;
        let mut entries = Vec::with_capacity(capacity as usize);
        entries.resize_with(capacity as usize, || None);

        // Generate a unique epoch: time + PID + counter (no external rand dependency).
        // Counter guarantees uniqueness even for rapid buffer re-creation within same process.
        let epoch = (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u32)
            .wrapping_add(std::process::id())
            .wrapping_add(EPOCH_COUNTER.fetch_add(1, Ordering::Relaxed));

        Self {
            entries: entries.into_boxed_slice(),
            mask: capacity - 1,
            head_offset: 0,
            tail_offset: 0,
            epoch,
            total_bytes: 0,
            last_write: Instant::now(),
        }
    }

    /// Push a message into the ring buffer. Returns the assigned offset.
    ///
    /// If the buffer is full (head - tail >= capacity), the oldest entry is
    /// evicted by advancing the tail.
    fn push(&mut self, data: Bytes) -> u64 {
        let capacity = self.mask + 1;

        // Evict oldest if buffer is full.
        if self.head_offset - self.tail_offset >= capacity {
            let evict_idx = (self.tail_offset & self.mask) as usize;
            if let Some(old) = self.entries[evict_idx].take() {
                self.total_bytes -= old.data.len();
            }
            self.tail_offset += 1;
        }

        let offset = self.head_offset;
        let idx = (offset & self.mask) as usize;
        let data_len = data.len();

        self.entries[idx] = Some(RecoveryEntry { data, offset });

        self.head_offset += 1;
        self.total_bytes += data_len;
        self.last_write = Instant::now();

        offset
    }

    /// Recover messages with offset strictly greater than `after_offset`.
    ///
    /// Returns `None` if `after_offset` is older than the tail (gap too large).
    /// Results are capped at `max_messages`.
    fn recover_since(&self, after_offset: u64, max_messages: usize) -> Option<Vec<Bytes>> {
        // If the first message we need (after_offset + 1) is before the tail,
        // those messages have been evicted -- gap too large.
        if self.head_offset > 0 && after_offset.saturating_add(1) < self.tail_offset {
            return None;
        }

        let start = after_offset.saturating_add(1);
        if start >= self.head_offset {
            // Nothing to recover -- client is up to date.
            return Some(Vec::new());
        }

        let count = ((self.head_offset - start) as usize).min(max_messages);
        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let offset = start + i as u64;
            let idx = (offset & self.mask) as usize;
            if let Some(entry) = &self.entries[idx]
                && entry.offset == offset
            {
                result.push(entry.data.clone());
            } else {
                // Slot mismatch should be unreachable, but if it occurs in release,
                // signal failure so the client re-subscribes rather than missing messages.
                debug_assert!(false, "slot {} mismatch: expected offset {}", idx, offset);
                return None;
            }
        }

        Some(result)
    }
}

// ---------------------------------------------------------------------------
// RecoveryManager -- thread-safe manager for all topic buffers
// ---------------------------------------------------------------------------

pub(crate) struct RecoveryManager {
    buffers: DashMap<String, TopicRecoveryBuffer>,
    config: RecoveryConfig,
    total_bytes: AtomicUsize,
}

impl RecoveryManager {
    pub(crate) fn new(config: RecoveryConfig) -> Self {
        Self {
            buffers: DashMap::new(),
            config,
            total_bytes: AtomicUsize::new(0),
        }
    }

    /// Push a message into the topic's recovery buffer.
    ///
    /// Creates the buffer lazily via the DashMap entry API if it doesn't exist.
    pub(crate) fn push(&self, topic: &str, data: Bytes) {
        let mut entry = self
            .buffers
            .entry(topic.to_owned())
            .or_insert_with(|| TopicRecoveryBuffer::new(self.config.buffer_size_bits));

        let buf = entry.value_mut();
        let old_bytes = buf.total_bytes;
        buf.push(data);
        let new_bytes = buf.total_bytes;

        // Update global byte counter with the delta (accounts for evictions).
        // DashMap entry API holds the shard write lock for this entire block,
        // so cleanup() cannot remove this buffer between the read and update.
        if new_bytes > old_bytes {
            self.total_bytes
                .fetch_add(new_bytes - old_bytes, Ordering::Relaxed);
        } else if old_bytes > new_bytes {
            self.total_bytes
                .fetch_sub(old_bytes - new_bytes, Ordering::Relaxed);
        }
    }

    /// Attempt to recover messages for a topic after a given offset.
    ///
    /// Validates that the epoch matches the current buffer epoch. If the epoch
    /// doesn't match (server restarted), returns `NotRecovered` with the current
    /// position so the client can re-subscribe.
    pub(crate) fn recover(&self, topic: &str, epoch: u32, after_offset: u64) -> RecoveryResult {
        let buf = match self.buffers.get(topic) {
            Some(b) => b,
            None => return RecoveryResult::NoHistory,
        };

        // Epoch mismatch means the server restarted -- client must re-subscribe.
        if buf.epoch != epoch {
            return RecoveryResult::NotRecovered {
                epoch: buf.epoch,
                offset: buf.head_offset,
            };
        }

        match buf.recover_since(after_offset, self.config.max_recovery_messages) {
            Some(publications) => {
                // Check if recovery was truncated by the cap -- if so, the client
                // is missing messages and should re-fetch from the application backend
                // (matches Centrifugo's behavior for recovery_max_publication_limit).
                let available = buf
                    .head_offset
                    .saturating_sub(after_offset.saturating_add(1))
                    as usize;
                if available > self.config.max_recovery_messages {
                    return RecoveryResult::NotRecovered {
                        epoch: buf.epoch,
                        offset: buf.head_offset,
                    };
                }
                let offset = buf.head_offset.saturating_sub(1);
                RecoveryResult::Recovered {
                    publications,
                    epoch: buf.epoch,
                    offset,
                }
            }
            None => RecoveryResult::NotRecovered {
                epoch: buf.epoch,
                offset: buf.head_offset,
            },
        }
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
    pub(crate) fn cleanup(&self) {
        let ttl_cutoff =
            Instant::now() - std::time::Duration::from_secs(self.config.history_ttl_secs);

        // First pass: remove TTL-expired buffers.
        let mut to_remove = Vec::new();
        for entry in self.buffers.iter() {
            if entry.value().last_write < ttl_cutoff {
                to_remove.push(entry.key().clone());
            }
        }
        for key in &to_remove {
            if let Some((_, removed)) = self.buffers.remove(key) {
                self.total_bytes
                    .fetch_sub(removed.total_bytes, Ordering::Relaxed);
            }
        }

        // Second pass: if still over budget, evict LRU until under budget.
        if self.total_bytes.load(Ordering::Relaxed) > self.config.global_memory_budget {
            let mut candidates: Vec<(String, Instant, usize)> = self
                .buffers
                .iter()
                .map(|e| (e.key().clone(), e.value().last_write, e.value().total_bytes))
                .collect();

            // Sort by last_write ascending (oldest first).
            candidates.sort_by_key(|(_, lw, _)| *lw);

            for (topic, _, _) in candidates {
                if self.total_bytes.load(Ordering::Relaxed) <= self.config.global_memory_budget {
                    break;
                }
                if let Some((_, removed)) = self.buffers.remove(&topic) {
                    self.total_bytes
                        .fetch_sub(removed.total_bytes, Ordering::Relaxed);
                }
            }
        }
    }

    /// Return the number of topic buffers currently held.
    pub(crate) fn topic_count(&self) -> usize {
        self.buffers.len()
    }

    /// Return the total bytes across all topic buffers.
    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_data(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    // --- TopicRecoveryBuffer tests ---

    #[test]
    fn test_push_recover_roundtrip() {
        let mut buf = TopicRecoveryBuffer::new(4); // 16 slots
        for i in 0..5 {
            buf.push(make_data(&format!("msg{i}")));
        }

        // Recover from offset 2 (should get messages at offsets 3 and 4).
        let recovered = buf.recover_since(2, 500).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(&recovered[0][..], b"msg3");
        assert_eq!(&recovered[1][..], b"msg4");
    }

    #[test]
    fn test_buffer_overflow() {
        let mut buf = TopicRecoveryBuffer::new(2); // 4 slots
        for i in 0..10 {
            buf.push(make_data(&format!("msg{i}")));
        }

        // Tail should have advanced. head=10, tail=6.
        assert_eq!(buf.head_offset, 10);
        assert_eq!(buf.tail_offset, 6);

        // Trying to recover from offset 3 (well before tail=6) should return None.
        assert!(buf.recover_since(3, 500).is_none());

        // Boundary: after_offset=5, start=6 which IS the tail -- should succeed.
        let boundary = buf.recover_since(5, 500).unwrap();
        assert_eq!(boundary.len(), 4); // offsets 6, 7, 8, 9
        assert_eq!(&boundary[0][..], b"msg6");

        // Recover from offset 7 should return msgs 8 and 9.
        let recovered = buf.recover_since(7, 500).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(&recovered[0][..], b"msg8");
        assert_eq!(&recovered[1][..], b"msg9");
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
}
