use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::pylock::lock;

// Both queues hold Python objects. A message the queue lets go of (evicted to
// make room, or removed by `clear`) is dropped only after the queue's lock is
// released: dropping the last reference to it runs Python code (`__del__`,
// weakref callbacks), and that code could call back into the same queue and
// wait for the lock forever.

// =============================================================================
// RustPriorityQueue - Original BinaryHeap version (backward compat)
// =============================================================================

/// Wrapper around a queued item that implements `Ord` for the `BinaryHeap`.
struct QueueItem {
    neg_priority: i32,
    sequence: u64,
    message: Py<PyAny>,
}

impl PartialEq for QueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.neg_priority == other.neg_priority && self.sequence == other.sequence
    }
}

impl Eq for QueueItem {}

impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .neg_priority
            .cmp(&self.neg_priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Priority queue backed by a `BinaryHeap`.
///
/// Higher priority numbers are dequeued first. Within the same priority,
/// items are dequeued in FIFO order.
///
/// Safe to share between threads, also on free-threaded Python: every call
/// runs under the queue's lock.
#[pyclass(frozen)]
pub struct RustPriorityQueue {
    state: Mutex<PriorityQueueState>,
}

/// The mutable part of [`RustPriorityQueue`], behind its lock.
struct PriorityQueueState {
    heap: BinaryHeap<QueueItem>,
    sequence: u64,
    max_size: usize,
}

/// Collections start at most this big and grow on demand, so a large
/// `max_size` / `window_size` does not reserve its full capacity up front.
pub(crate) const INITIAL_CAPACITY_CAP: usize = 1024;

#[pymethods]
impl RustPriorityQueue {
    #[new]
    fn new(max_size: usize) -> PyResult<Self> {
        if max_size == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "max_size must be at least 1",
            ));
        }
        Ok(Self {
            state: Mutex::new(PriorityQueueState {
                heap: BinaryHeap::with_capacity(max_size.min(INITIAL_CAPACITY_CAP)),
                sequence: 0,
                max_size,
            }),
        })
    }

    /// Push a message with the given priority. Returns false if full.
    fn push(&self, py: Python<'_>, priority: i32, message: Py<PyAny>) -> bool {
        let mut state = lock(py, &self.state);
        if state.heap.len() >= state.max_size {
            return false;
        }
        state.sequence += 1;
        let sequence = state.sequence;
        state.heap.push(QueueItem {
            neg_priority: -priority,
            sequence,
            message,
        });
        true
    }

    /// Pop the highest-priority message. Returns None if empty.
    fn pop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let item = lock(py, &self.state).heap.pop();
        match item {
            Some(item) => Ok(item.message.into_bound(py)),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// Drain up to `max_count` messages, highest priority first.
    fn drain<'py>(&self, py: Python<'py>, max_count: usize) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Py<PyAny>> = {
            let mut state = lock(py, &self.state);
            let count = max_count.min(state.heap.len());
            (0..count)
                .filter_map(|_| state.heap.pop().map(|item| item.message))
                .collect()
        };
        PyList::new(py, items)
    }

    fn len(&self, py: Python<'_>) -> usize {
        lock(py, &self.state).heap.len()
    }

    fn is_full(&self, py: Python<'_>) -> bool {
        let state = lock(py, &self.state);
        state.heap.len() >= state.max_size
    }

    fn clear(&self, py: Python<'_>) {
        let removed = std::mem::take(&mut lock(py, &self.state).heap);
        drop(removed); // after the lock, see the note at the top of this file
    }
}

// =============================================================================
// RustPriorityMessageQueue - Full parity with Python PriorityMessageQueue
// =============================================================================

/// 5 priority levels matching the Python implementation.
const PRIORITY_LEVELS: [i32; 5] = [10, 8, 5, 3, 1];
// CRITICAL=10, HIGH=8, NORMAL=5, LOW=3, BACKGROUND=1

/// Index mapping: priority level -> index in the Vec<VecDeque>.
/// We use a fixed-size array for O(1) lookup.
/// Priorities: 1->0, 3->1, 5->2, 8->3, 10->4
fn priority_to_index(priority: i32) -> usize {
    match priority {
        1 => 0,  // BACKGROUND
        3 => 1,  // LOW
        5 => 2,  // NORMAL
        8 => 3,  // HIGH
        10 => 4, // CRITICAL
        _ => 2,  // Default to NORMAL
    }
}

/// A queued message entry storing a timestamp (monotonic) and the Python dict.
struct MessageEntry {
    timestamp: Instant,
    message: Py<PyAny>,
}

/// Priority-based message queue with batching support.
///
/// Full parity with Python `PriorityMessageQueue`:
/// - 5 priority levels: CRITICAL(10), HIGH(8), NORMAL(5), LOW(3), BACKGROUND(1)
/// - Smart dropping: when full, drops lower priority messages first
/// - Batch dequeue ordered by priority (highest first)
/// - Full stats tracking (size, drops, utilization, backpressure)
///
/// Safe to share between threads, also on free-threaded Python: every call
/// runs under the queue's lock.
#[pyclass(frozen)]
pub struct RustPriorityMessageQueue {
    state: Mutex<MessageQueueState>,
}

/// The mutable part of [`RustPriorityMessageQueue`], behind its lock.
struct MessageQueueState {
    /// Per-priority FIFO queues. Index via priority_to_index().
    queues: Vec<VecDeque<MessageEntry>>,
    max_size: usize,
    batch_size: usize,
    /// Total message count across all queues.
    size: usize,
    /// Count of messages per priority level.
    priority_distribution: HashMap<i32, usize>,
    /// Count of dropped messages per priority level.
    dropped_count: HashMap<i32, usize>,
    /// Timestamp of the oldest message currently in the queue (monotonic).
    oldest_message_timestamp: Option<Instant>,
    /// Messages handed out by `dequeue_batch`, for `processing_rate`.
    throughput: Throughput,
}

/// Trailing window over which `processing_rate` is averaged.
const RATE_WINDOW: Duration = Duration::from_secs(10);
/// Dequeues closer together than this share one bucket, so the window holds
/// at most `RATE_WINDOW / RATE_BUCKET` entries however often it is called.
const RATE_BUCKET: Duration = Duration::from_millis(100);

/// Dequeue counts over the last `RATE_WINDOW`, in coarse time buckets.
struct Throughput {
    since: Instant,
    buckets: VecDeque<(Instant, usize)>,
}

impl Throughput {
    fn new(now: Instant) -> Self {
        Self {
            since: now,
            buckets: VecDeque::new(),
        }
    }

    fn record(&mut self, now: Instant, count: usize) {
        while let Some(&(at, _)) = self.buckets.front() {
            if now.duration_since(at) <= RATE_WINDOW {
                break;
            }
            self.buckets.pop_front();
        }
        match self.buckets.back_mut() {
            Some((at, n)) if now.duration_since(*at) < RATE_BUCKET => *n += count,
            _ => self.buckets.push_back((now, count)),
        }
    }

    /// Messages per second over the trailing window, or over the time since
    /// the queue was created or cleared when that is shorter (at least one
    /// second, so a single early batch does not read as a huge rate).
    fn rate(&self, now: Instant) -> f64 {
        let recent: usize = self
            .buckets
            .iter()
            .filter(|(at, _)| now.duration_since(*at) <= RATE_WINDOW)
            .map(|(_, n)| n)
            .sum();
        let span = now
            .duration_since(self.since)
            .clamp(Duration::from_secs(1), RATE_WINDOW);
        recent as f64 / span.as_secs_f64()
    }
}

impl MessageQueueState {
    fn new(max_size: usize, batch_size: usize) -> Self {
        // Create 5 VecDeques, one per priority level (indexed 0..5)
        let queues = vec![
            VecDeque::new(), // index 0: BACKGROUND (1)
            VecDeque::new(), // index 1: LOW (3)
            VecDeque::new(), // index 2: NORMAL (5)
            VecDeque::new(), // index 3: HIGH (8)
            VecDeque::new(), // index 4: CRITICAL (10)
        ];

        let mut priority_distribution = HashMap::new();
        let mut dropped_count = HashMap::new();
        for &p in &PRIORITY_LEVELS {
            priority_distribution.insert(p, 0);
            dropped_count.insert(p, 0);
        }

        Self {
            queues,
            max_size,
            batch_size,
            size: 0,
            priority_distribution,
            dropped_count,
            oldest_message_timestamp: None,
            throughput: Throughput::new(Instant::now()),
        }
    }

    /// Queue `message`. Returns whether it was accepted, and the lower-priority
    /// message evicted to make room for it (the caller drops that one after
    /// releasing the lock).
    ///
    /// A full queue makes room only at the expense of a strictly lower
    /// priority among BACKGROUND, LOW and NORMAL, lowest first and oldest
    /// first within a level. When nothing lower is queued the newcomer is
    /// rejected and counted as dropped under its own priority, so a burst of
    /// low-priority messages can never push out more important ones.
    fn enqueue(&mut self, message: Py<PyAny>, priority: i32) -> (bool, Option<MessageEntry>) {
        // Normalize priority to the closest valid level
        let priority = Self::normalize_priority(priority);
        let mut evicted = None;

        if self.size >= self.max_size {
            for &drop_priority in &[1, 3, 5] {
                if drop_priority >= priority {
                    break;
                }
                let idx = priority_to_index(drop_priority);
                if let Some(entry) = self.queues[idx].pop_front() {
                    evicted = Some(entry);
                    self.size = self.size.saturating_sub(1);
                    let dist = self.priority_distribution.entry(drop_priority).or_insert(0);
                    *dist = dist.saturating_sub(1);
                    *self.dropped_count.entry(drop_priority).or_insert(0) += 1;
                    break;
                }
            }

            if evicted.is_none() {
                *self.dropped_count.entry(priority).or_insert(0) += 1;
                return (false, None);
            }
            // The evicted message may have been the oldest one.
            self.refresh_oldest_timestamp();
        }

        let now = Instant::now();
        let idx = priority_to_index(priority);
        self.queues[idx].push_back(MessageEntry {
            timestamp: now,
            message,
        });
        self.size += 1;
        *self.priority_distribution.entry(priority).or_insert(0) += 1;

        // Update oldest message timestamp
        if self.oldest_message_timestamp.is_none() {
            self.oldest_message_timestamp = Some(now);
        }

        (true, evicted)
    }

    fn dequeue_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut batch: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(self.batch_size);

        // Process from highest to lowest priority: 10, 8, 5, 3, 1
        for &priority in &[10, 8, 5, 3, 1] {
            let idx = priority_to_index(priority);

            while !self.queues[idx].is_empty() && batch.len() < self.batch_size {
                if let Some(entry) = self.queues[idx].pop_front() {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            priority.into_pyobject(py)?.into_any(),
                            entry.message.into_bound(py),
                        ],
                    )?;
                    batch.push(tuple);
                    self.size = self.size.saturating_sub(1);
                    let dist = self.priority_distribution.entry(priority).or_insert(0);
                    *dist = dist.saturating_sub(1);
                }
            }
        }

        if !batch.is_empty() {
            self.refresh_oldest_timestamp();
            self.throughput.record(Instant::now(), batch.len());
        }

        PyList::new(py, batch)
    }

    /// Recompute the timestamp of the oldest queued message (the fronts of the
    /// per-priority FIFOs are their oldest entries).
    fn refresh_oldest_timestamp(&mut self) {
        self.oldest_message_timestamp = self
            .queues
            .iter()
            .filter_map(|q| q.front().map(|entry| entry.timestamp))
            .min();
    }

    /// Empty every queue, reset all statistics, and hand back what was queued
    /// (the caller drops it after releasing the lock).
    fn clear(&mut self) -> Vec<VecDeque<MessageEntry>> {
        let removed = self.queues.iter_mut().map(std::mem::take).collect();
        self.size = 0;
        for val in self.priority_distribution.values_mut() {
            *val = 0;
        }
        for val in self.dropped_count.values_mut() {
            *val = 0;
        }
        self.oldest_message_timestamp = None;
        self.throughput = Throughput::new(Instant::now());
        removed
    }

    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = PyDict::new(py);

        stats.set_item("size", self.size)?;
        stats.set_item("capacity", self.max_size)?;

        let utilization = if self.max_size > 0 {
            (self.size as f64 / self.max_size as f64) * 100.0
        } else {
            0.0
        };
        stats.set_item("utilization_percent", utilization)?;

        // priority_distribution: {priority: count}
        let dist = PyDict::new(py);
        for &p in &PRIORITY_LEVELS {
            let count = self.priority_distribution.get(&p).copied().unwrap_or(0);
            dist.set_item(p, count)?;
        }
        stats.set_item("priority_distribution", dist)?;

        // priority_queue_depths: {"priority_X_depth": count}
        let depths = PyDict::new(py);
        for &p in &PRIORITY_LEVELS {
            let idx = priority_to_index(p);
            let key = format!("priority_{}_depth", p);
            depths.set_item(key, self.queues[idx].len())?;
        }
        stats.set_item("priority_queue_depths", depths)?;

        // dropped_by_priority: {priority: dropped_count}
        let dropped = PyDict::new(py);
        let mut total_dropped: usize = 0;
        for &p in &PRIORITY_LEVELS {
            let count = self.dropped_count.get(&p).copied().unwrap_or(0);
            dropped.set_item(p, count)?;
            total_dropped += count;
        }
        stats.set_item("dropped_by_priority", dropped)?;
        stats.set_item("total_dropped", total_dropped)?;

        // backpressure: size > max_size * 0.8
        let backpressure = self.size as f64 > self.max_size as f64 * 0.8;
        stats.set_item("backpressure", backpressure)?;

        // oldest_message_age: seconds since oldest message was enqueued
        let oldest_age = match self.oldest_message_timestamp {
            Some(ts) => {
                let age = ts.elapsed().as_secs_f64();
                age.into_pyobject(py)?.into_any().unbind()
            }
            None => py.None(),
        };
        stats.set_item("oldest_message_age", oldest_age)?;

        // processing_rate: messages dequeued per second, recent window
        stats.set_item("processing_rate", self.throughput.rate(Instant::now()))?;

        Ok(stats)
    }

    /// Normalize an arbitrary priority value to the closest valid level.
    fn normalize_priority(priority: i32) -> i32 {
        let mut best = 5; // default NORMAL
        let mut best_dist = i32::MAX;
        for &p in &PRIORITY_LEVELS {
            let dist = (p - priority).abs();
            if dist < best_dist {
                best_dist = dist;
                best = p;
            }
        }
        best
    }
}

#[pymethods]
impl RustPriorityMessageQueue {
    #[new]
    #[pyo3(signature = (max_size=1000, batch_size=10))]
    fn new(max_size: usize, batch_size: usize) -> PyResult<Self> {
        if max_size == 0 || batch_size == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "max_size and batch_size must be at least 1",
            ));
        }
        Ok(Self {
            state: Mutex::new(MessageQueueState::new(max_size, batch_size)),
        })
    }

    /// Add a message to the queue with priority-based dropping.
    ///
    /// When the queue is full, the oldest message of a strictly lower priority
    /// is dropped to make room: BACKGROUND (1) first, then LOW (3), then
    /// NORMAL (5); HIGH and CRITICAL messages are never dropped. If no lower
    /// priority message is queued, the new message is rejected (returns False)
    /// and counted under its own priority in `dropped_by_priority`.
    #[pyo3(signature = (message, priority=5))]
    fn enqueue(&self, py: Python<'_>, message: Py<PyAny>, priority: i32) -> bool {
        let (accepted, evicted) = lock(py, &self.state).enqueue(message, priority);
        drop(evicted); // after the lock, see the note at the top of this file
        accepted
    }

    /// Get a batch of messages ordered by priority (highest first).
    ///
    /// Returns a list of (priority, message) tuples.
    fn dequeue_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        lock(py, &self.state).dequeue_batch(py)
    }

    /// Clear all queues.
    fn clear(&self, py: Python<'_>) {
        let removed = lock(py, &self.state).clear();
        drop(removed); // after the lock, see the note at the top of this file
    }

    /// Get queue statistics.
    ///
    /// Returns a dict with: size, capacity, utilization_percent,
    /// priority_distribution, priority_queue_depths, dropped_by_priority,
    /// total_dropped, backpressure, oldest_message_age, processing_rate.
    fn get_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        lock(py, &self.state).get_stats(py)
    }

    /// Current total message count.
    #[getter]
    fn size(&self, py: Python<'_>) -> usize {
        lock(py, &self.state).size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throughput_averages_over_the_trailing_window() {
        let t0 = Instant::now();
        let mut t = Throughput::new(t0);
        t.record(t0, 20);
        t.record(t0 + Duration::from_millis(50), 30);
        assert_eq!(t.buckets.len(), 1, "close dequeues share a bucket");
        // Younger than a second: averaged over the one-second floor.
        assert_eq!(t.rate(t0 + Duration::from_millis(500)), 50.0);
        // The same fifty spread over the five seconds since creation.
        assert_eq!(t.rate(t0 + Duration::from_secs(5)), 10.0);
        // Out of the window they no longer count, and are pruned on record.
        assert_eq!(t.rate(t0 + Duration::from_secs(11)), 0.0);
        t.record(t0 + Duration::from_secs(11), 10);
        assert_eq!(t.buckets.len(), 1);
        assert_eq!(t.rate(t0 + Duration::from_secs(11)), 1.0);
    }
}
