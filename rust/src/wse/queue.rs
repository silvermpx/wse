use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::time::Instant;

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
#[pyclass]
pub struct RustPriorityQueue {
    heap: BinaryHeap<QueueItem>,
    sequence: u64,
    max_size: usize,
}

#[pymethods]
impl RustPriorityQueue {
    #[new]
    fn new(max_size: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(max_size),
            sequence: 0,
            max_size,
        }
    }

    /// Push a message with the given priority. Returns false if full.
    fn push(&mut self, _py: Python, priority: i32, message: Py<PyAny>) -> bool {
        if self.heap.len() >= self.max_size {
            return false;
        }
        self.sequence += 1;
        self.heap.push(QueueItem {
            neg_priority: -priority,
            sequence: self.sequence,
            message,
        });
        true
    }

    /// Pop the highest-priority message. Returns None if empty.
    fn pop<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self.heap.pop() {
            Some(item) => Ok(item.message.into_bound(py)),
            None => Ok(py.None().into_bound(py)),
        }
    }

    /// Drain up to `max_count` messages, highest priority first.
    fn drain<'py>(&mut self, py: Python<'py>, max_count: usize) -> PyResult<Bound<'py, PyList>> {
        let count = max_count.min(self.heap.len());
        let items: Vec<Py<PyAny>> = (0..count)
            .filter_map(|_| self.heap.pop().map(|item| item.message))
            .collect();
        Ok(PyList::new(py, items)?)
    }

    fn len(&self) -> usize {
        self.heap.len()
    }

    fn is_full(&self) -> bool {
        self.heap.len() >= self.max_size
    }

    fn clear(&mut self) {
        self.heap.clear();
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
#[pyclass]
pub struct RustPriorityMessageQueue {
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
}

#[pymethods]
impl RustPriorityMessageQueue {
    #[new]
    #[pyo3(signature = (max_size=1000, batch_size=10))]
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
        }
    }

    /// Add a message to the queue with priority-based dropping.
    ///
    /// When queue is full:
    /// 1. Try to drop BACKGROUND (1) priority messages
    /// 2. Try to drop LOW (3) priority messages
    /// 3. Try to drop NORMAL (5) priority messages
    /// 4. If new message is LOW/NORMAL and still full, drop it
    /// 5. For HIGH/CRITICAL, drop oldest NORMAL as last resort
    /// 6. If absolutely full with only HIGH/CRITICAL, drop new message
    #[pyo3(signature = (message, priority=5))]
    fn enqueue(&mut self, message: Py<PyAny>, priority: i32) -> bool {
        // Normalize priority to the closest valid level
        let priority = Self::normalize_priority(priority);

        if self.size >= self.max_size {
            // Try to make room by dropping lower priority messages
            let mut dropped = false;

            // Try BACKGROUND(1), then LOW(3), then NORMAL(5)
            for &drop_priority in &[1, 3, 5] {
                let idx = priority_to_index(drop_priority);
                if !self.queues[idx].is_empty() {
                    self.queues[idx].pop_front();
                    self.size = self.size.saturating_sub(1);
                    let dist = self.priority_distribution.entry(drop_priority).or_insert(0);
                    *dist = dist.saturating_sub(1);
                    *self.dropped_count.entry(drop_priority).or_insert(0) += 1;
                    dropped = true;
                    break;
                }
            }

            if !dropped {
                // Queue full with only HIGH/CRITICAL messages
                if priority < 8 {
                    // Not HIGH or CRITICAL -> drop the new message
                    *self.dropped_count.entry(priority).or_insert(0) += 1;
                    return false;
                }
                // HIGH/CRITICAL but no room: still cannot enqueue
                // (queue is full of HIGH/CRITICAL only)
                // The Python code does not explicitly handle this final case
                // with a return false, but implicitly the message IS enqueued
                // because `dropped` stays false but we continue past the if-block.
                // However, size >= max_size and nothing was dropped, so we must
                // drop the incoming message to maintain the invariant.
                *self.dropped_count.entry(priority).or_insert(0) += 1;
                return false;
            }
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

        true
    }

    /// Get a batch of messages ordered by priority (highest first).
    ///
    /// Returns a list of (priority, message) tuples.
    fn dequeue_batch<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mut batch: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(self.batch_size);

        // Process from highest to lowest priority: 10, 8, 5, 3, 1
        for &priority in &[10, 8, 5, 3, 1] {
            let idx = priority_to_index(priority);

            while !self.queues[idx].is_empty() && batch.len() < self.batch_size {
                if let Some(entry) = self.queues[idx].pop_front() {
                    let tuple = PyTuple::new(
                        py,
                        &[priority.into_pyobject(py)?.into_any(), entry.message.into_bound(py)],
                    )?;
                    batch.push(tuple);
                    self.size = self.size.saturating_sub(1);
                    let dist = self.priority_distribution.entry(priority).or_insert(0);
                    *dist = dist.saturating_sub(1);
                }
            }
        }

        // Update oldest message timestamp
        if self.size == 0 {
            self.oldest_message_timestamp = None;
        } else if !batch.is_empty() {
            // Find the new oldest message across all queues
            let mut oldest: Option<Instant> = None;
            for q in &self.queues {
                if let Some(front) = q.front() {
                    match oldest {
                        None => oldest = Some(front.timestamp),
                        Some(current) => {
                            if front.timestamp < current {
                                oldest = Some(front.timestamp);
                            }
                        }
                    }
                }
            }
            self.oldest_message_timestamp = oldest;
        }

        PyList::new(py, batch)
    }

    /// Clear all queues.
    fn clear(&mut self) {
        for q in &mut self.queues {
            q.clear();
        }
        self.size = 0;
        for val in self.priority_distribution.values_mut() {
            *val = 0;
        }
        self.oldest_message_timestamp = None;
    }

    /// Get queue statistics.
    ///
    /// Returns a dict with: size, capacity, utilization_percent,
    /// priority_distribution, priority_queue_depths, dropped_by_priority,
    /// total_dropped, backpressure, oldest_message_age, processing_rate.
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

        // processing_rate: placeholder (matches Python)
        stats.set_item("processing_rate", 0.0)?;

        Ok(stats)
    }

    /// Current total message count.
    #[getter]
    fn size(&self) -> usize {
        self.size
    }
}

impl RustPriorityMessageQueue {
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
