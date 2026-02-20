use ahash::AHashSet;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

// ---------------------------------------------------------------------------
// RustSequencer -- simple version, kept for backward compatibility
// ---------------------------------------------------------------------------

/// Simple event sequencer with deduplication.
///
/// Assigns monotonically increasing sequence numbers and tracks recently
/// seen event IDs to detect duplicates. Old entries are evicted either when
/// the window is full (size-based) or via explicit `cleanup` (age-based).
///
/// Uses `ahash::AHashSet` for O(1) lookups with a fast non-cryptographic
/// hash, and a `VecDeque` to maintain insertion order for eviction.
#[pyclass]
pub struct RustSequencer {
    sequence: u64,
    seen_ids: AHashSet<String>,
    seen_ids_order: VecDeque<(String, Instant)>,
    window_size: usize,
}

#[pymethods]
impl RustSequencer {
    /// Create a new sequencer.
    ///
    /// # Arguments
    /// * `window_size` - Maximum number of event IDs to track for dedup.
    #[new]
    fn new(window_size: usize) -> Self {
        Self {
            sequence: 0,
            seen_ids: AHashSet::with_capacity(window_size),
            seen_ids_order: VecDeque::with_capacity(window_size),
            window_size,
        }
    }

    /// Increment the sequence counter and return the new value.
    fn next_seq(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Return the current sequence number without incrementing.
    fn get_current_seq(&self) -> u64 {
        self.sequence
    }

    /// Check if `event_id` is a duplicate.
    ///
    /// If the ID has been seen before, returns `true`.
    /// If not, records it and returns `false`. When the set exceeds
    /// `window_size`, the oldest entry is evicted to make room.
    fn is_duplicate(&mut self, event_id: &str) -> bool {
        if self.seen_ids.contains(event_id) {
            return true;
        }

        // Evict oldest if at capacity.
        if self.seen_ids.len() >= self.window_size
            && let Some((oldest_id, _)) = self.seen_ids_order.pop_front()
        {
            self.seen_ids.remove(&oldest_id);
        }

        let owned = event_id.to_owned();
        self.seen_ids.insert(owned.clone());
        self.seen_ids_order.push_back((owned, Instant::now()));
        false
    }

    /// Remove all entries older than `max_age_secs` seconds.
    ///
    /// Scans from the front of the ordered deque (oldest first) and removes
    /// entries whose timestamp is older than the cutoff. Stops as soon as
    /// it encounters an entry within the age window since the deque is
    /// ordered by insertion time.
    fn cleanup(&mut self, max_age_secs: f64) {
        let now = Instant::now();
        while let Some((id, timestamp)) = self.seen_ids_order.front() {
            let age = now.duration_since(*timestamp).as_secs_f64();
            if age > max_age_secs {
                let id = id.clone();
                self.seen_ids_order.pop_front();
                self.seen_ids.remove(&id);
            } else {
                // All remaining entries are newer.
                break;
            }
        }
    }

    /// Return the number of event IDs currently tracked.
    fn seen_count(&self) -> usize {
        self.seen_ids.len()
    }
}

// ---------------------------------------------------------------------------
// RustEventSequencer -- full parity with Python EventSequencer
// ---------------------------------------------------------------------------

/// A buffered event waiting for its turn to be delivered in-order.
/// Mirrors the Python `SequencedEvent` dataclass.
#[allow(dead_code)]
struct BufferedEvent {
    event_id: String,
    sequence: u64,
    timestamp: Instant,
    payload: Py<PyAny>,
}

/// Full-featured event sequencer with deduplication, per-topic ordering,
/// out-of-order buffering, and gap detection.
///
/// This is the Rust counterpart of the Python `EventSequencer` class from
/// `wse_event_sequencer.py`, providing complete API parity.
#[pyclass]
pub struct RustEventSequencer {
    /// Global monotonically-increasing sequence counter.
    sequence: u64,
    /// Fast set of recently seen event IDs for dedup.
    seen_ids: AHashSet<String>,
    /// Ordered queue for evicting oldest seen IDs.
    seen_ids_queue: VecDeque<String>,
    /// Maximum number of event IDs to track.
    window_size: usize,
    /// Maximum gap between expected and received sequence before reset.
    max_out_of_order: u64,

    /// Per-topic: the next expected sequence number.
    expected_sequences: HashMap<String, u64>,
    /// Per-topic: buffered future events keyed by sequence number.
    buffered_events: HashMap<String, BTreeMap<u64, BufferedEvent>>,

    // Stats
    duplicate_count: u64,
    out_of_order_count: u64,
    dropped_count: u64,
}

#[pymethods]
impl RustEventSequencer {
    /// Create a new event sequencer.
    ///
    /// # Arguments
    /// * `window_size` - Maximum number of event IDs to track for dedup (default 10000).
    /// * `max_out_of_order` - Maximum gap before resetting topic sequence (default 100).
    #[new]
    #[pyo3(signature = (window_size=10000, max_out_of_order=100))]
    fn new(window_size: usize, max_out_of_order: u64) -> Self {
        Self {
            sequence: 0,
            seen_ids: AHashSet::with_capacity(window_size),
            seen_ids_queue: VecDeque::with_capacity(window_size),
            window_size,
            max_out_of_order,
            expected_sequences: HashMap::new(),
            buffered_events: HashMap::new(),
            duplicate_count: 0,
            out_of_order_count: 0,
            dropped_count: 0,
        }
    }

    /// Increment the sequence counter and return the new value.
    fn next_seq(&mut self) -> u64 {
        self.sequence += 1;
        self.sequence
    }

    /// Return the current sequence number without incrementing.
    fn get_current_sequence(&self) -> u64 {
        self.sequence
    }

    /// Check if an event is a duplicate within the dedup window.
    ///
    /// If the ID has been seen before, increments `duplicate_count` and
    /// returns `true`. Otherwise records the ID and returns `false`.
    /// When the seen queue reaches `window_size`, the oldest entry is evicted
    /// from both the queue and the set before inserting the new one.
    ///
    /// Matches Python behavior: deque(maxlen=window_size) auto-evicts on
    /// append, then the set is cleaned via discard of the current front.
    fn is_duplicate(&mut self, event_id: &str) -> bool {
        if self.seen_ids.contains(event_id) {
            self.duplicate_count += 1;
            return true;
        }

        // Evict the oldest when the queue is at capacity.
        if self.seen_ids_queue.len() >= self.window_size
            && let Some(old_id) = self.seen_ids_queue.pop_front()
        {
            self.seen_ids.remove(&old_id);
        }

        let owned = event_id.to_owned();
        self.seen_ids.insert(owned.clone());
        self.seen_ids_queue.push_back(owned);

        false
    }

    /// Process an event with a sequence number, maintaining per-topic order.
    ///
    /// Returns:
    /// - A list of dicts if events can be delivered (the current event plus
    ///   any buffered events that are now in order).
    /// - `None` if the event was buffered or dropped.
    ///
    /// Behavior:
    /// - First event on a topic initializes tracking and is delivered immediately.
    /// - If `sequence == expected`: deliver it and flush consecutive buffered events.
    /// - If `sequence > expected` but within `max_out_of_order`: buffer for later.
    /// - If `sequence > expected` beyond `max_out_of_order`: reset topic, deliver current.
    /// - If `sequence < expected`: drop (old/duplicate event).
    #[pyo3(signature = (topic, sequence, event))]
    fn process_sequenced_event<'py>(
        &mut self,
        py: Python<'py>,
        topic: &str,
        sequence: u64,
        event: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let topic_owned = topic.to_owned();

        // Initialize topic tracking on first event.
        if !self.expected_sequences.contains_key(&topic_owned) {
            self.expected_sequences
                .insert(topic_owned.clone(), sequence + 1);
            self.buffered_events.insert(topic_owned, BTreeMap::new());

            let result = PyList::new(py, [event.clone()])?;
            return Ok(result.into_any());
        }

        let expected = *self.expected_sequences.get(&topic_owned).unwrap();

        if sequence == expected {
            // This is the expected sequence -- deliver it and any consecutive buffered.
            let mut events_to_deliver: Vec<Bound<'py, PyAny>> = Vec::new();
            events_to_deliver.push(event.clone());

            let mut next_expected = sequence + 1;

            if let Some(buffer) = self.buffered_events.get_mut(&topic_owned) {
                while buffer.contains_key(&next_expected) {
                    let buffered = buffer.remove(&next_expected).unwrap();
                    events_to_deliver.push(buffered.payload.into_bound(py));
                    next_expected += 1;
                }
            }

            self.expected_sequences.insert(topic_owned, next_expected);

            let result = PyList::new(py, events_to_deliver)?;
            Ok(result.into_any())
        } else if sequence > expected {
            // Future event -- either buffer or reset.
            self.out_of_order_count += 1;

            if sequence - expected > self.max_out_of_order {
                // Gap too large -- drop intermediates, reset, deliver current.
                self.dropped_count += sequence - expected - 1;
                self.expected_sequences
                    .insert(topic_owned.clone(), sequence + 1);
                if let Some(buffer) = self.buffered_events.get_mut(&topic_owned) {
                    buffer.clear();
                }

                let result = PyList::new(py, [event.clone()])?;
                Ok(result.into_any())
            } else {
                // Buffer the event for later delivery.
                let event_id: String = event
                    .get_item("id")
                    .ok()
                    .and_then(|v: Bound<'_, PyAny>| v.extract::<String>().ok())
                    .unwrap_or_default();

                let buffered = BufferedEvent {
                    event_id,
                    sequence,
                    timestamp: Instant::now(),
                    payload: event.clone().unbind(),
                };

                self.buffered_events
                    .entry(topic_owned)
                    .or_default()
                    .insert(sequence, buffered);

                Ok(py.None().into_bound(py))
            }
        } else {
            // Old event -- drop it.
            self.dropped_count += 1;
            Ok(py.None().into_bound(py))
        }
    }

    /// Reset sequence tracking for a specific topic or all topics.
    ///
    /// # Arguments
    /// * `topic` - If provided, reset only that topic. If `None`, reset all.
    #[pyo3(signature = (topic=None))]
    fn reset_sequence(&mut self, topic: Option<&str>) {
        match topic {
            Some(t) => {
                self.expected_sequences.remove(t);
                self.buffered_events.remove(t);
            }
            None => {
                self.expected_sequences.clear();
                self.buffered_events.clear();
            }
        }
    }

    /// Get detailed sequence statistics.
    ///
    /// Returns a dict with: current_sequence, duplicate_count,
    /// out_of_order_count, dropped_count, topics (per-topic stats with
    /// expected/buffered/gaps/buffered_sequences), total_topics,
    /// total_buffered, seen_ids_count.
    fn get_sequence_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = PyDict::new(py);
        let topic_stats = PyDict::new(py);
        let mut total_buffered: usize = 0;

        for (topic, &expected) in &self.expected_sequences {
            let empty_tree = BTreeMap::new();
            let buffered = self.buffered_events.get(topic).unwrap_or(&empty_tree);
            total_buffered += buffered.len();

            let topic_dict = PyDict::new(py);
            topic_dict.set_item("expected", expected)?;
            topic_dict.set_item("buffered", buffered.len())?;

            // Compute gaps.
            let gaps = PyList::empty(py);
            if !buffered.is_empty() {
                let sequences: Vec<u64> = buffered.keys().copied().collect();
                let mut last = expected;
                for &seq in &sequences {
                    if seq - last > 1 {
                        let gap = PyDict::new(py);
                        gap.set_item("start", last + 1)?;
                        gap.set_item("end", seq - 1)?;
                        gap.set_item("size", seq - last - 1)?;
                        gaps.append(gap)?;
                    }
                    last = seq;
                }
            }
            topic_dict.set_item("gaps", gaps)?;

            // buffered_sequences
            let buffered_seqs: Vec<u64> = buffered.keys().copied().collect();
            let buffered_seqs_list = PyList::new(py, &buffered_seqs)?;
            topic_dict.set_item("buffered_sequences", buffered_seqs_list)?;

            topic_stats.set_item(topic, topic_dict)?;
        }

        stats.set_item("current_sequence", self.sequence)?;
        stats.set_item("duplicate_count", self.duplicate_count)?;
        stats.set_item("out_of_order_count", self.out_of_order_count)?;
        stats.set_item("dropped_count", self.dropped_count)?;
        stats.set_item("topics", topic_stats)?;
        stats.set_item("total_topics", self.expected_sequences.len())?;
        stats.set_item("total_buffered", total_buffered)?;
        stats.set_item("seen_ids_count", self.seen_ids.len())?;

        Ok(stats)
    }

    /// Get statistics about buffered events (frontend compatible).
    ///
    /// Returns a dict with: total_topics, total_buffered, and per-topic
    /// buffer info (expected_sequence, buffered_count, buffered_sequences,
    /// gap_size, oldest_buffered_age).
    fn get_buffer_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = PyDict::new(py);
        let topics_dict = PyDict::new(py);
        let now = Instant::now();

        let total_buffered: usize = self
            .buffered_events
            .values()
            .map(|events| events.len())
            .sum();

        for (topic, &expected) in &self.expected_sequences {
            if let Some(buffered) = self.buffered_events.get(topic)
                && !buffered.is_empty()
            {
                let sequences: Vec<u64> = buffered.keys().copied().collect();
                let gap_size = sequences.first().map_or(0, |&first| first - expected);

                let oldest_age = buffered
                    .values()
                    .map(|e| now.duration_since(e.timestamp).as_secs_f64())
                    .fold(0.0_f64, f64::max);

                let topic_dict = PyDict::new(py);
                topic_dict.set_item("expected_sequence", expected)?;
                topic_dict.set_item("buffered_count", buffered.len())?;
                let seqs_list = PyList::new(py, &sequences)?;
                topic_dict.set_item("buffered_sequences", seqs_list)?;
                topic_dict.set_item("gap_size", gap_size)?;
                topic_dict.set_item("oldest_buffered_age", oldest_age)?;

                topics_dict.set_item(topic, topic_dict)?;
            }
        }

        stats.set_item("total_topics", self.expected_sequences.len())?;
        stats.set_item("total_buffered", total_buffered)?;
        stats.set_item("topics", topics_dict)?;

        Ok(stats)
    }

    /// Clean up buffered events older than 5 minutes and trim seen_ids.
    ///
    /// - Removes buffered events whose age exceeds 300 seconds.
    /// - If a topic has no remaining buffered events, removes the topic.
    /// - Trims the seen_ids set if it exceeds 1.5x the window_size.
    fn cleanup(&mut self) {
        let now = Instant::now();

        // Clean buffered events older than 5 minutes.
        let mut empty_topics: Vec<String> = Vec::new();

        for (topic, events) in &mut self.buffered_events {
            let to_remove: Vec<u64> = events
                .iter()
                .filter(|(_, e)| now.duration_since(e.timestamp).as_secs_f64() > 300.0)
                .map(|(&seq, _)| seq)
                .collect();

            for seq in to_remove {
                events.remove(&seq);
                self.dropped_count += 1;
            }

            if events.is_empty() {
                empty_topics.push(topic.clone());
            }
        }

        for topic in empty_topics {
            self.buffered_events.remove(&topic);
            self.expected_sequences.remove(&topic);
        }

        // Trim seen_ids if significantly over window_size.
        let threshold = (self.window_size as f64 * 1.5) as usize;
        if self.seen_ids.len() > threshold {
            let keep_count = self.window_size;
            let drain_count = self.seen_ids_queue.len().saturating_sub(keep_count);

            for _ in 0..drain_count {
                if let Some(old_id) = self.seen_ids_queue.pop_front() {
                    self.seen_ids.remove(&old_id);
                }
            }
        }
    }
}
