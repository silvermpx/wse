use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::{DashMap, DashSet};

/// Wall-clock time in milliseconds since UNIX epoch.
pub(crate) fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Debug)]
pub(crate) struct PresenceEntry {
    pub user_id: String,
    pub data: serde_json::Value,
    pub connections: Vec<ConnMeta>,
    pub updated_at: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnMeta {
    pub conn_id: String,
    #[allow(dead_code)]
    pub joined_at: u64,
}

pub(crate) struct PresenceStats {
    pub num_users: AtomicUsize,
    pub num_connections: AtomicUsize,
}

impl PresenceStats {
    pub fn new() -> Self {
        Self {
            num_users: AtomicUsize::new(0),
            num_connections: AtomicUsize::new(0),
        }
    }
}

/// Result of a track operation.
pub(crate) enum TrackResult {
    /// First connection for this user in this topic (emit join event).
    FirstJoin {
        user_id: String,
        data: serde_json::Value,
    },
    /// Additional connection for existing user (no event needed).
    ExistingUser,
    /// Topic at max capacity or data too large, tracking skipped.
    AtCapacity,
    /// No user_id available for this connection.
    NoUserId,
}

/// Result of an untrack operation.
pub(crate) enum UntrackResult {
    /// Last connection removed, user is gone (emit leave event).
    LastLeave {
        user_id: String,
        data: serde_json::Value,
    },
    /// Connection removed but user still has other connections (no event).
    StillPresent,
    /// Connection was not tracked in this topic.
    NotTracked,
}

pub(crate) struct PresenceManager {
    /// topic -> user_id -> PresenceEntry
    topic_presence: DashMap<String, DashMap<String, PresenceEntry>>,
    /// topic -> PresenceStats (incremental counters)
    topic_presence_stats: DashMap<String, PresenceStats>,
    /// conn_id -> user_id (from JWT auth)
    conn_user_id: DashMap<String, String>,
    /// conn_id -> topics where this conn has presence
    conn_presence_topics: DashMap<String, DashSet<String>>,
    /// Max bytes for presence data JSON
    max_data_size: usize,
    /// Max members per topic (0 = unlimited)
    max_members: usize,
}

impl PresenceManager {
    pub fn new(max_data_size: usize, max_members: usize) -> Self {
        Self {
            topic_presence: DashMap::new(),
            topic_presence_stats: DashMap::new(),
            conn_user_id: DashMap::new(),
            conn_presence_topics: DashMap::new(),
            max_data_size,
            max_members,
        }
    }

    /// Register a user_id for a connection (called on JWT auth).
    pub fn register_connection(&self, conn_id: &str, user_id: &str) {
        self.conn_user_id
            .insert(conn_id.to_owned(), user_id.to_owned());
    }

    /// Clean up all presence state for a connection (called on disconnect).
    /// Returns a list of (topic, UntrackResult) for each topic the conn had presence in.
    pub fn remove_connection(&self, conn_id: &str) -> Vec<(String, UntrackResult)> {
        let mut results = Vec::new();

        if let Some((_, topics)) = self.conn_presence_topics.remove(conn_id) {
            for topic_ref in topics.iter() {
                let topic = topic_ref.clone();
                let result = self.untrack_conn_from_topic(conn_id, &topic);
                results.push((topic, result));
            }
        }

        self.conn_user_id.remove(conn_id);

        results
    }

    /// Track a connection's presence in a topic.
    pub fn track(&self, conn_id: &str, topic: &str, data: &serde_json::Value) -> TrackResult {
        // Validate data size
        let data_str = serde_json::to_string(data).unwrap_or_default();
        if data_str.len() > self.max_data_size {
            return TrackResult::AtCapacity;
        }

        let user_id = match self.conn_user_id.get(conn_id) {
            Some(uid) => uid.value().clone(),
            None => return TrackResult::NoUserId,
        };

        let now = epoch_ms();
        let topic_map = self.topic_presence.entry(topic.to_owned()).or_default();

        // Check max members limit
        if self.max_members > 0
            && !topic_map.contains_key(&user_id)
            && topic_map.len() >= self.max_members
        {
            return TrackResult::AtCapacity;
        }

        let (is_first, actually_inserted) = {
            let mut entry = topic_map
                .entry(user_id.clone())
                .or_insert_with(|| PresenceEntry {
                    user_id: user_id.clone(),
                    data: data.clone(),
                    connections: Vec::new(),
                    updated_at: now,
                });
            // Check for first LOCAL connection (ignore remote sentinels from cluster sync)
            let was_empty = !entry
                .connections
                .iter()
                .any(|c| !c.conn_id.starts_with("__remote__"));
            // Add this connection if not already tracked
            let inserted = if !entry.connections.iter().any(|c| c.conn_id == conn_id) {
                entry.connections.push(ConnMeta {
                    conn_id: conn_id.to_owned(),
                    joined_at: now,
                });
                true
            } else {
                false
            };
            entry.data = data.clone();
            entry.updated_at = now;
            (was_empty, inserted)
        };

        // Track which topics this conn has presence in
        self.conn_presence_topics
            .entry(conn_id.to_owned())
            .or_default()
            .insert(topic.to_owned());

        // Update stats
        let stats = self
            .topic_presence_stats
            .entry(topic.to_owned())
            .or_insert_with(PresenceStats::new);
        if actually_inserted {
            stats.num_connections.fetch_add(1, Ordering::Relaxed);
        }

        if is_first {
            stats.num_users.fetch_add(1, Ordering::Relaxed);
            TrackResult::FirstJoin {
                user_id,
                data: data.clone(),
            }
        } else {
            TrackResult::ExistingUser
        }
    }

    /// Untrack a connection from a specific topic.
    fn untrack_conn_from_topic(&self, conn_id: &str, topic: &str) -> UntrackResult {
        let user_id = match self.conn_user_id.get(conn_id) {
            Some(uid) => uid.value().clone(),
            // Connection might already be cleaned up; try to find user_id from the presence entry
            None => {
                if let Some(topic_map) = self.topic_presence.get(topic) {
                    let mut found_user = None;
                    for entry_ref in topic_map.iter() {
                        if entry_ref.connections.iter().any(|c| c.conn_id == conn_id) {
                            found_user = Some(entry_ref.user_id.clone());
                            break;
                        }
                    }
                    match found_user {
                        Some(uid) => uid,
                        None => return UntrackResult::NotTracked,
                    }
                } else {
                    return UntrackResult::NotTracked;
                }
            }
        };

        if let Some(topic_map) = self.topic_presence.get(topic) {
            // Phase 1: Remove the connection from the entry (get_mut holds shard lock)
            let mut removed_conn = false;
            if let Some(mut entry) = topic_map.get_mut(&user_id) {
                let before = entry.connections.len();
                entry.connections.retain(|c| c.conn_id != conn_id);
                if entry.connections.len() < before {
                    removed_conn = true;
                    if let Some(stats) = self.topic_presence_stats.get(topic) {
                        stats.num_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }

            // Phase 2: Atomically remove entry only if connections is still empty
            // (prevents TOCTOU race where track() adds a connection between phases)
            if removed_conn
                && let Some((_, removed_entry)) =
                    topic_map.remove_if(&user_id, |_, entry| entry.connections.is_empty())
            {
                if let Some(stats) = self.topic_presence_stats.get(topic) {
                    let _ = stats.num_users.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |current| Some(current.saturating_sub(1)),
                    );
                }
                return UntrackResult::LastLeave {
                    user_id,
                    data: removed_entry.data,
                };
            }
        }

        UntrackResult::StillPresent
    }

    /// Untrack a connection from specific topics (called on unsubscribe).
    pub fn untrack_topics(&self, conn_id: &str, topics: &[String]) -> Vec<(String, UntrackResult)> {
        let mut results = Vec::new();
        for topic in topics {
            // Remove from conn_presence_topics tracking
            if let Some(conn_topics) = self.conn_presence_topics.get(conn_id) {
                conn_topics.remove(topic);
            }
            let result = self.untrack_conn_from_topic(conn_id, topic);
            results.push((topic.clone(), result));
        }
        results
    }

    /// Query all members in a topic.
    /// Returns: Vec<(user_id, data, connection_count)>
    pub fn query(&self, topic: &str) -> Vec<(String, serde_json::Value, usize)> {
        let mut members = Vec::new();
        if let Some(topic_map) = self.topic_presence.get(topic) {
            for entry_ref in topic_map.iter() {
                let entry = entry_ref.value();
                members.push((
                    entry.user_id.clone(),
                    entry.data.clone(),
                    entry.connections.len(),
                ));
            }
        }
        members
    }

    /// Get lightweight stats for a topic (O(1)).
    pub fn stats(&self, topic: &str) -> (usize, usize) {
        if let Some(stats) = self.topic_presence_stats.get(topic) {
            (
                stats.num_users.load(Ordering::Relaxed),
                stats.num_connections.load(Ordering::Relaxed),
            )
        } else {
            (0, 0)
        }
    }

    /// Update presence data for a connection across all its topics.
    /// Returns None on failure, Some((user_id, topics)) on success.
    pub fn update_data(
        &self,
        conn_id: &str,
        new_data: &serde_json::Value,
    ) -> Option<(String, Vec<String>)> {
        // Validate size
        let data_str = serde_json::to_string(new_data).unwrap_or_default();
        if data_str.len() > self.max_data_size {
            return None;
        }

        let user_id = match self.conn_user_id.get(conn_id) {
            Some(uid) => uid.value().clone(),
            None => return None,
        };

        let now = epoch_ms();
        let mut updated_topics = Vec::new();

        if let Some(topics) = self.conn_presence_topics.get(conn_id) {
            for topic_ref in topics.iter() {
                let topic = topic_ref.clone();
                if let Some(topic_map) = self.topic_presence.get(&topic)
                    && let Some(mut entry) = topic_map.get_mut(&user_id)
                {
                    entry.data = new_data.clone();
                    entry.updated_at = now;
                    updated_topics.push(topic);
                }
            }
        }

        Some((user_id, updated_topics))
    }

    /// Get total presence-tracked topics count.
    pub fn total_topics(&self) -> usize {
        self.topic_presence.len()
    }

    /// Get total unique user count across all topics.
    pub fn total_users(&self) -> usize {
        self.topic_presence
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }

    /// TTL sweep: remove presence entries where all connections are dead.
    /// Returns list of (topic, user_id, data) for entries that were removed.
    pub fn sweep_dead_connections<F>(
        &self,
        is_conn_alive: F,
    ) -> Vec<(String, String, serde_json::Value)>
    where
        F: Fn(&str) -> bool,
    {
        let mut removed = Vec::new();

        for topic_entry in self.topic_presence.iter() {
            let topic = topic_entry.key().clone();
            let topic_map = topic_entry.value();
            let mut dead_users = Vec::new();

            for user_entry in topic_map.iter() {
                let user_id = user_entry.key().clone();
                let entry = user_entry.value();
                let alive_count = entry
                    .connections
                    .iter()
                    .filter(|c| is_conn_alive(&c.conn_id))
                    .count();
                if alive_count == 0 {
                    dead_users.push((user_id, entry.data.clone(), entry.connections.len()));
                }
            }

            for (user_id, data, _dead_conn_count) in dead_users {
                // Collect dead conn_ids before removing the user entry
                let actual_conn_count = if let Some((_, entry)) = topic_map.remove(&user_id) {
                    let count = entry.connections.len();
                    for conn in &entry.connections {
                        self.conn_user_id.remove(&conn.conn_id);
                        self.conn_presence_topics.remove(&conn.conn_id);
                    }
                    count
                } else {
                    0
                };
                if let Some(stats) = self.topic_presence_stats.get(&topic) {
                    let _ = stats.num_users.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |current| Some(current.saturating_sub(1)),
                    );
                    let _ = stats.num_connections.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |current| Some(current.saturating_sub(actual_conn_count)),
                    );
                }
                removed.push((topic.clone(), user_id, data));
            }
        }

        removed
    }

    // -----------------------------------------------------------------------
    // Cluster presence sync: merge remote presence state
    // -----------------------------------------------------------------------

    /// Merge a remote join from a peer node. Does NOT emit events (caller handles broadcast).
    pub fn merge_remote_join(
        &self,
        topic: &str,
        user_id: &str,
        data: &serde_json::Value,
        updated_at: u64,
    ) {
        // Validate remote data size (same limit as local track)
        let data_str = data.to_string();
        if data_str.len() > self.max_data_size {
            eprintln!(
                "[WSE-Presence] Rejecting oversized remote presence data ({} bytes, max {})",
                data_str.len(),
                self.max_data_size
            );
            return;
        }
        let topic_map = self.topic_presence.entry(topic.to_owned()).or_default();

        // Enforce max_members limit for remote entries too
        if self.max_members > 0
            && !topic_map.contains_key(user_id)
            && topic_map.len() >= self.max_members
        {
            eprintln!(
                "[WSE-Presence] Rejecting remote join for {user_id}: topic at capacity ({})",
                self.max_members
            );
            return;
        }

        let mut entry = topic_map
            .entry(user_id.to_owned())
            .or_insert_with(|| PresenceEntry {
                user_id: user_id.to_owned(),
                data: data.clone(),
                connections: Vec::new(),
                updated_at,
            });
        // Last-write-wins: only update if newer
        if updated_at >= entry.updated_at {
            entry.data = data.clone();
            entry.updated_at = updated_at;
        }
        // Remote-only entry: add a sentinel connection so it appears in presence queries
        if entry.connections.is_empty() {
            entry.connections.push(ConnMeta {
                conn_id: format!("__remote__{user_id}"),
                joined_at: updated_at,
            });
            // Update stats
            let stats = self
                .topic_presence_stats
                .entry(topic.to_owned())
                .or_insert_with(PresenceStats::new);
            stats.num_users.fetch_add(1, Ordering::Relaxed);
            stats.num_connections.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Merge a remote leave from a peer node.
    pub fn merge_remote_leave(&self, topic: &str, user_id: &str) {
        if let Some(topic_map) = self.topic_presence.get(topic) {
            let mut should_remove_user = false;
            let mut removed_sentinel = false;

            if let Some(mut entry) = topic_map.get_mut(user_id) {
                // Remove the __remote__ sentinel specifically
                let before = entry.connections.len();
                entry
                    .connections
                    .retain(|c| !c.conn_id.starts_with("__remote__"));
                removed_sentinel = entry.connections.len() < before;
                // If no connections remain, remove the user entirely
                should_remove_user = entry.connections.is_empty();
            }

            if should_remove_user {
                // Use remove_if to prevent TOCTOU: track() could add a local
                // connection between get_mut release and this remove call.
                let actually_removed =
                    topic_map.remove_if(user_id, |_, entry| entry.connections.is_empty());
                if actually_removed.is_some()
                    && let Some(stats) = self.topic_presence_stats.get(topic)
                {
                    let _ = stats.num_users.fetch_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |current| Some(current.saturating_sub(1)),
                    );
                }
            }
            // Decrement connection count for each removed sentinel
            if removed_sentinel && let Some(stats) = self.topic_presence_stats.get(topic) {
                let _ = stats.num_connections.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| Some(current.saturating_sub(1)),
                );
            }
        }
    }

    /// Merge a remote data update (last-write-wins).
    pub fn merge_remote_update(
        &self,
        topic: &str,
        user_id: &str,
        data: &serde_json::Value,
        updated_at: u64,
    ) {
        // Validate remote data size (same limit as local track)
        let data_str = data.to_string();
        if data_str.len() > self.max_data_size {
            eprintln!(
                "[WSE-Presence] Rejecting oversized remote presence update ({} bytes, max {})",
                data_str.len(),
                self.max_data_size
            );
            return;
        }
        if let Some(topic_map) = self.topic_presence.get(topic)
            && let Some(mut entry) = topic_map.get_mut(user_id)
            && updated_at >= entry.updated_at
        {
            entry.data = data.clone();
            entry.updated_at = updated_at;
        }
    }

    /// Merge full presence state from a peer (called on peer connect).
    /// Expected JSON format: {"topic": {"user_id": {"data": {...}, "updated_at": 123}}}
    pub fn merge_full_state(&self, entries_json: &str) {
        // Cap incoming full state to 10MB to prevent OOM
        if entries_json.len() > 10_000_000 {
            eprintln!(
                "[Presence] Full state too large ({} bytes, max 10MB), rejecting",
                entries_json.len()
            );
            return;
        }
        let parsed: serde_json::Value = match serde_json::from_str(entries_json) {
            Ok(v) => v,
            Err(_) => return,
        };
        let topics = match parsed.as_object() {
            Some(t) => t,
            None => return,
        };
        for (topic, users_val) in topics {
            let users = match users_val.as_object() {
                Some(u) => u,
                None => continue,
            };
            for (user_id, entry_val) in users {
                let data = entry_val
                    .get("data")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let updated_at = entry_val
                    .get("updated_at")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                self.merge_remote_join(topic, user_id, &data, updated_at);
            }
        }
    }

    /// Serialize all local presence state to JSON for full sync with peers.
    /// Only includes entries with local connections (not re-syncing remote entries).
    pub fn serialize_full_state(&self) -> String {
        let mut state = serde_json::Map::new();
        for topic_entry in self.topic_presence.iter() {
            let topic = topic_entry.key();
            let topic_map = topic_entry.value();
            let mut users = serde_json::Map::new();
            for user_entry in topic_map.iter() {
                let entry = user_entry.value();
                // Only include entries with local connections (don't re-sync remote entries)
                if entry
                    .connections
                    .iter()
                    .any(|c| !c.conn_id.starts_with("__remote__"))
                {
                    users.insert(
                        entry.user_id.clone(),
                        serde_json::json!({
                            "data": entry.data,
                            "updated_at": entry.updated_at,
                        }),
                    );
                }
            }
            if !users.is_empty() {
                state.insert(topic.clone(), serde_json::Value::Object(users));
            }
        }
        serde_json::to_string(&serde_json::Value::Object(state)).unwrap_or_default()
    }
}
