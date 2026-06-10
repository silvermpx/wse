use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::{DashMap, DashSet};

/// Wall-clock time in milliseconds since UNIX epoch.
pub(crate) fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Prefix marking a synthetic "remote presence" sentinel connection -- a user
/// present on a peer node with no local socket here.
const REMOTE_PREFIX: &str = "__remote__";
/// Separator between origin and user_id inside a remote sentinel id. The Unit
/// Separator control char (0x1f) never appears in peer addresses or user ids.
const ORIGIN_SEP: char = '\u{1f}';

/// Sentinel connection id for `user_id` reported present by peer `origin`.
///
/// Keyed PER ORIGIN so a leave (or disconnect) from one peer removes only that
/// peer's sentinel and never erases a user still present on another peer. A
/// single global `__remote__{user_id}` sentinel (the old form) made one peer's
/// leave wipe the user cluster-wide and made per-peer ghost cleanup impossible.
fn remote_sentinel(origin: &str, user_id: &str) -> String {
    format!("{REMOTE_PREFIX}{origin}{ORIGIN_SEP}{user_id}")
}

/// Prefix matching every sentinel contributed by `origin` (for purge_origin).
fn remote_origin_prefix(origin: &str) -> String {
    format!("{REMOTE_PREFIX}{origin}{ORIGIN_SEP}")
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
    pub _joined_at: u64,
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

        let (is_first, actually_inserted, has_remote_sentinel) = {
            let mut entry = topic_map
                .entry(user_id.clone())
                .or_insert_with(|| PresenceEntry {
                    user_id: user_id.clone(),
                    data: data.clone(),
                    connections: Vec::new(),
                    updated_at: now,
                });
            // Check for remote sentinel (from cluster sync) while holding the shard lock
            let has_remote = entry
                .connections
                .iter()
                .any(|c| c.conn_id.starts_with("__remote__"));
            // Check for first LOCAL connection (ignore remote sentinels)
            let was_empty = !entry
                .connections
                .iter()
                .any(|c| !c.conn_id.starts_with("__remote__"));
            // Add this connection if not already tracked
            let inserted = if !entry.connections.iter().any(|c| c.conn_id == conn_id) {
                entry.connections.push(ConnMeta {
                    conn_id: conn_id.to_owned(),
                    _joined_at: now,
                });
                true
            } else {
                false
            };
            if now >= entry.updated_at {
                entry.data = data.clone();
                entry.updated_at = now;
            }
            (was_empty, inserted, has_remote)
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
            // Only increment num_users if no remote sentinel exists
            // (merge_remote_join already counted this user)
            if !has_remote_sentinel {
                stats.num_users.fetch_add(1, Ordering::Relaxed);
            }
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
                // Use remove_if to guard against concurrent track() adding a new conn
                let actual_conn_count = if let Some((_, entry)) =
                    topic_map.remove_if(&user_id, |_, e| {
                        // Only remove if all connections are still dead
                        e.connections.iter().all(|c| !is_conn_alive(&c.conn_id))
                    }) {
                    // Only count connections whose tracking entry still exists.
                    // untrack_conn_from_topic may have already removed some and
                    // decremented num_connections for them -- avoid double-decrement.
                    let mut count = 0usize;
                    for conn in &entry.connections {
                        if self.conn_user_id.remove(&conn.conn_id).is_some() {
                            count += 1;
                        }
                        self.conn_presence_topics.remove(&conn.conn_id);
                    }
                    count
                } else {
                    // Entry was either already removed or a live conn was added - skip
                    // But still clean up any dead local connections from the entry
                    if let Some(mut entry) = topic_map.get_mut(&user_id) {
                        let before = entry.connections.len();
                        entry.connections.retain(|c| {
                            c.conn_id.starts_with("__remote__") || is_conn_alive(&c.conn_id)
                        });
                        let removed_count = before - entry.connections.len();
                        if removed_count > 0
                            && let Some(stats) = self.topic_presence_stats.get(&topic)
                        {
                            let _ = stats.num_connections.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |current| Some(current.saturating_sub(removed_count)),
                            );
                        }
                    }
                    0
                };
                // Only decrement stats and emit leave if the entry was actually removed
                if actual_conn_count > 0 {
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
        }

        removed
    }

    // -----------------------------------------------------------------------
    // Cluster presence sync: merge remote presence state
    // -----------------------------------------------------------------------

    /// Merge a remote join from a peer node. Does NOT emit events (caller handles broadcast).
    /// `origin` is the reporting peer's address; the sentinel is keyed by it so
    /// each peer contributes its own and a leave/disconnect from one peer cannot
    /// erase a user still present on another.
    pub fn merge_remote_join(
        &self,
        topic: &str,
        user_id: &str,
        data: &serde_json::Value,
        updated_at: u64,
        origin: &str,
    ) {
        // Validate remote data size (same limit as local track)
        let data_str = data.to_string();
        if data_str.len() > self.max_data_size {
            tracing::warn!(
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
            tracing::warn!(
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
        // A user is "new" only if it had no connections at all before this join.
        let user_was_new = entry.connections.is_empty();
        // Last-write-wins: only update data if newer
        if updated_at >= entry.updated_at {
            entry.data = data.clone();
            entry.updated_at = updated_at;
        }
        // Add THIS origin's sentinel so the user appears in presence queries.
        // One sentinel per (origin, user): if the same peer re-announces, do not
        // double-count; if another peer already reported the user, this adds a
        // second sentinel so the user survives the first peer leaving.
        let sentinel = remote_sentinel(origin, user_id);
        let already_present = entry.connections.iter().any(|c| c.conn_id == sentinel);
        if !already_present {
            entry.connections.push(ConnMeta {
                conn_id: sentinel,
                _joined_at: updated_at,
            });
            let stats = self
                .topic_presence_stats
                .entry(topic.to_owned())
                .or_insert_with(PresenceStats::new);
            if user_was_new {
                stats.num_users.fetch_add(1, Ordering::Relaxed);
            }
            stats.num_connections.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Merge a remote leave from a peer node. `updated_at` + `origin` enable
    /// last-write-wins (a stale/reordered leave is ignored) and per-origin
    /// sentinel removal (only the leaving peer's sentinel is dropped).
    pub fn merge_remote_leave(&self, topic: &str, user_id: &str, updated_at: u64, origin: &str) {
        if let Some(topic_map) = self.topic_presence.get(topic) {
            let mut should_remove_user = false;
            let mut removed_sentinel = false;

            if let Some(mut entry) = topic_map.get_mut(user_id) {
                // Last-write-wins: ignore a leave that predates the current state
                // (e.g. leave@t then re-join@t+1 reordered on the wire), which
                // would otherwise wrongly remove a user that just re-joined.
                if updated_at < entry.updated_at {
                    return;
                }
                // Remove ONLY this origin's sentinel; other peers' sentinels and
                // local connections remain, so the user stays present if still
                // connected elsewhere.
                let sentinel = remote_sentinel(origin, user_id);
                let before = entry.connections.len();
                entry.connections.retain(|c| c.conn_id != sentinel);
                removed_sentinel = entry.connections.len() < before;
                // If no connections remain, remove the user entirely.
                should_remove_user = entry.connections.is_empty();
                // Advance the tombstone so a later stale join cannot resurrect it.
                if removed_sentinel && updated_at > entry.updated_at {
                    entry.updated_at = updated_at;
                }
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
            tracing::warn!(
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
    pub fn merge_full_state(&self, entries_json: &str, origin: &str) {
        // Cap incoming full state to 10MB to prevent OOM
        if entries_json.len() > 10_000_000 {
            tracing::error!(
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
                self.merge_remote_join(topic, user_id, &data, updated_at, origin);
            }
        }
    }

    /// Remove every presence sentinel contributed by `origin` -- called when a
    /// cluster peer disconnects. Users left with no remaining connections are
    /// dropped, so a crashed peer (which sends no per-user leave) cannot leave
    /// "ghost" members present forever and drift membership counts upward.
    pub fn purge_origin(&self, origin: &str) {
        let prefix = remote_origin_prefix(origin);
        for topic_entry in self.topic_presence.iter() {
            let topic = topic_entry.key();
            let topic_map = topic_entry.value();
            let mut conns_removed: usize = 0;
            let mut emptied_users: Vec<String> = Vec::new();

            for mut user_entry in topic_map.iter_mut() {
                let before = user_entry.connections.len();
                user_entry
                    .connections
                    .retain(|c| !c.conn_id.starts_with(&prefix));
                let removed = before - user_entry.connections.len();
                if removed > 0 {
                    conns_removed += removed;
                    if user_entry.connections.is_empty() {
                        emptied_users.push(user_entry.key().clone());
                    }
                }
            }

            let mut users_removed: usize = 0;
            for uid in emptied_users {
                // remove_if guards against a concurrent local track() re-adding a
                // connection between the iter_mut release and this removal.
                if topic_map
                    .remove_if(&uid, |_, e| e.connections.is_empty())
                    .is_some()
                {
                    users_removed += 1;
                }
            }

            if conns_removed > 0
                && let Some(stats) = self.topic_presence_stats.get(topic)
            {
                let _ =
                    stats
                        .num_connections
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                            Some(c.saturating_sub(conns_removed))
                        });
                if users_removed > 0 {
                    let _ =
                        stats
                            .num_users
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                                Some(c.saturating_sub(users_removed))
                            });
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> serde_json::Value {
        serde_json::json!({"status": "online"})
    }

    // R1 #3: per-origin sentinels -- a leave from one peer must not erase a user
    // still present on another peer.
    #[test]
    fn per_origin_sentinel_survives_other_peers_leave() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.merge_remote_join("room", "alice", &data(), 100, "peerA");
        mgr.merge_remote_join("room", "alice", &data(), 100, "peerB");

        // peerA reports alice leaving; peerB's sentinel must remain.
        mgr.merge_remote_leave("room", "alice", 101, "peerA");

        let topic_map = mgr.topic_presence.get("room").expect("topic exists");
        let entry = topic_map.get("alice").expect("alice still present");
        assert_eq!(entry.connections.len(), 1, "only peerB sentinel remains");
        assert!(entry.connections[0].conn_id.contains("peerB"));
    }

    // R1 #2: last-write-wins -- a stale (reordered) leave must not remove a user
    // whose newer re-join already landed.
    #[test]
    fn lww_ignores_stale_leave() {
        let mgr = PresenceManager::new(4096, 0);
        // Re-join applied at t=102 before the older leave (t=101) arrives.
        mgr.merge_remote_join("room", "bob", &data(), 102, "peerA");
        mgr.merge_remote_leave("room", "bob", 101, "peerA");

        let topic_map = mgr.topic_presence.get("room").expect("topic exists");
        assert!(
            topic_map.contains_key("bob"),
            "a stale leave must not remove a user that re-joined later"
        );
    }

    // R1 #4: ghost cleanup -- purge_origin removes only the crashed peer's
    // sentinels and the users left with no connections.
    #[test]
    fn purge_origin_removes_only_that_peers_ghosts() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.merge_remote_join("room", "alice", &data(), 10, "peerA");
        mgr.merge_remote_join("room", "bob", &data(), 10, "peerB");
        mgr.merge_remote_join("room", "carol", &data(), 10, "peerA");

        mgr.purge_origin("peerA");

        let topic_map = mgr.topic_presence.get("room").expect("topic exists");
        assert!(!topic_map.contains_key("alice"), "peerA ghost removed");
        assert!(!topic_map.contains_key("carol"), "peerA ghost removed");
        assert!(topic_map.contains_key("bob"), "peerB user retained");

        let stats = mgr.topic_presence_stats.get("room").expect("stats exist");
        assert_eq!(stats.num_users.load(Ordering::Relaxed), 1);
        assert_eq!(stats.num_connections.load(Ordering::Relaxed), 1);
    }

    // A user present on two peers survives purge of one of them.
    #[test]
    fn purge_origin_keeps_user_present_on_another_peer() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.merge_remote_join("room", "dave", &data(), 10, "peerA");
        mgr.merge_remote_join("room", "dave", &data(), 10, "peerB");

        mgr.purge_origin("peerA");

        let topic_map = mgr.topic_presence.get("room").expect("topic exists");
        let entry = topic_map.get("dave").expect("dave retained");
        assert_eq!(entry.connections.len(), 1, "only peerB sentinel remains");
        assert!(entry.connections[0].conn_id.contains("peerB"));

        let stats = mgr.topic_presence_stats.get("room").expect("stats exist");
        assert_eq!(stats.num_users.load(Ordering::Relaxed), 1);
        assert_eq!(stats.num_connections.load(Ordering::Relaxed), 1);
    }
}
