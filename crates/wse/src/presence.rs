use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use std::collections::{HashMap, HashSet};

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
    /// When it joined; for a peer's sentinel, the time of that peer's latest
    /// join, which its leaves are compared against.
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
    /// The user's last connection on this node left, but other nodes still
    /// report the user: peers must hear the leave, local subscribers must
    /// not (the user is still present).
    LastLocalLeave {
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
    conn_presence_topics: DashMap<String, HashSet<String>>,
    /// Max bytes for presence data JSON
    max_data_size: usize,
    /// Max members per topic (0 = unlimited)
    max_members: usize,
}

/// A topic's member map. A few shards are plenty for one topic's members; the
/// default shard count (cores x 4) costs several KiB per topic.
fn new_topic_map() -> DashMap<String, PresenceEntry> {
    DashMap::with_shard_amount(4)
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
        // Lock order everywhere in this file: topic_presence before
        // conn_presence_topics and topic_presence_stats.
        let topic_map = self
            .topic_presence
            .entry(topic.to_owned())
            .or_insert_with(new_topic_map);

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
                    joined_at: now,
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
                    let _ = stats.num_users.try_update(
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                        |current| Some(current.saturating_sub(1)),
                    );
                }
                drop(topic_map);
                self.remove_topic_if_empty(topic);
                return UntrackResult::LastLeave {
                    user_id,
                    data: removed_entry.data,
                };
            }
            // Only other nodes' sentinels left: this node's leave must still
            // reach them, or each keeps this node's sentinel for good.
            if removed_conn
                && let Some(entry) = topic_map.get(&user_id)
                && entry
                    .connections
                    .iter()
                    .all(|c| c.conn_id.starts_with(REMOTE_PREFIX))
            {
                return UntrackResult::LastLocalLeave {
                    user_id,
                    data: entry.data.clone(),
                };
            }
        }

        UntrackResult::StillPresent
    }

    /// Drop a topic's member map and counters once nobody is present in it;
    /// otherwise every topic ever tracked keeps an empty map for the life of
    /// the process. The counters go in the same critical section, so a
    /// concurrent track() of the topic sees either both or neither. Never call
    /// this while holding a guard into `topic_presence`.
    fn remove_topic_if_empty(&self, topic: &str) {
        self.topic_presence.remove_if(topic, |_, members| {
            if members.is_empty() {
                self.topic_presence_stats.remove(topic);
                true
            } else {
                false
            }
        });
    }

    /// Untrack a connection from specific topics (called on unsubscribe).
    pub fn untrack_topics(&self, conn_id: &str, topics: &[String]) -> Vec<(String, UntrackResult)> {
        let mut results = Vec::new();
        for topic in topics {
            // Remove from conn_presence_topics tracking
            if let Some(mut conn_topics) = self.conn_presence_topics.get_mut(conn_id) {
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

        let user_id = self.conn_user_id.get(conn_id)?.value().clone();

        let now = epoch_ms();
        let mut updated_topics = Vec::new();

        // Copy the topic list out first: holding this connection's entry while
        // locking topic maps would take the two maps in the opposite order to
        // track(), and on free-threaded Python the two calls can deadlock.
        let topics: Vec<String> = match self.conn_presence_topics.get(conn_id) {
            Some(topics) => topics.iter().cloned().collect(),
            None => Vec::new(),
        };
        for topic in topics {
            if let Some(topic_map) = self.topic_presence.get(&topic)
                && let Some(mut entry) = topic_map.get_mut(&user_id)
            {
                entry.data = new_data.clone();
                entry.updated_at = now;
                updated_topics.push(topic);
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
        let mut thinned_topics: Vec<String> = Vec::new();

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
                            let _ = stats.num_connections.try_update(
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
                        let _ = stats.num_users.try_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |current| Some(current.saturating_sub(1)),
                        );
                        let _ = stats.num_connections.try_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |current| Some(current.saturating_sub(actual_conn_count)),
                        );
                    }
                    removed.push((topic.clone(), user_id, data));
                    if thinned_topics.last() != Some(&topic) {
                        thinned_topics.push(topic.clone());
                    }
                }
            }
        }

        // After the iteration: removing map entries while iterating would wait
        // on the iterator's own shard lock.
        for topic in &thinned_topics {
            self.remove_topic_if_empty(topic);
        }

        removed
    }

    // -----------------------------------------------------------------------
    // Cluster presence sync: merge remote presence state
    // -----------------------------------------------------------------------

    /// Merge a remote join from a peer node. Does NOT emit events (caller handles broadcast).
    /// `origin` is the reporting peer's address; the sentinel is keyed by it so
    /// each peer contributes its own and a leave/disconnect from one peer cannot
    /// erase a user still present on another. True when the user was not
    /// present before: only then is it a join for local subscribers.
    pub fn merge_remote_join(
        &self,
        topic: &str,
        user_id: &str,
        data: &serde_json::Value,
        updated_at: u64,
        origin: &str,
    ) -> bool {
        // Validate remote data size (same limit as local track)
        let data_str = data.to_string();
        if data_str.len() > self.max_data_size {
            tracing::warn!(
                "[WSE-Presence] Rejecting oversized remote presence data ({} bytes, max {})",
                data_str.len(),
                self.max_data_size
            );
            return false;
        }
        let topic_map = self
            .topic_presence
            .entry(topic.to_owned())
            .or_insert_with(new_topic_map);

        // Enforce max_members limit for remote entries too
        if self.max_members > 0
            && !topic_map.contains_key(user_id)
            && topic_map.len() >= self.max_members
        {
            tracing::warn!(
                "[WSE-Presence] Rejecting remote join for {user_id}: topic at capacity ({})",
                self.max_members
            );
            return false;
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
        match entry.connections.iter_mut().find(|c| c.conn_id == sentinel) {
            Some(existing) => existing.joined_at = existing.joined_at.max(updated_at),
            None => {
                entry.connections.push(ConnMeta {
                    conn_id: sentinel,
                    joined_at: updated_at,
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
        user_was_new
    }

    /// Merge a remote leave from a peer node: drop that peer's sentinel, unless
    /// the peer joined again after `updated_at` (the peer's own clock, so its
    /// joins and leaves compare; a peer's frames arrive in order on its link).
    /// Returns the user's data when the user is gone from the topic: only
    /// then is it a leave for local subscribers.
    pub fn merge_remote_leave(
        &self,
        topic: &str,
        user_id: &str,
        updated_at: u64,
        origin: &str,
    ) -> Option<serde_json::Value> {
        let topic_map = self.topic_presence.get(topic)?;
        let sentinel = remote_sentinel(origin, user_id);
        let emptied = {
            let mut entry = topic_map.get_mut(user_id)?;
            let joined_at = entry
                .connections
                .iter()
                .find(|c| c.conn_id == sentinel)?
                .joined_at;
            if updated_at < joined_at {
                return None;
            }
            entry.connections.retain(|c| c.conn_id != sentinel);
            entry.connections.is_empty()
        };
        if let Some(stats) = self.topic_presence_stats.get(topic) {
            let _ = stats
                .num_connections
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some(c.saturating_sub(1))
                });
        }
        if !emptied {
            return None;
        }
        // remove_if: track() may have added a local connection meanwhile.
        let (_, removed) = topic_map.remove_if(user_id, |_, entry| entry.connections.is_empty())?;
        if let Some(stats) = self.topic_presence_stats.get(topic) {
            let _ = stats
                .num_users
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some(c.saturating_sub(1))
                });
        }
        drop(topic_map);
        self.remove_topic_if_empty(topic);
        Some(removed.data)
    }

    /// Merge a remote data update (last-write-wins).
    /// True when the update was newer and applied.
    pub fn merge_remote_update(
        &self,
        topic: &str,
        user_id: &str,
        data: &serde_json::Value,
        updated_at: u64,
    ) -> bool {
        // Validate remote data size (same limit as local track)
        let data_str = data.to_string();
        if data_str.len() > self.max_data_size {
            tracing::warn!(
                "[WSE-Presence] Rejecting oversized remote presence update ({} bytes, max {})",
                data_str.len(),
                self.max_data_size
            );
            return false;
        }
        if let Some(topic_map) = self.topic_presence.get(topic)
            && let Some(mut entry) = topic_map.get_mut(user_id)
            && updated_at >= entry.updated_at
        {
            entry.data = data.clone();
            entry.updated_at = updated_at;
            return true;
        }
        false
    }

    /// Merge full presence state from a peer, as it comes (older nodes send
    /// it so on connect).
    /// Expected JSON format: {"topic": {"user_id": {"data": {...}, "updated_at": 123}}}
    /// Returns (topic, user_id, data) of the users who were not present before.
    pub fn merge_full_state(
        &self,
        entries_json: &str,
        origin: &str,
    ) -> Vec<(String, String, serde_json::Value)> {
        self.merge_entries(entries_json, origin, None)
    }

    /// Merge one frame of a sync round from `origin` like a full state, and
    /// note each user it holds, per topic, in `held` for the round's end
    /// (`retain_origin`).
    pub fn merge_round_frame(
        &self,
        entries_json: &str,
        origin: &str,
        held: &mut HashMap<String, HashSet<String>>,
    ) -> Vec<(String, String, serde_json::Value)> {
        self.merge_entries(entries_json, origin, Some(held))
    }

    fn merge_entries(
        &self,
        entries_json: &str,
        origin: &str,
        mut held: Option<&mut HashMap<String, HashSet<String>>>,
    ) -> Vec<(String, String, serde_json::Value)> {
        let mut joined = Vec::new();
        // Cap incoming full state to 10MB to prevent OOM
        if entries_json.len() > 10_000_000 {
            tracing::error!(
                "[Presence] Full state too large ({} bytes, max 10MB), rejecting",
                entries_json.len()
            );
            return joined;
        }
        let parsed: serde_json::Value = match serde_json::from_str(entries_json) {
            Ok(v) => v,
            Err(_) => return joined,
        };
        let topics = match parsed.as_object() {
            Some(t) => t,
            None => return joined,
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
                if let Some(held) = held.as_deref_mut() {
                    held.entry(topic.clone())
                        .or_default()
                        .insert(user_id.clone());
                }
                if self.merge_remote_join(topic, user_id, &data, updated_at, origin) {
                    joined.push((topic.clone(), user_id.clone(), data));
                }
            }
        }
        joined
    }

    /// Remove every presence sentinel contributed by `origin` -- called when a
    /// cluster peer disconnects. Users left with no remaining connections are
    /// dropped, so a crashed peer (which sends no per-user leave) cannot leave
    /// "ghost" members present forever and drift membership counts upward.
    /// Returns (topic, user_id, data) of the users gone with it.
    pub fn purge_origin(&self, origin: &str) -> Vec<(String, String, serde_json::Value)> {
        self.drop_origin_where(origin, |_, _| true)
    }

    /// The end of a sync round from `origin`: drop its sentinels of every
    /// user the round did not hold (`held`, per topic), so a leave lost on
    /// the way leaves no ghost. Returns (topic, user_id, data) of the users
    /// gone with them.
    pub fn retain_origin(
        &self,
        origin: &str,
        held: &HashMap<String, HashSet<String>>,
    ) -> Vec<(String, String, serde_json::Value)> {
        self.drop_origin_where(origin, |topic, user| {
            !held.get(topic).is_some_and(|users| users.contains(user))
        })
    }

    /// Drop the sentinels of `origin` for the users `gone(topic, user)`
    /// names; users left with no connection are removed and returned.
    fn drop_origin_where(
        &self,
        origin: &str,
        gone_user: impl Fn(&str, &str) -> bool,
    ) -> Vec<(String, String, serde_json::Value)> {
        let prefix = remote_origin_prefix(origin);
        let mut thinned_topics: Vec<String> = Vec::new();
        let mut gone = Vec::new();
        for topic_entry in self.topic_presence.iter() {
            let topic = topic_entry.key();
            let topic_map = topic_entry.value();
            let mut conns_removed: usize = 0;
            let mut emptied_users: Vec<String> = Vec::new();

            for mut user_entry in topic_map.iter_mut() {
                if !gone_user(topic, user_entry.key()) {
                    continue;
                }
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
                if let Some((_, entry)) = topic_map.remove_if(&uid, |_, e| e.connections.is_empty())
                {
                    users_removed += 1;
                    gone.push((topic.clone(), uid, entry.data));
                }
            }

            if conns_removed > 0
                && let Some(stats) = self.topic_presence_stats.get(topic)
            {
                let _ =
                    stats
                        .num_connections
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                            Some(c.saturating_sub(conns_removed))
                        });
                if users_removed > 0 {
                    let _ = stats
                        .num_users
                        .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                            Some(c.saturating_sub(users_removed))
                        });
                }
            }
            if users_removed > 0 {
                thinned_topics.push(topic.clone());
            }
        }
        for topic in &thinned_topics {
            self.remove_topic_if_empty(topic);
        }
        gone
    }

    /// Serialize local presence state into one or more JSON chunks, each whose
    /// serialized length stays within `max_payload_bytes`. Only entries with a
    /// local connection are included (remote sentinels are not re-synced).
    ///
    /// The receiver merges each chunk additively (per-user via merge_remote_join),
    /// so several partial chunks reconstruct the same set as a single frame would;
    /// sent as a sync round, the round's end also drops what none of its chunks
    /// held. One frame for the whole state would be dropped once a busy node's
    /// state passed the cluster frame limit, leaving a joining peer blind.
    pub fn serialize_full_state_chunked(&self, max_payload_bytes: usize) -> Vec<String> {
        let mut chunks: Vec<String> = Vec::new();
        let mut current = serde_json::Map::new();
        let mut est: usize = 2; // running estimate of current's serialized len ("{}")

        for topic_entry in self.topic_presence.iter() {
            let topic = topic_entry.key();
            let topic_map = topic_entry.value();
            for user_entry in topic_map.iter() {
                let entry = user_entry.value();
                // Only local entries (don't re-sync remote sentinels).
                if !entry
                    .connections
                    .iter()
                    .any(|c| !c.conn_id.starts_with(REMOTE_PREFIX))
                {
                    continue;
                }
                let value = serde_json::json!({
                    "data": entry.data,
                    "updated_at": entry.updated_at,
                });
                // Conservative per-user contribution: topic key + user key + value
                // + JSON punctuation/quotes/nested braces.
                let add_est = topic.len() + entry.user_id.len() + value.to_string().len() + 24;

                // Flush the current chunk before it would exceed the budget (but
                // never flush an empty chunk -- a single oversized user still goes
                // out alone and is handled by encode_presence_full's own guard).
                if est + add_est > max_payload_bytes && !current.is_empty() {
                    if let Ok(s) = serde_json::to_string(&serde_json::Value::Object(
                        std::mem::take(&mut current),
                    )) {
                        chunks.push(s);
                    }
                    est = 2;
                }

                if let Some(obj) = current
                    .entry(topic.clone())
                    .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                    .as_object_mut()
                {
                    obj.insert(entry.user_id.clone(), value);
                }
                est += add_est;
            }
        }

        if !current.is_empty()
            && let Ok(s) = serde_json::to_string(&serde_json::Value::Object(current))
        {
            chunks.push(s);
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> serde_json::Value {
        serde_json::json!({"status": "online"})
    }

    fn members(mgr: &PresenceManager, topic: &str) -> Vec<String> {
        let mut users: Vec<String> = mgr.query(topic).into_iter().map(|m| m.0).collect();
        users.sort();
        users
    }

    // The user's last tab on this node closes while another node still has
    // one: peers must hear this node's leave, local subscribers must not.
    #[test]
    fn the_last_local_leave_is_told_to_peers_only() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.register_connection("c1", "alice");
        assert!(matches!(
            mgr.track("c1", "room", &data()),
            TrackResult::FirstJoin { .. }
        ));
        assert!(!mgr.merge_remote_join("room", "alice", &data(), 100, "peerB"));
        let results = mgr.remove_connection("c1");
        assert!(matches!(results[0].1, UntrackResult::LastLocalLeave { .. }));
        assert_eq!(members(&mgr, "room"), ["alice"]);
        // Then peerB's leave: now the user is gone, and that is the event.
        assert_eq!(
            mgr.merge_remote_leave("room", "alice", 200, "peerB"),
            Some(data())
        );
        assert!(members(&mgr, "room").is_empty());
    }

    // A peer's leave is compared with that peer's own join, not with the
    // newest write from anywhere (another node's clock, a local update).
    #[test]
    fn a_remote_leave_is_judged_by_its_own_peers_join() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.merge_remote_join("room", "alice", &data(), 100, "peerB");
        // A newer write from elsewhere moves the entry's own timestamp on.
        mgr.merge_remote_join("room", "alice", &data(), 500, "peerC");
        assert_eq!(mgr.merge_remote_leave("room", "alice", 150, "peerB"), None);
        let entry_conns = mgr
            .topic_presence
            .get("room")
            .unwrap()
            .get("alice")
            .unwrap()
            .connections
            .len();
        assert_eq!(entry_conns, 1, "peerB's sentinel is gone, peerC's stays");
        // A leave older than the peer's latest join is stale.
        mgr.merge_remote_join("room", "bob", &data(), 300, "peerB");
        assert_eq!(mgr.merge_remote_leave("room", "bob", 250, "peerB"), None);
        assert_eq!(members(&mgr, "room"), ["alice", "bob"]);
    }

    #[test]
    fn remote_merges_report_what_changed() {
        let mgr = PresenceManager::new(4096, 0);
        assert!(mgr.merge_remote_join("room", "alice", &data(), 100, "peerB"));
        assert!(!mgr.merge_remote_join("room", "alice", &data(), 101, "peerC"));
        assert!(mgr.merge_remote_update("room", "alice", &serde_json::json!({"s": 1}), 200));
        assert!(!mgr.merge_remote_update("room", "alice", &serde_json::json!({"s": 0}), 150));
        let full = r#"{"room": {"alice": {"data": {}, "updated_at": 1}, "carol": {"data": {}, "updated_at": 1}}}"#;
        let joined = mgr.merge_full_state(full, "peerD");
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].1, "carol");
        let gone = mgr.purge_origin("peerD");
        assert_eq!(
            gone.iter().map(|g| g.1.as_str()).collect::<Vec<_>>(),
            ["carol"]
        );
        assert_eq!(members(&mgr, "room"), ["alice"]);
    }

    // Per-origin sentinels: a leave from one peer must not erase a user
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

    // Last write wins: a stale (reordered) leave must not remove a user
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

    // Ghost cleanup: purge_origin removes only the crashed peer's
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

    // A sync round keeps of its origin exactly what it held: a leave lost on
    // the way leaves no ghost, and a user also on another peer stays.
    #[test]
    fn a_sync_round_drops_what_it_did_not_hold() {
        let mgr = PresenceManager::new(4096, 0);
        for user in ["alice", "bob", "dave"] {
            mgr.merge_remote_join("room", user, &data(), 10, "peerA");
        }
        mgr.merge_remote_join("lobby", "carol", &data(), 10, "peerA");
        mgr.merge_remote_join("room", "dave", &data(), 10, "peerB");

        // peerA's round in two frames: alice, and erin who is new.
        let mut held = HashMap::new();
        let first = r#"{"room": {"alice": {"data": {}, "updated_at": 20}}}"#;
        assert!(mgr.merge_round_frame(first, "peerA", &mut held).is_empty());
        let last = r#"{"room": {"erin": {"data": {}, "updated_at": 20}}}"#;
        let joined = mgr.merge_round_frame(last, "peerA", &mut held);
        assert_eq!(
            joined.iter().map(|j| j.1.as_str()).collect::<Vec<_>>(),
            ["erin"]
        );

        let mut gone: Vec<String> = mgr
            .retain_origin("peerA", &held)
            .into_iter()
            .map(|(topic, user, _)| format!("{topic}/{user}"))
            .collect();
        gone.sort();
        assert_eq!(gone, ["lobby/carol", "room/bob"]);
        assert_eq!(members(&mgr, "room"), ["alice", "dave", "erin"]);
        let room = mgr.topic_presence.get("room").expect("room");
        let dave = room.get("dave").expect("dave stays on peerB");
        assert_eq!(dave.connections.len(), 1);
        assert!(dave.connections[0].conn_id.contains("peerB"));
        assert!(mgr.topic_presence.get("lobby").is_none());
    }

    // Full-state sync is chunked, so a node with more presence than fits in one
    // frame still sends all of it.
    #[test]
    fn full_state_chunked_covers_all_users_and_splits() {
        let mgr = PresenceManager::new(4096, 0);
        let d = data();
        for i in 0..6 {
            let conn = format!("conn{i}");
            let uid = format!("user{i}");
            mgr.register_connection(&conn, &uid);
            mgr.track(&conn, "room", &d);
        }

        // Huge limit -> a single chunk with all six users.
        let one = mgr.serialize_full_state_chunked(1_000_000);
        assert_eq!(one.len(), 1, "everything fits in one chunk");
        let parsed: serde_json::Value = serde_json::from_str(&one[0]).expect("valid JSON");
        assert_eq!(parsed["room"].as_object().expect("room obj").len(), 6);

        // Tiny limit -> multiple chunks whose union still covers all six users,
        // each chunk independently valid JSON the receiver can merge additively.
        let many = mgr.serialize_full_state_chunked(60);
        assert!(many.len() > 1, "a small budget splits into multiple chunks");
        let mut seen = std::collections::HashSet::new();
        for chunk in &many {
            let v: serde_json::Value =
                serde_json::from_str(chunk).expect("each chunk is valid JSON");
            for users in v.as_object().expect("obj").values() {
                for uid in users.as_object().expect("users obj").keys() {
                    seen.insert(uid.clone());
                }
            }
        }
        assert_eq!(seen.len(), 6, "every user appears in some chunk");
    }

    #[test]
    fn empty_topics_are_dropped_with_their_counters() {
        let mgr = PresenceManager::new(4096, 0);
        mgr.register_connection("c1", "u1");
        mgr.track("c1", "room:1", &data());
        mgr.track("c1", "room:2", &data());
        assert_eq!(mgr.total_topics(), 2);
        mgr.untrack_topics("c1", &["room:1".to_string()]);
        assert_eq!(mgr.total_topics(), 1);
        mgr.remove_connection("c1");
        assert_eq!(mgr.total_topics(), 0);
        assert_eq!(mgr.stats("room:2"), (0, 0));
        assert!(mgr.topic_presence_stats.is_empty());
    }

    #[test]
    fn track_and_update_data_do_not_deadlock() {
        use std::sync::{Arc, mpsc};
        use std::time::Duration;

        let mgr = Arc::new(PresenceManager::new(4096, 0));
        mgr.register_connection("c1", "u1");
        let (done_tx, done_rx) = mpsc::channel();
        let workers: Vec<_> = (0..4)
            .map(|i| {
                let mgr = Arc::clone(&mgr);
                let done_tx = done_tx.clone();
                std::thread::spawn(move || {
                    for n in 0..2000 {
                        if i % 2 == 0 {
                            mgr.track("c1", &format!("room:{}", n % 8), &data());
                        } else {
                            mgr.update_data("c1", &data());
                        }
                    }
                    done_tx.send(()).unwrap();
                })
            })
            .collect();
        for _ in 0..4 {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("track() and update_data() deadlocked");
        }
        for worker in workers {
            worker.join().unwrap();
        }
    }
}
