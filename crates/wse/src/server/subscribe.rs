//! Subscribing connections to topics: the checks a request passes, the topic
//! index and cluster interest, the recovery of missed messages, and the
//! server's own answer to the clients' subscription requests. The Python API
//! and the connection task both come through here.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::control::TopicPosition;
use super::handler::Presence;
use super::{
    ConnectionHandle, QueueGroup, ServerCommand, SharedState, format_presence_msg, is_pattern,
};
use crate::cluster::ClusterCommand;
use crate::presence::{TrackResult, UntrackResult};
use crate::recovery::RecoveryResult;

/// Longest topic name accepted, in bytes.
pub(crate) const MAX_TOPIC_LEN: usize = 256;
/// Pattern subscriptions (names with `*` or `?`) one connection may hold.
pub(crate) const MAX_PATTERNS_PER_CONNECTION: usize = 16;
/// Topics one `subscription_update` of a client may ask for; the rest fail.
pub(crate) const MAX_TOPICS_PER_REQUEST: usize = 1024;
/// Topics a client may hold through its own requests when the server sets no
/// `max_subscriptions_per_connection`: the application may subscribe it to
/// more, a client alone not.
pub(crate) const CLIENT_SUBSCRIPTION_LIMIT: usize = 1024;
/// Streams of one topic a client may name in a recovery request: more than
/// its clients remember.
pub(crate) const MAX_STREAMS_PER_TOPIC: usize = 64;

/// What a recovery did for one topic, in the terms the clients read: the
/// topic's epoch and an offset (inclusive: the last one the client has, or
/// may take as had), whether the gap was filled, and how many messages were
/// replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopicRecovery {
    pub(crate) epoch: Option<u32>,
    pub(crate) offset: u64,
    pub(crate) recovered: bool,
    pub(crate) recoverable: bool,
    pub(crate) count: usize,
    /// Each stream of the topic this node held when the connection
    /// subscribed (epoch, last offset): where the client starts in it.
    pub(crate) streams: Vec<(u32, u64)>,
}

/// A topic's head and each of its streams' last offset, at one moment.
type Heads = (Option<(u32, u64)>, Vec<(u32, u64)>);

impl TopicRecovery {
    /// A topic with no position to report.
    pub(crate) const UNKNOWN: Self = Self {
        epoch: None,
        offset: 0,
        recovered: false,
        recoverable: false,
        count: 0,
        streams: Vec::new(),
    };

    pub(crate) fn epoch_hex(&self) -> Option<String> {
        self.epoch.map(|epoch| format!("{epoch:08x}"))
    }

    /// The streams as the clients read them: epoch (8 hex digits) -> offset.
    pub(crate) fn streams_json(&self) -> serde_json::Value {
        self.streams
            .iter()
            .map(|(epoch, offset)| (format!("{epoch:08x}"), serde_json::Value::from(*offset)))
            .collect::<serde_json::Map<_, _>>()
            .into()
    }
}

impl SharedState {
    /// The topics of `requested` that `conn_id` may take: allowed by its topic
    /// ACL, valid names (not empty, at most `MAX_TOPIC_LEN` bytes, no ASCII
    /// control characters: the cluster RESYNC newline-delimits topics), within
    /// its pattern budget and `max_subscriptions_per_connection`, for a
    /// plain subscription or a membership of `queue_group`.
    /// `Err(limit)` when the connection holds that many already.
    pub(crate) fn admit_topics(
        &self,
        conn_id: &str,
        requested: Vec<String>,
        queue_group: Option<&str>,
    ) -> Result<Vec<String>, usize> {
        let mut topics: Vec<String> = match self.topics.acls.get(conn_id) {
            Some(acl) => requested
                .into_iter()
                .filter(|t| acl.is_allowed(t))
                .collect(),
            None => requested,
        };
        let asked = topics.len();
        topics.retain(|t| {
            !t.is_empty() && t.len() <= MAX_TOPIC_LEN && !t.bytes().any(|b| b.is_ascii_control())
        });
        if topics.len() < asked {
            tracing::warn!(
                "[WSE] Refusing {} invalid topic name(s) (conn {conn_id})",
                asked - topics.len()
            );
        }
        // Patterns cost every publish a match: a few per connection.
        let held = self.conn_topics.get(conn_id);
        let mut patterns = held
            .as_ref()
            .map_or(0, |held| held.iter().filter(|t| is_pattern(t)).count());
        let mut over = 0usize;
        topics.retain(|t| {
            if !is_pattern(t) || held.as_ref().is_some_and(|held| held.contains(t)) {
                return true;
            }
            patterns += 1;
            if patterns > MAX_PATTERNS_PER_CONNECTION {
                over += 1;
                return false;
            }
            true
        });
        if over > 0 {
            tracing::warn!(
                "[WSE] Refusing {over} pattern(s): {MAX_PATTERNS_PER_CONNECTION} per connection (conn {conn_id})"
            );
        }
        let max_subs = self.max_subscriptions_per_connection;
        if max_subs > 0 {
            // Only a membership the connection does not hold yet takes
            // room: the same one again (a held topic, to recover it; or the
            // same queue group) does not, a new kind of it does.
            let memberships = self.conn_queue_groups.get(conn_id);
            let current = held.as_ref().map_or(0, |held| held.len())
                + memberships.as_ref().map_or(0, |v| v.len());
            let held_already = |t: &String| match queue_group {
                None => held.as_ref().is_some_and(|held| held.contains(t)),
                Some(group) => memberships
                    .as_ref()
                    .is_some_and(|v| v.iter().any(|(topic, g)| topic == t && g == group)),
            };
            let mut room = max_subs.saturating_sub(current);
            let mut refused = false;
            topics.retain(|t| {
                if held_already(t) {
                    return true;
                }
                if room == 0 {
                    refused = true;
                    return false;
                }
                room -= 1;
                true
            });
            if refused && topics.is_empty() {
                return Err(max_subs);
            }
        }
        drop(held);
        Ok(topics)
    }

    /// Subscribe `conn_id` to `topics` (already admitted): to fan-out, or as a
    /// member of `queue_group`. Both count as cluster interest.
    pub(crate) fn subscribe_topics(
        &self,
        conn_id: &str,
        handle: &ConnectionHandle,
        topics: &[String],
        queue_group: Option<&str>,
    ) {
        let mut newly_subscribed: Vec<String> = Vec::new();
        for topic in topics {
            match queue_group {
                Some(group_name) => {
                    {
                        let topic_groups = self.queue_groups.entry(topic.clone()).or_default();
                        let mut group =
                            topic_groups
                                .entry(group_name.to_owned())
                                .or_insert_with(|| QueueGroup {
                                    members: Vec::new(),
                                    next: AtomicUsize::new(0),
                                });
                        if !group.members.iter().any(|(id, _)| id == conn_id) {
                            group.members.push((conn_id.to_owned(), handle.clone()));
                        }
                    }
                    let mut memberships = self
                        .conn_queue_groups
                        .entry(conn_id.to_owned())
                        .or_default();
                    let pair = (topic.clone(), group_name.to_owned());
                    if !memberships.contains(&pair) {
                        memberships.push(pair);
                        newly_subscribed.push(topic.clone());
                    }
                }
                None => {
                    self.topics.subscribe(topic, conn_id, handle);
                    let is_new = self
                        .conn_topics
                        .entry(conn_id.to_owned())
                        .or_default()
                        .insert(topic.clone());
                    if is_new {
                        newly_subscribed.push(topic.clone());
                    }
                }
            }
        }
        self.add_local_interest(&newly_subscribed);
    }

    /// Unsubscribe `conn_id` from `topics` (all of them for `None`), its
    /// queue-group memberships of those topics included; presence leaves are
    /// announced. Returns the fan-out topics it left.
    pub(crate) fn unsubscribe_topics(
        &self,
        conn_id: &str,
        topics: Option<&[String]>,
    ) -> Vec<String> {
        let removed: Vec<String> = match topics {
            Some(topics) => {
                let mut removed = Vec::new();
                if let Some(held) = self.conn_topics.get(conn_id) {
                    for topic in topics {
                        if held.remove(topic).is_some() {
                            self.topics.unsubscribe(topic, conn_id);
                            removed.push(topic.clone());
                        }
                    }
                }
                self.conn_topics
                    .remove_if(conn_id, |_, held| held.is_empty());
                removed
            }
            None => self
                .conn_topics
                .remove(conn_id)
                .map(|(_, held)| {
                    held.iter()
                        .map(|topic| {
                            self.topics.unsubscribe(&topic, conn_id);
                            topic.clone()
                        })
                        .collect()
                })
                .unwrap_or_default(),
        };

        let mut left_groups: Vec<String> = Vec::new();
        if let Some(mut memberships) = self.conn_queue_groups.get_mut(conn_id) {
            memberships.retain(|(topic, group_name)| {
                if topics.is_some_and(|topics| !topics.contains(topic)) {
                    return true;
                }
                if let Some(topic_groups) = self.queue_groups.get(topic) {
                    if let Some(mut group) = topic_groups.get_mut(group_name) {
                        group.members.retain(|(id, _)| id != conn_id);
                    }
                    topic_groups.remove_if(group_name, |_, g| g.members.is_empty());
                }
                self.queue_groups
                    .remove_if(topic, |_, groups| groups.is_empty());
                left_groups.push(topic.clone());
                false
            });
        }
        self.conn_queue_groups
            .remove_if(conn_id, |_, v| v.is_empty());
        self.remove_local_interest(&left_groups);

        if let Some(ref pm) = self.presence {
            // Present on a topic it held through a queue group too.
            if left_groups.iter().all(|topic| removed.contains(topic)) {
                self.announce_untracked(pm.untrack_topics(conn_id, &removed));
            } else {
                let mut left = removed.clone();
                for topic in &left_groups {
                    if !left.contains(topic) {
                        left.push(topic.clone());
                    }
                }
                self.announce_untracked(pm.untrack_topics(conn_id, &left));
            }
        }
        self.remove_local_interest(&removed);
        removed
    }

    /// Announce what removing a connection from presence changed.
    pub(crate) fn announce_untracked(&self, results: Vec<(String, UntrackResult)>) {
        for (topic, result) in results {
            match result {
                UntrackResult::LastLeave { user_id, data } => {
                    self.announce_presence_leave(topic, user_id, data, true);
                }
                UntrackResult::LastLocalLeave { user_id, data } => {
                    self.announce_presence_leave(topic, user_id, data, false);
                }
                UntrackResult::StillPresent | UntrackResult::NotTracked => {}
            }
        }
    }

    /// A user's last connection on this node left `topic`: the cluster is
    /// told; with `local`, the user is gone everywhere and the application
    /// (drain mode) and the topic's subscribers are told too.
    pub(crate) fn announce_presence_leave(
        &self,
        topic: String,
        user_id: String,
        data: serde_json::Value,
        local: bool,
    ) {
        if local {
            self.tell_presence_leave(&topic, &user_id, &data);
        }
        let cluster_tx = self.cluster_cmd_tx.load_full();
        if let Some(tx) = cluster_tx {
            let _ = tx.send(ClusterCommand::PresenceUpdate {
                topic,
                user_id,
                action: 1, // leave
                data: serde_json::to_string(&data).unwrap_or_default(),
                updated_at: crate::presence::epoch_ms(),
            });
        }
    }

    /// Tell the running server's handler of a presence change.
    pub(crate) fn tell_presence(&self, event: Presence) {
        if let Some(events) = &*self.presence_events.load() {
            events(event);
        }
    }

    /// Track `conn_id`'s presence on `topics` with `data`: a user's first
    /// connection on a topic is told to the application, to the topic's
    /// subscribers (`presence_join`) and to the cluster.
    pub(crate) fn track_presence(
        &self,
        conn_id: &str,
        topics: &[String],
        data: &serde_json::Value,
    ) {
        let Some(pm) = &self.presence else {
            return;
        };
        let cluster_tx = self.cluster_cmd_tx.load_full();
        for topic in topics {
            // The user's first connection: new here, or present on another
            // node already, when only the peers hear of it.
            let (user_id, join_data, new) = match pm.track(conn_id, topic, data) {
                TrackResult::FirstJoin { user_id, data } => (user_id, data, true),
                TrackResult::FirstLocalJoin { user_id, data } => (user_id, data, false),
                _ => continue,
            };
            if new {
                self.tell_presence(Presence::Join {
                    topic: topic.clone(),
                    user_id: user_id.clone(),
                    data: join_data.clone(),
                });
                if let Some(tx) = &*self.commands.load() {
                    let _ = tx.send(ServerCommand::BroadcastLocal {
                        topic: topic.clone(),
                        data: format_presence_msg("presence_join", &user_id, &join_data),
                        skip_recovery: true,
                        publish: false,
                    });
                }
            }
            if let Some(ref ctx) = cluster_tx {
                let _ = ctx.send(ClusterCommand::PresenceUpdate {
                    topic: topic.clone(),
                    user_id: user_id.clone(),
                    action: 0, // join
                    data: serde_json::to_string(&join_data).unwrap_or_default(),
                    updated_at: crate::presence::epoch_ms(),
                });
            }
        }
    }

    /// New presence data for `conn_id`'s user: every topic it is present on
    /// hears `presence_update`, and so does the cluster. False over the size
    /// limit, or for a connection without presence.
    pub(crate) fn update_presence(&self, conn_id: &str, data: &serde_json::Value) -> bool {
        let Some(pm) = &self.presence else {
            return true;
        };
        let Some((user_id, topics)) = pm.update_data(conn_id, data) else {
            return false;
        };
        if let Some(tx) = &*self.commands.load() {
            let msg = format_presence_msg("presence_update", &user_id, data);
            for topic in &topics {
                let _ = tx.send(ServerCommand::BroadcastLocal {
                    topic: topic.clone(),
                    data: msg.clone(),
                    skip_recovery: true,
                    publish: false,
                });
            }
        }
        if let Some(ctx) = self.cluster_cmd_tx.load_full() {
            let data = serde_json::to_string(data).unwrap_or_default();
            let now = crate::presence::epoch_ms();
            for topic in topics {
                let _ = ctx.send(ClusterCommand::PresenceUpdate {
                    topic,
                    user_id: user_id.clone(),
                    action: 2, // update
                    data: data.clone(),
                    updated_at: now,
                });
            }
        }
        true
    }

    /// Tell the application and the topic's subscribers that a user left
    /// `topic`.
    pub(crate) fn tell_presence_leave(&self, topic: &str, user_id: &str, data: &serde_json::Value) {
        self.tell_presence(Presence::Leave {
            topic: topic.to_owned(),
            user_id: user_id.to_owned(),
            data: data.clone(),
        });
        if let Some(tx) = &*self.commands.load() {
            let _ = tx.send(ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data: format_presence_msg("presence_leave", user_id, data),
                skip_recovery: true,
                publish: false,
            });
        }
    }

    /// The (epoch, head offset) of `topic`'s recovery buffer, if it has one.
    /// Taken before a connection subscribes: every offset from the head on
    /// then reaches the connection live.
    pub(crate) fn topic_head(&self, topic: &str) -> Option<(u32, u64)> {
        self.recovery.as_ref().and_then(|r| r.get_position(topic))
    }

    /// Replay to `handle` the messages of `topic` the client missed after
    /// `position` (its last message, and from 3.1 its offset in each stream),
    /// queued before anything sent to the connection after this call.
    /// `head_before` is the topic's head taken once the connection
    /// subscribed, `streams` every stream's then.
    ///
    /// When nothing is recovered, the reported position is the last offset
    /// before the subscription: what follows reaches the client live, so a
    /// client that takes the position as its own skips nothing it will get.
    /// A replay cut short by the connection's outbound budget reports the
    /// last offset it queued, not recovered.
    pub(crate) fn recover_topic(
        &self,
        handle: &ConnectionHandle,
        topic: &str,
        position: Option<&TopicPosition>,
        head_before: Option<(u32, u64)>,
        streams: Vec<(u32, u64)>,
    ) -> TopicRecovery {
        let Some(ref recovery) = self.recovery else {
            return TopicRecovery::UNKNOWN;
        };
        let not_recovered = match head_before {
            Some((epoch, head)) if head > 0 => TopicRecovery {
                epoch: Some(epoch),
                offset: head - 1,
                recovered: false,
                recoverable: true,
                count: 0,
                streams: streams.clone(),
            },
            _ => TopicRecovery {
                streams: streams.clone(),
                ..TopicRecovery::UNKNOWN
            },
        };
        let Some(position) = position else {
            return not_recovered;
        };
        let (epoch, after) = (position.epoch, position.offset);
        let result = match &position.streams {
            Some(named) if named.len() <= MAX_STREAMS_PER_TOPIC => {
                recovery.recover_streams(topic, (epoch, after), named)
            }
            Some(_) => RecoveryResult::NotRecovered {
                epoch,
                offset: after,
            },
            None => recovery.recover(topic, epoch, after),
        };
        match result {
            RecoveryResult::Recovered {
                publications,
                epoch,
                offset,
            } => {
                let total = publications.len();
                let mut queued = 0;
                for payload in publications {
                    let Ok(text) = Utf8Bytes::try_from(payload) else {
                        break;
                    };
                    if !handle.send_one(
                        Message::Text(text),
                        self.max_outbound_queue_bytes,
                        self.slow_consumer_drops.shard(usize::from(handle.lane)),
                    ) {
                        break;
                    }
                    queued += 1;
                }
                TopicRecovery {
                    epoch: Some(epoch),
                    offset: if queued == total {
                        offset
                    } else {
                        after + queued as u64
                    },
                    recovered: queued == total,
                    recoverable: true,
                    count: queued,
                    streams,
                }
            }
            RecoveryResult::NotRecovered { epoch, offset } => {
                tracing::debug!(
                    "[WSE] {topic}: not recovered from {after} (epoch now {epoch:08x}, head {offset})"
                );
                not_recovered
            }
            RecoveryResult::NoHistory => not_recovered,
        }
    }

    /// Subscribe a connection the way a recovering client asks: admit the
    /// topics, subscribe, take each one's head, then replay each from its
    /// position in `anchors` up to that head. Every later message reaches the
    /// connection live: it is stored after the head was read, so after the
    /// subscription, and the lanes deliver it after it is stored. A message
    /// may come both ways; the client drops the second copy by its offset.
    /// Returns the topics taken (in order) and what recovery did for each.
    pub(crate) fn subscribe_and_recover(
        &self,
        conn_id: &str,
        handle: &ConnectionHandle,
        requested: Vec<String>,
        positions: &HashMap<String, TopicPosition>,
    ) -> Result<(Vec<String>, HashMap<String, TopicRecovery>), usize> {
        self.subscribe_and_recover_with(conn_id, handle, requested, positions, || {})
    }

    /// [`Self::subscribe_and_recover`], calling `subscribed` between the
    /// subscription and the reading of the heads (where a publish may land).
    pub(crate) fn subscribe_and_recover_with(
        &self,
        conn_id: &str,
        handle: &ConnectionHandle,
        requested: Vec<String>,
        positions: &HashMap<String, TopicPosition>,
        subscribed: impl FnOnce(),
    ) -> Result<(Vec<String>, HashMap<String, TopicRecovery>), usize> {
        let accepted = self.admit_topics(conn_id, requested, None)?;
        let streams_of = |t: &str| {
            self.recovery
                .as_ref()
                .map(|r| r.streams(t))
                .unwrap_or_default()
        };
        // Where the topics taken up without a position stood just before the
        // subscription: what was published after it is the client's.
        let before: Vec<_> = accepted
            .iter()
            .map(|t| (!positions.contains_key(t)).then(|| (self.topic_head(t), streams_of(t))))
            .collect();
        self.subscribe_topics(conn_id, handle, &accepted, None);
        subscribed();
        // Each topic's head and streams, read now that it is subscribed.
        let heads: Vec<_> = accepted
            .iter()
            .map(|t| (self.topic_head(t), streams_of(t)))
            .collect();
        let recovered = accepted
            .iter()
            .zip(heads)
            .zip(before)
            .map(|((topic, (head, streams)), before)| {
                let outcome = match before {
                    None => self.recover_topic(handle, topic, positions.get(topic), head, streams),
                    Some(before) => self.start_topic(handle, topic, before, (head, streams)),
                };
                (topic.clone(), outcome)
            })
            .collect();
        Ok((accepted, recovered))
    }

    /// A topic taken up without a position: the client starts where the
    /// reply says. What was published between `before` (the topic's head
    /// and streams just before the subscription) and `after` (just after)
    /// reaches the live queue only when published after the subscription,
    /// and the reply goes out ahead of the broadcasts: so it is replayed
    /// ahead of the reply, and the client starts at `after`. When it cannot
    /// be replayed, the client starts at `before`, and a message missing
    /// from what comes live reads as a gap.
    fn start_topic(
        &self,
        handle: &ConnectionHandle,
        topic: &str,
        before: Heads,
        after: Heads,
    ) -> TopicRecovery {
        let Some(ref recovery) = self.recovery else {
            return TopicRecovery::UNKNOWN;
        };
        let starting = |(head, streams): Heads| match head {
            Some((epoch, head)) if head > 0 => TopicRecovery {
                epoch: Some(epoch),
                offset: head - 1,
                recovered: false,
                recoverable: true,
                count: 0,
                streams,
            },
            _ => TopicRecovery {
                streams,
                ..TopicRecovery::UNKNOWN
            },
        };
        if before.1 == after.1 {
            return starting(after);
        }
        let Some(publications) = recovery.published_since(topic, &before.1) else {
            return starting(before);
        };
        for payload in publications {
            let Ok(text) = Utf8Bytes::try_from(payload) else {
                return starting(before);
            };
            if !handle.send_one(
                Message::Text(text),
                self.max_outbound_queue_bytes,
                self.slow_consumer_drops.shard(usize::from(handle.lane)),
            ) {
                return starting(before);
            }
        }
        starting(after)
    }
}

/// The compression a client asks for, in `client_hello` (`features.compression`,
/// `compression_threshold`) or `client_config` (`compression`,
/// `compression_threshold`): the threshold, 0 for none; None when it says
/// nothing.
pub(crate) fn requested_compression(
    compression: Option<&serde_json::Value>,
    threshold: Option<&serde_json::Value>,
) -> Option<usize> {
    match compression.and_then(serde_json::Value::as_bool) {
        Some(false) => Some(0),
        Some(true) => Some(
            threshold
                .and_then(serde_json::Value::as_u64)
                .map_or(1024, |t| usize::try_from(t).unwrap_or(usize::MAX)),
        ),
        None => None,
    }
}

/// A client's position for a topic as it sends it: `{"epoch": "<8 hex>",
/// "offset": <int>}`, and from 3.1 `"streams": {"<8 hex>": <int>, ...}`.
pub(crate) fn parse_position(position: &serde_json::Value) -> Option<TopicPosition> {
    let epoch = u32::from_str_radix(position.get("epoch")?.as_str()?, 16).ok()?;
    let offset = position.get("offset")?.as_u64()?;
    let streams = match position.get("streams") {
        None => None,
        Some(streams) => Some(
            streams
                .as_object()?
                .iter()
                .map(|(epoch, offset)| {
                    Some((u32::from_str_radix(epoch, 16).ok()?, offset.as_u64()?))
                })
                .collect::<Option<Vec<_>>>()?,
        ),
    };
    Some(TopicPosition {
        epoch,
        offset,
        streams,
    })
}

/// Answer a client's own `subscription_update` (with `client_subscriptions`
/// on). `subscribe` subscribes the topics the connection's ACL and limits
/// allow, replays what it missed from the positions it sent, then replies
/// with the outcome; the replay is queued before the reply. `unsubscribe`
/// gets no reply: the clients expect none.
#[cold]
#[inline(never)]
pub(crate) fn answer_subscription_request(
    state: &SharedState,
    conn_id: &str,
    handle: &ConnectionHandle,
    p: &serde_json::Value,
) {
    let topics: Vec<String> = p
        .get("topics")
        .and_then(serde_json::Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .filter_map(|t| t.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    match p.get("action").and_then(serde_json::Value::as_str) {
        Some("subscribe") => {}
        Some("unsubscribe") => {
            state.unsubscribe_topics(conn_id, Some(&topics));
            return;
        }
        _ => return,
    }
    let recover = p.get("recover").and_then(serde_json::Value::as_bool) == Some(true);
    let positions: HashMap<String, TopicPosition> = if recover {
        p.get("recovery")
            .and_then(serde_json::Value::as_object)
            .map(|positions| {
                positions
                    .iter()
                    .filter_map(|(topic, position)| {
                        Some((topic.clone(), parse_position(position)?))
                    })
                    .collect()
            })
            .unwrap_or_default()
    } else {
        HashMap::new()
    };
    // Each topic once, a bounded number per request, and on its own a client
    // holds at most CLIENT_SUBSCRIPTION_LIMIT unless the server sets a limit.
    let mut asked: Vec<String> = Vec::with_capacity(topics.len().min(MAX_TOPICS_PER_REQUEST));
    let mut seen: HashSet<&str> = HashSet::with_capacity(asked.capacity());
    for topic in &topics {
        if asked.len() == MAX_TOPICS_PER_REQUEST {
            break;
        }
        if seen.insert(topic) {
            asked.push(topic.clone());
        }
    }
    if state.max_subscriptions_per_connection == 0 {
        // Only topics it does not hold yet count against the cap.
        let held = state.conn_topics.get(conn_id);
        let mut room =
            CLIENT_SUBSCRIPTION_LIMIT.saturating_sub(held.as_ref().map_or(0, |held| held.len()));
        asked.retain(|t| {
            if held.as_ref().is_some_and(|held| held.contains(t)) {
                return true;
            }
            if room == 0 {
                return false;
            }
            room -= 1;
            true
        });
    }
    // Not accepted yet (no token, and the application has not called
    // accept_connection): every topic is refused.
    let (accepted, recovered) = if handle.out.admitted.load(Ordering::Acquire) && !asked.is_empty()
    {
        state
            .subscribe_and_recover(conn_id, handle, asked, &positions)
            .unwrap_or_default()
    } else {
        Default::default()
    };
    let taken: HashSet<&str> = accepted.iter().map(String::as_str).collect();
    let failed: Vec<&String> = topics
        .iter()
        .filter(|t| !taken.contains(t.as_str()))
        .collect();
    let recovery: serde_json::Map<String, serde_json::Value> = recovered
        .iter()
        .map(|(topic, r)| {
            (
                topic.clone(),
                serde_json::json!({
                    "epoch": r.epoch_hex(),
                    "offset": r.offset,
                    "recovered": r.recovered,
                    "count": r.count,
                    "streams": r.streams_json(),
                }),
            )
        })
        .collect();
    let reply = serde_json::json!({
        "c": "WSE",
        "t": "subscription_update",
        "id": uuid::Uuid::now_v7().to_string(),
        "p": {
            "success": failed.is_empty() || !accepted.is_empty(),
            "success_topics": accepted,
            "failed_topics": failed,
            "recovery": recovery,
        },
        "v": 1,
    });
    handle.send_one(
        Message::Text(reply.to_string().into()),
        state.max_outbound_queue_bytes,
        state.slow_consumer_drops.shard(usize::from(handle.lane)),
    );
}
