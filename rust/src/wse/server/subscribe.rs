//! Subscribing connections to topics: the checks a request passes, the topic
//! index and cluster interest, the recovery of missed messages, and the
//! server's own answer to the clients' subscription requests. The Python API
//! and the connection task both come through here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::state::InboundEvent;
use super::{
    ConnectionHandle, QueueGroup, ServerCommand, SharedState, format_presence_msg, is_pattern,
};
use crate::wse::cluster::ClusterCommand;
use crate::wse::presence::UntrackResult;
use crate::wse::recovery::RecoveryResult;

/// Longest topic name accepted, in bytes.
pub(crate) const MAX_TOPIC_LEN: usize = 256;
/// Pattern subscriptions (names with `*` or `?`) one connection may hold.
pub(crate) const MAX_PATTERNS_PER_CONNECTION: usize = 16;

/// What a recovery did for one topic, in the terms the clients read: the
/// topic's epoch and an offset (inclusive: the last one the client has, or
/// may take as had), whether the gap was filled, and how many messages were
/// replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopicRecovery {
    pub(crate) epoch: Option<u32>,
    pub(crate) offset: u64,
    pub(crate) recovered: bool,
    pub(crate) recoverable: bool,
    pub(crate) count: usize,
}

impl TopicRecovery {
    /// A topic with no position to report.
    const UNKNOWN: Self = Self {
        epoch: None,
        offset: 0,
        recovered: false,
        recoverable: false,
        count: 0,
    };

    pub(crate) fn epoch_hex(&self) -> Option<String> {
        self.epoch.map(|epoch| format!("{epoch:08x}"))
    }
}

impl SharedState {
    /// The topics of `requested` that `conn_id` may take: allowed by its topic
    /// ACL, valid names (not empty, at most `MAX_TOPIC_LEN` bytes, no ASCII
    /// control characters: the cluster RESYNC newline-delimits topics), within
    /// its pattern budget and `max_subscriptions_per_connection`.
    /// `Err(limit)` when the connection holds that many already.
    pub(crate) fn admit_topics(
        &self,
        conn_id: &str,
        requested: Vec<String>,
    ) -> Result<Vec<String>, usize> {
        let mut topics: Vec<String> = match self.topics.acls.get(conn_id) {
            Some(acl) => requested
                .into_iter()
                .filter(|t| acl.is_allowed(t))
                .collect(),
            None => requested,
        };
        topics.retain(|t| {
            let valid = !t.is_empty()
                && t.len() <= MAX_TOPIC_LEN
                && !t.bytes().any(|b| b.is_ascii_control());
            if !valid {
                tracing::warn!("[WSE] Refusing an invalid topic name (conn {conn_id})");
            }
            valid
        });
        // Patterns cost every publish a match: a few per connection.
        let held = self.conn_topics.get(conn_id);
        let mut patterns = held
            .as_ref()
            .map_or(0, |held| held.iter().filter(|t| is_pattern(t)).count());
        topics.retain(|t| {
            if !is_pattern(t) || held.as_ref().is_some_and(|held| held.contains(t)) {
                return true;
            }
            patterns += 1;
            if patterns > MAX_PATTERNS_PER_CONNECTION {
                tracing::warn!(
                    "[WSE] Refusing pattern {t:?}: {MAX_PATTERNS_PER_CONNECTION} per connection (conn {conn_id})"
                );
                return false;
            }
            true
        });
        drop(held);
        let max_subs = self.max_subscriptions_per_connection;
        if max_subs > 0 {
            let current = self.conn_topics.get(conn_id).map_or(0, |s| s.len())
                + self.conn_queue_groups.get(conn_id).map_or(0, |v| v.len());
            let room = max_subs.saturating_sub(current);
            if room == 0 {
                return Err(max_subs);
            }
            topics.truncate(room);
        }
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
            self.announce_untracked(pm.untrack_topics(conn_id, &removed));
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
                updated_at: crate::wse::presence::epoch_ms(),
            });
        }
    }

    /// Tell the application (drain mode) and the topic's subscribers that a
    /// user left `topic`.
    pub(crate) fn tell_presence_leave(&self, topic: &str, user_id: &str, data: &serde_json::Value) {
        if self.drain_mode.load(Ordering::Relaxed) {
            self.push_inbound(InboundEvent::PresenceLeave {
                topic: topic.to_owned(),
                user_id: user_id.to_owned(),
                data: data.clone(),
            });
        }
        if let Some(tx) = &*self.presence_broadcast_tx.load() {
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

    /// Replay to `handle` the messages of `topic` after `anchor` (the
    /// client's last (epoch, offset)), queued before anything sent to the
    /// connection after this call. `head_before` is the topic's head taken
    /// before the connection subscribed.
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
        anchor: Option<(u32, u64)>,
        head_before: Option<(u32, u64)>,
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
            },
            _ => TopicRecovery::UNKNOWN,
        };
        let Some((epoch, after)) = anchor else {
            return not_recovered;
        };
        match recovery.recover(topic, epoch, after) {
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
                        &self.slow_consumer_drops,
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
        anchors: &HashMap<String, (u32, u64)>,
    ) -> Result<(Vec<String>, HashMap<String, TopicRecovery>), usize> {
        let accepted = self.admit_topics(conn_id, requested)?;
        self.subscribe_topics(conn_id, handle, &accepted, None);
        let heads: Vec<Option<(u32, u64)>> = accepted.iter().map(|t| self.topic_head(t)).collect();
        let recovered = accepted
            .iter()
            .zip(heads)
            .map(|(topic, head)| {
                let anchor = anchors.get(topic).copied();
                (
                    topic.clone(),
                    self.recover_topic(handle, topic, anchor, head),
                )
            })
            .collect();
        Ok((accepted, recovered))
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
/// "offset": <int>}`.
pub(crate) fn parse_anchor(position: &serde_json::Value) -> Option<(u32, u64)> {
    let epoch = u32::from_str_radix(position.get("epoch")?.as_str()?, 16).ok()?;
    let offset = position.get("offset")?.as_u64()?;
    Some((epoch, offset))
}

/// Answer a client's own `subscription_update` (with `client_subscriptions`
/// on). `subscribe` subscribes the topics the connection's ACL and limits
/// allow, replays what it missed from the positions it sent, then replies
/// with the outcome; the replay is queued before the reply. `unsubscribe`
/// gets no reply: the clients expect none.
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
    let anchors: HashMap<String, (u32, u64)> = if recover {
        p.get("recovery")
            .and_then(serde_json::Value::as_object)
            .map(|positions| {
                positions
                    .iter()
                    .filter_map(|(topic, position)| Some((topic.clone(), parse_anchor(position)?)))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        HashMap::new()
    };
    let (accepted, recovered) = state
        .subscribe_and_recover(conn_id, handle, topics.clone(), &anchors)
        .unwrap_or_default();
    let failed: Vec<&String> = topics.iter().filter(|t| !accepted.contains(t)).collect();
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
        &state.slow_consumer_drops,
    );
}
