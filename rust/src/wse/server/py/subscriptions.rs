//! Topic ACLs, subscribe (with queue groups, presence and recovery) and unsubscribe.

use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use crate::wse::cluster::ClusterCommand;
use crate::wse::server::e2e::encrypt_outbound;
use crate::wse::server::state::InboundEvent;
use crate::wse::server::{QueueGroup, ServerCommand, WsFrame, format_presence_msg, pre_frame_text};

impl RustWSEServer {
    pub(super) fn set_topic_acl_impl(
        &self,
        conn_id: &str,
        allow: Option<Vec<String>>,
        deny: Option<Vec<String>>,
    ) {
        let acl = crate::jwt::TopicAcl {
            allow: allow.unwrap_or_default(),
            deny: deny.unwrap_or_default(),
        };
        self.shared.conn_topic_acl.insert(conn_id.to_owned(), acl);
    }

    pub(super) fn subscribe_connection_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<()> {
        // Filter topics through ACL if one exists for this connection.
        // No ACL = allow all (backward compatible).
        let mut topics = if let Some(acl) = self.shared.conn_topic_acl.get(conn_id) {
            topics
                .into_iter()
                .filter(|t| acl.is_allowed(t))
                .collect::<Vec<_>>()
        } else {
            topics
        };

        // Reject topics containing ASCII control characters. The cluster RESYNC
        // wire format newline-delimits topics, so a '\n' in a topic name would be
        // decoded by a peer as two separate interest entries -- corrupting that
        // peer's routing table (a topic-injection vector). Drop them at this
        // single chokepoint where topics enter local interest / RESYNC. Non-ASCII
        // UTF-8 topics are unaffected (only ASCII control bytes are rejected).
        topics.retain(|t| {
            if t.bytes().any(|b| b.is_ascii_control()) {
                tracing::warn!(
                    "[WSE] Rejecting subscribe to topic with control characters (conn {conn_id})"
                );
                false
            } else {
                true
            }
        });

        // Enforce per-connection subscription limit (0 = unlimited).
        // Count both regular subscriptions (conn_topics) and queue group
        // memberships (conn_queue_groups) against the same budget.
        let max_subs = self.shared.max_subscriptions_per_connection;
        if max_subs > 0 {
            let topic_count = self
                .shared
                .conn_topics
                .get(conn_id)
                .map(|s| s.len())
                .unwrap_or(0);
            let qg_count = self
                .shared
                .conn_queue_groups
                .get(conn_id)
                .map(|v| v.len())
                .unwrap_or(0);
            let current = topic_count + qg_count;
            let allowed = max_subs.saturating_sub(current);
            if allowed == 0 {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Connection {conn_id} reached max subscriptions ({max_subs})"
                )));
            }
            // Truncate topics to fit within remaining budget
            if topics.len() > allowed {
                topics.truncate(allowed);
            }
        }

        // The ConnectionHandle, before taking any lock.
        let handle = self.connection_handle(py, conn_id);

        // Topics this connection newly subscribed: counted for cluster interest
        // once the loop is done.
        let mut newly_subscribed: Vec<String> = Vec::new();

        for topic in &topics {
            if let Some(ref h) = handle {
                if let Some(ref qg_name) = queue_group {
                    // Queue group mode: add to queue_groups, NOT to topic_subscribers.
                    // Messages to this topic will round-robin to one member per group.
                    let topic_groups = self.shared.queue_groups.entry(topic.clone()).or_default();
                    let mut group =
                        topic_groups
                            .entry(qg_name.clone())
                            .or_insert_with(|| QueueGroup {
                                members: Vec::new(),
                                next: AtomicUsize::new(0),
                            });
                    // Avoid duplicate membership
                    if !group.members.iter().any(|(id, _)| id == conn_id) {
                        group.members.push((conn_id.to_owned(), h.clone()));
                    }
                    drop(group);
                    drop(topic_groups);
                    // Track for disconnect cleanup (avoid duplicates from repeated subscribe calls)
                    {
                        let mut memberships = self
                            .shared
                            .conn_queue_groups
                            .entry(conn_id.to_owned())
                            .or_default();
                        let pair = (topic.clone(), qg_name.clone());
                        if !memberships.contains(&pair) {
                            memberships.push(pair);
                        }
                    }
                } else {
                    // Normal fan-out mode: add to topic_subscribers
                    // Use single entry() call to detect fresh topic creation atomically
                    // (avoids TOCTOU race between contains_key and entry)
                    let outer = self
                        .shared
                        .topic_subscribers
                        .entry(topic.clone())
                        .or_default();
                    let is_new_entry = outer.insert(conn_id.to_owned(), h.clone()).is_none();
                    let is_first = is_new_entry && outer.len() == 1;
                    // fetch_add while holding shard lock to prevent race with concurrent
                    // unsubscribe (which also needs the shard lock for remove_if)
                    if is_first && (topic.contains('*') || topic.contains('?')) {
                        self.shared.glob_topic_count.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(outer);
                }
            } else {
                // Connection already disconnected -- skip topic tracking and cluster sub
                // to avoid phantom subscriptions in conn_topics and cluster interest
                continue;
            }
            // Queue-group members have their own cleanup via conn_queue_groups.
            // Skip conn_topics + cluster refcount tracking to avoid spurious UNSUB.
            if queue_group.is_some() {
                continue;
            }
            // Only increment refcount if this is genuinely new for this connection
            let is_new = self
                .shared
                .conn_topics
                .entry(conn_id.to_owned())
                .or_default()
                .insert(topic.clone());

            if is_new {
                newly_subscribed.push(topic.clone());
            }
        }
        // No Python code runs under the refcount lock: it is taken and released
        // here, before the presence data is read from Python below.
        self.shared.add_local_interest(&newly_subscribed);

        // Presence tracking
        if let (Some(pm), Some(pd)) = (&self.shared.presence, &presence_data) {
            let data = pyobj_to_json(pd.as_any());
            let drain = self.shared.drain_mode.load(Ordering::Relaxed);
            let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
            for topic in &topics {
                if let crate::wse::presence::TrackResult::FirstJoin {
                    user_id,
                    data: join_data,
                } = pm.track(conn_id, topic, &data)
                {
                    if drain {
                        self.shared.push_inbound(InboundEvent::PresenceJoin {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            data: join_data.clone(),
                        });
                    }
                    // Broadcast to topic subscribers via WebSocket
                    if let Ok(tx) = self.command_tx() {
                        let msg = format_presence_msg("presence_join", &user_id, &join_data);
                        let _ = tx.send(ServerCommand::BroadcastLocal {
                            topic: topic.clone(),
                            data: msg,
                            skip_recovery: true,
                            publish: false,
                        });
                    }
                    // Cluster sync: notify peers of presence join
                    if let Some(ref ctx) = cluster_tx {
                        let data_str = serde_json::to_string(&join_data).unwrap_or_default();
                        let now = crate::wse::presence::epoch_ms();
                        let _ = ctx.send(ClusterCommand::PresenceUpdate {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            action: 0, // join
                            data: data_str,
                            updated_at: now,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) fn subscribe_with_recovery_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        recover: bool,
        epoch: Option<String>,
        offset: Option<u64>,
    ) -> PyResult<Py<PyDict>> {
        // Step 1: Subscribe using existing logic (no presence tracking for recovery)
        self.subscribe_connection_impl(py, conn_id, topics.clone(), None, None)?;

        // Fail-closed authorization for the recovery path: subscribe_connection
        // above filters the LIVE subscription through the connection's topic ACL,
        // but the recovery loop below must apply the SAME filter -- otherwise a
        // connection scoped to user:SELF:* could pass user:OTHER:private and have
        // the recovery buffer replay another tenant's history (and get_position
        // leak that topic's existence + message counts). When no ACL is
        // configured the connection is unrestricted (the fail-open default is a
        // separate finding); here we only mirror whatever subscribe_connection did.
        let allowed: std::collections::HashSet<String> =
            if let Some(acl) = self.shared.conn_topic_acl.get(conn_id) {
                topics
                    .iter()
                    .filter(|t| acl.is_allowed(t.as_str()))
                    .cloned()
                    .collect()
            } else {
                topics.iter().cloned().collect()
            };

        // Step 2: Get connection tx + pending counter (clone them, drop the lock)
        let (tx, tx_pending) = if recover {
            if self.runtime().is_none() {
                return Err(PyRuntimeError::new_err("Not running"));
            }
            match self.connection_handle(py, conn_id) {
                Some(h) => (Some(h.tx), Some(h.pending)),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        let recovery_mgr = self.shared.recovery.as_ref();
        let max_pending = self.shared.max_outbound_queue_bytes;
        // Per-connection cipher so replayed messages are framed/encrypted exactly
        // like live ones (an owned clone -- no DashMap ref held across the loop).
        let conn_cipher = self.shared.conn_encryption.get(conn_id).map(|c| c.clone());
        let mut all_recovered = recover && !topics.is_empty();
        let result_dict = PyDict::new(py);
        let topics_dict = PyDict::new(py);

        for topic in &topics {
            let topic_dict = PyDict::new(py);

            // Fail-closed: a topic this connection is not authorized for is never
            // recovered and its position is never probed (no history replay, no
            // existence/count leak). It was already dropped from the live sub.
            if !allowed.contains(topic) {
                all_recovered = false;
                topic_dict.set_item("epoch", py.None())?;
                topic_dict.set_item("offset", 0u64)?;
                topic_dict.set_item("recoverable", false)?;
                topic_dict.set_item("recovered", false)?;
                topic_dict.set_item("count", 0)?;
                topics_dict.set_item(topic.as_str(), topic_dict)?;
                continue;
            }

            // Check if recovery is possible and requested
            let can_recover = recover
                && epoch.is_some()
                && offset.is_some()
                && recovery_mgr.is_some()
                && tx.is_some();

            if can_recover {
                let epoch_str = epoch.as_ref().unwrap();
                let after_offset = offset.unwrap();
                let mgr = recovery_mgr.unwrap();

                // Parse epoch from hex string
                let epoch_u32 = u32::from_str_radix(epoch_str, 16).map_err(|e| {
                    PyRuntimeError::new_err(format!("Invalid epoch hex string: {e}"))
                })?;

                let result = mgr.recover(topic, epoch_u32, after_offset);
                match result {
                    crate::wse::recovery::RecoveryResult::Recovered {
                        publications,
                        epoch: current_epoch,
                        offset: current_offset,
                    } => {
                        let count = publications.len();
                        let conn_tx = tx.as_ref().unwrap();
                        let conn_pending = tx_pending.as_ref().unwrap();
                        // The recovery buffer stores the UNFRAMED stamped JSON, so frame
                        // it -- or encrypt + frame for an E2E connection -- per connection
                        // at send time, exactly like the live fan-out. This stops a
                        // plaintext frame being replayed to an encrypted client (a
                        // confidentiality leak + an undecodable frame on the client).
                        // Replay is also gated on the connection's back-pressure budget so
                        // a slow reconnecting client is not flooded in one shot; on
                        // overflow we stop and the client re-detects the gap from the next
                        // live (e, o) and recovers again.
                        for payload in publications {
                            if max_pending > 0
                                && conn_pending.load(Ordering::Relaxed) >= max_pending
                            {
                                break;
                            }
                            let (frame, sz) = if let Some(ref c) = conn_cipher {
                                match encrypt_outbound(c, &payload) {
                                    Ok(enc) => {
                                        let mut buf = Vec::with_capacity(2 + enc.len());
                                        buf.extend_from_slice(b"E:");
                                        buf.extend_from_slice(&enc);
                                        let sz = buf.len() + 4;
                                        (WsFrame::Msg(Message::Binary(buf.into())), sz)
                                    }
                                    Err(_) => continue,
                                }
                            } else {
                                let framed =
                                    pre_frame_text(std::str::from_utf8(&payload).unwrap_or(""));
                                let sz = framed.len();
                                (WsFrame::PreFramed(framed), sz)
                            };
                            conn_pending.fetch_add(sz, Ordering::Relaxed);
                            if conn_tx.send(frame).is_err() {
                                conn_pending.fetch_sub(sz, Ordering::Relaxed);
                                break;
                            }
                        }
                        topic_dict.set_item("epoch", format!("{:08x}", current_epoch))?;
                        topic_dict.set_item("offset", current_offset)?;
                        topic_dict.set_item("recoverable", true)?;
                        topic_dict.set_item("recovered", true)?;
                        topic_dict.set_item("count", count)?;
                    }
                    crate::wse::recovery::RecoveryResult::NotRecovered {
                        epoch: current_epoch,
                        offset: current_offset,
                    } => {
                        all_recovered = false;
                        topic_dict.set_item("epoch", format!("{:08x}", current_epoch))?;
                        topic_dict.set_item("offset", current_offset.saturating_sub(1))?;
                        topic_dict.set_item("recoverable", true)?;
                        topic_dict.set_item("recovered", false)?;
                        topic_dict.set_item("count", 0)?;
                    }
                    crate::wse::recovery::RecoveryResult::NoHistory => {
                        all_recovered = false;
                        // Get current position if available (normalize to inclusive last offset)
                        if let Some((ep, off)) = mgr.get_position(topic) {
                            topic_dict.set_item("epoch", format!("{:08x}", ep))?;
                            topic_dict.set_item("offset", off.saturating_sub(1))?;
                            topic_dict.set_item("recoverable", true)?;
                        } else {
                            topic_dict.set_item("epoch", py.None())?;
                            topic_dict.set_item("offset", 0u64)?;
                            topic_dict.set_item("recoverable", false)?;
                        }
                        topic_dict.set_item("recovered", false)?;
                        topic_dict.set_item("count", 0)?;
                    }
                }
            } else {
                // No recovery requested or not possible -- just report current position
                // (normalize to inclusive last offset, consistent with Recovered path)
                all_recovered = false;
                if let Some(mgr) = recovery_mgr {
                    if let Some((ep, off)) = mgr.get_position(topic) {
                        topic_dict.set_item("epoch", format!("{:08x}", ep))?;
                        topic_dict.set_item("offset", off.saturating_sub(1))?;
                        topic_dict.set_item("recoverable", true)?;
                    } else {
                        topic_dict.set_item("epoch", py.None())?;
                        topic_dict.set_item("offset", 0u64)?;
                        topic_dict.set_item("recoverable", false)?;
                    }
                } else {
                    topic_dict.set_item("epoch", py.None())?;
                    topic_dict.set_item("offset", 0u64)?;
                    topic_dict.set_item("recoverable", false)?;
                }
                topic_dict.set_item("recovered", false)?;
                topic_dict.set_item("count", 0)?;
            }

            topics_dict.set_item(topic.as_str(), topic_dict)?;
        }

        result_dict.set_item("recovered", all_recovered)?;
        result_dict.set_item("topics", topics_dict)?;

        Ok(result_dict.into())
    }

    pub(super) fn unsubscribe_connection_impl(
        &self,
        conn_id: &str,
        topics: Option<Vec<String>>,
    ) -> PyResult<()> {
        let unsubscribe_all = topics.is_none();
        // Save requested topics for queue group cleanup (conn_queue_groups tracks separately from conn_topics)
        let requested_unsub: Vec<String> = match &topics {
            Some(t) => t.clone(),
            None => Vec::new(),
        };
        let removed_topics: Vec<String> = match topics {
            Some(topics) => {
                let mut actually_removed = Vec::new();
                for topic in &topics {
                    // Only count as removed if conn was actually subscribed
                    let was_subscribed =
                        if let Some(conn_entry) = self.shared.conn_topics.get(conn_id) {
                            conn_entry.remove(topic).is_some()
                        } else {
                            false
                        };
                    if was_subscribed {
                        if let Some(subscribers) = self.shared.topic_subscribers.get(topic) {
                            subscribers.remove(conn_id);
                        }
                        if self
                            .shared
                            .topic_subscribers
                            .remove_if(topic, |_, subs| subs.is_empty())
                            .is_some()
                            && (topic.contains('*') || topic.contains('?'))
                        {
                            self.shared.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        actually_removed.push(topic.clone());
                    }
                }
                // Clean up empty conn_topics entry to prevent DashSet leak
                self.shared
                    .conn_topics
                    .remove_if(conn_id, |_, topics| topics.is_empty());
                actually_removed
            }
            None => {
                let mut removed = Vec::new();
                if let Some((_, topics)) = self.shared.conn_topics.remove(conn_id) {
                    for topic_ref in topics.iter() {
                        let topic = topic_ref.clone();
                        if let Some(subscribers) = self.shared.topic_subscribers.get(&topic) {
                            subscribers.remove(conn_id);
                        }
                        if self
                            .shared
                            .topic_subscribers
                            .remove_if(&topic, |_, subs| subs.is_empty())
                            .is_some()
                            && (topic.contains('*') || topic.contains('?'))
                        {
                            self.shared.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        removed.push(topic);
                    }
                }
                removed
            }
        };

        // Queue group cleanup: when unsubscribing specific topics, only remove those;
        // when unsubscribing all (topics=None), remove all queue group memberships.
        if let Some(mut memberships) = self.shared.conn_queue_groups.get_mut(conn_id) {
            memberships.retain(|(topic, group_name)| {
                if !unsubscribe_all
                    && !removed_topics.contains(topic)
                    && !requested_unsub.contains(topic)
                {
                    return true; // keep: topic was not in unsubscribe request
                }
                if let Some(tg) = self.shared.queue_groups.get(topic) {
                    if let Some(mut g) = tg.get_mut(group_name) {
                        g.members.retain(|(id, _)| id != conn_id);
                    }
                    tg.remove_if(group_name, |_, g| g.members.is_empty());
                }
                self.shared
                    .queue_groups
                    .remove_if(topic, |_, groups| groups.is_empty());
                false // remove from memberships
            });
        }
        // Clean up empty conn_queue_groups entry
        self.shared
            .conn_queue_groups
            .remove_if(conn_id, |_, v| v.is_empty());

        // Presence cleanup for removed topics
        if let Some(ref pm) = self.shared.presence {
            let results = pm.untrack_topics(conn_id, &removed_topics);
            let drain = self.shared.drain_mode.load(Ordering::Relaxed);
            for (topic, result) in results {
                if let crate::wse::presence::UntrackResult::LastLeave { user_id, data } = result {
                    if drain {
                        self.shared.push_inbound(InboundEvent::PresenceLeave {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            data: data.clone(),
                        });
                    }
                    // Broadcast to topic subscribers via WebSocket
                    if let Ok(tx) = self.command_tx() {
                        let msg = format_presence_msg("presence_leave", &user_id, &data);
                        let _ = tx.send(ServerCommand::BroadcastLocal {
                            topic: topic.clone(),
                            data: msg,
                            skip_recovery: true,
                            publish: false,
                        });
                    }
                    // Cluster sync: notify peers of presence leave
                    let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
                    if let Some(ref ctx) = cluster_tx {
                        let data_str = serde_json::to_string(&data).unwrap_or_default();
                        let now = crate::wse::presence::epoch_ms();
                        let _ = ctx.send(ClusterCommand::PresenceUpdate {
                            topic,
                            user_id,
                            action: 1, // leave
                            data: data_str,
                            updated_at: now,
                        });
                    }
                }
            }
        }

        self.shared.remove_local_interest(&removed_topics);

        Ok(())
    }
}
