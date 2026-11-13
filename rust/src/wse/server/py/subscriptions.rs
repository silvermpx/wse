//! Topic ACLs, subscribe (with queue groups, presence and recovery) and
//! unsubscribe: the Python side of the engine's subscription module.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use crate::wse::cluster::ClusterCommand;
use crate::wse::server::state::InboundEvent;
use crate::wse::server::subscribe::{TopicRecovery, parse_anchor};
use crate::wse::server::{ServerCommand, format_presence_msg};

fn limit_error(conn_id: &str, max: usize) -> PyErr {
    PyRuntimeError::new_err(format!(
        "Connection {conn_id} reached max subscriptions ({max})"
    ))
}

impl RustWSEServer {
    pub(super) fn set_topic_acl_impl(
        &self,
        conn_id: &str,
        allow: Option<Vec<String>>,
        deny: Option<Vec<String>>,
    ) -> bool {
        // Only for a registered connection: its cleanup removes the entry.
        if !self.shared.conn_last_activity.contains_key(conn_id) {
            return false;
        }
        let acl = crate::wse::server::TopicAcl {
            allow: allow.unwrap_or_default(),
            deny: deny.unwrap_or_default(),
        };
        self.shared.topics.acls.insert(conn_id.to_owned(), acl);
        // Gone meanwhile: its cleanup may have run before the insert.
        if !self.shared.conn_last_activity.contains_key(conn_id) {
            self.shared.topics.acls.remove(conn_id);
            return false;
        }
        true
    }

    pub(super) fn subscribe_connection_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<Vec<String>> {
        let accepted = self
            .shared
            .admit_topics(conn_id, topics)
            .map_err(|max| limit_error(conn_id, max))?;
        // Gone: nothing to subscribe.
        let Some(handle) = self.connection_handle(py, conn_id) else {
            return Ok(Vec::new());
        };
        self.shared
            .subscribe_topics(conn_id, &handle, &accepted, queue_group.as_deref());

        // Presence tracking
        if let (Some(pm), Some(pd)) = (&self.shared.presence, &presence_data) {
            let data = pyobj_to_json(pd.as_any())?;
            let drain = self.shared.drain_mode.load(Ordering::Relaxed);
            let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
            for topic in &accepted {
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

        Ok(accepted)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the keyword arguments of subscribe_with_recovery()"
    )]
    pub(super) fn subscribe_with_recovery_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        recover: bool,
        epoch: Option<String>,
        offset: Option<u64>,
        positions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyDict>> {
        // Each topic recovers from its own position: `positions` maps a topic
        // to {"epoch": "<8 hex>", "offset": n}, the shape the clients send;
        // a single epoch/offset applies to every topic.
        let mut anchors: HashMap<String, (u32, u64)> = HashMap::new();
        if recover {
            if let Some(positions) = positions {
                for (topic, position) in positions.iter() {
                    let topic: String = topic.extract()?;
                    let anchor = parse_anchor(&pyobj_to_json(&position)?).ok_or_else(|| {
                        PyValueError::new_err(format!(
                            "positions[{topic:?}] must be {{\"epoch\": \"<8 hex digits>\", \"offset\": <int>}}"
                        ))
                    })?;
                    anchors.insert(topic, anchor);
                }
            } else if let (Some(epoch), Some(offset)) = (&epoch, offset) {
                let epoch = u32::from_str_radix(epoch, 16)
                    .map_err(|e| PyValueError::new_err(format!("Invalid epoch hex string: {e}")))?;
                for topic in &topics {
                    anchors.insert(topic.clone(), (epoch, offset));
                }
            }
        }
        let (accepted, recovered) = match self.connection_handle(py, conn_id) {
            Some(handle) => self
                .shared
                .subscribe_and_recover(conn_id, &handle, topics.clone(), &anchors)
                .map_err(|max| limit_error(conn_id, max))?,
            None => (Vec::new(), HashMap::new()),
        };

        let topics_dict = PyDict::new(py);
        let mut all_recovered = recover && !topics.is_empty();
        for topic in &topics {
            let subscribed = accepted.contains(topic);
            let r = recovered.get(topic).copied().unwrap_or(TopicRecovery {
                epoch: None,
                offset: 0,
                recovered: false,
                recoverable: false,
                count: 0,
            });
            all_recovered &= r.recovered;
            let topic_dict = PyDict::new(py);
            topic_dict.set_item("epoch", r.epoch_hex())?;
            topic_dict.set_item("offset", r.offset)?;
            topic_dict.set_item("recoverable", r.recoverable)?;
            topic_dict.set_item("recovered", r.recovered)?;
            topic_dict.set_item("count", r.count)?;
            topic_dict.set_item("subscribed", subscribed)?;
            topics_dict.set_item(topic.as_str(), topic_dict)?;
        }
        let result_dict = PyDict::new(py);
        result_dict.set_item("recovered", all_recovered)?;
        result_dict.set_item("topics", topics_dict)?;
        Ok(result_dict.into())
    }

    pub(super) fn unsubscribe_connection_impl(
        &self,
        conn_id: &str,
        topics: Option<Vec<String>>,
    ) -> PyResult<()> {
        self.shared.unsubscribe_topics(conn_id, topics.as_deref());
        Ok(())
    }
}
