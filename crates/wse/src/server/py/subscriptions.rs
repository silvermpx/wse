//! Topic ACLs, subscribe (with queue groups, presence and recovery) and
//! unsubscribe: the Python side of the engine's subscription module.

use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::pyobj_to_json;
use super::messaging::py_error;
use crate::server::Subscribe;
use crate::server::subscribe::parse_anchor;

impl RustWSEServer {
    pub(super) fn set_topic_acl_impl(
        &self,
        conn_id: &str,
        allow: Option<Vec<String>>,
        deny: Option<Vec<String>>,
    ) -> bool {
        self.engine().set_topic_acl(conn_id, allow, deny)
    }

    pub(super) fn subscribe_connection_impl(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<Vec<String>> {
        let presence = match presence_data {
            Some(data) if self.shared.presence.is_some() => Some(pyobj_to_json(data.as_any())?),
            _ => None,
        };
        let options = Subscribe {
            queue_group,
            presence,
        };
        let engine = self.engine();
        py.detach(|| engine.subscribe_blocking(conn_id, topics, &options))
            .map_err(py_error)
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
        let engine = self.engine();
        let asked = topics.clone();
        let results = py
            .detach(|| engine.subscribe_with_recovery_blocking(conn_id, topics, &anchors))
            .map_err(py_error)?;

        let topics_dict = PyDict::new(py);
        let mut all_recovered = recover && !asked.is_empty();
        for topic in &asked {
            let r = &results[topic];
            all_recovered &= r.recovered;
            let topic_dict = PyDict::new(py);
            topic_dict.set_item("epoch", r.epoch.clone())?;
            topic_dict.set_item("offset", r.offset)?;
            topic_dict.set_item("recoverable", r.recoverable)?;
            topic_dict.set_item("recovered", r.recovered)?;
            topic_dict.set_item("count", r.count)?;
            topic_dict.set_item("subscribed", r.subscribed)?;
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
        self.engine().unsubscribe(conn_id, topics.as_deref());
        Ok(())
    }
}
