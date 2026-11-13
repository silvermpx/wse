//! Presence queries and presence data updates.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use super::RustWSEServer;
use super::convert::{json_to_pyobj, pyobj_to_json};
use crate::wse::cluster::ClusterCommand;
use crate::wse::server::{ServerCommand, format_presence_msg};

impl RustWSEServer {
    pub(super) fn presence_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);

        if let Some(ref pm) = self.shared.presence {
            for (user_id, data, conn_count) in pm.query(topic) {
                let user_dict = PyDict::new(py);
                user_dict.set_item("data", json_to_pyobj(py, &data))?;
                user_dict.set_item("connections", conn_count)?;
                result.set_item(user_id.as_str(), user_dict)?;
            }
        }

        Ok(result.into())
    }

    pub(super) fn presence_stats_impl(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        if let Some(ref pm) = self.shared.presence {
            let (users, conns) = pm.stats(topic);
            result.set_item("num_users", users)?;
            result.set_item("num_connections", conns)?;
        } else {
            result.set_item("num_users", 0)?;
            result.set_item("num_connections", 0)?;
        }
        Ok(result.into())
    }

    pub(super) fn update_presence_impl(
        &self,
        conn_id: &str,
        data: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        if let Some(ref pm) = self.shared.presence {
            let new_data = pyobj_to_json(data.as_any())?;
            match pm.update_data(conn_id, &new_data) {
                None => {
                    return Err(PyRuntimeError::new_err(
                        "Failed to update presence data (size limit or unknown connection)",
                    ));
                }
                Some((user_id, topics)) => {
                    // Broadcast presence_update to all topics where the user has presence
                    let data_str = serde_json::to_string(&new_data).unwrap_or_default();
                    let now = crate::wse::presence::epoch_ms();
                    let cluster_tx = self.shared.cluster_cmd_tx.load_full();
                    if let Ok(tx) = self.command_tx() {
                        let msg = format_presence_msg("presence_update", &user_id, &new_data);
                        for topic in &topics {
                            let _ = tx.send(ServerCommand::BroadcastLocal {
                                topic: topic.clone(),
                                data: msg.clone(),
                                skip_recovery: true,
                                publish: false,
                            });
                        }
                    }
                    // Cluster sync: notify peers of presence update
                    if let Some(ref ctx) = cluster_tx {
                        for topic in topics {
                            let _ = ctx.send(ClusterCommand::PresenceUpdate {
                                topic,
                                user_id: user_id.clone(),
                                action: 2, // update
                                data: data_str.clone(),
                                updated_at: now,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
