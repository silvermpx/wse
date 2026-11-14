//! Health snapshot, Prometheus metrics, queue group info and the cluster dead-letter queue.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::RustWSEServer;

impl RustWSEServer {
    pub(super) fn get_queue_group_info_impl(
        &self,
        py: Python<'_>,
        topic: &str,
    ) -> PyResult<Py<PyDict>> {
        let groups = self.engine().queue_groups(topic);
        let dict = PyDict::new(py);
        for (group, members) in &groups {
            dict.set_item(group.as_str(), *members)?;
        }
        Ok(dict.into())
    }

    pub(super) fn health_snapshot_impl(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let health = self.engine().health();
        let dict = PyDict::new(py);
        dict.set_item("connections", health.connections)?;
        dict.set_item("inbound_queue_depth", health.inbound_queue_depth)?;
        dict.set_item("command_queue_depth", health.command_queue_depth)?;
        dict.set_item("inbound_dropped", health.inbound_dropped)?;
        dict.set_item("cluster_connected", health.cluster_peer_count > 0)?;
        dict.set_item("cluster_peer_count", health.cluster_peer_count)?;
        dict.set_item("cluster_messages_sent", health.cluster_messages_sent)?;
        dict.set_item(
            "cluster_messages_delivered",
            health.cluster_messages_delivered,
        )?;
        dict.set_item("cluster_messages_dropped", health.cluster_messages_dropped)?;
        dict.set_item("cluster_bytes_sent", health.cluster_bytes_sent)?;
        dict.set_item("cluster_bytes_received", health.cluster_bytes_received)?;
        dict.set_item("cluster_reconnect_count", health.cluster_reconnect_count)?;
        dict.set_item(
            "cluster_unknown_message_types",
            health.cluster_unknown_message_types,
        )?;
        dict.set_item("cluster_dlq_size", health.cluster_dlq_size)?;
        dict.set_item("uptime_secs", health.uptime_secs)?;
        dict.set_item("crypto", health.crypto)?;
        dict.set_item("recovery_enabled", health.recovery_enabled)?;
        dict.set_item("recovery_topic_count", health.recovery_topic_count)?;
        dict.set_item("recovery_total_bytes", health.recovery_total_bytes)?;
        dict.set_item("presence_enabled", health.presence_enabled)?;
        if health.presence_enabled {
            dict.set_item("presence_topics", health.presence_topics)?;
            dict.set_item("presence_total_users", health.presence_total_users)?;
        }
        Ok(dict.unbind())
    }

    pub(super) fn prometheus_metrics_impl(&self) -> String {
        self.engine().prometheus_metrics()
    }

    pub(super) fn get_cluster_dlq_entries_impl(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let entries = self.engine().take_dead_letters();
        let list = PyList::empty(py);
        for entry in &entries {
            let d = PyDict::new(py);
            d.set_item("topic", &entry.topic)?;
            d.set_item("payload", &entry.payload)?;
            d.set_item("peer_addr", &entry.peer_addr)?;
            d.set_item("error", &entry.error)?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }
}
