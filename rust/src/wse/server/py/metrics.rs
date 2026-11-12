//! Health snapshot, Prometheus metrics, queue group info and the cluster dead-letter queue.

use std::sync::atomic::Ordering;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use super::RustWSEServer;

impl RustWSEServer {
    pub(super) fn get_queue_group_info_impl(
        &self,
        py: Python<'_>,
        topic: &str,
    ) -> PyResult<Py<PyDict>> {
        // Copy first, build the dict after (see `cluster_info`).
        let groups: Vec<(String, usize)> = match self.shared.queue_groups.get(topic) {
            Some(topic_groups) => topic_groups
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().members.len()))
                .collect(),
            None => Vec::new(),
        };
        let dict = PyDict::new(py);
        for (group, members) in &groups {
            dict.set_item(group.as_str(), *members)?;
        }
        Ok(dict.into())
    }

    pub(super) fn health_snapshot_impl(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item(
            "connections",
            self.shared.connection_count.load(Ordering::Relaxed),
        )?;
        dict.set_item("inbound_queue_depth", self.shared.inbound_rx.len())?;
        dict.set_item(
            "inbound_dropped",
            self.shared.inbound_dropped.load(Ordering::Relaxed),
        )?;
        // Cluster metrics
        let cm = &self.shared.cluster_metrics;
        dict.set_item(
            "cluster_connected",
            cm.connected_peers.load(Ordering::Relaxed) > 0,
        )?;
        dict.set_item(
            "cluster_peer_count",
            cm.connected_peers.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_sent",
            cm.messages_sent.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_delivered",
            cm.messages_delivered.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_dropped",
            cm.messages_dropped.load(Ordering::Relaxed),
        )?;
        dict.set_item("cluster_bytes_sent", cm.bytes_sent.load(Ordering::Relaxed))?;
        dict.set_item(
            "cluster_bytes_received",
            cm.bytes_received.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_reconnect_count",
            cm.reconnect_count.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_unknown_message_types",
            cm.unknown_message_types.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_dlq_size",
            self.shared
                .cluster_dlq
                .try_lock()
                .map(|d| d.len())
                .unwrap_or(0),
        )?;
        dict.set_item("uptime_secs", self.uptime_secs())?;
        // Recovery metrics
        if let Some(ref recovery) = self.shared.recovery {
            dict.set_item("recovery_enabled", true)?;
            dict.set_item("recovery_topic_count", recovery.topic_count())?;
            dict.set_item("recovery_total_bytes", recovery.total_bytes())?;
        } else {
            dict.set_item("recovery_enabled", false)?;
            dict.set_item("recovery_topic_count", 0)?;
            dict.set_item("recovery_total_bytes", 0)?;
        }
        // Presence metrics
        if let Some(ref pm) = self.shared.presence {
            dict.set_item("presence_enabled", true)?;
            dict.set_item("presence_topics", pm.total_topics())?;
            dict.set_item("presence_total_users", pm.total_users())?;
        } else {
            dict.set_item("presence_enabled", false)?;
        }
        Ok(dict.unbind())
    }

    pub(super) fn prometheus_metrics_impl(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Gauges
        out.push_str("# HELP wse_connections Current active WebSocket connections\n");
        out.push_str("# TYPE wse_connections gauge\n");
        out.push_str(&format!(
            "wse_connections {}\n",
            self.shared.connection_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_queue_depth Drain queue backlog\n");
        out.push_str("# TYPE wse_inbound_queue_depth gauge\n");
        out.push_str(&format!(
            "wse_inbound_queue_depth {}\n",
            self.shared.inbound_rx.len()
        ));

        out.push_str("# HELP wse_uptime_seconds Server uptime in seconds\n");
        out.push_str("# TYPE wse_uptime_seconds gauge\n");
        out.push_str(&format!("wse_uptime_seconds {:.1}\n", self.uptime_secs()));

        out.push_str("# HELP wse_topics Number of active topics\n");
        out.push_str("# TYPE wse_topics gauge\n");
        out.push_str(&format!(
            "wse_topics {}\n",
            self.shared.topic_subscribers.len()
        ));

        // Counters
        out.push_str("# HELP wse_messages_received_total Total messages received from clients\n");
        out.push_str("# TYPE wse_messages_received_total counter\n");
        out.push_str(&format!(
            "wse_messages_received_total {}\n",
            self.shared.messages_received_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_messages_sent_total Total messages sent to clients\n");
        out.push_str("# TYPE wse_messages_sent_total counter\n");
        out.push_str(&format!(
            "wse_messages_sent_total {}\n",
            self.shared.messages_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_accepted_total Total connections accepted\n");
        out.push_str("# TYPE wse_connections_accepted_total counter\n");
        out.push_str(&format!(
            "wse_connections_accepted_total {}\n",
            self.shared
                .connections_accepted_total
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_rejected_total Total connections rejected\n");
        out.push_str("# TYPE wse_connections_rejected_total counter\n");
        out.push_str(&format!(
            "wse_connections_rejected_total {}\n",
            self.shared
                .connections_rejected_total
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_bytes_received_total Total bytes received from clients\n");
        out.push_str("# TYPE wse_bytes_received_total counter\n");
        out.push_str(&format!(
            "wse_bytes_received_total {}\n",
            self.shared.bytes_received_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_bytes_sent_total Total bytes sent to clients\n");
        out.push_str("# TYPE wse_bytes_sent_total counter\n");
        out.push_str(&format!(
            "wse_bytes_sent_total {}\n",
            self.shared.bytes_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_auth_failures_total Total authentication failures\n");
        out.push_str("# TYPE wse_auth_failures_total counter\n");
        out.push_str(&format!(
            "wse_auth_failures_total {}\n",
            self.shared.auth_failures_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_rate_limited_total Total messages dropped by rate limiter\n");
        out.push_str("# TYPE wse_rate_limited_total counter\n");
        out.push_str(&format!(
            "wse_rate_limited_total {}\n",
            self.shared.rate_limited_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_dropped_total Drain queue events dropped\n");
        out.push_str("# TYPE wse_inbound_dropped_total counter\n");
        out.push_str(&format!(
            "wse_inbound_dropped_total {}\n",
            self.shared.inbound_dropped.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP wse_slow_consumer_drops_total Messages dropped due to slow consumers\n",
        );
        out.push_str("# TYPE wse_slow_consumer_drops_total counter\n");
        out.push_str(&format!(
            "wse_slow_consumer_drops_total {}\n",
            self.shared.slow_consumer_drops.load(Ordering::Relaxed)
        ));

        // Cluster metrics
        let cm = &self.shared.cluster_metrics;
        out.push_str("# HELP wse_cluster_peers Connected cluster peers\n");
        out.push_str("# TYPE wse_cluster_peers gauge\n");
        out.push_str(&format!(
            "wse_cluster_peers {}\n",
            cm.connected_peers.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_messages_sent_total Messages sent to cluster peers\n");
        out.push_str("# TYPE wse_cluster_messages_sent_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_sent_total {}\n",
            cm.messages_sent.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP wse_cluster_messages_delivered_total Messages delivered from cluster peers\n",
        );
        out.push_str("# TYPE wse_cluster_messages_delivered_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_delivered_total {}\n",
            cm.messages_delivered.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_messages_dropped_total Messages dropped in cluster\n");
        out.push_str("# TYPE wse_cluster_messages_dropped_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_dropped_total {}\n",
            cm.messages_dropped.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_bytes_sent_total Bytes sent to cluster peers\n");
        out.push_str("# TYPE wse_cluster_bytes_sent_total counter\n");
        out.push_str(&format!(
            "wse_cluster_bytes_sent_total {}\n",
            cm.bytes_sent.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_bytes_received_total Bytes received from cluster peers\n");
        out.push_str("# TYPE wse_cluster_bytes_received_total counter\n");
        out.push_str(&format!(
            "wse_cluster_bytes_received_total {}\n",
            cm.bytes_received.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_reconnects_total Cluster peer reconnections\n");
        out.push_str("# TYPE wse_cluster_reconnects_total counter\n");
        out.push_str(&format!(
            "wse_cluster_reconnects_total {}\n",
            cm.reconnect_count.load(Ordering::Relaxed)
        ));

        // Recovery metrics (conditional)
        if let Some(ref recovery) = self.shared.recovery {
            out.push_str("# HELP wse_recovery_topics Topics with recovery buffers\n");
            out.push_str("# TYPE wse_recovery_topics gauge\n");
            out.push_str(&format!("wse_recovery_topics {}\n", recovery.topic_count()));

            out.push_str("# HELP wse_recovery_bytes Total bytes in recovery buffers\n");
            out.push_str("# TYPE wse_recovery_bytes gauge\n");
            out.push_str(&format!("wse_recovery_bytes {}\n", recovery.total_bytes()));
        }

        // Per-topic message counts (top 50 by volume)
        // Evict stale entries when map grows too large (prevents unbounded growth
        // for servers using user-scoped or UUID-keyed topics)
        {
            let map = &self.shared.topic_message_counts;
            if map.len() > 10_000 {
                let active_topics: std::collections::HashSet<String> = self
                    .shared
                    .topic_subscribers
                    .iter()
                    .map(|e| e.key().clone())
                    .collect();
                map.retain(|k, _| active_topics.contains(k));
            }
            if !map.is_empty() {
                let mut entries: Vec<(String, u64)> = map
                    .iter()
                    .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
                    .collect();
                entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.1));
                entries.truncate(50);

                out.push_str(
                    "# HELP wse_topic_messages_total Messages delivered per topic (top 50)\n",
                );
                out.push_str("# TYPE wse_topic_messages_total counter\n");
                for (topic, count) in &entries {
                    // Escape label value per Prometheus exposition format
                    let safe_topic = topic
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\n', "\\n");
                    out.push_str(&format!(
                        "wse_topic_messages_total{{topic=\"{}\"}} {}\n",
                        safe_topic, count
                    ));
                }
            }
        }

        // Presence metrics (conditional)
        if let Some(ref pm) = self.shared.presence {
            out.push_str("# HELP wse_presence_topics Topics with presence tracking\n");
            out.push_str("# TYPE wse_presence_topics gauge\n");
            out.push_str(&format!("wse_presence_topics {}\n", pm.total_topics()));

            out.push_str("# HELP wse_presence_users Total users across all topics\n");
            out.push_str("# TYPE wse_presence_users gauge\n");
            out.push_str(&format!("wse_presence_users {}\n", pm.total_users()));
        }

        out
    }

    pub(super) fn get_cluster_dlq_entries_impl(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let entries = match self.shared.cluster_dlq.try_lock() {
            Ok(mut dlq) => dlq.drain_all(),
            Err(_) => Vec::new(),
        };
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
