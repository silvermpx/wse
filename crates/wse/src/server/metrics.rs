//! What a server reports of itself: a health snapshot and the Prometheus
//! exposition.

use std::sync::atomic::Ordering;

use super::publisher::Publisher;
use super::state::SharedState;

/// A snapshot of a server's state.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Health {
    pub connections: usize,
    /// Events waiting for the Python application (drain mode).
    pub inbound_queue_depth: usize,
    /// Commands waiting for the command task.
    pub command_queue_depth: usize,
    /// Client messages dropped for want of room in the application's queue.
    pub inbound_dropped: u64,
    pub cluster_peer_count: u64,
    pub cluster_messages_sent: u64,
    pub cluster_messages_delivered: u64,
    pub cluster_messages_dropped: u64,
    pub cluster_bytes_sent: u64,
    pub cluster_bytes_received: u64,
    pub cluster_reconnect_count: u64,
    pub cluster_unknown_message_types: u64,
    /// Messages to peers kept for a retry.
    pub cluster_dlq_size: usize,
    /// Seconds since the server started; 0 while stopped.
    pub uptime_secs: f64,
    /// The crypto library of this build (cluster TLS, JWT, E2E).
    pub crypto: &'static str,
    pub recovery_enabled: bool,
    pub recovery_topic_count: usize,
    pub recovery_total_bytes: usize,
    pub presence_enabled: bool,
    pub presence_topics: usize,
    pub presence_total_users: usize,
}

impl SharedState {
    /// Seconds since the server started; 0 while stopped.
    pub(crate) fn uptime_secs(&self) -> f64 {
        self.started_at
            .lock()
            .map_or(0.0, |started| started.elapsed().as_secs_f64())
    }

    fn prometheus(&self) -> String {
        let shared = self;
        let uptime = self.uptime_secs();
        let mut out = String::with_capacity(4096);

        // Gauges
        out.push_str("# HELP wse_connections Current active WebSocket connections\n");
        out.push_str("# TYPE wse_connections gauge\n");
        out.push_str(&format!(
            "wse_connections {}\n",
            shared.connection_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_queue_depth Drain queue backlog\n");
        out.push_str("# TYPE wse_inbound_queue_depth gauge\n");
        out.push_str(&format!(
            "wse_inbound_queue_depth {}\n",
            shared.inbound_rx.len()
        ));

        out.push_str("# HELP wse_build_info Build facts as labels, value 1\n");
        out.push_str("# TYPE wse_build_info gauge\n");
        out.push_str(&format!(
            "wse_build_info{{version=\"{}\",crypto=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION"),
            crate::crypto::LIBRARY
        ));
        out.push_str("# HELP wse_uptime_seconds Server uptime in seconds\n");
        out.push_str("# TYPE wse_uptime_seconds gauge\n");
        out.push_str(&format!("wse_uptime_seconds {:.1}\n", uptime));

        out.push_str("# HELP wse_topics Number of active topics\n");
        out.push_str("# TYPE wse_topics gauge\n");
        out.push_str(&format!("wse_topics {}\n", shared.topics.subscribers.len()));

        // Counters
        out.push_str("# HELP wse_messages_received_total Total messages received from clients\n");
        out.push_str("# TYPE wse_messages_received_total counter\n");
        out.push_str(&format!(
            "wse_messages_received_total {}\n",
            shared.messages_received_total.sum()
        ));

        out.push_str(
            "# HELP wse_command_queue_depth Commands from Python waiting for the server\n",
        );
        out.push_str("# TYPE wse_command_queue_depth gauge\n");
        out.push_str(&format!(
            "wse_command_queue_depth {}\n",
            shared.commands_queued.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_messages_sent_total Total messages sent to clients\n");
        out.push_str("# TYPE wse_messages_sent_total counter\n");
        out.push_str(&format!(
            "wse_messages_sent_total {}\n",
            shared.messages_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_accepted_total Total connections accepted\n");
        out.push_str("# TYPE wse_connections_accepted_total counter\n");
        out.push_str(&format!(
            "wse_connections_accepted_total {}\n",
            shared.connections_accepted_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_rejected_total Total connections rejected\n");
        out.push_str("# TYPE wse_connections_rejected_total counter\n");
        out.push_str(&format!(
            "wse_connections_rejected_total {}\n",
            shared.connections_rejected_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_bytes_received_total Total bytes received from clients\n");
        out.push_str("# TYPE wse_bytes_received_total counter\n");
        out.push_str(&format!(
            "wse_bytes_received_total {}\n",
            shared.bytes_received_total.sum()
        ));

        out.push_str("# HELP wse_bytes_sent_total Total bytes sent to clients\n");
        out.push_str("# TYPE wse_bytes_sent_total counter\n");
        out.push_str(&format!(
            "wse_bytes_sent_total {}\n",
            shared.bytes_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_auth_failures_total Total authentication failures\n");
        out.push_str("# TYPE wse_auth_failures_total counter\n");
        out.push_str(&format!(
            "wse_auth_failures_total {}\n",
            shared.auth_failures_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_rate_limited_total Total messages dropped by rate limiter\n");
        out.push_str("# TYPE wse_rate_limited_total counter\n");
        out.push_str(&format!(
            "wse_rate_limited_total {}\n",
            shared.rate_limited_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_dropped_total Drain queue events dropped\n");
        out.push_str("# TYPE wse_inbound_dropped_total counter\n");
        out.push_str(&format!(
            "wse_inbound_dropped_total {}\n",
            shared.inbound_dropped.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP wse_slow_consumer_drops_total Messages dropped due to slow consumers\n",
        );
        out.push_str("# TYPE wse_slow_consumer_drops_total counter\n");
        out.push_str(&format!(
            "wse_slow_consumer_drops_total {}\n",
            shared.slow_consumer_drops.sum()
        ));

        // Cluster metrics
        let cm = &shared.cluster_metrics;
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
        if let Some(ref recovery) = shared.recovery {
            out.push_str("# HELP wse_recovery_topics Topics with recovery buffers\n");
            out.push_str("# TYPE wse_recovery_topics gauge\n");
            out.push_str(&format!("wse_recovery_topics {}\n", recovery.topic_count()));

            out.push_str("# HELP wse_recovery_bytes Total bytes in recovery buffers\n");
            out.push_str("# TYPE wse_recovery_bytes gauge\n");
            out.push_str(&format!("wse_recovery_bytes {}\n", recovery.total_bytes()));
        }

        // Per-topic message counts (top 50 by volume)
        {
            let map = &shared.topic_message_counts;
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
        if let Some(ref pm) = shared.presence {
            out.push_str("# HELP wse_presence_topics Topics with presence tracking\n");
            out.push_str("# TYPE wse_presence_topics gauge\n");
            out.push_str(&format!("wse_presence_topics {}\n", pm.total_topics()));

            out.push_str("# HELP wse_presence_users Total users across all topics\n");
            out.push_str("# TYPE wse_presence_users gauge\n");
            out.push_str(&format!("wse_presence_users {}\n", pm.total_users()));
        }

        out
    }
}

impl Publisher {
    /// The server's state now.
    pub fn health(&self) -> Health {
        let shared = &self.shared;
        let cm = &shared.cluster_metrics;
        let (recovery_topic_count, recovery_total_bytes) = shared
            .recovery
            .as_ref()
            .map_or((0, 0), |r| (r.topic_count(), r.total_bytes()));
        let (presence_topics, presence_total_users) = shared
            .presence
            .as_ref()
            .map_or((0, 0), |pm| (pm.total_topics(), pm.total_users()));
        Health {
            connections: shared.connection_count.load(Ordering::Relaxed),
            inbound_queue_depth: shared.inbound_rx.len(),
            command_queue_depth: shared.commands_queued.load(Ordering::Relaxed),
            inbound_dropped: shared.inbound_dropped.load(Ordering::Relaxed),
            cluster_peer_count: cm.connected_peers.load(Ordering::Relaxed),
            cluster_messages_sent: cm.messages_sent.load(Ordering::Relaxed),
            cluster_messages_delivered: cm.messages_delivered.load(Ordering::Relaxed),
            cluster_messages_dropped: cm.messages_dropped.load(Ordering::Relaxed),
            cluster_bytes_sent: cm.bytes_sent.load(Ordering::Relaxed),
            cluster_bytes_received: cm.bytes_received.load(Ordering::Relaxed),
            cluster_reconnect_count: cm.reconnect_count.load(Ordering::Relaxed),
            cluster_unknown_message_types: cm.unknown_message_types.load(Ordering::Relaxed),
            cluster_dlq_size: shared.cluster_dlq.try_lock().map(|d| d.len()).unwrap_or(0),
            uptime_secs: shared.uptime_secs(),
            crypto: crate::crypto::LIBRARY,
            recovery_enabled: shared.recovery.is_some(),
            recovery_topic_count,
            recovery_total_bytes,
            presence_enabled: shared.presence.is_some(),
            presence_topics,
            presence_total_users,
        }
    }

    /// The server's metrics in the Prometheus text exposition format.
    pub fn prometheus_metrics(&self) -> String {
        self.shared.prometheus()
    }
}
