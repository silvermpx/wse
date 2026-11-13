//! Joining the cluster mesh, cluster TLS reload and the peer list.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::sync::mpsc;

use super::RustWSEServer;
use crate::wse::cluster::ClusterCommand;

impl RustWSEServer {
    pub(super) fn connect_cluster_inner(
        &self,
        peers: Vec<String>,
        tls: (Option<String>, Option<String>, Option<String>),
        cluster_port: Option<u16>,
        seeds: Option<Vec<String>>,
        cluster_addr: Option<String>,
    ) -> PyResult<()> {
        let (tls_cert, tls_key, tls_ca) = tls;

        if self.shared.cluster_cmd_tx.read().unwrap().is_some() {
            return Err(PyRuntimeError::new_err("Cluster already connected"));
        }

        if seeds.is_some() && cluster_addr.is_none() {
            return Err(PyRuntimeError::new_err(
                "seeds= requires cluster_addr= so peers can connect back to this node",
            ));
        }

        let tls_config = match (tls_cert, tls_key, tls_ca) {
            (Some(cert), Some(key), Some(ca)) => Some(
                crate::wse::cluster::build_cluster_tls(&cert, &key, &ca)
                    .map_err(|e| PyRuntimeError::new_err(format!("TLS config error: {e}")))?,
            ),
            (None, None, None) => {
                match (
                    std::env::var("WSE_CLUSTER_TLS_CERT").ok(),
                    std::env::var("WSE_CLUSTER_TLS_KEY").ok(),
                    std::env::var("WSE_CLUSTER_TLS_CA").ok(),
                ) {
                    (Some(cert), Some(key), Some(ca)) => Some(
                        crate::wse::cluster::build_cluster_tls(&cert, &key, &ca).map_err(|e| {
                            PyRuntimeError::new_err(format!("TLS config error (from env): {e}"))
                        })?,
                    ),
                    (None, None, None) => None,
                    _ => {
                        return Err(PyRuntimeError::new_err(
                            "Partial TLS env vars: set all of WSE_CLUSTER_TLS_CERT, \
                             WSE_CLUSTER_TLS_KEY, WSE_CLUSTER_TLS_CA or none",
                        ));
                    }
                }
            }
            _ => {
                return Err(PyRuntimeError::new_err(
                    "Partial TLS config: provide all of tls_cert, tls_key, tls_ca or none",
                ));
            }
        };

        self.shared
            .cluster_tls_enabled
            .store(tls_config.is_some(), Ordering::Relaxed);

        let tls_holder = tls_config.map(|cfg| Arc::new(arc_swap::ArcSwap::from_pointee(cfg)));
        *self.shared.cluster_tls_holder.write().unwrap() = tls_holder.clone();

        let rt_handle = self
            .runtime()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
        *self.shared.cluster_cmd_tx.write().unwrap() = Some(cmd_tx);

        let (interest_tx, interest_rx) =
            mpsc::unbounded_channel::<crate::wse::cluster::InterestUpdate>();
        *self.shared.cluster_interest_tx.write().unwrap() = Some(interest_tx.clone());

        let instance_id = uuid::Uuid::now_v7().to_string();
        *self.shared.cluster_instance_id.lock().unwrap() = Some(instance_id.clone());
        let dlq = self.shared.cluster_dlq.clone();

        let effective_peers = if let Some(ref s) = seeds {
            s.clone()
        } else {
            peers
        };

        let (placeholder_new_peer_tx, _) = mpsc::unbounded_channel::<String>();
        let (placeholder_cmd_tx, _) =
            mpsc::unbounded_channel::<crate::wse::cluster::ClusterCommand>();
        let (placeholder_presence_tx, _) =
            mpsc::unbounded_channel::<crate::wse::cluster::PresencePeerFrame>();

        let ctx = crate::wse::cluster::ClusterContext {
            interest_tx,
            topic_subscribers: self.shared.topic_subscribers.clone(),
            glob_topic_count: self.shared.glob_topic_count.clone(),
            metrics: self.shared.cluster_metrics.clone(),
            new_peer_tx: placeholder_new_peer_tx,
            cmd_tx: placeholder_cmd_tx,
            presence_tx: placeholder_presence_tx,
            links: Arc::new(dashmap::DashMap::new()),
            known_peers: Arc::new(dashmap::DashSet::new()),
            local_topic_refcount: self.shared.local_topic_refcount.clone(),
            cluster_addr,
            max_outbound: self.shared.max_outbound_queue_bytes,
            slow_drops: Arc::clone(&self.shared.slow_consumer_drops),
            recovery: self.shared.recovery.clone(),
            topic_message_counts: self.shared.topic_message_counts.clone(),
            queue_groups: self.shared.queue_groups.clone(),
        };

        rt_handle.spawn(crate::wse::cluster::cluster_manager(
            effective_peers,
            instance_id,
            (cmd_rx, interest_rx),
            ctx,
            dlq,
            tls_holder,
            (
                cluster_port,
                self.shared.presence.clone(),
                self.cmd_tx.load_full().map(|tx| (*tx).clone()),
            ),
        ));

        Ok(())
    }

    pub(super) fn reload_cluster_tls_impl(
        &self,
        cert_path: String,
        key_path: String,
        ca_path: String,
    ) -> PyResult<()> {
        let guard = self.shared.cluster_tls_holder.read().unwrap();
        let holder = guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Cluster TLS not configured")
        })?;
        let new_config = crate::wse::cluster::build_cluster_tls(&cert_path, &key_path, &ca_path)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("TLS reload failed: {e}"))
            })?;
        holder.store(std::sync::Arc::new(new_config));
        tracing::info!("[WSE] Cluster TLS certificates reloaded");
        Ok(())
    }

    pub(super) fn cluster_info_impl(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        // Copy first, build Python objects after: no map shard lock is held
        // while Python allocates (a GC pause then would wait on any Python
        // thread blocked on that shard).
        let peers: Vec<crate::wse::cluster::PeerInfo> = self
            .shared
            .cluster_metrics
            .peer_info
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        let list = PyList::empty(py);
        for peer in &peers {
            let d = PyDict::new(py);
            d.set_item("address", &peer.address)?;
            d.set_item("instance_id", &peer.instance_id)?;
            d.set_item("capabilities", peer.capabilities)?;
            d.set_item("connected_at", peer.connected_at)?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }
}
