//! Joining the cluster mesh, cluster TLS reload and the peer list.

use std::sync::Arc;

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
        listen: (Option<u16>, Option<String>, bool),
        seeds: Option<Vec<String>>,
        cluster_addr: Option<String>,
    ) -> PyResult<()> {
        let (tls_cert, tls_key, tls_ca) = tls;
        let (cluster_port, cluster_host, single_port) = listen;

        if self.shared.cluster_cmd_tx.load().is_some() {
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

        // Where peers' links come in. The WebSocket port takes them only when
        // asked to: there anyone who reaches the port could join as a peer.
        let invalid = |msg: &str| Err(pyo3::exceptions::PyValueError::new_err(msg.to_owned()));
        match (cluster_port, single_port) {
            (Some(_), true) => {
                return invalid("cluster_port and single_port=True exclude each other");
            }
            (None, false) => {
                return invalid(
                    "connect_cluster needs cluster_port, the port peers' links come in on; \
                     or single_port=True to take them on the WebSocket port, unauthenticated, \
                     where that port is reachable only from the cluster's own network",
                );
            }
            _ => {}
        }
        if single_port && tls_config.is_some() {
            return invalid("cluster TLS needs cluster_port: single_port links are plaintext");
        }
        if cluster_host.is_some() && cluster_port.is_none() {
            return invalid("cluster_host needs cluster_port");
        }

        let rt_handle = self
            .runtime()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;
        let fanout = self
            .shared
            .fanout
            .read()
            .unwrap()
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;

        // Bound here, so a port in use is an error of this call.
        let (inbound_links, main_port_tx) = match cluster_port {
            Some(port) => {
                let host = cluster_host.as_deref().unwrap_or("0.0.0.0");
                let listener = std::net::TcpListener::bind((host, port))
                    .and_then(|listener| {
                        listener.set_nonblocking(true)?;
                        let _runtime = rt_handle.enter();
                        tokio::net::TcpListener::from_std(listener)
                    })
                    .map_err(|e| {
                        pyo3::exceptions::PyOSError::new_err(format!(
                            "cannot listen for cluster links on {host}:{port}: {e}"
                        ))
                    })?;
                tracing::info!("[WSE-Cluster] Cluster listener on {host}:{port}");
                (crate::wse::cluster::InboundLinks::Listener(listener), None)
            }
            None => {
                tracing::warn!(
                    "[WSE-Cluster] single_port: cluster links are taken on the WebSocket port, \
                     unauthenticated"
                );
                let (tx, rx) = mpsc::unbounded_channel();
                (crate::wse::cluster::InboundLinks::MainPort(rx), Some(tx))
            }
        };

        let tls_holder = tls_config.map(|cfg| Arc::new(arc_swap::ArcSwap::from_pointee(cfg)));
        *self.shared.cluster_tls_holder.write().unwrap() = tls_holder.clone();

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
        self.shared.cluster_cmd_tx.store(Some(Arc::new(cmd_tx)));

        let (interest_tx, interest_rx) = mpsc::channel::<crate::wse::cluster::InterestUpdate>(
            crate::wse::cluster::MANAGER_QUEUE_CAP,
        );
        *self.shared.cluster_main_port.write().unwrap() = main_port_tx;

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
            mpsc::channel::<crate::wse::cluster::PresencePeerFrame>(1);

        let ctx = crate::wse::cluster::ClusterContext {
            interest_tx,
            fanout,
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
                inbound_links,
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
