//! Joining a server to its cluster: the peers it links to, the port their
//! links come in on, TLS between nodes.

use std::sync::Arc;

use tokio::sync::mpsc;

use super::handle::Server;
use super::publisher::{Error, Publisher};
use super::state::SharedState;
use crate::cluster::{self, ClusterCommand};

/// The PEM files of a cluster's mutual TLS: this node's certificate and key,
/// and the CA every node's certificate is checked against.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterTls {
    pub cert: String,
    pub key: String,
    pub ca: String,
}

impl ClusterTls {
    pub fn new(cert: impl Into<String>, key: impl Into<String>, ca: impl Into<String>) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            ca: ca.into(),
        }
    }
}

/// How a server joins its cluster.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ClusterSettings {
    /// Peers to link to (`host:port`).
    pub peers: Vec<String>,
    /// Seed nodes instead of a full peer list: the rest is learned from them.
    /// Needs `cluster_addr`.
    pub seeds: Option<Vec<String>>,
    /// The address the other nodes reach this one at (`host:port`).
    pub cluster_addr: Option<String>,
    /// The port peers' links come in on.
    pub port: Option<u16>,
    /// The address that port listens on; 0.0.0.0 by default.
    pub host: Option<String>,
    /// Take peers' links on the WebSocket port instead of `port`: plaintext
    /// and unauthenticated, for a port only the cluster's own network
    /// reaches.
    pub single_port: bool,
    pub tls: Option<ClusterTls>,
}

/// A peer this node is linked to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterPeer {
    pub address: String,
    pub instance_id: String,
    pub capabilities: u32,
    /// When the link came up (milliseconds since the Unix epoch).
    pub connected_at: u64,
}

/// A message a peer did not take, kept for a retry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetter {
    pub topic: String,
    pub payload: String,
    pub peer_addr: String,
    pub error: String,
}

/// Join the cluster: on the current tokio runtime, with the server
/// running.
pub(crate) fn connect(shared: &Arc<SharedState>, settings: ClusterSettings) -> Result<(), Error> {
    if shared.cluster_cmd_tx.load().is_some() {
        return Err(Error::Cluster("Cluster already connected".to_owned()));
    }
    if settings.seeds.is_some() && settings.cluster_addr.is_none() {
        return Err(Error::Cluster(
            "seeds= requires cluster_addr= so peers can connect back to this node".to_owned(),
        ));
    }
    let tls_config = settings
        .tls
        .as_ref()
        .map(|tls| {
            cluster::build_cluster_tls(&tls.cert, &tls.key, &tls.ca)
                .map_err(|e| Error::Cluster(format!("TLS config error: {e}")))
        })
        .transpose()?;

    // Where peers' links come in. The WebSocket port takes them only when
    // asked to: there anyone who reaches the port could join as a peer.
    let invalid = |msg: &str| Err(Error::ClusterConfig(msg.to_owned()));
    match (settings.port, settings.single_port) {
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
    if settings.single_port && tls_config.is_some() {
        return invalid("cluster TLS needs cluster_port: single_port links are plaintext");
    }
    if settings.host.is_some() && settings.port.is_none() {
        return invalid("cluster_host needs cluster_port");
    }
    let fanout = shared
        .fanout
        .read()
        .unwrap()
        .clone()
        .ok_or_else(|| Error::Cluster("Server not started".to_owned()))?;

    // Bound here, so a port in use is an error of this call.
    let (inbound_links, main_port_tx) = match settings.port {
        Some(port) => {
            let host = settings.host.as_deref().unwrap_or("0.0.0.0");
            let listener = std::net::TcpListener::bind((host, port))
                .and_then(|listener| {
                    listener.set_nonblocking(true)?;
                    tokio::net::TcpListener::from_std(listener)
                })
                .map_err(|e| {
                    Error::Io(format!(
                        "cannot listen for cluster links on {host}:{port}: {e}"
                    ))
                })?;
            tracing::info!("[WSE-Cluster] Cluster listener on {host}:{port}");
            (cluster::InboundLinks::Listener(listener), None)
        }
        None => {
            tracing::warn!(
                "[WSE-Cluster] single_port: cluster links are taken on the WebSocket port, \
                 unauthenticated"
            );
            let (tx, rx) = mpsc::unbounded_channel();
            (cluster::InboundLinks::MainPort(rx), Some(tx))
        }
    };

    let tls_holder = tls_config.map(|cfg| Arc::new(arc_swap::ArcSwap::from_pointee(cfg)));
    *shared.cluster_tls_holder.write().unwrap() = tls_holder.clone();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
    shared.cluster_cmd_tx.store(Some(Arc::new(cmd_tx)));

    let (interest_tx, interest_rx) =
        mpsc::channel::<cluster::InterestUpdate>(cluster::MANAGER_QUEUE_CAP);
    *shared.cluster_main_port.write().unwrap() = main_port_tx;

    let instance_id = uuid::Uuid::now_v7().to_string();
    *shared.cluster_instance_id.lock().unwrap() = Some(instance_id.clone());
    let dlq = shared.cluster_dlq.clone();

    let peers = settings.seeds.unwrap_or(settings.peers);

    let (placeholder_new_peer_tx, _) = mpsc::unbounded_channel::<String>();
    let (placeholder_cmd_tx, _) = mpsc::unbounded_channel::<ClusterCommand>();
    let (placeholder_presence_tx, _) = mpsc::channel::<cluster::PresencePeerFrame>(1);

    let ctx = cluster::ClusterContext {
        interest_tx,
        fanout,
        metrics: shared.cluster_metrics.clone(),
        new_peer_tx: placeholder_new_peer_tx,
        cmd_tx: placeholder_cmd_tx,
        presence_tx: placeholder_presence_tx,
        links: Arc::new(dashmap::DashMap::new()),
        known_peers: Arc::new(dashmap::DashSet::new()),
        local_topic_refcount: shared.local_topic_refcount.clone(),
        cluster_addr: settings.cluster_addr,
        max_outbound: shared.max_outbound_queue_bytes,
        slow_drops: Arc::clone(&shared.slow_consumer_drops),
        recovery: shared.recovery.clone(),
        topic_message_counts: shared.topic_message_counts.clone(),
        queue_groups: shared.queue_groups.clone(),
    };

    tokio::spawn(cluster::cluster_manager(
        peers,
        instance_id,
        (cmd_rx, interest_rx),
        ctx,
        dlq,
        tls_holder,
        (
            inbound_links,
            shared.presence.clone(),
            shared
                .commands
                .load_full()
                .map(|commands| (*commands).clone()),
        ),
    ));
    Ok(())
}

impl Server {
    /// Join the cluster: links to its peers, and the port their links come
    /// in on. Call it on the runtime the server runs on, once started.
    pub fn connect_cluster(&self, settings: ClusterSettings) -> Result<(), Error> {
        connect(&self.publisher().shared, settings)
    }
}

impl Publisher {
    /// Whether this node is linked to at least one peer.
    pub fn cluster_connected(&self) -> bool {
        self.shared
            .cluster_metrics
            .connected_peers
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    }

    /// The peers this node is linked to.
    pub fn cluster_peers(&self) -> Vec<ClusterPeer> {
        self.shared
            .cluster_metrics
            .peer_info
            .iter()
            .map(|entry| {
                let peer = entry.value();
                ClusterPeer {
                    address: peer.address.clone(),
                    instance_id: peer.instance_id.clone(),
                    capabilities: peer.capabilities,
                    connected_at: peer.connected_at,
                }
            })
            .collect()
    }

    /// New certificates for the cluster's TLS: links made from now on use
    /// them.
    pub fn reload_cluster_tls(&self, tls: &ClusterTls) -> Result<(), Error> {
        let guard = self.shared.cluster_tls_holder.read().unwrap();
        let holder = guard
            .as_ref()
            .ok_or_else(|| Error::Cluster("Cluster TLS not configured".to_owned()))?;
        let config = cluster::build_cluster_tls(&tls.cert, &tls.key, &tls.ca)
            .map_err(|e| Error::Cluster(format!("TLS reload failed: {e}")))?;
        holder.store(Arc::new(config));
        tracing::info!("[WSE] Cluster TLS certificates reloaded");
        Ok(())
    }

    /// The messages peers did not take, taken out of the dead-letter queue.
    pub fn take_dead_letters(&self) -> Vec<DeadLetter> {
        match self.shared.cluster_dlq.try_lock() {
            Ok(mut dlq) => dlq
                .drain_all()
                .into_iter()
                .map(|entry| DeadLetter {
                    topic: entry.topic,
                    payload: entry.payload,
                    peer_addr: entry.peer_addr,
                    error: entry.error,
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}
