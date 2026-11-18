//! Joining a server to its cluster: the peers it links to, the port their
//! links come in on, TLS between nodes.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use super::handle::Server;
use super::publisher::{Error, Publisher};
use super::state::SharedState;
use crate::cluster::{self, ClusterCommand};

/// The PEM files of a cluster's mutual TLS, by their paths: this node's
/// certificate and key, and the CA every node's certificate is checked
/// against.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ClusterTls {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

impl ClusterTls {
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>, ca: impl Into<PathBuf>) -> Self {
        Self {
            cert: cert.into(),
            key: key.into(),
            ca: ca.into(),
        }
    }
}

/// How a server joins its cluster. Its links are TLS, always: from the
/// cluster's `secret`, or from certificate files (`tls`).
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ClusterSettings {
    /// Peers to link to (`host:port`; an IPv6 address with its zone by
    /// number, `[fe80::1%2]:9999`).
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
    /// Take peers' links on the WebSocket port instead of `port`, in TLS as
    /// on their own port.
    pub single_port: bool,
    /// The cluster's shared secret, at least 32 random bytes
    /// (`openssl rand -base64 32`): every node derives the same key from it
    /// and proves it in each link's TLS handshake, so a node without it
    /// cannot link. No certificates to make. It is wiped once read.
    pub secret: Option<Vec<u8>>,
    /// Mutual TLS from certificate files, instead of `secret`.
    pub tls: Option<ClusterTls>,
}

impl std::fmt::Debug for ClusterSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterSettings")
            .field("peers", &self.peers)
            .field("seeds", &self.seeds)
            .field("cluster_addr", &self.cluster_addr)
            .field("port", &self.port)
            .field("host", &self.host)
            .field("single_port", &self.single_port)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("tls", &self.tls)
            .finish()
    }
}

/// A peer this node is linked to.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterPeer {
    pub address: String,
    pub instance_id: String,
    pub capabilities: u32,
    /// When the link came up (milliseconds since the Unix epoch).
    pub connected_at: u64,
}

/// A message a peer did not take, kept for a retry.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeadLetter {
    pub topic: String,
    pub payload: String,
    pub peer_addr: String,
    pub error: String,
}

/// Whether `addr` gives an IPv6 zone by an interface's name
/// (`[fe80::1%eth0]:7000`). The cluster takes a zone by its number only
/// (`[fe80::1%2]:7000`): a link cannot be dialed by the name, and a node
/// that gave the name once and the number after would be two places.
pub(super) fn names_a_zone(addr: &str) -> bool {
    addr.rsplit_once(':')
        .and_then(|(host, _)| host.strip_prefix('['))
        .and_then(|host| host.strip_suffix(']'))
        .and_then(|host| host.split_once('%'))
        .is_some_and(|(_, zone)| zone.parse::<u32>().is_err())
}

/// Join the cluster: on the current tokio runtime, with the server
/// running.
pub(crate) fn connect(
    shared: &Arc<SharedState>,
    mut settings: ClusterSettings,
) -> Result<(), Error> {
    use zeroize::Zeroize;
    let invalid = |msg: &str| Err(Error::ClusterConfig(msg.to_owned()));
    if shared.cluster_cmd_tx.load().is_some() {
        return Err(Error::Cluster("Cluster already connected".to_owned()));
    }
    if settings.seeds.is_some() && settings.cluster_addr.is_none() {
        return Err(Error::Cluster(
            "seeds= requires cluster_addr= so peers can connect back to this node".to_owned(),
        ));
    }
    if let Some(addr) = settings
        .peers
        .iter()
        .chain(settings.seeds.iter().flatten())
        .chain(settings.cluster_addr.iter())
        .find(|addr| names_a_zone(addr))
    {
        return invalid(&format!(
            "{addr}: give an IPv6 zone by its number ([fe80::1%2]:9999), not by an interface's name"
        ));
    }
    // Links are TLS, always: a plaintext cluster would take anyone who
    // reaches its port as a peer, reading every topic and publishing to any.
    let tls_config = match (settings.secret.take(), settings.tls.as_ref()) {
        (Some(mut secret), None) => {
            let config = cluster::build_cluster_tls_from_secret(&secret);
            secret.zeroize();
            config.map_err(|e| Error::ClusterConfig(e.to_string()))?
        }
        (None, Some(tls)) => cluster::build_cluster_tls(&tls.cert, &tls.key, &tls.ca)
            .map_err(|e| Error::Cluster(format!("TLS config error: {e}")))?,
        (Some(mut secret), Some(_)) => {
            secret.zeroize();
            return invalid("a cluster secret or TLS certificates, not both");
        }
        (None, None) => {
            return invalid(
                "the cluster needs its secret (at least 32 random bytes: \
                 openssl rand -base64 32), or TLS certificates: its links are TLS, \
                 so a node without them cannot join",
            );
        }
    };

    // Where peers' links come in: their own port, or the WebSocket port
    // when asked to.
    match (settings.port, settings.single_port) {
        (Some(_), true) => {
            return invalid("cluster_port and single_port=True exclude each other");
        }
        (None, false) => {
            return invalid(
                "connect_cluster needs cluster_port, the port peers' links come in on; \
                 or single_port=True to take them on the WebSocket port",
            );
        }
        _ => {}
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
    // The port peers' links come in on, as bound.
    let (inbound_links, main_port_tx, links_port) = match settings.port {
        Some(port) => {
            let host = settings.host.as_deref().unwrap_or("0.0.0.0");
            let listener = std::net::TcpListener::bind((host, port))
                .and_then(|listener| {
                    listener.set_nonblocking(true)?;
                    tokio::net::TcpListener::from_std(listener)
                })
                .map_err(|e| {
                    Error::Io(
                        e.kind(),
                        format!("cannot listen for cluster links on {host}:{port}: {e}"),
                    )
                })?;
            tracing::info!("[WSE-Cluster] Cluster listener on {host}:{port}");
            let bound = listener.local_addr().map_or(port, |addr| addr.port());
            (cluster::InboundLinks::Listener(listener), None, bound)
        }
        None => {
            tracing::info!(
                "[WSE-Cluster] single_port: cluster links come in on the WebSocket port"
            );
            let (tx, rx) = mpsc::unbounded_channel();
            let port = shared.listen_port.load(Ordering::Relaxed);
            (cluster::InboundLinks::MainPort(rx), Some(tx), port)
        }
    };
    // Where peers reach this node, as its HELLO says: a node that comes up
    // there anew is known for one, whichever of the two dials.
    let reached_at = match (&settings.cluster_addr, links_port) {
        (Some(addr), _) => addr.clone(),
        (None, 0) => String::new(),
        (None, port) => format!(":{port}"),
    };

    let tls_holder = Some(Arc::new(arc_swap::ArcSwap::from_pointee(tls_config)));
    *shared.cluster_tls_holder.write().unwrap() = tls_holder.clone();

    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
    shared.cluster_cmd_tx.store(Some(Arc::new(cmd_tx)));

    let (interest_tx, interest_rx) =
        mpsc::channel::<cluster::InterestUpdate>(cluster::MANAGER_QUEUE_CAP);
    *shared.cluster_main_port.write().unwrap() = main_port_tx;

    // The name clients know this node by: one coming from it to another
    // node names it, and that node asks it what it took.
    let instance_id = shared.node_id.to_string();
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
        producers: Arc::clone(&shared.producers),
        producer_links: Arc::clone(&shared.producer_links),
        heard: Arc::new(dashmap::DashMap::new()),
        incarnations: Arc::new(dashmap::DashMap::new()),
        leaving: tokio_util::sync::CancellationToken::new(),
        reached_at,
    };

    // Waited for when the run ends: the node leaves the cluster then.
    let manager = tokio::spawn(cluster::cluster_manager(
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
    *shared.cluster_task.lock() = Some(manager);
    Ok(())
}

impl Server {
    /// Join the cluster: links to its peers, and the port their links come
    /// in on. Call it on the runtime the server runs on, once started.
    ///
    /// # Panics
    ///
    /// Outside a tokio runtime.
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
        if holder.load().from_secret {
            return Err(Error::Cluster(
                "the cluster's TLS comes from its secret: there are no certificates to reload"
                    .to_owned(),
            ));
        }
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
