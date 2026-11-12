//! What the cluster tasks share and exchange: context, commands, registrations,
//! interest updates, metrics and the dead-letter queue.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;

use crate::wse::server::ConnectionHandle;

// ---------------------------------------------------------------------------
// ClusterContext: shared state passed to all cluster peer tasks
// ---------------------------------------------------------------------------

/// Bundles the shared references passed to peer_reader, peer_dispatch_task,
/// run_peer_session, peer_connection_task, and handle_cluster_inbound_generic.
/// Eliminates 15+ repeated arguments from function signatures.
#[derive(Clone)]
pub(crate) struct ClusterContext {
    pub interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    pub topic_subscribers: Arc<DashMap<String, DashMap<String, ConnectionHandle>>>,
    pub glob_topic_count: Arc<AtomicUsize>,
    pub metrics: Arc<ClusterMetrics>,
    pub new_peer_tx: mpsc::UnboundedSender<String>,
    pub cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    pub presence_tx: mpsc::UnboundedSender<PresencePeerFrame>,
    pub connected_instances: Arc<DashMap<String, String>>,
    pub known_peers: Arc<DashSet<String>>,
    pub local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    pub cluster_addr: Option<String>,
    pub max_outbound: usize,
    pub slow_drops: Arc<AtomicU64>,
    pub recovery: Option<Arc<crate::wse::recovery::RecoveryManager>>,
    pub topic_message_counts: Arc<DashMap<String, AtomicU64>>,
    pub queue_groups: Arc<DashMap<String, DashMap<String, crate::wse::server::QueueGroup>>>,
}

// ---------------------------------------------------------------------------
// Commands (Python -> cluster subsystem)
// ---------------------------------------------------------------------------

pub(crate) enum ClusterCommand {
    Publish {
        topic: String,
        payload: String,
    },
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
    Shutdown,
    /// Notify cluster peers that this node is entering drain mode.
    Drain,
    /// Gossip a peer address to all connected peers (except the source).
    GossipPeerAnnounce {
        addr: String,
        exclude_peer: String,
    },
    /// Sync a single presence join/leave/update to cluster peers.
    PresenceUpdate {
        topic: String,
        user_id: String,
        action: u8, // 0 = join, 1 = leave, 2 = update
        data: String,
        updated_at: u64,
    },
}

/// Messages from inbound peer handlers to register/deregister their write channels
/// with the cluster manager, so inbound peers receive Publish/Sub/Unsub frames.
pub(crate) enum PeerRegistration {
    Register {
        peer_addr: String,
        write_tx: mpsc::Sender<Bytes>,
        negotiated_caps: u32,
    },
    Deregister {
        peer_addr: String,
    },
    UpdateCaps {
        peer_addr: String,
        negotiated_caps: u32,
    },
}

/// Presence frames received from a peer, forwarded to cluster_manager for processing.
pub(crate) enum PresencePeerFrame {
    Update {
        /// Reporting peer's address -- keys the per-origin presence sentinel.
        origin: String,
        topic: String,
        user_id: String,
        action: u8,
        data: String,
        updated_at: u64,
    },
    Full {
        /// Reporting peer's address -- keys the per-origin presence sentinels.
        origin: String,
        entries: String,
    },
}

pub(crate) enum InterestUpdate {
    Sub {
        peer_addr: String,
        topic: String,
    },
    Unsub {
        peer_addr: String,
        topic: String,
    },
    Resync {
        peer_addr: String,
        topics: Vec<String>,
        is_continuation: bool,
        /// Session generation -- filters stale continuations from prior connections.
        generation: u64,
    },
    PeerDisconnected {
        peer_addr: String,
        generation: u64,
    },
    /// Peer entered drain mode (lame duck): stop routing new messages to it, but
    /// keep the connection open for in-flight delivery (no presence purge, no
    /// reconnect implications -- unlike PeerDisconnected).
    Drain {
        peer_addr: String,
    },
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub(crate) struct ClusterMetrics {
    pub messages_sent: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub reconnect_count: AtomicU64,
    pub connected_peers: AtomicU64,
    pub unknown_message_types: AtomicU64,
    pub peer_info: DashMap<String, PeerInfo>,
}

#[derive(Clone)]
pub(crate) struct PeerInfo {
    pub address: String,
    pub instance_id: String,
    pub capabilities: u32,
    pub connected_at: u64,
}

impl ClusterMetrics {
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            connected_peers: AtomicU64::new(0),
            unknown_message_types: AtomicU64::new(0),
            peer_info: DashMap::new(),
        }
    }
}

impl ClusterMetrics {
    pub(super) fn add_delivered(&self, count: u64) {
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
    }
    pub(super) fn add_dropped(&self, count: u64) {
        self.messages_dropped.fetch_add(count, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Dead letter queue
// ---------------------------------------------------------------------------

pub(crate) struct ClusterDlqEntry {
    pub topic: String,
    pub payload: String,
    pub peer_addr: String,
    pub error: String,
}

pub(crate) struct ClusterDlq {
    entries: VecDeque<ClusterDlqEntry>,
    max_entries: usize,
}

impl ClusterDlq {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    pub fn push(&mut self, entry: ClusterDlqEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn drain_all(&mut self) -> Vec<ClusterDlqEntry> {
        self.entries.drain(..).collect()
    }
}
