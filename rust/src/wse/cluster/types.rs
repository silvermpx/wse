//! What the cluster tasks share and exchange: context, commands, registrations,
//! interest updates, metrics and the dead-letter queue.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::wse::server::ConnectionHandle;

// ---------------------------------------------------------------------------
// Live links, one per peer instance
// ---------------------------------------------------------------------------

/// The live link to one peer instance.
pub(crate) struct Link {
    /// Instance id of the node that dialed this link.
    pub dialer: String,
    /// Session number: a link's cleanup removes only its own entry, never the
    /// link that replaced it.
    pub session: u64,
    /// Ends the link's tasks; a link that a better duplicate replaces is
    /// closed through it.
    pub cancel: CancellationToken,
}

/// Live links by peer instance id.
pub(crate) type Links = DashMap<String, Link>;

/// Why a new link is not kept.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LinkRefused {
    /// The peer is this node itself (its own address in the peer list).
    SelfLink,
    /// Another link to the same instance exists and wins.
    Duplicate,
}

/// Register a new link to the instance `peer`, dialed by `link.dialer`.
///
/// Any node may dial any other, so a newly started node can join through an
/// older one. Two links to one instance appear when two nodes dial each other
/// at the same moment, or when one node knows a peer under two addresses.
/// Both nodes must then keep the same link, so the choice depends only on the
/// two links: the one dialed by the lower instance id wins. A peer that dials
/// again while we still hold its earlier link has given up on that one (a
/// half-open connection), so the new link replaces it; a second link dialed by
/// this node is surplus.
pub(crate) fn claim_link(
    links: &Links,
    local: &str,
    peer: &str,
    link: Link,
) -> Result<(), LinkRefused> {
    use dashmap::mapref::entry::Entry;
    if peer == local {
        return Err(LinkRefused::SelfLink);
    }
    match links.entry(peer.to_string()) {
        Entry::Vacant(e) => {
            e.insert(link);
            Ok(())
        }
        Entry::Occupied(mut e) => {
            let existing = e.get();
            let replace = if existing.dialer == link.dialer {
                link.dialer == peer
            } else {
                link.dialer < existing.dialer
            };
            if !replace {
                return Err(LinkRefused::Duplicate);
            }
            e.get().cancel.cancel();
            e.insert(link);
            Ok(())
        }
    }
}

/// The topics one peer has subscribers for. Exact names and glob patterns
/// are kept apart, so routing a publish costs one lookup plus the patterns,
/// not a pass over every topic the peer holds.
#[derive(Default)]
pub(crate) struct PeerInterest {
    exact: ahash::AHashSet<String>,
    patterns: Vec<String>,
}

impl PeerInterest {
    fn is_pattern(topic: &str) -> bool {
        topic.contains('*') || topic.contains('?')
    }

    pub fn len(&self) -> usize {
        self.exact.len() + self.patterns.len()
    }

    /// Add a topic unless the peer already holds `cap` of them. Returns
    /// whether the topic is held afterwards.
    pub fn insert(&mut self, topic: String, cap: usize) -> bool {
        if Self::is_pattern(&topic) {
            if self.patterns.contains(&topic) {
                return true;
            }
            if self.len() >= cap {
                return false;
            }
            self.patterns.push(topic);
        } else {
            if self.exact.contains(&topic) {
                return true;
            }
            if self.len() >= cap {
                return false;
            }
            self.exact.insert(topic);
        }
        true
    }

    pub fn remove(&mut self, topic: &str) {
        if Self::is_pattern(topic) {
            self.patterns.retain(|p| p != topic);
        } else {
            self.exact.remove(topic);
        }
    }

    /// Whether a publish to `topic` reaches a subscriber on this peer.
    pub fn wants(&self, topic: &str) -> bool {
        self.exact.contains(topic)
            || self
                .patterns
                .iter()
                .any(|p| crate::wse::server::glob_match(p, topic))
    }
}

/// Forget the link to `peer` if it is still the one from `session`.
pub(crate) fn release_link(links: &Links, peer: &str, session: u64) {
    links.remove_if(peer, |_, link| link.session == session);
}

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
    pub links: Arc<Links>,
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
        /// The message as local subscribers got it: stamped with its recovery
        /// position when recovery is on.
        payload: String,
        /// That position (epoch, offset), carried in the frame so peers store
        /// the message for cross-node recovery under the same position.
        position: Option<(u32, u64)>,
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
