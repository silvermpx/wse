//! What the cluster tasks share and exchange: context, commands, registrations,
//! interest updates, metrics and the dead-letter queue.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

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
    /// When the link last received anything, on `epoch_ms`: a link of the
    /// same dialer is replaced only once it fell silent.
    pub activity: Arc<AtomicU64>,
}

/// A link that received nothing for this long (two heartbeats) is taken
/// for dead when its dialer links again.
const LINK_SILENT_MS: u64 = super::HEARTBEAT_INTERVAL_SECS * 2 * 1000;

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
/// two links: the one dialed by the lower instance id wins. A node that dials
/// again while its earlier link is silent has given up on that one (a
/// half-open connection), so the new link replaces it; while the earlier link
/// is alive the node knows the other under two addresses, and the new link
/// is surplus on both sides.
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
                super::epoch_ms().saturating_sub(existing.activity.load(Ordering::Relaxed))
                    > LINK_SILENT_MS
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
                .any(|p| crate::server::glob_match(p, topic))
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
    pub interest_tx: mpsc::Sender<InterestUpdate>,
    pub fanout: crate::server::Fanout,
    pub metrics: Arc<ClusterMetrics>,
    pub new_peer_tx: mpsc::UnboundedSender<String>,
    pub cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    pub presence_tx: mpsc::Sender<PresencePeerFrame>,
    pub links: Arc<Links>,
    pub known_peers: Arc<DashSet<String>>,
    pub local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    pub cluster_addr: Option<String>,
    pub max_outbound: usize,
    pub slow_drops: Arc<crate::server::Tally>,
    pub recovery: Option<Arc<crate::recovery::RecoveryManager>>,
    pub topic_message_counts: Arc<DashMap<String, AtomicU64>>,
    pub queue_groups: Arc<DashMap<String, DashMap<String, crate::server::QueueGroup>>>,
    /// The clients' producers on this node, for a peer that asks.
    pub producers: Arc<crate::server::Producers>,
    /// The links that answer for producers, and the questions out.
    pub producer_links: Arc<super::ProducerLinks>,
    /// What this node heard of each peer, by its instance id: where the
    /// catch-up starts once a lost link to it is back (CAP_ARRIVAL).
    pub heard: Arc<DashMap<String, Arc<Heard>>>,
    /// The instance last met at each address this node dials: one that is
    /// another node now, after a link that ended without SHUTDOWN, took
    /// what it published meanwhile with it.
    pub incarnations: Arc<DashMap<String, String>>,
    /// Cancelled once this node leaves the cluster: every link ends, the
    /// ones that may saying SHUTDOWN.
    pub leaving: CancellationToken,
    /// Where peers reach this node, as its HELLO says: its cluster address,
    /// or `:port`, the port their links come in on, on the address this
    /// node's links come from.
    pub reached_at: String,
}

/// What a node heard of a peer: the last of the peer's arrivals it heard of
/// with nothing missing before it, as far as it can tell.
#[derive(Default)]
pub(crate) struct Heard {
    through: AtomicU64,
    /// A message of the peer was shed here since the catch-up: `through`
    /// stays where it was, so the next catch-up brings it again.
    shed: std::sync::atomic::AtomicBool,
    /// The current link's peer said SHUTDOWN.
    shut_down: std::sync::atomic::AtomicBool,
    /// The peer's links up now.
    links: std::sync::atomic::AtomicUsize,
    /// The last link ended without SHUTDOWN, and no catch-up from the peer
    /// came since: what it published after `through` reaches this node only
    /// if it comes back.
    unclean: std::sync::atomic::AtomicBool,
}

impl Heard {
    /// A live message of the peer came, its arrival `seq`.
    pub(crate) fn live(&self, seq: u64) {
        if !self.shed.load(Ordering::Relaxed) {
            self.through.fetch_max(seq, Ordering::Relaxed);
        }
    }

    /// A message of the peer was shed: the catch-up starts before it.
    pub(crate) fn shed(&self) {
        self.shed.store(true, Ordering::Relaxed);
    }

    /// The peer's catch-up covers up to `through`.
    pub(crate) fn caught_up(&self, through: u64) {
        self.unclean.store(false, Ordering::Relaxed);
        if !self.shed.load(Ordering::Relaxed) {
            self.through.fetch_max(through, Ordering::Relaxed);
        }
    }

    /// The peer said SHUTDOWN: it published nothing more.
    pub(crate) fn shut_down(&self) {
        self.shut_down.store(true, Ordering::Relaxed);
    }

    /// A link to the peer is up.
    pub(crate) fn linked(&self) {
        self.links.fetch_add(1, Ordering::Relaxed);
    }

    /// The link to the peer ended.
    pub(crate) fn ended(&self) {
        let _ = self
            .links
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |links| {
                Some(links.saturating_sub(1))
            });
        let clean = self.shut_down.swap(false, Ordering::Relaxed);
        self.unclean.store(!clean, Ordering::Relaxed);
    }

    /// The last link ended without SHUTDOWN, and no catch-up came since.
    pub(crate) fn unclean(&self) -> bool {
        self.unclean.load(Ordering::Relaxed)
    }

    /// The peer is gone (another node answers where it was), and what it
    /// published after what this node heard of it may be gone with it: its
    /// last link ended without SHUTDOWN and no catch-up came since, or a
    /// link of it still looks up and has not said SHUTDOWN (its end has not
    /// come through yet).
    pub(crate) fn gone_unclean(&self) -> bool {
        self.unclean()
            || (self.links.load(Ordering::Relaxed) > 0 && !self.shut_down.load(Ordering::Relaxed))
    }

    /// Where the next catch-up starts from: named in the request, after
    /// which what is shed counts again.
    pub(crate) fn asked(&self) -> u64 {
        self.shed.store(false, Ordering::Relaxed);
        self.through.load(Ordering::Relaxed)
    }
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
        /// Its number in this node's arrival order (0 without recovery):
        /// what a peer heard of this node goes by it.
        seq: u64,
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
        /// Told once the manager routes to the link: a catch-up owed to the
        /// peer is taken from the recovery buffers then.
        taken: Option<tokio::sync::oneshot::Sender<()>>,
        /// Ends the link: one that fell behind is made again, and catches up.
        link: tokio_util::sync::CancellationToken,
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
        /// The link session it came on: one that ended is not merged.
        generation: u64,
    },
    Full {
        /// Reporting peer's address -- keys the per-origin presence sentinels.
        origin: String,
        entries: String,
        /// Where the frame stands in a sync round; None from older nodes.
        round: Option<RoundPart>,
        /// The link session it came on: a round never spans two.
        generation: u64,
    },
}

/// A frame's place in a presence sync round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RoundPart {
    pub first: bool,
    pub last: bool,
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
        /// More frames of this round follow.
        more: bool,
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
    pub(crate) fn add_delivered(&self, count: u64) {
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
    }
    pub(crate) fn add_dropped(&self, count: u64) {
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
