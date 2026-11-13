//! State shared by the server tasks and the Python layer.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use ahash::AHashSet;
use crossbeam_channel::{Receiver as CBReceiver, Sender as CBSender, bounded};
use dashmap::{DashMap, DashSet};
use pyo3::prelude::*;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_tungstenite::tungstenite::protocol::Message;

use super::{BroadcastQueue, WsFrame};
use crate::jwt::JwtConfig;
use crate::wse::cluster::{ClusterCommand, ClusterDlq, ClusterMetrics};

/// Close codes the server sends. The clients reconnect on all of them; only
/// the auth codes (4401, 4403) stop them.
pub(crate) const CLOSE_NORMAL: u16 = 1000; // disconnect() by the application
pub(crate) const CLOSE_GOING_AWAY: u16 = 1001; // the server stops
pub(crate) const CLOSE_TRY_AGAIN_LATER: u16 = 1013; // too slow to read, or the server is full
pub(crate) const CLOSE_IDLE: u16 = 4408; // nothing received within idle_timeout
pub(crate) const CLOSE_RATE_LIMITED: u16 = 4429; // kept sending past the rate limit

/// How long a client gets to answer the server's close frame before its
/// socket is closed for it.
pub(crate) const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Ends a connection from the server side.
#[derive(Clone)]
pub(crate) struct Closer {
    /// Cancelled when the connection must go now: its read and write tasks
    /// stop and the socket is closed.
    token: tokio_util::sync::CancellationToken,
    /// Set once the server queued a close frame: the client's further
    /// messages are no longer delivered.
    closing: Arc<AtomicBool>,
}

impl Closer {
    pub(crate) fn new() -> Self {
        Self {
            token: tokio_util::sync::CancellationToken::new(),
            closing: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn token(&self) -> &tokio_util::sync::CancellationToken {
        &self.token
    }

    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Drop the connection now, without a close handshake.
    pub(crate) fn abort(&self) {
        self.closing.store(true, Ordering::Release);
        self.token.cancel();
    }

    /// Close from the server: queue a close frame with `code` on the
    /// connection's queue `tx`, stop delivering the client's messages, and
    /// close the socket if the client has not closed within `CLOSE_GRACE`.
    /// Only the first call counts. Runs on the tokio runtime.
    pub(crate) fn close(
        &self,
        tx: &mpsc::UnboundedSender<WsFrame>,
        pending: &AtomicUsize,
        code: u16,
        reason: &str,
    ) {
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

        if self.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        let frame = Message::Close(Some(CloseFrame {
            code: CloseCode::from(code),
            reason: reason.to_owned().into(),
        }));
        let bytes = 8 + reason.len();
        pending.fetch_add(bytes, Ordering::Relaxed);
        if tx.send(WsFrame::Msg(frame)).is_err() {
            pending.fetch_sub(bytes, Ordering::Relaxed);
        }
        let token = self.token.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(CLOSE_GRACE) => token.cancel(),
                () = token.cancelled() => {}
            }
        });
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionHandle {
    pub(crate) tx: mpsc::UnboundedSender<WsFrame>,
    /// Pending outbound bytes (not messages) for backpressure accounting.
    pub(crate) pending: Arc<AtomicUsize>,
    /// NATS-style direct buffer for broadcasts (bypasses mpsc channel): the frames
    /// themselves, shared with every other connection they go to, not copies.
    pub(crate) broadcast_buf: Arc<parking_lot::Mutex<BroadcastQueue>>,
    /// Wakes write task when broadcast data is appended to broadcast_buf.
    pub(crate) broadcast_notify: Arc<tokio::sync::Notify>,
    pub(crate) closer: Closer,
}

impl ConnectionHandle {
    /// See `Closer::close`.
    pub(crate) fn close(&self, code: u16, reason: &str) {
        self.closer.close(&self.tx, &self.pending, code, reason);
    }
}

// ---------------------------------------------------------------------------
// QueueGroup -- round-robin distribution within a topic
// ---------------------------------------------------------------------------

/// A queue group distributes messages round-robin to one member instead of
/// fan-out to all. Members are connections subscribed with the same queue_group name.
pub(crate) struct QueueGroup {
    pub(crate) members: Vec<(String, ConnectionHandle)>,
    pub(crate) next: AtomicUsize,
}

// ---------------------------------------------------------------------------
// Drain mode: inbound event queue (replaces per-message callbacks)
// ---------------------------------------------------------------------------

/// Inbound event types pushed to drain queue.
/// All events go through one FIFO queue to preserve ordering.
pub(super) enum InboundEvent {
    Connect {
        conn_id: String,
        cookies: String,
    },
    /// Rust validated JWT from cookie — user_id already resolved.
    /// Python can skip JWT decode and go straight to connection setup.
    AuthConnect {
        conn_id: String,
        user_id: String,
    },
    Message {
        conn_id: String,
        value: serde_json::Value,
    },
    RawText {
        conn_id: String,
        text: String,
    },
    Binary {
        conn_id: String,
        data: Vec<u8>,
    },
    Disconnect {
        conn_id: String,
    },
    PresenceJoin {
        topic: String,
        user_id: String,
        data: serde_json::Value,
    },
    PresenceLeave {
        topic: String,
        user_id: String,
        data: serde_json::Value,
    },
}

// ---------------------------------------------------------------------------
// Dedup state (accessed from send_event on the Python thread)
// ---------------------------------------------------------------------------

pub(super) struct DeduplicationState {
    pub(super) seen: AHashSet<String>,
    pub(super) queue: std::collections::VecDeque<String>,
    pub(super) max_entries: usize,
}

pub(crate) enum ServerCommand {
    SendText {
        conn_id: String,
        data: String,
    },
    SendBytes {
        conn_id: String,
        data: Vec<u8>,
    },
    SendPrebuilt {
        conn_id: String,
        message: Message,
    },
    BroadcastText {
        data: String,
    },
    BroadcastBytes {
        data: Vec<u8>,
    },
    BroadcastLocal {
        topic: String,
        data: String,
        skip_recovery: bool,
        /// Also publish to cluster peers, after stamping, with the exact
        /// recovery position the message got here.
        publish: bool,
    },
    Disconnect {
        conn_id: String,
    },
    GetConnections {
        reply: oneshot::Sender<Vec<String>>,
    },
    Shutdown,
    /// Graceful drain: stop accepting, close all clients with custom code, wait.
    Drain {
        close_code: u16,
        close_reason: String,
        timeout_secs: u64,
    },
}

/// Configuration for SharedState construction.
pub(crate) struct SharedStateConfig {
    pub max_connections: usize,
    pub jwt_config: Option<JwtConfig>,
    pub max_inbound_queue_size: usize,
    pub recovery: Option<Arc<crate::wse::recovery::RecoveryManager>>,
    pub presence_enabled: bool,
    pub presence_max_data_size: usize,
    pub presence_max_members: usize,
    pub rate_capacity: f64,
    pub rate_refill: f64,
    pub max_message_size: usize,
    pub ping_interval_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_outbound_queue_bytes: usize,
    pub max_subscriptions_per_connection: usize,
}

pub(crate) struct SharedState {
    pub(crate) connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    pub(super) max_connections: usize,
    pub(super) on_connect: std::sync::RwLock<Option<Py<PyAny>>>,
    pub(super) on_message: std::sync::RwLock<Option<Py<PyAny>>>,
    pub(super) on_disconnect: std::sync::RwLock<Option<Py<PyAny>>>,
    // Drain mode: when active, inbound events go to channel instead of callbacks
    pub(super) drain_mode: AtomicBool,
    // Lock-free bounded channel: Tokio tasks send (non-blocking), Python drains
    inbound_tx: CBSender<InboundEvent>,
    pub(super) inbound_rx: CBReceiver<InboundEvent>,
    // Counter for dropped events (observability)
    pub(super) inbound_dropped: AtomicU64,
    // Per-connection format preference (lock-free sync access for send_event)
    pub(super) conn_formats: DashMap<String, bool>,
    // Per-connection rate limiter state (cleaned up on disconnect)
    // Atomic connection count (lock-free read from Python without GIL blocking)
    pub(super) connection_count: AtomicUsize,
    // JWT config: when set, Rust validates JWT in handshake (zero GIL)
    pub(super) jwt_config: Option<JwtConfig>,
    // Graceful drain: when true, reject new connections
    pub(super) draining: AtomicBool,
    pub(crate) topic_subscribers: Arc<DashMap<String, DashMap<String, ConnectionHandle>>>,
    pub(super) conn_topics: DashMap<String, DashSet<String>>,
    // Per-connection topic ACL from JWT wse_topics claim (None entry = allow all)
    pub(super) conn_topic_acl: DashMap<String, crate::jwt::TopicAcl>,
    // Count of glob-pattern topics (contains * or ?) -- avoids O(T) scan in fanout_topic_direct
    pub(crate) glob_topic_count: Arc<AtomicUsize>,
    // Cluster protocol
    pub(super) cluster_cmd_tx: std::sync::RwLock<Option<mpsc::UnboundedSender<ClusterCommand>>>,
    pub(crate) cluster_metrics: Arc<ClusterMetrics>,
    pub(super) cluster_dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    pub(crate) cluster_instance_id: std::sync::Mutex<Option<String>>,
    pub(crate) cluster_interest_tx:
        std::sync::RwLock<Option<mpsc::UnboundedSender<crate::wse::cluster::InterestUpdate>>>,
    pub(crate) local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    // True when cluster TLS is configured (reject cluster connections on main port)
    pub(super) cluster_tls_enabled: AtomicBool,
    // ArcSwap holder for hot-reloading cluster TLS certificates at runtime
    pub(crate) cluster_tls_holder:
        std::sync::RwLock<Option<Arc<arc_swap::ArcSwap<crate::wse::cluster::ClusterTlsConfig>>>>,
    pub(crate) recovery: Option<Arc<crate::wse::recovery::RecoveryManager>>,
    // Per-connection last activity tracking for zombie detection
    pub(super) conn_last_activity: DashMap<String, std::time::Instant>,
    // Per-connection AES-256-GCM cipher for E2E encryption (optional, client-initiated)
    pub(super) conn_encryption: DashMap<String, Arc<crate::crypto::Aes256Gcm>>,
    // Presence tracking (None when disabled)
    pub(crate) presence: Option<Arc<crate::wse::presence::PresenceManager>>,
    // Queue groups: topic -> group_name -> QueueGroup (round-robin distribution)
    pub(crate) queue_groups: Arc<DashMap<String, DashMap<String, QueueGroup>>>,
    // Reverse index: conn_id -> set of (topic, group_name) for cleanup on disconnect
    pub(super) conn_queue_groups: DashMap<String, Vec<(String, String)>>,
    // Sender for presence broadcasts from disconnect cleanup paths
    // (where self.cmd_tx is not available, only Arc<SharedState>)
    pub(super) presence_broadcast_tx:
        std::sync::RwLock<Option<mpsc::UnboundedSender<ServerCommand>>>,
    // Configurable limits
    pub(super) rate_capacity: f64,
    pub(super) rate_refill: f64,
    pub(super) max_message_size: usize,
    pub(super) ping_interval_secs: u64,
    pub(super) idle_timeout_secs: u64,
    pub(crate) max_outbound_queue_bytes: usize,
    // Per-connection subscription limit (0 = unlimited)
    pub(super) max_subscriptions_per_connection: usize,
    // Prometheus counters
    pub(crate) slow_consumer_drops: Arc<AtomicU64>,
    pub(super) messages_received_total: AtomicU64,
    pub(super) messages_sent_total: AtomicU64,
    pub(super) connections_accepted_total: AtomicU64,
    pub(super) connections_rejected_total: AtomicU64,
    pub(super) bytes_received_total: AtomicU64,
    pub(super) bytes_sent_total: AtomicU64,
    pub(super) auth_failures_total: AtomicU64,
    pub(super) rate_limited_total: AtomicU64,
    // Per-topic message delivery counts (for top-N Prometheus metrics)
    pub(crate) topic_message_counts: Arc<DashMap<String, AtomicU64>>,
}

impl SharedState {
    /// One more local subscriber for each of `topics`; a topic's first one is
    /// advertised to cluster peers. Counted with or without a cluster, so
    /// connect_cluster() advertises (by RESYNC) what was subscribed before it.
    pub(crate) fn add_local_interest<'a>(&self, topics: impl IntoIterator<Item = &'a String>) {
        let cluster_tx = self
            .cluster_cmd_tx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut refcounts = self
            .local_topic_refcount
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for topic in topics {
            let count = refcounts.entry(topic.clone()).or_insert(0);
            *count += 1;
            if *count == 1
                && let Some(tx) = &cluster_tx
            {
                let _ = tx.send(ClusterCommand::Sub {
                    topic: topic.clone(),
                });
            }
        }
    }

    /// One fewer local subscriber for each of `topics`; a topic's last one
    /// leaving withdraws it from cluster peers.
    pub(crate) fn remove_local_interest<'a>(&self, topics: impl IntoIterator<Item = &'a String>) {
        let cluster_tx = self
            .cluster_cmd_tx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut refcounts = self
            .local_topic_refcount
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for topic in topics {
            let Some(count) = refcounts.get_mut(topic) else {
                continue;
            };
            *count = count.saturating_sub(1);
            if *count == 0 {
                refcounts.remove(topic);
                if let Some(tx) = &cluster_tx {
                    let _ = tx.send(ClusterCommand::Unsub {
                        topic: topic.clone(),
                    });
                }
            }
        }
    }

    pub(super) fn new(cfg: SharedStateConfig) -> Self {
        let (tx, rx) = bounded(cfg.max_inbound_queue_size);
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections: cfg.max_connections,
            on_connect: std::sync::RwLock::new(None),
            on_message: std::sync::RwLock::new(None),
            on_disconnect: std::sync::RwLock::new(None),
            drain_mode: AtomicBool::new(false),
            inbound_tx: tx,
            inbound_rx: rx,
            inbound_dropped: AtomicU64::new(0),
            conn_formats: DashMap::new(),
            connection_count: AtomicUsize::new(0),
            jwt_config: cfg.jwt_config,
            draining: AtomicBool::new(false),
            topic_subscribers: Arc::new(DashMap::new()),
            conn_topics: DashMap::new(),
            conn_topic_acl: DashMap::new(),
            glob_topic_count: Arc::new(AtomicUsize::new(0)),
            cluster_cmd_tx: std::sync::RwLock::new(None),
            cluster_metrics: Arc::new(ClusterMetrics::new()),
            cluster_dlq: Arc::new(std::sync::Mutex::new(ClusterDlq::new(1000))),
            cluster_instance_id: std::sync::Mutex::new(None),
            cluster_interest_tx: std::sync::RwLock::new(None),
            local_topic_refcount: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cluster_tls_enabled: AtomicBool::new(false),
            cluster_tls_holder: std::sync::RwLock::new(None),
            recovery: cfg.recovery,
            conn_last_activity: DashMap::new(),
            conn_encryption: DashMap::new(),
            presence: if cfg.presence_enabled {
                Some(Arc::new(crate::wse::presence::PresenceManager::new(
                    cfg.presence_max_data_size,
                    cfg.presence_max_members,
                )))
            } else {
                None
            },
            queue_groups: Arc::new(DashMap::new()),
            conn_queue_groups: DashMap::new(),
            presence_broadcast_tx: std::sync::RwLock::new(None),
            rate_capacity: cfg.rate_capacity,
            rate_refill: cfg.rate_refill,
            max_message_size: cfg.max_message_size,
            ping_interval_secs: cfg.ping_interval_secs,
            idle_timeout_secs: cfg.idle_timeout_secs,
            max_outbound_queue_bytes: cfg.max_outbound_queue_bytes,
            max_subscriptions_per_connection: cfg.max_subscriptions_per_connection,
            slow_consumer_drops: Arc::new(AtomicU64::new(0)),
            messages_received_total: AtomicU64::new(0),
            messages_sent_total: AtomicU64::new(0),
            connections_accepted_total: AtomicU64::new(0),
            connections_rejected_total: AtomicU64::new(0),
            bytes_received_total: AtomicU64::new(0),
            bytes_sent_total: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
            topic_message_counts: Arc::new(DashMap::new()),
        }
    }

    /// Push an event to the drain channel (lock-free, non-blocking).
    /// When channel is full, drops the event and increments counter.
    pub(super) fn push_inbound(&self, event: InboundEvent) {
        if self.inbound_tx.try_send(event).is_err() {
            self.inbound_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
