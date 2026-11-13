//! State shared by the server tasks and the Python layer.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use ahash::AHashSet;
use crossbeam_channel::{Receiver as CBReceiver, Sender as CBSender, unbounded};
use dashmap::{DashMap, DashSet};
use pyo3::prelude::*;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::protocol::Message;

use super::{BroadcastQueue, Fanout, TopicIndex, WsFrame};
use crate::jwt::JwtConfig;
use crate::wse::cluster::{ClusterCommand, ClusterDlq, ClusterMetrics};

/// Close codes the server sends. The clients reconnect on all of them but
/// the auth codes (4401 at the upgrade, 4403): those they retry only with a
/// new token from their token provider.
pub(crate) const CLOSE_NORMAL: u16 = 1000; // disconnect() by the application
pub(crate) const CLOSE_GOING_AWAY: u16 = 1001; // the server stops
pub(crate) const CLOSE_TRY_AGAIN_LATER: u16 = 1013; // too slow to read, or the server is full
pub(crate) const CLOSE_TOKEN_EXPIRED: u16 = 4403; // the JWT the connection opened with expired
pub(crate) const CLOSE_IDLE: u16 = 4408; // nothing received within idle_timeout
pub(crate) const CLOSE_RATE_LIMITED: u16 = 4429; // kept sending past the rate limit
pub(crate) const CLOSE_E2E_UNSUPPORTED: u16 = 4800; // no end-to-end encryption in common
pub(crate) const CLOSE_E2E_BAD_FRAME: u16 = 4802; // an E2E frame out of order, forged, or in plaintext
pub(crate) const CLOSE_E2E_EXHAUSTED: u16 = 4803; // the E2E session's counters are used up

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
    /// Stop the connection's read loop and write task when it must go now.
    /// They poll these with every message: an `Abortable` costs an atomic
    /// load where the token takes a lock.
    stops: Arc<std::sync::Mutex<Vec<futures_util::future::AbortHandle>>>,
}

impl Closer {
    pub(crate) fn new() -> Self {
        Self {
            token: tokio_util::sync::CancellationToken::new(),
            closing: Arc::new(AtomicBool::new(false)),
            stops: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// What one of the connection's loops is wrapped in (`Abortable`), so it
    /// stops as soon as the connection must go.
    pub(crate) fn stop_registration(&self) -> futures_util::future::AbortRegistration {
        let (handle, registration) = futures_util::future::AbortHandle::new_pair();
        let mut stops = self.stops.lock().unwrap_or_else(|e| e.into_inner());
        if self.token.is_cancelled() {
            handle.abort();
        } else {
            stops.push(handle);
        }
        registration
    }

    /// The connection goes now: its loops stop and the socket is closed.
    fn cancel(&self) {
        let mut stops = self.stops.lock().unwrap_or_else(|e| e.into_inner());
        self.token.cancel();
        for stop in stops.drain(..) {
            stop.abort();
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
        self.cancel();
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
        let closer = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(CLOSE_GRACE) => closer.cancel(),
                () = closer.token.cancelled() => {}
            }
        });
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionHandle {
    /// The control queue: server_ready, replies, direct sends, closes.
    pub(crate) tx: mpsc::UnboundedSender<WsFrame>,
    /// What a delivery touches, in one allocation.
    pub(crate) out: Arc<Outbox>,
    pub(crate) closer: Closer,
    /// The client asked for msgpack (`?format=msgpack`): what goes to it as
    /// JSON text is sent as an `M:` msgpack frame instead.
    pub(crate) msgpack: bool,
    /// The lane delivering to it (`Fanout`).
    pub(crate) lane: u8,
}

/// A connection's outbound state that every delivery reads or changes, kept
/// together so a delivery reaches one allocation instead of one per field.
#[derive(Default)]
pub(crate) struct Outbox {
    /// Pending outbound bytes (not messages), for the outbound budget.
    pub(crate) pending: AtomicUsize,
    /// Set when the write task is gone: deliveries stop.
    pub(crate) closed: AtomicBool,
    /// How the connection's frames are encrypted, set once by its key
    /// exchange.
    pub(crate) cipher: std::sync::OnceLock<Seal>,
    /// Broadcast frames for the write task, apart from the control queue:
    /// shared with every other connection they go to, not copies.
    pub(crate) frames: parking_lot::Mutex<BroadcastQueue>,
    /// Wakes the write task when frames are queued on an empty queue.
    pub(crate) notify: tokio::sync::Notify,
}

/// Marks the outbox closed when the write task ends, however it ends.
pub(crate) struct CloseOutbox(pub(crate) Arc<Outbox>);

impl Drop for CloseOutbox {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
    }
}

/// A connection's end-to-end encryption.
pub(crate) enum Seal {
    /// The first design, for older clients: one key, a random nonce; every
    /// producer seals for itself.
    Legacy(Arc<crate::crypto::Aes256Gcm>),
    /// A session with an authenticated server: the write task seals every
    /// frame, in the order of the wire (`WsFrame::Seal`).
    Session,
}

impl ConnectionHandle {
    /// See `Closer::close`.
    pub(crate) fn close(&self, code: u16, reason: &str) {
        self.closer.close(&self.tx, &self.out.pending, code, reason);
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

/// An event in the inbound queue. Connect, disconnect and presence events
/// always get in, so the application never misses a join or a leave; the
/// clients' messages are dropped while `max_inbound_queue_size` of them, or
/// `max_inbound_queue_bytes` of their size, are waiting.
pub(super) struct Queued {
    pub(super) event: InboundEvent,
    /// What a client's message counts against the byte budget; `None` for
    /// the lifecycle events.
    bytes: Option<usize>,
}

/// A message's room in the inbound budget while its callback runs.
pub(super) struct CallbackRoom {
    state: Arc<SharedState>,
    bytes: usize,
}

impl Drop for CallbackRoom {
    fn drop(&mut self) {
        self.state.release_message(self.bytes);
    }
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
    Shutdown,
    /// Graceful drain: stop accepting, close all clients with custom code, wait.
    Drain {
        close_code: u16,
        close_reason: String,
    },
    /// Drop every connection now, without waiting for its close handshake.
    AbortAll,
}

/// Configuration for SharedState construction.
pub(crate) struct SharedStateConfig {
    pub max_connections: usize,
    pub jwt_config: Option<JwtConfig>,
    pub max_inbound_queue_size: usize,
    pub max_inbound_queue_bytes: usize,
    pub allowed_origins: Option<AHashSet<String>>,
    pub client_subscriptions: bool,
    pub e2e_identity: Option<crate::crypto::IdentityKey>,
    pub e2e_legacy: bool,
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
    // Origins a browser may open a connection from (None: any)
    pub(super) allowed_origins: Option<AHashSet<String>>,
    // The server answers the clients' own subscribe/unsubscribe requests
    pub(super) client_subscriptions: bool,
    // E2E: the identity key signing each session handshake, and whether
    // older clients may still use the first design
    pub(super) e2e_identity: Option<crate::crypto::IdentityKey>,
    pub(super) e2e_legacy: bool,
    // Connections in an E2E session (send_event does not compress for them)
    pub(super) conn_sealed: DashSet<String>,
    // Callback mode. Shared, so a runtime thread takes one without attaching
    // to Python.
    pub(super) on_connect: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    pub(super) on_message: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    pub(super) on_disconnect: std::sync::RwLock<Option<Arc<Py<PyAny>>>>,
    // Drain mode: when active, inbound events go to channel instead of callbacks
    pub(super) drain_mode: AtomicBool,
    // Lock-free channel: Tokio tasks send (non-blocking), Python drains
    inbound_tx: CBSender<Queued>,
    pub(super) inbound_rx: CBReceiver<Queued>,
    // The clients' messages in the channel, their size and the limits of both
    inbound_messages: AtomicUsize,
    inbound_bytes: AtomicUsize,
    max_inbound_messages: usize,
    max_inbound_bytes: usize,
    // Client messages dropped over those limits (observability)
    pub(super) inbound_dropped: AtomicU64,
    // Per-connection format preference (lock-free sync access for send_event)
    pub(super) conn_formats: DashMap<String, bool>,
    // What a client asked of compression: the threshold in bytes, 0 = never
    // compress for it. Absent: the threshold send_event() is given.
    pub(super) conn_compression: DashMap<String, usize>,
    // Atomic connection count (lock-free read from Python without GIL blocking)
    pub(super) connection_count: AtomicUsize,
    // JWT config: when set, Rust validates JWT in handshake (zero GIL)
    // Fixed for the server's life: whether tokens are validated, and how;
    // the keys inside can be replaced (reload_jwt_keys).
    pub(super) jwt_config: Option<arc_swap::ArcSwap<JwtConfig>>,
    // Graceful drain: when true, reject new connections
    pub(super) draining: AtomicBool,
    // Topic subscribers, the subscribed patterns and the connections' topic
    // ACLs (from the JWT wse_topics claim or set_topic_acl; none = allow all)
    pub(crate) topics: TopicIndex,
    // Delivery on every core, while the server runs
    pub(crate) fanout: std::sync::RwLock<Option<Fanout>>,
    pub(super) conn_topics: DashMap<String, DashSet<String>>,
    // Cluster protocol
    pub(super) cluster_cmd_tx: arc_swap::ArcSwapOption<mpsc::UnboundedSender<ClusterCommand>>,
    pub(crate) cluster_metrics: Arc<ClusterMetrics>,
    pub(super) cluster_dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    pub(crate) cluster_instance_id: std::sync::Mutex<Option<String>>,
    pub(crate) local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    // Single-port mode only: the WebSocket port hands cluster links to the
    // cluster manager through this. None: cluster frames are refused there.
    pub(super) cluster_main_port: std::sync::RwLock<
        Option<mpsc::UnboundedSender<(tokio::net::TcpStream, std::net::SocketAddr)>>,
    >,
    // ArcSwap holder for hot-reloading cluster TLS certificates at runtime
    pub(crate) cluster_tls_holder:
        std::sync::RwLock<Option<Arc<arc_swap::ArcSwap<crate::wse::cluster::ClusterTlsConfig>>>>,
    pub(crate) recovery: Option<Arc<crate::wse::recovery::RecoveryManager>>,
    /// Each connection's last frame, in milliseconds since `clock_base`, for
    /// the idle check. The connection's read loop holds its own counter and
    /// stores into it with every frame: no map lookup on the read path.
    pub(super) conn_last_activity: DashMap<String, Arc<AtomicU64>>,
    pub(super) clock_base: std::time::Instant,
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
    // Commands waiting for the command task, as it last saw them
    pub(super) commands_queued: AtomicUsize,
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
    /// `at` in milliseconds since `clock_base`: what the activity counters hold.
    pub(super) fn activity_ms(&self, at: std::time::Instant) -> u64 {
        at.saturating_duration_since(self.clock_base).as_millis() as u64
    }

    /// One more local subscriber for each of `topics`; a topic's first one is
    /// advertised to cluster peers. Counted with or without a cluster, so
    /// connect_cluster() advertises (by RESYNC) what was subscribed before it.
    pub(crate) fn add_local_interest<'a>(&self, topics: impl IntoIterator<Item = &'a String>) {
        let cluster_tx = self.cluster_cmd_tx.load_full();
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
        let cluster_tx = self.cluster_cmd_tx.load_full();
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
        let (tx, rx) = unbounded();
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections: cfg.max_connections,
            allowed_origins: cfg.allowed_origins,
            client_subscriptions: cfg.client_subscriptions,
            e2e_identity: cfg.e2e_identity,
            e2e_legacy: cfg.e2e_legacy,
            conn_sealed: DashSet::new(),
            on_connect: std::sync::RwLock::new(None),
            on_message: std::sync::RwLock::new(None),
            on_disconnect: std::sync::RwLock::new(None),
            drain_mode: AtomicBool::new(false),
            inbound_tx: tx,
            inbound_rx: rx,
            inbound_messages: AtomicUsize::new(0),
            inbound_bytes: AtomicUsize::new(0),
            max_inbound_messages: cfg.max_inbound_queue_size,
            max_inbound_bytes: cfg.max_inbound_queue_bytes,
            inbound_dropped: AtomicU64::new(0),
            conn_formats: DashMap::new(),
            conn_compression: DashMap::new(),
            connection_count: AtomicUsize::new(0),
            jwt_config: cfg.jwt_config.map(arc_swap::ArcSwap::from_pointee),
            draining: AtomicBool::new(false),
            topics: TopicIndex::new(super::fanout::lane_count()),
            fanout: std::sync::RwLock::new(None),
            conn_topics: DashMap::new(),
            cluster_cmd_tx: arc_swap::ArcSwapOption::empty(),
            cluster_metrics: Arc::new(ClusterMetrics::new()),
            cluster_dlq: Arc::new(std::sync::Mutex::new(ClusterDlq::new(1000))),
            cluster_instance_id: std::sync::Mutex::new(None),
            local_topic_refcount: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cluster_main_port: std::sync::RwLock::new(None),
            cluster_tls_holder: std::sync::RwLock::new(None),
            recovery: cfg.recovery,
            conn_last_activity: DashMap::new(),
            clock_base: std::time::Instant::now(),
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
            commands_queued: AtomicUsize::new(0),
            connections_accepted_total: AtomicU64::new(0),
            connections_rejected_total: AtomicU64::new(0),
            bytes_received_total: AtomicU64::new(0),
            bytes_sent_total: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
            topic_message_counts: Arc::new(DashMap::new()),
        }
    }

    /// Queue a connect, disconnect or presence event for the drain: never
    /// dropped.
    pub(super) fn push_inbound(&self, event: InboundEvent) {
        let _ = self.inbound_tx.send(Queued { event, bytes: None });
    }

    /// Room for a client's message of `bytes` bytes among the messages
    /// waiting for the application: at most `max_inbound_queue_size` of them
    /// and `max_inbound_queue_bytes` of their size. A message larger than the
    /// whole byte budget still gets in when nothing else waits. Without room
    /// the message is counted in `inbound_dropped`.
    fn reserve_message(&self, bytes: usize) -> bool {
        let queued = self.inbound_messages.fetch_add(1, Ordering::AcqRel);
        let held = self.inbound_bytes.fetch_add(bytes, Ordering::AcqRel);
        if queued >= self.max_inbound_messages
            || (held > 0 && held.saturating_add(bytes) > self.max_inbound_bytes)
        {
            self.release_message(bytes);
            self.inbound_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    fn release_message(&self, bytes: usize) {
        self.inbound_messages.fetch_sub(1, Ordering::AcqRel);
        self.inbound_bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Queue a client's message of `bytes` bytes for the drain, or drop it
    /// when there is no room (see `reserve_message`).
    pub(super) fn push_message(&self, event: InboundEvent, bytes: usize) {
        if self.reserve_message(bytes) {
            let _ = self.inbound_tx.send(Queued {
                event,
                bytes: Some(bytes),
            });
        }
    }

    /// The drain took `queued` off the channel: its room is free again.
    pub(super) fn taken(&self, queued: Queued) -> InboundEvent {
        if let Some(bytes) = queued.bytes {
            self.release_message(bytes);
        }
        queued.event
    }

    /// Room for a message handed to an on_message callback, given back when
    /// the callback is done (see `reserve_message`).
    pub(super) fn callback_room(state: &Arc<Self>, bytes: usize) -> Option<CallbackRoom> {
        state.reserve_message(bytes).then(|| CallbackRoom {
            state: Arc::clone(state),
            bytes,
        })
    }
}
