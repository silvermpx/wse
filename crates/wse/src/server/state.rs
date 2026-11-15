//! State shared by the server tasks and the Python layer.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use ahash::AHashSet;
use crossbeam_channel::{Receiver as CBReceiver, Sender as CBSender, unbounded};
use dashmap::{DashMap, DashSet};

use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::signing::{ClientKey, ServerSigner};
use super::{BroadcastQueue, Fanout, TopicIndex, WsFrame};
use crate::cluster::{ClusterCommand, ClusterDlq, ClusterMetrics};
use crate::jwt::JwtConfig;

/// Close codes the server sends. The clients reconnect on all of them but
/// the auth codes (4401 at the upgrade, 4403): those they retry only with a
/// new token from their token provider.
pub(crate) const CLOSE_NORMAL: u16 = 1000; // disconnect() by the application
pub(crate) const CLOSE_GOING_AWAY: u16 = 1001; // the server stops
pub(crate) const CLOSE_INTERNAL_ERROR: u16 = 1011; // the application's handler panicked
pub(crate) const CLOSE_TRY_AGAIN_LATER: u16 = 1013; // too slow to read, or the server is full
pub(crate) const CLOSE_AUTH_FAILED: u16 = 4401; // the token, or the proof of its key, was refused
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
    /// Accepted: by its token, or by the application (`accept_connection`).
    /// Until then it gets no broadcasts and may not subscribe on its own.
    pub(crate) admitted: AtomicBool,
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
#[cfg_attr(not(feature = "python"), allow(dead_code))]
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
    /// A client's signed message that passed every check: its body's fields
    /// and `signature`, the proof.
    Signed {
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
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(super) struct Queued {
    pub(super) event: InboundEvent,
    /// What a client's message counts against the byte budget; `None` for
    /// the lifecycle events.
    bytes: Option<usize>,
}

/// The clients' messages waiting for the drain, and their size: every
/// connection's task adds to these and the drain takes from them, so they
/// keep a cache line of their own, away from what each message only reads.
#[repr(align(128))]
#[derive(Default)]
struct InboundRoom {
    messages: AtomicUsize,
    bytes: AtomicUsize,
}

/// What one drain took off the channel, given back to the budgets at once
/// when it is done.
#[derive(Default)]
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(super) struct Taken {
    messages: usize,
    bytes: usize,
    lifecycle: usize,
}

impl Taken {
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn take(&mut self, queued: Queued) -> InboundEvent {
        if let Some(bytes) = queued.bytes {
            self.messages += 1;
            self.bytes += bytes;
        } else if queued.event.is_lifecycle() {
            self.lifecycle += 1;
        }
        queued.event
    }
}

/// A message's room in the inbound budget while its callback runs.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
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

pub(crate) struct DeduplicationState {
    pub(super) seen: AHashSet<String>,
    pub(super) queue: std::collections::VecDeque<String>,
    pub(super) max_entries: usize,
}

/// How many commands wait for the command task before the application's
/// sends are refused (`Error::Full`); the Python layer waits for room then.
pub(crate) const COMMAND_QUEUE: usize = 65_536;

/// Where a running server's presence events go: its handler.
pub(crate) type PresenceEvents = Box<dyn Fn(super::handler::Presence) + Send + Sync>;

/// Where a connection the application binds to a key stands: the client's
/// proof and the application's accept come in either order, and the second
/// admits the connection when they name the same key.
pub(crate) enum Bind {
    /// The client proved this key (an uncompressed point) over its session.
    Proved(Vec<u8>),
    /// The application accepted the connection for this user and key.
    Expected { point: Vec<u8>, user_id: String },
    /// No proof that holds came by the admission deadline, or one came that
    /// does not hold: the application's accept is refused.
    Unproved,
}

/// The sending side of the command queue. It counts what waits in the queue,
/// so the application can be held back while the queue is full: a publisher
/// faster than the server then waits for it instead of filling memory.
#[derive(Clone)]
pub(crate) struct Commands {
    tx: mpsc::UnboundedSender<ServerCommand>,
    queued: Arc<AtomicUsize>,
}

impl Commands {
    pub(crate) fn new(tx: mpsc::UnboundedSender<ServerCommand>, queued: Arc<AtomicUsize>) -> Self {
        Self { tx, queued }
    }

    /// Queue `cmd` now, full queue or not: the server's own messages and
    /// the lifecycle commands.
    pub(crate) fn send(
        &self,
        cmd: ServerCommand,
    ) -> Result<(), mpsc::error::SendError<ServerCommand>> {
        self.queued.fetch_add(1, Ordering::Relaxed);
        self.tx.send(cmd).inspect_err(|_| {
            self.queued.fetch_sub(1, Ordering::Relaxed);
        })
    }

    /// Whether `COMMAND_QUEUE` commands wait already: the application's
    /// sends and broadcasts are refused until there is room.
    pub(crate) fn is_full(&self) -> bool {
        self.queued.load(Ordering::Relaxed) >= COMMAND_QUEUE
    }

    /// Whether the command task is gone: nothing more is taken.
    pub(crate) fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Block the calling thread until fewer than `COMMAND_QUEUE` commands
    /// wait, or the server is gone.
    pub(crate) fn wait_for_room(&self) {
        while self.is_full() && !self.tx.is_closed() {
            std::thread::sleep(std::time::Duration::from_micros(50));
        }
    }
}

/// What a client's decoded message costs the inbound budget: its size on the
/// wire, as the budget is set; or what its decoded value holds in memory when
/// that is many times more (a frame of many tiny elements holds tens of times
/// its own size).
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(crate) fn inbound_cost(wire: usize, value: &serde_json::Value) -> usize {
    let decoded = decoded_size(value);
    if decoded > wire.saturating_mul(DECODED_SIZE_RATIO) {
        decoded
    } else {
        wire
    }
}

/// How many times its wire size a decoded message may hold before it is
/// charged what it holds: ordinary JSON stays well under it.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
const DECODED_SIZE_RATIO: usize = 4;

#[cfg_attr(not(feature = "python"), allow(dead_code))]
fn decoded_size(value: &serde_json::Value) -> usize {
    use serde_json::Value;
    const NODE: usize = std::mem::size_of::<Value>();
    match value {
        Value::String(s) => NODE + s.len(),
        Value::Array(items) => NODE + items.iter().map(decoded_size).sum::<usize>(),
        Value::Object(map) => {
            NODE + map
                .iter()
                .map(|(k, v)| NODE + k.len() + decoded_size(v))
                .sum::<usize>()
        }
        _ => NODE,
    }
}

/// A counter every core adds to at once: one cache line for each shard, so
/// the cores never take a line from each other (nor from the fields around
/// it), and a reader sums the shards. A lane counts on its own shard, a
/// connection on its lane's.
pub(crate) struct Tally {
    shards: Box<[Shard]>,
}

/// Two lines: Intel fetches lines in pairs, Apple's are 128 bytes.
#[repr(align(128))]
#[derive(Default)]
struct Shard(AtomicU64);

impl Tally {
    pub(crate) fn new(shards: usize) -> Self {
        Self {
            shards: (0..shards.max(1)).map(|_| Shard::default()).collect(),
        }
    }

    /// The counter of shard `i` (any `i`: past the last, the first).
    #[inline]
    pub(crate) fn shard(&self, i: usize) -> &AtomicU64 {
        &self.shards.get(i).unwrap_or(&self.shards[0]).0
    }

    #[inline]
    pub(crate) fn add(&self, i: usize, n: u64) {
        self.shard(i).fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn sum(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.0.load(Ordering::Relaxed))
            .sum()
    }
}

/// A connection's last activity (`SharedState::activity_ms`), written by its
/// read loop on every frame. On a line of its own: cells made one after the
/// other for different connections would otherwise share a line, and the
/// cores reading those connections would take it from each other on every
/// message.
#[repr(align(128))]
#[derive(Default)]
pub(crate) struct Activity(AtomicU64);

impl std::ops::Deref for Activity {
    type Target = AtomicU64;

    fn deref(&self) -> &AtomicU64 {
        &self.0
    }
}

/// Connect and disconnect events the drain may hold whatever the server's
/// size (twice `max_connections`, at least this).
const MIN_LIFECYCLE_EVENTS: usize = 64;

impl InboundEvent {
    /// A connection's connect or disconnect event.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    fn is_lifecycle(&self) -> bool {
        matches!(
            self,
            Self::Connect { .. } | Self::AuthConnect { .. } | Self::Disconnect { .. }
        )
    }
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
    /// The application accepts a connection: `ready` (its server_ready) is
    /// sent, then broadcasts reach it and it may subscribe on its own.
    Accept {
        conn_id: String,
        ready: String,
    },
    Disconnect {
        conn_id: String,
    },
    /// Refuse a connection: `message` (an error), then a close with `code`.
    Refuse {
        conn_id: String,
        message: String,
        code: u16,
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
    pub recovery: Option<Arc<crate::recovery::RecoveryManager>>,
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
    pub signer: Option<ServerSigner>,
    pub signature_window_secs: u64,
    pub bind_sessions: bool,
}

pub(crate) struct SharedState {
    pub(crate) connections: Arc<parking_lot::RwLock<HashMap<String, ConnectionHandle>>>,
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
    // Signed messages: the server's key, how far a message's time may be
    // from the server's, each connection's key, and the (kid, n) taken
    // within the window, each until it is too old to come again.
    pub(crate) signer: Option<ServerSigner>,
    pub(super) signature_window_ms: u64,
    /// A token naming a key binds its connection to it (`Config::bind_sessions`).
    pub(super) bind_sessions: bool,
    pub(super) client_keys: DashMap<String, Arc<ClientKey>>,
    /// Connections the application binds to a key itself (no JWT
    /// configuration): its accept and the client's proof meet here.
    pub(super) binds: DashMap<String, Bind>,
    // Each user's connections, and each connection's user: what a send to a
    // user reaches.
    pub(crate) user_conns: DashMap<String, AHashSet<String>>,
    pub(crate) conn_user: DashMap<String, String>,
    pub(super) signed_seen: DashMap<String, u64>,
    // Drain mode: when active, inbound events go to channel instead of callbacks
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) drain_mode: AtomicBool,
    // Lock-free channel: Tokio tasks send (non-blocking), Python drains
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    inbound_tx: CBSender<Queued>,
    pub(super) inbound_rx: CBReceiver<Queued>,
    // The clients' messages in the channel, their size and the limits of both
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    inbound_room: InboundRoom,
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    max_inbound_messages: usize,
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    max_inbound_bytes: usize,
    // Connect and disconnect events in the channel: no message limit applies
    // to them (the application's view of its connections stays whole), but
    // past this many a server whose drain stalls takes no new connection.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    inbound_lifecycle: AtomicUsize,
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    max_lifecycle_events: usize,
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
    /// Topics whose last local subscriber left, kept of interest (count 0 in
    /// `local_topic_refcount`) until this time, with recovery on: see
    /// `RecoveryManager::interest_linger`.
    interest_linger: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    // Single-port mode only: the WebSocket port hands cluster links to the
    // cluster manager through this. None: cluster frames are refused there.
    pub(super) cluster_main_port: std::sync::RwLock<
        Option<mpsc::UnboundedSender<(tokio::net::TcpStream, std::net::SocketAddr)>>,
    >,
    // ArcSwap holder for hot-reloading cluster TLS certificates at runtime
    pub(crate) cluster_tls_holder:
        std::sync::RwLock<Option<Arc<arc_swap::ArcSwap<crate::cluster::ClusterTlsConfig>>>>,
    pub(crate) recovery: Option<Arc<crate::recovery::RecoveryManager>>,
    /// Each connection's last frame, in milliseconds since `clock_base`, for
    /// the idle check. The connection's read loop holds its own counter and
    /// stores into it with every frame: no map lookup on the read path.
    pub(super) conn_last_activity: DashMap<String, Arc<Activity>>,
    pub(super) clock_base: std::time::Instant,
    // Per-connection AES-256-GCM cipher for E2E encryption (optional, client-initiated)
    pub(super) conn_encryption: DashMap<String, Arc<crate::crypto::Aes256Gcm>>,
    // Presence tracking (None when disabled)
    pub(crate) presence: Option<Arc<crate::presence::PresenceManager>>,
    // Queue groups: topic -> group_name -> QueueGroup (round-robin distribution)
    pub(crate) queue_groups: Arc<DashMap<String, DashMap<String, QueueGroup>>>,
    // Reverse index: conn_id -> set of (topic, group_name) for cleanup on disconnect
    pub(super) conn_queue_groups: DashMap<String, Vec<(String, String)>>,
    // The running server's command queue: None while stopped.
    pub(crate) commands: arc_swap::ArcSwapOption<Commands>,
    // The running server's handler, for presence events.
    pub(crate) presence_events: arc_swap::ArcSwapOption<PresenceEvents>,
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
    pub(crate) slow_consumer_drops: Arc<Tally>,
    pub(super) messages_received_total: Tally,
    pub(super) messages_sent_total: AtomicU64,
    // Commands waiting for the command task
    pub(super) commands_queued: Arc<AtomicUsize>,
    pub(super) connections_accepted_total: AtomicU64,
    pub(super) connections_rejected_total: AtomicU64,
    pub(super) bytes_received_total: Tally,
    pub(super) bytes_sent_total: AtomicU64,
    pub(super) auth_failures_total: AtomicU64,
    pub(super) rate_limited_total: AtomicU64,
    // When the running server started: None while stopped.
    pub(crate) started_at: parking_lot::Mutex<Option<std::time::Instant>>,
    // send_event: the (connection, id) pairs sent lately, each sent once.
    pub(crate) dedup: std::sync::Mutex<DeduplicationState>,
    // Per-topic message delivery counts (for top-N Prometheus metrics)
    pub(crate) topic_message_counts: Arc<DashMap<String, AtomicU64>>,
}

impl SharedState {
    /// `conn_id` is `user_id`'s from now on: sends to the user reach it.
    pub(crate) fn register_user(&self, conn_id: &str, user_id: &str) {
        if let Some(previous) = self
            .conn_user
            .insert(conn_id.to_owned(), user_id.to_owned())
        {
            self.forget_user_conn(&previous, conn_id);
        }
        self.user_conns
            .entry(user_id.to_owned())
            .or_default()
            .insert(conn_id.to_owned());
    }

    /// `conn_id` is gone: its user no longer counts it.
    pub(crate) fn unregister_user(&self, conn_id: &str) {
        if let Some((_, user_id)) = self.conn_user.remove(conn_id) {
            self.forget_user_conn(&user_id, conn_id);
        }
    }

    fn forget_user_conn(&self, user_id: &str, conn_id: &str) {
        if let Some(mut conns) = self.user_conns.get_mut(user_id) {
            conns.remove(conn_id);
        }
        self.user_conns
            .remove_if(user_id, |_, conns| conns.is_empty());
    }

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
            if *count != 1 {
                continue;
            }
            // Lingering: still advertised and relayed, the interest unbroken.
            let lingered = self
                .interest_linger
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(topic)
                .is_some();
            if lingered {
                continue;
            }
            if let Some(recovery) = &self.recovery {
                recovery.interest_began(topic);
            }
            if let Some(tx) = &cluster_tx {
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
            if *count != 0 {
                continue;
            }
            // With recovery, the topic stays of interest for the history's
            // lifetime: a client coming back finds here what other nodes
            // published meanwhile.
            if let Some(recovery) = &self.recovery {
                self.interest_linger
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        topic.clone(),
                        std::time::Instant::now() + recovery.interest_linger(),
                    );
                continue;
            }
            refcounts.remove(topic);
            if let Some(tx) = &cluster_tx {
                let _ = tx.send(ClusterCommand::Unsub {
                    topic: topic.clone(),
                });
            }
        }
    }

    /// End the interest in topics that lingered their time without a local
    /// subscriber coming back.
    pub(crate) fn sweep_interest_linger(&self) {
        let now = std::time::Instant::now();
        let cluster_tx = self.cluster_cmd_tx.load_full();
        let mut refcounts = self
            .local_topic_refcount
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut linger = self
            .interest_linger
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        linger.retain(|topic, until| {
            if *until > now {
                return true;
            }
            if refcounts.get(topic) == Some(&0) {
                refcounts.remove(topic);
                if let Some(recovery) = &self.recovery {
                    recovery.interest_ended(topic);
                }
                if let Some(tx) = &cluster_tx {
                    let _ = tx.send(ClusterCommand::Unsub {
                        topic: topic.clone(),
                    });
                }
            }
            false
        });
    }

    pub(super) fn new(cfg: SharedStateConfig) -> Self {
        let (tx, rx) = unbounded();
        Self {
            connections: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            max_connections: cfg.max_connections,
            allowed_origins: cfg.allowed_origins,
            client_subscriptions: cfg.client_subscriptions,
            e2e_identity: cfg.e2e_identity,
            e2e_legacy: cfg.e2e_legacy,
            conn_sealed: DashSet::new(),
            signer: cfg.signer,
            signature_window_ms: cfg.signature_window_secs.saturating_mul(1000),
            bind_sessions: cfg.bind_sessions,
            client_keys: DashMap::new(),
            binds: DashMap::new(),
            user_conns: DashMap::new(),
            conn_user: DashMap::new(),
            signed_seen: DashMap::new(),
            drain_mode: AtomicBool::new(false),
            inbound_tx: tx,
            inbound_rx: rx,
            inbound_room: InboundRoom::default(),
            max_inbound_messages: cfg.max_inbound_queue_size,
            max_inbound_bytes: cfg.max_inbound_queue_bytes,
            inbound_lifecycle: AtomicUsize::new(0),
            max_lifecycle_events: cfg
                .max_connections
                .saturating_mul(2)
                .max(MIN_LIFECYCLE_EVENTS),
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
            interest_linger: std::sync::Mutex::new(HashMap::new()),
            cluster_main_port: std::sync::RwLock::new(None),
            cluster_tls_holder: std::sync::RwLock::new(None),
            recovery: cfg.recovery,
            conn_last_activity: DashMap::new(),
            clock_base: std::time::Instant::now(),
            conn_encryption: DashMap::new(),
            presence: if cfg.presence_enabled {
                Some(Arc::new(crate::presence::PresenceManager::new(
                    cfg.presence_max_data_size,
                    cfg.presence_max_members,
                )))
            } else {
                None
            },
            queue_groups: Arc::new(DashMap::new()),
            conn_queue_groups: DashMap::new(),
            commands: arc_swap::ArcSwapOption::empty(),
            presence_events: arc_swap::ArcSwapOption::empty(),
            rate_capacity: cfg.rate_capacity,
            rate_refill: cfg.rate_refill,
            max_message_size: cfg.max_message_size,
            ping_interval_secs: cfg.ping_interval_secs,
            idle_timeout_secs: cfg.idle_timeout_secs,
            max_outbound_queue_bytes: cfg.max_outbound_queue_bytes,
            max_subscriptions_per_connection: cfg.max_subscriptions_per_connection,
            slow_consumer_drops: Arc::new(Tally::new(super::fanout::lane_count())),
            messages_received_total: Tally::new(super::fanout::lane_count()),
            messages_sent_total: AtomicU64::new(0),
            commands_queued: Arc::default(),
            connections_accepted_total: AtomicU64::new(0),
            connections_rejected_total: AtomicU64::new(0),
            bytes_received_total: Tally::new(super::fanout::lane_count()),
            bytes_sent_total: AtomicU64::new(0),
            auth_failures_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
            started_at: parking_lot::Mutex::new(None),
            dedup: std::sync::Mutex::new(DeduplicationState {
                seen: AHashSet::with_capacity(50_000),
                queue: std::collections::VecDeque::with_capacity(50_000),
                max_entries: 50_000,
            }),
            topic_message_counts: Arc::new(DashMap::new()),
        }
    }

    /// Queue a connect, disconnect or presence event for the drain: never
    /// dropped.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn push_inbound(&self, event: InboundEvent) {
        if event.is_lifecycle() {
            self.inbound_lifecycle.fetch_add(1, Ordering::AcqRel);
        }
        let _ = self.inbound_tx.send(Queued { event, bytes: None });
    }

    /// Queue a connection's connect event, unless `max_lifecycle_events`
    /// wait already (the drain stalls while connections come and go): then
    /// false, and the connection is refused.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn push_connect(&self, event: InboundEvent) -> bool {
        if self.inbound_lifecycle.load(Ordering::Acquire) >= self.max_lifecycle_events {
            return false;
        }
        self.push_inbound(event);
        true
    }

    /// Room for a client's message of `bytes` bytes among the messages
    /// waiting for the application: at most `max_inbound_queue_size` of them
    /// and `max_inbound_queue_bytes` of their size. A message larger than the
    /// whole byte budget still gets in when nothing else waits. Without room
    /// the message is counted in `inbound_dropped`.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    fn reserve_message(&self, bytes: usize) -> bool {
        let queued = self.inbound_room.messages.fetch_add(1, Ordering::AcqRel);
        let held = self.inbound_room.bytes.fetch_add(bytes, Ordering::AcqRel);
        if queued >= self.max_inbound_messages
            || (held > 0 && held.saturating_add(bytes) > self.max_inbound_bytes)
        {
            self.release_message(bytes);
            self.inbound_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    fn release_message(&self, bytes: usize) {
        self.inbound_room.messages.fetch_sub(1, Ordering::AcqRel);
        self.inbound_room.bytes.fetch_sub(bytes, Ordering::AcqRel);
    }

    /// Queue a client's message of `bytes` bytes for the drain, or drop it
    /// when there is no room (see `reserve_message`). A decoded message is
    /// charged what it holds in memory when that is more. False when dropped.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn push_message(&self, event: InboundEvent, bytes: usize) -> bool {
        let bytes = match &event {
            InboundEvent::Message { value, .. } | InboundEvent::Signed { value, .. } => {
                inbound_cost(bytes, value)
            }
            _ => bytes,
        };
        if !self.reserve_message(bytes) {
            return false;
        }
        let queued = Queued {
            event,
            bytes: Some(bytes),
        };
        match self.inbound_tx.send(queued) {
            Ok(()) => true,
            Err(_) => {
                self.release_message(bytes);
                false
            }
        }
    }

    /// A drain took these off the channel: their room is free again, given
    /// back in one step for the whole batch.
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn give_back(&self, taken: Taken) {
        if taken.messages > 0 {
            self.inbound_room
                .messages
                .fetch_sub(taken.messages, Ordering::AcqRel);
            self.inbound_room
                .bytes
                .fetch_sub(taken.bytes, Ordering::AcqRel);
        }
        if taken.lifecycle > 0 {
            self.inbound_lifecycle
                .fetch_sub(taken.lifecycle, Ordering::AcqRel);
        }
    }

    /// Room for a message handed to an on_message callback, given back when
    /// the callback is done (see `reserve_message`).
    #[cfg_attr(not(feature = "python"), allow(dead_code))]
    pub(super) fn callback_room(state: &Arc<Self>, bytes: usize) -> Option<CallbackRoom> {
        state.reserve_message(bytes).then(|| CallbackRoom {
            state: Arc::clone(state),
            bytes,
        })
    }
}
