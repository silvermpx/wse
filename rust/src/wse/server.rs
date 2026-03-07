// =============================================================================
// WSE Rust WebSocket server: tokio + tungstenite + PyO3.
//
// Key optimizations:
//   - send_event(): full outbound pipeline in Rust (envelope+serialize+compress+send)
//   - Drain mode: Python polls Rust for batched inbound events (1 GIL per batch)
//   - Ping/pong handled entirely in Rust (zero Python round-trips)
//   - Write coalescing: feed() + batch try_recv() + single flush()
//   - TCP_NODELAY on accept
// =============================================================================

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use super::cluster::{ClusterCommand, ClusterDlq, ClusterMetrics};
use super::compression::{rmpv_to_serde_json, serde_json_to_rmpv};
use crate::jwt::{self, JwtConfig, parse_cookie_value};
use ahash::AHashSet;
use bytes::{BufMut, Bytes, BytesMut};
use crossbeam_channel::{Receiver as CBReceiver, Sender as CBSender, bounded};
use dashmap::{DashMap, DashSet};
use flate2::Compression;
use flate2::write::ZlibEncoder;
use futures_util::{SinkExt, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Frame types sent through per-connection channels.
/// `Msg` goes through tungstenite's framing pipeline (per-connection sends, control frames).
/// `PreFramed` contains complete WebSocket wire bytes that bypass tungstenite entirely
/// (broadcasts -- constructed once, sent to all connections).
#[derive(Clone)]
pub(crate) enum WsFrame {
    Msg(Message),
    PreFramed(Bytes),
}

/// Byte size of a WsFrame for backpressure accounting.
/// Counts payload + estimated framing overhead (~4 bytes for most messages).
#[inline]
fn ws_frame_size(frame: &WsFrame) -> usize {
    match frame {
        WsFrame::PreFramed(bytes) => bytes.len(),
        WsFrame::Msg(msg) => match msg {
            Message::Text(s) => s.len() + 4,
            Message::Binary(b) => b.len() + 4,
            Message::Ping(p) | Message::Pong(p) => p.len() + 4,
            Message::Close(_) => 8,
            Message::Frame(_) => 16,
        },
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionHandle {
    pub(crate) tx: mpsc::UnboundedSender<WsFrame>,
    /// Pending outbound bytes (not messages) for backpressure accounting.
    pub(crate) pending: Arc<AtomicUsize>,
    /// NATS-style direct byte buffer for broadcast_all (bypasses mpsc channel).
    pub(crate) broadcast_buf: Arc<parking_lot::Mutex<BytesMut>>,
    /// Wakes write task when broadcast data is appended to broadcast_buf.
    pub(crate) broadcast_notify: Arc<tokio::sync::Notify>,
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
// Pre-framed WebSocket encoding (server-to-client, no masking per RFC 6455)
// ---------------------------------------------------------------------------

/// Encode a complete WebSocket frame: header + payload.
/// Server-to-client frames are NOT masked, so all connections receive identical bytes.
pub(crate) fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Bytes {
    let len = payload.len();
    let header_len = if len < 126 {
        2
    } else if len < 65536 {
        4
    } else {
        10
    };
    let mut buf = BytesMut::with_capacity(header_len + len);
    buf.put_u8(0x80 | opcode); // FIN=1 + opcode
    if len < 126 {
        buf.put_u8(len as u8);
    } else if len < 65536 {
        buf.put_u8(126);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(127);
        buf.put_u64(len as u64);
    }
    buf.extend_from_slice(payload);
    buf.freeze()
}

pub(crate) fn pre_frame_text(data: &str) -> Bytes {
    encode_ws_frame(0x01, data.as_bytes())
}

pub(crate) fn pre_frame_binary(data: &[u8]) -> Bytes {
    encode_ws_frame(0x02, data)
}

/// Clone a TcpStream by duplicating the file descriptor.
/// Returns (original, clone) -- both refer to the same socket.
fn clone_tcp_stream(stream: TcpStream) -> std::io::Result<(TcpStream, TcpStream)> {
    let std_stream = stream.into_std()?;
    let std_clone = std_stream.try_clone()?;
    Ok((
        TcpStream::from_std(std_stream)?,
        TcpStream::from_std(std_clone)?,
    ))
}

// ---------------------------------------------------------------------------
// Drain mode: inbound event queue (replaces per-message callbacks)
// ---------------------------------------------------------------------------

/// Inbound event types pushed to drain queue.
/// All events go through one FIFO queue to preserve ordering.
enum InboundEvent {
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
// Dedup + Rate limiter state (accessed from send_event on Python thread)
// ---------------------------------------------------------------------------

struct DeduplicationState {
    seen: AHashSet<String>,
    queue: std::collections::VecDeque<String>,
    max_entries: usize,
}

struct PerConnRate {
    tokens: f64,
    last_refill: std::time::Instant,
    last_rate_limited: Option<std::time::Instant>,
    last_warning: Option<std::time::Instant>,
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

pub(crate) struct SharedState {
    pub(crate) connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    max_connections: usize,
    on_connect: std::sync::RwLock<Option<Py<PyAny>>>,
    on_message: std::sync::RwLock<Option<Py<PyAny>>>,
    on_disconnect: std::sync::RwLock<Option<Py<PyAny>>>,
    // Drain mode: when active, inbound events go to channel instead of callbacks
    drain_mode: AtomicBool,
    // Lock-free bounded channel: Tokio tasks send (non-blocking), Python drains
    inbound_tx: CBSender<InboundEvent>,
    inbound_rx: CBReceiver<InboundEvent>,
    // Counter for dropped events (observability)
    inbound_dropped: AtomicU64,
    // Per-connection format preference (lock-free sync access for send_event)
    conn_formats: DashMap<String, bool>,
    // Per-connection rate limiter state (cleaned up on disconnect)
    conn_rates: DashMap<String, PerConnRate>,
    // Atomic connection count (lock-free read from Python without GIL blocking)
    connection_count: AtomicUsize,
    // JWT config: when set, Rust validates JWT in handshake (zero GIL)
    jwt_config: Option<JwtConfig>,
    // Graceful drain: when true, reject new connections
    draining: AtomicBool,
    pub(crate) topic_subscribers: Arc<DashMap<String, DashMap<String, ConnectionHandle>>>,
    conn_topics: DashMap<String, DashSet<String>>,
    // Per-connection topic ACL from JWT wse_topics claim (None entry = allow all)
    conn_topic_acl: DashMap<String, crate::jwt::TopicAcl>,
    // Count of glob-pattern topics (contains * or ?) -- avoids O(T) scan in fanout_topic_direct
    pub(crate) glob_topic_count: Arc<AtomicUsize>,
    // Cluster protocol
    cluster_cmd_tx: std::sync::RwLock<Option<mpsc::UnboundedSender<ClusterCommand>>>,
    pub(crate) cluster_metrics: Arc<ClusterMetrics>,
    cluster_dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    pub(crate) cluster_instance_id: std::sync::Mutex<Option<String>>,
    pub(crate) cluster_interest_tx:
        std::sync::RwLock<Option<mpsc::UnboundedSender<super::cluster::InterestUpdate>>>,
    pub(crate) local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    // True when cluster TLS is configured (reject cluster connections on main port)
    cluster_tls_enabled: AtomicBool,
    // ArcSwap holder for hot-reloading cluster TLS certificates at runtime
    pub(crate) cluster_tls_holder:
        std::sync::RwLock<Option<Arc<arc_swap::ArcSwap<super::cluster::ClusterTlsConfig>>>>,
    pub(crate) recovery: Option<Arc<super::recovery::RecoveryManager>>,
    // Per-connection last activity tracking for zombie detection
    conn_last_activity: DashMap<String, std::time::Instant>,
    // Per-connection AES-256-GCM cipher for E2E encryption (optional, client-initiated)
    conn_encryption: DashMap<String, aes_gcm::Aes256Gcm>,
    // Presence tracking (None when disabled)
    pub(crate) presence: Option<Arc<super::presence::PresenceManager>>,
    // Queue groups: topic -> group_name -> QueueGroup (round-robin distribution)
    pub(crate) queue_groups: Arc<DashMap<String, DashMap<String, QueueGroup>>>,
    // Reverse index: conn_id -> set of (topic, group_name) for cleanup on disconnect
    conn_queue_groups: DashMap<String, Vec<(String, String)>>,
    // Sender for presence broadcasts from disconnect cleanup paths
    // (where self.cmd_tx is not available, only Arc<SharedState>)
    presence_broadcast_tx: std::sync::RwLock<Option<mpsc::UnboundedSender<ServerCommand>>>,
    // Configurable limits
    rate_capacity: f64,
    rate_refill: f64,
    max_message_size: usize,
    ping_interval_secs: u64,
    idle_timeout_secs: u64,
    pub(crate) max_outbound_queue_bytes: usize,
    // Prometheus counters
    pub(crate) slow_consumer_drops: Arc<AtomicU64>,
    messages_received_total: AtomicU64,
    messages_sent_total: AtomicU64,
    connections_accepted_total: AtomicU64,
    connections_rejected_total: AtomicU64,
    bytes_received_total: AtomicU64,
    bytes_sent_total: AtomicU64,
    auth_failures_total: AtomicU64,
    rate_limited_total: AtomicU64,
    // Per-topic message delivery counts (for top-N Prometheus metrics)
    pub(crate) topic_message_counts: Arc<DashMap<String, AtomicU64>>,
}

impl SharedState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_connections: usize,
        jwt_config: Option<JwtConfig>,
        max_inbound_queue_size: usize,
        recovery: Option<Arc<super::recovery::RecoveryManager>>,
        presence_enabled: bool,
        presence_max_data_size: usize,
        presence_max_members: usize,
        rate_capacity: f64,
        rate_refill: f64,
        max_message_size: usize,
        ping_interval_secs: u64,
        idle_timeout_secs: u64,
        max_outbound_queue_bytes: usize,
    ) -> Self {
        let (tx, rx) = bounded(max_inbound_queue_size);
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections,
            on_connect: std::sync::RwLock::new(None),
            on_message: std::sync::RwLock::new(None),
            on_disconnect: std::sync::RwLock::new(None),
            drain_mode: AtomicBool::new(false),
            inbound_tx: tx,
            inbound_rx: rx,
            inbound_dropped: AtomicU64::new(0),
            conn_formats: DashMap::new(),
            conn_rates: DashMap::new(),
            connection_count: AtomicUsize::new(0),
            jwt_config,
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
            recovery,
            conn_last_activity: DashMap::new(),
            conn_encryption: DashMap::new(),
            presence: if presence_enabled {
                Some(Arc::new(super::presence::PresenceManager::new(
                    presence_max_data_size,
                    presence_max_members,
                )))
            } else {
                None
            },
            queue_groups: Arc::new(DashMap::new()),
            conn_queue_groups: DashMap::new(),
            presence_broadcast_tx: std::sync::RwLock::new(None),
            rate_capacity,
            rate_refill,
            max_message_size,
            ping_interval_secs,
            idle_timeout_secs,
            max_outbound_queue_bytes,
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
    fn push_inbound(&self, event: InboundEvent) {
        if self.inbound_tx.try_send(event).is_err() {
            self.inbound_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ---------------------------------------------------------------------------
// PyObject <-> serde_json::Value conversion
// ---------------------------------------------------------------------------

/// Convert a Python object to serde_json::Value.
fn pyobj_to_json(obj: &Bound<'_, PyAny>) -> serde_json::Value {
    if obj.is_none() {
        return serde_json::Value::Null;
    }
    // Bool before int (bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>()
        && let Ok(b) = obj.extract::<bool>()
    {
        return serde_json::Value::Bool(b);
    }
    if obj.is_instance_of::<PyInt>()
        && let Ok(i) = obj.extract::<i64>()
    {
        return serde_json::Value::Number(i.into());
    }
    if obj.is_instance_of::<PyFloat>()
        && let Ok(f) = obj.extract::<f64>()
    {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::Null;
    }
    if obj.is_instance_of::<PyString>()
        && let Ok(s) = obj.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    if obj.is_instance_of::<PyDict>()
        && let Ok(dict) = obj.cast::<PyDict>()
    {
        let mut map = serde_json::Map::new();
        for (k, v) in dict.iter() {
            let key = k.extract::<String>().unwrap_or_else(|_| format!("{}", k));
            map.insert(key, pyobj_to_json(&v));
        }
        return serde_json::Value::Object(map);
    }
    if obj.is_instance_of::<PyList>()
        && let Ok(list) = obj.cast::<PyList>()
    {
        let arr: Vec<serde_json::Value> = list.iter().map(|item| pyobj_to_json(&item)).collect();
        return serde_json::Value::Array(arr);
    }
    if obj.is_instance_of::<PyTuple>()
        && let Ok(tup) = obj.cast::<PyTuple>()
    {
        let arr: Vec<serde_json::Value> = tup.iter().map(|item| pyobj_to_json(&item)).collect();
        return serde_json::Value::Array(arr);
    }
    if obj.is_instance_of::<PyBytes>()
        && let Ok(b) = obj.extract::<Vec<u8>>()
    {
        use std::fmt::Write as FmtWrite;
        let mut hex = String::with_capacity(b.len() * 2);
        for byte in &b {
            let _ = write!(hex, "{:02x}", byte);
        }
        return serde_json::Value::String(hex);
    }
    // Try isoformat() for datetime objects
    if let Ok(iso) = obj.call_method0("isoformat")
        && let Ok(s) = iso.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    // Fallback: str(obj)
    if let Ok(s) = obj.str()
        && let Ok(s) = s.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    serde_json::Value::Null
}

/// Convert serde_json::Value to a Python object.
fn json_to_pyobj(py: Python<'_>, val: &serde_json::Value) -> Py<PyAny> {
    match val {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => (*b)
            .into_pyobject(py)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py).unwrap().into_any().unbind()
            } else if let Some(f) = n.as_f64() {
                f.into_pyobject(py).unwrap().into_any().unbind()
            } else {
                py.None()
            }
        }
        serde_json::Value::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                let _ = list.append(json_to_pyobj(py, item));
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                let _ = dict.set_item(k, json_to_pyobj(py, v));
            }
            dict.into_any().unbind()
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a presence event as a JSON message with category.
pub(crate) fn format_presence_msg(
    event_type: &str,
    user_id: &str,
    data: &serde_json::Value,
) -> String {
    let payload = serde_json::json!({
        "c": "WSE",
        "t": event_type,
        "p": { "user_id": user_id, "data": data }
    });
    payload.to_string()
}

fn fire_on_connect(callback: &Py<PyAny>, conn_id: &str, cookies: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id, cookies)) {
            tracing::warn!("[WSE] on_connect error: {e}");
        }
    });
}

fn fire_on_disconnect(callback: &Py<PyAny>, conn_id: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id,)) {
            tracing::warn!("[WSE] on_disconnect error: {e}");
        }
    });
}

/// Normalize JSON keys: "type"→"t", "payload"→"p" (done in Rust, no GIL needed).
fn normalize_json_keys(mut val: serde_json::Value) -> serde_json::Value {
    if let serde_json::Value::Object(ref mut map) = val {
        if !map.contains_key("t")
            && let Some(v) = map.remove("type")
        {
            map.insert("t".to_string(), v);
        }
        if !map.contains_key("p")
            && let Some(v) = map.remove("payload")
        {
            map.insert("p".to_string(), v);
        }
    }
    val
}

fn message_category(msg_type: &str) -> &'static str {
    if msg_type.contains("_snapshot") || msg_type.starts_with("snapshot_") {
        return "S";
    }
    match msg_type {
        "server_ready"
        | "server_hello"
        | "client_hello_ack"
        | "connection_state_change"
        | "subscription_update"
        | "snapshot_complete"
        | "error"
        | "pong"
        | "PONG" => "WSE",
        _ => "U",
    }
}

/// Inject `"c"` field at the start of a JSON object from Python publishers.
/// Depth-aware scan: only matches `"t"` and `"c"` keys at the top-level object (depth 1),
/// ignoring occurrences inside nested objects or string values.
fn inject_category(data: &str) -> String {
    let bytes = data.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut t_val_range: Option<(usize, usize)> = None;
    let mut first_brace: Option<usize> = None;
    let mut i = 0;

    while i < bytes.len() {
        if in_string {
            if bytes[i] == b'\\' {
                i += 1; // skip the backslash
                if i < bytes.len() && bytes[i] == b'u' {
                    i += 5; // skip uXXXX (4 hex digits + the 'u')
                } else {
                    i += 1; // skip single escaped char
                }
                continue;
            }
            if bytes[i] == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b'{' => {
                depth += 1;
                if first_brace.is_none() {
                    first_brace = Some(i);
                }
                i += 1;
            }
            b'}' => {
                depth -= 1;
                i += 1;
            }
            // Top-level "c": key -- category already present
            b'"' if depth == 1 && bytes[i..].starts_with(b"\"c\":") => {
                return data.to_string();
            }
            // Top-level "t":"value" -- extract event type
            b'"' if depth == 1 && t_val_range.is_none() && bytes[i..].starts_with(b"\"t\":\"") => {
                let val_start = i + 5;
                let mut j = val_start;
                loop {
                    if j >= bytes.len() {
                        break;
                    }
                    if bytes[j] == b'\\' {
                        j += 1; // skip backslash
                        if j < bytes.len() && bytes[j] == b'u' {
                            j += 5; // skip uXXXX
                        } else {
                            j += 1; // skip single escaped char
                        }
                        continue;
                    }
                    if bytes[j] == b'"' {
                        t_val_range = Some((val_start, j));
                        i = j + 1;
                        break;
                    }
                    j += 1;
                }
                if t_val_range.is_none() {
                    i += 1;
                }
            }
            // Top-level "t": with non-string value (number, null, bool) -- use "U" (unknown) category.
            // Synthesize a range pointing to "U" so inject_category still adds the "c" field.
            b'"' if depth == 1
                && t_val_range.is_none()
                && bytes[i..].starts_with(b"\"t\":")
                && !bytes[i..].starts_with(b"\"t\":\"") =>
            {
                // Non-string t value -- set a synthetic range that will map to "U" category
                // Use 0..0 as a sentinel: message_category("") returns "U"
                t_val_range = Some((0, 0));
                i += 4; // skip past "t":
            }
            b'"' => {
                in_string = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    if let (Some((vs, ve)), Some(brace)) = (t_val_range, first_brace) {
        let event_type = &data[vs..ve];
        let cat = message_category(event_type);
        let mut result = String::with_capacity(data.len() + cat.len() + 8);
        result.push_str(&data[..=brace]);
        result.push_str("\"c\":\"");
        result.push_str(cat);
        result.push_str("\",");
        result.push_str(&data[brace + 1..]);
        return result;
    }

    data.to_string()
}

// ---------------------------------------------------------------------------
// JWT helper functions (used by handle_connection)
// ---------------------------------------------------------------------------

/// Send an auth error + Close frame over the WS channel, drain it, and return.
/// Used for JWT rejection to avoid duplicating the send+drain pattern.
/// Reject a WebSocket connection with an auth error JSON body and close code 4401.
/// `code` and `message` are included in the JSON error body only; the WS close
/// frame always uses generic code 4401 to avoid leaking auth failure details.
async fn reject_ws_auth(
    tx: &mpsc::UnboundedSender<WsFrame>,
    mut rx: mpsc::UnboundedReceiver<WsFrame>,
    mut write_half: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<TcpStream>,
        Message,
    >,
    code: &str,
    message: &str,
) {
    let err = build_error_json(code, message);
    let _ = tx.send(WsFrame::Msg(Message::Text(err.into())));
    let _ = tx.send(WsFrame::Msg(Message::Close(Some(CloseFrame {
        code: CloseCode::Library(4401),
        reason: "Authentication failed".into(),
    }))));
    let write_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let WsFrame::Msg(msg) = frame {
                let is_close = matches!(msg, Message::Close(_));
                if write_half.feed(msg).await.is_err() {
                    break;
                }
                if is_close {
                    break;
                }
            }
        }
        let _ = write_half.flush().await;
        let _ = write_half.close().await;
    });
    let _ = write_task.await;
}

/// Build a JSON error message for auth failures.
fn build_error_json(code: &str, message: &str) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let payload = serde_json::json!({
        "c": "WSE",
        "t": "error",
        "p": {
            "code": code,
            "message": message,
            "timestamp": ts,
        },
        "v": 1
    });
    payload.to_string()
}

// ---------------------------------------------------------------------------
// E2E encryption helpers (ECDH P-256 + AES-GCM-256)
// ---------------------------------------------------------------------------

/// Perform ECDH key exchange: generate server keypair, derive AES-256 key.
/// Returns (server_public_key_base64, Aes256Gcm cipher).
fn derive_connection_key(client_pubkey_b64: &str) -> Result<(String, aes_gcm::Aes256Gcm), String> {
    use aes_gcm::KeyInit;
    use base64::Engine;
    use hkdf::Hkdf;
    use p256::ecdh::diffie_hellman;
    use p256::elliptic_curve::sec1::ToEncodedPoint;
    use p256::{PublicKey, SecretKey};
    use sha2::Sha256;

    let client_pubkey_bytes = base64::engine::general_purpose::STANDARD
        .decode(client_pubkey_b64)
        .map_err(|e| format!("Invalid base64: {e}"))?;

    let client_pk = PublicKey::from_sec1_bytes(&client_pubkey_bytes)
        .map_err(|e| format!("Invalid ECDH public key: {e}"))?;

    let server_sk = SecretKey::random(&mut aes_gcm::aead::OsRng);
    let server_pk = server_sk.public_key();

    let shared = diffie_hellman(server_sk.to_nonzero_scalar(), client_pk.as_affine());

    let hkdf = Hkdf::<Sha256>::new(Some(b"wse-encryption"), shared.raw_secret_bytes());
    let mut aes_key = [0u8; 32];
    hkdf.expand(b"aes-gcm-key", &mut aes_key)
        .map_err(|e| format!("HKDF expand failed: {e}"))?;

    let cipher = aes_gcm::Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| format!("AES key init failed: {e}"))?;

    let server_pk_point = server_pk.to_encoded_point(false);
    let server_pk_b64 =
        base64::engine::general_purpose::STANDARD.encode(server_pk_point.as_bytes());

    Ok((server_pk_b64, cipher))
}

/// Encrypt plaintext with AES-GCM-256: returns 12-byte IV + ciphertext + 16-byte tag.
fn encrypt_outbound(cipher: &aes_gcm::Aes256Gcm, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    use aes_gcm::AeadCore;
    use aes_gcm::aead::Aead;

    let nonce = aes_gcm::Aes256Gcm::generate_nonce(&mut aes_gcm::aead::OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| format!("AES-GCM encrypt failed: {e}"))?;
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt an AES-GCM-256 payload (12-byte IV + ciphertext + 16-byte tag).
fn decrypt_inbound(cipher: &aes_gcm::Aes256Gcm, data: &[u8]) -> Result<Vec<u8>, String> {
    use aes_gcm::aead::Aead;

    if data.len() < 28 {
        return Err("Encrypted payload too short (min 28 bytes)".into());
    }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let nonce: [u8; 12] = nonce_bytes.try_into().expect("split_at(12)");
    cipher
        .decrypt(&nonce.into(), ciphertext)
        .map_err(|e| format!("AES-GCM decrypt failed: {e}"))
}

/// Build server_hello JSON response to client_hello (protocol negotiation).
fn build_server_hello(
    conn_id: &str,
    client_proto: u32,
    state: &SharedState,
    encryption_pubkey: Option<&str>,
) -> String {
    let proto = client_proto.min(2);
    let recovery_enabled = state.recovery.is_some();
    let cluster_enabled = state.cluster_cmd_tx.read().unwrap().is_some();
    let encryption_enabled = encryption_pubkey.is_some();
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let mut val = serde_json::json!({
        "c": "WSE",
        "t": "server_hello",
        "p": {
            "server_version": env!("CARGO_PKG_VERSION"),
            "protocol_version": proto,
            "features": {
                "compression": true,
                "recovery": recovery_enabled,
                "cluster": cluster_enabled,
                "batching": true,
                "priority_queue": true,
                "msgpack": true,
                "encryption": encryption_enabled
            },
            "limits": {
                "max_message_size": state.max_message_size,
                "rate_limit_capacity": state.rate_capacity as u64,
                "rate_limit_refill": state.rate_refill as u64
            },
            "ping_interval": state.ping_interval_secs * 1000,
            "connection_id": conn_id,
            "server_time": ts
        },
        "v": 1
    });
    if let Some(pubkey) = encryption_pubkey {
        val["p"]["encryption_public_key"] = serde_json::Value::String(pubkey.to_string());
    }
    val.to_string()
}

/// Build server_ready JSON message (sent immediately from Rust after JWT validation).
fn build_server_ready(conn_id: &str, user_id: &str, recovery_enabled: bool) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let msg_id = Uuid::now_v7().to_string();
    let val = serde_json::json!({
        "c": "WSE",
        "t": "server_ready",
        "id": msg_id,
        "ts": ts,
        "seq": 1,
        "p": {
            "message": "Connection established (Rust transport)",
            "details": {
                "version": 1,
                "features": {
                    "compression": true,
                    "encryption": false,
                    "batching": true,
                    "priority_queue": true,
                    "circuit_breaker": true,
                    "rust_transport": true,
                    "rust_jwt": true,
                    "recovery": recovery_enabled
                },
                "connection_id": conn_id,
                "server_time": ts,
                "user_id": user_id
            }
        },
        "v": 1
    });
    val.to_string()
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<SharedState>) {
    // Reject new connections during graceful drain
    if state.draining.load(Ordering::Relaxed) {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    // Early rejection before expensive handshake (best-effort, authoritative check below)
    if state.connection_count.load(Ordering::Relaxed) >= state.max_connections {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[WSE] Max connections reached (early check), rejecting {addr}");
        return;
    }

    // Clone the TCP stream FD for pre-framed raw writes (bypasses tungstenite).
    let (stream, mut raw_write) = match clone_tcp_stream(stream) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::error!("[WSE] Failed to clone TCP stream for {addr}: {e}");
            return;
        }
    };

    // Lock-free handshake: OnceLock captures cookie + format in the callback
    struct HandshakeResult {
        cookies: String,
        wants_msgpack: bool,
        authorization: Option<String>,
    }
    let handshake_data: Arc<std::sync::OnceLock<HandshakeResult>> =
        Arc::new(std::sync::OnceLock::new());
    let hd_clone = handshake_data.clone();

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(state.max_message_size);
    ws_config.max_frame_size = Some(state.max_message_size);
    let ws_stream = match tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        #[allow(clippy::result_large_err)]
        move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
            let cookies = req
                .headers()
                .get("cookie")
                .and_then(|cv| cv.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let wants_msgpack = req
                .uri()
                .query()
                .is_some_and(|q| q.contains("format=msgpack"));
            let authorization = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            let _ = hd_clone.set(HandshakeResult {
                cookies,
                wants_msgpack,
                authorization,
            });
            Ok(response)
        },
        Some(ws_config),
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!("[WSE] WS handshake failed for {addr}: {e}");
            return;
        }
    };

    let (cookie_str, use_msgpack, auth_header) = handshake_data
        .get()
        .map(|hd| {
            (
                hd.cookies.clone(),
                hd.wants_msgpack,
                hd.authorization.clone(),
            )
        })
        .unwrap_or_default();
    let conn_id: Arc<String> = Arc::new(Uuid::now_v7().to_string());
    let (write_half, read_half) = ws_stream.split();
    let mut read_half = read_half;
    let mut write_half = write_half;
    let (tx, mut rx) = mpsc::unbounded_channel::<WsFrame>();
    let pending = Arc::new(AtomicUsize::new(0));
    let broadcast_buf = Arc::new(parking_lot::Mutex::new(BytesMut::with_capacity(512)));
    let broadcast_notify = Arc::new(tokio::sync::Notify::new());
    let drain = state.drain_mode.load(Ordering::Relaxed);

    // ── JWT validation in Rust (zero GIL) ──────────────────────────────────
    // When jwt_config is set, validate JWT from cookie or Authorization header.
    // On success: send server_ready immediately, push AuthConnect to drain queue.
    // On failure: send error + close — connection never registered.
    let rust_auth_user_id: Option<String> = if let Some(ref jwt_cfg) = state.jwt_config {
        let token = parse_cookie_value(&cookie_str, &jwt_cfg.cookie_name).or_else(|| {
            auth_header.as_deref().and_then(|h| {
                // RFC 7235: auth-scheme is case-insensitive
                if h.get(..7)
                    .is_some_and(|p| p.eq_ignore_ascii_case("bearer "))
                {
                    h.get(7..)
                } else {
                    None
                }
            })
        });
        match token {
            Some(tok) => match jwt::jwt_decode(tok, jwt_cfg) {
                Ok(claims) => {
                    if let Some(user_id) = claims
                        .get("sub")
                        .and_then(|s| s.as_str())
                        .filter(|s| !s.is_empty())
                    {
                        // Extract optional topic ACL from wse_topics claim
                        if let Some(acl) = jwt::extract_topic_acl(&claims) {
                            state.conn_topic_acl.insert(conn_id.to_string(), acl);
                        }
                        Some(user_id.to_string())
                    } else {
                        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                        state
                            .connections_rejected_total
                            .fetch_add(1, Ordering::Relaxed);
                        reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Token missing user ID")
                            .await;
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!("[WSE] JWT validation failed for {addr}: {e}");
                    state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                    state
                        .connections_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Authentication failed")
                        .await;
                    return;
                }
            },
            None => {
                state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                state
                    .connections_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                reject_ws_auth(
                    &tx,
                    rx,
                    write_half,
                    "AUTH_REQUIRED",
                    "No JWT cookie or Authorization header",
                )
                .await;
                return;
            }
        }
    } else {
        None // No JWT config — delegate auth to Python
    };

    // Register connection
    {
        let mut conns = state.connections.write().await;
        if conns.len() >= state.max_connections {
            state
                .connections_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!("[WSE] Max connections reached, rejecting {addr}");
            // Clean up ACL entry inserted before registration check
            state.conn_topic_acl.remove(&*conn_id);
            let _ = write_half.close().await;
            return;
        }
        // Send server_ready BEFORE inserting into connections map.
        // While we hold the write lock, no broadcast can acquire a read lock,
        // so server_ready is guaranteed to be first in the channel.
        if let Some(ref user_id) = rust_auth_user_id {
            let recovery_enabled = state.recovery.is_some();
            let server_ready = build_server_ready(&conn_id, user_id, recovery_enabled);
            let sr_bytes = server_ready.len() + 4;
            pending.fetch_add(sr_bytes, Ordering::Relaxed);
            if tx
                .send(WsFrame::Msg(Message::Text(server_ready.into())))
                .is_err()
            {
                pending.fetch_sub(sr_bytes, Ordering::Relaxed);
            }
        }
        conns.insert(
            (*conn_id).clone(),
            ConnectionHandle {
                tx: tx.clone(),
                pending: pending.clone(),
                broadcast_buf: broadcast_buf.clone(),
                broadcast_notify: broadcast_notify.clone(),
            },
        );
        state.connection_count.store(conns.len(), Ordering::Relaxed);
        state
            .connections_accepted_total
            .fetch_add(1, Ordering::Relaxed);
    }
    // Store format preference for lock-free access from send_event()
    if use_msgpack {
        state.conn_formats.insert((*conn_id).clone(), true);
    }
    // Initialize last activity for zombie detection
    state
        .conn_last_activity
        .insert((*conn_id).clone(), std::time::Instant::now());

    // Fire on_connect (drain mode: push to queue; callback mode: background task)
    if drain {
        if let Some(user_id) = rust_auth_user_id {
            // Register user_id for presence lookups
            if let Some(ref pm) = state.presence {
                pm.register_connection(&conn_id, &user_id);
            }
            // Rust validated JWT — send AuthConnect (Python skips JWT decode)
            state.push_inbound(InboundEvent::AuthConnect {
                conn_id: (*conn_id).clone(),
                user_id,
            });
        } else {
            // No JWT config — send cookies for Python to validate
            state.push_inbound(InboundEvent::Connect {
                conn_id: (*conn_id).clone(),
                cookies: cookie_str.clone(),
            });
        }
    } else {
        let cb_guard = state.on_connect.read().unwrap();
        if let Some(ref cb) = *cb_guard
            && let Some(cb_clone) = Python::try_attach(|py| cb.clone_ref(py))
        {
            let cid = (*conn_id).clone();
            let ck = cookie_str.clone();
            drop(cb_guard);
            // Fire in background — don't block connection setup on GIL
            tokio::task::spawn_blocking(move || {
                fire_on_connect(&cb_clone, &cid, &ck);
            });
        }
    }

    // Write task: dual-path (NATS-style broadcast buffer + mpsc control channel).
    //
    // Phase 1 (top of loop): drain broadcast_buf (direct bytes from broadcast_all)
    // Phase 2 (select!): wait for control mpsc OR broadcast notify
    //
    // Control messages (server_ready, pong, close, send_event, topic) go through mpsc.
    // Broadcast data goes through direct buffer (bypasses mpsc entirely).
    let pending_write = pending.clone();
    let broadcast_buf_write = broadcast_buf;
    let broadcast_notify_write = broadcast_notify;
    let write_task = tokio::spawn(async move {
        let mut batch: Vec<WsFrame> = Vec::with_capacity(256);
        let mut raw_slices: Vec<Bytes> = Vec::with_capacity(256);
        // Skip Phase 1 on first iteration so server_ready (in mpsc) is sent before
        // any broadcast data (in broadcast_buf). After the first iteration, Phase 1
        // always runs to avoid the Notify lost-permit race.
        let mut first_iteration = true;

        loop {
            // Phase 1: Drain broadcast buffer (skip on first iteration for server_ready ordering).
            // This avoids the Notify lost-permit race: if select! cancelled notified()
            // on a previous iteration, the data is still in the buffer.
            let broadcast_data = if first_iteration {
                first_iteration = false;
                None
            } else {
                let mut buf = broadcast_buf_write.lock();
                if !buf.is_empty() {
                    Some(buf.split().freeze()) // zero-copy handoff
                } else {
                    None
                }
            };
            if let Some(data) = broadcast_data {
                let len = data.len();
                if raw_write.write_all(&data).await.is_err() {
                    pending_write.fetch_sub(len, Ordering::Relaxed);
                    break;
                }
                pending_write.fetch_sub(len, Ordering::Relaxed);
                continue; // check buffer again before blocking
            }

            // Phase 2: Buffer empty, wait for either source.
            tokio::select! {
                biased;

                // Control/topic messages (server_ready, close, pong, send_event, topic)
                n = rx.recv_many(&mut batch, 256) => {
                    if n == 0 {
                        break; // channel closed
                    }
                    let drained_bytes: usize = batch.iter().map(ws_frame_size).sum();

                    // Separate: PreFramed -> slices for writev, Msg -> tungstenite
                    raw_slices.clear();
                    let mut has_tung = false;
                    for frame in &batch {
                        match frame {
                            WsFrame::PreFramed(bytes) => raw_slices.push(bytes.clone()),
                            WsFrame::Msg(_) => has_tung = true,
                        }
                    }

                    let mut ok = true;

                    // Flush tungstenite Msgs FIRST to ensure its internal buffer
                    // is drained before raw_write uses the same fd
                    if has_tung && ok {
                        for frame in batch.drain(..) {
                            if let WsFrame::Msg(msg) = frame
                                && write_half.feed(msg).await.is_err()
                            {
                                ok = false;
                                break;
                            }
                        }
                        if ok && write_half.flush().await.is_err() {
                            ok = false;
                        }
                    }

                    // Write PreFramed via write_vectored (writev syscall)
                    if !raw_slices.is_empty() && ok {
                        let io_slices: Vec<std::io::IoSlice<'_>> = raw_slices
                            .iter()
                            .map(|b| std::io::IoSlice::new(b))
                            .collect();
                        let total: usize = raw_slices.iter().map(|b| b.len()).sum();

                        let written = match raw_write.write_vectored(&io_slices).await {
                            Ok(0) => { ok = false; 0 }
                            Ok(n) => n,
                            Err(_) => { ok = false; 0 }
                        };

                        if ok && written < total {
                            let mut remaining = BytesMut::with_capacity(total - written);
                            let mut skip = written;
                            for slice in &raw_slices {
                                if skip >= slice.len() {
                                    skip -= slice.len();
                                } else {
                                    remaining.extend_from_slice(&slice[skip..]);
                                    skip = 0;
                                }
                            }
                            if raw_write.write_all(&remaining).await.is_err() {
                                ok = false;
                            }
                        }
                    }

                    pending_write.fetch_sub(drained_bytes, Ordering::Relaxed);
                    batch.clear();

                    if !ok {
                        break;
                    }
                }

                // Broadcast buffer wakeup -- loop back to Phase 1 to drain
                _ = broadcast_notify_write.notified() => {
                    continue;
                }
            }
        }
        let _ = write_half.close().await;
    });

    // Cache on_message callback (only needed in callback mode)
    let cached_on_message: Option<Arc<Py<PyAny>>> = if !drain {
        let cb_guard = state.on_message.read().unwrap();
        cb_guard
            .as_ref()
            .and_then(|cb| Python::try_attach(|py| cb.clone_ref(py)).map(Arc::new))
    } else {
        None
    };

    // Read loop
    while let Some(Ok(msg)) = read_half.next().await {
        match msg {
            Message::Text(text) => {
                state
                    .messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .bytes_received_total
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                // Update last activity for zombie detection
                state
                    .conn_last_activity
                    .insert((*conn_id).clone(), std::time::Instant::now());
                let text_ref: &str = text.as_ref();

                // Strip prefix for JSON parsing
                let json_str = if text_ref.starts_with("WSE{") {
                    &text_ref[3..]
                } else if text_ref.starts_with("S{") || text_ref.starts_with("U{") {
                    &text_ref[1..]
                } else if text_ref.starts_with('{') {
                    text_ref
                } else {
                    // Non-JSON text — drain or callback
                    if drain {
                        state.push_inbound(InboundEvent::RawText {
                            conn_id: (*conn_id).clone(),
                            text: text.to_string(),
                        });
                    } else if let Some(ref cb) = cached_on_message {
                        let cb_arc = cb.clone();
                        let cid = Arc::clone(&conn_id);
                        let t = text.to_string();
                        tokio::task::spawn_blocking(move || {
                            Python::try_attach(|py| {
                                if let Err(e) = cb_arc.call1(py, (&**cid, &*t)) {
                                    tracing::warn!("[WSE] on_message error: {e}");
                                }
                            });
                        });
                    }
                    continue;
                };

                // Try to parse JSON in Rust
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(val) => {
                        // Fast-path: protocol messages handled entirely in Rust
                        if let Some(t_val) = val.get("t").and_then(|t| t.as_str()) {
                            if t_val == "client_hello" {
                                let p = val.get("p");
                                let client_proto =
                                    p.and_then(|p| p.get("protocol_version"))
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(1) as u32;

                                // Check if client requests E2E encryption
                                let wants_encryption = p
                                    .and_then(|p| p.get("features"))
                                    .and_then(|f| f.get("encryption"))
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);

                                let server_pubkey: Option<String> = if wants_encryption {
                                    p.and_then(|p| p.get("encryption_public_key"))
                                        .and_then(|v| v.as_str())
                                        .and_then(|b64| match derive_connection_key(b64) {
                                            Ok((server_pk_b64, cipher)) => {
                                                state
                                                    .conn_encryption
                                                    .insert((*conn_id).clone(), cipher);
                                                Some(server_pk_b64)
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "[WSE] ECDH key exchange failed for {}: {}",
                                                    conn_id, e
                                                );
                                                let err_msg = format!(
                                                    "{{\"c\":\"WSE\",\"t\":\"error\",\"p\":{{\"code\":\"KEY_EXCHANGE_FAILED\",\"message\":\"ECDH key exchange failed: {}\",\"recoverable\":true}}}}",
                                                    e.replace('\"', "\\\"")
                                                );
                                                let em_bytes = err_msg.len() + 4;
                                                pending.fetch_add(em_bytes, Ordering::Relaxed);
                                                if tx.send(WsFrame::Msg(Message::Text(err_msg.into()))).is_err() {
                                                    pending.fetch_sub(em_bytes, Ordering::Relaxed);
                                                }
                                                None
                                            }
                                        })
                                } else {
                                    None
                                };

                                let hello = build_server_hello(
                                    &conn_id,
                                    client_proto,
                                    &state,
                                    server_pubkey.as_deref(),
                                );
                                let h_bytes = hello.len() + 4;
                                pending.fetch_add(h_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Text(hello.into()))).is_err() {
                                    pending.fetch_sub(h_bytes, Ordering::Relaxed);
                                }
                                continue;
                            }
                            if t_val == "ping" {
                                let timestamp = val
                                    .get("p")
                                    .and_then(|p| p.get("timestamp"))
                                    .and_then(|ts| ts.as_i64())
                                    .unwrap_or(0);
                                let server_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis()
                                    as i64;
                                let pong = format!(
                                    "{{\"c\":\"WSE\",\"t\":\"PONG\",\"p\":{{\"client_timestamp\":{},\"server_timestamp\":{},\"latency\":{}}},\"v\":1}}",
                                    timestamp,
                                    server_ts,
                                    server_ts.saturating_sub(timestamp).max(0)
                                );
                                let po_bytes = pong.len() + 4;
                                pending.fetch_add(po_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Text(pong.into()))).is_err() {
                                    pending.fetch_sub(po_bytes, Ordering::Relaxed);
                                }
                                continue;
                            }
                            if t_val == "pong" || t_val == "PONG" {
                                continue;
                            }
                        }

                        // Normalize type→t, payload→p in serde_json (no GIL needed)
                        let val = normalize_json_keys(val);

                        // Drain mode: push serde_json::Value to queue (zero GIL)
                        if drain {
                            state.push_inbound(InboundEvent::Message {
                                conn_id: (*conn_id).clone(),
                                value: val,
                            });
                        } else if let Some(ref cb) = cached_on_message {
                            // Callback mode: convert to PyDict + call Python
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            tokio::task::spawn_blocking(move || {
                                Python::try_attach(|py| {
                                    let py_obj = json_to_pyobj(py, &val);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_obj)) {
                                        tracing::warn!("[WSE] on_message error: {e}");
                                    }
                                });
                            });
                        }
                    }
                    Err(_) => {
                        // JSON parse failed — pass raw string
                        if drain {
                            state.push_inbound(InboundEvent::RawText {
                                conn_id: (*conn_id).clone(),
                                text: text.to_string(),
                            });
                        } else if let Some(ref cb) = cached_on_message {
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            let t = text.to_string();
                            tokio::task::spawn_blocking(move || {
                                Python::try_attach(|py| {
                                    if let Err(e) = cb_arc.call1(py, (&**cid, &*t)) {
                                        tracing::warn!("[WSE] on_message error: {e}");
                                    }
                                });
                            });
                        }
                    }
                }
            }
            Message::Binary(data) => {
                state
                    .messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .bytes_received_total
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                // Update last activity for zombie detection
                state
                    .conn_last_activity
                    .insert((*conn_id).clone(), std::time::Instant::now());

                // E2E encryption: detect E: prefix and decrypt before processing
                if data.len() >= 2 && data[0] == b'E' && data[1] == b':' {
                    if let Some(cipher_ref) = state.conn_encryption.get(&*conn_id) {
                        match decrypt_inbound(&cipher_ref, &data[2..]) {
                            Ok(plaintext) => {
                                // Decrypted payload is UTF-8 text (JSON with optional prefix)
                                if let Ok(text) = std::str::from_utf8(&plaintext) {
                                    let json_str = if text.starts_with("WSE{") {
                                        &text[3..]
                                    } else if text.starts_with("S{") || text.starts_with("U{") {
                                        &text[1..]
                                    } else if text.starts_with('{') {
                                        text
                                    } else {
                                        if drain {
                                            state.push_inbound(InboundEvent::RawText {
                                                conn_id: (*conn_id).clone(),
                                                text: text.to_string(),
                                            });
                                        }
                                        continue;
                                    };
                                    if let Ok(val) =
                                        serde_json::from_str::<serde_json::Value>(json_str)
                                    {
                                        let val = normalize_json_keys(val);
                                        if drain {
                                            state.push_inbound(InboundEvent::Message {
                                                conn_id: (*conn_id).clone(),
                                                value: val,
                                            });
                                        } else if let Some(ref cb) = cached_on_message {
                                            let cb_arc = cb.clone();
                                            let cid = Arc::clone(&conn_id);
                                            tokio::task::spawn_blocking(move || {
                                                Python::try_attach(|py| {
                                                    let py_obj = json_to_pyobj(py, &val);
                                                    if let Err(e) =
                                                        cb_arc.call1(py, (&**cid, py_obj))
                                                    {
                                                        tracing::warn!(
                                                            "[WSE] on_message (decrypted) error: {e}"
                                                        );
                                                    }
                                                });
                                            });
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] Decryption failed for {}: {}", conn_id, e);
                            }
                        }
                    } else {
                        tracing::error!(
                            "[WSE] Received E: frame but no encryption key for {}",
                            conn_id
                        );
                    }
                    continue;
                }

                // Check if this connection uses msgpack (lock-free DashMap read)
                let is_msgpack = state
                    .conn_formats
                    .get(&*conn_id)
                    .map(|v| *v)
                    .unwrap_or(false);

                if drain {
                    if is_msgpack {
                        // Parse msgpack in Rust → push pre-parsed dict (same as JSON text path)
                        match rmpv::decode::read_value(&mut &data[..]) {
                            Ok(rmpv_val) => {
                                let json_val = rmpv_to_serde_json(&rmpv_val);
                                let json_val = normalize_json_keys(json_val);
                                state.push_inbound(InboundEvent::Message {
                                    conn_id: (*conn_id).clone(),
                                    value: json_val,
                                });
                            }
                            Err(_) => {
                                // Not valid msgpack — pass raw bytes
                                state.push_inbound(InboundEvent::Binary {
                                    conn_id: (*conn_id).clone(),
                                    data: data.to_vec(),
                                });
                            }
                        }
                    } else {
                        state.push_inbound(InboundEvent::Binary {
                            conn_id: (*conn_id).clone(),
                            data: data.to_vec(),
                        });
                    }
                } else if let Some(ref cb) = cached_on_message {
                    let cb_arc = cb.clone();
                    let cid = Arc::clone(&conn_id);
                    let d = data.to_vec();
                    if is_msgpack {
                        // Parse msgpack and pass as dict to callback (same as JSON text)
                        tokio::task::spawn_blocking(move || {
                            Python::try_attach(|py| match rmpv::decode::read_value(&mut &d[..]) {
                                Ok(rmpv_val) => {
                                    let json_val = rmpv_to_serde_json(&rmpv_val);
                                    let json_val = normalize_json_keys(json_val);
                                    let py_obj = json_to_pyobj(py, &json_val);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_obj)) {
                                        tracing::warn!("[WSE] on_message (msgpack) error: {e}");
                                    }
                                }
                                Err(_) => {
                                    let py_bytes = PyBytes::new(py, &d);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                        tracing::warn!("[WSE] on_message (binary) error: {e}");
                                    }
                                }
                            });
                        });
                    } else {
                        tokio::task::spawn_blocking(move || {
                            Python::try_attach(|py| {
                                let py_bytes = PyBytes::new(py, &d);
                                if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                    tracing::warn!("[WSE] on_message (binary) error: {e}");
                                }
                            });
                        });
                    }
                }
            }
            Message::Ping(payload) => {
                let pong_bytes = payload.len() + 4;
                pending.fetch_add(pong_bytes, Ordering::Relaxed);
                if tx.send(WsFrame::Msg(Message::Pong(payload))).is_err() {
                    pending.fetch_sub(pong_bytes, Ordering::Relaxed);
                }
            }
            Message::Close(_) => break,
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    // Cleanup - check if zombie detection already removed this connection
    let already_removed = {
        let mut conns = state.connections.write().await;
        let was_present = conns.remove(&*conn_id).is_some();
        state.connection_count.store(conns.len(), Ordering::Relaxed);
        !was_present
    };
    state.conn_formats.remove(&*conn_id);
    state.conn_rates.remove(&*conn_id);
    state.conn_last_activity.remove(&*conn_id);
    state.conn_encryption.remove(&*conn_id);
    state.conn_topic_acl.remove(&*conn_id);
    // Queue group cleanup: remove this connection from all queue groups
    if let Some((_, memberships)) = state.conn_queue_groups.remove(&*conn_id) {
        for (topic, group_name) in memberships {
            if let Some(topic_groups) = state.queue_groups.get(&topic) {
                if let Some(mut group) = topic_groups.get_mut(&group_name) {
                    group.members.retain(|(id, _)| id != &*conn_id);
                }
                // Clean up empty group
                topic_groups.remove_if(&group_name, |_, g| g.members.is_empty());
            }
            // Clean up empty topic entry
            state
                .queue_groups
                .remove_if(&topic, |_, groups| groups.is_empty());
        }
    }
    // If zombie detection already removed this connection and emitted
    // a disconnect event, skip the rest to avoid duplicate events.
    if already_removed {
        drop(tx);
        let _ = write_task.await;
        return;
    }
    // Presence cleanup: untrack from all topics before disconnect event
    if let Some(ref pm) = state.presence {
        let results = pm.remove_connection(&conn_id);
        let cluster_tx = state.cluster_cmd_tx.read().unwrap().clone();
        for (topic, result) in results {
            if let super::presence::UntrackResult::LastLeave { user_id, data } = result {
                if drain {
                    state.push_inbound(InboundEvent::PresenceLeave {
                        topic: topic.clone(),
                        user_id: user_id.clone(),
                        data: data.clone(),
                    });
                }
                // Broadcast to topic subscribers via WebSocket
                let tx = state.presence_broadcast_tx.read().unwrap().clone();
                if let Some(tx) = tx {
                    let msg = format_presence_msg("presence_leave", &user_id, &data);
                    let _ = tx.send(ServerCommand::BroadcastLocal {
                        topic: topic.clone(),
                        data: msg,
                        skip_recovery: true,
                    });
                }
                // Cluster sync: notify peers of presence leave
                if let Some(ref ctx) = cluster_tx {
                    let data_str = serde_json::to_string(&data).unwrap_or_default();
                    let now = super::presence::epoch_ms();
                    let _ = ctx.send(ClusterCommand::PresenceUpdate {
                        topic,
                        user_id,
                        action: 1, // leave
                        data: data_str,
                        updated_at: now,
                    });
                }
            }
        }
    }
    // Pub/Sub cleanup: remove connection from all subscribed topics
    if let Some((_, topics)) = state.conn_topics.remove(&*conn_id) {
        let mut disconnected_topics = Vec::new();
        for topic_ref in topics.iter() {
            let topic = topic_ref.clone();
            if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                subscribers.remove(&*conn_id);
            }
            if state
                .topic_subscribers
                .remove_if(&topic, |_, subs| subs.is_empty())
                .is_some()
                && (topic.contains('*') || topic.contains('?'))
            {
                state.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
            }
            disconnected_topics.push(topic);
        }

        // Emit UNSUB to cluster for topics whose refcount went 1->0
        let cluster_tx = state.cluster_cmd_tx.read().unwrap().clone();
        if let Some(tx) = cluster_tx {
            let mut refcounts = state
                .local_topic_refcount
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for topic in &disconnected_topics {
                if let Some(count) = refcounts.get_mut(topic) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        refcounts.remove(topic);
                        let _ = tx.send(ClusterCommand::Unsub {
                            topic: topic.clone(),
                        });
                    }
                }
            }
        }
    }
    if drain {
        state.push_inbound(InboundEvent::Disconnect {
            conn_id: (*conn_id).clone(),
        });
    } else {
        let cb_guard = state.on_disconnect.read().unwrap();
        if let Some(ref cb) = *cb_guard
            && let Some(cb_clone) = Python::try_attach(|py| cb.clone_ref(py))
        {
            let cid = (*conn_id).clone();
            drop(cb_guard);
            // Fire in background — don't block cleanup on GIL
            tokio::task::spawn_blocking(move || {
                fire_on_disconnect(&cb_clone, &cid);
            });
        }
    }
    drop(tx);
    let _ = write_task.await;
}

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pb = pattern.as_bytes();
    let tb = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);

    while ti < tb.len() {
        if pi < pb.len() && (pb[pi] == b'?' || pb[pi] == tb[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pb.len() && pb[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pb.len() && pb[pi] == b'*' {
        pi += 1;
    }
    pi == pb.len()
}

// ---------------------------------------------------------------------------
// Parallel fan-out helpers
// ---------------------------------------------------------------------------

/// NATS-style direct buffer fan-out for broadcast_all (BroadcastText/BroadcastBytes).
/// Appends pre-framed bytes to each connection's broadcast_buf under parking_lot::Mutex,
/// bypassing the mpsc channel entirely. Per-connection cost: ~15-20ns vs ~80-130ns for mpsc.
/// Only calls notify_one() when buffer transitions from empty to non-empty (skip when
/// write task is already awake draining data).
#[inline]
fn fanout_broadcast_direct(
    conns: &HashMap<String, ConnectionHandle>,
    data: &Bytes,
    max_pending: usize,
    slow_drops: &AtomicU64,
) {
    let data_len = data.len();
    for h in conns.values() {
        // Skip dead connections (write task exited but cleanup hasn't run yet)
        if h.tx.is_closed() {
            continue;
        }
        if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        h.pending.fetch_add(data_len, Ordering::Relaxed);
        let was_empty = {
            let mut buf = h.broadcast_buf.lock();
            let empty = buf.is_empty();
            buf.extend_from_slice(data);
            empty
        };
        if was_empty {
            h.broadcast_notify.notify_one();
        }
    }
}

/// NATS-style direct buffer fan-out for topic-filtered connections (BroadcastLocal + cluster).
/// Iterates ConnectionHandle values stored directly in topic_subscribers DashMap,
/// eliminating the per-connection HashMap lookup that was the main bottleneck.
/// Returns (delivered, dropped) counts for metrics tracking.
#[inline]
pub(crate) fn fanout_topic_direct(
    topic_subscribers: &DashMap<String, DashMap<String, ConnectionHandle>>,
    topic: &str,
    data: &Bytes,
    max_pending: usize,
    slow_drops: &AtomicU64,
    glob_count: usize,
) -> (u64, u64) {
    let data_len = data.len();
    let mut delivered = 0u64;
    let mut dropped = 0u64;

    // Check if any glob patterns exist (rare -- most deployments use exact topics)
    let has_globs = glob_count > 0;

    if !has_globs {
        // Fast path: exact match only, iterate handles directly (no HashMap lookup)
        if let Some(subs) = topic_subscribers.get(topic) {
            for entry in subs.iter() {
                let h = entry.value();
                if h.tx.is_closed() {
                    continue;
                }
                if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                    slow_drops.fetch_add(1, Ordering::Relaxed);
                    dropped += 1;
                    continue;
                }
                h.pending.fetch_add(data_len, Ordering::Relaxed);
                let was_empty = {
                    let mut buf = h.broadcast_buf.lock();
                    let empty = buf.is_empty();
                    buf.extend_from_slice(data);
                    empty
                };
                if was_empty {
                    h.broadcast_notify.notify_one();
                }
                delivered += 1;
            }
        }
    } else {
        // Slow path: glob patterns exist, need AHashSet for dedup
        let mut seen = AHashSet::new();

        // Exact topic match
        if let Some(subs) = topic_subscribers.get(topic) {
            for entry in subs.iter() {
                if seen.insert(entry.key().clone()) {
                    let h = entry.value();
                    if h.tx.is_closed() {
                        continue;
                    }
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        slow_drops.fetch_add(1, Ordering::Relaxed);
                        dropped += 1;
                        continue;
                    }
                    h.pending.fetch_add(data_len, Ordering::Relaxed);
                    let was_empty = {
                        let mut buf = h.broadcast_buf.lock();
                        let empty = buf.is_empty();
                        buf.extend_from_slice(data);
                        empty
                    };
                    if was_empty {
                        h.broadcast_notify.notify_one();
                    }
                    delivered += 1;
                }
            }
        }

        // Glob pattern match
        for topic_entry in topic_subscribers.iter() {
            let pattern = topic_entry.key();
            if (pattern.contains('*') || pattern.contains('?')) && glob_match(pattern, topic) {
                for entry in topic_entry.value().iter() {
                    if seen.insert(entry.key().clone()) {
                        let h = entry.value();
                        if h.tx.is_closed() {
                            continue;
                        }
                        if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            dropped += 1;
                            continue;
                        }
                        h.pending.fetch_add(data_len, Ordering::Relaxed);
                        let was_empty = {
                            let mut buf = h.broadcast_buf.lock();
                            let empty = buf.is_empty();
                            buf.extend_from_slice(data);
                            empty
                        };
                        if was_empty {
                            h.broadcast_notify.notify_one();
                        }
                        delivered += 1;
                    }
                }
            }
        }
    }

    (delivered, dropped)
}

/// Trait for fan-out metrics tracking. Implemented by ClusterMetrics.
pub(crate) trait FanoutMetrics: Send + Sync + 'static {
    fn add_delivered(&self, count: u64);
    fn add_dropped(&self, count: u64);
}

// ---------------------------------------------------------------------------
// Command processor
// ---------------------------------------------------------------------------

async fn process_commands(
    mut cmd_rx: mpsc::UnboundedReceiver<ServerCommand>,
    state: Arc<SharedState>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ServerCommand::SendText { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = data.len() + 4;
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx.send(WsFrame::Msg(Message::Text(data.into()))).is_err() {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = data.len() + 4;
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx
                            .send(WsFrame::Msg(Message::Binary(data.into())))
                            .is_err()
                        {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = ws_frame_size(&WsFrame::Msg(message.clone()));
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx.send(WsFrame::Msg(message)).is_err() {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::BroadcastText { data } => {
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                if state.conn_encryption.is_empty() {
                    // NATS-style: direct buffer append (bypasses mpsc channel)
                    let preframed = pre_frame_text(&data);
                    let guard = state.connections.read().await;
                    fanout_broadcast_direct(&guard, &preframed, max_pending, slow_drops);
                } else {
                    let preframed = pre_frame_text(&data);
                    let guard = state.connections.read().await;
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), h.pending.clone(), cipher_ref.clone()));
                        } else {
                            // Plain connection: direct buffer append
                            let data_len = preframed.len();
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            h.pending.fetch_add(data_len, Ordering::Relaxed);
                            let was_empty = {
                                let mut buf = h.broadcast_buf.lock();
                                let empty = buf.is_empty();
                                buf.extend_from_slice(&preframed);
                                empty
                            };
                            if was_empty {
                                h.broadcast_notify.notify_one();
                            }
                        }
                    }
                    drop(guard);
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, data.as_bytes()) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
            }
            ServerCommand::BroadcastBytes { data } => {
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                if state.conn_encryption.is_empty() {
                    // NATS-style: direct buffer append (bypasses mpsc channel)
                    let preframed = pre_frame_binary(&data);
                    let guard = state.connections.read().await;
                    fanout_broadcast_direct(&guard, &preframed, max_pending, slow_drops);
                } else {
                    let preframed = pre_frame_binary(&data);
                    let guard = state.connections.read().await;
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), h.pending.clone(), cipher_ref.clone()));
                        } else {
                            // Plain connection: direct buffer append
                            let data_len = preframed.len();
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            h.pending.fetch_add(data_len, Ordering::Relaxed);
                            let was_empty = {
                                let mut buf = h.broadcast_buf.lock();
                                let empty = buf.is_empty();
                                buf.extend_from_slice(&preframed);
                                empty
                            };
                            if was_empty {
                                h.broadcast_notify.notify_one();
                            }
                        }
                    }
                    drop(guard);
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, &data) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
            }
            ServerCommand::BroadcastLocal {
                topic,
                data,
                skip_recovery,
            } => {
                let preframed = pre_frame_text(&data);
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                // Store in recovery buffer (skip for presence events)
                if !skip_recovery && let Some(ref recovery) = state.recovery {
                    recovery.push(&topic, preframed.clone());
                }
                // Track per-topic message count for Prometheus top-N metrics
                state
                    .topic_message_counts
                    .entry(topic.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                if state.conn_encryption.is_empty() {
                    // Fast path: NATS-style direct buffer (bypasses mpsc channel)
                    fanout_topic_direct(
                        &state.topic_subscribers,
                        &topic,
                        &preframed,
                        max_pending,
                        slow_drops,
                        state.glob_topic_count.load(Ordering::Relaxed),
                    );
                } else {
                    // Mixed: direct buffer for plain, mpsc for encrypted
                    // No connections RwLock needed -- handles are in topic_subscribers
                    let mut enc_senders = Vec::new();
                    let mut seen = AHashSet::new();
                    if let Some(subs) = state.topic_subscribers.get(&topic) {
                        for entry in subs.iter() {
                            let cid = entry.key().clone();
                            if seen.insert(cid.clone()) {
                                let h = entry.value();
                                if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                    enc_senders.push((
                                        h.tx.clone(),
                                        h.pending.clone(),
                                        cipher_ref.clone(),
                                    ));
                                } else {
                                    // Plain connection: direct buffer append
                                    if h.tx.is_closed() {
                                        continue;
                                    }
                                    let data_len = preframed.len();
                                    if max_pending > 0
                                        && h.pending.load(Ordering::Relaxed) >= max_pending
                                    {
                                        slow_drops.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                    h.pending.fetch_add(data_len, Ordering::Relaxed);
                                    let was_empty = {
                                        let mut buf = h.broadcast_buf.lock();
                                        let empty = buf.is_empty();
                                        buf.extend_from_slice(&preframed);
                                        empty
                                    };
                                    if was_empty {
                                        h.broadcast_notify.notify_one();
                                    }
                                }
                            }
                        }
                    }
                    // Glob pattern matching
                    for topic_entry in state.topic_subscribers.iter() {
                        let pattern = topic_entry.key();
                        if (pattern.contains('*') || pattern.contains('?'))
                            && glob_match(pattern, &topic)
                        {
                            for entry in topic_entry.value().iter() {
                                let cid = entry.key().clone();
                                if seen.insert(cid.clone()) {
                                    let h = entry.value();
                                    if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                        enc_senders.push((
                                            h.tx.clone(),
                                            h.pending.clone(),
                                            cipher_ref.clone(),
                                        ));
                                    } else {
                                        // Plain connection: direct buffer append
                                        if h.tx.is_closed() {
                                            continue;
                                        }
                                        let data_len = preframed.len();
                                        if max_pending > 0
                                            && h.pending.load(Ordering::Relaxed) >= max_pending
                                        {
                                            slow_drops.fetch_add(1, Ordering::Relaxed);
                                            continue;
                                        }
                                        h.pending.fetch_add(data_len, Ordering::Relaxed);
                                        let was_empty = {
                                            let mut buf = h.broadcast_buf.lock();
                                            let empty = buf.is_empty();
                                            buf.extend_from_slice(&preframed);
                                            empty
                                        };
                                        if was_empty {
                                            h.broadcast_notify.notify_one();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, data.as_bytes()) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
                // Queue group dispatch: round-robin to one member per group.
                // Retry on dead/backpressured members to avoid silent message loss.
                if let Some(topic_groups) = state.queue_groups.get(&topic) {
                    for mut group_entry in topic_groups.iter_mut() {
                        let group = group_entry.value_mut();
                        let len = group.members.len();
                        if len == 0 {
                            continue;
                        }
                        let base = group.next.fetch_add(1, Ordering::Relaxed);
                        let data_len = preframed.len();
                        let mut delivered = false;
                        for attempt in 0..len {
                            let idx = (base + attempt) % len;
                            let (_, ref h) = group.members[idx];
                            if h.tx.is_closed() {
                                continue;
                            }
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            h.pending.fetch_add(data_len, Ordering::Relaxed);
                            let was_empty = {
                                let mut buf = h.broadcast_buf.lock();
                                let empty = buf.is_empty();
                                buf.extend_from_slice(&preframed);
                                empty
                            };
                            if was_empty {
                                h.broadcast_notify.notify_one();
                            }
                            delivered = true;
                            break;
                        }
                        if !delivered {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::Disconnect { conn_id } => {
                // Control message: send unconditionally
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(Message::Close(None))).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
            }
            ServerCommand::GetConnections { reply } => {
                let guard = state.connections.read().await;
                let _ = reply.send(guard.keys().cloned().collect());
            }
            ServerCommand::Drain {
                close_code,
                close_reason,
                timeout_secs,
            } => {
                // Set draining flag to reject new connections
                state.draining.store(true, Ordering::Relaxed);
                tracing::info!("[WSE] Entering drain mode (lame duck)");

                // Notify cluster peers
                if let Some(tx) = state.cluster_cmd_tx.read().unwrap().as_ref() {
                    let _ = tx.send(ClusterCommand::Drain);
                }

                // Send close frame with custom code to all clients
                let close_frame = Message::Close(Some(CloseFrame {
                    code: CloseCode::from(close_code),
                    reason: close_reason.into(),
                }));
                let guard = state.connections.read().await;
                let n = guard.len();
                for h in guard.values() {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(close_frame.clone())).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
                drop(guard);
                tracing::info!(
                    "[WSE] Drain: sent close to {n} connections, waiting up to {timeout_secs}s"
                );

                // Spawn drain wait as separate task so command processor stays responsive
                let drain_state = state.clone();
                tokio::spawn(async move {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
                    loop {
                        if drain_state.connection_count.load(Ordering::Relaxed) == 0 {
                            tracing::info!("[WSE] Drain complete: all connections closed");
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            let remaining =
                                drain_state.connection_count.load(Ordering::Relaxed);
                            tracing::warn!(
                                "[WSE] Drain timeout: {remaining} connections remaining"
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
            ServerCommand::Shutdown => {
                let close_frame = Message::Close(Some(CloseFrame {
                    code: CloseCode::Away,
                    reason: "server shutting down".into(),
                }));
                let guard = state.connections.read().await;
                for h in guard.values() {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(close_frame.clone())).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
                drop(guard);
                // Drain period: allow in-flight messages to flush before exiting
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PyO3 class
// ---------------------------------------------------------------------------

#[pyclass]
pub struct RustWSEServer {
    host: String,
    port: u16,
    shared: Arc<SharedState>,
    cmd_tx: Option<mpsc::UnboundedSender<ServerCommand>>,
    thread_handle: Option<thread::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    dedup: Arc<std::sync::Mutex<DeduplicationState>>,
    rt_handle: Option<tokio::runtime::Handle>,
    started_at: Option<std::time::Instant>,
}

#[pymethods]
impl RustWSEServer {
    #[new]
    #[pyo3(signature = (host, port, max_connections = 1000, jwt_secret = None, jwt_issuer = None, jwt_audience = None, jwt_cookie_name = None, jwt_previous_secret = None, jwt_key_id = None, jwt_algorithm = None, jwt_private_key = None, max_inbound_queue_size = 131072, recovery_enabled = false, recovery_buffer_size = 128, recovery_ttl = 300, recovery_max_messages = 500, recovery_memory_budget = 268435456, presence_enabled = false, presence_max_data_size = 4096, presence_max_members = 0, rate_limit_capacity = 100_000.0, rate_limit_refill = 10_000.0, max_message_size = 1_048_576, ping_interval = 25, idle_timeout = 60, max_outbound_queue_bytes = 16_777_216))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: String,
        port: u16,
        max_connections: usize,
        jwt_secret: Option<Vec<u8>>,
        jwt_issuer: Option<String>,
        jwt_audience: Option<String>,
        jwt_cookie_name: Option<String>,
        jwt_previous_secret: Option<Vec<u8>>,
        jwt_key_id: Option<String>,
        jwt_algorithm: Option<String>,
        jwt_private_key: Option<Vec<u8>>,
        max_inbound_queue_size: usize,
        recovery_enabled: bool,
        recovery_buffer_size: usize,
        recovery_ttl: u64,
        recovery_max_messages: usize,
        recovery_memory_budget: usize,
        presence_enabled: bool,
        presence_max_data_size: usize,
        presence_max_members: usize,
        rate_limit_capacity: f64,
        rate_limit_refill: f64,
        max_message_size: usize,
        ping_interval: u64,
        idle_timeout: u64,
        max_outbound_queue_bytes: usize,
    ) -> PyResult<Self> {
        if ping_interval >= idle_timeout {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "ping_interval ({ping_interval}s) must be less than idle_timeout ({idle_timeout}s)"
            )));
        }
        let jwt_alg = match jwt_algorithm.as_deref() {
            None | Some("HS256") => jwt::JwtAlgorithm::HS256,
            Some("RS256") => jwt::JwtAlgorithm::RS256,
            Some("ES256") => jwt::JwtAlgorithm::ES256,
            Some(other) => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unsupported jwt_algorithm: {other}. Supported: HS256, RS256, ES256"
                )));
            }
        };
        // Algorithm-specific key validation at startup
        if jwt_secret.is_some() {
            match jwt_alg {
                jwt::JwtAlgorithm::HS256 => {
                    if let Some(ref s) = jwt_secret
                        && s.len() < 32
                    {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "jwt_secret must be at least 32 bytes (RFC 7518 HS256 minimum)",
                        ));
                    }
                    if let Some(ref s) = jwt_previous_secret
                        && s.len() < 32
                    {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "jwt_previous_secret must be at least 32 bytes",
                        ));
                    }
                }
                jwt::JwtAlgorithm::RS256 => {
                    if let Some(ref s) = jwt_secret {
                        jsonwebtoken::DecodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 public key PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt_previous_secret {
                        jsonwebtoken::DecodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 jwt_previous_secret PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt_private_key {
                        jsonwebtoken::EncodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 jwt_private_key PEM: {e}"
                            ))
                        })?;
                    }
                }
                jwt::JwtAlgorithm::ES256 => {
                    if let Some(ref s) = jwt_secret {
                        jsonwebtoken::DecodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 public key PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt_previous_secret {
                        jsonwebtoken::DecodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 jwt_previous_secret PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt_private_key {
                        jsonwebtoken::EncodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 jwt_private_key PEM: {e}"
                            ))
                        })?;
                    }
                }
            }
        }
        // Private key validated above but not stored in JwtConfig -- rust_jwt_encode
        // takes the key directly as an argument. Avoids keeping key material in memory.
        drop(jwt_private_key);
        let jwt_config = jwt_secret.map(|secret| JwtConfig {
            algorithm: jwt_alg,
            secret,
            previous_secret: jwt_previous_secret,
            key_id: jwt_key_id,
            issuer: jwt_issuer.unwrap_or_default(),
            audience: jwt_audience.unwrap_or_default(),
            cookie_name: jwt_cookie_name.unwrap_or_else(|| "access_token".to_string()),
        });
        let recovery = if recovery_enabled {
            // Find the power-of-two exponent for buffer size (minimum 16 slots)
            let clamped = recovery_buffer_size.max(16);
            let bits = if clamped.is_power_of_two() {
                clamped.trailing_zeros()
            } else {
                clamped.next_power_of_two().trailing_zeros()
            };
            Some(Arc::new(super::recovery::RecoveryManager::new(
                super::recovery::RecoveryConfig {
                    buffer_size_bits: bits,
                    history_ttl_secs: recovery_ttl,
                    max_recovery_messages: recovery_max_messages,
                    global_memory_budget: recovery_memory_budget,
                },
            )))
        } else {
            None
        };
        Ok(Self {
            host,
            port,
            shared: Arc::new(SharedState::new(
                max_connections,
                jwt_config,
                max_inbound_queue_size,
                recovery,
                presence_enabled,
                presence_max_data_size,
                presence_max_members,
                rate_limit_capacity,
                rate_limit_refill,
                max_message_size,
                ping_interval,
                idle_timeout,
                max_outbound_queue_bytes,
            )),
            cmd_tx: None,
            thread_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            dedup: Arc::new(std::sync::Mutex::new(DeduplicationState {
                seen: AHashSet::with_capacity(50_000),
                queue: std::collections::VecDeque::with_capacity(50_000),
                max_entries: 50_000,
            })),
            rt_handle: None,
            started_at: None,
        })
    }

    fn set_callbacks(
        &self,
        py: Python<'_>,
        on_connect: Py<PyAny>,
        on_message: Py<PyAny>,
        on_disconnect: Py<PyAny>,
    ) -> PyResult<()> {
        if !on_connect.bind(py).is_callable()
            || !on_message.bind(py).is_callable()
            || !on_disconnect.bind(py).is_callable()
        {
            return Err(PyRuntimeError::new_err("All callbacks must be callable"));
        }
        let shared = self.shared.clone();
        *shared.on_connect.write().unwrap() = Some(on_connect);
        *shared.on_message.write().unwrap() = Some(on_message);
        *shared.on_disconnect.write().unwrap() = Some(on_disconnect);
        Ok(())
    }

    fn start(&mut self) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("Server is already running"));
        }
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("wse_accel=info")),
            )
            .with_target(false)
            .try_init();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ServerCommand>();
        self.cmd_tx = Some(cmd_tx.clone());
        // Store a clone for presence broadcasts from disconnect cleanup paths
        *self.shared.presence_broadcast_tx.write().unwrap() = Some(cmd_tx.clone());
        let host = self.host.clone();
        let port = self.port;
        let running = self.running.clone();
        let shared = self.shared.clone();
        let (rt_handle_tx, rt_handle_rx) =
            std::sync::mpsc::sync_channel::<tokio::runtime::Handle>(1);

        let handle = thread::Builder::new()
            .name("wse-tokio-rt".into())
            .spawn(move || {
                let rt = match Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("[WSE] Failed to create runtime: {e}");
                        return;
                    }
                };
                let _ = rt_handle_tx.send(rt.handle().clone());
                rt.block_on(async move {
                    let bind_addr = format!("{host}:{port}");
                    let listener = match TcpListener::bind(&bind_addr).await {
                        Ok(l) => {
                            tracing::info!("[WSE] Listening on {bind_addr}");
                            l
                        }
                        Err(e) => {
                            tracing::error!("[WSE] Failed to bind {bind_addr}: {e}");
                            return;
                        }
                    };
                    running.store(true, Ordering::SeqCst);
                    let cmd_handle = tokio::spawn(process_commands(cmd_rx, shared.clone()));
                    // Spawn periodic recovery cleanup if recovery is enabled
                    if let Some(ref recovery) = shared.recovery {
                        let rm = recovery.clone();
                        tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(Duration::from_secs(30));
                            loop {
                                interval.tick().await;
                                rm.cleanup();
                            }
                        });
                    }
                    // Presence TTL sweep (every 30 seconds)
                    if let Some(ref pm) = shared.presence {
                        let sweep_pm = pm.clone();
                        let sweep_state = shared.clone();
                        let sweep_running = running.clone();
                        tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(Duration::from_secs(30));
                            loop {
                                interval.tick().await;
                                if !sweep_running.load(Ordering::Relaxed) {
                                    break;
                                }
                                let removed =
                                    sweep_pm.sweep_dead_connections(|conn_id| {
                                        if conn_id.starts_with("__remote__") {
                                            return true;
                                        }
                                        sweep_state
                                            .conn_last_activity
                                            .contains_key(conn_id)
                                    });
                                if removed.is_empty() {
                                    continue;
                                }
                                let drain = sweep_state
                                    .drain_mode
                                    .load(Ordering::Relaxed);
                                let broadcast_tx = sweep_state
                                    .presence_broadcast_tx
                                    .read()
                                    .unwrap()
                                    .clone();
                                let cluster_tx = sweep_state
                                    .cluster_cmd_tx
                                    .read()
                                    .unwrap()
                                    .clone();
                                for (topic, user_id, data) in removed {
                                    if drain {
                                        sweep_state.push_inbound(
                                            InboundEvent::PresenceLeave {
                                                topic: topic.clone(),
                                                user_id: user_id.clone(),
                                                data: data.clone(),
                                            },
                                        );
                                    }
                                    let msg = format_presence_msg(
                                        "presence_leave",
                                        &user_id,
                                        &data,
                                    );
                                    if let Some(ref tx) = broadcast_tx {
                                        let _ = tx.send(
                                            ServerCommand::BroadcastLocal {
                                                topic: topic.clone(),
                                                data: msg,
                                                skip_recovery: true,
                                            },
                                        );
                                    }
                                    if let Some(ref ctx) = cluster_tx {
                                        let data_str = serde_json::to_string(
                                            &data,
                                        )
                                        .unwrap_or_default();
                                        let now =
                                            super::presence::epoch_ms();
                                        let _ = ctx.send(
                                            ClusterCommand::PresenceUpdate {
                                                topic,
                                                user_id,
                                                action: 1,
                                                data: data_str,
                                                updated_at: now,
                                            },
                                        );
                                    }
                                }
                            }
                        });
                    }
                    // Server-initiated ping + zombie detection + slow consumer detection
                    {
                        let ping_state = shared.clone();
                        tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(Duration::from_secs(ping_state.ping_interval_secs));
                            let idle_timeout = Duration::from_secs(ping_state.idle_timeout_secs);
                            let max_pending = ping_state.max_outbound_queue_bytes;
                            // Two-phase slow consumer detection: connections suspected
                            // of being slow (queue full on first check). If still full
                            // on the next tick, they get disconnected.
                            let mut suspected_slow: AHashSet<String> = AHashSet::new();
                            loop {
                                interval.tick().await;
                                let now = std::time::Instant::now();
                                // Collect senders + pending counters under read lock
                                let snapshot: Vec<(
                                    String,
                                    mpsc::UnboundedSender<WsFrame>,
                                    Arc<AtomicUsize>,
                                )> = {
                                    let conns =
                                        ping_state.connections.read().await;
                                    conns
                                        .iter()
                                        .map(|(cid, h)| {
                                            (cid.clone(), h.tx.clone(), h.pending.clone())
                                        })
                                        .collect()
                                };
                                // Trim stale entries from suspected_slow (disconnected connections)
                                if !suspected_slow.is_empty() {
                                    let snapshot_ids: ahash::AHashSet<&str> = snapshot.iter().map(|(cid, _, _)| cid.as_str()).collect();
                                    suspected_slow.retain(|id| snapshot_ids.contains(id.as_str()));
                                }
                                let mut force_remove = Vec::new();
                                for (cid, tx, pending) in &snapshot {
                                    // Slow consumer detection via pending counter
                                    if max_pending > 0
                                        && pending.load(Ordering::Relaxed) >= max_pending
                                    {
                                        if suspected_slow.contains(cid) {
                                            // Second tick still full: disconnect
                                            pending.fetch_add(8, Ordering::Relaxed);
                                            if tx.send(WsFrame::Msg(
                                                Message::Close(None),
                                            )).is_err() {
                                                pending.fetch_sub(8, Ordering::Relaxed);
                                            }
                                            ping_state
                                                .conn_last_activity
                                                .remove(cid);
                                            suspected_slow.remove(cid);
                                            continue;
                                        } else {
                                            // First tick: suspect -- skip ping for this connection
                                            suspected_slow.insert(cid.clone());
                                            continue;
                                        }
                                    } else {
                                        // Queue not full: clear suspicion
                                        suspected_slow.remove(cid);
                                    }

                                    if let Some(last) =
                                        ping_state.conn_last_activity.get(cid)
                                    {
                                        // Check zombie (no activity for idle_timeout)
                                        if now.duration_since(*last) > idle_timeout
                                        {
                                            pending.fetch_add(8, Ordering::Relaxed);
                                            if tx.send(WsFrame::Msg(
                                                Message::Close(None),
                                            )).is_err() {
                                                pending.fetch_sub(8, Ordering::Relaxed);
                                            }
                                            // Remove activity entry; next tick
                                            // will force-remove if still present
                                            ping_state
                                                .conn_last_activity
                                                .remove(cid);
                                            continue;
                                        }
                                        // Send server ping
                                        let ts =
                                            chrono::Utc::now().to_rfc3339_opts(
                                                chrono::SecondsFormat::Millis,
                                                true,
                                            );
                                        let ping_msg = format!(
                                            "{{\"c\":\"WSE\",\"t\":\"ping\",\"p\":{{\"server_time\":\"{ts}\"}}}}"
                                        );
                                        let pm_bytes = ping_msg.len() + 4;
                                        pending.fetch_add(pm_bytes, Ordering::Relaxed);
                                        if tx.send(WsFrame::Msg(
                                            Message::Text(ping_msg.into()),
                                        )).is_err() {
                                            pending.fetch_sub(pm_bytes, Ordering::Relaxed);
                                        }
                                    } else {
                                        // No activity entry = Close was sent last
                                        // tick but connection didn't terminate.
                                        // Force-remove this zombie.
                                        force_remove.push(cid.clone());
                                    }
                                }
                                // Force-remove zombies that didn't respond to Close
                                if !force_remove.is_empty() {
                                    let drain = ping_state.drain_mode.load(Ordering::Relaxed);
                                    // Presence cleanup for zombies
                                    if let Some(ref pm) = ping_state.presence {
                                        let broadcast_tx = ping_state.presence_broadcast_tx.read().unwrap().clone();
                                        let cluster_tx = ping_state.cluster_cmd_tx.read().unwrap().clone();
                                        for cid in &force_remove {
                                            let results = pm.remove_connection(cid);
                                            for (topic, result) in results {
                                                if let super::presence::UntrackResult::LastLeave { user_id, data } = result {
                                                    if drain {
                                                        ping_state.push_inbound(InboundEvent::PresenceLeave {
                                                            topic: topic.clone(),
                                                            user_id: user_id.clone(),
                                                            data: data.clone(),
                                                        });
                                                    }
                                                    if let Some(ref tx) = broadcast_tx {
                                                        let msg = format_presence_msg("presence_leave", &user_id, &data);
                                                        let _ = tx.send(ServerCommand::BroadcastLocal {
                                                            topic: topic.clone(),
                                                            data: msg,
                                                            skip_recovery: true,
                                                        });
                                                    }
                                                    if let Some(ref ctx) = cluster_tx {
                                                        let data_str = serde_json::to_string(&data).unwrap_or_default();
                                                        let now = super::presence::epoch_ms();
                                                        let _ = ctx.send(ClusterCommand::PresenceUpdate {
                                                            topic,
                                                            user_id,
                                                            action: 1,
                                                            data: data_str,
                                                            updated_at: now,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    // Remove connections FIRST (mirrors normal disconnect order).
                                    // Track which cids were actually present -- handle_connection
                                    // may have already cleaned up if the connection closed naturally.
                                    let mut was_present: ahash::AHashSet<String> = ahash::AHashSet::new();
                                    {
                                        let mut conns =
                                            ping_state.connections.write().await;
                                        for cid in &force_remove {
                                            if conns.remove(cid).is_some() {
                                                was_present.insert(cid.clone());
                                            }
                                            ping_state.conn_formats.remove(cid);
                                            ping_state.conn_rates.remove(cid);
                                            ping_state.conn_last_activity.remove(cid);
                                            ping_state.conn_encryption.remove(cid);
                                            ping_state.conn_topic_acl.remove(cid);
                                            // Queue group cleanup for zombies
                                            if let Some((_, memberships)) = ping_state.conn_queue_groups.remove(cid) {
                                                for (topic, group_name) in memberships {
                                                    if let Some(tg) = ping_state.queue_groups.get(&topic) {
                                                        if let Some(mut g) = tg.get_mut(&group_name) {
                                                            g.members.retain(|(id, _)| id != cid);
                                                        }
                                                        tg.remove_if(&group_name, |_, g| g.members.is_empty());
                                                    }
                                                    ping_state.queue_groups.remove_if(&topic, |_, groups| groups.is_empty());
                                                }
                                            }
                                        }
                                        ping_state.connection_count.store(
                                            conns.len(),
                                            Ordering::Relaxed,
                                        );
                                    }
                                    // Topic subscription cleanup for zombies (only if we actually removed them)
                                    let cluster_tx = ping_state.cluster_cmd_tx.read().unwrap().clone();
                                    for cid in &force_remove {
                                        if !was_present.contains(cid) {
                                            continue; // handle_connection already cleaned up
                                        }
                                        if let Some((_, topics)) = ping_state.conn_topics.remove(cid) {
                                            let mut disconnected_topics = Vec::new();
                                            for topic_ref in topics.iter() {
                                                let topic = topic_ref.clone();
                                                if let Some(subscribers) = ping_state.topic_subscribers.get(&topic) {
                                                    subscribers.remove(cid);
                                                }
                                                if ping_state.topic_subscribers.remove_if(&topic, |_, subs| subs.is_empty()).is_some()
                                                    && (topic.contains('*') || topic.contains('?'))
                                                {
                                                    ping_state.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
                                                }
                                                disconnected_topics.push(topic);
                                            }
                                            if let Some(ref tx) = cluster_tx {
                                                let mut refcounts = ping_state
                                                    .local_topic_refcount
                                                    .lock()
                                                    .unwrap_or_else(|e| e.into_inner());
                                                for topic in &disconnected_topics {
                                                    if let Some(count) = refcounts.get_mut(topic) {
                                                        *count = count.saturating_sub(1);
                                                        if *count == 0 {
                                                            refcounts.remove(topic);
                                                            let _ = tx.send(ClusterCommand::Unsub {
                                                                topic: topic.clone(),
                                                            });
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        // Emit disconnect event AFTER cleanup (mirrors normal path)
                                        if drain {
                                            ping_state.push_inbound(InboundEvent::Disconnect {
                                                conn_id: cid.clone(),
                                            });
                                        } else {
                                            let cb_guard = ping_state.on_disconnect.read().unwrap();
                                            if let Some(ref cb) = *cb_guard
                                                && let Some(cb_clone) = Python::try_attach(|py| cb.clone_ref(py))
                                            {
                                                let cid_owned = cid.clone();
                                                drop(cb_guard);
                                                tokio::task::spawn_blocking(move || {
                                                    fire_on_disconnect(&cb_clone, &cid_owned);
                                                });
                                            }
                                        }
                                    }
                                    tracing::warn!(
                                        "[WSE] Force-removed {} zombie connections",
                                        force_remove.len()
                                    );
                                }
                            }
                        });
                    }
                    loop {
                        tokio::select! {
                            res = listener.accept() => {
                                match res {
                                    Ok((stream, addr)) => {
                                        let _ = stream.set_nodelay(true);
                                        // Larger send buffer for broadcast throughput (default 16KB -> 256KB)
                                        let sock = socket2::SockRef::from(&stream);
                                        let _ = sock.set_send_buffer_size(262_144);
                                        let shared2 = shared.clone();
                                        tokio::spawn(async move {
                                            // Protocol detection: peek first 4 bytes to distinguish
                                            // cluster binary frames (4-byte big-endian length prefix:
                                            // 0x00 0x00 0x00 XX for frames < 256 bytes) from HTTP/WS
                                            // (first byte is ASCII letter like 'G' for GET) or TLS
                                            // (first byte 0x16 for ClientHello).
                                            let mut peek = [0u8; 4];
                                            let is_cluster = match tokio::time::timeout(
                                                Duration::from_secs(5),
                                                stream.peek(&mut peek),
                                            ).await {
                                                Ok(Ok(n)) if n >= 4 => {
                                                    // Cluster frames start with 4-byte big-endian length prefix.
                                                    // Valid HELLO is 20-200 bytes, so first 3 bytes are 0x00.
                                                    // HTTP starts with ASCII letter (GET, POST, etc).
                                                    // TLS ClientHello starts with 0x16.
                                                    peek[0] == 0x00 && peek[1] == 0x00 && peek[2] == 0x00 && peek[3] > 0
                                                }
                                                _ => false,
                                            };
                                            if is_cluster {
                                                // Reject cluster connections on main port when mTLS is
                                                // configured or cluster is not yet initialized.
                                                let cluster_ready = shared2
                                                    .cluster_interest_tx
                                                    .read()
                                                    .unwrap()
                                                    .is_some();
                                                if shared2.cluster_tls_enabled.load(Ordering::Relaxed)
                                                    || !cluster_ready
                                                {
                                                    if shared2.cluster_tls_enabled.load(Ordering::Relaxed) {
                                                        tracing::warn!(
                                                            "[WSE] Rejecting cluster connection on main port \
                                                             (mTLS required, use dedicated cluster_port): {addr}"
                                                        );
                                                    }
                                                    drop(stream);
                                                    return;
                                                }
                                                let interest_tx = shared2
                                                    .cluster_interest_tx
                                                    .read()
                                                    .unwrap()
                                                    .clone();
                                                if let Some(interest_tx) = interest_tx {
                                                    super::cluster::handle_cluster_inbound(
                                                        stream, addr, &shared2, interest_tx,
                                                    )
                                                    .await;
                                                } else {
                                                    tracing::warn!(
                                                        "[WSE-Cluster] No interest channel for inbound {addr}"
                                                    );
                                                }
                                            } else {
                                                handle_connection(stream, addr, shared2).await;
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        if !running.load(Ordering::SeqCst) { break; }
                                        tracing::error!("[WSE] Accept error: {e}");
                                    }
                                }
                            }
                            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                                if !running.load(Ordering::SeqCst) { break; }
                            }
                        }
                    }
                    let _ = cmd_handle.await;
                    running.store(false, Ordering::SeqCst);
                    tracing::info!("[WSE] Shut down");
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Spawn error: {e}")))?;
        self.thread_handle = Some(handle);
        self.rt_handle = rt_handle_rx.recv().ok();
        self.started_at = Some(std::time::Instant::now());
        Ok(())
    }

    /// Enter graceful drain mode (lame duck).
    ///
    /// 1. Stops accepting new connections
    /// 2. Notifies cluster peers via DRAIN frame
    /// 3. Sends Close frame with custom code to all clients
    /// 4. Waits up to `timeout` seconds for clients to disconnect
    ///
    /// Call `stop()` after `drain()` to fully shut down.
    #[pyo3(signature = (close_code = 4300, close_reason = "", timeout = 30))]
    fn drain(&self, close_code: u16, close_reason: &str, timeout: u64) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::Drain {
            close_code,
            close_reason: close_reason.to_owned(),
            timeout_secs: timeout,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }
        // Shutdown cluster
        if let Ok(guard) = self.shared.cluster_cmd_tx.read()
            && let Some(ref tx) = *guard
        {
            let _ = tx.send(ClusterCommand::Shutdown);
        }
        if let Some(ref tx) = self.cmd_tx {
            let _ = tx.send(ServerCommand::Shutdown);
        }
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.thread_handle.take() {
            let panicked = py.detach(|| handle.join().is_err());
            if panicked {
                return Err(PyRuntimeError::new_err("Server thread panicked"));
            }
        }
        self.cmd_tx = None;
        *self.shared.cluster_cmd_tx.write().unwrap() = None;
        Ok(())
    }

    // -- send methods -------------------------------------------------------

    fn send(&self, conn_id: &str, data: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::SendText {
            conn_id: conn_id.to_owned(),
            data: data.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    fn send_bytes(&self, conn_id: &str, data: &[u8]) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::SendBytes {
            conn_id: conn_id.to_owned(),
            data: data.to_vec(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Full outbound pipeline in Rust: PyDict -> envelope -> JSON -> compress -> send.
    /// Returns byte count sent.
    #[pyo3(signature = (conn_id, event, compression_threshold = 1024))]
    fn send_event(
        &self,
        _py: Python<'_>,
        conn_id: &str,
        event: &Bound<'_, PyDict>,
        compression_threshold: usize,
    ) -> PyResult<usize> {
        let cmd_tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;

        // #7: Dedup — skip if this event ID was already sent
        if let Ok(Some(id_val)) = event.get_item("id")
            && let Ok(id_str) = id_val.extract::<String>()
        {
            let mut dedup = self.dedup.lock().unwrap();
            if !dedup.seen.insert(id_str.clone()) {
                return Ok(0); // duplicate, skip serialize+send
            }
            dedup.queue.push_back(id_str);
            while dedup.queue.len() > dedup.max_entries {
                if let Some(old) = dedup.queue.pop_front() {
                    dedup.seen.remove(&old);
                }
            }
        }

        // #8: Per-connection rate limiter (token bucket) with client feedback
        {
            let mut entry = self
                .shared
                .conn_rates
                .entry(conn_id.to_owned())
                .or_insert_with(|| PerConnRate {
                    tokens: self.shared.rate_capacity,
                    last_refill: std::time::Instant::now(),
                    last_rate_limited: None,
                    last_warning: None,
                });
            let rate = entry.value_mut();
            let elapsed = rate.last_refill.elapsed().as_secs_f64();
            rate.tokens =
                (rate.tokens + elapsed * self.shared.rate_refill).min(self.shared.rate_capacity);
            rate.last_refill = std::time::Instant::now();

            if rate.tokens < 1.0 {
                self.shared
                    .rate_limited_total
                    .fetch_add(1, Ordering::Relaxed);
                // Send rate_limited error to client (throttled: max once per second)
                let should_notify = rate
                    .last_rate_limited
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_notify {
                    let err = "{\"c\":\"WSE\",\"t\":\"error\",\"p\":{\"code\":\"RATE_LIMITED\",\"message\":\"Rate limit exceeded\",\"retry_after\":1.0},\"v\":1}".to_string();
                    cmd_tx
                        .send(ServerCommand::SendText {
                            conn_id: conn_id.to_owned(),
                            data: err,
                        })
                        .ok();
                    rate.last_rate_limited = Some(std::time::Instant::now());
                }
                return Ok(0);
            }

            // Warning at 20% remaining capacity (throttled: max once per second)
            let warning_threshold = self.shared.rate_capacity * 0.2;
            if rate.tokens < warning_threshold {
                let should_warn = rate
                    .last_warning
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_warn {
                    let warning = format!(
                        "{{\"c\":\"WSE\",\"t\":\"rate_limit_warning\",\"p\":{{\"current_rate\":{},\"limit\":{},\"remaining\":{},\"retry_after\":1.0}},\"v\":1}}",
                        (self.shared.rate_capacity - rate.tokens) as u64,
                        self.shared.rate_capacity as u64,
                        rate.tokens as u64,
                    );
                    cmd_tx
                        .send(ServerCommand::SendText {
                            conn_id: conn_id.to_owned(),
                            data: warning,
                        })
                        .ok();
                    rate.last_warning = Some(std::time::Instant::now());
                }
            }

            rate.tokens -= 1.0;
        }

        // Build serde_json map from PyDict: c first, t second, v last
        let mut map = serde_json::Map::new();
        let mut msg_type = String::new();

        // Extract 't' value (need it to determine category)
        if let Ok(Some(t_val)) = event.get_item("t")
            && let Ok(s) = t_val.extract::<String>()
        {
            msg_type = s;
        }

        // c first (category)
        let cat = message_category(&msg_type);
        map.insert("c".to_string(), serde_json::Value::String(cat.to_string()));

        // t second
        if !msg_type.is_empty() {
            map.insert("t".to_string(), serde_json::Value::String(msg_type.clone()));
        }

        // All other keys (skip t, v, c)
        for (k, v) in event.iter() {
            let key = match k.extract::<String>() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if key == "t" || key == "v" || key == "c" {
                continue;
            }
            map.insert(key, pyobj_to_json(&v));
        }

        // Envelope: id, seq, ts
        if !map.contains_key("id") {
            map.insert(
                "id".to_string(),
                serde_json::Value::String(Uuid::now_v7().to_string()),
            );
        }

        static GLOBAL_SEQ: AtomicU64 = AtomicU64::new(0);
        if !map.contains_key("seq") {
            map.insert(
                "seq".to_string(),
                serde_json::Value::Number(GLOBAL_SEQ.fetch_add(1, Ordering::Relaxed).into()),
            );
        }

        if !map.contains_key("ts") {
            let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            map.insert("ts".to_string(), serde_json::Value::String(ts));
        }

        // v goes last
        map.insert("v".to_string(), serde_json::Value::Number(1.into()));

        // Check if this connection wants msgpack (lock-free DashMap read)
        let use_msgpack = self
            .shared
            .conn_formats
            .get(conn_id)
            .map(|v| *v)
            .unwrap_or(false);

        // Check if this connection has E2E encryption
        let conn_cipher = self.shared.conn_encryption.get(conn_id);

        let byte_count;

        if use_msgpack {
            // Msgpack path: M: prefix + rmpv serialize (binary frame)
            let rmpv_val = serde_json_to_rmpv(&serde_json::Value::Object(map));
            let mut buf = Vec::new();
            rmpv::encode::write_value(&mut buf, &rmpv_val)
                .map_err(|e| PyRuntimeError::new_err(format!("Msgpack error: {e}")))?;
            let mut final_bytes = Vec::with_capacity(buf.len() + 2);
            final_bytes.extend_from_slice(b"M:");
            final_bytes.extend_from_slice(&buf);

            // Encrypt if E2E enabled for this connection
            let final_bytes = if let Some(ref cipher) = conn_cipher {
                let encrypted = encrypt_outbound(cipher, &final_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Encrypt error: {e}")))?;
                let mut enc_buf = Vec::with_capacity(2 + encrypted.len());
                enc_buf.extend_from_slice(b"E:");
                enc_buf.extend_from_slice(&encrypted);
                enc_buf
            } else {
                final_bytes
            };

            byte_count = final_bytes.len();
            cmd_tx
                .send(ServerCommand::SendPrebuilt {
                    conn_id: conn_id.to_owned(),
                    message: Message::Binary(final_bytes.into()),
                })
                .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        } else {
            // JSON path: serialize (no wire prefix, c is inside JSON)
            let json_str = serde_json::to_string(&serde_json::Value::Object(map))
                .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {e}")))?;

            let payload_bytes = json_str.as_bytes();

            if let Some(ref cipher) = conn_cipher {
                // E2E encrypted: encrypt plaintext payload, send as E: prefix binary
                let encrypted = encrypt_outbound(cipher, payload_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Encrypt error: {e}")))?;
                let mut final_bytes = Vec::with_capacity(2 + encrypted.len());
                final_bytes.extend_from_slice(b"E:");
                final_bytes.extend_from_slice(&encrypted);
                byte_count = final_bytes.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(final_bytes.into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            } else if compression_threshold > 0 && payload_bytes.len() > compression_threshold {
                // Compressed: C: prefix + zlib (binary frame)
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
                encoder
                    .write_all(payload_bytes)
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress error: {e}")))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| PyRuntimeError::new_err(format!("Compress finish: {e}")))?;
                let mut final_bytes = Vec::with_capacity(compressed.len() + 2);
                final_bytes.extend_from_slice(b"C:");
                final_bytes.extend_from_slice(&compressed);
                byte_count = final_bytes.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(final_bytes.into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            } else {
                // Uncompressed JSON: binary frame (skips UTF-8 validation overhead)
                byte_count = json_str.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(json_str.into_bytes().into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            }
        }

        self.shared
            .messages_sent_total
            .fetch_add(1, Ordering::Relaxed);
        self.shared
            .bytes_sent_total
            .fetch_add(byte_count as u64, Ordering::Relaxed);
        Ok(byte_count)
    }

    fn broadcast_all(&self, data: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        let data = inject_category(data);
        tx.send(ServerCommand::BroadcastText { data })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    fn broadcast_all_bytes(&self, data: &[u8]) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::BroadcastBytes {
            data: data.to_vec(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    // -- query / management -------------------------------------------------

    fn get_connection_count(&self) -> usize {
        self.shared.connection_count.load(Ordering::Relaxed)
    }

    fn get_connections(&self) -> PyResult<Vec<String>> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(ServerCommand::GetConnections { reply: reply_tx })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        reply_rx
            .blocking_recv()
            .map_err(|_| PyRuntimeError::new_err("Reply dropped"))
    }

    fn disconnect(&self, conn_id: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::Disconnect {
            conn_id: conn_id.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    // -- drain mode -----------------------------------------------------------

    /// Enable drain mode. Inbound events are queued instead of calling
    /// on_message/on_connect/on_disconnect callbacks. Call drain_inbound()
    /// from a Python thread to retrieve batches.
    fn enable_drain_mode(&self) {
        self.shared.drain_mode.store(true, Ordering::SeqCst);
    }

    /// Drain up to max_count inbound events. Blocks up to timeout_ms waiting
    /// for at least one event. Returns list of (event_type, conn_id, data) tuples.
    ///
    /// event_type: "connect" | "auth_connect" | "msg" | "raw" | "bin" | "disconnect"
    /// data: str (for "connect"=cookies, "auth_connect"=user_id, "raw"),
    ///       dict (for "msg"), bytes (for "bin"), None (for "disconnect")
    ///
    /// GIL is released while waiting on channel, acquired once for batch conversion.
    #[pyo3(signature = (max_count = 256, timeout_ms = 50))]
    fn drain_inbound(
        &self,
        py: Python<'_>,
        max_count: usize,
        timeout_ms: u64,
    ) -> PyResult<Py<PyList>> {
        // Collect events with GIL released (channel recv happens without GIL)
        let events: Vec<InboundEvent> = py.detach(|| {
            let mut events = Vec::with_capacity(max_count.min(4096));

            // First event: block with timeout (efficient — no busy wait)
            match self
                .shared
                .inbound_rx
                .recv_timeout(Duration::from_millis(timeout_ms))
            {
                Ok(event) => events.push(event),
                Err(_) => return events, // Timeout or disconnected
            }

            // Drain remaining without blocking (batch read)
            while events.len() < max_count {
                match self.shared.inbound_rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(_) => break,
                }
            }

            events
        });

        // Convert all events to Python in one GIL hold
        let list = PyList::empty(py);
        for event in &events {
            match event {
                InboundEvent::Connect { conn_id, cookies } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "connect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            cookies.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::AuthConnect { conn_id, user_id } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "auth_connect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            user_id.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Message { conn_id, value } => {
                    let py_data = json_to_pyobj(py, value);
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "msg".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py_data.clone_ref(py).into_bound(py).into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::RawText { conn_id, text } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "raw".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            text.as_str().into_pyobject(py).unwrap().into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Binary { conn_id, data } => {
                    let py_bytes = PyBytes::new(py, data);
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "bin".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py_bytes.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::Disconnect { conn_id } => {
                    let tuple = PyTuple::new(
                        py,
                        &[
                            "disconnect".into_pyobject(py).unwrap().into_any(),
                            conn_id.as_str().into_pyobject(py).unwrap().into_any(),
                            py.None().into_bound(py).into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::PresenceJoin {
                    topic,
                    user_id,
                    data,
                } => {
                    let payload = PyDict::new(py);
                    payload.set_item("topic", topic.as_str())?;
                    payload.set_item("user_id", user_id.as_str())?;
                    payload.set_item("data", json_to_pyobj(py, data))?;
                    let tuple = PyTuple::new(
                        py,
                        &[
                            PyString::new(py, "presence_join").into_any(),
                            py.None().into_bound(py).into_any(),
                            payload.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
                InboundEvent::PresenceLeave {
                    topic,
                    user_id,
                    data,
                } => {
                    let payload = PyDict::new(py);
                    payload.set_item("topic", topic.as_str())?;
                    payload.set_item("user_id", user_id.as_str())?;
                    payload.set_item("data", json_to_pyobj(py, data))?;
                    let tuple = PyTuple::new(
                        py,
                        &[
                            PyString::new(py, "presence_leave").into_any(),
                            py.None().into_bound(py).into_any(),
                            payload.into_any(),
                        ],
                    )?;
                    list.append(tuple)?;
                }
            }
        }
        Ok(list.unbind())
    }

    fn inbound_dropped_count(&self) -> u64 {
        self.shared.inbound_dropped.load(Ordering::Relaxed)
    }

    fn inbound_queue_depth(&self) -> usize {
        self.shared.inbound_rx.len()
    }

    // -- Cluster Protocol -----------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (peers, tls_cert=None, tls_key=None, tls_ca=None, cluster_port=None, seeds=None, cluster_addr=None))]
    fn connect_cluster(
        &self,
        peers: Vec<String>,
        tls_cert: Option<String>,
        tls_key: Option<String>,
        tls_ca: Option<String>,
        cluster_port: Option<u16>,
        seeds: Option<Vec<String>>,
        cluster_addr: Option<String>,
    ) -> PyResult<()> {
        if self.shared.cluster_cmd_tx.read().unwrap().is_some() {
            return Err(PyRuntimeError::new_err("Cluster already connected"));
        }

        // Validate: seeds requires cluster_addr so other nodes can reach us
        if seeds.is_some() && cluster_addr.is_none() {
            return Err(PyRuntimeError::new_err(
                "seeds= requires cluster_addr= so peers can connect back to this node",
            ));
        }

        // Resolve TLS config: explicit params > env vars > None (plaintext)
        let tls_config = match (tls_cert, tls_key, tls_ca) {
            (Some(cert), Some(key), Some(ca)) => Some(
                super::cluster::build_cluster_tls(&cert, &key, &ca)
                    .map_err(|e| PyRuntimeError::new_err(format!("TLS config error: {e}")))?,
            ),
            (None, None, None) => {
                match (
                    std::env::var("WSE_CLUSTER_TLS_CERT").ok(),
                    std::env::var("WSE_CLUSTER_TLS_KEY").ok(),
                    std::env::var("WSE_CLUSTER_TLS_CA").ok(),
                ) {
                    (Some(cert), Some(key), Some(ca)) => Some(
                        super::cluster::build_cluster_tls(&cert, &key, &ca).map_err(|e| {
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

        // Track whether cluster TLS is enabled so main port can reject
        // unauthenticated cluster connections
        self.shared
            .cluster_tls_enabled
            .store(tls_config.is_some(), Ordering::Relaxed);

        // Wrap TLS config in ArcSwap for hot-reload support. The holder is
        // stored in SharedState so reload_cluster_tls() can swap it later.
        let tls_holder = tls_config.map(|cfg| Arc::new(arc_swap::ArcSwap::from_pointee(cfg)));
        *self.shared.cluster_tls_holder.write().unwrap() = tls_holder.clone();

        let rt_handle = self
            .rt_handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
        *self.shared.cluster_cmd_tx.write().unwrap() = Some(cmd_tx);

        let (interest_tx, interest_rx) =
            mpsc::unbounded_channel::<super::cluster::InterestUpdate>();
        *self.shared.cluster_interest_tx.write().unwrap() = Some(interest_tx.clone());

        let instance_id = uuid::Uuid::now_v7().to_string();
        *self.shared.cluster_instance_id.lock().unwrap() = Some(instance_id.clone());
        let connections = self.shared.connections.clone();
        let topic_subscribers = self.shared.topic_subscribers.clone();
        let metrics = self.shared.cluster_metrics.clone();
        let dlq = self.shared.cluster_dlq.clone();
        let local_topic_refcount = self.shared.local_topic_refcount.clone();

        // seeds= mode: use seeds as initial peers, enable discovery
        // peers= mode: use static peer list, no discovery
        let effective_peers = if let Some(ref s) = seeds {
            s.clone()
        } else {
            peers
        };

        rt_handle.spawn(super::cluster::cluster_manager(
            effective_peers,
            instance_id,
            cmd_rx,
            interest_tx,
            interest_rx,
            connections,
            topic_subscribers,
            self.shared.glob_topic_count.clone(),
            metrics,
            dlq,
            local_topic_refcount,
            tls_holder,
            cluster_port,
            cluster_addr,
            self.shared.presence.clone(),
            self.cmd_tx.clone(),
            self.shared.max_outbound_queue_bytes,
            Arc::clone(&self.shared.slow_consumer_drops),
            self.shared.recovery.clone(),
            self.shared.topic_message_counts.clone(),
            self.shared.queue_groups.clone(),
        ));

        Ok(())
    }

    fn cluster_connected(&self) -> bool {
        self.shared
            .cluster_metrics
            .connected_peers
            .load(Ordering::Relaxed)
            > 0
    }

    fn cluster_peers_count(&self) -> u64 {
        self.shared
            .cluster_metrics
            .connected_peers
            .load(Ordering::Relaxed)
    }

    /// Hot-reload cluster TLS certificates without restarting the server.
    /// New connections will use the updated certs; in-flight handshakes
    /// complete with the previous config (TlsAcceptor/TlsConnector use Arc internally).
    #[pyo3(signature = (cert_path, key_path, ca_path))]
    fn reload_cluster_tls(
        &self,
        cert_path: String,
        key_path: String,
        ca_path: String,
    ) -> PyResult<()> {
        let guard = self.shared.cluster_tls_holder.read().unwrap();
        let holder = guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("Cluster TLS not configured")
        })?;
        let new_config = super::cluster::build_cluster_tls(&cert_path, &key_path, &ca_path)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("TLS reload failed: {e}"))
            })?;
        holder.store(std::sync::Arc::new(new_config));
        tracing::info!("[WSE] Cluster TLS certificates reloaded");
        Ok(())
    }

    fn cluster_info(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let list = PyList::empty(py);
        for entry in self.shared.cluster_metrics.peer_info.iter() {
            let d = PyDict::new(py);
            d.set_item("address", &entry.value().address)?;
            d.set_item("instance_id", &entry.value().instance_id)?;
            d.set_item("capabilities", entry.value().capabilities)?;
            d.set_item("connected_at", entry.value().connected_at)?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }

    // -- Topic authorization --------------------------------------------------

    /// Set topic ACL for a connection programmatically (for non-JWT auth).
    ///
    /// - allow: list of glob patterns (empty = allow all)
    /// - deny: list of glob patterns (deny takes precedence over allow)
    #[pyo3(signature = (conn_id, allow = None, deny = None))]
    fn set_topic_acl(&self, conn_id: &str, allow: Option<Vec<String>>, deny: Option<Vec<String>>) {
        let acl = crate::jwt::TopicAcl {
            allow: allow.unwrap_or_default(),
            deny: deny.unwrap_or_default(),
        };
        self.shared.conn_topic_acl.insert(conn_id.to_owned(), acl);
    }

    // -- Topic subscriptions --------------------------------------------------

    #[pyo3(signature = (conn_id, topics, presence_data = None, queue_group = None))]
    fn subscribe_connection(
        &self,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<()> {
        // Filter topics through ACL if one exists for this connection.
        // No ACL = allow all (backward compatible).
        let topics = if let Some(acl) = self.shared.conn_topic_acl.get(conn_id) {
            topics
                .into_iter()
                .filter(|t| acl.is_allowed(t))
                .collect::<Vec<_>>()
        } else {
            topics
        };

        let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();

        // Get ConnectionHandle FIRST (may block_on tokio), before acquiring any mutexes.
        // This prevents deadlock: tokio tasks acquire connections.write() then local_topic_refcount,
        // so we must follow the same order.
        let handle = self.rt_handle.as_ref().and_then(|rt| {
            let guard = rt.block_on(self.shared.connections.read());
            guard.get(conn_id).cloned()
        });

        // Acquire refcount lock AFTER block_on completes
        let mut refcounts = if cluster_tx.is_some() {
            Some(
                self.shared
                    .local_topic_refcount
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            )
        } else {
            None
        };

        for topic in &topics {
            if let Some(ref h) = handle {
                if let Some(ref qg_name) = queue_group {
                    // Queue group mode: add to queue_groups, NOT to topic_subscribers.
                    // Messages to this topic will round-robin to one member per group.
                    let topic_groups = self.shared.queue_groups.entry(topic.clone()).or_default();
                    let mut group =
                        topic_groups
                            .entry(qg_name.clone())
                            .or_insert_with(|| QueueGroup {
                                members: Vec::new(),
                                next: AtomicUsize::new(0),
                            });
                    // Avoid duplicate membership
                    if !group.members.iter().any(|(id, _)| id == conn_id) {
                        group.members.push((conn_id.to_owned(), h.clone()));
                    }
                    drop(group);
                    drop(topic_groups);
                    // Track for disconnect cleanup (avoid duplicates from repeated subscribe calls)
                    {
                        let mut memberships = self
                            .shared
                            .conn_queue_groups
                            .entry(conn_id.to_owned())
                            .or_default();
                        let pair = (topic.clone(), qg_name.clone());
                        if !memberships.contains(&pair) {
                            memberships.push(pair);
                        }
                    }
                } else {
                    // Normal fan-out mode: add to topic_subscribers
                    // Use single entry() call to detect fresh topic creation atomically
                    // (avoids TOCTOU race between contains_key and entry)
                    let outer = self
                        .shared
                        .topic_subscribers
                        .entry(topic.clone())
                        .or_default();
                    let is_new_entry = outer.insert(conn_id.to_owned(), h.clone()).is_none();
                    let is_first = is_new_entry && outer.len() == 1;
                    // fetch_add while holding shard lock to prevent race with concurrent
                    // unsubscribe (which also needs the shard lock for remove_if)
                    if is_first && (topic.contains('*') || topic.contains('?')) {
                        self.shared.glob_topic_count.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(outer);
                }
            } else {
                // Connection already disconnected -- skip topic tracking and cluster sub
                // to avoid phantom subscriptions in conn_topics and cluster interest
                continue;
            }
            // Queue-group members have their own cleanup via conn_queue_groups.
            // Skip conn_topics + cluster refcount tracking to avoid spurious UNSUB.
            if queue_group.is_some() {
                continue;
            }
            // Only increment refcount if this is genuinely new for this connection
            let is_new = self
                .shared
                .conn_topics
                .entry(conn_id.to_owned())
                .or_default()
                .insert(topic.clone());

            if is_new && let (Some(tx), Some(rc)) = (&cluster_tx, &mut refcounts) {
                let count = rc.entry(topic.clone()).or_insert(0);
                *count += 1;
                if *count == 1 {
                    let _ = tx.send(ClusterCommand::Sub {
                        topic: topic.clone(),
                    });
                }
            }
        }

        // Presence tracking
        if let (Some(pm), Some(pd)) = (&self.shared.presence, &presence_data) {
            let data = pyobj_to_json(pd.as_any());
            let drain = self.shared.drain_mode.load(Ordering::Relaxed);
            let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
            for topic in &topics {
                if let super::presence::TrackResult::FirstJoin {
                    user_id,
                    data: join_data,
                } = pm.track(conn_id, topic, &data)
                {
                    if drain {
                        self.shared.push_inbound(InboundEvent::PresenceJoin {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            data: join_data.clone(),
                        });
                    }
                    // Broadcast to topic subscribers via WebSocket
                    if let Some(ref tx) = self.cmd_tx {
                        let msg = format_presence_msg("presence_join", &user_id, &join_data);
                        let _ = tx.send(ServerCommand::BroadcastLocal {
                            topic: topic.clone(),
                            data: msg,
                            skip_recovery: true,
                        });
                    }
                    // Cluster sync: notify peers of presence join
                    if let Some(ref ctx) = cluster_tx {
                        let data_str = serde_json::to_string(&join_data).unwrap_or_default();
                        let now = super::presence::epoch_ms();
                        let _ = ctx.send(ClusterCommand::PresenceUpdate {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            action: 0, // join
                            data: data_str,
                            updated_at: now,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    #[pyo3(signature = (conn_id, topics, recover = false, epoch = None, offset = None))]
    fn subscribe_with_recovery(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        recover: bool,
        epoch: Option<String>,
        offset: Option<u64>,
    ) -> PyResult<Py<PyDict>> {
        // Step 1: Subscribe using existing logic (no presence tracking for recovery)
        self.subscribe_connection(conn_id, topics.clone(), None, None)?;

        // Step 2: Get connection tx + pending counter (clone them, drop the lock)
        let (tx, tx_pending) = if recover {
            let rt = self
                .rt_handle
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
            let guard = rt.block_on(self.shared.connections.read());
            match guard.get(conn_id) {
                Some(h) => (Some(h.tx.clone()), Some(h.pending.clone())),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        let recovery_mgr = self.shared.recovery.as_ref();
        let mut all_recovered = recover && !topics.is_empty();
        let result_dict = PyDict::new(py);
        let topics_dict = PyDict::new(py);

        for topic in &topics {
            let topic_dict = PyDict::new(py);

            // Check if recovery is possible and requested
            let can_recover = recover
                && epoch.is_some()
                && offset.is_some()
                && recovery_mgr.is_some()
                && tx.is_some();

            if can_recover {
                let epoch_str = epoch.as_ref().unwrap();
                let after_offset = offset.unwrap();
                let mgr = recovery_mgr.unwrap();

                // Parse epoch from hex string
                let epoch_u32 = u32::from_str_radix(epoch_str, 16).map_err(|e| {
                    PyRuntimeError::new_err(format!("Invalid epoch hex string: {e}"))
                })?;

                let result = mgr.recover(topic, epoch_u32, after_offset);
                match result {
                    super::recovery::RecoveryResult::Recovered {
                        publications,
                        epoch: current_epoch,
                        offset: current_offset,
                    } => {
                        let count = publications.len();
                        let conn_tx = tx.as_ref().unwrap();
                        let conn_pending = tx_pending.as_ref().unwrap();
                        for pub_bytes in publications {
                            let pb_size = pub_bytes.len();
                            conn_pending.fetch_add(pb_size, Ordering::Relaxed);
                            if conn_tx.send(WsFrame::PreFramed(pub_bytes)).is_err() {
                                conn_pending.fetch_sub(pb_size, Ordering::Relaxed);
                            }
                        }
                        topic_dict.set_item("epoch", format!("{:08x}", current_epoch))?;
                        topic_dict.set_item("offset", current_offset)?;
                        topic_dict.set_item("recoverable", true)?;
                        topic_dict.set_item("recovered", true)?;
                        topic_dict.set_item("count", count)?;
                    }
                    super::recovery::RecoveryResult::NotRecovered {
                        epoch: current_epoch,
                        offset: current_offset,
                    } => {
                        all_recovered = false;
                        topic_dict.set_item("epoch", format!("{:08x}", current_epoch))?;
                        topic_dict.set_item("offset", current_offset.saturating_sub(1))?;
                        topic_dict.set_item("recoverable", true)?;
                        topic_dict.set_item("recovered", false)?;
                        topic_dict.set_item("count", 0)?;
                    }
                    super::recovery::RecoveryResult::NoHistory => {
                        all_recovered = false;
                        // Get current position if available (normalize to inclusive last offset)
                        if let Some((ep, off)) = mgr.get_position(topic) {
                            topic_dict.set_item("epoch", format!("{:08x}", ep))?;
                            topic_dict.set_item("offset", off.saturating_sub(1))?;
                            topic_dict.set_item("recoverable", true)?;
                        } else {
                            topic_dict.set_item("epoch", py.None())?;
                            topic_dict.set_item("offset", 0u64)?;
                            topic_dict.set_item("recoverable", false)?;
                        }
                        topic_dict.set_item("recovered", false)?;
                        topic_dict.set_item("count", 0)?;
                    }
                }
            } else {
                // No recovery requested or not possible -- just report current position
                // (normalize to inclusive last offset, consistent with Recovered path)
                all_recovered = false;
                if let Some(mgr) = recovery_mgr {
                    if let Some((ep, off)) = mgr.get_position(topic) {
                        topic_dict.set_item("epoch", format!("{:08x}", ep))?;
                        topic_dict.set_item("offset", off.saturating_sub(1))?;
                        topic_dict.set_item("recoverable", true)?;
                    } else {
                        topic_dict.set_item("epoch", py.None())?;
                        topic_dict.set_item("offset", 0u64)?;
                        topic_dict.set_item("recoverable", false)?;
                    }
                } else {
                    topic_dict.set_item("epoch", py.None())?;
                    topic_dict.set_item("offset", 0u64)?;
                    topic_dict.set_item("recoverable", false)?;
                }
                topic_dict.set_item("recovered", false)?;
                topic_dict.set_item("count", 0)?;
            }

            topics_dict.set_item(topic.as_str(), topic_dict)?;
        }

        result_dict.set_item("recovered", all_recovered)?;
        result_dict.set_item("topics", topics_dict)?;

        Ok(result_dict.into())
    }

    #[pyo3(signature = (conn_id, topics = None))]
    fn unsubscribe_connection(&self, conn_id: &str, topics: Option<Vec<String>>) -> PyResult<()> {
        // Acquire refcount lock first to keep subscription + refcount atomic
        let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
        let mut refcounts = if cluster_tx.is_some() {
            Some(
                self.shared
                    .local_topic_refcount
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            )
        } else {
            None
        };

        let unsubscribe_all = topics.is_none();
        // Save requested topics for queue group cleanup (conn_queue_groups tracks separately from conn_topics)
        let requested_unsub: Vec<String> = match &topics {
            Some(t) => t.clone(),
            None => Vec::new(),
        };
        let removed_topics: Vec<String> = match topics {
            Some(topics) => {
                let mut actually_removed = Vec::new();
                for topic in &topics {
                    // Only count as removed if conn was actually subscribed
                    let was_subscribed =
                        if let Some(conn_entry) = self.shared.conn_topics.get(conn_id) {
                            conn_entry.remove(topic).is_some()
                        } else {
                            false
                        };
                    if was_subscribed {
                        if let Some(subscribers) = self.shared.topic_subscribers.get(topic) {
                            subscribers.remove(conn_id);
                        }
                        if self
                            .shared
                            .topic_subscribers
                            .remove_if(topic, |_, subs| subs.is_empty())
                            .is_some()
                            && (topic.contains('*') || topic.contains('?'))
                        {
                            self.shared.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        actually_removed.push(topic.clone());
                    }
                }
                // Clean up empty conn_topics entry to prevent DashSet leak
                self.shared
                    .conn_topics
                    .remove_if(conn_id, |_, topics| topics.is_empty());
                actually_removed
            }
            None => {
                let mut removed = Vec::new();
                if let Some((_, topics)) = self.shared.conn_topics.remove(conn_id) {
                    for topic_ref in topics.iter() {
                        let topic = topic_ref.clone();
                        if let Some(subscribers) = self.shared.topic_subscribers.get(&topic) {
                            subscribers.remove(conn_id);
                        }
                        if self
                            .shared
                            .topic_subscribers
                            .remove_if(&topic, |_, subs| subs.is_empty())
                            .is_some()
                            && (topic.contains('*') || topic.contains('?'))
                        {
                            self.shared.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
                        }
                        removed.push(topic);
                    }
                }
                removed
            }
        };

        // Queue group cleanup: when unsubscribing specific topics, only remove those;
        // when unsubscribing all (topics=None), remove all queue group memberships.
        if let Some(mut memberships) = self.shared.conn_queue_groups.get_mut(conn_id) {
            memberships.retain(|(topic, group_name)| {
                if !unsubscribe_all
                    && !removed_topics.contains(topic)
                    && !requested_unsub.contains(topic)
                {
                    return true; // keep: topic was not in unsubscribe request
                }
                if let Some(tg) = self.shared.queue_groups.get(topic) {
                    if let Some(mut g) = tg.get_mut(group_name) {
                        g.members.retain(|(id, _)| id != conn_id);
                    }
                    tg.remove_if(group_name, |_, g| g.members.is_empty());
                }
                self.shared
                    .queue_groups
                    .remove_if(topic, |_, groups| groups.is_empty());
                false // remove from memberships
            });
        }
        // Clean up empty conn_queue_groups entry
        self.shared
            .conn_queue_groups
            .remove_if(conn_id, |_, v| v.is_empty());

        // Presence cleanup for removed topics
        if let Some(ref pm) = self.shared.presence {
            let results = pm.untrack_topics(conn_id, &removed_topics);
            let drain = self.shared.drain_mode.load(Ordering::Relaxed);
            for (topic, result) in results {
                if let super::presence::UntrackResult::LastLeave { user_id, data } = result {
                    if drain {
                        self.shared.push_inbound(InboundEvent::PresenceLeave {
                            topic: topic.clone(),
                            user_id: user_id.clone(),
                            data: data.clone(),
                        });
                    }
                    // Broadcast to topic subscribers via WebSocket
                    if let Some(ref tx) = self.cmd_tx {
                        let msg = format_presence_msg("presence_leave", &user_id, &data);
                        let _ = tx.send(ServerCommand::BroadcastLocal {
                            topic: topic.clone(),
                            data: msg,
                            skip_recovery: true,
                        });
                    }
                    // Cluster sync: notify peers of presence leave
                    if let Some(ref ctx) = cluster_tx {
                        let data_str = serde_json::to_string(&data).unwrap_or_default();
                        let now = super::presence::epoch_ms();
                        let _ = ctx.send(ClusterCommand::PresenceUpdate {
                            topic,
                            user_id,
                            action: 1, // leave
                            data: data_str,
                            updated_at: now,
                        });
                    }
                }
            }
        }

        // Emit UNSUB to cluster for topics whose refcount went 1->0
        if let (Some(tx), Some(rc)) = (&cluster_tx, &mut refcounts) {
            for topic in &removed_topics {
                if let Some(count) = rc.get_mut(topic) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        rc.remove(topic);
                        let _ = tx.send(ClusterCommand::Unsub {
                            topic: topic.clone(),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Fan-out to topic subscribers locally.
    fn broadcast_local(&self, topic: &str, data: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        let data = inject_category(data);
        tx.send(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data,
            skip_recovery: false,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Fan-out locally + cluster PUBLISH for multi-instance coordination.
    fn broadcast(&self, topic: &str, data: &str) -> PyResult<()> {
        let data = inject_category(data);

        // Local dispatch (always)
        if let Some(ref tx) = self.cmd_tx {
            let _ = tx.send(ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data: data.clone(),
                skip_recovery: false,
            });
        }

        // Cluster publish (if connected)
        let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
        if let Some(tx) = cluster_tx {
            tx.send(ClusterCommand::Publish {
                topic: topic.to_owned(),
                payload: data,
            })
            .map_err(|_| PyRuntimeError::new_err("Cluster command channel closed"))?;
        }
        Ok(())
    }

    fn get_topic_subscriber_count(&self, topic: &str) -> usize {
        self.shared
            .topic_subscribers
            .get(topic)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Get queue group info for a topic. Returns dict of group_name -> member_count.
    fn get_queue_group_info(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        if let Some(topic_groups) = self.shared.queue_groups.get(topic) {
            for entry in topic_groups.iter() {
                dict.set_item(entry.key().as_str(), entry.value().members.len())?;
            }
        }
        Ok(dict.into())
    }

    fn health_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item(
            "connections",
            self.shared.connection_count.load(Ordering::Relaxed),
        )?;
        dict.set_item("inbound_queue_depth", self.shared.inbound_rx.len())?;
        dict.set_item(
            "inbound_dropped",
            self.shared.inbound_dropped.load(Ordering::Relaxed),
        )?;
        // Cluster metrics
        let cm = &self.shared.cluster_metrics;
        dict.set_item(
            "cluster_connected",
            cm.connected_peers.load(Ordering::Relaxed) > 0,
        )?;
        dict.set_item(
            "cluster_peer_count",
            cm.connected_peers.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_sent",
            cm.messages_sent.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_delivered",
            cm.messages_delivered.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_messages_dropped",
            cm.messages_dropped.load(Ordering::Relaxed),
        )?;
        dict.set_item("cluster_bytes_sent", cm.bytes_sent.load(Ordering::Relaxed))?;
        dict.set_item(
            "cluster_bytes_received",
            cm.bytes_received.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_reconnect_count",
            cm.reconnect_count.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_unknown_message_types",
            cm.unknown_message_types.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "cluster_dlq_size",
            self.shared
                .cluster_dlq
                .try_lock()
                .map(|d| d.len())
                .unwrap_or(0),
        )?;
        dict.set_item(
            "uptime_secs",
            self.started_at
                .map(|s| s.elapsed().as_secs_f64())
                .unwrap_or(0.0),
        )?;
        // Recovery metrics
        if let Some(ref recovery) = self.shared.recovery {
            dict.set_item("recovery_enabled", true)?;
            dict.set_item("recovery_topic_count", recovery.topic_count())?;
            dict.set_item("recovery_total_bytes", recovery.total_bytes())?;
        } else {
            dict.set_item("recovery_enabled", false)?;
            dict.set_item("recovery_topic_count", 0)?;
            dict.set_item("recovery_total_bytes", 0)?;
        }
        // Presence metrics
        if let Some(ref pm) = self.shared.presence {
            dict.set_item("presence_enabled", true)?;
            dict.set_item("presence_topics", pm.total_topics())?;
            dict.set_item("presence_total_users", pm.total_users())?;
        } else {
            dict.set_item("presence_enabled", false)?;
        }
        Ok(dict.unbind())
    }

    fn prometheus_metrics(&self) -> String {
        let mut out = String::with_capacity(4096);

        // Gauges
        out.push_str("# HELP wse_connections Current active WebSocket connections\n");
        out.push_str("# TYPE wse_connections gauge\n");
        out.push_str(&format!(
            "wse_connections {}\n",
            self.shared.connection_count.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_queue_depth Drain queue backlog\n");
        out.push_str("# TYPE wse_inbound_queue_depth gauge\n");
        out.push_str(&format!(
            "wse_inbound_queue_depth {}\n",
            self.shared.inbound_rx.len()
        ));

        out.push_str("# HELP wse_uptime_seconds Server uptime in seconds\n");
        out.push_str("# TYPE wse_uptime_seconds gauge\n");
        out.push_str(&format!(
            "wse_uptime_seconds {:.1}\n",
            self.started_at
                .map(|s| s.elapsed().as_secs_f64())
                .unwrap_or(0.0)
        ));

        out.push_str("# HELP wse_topics Number of active topics\n");
        out.push_str("# TYPE wse_topics gauge\n");
        out.push_str(&format!(
            "wse_topics {}\n",
            self.shared.topic_subscribers.len()
        ));

        // Counters
        out.push_str("# HELP wse_messages_received_total Total messages received from clients\n");
        out.push_str("# TYPE wse_messages_received_total counter\n");
        out.push_str(&format!(
            "wse_messages_received_total {}\n",
            self.shared.messages_received_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_messages_sent_total Total messages sent to clients\n");
        out.push_str("# TYPE wse_messages_sent_total counter\n");
        out.push_str(&format!(
            "wse_messages_sent_total {}\n",
            self.shared.messages_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_accepted_total Total connections accepted\n");
        out.push_str("# TYPE wse_connections_accepted_total counter\n");
        out.push_str(&format!(
            "wse_connections_accepted_total {}\n",
            self.shared
                .connections_accepted_total
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_connections_rejected_total Total connections rejected\n");
        out.push_str("# TYPE wse_connections_rejected_total counter\n");
        out.push_str(&format!(
            "wse_connections_rejected_total {}\n",
            self.shared
                .connections_rejected_total
                .load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_bytes_received_total Total bytes received from clients\n");
        out.push_str("# TYPE wse_bytes_received_total counter\n");
        out.push_str(&format!(
            "wse_bytes_received_total {}\n",
            self.shared.bytes_received_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_bytes_sent_total Total bytes sent to clients\n");
        out.push_str("# TYPE wse_bytes_sent_total counter\n");
        out.push_str(&format!(
            "wse_bytes_sent_total {}\n",
            self.shared.bytes_sent_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_auth_failures_total Total authentication failures\n");
        out.push_str("# TYPE wse_auth_failures_total counter\n");
        out.push_str(&format!(
            "wse_auth_failures_total {}\n",
            self.shared.auth_failures_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_rate_limited_total Total messages dropped by rate limiter\n");
        out.push_str("# TYPE wse_rate_limited_total counter\n");
        out.push_str(&format!(
            "wse_rate_limited_total {}\n",
            self.shared.rate_limited_total.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_inbound_dropped_total Drain queue events dropped\n");
        out.push_str("# TYPE wse_inbound_dropped_total counter\n");
        out.push_str(&format!(
            "wse_inbound_dropped_total {}\n",
            self.shared.inbound_dropped.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP wse_slow_consumer_drops_total Messages dropped due to slow consumers\n",
        );
        out.push_str("# TYPE wse_slow_consumer_drops_total counter\n");
        out.push_str(&format!(
            "wse_slow_consumer_drops_total {}\n",
            self.shared.slow_consumer_drops.load(Ordering::Relaxed)
        ));

        // Cluster metrics
        let cm = &self.shared.cluster_metrics;
        out.push_str("# HELP wse_cluster_peers Connected cluster peers\n");
        out.push_str("# TYPE wse_cluster_peers gauge\n");
        out.push_str(&format!(
            "wse_cluster_peers {}\n",
            cm.connected_peers.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_messages_sent_total Messages sent to cluster peers\n");
        out.push_str("# TYPE wse_cluster_messages_sent_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_sent_total {}\n",
            cm.messages_sent.load(Ordering::Relaxed)
        ));

        out.push_str(
            "# HELP wse_cluster_messages_delivered_total Messages delivered from cluster peers\n",
        );
        out.push_str("# TYPE wse_cluster_messages_delivered_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_delivered_total {}\n",
            cm.messages_delivered.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_messages_dropped_total Messages dropped in cluster\n");
        out.push_str("# TYPE wse_cluster_messages_dropped_total counter\n");
        out.push_str(&format!(
            "wse_cluster_messages_dropped_total {}\n",
            cm.messages_dropped.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_bytes_sent_total Bytes sent to cluster peers\n");
        out.push_str("# TYPE wse_cluster_bytes_sent_total counter\n");
        out.push_str(&format!(
            "wse_cluster_bytes_sent_total {}\n",
            cm.bytes_sent.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_bytes_received_total Bytes received from cluster peers\n");
        out.push_str("# TYPE wse_cluster_bytes_received_total counter\n");
        out.push_str(&format!(
            "wse_cluster_bytes_received_total {}\n",
            cm.bytes_received.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP wse_cluster_reconnects_total Cluster peer reconnections\n");
        out.push_str("# TYPE wse_cluster_reconnects_total counter\n");
        out.push_str(&format!(
            "wse_cluster_reconnects_total {}\n",
            cm.reconnect_count.load(Ordering::Relaxed)
        ));

        // Recovery metrics (conditional)
        if let Some(ref recovery) = self.shared.recovery {
            out.push_str("# HELP wse_recovery_topics Topics with recovery buffers\n");
            out.push_str("# TYPE wse_recovery_topics gauge\n");
            out.push_str(&format!("wse_recovery_topics {}\n", recovery.topic_count()));

            out.push_str("# HELP wse_recovery_bytes Total bytes in recovery buffers\n");
            out.push_str("# TYPE wse_recovery_bytes gauge\n");
            out.push_str(&format!("wse_recovery_bytes {}\n", recovery.total_bytes()));
        }

        // Per-topic message counts (top 50 by volume)
        // Evict stale entries when map grows too large (prevents unbounded growth
        // for servers using user-scoped or UUID-keyed topics)
        {
            let map = &self.shared.topic_message_counts;
            if map.len() > 10_000 {
                let active_topics: std::collections::HashSet<String> = self
                    .shared
                    .topic_subscribers
                    .iter()
                    .map(|e| e.key().clone())
                    .collect();
                map.retain(|k, _| active_topics.contains(k));
            }
            if !map.is_empty() {
                let mut entries: Vec<(String, u64)> = map
                    .iter()
                    .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
                    .collect();
                entries.sort_unstable_by(|a, b| b.1.cmp(&a.1));
                entries.truncate(50);

                out.push_str(
                    "# HELP wse_topic_messages_total Messages delivered per topic (top 50)\n",
                );
                out.push_str("# TYPE wse_topic_messages_total counter\n");
                for (topic, count) in &entries {
                    // Escape label value per Prometheus exposition format
                    let safe_topic = topic
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('\n', "\\n");
                    out.push_str(&format!(
                        "wse_topic_messages_total{{topic=\"{}\"}} {}\n",
                        safe_topic, count
                    ));
                }
            }
        }

        // Presence metrics (conditional)
        if let Some(ref pm) = self.shared.presence {
            out.push_str("# HELP wse_presence_topics Topics with presence tracking\n");
            out.push_str("# TYPE wse_presence_topics gauge\n");
            out.push_str(&format!("wse_presence_topics {}\n", pm.total_topics()));

            out.push_str("# HELP wse_presence_users Total users across all topics\n");
            out.push_str("# TYPE wse_presence_users gauge\n");
            out.push_str(&format!("wse_presence_users {}\n", pm.total_users()));
        }

        out
    }

    fn get_cluster_dlq_entries(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let entries = match self.shared.cluster_dlq.try_lock() {
            Ok(mut dlq) => dlq.drain_all(),
            Err(_) => Vec::new(),
        };
        let list = PyList::empty(py);
        for entry in &entries {
            let d = PyDict::new(py);
            d.set_item("topic", &entry.topic)?;
            d.set_item("payload", &entry.payload)?;
            d.set_item("peer_addr", &entry.peer_addr)?;
            d.set_item("error", &entry.error)?;
            list.append(d)?;
        }
        Ok(list.unbind())
    }

    /// Get all members currently present in a topic.
    /// Returns dict: {user_id: {"data": {...}, "connections": N}}
    fn presence(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);

        if let Some(ref pm) = self.shared.presence {
            for (user_id, data, conn_count) in pm.query(topic) {
                let user_dict = PyDict::new(py);
                user_dict.set_item("data", json_to_pyobj(py, &data))?;
                user_dict.set_item("connections", conn_count)?;
                result.set_item(user_id.as_str(), user_dict)?;
            }
        }

        Ok(result.into())
    }

    /// Get lightweight presence counters for a topic (O(1), no iteration).
    /// Returns dict: {"num_users": N, "num_connections": M}
    fn presence_stats(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        let result = PyDict::new(py);
        if let Some(ref pm) = self.shared.presence {
            let (users, conns) = pm.stats(topic);
            result.set_item("num_users", users)?;
            result.set_item("num_connections", conns)?;
        } else {
            result.set_item("num_users", 0)?;
            result.set_item("num_connections", 0)?;
        }
        Ok(result.into())
    }

    /// Update presence data for a connection across all its presence topics.
    #[pyo3(signature = (conn_id, data))]
    fn update_presence(&self, conn_id: &str, data: &Bound<'_, PyDict>) -> PyResult<()> {
        if let Some(ref pm) = self.shared.presence {
            let new_data = pyobj_to_json(data.as_any());
            match pm.update_data(conn_id, &new_data) {
                None => {
                    return Err(PyRuntimeError::new_err(
                        "Failed to update presence data (size limit or unknown connection)",
                    ));
                }
                Some((user_id, topics)) => {
                    // Broadcast presence_update to all topics where the user has presence
                    let data_str = serde_json::to_string(&new_data).unwrap_or_default();
                    let now = super::presence::epoch_ms();
                    let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
                    if let Some(ref tx) = self.cmd_tx {
                        let msg = format_presence_msg("presence_update", &user_id, &new_data);
                        for topic in &topics {
                            let _ = tx.send(ServerCommand::BroadcastLocal {
                                topic: topic.clone(),
                                data: msg.clone(),
                                skip_recovery: true,
                            });
                        }
                    }
                    // Cluster sync: notify peers of presence update
                    if let Some(ref ctx) = cluster_tx {
                        for topic in topics {
                            let _ = ctx.send(ClusterCommand::PresenceUpdate {
                                topic,
                                user_id: user_id.clone(),
                                action: 2, // update
                                data: data_str.clone(),
                                updated_at: now,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_ws_frame_small() {
        // Text frame, 5-byte payload "hello"
        let frame = encode_ws_frame(0x01, b"hello");
        assert_eq!(frame[0], 0x81); // FIN=1 + opcode=text
        assert_eq!(frame[1], 5); // payload length
        assert_eq!(&frame[2..], b"hello");
        assert_eq!(frame.len(), 7); // 2-byte header + 5 payload
    }

    #[test]
    fn test_encode_ws_frame_medium() {
        // 200-byte payload (uses 2-byte extended length)
        let data = vec![0x42u8; 200];
        let frame = encode_ws_frame(0x02, &data);
        assert_eq!(frame[0], 0x82); // FIN=1 + opcode=binary
        assert_eq!(frame[1], 126); // extended 16-bit length marker
        assert_eq!(u16::from_be_bytes([frame[2], frame[3]]), 200);
        assert_eq!(&frame[4..], &data[..]);
        assert_eq!(frame.len(), 204); // 4-byte header + 200 payload
    }

    #[test]
    fn test_encode_ws_frame_large() {
        // 70000-byte payload (uses 8-byte extended length)
        let data = vec![0xAA; 70000];
        let frame = encode_ws_frame(0x01, &data);
        assert_eq!(frame[0], 0x81);
        assert_eq!(frame[1], 127); // extended 64-bit length marker
        let len = u64::from_be_bytes(frame[2..10].try_into().unwrap());
        assert_eq!(len, 70000);
        assert_eq!(frame.len(), 10 + 70000);
    }

    #[test]
    fn test_pre_frame_text() {
        let frame = pre_frame_text("hi");
        assert_eq!(frame[0], 0x81); // text opcode
        assert_eq!(frame[1], 2);
        assert_eq!(&frame[2..], b"hi");
    }

    #[test]
    fn test_pre_frame_binary() {
        let frame = pre_frame_binary(&[1, 2, 3]);
        assert_eq!(frame[0], 0x82); // binary opcode
        assert_eq!(frame[1], 3);
        assert_eq!(&frame[2..], &[1, 2, 3]);
    }

    // -- Queue group tests ----------------------------------------------------

    fn make_handle() -> (ConnectionHandle, mpsc::UnboundedReceiver<WsFrame>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let h = ConnectionHandle {
            tx,
            pending: Arc::new(AtomicUsize::new(0)),
            broadcast_buf: Arc::new(parking_lot::Mutex::new(BytesMut::new())),
            broadcast_notify: Arc::new(tokio::sync::Notify::new()),
        };
        (h, rx)
    }

    #[test]
    fn test_queue_group_round_robin() {
        // 3 members in one group, 9 messages -> each gets 3
        let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
        let topic = "work".to_string();

        let (h1, _rx1) = make_handle();
        let (h2, _rx2) = make_handle();
        let (h3, _rx3) = make_handle();

        let group = QueueGroup {
            members: vec![
                ("c1".to_string(), h1),
                ("c2".to_string(), h2),
                ("c3".to_string(), h3),
            ],
            next: AtomicUsize::new(0),
        };
        let inner: DashMap<String, QueueGroup> = DashMap::new();
        inner.insert("workers".to_string(), group);
        queue_groups.insert(topic.clone(), inner);

        let data = pre_frame_text("hello");
        // Send 9 messages through round-robin
        for _ in 0..9 {
            if let Some(tg) = queue_groups.get(&topic) {
                for mut entry in tg.iter_mut() {
                    let g = entry.value_mut();
                    let idx = g.next.fetch_add(1, Ordering::Relaxed) % g.members.len();
                    let (_, ref h) = g.members[idx];
                    h.pending.fetch_add(data.len(), Ordering::Relaxed);
                    let mut buf = h.broadcast_buf.lock();
                    buf.extend_from_slice(&data);
                }
            }
        }

        // Check each member got 3 messages worth of data
        let tg = queue_groups.get(&topic).unwrap();
        let g = tg.get("workers").unwrap();
        for (_, h) in &g.members {
            assert_eq!(
                h.pending.load(Ordering::Relaxed),
                data.len() * 3,
                "Each member should receive 3 messages"
            );
        }
    }

    #[test]
    fn test_queue_group_and_fanout_coexist() {
        // Fan-out subscribers and queue group members can exist for the same topic
        let topic_subscribers: DashMap<String, DashMap<String, ConnectionHandle>> = DashMap::new();
        let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
        let topic = "events".to_string();

        // Fan-out subscriber
        let (h_fanout, _rx_fanout) = make_handle();
        let inner_subs: DashMap<String, ConnectionHandle> = DashMap::new();
        inner_subs.insert("fanout1".to_string(), h_fanout);
        topic_subscribers.insert(topic.clone(), inner_subs);

        // Queue group member
        let (h_qg, _rx_qg) = make_handle();
        let group = QueueGroup {
            members: vec![("qg1".to_string(), h_qg)],
            next: AtomicUsize::new(0),
        };
        let inner_groups: DashMap<String, QueueGroup> = DashMap::new();
        inner_groups.insert("workers".to_string(), group);
        queue_groups.insert(topic.clone(), inner_groups);

        // Both should exist independently
        assert_eq!(topic_subscribers.get(&topic).unwrap().len(), 1);
        assert_eq!(queue_groups.get(&topic).unwrap().len(), 1);
    }

    #[test]
    fn test_queue_group_member_disconnect() {
        let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
        let conn_queue_groups: DashMap<String, Vec<(String, String)>> = DashMap::new();

        let (h1, _rx1) = make_handle();
        let (h2, _rx2) = make_handle();

        let group = QueueGroup {
            members: vec![("c1".to_string(), h1), ("c2".to_string(), h2)],
            next: AtomicUsize::new(0),
        };
        let inner: DashMap<String, QueueGroup> = DashMap::new();
        inner.insert("workers".to_string(), group);
        queue_groups.insert("work".to_string(), inner);

        conn_queue_groups.insert(
            "c1".to_string(),
            vec![("work".to_string(), "workers".to_string())],
        );

        // Simulate disconnect cleanup for c1
        if let Some((_, memberships)) = conn_queue_groups.remove("c1") {
            for (topic, group_name) in memberships {
                if let Some(tg) = queue_groups.get(&topic) {
                    if let Some(mut g) = tg.get_mut(&group_name) {
                        g.members.retain(|(id, _)| id != "c1");
                    }
                    tg.remove_if(&group_name, |_, g| g.members.is_empty());
                }
                queue_groups.remove_if(&topic, |_, groups| groups.is_empty());
            }
        }

        // c2 should still be a member
        let tg = queue_groups.get("work").unwrap();
        let g = tg.get("workers").unwrap();
        assert_eq!(g.members.len(), 1);
        assert_eq!(g.members[0].0, "c2");
    }

    #[test]
    fn test_queue_group_empty_cleanup() {
        let queue_groups: DashMap<String, DashMap<String, QueueGroup>> = DashMap::new();
        let (h1, _rx1) = make_handle();

        let group = QueueGroup {
            members: vec![("c1".to_string(), h1)],
            next: AtomicUsize::new(0),
        };
        let inner: DashMap<String, QueueGroup> = DashMap::new();
        inner.insert("workers".to_string(), group);
        queue_groups.insert("work".to_string(), inner);

        // Remove the only member
        {
            let tg = queue_groups.get("work").unwrap();
            let mut g = tg.get_mut("workers").unwrap();
            g.members.retain(|(id, _)| id != "c1");
        }
        // Clean up empty groups
        if let Some(tg) = queue_groups.get("work") {
            tg.remove_if("workers", |_, g| g.members.is_empty());
        }
        queue_groups.remove_if("work", |_, groups| groups.is_empty());

        // Everything should be cleaned up
        assert!(queue_groups.is_empty());
    }

    #[test]
    fn test_topic_metrics_counting() {
        let counts: DashMap<String, AtomicU64> = DashMap::new();

        // First insert
        counts.insert("chat.general".to_string(), AtomicU64::new(1));

        // Subsequent increments
        if let Some(counter) = counts.get("chat.general") {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(counter) = counts.get("chat.general") {
            counter.fetch_add(1, Ordering::Relaxed);
        }

        // Different topic
        counts.insert("events.system".to_string(), AtomicU64::new(1));

        assert_eq!(
            counts.get("chat.general").unwrap().load(Ordering::Relaxed),
            3
        );
        assert_eq!(
            counts.get("events.system").unwrap().load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_topic_metrics_top_n_limit() {
        let counts: DashMap<String, AtomicU64> = DashMap::new();

        // Insert 60 topics
        for i in 0..60 {
            counts.insert(format!("topic.{i}"), AtomicU64::new(i as u64));
        }

        // Simulate top-50 extraction
        let mut entries: Vec<(String, u64)> = counts
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        entries.sort_unstable_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(50);

        assert_eq!(entries.len(), 50);
        // Highest should be 59
        assert_eq!(entries[0].1, 59);
        // 50th should be 10 (topics 0-9 excluded)
        assert_eq!(entries[49].1, 10);
    }
}
