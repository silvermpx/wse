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
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
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

pub(crate) struct ConnectionHandle {
    pub(crate) tx: mpsc::UnboundedSender<WsFrame>,
}

// ---------------------------------------------------------------------------
// Pre-framed WebSocket encoding (server-to-client, no masking per RFC 6455)
// ---------------------------------------------------------------------------

/// Encode a complete WebSocket frame: header + payload.
/// Server-to-client frames are NOT masked, so all connections receive identical bytes.
fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Bytes {
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

const RATE_CAPACITY: f64 = 100_000.0;
const RATE_REFILL: f64 = 10_000.0; // tokens per second

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
}

pub(crate) struct SharedState {
    pub(crate) connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    max_connections: usize,
    on_connect: RwLock<Option<Py<PyAny>>>,
    on_message: RwLock<Option<Py<PyAny>>>,
    on_disconnect: RwLock<Option<Py<PyAny>>>,
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
    pub(crate) topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    conn_topics: DashMap<String, DashSet<String>>,
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
    pub(crate) recovery: Option<Arc<super::recovery::RecoveryManager>>,
    // Per-connection last activity tracking for zombie detection
    conn_last_activity: DashMap<String, std::time::Instant>,
    // Per-connection AES-256-GCM cipher for E2E encryption (optional, client-initiated)
    conn_encryption: DashMap<String, aes_gcm::Aes256Gcm>,
    // Presence tracking (None when disabled)
    pub(crate) presence: Option<Arc<super::presence::PresenceManager>>,
    // Sender for presence broadcasts from disconnect cleanup paths
    // (where self.cmd_tx is not available, only Arc<SharedState>)
    presence_broadcast_tx: std::sync::RwLock<Option<mpsc::UnboundedSender<ServerCommand>>>,
}

impl SharedState {
    fn new(
        max_connections: usize,
        jwt_config: Option<JwtConfig>,
        max_inbound_queue_size: usize,
        recovery: Option<Arc<super::recovery::RecoveryManager>>,
        presence_enabled: bool,
        presence_max_data_size: usize,
        presence_max_members: usize,
    ) -> Self {
        let cap = max_inbound_queue_size.min(131072);
        let (tx, rx) = bounded(cap);
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections,
            on_connect: RwLock::new(None),
            on_message: RwLock::new(None),
            on_disconnect: RwLock::new(None),
            drain_mode: AtomicBool::new(false),
            inbound_tx: tx,
            inbound_rx: rx,
            inbound_dropped: AtomicU64::new(0),
            conn_formats: DashMap::new(),
            conn_rates: DashMap::new(),
            connection_count: AtomicUsize::new(0),
            jwt_config,
            topic_subscribers: Arc::new(DashMap::new()),
            conn_topics: DashMap::new(),
            cluster_cmd_tx: std::sync::RwLock::new(None),
            cluster_metrics: Arc::new(ClusterMetrics::new()),
            cluster_dlq: Arc::new(std::sync::Mutex::new(ClusterDlq::new(1000))),
            cluster_instance_id: std::sync::Mutex::new(None),
            cluster_interest_tx: std::sync::RwLock::new(None),
            local_topic_refcount: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cluster_tls_enabled: AtomicBool::new(false),
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
            presence_broadcast_tx: std::sync::RwLock::new(None),
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

/// Format a presence event as a WSE wire message.
/// Returns e.g. `WSE{"t":"presence_join","p":{"user_id":"alice","data":{"status":"online"}}}`
pub(crate) fn format_presence_msg(
    event_type: &str,
    user_id: &str,
    data: &serde_json::Value,
) -> String {
    let payload = serde_json::json!({
        "t": event_type,
        "p": { "user_id": user_id, "data": data }
    });
    format!("WSE{payload}")
}

fn fire_on_connect(callback: &Py<PyAny>, conn_id: &str, cookies: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id, cookies)) {
            eprintln!("[WSE] on_connect error: {e}");
        }
    });
}

fn fire_on_disconnect(callback: &Py<PyAny>, conn_id: &str) {
    Python::try_attach(|py| {
        if let Err(e) = callback.call1(py, (conn_id,)) {
            eprintln!("[WSE] on_disconnect error: {e}");
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

// ---------------------------------------------------------------------------
// JWT helper functions (used by handle_connection)
// ---------------------------------------------------------------------------

/// Send an auth error + Close frame over the WS channel, drain it, and return.
/// Used for JWT rejection to avoid duplicating the send+drain pattern.
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
    let _ = tx.send(WsFrame::Msg(Message::Close(None)));
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
        "t": "error",
        "p": {
            "code": code,
            "message": message,
            "timestamp": ts,
        },
        "v": 1
    });
    format!("WSE{}", payload)
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
                "max_message_size": 1_048_576,
                "rate_limit_capacity": RATE_CAPACITY as u64,
                "rate_limit_refill": RATE_REFILL as u64
            },
            "ping_interval": 25000,
            "connection_id": conn_id,
            "server_time": ts
        },
        "v": 1
    });
    if let Some(pubkey) = encryption_pubkey {
        val["p"]["encryption_public_key"] = serde_json::Value::String(pubkey.to_string());
    }
    format!("WSE{val}")
}

/// Build server_ready JSON message (sent immediately from Rust after JWT validation).
fn build_server_ready(conn_id: &str, user_id: &str, recovery_enabled: bool) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let msg_id = Uuid::now_v7().to_string();
    let val = serde_json::json!({
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
    format!("WSE{val}")
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<SharedState>) {
    // Early rejection before expensive handshake (best-effort, authoritative check below)
    if state.connection_count.load(Ordering::Relaxed) >= state.max_connections {
        eprintln!("[WSE] Max connections reached (early check), rejecting {addr}");
        return;
    }

    // Clone the TCP stream FD for pre-framed raw writes (bypasses tungstenite).
    let (stream, mut raw_write) = match clone_tcp_stream(stream) {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("[WSE] Failed to clone TCP stream for {addr}: {e}");
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
    ws_config.max_message_size = Some(1_048_576); // 1 MB (matches server_hello)
    ws_config.max_frame_size = Some(1_048_576);
    let ws_stream = match tokio_tungstenite::accept_hdr_async_with_config(
        stream,
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
            eprintln!("[WSE] WS handshake failed for {addr}: {e}");
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
    let drain = state.drain_mode.load(Ordering::Relaxed);

    // ── JWT validation in Rust (zero GIL) ──────────────────────────────────
    // When jwt_config is set, validate JWT from cookie or Authorization header.
    // On success: send server_ready immediately, push AuthConnect to drain queue.
    // On failure: send error + close — connection never registered.
    let rust_auth_user_id: Option<String> = if let Some(ref jwt_cfg) = state.jwt_config {
        let token = parse_cookie_value(&cookie_str, &jwt_cfg.cookie_name).or_else(|| {
            auth_header.as_deref().and_then(|h| {
                // RFC 7235: auth-scheme is case-insensitive
                if h.len() > 7 && h[..7].eq_ignore_ascii_case("bearer ") {
                    Some(&h[7..])
                } else {
                    None
                }
            })
        });
        match token {
            Some(tok) => match jwt::jwt_decode(tok, jwt_cfg) {
                Ok(claims) => {
                    if let Some(user_id) = claims.get("sub").and_then(|s| s.as_str()) {
                        Some(user_id.to_string())
                    } else {
                        reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Token missing user ID")
                            .await;
                        return;
                    }
                }
                Err(e) => {
                    eprintln!("[WSE] JWT validation failed for {addr}: {e}");
                    reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Authentication failed")
                        .await;
                    return;
                }
            },
            None => {
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
            eprintln!("[WSE] Max connections reached, rejecting {addr}");
            let _ = write_half.close().await;
            return;
        }
        // Send server_ready BEFORE inserting into connections map.
        // While we hold the write lock, no broadcast can acquire a read lock,
        // so server_ready is guaranteed to be first in the channel.
        if let Some(ref user_id) = rust_auth_user_id {
            let recovery_enabled = state.recovery.is_some();
            let server_ready = build_server_ready(&conn_id, user_id, recovery_enabled);
            let _ = tx.send(WsFrame::Msg(Message::Text(server_ready.into())));
        }
        conns.insert((*conn_id).clone(), ConnectionHandle { tx: tx.clone() });
        state.connection_count.store(conns.len(), Ordering::Relaxed);
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
        let cb_guard = state.on_connect.read().await;
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

    // Write task: recv_many batch drain + manual concatenation.
    //
    // Instead of BufWriter (copies each frame into 8KB internal buffer), we:
    //   1. Drain up to 256 frames at once with recv_many (bulk atomic drain)
    //   2. Separate PreFramed (raw broadcast bytes) from Msg (tungstenite control/per-conn)
    //   3. Write all PreFramed via write_vectored (one writev syscall, zero memcpy)
    //   4. Feed all Msg to tungstenite, single flush
    let write_task = tokio::spawn(async move {
        let mut batch: Vec<WsFrame> = Vec::with_capacity(256);

        loop {
            // Drain up to 256 pending frames in one bulk operation
            let n = rx.recv_many(&mut batch, 256).await;
            if n == 0 {
                break; // channel closed
            }

            // Separate: PreFramed -> slices for writev, Msg -> tungstenite
            let mut raw_slices: Vec<Bytes> = Vec::new();
            let mut has_tung = false;
            for frame in &batch {
                match frame {
                    WsFrame::PreFramed(bytes) => raw_slices.push(bytes.clone()),
                    WsFrame::Msg(_) => has_tung = true,
                }
            }

            let mut ok = true;

            // Write all PreFramed via write_vectored (one writev syscall, zero memcpy).
            // Single call + fallback: writev once, if partial write then write_all remainder.
            if !raw_slices.is_empty() {
                let io_slices: Vec<std::io::IoSlice<'_>> = raw_slices
                    .iter()
                    .map(|b| std::io::IoSlice::new(b))
                    .collect();
                let total: usize = raw_slices.iter().map(|b| b.len()).sum();

                let written = match raw_write.write_vectored(&io_slices).await {
                    Ok(0) => {
                        ok = false;
                        0
                    }
                    Ok(n) => n,
                    Err(_) => {
                        ok = false;
                        0
                    }
                };

                // Partial write: concatenate remaining bytes and write_all
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

            // Feed all tungstenite Msgs, single flush.
            // Always attempt Msg frames even if PreFramed write failed --
            // Close frames are critical control frames that must be sent.
            if has_tung {
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

            // Always clear: covers PreFramed-only batches (drain didn't run)
            // and partial drain (ok=false broke out early).
            batch.clear();

            if !ok {
                break;
            }
        }
        let _ = write_half.close().await;
    });

    // Cache on_message callback (only needed in callback mode)
    let cached_on_message: Option<Arc<Py<PyAny>>> = if !drain {
        let cb_guard = state.on_message.read().await;
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
                                    eprintln!("[WSE] on_message error: {e}");
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
                                                eprintln!(
                                                    "[WSE] ECDH key exchange failed for {}: {}",
                                                    conn_id, e
                                                );
                                                let err_msg = format!(
                                                    "WSE{{\"t\":\"error\",\"p\":{{\"code\":\"KEY_EXCHANGE_FAILED\",\"message\":\"ECDH key exchange failed: {}\",\"recoverable\":true}}}}",
                                                    e.replace('\"', "\\\"")
                                                );
                                                let _ = tx.send(WsFrame::Msg(Message::Text(err_msg.into())));
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
                                let _ = tx.send(WsFrame::Msg(Message::Text(hello.into())));
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
                                    "WSE{{\"t\":\"PONG\",\"p\":{{\"client_timestamp\":{},\"server_timestamp\":{},\"latency\":{}}},\"v\":1}}",
                                    timestamp,
                                    server_ts,
                                    server_ts.saturating_sub(timestamp).max(0)
                                );
                                let _ = tx.send(WsFrame::Msg(Message::Text(pong.into())));
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
                                        eprintln!("[WSE] on_message error: {e}");
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
                                        eprintln!("[WSE] on_message error: {e}");
                                    }
                                });
                            });
                        }
                    }
                }
            }
            Message::Binary(data) => {
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
                                                        eprintln!(
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
                                eprintln!("[WSE] Decryption failed for {}: {}", conn_id, e);
                            }
                        }
                    } else {
                        eprintln!(
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
                                        eprintln!("[WSE] on_message (msgpack) error: {e}");
                                    }
                                }
                                Err(_) => {
                                    let py_bytes = PyBytes::new(py, &d);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                        eprintln!("[WSE] on_message (binary) error: {e}");
                                    }
                                }
                            });
                        });
                    } else {
                        tokio::task::spawn_blocking(move || {
                            Python::try_attach(|py| {
                                let py_bytes = PyBytes::new(py, &d);
                                if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                    eprintln!("[WSE] on_message (binary) error: {e}");
                                }
                            });
                        });
                    }
                }
            }
            Message::Ping(payload) => {
                let _ = tx.send(WsFrame::Msg(Message::Pong(payload)));
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
            state
                .topic_subscribers
                .remove_if(&topic, |_, subs| subs.is_empty());
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
        let cb_guard = state.on_disconnect.read().await;
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

/// Collect subscriber sender handles for a topic, with dedup.
/// Caller must hold a read guard on the connections map.
pub(crate) fn collect_topic_senders(
    conns: &HashMap<String, ConnectionHandle>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    topic: &str,
) -> Vec<mpsc::UnboundedSender<WsFrame>> {
    let mut seen = AHashSet::new();
    let mut senders = Vec::new();

    // Exact topic match
    if let Some(conn_ids) = topic_subscribers.get(topic) {
        senders.reserve(conn_ids.len());
        for conn_id_ref in conn_ids.iter() {
            let cid = conn_id_ref.key();
            if seen.insert(cid.clone())
                && let Some(handle) = conns.get(cid)
            {
                senders.push(handle.tx.clone());
            }
        }
    }

    // Glob pattern match
    for entry in topic_subscribers.iter() {
        let pattern = entry.key();
        if (pattern.contains('*') || pattern.contains('?')) && glob_match(pattern, topic) {
            for conn_id_ref in entry.value().iter() {
                let cid = conn_id_ref.key();
                if seen.insert(cid.clone())
                    && let Some(handle) = conns.get(cid)
                {
                    senders.push(handle.tx.clone());
                }
            }
        }
    }

    senders
}

/// Fan-out a pre-built message to collected sender handles.
/// Sequential fan-out: enqueue pre-built message to all collected sender handles.
/// Non-blocking channel sends -- each connection's writer task handles actual I/O.
fn fanout_to_senders(senders: Vec<mpsc::UnboundedSender<WsFrame>>, frame: WsFrame) {
    for tx in &senders {
        let _ = tx.send(frame.clone());
    }
}

/// Trait for fan-out metrics tracking. Implemented by ClusterMetrics.
pub(crate) trait FanoutMetrics: Send + Sync + 'static {
    fn add_delivered(&self, count: u64);
    fn add_dropped(&self, count: u64);
}

/// Fan-out with metrics tracking (for cluster dispatch).
/// Takes Arc<M> where M implements FanoutMetrics, enabling spawned tasks
/// to update metrics with 'static lifetime.
pub(crate) fn fanout_to_senders_with_metrics<M: FanoutMetrics>(
    senders: Vec<mpsc::UnboundedSender<WsFrame>>,
    frame: WsFrame,
    metrics: &Arc<M>,
) {
    let mut d = 0u64;
    let mut f = 0u64;
    for tx in &senders {
        if tx.send(frame.clone()).is_ok() {
            d += 1;
        } else {
            f += 1;
        }
    }
    metrics.add_delivered(d);
    if f > 0 {
        metrics.add_dropped(f);
    }
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
                    let _ = h.tx.send(WsFrame::Msg(Message::Text(data.into())));
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(Message::Binary(data.into())));
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(message));
                }
            }
            ServerCommand::BroadcastText { data } => {
                if state.conn_encryption.is_empty() {
                    // Fast path: no encrypted connections, zero overhead
                    let frame = WsFrame::PreFramed(pre_frame_text(&data));
                    let senders: Vec<mpsc::UnboundedSender<WsFrame>> = {
                        let guard = state.connections.read().await;
                        guard.values().map(|h| h.tx.clone()).collect()
                    };
                    fanout_to_senders(senders, frame);
                } else {
                    let preframed = pre_frame_text(&data);
                    let guard = state.connections.read().await;
                    let mut plain_senders = Vec::new();
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), cipher_ref.clone()));
                        } else {
                            plain_senders.push(h.tx.clone());
                        }
                    }
                    drop(guard);
                    if !plain_senders.is_empty() {
                        fanout_to_senders(plain_senders, WsFrame::PreFramed(preframed));
                    }
                    for (tx, cipher) in &enc_senders {
                        if let Ok(enc) = encrypt_outbound(cipher, data.as_bytes()) {
                            let mut buf = Vec::with_capacity(2 + enc.len());
                            buf.extend_from_slice(b"E:");
                            buf.extend_from_slice(&enc);
                            let _ = tx.send(WsFrame::Msg(Message::Binary(buf.into())));
                        }
                    }
                }
            }
            ServerCommand::BroadcastBytes { data } => {
                if state.conn_encryption.is_empty() {
                    let frame = WsFrame::PreFramed(pre_frame_binary(&data));
                    let senders: Vec<mpsc::UnboundedSender<WsFrame>> = {
                        let guard = state.connections.read().await;
                        guard.values().map(|h| h.tx.clone()).collect()
                    };
                    fanout_to_senders(senders, frame);
                } else {
                    let preframed = pre_frame_binary(&data);
                    let guard = state.connections.read().await;
                    let mut plain_senders = Vec::new();
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), cipher_ref.clone()));
                        } else {
                            plain_senders.push(h.tx.clone());
                        }
                    }
                    drop(guard);
                    if !plain_senders.is_empty() {
                        fanout_to_senders(plain_senders, WsFrame::PreFramed(preframed));
                    }
                    for (tx, cipher) in &enc_senders {
                        if let Ok(enc) = encrypt_outbound(cipher, &data) {
                            let mut buf = Vec::with_capacity(2 + enc.len());
                            buf.extend_from_slice(b"E:");
                            buf.extend_from_slice(&enc);
                            let _ = tx.send(WsFrame::Msg(Message::Binary(buf.into())));
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
                // Store in recovery buffer (skip for presence events)
                if !skip_recovery && let Some(ref recovery) = state.recovery {
                    recovery.push(&topic, preframed.clone());
                }
                if state.conn_encryption.is_empty() {
                    // Fast path: no encrypted connections
                    let senders = {
                        let guard = state.connections.read().await;
                        collect_topic_senders(&guard, &state.topic_subscribers, &topic)
                    };
                    fanout_to_senders(senders, WsFrame::PreFramed(preframed));
                } else {
                    let guard = state.connections.read().await;
                    let mut plain_senders = Vec::new();
                    let mut enc_senders = Vec::new();
                    let mut seen = AHashSet::new();
                    if let Some(subs) = state.topic_subscribers.get(&topic) {
                        for cid_ref in subs.iter() {
                            let cid = cid_ref.key().clone();
                            if seen.insert(cid.clone())
                                && let Some(h) = guard.get(&cid)
                            {
                                if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                    enc_senders.push((h.tx.clone(), cipher_ref.clone()));
                                } else {
                                    plain_senders.push(h.tx.clone());
                                }
                            }
                        }
                    }
                    // Glob pattern matching
                    for entry in state.topic_subscribers.iter() {
                        let pattern = entry.key();
                        if (pattern.contains('*') || pattern.contains('?'))
                            && glob_match(pattern, &topic)
                        {
                            for cid_ref in entry.value().iter() {
                                let cid = cid_ref.key().clone();
                                if seen.insert(cid.clone())
                                    && let Some(h) = guard.get(&cid)
                                {
                                    if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                        enc_senders.push((h.tx.clone(), cipher_ref.clone()));
                                    } else {
                                        plain_senders.push(h.tx.clone());
                                    }
                                }
                            }
                        }
                    }
                    drop(guard);
                    if !plain_senders.is_empty() {
                        fanout_to_senders(plain_senders, WsFrame::PreFramed(preframed));
                    }
                    for (tx, cipher) in &enc_senders {
                        if let Ok(enc) = encrypt_outbound(cipher, data.as_bytes()) {
                            let mut buf = Vec::with_capacity(2 + enc.len());
                            buf.extend_from_slice(b"E:");
                            buf.extend_from_slice(&enc);
                            let _ = tx.send(WsFrame::Msg(Message::Binary(buf.into())));
                        }
                    }
                }
            }
            ServerCommand::Disconnect { conn_id } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(Message::Close(None)));
                }
            }
            ServerCommand::GetConnections { reply } => {
                let guard = state.connections.read().await;
                let _ = reply.send(guard.keys().cloned().collect());
            }
            ServerCommand::Shutdown => {
                let guard = state.connections.read().await;
                for h in guard.values() {
                    let _ = h.tx.send(WsFrame::Msg(Message::Close(None)));
                }
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
    #[pyo3(signature = (host, port, max_connections = 1000, jwt_secret = None, jwt_issuer = None, jwt_audience = None, jwt_cookie_name = None, max_inbound_queue_size = 131072, recovery_enabled = false, recovery_buffer_size = 128, recovery_ttl = 300, recovery_max_messages = 500, recovery_memory_budget = 268435456, presence_enabled = false, presence_max_data_size = 4096, presence_max_members = 0))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        host: String,
        port: u16,
        max_connections: usize,
        jwt_secret: Option<Vec<u8>>,
        jwt_issuer: Option<String>,
        jwt_audience: Option<String>,
        jwt_cookie_name: Option<String>,
        max_inbound_queue_size: usize,
        recovery_enabled: bool,
        recovery_buffer_size: usize,
        recovery_ttl: u64,
        recovery_max_messages: usize,
        recovery_memory_budget: usize,
        presence_enabled: bool,
        presence_max_data_size: usize,
        presence_max_members: usize,
    ) -> Self {
        let jwt_config = jwt_secret.map(|secret| {
            eprintln!(
                "[WSE] JWT auth enabled (issuer={}, audience={})",
                jwt_issuer.as_deref().unwrap_or("none"),
                jwt_audience.as_deref().unwrap_or("none"),
            );
            JwtConfig {
                secret,
                issuer: jwt_issuer.unwrap_or_default(),
                audience: jwt_audience.unwrap_or_default(),
                cookie_name: jwt_cookie_name.unwrap_or_else(|| "access_token".to_string()),
            }
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
        Self {
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
        }
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
        let rt =
            Runtime::new().map_err(|e| PyRuntimeError::new_err(format!("Runtime error: {e}")))?;
        rt.block_on(async {
            *shared.on_connect.write().await = Some(on_connect);
            *shared.on_message.write().await = Some(on_message);
            *shared.on_disconnect.write().await = Some(on_disconnect);
        });
        Ok(())
    }

    fn start(&mut self) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("Server is already running"));
        }
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
                        eprintln!("[WSE] Failed to create runtime: {e}");
                        return;
                    }
                };
                let _ = rt_handle_tx.send(rt.handle().clone());
                rt.block_on(async move {
                    let bind_addr = format!("{host}:{port}");
                    let listener = match TcpListener::bind(&bind_addr).await {
                        Ok(l) => {
                            eprintln!("[WSE] Listening on {bind_addr}");
                            l
                        }
                        Err(e) => {
                            eprintln!("[WSE] Failed to bind {bind_addr}: {e}");
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
                    // Server-initiated ping + zombie detection
                    {
                        let ping_state = shared.clone();
                        tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(Duration::from_secs(25));
                            let idle_timeout = Duration::from_secs(60);
                            loop {
                                interval.tick().await;
                                let now = std::time::Instant::now();
                                // Collect senders under read lock, then release
                                let snapshot: Vec<(
                                    String,
                                    mpsc::UnboundedSender<WsFrame>,
                                )> = {
                                    let conns =
                                        ping_state.connections.read().await;
                                    conns
                                        .iter()
                                        .map(|(cid, h)| {
                                            (cid.clone(), h.tx.clone())
                                        })
                                        .collect()
                                };
                                let mut force_remove = Vec::new();
                                for (cid, tx) in &snapshot {
                                    if let Some(last) =
                                        ping_state.conn_last_activity.get(cid)
                                    {
                                        // Check zombie (no activity for 60s)
                                        if now.duration_since(*last) > idle_timeout
                                        {
                                            let _ = tx.send(WsFrame::Msg(
                                                Message::Close(None),
                                            ));
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
                                            "WSE{{\"t\":\"ping\",\"p\":{{\"server_time\":\"{ts}\"}}}}"
                                        );
                                        let _ = tx.send(WsFrame::Msg(
                                            Message::Text(ping_msg.into()),
                                        ));
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
                                    // Remove connections FIRST (mirrors normal disconnect order)
                                    {
                                        let mut conns =
                                            ping_state.connections.write().await;
                                        for cid in &force_remove {
                                            conns.remove(cid);
                                            ping_state.conn_formats.remove(cid);
                                            ping_state.conn_rates.remove(cid);
                                            ping_state.conn_last_activity.remove(cid);
                                            ping_state.conn_encryption.remove(cid);
                                        }
                                        ping_state.connection_count.store(
                                            conns.len(),
                                            Ordering::Relaxed,
                                        );
                                    }
                                    // Topic subscription cleanup for zombies
                                    let cluster_tx = ping_state.cluster_cmd_tx.read().unwrap().clone();
                                    for cid in &force_remove {
                                        if let Some((_, topics)) = ping_state.conn_topics.remove(cid) {
                                            let mut disconnected_topics = Vec::new();
                                            for topic_ref in topics.iter() {
                                                let topic = topic_ref.clone();
                                                if let Some(subscribers) = ping_state.topic_subscribers.get(&topic) {
                                                    subscribers.remove(cid);
                                                }
                                                ping_state.topic_subscribers.remove_if(&topic, |_, subs| subs.is_empty());
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
                                            let cb_guard = ping_state.on_disconnect.read().await;
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
                                    eprintln!(
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
                                                        eprintln!(
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
                                                    eprintln!(
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
                                        eprintln!("[WSE] Accept error: {e}");
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
                    eprintln!("[WSE] Shut down");
                });
            })
            .map_err(|e| PyRuntimeError::new_err(format!("Spawn error: {e}")))?;
        self.thread_handle = Some(handle);
        self.rt_handle = rt_handle_rx.recv().ok();
        self.started_at = Some(std::time::Instant::now());
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
                    tokens: RATE_CAPACITY,
                    last_refill: std::time::Instant::now(),
                    last_rate_limited: None,
                    last_warning: None,
                });
            let rate = entry.value_mut();
            let elapsed = rate.last_refill.elapsed().as_secs_f64();
            rate.tokens = (rate.tokens + elapsed * RATE_REFILL).min(RATE_CAPACITY);
            rate.last_refill = std::time::Instant::now();

            if rate.tokens < 1.0 {
                // Send rate_limited error to client (throttled: max once per second)
                let should_notify = rate
                    .last_rate_limited
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_notify {
                    let err = "WSE{\"t\":\"error\",\"p\":{\"code\":\"RATE_LIMITED\",\"message\":\"Rate limit exceeded\",\"retry_after\":1.0},\"v\":1}".to_string();
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
            let warning_threshold = RATE_CAPACITY * 0.2;
            if rate.tokens < warning_threshold {
                let should_warn = rate
                    .last_warning
                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(1));
                if should_warn {
                    let warning = format!(
                        "WSE{{\"t\":\"rate_limit_warning\",\"p\":{{\"current_rate\":{},\"limit\":{},\"remaining\":{},\"retry_after\":1.0}},\"v\":1}}",
                        (RATE_CAPACITY - rate.tokens) as u64,
                        RATE_CAPACITY as u64,
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

        // Build serde_json map from PyDict, with t first
        let mut map = serde_json::Map::new();
        let mut msg_type = String::new();

        // Extract 't' first
        if let Ok(Some(t_val)) = event.get_item("t")
            && let Ok(s) = t_val.extract::<String>()
        {
            msg_type = s.clone();
            map.insert("t".to_string(), serde_json::Value::String(s));
        }

        // All other keys (skip t, v, _msg_cat)
        for (k, v) in event.iter() {
            let key = match k.extract::<String>() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if key == "t" || key == "v" || key == "_msg_cat" {
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
            // JSON path: serialize + category prefix
            let json_str = serde_json::to_string(&serde_json::Value::Object(map))
                .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {e}")))?;

            let category = message_category(&msg_type);
            let payload_str = format!("{}{}", category, json_str);
            let payload_bytes = payload_str.as_bytes();

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
                byte_count = payload_str.len();
                cmd_tx
                    .send(ServerCommand::SendPrebuilt {
                        conn_id: conn_id.to_owned(),
                        message: Message::Binary(payload_str.into_bytes().into()),
                    })
                    .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
            }
        }

        Ok(byte_count)
    }

    fn broadcast_all(&self, data: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::BroadcastText {
            data: data.to_owned(),
        })
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
            metrics,
            dlq,
            local_topic_refcount,
            tls_config,
            cluster_port,
            cluster_addr,
            self.shared.presence.clone(),
            self.cmd_tx.clone(),
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

    // -- Topic subscriptions --------------------------------------------------

    #[pyo3(signature = (conn_id, topics, presence_data = None))]
    fn subscribe_connection(
        &self,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<()> {
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

        for topic in &topics {
            self.shared
                .topic_subscribers
                .entry(topic.clone())
                .or_default()
                .insert(conn_id.to_owned());
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
        self.subscribe_connection(conn_id, topics.clone(), None)?;

        // Step 2: Get connection tx (clone it, drop the lock)
        let tx = if recover {
            let rt = self
                .rt_handle
                .as_ref()
                .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
            let guard = rt.block_on(self.shared.connections.read());
            guard.get(conn_id).map(|h| h.tx.clone())
        } else {
            None
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
                        for pub_bytes in publications {
                            let _ = conn_tx.send(WsFrame::PreFramed(pub_bytes));
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
                        self.shared
                            .topic_subscribers
                            .remove_if(topic, |_, subs| subs.is_empty());
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
                        self.shared
                            .topic_subscribers
                            .remove_if(&topic, |_, subs| subs.is_empty());
                        removed.push(topic);
                    }
                }
                removed
            }
        };

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
        tx.send(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data: data.to_owned(),
            skip_recovery: false,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Fan-out locally + cluster PUBLISH for multi-instance coordination.
    fn broadcast(&self, topic: &str, data: &str) -> PyResult<()> {
        // Local dispatch (always)
        if let Some(ref tx) = self.cmd_tx {
            let _ = tx.send(ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data: data.to_owned(),
                skip_recovery: false,
            });
        }

        // Cluster publish (if connected)
        let cluster_tx = self.shared.cluster_cmd_tx.read().unwrap().clone();
        if let Some(tx) = cluster_tx {
            tx.send(ClusterCommand::Publish {
                topic: topic.to_owned(),
                payload: data.to_owned(),
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
}
