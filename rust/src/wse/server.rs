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
use super::redis_pubsub::{DeadLetterQueue, RedisCommand, RedisMetrics};
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
use tokio_tungstenite::tungstenite::protocol::Message;
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
}

const RATE_CAPACITY: f64 = 100_000.0;
const RATE_REFILL: f64 = 10_000.0; // tokens per second

// ---------------------------------------------------------------------------
// Parallel fan-out tuning
// ---------------------------------------------------------------------------
const PARALLEL_FANOUT_THRESHOLD: usize = 256;
const FANOUT_CHUNK_SIZE: usize = 512;

enum ServerCommand {
    SendText { conn_id: String, data: String },
    SendBytes { conn_id: String, data: Vec<u8> },
    SendPrebuilt { conn_id: String, message: Message },
    BroadcastText { data: String },
    BroadcastBytes { data: Vec<u8> },
    BroadcastLocal { topic: String, data: String },
    Disconnect { conn_id: String },
    GetConnections { reply: oneshot::Sender<Vec<String>> },
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
    conn_rates: std::sync::Mutex<HashMap<String, PerConnRate>>,
    // Atomic connection count (lock-free read from Python without GIL blocking)
    connection_count: AtomicUsize,
    // JWT config: when set, Rust validates JWT in handshake (zero GIL)
    jwt_config: Option<JwtConfig>,
    pub(crate) topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    conn_topics: DashMap<String, DashSet<String>>,
    redis_cmd_tx: std::sync::Mutex<Option<mpsc::UnboundedSender<RedisCommand>>>,
    redis_metrics: Arc<RedisMetrics>,
    redis_dlq: Arc<std::sync::Mutex<DeadLetterQueue>>,
    // Cluster protocol
    cluster_cmd_tx: std::sync::Mutex<Option<mpsc::Sender<ClusterCommand>>>,
    pub(crate) cluster_metrics: Arc<ClusterMetrics>,
    cluster_dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    pub(crate) cluster_instance_id: std::sync::Mutex<Option<String>>,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

impl SharedState {
    fn new(
        max_connections: usize,
        jwt_config: Option<JwtConfig>,
        max_inbound_queue_size: usize,
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
            conn_rates: std::sync::Mutex::new(HashMap::new()),
            connection_count: AtomicUsize::new(0),
            jwt_config,
            topic_subscribers: Arc::new(DashMap::new()),
            conn_topics: DashMap::new(),
            redis_cmd_tx: std::sync::Mutex::new(None),
            redis_metrics: Arc::new(RedisMetrics::new()),
            redis_dlq: Arc::new(std::sync::Mutex::new(DeadLetterQueue::new(1000))),
            cluster_cmd_tx: std::sync::Mutex::new(None),
            cluster_metrics: Arc::new(ClusterMetrics::new()),
            cluster_dlq: Arc::new(std::sync::Mutex::new(ClusterDlq::new(1000))),
            cluster_instance_id: std::sync::Mutex::new(None),
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

/// Build a JSON error message for auth failures.
fn build_error_json(code: &str, message: &str) -> String {
    let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    format!(
        r#"WSE{{"t":"error","p":{{"code":"{}","message":"{}","timestamp":"{}"}},"v":1}}"#,
        code, message, ts,
    )
}

/// Build server_ready JSON message (sent immediately from Rust after JWT validation).
fn build_server_ready(conn_id: &str, user_id: &str) -> String {
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
                    "rust_jwt": true
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
    // Clone the TCP stream FD for pre-framed raw writes (bypasses tungstenite).
    let (stream, raw_write) = match clone_tcp_stream(stream) {
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

    let ws_stream = match tokio_tungstenite::accept_hdr_async(
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
        let token = parse_cookie_value(&cookie_str, "access_token").or_else(|| {
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
                        let err = build_error_json("AUTH_FAILED", "Token missing user ID");
                        let _ = tx.send(WsFrame::Msg(Message::Text(err.into())));
                        let _ = tx.send(WsFrame::Msg(Message::Close(None)));
                        let write_task = tokio::spawn(async move {
                            while let Some(frame) = rx.recv().await {
                                if let WsFrame::Msg(msg) = frame
                                    && write_half.feed(msg).await.is_err()
                                {
                                    break;
                                }
                            }
                            let _ = write_half.flush().await;
                            let _ = write_half.close().await;
                        });
                        let _ = write_task.await;
                        return;
                    }
                }
                Err(e) => {
                    let err = build_error_json("AUTH_FAILED", &format!("{e}"));
                    let _ = tx.send(WsFrame::Msg(Message::Text(err.into())));
                    let _ = tx.send(WsFrame::Msg(Message::Close(None)));
                    let write_task = tokio::spawn(async move {
                        while let Some(frame) = rx.recv().await {
                            if let WsFrame::Msg(msg) = frame
                                && write_half.feed(msg).await.is_err()
                            {
                                break;
                            }
                        }
                        let _ = write_half.flush().await;
                        let _ = write_half.close().await;
                    });
                    let _ = write_task.await;
                    return;
                }
            },
            None => {
                let err = build_error_json(
                    "AUTH_REQUIRED",
                    "No access_token cookie or Authorization header",
                );
                let _ = tx.send(WsFrame::Msg(Message::Text(err.into())));
                let _ = tx.send(WsFrame::Msg(Message::Close(None)));
                let write_task = tokio::spawn(async move {
                    while let Some(frame) = rx.recv().await {
                        if let WsFrame::Msg(msg) = frame
                            && write_half.feed(msg).await.is_err()
                        {
                            break;
                        }
                    }
                    let _ = write_half.flush().await;
                    let _ = write_half.close().await;
                });
                let _ = write_task.await;
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
        conns.insert((*conn_id).clone(), ConnectionHandle { tx: tx.clone() });
        state.connection_count.fetch_add(1, Ordering::Relaxed);
    }
    // Store format preference for lock-free access from send_event()
    if use_msgpack {
        state.conn_formats.insert((*conn_id).clone(), true);
    }

    // If Rust validated JWT, send server_ready IMMEDIATELY (zero GIL)
    if let Some(ref user_id) = rust_auth_user_id {
        let server_ready = build_server_ready(&conn_id, user_id);
        let _ = tx.send(WsFrame::Msg(Message::Text(server_ready.into())));
    }

    // Fire on_connect (drain mode: push to queue; callback mode: background task)
    if drain {
        if let Some(user_id) = rust_auth_user_id {
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
        if let Some(ref cb) = *cb_guard {
            let cb_clone = Python::try_attach(|py| cb.clone_ref(py)).expect("GIL attach failed");
            let cid = (*conn_id).clone();
            let ck = cookie_str.clone();
            drop(cb_guard);
            // Fire in background — don't block connection setup on GIL
            tokio::task::spawn_blocking(move || {
                fire_on_connect(&cb_clone, &cid, &ck);
            });
        }
    }

    // Write task with coalescing + pre-framed broadcast support.
    // `write_half` handles tungstenite Messages (per-connection sends, control frames).
    // `raw_write` handles pre-framed bytes (broadcasts) written directly to TCP.
    // BufWriter batches multiple PreFramed frames into a single TCP write (one syscall).
    let mut raw_write = tokio::io::BufWriter::new(raw_write);
    let write_task = tokio::spawn(async move {
        loop {
            let frame = match rx.recv().await {
                Some(f) => f,
                None => break,
            };

            let mut tung_dirty = false;
            let mut ok = true;

            // Process first frame
            match frame {
                WsFrame::Msg(msg) => {
                    if write_half.feed(msg).await.is_err() {
                        break;
                    }
                    tung_dirty = true;
                }
                WsFrame::PreFramed(bytes) => {
                    // tungstenite buffer is clean at start of batch
                    if raw_write.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            }

            // Coalescing: drain pending frames
            let mut count = 1u32;
            while ok && count < 64 {
                match rx.try_recv() {
                    Ok(WsFrame::Msg(msg)) => {
                        // Must flush raw_write before tungstenite write
                        if raw_write.flush().await.is_err() {
                            ok = false;
                            break;
                        }
                        if write_half.feed(msg).await.is_err() {
                            ok = false;
                            break;
                        }
                        tung_dirty = true;
                        count += 1;
                    }
                    Ok(WsFrame::PreFramed(bytes)) => {
                        // Must flush tungstenite before raw write
                        if tung_dirty {
                            if write_half.flush().await.is_err() {
                                ok = false;
                                break;
                            }
                            tung_dirty = false;
                        }
                        if raw_write.write_all(&bytes).await.is_err() {
                            ok = false;
                            break;
                        }
                        count += 1;
                    }
                    Err(_) => break,
                }
            }

            if !ok {
                break;
            }
            // Final flush: raw BufWriter + tungstenite if dirty
            if raw_write.flush().await.is_err() {
                break;
            }
            if tung_dirty && write_half.flush().await.is_err() {
                break;
            }
        }
        let _ = write_half.close().await;
    });

    // Cache on_message callback (only needed in callback mode)
    let cached_on_message: Option<Arc<Py<PyAny>>> = if !drain {
        let cb_guard = state.on_message.read().await;
        cb_guard.as_ref().map(|cb| {
            Arc::new(Python::try_attach(|py| cb.clone_ref(py)).expect("GIL attach failed"))
        })
    } else {
        None
    };

    // Read loop
    while let Some(Ok(msg)) = read_half.next().await {
        match msg {
            Message::Text(text) => {
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
                        // Fast-path: ping handled entirely in Rust
                        if let Some(t_val) = val.get("t").and_then(|t| t.as_str()) {
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
                                    "WSE{{\"t\":\"PONG\",\"p\":{{\"client_timestamp\":{},\"server_timestamp\":{},\"latency\":{}}},\"v\":2}}",
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

    // Cleanup
    {
        let mut conns = state.connections.write().await;
        conns.remove(&*conn_id);
    }
    state.conn_formats.remove(&*conn_id);
    {
        state.conn_rates.lock().unwrap().remove(&*conn_id);
    }
    // Pub/Sub cleanup: remove connection from all subscribed topics
    if let Some((_, topics)) = state.conn_topics.remove(&*conn_id) {
        for topic_ref in topics.iter() {
            let topic = topic_ref.clone();
            if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                subscribers.remove(&*conn_id);
                if subscribers.is_empty() {
                    drop(subscribers);
                    state.topic_subscribers.remove(&topic);
                }
            }
        }
    }
    state.connection_count.fetch_sub(1, Ordering::Relaxed);
    if drain {
        state.push_inbound(InboundEvent::Disconnect {
            conn_id: (*conn_id).clone(),
        });
    } else {
        let cb_guard = state.on_disconnect.read().await;
        if let Some(ref cb) = *cb_guard {
            let cb_clone = Python::try_attach(|py| cb.clone_ref(py)).expect("GIL attach failed");
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
// Parallel fan-out helpers
// ---------------------------------------------------------------------------

/// Collect subscriber sender handles for a topic, with dedup.
/// Holds connections read lock only during collection (not during sends).
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
        if (pattern.contains('*') || pattern.contains('?'))
            && super::redis_pubsub::glob_match(pattern, topic)
        {
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
/// Parallel when above threshold, sequential otherwise.
fn fanout_to_senders(senders: Vec<mpsc::UnboundedSender<WsFrame>>, frame: WsFrame) {
    let n = senders.len();
    if n == 0 {
        return;
    }
    if n < PARALLEL_FANOUT_THRESHOLD {
        for tx in &senders {
            let _ = tx.send(frame.clone());
        }
    } else {
        let mut iter = senders.into_iter();
        loop {
            let chunk: Vec<mpsc::UnboundedSender<WsFrame>> =
                iter.by_ref().take(FANOUT_CHUNK_SIZE).collect();
            if chunk.is_empty() {
                break;
            }
            let chunk_frame = frame.clone();
            tokio::spawn(async move {
                for tx in &chunk {
                    let _ = tx.send(chunk_frame.clone());
                }
            });
        }
    }
}

/// Trait for fan-out metrics tracking. Implemented by ClusterMetrics and RedisMetrics.
pub(crate) trait FanoutMetrics: Send + Sync + 'static {
    fn add_delivered(&self, count: u64);
    fn add_dropped(&self, count: u64);
}

/// Fan-out with metrics tracking (for cluster/redis dispatch).
/// Takes Arc<M> where M implements FanoutMetrics, enabling spawned tasks
/// to update metrics with 'static lifetime.
pub(crate) fn fanout_to_senders_with_metrics<M: FanoutMetrics>(
    senders: Vec<mpsc::UnboundedSender<WsFrame>>,
    frame: WsFrame,
    metrics: &Arc<M>,
) {
    let n = senders.len();
    if n == 0 {
        return;
    }
    if n < PARALLEL_FANOUT_THRESHOLD {
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
    } else {
        let mut iter = senders.into_iter();
        loop {
            let chunk: Vec<mpsc::UnboundedSender<WsFrame>> =
                iter.by_ref().take(FANOUT_CHUNK_SIZE).collect();
            if chunk.is_empty() {
                break;
            }
            let chunk_frame = frame.clone();
            let m = metrics.clone();
            tokio::spawn(async move {
                let mut d = 0u64;
                let mut f = 0u64;
                for tx in &chunk {
                    if tx.send(chunk_frame.clone()).is_ok() {
                        d += 1;
                    } else {
                        f += 1;
                    }
                }
                m.add_delivered(d);
                if f > 0 {
                    m.add_dropped(f);
                }
            });
        }
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
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(Message::Text(data.into())));
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(Message::Binary(data.into())));
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(message));
                }
            }
            ServerCommand::BroadcastText { data } => {
                let frame = WsFrame::PreFramed(pre_frame_text(&data));
                let senders: Vec<mpsc::UnboundedSender<WsFrame>> = {
                    let conns = state.connections.read().await;
                    conns.values().map(|h| h.tx.clone()).collect()
                };
                fanout_to_senders(senders, frame);
            }
            ServerCommand::BroadcastBytes { data } => {
                let frame = WsFrame::PreFramed(pre_frame_binary(&data));
                let senders: Vec<mpsc::UnboundedSender<WsFrame>> = {
                    let conns = state.connections.read().await;
                    conns.values().map(|h| h.tx.clone()).collect()
                };
                fanout_to_senders(senders, frame);
            }
            ServerCommand::BroadcastLocal { topic, data } => {
                let frame = WsFrame::PreFramed(pre_frame_text(&data));
                let senders = {
                    let conns = state.connections.read().await;
                    collect_topic_senders(&conns, &state.topic_subscribers, &topic)
                };
                fanout_to_senders(senders, frame);
            }
            ServerCommand::Disconnect { conn_id } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(WsFrame::Msg(Message::Close(None)));
                }
            }
            ServerCommand::GetConnections { reply } => {
                let conns = state.connections.read().await;
                let _ = reply.send(conns.keys().cloned().collect());
            }
            ServerCommand::Shutdown => {
                let conns = state.connections.read().await;
                for h in conns.values() {
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
    #[pyo3(signature = (host, port, max_connections = 1000, jwt_secret = None, jwt_issuer = None, jwt_audience = None, max_inbound_queue_size = 131072))]
    fn new(
        host: String,
        port: u16,
        max_connections: usize,
        jwt_secret: Option<Vec<u8>>,
        jwt_issuer: Option<String>,
        jwt_audience: Option<String>,
        max_inbound_queue_size: usize,
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
            }
        });
        Self {
            host,
            port,
            shared: Arc::new(SharedState::new(
                max_connections,
                jwt_config,
                max_inbound_queue_size,
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
                    loop {
                        tokio::select! {
                            res = listener.accept() => {
                                match res {
                                    Ok((stream, addr)) => {
                                        let _ = stream.set_nodelay(true);
                                        let shared2 = shared.clone();
                                        tokio::spawn(async move {
                                            // Protocol detection: peek first byte to distinguish
                                            // cluster binary frames (first byte 0x00 = length prefix
                                            // for frames < 16MB) from HTTP/WS (first byte is ASCII
                                            // letter like 'G' for GET). Timeout handles misbehaving
                                            // clients that connect but never send data.
                                            // Note: TLS ClientHello starts with 0x16 -- if direct
                                            // TLS termination is added, update this check.
                                            let mut peek = [0u8; 1];
                                            let is_cluster = match tokio::time::timeout(
                                                Duration::from_secs(5),
                                                stream.peek(&mut peek),
                                            ).await {
                                                Ok(Ok(1)) => peek[0] == 0x00,
                                                _ => false,
                                            };
                                            if is_cluster {
                                                super::cluster::handle_cluster_inbound(
                                                    stream, addr, &shared2,
                                                ).await;
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
        if let Ok(guard) = self.shared.cluster_cmd_tx.lock()
            && let Some(ref tx) = *guard
        {
            let _ = tx.try_send(ClusterCommand::Shutdown);
        }
        // Shutdown Redis
        if let Ok(guard) = self.shared.redis_cmd_tx.lock()
            && let Some(ref tx) = *guard
        {
            let _ = tx.send(RedisCommand::Shutdown);
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
        *self.shared.cluster_cmd_tx.lock().unwrap() = None;
        *self.shared.redis_cmd_tx.lock().unwrap() = None;
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

        // #8: Per-connection rate limiter (token bucket)
        {
            let mut rates = self.shared.conn_rates.lock().unwrap();
            let state = rates
                .entry(conn_id.to_owned())
                .or_insert_with(|| PerConnRate {
                    tokens: RATE_CAPACITY,
                    last_refill: std::time::Instant::now(),
                });
            let elapsed = state.last_refill.elapsed().as_secs_f64();
            state.tokens = (state.tokens + elapsed * RATE_REFILL).min(RATE_CAPACITY);
            state.last_refill = std::time::Instant::now();
            if state.tokens < 1.0 {
                return Ok(0); // rate limited
            }
            state.tokens -= 1.0;
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

            if compression_threshold > 0 && payload_bytes.len() > compression_threshold {
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

    // -- Redis Pub/Sub --------------------------------------------------------

    fn connect_redis(&self, url: String) -> PyResult<()> {
        // Mutual exclusion: error if cluster is connected
        if self.shared.cluster_cmd_tx.lock().unwrap().is_some() {
            return Err(PyRuntimeError::new_err(
                "Cannot connect Redis: Cluster is already connected. Use one backend at a time.",
            ));
        }
        let rt_handle = self
            .rt_handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<RedisCommand>();
        *self.shared.redis_cmd_tx.lock().unwrap() = Some(cmd_tx);

        let connections = self.shared.connections.clone();
        let topic_subscribers = self.shared.topic_subscribers.clone();
        let metrics = self.shared.redis_metrics.clone();
        let dlq = self.shared.redis_dlq.clone();
        rt_handle.spawn(super::redis_pubsub::listener_task(
            url,
            connections,
            topic_subscribers,
            cmd_rx,
            metrics,
            dlq,
        ));
        Ok(())
    }

    // -- Cluster Protocol -----------------------------------------------------

    fn connect_cluster(&self, peers: Vec<String>) -> PyResult<()> {
        // Mutual exclusion: error if Redis is connected
        if self.shared.redis_cmd_tx.lock().unwrap().is_some() {
            return Err(PyRuntimeError::new_err(
                "Cannot connect cluster: Redis is already connected. Use one backend at a time.",
            ));
        }
        if self.shared.cluster_cmd_tx.lock().unwrap().is_some() {
            return Err(PyRuntimeError::new_err("Cluster already connected"));
        }

        let rt_handle = self
            .rt_handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Server not started"))?;

        let (cmd_tx, cmd_rx) = mpsc::channel::<ClusterCommand>(131072);
        *self.shared.cluster_cmd_tx.lock().unwrap() = Some(cmd_tx);

        let instance_id = uuid::Uuid::now_v7().to_string();
        *self.shared.cluster_instance_id.lock().unwrap() = Some(instance_id.clone());
        let connections = self.shared.connections.clone();
        let topic_subscribers = self.shared.topic_subscribers.clone();
        let metrics = self.shared.cluster_metrics.clone();
        let dlq = self.shared.cluster_dlq.clone();

        rt_handle.spawn(super::cluster::cluster_manager(
            peers,
            instance_id,
            cmd_rx,
            connections,
            topic_subscribers,
            metrics,
            dlq,
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

    fn subscribe_connection(&self, conn_id: &str, topics: Vec<String>) -> PyResult<()> {
        for topic in &topics {
            self.shared
                .topic_subscribers
                .entry(topic.clone())
                .or_default()
                .insert(conn_id.to_owned());
            self.shared
                .conn_topics
                .entry(conn_id.to_owned())
                .or_default()
                .insert(topic.clone());
        }
        Ok(())
    }

    #[pyo3(signature = (conn_id, topics = None))]
    fn unsubscribe_connection(&self, conn_id: &str, topics: Option<Vec<String>>) -> PyResult<()> {
        match topics {
            Some(topics) => {
                for topic in &topics {
                    if let Some(subscribers) = self.shared.topic_subscribers.get(topic) {
                        subscribers.remove(conn_id);
                        if subscribers.is_empty() {
                            drop(subscribers);
                            self.shared.topic_subscribers.remove(topic);
                        }
                    }
                    if let Some(conn_entry) = self.shared.conn_topics.get(conn_id) {
                        conn_entry.remove(topic);
                    }
                }
            }
            None => {
                if let Some((_, topics)) = self.shared.conn_topics.remove(conn_id) {
                    for topic_ref in topics.iter() {
                        let topic = topic_ref.clone();
                        if let Some(subscribers) = self.shared.topic_subscribers.get(&topic) {
                            subscribers.remove(conn_id);
                            if subscribers.is_empty() {
                                drop(subscribers);
                                self.shared.topic_subscribers.remove(&topic);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Fan-out to topic subscribers locally without Redis round-trip.
    fn broadcast_local(&self, topic: &str, data: &str) -> PyResult<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        tx.send(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data: data.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Fan-out locally + cluster/Redis PUBLISH for multi-instance coordination.
    fn broadcast(&self, topic: &str, data: &str) -> PyResult<()> {
        // Local dispatch (always)
        if let Some(ref tx) = self.cmd_tx {
            let _ = tx.send(ServerCommand::BroadcastLocal {
                topic: topic.to_owned(),
                data: data.to_owned(),
            });
        }

        // Cluster publish (if connected) -- clone sender to avoid holding mutex
        let cluster_tx = self.shared.cluster_cmd_tx.lock().unwrap().clone();
        if let Some(tx) = cluster_tx {
            tx.try_send(ClusterCommand::Publish {
                topic: topic.to_owned(),
                payload: data.to_owned(),
            })
            .map_err(|_| PyRuntimeError::new_err("Cluster command channel full"))?;
            return Ok(());
        }

        // Redis publish (fallback, if connected)
        let guard = self.shared.redis_cmd_tx.lock().unwrap();
        if let Some(ref tx) = *guard {
            tx.send(RedisCommand::Publish {
                channel: format!("wse:{topic}"),
                payload: data.to_owned(),
            })
            .map_err(|_| PyRuntimeError::new_err("Redis command channel closed"))?;
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

    fn redis_connected(&self) -> bool {
        self.shared.redis_metrics.connected.load(Ordering::Relaxed)
    }

    fn health_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let m = &self.shared.redis_metrics;
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
        dict.set_item("redis_connected", m.connected.load(Ordering::Relaxed))?;
        dict.set_item(
            "redis_messages_received",
            m.messages_received.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_messages_published",
            m.messages_published.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_messages_delivered",
            m.messages_delivered.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_messages_dropped",
            m.messages_dropped.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_publish_errors",
            m.publish_errors.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_reconnect_count",
            m.reconnect_count.load(Ordering::Relaxed),
        )?;
        dict.set_item(
            "redis_dlq_size",
            self.shared
                .redis_dlq
                .try_lock()
                .map(|dlq| dlq.len())
                .unwrap_or(0),
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
        Ok(dict.unbind())
    }

    fn get_dlq_entries(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let entries = match self.shared.redis_dlq.try_lock() {
            Ok(mut dlq) => dlq.drain_all(),
            Err(_) => Vec::new(),
        };
        let list = PyList::empty(py);
        for entry in &entries {
            let d = PyDict::new(py);
            d.set_item("channel", &entry.channel)?;
            d.set_item("payload", &entry.payload)?;
            d.set_item("error", &entry.error)?;
            list.append(d)?;
        }
        Ok(list.unbind())
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
