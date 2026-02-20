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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use ahash::AHashSet;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use futures_util::{SinkExt, StreamExt};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct ConnectionHandle {
    tx: mpsc::UnboundedSender<Message>,
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

enum ServerCommand {
    SendText { conn_id: String, data: String },
    SendBytes { conn_id: String, data: Vec<u8> },
    SendPrebuilt { conn_id: String, message: Message },
    BroadcastText { data: String },
    BroadcastBytes { data: Vec<u8> },
    Disconnect { conn_id: String },
    GetConnections { reply: oneshot::Sender<Vec<String>> },
    GetConnectionCount { reply: oneshot::Sender<usize> },
    Shutdown,
}

struct SharedState {
    connections: RwLock<HashMap<String, ConnectionHandle>>,
    max_connections: usize,
    on_connect: RwLock<Option<Py<PyAny>>>,
    on_message: RwLock<Option<Py<PyAny>>>,
    on_disconnect: RwLock<Option<Py<PyAny>>>,
    // Drain mode: when active, inbound events go to queue instead of callbacks
    drain_mode: AtomicBool,
    inbound_queue: std::sync::Mutex<std::collections::VecDeque<InboundEvent>>,
    inbound_condvar: std::sync::Condvar,
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

impl SharedState {
    fn new(max_connections: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            max_connections,
            on_connect: RwLock::new(None),
            on_message: RwLock::new(None),
            on_disconnect: RwLock::new(None),
            drain_mode: AtomicBool::new(false),
            inbound_queue: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(4096)),
            inbound_condvar: std::sync::Condvar::new(),
        }
    }

    /// Push an event to the drain queue and wake the drain caller.
    fn push_inbound(&self, event: InboundEvent) {
        let mut queue = self.inbound_queue.lock().unwrap();
        queue.push_back(event);
        self.inbound_condvar.notify_one();
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
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(stream: TcpStream, addr: SocketAddr, state: Arc<SharedState>) {
    let cookies = Arc::new(std::sync::Mutex::new(String::new()));
    let cookies_clone = cookies.clone();

    let ws_stream = match tokio_tungstenite::accept_hdr_async(
        stream,
        move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
            if let Some(cv) = req.headers().get("cookie")
                && let Ok(s) = cv.to_str()
            {
                *cookies_clone.lock().unwrap() = s.to_string();
            }
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

    let cookie_str = cookies.lock().unwrap().clone();
    let conn_id: Arc<String> = Arc::new(Uuid::now_v7().to_string());
    let (write_half, read_half) = ws_stream.split();
    let mut read_half = read_half;
    let mut write_half = write_half;
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    let drain = state.drain_mode.load(Ordering::Relaxed);

    // Register connection
    {
        let mut conns = state.connections.write().await;
        if conns.len() >= state.max_connections {
            eprintln!("[WSE] Max connections reached, rejecting {addr}");
            let _ = write_half.close().await;
            return;
        }
        conns.insert((*conn_id).clone(), ConnectionHandle { tx: tx.clone() });
    }

    // Fire on_connect (drain mode: push to queue; callback mode: call Python)
    if drain {
        state.push_inbound(InboundEvent::Connect {
            conn_id: (*conn_id).clone(),
            cookies: cookie_str.clone(),
        });
    } else {
        let cb_guard = state.on_connect.read().await;
        if let Some(ref cb) = *cb_guard {
            let cb_clone = Python::try_attach(|py| cb.clone_ref(py)).expect("GIL attach failed");
            let cid = Arc::clone(&conn_id);
            let ck = cookie_str.clone();
            drop(cb_guard);
            fire_on_connect(&cb_clone, &cid, &ck);
        }
    }

    // Write task with coalescing
    let write_task = tokio::spawn(async move {
        loop {
            let msg = match rx.recv().await {
                Some(m) => m,
                None => break,
            };
            if write_half.feed(msg).await.is_err() {
                break;
            }
            let mut feed_ok = true;
            let mut count = 1u32;
            while count < 64 {
                match rx.try_recv() {
                    Ok(msg) => {
                        if write_half.feed(msg).await.is_err() {
                            feed_ok = false;
                            break;
                        }
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
            if !feed_ok || write_half.flush().await.is_err() {
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
                                let _ = tx.send(Message::Text(pong.into()));
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
                if drain {
                    state.push_inbound(InboundEvent::Binary {
                        conn_id: (*conn_id).clone(),
                        data: data.to_vec(),
                    });
                } else if let Some(ref cb) = cached_on_message {
                    let cb_arc = cb.clone();
                    let cid = Arc::clone(&conn_id);
                    let d = data.to_vec();
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
            Message::Ping(payload) => {
                let _ = tx.send(Message::Pong(payload));
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
    if drain {
        state.push_inbound(InboundEvent::Disconnect {
            conn_id: (*conn_id).clone(),
        });
    } else {
        let cb_guard = state.on_disconnect.read().await;
        if let Some(ref cb) = *cb_guard {
            let cb_clone = Python::try_attach(|py| cb.clone_ref(py)).expect("GIL attach failed");
            let cid = Arc::clone(&conn_id);
            drop(cb_guard);
            fire_on_disconnect(&cb_clone, &cid);
        }
    }
    drop(tx);
    let _ = write_task.await;
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
                    let _ = h.tx.send(Message::Text(data.into()));
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(Message::Binary(data.into()));
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(message);
                }
            }
            ServerCommand::BroadcastText { data } => {
                let conns = state.connections.read().await;
                for h in conns.values() {
                    let _ = h.tx.send(Message::Text(data.clone().into()));
                }
            }
            ServerCommand::BroadcastBytes { data } => {
                let conns = state.connections.read().await;
                for h in conns.values() {
                    let _ = h.tx.send(Message::Binary(data.clone().into()));
                }
            }
            ServerCommand::Disconnect { conn_id } => {
                let conns = state.connections.read().await;
                if let Some(h) = conns.get(&conn_id) {
                    let _ = h.tx.send(Message::Close(None));
                }
            }
            ServerCommand::GetConnections { reply } => {
                let conns = state.connections.read().await;
                let _ = reply.send(conns.keys().cloned().collect());
            }
            ServerCommand::GetConnectionCount { reply } => {
                let conns = state.connections.read().await;
                let _ = reply.send(conns.len());
            }
            ServerCommand::Shutdown => {
                let conns = state.connections.read().await;
                for h in conns.values() {
                    let _ = h.tx.send(Message::Close(None));
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
    conn_rates: Arc<std::sync::Mutex<HashMap<String, PerConnRate>>>,
}

#[pymethods]
impl RustWSEServer {
    #[new]
    #[pyo3(signature = (host, port, max_connections = 1000))]
    fn new(host: String, port: u16, max_connections: usize) -> Self {
        Self {
            host,
            port,
            shared: Arc::new(SharedState::new(max_connections)),
            cmd_tx: None,
            thread_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            dedup: Arc::new(std::sync::Mutex::new(DeduplicationState {
                seen: AHashSet::with_capacity(50_000),
                queue: std::collections::VecDeque::with_capacity(50_000),
                max_entries: 50_000,
            })),
            conn_rates: Arc::new(std::sync::Mutex::new(HashMap::new())),
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
                                        tokio::spawn(handle_connection(stream, addr, shared.clone()));
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
        Ok(())
    }

    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
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
            let mut rates = self.conn_rates.lock().unwrap();
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

        // Serialize
        let json_str = serde_json::to_string(&serde_json::Value::Object(map))
            .map_err(|e| PyRuntimeError::new_err(format!("JSON error: {e}")))?;

        // Category prefix + payload
        let category = message_category(&msg_type);
        let payload_str = format!("{}{}", category, json_str);
        let payload_bytes = payload_str.as_bytes();
        let byte_count;

        if compression_threshold > 0 && payload_bytes.len() > compression_threshold {
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
            byte_count = payload_str.len();
            cmd_tx
                .send(ServerCommand::SendPrebuilt {
                    conn_id: conn_id.to_owned(),
                    message: Message::Text(payload_str.into()),
                })
                .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        }

        Ok(byte_count)
    }

    fn broadcast(&self, data: &str) -> PyResult<()> {
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

    fn broadcast_bytes(&self, data: &[u8]) -> PyResult<()> {
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

    fn get_connection_count(&self) -> PyResult<usize> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Not running"))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(ServerCommand::GetConnectionCount { reply: reply_tx })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        reply_rx
            .blocking_recv()
            .map_err(|_| PyRuntimeError::new_err("Reply dropped"))
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
    /// event_type: "connect" | "msg" | "raw" | "bin" | "disconnect"
    /// data: dict (for "msg"), str (for "connect"=cookies, "raw"), bytes (for "bin"), None (for "disconnect")
    ///
    /// GIL is released while waiting on condvar, acquired once for batch conversion.
    #[pyo3(signature = (max_count = 256, timeout_ms = 50))]
    fn drain_inbound(
        &self,
        py: Python<'_>,
        max_count: usize,
        timeout_ms: u64,
    ) -> PyResult<Py<PyList>> {
        // Collect events with GIL released (condvar wait happens without GIL)
        let events: Vec<InboundEvent> = py.detach(|| {
            let mut guard = self.shared.inbound_queue.lock().unwrap();
            if guard.is_empty() {
                let timeout = Duration::from_millis(timeout_ms);
                let (new_guard, _) = self
                    .shared
                    .inbound_condvar
                    .wait_timeout(guard, timeout)
                    .unwrap();
                guard = new_guard;
            }
            let count = guard.len().min(max_count);
            guard.drain(..count).collect()
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
}
