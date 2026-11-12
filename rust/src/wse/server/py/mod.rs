//! The Python binding layer: the `RustWSEServer` class.
//!
//! This file holds everything Python sees: the class, its constructor
//! parameters and the single `#[pymethods]` block (PyO3 takes one per class
//! without its `multiple-pymethods` feature), with every signature, default and
//! docstring. Apart from the constructor and the short accessors, each method
//! delegates to an `*_impl` helper in a topic file.

mod callbacks;
mod cluster;
mod convert;
mod inbound;
mod lifecycle;
mod messaging;
mod metrics;
mod presence;
mod subscriptions;

pub(super) use callbacks::{fire_on_connect, fire_on_disconnect};
pub(super) use convert::json_to_pyobj;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use ahash::AHashSet;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::sync::{mpsc, oneshot};
use zeroize::Zeroizing;

use crate::jwt::{self, JwtConfig};
use crate::wse::server::state::DeduplicationState;
use crate::wse::server::{ServerCommand, SharedState, SharedStateConfig};

// ---------------------------------------------------------------------------
// PyO3 class
// ---------------------------------------------------------------------------

/// The server object is shared by every Python thread that uses it: a drain
/// loop sits in `drain_inbound()` while other threads broadcast, and `stop()`
/// comes from yet another one. So every method takes `&self` (`frozen`) and the
/// few fields that change do so behind their own synchronization. A `&mut self`
/// method would fail with "Already borrowed" whenever another thread is inside
/// the object - with the GIL too, since `drain_inbound()` releases it while it
/// waits.
#[pyclass(frozen)]
pub struct RustWSEServer {
    host: String,
    port: u16,
    shared: Arc<SharedState>,
    /// Command channel into the running server, `None` while stopped. Loaded on
    /// every send and broadcast, replaced only by `start()` and `stop()`.
    cmd_tx: arc_swap::ArcSwapOption<mpsc::UnboundedSender<ServerCommand>>,
    /// The server thread while one exists. Its lock also serializes `start()`
    /// and `stop()` against each other.
    lifecycle: std::sync::Mutex<Option<thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    dedup: Arc<std::sync::Mutex<DeduplicationState>>,
    rt_handle: parking_lot::RwLock<Option<tokio::runtime::Handle>>,
    started_at: parking_lot::Mutex<Option<std::time::Instant>>,
}

/// The command sender of the running server, loaded from `RustWSEServer::cmd_tx`
/// without touching its reference count.
struct CommandTx(arc_swap::Guard<Option<Arc<mpsc::UnboundedSender<ServerCommand>>>>);

impl std::ops::Deref for CommandTx {
    type Target = mpsc::UnboundedSender<ServerCommand>;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_deref()
            .expect("CommandTx is only built while a sender is stored")
    }
}

// Parameter groups for RustWSEServer::new() -- keeps the PyO3 #[new] shim clean
struct ServerParams {
    max_connections: usize,
    max_inbound_queue_size: usize,
    max_message_size: usize,
    ping_interval: u64,
    idle_timeout: u64,
    max_outbound_queue_bytes: usize,
    max_subscriptions_per_connection: usize,
    rate_limit_capacity: f64,
    rate_limit_refill: f64,
}

struct JwtParams {
    jwt_secret: Option<Vec<u8>>,
    jwt_issuer: Option<String>,
    jwt_audience: Option<String>,
    jwt_cookie_name: Option<String>,
    jwt_previous_secret: Option<Vec<u8>>,
    jwt_key_id: Option<String>,
    jwt_algorithm: Option<String>,
    jwt_private_key: Option<Vec<u8>>,
}

struct RecoveryParams {
    recovery_enabled: bool,
    recovery_buffer_size: usize,
    recovery_ttl: u64,
    recovery_max_messages: usize,
    recovery_memory_budget: usize,
}

struct PresenceParams {
    presence_enabled: bool,
    presence_max_data_size: usize,
    presence_max_members: usize,
}

impl RustWSEServer {
    fn build(
        host: String,
        port: u16,
        srv: ServerParams,
        jwt: JwtParams,
        rec: RecoveryParams,
        pres: PresenceParams,
    ) -> PyResult<Self> {
        if srv.ping_interval >= srv.idle_timeout {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "ping_interval ({}s) must be less than idle_timeout ({}s)",
                srv.ping_interval, srv.idle_timeout
            )));
        }
        let jwt_alg = match jwt.jwt_algorithm.as_deref() {
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
        if jwt.jwt_secret.is_some() {
            match jwt_alg {
                jwt::JwtAlgorithm::HS256 => {
                    if let Some(ref s) = jwt.jwt_secret
                        && s.len() < 32
                    {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "jwt_secret must be at least 32 bytes (RFC 7518 HS256 minimum)",
                        ));
                    }
                    if let Some(ref s) = jwt.jwt_previous_secret
                        && s.len() < 32
                    {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "jwt_previous_secret must be at least 32 bytes",
                        ));
                    }
                }
                jwt::JwtAlgorithm::RS256 => {
                    if let Some(ref s) = jwt.jwt_secret {
                        jsonwebtoken::DecodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 public key PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt.jwt_previous_secret {
                        jsonwebtoken::DecodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 jwt_previous_secret PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt.jwt_private_key {
                        jsonwebtoken::EncodingKey::from_rsa_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid RS256 jwt_private_key PEM: {e}"
                            ))
                        })?;
                    }
                }
                jwt::JwtAlgorithm::ES256 => {
                    if let Some(ref s) = jwt.jwt_secret {
                        jsonwebtoken::DecodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 public key PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt.jwt_previous_secret {
                        jsonwebtoken::DecodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 jwt_previous_secret PEM: {e}"
                            ))
                        })?;
                    }
                    if let Some(ref s) = jwt.jwt_private_key {
                        jsonwebtoken::EncodingKey::from_ec_pem(s).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "Invalid ES256 jwt_private_key PEM: {e}"
                            ))
                        })?;
                    }
                }
            }
        }
        drop(jwt.jwt_private_key);
        let jwt_config = jwt.jwt_secret.map(|secret| JwtConfig {
            algorithm: jwt_alg,
            secret: Zeroizing::new(secret),
            previous_secret: jwt.jwt_previous_secret.map(Zeroizing::new),
            key_id: jwt.jwt_key_id,
            issuer: jwt.jwt_issuer.unwrap_or_default(),
            audience: jwt.jwt_audience.unwrap_or_default(),
            cookie_name: jwt
                .jwt_cookie_name
                .unwrap_or_else(|| "access_token".to_string()),
        });
        let recovery = if rec.recovery_enabled {
            let clamped = rec.recovery_buffer_size.max(16);
            let bits = if clamped.is_power_of_two() {
                clamped.trailing_zeros()
            } else {
                clamped.next_power_of_two().trailing_zeros()
            };
            Some(Arc::new(crate::wse::recovery::RecoveryManager::new(
                crate::wse::recovery::RecoveryConfig {
                    buffer_size_bits: bits,
                    history_ttl_secs: rec.recovery_ttl,
                    max_recovery_messages: rec.recovery_max_messages,
                    global_memory_budget: rec.recovery_memory_budget,
                },
            )))
        } else {
            None
        };
        Ok(Self {
            host,
            port,
            shared: Arc::new(SharedState::new(SharedStateConfig {
                max_connections: srv.max_connections,
                jwt_config,
                max_inbound_queue_size: srv.max_inbound_queue_size,
                recovery,
                presence_enabled: pres.presence_enabled,
                presence_max_data_size: pres.presence_max_data_size,
                presence_max_members: pres.presence_max_members,
                rate_capacity: srv.rate_limit_capacity,
                rate_refill: srv.rate_limit_refill,
                max_message_size: srv.max_message_size,
                ping_interval_secs: srv.ping_interval,
                idle_timeout_secs: srv.idle_timeout,
                max_outbound_queue_bytes: srv.max_outbound_queue_bytes,
                max_subscriptions_per_connection: srv.max_subscriptions_per_connection,
            })),
            cmd_tx: arc_swap::ArcSwapOption::empty(),
            lifecycle: std::sync::Mutex::new(None),
            running: Arc::new(AtomicBool::new(false)),
            dedup: Arc::new(std::sync::Mutex::new(DeduplicationState {
                seen: AHashSet::with_capacity(50_000),
                queue: std::collections::VecDeque::with_capacity(50_000),
                max_entries: 50_000,
            })),
            rt_handle: parking_lot::RwLock::new(None),
            started_at: parking_lot::Mutex::new(None),
        })
    }

    /// The command channel of the running server, or `RuntimeError("Not running")`.
    fn command_tx(&self) -> PyResult<CommandTx> {
        let guard = self.cmd_tx.load();
        if guard.is_none() {
            return Err(PyRuntimeError::new_err("Not running"));
        }
        Ok(CommandTx(guard))
    }

    /// The tokio runtime of the server, once it has been started.
    fn runtime(&self) -> Option<tokio::runtime::Handle> {
        self.rt_handle.read().clone()
    }

    /// Seconds since the last `start()`, 0.0 before the first one.
    fn uptime_secs(&self) -> f64 {
        self.started_at
            .lock()
            .map_or(0.0, |s| s.elapsed().as_secs_f64())
    }
}

#[pymethods]
impl RustWSEServer {
    /// Create a new WSE server instance.
    ///
    /// The server runs all transport in Rust (TCP, TLS, WS framing, JWT auth,
    /// compression, encryption, ping/pong). Python handles application logic
    /// via drain mode or callbacks.
    #[new]
    #[pyo3(signature = (host, port, max_connections = 1000, jwt_secret = None, jwt_issuer = None, jwt_audience = None, jwt_cookie_name = None, jwt_previous_secret = None, jwt_key_id = None, jwt_algorithm = None, jwt_private_key = None, max_inbound_queue_size = 131072, recovery_enabled = false, recovery_buffer_size = 128, recovery_ttl = 300, recovery_max_messages = 500, recovery_memory_budget = 268435456, presence_enabled = false, presence_max_data_size = 4096, presence_max_members = 0, rate_limit_capacity = 100_000.0, rate_limit_refill = 10_000.0, max_message_size = 1_048_576, ping_interval = 25, idle_timeout = 60, max_outbound_queue_bytes = 16_777_216, max_subscriptions_per_connection = 0))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 __init__: 27 Python kwargs with defaults"
    )]
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
        max_subscriptions_per_connection: usize,
    ) -> PyResult<Self> {
        Self::build(
            host,
            port,
            ServerParams {
                max_connections,
                max_inbound_queue_size,
                max_message_size,
                ping_interval,
                idle_timeout,
                max_outbound_queue_bytes,
                max_subscriptions_per_connection,
                rate_limit_capacity,
                rate_limit_refill,
            },
            JwtParams {
                jwt_secret,
                jwt_issuer,
                jwt_audience,
                jwt_cookie_name,
                jwt_previous_secret,
                jwt_key_id,
                jwt_algorithm,
                jwt_private_key,
            },
            RecoveryParams {
                recovery_enabled,
                recovery_buffer_size,
                recovery_ttl,
                recovery_max_messages,
                recovery_memory_budget,
            },
            PresenceParams {
                presence_enabled,
                presence_max_data_size,
                presence_max_members,
            },
        )
    }

    /// Register Python callbacks for connection events.
    ///
    /// - on_connect(conn_id, cookies) -- called when a client connects
    /// - on_message(conn_id, data) -- called when a message is received
    /// - on_disconnect(conn_id) -- called when a client disconnects
    ///
    /// Alternative to drain mode. Cannot be used together with enable_drain_mode().
    fn set_callbacks(
        &self,
        py: Python<'_>,
        on_connect: Py<PyAny>,
        on_message: Py<PyAny>,
        on_disconnect: Py<PyAny>,
    ) -> PyResult<()> {
        self.set_callbacks_impl(py, on_connect, on_message, on_disconnect)
    }

    /// Start the WebSocket server on a background thread.
    ///
    /// Spawns a dedicated tokio runtime, binds to host:port, and begins
    /// accepting connections. Raises RuntimeError if already running.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        self.start_impl(py)
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
        self.drain_impl(close_code, close_reason, timeout)
    }

    /// Stop the server and close all connections.
    ///
    /// Shuts down the cluster mesh, sends a Shutdown command, and joins
    /// the server thread. Safe to call if not running (returns Ok).
    ///
    /// Callable from any thread, also while other threads are inside the
    /// server (for example a drain loop waiting in `drain_inbound()`).
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        self.stop_impl(py)
    }

    // -- send methods -------------------------------------------------------

    /// Send a raw text frame to a single connection.
    fn send(&self, conn_id: &str, data: &str) -> PyResult<()> {
        self.send_impl(conn_id, data)
    }

    /// Send a raw binary frame to a single connection.
    fn send_bytes(&self, conn_id: &str, data: &[u8]) -> PyResult<()> {
        self.send_bytes_impl(conn_id, data)
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
        self.send_event_impl(_py, conn_id, event, compression_threshold)
    }

    /// Send a text frame to all connected clients (no topic filtering).
    fn broadcast_all(&self, data: &str) -> PyResult<()> {
        self.broadcast_all_impl(data)
    }

    /// Send a binary frame to all connected clients.
    fn broadcast_all_bytes(&self, data: &[u8]) -> PyResult<()> {
        self.broadcast_all_bytes_impl(data)
    }

    // -- query / management -------------------------------------------------

    /// Number of active connections (lock-free AtomicUsize read).
    fn get_connection_count(&self) -> usize {
        self.shared.connection_count.load(Ordering::Relaxed)
    }

    /// List all active connection IDs.
    fn get_connections(&self) -> PyResult<Vec<String>> {
        let tx = self.command_tx()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(ServerCommand::GetConnections { reply: reply_tx })
            .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        reply_rx
            .blocking_recv()
            .map_err(|_| PyRuntimeError::new_err("Reply dropped"))
    }

    /// Force-disconnect a connection by sending a Close frame.
    fn disconnect(&self, conn_id: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::Disconnect {
            conn_id: conn_id.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Return True if the server is running.
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
    /// event_type: "connect" | "auth_connect" | "msg" | "raw" | "bin" |
    ///             "disconnect" | "presence_join" | "presence_leave"
    ///
    /// data: str (for "connect"=cookies, "auth_connect"=user_id, "raw"),
    ///       dict (for "msg", "presence_join", "presence_leave"),
    ///       bytes (for "bin"), None (for "disconnect")
    ///
    /// GIL is released while waiting on channel, acquired once for batch conversion.
    #[pyo3(signature = (max_count = 256, timeout_ms = 50))]
    fn drain_inbound(
        &self,
        py: Python<'_>,
        max_count: usize,
        timeout_ms: u64,
    ) -> PyResult<Py<PyList>> {
        self.drain_inbound_impl(py, max_count, timeout_ms)
    }

    /// Number of inbound events dropped due to queue overflow.
    fn inbound_dropped_count(&self) -> u64 {
        self.shared.inbound_dropped.load(Ordering::Relaxed)
    }

    /// Current number of events waiting in the inbound queue.
    fn inbound_queue_depth(&self) -> usize {
        self.shared.inbound_rx.len()
    }

    // -- Cluster Protocol -----------------------------------------------------

    /// Connect to cluster peers via TCP mesh protocol.
    ///
    /// Two modes: static peers (pass peer addresses directly) or seed-based
    /// discovery (pass seeds + cluster_addr for gossip). TLS is optional;
    /// falls back to WSE_CLUSTER_TLS_CERT/KEY/CA env vars if not provided.
    ///
    /// Raises RuntimeError if cluster is already connected or if seeds
    /// is used without cluster_addr.
    #[pyo3(signature = (peers, tls_cert=None, tls_key=None, tls_ca=None, cluster_port=None, seeds=None, cluster_addr=None))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 boundary: &self + 7 Python kwargs"
    )]
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
        self.connect_cluster_inner(
            peers,
            (tls_cert, tls_key, tls_ca),
            cluster_port,
            seeds,
            cluster_addr,
        )
    }

    /// Return True if at least one cluster peer is connected.
    fn cluster_connected(&self) -> bool {
        self.shared
            .cluster_metrics
            .connected_peers
            .load(Ordering::Relaxed)
            > 0
    }

    /// Number of connected cluster peers.
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
        self.reload_cluster_tls_impl(cert_path, key_path, ca_path)
    }

    /// List connected cluster peers with address, instance_id, capabilities,
    /// and connected_at timestamp. Returns a list of dicts.
    fn cluster_info(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.cluster_info_impl(py)
    }

    // -- Topic authorization --------------------------------------------------

    /// Set topic ACL for a connection programmatically (for non-JWT auth).
    ///
    /// - allow: list of glob patterns (empty = allow all)
    /// - deny: list of glob patterns (deny takes precedence over allow)
    #[pyo3(signature = (conn_id, allow = None, deny = None))]
    fn set_topic_acl(&self, conn_id: &str, allow: Option<Vec<String>>, deny: Option<Vec<String>>) {
        self.set_topic_acl_impl(conn_id, allow, deny)
    }

    // -- Topic subscriptions --------------------------------------------------

    /// Subscribe a connection to topics.
    ///
    /// Optionally set initial presence data (requires presence_enabled=True).
    /// If queue_group is set, the connection joins a named group for
    /// round-robin dispatch instead of standard fan-out.
    #[pyo3(signature = (conn_id, topics, presence_data = None, queue_group = None))]
    fn subscribe_connection(
        &self,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<()> {
        self.subscribe_connection_impl(conn_id, topics, presence_data, queue_group)
    }

    /// Subscribe with message recovery support.
    ///
    /// Returns a dict with per-topic recovery status:
    /// {recovered: bool, topics: {topic: {epoch, offset, recoverable, recovered, count}}}
    ///
    /// When recover=True, replays missed messages from the recovery buffer.
    /// Pass epoch (hex string) and offset from a previous session to resume.
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
        self.subscribe_with_recovery_impl(py, conn_id, topics, recover, epoch, offset)
    }

    /// Unsubscribe a connection from topics.
    /// If topics is None, unsubscribe from all topics.
    #[pyo3(signature = (conn_id, topics = None))]
    fn unsubscribe_connection(&self, conn_id: &str, topics: Option<Vec<String>>) -> PyResult<()> {
        self.unsubscribe_connection_impl(conn_id, topics)
    }

    /// Fan-out to topic subscribers locally.
    fn broadcast_local(&self, topic: &str, data: &str) -> PyResult<()> {
        self.broadcast_local_impl(topic, data)
    }

    /// Fan-out locally + cluster PUBLISH for multi-instance coordination.
    fn broadcast(&self, topic: &str, data: &str) -> PyResult<()> {
        self.broadcast_impl(topic, data)
    }

    /// Number of fan-out subscribers on a topic (excludes queue group members).
    fn get_topic_subscriber_count(&self, topic: &str) -> usize {
        self.shared
            .topic_subscribers
            .get(topic)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Get queue group info for a topic. Returns dict of group_name -> member_count.
    fn get_queue_group_info(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        self.get_queue_group_info_impl(py, topic)
    }

    /// Full server health snapshot as a dict.
    ///
    /// Keys: connections, inbound_queue_depth, inbound_dropped,
    /// cluster_connected, cluster_peer_count, cluster_messages_sent/delivered/dropped,
    /// cluster_bytes_sent/received, cluster_reconnect_count, cluster_dlq_size,
    /// uptime_secs, recovery_enabled/topic_count/total_bytes,
    /// presence_enabled/topics/total_users.
    fn health_snapshot(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        self.health_snapshot_impl(py)
    }

    /// Return Prometheus exposition format metrics string.
    ///
    /// Includes gauges (connections, queue depth, uptime, drops),
    /// per-topic counters (top 50 by volume), cluster metrics,
    /// and optional recovery/presence gauges.
    fn prometheus_metrics(&self) -> String {
        self.prometheus_metrics_impl()
    }

    /// Drain and return dead-letter-queue entries from failed cluster sends.
    ///
    /// Each entry is a dict with keys: topic, payload, peer_addr, error.
    /// Entries are consumed (not peeked) -- calling twice returns different results.
    fn get_cluster_dlq_entries(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.get_cluster_dlq_entries_impl(py)
    }

    /// Get all members currently present in a topic.
    /// Returns dict: {user_id: {"data": {...}, "connections": N}}
    fn presence(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        self.presence_impl(py, topic)
    }

    /// Get lightweight presence counters for a topic (O(1), no iteration).
    /// Returns dict: {"num_users": N, "num_connections": M}
    fn presence_stats(&self, py: Python<'_>, topic: &str) -> PyResult<Py<PyDict>> {
        self.presence_stats_impl(py, topic)
    }

    /// Update presence data for a connection across all its presence topics.
    #[pyo3(signature = (conn_id, data))]
    fn update_presence(&self, conn_id: &str, data: &Bound<'_, PyDict>) -> PyResult<()> {
        self.update_presence_impl(conn_id, data)
    }
}
