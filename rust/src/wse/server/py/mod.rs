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
use zeroize::Zeroizing;

use crate::jwt::{self, JwtConfig};
use crate::wse::server::state::DeduplicationState;
use crate::wse::server::{Commands, ServerCommand, SharedState, SharedStateConfig};

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
    cmd_tx: arc_swap::ArcSwapOption<Commands>,
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
struct CommandTx(arc_swap::Guard<Option<Arc<Commands>>>);

impl std::ops::Deref for CommandTx {
    type Target = Commands;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_deref()
            .expect("CommandTx is only built while a sender is stored")
    }
}

impl Drop for RustWSEServer {
    /// Dropped while running, without stop(): the server is told to stop and
    /// its thread ends by itself; nothing waits for it here.
    fn drop(&mut self) {
        if self.running.swap(false, Ordering::SeqCst) {
            if let Some(tx) = self.shared.cluster_cmd_tx.load_full() {
                let _ = tx.send(crate::wse::cluster::ClusterCommand::Shutdown);
            }
            if let Some(tx) = self.cmd_tx.load_full() {
                let _ = tx.send(ServerCommand::Shutdown);
            }
        }
    }
}

// Parameter groups for RustWSEServer::new() -- keeps the PyO3 #[new] shim clean
struct ServerParams {
    max_connections: usize,
    max_inbound_queue_size: usize,
    max_inbound_queue_bytes: usize,
    allowed_origins: Option<Vec<String>>,
    client_subscriptions: bool,
    e2e_identity_key: Option<Vec<u8>>,
    e2e_legacy: bool,
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
        let at_least_one = [
            ("max_connections", srv.max_connections),
            ("max_inbound_queue_size", srv.max_inbound_queue_size),
            ("max_inbound_queue_bytes", srv.max_inbound_queue_bytes),
            ("max_message_size", srv.max_message_size),
        ];
        for (name, value) in at_least_one {
            if value == 0 {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{name} must be at least 1"
                )));
            }
        }
        if srv.ping_interval == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "ping_interval must be at least 1 second",
            ));
        }
        // NaN is refused; infinity means no limit.
        if srv.rate_limit_capacity.is_nan() || srv.rate_limit_capacity < 1.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "rate_limit_capacity must be at least 1, got {}",
                srv.rate_limit_capacity
            )));
        }
        if srv.rate_limit_refill.is_nan() || srv.rate_limit_refill <= 0.0 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "rate_limit_refill must be above 0, got {}",
                srv.rate_limit_refill
            )));
        }
        let allowed_origins = srv
            .allowed_origins
            .map(|origins| {
                origins
                    .iter()
                    .map(|origin| crate::wse::server::normalize_origin(origin))
                    .collect::<Result<AHashSet<String>, String>>()
            })
            .transpose()
            .map_err(pyo3::exceptions::PyValueError::new_err)?;
        let e2e_identity = srv
            .e2e_identity_key
            .as_deref()
            .map(crate::crypto::IdentityKey::from_pkcs8)
            .transpose()
            .map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(
                    "e2e_identity_key must be an ECDSA P-256 private key in PKCS#8 (PEM or DER)",
                )
            })?;
        if srv.ping_interval >= srv.idle_timeout {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "ping_interval ({}s) must be less than idle_timeout ({}s)",
                srv.ping_interval, srv.idle_timeout
            )));
        }
        // Every jwt_* option needs jwt_secret: without it the server runs with
        // no authentication, and an issuer or audience set alone would only
        // look like protection.
        if jwt.jwt_secret.is_none() {
            let set: Vec<&str> = [
                ("jwt_issuer", jwt.jwt_issuer.is_some()),
                ("jwt_audience", jwt.jwt_audience.is_some()),
                ("jwt_previous_secret", jwt.jwt_previous_secret.is_some()),
                ("jwt_key_id", jwt.jwt_key_id.is_some()),
                ("jwt_algorithm", jwt.jwt_algorithm.is_some()),
                ("jwt_private_key", jwt.jwt_private_key.is_some()),
                ("jwt_cookie_name", jwt.jwt_cookie_name.is_some()),
            ]
            .into_iter()
            .filter_map(|(name, is_set)| is_set.then_some(name))
            .collect();
            if !set.is_empty() {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{} set without jwt_secret: the server would accept every connection \
                     unauthenticated. Pass jwt_secret (the HMAC secret, or the public key \
                     for the asymmetric algorithms) or drop these options.",
                    set.join(", ")
                )));
            }
        }
        let jwt_alg = match jwt.jwt_algorithm.as_deref() {
            None => jwt::JwtAlgorithm::HS256,
            Some(name) => jwt::JwtAlgorithm::from_str(name).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Unsupported jwt_algorithm: {name}. Supported: {}",
                    jwt::JwtAlgorithm::supported()
                ))
            })?,
        };
        // Keys are checked against the algorithm at startup, not at the first
        // connection.
        let key_error = |option: &str, e: jwt::JwtError| {
            pyo3::exceptions::PyValueError::new_err(format!("{option} for {jwt_alg:?}: {e}"))
        };
        if let Some(ref key) = jwt.jwt_secret {
            jwt::check_verification_key(jwt_alg, key).map_err(|e| key_error("jwt_secret", e))?;
        }
        if let Some(ref key) = jwt.jwt_previous_secret {
            jwt::check_verification_key(jwt_alg, key)
                .map_err(|e| key_error("jwt_previous_secret", e))?;
        }
        if let Some(ref key) = jwt.jwt_private_key {
            jwt::check_signing_key(jwt_alg, key).map_err(|e| key_error("jwt_private_key", e))?;
        }
        let jwt_config = jwt.jwt_secret.map(|secret| JwtConfig {
            algorithm: jwt_alg,
            secret: Zeroizing::new(secret),
            previous_secret: jwt.jwt_previous_secret.map(Zeroizing::new),
            signing_key: jwt.jwt_private_key.map(Zeroizing::new),
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
                max_inbound_queue_bytes: srv.max_inbound_queue_bytes,
                allowed_origins,
                client_subscriptions: srv.client_subscriptions,
                e2e_identity,
                e2e_legacy: srv.e2e_legacy,
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

    /// The handle of a registered connection. Looked up detached: the lookup
    /// waits on the runtime, whose threads may need this thread's interpreter
    /// meanwhile (a callback being handed over, a stop-the-world pause).
    fn connection_handle(
        &self,
        py: Python<'_>,
        conn_id: &str,
    ) -> Option<crate::wse::server::ConnectionHandle> {
        let rt = self.runtime()?;
        let shared = self.shared.clone();
        py.detach(move || {
            rt.block_on(async { shared.connections.read().await.get(conn_id).cloned() })
        })
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
    #[pyo3(signature = (host, port, max_connections = 1000, jwt_secret = None, jwt_issuer = None, jwt_audience = None, jwt_cookie_name = None, jwt_previous_secret = None, jwt_key_id = None, jwt_algorithm = None, jwt_private_key = None, max_inbound_queue_size = 131072, recovery_enabled = false, recovery_buffer_size = 128, recovery_ttl = 300, recovery_max_messages = 500, recovery_memory_budget = 268435456, presence_enabled = false, presence_max_data_size = 4096, presence_max_members = 0, rate_limit_capacity = 100_000.0, rate_limit_refill = 10_000.0, max_message_size = 1_048_576, ping_interval = 25, idle_timeout = 60, max_outbound_queue_bytes = 16_777_216, max_subscriptions_per_connection = 0, max_inbound_queue_bytes = 67_108_864, allowed_origins = None, client_subscriptions = false, e2e_identity_key = None, e2e_legacy = true))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 __init__: 32 Python kwargs with defaults"
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
        max_inbound_queue_bytes: usize,
        allowed_origins: Option<Vec<String>>,
        client_subscriptions: bool,
        e2e_identity_key: Option<Vec<u8>>,
        e2e_legacy: bool,
    ) -> PyResult<Self> {
        Self::build(
            host,
            port,
            ServerParams {
                max_connections,
                max_inbound_queue_size,
                max_inbound_queue_bytes,
                allowed_origins,
                client_subscriptions,
                e2e_identity_key,
                e2e_legacy,
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
    /// - on_connect(conn_id, user_id) -- called when a client connects; the
    ///   user id the server validated, or without jwt_secret the Cookie header
    /// - on_message(conn_id, data) -- called when a message is received
    /// - on_disconnect(conn_id) -- called when a client disconnects
    ///
    /// The callbacks of one connection run one after another in the order of
    /// its events. Alternative to drain mode. Cannot be used together with
    /// enable_drain_mode().
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
    /// Binds to host:port, spawns a dedicated tokio runtime and begins
    /// accepting connections. Raises RuntimeError if already running and
    /// OSError if host:port cannot be bound.
    fn start(&self, py: Python<'_>) -> PyResult<()> {
        self.start_impl(py)
    }

    /// Enter graceful drain mode (lame duck).
    ///
    /// 1. Stops accepting new connections (upgrades get HTTP 503)
    /// 2. Notifies cluster peers via DRAIN frame
    /// 3. Closes every client with `close_code`; a client that does not
    ///    answer within 5 seconds is disconnected
    /// 4. Waits up to `timeout` seconds for clients to disconnect, then
    ///    drops the rest
    ///
    /// Returns how many connections had to be dropped at the timeout. Call
    /// `stop()` after `drain()` to fully shut down.
    #[pyo3(signature = (close_code = 4300, close_reason = "", timeout = 30))]
    fn drain(
        &self,
        py: Python<'_>,
        close_code: u16,
        close_reason: &str,
        timeout: u64,
    ) -> PyResult<usize> {
        self.drain_impl(py, close_code, close_reason, timeout)
    }

    /// True between drain() and the next start().
    fn is_draining(&self) -> bool {
        self.shared.draining.load(Ordering::Relaxed)
    }

    /// `with RustWSEServer(...) as server:` starts the server (unless it
    /// runs already) and stops it at the end of the block.
    fn __enter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        if !slf.running.load(Ordering::SeqCst) {
            slf.start_impl(py)?;
        }
        Ok(slf)
    }

    /// Stops the server; an exception in the block is not swallowed.
    #[pyo3(signature = (exc_type = None, exc = None, traceback = None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc, traceback);
        self.stop_impl(py)?;
        Ok(false)
    }

    /// Stop the server and close all connections (close code 1001).
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
    fn send(&self, py: Python<'_>, conn_id: &str, data: &str) -> PyResult<()> {
        self.send_impl(py, conn_id, data)
    }

    /// Send a raw binary frame to a single connection.
    fn send_bytes(&self, py: Python<'_>, conn_id: &str, data: &[u8]) -> PyResult<()> {
        self.send_bytes_impl(py, conn_id, data)
    }

    /// Full outbound pipeline in Rust: PyDict -> envelope -> JSON -> compress -> send.
    /// Returns the size of the frame handed to the connection, 0 for an event
    /// id this connection already got. A connection whose outbound queue is
    /// over `max_outbound_queue_bytes` drops it.
    #[pyo3(signature = (conn_id, event, compression_threshold = 1024))]
    fn send_event(
        &self,
        py: Python<'_>,
        conn_id: &str,
        event: &Bound<'_, PyDict>,
        compression_threshold: usize,
    ) -> PyResult<usize> {
        self.send_event_impl(py, conn_id, event, compression_threshold)
    }

    /// Send a text frame to all connected clients (no topic filtering).
    fn broadcast_all(&self, py: Python<'_>, data: &str) -> PyResult<()> {
        self.broadcast_all_impl(py, data)
    }

    /// Send a binary frame to all connected clients.
    fn broadcast_all_bytes(&self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        self.broadcast_all_bytes_impl(py, data)
    }

    // -- query / management -------------------------------------------------

    /// Number of active connections (lock-free AtomicUsize read).
    fn get_connection_count(&self) -> usize {
        self.shared.connection_count.load(Ordering::Relaxed)
    }

    /// List all active connection IDs, read without waiting for the server.
    fn get_connections(&self) -> Vec<String> {
        // An activity entry lives from a connection's registration to its
        // cleanup.
        self.shared
            .conn_last_activity
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Close a connection with code 1000. Its messages after that are not
    /// delivered, and a client that does not answer within 5 seconds is
    /// disconnected.
    fn disconnect(&self, conn_id: &str) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::Disconnect {
            conn_id: conn_id.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    /// Accept a connection the application authenticated itself (a server
    /// without `jwt_secret`): it gets server_ready, the message the clients
    /// wait for before they send anything, and `user_id` becomes its user
    /// for presence. Call it once per connection, after checking the
    /// cookies of its "connect" event. Returns False when the connection is
    /// gone. A server that validates tokens itself sends server_ready on its
    /// own and raises RuntimeError here.
    fn accept_connection(&self, conn_id: &str, user_id: &str) -> PyResult<bool> {
        self.accept_connection_impl(conn_id, user_id)
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

    /// Client messages dropped over `max_inbound_queue_size` or
    /// `max_inbound_queue_bytes`. Connect, disconnect and presence events are
    /// never dropped.
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
    /// Peers' links come in on `cluster_port` (bound on `cluster_host`,
    /// 0.0.0.0 by default), or with `single_port=True` on the WebSocket port:
    /// plaintext and unauthenticated, for a port only the cluster's own
    /// network reaches. One of the two is required.
    ///
    /// Raises RuntimeError if cluster is already connected or if seeds
    /// is used without cluster_addr, ValueError for a missing or
    /// contradictory listening option, OSError if `cluster_port` cannot be
    /// bound.
    #[pyo3(signature = (peers, tls_cert=None, tls_key=None, tls_ca=None, cluster_port=None, seeds=None, cluster_addr=None, cluster_host=None, single_port=false))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 boundary: &self + 9 Python kwargs"
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
        cluster_host: Option<String>,
        single_port: bool,
    ) -> PyResult<()> {
        self.connect_cluster_inner(
            peers,
            (tls_cert, tls_key, tls_ca),
            (cluster_port, cluster_host, single_port),
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

    /// Sign a token for user `sub` with this server's configuration: its
    /// algorithm and key (the HMAC secret, or `jwt_private_key` for the
    /// asymmetric algorithms), `jwt_key_id`, and its issuer and audience when
    /// set. Valid for `ttl` seconds; `claims` adds or overrides others.
    /// Raises RuntimeError on a server without jwt_secret, or with an
    /// asymmetric algorithm but no jwt_private_key.
    #[pyo3(signature = (sub, ttl = 900, claims = None))]
    fn issue_token(
        &self,
        sub: &str,
        ttl: u64,
        claims: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let holder = self.shared.jwt_config.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("this server does not validate tokens (no jwt_secret)")
        })?;
        let cfg = holder.load();
        let key: &[u8] = match cfg.algorithm.key_kind() {
            jwt::KeyKind::Secret { .. } => &cfg.secret,
            _ => cfg.signing_key.as_deref().ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "{:?} signs with jwt_private_key, which this server was not given",
                    cfg.algorithm
                ))
            })?,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut token = serde_json::Map::new();
        token.insert("iat".into(), now.into());
        token.insert("exp".into(), now.saturating_add(ttl).into());
        if !cfg.issuer.is_empty() {
            token.insert("iss".into(), cfg.issuer.clone().into());
        }
        if !cfg.audience.is_empty() {
            token.insert("aud".into(), cfg.audience.clone().into());
        }
        if let Some(claims) = claims
            && let serde_json::Value::Object(extra) = jwt::pydict_to_json(claims)?
        {
            token.extend(extra);
        }
        token.insert("sub".into(), sub.into());
        jwt::jwt_encode(
            &serde_json::Value::Object(token),
            key,
            cfg.algorithm,
            cfg.key_id.as_deref(),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("JWT encode error: {e}")))
    }

    /// The public key clients pin for end-to-end encryption: base64 of the
    /// uncompressed P-256 point of `e2e_identity_key`, None without one.
    fn e2e_identity_public_key(&self) -> Option<String> {
        use base64::Engine;
        self.shared
            .e2e_identity
            .as_ref()
            .map(|k| base64::engine::general_purpose::STANDARD.encode(k.public_key()))
    }

    /// Replace the JWT verification keys without a restart, for a key
    /// rotation: `jwt_secret` (the HMAC secret, or the public key of the
    /// asymmetric algorithms) and optionally `jwt_previous_secret`, accepted
    /// too while tokens signed with it are still out. The algorithm, issuer,
    /// audience and cookie name stay. Open connections are not affected; new
    /// ones are checked with the new keys. Raises RuntimeError on a server
    /// without jwt_secret, ValueError for a key that does not fit.
    #[pyo3(signature = (jwt_secret, jwt_previous_secret = None, jwt_key_id = None, jwt_private_key = None))]
    fn reload_jwt_keys(
        &self,
        jwt_secret: Vec<u8>,
        jwt_previous_secret: Option<Vec<u8>>,
        jwt_key_id: Option<String>,
        jwt_private_key: Option<Vec<u8>>,
    ) -> PyResult<()> {
        let holder = self.shared.jwt_config.as_ref().ok_or_else(|| {
            PyRuntimeError::new_err("this server does not validate tokens (no jwt_secret)")
        })?;
        let current = holder.load();
        let alg = current.algorithm;
        let key_error = |option: &str, e: jwt::JwtError| {
            pyo3::exceptions::PyValueError::new_err(format!("{option} for {alg:?}: {e}"))
        };
        jwt::check_verification_key(alg, &jwt_secret).map_err(|e| key_error("jwt_secret", e))?;
        if let Some(ref key) = jwt_previous_secret {
            jwt::check_verification_key(alg, key)
                .map_err(|e| key_error("jwt_previous_secret", e))?;
        }
        if let Some(ref key) = jwt_private_key {
            jwt::check_signing_key(alg, key).map_err(|e| key_error("jwt_private_key", e))?;
        }
        holder.store(Arc::new(JwtConfig {
            algorithm: alg,
            secret: Zeroizing::new(jwt_secret),
            previous_secret: jwt_previous_secret.map(Zeroizing::new),
            signing_key: jwt_private_key
                .map(Zeroizing::new)
                .or_else(|| current.signing_key.clone()),
            key_id: jwt_key_id.or_else(|| current.key_id.clone()),
            issuer: current.issuer.clone(),
            audience: current.audience.clone(),
            cookie_name: current.cookie_name.clone(),
        }));
        tracing::info!("[WSE] JWT keys reloaded");
        Ok(())
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
    ///
    /// Returns False (and stores nothing) when the connection is gone.
    #[pyo3(signature = (conn_id, allow = None, deny = None))]
    fn set_topic_acl(
        &self,
        conn_id: &str,
        allow: Option<Vec<String>>,
        deny: Option<Vec<String>>,
    ) -> bool {
        self.set_topic_acl_impl(conn_id, allow, deny)
    }

    // -- Topic subscriptions --------------------------------------------------

    /// Subscribe a connection to topics.
    ///
    /// Optionally set initial presence data (requires presence_enabled=True).
    /// If queue_group is set, the connection joins a named group for
    /// round-robin dispatch instead of standard fan-out.
    ///
    /// Returns the topics it took: not those its topic ACL denies, invalid
    /// names (empty, over 256 bytes, control characters), patterns past 16
    /// per connection, or what exceeds max_subscriptions_per_connection.
    #[pyo3(signature = (conn_id, topics, presence_data = None, queue_group = None))]
    fn subscribe_connection(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        presence_data: Option<&Bound<'_, PyDict>>,
        queue_group: Option<String>,
    ) -> PyResult<Vec<String>> {
        self.subscribe_connection_impl(py, conn_id, topics, presence_data, queue_group)
    }

    /// Subscribe with message recovery support.
    ///
    /// Returns a dict with per-topic recovery status:
    /// {recovered: bool, topics: {topic: {epoch, offset, recoverable, recovered,
    /// count, subscribed}}}
    ///
    /// When recover=True, replays missed messages from the recovery buffer,
    /// before anything sent to the connection afterwards. `positions` maps
    /// each topic to its own {"epoch": "<8 hex>", "offset": n}, the shape the
    /// clients send in `p.recovery`; a single `epoch` and `offset` apply to
    /// every topic. `offset` in the result is the last offset the client has
    /// (or may take as had): when nothing was recovered, everything after it
    /// reaches the connection live. A replay cut short by the outbound budget
    /// reports recovered=False and the last offset it queued.
    #[pyo3(signature = (conn_id, topics, recover = false, epoch = None, offset = None, positions = None))]
    #[expect(
        clippy::too_many_arguments,
        reason = "PyO3 boundary: &self + 7 Python arguments"
    )]
    fn subscribe_with_recovery(
        &self,
        py: Python<'_>,
        conn_id: &str,
        topics: Vec<String>,
        recover: bool,
        epoch: Option<String>,
        offset: Option<u64>,
        positions: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyDict>> {
        self.subscribe_with_recovery_impl(py, conn_id, topics, recover, epoch, offset, positions)
    }

    /// Unsubscribe a connection from topics.
    /// If topics is None, unsubscribe from all topics.
    #[pyo3(signature = (conn_id, topics = None))]
    fn unsubscribe_connection(&self, conn_id: &str, topics: Option<Vec<String>>) -> PyResult<()> {
        self.unsubscribe_connection_impl(conn_id, topics)
    }

    /// Fan-out to topic subscribers locally.
    fn broadcast_local(&self, py: Python<'_>, topic: &str, data: &str) -> PyResult<()> {
        self.broadcast_local_impl(py, topic, data)
    }

    /// Fan-out locally + cluster PUBLISH for multi-instance coordination.
    fn broadcast(&self, py: Python<'_>, topic: &str, data: &str) -> PyResult<()> {
        self.broadcast_impl(py, topic, data)
    }

    /// Number of fan-out subscribers on a topic (excludes queue group members).
    fn get_topic_subscriber_count(&self, topic: &str) -> usize {
        self.shared
            .topics
            .subscribers
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
