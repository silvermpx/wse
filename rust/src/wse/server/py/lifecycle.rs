//! Lifecycle of the server: callbacks, start, graceful drain, stop.
//!
//! `start_impl` also holds the server runtime: the accept loop, the recovery
//! cleanup, the presence TTL sweep and the ping / zombie / slow-consumer task.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use ahash::AHashSet;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::sync::RwLockExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::RustWSEServer;
use crate::wse::cluster::ClusterCommand;
use crate::wse::pylock::lock;
use crate::wse::server::commands::process_commands;
use crate::wse::server::connection::handle_connection;
use crate::wse::server::handshake::{HandshakeGuard, MAX_PENDING_HANDSHAKES};
use crate::wse::server::state::InboundEvent;
use crate::wse::server::{
    CLOSE_IDLE, CLOSE_TRY_AGAIN_LATER, ConnectionHandle, ServerCommand, WsFrame,
    format_presence_msg,
};

/// TCP keepalive of accepted connections: the first probe after a minute of
/// silence, then every 10 seconds.
fn tcp_keepalive() -> socket2::TcpKeepalive {
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    let keepalive = keepalive.with_interval(Duration::from_secs(10));
    keepalive
}

impl RustWSEServer {
    pub(super) fn set_callbacks_impl(
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
        // Waited for detached (`write_py_attached`), so a reader on another
        // thread is never kept from its attach. The callbacks being replaced
        // are dropped after the guards: that may run Python code.
        let shared = self.shared.clone();
        let old_connect = shared
            .on_connect
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_connect));
        let old_message = shared
            .on_message
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_message));
        let old_disconnect = shared
            .on_disconnect
            .write_py_attached(py)
            .unwrap()
            .replace(Arc::new(on_disconnect));
        drop((old_connect, old_message, old_disconnect));
        Ok(())
    }

    pub(super) fn start_impl(&self, py: Python<'_>) -> PyResult<()> {
        let mut server_thread = lock(py, &self.lifecycle);
        if server_thread
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            return Err(PyRuntimeError::new_err("Server is already running"));
        }
        // A server thread that ended by itself (it could not bind) is reaped
        // here; it has finished, so the join returns at once.
        if let Some(handle) = server_thread.take() {
            let _ = handle.join();
        }
        // The crate's log targets start with its library name, `_wse_accel`.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("_wse_accel=info")),
            )
            .with_target(false)
            .try_init();
        // The runtime and the listener are made here, so that a port in use
        // or a runtime that cannot start is an error of start().
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("wse-runtime")
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("cannot start the runtime: {e}")))?;
        let listener = std::net::TcpListener::bind((self.host.as_str(), self.port))
            .and_then(|listener| {
                listener.set_nonblocking(true)?;
                let _runtime = rt.enter();
                TcpListener::from_std(listener)
            })
            .map_err(|e| {
                pyo3::exceptions::PyOSError::new_err(format!(
                    "cannot listen on {}:{}: {e}",
                    self.host, self.port
                ))
            })?;
        tracing::info!("[WSE] Listening on {}:{}", self.host, self.port);
        // A drain of the last run is over.
        self.shared.draining.store(false, Ordering::SeqCst);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ServerCommand>();
        self.cmd_tx.store(Some(Arc::new(cmd_tx.clone())));
        // Store a clone for presence broadcasts from disconnect cleanup paths
        *self.shared.presence_broadcast_tx.write().unwrap() = Some(cmd_tx.clone());
        let running = self.running.clone();
        let shared = self.shared.clone();
        let rt_handle = rt.handle().clone();

        self.running.store(true, Ordering::SeqCst);
        let handle = thread::Builder::new()
            .name("wse-server".into())
            .spawn(move || {
                rt.block_on(async move {
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
                                                publish: false,
                                            },
                                        );
                                    }
                                    if let Some(ref ctx) = cluster_tx {
                                        let data_str = serde_json::to_string(
                                            &data,
                                        )
                                        .unwrap_or_default();
                                        let now =
                                            crate::wse::presence::epoch_ms();
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
                                let snapshot: Vec<(String, ConnectionHandle)> = {
                                    let conns = ping_state.connections.read().await;
                                    conns
                                        .iter()
                                        .map(|(cid, h)| (cid.clone(), h.clone()))
                                        .collect()
                                };
                                // Forget suspects that disconnected meanwhile.
                                if !suspected_slow.is_empty() {
                                    let live: AHashSet<&str> =
                                        snapshot.iter().map(|(cid, _)| cid.as_str()).collect();
                                    suspected_slow.retain(|id| live.contains(id.as_str()));
                                }
                                for (cid, handle) in &snapshot {
                                    // A slow consumer: its queue is still full on a
                                    // second tick. It gets a close frame; if it does
                                    // not read that either, the close grace drops it.
                                    if max_pending > 0
                                        && handle.pending.load(Ordering::Relaxed) >= max_pending
                                    {
                                        if suspected_slow.remove(cid) {
                                            handle.close(CLOSE_TRY_AGAIN_LATER, "too slow to read");
                                        } else {
                                            suspected_slow.insert(cid.clone());
                                        }
                                        continue;
                                    }
                                    suspected_slow.remove(cid);

                                    // Copied out: holding the map entry while the
                                    // connection's cleanup removes it would deadlock
                                    // on its shard lock.
                                    let last_seen = ping_state
                                        .conn_last_activity
                                        .get(cid)
                                        .map(|entry| *entry);
                                    let Some(last) = last_seen else {
                                        continue; // gone since the snapshot
                                    };
                                    if now.duration_since(last) > idle_timeout {
                                        handle.close(CLOSE_IDLE, "idle timeout");
                                        continue;
                                    }
                                    if handle.closer.is_closing() {
                                        continue;
                                    }
                                    let ts = chrono::Utc::now()
                                        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                                    let ping_msg = format!(
                                        "{{\"c\":\"WSE\",\"t\":\"ping\",\"p\":{{\"server_time\":\"{ts}\"}}}}"
                                    );
                                    let pm_bytes = ping_msg.len() + 4;
                                    handle.pending.fetch_add(pm_bytes, Ordering::Relaxed);
                                    if handle
                                        .tx
                                        .send(WsFrame::Msg(Message::Text(ping_msg.into())))
                                        .is_err()
                                    {
                                        handle.pending.fetch_sub(pm_bytes, Ordering::Relaxed);
                                    }
                                }
                            }
                        });
                    }
                    // Bounds the number of connections in the pre-handshake phase
                    // (slowloris): max_connections only counts registered ones.
                    let handshakes_in_flight = Arc::new(AtomicUsize::new(0));
                    let mut accept_pause = Duration::ZERO;
                    loop {
                        tokio::select! {
                            res = listener.accept() => {
                                match res {
                                    Ok((stream, addr)) => {
                                        accept_pause = Duration::ZERO;
                                        if handshakes_in_flight.load(Ordering::Relaxed)
                                            >= MAX_PENDING_HANDSHAKES
                                        {
                                            tracing::warn!(
                                                "[WSE] Pre-handshake cap reached ({MAX_PENDING_HANDSHAKES}), dropping {addr}"
                                            );
                                            continue; // drop the stream
                                        }
                                        let hs_guard =
                                            HandshakeGuard::new(handshakes_in_flight.clone());
                                        let _ = stream.set_nodelay(true);
                                        // Larger send buffer for broadcast throughput (default 16KB -> 256KB)
                                        let sock = socket2::SockRef::from(&stream);
                                        let _ = sock.set_send_buffer_size(262_144);
                                        // A peer that vanished without a FIN is found
                                        // by the kernel too, not only by idle_timeout.
                                        let _ = sock.set_tcp_keepalive(&tcp_keepalive());
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
                                                // Only in single-port mode, and then the
                                                // cluster manager takes the link over.
                                                drop(hs_guard);
                                                let main_port = shared2
                                                    .cluster_main_port
                                                    .read()
                                                    .unwrap()
                                                    .clone();
                                                match main_port {
                                                    Some(manager) => {
                                                        let _ = manager.send((stream, addr));
                                                    }
                                                    None => tracing::debug!(
                                                        "[WSE] Refused a cluster link on the WebSocket port from {addr}"
                                                    ),
                                                }
                                            } else {
                                                handle_connection(stream, addr, shared2, hs_guard).await;
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        if !running.load(Ordering::SeqCst) { break; }
                                        // Out of file descriptors, say: retrying at
                                        // once would spin.
                                        accept_pause = (accept_pause * 2)
                                            .clamp(Duration::from_millis(10), Duration::from_secs(1));
                                        tracing::error!("[WSE] Accept error: {e} (retrying in {accept_pause:?})");
                                        tokio::time::sleep(accept_pause).await;
                                        continue;
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
                // Callbacks still running get five seconds; a callback stuck
                // in slow I/O does not hold the shutdown forever.
                rt.shutdown_timeout(Duration::from_secs(5));
            })
            .map_err(|e| {
                self.running.store(false, Ordering::SeqCst);
                self.cmd_tx.store(None);
                PyRuntimeError::new_err(format!("Spawn error: {e}"))
            })?;
        *self.rt_handle.write() = Some(rt_handle);
        *self.started_at.lock() = Some(std::time::Instant::now());
        *server_thread = Some(handle);
        Ok(())
    }

    pub(super) fn drain_impl(
        &self,
        py: Python<'_>,
        close_code: u16,
        close_reason: &str,
        timeout: u64,
    ) -> PyResult<usize> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::Drain {
            close_code,
            close_reason: close_reason.to_owned(),
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        // Wait detached for the clients to go; the rest are closed at the
        // deadline.
        let shared = self.shared.clone();
        let remaining = py.detach(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(timeout);
            loop {
                let left = shared.connection_count.load(Ordering::Relaxed);
                if left == 0 || std::time::Instant::now() >= deadline {
                    return left;
                }
                thread::sleep(Duration::from_millis(50));
            }
        });
        if remaining > 0 {
            tracing::warn!("[WSE] Drain timeout: closing the {remaining} remaining connections");
            let _ = tx.send(ServerCommand::AbortAll);
        } else {
            tracing::info!("[WSE] Drain complete: all connections closed");
        }
        Ok(remaining)
    }

    /// Whether the server runs on the current thread: a callback, or code a
    /// callback runs.
    fn on_own_runtime(&self) -> bool {
        let own = self.runtime().map(|handle| handle.id());
        own.is_some() && tokio::runtime::Handle::try_current().ok().map(|h| h.id()) == own
    }

    pub(super) fn stop_impl(&self, py: Python<'_>) -> PyResult<()> {
        let mut server_thread = lock(py, &self.lifecycle);
        let Some(handle) = server_thread.take() else {
            return Ok(());
        };
        let on_own_runtime = self.on_own_runtime();
        // Shutdown cluster
        if let Ok(guard) = self.shared.cluster_cmd_tx.read()
            && let Some(ref tx) = *guard
        {
            let _ = tx.send(ClusterCommand::Shutdown);
        }
        if let Ok(tx) = self.command_tx() {
            let _ = tx.send(ServerCommand::Shutdown);
        }
        self.running.store(false, Ordering::SeqCst);
        // From a callback the server thread is not joined: it waits for its
        // callbacks, this one included. It stops by itself.
        let panicked = if on_own_runtime {
            drop(handle);
            false
        } else {
            // Detached while the server closes its connections (up to three
            // seconds): other Python threads keep running, and a start() from
            // another thread waits for the `lifecycle` lock without holding
            // up the interpreter.
            py.detach(move || handle.join().is_err())
        };
        self.cmd_tx.store(None);
        *self.shared.presence_broadcast_tx.write().unwrap() = None;
        *self.shared.cluster_cmd_tx.write().unwrap() = None;
        *self.shared.cluster_main_port.write().unwrap() = None;
        *self.shared.cluster_instance_id.lock().unwrap() = None;
        *self.rt_handle.write() = None;
        *self.started_at.lock() = None;
        drop(server_thread);
        if panicked {
            return Err(PyRuntimeError::new_err("Server thread panicked"));
        }
        Ok(())
    }
}
