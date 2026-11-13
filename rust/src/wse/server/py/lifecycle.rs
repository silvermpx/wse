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
use tokio::runtime::Runtime;
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
        // Server threads read these slots and then attach to Python while still
        // holding the read guard (to clone the callback). Waiting for the write
        // lock attached would block that attach - the GIL, or a stop-the-world
        // pause of the free-threaded build - and neither side could go on, so
        // the wait happens detached (`write_py_attached`). The callbacks being
        // replaced are dropped after the guards: that may run Python code.
        let shared = self.shared.clone();
        let old_connect = shared
            .on_connect
            .write_py_attached(py)
            .unwrap()
            .replace(on_connect);
        let old_message = shared
            .on_message
            .write_py_attached(py)
            .unwrap()
            .replace(on_message);
        let old_disconnect = shared
            .on_disconnect
            .write_py_attached(py)
            .unwrap()
            .replace(on_disconnect);
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
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("wse_accel=info")),
            )
            .with_target(false)
            .try_init();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ServerCommand>();
        self.cmd_tx.store(Some(Arc::new(cmd_tx.clone())));
        // Store a clone for presence broadcasts from disconnect cleanup paths
        *self.shared.presence_broadcast_tx.write().unwrap() = Some(cmd_tx.clone());
        let host = self.host.clone();
        let port = self.port;
        let running = self.running.clone();
        let shared = self.shared.clone();
        let (rt_handle_tx, rt_handle_rx) =
            std::sync::mpsc::sync_channel::<tokio::runtime::Handle>(1);

        // Running from here on, before the listener is bound: a drain loop
        // started right after start() (`while server.is_running(): ...`) must
        // not see a stale `false` and quit. The server thread clears the flag
        // again if it cannot create its runtime or bind the address.
        self.running.store(true, Ordering::SeqCst);
        let handle = thread::Builder::new()
            .name("wse-tokio-rt".into())
            .spawn(move || {
                let rt = match Runtime::new() {
                    Ok(rt) => rt,
                    Err(e) => {
                        tracing::error!("[WSE] Failed to create runtime: {e}");
                        running.store(false, Ordering::SeqCst);
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
                            running.store(false, Ordering::SeqCst);
                            return;
                        }
                    };
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
                    loop {
                        tokio::select! {
                            res = listener.accept() => {
                                match res {
                                    Ok((stream, addr)) => {
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
                                                // Reject cluster connections on main port:
                                                // - always when mTLS is configured (use dedicated cluster_port)
                                                // - always when TLS is NOT configured (unauthenticated injection risk)
                                                // - when cluster is not yet initialized
                                                let cluster_ready = shared2
                                                    .cluster_interest_tx
                                                    .read()
                                                    .unwrap()
                                                    .is_some();
                                                let tls_enabled = shared2.cluster_tls_enabled.load(Ordering::Relaxed);
                                                if tls_enabled || !cluster_ready {
                                                    if tls_enabled {
                                                        tracing::warn!(
                                                            "[WSE] Rejecting cluster connection on main port \
                                                             (mTLS required, use dedicated cluster_port): {addr}"
                                                        );
                                                    }
                                                    drop(stream);
                                                    return;
                                                }
                                                // Non-TLS cluster on main port: reject unless cluster_port
                                                // is explicitly not set (backward compat for single-port
                                                // deployments). Log a warning for security awareness.
                                                tracing::warn!(
                                                    "[WSE] Accepting plaintext cluster connection on main port \
                                                     from {addr} -- configure cluster TLS for production use"
                                                );
                                                let interest_tx = shared2
                                                    .cluster_interest_tx
                                                    .read()
                                                    .unwrap()
                                                    .clone();
                                                if let Some(interest_tx) = interest_tx {
                                                    crate::wse::cluster::handle_cluster_inbound(
                                                        stream, addr, &shared2, interest_tx,
                                                    )
                                                    .await;
                                                } else {
                                                    tracing::warn!(
                                                        "[WSE-Cluster] No interest channel for inbound {addr}"
                                                    );
                                                }
                                            } else {
                                                handle_connection(stream, addr, shared2, hs_guard).await;
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
            .map_err(|e| {
                self.running.store(false, Ordering::SeqCst);
                self.cmd_tx.store(None);
                PyRuntimeError::new_err(format!("Spawn error: {e}"))
            })?;
        // The runtime hands its handle over as soon as it exists; wait for it
        // detached, so other Python threads keep running meanwhile.
        let rt_handle = py.detach(move || rt_handle_rx.recv().ok());
        *self.rt_handle.write() = rt_handle;
        *self.started_at.lock() = Some(std::time::Instant::now());
        *server_thread = Some(handle);
        Ok(())
    }

    pub(super) fn drain_impl(
        &self,
        close_code: u16,
        close_reason: &str,
        timeout: u64,
    ) -> PyResult<()> {
        let tx = self.command_tx()?;
        tx.send(ServerCommand::Drain {
            close_code,
            close_reason: close_reason.to_owned(),
            timeout_secs: timeout,
        })
        .map_err(|_| PyRuntimeError::new_err("Channel closed"))?;
        Ok(())
    }

    pub(super) fn stop_impl(&self, py: Python<'_>) -> PyResult<()> {
        let mut server_thread = lock(py, &self.lifecycle);
        let Some(handle) = server_thread.take() else {
            return Ok(());
        };
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
        // Detached while the server drains (about two seconds): other Python
        // threads keep running, and a start() from another thread waits for
        // the `lifecycle` lock without holding up the interpreter.
        let panicked = py.detach(move || handle.join().is_err());
        self.cmd_tx.store(None);
        *self.shared.cluster_cmd_tx.write().unwrap() = None;
        drop(server_thread);
        if panicked {
            return Err(PyRuntimeError::new_err("Server thread panicked"));
        }
        Ok(())
    }
}
