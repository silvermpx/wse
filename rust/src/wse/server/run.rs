//! A running server: its delivery lanes, its command task, its periodic
//! tasks and the accept loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::AHashSet;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::commands::process_commands;
use super::connection::handle_connection;
use super::handler::Handler;
use super::handshake::{HANDSHAKE_TIMEOUT, HandshakeGuard, MAX_PENDING_HANDSHAKES};
use super::{
    CLOSE_IDLE, CLOSE_TRY_AGAIN_LATER, Commands, ConnectionHandle, Fanout, ServerCommand,
    SharedState,
};

/// TCP keepalive of accepted connections: the first probe after a minute of
/// silence, then every 10 seconds.
fn tcp_keepalive() -> socket2::TcpKeepalive {
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    let keepalive = keepalive.with_interval(Duration::from_secs(10));
    keepalive
}

/// What a server's start makes before it serves: the command queue, and a
/// delivery task for each lane, running before the first command.
pub(crate) struct Started {
    pub(crate) publisher: super::Publisher,
    commands_rx: mpsc::UnboundedReceiver<ServerCommand>,
    fanout: Fanout,
}

/// Start `shared`'s server on the current runtime: its lanes run from here
/// on, its commands queue until `serve` takes them.
pub(crate) fn start<H: Handler>(shared: &Arc<SharedState>, handler: &Arc<H>) -> Started {
    // A drain of the last run is over.
    shared.draining.store(false, Ordering::SeqCst);
    let fanout = Fanout::start(
        shared.topics.clone(),
        shared.max_outbound_queue_bytes,
        Arc::clone(&shared.slow_consumer_drops),
    );
    *shared.fanout.write().unwrap() = Some(fanout.clone());
    let (tx, commands_rx) = mpsc::unbounded_channel::<ServerCommand>();
    shared.commands_queued.store(0, Ordering::Relaxed);
    let commands = Commands::new(tx, Arc::clone(&shared.commands_queued));
    // The handler hears of presence changes.
    let presence_handler = Arc::clone(handler);
    shared
        .presence_events
        .store(Some(Arc::new(Box::new(move |event| {
            presence_handler.presence(event)
        }))));
    // For presence broadcasts from the disconnect cleanup paths.
    shared
        .presence_broadcast_tx
        .store(Some(Arc::new(commands.clone())));
    Started {
        publisher: super::Publisher::new(Arc::clone(shared), commands),
        commands_rx,
        fanout,
    }
}

/// Serve on `listener` until `running` is false: the command task, the
/// periodic tasks (recovery cleanup, topic counter sweep, signed-message
/// window, presence TTL sweep, ping / idle / slow-consumer check) and the
/// accept loop. Returns once the command task has ended (`Shutdown`).
pub(crate) async fn serve<H: Handler>(
    shared: Arc<SharedState>,
    started: Started,
    listener: TcpListener,
    handler: Arc<H>,
    running: Arc<AtomicBool>,
) {
    let Started {
        commands_rx: cmd_rx,
        fanout,
        ..
    } = started;
    let cmd_handle = tokio::spawn(process_commands(cmd_rx, shared.clone(), fanout));
    // Spawn periodic recovery cleanup if recovery is enabled
    if let Some(ref recovery) = shared.recovery {
        let rm = recovery.clone();
        let cleanup_state = shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                rm.cleanup();
                cleanup_state.sweep_interest_linger();
            }
        });
    }
    // Per-topic delivery counters of topics nobody subscribes
    // to any more: dropped once there are many (user- or
    // UUID-scoped topics), whether or not metrics are read.
    {
        let counts_state = shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let counts = &counts_state.topic_message_counts;
                if counts.len() > 10_000 {
                    counts.retain(|topic, _| counts_state.topics.subscribers.contains_key(topic));
                }
            }
        });
    }
    // Signed messages: a (kid, n) is forgotten once its time
    // is out of the window, as it could not be taken again.
    if shared.signer.is_some() {
        let seen_state = shared.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let now = crate::wse::server::signing::now_ms();
                seen_state.signed_seen.retain(|_, until| *until >= now);
            }
        });
    }
    // Presence TTL sweep (every 30 seconds)
    if let Some(ref pm) = shared.presence {
        let sweep_pm = pm.clone();
        let sweep_state = shared.clone();
        let sweep_running = running.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if !sweep_running.load(Ordering::Relaxed) {
                    break;
                }
                let removed = sweep_pm.sweep_dead_connections(|conn_id| {
                    if conn_id.starts_with("__remote__") {
                        return true;
                    }
                    sweep_state.conn_last_activity.contains_key(conn_id)
                });
                if removed.is_empty() {
                    continue;
                }
                for (topic, user_id, data) in removed {
                    sweep_state.announce_presence_leave(topic, user_id, data, true);
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
                    if max_pending > 0 && handle.out.pending.load(Ordering::Relaxed) >= max_pending
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
                        .map(|entry| entry.load(Ordering::Relaxed));
                    let Some(last) = last_seen else {
                        continue; // gone since the snapshot
                    };
                    let idle_ms = ping_state.activity_ms(now).saturating_sub(last);
                    if u128::from(idle_ms) > idle_timeout.as_millis() {
                        handle.close(CLOSE_IDLE, "idle timeout");
                        continue;
                    }
                    if handle.closer.is_closing() {
                        continue;
                    }
                    let ts =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
                    let ping_msg = format!(
                        "{{\"c\":\"WSE\",\"t\":\"ping\",\"p\":{{\"server_time\":\"{ts}\"}}}}"
                    );
                    // Sealed in an E2E session.
                    handle.send_control(Message::Text(ping_msg.into()));
                }
            }
        });
    }
    // Bounds the number of connections in the pre-handshake phase
    // (slowloris): max_connections only counts registered ones.
    let handshakes_in_flight = Arc::new(AtomicUsize::new(0));
    let mut refused_handshakes: u64 = 0;
    let mut last_refused_log = tokio::time::Instant::now() - Duration::from_secs(1);
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
                            // Logged once a second at most: a flood
                            // of sockets is not a flood of lines.
                            refused_handshakes += 1;
                            if last_refused_log.elapsed() >= Duration::from_secs(1) {
                                tracing::warn!(
                                    "[WSE] Pre-handshake cap reached ({MAX_PENDING_HANDSHAKES}), dropped {refused_handshakes} socket(s), the last from {addr}"
                                );
                                refused_handshakes = 0;
                                last_refused_log = tokio::time::Instant::now();
                            }
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
                        let handler = Arc::clone(&handler);
                        // Cluster links come on this port only in
                        // single-port mode: the peek is for that.
                        let single_port =
                            shared.cluster_main_port.read().unwrap().is_some();
                        tokio::spawn(async move {
                            // Protocol detection: peek first 4 bytes to distinguish
                            // cluster binary frames (4-byte big-endian length prefix:
                            // 0x00 0x00 0x00 XX for frames < 256 bytes) from HTTP/WS
                            // (first byte is ASCII letter like 'G' for GET) or TLS
                            // (first byte 0x16 for ClientHello).
                            let mut peek = [0u8; 4];
                            let is_cluster = single_port && match tokio::time::timeout(
                                HANDSHAKE_TIMEOUT,
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
                                handle_connection(stream, addr, shared2, hs_guard, handler).await;
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
}

/// What a stopped server leaves: no lanes, no command queue, no cluster.
pub(crate) fn stopped(shared: &SharedState) {
    shared.presence_broadcast_tx.store(None);
    shared.presence_events.store(None);
    *shared.fanout.write().unwrap() = None;
    shared.cluster_cmd_tx.store(None);
    *shared.cluster_main_port.write().unwrap() = None;
    *shared.cluster_instance_id.lock().unwrap() = None;
}
