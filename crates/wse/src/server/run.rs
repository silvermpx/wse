//! A running server: its delivery lanes, its command task, its periodic
//! tasks and the accept loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use ahash::AHashSet;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::commands::process_commands;
use super::connection::handle_connection;
use super::handler::{Handler, guarded};
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
    commands_rx: mpsc::UnboundedReceiver<ServerCommand>,
    fanout: Fanout,
}

/// Start `shared`'s server on the current runtime: its lanes run from here
/// on, its commands queue until `serve` takes them.
pub(crate) fn start<H: Handler>(shared: &Arc<SharedState>, handler: &Arc<H>, port: u16) -> Started {
    // A drain of the last run is over.
    shared.draining.store(false, Ordering::SeqCst);
    shared.listen_port.store(port, Ordering::Relaxed);
    *shared.started_at.lock() = Some(std::time::Instant::now());
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
            if guarded(|| presence_handler.presence(event)).is_err() {
                tracing::error!("[WSE] Handler::presence panicked");
            }
        }))));
    // For presence broadcasts from the disconnect cleanup paths.
    shared.commands.store(Some(Arc::new(commands)));
    Started {
        commands_rx,
        fanout,
    }
}

/// Serve on `listener` until `running` is false: the command task, the
/// periodic tasks (recovery cleanup, topic counter sweep, signed-message
/// window, presence TTL sweep, ping / idle / slow-consumer check) and the
/// accept loop. Returns once the command task has ended (`Shutdown`) and
/// every task of this run with it, the connections' cleanup and their
/// disconnect events included; what the run set up is gone then.
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
    // This run's tasks: ended by `stop`, and waited for before the run ends.
    let stop = CancellationToken::new();
    let tasks = TaskTracker::new();
    // Spawn periodic recovery cleanup if recovery is enabled
    if let Some(ref recovery) = shared.recovery {
        let rm = recovery.clone();
        let cleanup_state = shared.clone();
        let cleanup_stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(crate::recovery::CLEANUP_INTERVAL);
            while tick(&mut interval, &cleanup_stop).await {
                rm.cleanup();
                cleanup_state.sweep_interest_linger();
            }
        });
        // Where the arrival order stands once a second: a peer that comes
        // back is sent what this node published since its interest began.
        let rm = recovery.clone();
        let mark_stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(crate::recovery::MARK_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            while tick(&mut interval, &mark_stop).await {
                rm.mark();
            }
        });
    }
    // Per-topic delivery counters of topics nobody subscribes
    // to any more: dropped once there are many (user- or
    // UUID-scoped topics), whether or not metrics are read.
    {
        let counts_state = shared.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            while tick(&mut interval, &stop).await {
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
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            while tick(&mut interval, &stop).await {
                let now = crate::server::signing::now_ms();
                seen_state.signed_seen.retain(|_, until| *until >= now);
            }
        });
    }
    // The clients' producers: past the bound the longest-disconnected are
    // forgotten within a second, and every one a day after its last
    // connection ended.
    // A pass through the table runs on a thread of its own: tens of
    // milliseconds on a large one, never on a thread connections run on.
    {
        let state = shared.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut ticks = 0u32;
            while tick(&mut interval, &stop).await {
                ticks = ticks.wrapping_add(1);
                let connections = state.connection_count.load(Ordering::Relaxed);
                let day = ticks.is_multiple_of(60);
                if day || state.producers.over(connections) {
                    let state = Arc::clone(&state);
                    let _ = tokio::task::spawn_blocking(move || {
                        state.producers.bound(connections);
                        if day {
                            state.producers.sweep(super::acks::now_ms());
                        }
                    })
                    .await;
                }
            }
        });
    }
    // Presence TTL sweep (every 30 seconds)
    if let Some(ref pm) = shared.presence {
        let sweep_pm = pm.clone();
        let sweep_state = shared.clone();
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            while tick(&mut interval, &stop).await {
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
        let stop = stop.clone();
        tasks.spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(ping_state.ping_interval_secs));
            let idle_timeout = Duration::from_secs(ping_state.idle_timeout_secs);
            let max_pending = ping_state.max_outbound_queue_bytes;
            // Two-phase slow consumer detection: connections suspected
            // of being slow (queue full on first check). If still full
            // on the next tick, they get disconnected.
            let mut suspected_slow: AHashSet<String> = AHashSet::new();
            while tick(&mut interval, &stop).await {
                let now = std::time::Instant::now();
                let snapshot: Vec<(String, ConnectionHandle)> = {
                    let conns = ping_state.connections.read();
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
                    // Not admitted yet (an end-to-end session or its proof
                    // under way, or the application's accept): nothing goes
                    // to it but its handshake.
                    if handle.closer.is_closing() || !handle.out.admitted.load(Ordering::Acquire) {
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
    // How soon a stop is seen with no connection coming: one timer for the
    // whole loop, which a connection does not set back.
    let mut stopping = tokio::time::interval(Duration::from_millis(100));
    stopping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        // Seen before each connection is taken: connections that keep coming
        // never hold a stop back.
        if !running.load(Ordering::SeqCst) {
            break;
        }
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
                        let stop = stop.clone();
                        tasks.spawn(async move {
                            // Protocol detection: peek first 4 bytes to distinguish
                            // cluster binary frames (4-byte big-endian length prefix:
                            // 0x00 0x00 0x00 XX for frames < 256 bytes) from HTTP/WS
                            // (first byte is ASCII letter like 'G' for GET) or TLS
                            // (first byte 0x16 for ClientHello).
                            let mut peek = [0u8; 4];
                            let peeked = if single_port {
                                tokio::select! {
                                    peeked = tokio::time::timeout(
                                        HANDSHAKE_TIMEOUT,
                                        stream.peek(&mut peek),
                                    ) => Some(peeked),
                                    // The server stops: a socket not yet
                                    // read just goes.
                                    () = stop.cancelled() => return,
                                }
                            } else {
                                None
                            };
                            // A cluster link starts with a TLS ClientHello (a
                            // handshake record, 0x16 0x03): the WebSocket port
                            // speaks plain HTTP itself, which starts with a
                            // letter (GET).
                            let is_cluster = match peeked {
                                Some(Ok(Ok(n))) if n >= 2 => peek[0] == 0x16 && peek[1] == 0x03,
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
                                handle_connection(stream, addr, shared2, hs_guard, handler, stop)
                                    .await;
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
            _ = stopping.tick() => {}
        }
    }
    // No more connections: the port goes now, not after the cleanup below.
    // Held, it would take connections nobody serves (a client coming back
    // at once would wait for its connect timeout); closed, a client is
    // refused, and comes back as soon as a server is there again.
    drop(listener);
    let _ = cmd_handle.await;
    leave_cluster(&shared).await;
    // The command task closed every connection: the periodic tasks end, and
    // the connections finish their cleanup.
    stop.cancel();
    tasks.close();
    tasks.wait().await;
    running.store(false, Ordering::SeqCst);
    // Lanes, queues and the handler go with the run, however it was stopped
    // (`stop`, or the server dropped).
    stopped(&shared);
    tracing::info!("[WSE] Shut down");
}

/// Out of the cluster once the command task is done: every publish it took
/// is on its way to the peers, and the links say SHUTDOWN after them and
/// close. Waited for a moment, so the peers know this node left on purpose
/// before its runtime goes.
async fn leave_cluster(shared: &SharedState) {
    let manager = shared.cluster_task.lock().take();
    let Some(manager) = manager else {
        return;
    };
    if let Some(tx) = shared.cluster_cmd_tx.load_full() {
        let _ = tx.send(crate::cluster::ClusterCommand::Shutdown);
    }
    let within = crate::cluster::LEAVE_WITHIN + Duration::from_millis(500);
    let _ = tokio::time::timeout(within, manager).await;
}

/// The next tick of a periodic task; false once its run is stopping.
async fn tick(interval: &mut tokio::time::Interval, stop: &CancellationToken) -> bool {
    tokio::select! {
        () = stop.cancelled() => false,
        _ = interval.tick() => true,
    }
}

/// What a stopped server leaves: no lanes, no command queue, no cluster.
pub(crate) fn stopped(shared: &SharedState) {
    // In no cluster any more: what other nodes had present goes with it.
    if let Some(pm) = &shared.presence {
        pm.purge_remote();
    }
    *shared.started_at.lock() = None;
    shared.commands.store(None);
    shared.presence_events.store(None);
    *shared.fanout.write().unwrap() = None;
    shared.cluster_cmd_tx.store(None);
    *shared.cluster_main_port.write().unwrap() = None;
    *shared.cluster_instance_id.lock().unwrap() = None;
}
