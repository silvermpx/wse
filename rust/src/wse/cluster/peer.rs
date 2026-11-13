//! One peer link: writer, reader, dispatch and heartbeat tasks, the outbound
//! session and its reconnect loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rustls::pki_types::ServerName;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::protocol::write_framed;
use super::{
    ClusterCommand, ClusterContext, ClusterFrame, ClusterMetrics, ClusterTlsConfig, DispatchItem,
    HEARTBEAT_INTERVAL_SECS, HEARTBEAT_TIMEOUT_SECS, InterestUpdate, LOCAL_CAPABILITIES, Link,
    LinkRefused, MAX_BATCH_BYTES, MAX_FRAME_SIZE, PEER_DISPATCH_QUEUE_CAP, PEER_SESSION_GENERATION,
    PRESENCE_ROUND, PRESENCE_ROUND_FIRST, PRESENCE_ROUND_LAST, PROTOCOL_VERSION, PeerInfo,
    PeerRegistration, PresencePeerFrame, RoundPart, claim_link, decode_frame, encode_hello,
    encode_peer_announce, encode_peer_list, encode_ping, encode_pong, epoch_ms,
    negotiate_capabilities, negotiate_version, release_link,
};
use crate::wse::reliability::{CircuitBreaker, ExponentialBackoff};

// ---------------------------------------------------------------------------
// Per-peer writer task (write coalescing: recv + try_recv drain)
// ---------------------------------------------------------------------------

pub(super) async fn peer_writer<W: AsyncWriteExt + Unpin>(
    mut rx: mpsc::Receiver<Bytes>,
    mut writer: W,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
) {
    // Whichever of a link's tasks ends first ends the link: its cleanup then
    // runs and the dialer reconnects.
    let _end_link = cancel.clone().drop_guard();
    let mut buf = BytesMut::with_capacity(MAX_BATCH_BYTES);

    loop {
        let first = tokio::select! {
            msg = rx.recv() => msg,
            () = cancel.cancelled() => break,
        };

        let Some(first) = first else { break };

        // Frame the first message
        write_framed(&mut buf, &first);

        // Drain everything else ready (non-blocking coalescing)
        while buf.len() < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(data) => write_framed(&mut buf, &data),
                Err(_) => break,
            }
        }

        let bytes_count = buf.len() as u64;
        if writer.write_all(&buf).await.is_err() {
            break;
        }
        // flush() is required for TLS -- rustls buffers encrypted data internally
        // and only writes to the TCP socket on flush. For plain TCP this is a no-op.
        if writer.flush().await.is_err() {
            break;
        }
        metrics.bytes_sent.fetch_add(bytes_count, Ordering::Relaxed);
        buf.clear();
    }
}

// ---------------------------------------------------------------------------
// Per-peer reader task
// ---------------------------------------------------------------------------

/// Reads frames from a peer TCP connection. Decoded MSG payloads are pushed to
/// a dispatch channel (non-blocking) so fan-out never stalls TCP reads.
/// BufReader (64KB) reduces read syscalls from 2/frame to ~1 per 650 frames.
pub(super) async fn peer_reader<R: AsyncReadExt + Unpin>(
    reader: R,
    peer_write_tx: mpsc::Sender<Bytes>,
    peer_addr: String,
    ctx: ClusterContext,
    dispatch_tx: mpsc::Sender<DispatchItem>,
    cancel: CancellationToken,
    session: (Arc<AtomicU64>, u64),
) {
    let _end_link = cancel.clone().drop_guard();
    let (last_activity, generation) = session;
    // BufReader: bulk reads from TCP into 64KB buffer, serves read_exact from memory
    let mut reader = BufReader::with_capacity(65_536, reader);
    let mut len_buf = [0u8; 4];

    loop {
        // Read frame length (4 bytes, big-endian)
        let read_result = tokio::select! {
            r = reader.read_exact(&mut len_buf) => r,
            () = cancel.cancelled() => break,
        };
        if read_result.is_err() {
            break;
        }

        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            tracing::warn!("[WSE-Cluster] Invalid frame size: {frame_len}, disconnecting");
            break;
        }

        // Read frame payload
        let mut frame_buf = BytesMut::zeroed(frame_len);
        let read_result = tokio::select! {
            r = reader.read_exact(&mut frame_buf) => r,
            () = cancel.cancelled() => break,
        };
        if read_result.is_err() {
            break;
        }

        ctx.metrics
            .bytes_received
            .fetch_add((4 + frame_len) as u64, Ordering::Relaxed);

        // Decode and dispatch
        match decode_frame(frame_buf) {
            Some(ClusterFrame::Msg {
                topic,
                payload,
                recovery,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // Bounded dispatch: shed (don't block the reader) when local fan-out
                // falls behind, so a fast peer cannot grow an unbounded backlog -> OOM.
                // Shed MSGs stay recoverable via the recovery buffer; the reader remains
                // responsive to control/heartbeat frames on the same loop.
                if dispatch_tx.try_send((topic, payload, recovery)).is_err() {
                    ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            Some(ClusterFrame::Ping) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let mut pong = BytesMut::new();
                encode_pong(&mut pong);
                let _ = peer_write_tx.try_send(pong.freeze());
            }
            Some(ClusterFrame::Pong) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
            }
            Some(ClusterFrame::Shutdown) => {
                tracing::info!("[WSE-Cluster] Peer sent SHUTDOWN");
                break;
            }
            Some(ClusterFrame::Drain) => {
                tracing::info!("[WSE-Cluster] Peer entering drain mode (lame duck)");
                // Stop routing NEW messages to the draining peer (clear its remote
                // interest) but keep the connection open for in-flight delivery.
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let update = InterestUpdate::Drain {
                    peer_addr: peer_addr.clone(),
                };
                if !to_manager(&ctx.interest_tx, update, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::Hello { .. }) => {
                // Unexpected HELLO after handshake, ignore
            }
            Some(ClusterFrame::Unknown { msg_type }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                ctx.metrics
                    .unknown_message_types
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "[WSE-Cluster] Unknown message type 0x{msg_type:02x}, skipping frame"
                );
            }
            Some(ClusterFrame::Sub { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let update = InterestUpdate::Sub {
                    peer_addr: peer_addr.clone(),
                    topic,
                };
                if !to_manager(&ctx.interest_tx, update, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::Unsub { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let update = InterestUpdate::Unsub {
                    peer_addr: peer_addr.clone(),
                    topic,
                };
                if !to_manager(&ctx.interest_tx, update, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::Resync {
                topics,
                is_continuation,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let update = InterestUpdate::Resync {
                    peer_addr: peer_addr.clone(),
                    topics,
                    is_continuation,
                    generation,
                };
                if !to_manager(&ctx.interest_tx, update, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::PeerAnnounce { addr }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // Notify cluster_manager to connect to new peer
                let _ = ctx.new_peer_tx.send(addr.clone());
                // Gossip to all other peers (excluding sender)
                let _ = ctx.cmd_tx.send(ClusterCommand::GossipPeerAnnounce {
                    addr,
                    exclude_peer: peer_addr.clone(),
                });
            }
            Some(ClusterFrame::PeerList { addrs }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                for addr in addrs {
                    let _ = ctx.new_peer_tx.send(addr);
                }
            }
            Some(ClusterFrame::PresenceUpdate {
                topic,
                user_id,
                action,
                data,
                updated_at,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let frame = PresencePeerFrame::Update {
                    origin: peer_addr.clone(),
                    topic,
                    user_id,
                    action,
                    data,
                    updated_at,
                };
                if !to_manager(&ctx.presence_tx, frame, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::PresenceFull { entries, flags }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let round = (flags & PRESENCE_ROUND != 0).then_some(RoundPart {
                    first: flags & PRESENCE_ROUND_FIRST != 0,
                    last: flags & PRESENCE_ROUND_LAST != 0,
                });
                let frame = PresencePeerFrame::Full {
                    origin: peer_addr.clone(),
                    entries,
                    round,
                    generation,
                };
                if !to_manager(&ctx.presence_tx, frame, &cancel).await {
                    break;
                }
            }
            None => {
                tracing::error!("[WSE-Cluster] Failed to decode frame, disconnecting");
                break;
            }
        }
    }
}

/// Hand an update to the manager, waiting while its queue is full: a peer
/// sending faster than the manager keeps up is slowed down at its socket,
/// not buffered without bound. False once the link ends.
async fn to_manager<T>(tx: &mpsc::Sender<T>, item: T, cancel: &CancellationToken) -> bool {
    tokio::select! {
        sent = tx.send(item) => sent.is_ok(),
        () = cancel.cancelled() => false,
    }
}

/// Tell the manager a link ended. It must hear of every one (its interest
/// and presence go with it), so this waits for room, except on shutdown:
/// the manager reads no more then.
pub(super) async fn tell_disconnected(
    ctx: &ClusterContext,
    peer_addr: String,
    generation: u64,
    global_cancel: &CancellationToken,
) {
    let update = InterestUpdate::PeerDisconnected {
        peer_addr,
        generation,
    };
    tokio::select! {
        _ = ctx.interest_tx.send(update) => {}
        () = global_cancel.cancelled() => {}
    }
}

// ---------------------------------------------------------------------------
// Per-peer dispatch task: receives decoded (topic, payload) from reader,
// pre-frames as WebSocket and fans out via NATS-style direct buffer.
// Decouples TCP reads from RwLock acquisition on the connections map.
// ---------------------------------------------------------------------------

pub(super) async fn peer_dispatch_task(
    mut dispatch_rx: mpsc::Receiver<DispatchItem>,
    ctx: ClusterContext,
    cancel: CancellationToken,
) {
    let _end_link = cancel.clone().drop_guard();
    let mut batch: Vec<DispatchItem> = Vec::with_capacity(64);

    loop {
        // Wait for first message (blocking)
        let msg = tokio::select! {
            m = dispatch_rx.recv() => m,
            () = cancel.cancelled() => break,
        };
        let Some(first) = msg else { break };
        batch.push(first);

        // Drain any additional queued messages (non-blocking)
        while batch.len() < 256 {
            match dispatch_rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }

        // No RwLock needed -- handles are stored directly in topic_subscribers
        let mut total_delivered = 0u64;
        let mut total_dropped = 0u64;

        for (topic, payload, rec) in batch.drain(..) {
            // Store the unframed payload in the foreign recovery buffer
            // (cross-node recovery), the form the local buffer stores too.
            if let (Some(rm), Some((epoch, offset))) = (&ctx.recovery, rec) {
                rm.push_foreign(&topic, payload.clone(), epoch, offset);
            }

            let msg = crate::wse::server::Outgoing::text(payload.clone());
            let (d, f) = ctx
                .topics
                .fanout(&topic, &msg, ctx.max_outbound, &ctx.slow_drops);
            total_delivered += d;
            total_dropped += f;

            // Track per-topic message count for Prometheus top-N metrics
            ctx.topic_message_counts
                .entry(topic.clone())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, Ordering::Relaxed);

            // Queue groups: one member each, round-robin, without the
            // recovery stamp (a member sees only some of the offsets).
            if ctx.queue_groups.contains_key(&topic) {
                let (d, f) = crate::wse::server::fanout_queue_groups(
                    &ctx.queue_groups,
                    &topic,
                    &crate::wse::server::Outgoing::text(crate::wse::server::unstamped(&payload)),
                    ctx.max_outbound,
                    &ctx.slow_drops,
                );
                total_delivered += d;
                total_dropped += f;
            }
        }

        ctx.metrics.add_delivered(total_delivered);
        if total_dropped > 0 {
            ctx.metrics.add_dropped(total_dropped);
        }
    }
}

// ---------------------------------------------------------------------------
// Heartbeat task
// ---------------------------------------------------------------------------

pub(super) async fn heartbeat_task(
    peer_write_tx: mpsc::Sender<Bytes>,
    cancel: CancellationToken,
    last_activity: Arc<AtomicU64>,
) {
    let _end_link = cancel.clone().drop_guard();
    let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    let timeout_ms = HEARTBEAT_TIMEOUT_SECS * 1000;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check if peer is dead (no activity for HEARTBEAT_TIMEOUT_SECS)
                let last = last_activity.load(Ordering::Relaxed);
                if epoch_ms().saturating_sub(last) > timeout_ms {
                    tracing::warn!("[WSE-Cluster] Peer heartbeat timeout ({}s no activity)", timeout_ms / 1000);
                    cancel.cancel();
                    break;
                }
                let mut ping = BytesMut::new();
                encode_ping(&mut ping);
                match peer_write_tx.try_send(ping.freeze()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Write channel full -- ping couldn't be queued.
                        // Do NOT treat as liveness evidence: a TCP-dead peer with a full
                        // send buffer also triggers Full, and updating last_activity here
                        // would prevent the timeout from ever firing.
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            () = cancel.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Handshake helpers
// ---------------------------------------------------------------------------

/// Read one length-prefixed frame (the HELLO of a handshake). `Ok(None)` is a
/// frame of an impossible size or one that does not decode.
pub(super) async fn read_frame<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<ClusterFrame>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let frame_len = u32::from_be_bytes(len_buf) as usize;
    if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
        return Ok(None);
    }
    let mut frame_buf = BytesMut::zeroed(frame_len);
    reader.read_exact(&mut frame_buf).await?;
    Ok(decode_frame(frame_buf))
}

/// The TLS server name for a peer address: its IP address or DNS name without
/// the port. IPv6 addresses come bracketed (`[::1]:9000`).
pub(super) fn server_name_for(addr: &str) -> Option<ServerName<'static>> {
    if let Ok(sock) = addr.parse::<std::net::SocketAddr>() {
        return Some(ServerName::IpAddress(sock.ip().into()));
    }
    let (host, port) = addr.rsplit_once(':')?;
    port.parse::<u16>().ok()?;
    ServerName::try_from(host.to_owned()).ok()
}

// ---------------------------------------------------------------------------
// Outbound link: handshake, session and the dial loop
// ---------------------------------------------------------------------------

/// How an outbound attempt ended, and so what the dial loop does next.
enum Outcome {
    /// A session ran and the link closed: dial again.
    Ended,
    /// The handshake failed: back off, counting a failure.
    Failed,
    /// The peer is already linked to this node through a link that wins:
    /// back off and try again later, in case that link goes away.
    Duplicate,
    /// The address is this node's own: stop dialing it.
    SelfLink,
    /// The cluster is shutting down.
    Shutdown,
}

/// HELLO exchange, then the session until the link closes. Generic over
/// AsyncRead/AsyncWrite for both plaintext TCP and TLS streams.
async fn run_peer_session<R, W>(
    mut reader: R,
    mut writer: W,
    ids: (&str, &str),
    ctx: &ClusterContext,
    peer_reg_tx: &mpsc::UnboundedSender<PeerRegistration>,
    global_cancel: &CancellationToken,
) -> Outcome
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (peer_addr, instance_id) = ids;

    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
        tracing::warn!("[WSE-Cluster] Failed to send HELLO to {peer_addr}");
        return Outcome::Failed;
    }

    let peer_hello =
        match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut reader)).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(_)) => {
                // A peer closes without answering when it keeps another link to
                // this node, or when it is an older version that only accepts
                // links from the lower instance id.
                tracing::debug!("[WSE-Cluster] {peer_addr} closed the link during the handshake");
                return Outcome::Failed;
            }
            Err(_) => {
                tracing::warn!("[WSE-Cluster] No HELLO from {peer_addr} within 5s");
                return Outcome::Failed;
            }
        };

    let (peer_instance_id, peer_version, peer_caps) = match peer_hello {
        Some(ClusterFrame::Hello {
            instance_id: peer_id,
            protocol_version,
            capabilities,
        }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
            Some(v) => (
                peer_id,
                v,
                negotiate_capabilities(LOCAL_CAPABILITIES, capabilities),
            ),
            None => {
                tracing::warn!(
                    "[WSE-Cluster] Version mismatch with {peer_addr}: local={PROTOCOL_VERSION}, remote={protocol_version}"
                );
                return Outcome::Failed;
            }
        },
        _ => {
            tracing::warn!("[WSE-Cluster] Invalid HELLO from {peer_addr}");
            return Outcome::Failed;
        }
    };

    let peer_cancel = global_cancel.child_token();
    let session_gen = PEER_SESSION_GENERATION.fetch_add(1, Ordering::AcqRel);
    let link = Link {
        dialer: instance_id.to_owned(),
        session: session_gen,
        cancel: peer_cancel.clone(),
    };
    match claim_link(&ctx.links, instance_id, &peer_instance_id, link) {
        Ok(()) => {}
        Err(LinkRefused::SelfLink) => {
            tracing::info!("[WSE-Cluster] {peer_addr} is this node's own address; not dialing it");
            return Outcome::SelfLink;
        }
        Err(LinkRefused::Duplicate) => {
            tracing::debug!(
                "[WSE-Cluster] Already linked to instance {peer_instance_id}; keeping that link instead of {peer_addr}"
            );
            return Outcome::Duplicate;
        }
    }

    tracing::info!(
        "[WSE-Cluster] Connected to {peer_addr} (v{peer_version}, caps=0x{peer_caps:08x})"
    );
    ctx.metrics.connected_peers.fetch_add(1, Ordering::Relaxed);
    ctx.metrics.peer_info.insert(
        peer_addr.to_owned(),
        PeerInfo {
            address: peer_addr.to_owned(),
            instance_id: peer_instance_id.clone(),
            capabilities: peer_caps,
            connected_at: epoch_ms(),
        },
    );

    // This node's interest (RESYNC) and presence follow from the manager once
    // the link is registered, in order with every change after them.

    // PEER_ANNOUNCE with our cluster address (discovery mode)
    if let Some(ref our_addr) = ctx.cluster_addr {
        let mut announce_buf = BytesMut::new();
        encode_peer_announce(&mut announce_buf, our_addr);
        let mut announce_frame = BytesMut::new();
        write_framed(&mut announce_frame, &announce_buf);
        let _ = writer.write_all(&announce_frame).await;
        let _ = writer.flush().await;
    }

    // PEER_LIST with all known peers
    let addrs: Vec<String> = ctx.known_peers.iter().map(|r| r.key().clone()).collect();
    if !addrs.is_empty() {
        let mut list_buf = BytesMut::new();
        encode_peer_list(&mut list_buf, &addrs);
        let mut list_frame = BytesMut::new();
        write_framed(&mut list_frame, &list_buf);
        let _ = writer.write_all(&list_frame).await;
        let _ = writer.flush().await;
    }

    let (peer_write_tx, peer_write_rx) = mpsc::channel::<Bytes>(10_000);
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

    let writer_handle = tokio::spawn(peer_writer(
        peer_write_rx,
        writer,
        peer_cancel.clone(),
        ctx.metrics.clone(),
    ));

    let (dispatch_tx, dispatch_rx) = mpsc::channel::<DispatchItem>(PEER_DISPATCH_QUEUE_CAP);
    let dispatch_handle = tokio::spawn(peer_dispatch_task(
        dispatch_rx,
        ctx.clone(),
        peer_cancel.clone(),
    ));

    let reader_handle = tokio::spawn(peer_reader(
        reader,
        peer_write_tx.clone(),
        peer_addr.to_owned(),
        ctx.clone(),
        dispatch_tx,
        peer_cancel.clone(),
        (last_activity.clone(), session_gen),
    ));

    let heartbeat_handle = tokio::spawn(heartbeat_task(
        peer_write_tx.clone(),
        peer_cancel.clone(),
        last_activity,
    ));

    // From now on the manager routes publishes, interest and presence here,
    // with the capabilities this handshake negotiated, starting with this
    // node's RESYNC and presence.
    let _ = peer_reg_tx.send(PeerRegistration::Register {
        peer_addr: peer_addr.to_owned(),
        write_tx: peer_write_tx,
        negotiated_caps: peer_caps,
    });

    // The link ends when any of its tasks ends, when a better duplicate
    // replaces it, or on shutdown (the token is a child of the global one).
    peer_cancel.cancelled().await;
    let _ = writer_handle.await;
    let _ = reader_handle.await;
    let _ = dispatch_handle.await;
    let _ = heartbeat_handle.await;

    let _ = peer_reg_tx.send(PeerRegistration::Deregister {
        peer_addr: peer_addr.to_owned(),
    });
    ctx.metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    ctx.metrics.peer_info.remove(peer_addr);
    release_link(&ctx.links, &peer_instance_id, session_gen);
    tell_disconnected(ctx, peer_addr.to_owned(), session_gen, global_cancel).await;
    tracing::info!("[WSE-Cluster] Disconnected from {peer_addr}");

    if global_cancel.is_cancelled() {
        Outcome::Shutdown
    } else {
        Outcome::Ended
    }
}

/// Consecutive failed connects after which a peer learned through gossip is
/// given up: it has most likely left the cluster. Static peers are dialed for
/// as long as the node runs.
const MAX_UNREACHABLE_ATTEMPTS: u32 = 10;

async fn peer_connection_task(
    peer_addr: String,
    instance_id: String,
    is_static: bool,
    ctx: ClusterContext,
    tls_config: Option<Arc<arc_swap::ArcSwap<ClusterTlsConfig>>>,
    peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
    global_cancel: CancellationToken,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut breaker = CircuitBreaker::new();
    let mut unreachable = 0u32;

    loop {
        if global_cancel.is_cancelled() {
            break;
        }
        if !is_static && unreachable >= MAX_UNREACHABLE_ATTEMPTS {
            tracing::info!(
                "[WSE-Cluster] Giving up on {peer_addr}: unreachable for {unreachable} attempts"
            );
            ctx.known_peers.remove(&peer_addr);
            break;
        }

        if !breaker.can_execute() {
            let delay = breaker.reset_timeout;
            tracing::warn!(
                "[WSE-Cluster] Circuit open for {peer_addr}, waiting {:.0}s",
                delay.as_secs_f64()
            );
            tokio::select! {
                () = tokio::time::sleep(delay) => continue,
                () = global_cancel.cancelled() => break,
            }
        }

        let connected =
            tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&peer_addr)).await;
        let stream = match connected {
            Ok(Ok(stream)) => stream,
            failed => {
                unreachable += 1;
                breaker.record_failure();
                ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                match failed {
                    Ok(Err(e)) => tracing::warn!(
                        "[WSE-Cluster] Connect failed {peer_addr}: {e}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    ),
                    _ => tracing::warn!(
                        "[WSE-Cluster] Connect timeout {peer_addr}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    ),
                }
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
        };
        unreachable = 0;

        // TCP_NODELAY + keepalive + larger buffers for throughput
        let _ = stream.set_nodelay(true);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(10))
            .with_interval(Duration::from_secs(5));
        let sock_ref = SockRef::from(&stream);
        let _ = sock_ref.set_tcp_keepalive(&keepalive);
        let _ = sock_ref.set_recv_buffer_size(262_144);
        let _ = sock_ref.set_send_buffer_size(262_144);

        // The TLS config is read per attempt, so a reload applies to the next dial.
        let tls_snap = tls_config.as_ref().map(|holder| holder.load());
        let outcome = if let Some(ref tls) = tls_snap {
            let Some(server_name) = server_name_for(&peer_addr) else {
                tracing::error!(
                    "[WSE-Cluster] {peer_addr} is not a host:port address usable for TLS; not dialing it"
                );
                break;
            };
            match tokio::time::timeout(
                Duration::from_secs(5),
                tls.connector.connect(server_name, stream),
            )
            .await
            {
                Ok(Ok(tls_stream)) => {
                    let (reader, writer) = tokio::io::split(tls_stream);
                    run_peer_session(
                        reader,
                        writer,
                        (&peer_addr, &instance_id),
                        &ctx,
                        &peer_reg_tx,
                        &global_cancel,
                    )
                    .await
                }
                Ok(Err(e)) => {
                    tracing::warn!("[WSE-Cluster] TLS handshake failed {peer_addr}: {e}");
                    Outcome::Failed
                }
                Err(_) => {
                    tracing::warn!("[WSE-Cluster] TLS handshake timeout {peer_addr}");
                    Outcome::Failed
                }
            }
        } else {
            let (reader, writer) = stream.into_split();
            run_peer_session(
                reader,
                writer,
                (&peer_addr, &instance_id),
                &ctx,
                &peer_reg_tx,
                &global_cancel,
            )
            .await
        };

        let delay = match outcome {
            Outcome::Shutdown | Outcome::SelfLink => break,
            Outcome::Ended => {
                breaker.record_success();
                backoff.reset();
                ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                backoff.next_delay()
            }
            Outcome::Failed => {
                breaker.record_failure();
                ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                backoff.next_delay()
            }
            // The peer answered: no failure, but no need to redial soon.
            Outcome::Duplicate => backoff.next_delay(),
        };
        tracing::debug!(
            "[WSE-Cluster] Dialing {peer_addr} again in {:.1}s",
            delay.as_secs_f64()
        );
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = global_cancel.cancelled() => break,
        }
    }
}

/// Shared context for spawning peer connection tasks.
pub(super) struct PeerSpawnCtx {
    pub(super) instance_id: String,
    pub(super) ctx: ClusterContext,
    pub(super) global_cancel: CancellationToken,
    pub(super) tls_config: Option<Arc<arc_swap::ArcSwap<ClusterTlsConfig>>>,
    pub(super) peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
}

impl PeerSpawnCtx {
    /// Start dialing `peer_addr`. A static peer (from the configuration) is
    /// dialed for as long as the node runs; one learned through gossip is
    /// given up after it stays unreachable.
    pub(super) fn spawn(&self, peer_addr: String, is_static: bool) -> tokio::task::JoinHandle<()> {
        tokio::spawn(peer_connection_task(
            peer_addr,
            self.instance_id.clone(),
            is_static,
            self.ctx.clone(),
            self.tls_config.clone(),
            self.peer_reg_tx.clone(),
            self.global_cancel.clone(),
        ))
    }
}
