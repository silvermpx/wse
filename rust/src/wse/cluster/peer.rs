//! One peer link: writer, reader, dispatch and heartbeat tasks, the outbound
//! session and its reconnect loop.

use std::collections::HashMap;
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
    CAP_PRESENCE, CLUSTER_RESYNC_INTERVAL_SECS, ClusterCommand, ClusterContext, ClusterFrame,
    ClusterMetrics, ClusterTlsConfig, DispatchItem, HEARTBEAT_INTERVAL_SECS,
    HEARTBEAT_TIMEOUT_SECS, InterestUpdate, LOCAL_CAPABILITIES, MAX_BATCH_BYTES, MAX_FRAME_SIZE,
    PEER_DISPATCH_QUEUE_CAP, PEER_SESSION_GENERATION, PROTOCOL_VERSION, PeerInfo, PeerRegistration,
    PresencePeerFrame, cluster_link_survives, decode_frame, encode_hello, encode_peer_announce,
    encode_peer_list, encode_ping, encode_pong, encode_presence_full, encode_resync, epoch_ms,
    negotiate_capabilities, negotiate_version,
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
                let _ = ctx.interest_tx.send(InterestUpdate::Drain {
                    peer_addr: peer_addr.clone(),
                });
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
                let _ = ctx.interest_tx.send(InterestUpdate::Sub {
                    peer_addr: peer_addr.clone(),
                    topic,
                });
            }
            Some(ClusterFrame::Unsub { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = ctx.interest_tx.send(InterestUpdate::Unsub {
                    peer_addr: peer_addr.clone(),
                    topic,
                });
            }
            Some(ClusterFrame::Resync {
                topics,
                is_continuation,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = ctx.interest_tx.send(InterestUpdate::Resync {
                    peer_addr: peer_addr.clone(),
                    topics,
                    is_continuation,
                    generation,
                });
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
                let _ = ctx.presence_tx.send(PresencePeerFrame::Update {
                    origin: peer_addr.clone(),
                    topic,
                    user_id,
                    action,
                    data,
                    updated_at,
                });
            }
            Some(ClusterFrame::PresenceFull { entries }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = ctx.presence_tx.send(PresencePeerFrame::Full {
                    origin: peer_addr.clone(),
                    entries,
                });
            }
            None => {
                tracing::error!("[WSE-Cluster] Failed to decode frame, disconnecting");
                break;
            }
        }
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
            let preframed = crate::wse::server::encode_ws_frame(0x01, &payload);

            // Store the UNFRAMED payload in the foreign recovery buffer (cross-node
            // recovery), matching the local buffer's stored form so subscribe_with_
            // recovery frames/encrypts replayed bytes per connection. Live fan-out
            // below still uses the shared pre-framed bytes.
            if let (Some(rm), Some((epoch, offset))) = (&ctx.recovery, rec) {
                rm.push_foreign(&topic, payload.clone(), epoch, offset);
            }

            let (d, f) = crate::wse::server::fanout_topic_direct(
                &ctx.topic_subscribers,
                &topic,
                &preframed,
                ctx.max_outbound,
                &ctx.slow_drops,
                ctx.glob_topic_count.load(Ordering::Relaxed),
            );
            total_delivered += d;
            total_dropped += f;

            // Track per-topic message count for Prometheus top-N metrics
            ctx.topic_message_counts
                .entry(topic.clone())
                .or_insert_with(|| AtomicU64::new(0))
                .fetch_add(1, Ordering::Relaxed);

            // Cross-cluster queue group dispatch: round-robin to one member per group
            if let Some(topic_groups) = ctx.queue_groups.get(&topic) {
                for mut group_entry in topic_groups.iter_mut() {
                    let group = group_entry.value_mut();
                    let len = group.members.len();
                    if len == 0 {
                        continue;
                    }
                    let base = group.next.fetch_add(1, Ordering::Relaxed);
                    let data_len = preframed.len();
                    let mut delivered = false;
                    for attempt in 0..len {
                        let idx = (base + attempt) % len;
                        let (_, ref h) = group.members[idx];
                        if h.tx.is_closed() {
                            continue;
                        }
                        if ctx.max_outbound > 0
                            && h.pending.load(Ordering::Relaxed) >= ctx.max_outbound
                        {
                            ctx.slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        h.pending.fetch_add(data_len, Ordering::Relaxed);
                        let was_empty = {
                            let mut buf = h.broadcast_buf.lock();
                            let empty = buf.is_empty();
                            buf.push(&preframed);
                            empty
                        };
                        if was_empty {
                            h.broadcast_notify.notify_one();
                        }
                        delivered = true;
                        total_delivered += 1;
                        break;
                    }
                    if !delivered {
                        total_dropped += 1;
                    }
                }
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
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    let mut resync_interval =
        tokio::time::interval(Duration::from_secs(CLUSTER_RESYNC_INTERVAL_SECS));
    resync_interval.reset(); // don't RESYNC immediately (the handshake already did)
    let timeout_ms = HEARTBEAT_TIMEOUT_SECS * 1000;
    loop {
        tokio::select! {
            _ = resync_interval.tick() => {
                // Self-heal interest divergence from a dropped SUB/UNSUB: re-send
                // the full local interest set. Inner frames go through the channel
                // unframed (peer_writer adds the length prefix), like every other
                // peer send.
                let local_topics: Vec<String> = match local_topic_refcount.lock() {
                    Ok(g) => g.keys().cloned().collect(),
                    Err(e) => e.into_inner().keys().cloned().collect(),
                };
                for frame_buf in encode_resync(&local_topics) {
                    if peer_write_tx.try_send(frame_buf.freeze()).is_err() {
                        break; // channel full/closed; the next tick retries
                    }
                }
            }
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
// Single peer connection task (connect + HELLO + spawn reader/writer/heartbeat)
// ---------------------------------------------------------------------------

/// Run HELLO exchange, RESYNC, then spawn reader/writer/heartbeat and forward data.
/// Generic over AsyncRead/AsyncWrite to support both plaintext TCP and TLS streams.
/// Returns when the peer disconnects or cancellation is triggered.
async fn run_peer_session<R, W>(
    mut reader: R,
    mut writer: W,
    ids: (&str, &str),
    data_rx: &mut mpsc::Receiver<Bytes>,
    ctx: &ClusterContext,
    reliability: (&mut ExponentialBackoff, &mut CircuitBreaker),
    session: (
        &Option<Arc<crate::wse::presence::PresenceManager>>,
        &CancellationToken,
    ),
) -> (bool, u32)
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    let (peer_addr, instance_id) = ids;
    let (backoff, breaker) = reliability;
    let (presence, global_cancel) = session;
    // Send HELLO
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
        breaker.record_failure();
        tracing::warn!("[WSE-Cluster] Failed to send HELLO to {peer_addr}");
        return (false, 0);
    }

    // Read HELLO response
    let peer_hello = match tokio::time::timeout(Duration::from_secs(5), async {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            return Ok::<Option<ClusterFrame>, std::io::Error>(None);
        }
        let mut frame_buf = BytesMut::zeroed(frame_len);
        reader.read_exact(&mut frame_buf).await?;
        Ok(decode_frame(frame_buf))
    })
    .await
    {
        Ok(Ok(frame)) => frame,
        _ => None,
    };

    let (peer_instance_id, _peer_version, peer_caps) = match peer_hello {
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
                breaker.record_failure();
                return (false, 0);
            }
        },
        _ => {
            tracing::warn!("[WSE-Cluster] Invalid HELLO from {peer_addr}");
            breaker.record_failure();
            return (false, 0);
        }
    };

    // Tiebreaker: we are the DIALER here. If our instance_id is not lower than the
    // peer's, the peer is the designated dialer -- drop our outbound (it will dial
    // us and we accept that inbound), so exactly one link survives per pair. Equal
    // ids = a self-connection, also dropped. Do NOT touch connected_instances: the
    // peer's inbound link may already hold its instance_id and removing it would
    // clobber a valid link. Return success so peer_connection_task does not treat
    // this as a failure and tight-loop; a later reconnect just yields again.
    if !cluster_link_survives(instance_id, &peer_instance_id) {
        tracing::debug!(
            "[WSE-Cluster] Yielding outbound to {peer_addr}: instance {peer_instance_id} is the designated dialer"
        );
        return (true, peer_caps);
    }

    // Duplicate connection prevention: atomic check-and-insert via entry() API
    // to avoid TOCTOU race between contains_key and insert
    {
        use dashmap::mapref::entry::Entry;
        match ctx.connected_instances.entry(peer_instance_id.clone()) {
            Entry::Occupied(_) => {
                tracing::info!(
                    "[WSE-Cluster] Already connected to instance {}, dropping duplicate from {peer_addr}",
                    peer_instance_id
                );
                // Record failure so peer_connection_task backs off instead of spinning
                breaker.record_failure();
                return (false, peer_caps);
            }
            Entry::Vacant(e) => {
                e.insert(peer_addr.to_string());
            }
        }
    }

    // Connected successfully
    tracing::info!(
        "[WSE-Cluster] Connected to {peer_addr} (v{_peer_version}, caps=0x{peer_caps:08x})"
    );
    breaker.record_success();
    backoff.reset();
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

    // Send RESYNC with all current local topics (before spawning writer task)
    {
        let local_topics: Vec<String> = {
            let refcounts = ctx
                .local_topic_refcount
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            refcounts.keys().cloned().collect()
        };
        if !local_topics.is_empty() {
            let resync_frames = encode_resync(&local_topics);
            let mut wire_buf = BytesMut::new();
            for frame in &resync_frames {
                write_framed(&mut wire_buf, frame);
            }
            if writer.write_all(&wire_buf).await.is_err() || writer.flush().await.is_err() {
                tracing::warn!("[WSE-Cluster] Failed to send RESYNC to {peer_addr}");
                ctx.connected_instances.remove(&peer_instance_id);
                ctx.metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
                ctx.metrics.peer_info.remove(peer_addr);
                return (false, peer_caps); // Will reconnect
            }
        }
    }

    // Send PEER_ANNOUNCE with our cluster address (if in discovery mode)
    if let Some(ref our_addr) = ctx.cluster_addr {
        let mut announce_buf = BytesMut::new();
        encode_peer_announce(&mut announce_buf, our_addr);
        let mut announce_frame = BytesMut::new();
        write_framed(&mut announce_frame, &announce_buf);
        let _ = writer.write_all(&announce_frame).await;
        let _ = writer.flush().await;
    }

    // Send PEER_LIST with all known peers
    {
        let addrs: Vec<String> = ctx.known_peers.iter().map(|r| r.key().clone()).collect();
        if !addrs.is_empty() {
            let mut list_buf = BytesMut::new();
            encode_peer_list(&mut list_buf, &addrs);
            let mut list_frame = BytesMut::new();
            write_framed(&mut list_frame, &list_buf);
            let _ = writer.write_all(&list_frame).await;
            let _ = writer.flush().await;
        }
    }

    // Send full presence state to new peer (only if peer supports presence)
    if peer_caps & CAP_PRESENCE != 0
        && let Some(pm) = presence
    {
        // Chunked so a node with >1 frame of presence still sends its full state
        // (a single oversized frame was previously dropped, leaving the peer blind).
        for entries in pm.serialize_full_state_chunked(MAX_FRAME_SIZE - 1024) {
            let mut buf = BytesMut::new();
            if encode_presence_full(&mut buf, &entries) {
                let mut frame = BytesMut::new();
                write_framed(&mut frame, &buf);
                if let Err(e) = writer.write_all(&frame).await {
                    tracing::warn!("[WSE-Cluster] Failed to send PresenceFull: {e}");
                }
                let _ = writer.flush().await;
            }
        }
    }

    // Spawn reader, writer, heartbeat with child cancellation token
    let peer_cancel = global_cancel.child_token();
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

    let session_gen = PEER_SESSION_GENERATION.fetch_add(1, Ordering::AcqRel);
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
        ctx.local_topic_refcount.clone(),
    ));

    // Forward data from cluster manager to this peer's writer
    loop {
        let data = tokio::select! {
            data = data_rx.recv() => data,
            () = peer_cancel.cancelled() => break,
            () = global_cancel.cancelled() => {
                peer_cancel.cancel();
                break;
            }
        };

        match data {
            Some(frame_data) => {
                if peer_write_tx.try_send(frame_data).is_err() {
                    ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => {
                // Manager channel closed (shutdown)
                peer_cancel.cancel();
                break;
            }
        }
    }

    // Cleanup
    peer_cancel.cancel();
    let _ = writer_handle.await;
    let _ = reader_handle.await;
    let _ = dispatch_handle.await;
    let _ = heartbeat_handle.await;
    ctx.metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    ctx.metrics.peer_info.remove(peer_addr);
    ctx.connected_instances.remove(&peer_instance_id);
    let _ = ctx.interest_tx.send(InterestUpdate::PeerDisconnected {
        peer_addr: peer_addr.to_owned(),
        generation: session_gen,
    });
    tracing::info!("[WSE-Cluster] Disconnected from {peer_addr}");

    (global_cancel.is_cancelled(), peer_caps)
}

async fn peer_connection_task(
    peer_addr: String,
    instance_id: String,
    mut data_rx: mpsc::Receiver<Bytes>,
    ctx: ClusterContext,
    tls_config: Option<Arc<arc_swap::ArcSwap<ClusterTlsConfig>>>,
    peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
    session: (
        Option<Arc<crate::wse::presence::PresenceManager>>,
        CancellationToken,
    ),
) {
    let (presence, global_cancel) = session;
    let mut backoff = ExponentialBackoff::new();
    let mut breaker = CircuitBreaker::new();

    loop {
        if global_cancel.is_cancelled() {
            break;
        }

        // Drain stale messages buffered during backoff/reconnect to prevent
        // unbounded memory growth and replaying outdated data to the peer.
        while data_rx.try_recv().is_ok() {}

        // Circuit breaker check
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

        // TCP connect with timeout
        let stream = match tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(&peer_addr),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                breaker.record_failure();
                ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                tracing::warn!(
                    "[WSE-Cluster] Connect failed {peer_addr}: {e}. Retry in {:.1}s",
                    delay.as_secs_f64()
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
            Err(_) => {
                breaker.record_failure();
                ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                tracing::warn!(
                    "[WSE-Cluster] Connect timeout {peer_addr}. Retry in {:.1}s",
                    delay.as_secs_f64()
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
        };

        // TCP_NODELAY + keepalive + larger buffers for throughput
        let _ = stream.set_nodelay(true);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(10))
            .with_interval(Duration::from_secs(5));
        let sock_ref = SockRef::from(&stream);
        let _ = sock_ref.set_tcp_keepalive(&keepalive);
        let _ = sock_ref.set_recv_buffer_size(262_144);
        let _ = sock_ref.set_send_buffer_size(262_144);

        // TLS handshake or plaintext split, then run session.
        // Load current TLS config snapshot -- safe even if a reload swaps the
        // config mid-flight because TlsAcceptor/TlsConnector internally use Arc.
        let tls_snap = tls_config.as_ref().map(|holder| holder.load());
        let result = if let Some(ref tls) = tls_snap {
            // Extract hostname/IP from peer_addr for SNI
            let host = peer_addr.split(':').next().unwrap_or(&peer_addr);
            let server_name = match host.parse::<std::net::IpAddr>() {
                Ok(ip) => ServerName::IpAddress(ip.into()),
                Err(_) => match ServerName::try_from(host.to_owned()) {
                    Ok(name) => name,
                    Err(e) => {
                        breaker.record_failure();
                        let delay = backoff.next_delay();
                        tracing::error!(
                            "[WSE-Cluster] Invalid peer address for TLS SNI '{host}': {e}. Retry in {:.1}s",
                            delay.as_secs_f64()
                        );
                        tokio::select! {
                            () = tokio::time::sleep(delay) => continue,
                            () = global_cancel.cancelled() => break,
                        }
                    }
                },
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
                        &mut data_rx,
                        &ctx,
                        (&mut backoff, &mut breaker),
                        (&presence, &global_cancel),
                    )
                    .await
                }
                Ok(Err(e)) => {
                    breaker.record_failure();
                    ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        "[WSE-Cluster] TLS handshake failed {peer_addr}: {e}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        () = global_cancel.cancelled() => break,
                    }
                }
                Err(_) => {
                    breaker.record_failure();
                    ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    let delay = backoff.next_delay();
                    tracing::warn!(
                        "[WSE-Cluster] TLS handshake timeout {peer_addr}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        () = global_cancel.cancelled() => break,
                    }
                }
            }
        } else {
            // Plaintext: split into owned halves
            let (reader, writer) = stream.into_split();
            run_peer_session(
                reader,
                writer,
                (&peer_addr, &instance_id),
                &mut data_rx,
                &ctx,
                (&mut backoff, &mut breaker),
                (&presence, &global_cancel),
            )
            .await
        };

        let (should_stop, negotiated_caps) = result;

        // Update outbound peer caps after handshake negotiation
        if negotiated_caps != 0 {
            let _ = peer_reg_tx.send(PeerRegistration::UpdateCaps {
                peer_addr: peer_addr.clone(),
                negotiated_caps,
            });
        }

        if should_stop {
            break;
        }

        // Reconnect
        ctx.metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
        let delay = backoff.next_delay();
        tracing::info!(
            "[WSE-Cluster] Reconnecting to {peer_addr} in {:.1}s",
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
    pub(super) presence: Option<Arc<crate::wse::presence::PresenceManager>>,
}

impl PeerSpawnCtx {
    pub(super) fn spawn(
        &self,
        peer_addr: String,
        data_rx: mpsc::Receiver<Bytes>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(peer_connection_task(
            peer_addr,
            self.instance_id.clone(),
            data_rx,
            self.ctx.clone(),
            self.tls_config.clone(),
            self.peer_reg_tx.clone(),
            (self.presence.clone(), self.global_cancel.clone()),
        ))
    }
}
