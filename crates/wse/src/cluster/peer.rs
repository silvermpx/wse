//! One peer link: writer, reader, dispatch and heartbeat tasks, the outbound
//! session and its reconnect loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rustls::pki_types::ServerName;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::protocol::{encode_lost, write_framed};
use super::{
    CAP_ARRIVAL, CAP_RESUME, CAP_RESUME_TOPICS, ClusterCommand, ClusterContext, ClusterFrame,
    ClusterMetrics, ClusterTlsConfig, DispatchItem, HEARTBEAT_INTERVAL_SECS,
    HEARTBEAT_TIMEOUT_SECS, Heard, InterestUpdate, Link, LinkRefused, MAX_BATCH_BYTES,
    MAX_FRAME_SIZE, MAX_RESUME_POINTS, PEER_DISPATCH_QUEUE_CAP, PEER_SESSION_GENERATION,
    PRESENCE_ROUND, PRESENCE_ROUND_FIRST, PRESENCE_ROUND_LAST, PROTOCOL_VERSION, PeerInfo,
    PeerRegistration, PresencePeerFrame, RESUME_TIMEOUT, ResumePoint, ResumeTopic, RoundPart,
    claim_link, decode_frame, encode_hello, encode_peer_announce, encode_peer_list, encode_ping,
    encode_pong, encode_replayed, encode_resume, epoch_ms, local_capabilities,
    negotiate_capabilities, negotiate_version, release_link,
};
use crate::reliability::{CircuitBreaker, ExponentialBackoff};

// ---------------------------------------------------------------------------
// Per-peer writer task (write coalescing: recv + try_recv drain)
// ---------------------------------------------------------------------------

pub(super) async fn peer_writer<W: AsyncWriteExt + Unpin>(
    mut rx: mpsc::Receiver<Bytes>,
    mut writer: W,
    cancel: CancellationToken,
    leaving: CancellationToken,
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
        // flush() is required for TLS -- rustls buffers encrypted data internally
        // and only writes to the TCP socket on flush. For plain TCP this is a no-op.
        // A peer that stops reading fills the socket: the write waits on it
        // only until the link is cancelled (its heartbeat ran out, say). The
        // buffer is consumed as it is written: what a cancel leaves of the
        // batch stays in it, whole frames behind the part on the wire.
        let written = tokio::select! {
            written = async {
                writer.write_all_buf(&mut buf).await?;
                writer.flush().await
            } => written,
            () = cancel.cancelled() => break,
        };
        if written.is_err() {
            return;
        }
        metrics.bytes_sent.fetch_add(bytes_count, Ordering::Relaxed);
    }
    if leaving.is_cancelled() {
        // The node leaves: the rest of the batch and what was queued after
        // it go out, the SHUTDOWN last, within a moment, then this side
        // closes, so the peer reads all of it.
        rx.close();
        let _ =
            tokio::time::timeout(LINK_END_WITHIN, write_out(&mut writer, &mut buf, &mut rx)).await;
    }
}

/// What a link of a node that leaves still writes: the rest of `buf`, then
/// what is queued, a batch at a time, then its close.
async fn write_out<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    buf: &mut BytesMut,
    rx: &mut mpsc::Receiver<Bytes>,
) -> std::io::Result<()> {
    loop {
        while buf.len() < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(data) => write_framed(buf, &data),
                Err(_) => break,
            }
        }
        if buf.is_empty() {
            break;
        }
        writer.write_all_buf(buf).await?;
    }
    writer.flush().await?;
    writer.shutdown().await
}

/// How long a link of a node that leaves still writes what was queued for
/// it, and reads what the peer still sends (so closing does not reset what
/// this side wrote last).
const LINK_END_WITHIN: Duration = Duration::from_secs(1);

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
    session: (Arc<AtomicU64>, u64, Arc<Heard>),
) {
    let _end_link = cancel.clone().drop_guard();
    let (last_activity, generation, heard) = session;
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
                arrival,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // Bounded dispatch: shed (don't block the reader) when local fan-out
                // falls behind, so a fast peer cannot grow an unbounded backlog -> OOM.
                // A shed message is not held here, and the next catch-up from
                // the peer starts before it; the reader remains responsive to
                // control/heartbeat frames on the same loop.
                let item = DispatchItem::Message(topic, payload, recovery, arrival);
                if let Err(shed) = dispatch_tx.try_send(item) {
                    ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                    heard.shed();
                    if let (DispatchItem::Message(topic, ..), Some(rm)) =
                        (shed.into_inner(), &ctx.recovery)
                    {
                        rm.not_kept(&topic);
                    }
                }
            }
            Some(ClusterFrame::Replayed {
                topic,
                payload,
                position,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // What this node missed while the link was down: delivered,
                // so it waits for room instead of being shed.
                let replayed = DispatchItem::Message(topic, payload, Some(position), None);
                if !to_manager(&dispatch_tx, replayed, &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::Lost { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // In order with the messages replayed before it.
                if !to_manager(&dispatch_tx, DispatchItem::Lost(topic), &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::CaughtUp { through }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // Heard once what came before it is delivered.
                if !to_manager(&dispatch_tx, DispatchItem::CaughtUp(through), &cancel).await {
                    break;
                }
            }
            Some(ClusterFrame::Resume { .. }) => {
                // Taken during the handshake only.
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
                // Its end is clean once what came before it is delivered:
                // the dispatch task says so when it gets there.
                let _ = to_manager(&dispatch_tx, DispatchItem::Shutdown, &cancel).await;
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
            Some(ClusterFrame::ProducerQuery { id, user, producer }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // The client went on on the asking node: handed over.
                let decided = ctx.producers.hand_over(user.as_deref(), &producer);
                let mut frame = BytesMut::new();
                super::protocol::encode_producer_state(&mut frame, id, decided.as_ref());
                let _ = peer_write_tx.try_send(frame.freeze());
            }
            Some(ClusterFrame::ProducerState { id, decided }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                ctx.producer_links.answer(id, decided);
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
                more,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let update = InterestUpdate::Resync {
                    peer_addr: peer_addr.clone(),
                    topics,
                    is_continuation,
                    more,
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
                    generation,
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
    // A node that leaves reads on, a moment, until the peer closes: a
    // socket closed with what the peer sent unread resets the link, and the
    // peer could lose what this side wrote last (its SHUTDOWN).
    if ctx.leaving.is_cancelled() {
        let _ = tokio::time::timeout(LINK_END_WITHIN, async {
            while let Ok(unread) = reader.fill_buf().await {
                let unread = unread.len();
                if unread == 0 {
                    break;
                }
                reader.consume(unread);
            }
        })
        .await;
    }
}

/// The instance met at `slot`, the address a node is reached at. Another
/// instance there than the one met last means that one is gone: unless it
/// said SHUTDOWN, or caught up since its last link ended, what it published
/// after what was heard of it is gone too, and a client here whose last
/// message came before is not told it missed nothing.
pub(super) fn met_at(ctx: &ClusterContext, slot: &str, instance: &str) {
    let before = ctx
        .incarnations
        .insert(slot.to_owned(), instance.to_owned());
    let Some(before) = before.filter(|before| before != instance) else {
        return;
    };
    let gone_unclean = ctx
        .heard
        .remove(&before)
        .is_some_and(|(_, heard)| heard.gone_unclean());
    if gone_unclean && let Some(rm) = &ctx.recovery {
        tracing::warn!(
            "[WSE-Cluster] {slot} is another node now, and the last went without SHUTDOWN: what it published meanwhile is gone"
        );
        rm.lost_everything();
    }
}

/// Hand an update to the manager, waiting while its queue is full: a peer
/// sending faster than the manager keeps up is slowed down at its socket,
/// not buffered without bound. False once the link ends.
async fn to_manager<T>(tx: &mpsc::Sender<T>, item: T, cancel: &CancellationToken) -> bool {
    tokio::select! {
        // An item with room for it goes, even once the link ends.
        biased;
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
// Per-peer dispatch task: receives decoded (topic, payload) from the reader,
// stores it for recovery and hands it to the lanes that deliver it. Decouples
// TCP reads from the delivery.
// ---------------------------------------------------------------------------

pub(super) async fn peer_dispatch_task(
    mut dispatch_rx: mpsc::Receiver<DispatchItem>,
    ctx: ClusterContext,
    cancel: CancellationToken,
    heard: Arc<Heard>,
) {
    let _end_link = cancel.clone().drop_guard();
    let mut batch: Vec<DispatchItem> = Vec::with_capacity(64);

    loop {
        // What the reader took goes first, the link's end after it: a
        // message read before the end is still delivered, and a SHUTDOWN is
        // only taken once all that came before it is here.
        let msg = tokio::select! {
            biased;
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

        // Queue-group deliveries, counted here; the lanes count the rest.
        let mut total_delivered = 0u64;
        let mut total_dropped = 0u64;

        for item in batch.drain(..) {
            let (topic, payload, rec, arrival) = match item {
                DispatchItem::Message(topic, payload, rec, arrival) => {
                    (topic, payload, rec, arrival)
                }
                DispatchItem::Lost(topic) => {
                    if let Some(rm) = &ctx.recovery {
                        rm.lost(&topic);
                    }
                    continue;
                }
                DispatchItem::CaughtUp(through) => {
                    heard.caught_up(through);
                    continue;
                }
                DispatchItem::Shutdown => {
                    heard.shut_down();
                    continue;
                }
            };
            // Heard of once it is here: delivered below, or held already.
            if let Some(seq) = arrival {
                heard.live(seq);
            }
            // Store the unframed payload in the foreign recovery buffer
            // (cross-node recovery), the form the local buffer stores too.
            // One this node has already (a peer catching it up sends from
            // where it stands with nothing missing) is not delivered again.
            // One with no position (its node had no room to keep it) is
            // held nowhere: a client here whose last message of the topic
            // came before it is not told it missed nothing.
            if let Some(rm) = &ctx.recovery {
                match rec {
                    Some((epoch, offset)) => {
                        if !rm.push_foreign(&topic, payload.clone(), epoch, offset) {
                            continue;
                        }
                    }
                    None => rm.not_kept(&topic),
                }
            }

            // Per-topic message count for the Prometheus top-N metrics.
            crate::server::count_message(&ctx.topic_message_counts, &topic);

            // Queue groups: one member each, round-robin, without the
            // recovery stamp (a member sees only some of the offsets).
            let (d, f) = ctx
                .fanout
                .queue_groups(
                    &ctx.queue_groups,
                    &topic,
                    || crate::server::Outgoing::text(crate::server::unstamped(&payload)),
                    ctx.max_outbound,
                    ctx.slow_drops.shard(0),
                )
                .await;
            total_delivered += d;
            total_dropped += f;

            ctx.fanout
                .publish(
                    topic,
                    || crate::server::Outgoing::text(payload),
                    Some(&ctx.metrics),
                )
                .await;
        }

        if total_delivered > 0 {
            ctx.metrics.add_delivered(total_delivered);
        }
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

/// What a peer's RESUME request asks for: where it stands in the streams it
/// holds, and the topics it has (CAP_RESUME_TOPICS).
#[derive(Debug, Default)]
pub(super) struct Wanted {
    points: Vec<ResumePoint>,
    topics: Vec<ResumeTopic>,
    /// The last of this node's arrivals the peer heard of (CAP_ARRIVAL).
    heard: Option<u64>,
}

/// The catch-up on a new link (CAP_RESUME): this node's RESUME request goes
/// out while the peer's comes in, so a large one each way cannot stall on
/// the other. With CAP_RESUME_TOPICS (`peer_caps`) it names the topics this
/// node has too. The peer's request, or None when it did not come whole in
/// time.
pub(super) async fn exchange_resume<R, W>(
    reader: &mut R,
    writer: &mut W,
    ctx: &ClusterContext,
    peer: (u32, &Heard),
) -> Option<Wanted>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let (peer_caps, heard) = peer;
    let (points, topics) = ctx.recovery.as_ref().map_or_else(Default::default, |rm| {
        let topics = if peer_caps & CAP_RESUME_TOPICS != 0 {
            rm.interests()
        } else {
            Vec::new()
        };
        (rm.foreign_positions(), topics)
    });
    let heard = (peer_caps & CAP_ARRIVAL != 0).then(|| heard.asked());
    let mut request = BytesMut::new();
    for frame in encode_resume(&points, &topics, heard) {
        write_framed(&mut request, &frame);
    }
    let send = async {
        writer.write_all(&request).await?;
        writer.flush().await
    };
    let receive = async {
        let mut wanted = Wanted::default();
        loop {
            let Ok(Some(ClusterFrame::Resume {
                points,
                topics,
                heard,
                last,
            })) = read_frame(reader).await
            else {
                return None;
            };
            wanted.heard = wanted.heard.max(heard);
            let asked = wanted.points.len() + wanted.topics.len();
            if asked + points.len() + topics.len() > MAX_RESUME_POINTS {
                return None;
            }
            wanted.points.extend(points);
            wanted.topics.extend(topics);
            if last {
                return Some(wanted);
            }
        }
    };
    // Both ways within the time: a peer that does not read ours holds the
    // link no longer than one that does not send its own.
    let (sent, received) =
        tokio::time::timeout(RESUME_TIMEOUT, async { tokio::join!(send, receive) })
            .await
            .ok()?;
    sent.ok()?;
    received
}

/// A write to a peer that must make progress within the heartbeat timeout:
/// one that stopped reading does not hold the link for good.
async fn write_progressing<W>(writer: &mut W, bytes: &[u8]) -> std::io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let stalled = || std::io::Error::new(std::io::ErrorKind::TimedOut, "the peer stopped reading");
    let within = Duration::from_secs(HEARTBEAT_TIMEOUT_SECS);
    tokio::time::timeout(within, writer.write_all(bytes))
        .await
        .map_err(|_| stalled())??;
    tokio::time::timeout(within, writer.flush())
        .await
        .map_err(|_| stalled())?
}

/// The replayed messages being written to a link, in batches.
struct Replay {
    out: BytesMut,
    frame: BytesMut,
    sent: u64,
    bytes: u64,
}

impl Replay {
    async fn push<W>(
        &mut self,
        writer: &mut W,
        topic: &str,
        position: (u32, u64),
        data: &[u8],
    ) -> std::io::Result<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let Ok(payload) = std::str::from_utf8(data) else {
            return Ok(());
        };
        self.frame.clear();
        if !encode_replayed(&mut self.frame, topic, payload, position.0, position.1) {
            return Ok(());
        }
        write_framed(&mut self.out, &self.frame);
        self.sent += 1;
        if self.out.len() >= MAX_BATCH_BYTES {
            write_progressing(writer, &self.out).await?;
            self.bytes += self.out.len() as u64;
            self.out.clear();
        }
        Ok(())
    }
}

/// What the peer missed of the streams this node publishes, after where its
/// RESUME request says it stands, and of the topics it has, what this node
/// published to them since its interest began in a stream it did not name
/// (one begun while the link was down): written to the link before anything
/// live goes to it.
pub(super) async fn write_replay<W>(
    writer: &mut W,
    wanted: &Wanted,
    ctx: &ClusterContext,
) -> std::io::Result<u64>
where
    W: AsyncWriteExt + Unpin,
{
    let Some(rm) = &ctx.recovery else {
        return Ok(0);
    };
    // The manager routes to the link already: what this node publishes from
    // here on reaches the peer live, after this.
    let through = rm.arrived_through();
    let mut replay = Replay {
        out: BytesMut::with_capacity(MAX_BATCH_BYTES),
        frame: BytesMut::new(),
        sent: 0,
        bytes: 0,
    };
    for (topic, epoch, after) in &wanted.points {
        for (offset, data) in rm.published_after(topic, *epoch, *after) {
            replay.push(writer, topic, (*epoch, offset), &data).await?;
        }
    }
    let named: std::collections::HashSet<(&str, u32)> = wanted
        .points
        .iter()
        .map(|(topic, epoch, _)| (topic.as_str(), *epoch))
        .collect();
    // Where the peer stands in this node's arrival order for a topic it has
    // since `age` ago: past what it heard of this node, and past where its
    // interest began (what came before was never relayed to it).
    let heard = wanted.heard.unwrap_or(0);
    let unheard_after = |age: u64| heard.max(rm.arrived_before(Duration::from_millis(age)));
    // Each topic of this node's the peer has, by its name or by a pattern
    // that matches it: once, from the earliest of those.
    let mut unheard: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for (topic, age) in &wanted.topics {
        let after = unheard_after(*age);
        let matched = if crate::server::is_pattern(topic) {
            rm.own_topics_matching(topic)
        } else {
            vec![topic.clone()]
        };
        for topic in matched {
            let from = unheard.entry(topic).or_insert(u64::MAX);
            *from = (*from).min(after);
        }
    }
    for (topic, after) in &unheard {
        let named = |epoch: u32| named.contains(&(topic.as_str(), epoch));
        let Some((epoch, messages)) = rm.published_unheard(topic, *after, named) else {
            continue;
        };
        for (offset, data) in messages {
            replay.push(writer, topic, (epoch, offset), &data).await?;
        }
    }
    // Of each topic the peer has, by its name or a pattern, what it did not
    // hear of and this node no longer holds, or never could: after what was
    // replayed, so a client of the peer whose last message came before it is
    // not told it missed nothing.
    let mut lost: Vec<String> = wanted
        .topics
        .iter()
        .flat_map(|(topic, age)| rm.lost_after(topic, unheard_after(*age)))
        .collect();
    lost.sort_unstable();
    lost.dedup();
    for name in &lost {
        replay.frame.clear();
        if encode_lost(&mut replay.frame, name) {
            write_framed(&mut replay.out, &replay.frame);
        }
    }
    // The peer heard of this node up to here: a later catch-up starts after.
    if wanted.heard.is_some() {
        replay.frame.clear();
        super::protocol::encode_caught_up(&mut replay.frame, through);
        write_framed(&mut replay.out, &replay.frame);
    }
    write_progressing(writer, &replay.out).await?;
    let bytes = replay.bytes + replay.out.len() as u64;
    ctx.metrics
        .messages_sent
        .fetch_add(replay.sent, Ordering::Relaxed);
    ctx.metrics.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    Ok(replay.sent)
}

/// Hand a live link to the manager, which routes publishes, interest and
/// presence to it from then on, starting with this node's RESYNC and
/// presence: they wait in the link's channel until its writer runs. With a
/// catch-up owed, the peer first gets what it missed, taken from the
/// recovery buffers once the manager routes to the link: every message is
/// in one or the other, and one in both is delivered there once. False when
/// the link failed meanwhile.
pub(super) async fn go_live<W>(
    writer: &mut W,
    peer_reg_tx: &mpsc::UnboundedSender<PeerRegistration>,
    link: (&str, mpsc::Sender<Bytes>, u32, CancellationToken),
    wanted: Option<Wanted>,
    ctx: &ClusterContext,
) -> bool
where
    W: AsyncWriteExt + Unpin,
{
    let (peer_addr, write_tx, negotiated_caps, link) = link;
    let (taken, routed) = match wanted {
        Some(_) => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            (Some(tx), Some(rx))
        }
        None => (None, None),
    };
    let _ = peer_reg_tx.send(PeerRegistration::Register {
        peer_addr: peer_addr.to_owned(),
        write_tx,
        negotiated_caps,
        taken,
        link,
    });
    let (Some(wanted), Some(routed)) = (wanted, routed) else {
        return true;
    };
    // An Err: the manager stopped, and the cluster with it.
    let _ = routed.await;
    match write_replay(writer, &wanted, ctx).await {
        Ok(0) => true,
        Ok(sent) => {
            tracing::info!("[WSE-Cluster] Sent {peer_addr} the {sent} messages it missed");
            true
        }
        Err(_) => false,
    }
}

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

    let local_caps = local_capabilities(ctx);
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, local_caps, &ctx.reached_at);
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
            ..
        }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
            Some(v) => (peer_id, v, negotiate_capabilities(local_caps, capabilities)),
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

    met_at(ctx, peer_addr, &peer_instance_id);

    let peer_cancel = global_cancel.child_token();
    let session_gen = PEER_SESSION_GENERATION.fetch_add(1, Ordering::AcqRel);
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));
    let link = Link {
        dialer: instance_id.to_owned(),
        session: session_gen,
        cancel: peer_cancel.clone(),
        activity: Arc::clone(&last_activity),
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

    // What this node heard of the peer, kept across its links.
    let heard = Arc::clone(&ctx.heard.entry(peer_instance_id.clone()).or_default());
    // Catch-up: each node first says where it stands in the other's streams.
    let wanted = if peer_caps & CAP_RESUME != 0 {
        let resumed = exchange_resume(&mut reader, &mut writer, ctx, (peer_caps, &heard)).await;
        let Some(wanted) = resumed else {
            tracing::warn!("[WSE-Cluster] No RESUME from {peer_addr} within {RESUME_TIMEOUT:?}");
            release_link(&ctx.links, &peer_instance_id, session_gen);
            return Outcome::Failed;
        };
        Some(wanted)
    } else {
        None
    };
    heard.linked();

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
    if peer_caps & super::CAP_PRODUCERS != 0 {
        ctx.producer_links
            .register(&peer_instance_id, peer_write_tx.clone(), session_gen);
    }

    let (dispatch_tx, dispatch_rx) = mpsc::channel::<DispatchItem>(PEER_DISPATCH_QUEUE_CAP);
    let dispatch_handle = tokio::spawn(peer_dispatch_task(
        dispatch_rx,
        ctx.clone(),
        peer_cancel.clone(),
        Arc::clone(&heard),
    ));

    let reader_handle = tokio::spawn(peer_reader(
        reader,
        peer_write_tx.clone(),
        peer_addr.to_owned(),
        ctx.clone(),
        dispatch_tx,
        peer_cancel.clone(),
        (last_activity.clone(), session_gen, Arc::clone(&heard)),
    ));

    if !go_live(
        &mut writer,
        peer_reg_tx,
        (
            peer_addr,
            peer_write_tx.clone(),
            peer_caps,
            peer_cancel.clone(),
        ),
        wanted,
        ctx,
    )
    .await
    {
        peer_cancel.cancel();
    }

    let writer_handle = tokio::spawn(peer_writer(
        peer_write_rx,
        writer,
        peer_cancel.clone(),
        global_cancel.clone(),
        ctx.metrics.clone(),
    ));

    let heartbeat_handle = tokio::spawn(heartbeat_task(
        peer_write_tx,
        peer_cancel.clone(),
        last_activity,
    ));

    // The link ends when any of its tasks ends, when a better duplicate
    // replaces it, or on shutdown (the token is a child of the global one).
    peer_cancel.cancelled().await;
    let _ = writer_handle.await;
    let _ = reader_handle.await;
    let _ = dispatch_handle.await;
    let _ = heartbeat_handle.await;
    heard.ended();

    let _ = peer_reg_tx.send(PeerRegistration::Deregister {
        peer_addr: peer_addr.to_owned(),
    });
    ctx.metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    ctx.metrics.peer_info.remove(peer_addr);
    ctx.producer_links.release(&peer_instance_id, session_gen);
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
    /// The manager's link tasks: a dial task runs among them.
    pub(super) links: tokio_util::task::TaskTracker,
}

impl PeerSpawnCtx {
    /// Start dialing `peer_addr`. A static peer (from the configuration) is
    /// dialed for as long as the node runs; one learned through gossip is
    /// given up after it stays unreachable.
    pub(super) fn spawn(&self, peer_addr: String, is_static: bool) -> tokio::task::JoinHandle<()> {
        self.links.spawn(peer_connection_task(
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
