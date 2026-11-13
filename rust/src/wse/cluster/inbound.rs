//! Inbound peer links: the handshake and session of a peer that dialed us, on
//! the cluster port or (legacy) on the main port.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::peer::{heartbeat_task, peer_dispatch_task, peer_reader, peer_writer, read_frame};
use super::protocol::write_framed;
use super::{
    CAP_PRESENCE, ClusterCommand, ClusterContext, ClusterFrame, DispatchItem, InterestUpdate,
    LOCAL_CAPABILITIES, Link, LinkRefused, Links, MAX_FRAME_SIZE, PEER_DISPATCH_QUEUE_CAP,
    PEER_SESSION_GENERATION, PROTOCOL_VERSION, PeerInfo, PeerRegistration, PresencePeerFrame,
    claim_link, encode_hello, encode_peer_announce, encode_peer_list, encode_presence_full,
    encode_resync, epoch_ms, negotiate_capabilities, negotiate_version, release_link,
};

// ---------------------------------------------------------------------------
// Inbound cluster connection handler (generic over transport)
// ---------------------------------------------------------------------------

/// Handle inbound cluster peer connection. Generic over stream type (TLS or plaintext).
pub(super) async fn handle_cluster_inbound_generic<S>(
    stream: S,
    addr: SocketAddr,
    instance_id: &str,
    ctx: ClusterContext,
    peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
    global_cancel: CancellationToken,
    presence: Option<Arc<crate::wse::presence::PresenceManager>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Read peer's HELLO frame
    let peer_hello =
        match tokio::time::timeout(Duration::from_secs(5), read_frame(&mut reader)).await {
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
                    "[WSE-Cluster] Version mismatch with inbound {addr}: local={PROTOCOL_VERSION}, remote={protocol_version}"
                );
                return;
            }
        },
        _ => {
            tracing::warn!("[WSE-Cluster] Invalid HELLO from inbound {addr}");
            return;
        }
    };

    // Claim the link. Refused, it closes before our HELLO: the dialer keeps
    // retrying with backoff, in case the link that won goes away.
    let cancel = global_cancel.child_token();
    let session_gen = PEER_SESSION_GENERATION.fetch_add(1, Ordering::AcqRel);
    let link = Link {
        dialer: peer_instance_id.clone(),
        session: session_gen,
        cancel: cancel.clone(),
    };
    match claim_link(&ctx.links, instance_id, &peer_instance_id, link) {
        Ok(()) => {}
        Err(LinkRefused::SelfLink) => {
            tracing::debug!("[WSE-Cluster] Closing inbound from {addr}: it is this node");
            return;
        }
        Err(LinkRefused::Duplicate) => {
            tracing::debug!(
                "[WSE-Cluster] Closing inbound from {addr}: already linked to instance {peer_instance_id}"
            );
            return;
        }
    }

    // Send our HELLO response
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
        tracing::error!("[WSE-Cluster] Failed to send HELLO to inbound {addr}");
        release_link(&ctx.links, &peer_instance_id, session_gen);
        return;
    }

    // Send RESYNC with all current local topics (before spawning writer task)
    {
        let local_topics: Vec<String> = {
            let refcounts = ctx
                .local_topic_refcount
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            refcounts.keys().cloned().collect()
        };
        {
            // Sent even when empty: until a peer's first RESYNC arrives, it is
            // sent every publish.
            let resync_frames = encode_resync(&local_topics);
            let mut wire_buf = BytesMut::new();
            for frame in &resync_frames {
                write_framed(&mut wire_buf, frame);
            }
            if writer.write_all(&wire_buf).await.is_err() || writer.flush().await.is_err() {
                tracing::warn!("[WSE-Cluster] Failed to send RESYNC to inbound {addr}");
                release_link(&ctx.links, &peer_instance_id, session_gen);
                return;
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
                    tracing::warn!(
                        "[WSE-Cluster] Failed to send PresenceFull to inbound {addr}: {e}"
                    );
                }
                let _ = writer.flush().await;
            }
        }
    }

    tracing::info!(
        "[WSE-Cluster] Accepted inbound peer from {addr} (v{_peer_version}, caps=0x{peer_caps:08x})"
    );
    let peer_addr_str = addr.to_string();
    ctx.metrics.connected_peers.fetch_add(1, Ordering::Relaxed);
    ctx.metrics.peer_info.insert(
        peer_addr_str.clone(),
        PeerInfo {
            address: peer_addr_str.clone(),
            instance_id: peer_instance_id.clone(),
            capabilities: peer_caps,
            connected_at: epoch_ms(),
        },
    );

    let (write_tx, write_rx) = mpsc::channel::<Bytes>(10_000);
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

    // Register this inbound peer's write channel with the cluster manager
    // so it receives Publish/Sub/Unsub frames (bidirectional routing)
    let _ = peer_reg_tx.send(PeerRegistration::Register {
        peer_addr: peer_addr_str.clone(),
        write_tx: write_tx.clone(),
        negotiated_caps: peer_caps,
    });

    let writer_handle = tokio::spawn(peer_writer(
        write_rx,
        writer,
        cancel.clone(),
        ctx.metrics.clone(),
    ));

    let (dispatch_tx, dispatch_rx) = mpsc::channel::<DispatchItem>(PEER_DISPATCH_QUEUE_CAP);

    let dispatch_handle =
        tokio::spawn(peer_dispatch_task(dispatch_rx, ctx.clone(), cancel.clone()));

    let reader_handle = tokio::spawn(peer_reader(
        reader,
        write_tx.clone(),
        peer_addr_str.clone(),
        ctx.clone(),
        dispatch_tx,
        cancel.clone(),
        (last_activity.clone(), session_gen),
    ));

    let heartbeat_handle = tokio::spawn(heartbeat_task(
        write_tx,
        cancel.clone(),
        last_activity,
        ctx.local_topic_refcount.clone(),
    ));

    // The link ends when any of its tasks ends (each cancels the token as it
    // exits), when a better duplicate replaces it, or on shutdown. Only then
    // are the tasks awaited, so none is polled again after it finished.
    cancel.cancelled().await;
    let _ = reader_handle.await;
    let _ = writer_handle.await;
    let _ = dispatch_handle.await;
    let _ = heartbeat_handle.await;

    // Deregister inbound peer from the cluster manager's peer_txs
    let _ = peer_reg_tx.send(PeerRegistration::Deregister {
        peer_addr: peer_addr_str.clone(),
    });
    release_link(&ctx.links, &peer_instance_id, session_gen);
    ctx.metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    ctx.metrics.peer_info.remove(&peer_addr_str);
    let _ = ctx.interest_tx.send(InterestUpdate::PeerDisconnected {
        peer_addr: peer_addr_str,
        generation: session_gen,
    });
    tracing::info!("[WSE-Cluster] Inbound peer {addr} disconnected");
}

/// Legacy inbound handler: used when cluster connections arrive on the main port (no separate cluster listener).
/// Kept for backwards compatibility when cluster_port is not configured.
pub(crate) async fn handle_cluster_inbound(
    stream: TcpStream,
    addr: SocketAddr,
    shared: &crate::wse::server::SharedState,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
) {
    let instance_id = match shared
        .cluster_instance_id
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
    {
        Some(id) => id,
        None => {
            tracing::warn!("[WSE-Cluster] Rejected inbound from {addr}: cluster not configured");
            return;
        }
    };

    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(5));
    let sock_ref = SockRef::from(&stream);
    let _ = sock_ref.set_tcp_keepalive(&keepalive);
    let _ = sock_ref.set_recv_buffer_size(262_144);
    let _ = sock_ref.set_send_buffer_size(262_144);

    // Legacy path: no cluster_manager, so create dummy channels for discovery and registration
    // (messages sent here are just dropped -- discovery/registration only works with cluster_port)
    let (new_peer_tx, _new_peer_rx) = mpsc::unbounded_channel::<String>();
    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
    let (peer_reg_tx, _peer_reg_rx) = mpsc::unbounded_channel::<PeerRegistration>();
    let (presence_tx, _presence_rx) = mpsc::unbounded_channel::<PresencePeerFrame>();
    // Shared across all legacy inbound calls so duplicate detection actually works.
    // Note: this static persists across server restarts within the same process, but entries
    // are always cleaned up in handle_cluster_inbound_generic on disconnect.
    // Legacy path is mutually exclusive with connect_cluster (which uses its own DashMap).
    static LEGACY_LINKS: std::sync::OnceLock<Arc<Links>> = std::sync::OnceLock::new();
    let links = LEGACY_LINKS
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone();
    // Legacy path has no global_cancel -- create a standalone token.
    // Cleanup happens via tokio runtime drop on server stop.
    let legacy_cancel = CancellationToken::new();

    // Legacy path has no gossip discovery
    let legacy_known_peers: Arc<DashSet<String>> = Arc::new(DashSet::new());

    let ctx = ClusterContext {
        interest_tx,
        topic_subscribers: shared.topic_subscribers.clone(),
        glob_topic_count: shared.glob_topic_count.clone(),
        metrics: shared.cluster_metrics.clone(),
        new_peer_tx,
        cmd_tx,
        presence_tx,
        links,
        known_peers: legacy_known_peers,
        local_topic_refcount: shared.local_topic_refcount.clone(),
        cluster_addr: None,
        max_outbound: shared.max_outbound_queue_bytes,
        slow_drops: Arc::clone(&shared.slow_consumer_drops),
        recovery: shared.recovery.clone(),
        topic_message_counts: shared.topic_message_counts.clone(),
        queue_groups: shared.queue_groups.clone(),
    };

    handle_cluster_inbound_generic(
        stream,
        addr,
        &instance_id,
        ctx,
        peer_reg_tx,
        legacy_cancel,
        shared.presence.clone(),
    )
    .await;
}
