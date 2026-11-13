//! Inbound peer links: their admission, and the handshake and session of a
//! peer that dialed us, on the cluster port or, in single-port mode, on the
//! WebSocket port.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use socket2::SockRef;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::peer::{heartbeat_task, peer_dispatch_task, peer_reader, peer_writer, read_frame};
use super::protocol::write_framed;
use super::{
    CAP_PRESENCE, ClusterContext, ClusterFrame, ClusterTlsConfig, DispatchItem, InterestUpdate,
    LOCAL_CAPABILITIES, Link, LinkRefused, MAX_CLUSTER_PEERS, MAX_FRAME_SIZE,
    PEER_DISPATCH_QUEUE_CAP, PEER_SESSION_GENERATION, PROTOCOL_VERSION, PeerInfo, PeerRegistration,
    claim_link, encode_hello, encode_peer_announce, encode_peer_list, encode_presence_full,
    encode_resync, epoch_ms, negotiate_capabilities, negotiate_version, release_link,
};

// ---------------------------------------------------------------------------
// Inbound links
// ---------------------------------------------------------------------------

/// Where this node takes links from its peers.
pub(crate) enum InboundLinks {
    /// Its own cluster port, bound by connect_cluster().
    Listener(tokio::net::TcpListener),
    /// The WebSocket port (single-port mode): it hands over every connection
    /// that starts like a cluster frame.
    MainPort(mpsc::UnboundedReceiver<(tokio::net::TcpStream, SocketAddr)>),
}

/// Admission of inbound links. Between accept and HELLO a link holds one of
/// `MAX_PENDING_HELLOS` slots, and one address no more than
/// `MAX_PENDING_HELLOS_PER_IP` of them, so junk connections from one host
/// cannot keep the real peers out; past HELLO it holds one of
/// `MAX_CLUSTER_PEERS` sessions.
#[derive(Default)]
pub(super) struct InboundGate {
    pending: std::sync::Mutex<HashMap<IpAddr, usize>>,
    pending_total: std::sync::atomic::AtomicUsize,
    sessions: std::sync::atomic::AtomicUsize,
}

const MAX_PENDING_HELLOS: usize = 64;
const MAX_PENDING_HELLOS_PER_IP: usize = 4;

/// A link's place in the gate until HELLO; `session()` trades it for a
/// session slot.
pub(super) struct Admitted {
    gate: Arc<InboundGate>,
    ip: IpAddr,
    pending: bool,
    session: bool,
}

impl InboundGate {
    pub(super) fn admit(self: &Arc<Self>, addr: SocketAddr) -> Option<Admitted> {
        let ip = addr.ip();
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let from_ip = pending.entry(ip).or_insert(0);
        if *from_ip >= MAX_PENDING_HELLOS_PER_IP
            || self.pending_total.load(Ordering::Relaxed) >= MAX_PENDING_HELLOS
        {
            if *from_ip == 0 {
                pending.remove(&ip);
            }
            tracing::warn!("[WSE-Cluster] Too many links waiting for HELLO, refusing {addr}");
            return None;
        }
        *from_ip += 1;
        self.pending_total.fetch_add(1, Ordering::Relaxed);
        Some(Admitted {
            gate: Arc::clone(self),
            ip,
            pending: true,
            session: false,
        })
    }
}

impl Admitted {
    fn leave_pending(&mut self) {
        if std::mem::take(&mut self.pending) {
            let mut pending = self.gate.pending.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(from_ip) = pending.get_mut(&self.ip) {
                *from_ip -= 1;
                if *from_ip == 0 {
                    pending.remove(&self.ip);
                }
            }
            self.gate.pending_total.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// HELLO arrived: a session slot, or false when all are taken.
    fn session(&mut self) -> bool {
        self.leave_pending();
        let taken = self
            .gate
            .sessions
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < MAX_CLUSTER_PEERS).then_some(n + 1)
            })
            .is_ok();
        self.session = taken;
        taken
    }
}

impl Drop for Admitted {
    fn drop(&mut self) {
        self.leave_pending();
        if self.session {
            self.gate.sessions.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// What an inbound session needs from the manager.
#[derive(Clone)]
pub(super) struct InboundSessionCtx {
    pub(super) instance_id: String,
    pub(super) ctx: ClusterContext,
    pub(super) peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
    pub(super) global_cancel: CancellationToken,
    pub(super) presence: Option<Arc<crate::wse::presence::PresenceManager>>,
}

impl InboundSessionCtx {
    pub(super) async fn run<S>(self, stream: S, addr: SocketAddr, admitted: Admitted)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        handle_cluster_inbound_generic(stream, addr, self, admitted).await;
    }
}

/// Accept links on the cluster port until the cluster shuts down. An accept
/// error (out of file descriptors, say) is retried after a pause that grows
/// from 10 ms to a second, and logged once per pause.
pub(super) async fn accept_cluster_links(
    listener: tokio::net::TcpListener,
    tls: Option<Arc<arc_swap::ArcSwap<ClusterTlsConfig>>>,
    gate: Arc<InboundGate>,
    inbound: InboundSessionCtx,
) {
    let cancel = inbound.global_cancel.clone();
    let mut pause = Duration::ZERO;
    loop {
        let accepted = tokio::select! {
            res = listener.accept() => res,
            () = cancel.cancelled() => break,
        };
        let (stream, addr) = match accepted {
            Ok(link) => {
                pause = Duration::ZERO;
                link
            }
            Err(e) => {
                pause = (pause * 2).clamp(Duration::from_millis(10), Duration::from_secs(1));
                tracing::error!("[WSE-Cluster] Cluster accept error: {e} (retrying in {pause:?})");
                tokio::select! {
                    () = tokio::time::sleep(pause) => continue,
                    () = cancel.cancelled() => break,
                }
            }
        };
        let Some(admitted) = gate.admit(addr) else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        let sock = SockRef::from(&stream);
        let _ = sock.set_send_buffer_size(262_144);
        let _ = sock.set_recv_buffer_size(262_144);
        let tls_snap = tls.as_ref().map(|holder| holder.load_full());
        let inbound = inbound.clone();
        tokio::spawn(async move {
            match tls_snap {
                Some(tls_cfg) => {
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        tls_cfg.acceptor.accept(stream),
                    )
                    .await
                    {
                        Ok(Ok(tls_stream)) => inbound.run(tls_stream, addr, admitted).await,
                        Ok(Err(e)) => {
                            tracing::warn!("[WSE-Cluster] TLS accept failed from {addr}: {e}");
                        }
                        Err(_) => tracing::warn!("[WSE-Cluster] TLS accept timeout from {addr}"),
                    }
                }
                None => inbound.run(stream, addr, admitted).await,
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Inbound cluster connection handler (generic over transport)
// ---------------------------------------------------------------------------

/// Handle inbound cluster peer connection. Generic over stream type (TLS or plaintext).
async fn handle_cluster_inbound_generic<S>(
    stream: S,
    addr: SocketAddr,
    session: InboundSessionCtx,
    mut admitted: Admitted,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let InboundSessionCtx {
        instance_id,
        ctx,
        peer_reg_tx,
        global_cancel,
        presence,
    } = session;
    let instance_id: &str = &instance_id;
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

    if !admitted.session() {
        tracing::warn!(
            "[WSE-Cluster] Refusing inbound {addr}: {MAX_CLUSTER_PEERS} peer sessions already"
        );
        return;
    }

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
