//! The cluster manager: owns every peer link and routes commands, interest,
//! gossip, registrations and presence between them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::inbound::{InboundGate, InboundLinks, InboundSessionCtx, accept_cluster_links};
use super::peer::PeerSpawnCtx;
use super::{
    CAP_COMPRESSION, CAP_INTEREST_ROUTING, CAP_PRESENCE, CAP_RECOVERY,
    CLUSTER_RESYNC_INTERVAL_SECS, ClusterCommand, ClusterContext, ClusterDlq, ClusterDlqEntry,
    ClusterTlsConfig, InterestUpdate, MANAGER_QUEUE_CAP, MAX_CLUSTER_PEERS, MAX_FRAME_SIZE,
    MAX_REMOTE_INTEREST_PER_PEER, PRESENCE_ROUND_FIRST, PRESENCE_ROUND_LAST, PeerInterest,
    PeerRegistration, PresencePeerFrame, encode_drain, encode_msg, encode_msg_compressed,
    encode_msg_with_recovery, encode_peer_announce, encode_presence_round, encode_presence_update,
    encode_resync, encode_shutdown, encode_sub, encode_unsub,
};
use crate::wse::presence::PresenceManager;
use crate::wse::server::subscribe::MAX_TOPIC_LEN;

/// Queue `frames` on a link whole or not at all: a RESYNC or a presence
/// round cut short would leave the peer with part of the set.
pub(super) fn queue_whole(tx: &mpsc::Sender<Bytes>, frames: &[Bytes]) -> bool {
    match tx.try_reserve_many(frames.len()) {
        Ok(permits) => {
            for (permit, frame) in permits.zip(frames) {
                permit.send(frame.clone());
            }
            true
        }
        Err(_) => false,
    }
}

/// This node's interest as RESYNC frames; sent even when empty: until a
/// peer's first RESYNC arrives, it gets every publish.
fn interest_frames(local: &std::sync::Mutex<HashMap<String, usize>>) -> Vec<Bytes> {
    let topics: Vec<String> = local
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect();
    encode_resync(&topics)
        .into_iter()
        .map(BytesMut::freeze)
        .collect()
}

/// This node's local presence as one sync round: the receiver keeps of this
/// node exactly what the round holds. An empty state is a round too.
pub(super) fn presence_round(pm: &PresenceManager) -> Vec<Bytes> {
    let mut chunks: Vec<String> = pm
        .serialize_full_state_chunked(MAX_FRAME_SIZE - 1024)
        .into_iter()
        .filter(|chunk| chunk.len() <= MAX_FRAME_SIZE - 8)
        .collect();
    if chunks.is_empty() {
        chunks.push("{}".to_owned());
    }
    let last = chunks.len() - 1;
    chunks
        .iter()
        .enumerate()
        .filter_map(|(i, entries)| {
            let mut edges = 0;
            if i == 0 {
                edges |= PRESENCE_ROUND_FIRST;
            }
            if i == last {
                edges |= PRESENCE_ROUND_LAST;
            }
            let mut buf = BytesMut::new();
            encode_presence_round(&mut buf, entries, edges).then(|| buf.freeze())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Cluster manager: orchestrates all peer connections
// ---------------------------------------------------------------------------

/// Broadcast a presence event of a remote user to the topic's local
/// subscribers.
fn tell_presence(
    server_cmd_tx: &Option<crate::wse::server::Commands>,
    kind: &str,
    topic: String,
    user_id: &str,
    data: &serde_json::Value,
) {
    if let Some(tx) = server_cmd_tx {
        let _ = tx.send(crate::wse::server::ServerCommand::BroadcastLocal {
            topic,
            data: crate::wse::server::format_presence_msg(kind, user_id, data),
            skip_recovery: true,
            publish: false,
        });
    }
}

pub(crate) async fn cluster_manager(
    peers: Vec<String>,
    instance_id: String,
    channels: (
        mpsc::UnboundedReceiver<ClusterCommand>,
        mpsc::Receiver<InterestUpdate>,
    ),
    ctx: ClusterContext,
    dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    tls_config: Option<Arc<arc_swap::ArcSwap<ClusterTlsConfig>>>,
    cluster_opts: (
        InboundLinks,
        Option<Arc<crate::wse::presence::PresenceManager>>,
        Option<crate::wse::server::Commands>,
    ),
) {
    let (mut cmd_rx, mut interest_rx) = channels;
    let (inbound_links, presence, server_cmd_tx) = cluster_opts;
    let global_cancel = CancellationToken::new();

    // Remote interest: the topics each peer has subscribers for. A peer with
    // no entry yet (no RESYNC received) gets every publish.
    let mut remote_interest: HashMap<String, PeerInterest> = HashMap::new();
    // Peers in drain mode: they get nothing new until they disconnect.
    let mut draining: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Track the latest session generation per peer to filter stale RESYNC continuations
    let mut peer_generation: HashMap<String, u64> = HashMap::new();
    // Peers that missed a presence frame of this node (a full link): they get
    // a sync round on the next tick.
    let mut presence_dirty: HashSet<String> = HashSet::new();
    // Presence sync rounds being received, per origin: the link session they
    // came on and the users of each topic seen so far.
    let mut presence_rounds: HashMap<String, (u64, HashMap<String, HashSet<String>>)> =
        HashMap::new();
    // The full interest to every peer, and the rounds owed, once a period.
    let mut sync_tick = tokio::time::interval(Duration::from_secs(CLUSTER_RESYNC_INTERVAL_SECS));
    sync_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sync_tick.reset();

    // Static peers have their own reconnect loop; never remove them from known_peers
    let static_peers: std::collections::HashSet<String> = peers.iter().cloned().collect();

    // Manager-internal channels: peer tasks send back to the manager via these.
    // The senders are placed into the ClusterContext that gets cloned to each task.
    let (new_peer_tx, mut new_peer_rx) = mpsc::unbounded_channel::<String>();
    let (gossip_cmd_tx, mut gossip_cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
    let (peer_reg_tx, mut peer_reg_rx) = mpsc::unbounded_channel::<PeerRegistration>();
    let (presence_peer_tx, mut presence_peer_rx) =
        mpsc::channel::<PresencePeerFrame>(MANAGER_QUEUE_CAP);

    // Build the full ClusterContext by overriding the manager-internal channel senders
    let ctx = ClusterContext {
        new_peer_tx,
        cmd_tx: gossip_cmd_tx,
        presence_tx: presence_peer_tx,
        links: Arc::new(DashMap::new()),
        known_peers: Arc::new(DashSet::new()),
        ..ctx
    };

    // Live links, outbound and inbound alike, as their sessions register
    // them: (peer_addr, write channel, negotiated caps). Only these receive
    // publishes, interest and presence, so nothing queues up for a peer that
    // is not connected.
    let mut peer_txs: Vec<(String, mpsc::Sender<Bytes>, u32)> = Vec::new();
    // Dial tasks by address, static and discovered.
    let mut dialing: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    let spawn_ctx = PeerSpawnCtx {
        instance_id: instance_id.clone(),
        ctx: ctx.clone(),
        global_cancel: global_cancel.clone(),
        tls_config: tls_config.clone(),
        peer_reg_tx: peer_reg_tx.clone(),
    };

    for peer_addr in &peers {
        ctx.known_peers.insert(peer_addr.clone());
        if !dialing.contains_key(peer_addr) {
            dialing.insert(peer_addr.clone(), spawn_ctx.spawn(peer_addr.clone(), true));
        }
    }

    // Inbound links: from the cluster listener, or handed over by the
    // WebSocket port in single-port mode.
    let gate = Arc::new(InboundGate::default());
    let inbound = InboundSessionCtx {
        instance_id: instance_id.clone(),
        ctx: ctx.clone(),
        peer_reg_tx: peer_reg_tx.clone(),
        global_cancel: global_cancel.clone(),
    };
    match inbound_links {
        InboundLinks::Listener(listener) => {
            tokio::spawn(accept_cluster_links(
                listener,
                tls_config.clone(),
                gate,
                inbound,
            ));
        }
        InboundLinks::MainPort(mut handed_over) => {
            let cancel = global_cancel.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        link = handed_over.recv() => {
                            let Some((stream, addr)) = link else { break };
                            if let Some(admitted) = gate.admit(addr) {
                                let inbound = inbound.clone();
                                tokio::spawn(async move {
                                    inbound.run(stream, addr, admitted).await;
                                });
                            }
                        }
                        () = cancel.cancelled() => break,
                    }
                }
            });
        }
    }

    tracing::info!(
        "[WSE-Cluster] Manager started, {} peers configured",
        peers.len()
    );

    // Main loop: fan out commands to all peer channels
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ClusterCommand::Publish { topic, payload, position }) => {
                        // The position the origin's command task reserved for this
                        // very message (reading the head here raced with it).
                        let rec_pos = position;

                        // Pre-encode frames: with/without recovery, with/without compression
                        let recovery_compressed = if let Some((epoch, offset)) = rec_pos {
                            let mut buf = BytesMut::new();
                            if encode_msg_with_recovery(&mut buf, &topic, &payload, epoch, offset, true) {
                                Some(buf.freeze())
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        let recovery_plain = if let Some((epoch, offset)) = rec_pos {
                            let mut buf = BytesMut::new();
                            if encode_msg_with_recovery(&mut buf, &topic, &payload, epoch, offset, false) {
                                Some(buf.freeze())
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        let mut compressed_data = BytesMut::new();
                        let has_compressed = encode_msg_compressed(&mut compressed_data, &topic, &payload);
                        let compressed_bytes = if has_compressed { Some(compressed_data.freeze()) } else { None };

                        let mut plain_data = BytesMut::new();
                        if !encode_msg(&mut plain_data, &topic, &payload) {
                            tracing::warn!(
                                "[WSE-Cluster] Dropping publish: topic too long ({} bytes)",
                                topic.len()
                            );
                            continue;
                        }
                        let plain_bytes = plain_data.freeze();

                        for (peer_addr, tx, caps) in &peer_txs {
                            if draining.contains(peer_addr) {
                                continue;
                            }
                            let interested = remote_interest
                                .get(peer_addr)
                                .is_none_or(|interest| interest.wants(&topic));
                            if interested {
                                let frame = if caps & CAP_RECOVERY != 0 {
                                    if caps & CAP_COMPRESSION != 0 {
                                        recovery_compressed.as_ref().unwrap_or(&plain_bytes).clone()
                                    } else {
                                        recovery_plain.as_ref().unwrap_or(&plain_bytes).clone()
                                    }
                                } else if caps & CAP_COMPRESSION != 0 {
                                    compressed_bytes.as_ref().unwrap_or(&plain_bytes).clone()
                                } else {
                                    plain_bytes.clone()
                                };
                                // The peer drops a frame over its limit together
                                // with the link, so an oversized one is not sent.
                                if frame.len() > MAX_FRAME_SIZE {
                                    ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                    warn_oversized(&topic, frame.len());
                                    continue;
                                }
                                match tx.try_send(frame) {
                                    Ok(()) => {
                                        ctx.metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                        tracing::warn!("[WSE-Cluster] Peer {peer_addr} backpressure, dropping message");
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        ctx.metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                        if let Ok(mut dlq_guard) = dlq.lock() {
                                            dlq_guard.push(ClusterDlqEntry {
                                                topic: topic.clone(),
                                                payload: payload.clone(),
                                                peer_addr: peer_addr.clone(),
                                                error: "peer channel closed".to_string(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Sub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_sub(&mut frame_data, &topic) {
                            let frame_bytes = frame_data.freeze();
                            for (_, tx, caps) in &peer_txs {
                                if caps & CAP_INTEREST_ROUTING != 0 {
                                    let _ = tx.try_send(frame_bytes.clone());
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Unsub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_unsub(&mut frame_data, &topic) {
                            let frame_bytes = frame_data.freeze();
                            for (_, tx, caps) in &peer_txs {
                                if caps & CAP_INTEREST_ROUTING != 0 {
                                    let _ = tx.try_send(frame_bytes.clone());
                                }
                            }
                        }
                    }
                    // GossipPeerAnnounce is handled exclusively via gossip_cmd_rx below
                    Some(ClusterCommand::GossipPeerAnnounce { .. }) => {}
                    Some(ClusterCommand::PresenceUpdate { topic, user_id, action, data, updated_at }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_presence_update(&mut frame_data, &topic, &user_id, action, &data, updated_at) {
                            let frame_bytes = frame_data.freeze();
                            for (peer_addr, tx, caps) in &peer_txs {
                                if caps & CAP_PRESENCE != 0 {
                                    match tx.try_send(frame_bytes.clone()) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            tracing::warn!("[WSE-Cluster] Peer {peer_addr} backpressure, dropping presence update");
                                            presence_dirty.insert(peer_addr.clone());
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            if let Ok(mut dlq_guard) = dlq.lock() {
                                                dlq_guard.push(ClusterDlqEntry {
                                                    topic: topic.clone(),
                                                    payload: data.clone(),
                                                    peer_addr: peer_addr.clone(),
                                                    error: "presence update failed".to_string(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Drain) => {
                        tracing::info!("[WSE-Cluster] Sending DRAIN to all peers");
                        let mut drain_data = BytesMut::new();
                        encode_drain(&mut drain_data);
                        let drain_bytes = drain_data.freeze();
                        for (peer_addr, tx, _) in &peer_txs {
                            if tx.try_send(drain_bytes.clone()).is_err() {
                                tracing::warn!("[WSE-Cluster] Could not send DRAIN to {peer_addr}");
                            }
                        }
                    }
                    Some(ClusterCommand::Shutdown) | None => {
                        tracing::info!("[WSE-Cluster] Manager shutting down");
                        let mut shutdown_data = BytesMut::new();
                        encode_shutdown(&mut shutdown_data);
                        let shutdown_bytes = shutdown_data.freeze();
                        for (peer_addr, tx, _) in &peer_txs {
                            if tx.try_send(shutdown_bytes.clone()).is_err() {
                                tracing::warn!("[WSE-Cluster] Could not send SHUTDOWN to {peer_addr}");
                            }
                        }
                        global_cancel.cancel();
                        break;
                    }
                }
            }
            // Dynamic peer discovery: spawn connection tasks for newly discovered peers
            Some(new_addr) = new_peer_rx.recv() => {
                if !valid_peer_addr(&new_addr) {
                    tracing::debug!("[WSE-Cluster] Ignoring invalid peer address from gossip: {new_addr}");
                    continue;
                }
                let is_self = ctx.cluster_addr.as_deref() == Some(&new_addr);
                let already_known = !ctx.known_peers.insert(new_addr.clone());
                if !is_self && !already_known {
                    dialing.retain(|_, task| !task.is_finished());
                    if dialing.contains_key(&new_addr) {
                        // Still being dialed.
                    } else if dialing.len() >= MAX_CLUSTER_PEERS {
                        tracing::warn!(
                            "[WSE-Cluster] Ignoring discovered peer {new_addr}: at capacity ({MAX_CLUSTER_PEERS} peers)"
                        );
                    } else {
                        dialing.insert(new_addr.clone(), spawn_ctx.spawn(new_addr.clone(), false));
                        tracing::info!("[WSE-Cluster] Discovered new peer: {new_addr}");
                    }
                }
            }
            // Handle gossip commands from peer tasks
            Some(gossip_cmd) = gossip_cmd_rx.recv() => {
                if let ClusterCommand::GossipPeerAnnounce { addr, exclude_peer } = gossip_cmd {
                    // Validate address before processing
                    if !valid_peer_addr(&addr) {
                        tracing::debug!("[WSE-Cluster] Ignoring invalid gossip address: {addr}");
                        continue;
                    }
                    let is_self = ctx.cluster_addr.as_deref() == Some(&addr);
                    if is_self {
                        continue;
                    }
                    // Forward PEER_ANNOUNCE only for newly discovered peers
                    // to prevent gossip amplification in dense clusters
                    let is_new = !ctx.known_peers.contains(&addr);
                    if is_new {
                        let mut frame_data = BytesMut::new();
                        encode_peer_announce(&mut frame_data, &addr);
                        let frame_bytes = frame_data.freeze();
                        for (peer_addr, tx, _) in &peer_txs {
                            if *peer_addr != exclude_peer {
                                let _ = tx.try_send(frame_bytes.clone());
                            }
                        }
                    }
                    // Only spawn connection to this peer if we don't already know it
                    if ctx.known_peers.insert(addr.clone()) {
                        dialing.retain(|_, task| !task.is_finished());
                        if dialing.contains_key(&addr) {
                            // Still being dialed.
                        } else if dialing.len() >= MAX_CLUSTER_PEERS {
                            tracing::warn!(
                                "[WSE-Cluster] Ignoring gossip peer {addr}: at capacity ({MAX_CLUSTER_PEERS} peers)"
                            );
                        } else {
                            dialing.insert(addr.clone(), spawn_ctx.spawn(addr.clone(), false));
                            tracing::info!("[WSE-Cluster] Discovered peer via gossip: {addr}");
                        }
                    }
                }
            }
            update = interest_rx.recv() => {
                match update {
                    Some(InterestUpdate::Sub { peer_addr, topic }) => {
                        // Bounded: a new topic past the cap is dropped, and so is
                        // a name longer than any node subscribes.
                        if topic.len() <= MAX_TOPIC_LEN {
                            remote_interest
                                .entry(peer_addr)
                                .or_default()
                                .insert(topic, MAX_REMOTE_INTEREST_PER_PEER);
                        }
                    }
                    Some(InterestUpdate::Unsub { peer_addr, topic }) => {
                        if let Some(interest) = remote_interest.get_mut(&peer_addr) {
                            interest.remove(&topic);
                        }
                    }
                    Some(InterestUpdate::Resync { peer_addr, topics, is_continuation, generation }) => {
                        // The first frame of a RESYNC replaces the set; a continuation
                        // extends it, and only for the session that started it.
                        if is_continuation && peer_generation.get(&peer_addr) != Some(&generation) {
                            continue;
                        }
                        if !is_continuation {
                            peer_generation.insert(peer_addr.clone(), generation);
                            remote_interest.insert(peer_addr.clone(), PeerInterest::default());
                        }
                        let interest = remote_interest.entry(peer_addr.clone()).or_default();
                        let offered = topics.len();
                        let kept = topics
                            .into_iter()
                            .filter(|t| {
                                t.len() <= MAX_TOPIC_LEN
                                    && interest.insert(t.clone(), MAX_REMOTE_INTEREST_PER_PEER)
                            })
                            .count();
                        if kept < offered {
                            tracing::warn!(
                                "[WSE-Cluster] {peer_addr} advertised {} topics past {MAX_REMOTE_INTEREST_PER_PEER} or longer than {MAX_TOPIC_LEN} bytes; ignored",
                                offered - kept
                            );
                        }
                    }
                    Some(InterestUpdate::PeerDisconnected { peer_addr, generation }) => {
                        // Only process disconnect if generation matches current session.
                        // Prevents a stale disconnect from clearing a newer session's interest.
                        let stale = peer_generation
                            .get(&peer_addr)
                            .is_some_and(|g| *g != generation);
                        if !stale {
                            // Purge this peer's presence sentinels so a crashed peer
                            // (no per-user leave) leaves no ghost members behind.
                            if let Some(ref pm) = presence {
                                for (topic, user_id, data) in pm.purge_origin(&peer_addr) {
                                    tell_presence(&server_cmd_tx, "presence_leave", topic, &user_id, &data);
                                }
                            }
                            remote_interest.remove(&peer_addr);
                            peer_generation.remove(&peer_addr);
                            presence_rounds.remove(&peer_addr);
                            draining.remove(&peer_addr);
                            if !static_peers.contains(&peer_addr) {
                                ctx.known_peers.remove(&peer_addr);
                            }
                        }
                    }
                    Some(InterestUpdate::Drain { peer_addr }) => {
                        // Lame duck: route nothing new to the draining peer until it
                        // disconnects. The link stays up for in-flight delivery.
                        draining.insert(peer_addr);
                    }
                    None => {
                        // Interest channel closed, continue running
                    }
                }
            }
            // Handle inbound peer registrations/deregistrations
            Some(reg) = peer_reg_rx.recv() => {
                match reg {
                    PeerRegistration::Register { peer_addr, write_tx, negotiated_caps } => {
                        // A live session, outbound or inbound, with the caps its
                        // handshake negotiated. This node's interest and presence
                        // go out from here, in the order of every SUB, UNSUB and
                        // presence update after them: a change made while the
                        // link came up reaches the peer after its snapshot.
                        if !queue_whole(&write_tx, &interest_frames(&ctx.local_topic_refcount)) {
                            tracing::warn!(
                                "[WSE-Cluster] Could not queue RESYNC for {peer_addr}; the next follows within {CLUSTER_RESYNC_INTERVAL_SECS}s"
                            );
                        }
                        presence_dirty.remove(&peer_addr);
                        if negotiated_caps & CAP_PRESENCE != 0
                            && let Some(pm) = &presence
                            && !queue_whole(&write_tx, &presence_round(pm))
                        {
                            presence_dirty.insert(peer_addr.clone());
                        }
                        peer_txs.retain(|(a, _, _)| a != &peer_addr);
                        peer_txs.push((peer_addr.clone(), write_tx, negotiated_caps));
                        tracing::debug!("[WSE-Cluster] Link up: {peer_addr} (caps=0x{negotiated_caps:08x})");
                    }
                    PeerRegistration::Deregister { peer_addr } => {
                        peer_txs.retain(|(a, _, _)| a != &peer_addr);
                        presence_dirty.remove(&peer_addr);
                        // A gossip-discovered peer that is no longer dialed can be
                        // discovered again.
                        if !static_peers.contains(&peer_addr) && !dialing.contains_key(&peer_addr) {
                            ctx.known_peers.remove(&peer_addr);
                        }
                    }
                }
            }
            // Handle presence frames received from peers
            Some(pf) = presence_peer_rx.recv() => {
                if let Some(ref pm) = presence {
                    match pf {
                        PresencePeerFrame::Update { origin, topic, user_id, action, data, updated_at } => {
                            let data_val: serde_json::Value = serde_json::from_str(&data).unwrap_or(serde_json::Value::Null);
                            // Local subscribers hear of a change in membership
                            // only: a join of a user already present (another
                            // node) or a leave of one still present is none.
                            let event = match action {
                                0 => pm
                                    .merge_remote_join(&topic, &user_id, &data_val, updated_at, &origin)
                                    .then_some(("presence_join", data_val)),
                                1 => pm
                                    .merge_remote_leave(&topic, &user_id, updated_at, &origin)
                                    .map(|left| ("presence_leave", left)),
                                2 => pm
                                    .merge_remote_update(&topic, &user_id, &data_val, updated_at)
                                    .then_some(("presence_update", data_val)),
                                _ => None, // Ignore unknown actions
                            };
                            if let Some((kind, data)) = event {
                                tell_presence(&server_cmd_tx, kind, topic, &user_id, &data);
                            }
                        }
                        PresencePeerFrame::Full { origin, entries, round, generation } => {
                            if round.is_some_and(|part| part.first) {
                                presence_rounds.insert(origin.clone(), (generation, HashMap::new()));
                            }
                            // A frame of a round whose start was not seen here (an
                            // older session's) is merged like any full state.
                            let joined = match presence_rounds.get_mut(&origin) {
                                Some((session, held)) if round.is_some() && *session == generation => {
                                    pm.merge_round_frame(&entries, &origin, held)
                                }
                                _ => pm.merge_full_state(&entries, &origin),
                            };
                            for (topic, user_id, data) in joined {
                                tell_presence(&server_cmd_tx, "presence_join", topic, &user_id, &data);
                            }
                            // The round is complete: what it did not hold is gone.
                            if round.is_some_and(|part| part.last)
                                && presence_rounds.get(&origin).is_some_and(|(session, _)| *session == generation)
                                && let Some((_, held)) = presence_rounds.remove(&origin)
                            {
                                for (topic, user_id, data) in pm.retain_origin(&origin, &held) {
                                    tell_presence(&server_cmd_tx, "presence_leave", topic, &user_id, &data);
                                }
                            }
                        }
                    }
                }
            }
            _ = sync_tick.tick() => {
                // Self-heal: the full interest to every peer (a SUB or UNSUB
                // dropped on a full link is corrected), and a presence round
                // to the peers that missed a presence frame.
                let interest = interest_frames(&ctx.local_topic_refcount);
                let round = match &presence {
                    Some(pm) if !presence_dirty.is_empty() => Some(presence_round(pm)),
                    _ => None,
                };
                for (peer_addr, tx, caps) in &peer_txs {
                    if !queue_whole(tx, &interest) {
                        tracing::debug!("[WSE-Cluster] {peer_addr} busy; its RESYNC waits a period");
                    }
                    if let Some(round) = &round
                        && caps & CAP_PRESENCE != 0
                        && presence_dirty.contains(peer_addr)
                        && queue_whole(tx, round)
                    {
                        presence_dirty.remove(peer_addr);
                    }
                }
            }
            () = global_cancel.cancelled() => break,
        }

        // Prune dead peer channels (receiver dropped = peer task exited)
        peer_txs.retain(|(addr, tx, _)| {
            if tx.is_closed() {
                tracing::debug!("[WSE-Cluster] Pruning dead peer channel: {addr}");
                false
            } else {
                true
            }
        });
    }

    // Wait for all dial tasks to finish
    for (_, task) in dialing {
        let _ = task.await;
    }
    tracing::info!("[WSE-Cluster] Manager stopped");
}

/// Whether an address from gossip may be dialed: `ip:port`, `[ipv6]:port` or
/// `dns-name:port`. Anything else (a path, a URL, a port of zero) is refused.
pub(super) fn valid_peer_addr(addr: &str) -> bool {
    if let Ok(sock) = addr.parse::<std::net::SocketAddr>() {
        return sock.port() != 0;
    }
    let Some((host, port)) = addr.rsplit_once(':') else {
        return false;
    };
    let port_ok = port.parse::<u16>().is_ok_and(|p| p != 0);
    let host_ok = !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        });
    port_ok && host_ok
}

/// Warn about a publish too large to send to peers, at most every ten seconds.
fn warn_oversized(topic: &str, size: usize) {
    use std::sync::atomic::AtomicU64;
    static LAST_WARN_MS: AtomicU64 = AtomicU64::new(0);
    let now = super::epoch_ms();
    let last = LAST_WARN_MS.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 10_000
        && LAST_WARN_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(
            "[WSE-Cluster] Not sending a {size}-byte publish to '{topic}' to cluster peers: \
             the frame limit is {MAX_FRAME_SIZE} bytes (local subscribers still get it)"
        );
    }
}
