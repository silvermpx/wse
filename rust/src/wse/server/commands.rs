//! The command processor: executes the `ServerCommand`s sent to the running server.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ahash::AHashSet;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

use super::e2e::encrypt_outbound;
use super::fanout::fanout_broadcast_direct;
use super::frames::ws_frame_size;
use super::{
    ServerCommand, SharedState, WsFrame, fanout_topic_direct, glob_match, pre_frame_binary,
    pre_frame_text, stamp_recovery_fields,
};
use crate::wse::cluster::ClusterCommand;

// ---------------------------------------------------------------------------
// Command processor
// ---------------------------------------------------------------------------

pub(super) async fn process_commands(
    mut cmd_rx: mpsc::UnboundedReceiver<ServerCommand>,
    state: Arc<SharedState>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ServerCommand::SendText { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = data.len() + 4;
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx.send(WsFrame::Msg(Message::Text(data.into()))).is_err() {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = data.len() + 4;
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx
                            .send(WsFrame::Msg(Message::Binary(data.into())))
                            .is_err()
                        {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let max_pending = state.max_outbound_queue_bytes;
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        state.slow_consumer_drops.fetch_add(1, Ordering::Relaxed);
                    } else {
                        let msg_bytes = ws_frame_size(&WsFrame::Msg(message.clone()));
                        h.pending.fetch_add(msg_bytes, Ordering::Relaxed);
                        if h.tx.send(WsFrame::Msg(message)).is_err() {
                            h.pending.fetch_sub(msg_bytes, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::BroadcastText { data } => {
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                if state.conn_encryption.is_empty() {
                    // NATS-style: direct buffer append (bypasses mpsc channel)
                    let preframed = pre_frame_text(&data);
                    let guard = state.connections.read().await;
                    fanout_broadcast_direct(&guard, &preframed, max_pending, slow_drops);
                } else {
                    let preframed = pre_frame_text(&data);
                    let guard = state.connections.read().await;
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), h.pending.clone(), cipher_ref.clone()));
                        } else {
                            // Plain connection: direct buffer append
                            let data_len = preframed.len();
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
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
                        }
                    }
                    drop(guard);
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, data.as_bytes()) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
            }
            ServerCommand::BroadcastBytes { data } => {
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                if state.conn_encryption.is_empty() {
                    // NATS-style: direct buffer append (bypasses mpsc channel)
                    let preframed = pre_frame_binary(&data);
                    let guard = state.connections.read().await;
                    fanout_broadcast_direct(&guard, &preframed, max_pending, slow_drops);
                } else {
                    let preframed = pre_frame_binary(&data);
                    let guard = state.connections.read().await;
                    let mut enc_senders = Vec::new();
                    for (cid, h) in guard.iter() {
                        if let Some(cipher_ref) = state.conn_encryption.get(cid) {
                            enc_senders.push((h.tx.clone(), h.pending.clone(), cipher_ref.clone()));
                        } else {
                            // Plain connection: direct buffer append
                            let data_len = preframed.len();
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
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
                        }
                    }
                    drop(guard);
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, &data) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
            }
            ServerCommand::BroadcastLocal {
                topic,
                data,
                skip_recovery,
                publish,
            } => {
                // Stamp the JSON with this message's (topic, epoch, offset) and store
                // the UNFRAMED stamped payload, so the live copy and any replayed copy
                // are byte-identical and clients can dedup / detect gaps / recover from
                // their last-seen position. The offset is reserved atomically inside
                // push_stamped. Presence events (skip_recovery) are neither stamped nor
                // stored. The encrypted fan-out below encrypts this same `stamped` JSON
                // and the recovery-replay path frames/encrypts it per connection, so
                // there is exactly one wire form per (topic, offset).
                let mut position = None;
                let stamped: Bytes = if !skip_recovery && let Some(ref recovery) = state.recovery {
                    recovery.push_stamped(&topic, |epoch, offset| {
                        position = Some((epoch, offset));
                        Bytes::from(stamp_recovery_fields(&data, &topic, epoch, offset))
                    })
                } else {
                    Bytes::from(data)
                };
                let stamped_str = std::str::from_utf8(&stamped).unwrap_or("");
                if publish {
                    let cluster_tx = state
                        .cluster_cmd_tx
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    if let Some(tx) = cluster_tx {
                        let _ = tx.send(ClusterCommand::Publish {
                            topic: topic.clone(),
                            payload: stamped_str.to_owned(),
                            position,
                        });
                    }
                }
                let preframed = pre_frame_text(stamped_str);
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                // Track per-topic message count for Prometheus top-N metrics
                state
                    .topic_message_counts
                    .entry(topic.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                if state.conn_encryption.is_empty() {
                    // Fast path: NATS-style direct buffer (bypasses mpsc channel)
                    fanout_topic_direct(
                        &state.topic_subscribers,
                        &topic,
                        &preframed,
                        max_pending,
                        slow_drops,
                        state.glob_topic_count.load(Ordering::Relaxed),
                    );
                } else {
                    // Mixed: direct buffer for plain, mpsc for encrypted
                    // No connections RwLock needed -- handles are in topic_subscribers
                    let mut enc_senders = Vec::new();
                    let mut seen = AHashSet::new();
                    if let Some(subs) = state.topic_subscribers.get(&topic) {
                        for entry in subs.iter() {
                            let cid = entry.key().clone();
                            if seen.insert(cid.clone()) {
                                let h = entry.value();
                                if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                    enc_senders.push((
                                        h.tx.clone(),
                                        h.pending.clone(),
                                        cipher_ref.clone(),
                                    ));
                                } else {
                                    // Plain connection: direct buffer append
                                    if h.tx.is_closed() {
                                        continue;
                                    }
                                    let data_len = preframed.len();
                                    if max_pending > 0
                                        && h.pending.load(Ordering::Relaxed) >= max_pending
                                    {
                                        slow_drops.fetch_add(1, Ordering::Relaxed);
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
                                }
                            }
                        }
                    }
                    // Glob pattern matching
                    for topic_entry in state.topic_subscribers.iter() {
                        let pattern = topic_entry.key();
                        if (pattern.contains('*') || pattern.contains('?'))
                            && glob_match(pattern, &topic)
                        {
                            for entry in topic_entry.value().iter() {
                                let cid = entry.key().clone();
                                if seen.insert(cid.clone()) {
                                    let h = entry.value();
                                    if let Some(cipher_ref) = state.conn_encryption.get(&cid) {
                                        enc_senders.push((
                                            h.tx.clone(),
                                            h.pending.clone(),
                                            cipher_ref.clone(),
                                        ));
                                    } else {
                                        // Plain connection: direct buffer append
                                        if h.tx.is_closed() {
                                            continue;
                                        }
                                        let data_len = preframed.len();
                                        if max_pending > 0
                                            && h.pending.load(Ordering::Relaxed) >= max_pending
                                        {
                                            slow_drops.fetch_add(1, Ordering::Relaxed);
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
                                    }
                                }
                            }
                        }
                    }
                    for (tx, pending, cipher) in &enc_senders {
                        if max_pending > 0 && pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        match encrypt_outbound(cipher, &stamped) {
                            Ok(enc) => {
                                let mut buf = Vec::with_capacity(2 + enc.len());
                                buf.extend_from_slice(b"E:");
                                buf.extend_from_slice(&enc);
                                let enc_bytes = buf.len() + 4;
                                pending.fetch_add(enc_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Binary(buf.into()))).is_err() {
                                    pending.fetch_sub(enc_bytes, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] encrypt_outbound failed: {e}");
                            }
                        }
                    }
                }
                // Queue group dispatch: round-robin to one member per group.
                // Retry on dead/backpressured members to avoid silent message loss.
                if let Some(topic_groups) = state.queue_groups.get(&topic) {
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
                            if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                                slow_drops.fetch_add(1, Ordering::Relaxed);
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
                            break;
                        }
                        if !delivered {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            ServerCommand::Disconnect { conn_id } => {
                // Control message: send unconditionally
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(Message::Close(None))).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
            }
            ServerCommand::GetConnections { reply } => {
                let guard = state.connections.read().await;
                let _ = reply.send(guard.keys().cloned().collect());
            }
            ServerCommand::Drain {
                close_code,
                close_reason,
                timeout_secs,
            } => {
                // Set draining flag to reject new connections
                state.draining.store(true, Ordering::Relaxed);
                tracing::info!("[WSE] Entering drain mode (lame duck)");

                // Notify cluster peers
                if let Some(tx) = state.cluster_cmd_tx.read().unwrap().as_ref() {
                    let _ = tx.send(ClusterCommand::Drain);
                }

                // Send close frame with custom code to all clients
                let close_frame = Message::Close(Some(CloseFrame {
                    code: CloseCode::from(close_code),
                    reason: close_reason.into(),
                }));
                let guard = state.connections.read().await;
                let n = guard.len();
                for h in guard.values() {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(close_frame.clone())).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
                drop(guard);
                tracing::info!(
                    "[WSE] Drain: sent close to {n} connections, waiting up to {timeout_secs}s"
                );

                // Spawn drain wait as separate task so command processor stays responsive
                let drain_state = state.clone();
                tokio::spawn(async move {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
                    loop {
                        if drain_state.connection_count.load(Ordering::Relaxed) == 0 {
                            tracing::info!("[WSE] Drain complete: all connections closed");
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            let remaining = drain_state.connection_count.load(Ordering::Relaxed);
                            tracing::warn!(
                                "[WSE] Drain timeout: {remaining} connections remaining"
                            );
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                });
            }
            ServerCommand::Shutdown => {
                let close_frame = Message::Close(Some(CloseFrame {
                    code: CloseCode::Away,
                    reason: "server shutting down".into(),
                }));
                let guard = state.connections.read().await;
                for h in guard.values() {
                    h.pending.fetch_add(8, Ordering::Relaxed);
                    if h.tx.send(WsFrame::Msg(close_frame.clone())).is_err() {
                        h.pending.fetch_sub(8, Ordering::Relaxed);
                    }
                }
                drop(guard);
                // Drain period: allow in-flight messages to flush before exiting
                tokio::time::sleep(Duration::from_secs(2)).await;
                break;
            }
        }
    }
}
