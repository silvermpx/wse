//! The command processor: executes the `ServerCommand`s sent to the running server.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

use super::fanout::fanout_broadcast_direct;
use super::frames::ws_frame_size;
use super::{
    CLOSE_GOING_AWAY, CLOSE_NORMAL, Outgoing, ServerCommand, SharedState, WsFrame,
    fanout_queue_groups, stamp_recovery_fields,
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
        state.commands_queued.store(cmd_rx.len(), Ordering::Relaxed);
        match cmd {
            ServerCommand::SendText { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    h.send_one(
                        Message::Text(data.into()),
                        state.max_outbound_queue_bytes,
                        &state.slow_consumer_drops,
                    );
                }
            }
            ServerCommand::SendBytes { conn_id, data } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    h.send_one(
                        Message::Binary(data.into()),
                        state.max_outbound_queue_bytes,
                        &state.slow_consumer_drops,
                    );
                }
            }
            ServerCommand::SendPrebuilt { conn_id, message } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    let bytes = ws_frame_size(&WsFrame::Msg(message.clone())) as u64;
                    // Sealed for the connection's E2E key, if it has one.
                    if h.send_one(
                        message,
                        state.max_outbound_queue_bytes,
                        &state.slow_consumer_drops,
                    ) {
                        // Counted once really queued.
                        state.messages_sent_total.fetch_add(1, Ordering::Relaxed);
                        state.bytes_sent_total.fetch_add(bytes, Ordering::Relaxed);
                    }
                }
            }
            ServerCommand::BroadcastText { data } => {
                let msg = Outgoing::text(Bytes::from(data));
                let guard = state.connections.read().await;
                fanout_broadcast_direct(
                    &guard,
                    &msg,
                    state.max_outbound_queue_bytes,
                    &state.slow_consumer_drops,
                );
            }
            ServerCommand::BroadcastBytes { data } => {
                let msg = Outgoing::binary(Bytes::from(data));
                let guard = state.connections.read().await;
                fanout_broadcast_direct(
                    &guard,
                    &msg,
                    state.max_outbound_queue_bytes,
                    &state.slow_consumer_drops,
                );
            }
            ServerCommand::BroadcastLocal {
                topic,
                data,
                skip_recovery,
                publish,
            } => {
                // Stamp the JSON with this message's (topic, epoch, offset) and store
                // the stamped payload unframed, so the live copy and any replayed copy
                // are byte-identical and clients can dedup, detect gaps and recover
                // from their last-seen position. Presence events (skip_recovery) are
                // neither stamped nor stored.
                let original = Bytes::from(data);
                let mut position = None;
                let stamped: Bytes = if !skip_recovery && let Some(ref recovery) = state.recovery {
                    let text = std::str::from_utf8(&original).unwrap_or("");
                    recovery.push_stamped(&topic, |epoch, offset| {
                        position = Some((epoch, offset));
                        Bytes::from(stamp_recovery_fields(text, &topic, epoch, offset))
                    })
                } else {
                    original.clone()
                };
                if publish {
                    let cluster_tx = state
                        .cluster_cmd_tx
                        .read()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone();
                    if let Some(tx) = cluster_tx {
                        let _ = tx.send(ClusterCommand::Publish {
                            topic: topic.clone(),
                            payload: String::from_utf8_lossy(&stamped).into_owned(),
                            position,
                        });
                    }
                }
                // Track per-topic message count for Prometheus top-N metrics
                state
                    .topic_message_counts
                    .entry(topic.clone())
                    .or_insert_with(|| AtomicU64::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                let max_pending = state.max_outbound_queue_bytes;
                let slow_drops = &state.slow_consumer_drops;
                state
                    .topics
                    .fanout(&topic, &Outgoing::text(stamped), max_pending, slow_drops);
                // A queue-group member sees only some offsets of the topic: it
                // gets the message without the recovery stamp.
                if state.queue_groups.contains_key(&topic) {
                    fanout_queue_groups(
                        &state.queue_groups,
                        &topic,
                        &Outgoing::text(original),
                        max_pending,
                        slow_drops,
                    );
                }
            }
            ServerCommand::Disconnect { conn_id } => {
                let guard = state.connections.read().await;
                if let Some(h) = guard.get(&conn_id) {
                    h.close(CLOSE_NORMAL, "");
                }
            }
            ServerCommand::Drain {
                close_code,
                close_reason,
            } => {
                // Set draining flag to reject new connections
                state.draining.store(true, Ordering::Relaxed);
                tracing::info!("[WSE] Entering drain mode (lame duck)");

                // Notify cluster peers
                if let Some(tx) = state.cluster_cmd_tx.read().unwrap().as_ref() {
                    let _ = tx.send(ClusterCommand::Drain);
                }

                // Close every client with the drain code; one that does not answer
                // is dropped after the close grace.
                let guard = state.connections.read().await;
                let n = guard.len();
                for h in guard.values() {
                    h.close(close_code, &close_reason);
                }
                drop(guard);
                tracing::info!("[WSE] Drain: sent close to {n} connections");
            }
            ServerCommand::AbortAll => {
                for h in state.connections.read().await.values() {
                    h.closer.abort();
                }
            }
            ServerCommand::Shutdown => {
                let guard = state.connections.read().await;
                for h in guard.values() {
                    h.close(CLOSE_GOING_AWAY, "server shutting down");
                }
                drop(guard);
                // Up to two seconds for the clients to answer, then the rest
                // are dropped. Either way every connection's cleanup runs
                // (disconnect events, presence, maps) before the runtime goes,
                // so a later start() begins with none left over.
                wait_for_no_connections(&state, Duration::from_secs(2)).await;
                for h in state.connections.read().await.values() {
                    h.closer.abort();
                }
                wait_for_no_connections(&state, Duration::from_secs(1)).await;
                break;
            }
        }
    }
}

async fn wait_for_no_connections(state: &SharedState, limit: Duration) {
    let deadline = tokio::time::Instant::now() + limit;
    while state.connection_count.load(Ordering::Relaxed) > 0
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
