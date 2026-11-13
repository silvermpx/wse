//! One client connection, from the WebSocket upgrade to the cleanup.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use uuid::Uuid;

use super::e2e::{decrypt_inbound, derive_connection_key};
use super::frames::{write_all_frames, ws_frame_size};
use super::handshake::{HandshakeGuard, reject_ws_auth};
use super::messages::{build_server_hello, build_server_ready, normalize_json_keys};
use super::py::{fire_on_connect, fire_on_disconnect, json_to_pyobj};
use super::state::InboundEvent;
use super::{
    BroadcastQueue, ConnectionHandle, ServerCommand, SharedState, WsFrame, format_presence_msg,
};
use crate::jwt::{self, parse_cookie_value};
use crate::wse::cluster::ClusterCommand;
use crate::wse::compression::{decode_msgpack_guarded, rmpv_to_serde_json};

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Max concurrent on_message callbacks (blocking tasks) per connection in
/// callback mode. Bounds inbound flooding from exhausting the blocking pool.
const MAX_INFLIGHT_CALLBACKS: usize = 256;

/// Spawn an on_message callback as a bounded blocking task. If `MAX_INFLIGHT_
/// CALLBACKS` are already running for this connection the inbound work is SHED
/// (counted as rate-limited) rather than spawning an unbounded task -- a client
/// flooding inbound messages otherwise exhausts the blocking pool and contends
/// the GIL. The permit is released when the blocking task finishes.
fn spawn_bounded_callback<F>(sem: &Arc<tokio::sync::Semaphore>, state: &Arc<SharedState>, work: F)
where
    F: FnOnce() + Send + 'static,
{
    match sem.clone().try_acquire_owned() {
        Ok(permit) => {
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                work();
            });
        }
        Err(_) => {
            state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Clone a TcpStream by duplicating the file descriptor.
/// Returns (original, clone) -- both refer to the same socket.
fn clone_tcp_stream(stream: TcpStream) -> std::io::Result<(TcpStream, TcpStream)> {
    let std_stream = stream.into_std()?;
    let std_clone = std_stream.try_clone()?;
    Ok((
        TcpStream::from_std(std_stream)?,
        TcpStream::from_std(std_clone)?,
    ))
}

pub(super) async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<SharedState>,
    hs_guard: HandshakeGuard,
) {
    // Reject new connections during graceful drain
    if state.draining.load(Ordering::Relaxed) {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    // Early rejection before expensive handshake (best-effort, authoritative check below)
    if state.connection_count.load(Ordering::Relaxed) >= state.max_connections {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[WSE] Max connections reached (early check), rejecting {addr}");
        return;
    }

    // Clone the TCP stream FD for pre-framed raw writes (bypasses tungstenite).
    let (stream, mut raw_write) = match clone_tcp_stream(stream) {
        Ok(pair) => pair,
        Err(e) => {
            tracing::error!("[WSE] Failed to clone TCP stream for {addr}: {e}");
            return;
        }
    };

    // Lock-free handshake: OnceLock captures cookie + format in the callback
    struct HandshakeResult {
        cookies: String,
        wants_msgpack: bool,
        authorization: Option<String>,
    }
    let handshake_data: Arc<std::sync::OnceLock<HandshakeResult>> =
        Arc::new(std::sync::OnceLock::new());
    let hd_clone = handshake_data.clone();

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(state.max_message_size);
    ws_config.max_frame_size = Some(state.max_message_size);
    // 10-second timeout prevents slow loris attacks on the handshake
    let ws_stream = match tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::accept_hdr_async_with_config(
            stream,
            #[expect(
                clippy::result_large_err,
                reason = "the tungstenite accept_hdr callback signature fixes Err = ErrorResponse; we cannot box it"
            )]
            move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
                let cookies = req
                    .headers()
                    .get("cookie")
                    .and_then(|cv| cv.to_str().ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let wants_msgpack = req
                    .uri()
                    .query()
                    .is_some_and(|q| q.contains("format=msgpack"));
                let authorization = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let _ = hd_clone.set(HandshakeResult {
                    cookies,
                    wants_msgpack,
                    authorization,
                });
                Ok(response)
            },
            Some(ws_config),
        ),
    )
    .await
    {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            tracing::warn!("[WSE] WS handshake failed for {addr}: {e}");
            return;
        }
        Err(_) => {
            tracing::warn!("[WSE] WS handshake timeout for {addr}");
            return;
        }
    };

    let (cookie_str, use_msgpack, auth_header) = handshake_data
        .get()
        .map(|hd| {
            (
                hd.cookies.clone(),
                hd.wants_msgpack,
                hd.authorization.clone(),
            )
        })
        .unwrap_or_default();
    let conn_id: Arc<String> = Arc::new(Uuid::now_v7().to_string());
    let (write_half, read_half) = ws_stream.split();
    let mut read_half = read_half;
    let mut write_half = write_half;
    let (tx, mut rx) = mpsc::unbounded_channel::<WsFrame>();
    let pending = Arc::new(AtomicUsize::new(0));
    let broadcast_buf = Arc::new(parking_lot::Mutex::new(BroadcastQueue::new()));
    let broadcast_notify = Arc::new(tokio::sync::Notify::new());
    let drain = state.drain_mode.load(Ordering::Relaxed);

    // ── JWT validation in Rust (zero GIL) ──────────────────────────────────
    // When jwt_config is set, validate JWT from cookie or Authorization header.
    // On success: send server_ready immediately, push AuthConnect to drain queue.
    // On failure: send error + close — connection never registered.
    let rust_auth_user_id: Option<String> = if let Some(ref jwt_cfg) = state.jwt_config {
        let token = parse_cookie_value(&cookie_str, &jwt_cfg.cookie_name).or_else(|| {
            auth_header.as_deref().and_then(|h| {
                // RFC 7235: auth-scheme is case-insensitive
                if h.get(..7)
                    .is_some_and(|p| p.eq_ignore_ascii_case("bearer "))
                {
                    h.get(7..)
                } else {
                    None
                }
            })
        });
        match token {
            Some(tok) => match jwt::jwt_decode(tok, jwt_cfg) {
                Ok(claims) => {
                    if let Some(user_id) = claims
                        .get("sub")
                        .and_then(|s| s.as_str())
                        .filter(|s| !s.is_empty())
                    {
                        // Extract optional topic ACL from wse_topics claim
                        if let Some(acl) = jwt::extract_topic_acl(&claims) {
                            state.conn_topic_acl.insert(conn_id.to_string(), acl);
                        }
                        Some(user_id.to_string())
                    } else {
                        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                        state
                            .connections_rejected_total
                            .fetch_add(1, Ordering::Relaxed);
                        reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Token missing user ID")
                            .await;
                        return;
                    }
                }
                Err(e) => {
                    tracing::warn!("[WSE] JWT validation failed for {addr}: {e}");
                    state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                    state
                        .connections_rejected_total
                        .fetch_add(1, Ordering::Relaxed);
                    reject_ws_auth(&tx, rx, write_half, "AUTH_FAILED", "Authentication failed")
                        .await;
                    return;
                }
            },
            None => {
                state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                state
                    .connections_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                reject_ws_auth(
                    &tx,
                    rx,
                    write_half,
                    "AUTH_REQUIRED",
                    "No JWT cookie or Authorization header",
                )
                .await;
                return;
            }
        }
    } else {
        None // No JWT config — delegate auth to Python
    };

    // Register connection
    {
        let mut conns = state.connections.write().await;
        if conns.len() >= state.max_connections {
            state
                .connections_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!("[WSE] Max connections reached, rejecting {addr}");
            // Clean up ACL entry inserted before registration check
            state.conn_topic_acl.remove(&*conn_id);
            let _ = write_half.close().await;
            return;
        }
        // Send server_ready BEFORE inserting into connections map.
        // While we hold the write lock, no broadcast can acquire a read lock,
        // so server_ready is guaranteed to be first in the channel.
        if let Some(ref user_id) = rust_auth_user_id {
            let recovery_enabled = state.recovery.is_some();
            let server_ready = build_server_ready(&conn_id, user_id, recovery_enabled);
            let sr_bytes = server_ready.len() + 4;
            pending.fetch_add(sr_bytes, Ordering::Relaxed);
            if tx
                .send(WsFrame::Msg(Message::Text(server_ready.into())))
                .is_err()
            {
                pending.fetch_sub(sr_bytes, Ordering::Relaxed);
            }
        }
        conns.insert(
            (*conn_id).clone(),
            ConnectionHandle {
                tx: tx.clone(),
                pending: pending.clone(),
                broadcast_buf: broadcast_buf.clone(),
                broadcast_notify: broadcast_notify.clone(),
            },
        );
        state.connection_count.store(conns.len(), Ordering::Relaxed);
        state
            .connections_accepted_total
            .fetch_add(1, Ordering::Relaxed);
    }
    // Handshake + registration complete -- release the pre-handshake slot. (On any
    // earlier return, e.g. a failed/timed-out handshake, hs_guard drops here too.)
    drop(hs_guard);
    // Store format preference for lock-free access from send_event()
    if use_msgpack {
        state.conn_formats.insert((*conn_id).clone(), true);
    }
    // Initialize last activity for zombie detection
    state
        .conn_last_activity
        .insert((*conn_id).clone(), std::time::Instant::now());

    // Fire on_connect (drain mode: push to queue; callback mode: background task)
    if drain {
        if let Some(user_id) = rust_auth_user_id {
            // Register user_id for presence lookups
            if let Some(ref pm) = state.presence {
                pm.register_connection(&conn_id, &user_id);
            }
            // Rust validated JWT — send AuthConnect (Python skips JWT decode)
            state.push_inbound(InboundEvent::AuthConnect {
                conn_id: (*conn_id).clone(),
                user_id,
            });
        } else {
            // No JWT config — send cookies for Python to validate
            state.push_inbound(InboundEvent::Connect {
                conn_id: (*conn_id).clone(),
                cookies: cookie_str.clone(),
            });
        }
    } else {
        let cb_guard = state.on_connect.read().unwrap();
        if let Some(ref cb) = *cb_guard
            && let Some(cb_clone) = Python::try_attach(|py| cb.clone_ref(py))
        {
            let cid = (*conn_id).clone();
            let ck = cookie_str.clone();
            drop(cb_guard);
            // Fire in background — don't block connection setup on GIL
            tokio::task::spawn_blocking(move || {
                fire_on_connect(&cb_clone, &cid, &ck);
            });
        }
    }

    // Write task: dual-path (NATS-style broadcast buffer + mpsc control channel).
    //
    // Phase 1 (top of loop): drain broadcast_buf (direct bytes from broadcast_all)
    // Phase 2 (select!): wait for control mpsc OR broadcast notify
    //
    // Control messages (server_ready, pong, close, send_event, topic) go through mpsc.
    // Broadcast data goes through direct buffer (bypasses mpsc entirely).
    let pending_write = pending.clone();
    let broadcast_buf_write = broadcast_buf;
    let broadcast_notify_write = broadcast_notify;
    let write_task = tokio::spawn(async move {
        let mut batch: Vec<WsFrame> = Vec::with_capacity(256);
        let mut raw_slices: Vec<Bytes> = Vec::with_capacity(256);
        // Skip Phase 1 on first iteration so server_ready (in mpsc) is sent before
        // any broadcast data (in broadcast_buf). After the first iteration, Phase 1
        // always runs to avoid the Notify lost-permit race.
        let mut first_iteration = true;

        loop {
            // Phase 1: Drain broadcast buffer (skip on first iteration for server_ready ordering).
            // This avoids the Notify lost-permit race: if select! cancelled notified()
            // on a previous iteration, the data is still in the buffer.
            let broadcast_data = if first_iteration {
                first_iteration = false;
                None
            } else {
                let mut buf = broadcast_buf_write.lock();
                if !buf.is_empty() {
                    Some(buf.take()) // the shared frames, no copy
                } else {
                    None
                }
            };
            if let Some(frames) = broadcast_data {
                let len: usize = frames.iter().map(|f| f.len()).sum();
                if write_all_frames(&mut raw_write, &frames).await.is_err() {
                    let _ = pending_write.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        Some(cur.saturating_sub(len))
                    });
                    break;
                }
                let _ = pending_write.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(len))
                });
                continue; // check buffer again before blocking
            }

            // Phase 2: Buffer empty, wait for either source.
            tokio::select! {
                biased;

                // Control/topic messages (server_ready, close, pong, send_event, topic)
                n = rx.recv_many(&mut batch, 256) => {
                    if n == 0 {
                        break; // channel closed
                    }
                    let drained_bytes: usize = batch.iter().map(ws_frame_size).sum();

                    // Separate: PreFramed -> slices for writev, Msg -> tungstenite
                    raw_slices.clear();
                    let mut has_tung = false;
                    for frame in &batch {
                        match frame {
                            WsFrame::PreFramed(bytes) => raw_slices.push(bytes.clone()),
                            WsFrame::Msg(_) => has_tung = true,
                        }
                    }

                    let mut ok = true;

                    // Flush tungstenite Msgs FIRST to ensure its internal buffer
                    // is drained before raw_write uses the same fd
                    if has_tung && ok {
                        for frame in batch.drain(..) {
                            if let WsFrame::Msg(msg) = frame
                                && write_half.feed(msg).await.is_err()
                            {
                                ok = false;
                                break;
                            }
                        }
                        if ok && write_half.flush().await.is_err() {
                            ok = false;
                        }
                    }

                    // Write PreFramed via write_vectored (writev syscall)
                    if !raw_slices.is_empty() && ok {
                        let io_slices: Vec<std::io::IoSlice<'_>> = raw_slices
                            .iter()
                            .map(|b| std::io::IoSlice::new(b))
                            .collect();
                        let total: usize = raw_slices.iter().map(|b| b.len()).sum();

                        let written = match raw_write.write_vectored(&io_slices).await {
                            Ok(0) => { ok = false; 0 }
                            Ok(n) => n,
                            Err(_) => { ok = false; 0 }
                        };

                        if ok && written < total {
                            let mut remaining = BytesMut::with_capacity(total - written);
                            let mut skip = written;
                            for slice in &raw_slices {
                                if skip >= slice.len() {
                                    skip -= slice.len();
                                } else {
                                    remaining.extend_from_slice(&slice[skip..]);
                                    skip = 0;
                                }
                            }
                            if raw_write.write_all(&remaining).await.is_err() {
                                ok = false;
                            }
                        }
                    }

                    let _ = pending_write.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                        Some(cur.saturating_sub(drained_bytes))
                    });
                    batch.clear();

                    if !ok {
                        break;
                    }
                }

                // Broadcast buffer wakeup -- loop back to Phase 1 to drain
                _ = broadcast_notify_write.notified() => {
                    continue;
                }
            }
        }
        let _ = write_half.close().await;
    });

    // Cache on_message callback (only needed in callback mode)
    let cached_on_message: Option<Arc<Py<PyAny>>> = if !drain {
        let cb_guard = state.on_message.read().unwrap();
        cb_guard
            .as_ref()
            .and_then(|cb| Python::try_attach(|py| cb.clone_ref(py)).map(Arc::new))
    } else {
        None
    };
    // Bounds concurrent on_message callbacks in CALLBACK mode. Each inbound
    // message spawns a blocking task; without a cap a flood exhausts the blocking
    // thread pool and contends the GIL. When saturated the inbound is shed
    // (counted as rate-limited). Drain mode uses the bounded inbound_tx instead.
    let callback_sem = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_CALLBACKS));

    // Read loop
    while let Some(Ok(msg)) = read_half.next().await {
        match msg {
            Message::Text(text) => {
                state
                    .messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .bytes_received_total
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
                // Update last activity for zombie detection
                state
                    .conn_last_activity
                    .insert((*conn_id).clone(), std::time::Instant::now());
                let text_ref: &str = text.as_ref();

                // Strip prefix for JSON parsing
                let json_str = if text_ref.starts_with("WSE{") {
                    &text_ref[3..]
                } else if text_ref.starts_with("S{") || text_ref.starts_with("U{") {
                    &text_ref[1..]
                } else if text_ref.starts_with('{') {
                    text_ref
                } else {
                    // Non-JSON text — drain or callback
                    if drain {
                        state.push_inbound(InboundEvent::RawText {
                            conn_id: (*conn_id).clone(),
                            text: text.to_string(),
                        });
                    } else if let Some(ref cb) = cached_on_message {
                        let cb_arc = cb.clone();
                        let cid = Arc::clone(&conn_id);
                        let t = text.to_string();
                        spawn_bounded_callback(&callback_sem, &state, move || {
                            Python::try_attach(|py| {
                                if let Err(e) = cb_arc.call1(py, (&**cid, &*t)) {
                                    tracing::warn!("[WSE] on_message error: {e}");
                                }
                            });
                        });
                    }
                    continue;
                };

                // Try to parse JSON in Rust
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(val) => {
                        // Fast-path: protocol messages handled entirely in Rust
                        if let Some(t_val) = val.get("t").and_then(|t| t.as_str()) {
                            if t_val == "client_hello" {
                                let p = val.get("p");
                                let client_proto =
                                    p.and_then(|p| p.get("protocol_version"))
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(1) as u32;

                                // Check if client requests E2E encryption
                                let wants_encryption = p
                                    .and_then(|p| p.get("features"))
                                    .and_then(|f| f.get("encryption"))
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false);

                                // Once-only key exchange: never re-derive the cipher
                                // if this connection already has one. A repeated
                                // client_hello would otherwise overwrite the AES key
                                // mid-session (making in-flight encrypted messages
                                // undecryptable) and is a cheap ECDH-keygen DoS amplifier.
                                let server_pubkey: Option<String> = if wants_encryption
                                    && !state.conn_encryption.contains_key(&*conn_id)
                                {
                                    p.and_then(|p| p.get("encryption_public_key"))
                                        .and_then(|v| v.as_str())
                                        .and_then(|b64| match derive_connection_key(b64) {
                                            Ok((server_pk_b64, cipher)) => {
                                                state
                                                    .conn_encryption
                                                    .insert((*conn_id).clone(), cipher);
                                                Some(server_pk_b64)
                                            }
                                            Err(e) => {
                                                tracing::error!(
                                                    "[WSE] ECDH key exchange failed for {}: {}",
                                                    conn_id, e
                                                );
                                                let err_msg = format!(
                                                    "{{\"c\":\"WSE\",\"t\":\"error\",\"p\":{{\"code\":\"KEY_EXCHANGE_FAILED\",\"message\":\"ECDH key exchange failed: {}\",\"recoverable\":true}}}}",
                                                    e.replace('\"', "\\\"")
                                                );
                                                let em_bytes = err_msg.len() + 4;
                                                pending.fetch_add(em_bytes, Ordering::Relaxed);
                                                if tx.send(WsFrame::Msg(Message::Text(err_msg.into()))).is_err() {
                                                    pending.fetch_sub(em_bytes, Ordering::Relaxed);
                                                }
                                                None
                                            }
                                        })
                                } else {
                                    None
                                };

                                let hello = build_server_hello(
                                    &conn_id,
                                    client_proto,
                                    &state,
                                    server_pubkey.as_deref(),
                                );
                                let h_bytes = hello.len() + 4;
                                pending.fetch_add(h_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Text(hello.into()))).is_err() {
                                    pending.fetch_sub(h_bytes, Ordering::Relaxed);
                                }
                                continue;
                            }
                            if t_val == "ping" {
                                let timestamp = val
                                    .get("p")
                                    .and_then(|p| p.get("timestamp"))
                                    .and_then(|ts| ts.as_i64())
                                    .unwrap_or(0);
                                let server_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis()
                                    as i64;
                                let pong = format!(
                                    "{{\"c\":\"WSE\",\"t\":\"PONG\",\"p\":{{\"client_timestamp\":{},\"server_timestamp\":{},\"latency\":{}}},\"v\":1}}",
                                    timestamp,
                                    server_ts,
                                    server_ts.saturating_sub(timestamp).max(0)
                                );
                                let po_bytes = pong.len() + 4;
                                pending.fetch_add(po_bytes, Ordering::Relaxed);
                                if tx.send(WsFrame::Msg(Message::Text(pong.into()))).is_err() {
                                    pending.fetch_sub(po_bytes, Ordering::Relaxed);
                                }
                                continue;
                            }
                            if t_val == "pong" || t_val == "PONG" {
                                continue;
                            }
                        }

                        // Normalize type→t, payload→p in serde_json (no GIL needed)
                        let val = normalize_json_keys(val);

                        // Drain mode: push serde_json::Value to queue (zero GIL)
                        if drain {
                            state.push_inbound(InboundEvent::Message {
                                conn_id: (*conn_id).clone(),
                                value: val,
                            });
                        } else if let Some(ref cb) = cached_on_message {
                            // Callback mode: convert to PyDict + call Python
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            spawn_bounded_callback(&callback_sem, &state, move || {
                                Python::try_attach(|py| {
                                    let py_obj = json_to_pyobj(py, &val);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_obj)) {
                                        tracing::warn!("[WSE] on_message error: {e}");
                                    }
                                });
                            });
                        }
                    }
                    Err(_) => {
                        // JSON parse failed — pass raw string
                        if drain {
                            state.push_inbound(InboundEvent::RawText {
                                conn_id: (*conn_id).clone(),
                                text: text.to_string(),
                            });
                        } else if let Some(ref cb) = cached_on_message {
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            let t = text.to_string();
                            spawn_bounded_callback(&callback_sem, &state, move || {
                                Python::try_attach(|py| {
                                    if let Err(e) = cb_arc.call1(py, (&**cid, &*t)) {
                                        tracing::warn!("[WSE] on_message error: {e}");
                                    }
                                });
                            });
                        }
                    }
                }
            }
            Message::Binary(data) => {
                state
                    .messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .bytes_received_total
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                // Update last activity for zombie detection
                state
                    .conn_last_activity
                    .insert((*conn_id).clone(), std::time::Instant::now());

                // E2E encryption: detect E: prefix and decrypt before processing
                if data.len() >= 2 && data[0] == b'E' && data[1] == b':' {
                    if let Some(cipher_ref) = state.conn_encryption.get(&*conn_id) {
                        match decrypt_inbound(&cipher_ref, &data[2..]) {
                            Ok(plaintext) => {
                                // Decrypted payload is UTF-8 text (JSON with optional prefix)
                                if let Ok(text) = std::str::from_utf8(&plaintext) {
                                    let json_str = if text.starts_with("WSE{") {
                                        &text[3..]
                                    } else if text.starts_with("S{") || text.starts_with("U{") {
                                        &text[1..]
                                    } else if text.starts_with('{') {
                                        text
                                    } else {
                                        if drain {
                                            state.push_inbound(InboundEvent::RawText {
                                                conn_id: (*conn_id).clone(),
                                                text: text.to_string(),
                                            });
                                        }
                                        continue;
                                    };
                                    if let Ok(val) =
                                        serde_json::from_str::<serde_json::Value>(json_str)
                                    {
                                        let val = normalize_json_keys(val);
                                        if drain {
                                            state.push_inbound(InboundEvent::Message {
                                                conn_id: (*conn_id).clone(),
                                                value: val,
                                            });
                                        } else if let Some(ref cb) = cached_on_message {
                                            let cb_arc = cb.clone();
                                            let cid = Arc::clone(&conn_id);
                                            spawn_bounded_callback(
                                                &callback_sem,
                                                &state,
                                                move || {
                                                    Python::try_attach(|py| {
                                                        let py_obj = json_to_pyobj(py, &val);
                                                        if let Err(e) =
                                                            cb_arc.call1(py, (&**cid, py_obj))
                                                        {
                                                            tracing::warn!(
                                                                "[WSE] on_message (decrypted) error: {e}"
                                                            );
                                                        }
                                                    });
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("[WSE] Decryption failed for {}: {}", conn_id, e);
                            }
                        }
                    } else {
                        tracing::error!(
                            "[WSE] Received E: frame but no encryption key for {}",
                            conn_id
                        );
                    }
                    continue;
                }

                // Check if this connection uses msgpack (lock-free DashMap read)
                let is_msgpack = state
                    .conn_formats
                    .get(&*conn_id)
                    .map(|v| *v)
                    .unwrap_or(false);

                if drain {
                    if is_msgpack {
                        // Parse msgpack in Rust → push pre-parsed dict (same as JSON text path)
                        match decode_msgpack_guarded(&data[..]) {
                            Ok(rmpv_val) => {
                                let json_val = rmpv_to_serde_json(&rmpv_val);
                                let json_val = normalize_json_keys(json_val);
                                state.push_inbound(InboundEvent::Message {
                                    conn_id: (*conn_id).clone(),
                                    value: json_val,
                                });
                            }
                            Err(_) => {
                                // Not valid msgpack — pass raw bytes
                                state.push_inbound(InboundEvent::Binary {
                                    conn_id: (*conn_id).clone(),
                                    data: data.to_vec(),
                                });
                            }
                        }
                    } else {
                        state.push_inbound(InboundEvent::Binary {
                            conn_id: (*conn_id).clone(),
                            data: data.to_vec(),
                        });
                    }
                } else if let Some(ref cb) = cached_on_message {
                    let cb_arc = cb.clone();
                    let cid = Arc::clone(&conn_id);
                    let d = data.to_vec();
                    if is_msgpack {
                        // Parse msgpack and pass as dict to callback (same as JSON text)
                        spawn_bounded_callback(&callback_sem, &state, move || {
                            Python::try_attach(|py| match decode_msgpack_guarded(&d[..]) {
                                Ok(rmpv_val) => {
                                    let json_val = rmpv_to_serde_json(&rmpv_val);
                                    let json_val = normalize_json_keys(json_val);
                                    let py_obj = json_to_pyobj(py, &json_val);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_obj)) {
                                        tracing::warn!("[WSE] on_message (msgpack) error: {e}");
                                    }
                                }
                                Err(_) => {
                                    let py_bytes = PyBytes::new(py, &d);
                                    if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                        tracing::warn!("[WSE] on_message (binary) error: {e}");
                                    }
                                }
                            });
                        });
                    } else {
                        spawn_bounded_callback(&callback_sem, &state, move || {
                            Python::try_attach(|py| {
                                let py_bytes = PyBytes::new(py, &d);
                                if let Err(e) = cb_arc.call1(py, (&**cid, py_bytes)) {
                                    tracing::warn!("[WSE] on_message (binary) error: {e}");
                                }
                            });
                        });
                    }
                }
            }
            Message::Ping(payload) => {
                let pong_bytes = payload.len() + 4;
                pending.fetch_add(pong_bytes, Ordering::Relaxed);
                if tx.send(WsFrame::Msg(Message::Pong(payload))).is_err() {
                    pending.fetch_sub(pong_bytes, Ordering::Relaxed);
                }
            }
            Message::Close(_) => break,
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    // Cleanup - check if zombie detection already removed this connection
    let already_removed = {
        let mut conns = state.connections.write().await;
        let was_present = conns.remove(&*conn_id).is_some();
        state.connection_count.store(conns.len(), Ordering::Relaxed);
        !was_present
    };
    state.conn_formats.remove(&*conn_id);
    state.conn_rates.remove(&*conn_id);
    state.conn_last_activity.remove(&*conn_id);
    state.conn_encryption.remove(&*conn_id);
    state.conn_topic_acl.remove(&*conn_id);
    // Queue group cleanup: remove this connection from all queue groups
    if let Some((_, memberships)) = state.conn_queue_groups.remove(&*conn_id) {
        for (topic, group_name) in memberships {
            if let Some(topic_groups) = state.queue_groups.get(&topic) {
                if let Some(mut group) = topic_groups.get_mut(&group_name) {
                    group.members.retain(|(id, _)| id != &*conn_id);
                }
                // Clean up empty group
                topic_groups.remove_if(&group_name, |_, g| g.members.is_empty());
            }
            // Clean up empty topic entry
            state
                .queue_groups
                .remove_if(&topic, |_, groups| groups.is_empty());
        }
    }
    // If zombie detection already removed this connection and emitted
    // a disconnect event, skip the rest to avoid duplicate events.
    if already_removed {
        drop(tx);
        let _ = write_task.await;
        return;
    }
    // Presence cleanup: untrack from all topics before disconnect event
    if let Some(ref pm) = state.presence {
        let results = pm.remove_connection(&conn_id);
        let cluster_tx = state.cluster_cmd_tx.read().unwrap().clone();
        for (topic, result) in results {
            if let crate::wse::presence::UntrackResult::LastLeave { user_id, data } = result {
                if drain {
                    state.push_inbound(InboundEvent::PresenceLeave {
                        topic: topic.clone(),
                        user_id: user_id.clone(),
                        data: data.clone(),
                    });
                }
                // Broadcast to topic subscribers via WebSocket
                let tx = state.presence_broadcast_tx.read().unwrap().clone();
                if let Some(tx) = tx {
                    let msg = format_presence_msg("presence_leave", &user_id, &data);
                    let _ = tx.send(ServerCommand::BroadcastLocal {
                        topic: topic.clone(),
                        data: msg,
                        skip_recovery: true,
                        publish: false,
                    });
                }
                // Cluster sync: notify peers of presence leave
                if let Some(ref ctx) = cluster_tx {
                    let data_str = serde_json::to_string(&data).unwrap_or_default();
                    let now = crate::wse::presence::epoch_ms();
                    let _ = ctx.send(ClusterCommand::PresenceUpdate {
                        topic,
                        user_id,
                        action: 1, // leave
                        data: data_str,
                        updated_at: now,
                    });
                }
            }
        }
    }
    // Pub/Sub cleanup: remove connection from all subscribed topics
    if let Some((_, topics)) = state.conn_topics.remove(&*conn_id) {
        let mut disconnected_topics = Vec::new();
        for topic_ref in topics.iter() {
            let topic = topic_ref.clone();
            if let Some(subscribers) = state.topic_subscribers.get(&topic) {
                subscribers.remove(&*conn_id);
            }
            if state
                .topic_subscribers
                .remove_if(&topic, |_, subs| subs.is_empty())
                .is_some()
                && (topic.contains('*') || topic.contains('?'))
            {
                state.glob_topic_count.fetch_sub(1, Ordering::Relaxed);
            }
            disconnected_topics.push(topic);
        }

        state.remove_local_interest(&disconnected_topics);
    }
    if drain {
        state.push_inbound(InboundEvent::Disconnect {
            conn_id: (*conn_id).clone(),
        });
    } else {
        let cb_guard = state.on_disconnect.read().unwrap();
        if let Some(ref cb) = *cb_guard
            && let Some(cb_clone) = Python::try_attach(|py| cb.clone_ref(py))
        {
            let cid = (*conn_id).clone();
            drop(cb_guard);
            // Fire in background — don't block cleanup on GIL
            tokio::task::spawn_blocking(move || {
                fire_on_disconnect(&cb_clone, &cid);
            });
        }
    }
    drop(tx);
    let _ = write_task.await;
}
