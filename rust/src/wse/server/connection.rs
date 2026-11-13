//! One client connection, from the WebSocket upgrade to the cleanup.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{StatusCode, header};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use uuid::Uuid;

use super::e2e::{decrypt_inbound, derive_connection_key};
use super::frames::{
    close_frame, encode_message, encode_ws_frame, write_all_frames, ws_frame_size,
};
use super::handshake::{HandshakeGuard, normalize_origin, reject_ws_auth};
use super::messages::{build_server_hello, build_server_ready, normalize_json_keys};
use super::py::{fire_on_connect, fire_on_disconnect, json_to_pyobj};
use super::state::InboundEvent;
use super::{
    BroadcastQueue, CLOSE_GRACE, CLOSE_RATE_LIMITED, CLOSE_TOKEN_EXPIRED, CLOSE_TRY_AGAIN_LATER,
    Closer, ConnectionHandle, ServerCommand, SharedState, WsFrame, format_presence_msg,
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

/// Run an on_message callback on the blocking pool. It needs one of the
/// connection's `MAX_INFLIGHT_CALLBACKS` permits (a client flooding messages
/// is shed, counted in `rate_limited_total`) and room for its `bytes` in the
/// server's budget of messages waiting for the application, the same one
/// drain mode uses (counted in `inbound_dropped` when there is none).
fn spawn_bounded_callback<F>(
    sem: &Arc<tokio::sync::Semaphore>,
    state: &Arc<SharedState>,
    bytes: usize,
    work: F,
) where
    F: FnOnce() + Send + 'static,
{
    let Ok(permit) = sem.clone().try_acquire_owned() else {
        state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let Some(room) = SharedState::callback_room(state, bytes) else {
        return;
    };
    tokio::task::spawn_blocking(move || {
        let _held = (permit, room);
        work();
    });
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

/// The moment a token with expiry `exp` (seconds since the epoch) stops
/// being valid: `exp` plus the clock skew the validation allows.
fn token_valid_until(exp: u64) -> tokio::time::Instant {
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let left = exp
        .saturating_add(jwt::CLOCK_SKEW_SECS)
        .saturating_sub(now_unix);
    // Far-future values would overflow the timer; a year is never reached.
    tokio::time::Instant::now() + Duration::from_secs(left.min(365 * 24 * 3600))
}

/// Why the upgrade was refused, if it was.
const ACCEPTED: u8 = 0;
const REFUSED_FULL: u8 = 1;
const REFUSED_ORIGIN: u8 = 2;

/// The answer to an upgrade the server does not take: 503 (with
/// Retry-After) when it is full or draining, 403 for a foreign origin.
fn refusal(status: StatusCode) -> ErrorResponse {
    let mut response = ErrorResponse::new(None);
    *response.status_mut() = status;
    if status == StatusCode::SERVICE_UNAVAILABLE {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("5"));
    }
    response
}

/// The client socket as tungstenite sees it. Once the upgrade is done its
/// writes go nowhere: the connection's write task writes every frame to the
/// client, so an automatic pong or close reply of tungstenite's cannot land in
/// the middle of one of its frames.
struct ServerSocket {
    inner: TcpStream,
    silent: bool,
}

impl AsyncRead for ServerSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ServerSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.silent {
            return Poll::Ready(Ok(buf.len()));
        }
        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.silent {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.silent {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

const RATE_LIMITED_ERROR: &str = "{\"c\":\"WSE\",\"t\":\"error\",\"p\":{\"code\":\"RATE_LIMITED\",\"message\":\"Rate limit exceeded\",\"retry_after\":1.0},\"v\":1}";

/// A client's bad frames are logged at most once every 10 seconds per
/// connection: a client sending garbage must not flood the log.
#[derive(Default)]
struct FrameErrorLog {
    last: Option<Instant>,
    suppressed: u64,
}

impl FrameErrorLog {
    fn warn(&mut self, conn_id: &str, what: &str) {
        let now = Instant::now();
        if self
            .last
            .is_some_and(|t| now.duration_since(t) < Duration::from_secs(10))
        {
            self.suppressed += 1;
            return;
        }
        if self.suppressed > 0 {
            tracing::warn!(
                "[WSE] {conn_id}: {what} ({} more since the last report)",
                self.suppressed
            );
        } else {
            tracing::warn!("[WSE] {conn_id}: {what}");
        }
        self.last = Some(now);
        self.suppressed = 0;
    }
}

/// Control frames the write task takes from the queue at a time, before it
/// turns to the broadcast frames again.
const CONTROL_BATCH: usize = 256;

fn release(pending: &AtomicUsize, bytes: usize) {
    let _ = pending.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        Some(cur.saturating_sub(bytes))
    });
}

/// How a batch of control frames went out.
enum Sent {
    Frames,
    /// The server's close frame went out: nothing may follow it.
    Close,
    Failed,
}

/// Write the queued control frames in order, up to the server's close frame;
/// whatever was queued after the close is dropped.
async fn write_control(
    sock: &mut TcpStream,
    batch: &mut Vec<WsFrame>,
    wire: &mut VecDeque<Bytes>,
    pending: &AtomicUsize,
) -> Sent {
    let queued: usize = batch.iter().map(ws_frame_size).sum();
    let mut close = false;
    for frame in batch.drain(..) {
        match frame {
            WsFrame::PreFramed(bytes) => wire.push_back(bytes),
            WsFrame::Msg(msg) => {
                close = matches!(msg, Message::Close(_));
                if let Some(bytes) = encode_message(&msg) {
                    wire.push_back(bytes);
                }
                if close {
                    break;
                }
            }
        }
    }
    let result = write_all_frames(sock, wire).await;
    wire.clear();
    release(pending, queued);
    match (result, close) {
        (Err(_), _) => Sent::Failed,
        (Ok(()), true) => Sent::Close,
        (Ok(()), false) => Sent::Frames,
    }
}

/// What the read side saw of the client's close frame, for the reply: no
/// close frame, a close frame without a code, or the code.
const NO_PEER_CLOSE: u32 = 0;
const PEER_CLOSE_WITHOUT_CODE: u32 = u32::MAX;

/// The reply to a close the client started: its own code (RFC 6455 5.5.1).
fn close_reply(peer: u32) -> Option<Bytes> {
    match peer {
        NO_PEER_CLOSE => None,
        PEER_CLOSE_WITHOUT_CODE => Some(encode_ws_frame(0x08, &[])),
        code => Some(close_frame(code as u16, "")),
    }
}

/// Queue a frame the server makes on its own (server_hello, PONG, an error).
/// A client that sends but does not read must not grow its queue with them:
/// over `max_outbound_queue_bytes` the frame is dropped.
fn queue_reply(
    tx: &mpsc::UnboundedSender<WsFrame>,
    pending: &AtomicUsize,
    max_pending: usize,
    msg: Message,
) {
    let frame = WsFrame::Msg(msg);
    let bytes = ws_frame_size(&frame);
    if max_pending > 0 && pending.load(Ordering::Relaxed) + bytes > max_pending {
        return;
    }
    pending.fetch_add(bytes, Ordering::Relaxed);
    if tx.send(frame).is_err() {
        release(pending, bytes);
    }
}

/// The client's messages per connection: a token bucket of
/// `rate_limit_capacity` messages, refilled at `rate_limit_refill` per second.
/// The client is warned at 20% left, its messages are dropped below one token,
/// and it is closed once it has kept sending into an empty bucket for
/// `ABUSE_AFTER`.
struct InboundLimit {
    capacity: f64,
    refill: f64,
    tokens: f64,
    last_refill: Instant,
    last_warning: Option<Instant>,
    last_notice: Option<Instant>,
    last_drop: Option<Instant>,
    /// First drop of the current run of drops, none more than a second apart.
    drops_since: Option<Instant>,
}

enum Inbound {
    Pass,
    /// Passed; tell the client how close it is to the limit.
    Warn {
        used: f64,
        left: f64,
    },
    /// Dropped; `notify`: tell the client (at most once a second).
    Limited {
        notify: bool,
    },
    Close,
}

impl InboundLimit {
    const ABUSE_AFTER: Duration = Duration::from_secs(10);

    fn new(capacity: f64, refill: f64) -> Self {
        Self {
            capacity,
            refill,
            tokens: capacity,
            last_refill: Instant::now(),
            last_warning: None,
            last_notice: None,
            last_drop: None,
            drops_since: None,
        }
    }

    fn check(&mut self) -> Inbound {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill).min(self.capacity);
        self.last_refill = now;
        let a_second_since = |last: Option<Instant>| {
            last.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1))
        };
        if self.tokens < 1.0 {
            let since = match self.drops_since {
                Some(since) if !a_second_since(self.last_drop) => since,
                _ => now,
            };
            self.drops_since = Some(since);
            self.last_drop = Some(now);
            if now.duration_since(since) >= Self::ABUSE_AFTER {
                return Inbound::Close;
            }
            let notify = a_second_since(self.last_notice);
            if notify {
                self.last_notice = Some(now);
            }
            return Inbound::Limited { notify };
        }
        self.tokens -= 1.0;
        if self.tokens < self.capacity * 0.2 && a_second_since(self.last_warning) {
            self.last_warning = Some(now);
            return Inbound::Warn {
                used: self.capacity - self.tokens,
                left: self.tokens,
            };
        }
        Inbound::Pass
    }
}

pub(super) async fn handle_connection(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<SharedState>,
    hs_guard: HandshakeGuard,
) {
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
    // A draining or full server answers the upgrade with 503 and Retry-After
    // (the count is checked again when the connection registers); a browser
    // page from an origin not in allowed_origins gets 403.
    let refused = Arc::new(AtomicU8::new(ACCEPTED));
    let refused_in_upgrade = refused.clone();
    let gate = state.clone();

    let mut ws_config = WebSocketConfig::default();
    ws_config.max_message_size = Some(state.max_message_size);
    ws_config.max_frame_size = Some(state.max_message_size);
    // 10-second timeout prevents slow loris attacks on the handshake
    let ws_stream = match tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::accept_hdr_async_with_config(
            ServerSocket {
                inner: stream,
                silent: false,
            },
            #[expect(
                clippy::result_large_err,
                reason = "the tungstenite accept_hdr callback signature fixes Err = ErrorResponse; we cannot box it"
            )]
            move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
                if gate.draining.load(Ordering::Relaxed)
                    || gate.connection_count.load(Ordering::Relaxed) >= gate.max_connections
                {
                    refused_in_upgrade.store(REFUSED_FULL, Ordering::Relaxed);
                    return Err(refusal(StatusCode::SERVICE_UNAVAILABLE));
                }
                // Only browsers send Origin, and only they carry a victim's
                // cookies to another site's page.
                if let Some(ref allowed) = gate.allowed_origins
                    && let Some(origin) = req.headers().get("origin")
                    && !origin
                        .to_str()
                        .ok()
                        .and_then(|o| normalize_origin(o).ok())
                        .is_some_and(|o| allowed.contains(&o))
                {
                    refused_in_upgrade.store(REFUSED_ORIGIN, Ordering::Relaxed);
                    return Err(refusal(StatusCode::FORBIDDEN));
                }
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
        Ok(Err(_)) if refused.load(Ordering::Relaxed) != ACCEPTED => {
            state
                .connections_rejected_total
                .fetch_add(1, Ordering::Relaxed);
            if refused.load(Ordering::Relaxed) == REFUSED_ORIGIN {
                tracing::warn!("[WSE] Origin not allowed, refused {addr}");
            } else if !state.draining.load(Ordering::Relaxed) {
                tracing::warn!("[WSE] Max connections reached, refused {addr}");
            }
            return;
        }
        Ok(Err(e)) => {
            tracing::warn!("[WSE] WS handshake failed for {addr}: {e}");
            return;
        }
        Err(_) => {
            tracing::warn!("[WSE] WS handshake timeout for {addr}");
            return;
        }
    };

    // From here on tungstenite only reads.
    let mut read_half = ws_stream;
    read_half.get_mut().silent = true;

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
    let (tx, mut rx) = mpsc::unbounded_channel::<WsFrame>();
    let pending = Arc::new(AtomicUsize::new(0));
    let broadcast_buf = Arc::new(parking_lot::Mutex::new(BroadcastQueue::new()));
    let broadcast_notify = Arc::new(tokio::sync::Notify::new());
    let drain = state.drain_mode.load(Ordering::Relaxed);

    // ── JWT validation in Rust (zero GIL) ──────────────────────────────────
    // When jwt_config is set, validate JWT from cookie or Authorization header.
    // On success: send server_ready immediately, push AuthConnect to drain queue.
    // On failure: send error + close — connection never registered.
    // When the token stops being valid (its exp, plus the clock skew the
    // validation allows).
    let mut token_deadline: Option<tokio::time::Instant> = None;
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
                        token_deadline = claims
                            .get("exp")
                            .and_then(serde_json::Value::as_u64)
                            .map(token_valid_until);
                        Some(user_id.to_string())
                    } else {
                        state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                        state
                            .connections_rejected_total
                            .fetch_add(1, Ordering::Relaxed);
                        reject_ws_auth(&mut raw_write, "AUTH_FAILED", "Token missing user ID")
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
                    reject_ws_auth(&mut raw_write, "AUTH_FAILED", "Authentication failed").await;
                    return;
                }
            },
            None => {
                state.auth_failures_total.fetch_add(1, Ordering::Relaxed);
                state
                    .connections_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                reject_ws_auth(
                    &mut raw_write,
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

    // Lets the server end this connection: a close frame, then the socket.
    let closer = Closer::new();

    // Register connection
    let registered = {
        let mut conns = state.connections.write().await;
        let room = conns.len() < state.max_connections;
        if room {
            // server_ready goes into the queue before the connection is in the
            // map: while the write lock is held no broadcast can reach it, so
            // server_ready is the first frame.
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
                    closer: closer.clone(),
                },
            );
            state.connection_count.store(conns.len(), Ordering::Relaxed);
        }
        room
    };
    if !registered {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        tracing::warn!("[WSE] Max connections reached, rejecting {addr}");
        // The ACL was stored before the check.
        state.conn_topic_acl.remove(&*conn_id);
        let frames = VecDeque::from([close_frame(CLOSE_TRY_AGAIN_LATER, "server full")]);
        let _ = tokio::time::timeout(
            Duration::from_secs(1),
            write_all_frames(&mut raw_write, &frames),
        )
        .await;
        return;
    }
    state
        .connections_accepted_total
        .fetch_add(1, Ordering::Relaxed);
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

    // Write task: the only writer to the socket. Two sources: the control
    // queue (server_ready, replies, send_event, close) and the broadcast buffer
    // (fan-out frames shared with every subscriber, not copied).
    let pending_write = pending.clone();
    let broadcast_buf_write = broadcast_buf;
    let broadcast_notify_write = broadcast_notify;
    // `finish`: the read side is done, send nothing more but the close reply.
    // The closer's token: the socket goes now, even in the middle of a frame.
    let finish = tokio_util::sync::CancellationToken::new();
    let write_finish = finish.clone();
    let write_abort = closer.token().clone();
    let peer_close = Arc::new(AtomicU32::new(NO_PEER_CLOSE));
    let write_peer_close = peer_close.clone();
    let write_task = tokio::spawn(async move {
        let mut batch: Vec<WsFrame> = Vec::with_capacity(CONTROL_BATCH);
        let mut wire: VecDeque<Bytes> = VecDeque::with_capacity(CONTROL_BATCH);
        let mut close_sent = false;
        let work = async {
            loop {
                if write_finish.is_cancelled() {
                    break;
                }
                // Control frames first, without waiting: server_ready goes out
                // before any broadcast, and a close, a pong or a reply is not
                // held back by a busy topic.
                while batch.len() < CONTROL_BATCH {
                    match rx.try_recv() {
                        Ok(frame) => batch.push(frame),
                        Err(_) => break,
                    }
                }
                let mut wrote = false;
                if !batch.is_empty() {
                    wrote = true;
                    match write_control(&mut raw_write, &mut batch, &mut wire, &pending_write).await
                    {
                        Sent::Frames => {}
                        Sent::Close => {
                            close_sent = true;
                            break;
                        }
                        Sent::Failed => break,
                    }
                }
                // Then the broadcast frames queued so far.
                let shared = {
                    let mut buf = broadcast_buf_write.lock();
                    (!buf.is_empty()).then(|| buf.take())
                };
                if let Some(frames) = shared {
                    wrote = true;
                    let len: usize = frames.iter().map(Bytes::len).sum();
                    let result = write_all_frames(&mut raw_write, &frames).await;
                    release(&pending_write, len);
                    if result.is_err() {
                        break;
                    }
                }
                if wrote {
                    continue;
                }
                tokio::select! {
                    biased;
                    () = write_finish.cancelled() => break,
                    n = rx.recv_many(&mut batch, CONTROL_BATCH) => {
                        if n == 0 {
                            break; // every sender is gone
                        }
                    }
                    () = broadcast_notify_write.notified() => {}
                }
            }
        };
        let finished = tokio::select! {
            biased;
            () = write_abort.cancelled() => false,
            () = work => true,
        };
        // The client closed first: answer it.
        if finished
            && !close_sent
            && let Some(reply) = close_reply(write_peer_close.load(Ordering::Acquire))
        {
            let frames = VecDeque::from([reply]);
            tokio::select! {
                () = write_abort.cancelled() => {}
                _ = write_all_frames(&mut raw_write, &frames) => {}
            }
        }
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

    let max_pending = state.max_outbound_queue_bytes;
    let mut inbound_limit = InboundLimit::new(state.rate_capacity, state.rate_refill);
    // One server_hello per connection: a client repeating client_hello would
    // otherwise get one back each time.
    let mut hello_answered = false;
    let mut frame_errors = FrameErrorLog::default();

    // A connection whose token expires is closed (4403): the clients come
    // back with a new token from their token provider.
    let mut expiry = token_deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline)));

    // Read loop. It ends when the client closes or the socket fails, and when
    // the server closed the connection and the client did not answer in time.
    let read_cancel = closer.token().clone();
    loop {
        let msg = tokio::select! {
            biased;
            () = read_cancel.cancelled() => break,
            () = async { if let Some(sleep) = expiry.as_mut() { sleep.await } }, if expiry.is_some() => {
                expiry = None;
                closer.close(&tx, &pending, CLOSE_TOKEN_EXPIRED, "token expired");
                continue;
            }
            next = read_half.next() => match next {
                Some(Ok(msg)) => msg,
                _ => break,
            },
        };
        // After the server queued a close, the client's further messages are
        // not delivered: only its close frame, or the grace period, ends this.
        if closer.is_closing() && !matches!(msg, Message::Close(_)) {
            continue;
        }
        // Every frame counts as activity, protocol pings and pongs included:
        // a plain WebSocket client may keep the link alive with those alone.
        if let Some(mut last) = state.conn_last_activity.get_mut(&*conn_id) {
            *last = std::time::Instant::now();
        }
        if matches!(msg, Message::Text(_) | Message::Binary(_)) {
            match inbound_limit.check() {
                Inbound::Pass => {}
                Inbound::Warn { used, left } => {
                    let warning = format!(
                        "{{\"c\":\"WSE\",\"t\":\"rate_limit_warning\",\"p\":{{\"current_rate\":{},\"limit\":{},\"remaining\":{},\"retry_after\":1.0}},\"v\":1}}",
                        used as u64, inbound_limit.capacity as u64, left as u64,
                    );
                    queue_reply(&tx, &pending, max_pending, Message::Text(warning.into()));
                }
                Inbound::Limited { notify } => {
                    state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
                    if notify {
                        queue_reply(
                            &tx,
                            &pending,
                            max_pending,
                            Message::Text(RATE_LIMITED_ERROR.into()),
                        );
                    }
                    continue;
                }
                Inbound::Close => {
                    state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
                    closer.close(&tx, &pending, CLOSE_RATE_LIMITED, "rate limit exceeded");
                    continue;
                }
            }
        }
        match msg {
            Message::Text(text) => {
                state
                    .messages_received_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .bytes_received_total
                    .fetch_add(text.len() as u64, Ordering::Relaxed);
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
                        state.push_message(
                            InboundEvent::RawText {
                                conn_id: (*conn_id).clone(),
                                text: text.to_string(),
                            },
                            text.len(),
                        );
                    } else if let Some(ref cb) = cached_on_message {
                        let cb_arc = cb.clone();
                        let cid = Arc::clone(&conn_id);
                        let t = text.to_string();
                        spawn_bounded_callback(&callback_sem, &state, t.len(), move || {
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
                                if hello_answered {
                                    continue;
                                }
                                hello_answered = true;
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
                                let key = if wants_encryption {
                                    p.and_then(|p| p.get("encryption_public_key"))
                                        .and_then(|v| v.as_str())
                                        .and_then(|b64| match derive_connection_key(b64) {
                                            Ok(key) => Some(key),
                                            Err(e) => {
                                                tracing::warn!(
                                                    "[WSE] ECDH key exchange failed for {}: {}",
                                                    conn_id, e
                                                );
                                                let err_msg = format!(
                                                    "{{\"c\":\"WSE\",\"t\":\"error\",\"p\":{{\"code\":\"KEY_EXCHANGE_FAILED\",\"message\":\"ECDH key exchange failed: {}\",\"recoverable\":true}}}}",
                                                    e.replace('\"', "\\\"")
                                                );
                                                queue_reply(
                                                    &tx,
                                                    &pending,
                                                    max_pending,
                                                    Message::Text(err_msg.into()),
                                                );
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
                                    key.as_ref()
                                        .map(|(server_public, _)| server_public.as_str()),
                                );
                                queue_reply(
                                    &tx,
                                    &pending,
                                    max_pending,
                                    Message::Text(hello.into()),
                                );
                                // The key is in use only after server_hello is queued:
                                // an encrypted frame never reaches the client before
                                // the server's public key.
                                if let Some((_, cipher)) = key {
                                    state.conn_encryption.insert((*conn_id).clone(), cipher);
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
                                queue_reply(&tx, &pending, max_pending, Message::Text(pong.into()));
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
                            state.push_message(
                                InboundEvent::Message {
                                    conn_id: (*conn_id).clone(),
                                    value: val,
                                },
                                text.len(),
                            );
                        } else if let Some(ref cb) = cached_on_message {
                            // Callback mode: convert to PyDict + call Python
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            spawn_bounded_callback(&callback_sem, &state, text.len(), move || {
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
                            state.push_message(
                                InboundEvent::RawText {
                                    conn_id: (*conn_id).clone(),
                                    text: text.to_string(),
                                },
                                text.len(),
                            );
                        } else if let Some(ref cb) = cached_on_message {
                            let cb_arc = cb.clone();
                            let cid = Arc::clone(&conn_id);
                            let t = text.to_string();
                            spawn_bounded_callback(&callback_sem, &state, t.len(), move || {
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
                                            state.push_message(
                                                InboundEvent::RawText {
                                                    conn_id: (*conn_id).clone(),
                                                    text: text.to_string(),
                                                },
                                                text.len(),
                                            );
                                        }
                                        continue;
                                    };
                                    if let Ok(val) =
                                        serde_json::from_str::<serde_json::Value>(json_str)
                                    {
                                        let val = normalize_json_keys(val);
                                        if drain {
                                            state.push_message(
                                                InboundEvent::Message {
                                                    conn_id: (*conn_id).clone(),
                                                    value: val,
                                                },
                                                text.len(),
                                            );
                                        } else if let Some(ref cb) = cached_on_message {
                                            let cb_arc = cb.clone();
                                            let cid = Arc::clone(&conn_id);
                                            spawn_bounded_callback(
                                                &callback_sem,
                                                &state,
                                                text.len(),
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
                            Err(e) => frame_errors.warn(&conn_id, &e),
                        }
                    } else {
                        frame_errors.warn(&conn_id, "encrypted frame before the key exchange");
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
                                state.push_message(
                                    InboundEvent::Message {
                                        conn_id: (*conn_id).clone(),
                                        value: json_val,
                                    },
                                    data.len(),
                                );
                            }
                            Err(_) => {
                                // Not valid msgpack — pass raw bytes
                                state.push_message(
                                    InboundEvent::Binary {
                                        conn_id: (*conn_id).clone(),
                                        data: data.to_vec(),
                                    },
                                    data.len(),
                                );
                            }
                        }
                    } else {
                        state.push_message(
                            InboundEvent::Binary {
                                conn_id: (*conn_id).clone(),
                                data: data.to_vec(),
                            },
                            data.len(),
                        );
                    }
                } else if let Some(ref cb) = cached_on_message {
                    let cb_arc = cb.clone();
                    let cid = Arc::clone(&conn_id);
                    let d = data.to_vec();
                    if is_msgpack {
                        // Parse msgpack and pass as dict to callback (same as JSON text)
                        spawn_bounded_callback(&callback_sem, &state, d.len(), move || {
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
                        spawn_bounded_callback(&callback_sem, &state, d.len(), move || {
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
                queue_reply(&tx, &pending, max_pending, Message::Pong(payload));
            }
            Message::Close(frame) => {
                let code = frame.map_or(PEER_CLOSE_WITHOUT_CODE, |f| {
                    if f.code.is_allowed() {
                        u32::from(u16::from(f.code))
                    } else {
                        1002 // protocol error
                    }
                });
                peer_close.store(code, Ordering::Release);
                break;
            }
            Message::Pong(_) | Message::Frame(_) => {}
        }
    }
    finish.cancel();

    // Cleanup
    {
        let mut conns = state.connections.write().await;
        conns.remove(&*conn_id);
        state.connection_count.store(conns.len(), Ordering::Relaxed);
    }
    state.conn_formats.remove(&*conn_id);
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
    // A client that reads nothing more is not waited for past the grace.
    let mut write_task = write_task;
    if tokio::time::timeout(CLOSE_GRACE, &mut write_task)
        .await
        .is_err()
    {
        closer.abort();
        let _ = write_task.await;
    }
}
