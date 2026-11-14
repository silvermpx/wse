//! One client connection, from the WebSocket upgrade to the cleanup.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{StatusCode, header};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};
use uuid::Uuid;

use super::e2e::{decrypt_inbound, derive_connection_key};
use super::e2e_session::{self, RecvState, SendState};
use super::fanout::BEHIND_BYTES;
use super::frames::{
    close_frame, encode_message, encode_ws_frame, write_all_frames, ws_frame_size,
};
use super::handler::{Connect, Handler, Inbound};
use super::handshake::{HANDSHAKE_TIMEOUT, HandshakeGuard, normalize_origin, reject_ws_auth};
use super::messages::{build_server_hello, build_server_ready, normalize_json_keys};
use super::signing::{self, ClientKey, Refused};
use super::state::{Activity, CloseOutbox, Outbox, Seal};
use super::subscribe::{answer_subscription_request, requested_compression};
use super::{
    CLOSE_E2E_BAD_FRAME, CLOSE_E2E_EXHAUSTED, CLOSE_E2E_UNSUPPORTED, CLOSE_GRACE,
    CLOSE_RATE_LIMITED, CLOSE_TOKEN_EXPIRED, CLOSE_TRY_AGAIN_LATER, Closer, ConnectionHandle,
    SharedState, WsFrame,
};
use crate::jwt::{self, parse_cookie_value};
use crate::wse::msgpack::{decode_msgpack_guarded, rmpv_to_serde_json};

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

/// Answer a protocol message the server handles itself (a ping, a client's
/// compression wish, a subscription request when the server takes them),
/// whatever form it came in: sealed or in msgpack. True when `val` was one.
fn answer_protocol_message(
    state: &SharedState,
    conn_id: &str,
    handle: &ConnectionHandle,
    max_pending: usize,
    val: &serde_json::Value,
) -> Protocol {
    match val.get("t").and_then(|t| t.as_str()) {
        Some("ping") => {
            queue_reply(handle, max_pending, Message::Text(pong_for(val).into()));
            Protocol::Answered
        }
        Some("pong" | "PONG" | "client_hello") => Protocol::Answered,
        Some("client_config") => {
            apply_client_config(state, conn_id, val.get("p"));
            Protocol::Answered
        }
        Some("subscription_update") if state.client_subscriptions => {
            if let Some(p) = val.get("p") {
                answer_subscription_request(state, conn_id, handle, p);
            }
            Protocol::Answered
        }
        Some("signed") => Protocol::Signed,
        _ => Protocol::Application,
    }
}

/// What a client's message is to the server.
enum Protocol {
    /// A protocol message, answered.
    Answered,
    /// A signed message (`take_signed`).
    Signed,
    /// The application's.
    Application,
}

/// The application learns of a connection: the validated user id (its user
/// for presence), or without jwt_secret the cookies for it to check. None
/// when it has no room for the connection now (it is then refused).
#[must_use]
fn tell_application<H: Handler>(
    state: &Arc<SharedState>,
    conn_id: &Arc<String>,
    user_id: Option<&str>,
    cookies: &str,
    handler: &H,
) -> Option<H::Connection> {
    let app = handler.connect(&Connect {
        conn_id,
        user_id,
        cookies,
    })?;
    if let (Some(pm), Some(user_id)) = (&state.presence, user_id) {
        pm.register_connection(conn_id, user_id);
    }
    Some(app)
}

/// A client that announced E2E now has its session: accepted by its token,
/// it gets server_ready (sealed) and joins the broadcasts; then the
/// application learns of it. False when the application has no room for it.
///
/// Called from the connection's read loop: only the connection's own
/// cleanup, after that loop, takes it out of the maps, so it is still in
/// them here and the join needs no lock. The read loop waits on nothing but
/// the socket.
#[must_use]
fn admit_after_session<H: Handler>(
    state: &Arc<SharedState>,
    conn_id: &Arc<String>,
    handle: &ConnectionHandle,
    user_id: Option<&str>,
    cookies: &str,
    handler: &H,
) -> Option<H::Connection> {
    if let Some(user) = user_id {
        let ready = build_server_ready(conn_id, user, state);
        handle.send_control(Message::Text(ready.into()));
        handle.out.admitted.store(true, Ordering::Release);
        state.topics.join(conn_id, handle);
    }
    tell_application(state, conn_id, user_id, cookies, handler)
}

/// What the read loop's paths need of their connection.
#[derive(Clone, Copy)]
struct Conn<'a> {
    state: &'a Arc<SharedState>,
    conn_id: &'a Arc<String>,
    handle: &'a ConnectionHandle,
    tx: &'a mpsc::UnboundedSender<WsFrame>,
    closer: &'a Closer,
    max_pending: usize,
}

impl Conn<'_> {
    /// Tell the client why in `reply`, then close with `code`.
    fn refuse(&self, reply: &'static str, code: u16, reason: &str) {
        queue_reply(self.handle, self.max_pending, Message::Text(reply.into()));
        self.closer
            .close(self.tx, &self.handle.out.pending, code, reason);
    }
}

/// The read loop's state a client_hello may change, and what admitting the
/// connection after its E2E session takes.
struct Admission<'a, H: Handler> {
    announced_e2e: bool,
    session: &'a mut Option<RecvState>,
    awaiting_session: &'a mut bool,
    /// The application's side of the connection, once it heard of it.
    app: &'a mut Option<H::Connection>,
    handler: &'a H,
    user_id: Option<&'a str>,
    cookies: &'a str,
}

/// The client's hello, once a connection: compression, then an E2E session
/// (announced at the upgrade) or the first design's key exchange, then
/// server_hello. Out of line: the read loop's hot path is the client's
/// messages, and this code sat in the middle of it.
#[cold]
#[inline(never)]
fn answer_client_hello<H: Handler>(
    conn: &Conn<'_>,
    val: &serde_json::Value,
    admission: Admission<'_, H>,
) {
    let (state, conn_id) = (conn.state, conn.conn_id);
    let p = val.get("p");
    if let Some(threshold) = requested_compression(
        p.and_then(|p| p.get("features"))
            .and_then(|f| f.get("compression")),
        p.and_then(|p| p.get("compression_threshold")),
    ) {
        state
            .conn_compression
            .insert((**conn_id).clone(), threshold);
    }
    let client_proto = p
        .and_then(|p| p.get("protocol_version"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;

    // An E2E session with an authenticated server. It is opened first,
    // announced at the upgrade (`e2e=1`): the server holds everything else
    // back until then, so nothing goes in plaintext.
    let offer = p.and_then(|p| p.get("e2e"));
    if offer.is_some() != admission.announced_e2e {
        conn.refuse(
            E2E_UNSUPPORTED_ERROR,
            CLOSE_E2E_UNSUPPORTED,
            "e2e session not announced",
        );
        return;
    }
    if let Some(offer) = offer {
        let accepted = e2e_session::parse_offer(offer)
            .and_then(|offer| e2e_session::accept(&offer, conn_id, state.e2e_identity.as_ref()));
        match accepted {
            Ok(accepted) => {
                let hello =
                    build_server_hello(conn_id, client_proto, state, None, Some(accepted.hello));
                queue_reply(conn.handle, conn.max_pending, Message::Text(hello.into()));
                // From here on the write task seals every frame, and the
                // client sends nothing else.
                let _ = conn.tx.send(WsFrame::InstallSeal(Box::new(accepted.send)));
                *admission.session = Some(accepted.recv);
                state.conn_sealed.insert((**conn_id).clone());
                let _ = conn.handle.out.cipher.set(Seal::Session);
                *admission.awaiting_session = false;
                *admission.app = admit_after_session(
                    state,
                    conn_id,
                    conn.handle,
                    admission.user_id,
                    admission.cookies,
                    admission.handler,
                );
                if admission.app.is_none() {
                    conn.closer.close(
                        conn.tx,
                        &conn.handle.out.pending,
                        CLOSE_TRY_AGAIN_LATER,
                        "server busy",
                    );
                }
            }
            Err(_) => conn.refuse(
                E2E_UNSUPPORTED_ERROR,
                CLOSE_E2E_UNSUPPORTED,
                "e2e suite unsupported",
            ),
        }
        return;
    }

    // Check if client requests E2E encryption (the first design)
    let wants_encryption = p
        .and_then(|p| p.get("features"))
        .and_then(|f| f.get("encryption"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if wants_encryption && !state.e2e_legacy {
        conn.refuse(
            E2E_UNSUPPORTED_ERROR,
            CLOSE_E2E_UNSUPPORTED,
            "e2e suite unsupported",
        );
        return;
    }

    // Once-only key exchange: never re-derive the cipher if this connection
    // already has one. A repeated client_hello would otherwise overwrite the
    // AES key mid-session (making in-flight encrypted messages
    // undecryptable) and is a cheap ECDH-keygen DoS amplifier.
    let key = if wants_encryption {
        p.and_then(|p| p.get("encryption_public_key"))
            .and_then(|v| v.as_str())
            .and_then(|b64| match derive_connection_key(b64) {
                Ok(key) => Some(key),
                Err(e) => {
                    tracing::warn!("[WSE] ECDH key exchange failed for {}: {}", conn_id, e);
                    let err_msg = format!(
                        "{{\"c\":\"WSE\",\"t\":\"error\",\"p\":{{\"code\":\"KEY_EXCHANGE_FAILED\",\"message\":\"ECDH key exchange failed: {}\",\"recoverable\":true}}}}",
                        e.replace('\"', "\\\"")
                    );
                    queue_reply(conn.handle, conn.max_pending, Message::Text(err_msg.into()));
                    None
                }
            })
    } else {
        None
    };

    let hello = build_server_hello(
        conn_id,
        client_proto,
        state,
        key.as_ref()
            .map(|(server_public, _)| server_public.as_str()),
        None,
    );
    queue_reply(conn.handle, conn.max_pending, Message::Text(hello.into()));
    // The key is in use only after server_hello is queued: an encrypted frame
    // never reaches the client before the server's public key.
    if let Some((_, cipher)) = key {
        state
            .conn_encryption
            .insert((**conn_id).clone(), cipher.clone());
        let _ = conn.handle.out.cipher.set(Seal::Legacy(cipher));
    }
}

/// Before its session, a client that announced E2E sends client_hello and
/// heartbeats only.
#[cold]
#[inline(never)]
fn handshake_message(text: &str) -> bool {
    let json = text
        .strip_prefix("WSE")
        .filter(|rest| rest.starts_with('{'))
        .unwrap_or(text);
    serde_json::from_str::<serde_json::Value>(json)
        .ok()
        .and_then(|v| {
            v.get("t")
                .and_then(serde_json::Value::as_str)
                .map(|t| matches!(t, "client_hello" | "ping" | "pong" | "PONG"))
        })
        .unwrap_or(false)
}

/// A client's signed message: checked against the connection's key and the
/// server's (`signing::check`), taken once per key and `n`, handed to the
/// application with its proof (drain kind `signed`, or `on_signed_message`;
/// `on_message` without the proof when there is no such callback) and then
/// answered with the server's signed receipt. Refused with a
/// `SIGNATURE_REFUSED` error otherwise, `busy` when the application cannot
/// take it now: then it was not received and its `n` stays free.
#[inline(never)]
fn take_signed<H: Handler>(
    conn: &Conn<'_>,
    envelope: &serde_json::Value,
    size: usize,
    handler: &H,
    app: &mut Option<H::Connection>,
) {
    let Conn {
        state,
        conn_id,
        handle,
        max_pending,
        ..
    } = *conn;
    let now = signing::now_ms();
    let window = state.signature_window_ms;
    let checked = match &state.signer {
        None => Err(Refused::NotEnabled),
        Some(signer) => {
            let key = state
                .client_keys
                .get(conn_id.as_str())
                .map(|key| Arc::clone(key.value()));
            signing::check(envelope, key.as_deref(), &signer.kid, now, window)
        }
    }
    .and_then(|verified| {
        // Remembered until its time is out of the window: it could not come
        // again after that.
        let ts = verified
            .message
            .get("ts")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(now);
        let seen = format!("{}:{}", verified.kid, verified.n);
        match state.signed_seen.entry(seen.clone()) {
            dashmap::Entry::Occupied(_) => Err(Refused::Replayed),
            dashmap::Entry::Vacant(slot) => {
                slot.insert(ts.saturating_add(window));
                Ok((verified, seen))
            }
        }
    });
    let (verified, seen) = match checked {
        Ok(taken) => taken,
        Err(refused) => {
            // The message's n, when its body reads, so the client knows which.
            let n = envelope
                .get("sb")
                .and_then(serde_json::Value::as_str)
                .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
                .and_then(|body| body.get("n").and_then(|n| n.as_str()).map(str::to_owned));
            refuse_signed(handle, max_pending, refused, n);
            return;
        }
    };
    let receipt = state
        .signer
        .as_ref()
        .and_then(|signer| signer.receipt(&verified, now).ok());
    let n = verified.n.clone();
    let value = verified.into_value();
    if app
        .as_mut()
        .is_some_and(|app| handler.signed(app, value, size))
    {
        if let Some(receipt) = receipt {
            queue_reply(handle, max_pending, Message::Text(receipt.into()));
        }
    } else {
        // Not received: the client may send it again with the same n.
        state.signed_seen.remove(&seen);
        refuse_signed(handle, max_pending, Refused::Busy, Some(n));
    }
}

/// The `SIGNATURE_REFUSED` error for a signed message, naming its `n` when
/// known.
fn refuse_signed(
    handle: &ConnectionHandle,
    max_pending: usize,
    refused: Refused,
    n: Option<String>,
) {
    let mut p = serde_json::json!({
        "code": "SIGNATURE_REFUSED",
        "reason": refused.reason(),
        "message": refused.message(),
        "recoverable": true,
    });
    if let Some(n) = n {
        p["n"] = serde_json::Value::String(n);
    }
    let error = serde_json::json!({"c": "WSE", "t": "error", "p": p, "v": 1});
    queue_reply(handle, max_pending, Message::Text(error.to_string().into()));
}

/// A msgpack frame from a client that asked for msgpack: one value, as is or
/// behind the `M:` prefix the server's own msgpack frames carry.
fn inbound_msgpack(data: &[u8]) -> Result<rmpv::Value, ()> {
    decode_msgpack_guarded(data.strip_prefix(b"M:").unwrap_or(data))
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

/// A client's `client_config`: what it asks of compression from now on.
#[cold]
#[inline(never)]
fn apply_client_config(state: &SharedState, conn_id: &str, p: Option<&serde_json::Value>) {
    if let Some(threshold) = requested_compression(
        p.and_then(|p| p.get("compression")),
        p.and_then(|p| p.get("compression_threshold")),
    ) {
        state.conn_compression.insert(conn_id.to_owned(), threshold);
    }
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

const E2E_UNSUPPORTED_ERROR: &str = "{\"c\":\"WSE\",\"t\":\"error\",\"p\":{\"code\":\"E2E_UNSUPPORTED\",\"message\":\"No end-to-end encryption in common with the server\",\"recoverable\":false},\"v\":1}";

/// The PONG answering a client's JSON `ping`.
#[cold]
#[inline(never)]
fn pong_for(ping: &serde_json::Value) -> String {
    let timestamp = ping
        .get("p")
        .and_then(|p| p.get("timestamp"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let server_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    format!(
        "{{\"c\":\"WSE\",\"t\":\"PONG\",\"p\":{{\"client_timestamp\":{},\"server_timestamp\":{},\"latency\":{}}},\"v\":1}}",
        timestamp,
        server_ts,
        server_ts.saturating_sub(timestamp).max(0)
    )
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

/// Count `bytes` written off `pending`; returns what was pending before.
fn release(pending: &AtomicUsize, bytes: usize) -> usize {
    pending
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some(cur.saturating_sub(bytes))
        })
        .unwrap_or_else(|cur| cur)
}

/// How a batch of control frames went out.
enum Sent {
    Frames,
    /// The server's close frame went out: nothing may follow it.
    Close,
    Failed,
}

/// Write the queued control frames in order, up to the server's close frame;
/// whatever was queued after the close is dropped. `seal` is the connection's
/// E2E session sending state, once installed: sealing here, in queue order,
/// keeps its sequence numbers in the order of the wire.
async fn write_control(
    sock: &mut TcpStream,
    batch: &mut Vec<WsFrame>,
    wire: &mut VecDeque<Bytes>,
    pending: &AtomicUsize,
    seal: &mut Option<SendState>,
) -> Sent {
    let queued: usize = batch.iter().map(ws_frame_size).sum();
    let mut close = false;
    for frame in batch.drain(..) {
        let frame = match frame {
            // Behind the session's keys every data frame is sealed, also one
            // queued by a sender that looked just before the switch.
            WsFrame::Msg(Message::Text(text)) if seal.is_some() => WsFrame::Seal(Bytes::from(text)),
            WsFrame::Msg(Message::Binary(data)) if seal.is_some() => WsFrame::Seal(data),
            frame => frame,
        };
        let msg = match frame {
            WsFrame::Msg(msg) => msg,
            WsFrame::InstallSeal(state) => {
                *seal = Some(*state);
                continue;
            }
            WsFrame::Seal(plaintext) => match seal.as_mut().map(|s| s.seal(&plaintext)) {
                Some(Ok(body)) => Message::Binary(body.into()),
                // The session's counters are used up: the client reconnects.
                Some(Err(_)) => Message::Close(Some(CloseFrame {
                    code: CloseCode::from(CLOSE_E2E_EXHAUSTED),
                    reason: "e2e limit reached".into(),
                })),
                None => continue,
            },
        };
        close = matches!(msg, Message::Close(_));
        if let Some(bytes) = encode_message(&msg) {
            wire.push_back(bytes);
        }
        if close {
            break;
        }
    }
    let result = write_all_frames(sock, wire).await;
    wire.clear();
    let _ = release(pending, queued);
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
fn queue_reply(handle: &ConnectionHandle, max_pending: usize, msg: Message) {
    let bytes = ws_frame_size(&WsFrame::Msg(msg.clone()));
    if max_pending > 0 && handle.out.pending.load(Ordering::Relaxed) + bytes > max_pending {
        return;
    }
    handle.send_control(msg);
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

enum Rate {
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

    fn check(&mut self, now: Instant) -> Rate {
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
                return Rate::Close;
            }
            let notify = a_second_since(self.last_notice);
            if notify {
                self.last_notice = Some(now);
            }
            return Rate::Limited { notify };
        }
        self.tokens -= 1.0;
        if self.tokens < self.capacity * 0.2 && a_second_since(self.last_warning) {
            self.last_warning = Some(now);
            return Rate::Warn {
                used: self.capacity - self.tokens,
                left: self.tokens,
            };
        }
        Rate::Pass
    }
}

/// What the read loop does after a frame.
enum Flow {
    Next,
    Stop,
}

/// A connection's reading side: what it keeps from frame to frame. The read
/// loop only waits for the socket; each frame goes through `on_frame`, a
/// plain function the compiler optimizes on its own, not as a branch of the
/// connection's whole state machine.
struct Reader<'a, H: Handler> {
    conn: Conn<'a>,
    activity: &'a Activity,
    /// The code of the client's close frame, for the write task's reply.
    peer_close: &'a AtomicU32,
    handler: &'a H,
    announced_e2e: bool,
    user_id: Option<&'a str>,
    cookies: &'a str,
    inbound_limit: InboundLimit,
    frame_errors: FrameErrorLog,
    /// One server_hello per connection: a client repeating client_hello
    /// would otherwise get one back each time.
    hello_answered: bool,
    awaiting_session: bool,
    /// The client's sending state of an E2E session: frames open in order only.
    session: Option<RecvState>,
    /// The application's side of the connection, once it heard of it.
    app: Option<H::Connection>,
}

/// Hand a client's message to the application; false when it has no room
/// for it (or has not heard of the connection).
#[inline]
fn deliver<H: Handler>(
    handler: &H,
    app: &mut Option<H::Connection>,
    message: Inbound,
    size: usize,
) -> bool {
    app.as_mut()
        .is_some_and(|app| handler.message(app, message, size))
}

impl<H: Handler> Reader<'_, H> {
    /// One frame from the client.
    fn on_frame(&mut self, msg: Message) -> Flow {
        let conn = self.conn;
        let Conn {
            state,
            conn_id,
            handle: own_handle,
            tx,
            closer,
            max_pending,
        } = conn;
        let out = &own_handle.out;
        let activity = self.activity;
        let peer_close = self.peer_close;
        let announced_e2e = self.announced_e2e;
        let rust_auth_user_id = self.user_id;
        let cookie_str = self.cookies;
        let handler = self.handler;
        let session = &mut self.session;
        let awaiting_session = &mut self.awaiting_session;
        let hello_answered = &mut self.hello_answered;
        let app = &mut self.app;
        let inbound_limit = &mut self.inbound_limit;
        let frame_errors = &mut self.frame_errors;
        // After the server queued a close, the client's further messages are
        // not delivered: only its close frame, or the grace period, ends this.
        if closer.is_closing() && !matches!(msg, Message::Close(_)) {
            return Flow::Next;
        }
        // Every frame counts as activity, protocol pings and pongs included:
        // a plain WebSocket client may keep the link alive with those alone.
        let now = Instant::now();
        activity.store(state.activity_ms(now), Ordering::Relaxed);
        // Before its session a client that announced E2E sends
        // client_hello and heartbeats; anything else is not taken (the
        // application does not know the connection yet).
        if *awaiting_session
            && match &msg {
                Message::Text(text) => !handshake_message(text),
                Message::Binary(_) => true,
                _ => false,
            }
        {
            return Flow::Next;
        }
        // In an E2E session the client sends sealed frames only.
        if session.is_some()
            && match &msg {
                Message::Text(_) => true,
                Message::Binary(data) => !data.starts_with(b"E:"),
                _ => false,
            }
        {
            closer.close(tx, &out.pending, CLOSE_E2E_BAD_FRAME, "e2e frame refused");
            return Flow::Next;
        }
        if matches!(msg, Message::Text(_) | Message::Binary(_)) {
            match inbound_limit.check(now) {
                Rate::Pass => {}
                Rate::Warn { used, left } => {
                    let warning = format!(
                        "{{\"c\":\"WSE\",\"t\":\"rate_limit_warning\",\"p\":{{\"current_rate\":{},\"limit\":{},\"remaining\":{},\"retry_after\":1.0}},\"v\":1}}",
                        used as u64, inbound_limit.capacity as u64, left as u64,
                    );
                    queue_reply(own_handle, max_pending, Message::Text(warning.into()));
                }
                Rate::Limited { notify } => {
                    state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
                    if notify {
                        queue_reply(
                            own_handle,
                            max_pending,
                            Message::Text(RATE_LIMITED_ERROR.into()),
                        );
                    }
                    return Flow::Next;
                }
                Rate::Close => {
                    state.rate_limited_total.fetch_add(1, Ordering::Relaxed);
                    closer.close(tx, &out.pending, CLOSE_RATE_LIMITED, "rate limit exceeded");
                    return Flow::Next;
                }
            }
        }
        match msg {
            Message::Text(text) => {
                let shard = usize::from(own_handle.lane);
                state.messages_received_total.add(shard, 1);
                state.bytes_received_total.add(shard, text.len() as u64);
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
                    deliver(handler, app, Inbound::Text(text.to_string()), text.len());
                    return Flow::Next;
                };

                // Try to parse JSON in Rust
                match serde_json::from_str::<serde_json::Value>(json_str) {
                    Ok(val) => {
                        // Fast-path: protocol messages handled entirely in Rust
                        if let Some(t_val) = val.get("t").and_then(|t| t.as_str()) {
                            if t_val == "client_hello" {
                                if !*hello_answered {
                                    *hello_answered = true;
                                    answer_client_hello(
                                        &conn,
                                        &val,
                                        Admission {
                                            announced_e2e,
                                            session,
                                            awaiting_session,
                                            app,
                                            handler,
                                            user_id: rust_auth_user_id,
                                            cookies: cookie_str,
                                        },
                                    );
                                }
                                return Flow::Next;
                            }
                            if t_val == "signed" {
                                take_signed(&conn, &val, text.len(), handler, app);
                                return Flow::Next;
                            }
                            if t_val == "ping" {
                                queue_reply(
                                    own_handle,
                                    max_pending,
                                    Message::Text(pong_for(&val).into()),
                                );
                                return Flow::Next;
                            }
                            if t_val == "pong" || t_val == "PONG" {
                                return Flow::Next;
                            }
                            if t_val == "client_config" {
                                apply_client_config(state, conn_id, val.get("p"));
                                return Flow::Next;
                            }
                            if t_val == "subscription_update" && state.client_subscriptions {
                                if let Some(p) = val.get("p") {
                                    answer_subscription_request(state, conn_id, own_handle, p);
                                }
                                return Flow::Next;
                            }
                        }

                        // Normalize type→t, payload→p in serde_json (no GIL needed)
                        let val = normalize_json_keys(val);

                        // Drain mode: push serde_json::Value to queue (zero GIL)
                        deliver(handler, app, Inbound::Json(val), text.len());
                    }
                    Err(_) => {
                        // JSON parse failed — pass raw string
                        deliver(handler, app, Inbound::Text(text.to_string()), text.len());
                    }
                }
            }
            Message::Binary(data) => {
                let shard = usize::from(own_handle.lane);
                state.messages_received_total.add(shard, 1);
                state.bytes_received_total.add(shard, data.len() as u64);

                // E2E encryption: detect E: prefix and decrypt before processing
                if data.len() >= 2 && data[0] == b'E' && data[1] == b':' {
                    let opened = match (session.as_mut(), out.cipher.get()) {
                        (Some(session), _) => match session.open(&data) {
                            Ok(Some(plaintext)) => Some(Ok(plaintext)),
                            // The client's close_notify: it closes next.
                            Ok(None) => return Flow::Next,
                            Err(_) => {
                                closer.close(
                                    tx,
                                    &out.pending,
                                    CLOSE_E2E_BAD_FRAME,
                                    "e2e frame refused",
                                );
                                return Flow::Next;
                            }
                        },
                        (None, Some(Seal::Legacy(cipher))) => {
                            Some(decrypt_inbound(cipher, &data[2..]))
                        }
                        (None, _) => None,
                    };
                    if let Some(opened) = opened {
                        match opened {
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
                                        deliver(
                                            handler,
                                            app,
                                            Inbound::Text(text.to_string()),
                                            text.len(),
                                        );
                                        return Flow::Next;
                                    };
                                    if let Ok(val) =
                                        serde_json::from_str::<serde_json::Value>(json_str)
                                    {
                                        let val = normalize_json_keys(val);
                                        // The clients encrypt their protocol
                                        // messages too (all of them in a session).
                                        match answer_protocol_message(
                                            state,
                                            conn_id,
                                            own_handle,
                                            max_pending,
                                            &val,
                                        ) {
                                            Protocol::Answered => return Flow::Next,
                                            Protocol::Signed => {
                                                take_signed(&conn, &val, text.len(), handler, app);
                                                return Flow::Next;
                                            }
                                            Protocol::Application => {}
                                        }
                                        deliver(handler, app, Inbound::Json(val), text.len());
                                    }
                                }
                            }
                            Err(e) => frame_errors.warn(conn_id, &e),
                        }
                    } else {
                        frame_errors.warn(conn_id, "encrypted frame before the key exchange");
                    }
                    return Flow::Next;
                }

                // Check if this connection uses msgpack (lock-free DashMap read)
                let is_msgpack = state
                    .conn_formats
                    .get(conn_id.as_str())
                    .map(|v| *v)
                    .unwrap_or(false);

                // A msgpack client's message, decoded once here (the same
                // dict the JSON text path makes); anything else stays bytes.
                let decoded = if is_msgpack {
                    inbound_msgpack(&data[..])
                        .ok()
                        .map(|value| normalize_json_keys(rmpv_to_serde_json(&value)))
                } else {
                    None
                };
                // Its protocol messages come in msgpack too: the server
                // answers those itself.
                if let Some(val) = &decoded {
                    match answer_protocol_message(state, conn_id, own_handle, max_pending, val) {
                        Protocol::Answered => return Flow::Next,
                        Protocol::Signed => {
                            take_signed(&conn, val, data.len(), handler, app);
                            return Flow::Next;
                        }
                        Protocol::Application => {}
                    }
                }

                let message = match decoded {
                    Some(value) => Inbound::Json(value),
                    None => Inbound::Binary(data.to_vec()),
                };
                deliver(handler, app, message, data.len());
            }
            Message::Ping(payload) => {
                queue_reply(own_handle, max_pending, Message::Pong(payload));
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
                return Flow::Stop;
            }
            Message::Pong(_) | Message::Frame(_) => {}
        }
        Flow::Next
    }
}

pub(crate) async fn handle_connection<H: Handler>(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<SharedState>,
    hs_guard: HandshakeGuard,
    handler: Arc<H>,
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
        refuses_compression: bool,
        /// The client opens an E2E session first (`e2e=1`).
        wants_e2e: bool,
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
    // A socket that sends nothing does not hold its place for long.
    let ws_stream = match tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
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
                let query = req.uri().query().unwrap_or("");
                let wants_msgpack = query.contains("format=msgpack");
                let refuses_compression = query.split('&').any(|kv| kv == "compression=false");
                let wants_e2e = query.split('&').any(|kv| kv == "e2e=1");
                let authorization = req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let _ = hd_clone.set(HandshakeResult {
                    cookies,
                    wants_msgpack,
                    refuses_compression,
                    wants_e2e,
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

    let (cookie_str, use_msgpack, refuses_compression, announced_e2e, auth_header) = handshake_data
        .get()
        .map(|hd| {
            (
                hd.cookies.clone(),
                hd.wants_msgpack,
                hd.refuses_compression,
                hd.wants_e2e,
                hd.authorization.clone(),
            )
        })
        .unwrap_or_default();
    let conn_id: Arc<String> = Arc::new(Uuid::now_v7().to_string());
    let (tx, mut rx) = mpsc::unbounded_channel::<WsFrame>();
    let out = Arc::new(Outbox::default());

    // ── JWT validation in Rust (zero GIL) ──────────────────────────────────
    // When jwt_config is set, validate JWT from cookie or Authorization header.
    // On success: send server_ready immediately, then the application hears of it.
    // On failure: send error + close — connection never registered.
    // When the token stops being valid (its exp, plus the clock skew the
    // validation allows).
    let mut token_deadline: Option<tokio::time::Instant> = None;
    let rust_auth_user_id: Option<String> = if let Some(ref jwt_holder) = state.jwt_config {
        let jwt_cfg = &*jwt_holder.load();
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
                            state.topics.acls.insert(conn_id.to_string(), acl);
                        }
                        // The key the token binds to its holder (`cnf`), for
                        // the client's signed messages.
                        if let Some(key) = ClientKey::from_claims(&claims) {
                            state.client_keys.insert(conn_id.to_string(), Arc::new(key));
                        }
                        token_deadline = jwt::exp_seconds(&claims).map(token_valid_until);
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

    // This connection as the rest of the server sees it.
    let own_handle = ConnectionHandle {
        tx: tx.clone(),
        out: Arc::clone(&out),
        closer: closer.clone(),
        msgpack: use_msgpack,
        lane: state.topics.next_lane(),
    };

    // Register connection
    let registered = {
        let mut conns = state.connections.write().await;
        // A draining or stopping server takes no one: the client comes back
        // to another server or later.
        let room = conns.len() < state.max_connections && !state.draining.load(Ordering::SeqCst);
        if room {
            // server_ready goes into the queue before the connection is in the
            // maps: no broadcast can reach it before, so server_ready is the
            // first frame.
            if let Some(ref user_id) = rust_auth_user_id
                && !announced_e2e
            {
                let server_ready = build_server_ready(&conn_id, user_id, &state);
                let sr_bytes = server_ready.len() + 4;
                out.pending.fetch_add(sr_bytes, Ordering::Relaxed);
                if tx
                    .send(WsFrame::Msg(Message::Text(server_ready.into())))
                    .is_err()
                {
                    out.pending.fetch_sub(sr_bytes, Ordering::Relaxed);
                }
            }
            conns.insert((*conn_id).clone(), own_handle.clone());
            // Accepted by its token. Without one, the application accepts it
            // (ServerCommand::Accept) before broadcasts reach it. A client that
            // announced E2E is admitted once its session exists.
            if rust_auth_user_id.is_some() && !announced_e2e {
                out.admitted.store(true, Ordering::Release);
                state.topics.join(&conn_id, &own_handle);
            }
            state.connection_count.store(conns.len(), Ordering::Relaxed);
        }
        room
    };
    if !registered {
        state
            .connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
        let reason = if state.draining.load(Ordering::SeqCst) {
            tracing::debug!("[WSE] Draining, refusing {addr}");
            "server draining"
        } else {
            tracing::warn!("[WSE] Max connections reached, rejecting {addr}");
            "server full"
        };
        // The ACL and the client key were stored before the check.
        state.topics.acls.remove(&*conn_id);
        state.client_keys.remove(&*conn_id);
        let frames = VecDeque::from([close_frame(CLOSE_TRY_AGAIN_LATER, reason)]);
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
    if refuses_compression {
        state.conn_compression.insert((*conn_id).clone(), 0);
    }
    // Its last activity, for the idle check: updated by the read loop.
    let activity = Arc::new(Activity::default());
    activity.store(state.activity_ms(Instant::now()), Ordering::Relaxed);
    state
        .conn_last_activity
        .insert((*conn_id).clone(), activity.clone());

    // A client that announced E2E is unknown to the application until its
    // session exists: then all it gets is sealed.
    let awaiting_session = announced_e2e;
    // The application's side of the connection, once it heard of it (and so
    // hears of its end). No room for it: refused, to come back.
    let app = if announced_e2e {
        None
    } else {
        tell_application(
            &state,
            &conn_id,
            rust_auth_user_id.as_deref(),
            &cookie_str,
            &*handler,
        )
    };
    if !announced_e2e && app.is_none() {
        closer.close(&tx, &out.pending, CLOSE_TRY_AGAIN_LATER, "server busy");
    }

    // Write task: the only writer to the socket. Two sources: the control
    // queue (server_ready, replies, send_event, close) and the broadcast buffer
    // (fan-out frames shared with every subscriber, not copied).
    let out_write = Arc::clone(&out);
    let lane_drained = state.topics.drained(own_handle.lane);
    // `finish`: the read side is done, send nothing more but the close reply.
    // The closer's token: the socket goes now, even in the middle of a frame.
    let finish = Arc::new(AtomicBool::new(false));
    let write_finish = finish.clone();
    let write_abort = closer.token().clone();
    let write_stop = closer.stop_registration();
    let peer_close = Arc::new(AtomicU32::new(NO_PEER_CLOSE));
    let write_peer_close = peer_close.clone();
    let write_task = tokio::spawn(async move {
        // Deliveries stop when this task ends, however it ends.
        let _closed = CloseOutbox(Arc::clone(&out_write));
        let mut batch: Vec<WsFrame> = Vec::with_capacity(CONTROL_BATCH);
        let mut wire: VecDeque<Bytes> = VecDeque::with_capacity(CONTROL_BATCH);
        // The broadcast frames being written; swapped with the queue's, so
        // both keep their room and no write allocates.
        let mut shared: VecDeque<Bytes> = VecDeque::new();
        let mut seal: Option<SendState> = None;
        let mut close_sent = false;
        let work = async {
            loop {
                if write_finish.load(Ordering::Acquire) {
                    break;
                }
                // The broadcast frames queued so far, taken before the control
                // frames: a control frame queued before one of them (an
                // accepted connection's server_ready, queued before it joins
                // the broadcasts) is then in the control queue too.
                {
                    let mut queue = out_write.frames.lock();
                    if !queue.is_empty() {
                        queue.swap_into(&mut shared);
                    }
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
                    match write_control(
                        &mut raw_write,
                        &mut batch,
                        &mut wire,
                        &out_write.pending,
                        &mut seal,
                    )
                    .await
                    {
                        Sent::Frames => {}
                        Sent::Close => {
                            close_sent = true;
                            break;
                        }
                        Sent::Failed => break,
                    }
                }
                // Then the broadcast frames.
                if !shared.is_empty() {
                    wrote = true;
                    let len: usize = shared.iter().map(Bytes::len).sum();
                    let result = write_all_frames(&mut raw_write, &shared).await;
                    shared.clear();
                    let before = release(&out_write.pending, len);
                    // Its lane may wait for the connections to catch up.
                    if before >= BEHIND_BYTES && before.saturating_sub(len) < BEHIND_BYTES {
                        lane_drained.notify_one();
                    }
                    if result.is_err() {
                        break;
                    }
                }
                if wrote {
                    continue;
                }
                tokio::select! {
                    biased;
                    n = rx.recv_many(&mut batch, CONTROL_BATCH) => {
                        if n == 0 {
                            break; // every sender is gone
                        }
                    }
                    () = out_write.notify.notified() => {}
                }
            }
        };
        // Stopped at once when the connection must go, even in the middle of
        // a frame.
        let finished = futures_util::future::Abortable::new(work, write_stop)
            .await
            .is_ok();
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

    let mut reader = Reader {
        conn: Conn {
            state: &state,
            conn_id: &conn_id,
            handle: &own_handle,
            tx: &tx,
            closer: &closer,
            max_pending: state.max_outbound_queue_bytes,
        },
        activity: &activity,
        peer_close: &peer_close,
        handler: &*handler,
        announced_e2e,
        user_id: rust_auth_user_id.as_deref(),
        cookies: &cookie_str,
        inbound_limit: InboundLimit::new(state.rate_capacity, state.rate_refill),
        frame_errors: FrameErrorLog::default(),
        hello_answered: false,
        awaiting_session,
        session: None,
        app,
    };

    // A connection whose token expires is closed (4403): the clients come
    // back with a new token from their token provider.
    let mut expiry = token_deadline.map(|deadline| Box::pin(tokio::time::sleep_until(deadline)));

    // Read loop. It ends when the client closes or the socket fails, and when
    // the server closed the connection and the client did not answer in time.
    let read_stop = closer.stop_registration();
    let read_loop = async {
        loop {
            let msg = tokio::select! {
                biased;
                () = async { if let Some(sleep) = expiry.as_mut() { sleep.await } }, if expiry.is_some() => {
                    expiry = None;
                    closer.close(&tx, &out.pending, CLOSE_TOKEN_EXPIRED, "token expired");
                    continue;
                }
                next = read_half.next() => match next {
                    Some(Ok(msg)) => msg,
                    _ => break,
                },
            };
            match reader.on_frame(msg) {
                Flow::Next => {}
                Flow::Stop => break,
            }
        }
    };
    // Stopped at once when the connection must go (`Closer`).
    let _ = futures_util::future::Abortable::new(read_loop, read_stop).await;
    let app = reader.app.take();
    // The read side is done: the write task sends the close reply and ends.
    finish.store(true, Ordering::Release);
    own_handle.out.notify.notify_one();

    // Cleanup
    {
        let mut conns = state.connections.write().await;
        conns.remove(&*conn_id);
        state.topics.leave(&conn_id, own_handle.lane);
        state.connection_count.store(conns.len(), Ordering::Relaxed);
    }
    state.conn_formats.remove(&*conn_id);
    state.conn_compression.remove(&*conn_id);
    state.conn_sealed.remove(&*conn_id);
    state.conn_last_activity.remove(&*conn_id);
    state.conn_encryption.remove(&*conn_id);
    state.topics.acls.remove(&*conn_id);
    state.client_keys.remove(&*conn_id);
    // Queue group cleanup: remove this connection from all queue groups
    if let Some((_, memberships)) = state.conn_queue_groups.remove(&*conn_id) {
        let mut left_groups = Vec::with_capacity(memberships.len());
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
            left_groups.push(topic);
        }
        state.remove_local_interest(&left_groups);
    }
    // Presence cleanup: untrack from all topics before disconnect event
    if let Some(ref pm) = state.presence {
        state.announce_untracked(pm.remove_connection(&conn_id));
    }
    // Pub/Sub cleanup: remove connection from all subscribed topics
    if let Some((_, topics)) = state.conn_topics.remove(&*conn_id) {
        let mut disconnected_topics = Vec::new();
        for topic_ref in topics.iter() {
            let topic = topic_ref.clone();
            state.topics.unsubscribe(&topic, &conn_id);
            disconnected_topics.push(topic);
        }

        state.remove_local_interest(&disconnected_topics);
    }
    // A connection the application never heard of (an announced client that
    // never opened its session, or one refused for want of room) gets no
    // disconnect event either.
    if let Some(app) = app {
        handler.disconnect(app);
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
