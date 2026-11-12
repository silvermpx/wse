//! WebSocket upgrade helpers: the auth rejection, the pre-handshake cap and its guard.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};

use super::WsFrame;
use super::messages::build_error_json;

// ---------------------------------------------------------------------------
// JWT helper functions (used by handle_connection)
// ---------------------------------------------------------------------------

/// Send an auth error + Close frame over the WS channel, drain it, and return.
/// Used for JWT rejection to avoid duplicating the send+drain pattern.
/// Reject a WebSocket connection with an auth error JSON body and close code 4401.
/// `code` and `message` are included in the JSON error body only; the WS close
/// frame always uses generic code 4401 to avoid leaking auth failure details.
pub(super) async fn reject_ws_auth(
    tx: &mpsc::UnboundedSender<WsFrame>,
    mut rx: mpsc::UnboundedReceiver<WsFrame>,
    mut write_half: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<TcpStream>,
        Message,
    >,
    code: &str,
    message: &str,
) {
    let err = build_error_json(code, message);
    let _ = tx.send(WsFrame::Msg(Message::Text(err.into())));
    let _ = tx.send(WsFrame::Msg(Message::Close(Some(CloseFrame {
        code: CloseCode::Library(4401),
        reason: "Authentication failed".into(),
    }))));
    let write_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if let WsFrame::Msg(msg) = frame {
                let is_close = matches!(msg, Message::Close(_));
                if write_half.feed(msg).await.is_err() {
                    break;
                }
                if is_close {
                    break;
                }
            }
        }
        let _ = write_half.flush().await;
        let _ = write_half.close().await;
    });
    let _ = write_task.await;
}

/// Max connections allowed to sit in the pre-handshake / handshake phase at once.
/// `max_connections` only counts REGISTERED connections (a connection registers
/// after the WS upgrade + JWT complete), so without this a slowloris that opens
/// sockets but never completes the upgrade -- each held up to the 10s handshake
/// timeout -- exhausts tasks and file descriptors well before any limit fires.
pub(super) const MAX_PENDING_HANDSHAKES: usize = 512;

/// RAII counter for in-flight (pre-registration) handshakes: increments on
/// construction, decrements when the handshake task ends (success or failure).
pub(super) struct HandshakeGuard(Arc<AtomicUsize>);

impl HandshakeGuard {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for HandshakeGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}
