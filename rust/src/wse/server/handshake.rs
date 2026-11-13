//! WebSocket upgrade helpers: the auth rejection, the pre-handshake cap and its guard.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::net::TcpStream;

use super::frames::{close_frame, write_all_frames};
use super::messages::build_error_json;
use super::pre_frame_text;

// ---------------------------------------------------------------------------
// JWT helper functions (used by handle_connection)
// ---------------------------------------------------------------------------

/// Refuse a connection whose token failed: an error message with `code` and
/// `message`, then a close frame with the generic code 4401, so the close
/// code does not tell which check failed. Written straight to the socket:
/// the connection never got a write task.
pub(super) async fn reject_ws_auth(sock: &mut TcpStream, code: &str, message: &str) {
    use tokio::io::AsyncWriteExt;

    let frames = std::collections::VecDeque::from([
        pre_frame_text(&build_error_json(code, message)),
        close_frame(4401, "Authentication failed"),
    ]);
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        write_all_frames(sock, &frames),
    )
    .await;
    let _ = sock.shutdown().await;
}

/// An origin as browsers send it in the `Origin` header: `scheme://host` or
/// `scheme://host:port`, in lower case, without a trailing slash and without
/// the scheme's default port.
pub(crate) fn normalize_origin(origin: &str) -> Result<String, String> {
    let invalid = || format!("allowed_origins: {origin:?} is not scheme://host[:port]");
    let lowered = origin.trim().trim_end_matches('/').to_ascii_lowercase();
    let Some((scheme, host)) = lowered.split_once("://") else {
        return Err(invalid());
    };
    if scheme.is_empty() || host.is_empty() || host.contains(['/', '?', '#', '@', ' ']) {
        return Err(invalid());
    }
    let default_port = match scheme {
        "http" | "ws" => Some(":80"),
        "https" | "wss" => Some(":443"),
        _ => None,
    };
    let host = default_port
        .and_then(|port| host.strip_suffix(port))
        .unwrap_or(host);
    Ok(format!("{scheme}://{host}"))
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
