//! A frame's data to the session, on every platform: read as JSON or
//! msgpack, opened first in an end-to-end session.

use bytes::Bytes;

use crate::decode::{Body, Decoder};
use crate::handler::Handler;
use crate::session::Session;

/// What a frame may carry past the message itself: a sealed frame's `E:`,
/// header and tag. A frame larger than `max_message_size` and this is taken
/// by no client, natively (the WebSocket's own limit) or in the browser.
pub(crate) const FRAME_OVERHEAD: usize = 32;

/// The data of a frame.
pub(crate) enum Data<'a> {
    /// A text frame, and the bytes it came in when a message may keep them.
    Text(&'a str, Option<&'a Bytes>),
    Binary(&'a Bytes),
}

/// A data frame to the session; true when it shows the server is alive.
pub(crate) fn on_data<H: Handler>(
    decoder: &mut Decoder,
    session: &mut Session<H>,
    data: Data<'_>,
) -> bool {
    if session.sealing.is_some() {
        return on_session_data(session, data);
    }
    match data {
        Data::Text(text, frame) => {
            let _ = session.on_json(text, frame);
        }
        Data::Binary(data) => {
            // A frame with the protocol's prefix is the protocol's; one only
            // guessed at, and not a message after all, is the application's.
            let prefixed = matches!(data.as_ref(), [b'C' | b'M' | b'E', b':', ..]);
            let understood = match decoder.binary(data) {
                Ok(Body::Json(json)) => session.on_json(json, Some(data)),
                Ok(Body::Msgpack(bytes)) => session.on_msgpack(bytes, Some(data)),
                Ok(Body::Sealed) => {
                    tracing::warn!("[WSE] sealed frame without an end-to-end session dropped");
                    true
                }
                Ok(Body::Binary) => false,
                Err(e) if prefixed => {
                    tracing::warn!("[WSE] frame of {} bytes dropped: {e}", data.len());
                    true
                }
                Err(_) => false,
            };
            if !understood && !prefixed {
                session.handler.binary(data);
            }
        }
    }
    true
}

/// A frame on a connection with an end-to-end session: sealed, or one of
/// the few that may come before the session opens.
fn on_session_data<H: Handler>(session: &mut Session<H>, data: Data<'_>) -> bool {
    match data {
        Data::Binary(data) if data.starts_with(b"E:") => {
            let Some(sealing) = session.sealing.as_mut() else {
                return false;
            };
            match sealing.open(data) {
                Ok(Some(plain)) => {
                    // Kept as shared bytes: messages kept by the application
                    // slice them, not copy them.
                    let plain = Bytes::from(plain);
                    let understood = match plain.strip_prefix(b"M:") {
                        Some(msgpack) => session.on_msgpack(msgpack, Some(&plain)),
                        None => std::str::from_utf8(&plain)
                            .ok()
                            .filter(|text| text.trim_start().starts_with('{'))
                            .is_some_and(|text| session.on_json(text, Some(&plain))),
                    };
                    // Bytes the server application sent as they are, sealed.
                    if !understood {
                        session.handler.binary(&plain);
                    }
                    true
                }
                // The server's close notice: its close frame follows.
                Ok(None) => true,
                Err(error) => {
                    session.fail(error);
                    false
                }
            }
        }
        Data::Text(text, _) => {
            session.on_unsealed(Some(text));
            false
        }
        Data::Binary(data) => {
            session.on_unsealed(std::str::from_utf8(data).ok());
            false
        }
    }
}
