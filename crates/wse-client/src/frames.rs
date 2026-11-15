//! A frame's data to the session, on every platform: read as JSON or
//! msgpack, opened first in an end-to-end session.

use bytes::Bytes;

use crate::decode::{Body, Decoder};
use crate::handler::Handler;
use crate::session::Session;

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
        Data::Text(text, frame) => session.on_json(text, frame),
        Data::Binary(data) => match decoder.binary(data) {
            Ok(Body::Json(json)) => session.on_json(json, Some(data)),
            Ok(Body::Msgpack(bytes)) => session.on_msgpack(bytes, Some(data)),
            Ok(Body::Sealed) => {
                tracing::warn!("[WSE] sealed frame without an end-to-end session dropped");
            }
            Ok(Body::Binary) => session.handler.binary(data),
            Err(e) => tracing::warn!("[WSE] frame of {} bytes dropped: {e}", data.len()),
        },
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
                    match plain.strip_prefix(b"M:") {
                        Some(msgpack) => session.on_msgpack(msgpack, None),
                        None => match std::str::from_utf8(&plain) {
                            Ok(text) => session.on_json(text, None),
                            Err(_) => tracing::warn!("[WSE] sealed frame not UTF-8 dropped"),
                        },
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
