//! WebSocket frames: frame types, pre-framed encoding and the per-connection
//! broadcast queue with its vectored writer.

use bytes::{BufMut, Bytes, BytesMut};
use tokio_tungstenite::tungstenite::protocol::Message;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// What goes through a connection's control queue; the write task encodes
/// it (`encode_message`). Broadcast frames go through the broadcast buffer.
pub(crate) enum WsFrame {
    Msg(Message),
    /// A message for an E2E session: sealed by the write task, which alone
    /// holds the session's sending state.
    Seal(Bytes),
    /// The sending state of an E2E session, queued right after server_hello.
    InstallSeal(Box<super::e2e_session::SendState>),
}

/// Byte size of a WsFrame for backpressure accounting.
/// Counts payload + estimated framing overhead (~4 bytes for most messages).
#[inline]
pub(super) fn ws_frame_size(frame: &WsFrame) -> usize {
    match frame {
        // Header, tag and frame overhead of a sealed frame.
        WsFrame::Seal(plaintext) => plaintext.len() + 36,
        WsFrame::InstallSeal(_) => 0,
        WsFrame::Msg(msg) => match msg {
            Message::Text(s) => s.len() + 4,
            Message::Binary(b) => b.len() + 4,
            Message::Ping(p) | Message::Pong(p) => p.len() + 4,
            Message::Close(frame) => 8 + frame.as_ref().map_or(0, |f| f.reason.len()),
            Message::Frame(_) => 16,
        },
    }
}

/// Frames up to this size are copied into the connection's own buffer; larger
/// ones are shared. Copying a small frame costs less than a shared reference
/// count, and it keeps every entry of a vectored write at least this big, so a
/// write of `MAX_IOV` entries moves at least 512 KiB.
const SHARE_FRAMES_ABOVE: usize = 1024;

/// The broadcast frames waiting for one connection. A large frame is queued as
/// a `Bytes`: one more reference to the buffer every subscriber shares, instead
/// of a copy per connection, so the memory of a backlog is the distinct frames,
/// not frames times connections. Small frames are copied together into `small`,
/// as one contiguous segment, in order with the shared ones.
#[derive(Default)]
pub(crate) struct BroadcastQueue {
    frames: std::collections::VecDeque<Bytes>,
    small: BytesMut,
}

impl BroadcastQueue {
    pub(crate) fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.small.is_empty()
    }

    pub(crate) fn push(&mut self, frame: &Bytes) {
        if frame.len() <= SHARE_FRAMES_ABOVE {
            self.small.extend_from_slice(frame);
        } else {
            self.seal_small();
            self.frames.push_back(frame.clone());
        }
    }

    /// Move every queued frame into `out`, which must be empty, for one
    /// vectored write; the queue keeps `out`'s room for the next frames, so
    /// a write task that clears `out` after each write allocates nothing.
    pub(crate) fn swap_into(&mut self, out: &mut std::collections::VecDeque<Bytes>) {
        debug_assert!(out.is_empty());
        self.seal_small();
        std::mem::swap(&mut self.frames, out);
    }

    /// Close the segment of small frames, so what comes next goes after it.
    fn seal_small(&mut self) {
        if !self.small.is_empty() {
            self.frames.push_back(self.small.split().freeze());
        }
    }
}

/// Write `frames` in order with as few `writev` calls as the socket allows.
pub(super) async fn write_all_frames<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    frames: &std::collections::VecDeque<Bytes>,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    // Frames per writev, gathered on the stack: no allocation per write.
    const MAX_IOV: usize = 16;
    let mut index = 0; // first frame not fully written
    let mut offset = 0; // bytes of it already written
    while index < frames.len() {
        let mut slices = [std::io::IoSlice::new(&[]); MAX_IOV];
        slices[0] = std::io::IoSlice::new(&frames[index][offset..]);
        let mut count = 1;
        for frame in frames.iter().skip(index + 1).take(MAX_IOV - 1) {
            slices[count] = std::io::IoSlice::new(frame);
            count += 1;
        }
        let mut written = w.write_vectored(&slices[..count]).await?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        while written > 0 {
            let left = frames[index].len() - offset;
            if written >= left {
                written -= left;
                index += 1;
                offset = 0;
            } else {
                offset += written;
                written = 0;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Pre-framed WebSocket encoding (server-to-client, no masking per RFC 6455)
// ---------------------------------------------------------------------------

/// Encode a complete WebSocket frame: header + payload.
/// Server-to-client frames are NOT masked, so all connections receive identical bytes.
pub(crate) fn encode_ws_frame(opcode: u8, payload: &[u8]) -> Bytes {
    let len = payload.len();
    let header_len = if len < 126 {
        2
    } else if len < 65536 {
        4
    } else {
        10
    };
    let mut buf = BytesMut::with_capacity(header_len + len);
    buf.put_u8(0x80 | opcode); // FIN=1 + opcode
    if len < 126 {
        buf.put_u8(len as u8);
    } else if len < 65536 {
        buf.put_u8(126);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(127);
        buf.put_u64(len as u64);
    }
    buf.extend_from_slice(payload);
    buf.freeze()
}

/// A close frame: the code, then the reason cut to the 123 bytes a control
/// frame leaves for it (at a character boundary).
pub(crate) fn close_frame(code: u16, reason: &str) -> Bytes {
    let mut end = reason.len().min(123);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    let mut payload = Vec::with_capacity(2 + end);
    payload.extend_from_slice(&code.to_be_bytes());
    payload.extend_from_slice(&reason.as_bytes()[..end]);
    encode_ws_frame(0x08, &payload)
}

/// The wire bytes of a message, as the server sends it (unmasked). `None`
/// for a raw tungstenite frame, which the server never queues.
pub(crate) fn encode_message(msg: &Message) -> Option<Bytes> {
    Some(match msg {
        Message::Text(text) => encode_ws_frame(0x01, text.as_bytes()),
        Message::Binary(data) => encode_ws_frame(0x02, data),
        Message::Ping(data) => encode_ws_frame(0x09, data),
        Message::Pong(data) => encode_ws_frame(0x0A, data),
        Message::Close(Some(frame)) => close_frame(u16::from(frame.code), &frame.reason),
        Message::Close(None) => encode_ws_frame(0x08, &[]),
        Message::Frame(_) => return None,
    })
}

pub(crate) fn pre_frame_text(data: &str) -> Bytes {
    encode_ws_frame(0x01, data.as_bytes())
}

#[cfg(test)]
pub(crate) fn pre_frame_binary(data: &[u8]) -> Bytes {
    encode_ws_frame(0x02, data)
}
