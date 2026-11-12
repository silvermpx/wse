//! WebSocket frames: frame types, pre-framed encoding and the per-connection
//! broadcast queue with its vectored writer.

use bytes::{BufMut, Bytes, BytesMut};
use tokio_tungstenite::tungstenite::protocol::Message;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Frame types sent through per-connection channels.
/// `Msg` goes through tungstenite's framing pipeline (per-connection sends, control frames).
/// `PreFramed` contains complete WebSocket wire bytes that bypass tungstenite entirely
/// (broadcasts -- constructed once, sent to all connections).
#[derive(Clone)]
pub(crate) enum WsFrame {
    Msg(Message),
    PreFramed(Bytes),
}

/// Byte size of a WsFrame for backpressure accounting.
/// Counts payload + estimated framing overhead (~4 bytes for most messages).
#[inline]
pub(super) fn ws_frame_size(frame: &WsFrame) -> usize {
    match frame {
        WsFrame::PreFramed(bytes) => bytes.len(),
        WsFrame::Msg(msg) => match msg {
            Message::Text(s) => s.len() + 4,
            Message::Binary(b) => b.len() + 4,
            Message::Ping(p) | Message::Pong(p) => p.len() + 4,
            Message::Close(_) => 8,
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
    pub(crate) fn new() -> Self {
        Self::default()
    }

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

    /// Take every queued frame, for one vectored write.
    pub(crate) fn take(&mut self) -> std::collections::VecDeque<Bytes> {
        self.seal_small();
        std::mem::take(&mut self.frames)
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
    const MAX_IOV: usize = 512;
    let mut index = 0; // first frame not fully written
    let mut offset = 0; // bytes of it already written
    while index < frames.len() {
        let mut slices: Vec<std::io::IoSlice<'_>> =
            Vec::with_capacity(MAX_IOV.min(frames.len() - index));
        slices.push(std::io::IoSlice::new(&frames[index][offset..]));
        for frame in frames.iter().skip(index + 1).take(MAX_IOV - 1) {
            slices.push(std::io::IoSlice::new(frame));
        }
        let mut written = w.write_vectored(&slices).await?;
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

pub(crate) fn pre_frame_text(data: &str) -> Bytes {
    encode_ws_frame(0x01, data.as_bytes())
}

pub(crate) fn pre_frame_binary(data: &[u8]) -> Bytes {
    encode_ws_frame(0x02, data)
}
