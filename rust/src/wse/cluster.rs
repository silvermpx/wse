// Types will be used in subsequent tasks (peer management, server integration)
#![allow(dead_code)]

use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;

use bytes::{Buf, BufMut, BytesMut};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MAGIC: &[u8; 4] = b"WSE\x00";
pub(crate) const PROTOCOL_VERSION: u16 = 1;
pub(crate) const MAX_FRAME_SIZE: usize = 1_048_576; // 1MB
pub(crate) const HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const HEARTBEAT_TIMEOUT_SECS: u64 = 15;
pub(crate) const MAX_BATCH_BYTES: usize = 65_536;
pub(crate) const PER_PEER_CHANNEL_CAP: usize = 8192;

// ---------------------------------------------------------------------------
// Message types
// ---------------------------------------------------------------------------

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MsgType {
    Msg = 0x01,
    Ping = 0x02,
    Pong = 0x03,
    Hello = 0x04,
    Shutdown = 0x05,
}

impl MsgType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Msg),
            0x02 => Some(Self::Ping),
            0x03 => Some(Self::Pong),
            0x04 => Some(Self::Hello),
            0x05 => Some(Self::Shutdown),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands (Python -> cluster subsystem)
// ---------------------------------------------------------------------------

pub(crate) enum ClusterCommand {
    Publish { topic: String, payload: String },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub(crate) struct ClusterMetrics {
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub reconnect_count: AtomicU64,
    pub connected_peers: AtomicU64,
}

impl ClusterMetrics {
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            connected_peers: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Dead letter queue
// ---------------------------------------------------------------------------

pub(crate) struct ClusterDlqEntry {
    pub topic: String,
    pub payload: String,
    pub peer_addr: String,
    pub error: String,
}

pub(crate) struct ClusterDlq {
    entries: VecDeque<ClusterDlqEntry>,
    max_entries: usize,
}

impl ClusterDlq {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    pub fn push(&mut self, entry: ClusterDlqEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn drain_all(&mut self) -> Vec<ClusterDlqEntry> {
        self.entries.drain(..).collect()
    }
}

// ---------------------------------------------------------------------------
// Wire protocol encoding
// ---------------------------------------------------------------------------

/// Encode a MSG frame into buf.
/// Format: [u8 type][u8 flags][u16 LE topic_len][u32 LE payload_len][topic][payload]
pub(crate) fn encode_msg(buf: &mut BytesMut, topic: &str, payload: &str) {
    let topic_bytes = topic.as_bytes();
    let payload_bytes = payload.as_bytes();
    buf.reserve(8 + topic_bytes.len() + payload_bytes.len());
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(topic_bytes);
    buf.put_slice(payload_bytes);
}

/// Encode a PING frame (header only, 8 bytes).
pub(crate) fn encode_ping(buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_u8(MsgType::Ping as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
}

/// Encode a PONG frame (header only, 8 bytes).
pub(crate) fn encode_pong(buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_u8(MsgType::Pong as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
}

/// Encode a HELLO frame.
/// Payload: magic(4) + version(2) + id_len(2) + id_bytes(N) + capabilities(4)
pub(crate) fn encode_hello(buf: &mut BytesMut, instance_id: &str) {
    let id_bytes = instance_id.as_bytes();
    let payload_len = 4 + 2 + 2 + id_bytes.len() + 4;
    buf.reserve(8 + payload_len);
    // Header
    buf.put_u8(MsgType::Hello as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(0); // topic_len (unused for HELLO)
    buf.put_u32_le(payload_len as u32);
    // Payload
    buf.put_slice(MAGIC);
    buf.put_u16_le(PROTOCOL_VERSION);
    buf.put_u16_le(id_bytes.len() as u16);
    buf.put_slice(id_bytes);
    buf.put_u32_le(0); // capabilities (reserved)
}

/// Encode a SHUTDOWN frame (header only, 8 bytes).
pub(crate) fn encode_shutdown(buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_u8(MsgType::Shutdown as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
}

// ---------------------------------------------------------------------------
// Wire protocol decoding
// ---------------------------------------------------------------------------

/// Decoded frame from a peer.
#[derive(Debug, PartialEq)]
pub(crate) enum ClusterFrame {
    Msg {
        topic: String,
        payload: String,
    },
    Ping,
    Pong,
    Hello {
        instance_id: String,
        protocol_version: u16,
    },
    Shutdown,
}

/// Decode a frame from raw bytes (after outer length prefix is stripped).
/// Returns None on any parse error.
pub(crate) fn decode_frame(mut data: BytesMut) -> Option<ClusterFrame> {
    if data.remaining() < 8 {
        return None;
    }
    let msg_type = MsgType::from_u8(data.get_u8())?;
    let _flags = data.get_u8();
    let topic_len = data.get_u16_le() as usize;
    let payload_len = data.get_u32_le() as usize;

    match msg_type {
        MsgType::Msg => {
            if data.remaining() < topic_len + payload_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            let payload = String::from_utf8(data.split_to(payload_len).to_vec()).ok()?;
            Some(ClusterFrame::Msg { topic, payload })
        }
        MsgType::Ping => Some(ClusterFrame::Ping),
        MsgType::Pong => Some(ClusterFrame::Pong),
        MsgType::Hello => {
            // Minimum HELLO payload: magic(4) + version(2) + id_len(2) + capabilities(4) = 12
            if payload_len < 12 || data.remaining() < payload_len {
                return None;
            }
            let magic = data.split_to(4);
            if magic.as_ref() != MAGIC {
                return None;
            }
            let version = data.get_u16_le();
            let id_len = data.get_u16_le() as usize;
            if data.remaining() < id_len + 4 {
                return None;
            }
            let instance_id = String::from_utf8(data.split_to(id_len).to_vec()).ok()?;
            let _capabilities = data.get_u32_le();
            Some(ClusterFrame::Hello {
                instance_id,
                protocol_version: version,
            })
        }
        MsgType::Shutdown => Some(ClusterFrame::Shutdown),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_msg() {
        let mut buf = BytesMut::new();
        encode_msg(&mut buf, "chat.general", "hello world");
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "chat.general".into(),
                payload: "hello world".into(),
            }
        );
    }

    #[test]
    fn test_encode_decode_ping_pong() {
        let mut buf = BytesMut::new();
        encode_ping(&mut buf);
        assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Ping);

        let mut buf = BytesMut::new();
        encode_pong(&mut buf);
        assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Pong);
    }

    #[test]
    fn test_encode_decode_hello() {
        let mut buf = BytesMut::new();
        encode_hello(&mut buf, "instance-abc-123");
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Hello {
                instance_id: "instance-abc-123".into(),
                protocol_version: 1,
            }
        );
    }

    #[test]
    fn test_encode_decode_shutdown() {
        let mut buf = BytesMut::new();
        encode_shutdown(&mut buf);
        assert_eq!(decode_frame(buf).unwrap(), ClusterFrame::Shutdown);
    }

    #[test]
    fn test_decode_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_u8(MsgType::Hello as u8);
        buf.put_u8(0);
        buf.put_u16_le(0);
        buf.put_u32_le(12);
        buf.put_slice(b"BAD\x00"); // wrong magic
        buf.put_u16_le(1);
        buf.put_u16_le(0);
        buf.put_u32_le(0);
        assert!(decode_frame(buf).is_none());
    }

    #[test]
    fn test_decode_truncated_frame() {
        let mut buf = BytesMut::new();
        buf.put_u8(MsgType::Msg as u8);
        buf.put_u8(0);
        buf.put_u16_le(5);
        buf.put_u32_le(100); // claims 100 bytes payload
        buf.put_slice(b"hello"); // only 5 bytes topic, no payload
        assert!(decode_frame(buf).is_none());
    }

    #[test]
    fn test_decode_unknown_type() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u8(0);
        buf.put_u16_le(0);
        buf.put_u32_le(0);
        assert!(decode_frame(buf).is_none());
    }

    #[test]
    fn test_dlq_push_and_drain() {
        let mut dlq = ClusterDlq::new(3);
        dlq.push(ClusterDlqEntry {
            topic: "a".into(),
            payload: "1".into(),
            peer_addr: "1.2.3.4".into(),
            error: "err".into(),
        });
        dlq.push(ClusterDlqEntry {
            topic: "b".into(),
            payload: "2".into(),
            peer_addr: "1.2.3.4".into(),
            error: "err".into(),
        });
        assert_eq!(dlq.len(), 2);
        let entries = dlq.drain_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(dlq.len(), 0);
    }

    #[test]
    fn test_dlq_evicts_oldest() {
        let mut dlq = ClusterDlq::new(2);
        for i in 0..3 {
            dlq.push(ClusterDlqEntry {
                topic: format!("t{i}"),
                payload: format!("{i}"),
                peer_addr: "x".into(),
                error: "e".into(),
            });
        }
        assert_eq!(dlq.len(), 2);
        let entries = dlq.drain_all();
        assert_eq!(entries[0].topic, "t1");
        assert_eq!(entries[1].topic, "t2");
    }

    #[test]
    fn test_encode_decode_msg_empty() {
        let mut buf = BytesMut::new();
        encode_msg(&mut buf, "", "");
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "".into(),
                payload: "".into(),
            }
        );
    }

    #[test]
    fn test_encode_decode_msg_large_payload() {
        let topic = "benchmark.load";
        let payload = "x".repeat(100_000);
        let mut buf = BytesMut::new();
        encode_msg(&mut buf, topic, &payload);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: topic.into(),
                payload,
            }
        );
    }

    #[test]
    fn test_decode_too_short() {
        let buf = BytesMut::from(&[0u8; 4][..]);
        assert!(decode_frame(buf).is_none());
    }
}
