//! The cluster wire protocol: message types, frame encoding and decoding, the
//! length-prefix framing and version / capability negotiation.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{
    COMPRESSION_THRESHOLD, FLAG_COMPRESSED, FLAG_HAS_RECOVERY, MAGIC, MAX_CLUSTER_PEERS,
    MAX_FRAME_SIZE, PROTOCOL_VERSION, PROTOCOL_VERSION_MIN,
};

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
    Sub = 0x06,
    Unsub = 0x07,
    Resync = 0x08,
    PeerAnnounce = 0x09,
    PeerList = 0x0A,
    PresenceUpdate = 0x0B,
    PresenceFull = 0x0C,
    Drain = 0x0D,
}

impl MsgType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Msg),
            0x02 => Some(Self::Ping),
            0x03 => Some(Self::Pong),
            0x04 => Some(Self::Hello),
            0x05 => Some(Self::Shutdown),
            0x06 => Some(Self::Sub),
            0x07 => Some(Self::Unsub),
            0x08 => Some(Self::Resync),
            0x09 => Some(Self::PeerAnnounce),
            0x0A => Some(Self::PeerList),
            0x0B => Some(Self::PresenceUpdate),
            0x0C => Some(Self::PresenceFull),
            0x0D => Some(Self::Drain),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Wire protocol encoding
// ---------------------------------------------------------------------------

/// Encode a MSG frame into buf. Returns false if topic exceeds 64KB.
/// Format: [u8 type][u8 flags][u16 LE topic_len][u32 LE payload_len][topic][payload]
pub(crate) fn encode_msg(buf: &mut BytesMut, topic: &str, payload: &str) -> bool {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        tracing::warn!(
            "[WSE-Cluster] Topic too long ({} bytes, max {}), dropping message",
            topic_bytes.len(),
            u16::MAX
        );
        return false;
    }
    let payload_bytes = payload.as_bytes();
    let total_frame = 8 + topic_bytes.len() + payload_bytes.len();
    if total_frame > MAX_FRAME_SIZE {
        tracing::warn!(
            "[WSE-Cluster] Frame too large ({total_frame} bytes, max {MAX_FRAME_SIZE}), dropping message",
        );
        return false;
    }
    buf.reserve(total_frame);
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(topic_bytes);
    buf.put_slice(payload_bytes);
    true
}

/// Encode a MSG frame with zstd compression on the payload.
/// Compresses only if payload >= COMPRESSION_THRESHOLD and compressed size < original.
/// Falls back to uncompressed encoding if compression doesn't help.
pub(crate) fn encode_msg_compressed(buf: &mut BytesMut, topic: &str, payload: &str) -> bool {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        tracing::warn!(
            "[WSE-Cluster] Topic too long ({} bytes, max {}), dropping message",
            topic_bytes.len(),
            u16::MAX
        );
        return false;
    }
    let payload_bytes = payload.as_bytes();

    // Only attempt compression for payloads above threshold
    if payload_bytes.len() >= COMPRESSION_THRESHOLD
        && let Ok(compressed) = zstd::bulk::compress(payload_bytes, 1)
        && compressed.len() < payload_bytes.len()
    {
        buf.reserve(8 + topic_bytes.len() + compressed.len());
        buf.put_u8(MsgType::Msg as u8);
        buf.put_u8(FLAG_COMPRESSED);
        buf.put_u16_le(topic_bytes.len() as u16);
        buf.put_u32_le(compressed.len() as u32);
        buf.put_slice(topic_bytes);
        buf.put_slice(&compressed);
        return true;
    }

    // Fallback: uncompressed
    encode_msg(buf, topic, payload)
}

/// Encode a MSG frame with recovery trailer (origin epoch + offset).
/// Optionally compresses the payload with zstd. The 12-byte recovery trailer
/// is appended AFTER the payload (not included in payload_len).
pub(crate) fn encode_msg_with_recovery(
    buf: &mut BytesMut,
    topic: &str,
    payload: &str,
    origin_epoch: u32,
    origin_offset: u64,
    compress: bool,
) -> bool {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return false;
    }
    let payload_bytes = payload.as_bytes();

    if compress
        && payload_bytes.len() >= COMPRESSION_THRESHOLD
        && let Ok(compressed) = zstd::bulk::compress(payload_bytes, 1)
        && compressed.len() < payload_bytes.len()
    {
        let total = 8 + topic_bytes.len() + compressed.len() + 12; // +12 for recovery trailer
        if total > MAX_FRAME_SIZE {
            return false;
        }
        buf.reserve(total);
        buf.put_u8(MsgType::Msg as u8);
        buf.put_u8(FLAG_COMPRESSED | FLAG_HAS_RECOVERY);
        buf.put_u16_le(topic_bytes.len() as u16);
        buf.put_u32_le(compressed.len() as u32);
        buf.put_slice(topic_bytes);
        buf.put_slice(&compressed);
        buf.put_u32_le(origin_epoch);
        buf.put_u64_le(origin_offset);
        return true;
    }

    // Uncompressed with recovery trailer
    let total = 8 + topic_bytes.len() + payload_bytes.len() + 12;
    if total > MAX_FRAME_SIZE {
        return false;
    }
    buf.reserve(total);
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(FLAG_HAS_RECOVERY);
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(topic_bytes);
    buf.put_slice(payload_bytes);
    buf.put_u32_le(origin_epoch);
    buf.put_u64_le(origin_offset);
    true
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
pub(crate) fn encode_hello(buf: &mut BytesMut, instance_id: &str, capabilities: u32) {
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
    buf.put_u32_le(capabilities);
}

/// Encode a SHUTDOWN frame (header only, 8 bytes).
pub(crate) fn encode_shutdown(buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_u8(MsgType::Shutdown as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
}

/// Encode a DRAIN frame. Header-only, 8 bytes (like Shutdown).
/// Tells peers this node is entering lame-duck mode.
pub(crate) fn encode_drain(buf: &mut BytesMut) {
    buf.reserve(8);
    buf.put_u8(MsgType::Drain as u8);
    buf.put_u8(0);
    buf.put_u16_le(0);
    buf.put_u32_le(0);
}

/// Encode a SUB frame. Returns false if topic exceeds 64KB.
/// Format: [u8 type=0x06][u8 flags][u16 LE topic_len][u32 LE payload_len=0][topic]
pub(crate) fn encode_sub(buf: &mut BytesMut, topic: &str) -> bool {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return false;
    }
    buf.reserve(8 + topic_bytes.len());
    buf.put_u8(MsgType::Sub as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(0); // no payload
    buf.put_slice(topic_bytes);
    true
}

/// Encode an UNSUB frame. Returns false if topic exceeds 64KB.
/// Format: [u8 type=0x07][u8 flags][u16 LE topic_len][u32 LE payload_len=0][topic]
pub(crate) fn encode_unsub(buf: &mut BytesMut, topic: &str) -> bool {
    let topic_bytes = topic.as_bytes();
    if topic_bytes.len() > u16::MAX as usize {
        return false;
    }
    buf.reserve(8 + topic_bytes.len());
    buf.put_u8(MsgType::Unsub as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(0); // no payload
    buf.put_slice(topic_bytes);
    true
}

/// Encode RESYNC frame(s). Topics are joined by "\n" in the payload.
/// Format: [u8 type=0x08][u8 flags][u16 LE topic_len=0][u32 LE payload_len][payload]
/// flags=0x00 for first/only frame (replace semantics), 0x01 for continuation (extend).
/// Returns multiple frames if the payload exceeds MAX_FRAME_SIZE.
pub(crate) fn encode_resync(topics: &[String]) -> Vec<BytesMut> {
    let payload = topics.join("\n");
    let payload_bytes = payload.as_bytes();

    fn encode_single(chunk: &[u8], flags: u8) -> BytesMut {
        let mut buf = BytesMut::with_capacity(8 + chunk.len());
        buf.put_u8(MsgType::Resync as u8);
        buf.put_u8(flags);
        buf.put_u16_le(0);
        buf.put_u32_le(chunk.len() as u32);
        buf.put_slice(chunk);
        buf
    }

    if payload_bytes.is_empty() || payload_bytes.len() <= MAX_FRAME_SIZE {
        return vec![encode_single(payload_bytes, 0x00)];
    }
    // Multi-frame: split at newline boundaries to stay under MAX_FRAME_SIZE
    // First frame: flags=0x00 (replace), continuation frames: flags=0x01 (extend)
    let mut frames = Vec::new();
    let mut start = 0;
    while start < payload_bytes.len() {
        let remaining = payload_bytes.len() - start;
        // Reserve 8 bytes for the wire header to stay under MAX_FRAME_SIZE on the wire
        let chunk_limit = MAX_FRAME_SIZE - 8;
        let chunk_end = if remaining <= chunk_limit {
            payload_bytes.len()
        } else {
            let window_end = start + chunk_limit;
            match payload_bytes[start..window_end]
                .iter()
                .rposition(|&b| b == b'\n')
            {
                Some(pos) => start + pos,
                None => window_end,
            }
        };
        let flags = if frames.is_empty() { 0x00 } else { 0x01 };
        frames.push(encode_single(&payload_bytes[start..chunk_end], flags));
        start = if chunk_end < payload_bytes.len() && payload_bytes[chunk_end] == b'\n' {
            chunk_end + 1
        } else {
            chunk_end
        };
    }
    frames
}

/// Encode PEER_ANNOUNCE: announces this node's cluster address to a peer.
/// Format: [u8 type=0x09][u8 flags][u16 LE addr_len][u32 LE 0][addr_bytes]
pub(crate) fn encode_peer_announce(buf: &mut BytesMut, addr: &str) {
    let addr_bytes = addr.as_bytes();
    if addr_bytes.len() > u16::MAX as usize {
        tracing::warn!(
            "[WSE-Cluster] Peer address too long ({} bytes), dropping announce",
            addr_bytes.len()
        );
        return;
    }
    buf.reserve(8 + addr_bytes.len());
    buf.put_u8(MsgType::PeerAnnounce as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(addr_bytes.len() as u16);
    buf.put_u32_le(0); // payload_len (unused)
    buf.put_slice(addr_bytes);
}

/// Encode PEER_LIST: sends the list of all known peer addresses.
/// Format: [u8 type=0x0A][u8 flags][u16 LE 0][u32 LE payload_len][payload]
/// Payload: [u16 LE count][u16 LE addr1_len][addr1_bytes]...
pub(crate) fn encode_peer_list(buf: &mut BytesMut, addrs: &[String]) {
    // Cap at u16::MAX entries and skip addresses that exceed u16::MAX bytes
    let safe_addrs: Vec<&String> = addrs
        .iter()
        .filter(|a| a.len() <= u16::MAX as usize)
        .take(u16::MAX as usize)
        .collect();
    let mut payload = BytesMut::new();
    payload.put_u16_le(safe_addrs.len() as u16);
    for addr in &safe_addrs {
        payload.put_u16_le(addr.len() as u16);
        payload.extend_from_slice(addr.as_bytes());
    }
    buf.reserve(8 + payload.len());
    buf.put_u8(MsgType::PeerList as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(0); // topic_len (unused)
    buf.put_u32_le(payload.len() as u32);
    buf.extend_from_slice(&payload);
}

/// Encode a PRESENCE_UPDATE frame.
/// Payload: [action(1)][updated_at(8)][user_id_len(2)][user_id][data]
pub(crate) fn encode_presence_update(
    buf: &mut BytesMut,
    topic: &str,
    user_id: &str,
    action: u8,
    data: &str,
    updated_at: u64,
) -> bool {
    let topic_bytes = topic.as_bytes();
    let user_id_bytes = user_id.as_bytes();
    let data_bytes = data.as_bytes();
    if topic_bytes.len() > u16::MAX as usize || user_id_bytes.len() > u16::MAX as usize {
        return false;
    }
    let payload_len = 1 + 8 + 2 + user_id_bytes.len() + data_bytes.len();
    let total_frame = 8 + topic_bytes.len() + payload_len;
    // Guard the total frame like every other encoder. presence_max_data_size is
    // operator-configurable; without this a large presence update would emit a
    // frame the receiving peer_reader rejects (> MAX_FRAME_SIZE), disconnecting
    // the peer and flapping the link on ordinary presence traffic.
    if total_frame > MAX_FRAME_SIZE {
        tracing::warn!(
            "[WSE-Cluster] PresenceUpdate frame too large ({total_frame} bytes, max {MAX_FRAME_SIZE}), dropping",
        );
        return false;
    }
    buf.reserve(total_frame);
    buf.put_u8(MsgType::PresenceUpdate as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(payload_len as u32);
    buf.put_slice(topic_bytes);
    buf.put_u8(action);
    buf.put_u64_le(updated_at);
    buf.put_u16_le(user_id_bytes.len() as u16);
    buf.put_slice(user_id_bytes);
    buf.put_slice(data_bytes);
    true
}

/// Encode a PRESENCE_FULL frame (full presence state sync).
pub(crate) fn encode_presence_full(buf: &mut BytesMut, entries_json: &str) -> bool {
    let payload_bytes = entries_json.as_bytes();
    let total_frame = 8 + payload_bytes.len();
    if total_frame > MAX_FRAME_SIZE {
        tracing::warn!(
            "[WSE-Cluster] PresenceFull frame too large ({total_frame} bytes, max {MAX_FRAME_SIZE}), dropping",
        );
        return false;
    }
    let topic_len: u16 = 0; // No topic for full sync
    buf.reserve(total_frame);
    buf.put_u8(MsgType::PresenceFull as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_len);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(payload_bytes);
    true
}

// ---------------------------------------------------------------------------
// Framing helpers (4-byte BE length prefix)
// ---------------------------------------------------------------------------

pub(super) fn write_framed(buf: &mut BytesMut, inner: &[u8]) {
    buf.reserve(4 + inner.len());
    buf.put_u32(inner.len() as u32); // big-endian length prefix
    buf.put_slice(inner);
}

// ---------------------------------------------------------------------------
// Wire protocol decoding
// ---------------------------------------------------------------------------

/// Decoded frame from a peer.
#[derive(Debug, PartialEq)]
pub(crate) enum ClusterFrame {
    Msg {
        topic: String,
        payload: Bytes,
        /// Origin node's recovery position (epoch, offset) for cross-node recovery.
        recovery: Option<(u32, u64)>,
    },
    Ping,
    Pong,
    Hello {
        instance_id: String,
        protocol_version: u16,
        capabilities: u32,
    },
    Shutdown,
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
    Resync {
        topics: Vec<String>,
        is_continuation: bool,
    },
    PeerAnnounce {
        addr: String,
    },
    PeerList {
        addrs: Vec<String>,
    },
    PresenceUpdate {
        topic: String,
        user_id: String,
        action: u8,
        data: String,
        updated_at: u64,
    },
    PresenceFull {
        entries: String,
    },
    /// Graceful drain: this node is shutting down, migrate connections.
    Drain,
    Unknown {
        msg_type: u8,
    },
}

/// Decode a frame from raw bytes (after outer length prefix is stripped).
/// Returns None on any parse error (except unknown message types, which return Unknown).
pub(crate) fn decode_frame(mut data: BytesMut) -> Option<ClusterFrame> {
    if data.remaining() < 8 {
        return None;
    }
    let raw_type = data.get_u8();
    let flags = data.get_u8();
    let topic_len = data.get_u16_le() as usize;
    let payload_len = data.get_u32_le() as usize;

    match MsgType::from_u8(raw_type) {
        Some(MsgType::Msg) => {
            if data.remaining() < topic_len + payload_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            let payload = if flags & FLAG_COMPRESSED != 0 {
                let compressed = data.split_to(payload_len);
                // zstd::bulk::decompress enforces MAX_FRAME_SIZE output cap (decompression bomb protection).
                let decompressed = match zstd::bulk::decompress(&compressed, MAX_FRAME_SIZE) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(
                            "[WSE-Cluster] Decompression failed ({payload_len} bytes compressed): {e}"
                        );
                        return None;
                    }
                };
                Bytes::from(decompressed)
            } else {
                data.split_to(payload_len).freeze() // zero-copy: BytesMut -> Bytes
            };
            // Parse optional recovery trailer (12 bytes: u32 epoch + u64 offset)
            let recovery = if flags & FLAG_HAS_RECOVERY != 0 {
                if data.remaining() < 12 {
                    return None; // malformed: flag set but recovery trailer missing
                }
                let epoch = data.get_u32_le();
                let offset = data.get_u64_le();
                Some((epoch, offset))
            } else {
                None
            };
            Some(ClusterFrame::Msg {
                topic,
                payload,
                recovery,
            })
        }
        Some(MsgType::Ping) => Some(ClusterFrame::Ping),
        Some(MsgType::Pong) => Some(ClusterFrame::Pong),
        Some(MsgType::Hello) => {
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
            if id_len > 256 {
                tracing::warn!(
                    "[WSE-Cluster] Instance ID too long: {} bytes, max 256",
                    id_len
                );
                return None;
            }
            if data.remaining() < id_len + 4 {
                return None;
            }
            let instance_id = String::from_utf8(data.split_to(id_len).to_vec()).ok()?;
            let capabilities = data.get_u32_le();
            Some(ClusterFrame::Hello {
                instance_id,
                protocol_version: version,
                capabilities,
            })
        }
        Some(MsgType::Shutdown) => Some(ClusterFrame::Shutdown),
        Some(MsgType::Drain) => Some(ClusterFrame::Drain),
        Some(MsgType::Sub) => {
            if data.remaining() < topic_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            Some(ClusterFrame::Sub { topic })
        }
        Some(MsgType::Unsub) => {
            if data.remaining() < topic_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            Some(ClusterFrame::Unsub { topic })
        }
        Some(MsgType::Resync) => {
            if data.remaining() < payload_len {
                return None;
            }
            let topics = if payload_len == 0 {
                Vec::new()
            } else {
                let raw_bytes = data.split_to(payload_len);
                let slice: &[u8] = &raw_bytes;
                slice
                    .split(|&b| b == b'\n')
                    .filter(|chunk| !chunk.is_empty())
                    .filter_map(|chunk| String::from_utf8(chunk.to_vec()).ok())
                    .collect()
            };
            Some(ClusterFrame::Resync {
                topics,
                is_continuation: flags & 0x01 != 0,
            })
        }
        Some(MsgType::PeerAnnounce) => {
            if data.remaining() < topic_len {
                return None;
            }
            let addr = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            Some(ClusterFrame::PeerAnnounce { addr })
        }
        Some(MsgType::PeerList) => {
            if data.remaining() < payload_len {
                return None;
            }
            if payload_len < 2 {
                return Some(ClusterFrame::PeerList { addrs: Vec::new() });
            }
            let payload = data.split_to(payload_len);
            // Cap both the pre-allocation and the loop at MAX_CLUSTER_PEERS so a
            // declared count (u16, up to 65535) cannot force a large transient
            // allocation, and a 1 MB frame of tiny entries cannot expand into a
            // huge addr Vec. A real peer list never exceeds the peer cap.
            let count =
                (u16::from_le_bytes([payload[0], payload[1]]) as usize).min(MAX_CLUSTER_PEERS);
            let mut addrs = Vec::with_capacity(count);
            let mut pos = 2usize;
            for _ in 0..count {
                if pos + 2 > payload.len() {
                    break;
                }
                let alen = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2;
                if pos + alen > payload.len() {
                    break;
                }
                if let Ok(a) = std::str::from_utf8(&payload[pos..pos + alen]) {
                    addrs.push(a.to_string());
                }
                pos += alen;
            }
            Some(ClusterFrame::PeerList { addrs })
        }
        Some(MsgType::PresenceUpdate) => {
            if data.remaining() < topic_len + payload_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            if data.remaining() < 1 + 8 + 2 {
                return None;
            }
            let action = data.get_u8();
            let updated_at = data.get_u64_le();
            let user_id_len = data.get_u16_le() as usize;
            if data.remaining() < user_id_len {
                return None;
            }
            let user_id = String::from_utf8(data.split_to(user_id_len).to_vec()).ok()?;
            let remaining_data = payload_len.saturating_sub(1 + 8 + 2 + user_id_len);
            let presence_data = if remaining_data > 0 && data.remaining() >= remaining_data {
                String::from_utf8(data.split_to(remaining_data).to_vec()).ok()?
            } else {
                String::new()
            };
            Some(ClusterFrame::PresenceUpdate {
                topic,
                user_id,
                action,
                data: presence_data,
                updated_at,
            })
        }
        Some(MsgType::PresenceFull) => {
            if data.remaining() < payload_len {
                return None;
            }
            let entries = String::from_utf8(data.split_to(payload_len).to_vec()).ok()?;
            Some(ClusterFrame::PresenceFull { entries })
        }
        None => {
            // Unknown message type from a newer peer -- safe to skip
            // (outer 4-byte framing means we already consumed the right number of bytes)
            Some(ClusterFrame::Unknown { msg_type: raw_type })
        }
    }
}

// ---------------------------------------------------------------------------
// Version / capability negotiation
// ---------------------------------------------------------------------------

/// Negotiate protocol version. Returns min(local, remote) if >= PROTOCOL_VERSION_MIN.
pub(crate) fn negotiate_version(local_ver: u16, remote_ver: u16) -> Option<u16> {
    let negotiated = local_ver.min(remote_ver);
    if negotiated >= PROTOCOL_VERSION_MIN {
        Some(negotiated)
    } else {
        None
    }
}

/// Negotiate capabilities: bitwise AND (both must support a feature for it to be active).
pub(crate) fn negotiate_capabilities(local: u32, remote: u32) -> u32 {
    local & remote
}

/// Deterministic duplicate-connection tiebreaker. When two nodes discover each
/// other they BOTH dial, yielding two TCP links per pair; each node would then
/// decide locally which to keep and, in ~half of races, keep opposite links so
/// both get torn down and both reconnect (flapping). The surviving link is the
/// one whose DIALER has the lexicographically-lower instance_id: the lower-id
/// node keeps its outbound and the higher-id node rejects its own outbound and
/// accepts the inbound. Equal ids (a self-connection) survive on neither side,
/// so a node never connects to itself even if its own address is in its peer list.
pub(crate) fn cluster_link_survives(dialer_id: &str, receiver_id: &str) -> bool {
    dialer_id < receiver_id
}
