use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;

use super::reliability::{CircuitBreaker, ExponentialBackoff};
use super::server::ConnectionHandle;

/// Wall-clock time in milliseconds since UNIX epoch (for lock-free heartbeat tracking).
fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const MAGIC: &[u8; 4] = b"WSE\x00";
pub(crate) const PROTOCOL_VERSION: u16 = 1;
pub(crate) const MAX_FRAME_SIZE: usize = 1_048_576; // 1MB
pub(crate) const HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const HEARTBEAT_TIMEOUT_SECS: u64 = 15;
pub(crate) const MAX_BATCH_BYTES: usize = 65_536;
pub(crate) const MAX_CLUSTER_PEERS: usize = 256;
// Cluster peer channels are bounded (10K messages) to prevent OOM from slow peers.
// When a peer falls behind, messages are dropped and metrics.messages_dropped incremented.

pub(crate) const PROTOCOL_VERSION_MIN: u16 = 1;

// Capability flags (negotiated via bitwise AND of both peers' values)
pub(crate) const CAP_INTEREST_ROUTING: u32 = 1 << 0; // Phase 2: SUB/UNSUB
pub(crate) const CAP_COMPRESSION: u32 = 1 << 1; // Phase 6: zstd compression
pub(crate) const CAP_PRESENCE: u32 = 1 << 2; // Presence sync

/// Our capabilities bitmask
pub(crate) const LOCAL_CAPABILITIES: u32 = CAP_INTEREST_ROUTING | CAP_COMPRESSION | CAP_PRESENCE;

// Per-frame flags (bit 0 of the flags byte in every frame header)
// bit 0: compressed (zstd), bits 1-7: reserved
const FLAG_COMPRESSED: u8 = 0x01;

// Minimum payload size for compression to be worthwhile (bytes).
// Below this threshold, zstd overhead exceeds savings.
const COMPRESSION_THRESHOLD: usize = 256;

// ---------------------------------------------------------------------------
// TLS configuration
// ---------------------------------------------------------------------------

/// Pre-built TLS configs for both server (accept) and client (connect) roles.
/// A single node cert+key is used for both roles (like CockroachDB).
#[derive(Clone)]
pub(crate) struct ClusterTlsConfig {
    pub(crate) acceptor: TlsAcceptor,
    pub(crate) connector: TlsConnector,
}

/// Build cluster TLS config from PEM file paths.
/// Requires: node cert, node private key (unencrypted), CA cert.
pub(crate) fn build_cluster_tls(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<ClusterTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| format!("Failed to open cert file '{}': {}", cert_path, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse certs from '{}': {}", cert_path, e))?;
    if certs.is_empty() {
        return Err(format!("No certificates found in '{}'", cert_path).into());
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("Failed to load private key '{}': {}", key_path, e))?;

    let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(ca_path)
        .map_err(|e| format!("Failed to open CA file '{}': {}", ca_path, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse CA certs from '{}': {}", ca_path, e))?;
    if ca_certs.is_empty() {
        return Err(format!("No CA certificates found in '{}'", ca_path).into());
    }

    let mut root_store = RootCertStore::empty();
    for cert in &ca_certs {
        root_store.add(cert.clone())?;
    }

    // Server config: require client certs (mTLS)
    let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
        .build()
        .map_err(|e| format!("Failed to build client verifier: {}", e))?;
    let server_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs.clone(), key.clone_key())
        .map_err(|e| format!("Failed to build server TLS config: {}", e))?;

    // Client config: present our cert, verify server against CA
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("Failed to build client TLS config: {}", e))?;

    Ok(ClusterTlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(server_config)),
        connector: TlsConnector::from(Arc::new(client_config)),
    })
}

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
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands (Python -> cluster subsystem)
// ---------------------------------------------------------------------------

pub(crate) enum ClusterCommand {
    Publish {
        topic: String,
        payload: String,
    },
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
    Shutdown,
    /// Gossip a peer address to all connected peers (except the source).
    GossipPeerAnnounce {
        addr: String,
        exclude_peer: String,
    },
    /// Sync a single presence join/leave/update to cluster peers.
    PresenceUpdate {
        topic: String,
        user_id: String,
        action: u8, // 0 = join, 1 = leave, 2 = update
        data: String,
        updated_at: u64,
    },
}

/// Messages from inbound peer handlers to register/deregister their write channels
/// with the cluster manager, so inbound peers receive Publish/Sub/Unsub frames.
pub(crate) enum PeerRegistration {
    Register {
        peer_addr: String,
        write_tx: mpsc::Sender<Bytes>,
        negotiated_caps: u32,
    },
    Deregister {
        peer_addr: String,
    },
}

/// Presence frames received from a peer, forwarded to cluster_manager for processing.
pub(crate) enum PresencePeerFrame {
    Update {
        topic: String,
        user_id: String,
        action: u8,
        data: String,
        updated_at: u64,
    },
    Full {
        entries: String,
    },
}

pub(crate) enum InterestUpdate {
    Sub {
        peer_addr: String,
        topic: String,
    },
    Unsub {
        peer_addr: String,
        topic: String,
    },
    Resync {
        peer_addr: String,
        topics: Vec<String>,
    },
    PeerDisconnected {
        peer_addr: String,
    },
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub(crate) struct ClusterMetrics {
    pub messages_sent: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub reconnect_count: AtomicU64,
    pub connected_peers: AtomicU64,
    pub unknown_message_types: AtomicU64,
}

impl ClusterMetrics {
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            connected_peers: AtomicU64::new(0),
            unknown_message_types: AtomicU64::new(0),
        }
    }
}

impl super::server::FanoutMetrics for ClusterMetrics {
    fn add_delivered(&self, count: u64) {
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
    }
    fn add_dropped(&self, count: u64) {
        self.messages_dropped.fetch_add(count, Ordering::Relaxed);
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

    #[allow(dead_code)]
    pub fn snapshot(&self) -> Vec<(String, String, String)> {
        self.entries
            .iter()
            .map(|e| (e.topic.clone(), e.peer_addr.clone(), e.error.clone()))
            .collect()
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
        eprintln!(
            "[WSE-Cluster] Topic too long ({} bytes, max {}), dropping message",
            topic_bytes.len(),
            u16::MAX
        );
        return false;
    }
    let payload_bytes = payload.as_bytes();
    let total_frame = 8 + topic_bytes.len() + payload_bytes.len();
    if total_frame > MAX_FRAME_SIZE {
        eprintln!(
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
        eprintln!(
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

/// Encode a RESYNC frame. Topics are joined by "\n" in the payload.
/// Format: [u8 type=0x08][u8 flags][u16 LE topic_len=0][u32 LE payload_len][payload]
/// If the payload exceeds MAX_FRAME_SIZE, topics are truncated to fit.
pub(crate) fn encode_resync(buf: &mut BytesMut, topics: &[String]) {
    let payload = topics.join("\n");
    let payload_bytes = payload.as_bytes();
    // Guard: truncate if payload exceeds MAX_FRAME_SIZE to prevent peer disconnect
    let payload_bytes = if payload_bytes.len() > MAX_FRAME_SIZE {
        eprintln!(
            "[WSE-Cluster] RESYNC payload too large ({} bytes, {} topics), truncating to fit MAX_FRAME_SIZE",
            payload_bytes.len(),
            topics.len()
        );
        // Find the last complete topic that fits within MAX_FRAME_SIZE
        let truncated = &payload_bytes[..MAX_FRAME_SIZE];
        match truncated.iter().rposition(|&b| b == b'\n') {
            Some(pos) => &payload_bytes[..pos],
            None => truncated,
        }
    } else {
        payload_bytes
    };
    buf.reserve(8 + payload_bytes.len());
    buf.put_u8(MsgType::Resync as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(0); // no topic field
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(payload_bytes);
}

/// Encode PEER_ANNOUNCE: announces this node's cluster address to a peer.
/// Format: [u8 type=0x09][u8 flags][u16 LE addr_len][u32 LE 0][addr_bytes]
pub(crate) fn encode_peer_announce(buf: &mut BytesMut, addr: &str) {
    let addr_bytes = addr.as_bytes();
    if addr_bytes.len() > u16::MAX as usize {
        eprintln!(
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
    buf.reserve(8 + topic_bytes.len() + payload_len);
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
        eprintln!(
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

fn write_framed(buf: &mut BytesMut, inner: &[u8]) {
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
        payload: String,
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
                        eprintln!(
                            "[WSE-Cluster] Decompression failed ({payload_len} bytes compressed): {e}"
                        );
                        return None;
                    }
                };
                String::from_utf8(decompressed).ok()?
            } else {
                String::from_utf8(data.split_to(payload_len).to_vec()).ok()?
            };
            Some(ClusterFrame::Msg { topic, payload })
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
                eprintln!(
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
                let raw = String::from_utf8(data.split_to(payload_len).to_vec()).ok()?;
                raw.split('\n')
                    .filter(|t| !t.is_empty())
                    .map(String::from)
                    .collect()
            };
            Some(ClusterFrame::Resync { topics })
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
            let count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
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

// ---------------------------------------------------------------------------
// Per-peer writer task (write coalescing: recv + try_recv drain)
// ---------------------------------------------------------------------------

async fn peer_writer<W: AsyncWriteExt + Unpin>(
    mut rx: mpsc::Receiver<Bytes>,
    mut writer: W,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
) {
    let mut buf = BytesMut::with_capacity(MAX_BATCH_BYTES);

    loop {
        let first = tokio::select! {
            msg = rx.recv() => msg,
            () = cancel.cancelled() => break,
        };

        let Some(first) = first else { break };

        // Frame the first message
        write_framed(&mut buf, &first);

        // Drain everything else ready (non-blocking coalescing)
        while buf.len() < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(data) => write_framed(&mut buf, &data),
                Err(_) => break,
            }
        }

        let bytes_count = buf.len() as u64;
        if writer.write_all(&buf).await.is_err() {
            break;
        }
        // flush() is required for TLS -- rustls buffers encrypted data internally
        // and only writes to the TCP socket on flush. For plain TCP this is a no-op.
        if writer.flush().await.is_err() {
            break;
        }
        metrics.bytes_sent.fetch_add(bytes_count, Ordering::Relaxed);
        buf.clear();
    }
}

// ---------------------------------------------------------------------------
// Per-peer dispatch task (decoupled from reader for throughput)
// ---------------------------------------------------------------------------

/// Dispatch task: receives decoded messages from the reader via channel and fans
/// out to WebSocket connections in batches. Acquires read lock once per batch.
async fn peer_dispatch_task(
    mut rx: mpsc::UnboundedReceiver<(String, String)>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
) {
    let mut batch: Vec<(String, String)> = Vec::with_capacity(256);

    loop {
        let n = tokio::select! {
            n = rx.recv_many(&mut batch, 256) => n,
            () = cancel.cancelled() => break,
        };
        if n == 0 {
            break;
        }

        // Single read lock for entire batch
        let guard = connections.read().await;
        for (topic, payload) in &batch {
            let frame = super::server::WsFrame::PreFramed(super::server::pre_frame_text(payload));
            let senders = super::server::collect_topic_senders(&guard, &topic_subscribers, topic);
            super::server::fanout_to_senders_with_metrics(senders, frame, &metrics);
        }
        drop(guard);
        batch.clear();
    }
}

// ---------------------------------------------------------------------------
// Per-peer reader task
// ---------------------------------------------------------------------------

/// Reads frames from a peer TCP connection. Decoded MSG payloads are pushed to
/// a dispatch channel (non-blocking) so fan-out never stalls TCP reads.
/// BufReader (64KB) reduces read syscalls from 2/frame to ~1 per 650 frames.
#[allow(clippy::too_many_arguments)]
async fn peer_reader<R: AsyncReadExt + Unpin>(
    reader: R,
    peer_write_tx: mpsc::Sender<Bytes>,
    peer_addr: String,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
    last_activity: Arc<AtomicU64>,
    new_peer_tx: mpsc::UnboundedSender<String>,
    cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    presence_tx: mpsc::UnboundedSender<PresencePeerFrame>,
) {
    // Dispatch channel: decouples TCP read from fan-out to WebSocket connections.
    let (dispatch_tx, dispatch_rx) = mpsc::unbounded_channel::<(String, String)>();

    let dispatch_handle = tokio::spawn(peer_dispatch_task(
        dispatch_rx,
        connections,
        topic_subscribers,
        cancel.clone(),
        metrics.clone(),
    ));

    // BufReader: bulk reads from TCP into 64KB buffer, serves read_exact from memory
    let mut reader = BufReader::with_capacity(65_536, reader);
    let mut len_buf = [0u8; 4];

    loop {
        // Read frame length (4 bytes, big-endian)
        let read_result = tokio::select! {
            r = reader.read_exact(&mut len_buf) => r,
            () = cancel.cancelled() => break,
        };
        if read_result.is_err() {
            break;
        }

        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            eprintln!("[WSE-Cluster] Invalid frame size: {frame_len}, disconnecting");
            break;
        }

        // Read frame payload
        let mut frame_buf = BytesMut::zeroed(frame_len);
        let read_result = tokio::select! {
            r = reader.read_exact(&mut frame_buf) => r,
            () = cancel.cancelled() => break,
        };
        if read_result.is_err() {
            break;
        }

        metrics
            .bytes_received
            .fetch_add((4 + frame_len) as u64, Ordering::Relaxed);

        // Decode and dispatch
        match decode_frame(frame_buf) {
            Some(ClusterFrame::Msg { topic, payload }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = dispatch_tx.send((topic, payload));
            }
            Some(ClusterFrame::Ping) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let mut pong = BytesMut::new();
                encode_pong(&mut pong);
                let _ = peer_write_tx.try_send(pong.freeze());
            }
            Some(ClusterFrame::Pong) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
            }
            Some(ClusterFrame::Shutdown) => {
                eprintln!("[WSE-Cluster] Peer sent SHUTDOWN");
                break;
            }
            Some(ClusterFrame::Hello { .. }) => {
                // Unexpected HELLO after handshake, ignore
            }
            Some(ClusterFrame::Unknown { msg_type }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                metrics
                    .unknown_message_types
                    .fetch_add(1, Ordering::Relaxed);
                eprintln!("[WSE-Cluster] Unknown message type 0x{msg_type:02x}, skipping frame");
            }
            Some(ClusterFrame::Sub { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = interest_tx.send(InterestUpdate::Sub {
                    peer_addr: peer_addr.clone(),
                    topic,
                });
            }
            Some(ClusterFrame::Unsub { topic }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = interest_tx.send(InterestUpdate::Unsub {
                    peer_addr: peer_addr.clone(),
                    topic,
                });
            }
            Some(ClusterFrame::Resync { topics }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = interest_tx.send(InterestUpdate::Resync {
                    peer_addr: peer_addr.clone(),
                    topics,
                });
            }
            Some(ClusterFrame::PeerAnnounce { addr }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                // Notify cluster_manager to connect to new peer
                let _ = new_peer_tx.send(addr.clone());
                // Gossip to all other peers (excluding sender)
                let _ = cmd_tx.send(ClusterCommand::GossipPeerAnnounce {
                    addr,
                    exclude_peer: peer_addr.clone(),
                });
            }
            Some(ClusterFrame::PeerList { addrs }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                for addr in addrs {
                    let _ = new_peer_tx.send(addr);
                }
            }
            Some(ClusterFrame::PresenceUpdate {
                topic,
                user_id,
                action,
                data,
                updated_at,
            }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = presence_tx.send(PresencePeerFrame::Update {
                    topic,
                    user_id,
                    action,
                    data,
                    updated_at,
                });
            }
            Some(ClusterFrame::PresenceFull { entries }) => {
                last_activity.store(epoch_ms(), Ordering::Relaxed);
                let _ = presence_tx.send(PresencePeerFrame::Full { entries });
            }
            None => {
                eprintln!("[WSE-Cluster] Failed to decode frame, disconnecting");
                break;
            }
        }
    }

    // Ensure dispatch task is cleaned up
    drop(dispatch_tx);
    let _ = dispatch_handle.await;
}

// ---------------------------------------------------------------------------
// Heartbeat task
// ---------------------------------------------------------------------------

async fn heartbeat_task(
    peer_write_tx: mpsc::Sender<Bytes>,
    cancel: CancellationToken,
    last_activity: Arc<AtomicU64>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    let timeout_ms = HEARTBEAT_TIMEOUT_SECS * 1000;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check if peer is dead (no activity for HEARTBEAT_TIMEOUT_SECS)
                let last = last_activity.load(Ordering::Relaxed);
                if epoch_ms().saturating_sub(last) > timeout_ms {
                    eprintln!("[WSE-Cluster] Peer heartbeat timeout ({}s no activity)", timeout_ms / 1000);
                    cancel.cancel();
                    break;
                }
                let mut ping = BytesMut::new();
                encode_ping(&mut ping);
                match peer_write_tx.try_send(ping.freeze()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Peer backpressured -- skip this ping but keep monitoring.
                        // The timeout check above will still fire if peer goes silent.
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            () = cancel.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Single peer connection task (connect + HELLO + spawn reader/writer/heartbeat)
// ---------------------------------------------------------------------------

/// Run HELLO exchange, RESYNC, then spawn reader/writer/heartbeat and forward data.
/// Generic over AsyncRead/AsyncWrite to support both plaintext TCP and TLS streams.
/// Returns when the peer disconnects or cancellation is triggered.
#[allow(clippy::too_many_arguments)]
async fn run_peer_session<R, W>(
    mut reader: R,
    mut writer: W,
    peer_addr: &str,
    instance_id: &str,
    data_rx: &mut mpsc::Receiver<Bytes>,
    interest_tx: &mpsc::UnboundedSender<InterestUpdate>,
    connections: &Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: &Arc<DashMap<String, DashSet<String>>>,
    metrics: &Arc<ClusterMetrics>,
    global_cancel: &CancellationToken,
    local_topic_refcount: &Arc<std::sync::Mutex<HashMap<String, usize>>>,
    backoff: &mut ExponentialBackoff,
    breaker: &mut CircuitBreaker,
    new_peer_tx: &mpsc::UnboundedSender<String>,
    cmd_tx: &mpsc::UnboundedSender<ClusterCommand>,
    cluster_addr: &Option<String>,
    known_peers: &Arc<DashSet<String>>,
    connected_instances: &Arc<DashMap<String, String>>,
    presence_tx: &mpsc::UnboundedSender<PresencePeerFrame>,
    presence: &Option<Arc<super::presence::PresenceManager>>,
) -> bool
where
    R: AsyncReadExt + Unpin + Send + 'static,
    W: AsyncWriteExt + Unpin + Send + 'static,
{
    // Send HELLO
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
        breaker.record_failure();
        let delay = backoff.next_delay();
        eprintln!(
            "[WSE-Cluster] Failed to send HELLO to {peer_addr}. Retry in {:.1}s",
            delay.as_secs_f64()
        );
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = global_cancel.cancelled() => return true,
        }
        return false;
    }

    // Read HELLO response
    let peer_hello = match tokio::time::timeout(Duration::from_secs(5), async {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            return Ok::<Option<ClusterFrame>, std::io::Error>(None);
        }
        let mut frame_buf = BytesMut::zeroed(frame_len);
        reader.read_exact(&mut frame_buf).await?;
        Ok(decode_frame(frame_buf))
    })
    .await
    {
        Ok(Ok(frame)) => frame,
        _ => None,
    };

    let (peer_instance_id, _peer_version, peer_caps) = match peer_hello {
        Some(ClusterFrame::Hello {
            instance_id: peer_id,
            protocol_version,
            capabilities,
        }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
            Some(v) => (
                peer_id,
                v,
                negotiate_capabilities(LOCAL_CAPABILITIES, capabilities),
            ),
            None => {
                eprintln!(
                    "[WSE-Cluster] Version mismatch with {peer_addr}: local={PROTOCOL_VERSION}, remote={protocol_version}"
                );
                breaker.record_failure();
                let delay = backoff.next_delay();
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = global_cancel.cancelled() => return true,
                }
                return false;
            }
        },
        _ => {
            eprintln!("[WSE-Cluster] Invalid HELLO from {peer_addr}");
            breaker.record_failure();
            let delay = backoff.next_delay();
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = global_cancel.cancelled() => return true,
            }
            return false;
        }
    };

    // Duplicate connection prevention: atomic check-and-insert via entry() API
    // to avoid TOCTOU race between contains_key and insert
    {
        use dashmap::mapref::entry::Entry;
        match connected_instances.entry(peer_instance_id.clone()) {
            Entry::Occupied(_) => {
                eprintln!(
                    "[WSE-Cluster] Already connected to instance {}, dropping duplicate from {peer_addr}",
                    peer_instance_id
                );
                return false;
            }
            Entry::Vacant(e) => {
                e.insert(peer_addr.to_string());
            }
        }
    }

    // Connected successfully
    eprintln!("[WSE-Cluster] Connected to {peer_addr} (v{_peer_version}, caps=0x{peer_caps:08x})");
    breaker.record_success();
    backoff.reset();
    metrics.connected_peers.fetch_add(1, Ordering::Relaxed);

    // Send RESYNC with all current local topics (before spawning writer task)
    {
        let local_topics: Vec<String> = {
            let refcounts = local_topic_refcount
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            refcounts.keys().cloned().collect()
        };
        if !local_topics.is_empty() {
            let mut resync_buf = BytesMut::new();
            encode_resync(&mut resync_buf, &local_topics);
            let mut resync_frame = BytesMut::new();
            write_framed(&mut resync_frame, &resync_buf);
            if writer.write_all(&resync_frame).await.is_err() || writer.flush().await.is_err() {
                eprintln!("[WSE-Cluster] Failed to send RESYNC to {peer_addr}");
                connected_instances.remove(&peer_instance_id);
                metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
                return false; // Will reconnect
            }
        }
    }

    // Send PEER_ANNOUNCE with our cluster address (if in discovery mode)
    if let Some(our_addr) = cluster_addr {
        let mut announce_buf = BytesMut::new();
        encode_peer_announce(&mut announce_buf, our_addr);
        let mut announce_frame = BytesMut::new();
        write_framed(&mut announce_frame, &announce_buf);
        let _ = writer.write_all(&announce_frame).await;
        let _ = writer.flush().await;
    }

    // Send PEER_LIST with all known peers
    {
        let addrs: Vec<String> = known_peers.iter().map(|r| r.key().clone()).collect();
        if !addrs.is_empty() {
            let mut list_buf = BytesMut::new();
            encode_peer_list(&mut list_buf, &addrs);
            let mut list_frame = BytesMut::new();
            write_framed(&mut list_frame, &list_buf);
            let _ = writer.write_all(&list_frame).await;
            let _ = writer.flush().await;
        }
    }

    // Send full presence state to new peer (only if peer supports presence)
    if peer_caps & CAP_PRESENCE != 0
        && let Some(pm) = presence
    {
        let entries = pm.serialize_full_state();
        if !entries.is_empty() && entries != "{}" {
            let mut buf = BytesMut::new();
            if encode_presence_full(&mut buf, &entries) {
                let mut frame = BytesMut::new();
                write_framed(&mut frame, &buf);
                if let Err(e) = writer.write_all(&frame).await {
                    eprintln!("[WSE-Cluster] Failed to send PresenceFull: {e}");
                }
                let _ = writer.flush().await;
            }
        }
    }

    // Spawn reader, writer, heartbeat with child cancellation token
    let peer_cancel = global_cancel.child_token();
    let (peer_write_tx, peer_write_rx) = mpsc::channel::<Bytes>(10_000);
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

    let writer_handle = tokio::spawn(peer_writer(
        peer_write_rx,
        writer,
        peer_cancel.clone(),
        metrics.clone(),
    ));

    let reader_handle = tokio::spawn(peer_reader(
        reader,
        peer_write_tx.clone(),
        peer_addr.to_owned(),
        interest_tx.clone(),
        connections.clone(),
        topic_subscribers.clone(),
        peer_cancel.clone(),
        metrics.clone(),
        last_activity.clone(),
        new_peer_tx.clone(),
        cmd_tx.clone(),
        presence_tx.clone(),
    ));

    let heartbeat_handle = tokio::spawn(heartbeat_task(
        peer_write_tx.clone(),
        peer_cancel.clone(),
        last_activity,
    ));

    // Forward data from cluster manager to this peer's writer
    loop {
        let data = tokio::select! {
            data = data_rx.recv() => data,
            () = peer_cancel.cancelled() => break,
            () = global_cancel.cancelled() => {
                peer_cancel.cancel();
                break;
            }
        };

        match data {
            Some(frame_data) => {
                let _ = peer_write_tx.try_send(frame_data);
            }
            None => {
                // Manager channel closed (shutdown)
                peer_cancel.cancel();
                break;
            }
        }
    }

    // Cleanup
    peer_cancel.cancel();
    let _ = writer_handle.await;
    let _ = reader_handle.await;
    let _ = heartbeat_handle.await;
    metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    connected_instances.remove(&peer_instance_id);
    let _ = interest_tx.send(InterestUpdate::PeerDisconnected {
        peer_addr: peer_addr.to_owned(),
    });
    eprintln!("[WSE-Cluster] Disconnected from {peer_addr}");

    global_cancel.is_cancelled()
}

#[allow(clippy::too_many_arguments)]
async fn peer_connection_task(
    peer_addr: String,
    instance_id: String,
    mut data_rx: mpsc::Receiver<Bytes>,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    global_cancel: CancellationToken,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    tls_config: Option<ClusterTlsConfig>,
    new_peer_tx: mpsc::UnboundedSender<String>,
    cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    cluster_addr: Option<String>,
    known_peers: Arc<DashSet<String>>,
    connected_instances: Arc<DashMap<String, String>>,
    presence_tx: mpsc::UnboundedSender<PresencePeerFrame>,
    presence: Option<Arc<super::presence::PresenceManager>>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut breaker = CircuitBreaker::new();

    loop {
        if global_cancel.is_cancelled() {
            break;
        }

        // Drain stale messages buffered during backoff/reconnect to prevent
        // unbounded memory growth and replaying outdated data to the peer.
        while data_rx.try_recv().is_ok() {}

        // Circuit breaker check
        if !breaker.can_execute() {
            let delay = breaker.reset_timeout;
            eprintln!(
                "[WSE-Cluster] Circuit open for {peer_addr}, waiting {:.0}s",
                delay.as_secs_f64()
            );
            tokio::select! {
                () = tokio::time::sleep(delay) => continue,
                () = global_cancel.cancelled() => break,
            }
        }

        // TCP connect with timeout
        let stream = match tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect(&peer_addr),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                breaker.record_failure();
                metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                eprintln!(
                    "[WSE-Cluster] Connect failed {peer_addr}: {e}. Retry in {:.1}s",
                    delay.as_secs_f64()
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
            Err(_) => {
                breaker.record_failure();
                metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                eprintln!(
                    "[WSE-Cluster] Connect timeout {peer_addr}. Retry in {:.1}s",
                    delay.as_secs_f64()
                );
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
        };

        // TCP_NODELAY + keepalive + larger buffers for throughput
        let _ = stream.set_nodelay(true);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(10))
            .with_interval(Duration::from_secs(5));
        let sock_ref = SockRef::from(&stream);
        let _ = sock_ref.set_tcp_keepalive(&keepalive);
        let _ = sock_ref.set_recv_buffer_size(262_144);
        let _ = sock_ref.set_send_buffer_size(262_144);

        // TLS handshake or plaintext split, then run session
        let should_stop = if let Some(ref tls) = tls_config {
            // Extract hostname/IP from peer_addr for SNI
            let host = peer_addr.split(':').next().unwrap_or(&peer_addr);
            let server_name = match host.parse::<std::net::IpAddr>() {
                Ok(ip) => ServerName::IpAddress(ip.into()),
                Err(_) => match ServerName::try_from(host.to_owned()) {
                    Ok(name) => name,
                    Err(e) => {
                        breaker.record_failure();
                        let delay = backoff.next_delay();
                        eprintln!(
                            "[WSE-Cluster] Invalid peer address for TLS SNI '{host}': {e}. Retry in {:.1}s",
                            delay.as_secs_f64()
                        );
                        tokio::select! {
                            () = tokio::time::sleep(delay) => continue,
                            () = global_cancel.cancelled() => break,
                        }
                    }
                },
            };
            match tokio::time::timeout(
                Duration::from_secs(5),
                tls.connector.connect(server_name, stream),
            )
            .await
            {
                Ok(Ok(tls_stream)) => {
                    let (reader, writer) = tokio::io::split(tls_stream);
                    run_peer_session(
                        reader,
                        writer,
                        &peer_addr,
                        &instance_id,
                        &mut data_rx,
                        &interest_tx,
                        &connections,
                        &topic_subscribers,
                        &metrics,
                        &global_cancel,
                        &local_topic_refcount,
                        &mut backoff,
                        &mut breaker,
                        &new_peer_tx,
                        &cmd_tx,
                        &cluster_addr,
                        &known_peers,
                        &connected_instances,
                        &presence_tx,
                        &presence,
                    )
                    .await
                }
                Ok(Err(e)) => {
                    breaker.record_failure();
                    metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    let delay = backoff.next_delay();
                    eprintln!(
                        "[WSE-Cluster] TLS handshake failed {peer_addr}: {e}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        () = global_cancel.cancelled() => break,
                    }
                }
                Err(_) => {
                    breaker.record_failure();
                    metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    let delay = backoff.next_delay();
                    eprintln!(
                        "[WSE-Cluster] TLS handshake timeout {peer_addr}. Retry in {:.1}s",
                        delay.as_secs_f64()
                    );
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        () = global_cancel.cancelled() => break,
                    }
                }
            }
        } else {
            // Plaintext: split into owned halves
            let (reader, writer) = stream.into_split();
            run_peer_session(
                reader,
                writer,
                &peer_addr,
                &instance_id,
                &mut data_rx,
                &interest_tx,
                &connections,
                &topic_subscribers,
                &metrics,
                &global_cancel,
                &local_topic_refcount,
                &mut backoff,
                &mut breaker,
                &new_peer_tx,
                &cmd_tx,
                &cluster_addr,
                &known_peers,
                &connected_instances,
                &presence_tx,
                &presence,
            )
            .await
        };

        if should_stop {
            break;
        }

        // Reconnect
        metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
        let delay = backoff.next_delay();
        eprintln!(
            "[WSE-Cluster] Reconnecting to {peer_addr} in {:.1}s",
            delay.as_secs_f64()
        );
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = global_cancel.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster manager: orchestrates all peer connections
// ---------------------------------------------------------------------------

/// Shared context for spawning peer connection tasks (avoids duplicating 15 clone() calls).
struct PeerSpawnCtx {
    instance_id: String,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    global_cancel: CancellationToken,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    tls_config: Option<ClusterTlsConfig>,
    new_peer_tx: mpsc::UnboundedSender<String>,
    gossip_cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    cluster_addr: Option<String>,
    known_peers: Arc<DashSet<String>>,
    connected_instances: Arc<DashMap<String, String>>,
    presence_tx: mpsc::UnboundedSender<PresencePeerFrame>,
    presence: Option<Arc<super::presence::PresenceManager>>,
}

impl PeerSpawnCtx {
    fn spawn(
        &self,
        peer_addr: String,
        data_rx: mpsc::Receiver<Bytes>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(peer_connection_task(
            peer_addr,
            self.instance_id.clone(),
            data_rx,
            self.interest_tx.clone(),
            self.connections.clone(),
            self.topic_subscribers.clone(),
            self.metrics.clone(),
            self.global_cancel.clone(),
            self.local_topic_refcount.clone(),
            self.tls_config.clone(),
            self.new_peer_tx.clone(),
            self.gossip_cmd_tx.clone(),
            self.cluster_addr.clone(),
            self.known_peers.clone(),
            self.connected_instances.clone(),
            self.presence_tx.clone(),
            self.presence.clone(),
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn cluster_manager(
    peers: Vec<String>,
    instance_id: String,
    mut cmd_rx: mpsc::UnboundedReceiver<ClusterCommand>,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    mut interest_rx: mpsc::UnboundedReceiver<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    tls_config: Option<ClusterTlsConfig>,
    cluster_port: Option<u16>,
    cluster_addr: Option<String>,
    presence: Option<Arc<super::presence::PresenceManager>>,
    server_cmd_tx: Option<mpsc::UnboundedSender<super::server::ServerCommand>>,
) {
    let global_cancel = CancellationToken::new();

    // Remote interest table: peer_addr -> set of topics that peer is interested in
    let mut remote_interest: HashMap<String, ahash::AHashSet<String>> = HashMap::new();

    // Dynamic peer tracking
    let known_peers: Arc<DashSet<String>> = Arc::new(DashSet::new());
    let connected_instances: Arc<DashMap<String, String>> = Arc::new(DashMap::new());
    // Static peers have their own reconnect loop; never remove them from known_peers
    let static_peers: std::collections::HashSet<String> = peers.iter().cloned().collect();

    // Channel for peer_reader to notify about newly discovered peers
    let (new_peer_tx, mut new_peer_rx) = mpsc::unbounded_channel::<String>();

    // Reusable cmd_tx for peer tasks to send gossip commands back to manager
    let (gossip_cmd_tx, mut gossip_cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();

    // Channel for inbound peer handlers to register/deregister their write channels
    let (peer_reg_tx, mut peer_reg_rx) = mpsc::unbounded_channel::<PeerRegistration>();

    // Channel for presence frames received from peers
    let (presence_peer_tx, mut presence_peer_rx) = mpsc::unbounded_channel::<PresencePeerFrame>();

    // Per-peer channels: manager sends encoded BytesMut, peer task forwards to writer
    // Each entry: (peer_addr, write_channel, negotiated_caps)
    let mut peer_txs: Vec<(String, mpsc::Sender<Bytes>, u32)> = Vec::new();
    let mut peer_handles = Vec::new();

    let spawn_ctx = PeerSpawnCtx {
        instance_id: instance_id.clone(),
        interest_tx: interest_tx.clone(),
        connections: connections.clone(),
        topic_subscribers: topic_subscribers.clone(),
        metrics: metrics.clone(),
        global_cancel: global_cancel.clone(),
        local_topic_refcount: local_topic_refcount.clone(),
        tls_config: tls_config.clone(),
        new_peer_tx: new_peer_tx.clone(),
        gossip_cmd_tx: gossip_cmd_tx.clone(),
        cluster_addr: cluster_addr.clone(),
        known_peers: known_peers.clone(),
        connected_instances: connected_instances.clone(),
        presence_tx: presence_peer_tx.clone(),
        presence: presence.clone(),
    };

    for peer_addr in &peers {
        known_peers.insert(peer_addr.clone());
        let (tx, rx) = mpsc::channel::<Bytes>(10_000);
        // Outbound peers: caps are negotiated in run_peer_session; default to full caps
        // since all peers in current deployment run the same version.
        peer_txs.push((peer_addr.clone(), tx, LOCAL_CAPABILITIES));
        peer_handles.push(spawn_ctx.spawn(peer_addr.clone(), rx));
    }

    // Prune completed peer handles periodically to prevent unbounded growth
    let prune_handles = |handles: &mut Vec<tokio::task::JoinHandle<()>>| {
        if handles.len().is_multiple_of(64) {
            handles.retain(|h| !h.is_finished());
        }
    };

    // Start cluster listener for inbound peer connections (separate port)
    // Limit concurrent inbound connections to prevent resource exhaustion
    const MAX_INBOUND_CLUSTER_CONNS: usize = 128;
    if let Some(port) = cluster_port {
        let bind_addr = format!("0.0.0.0:{}", port);
        match tokio::net::TcpListener::bind(&bind_addr).await {
            Ok(cluster_listener) => {
                eprintln!("[WSE-Cluster] Cluster listener on {bind_addr}");
                let tls_cfg = tls_config.clone();
                let inst_id = instance_id.clone();
                let int_tx = interest_tx.clone();
                let conns = connections.clone();
                let subs = topic_subscribers.clone();
                let met = metrics.clone();
                let cancel = global_cancel.clone();
                let lrc = local_topic_refcount.clone();
                let npt = new_peer_tx.clone();
                let gct = gossip_cmd_tx.clone();
                let ci = connected_instances.clone();
                let prt = peer_reg_tx.clone();
                let ca = cluster_addr.clone();
                let kp = known_peers.clone();
                let pptx = presence_peer_tx.clone();
                let ppres = presence.clone();
                let conn_semaphore =
                    Arc::new(tokio::sync::Semaphore::new(MAX_INBOUND_CLUSTER_CONNS));
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            res = cluster_listener.accept() => {
                                match res {
                                    Ok((stream, addr)) => {
                                        let permit = match conn_semaphore.clone().try_acquire_owned() {
                                            Ok(permit) => permit,
                                            Err(_) => {
                                                eprintln!("[WSE-Cluster] Max inbound connections reached, rejecting {addr}");
                                                drop(stream);
                                                continue;
                                            }
                                        };
                                        let _ = stream.set_nodelay(true);
                                        let sock = SockRef::from(&stream);
                                        let _ = sock.set_send_buffer_size(262_144);
                                        let _ = sock.set_recv_buffer_size(262_144);
                                        let tls = tls_cfg.clone();
                                        let int_tx2 = int_tx.clone();
                                        let conns2 = conns.clone();
                                        let subs2 = subs.clone();
                                        let met2 = met.clone();
                                        let inst2 = inst_id.clone();
                                        let lrc2 = lrc.clone();
                                        let npt2 = npt.clone();
                                        let gct2 = gct.clone();
                                        let ci2 = ci.clone();
                                        let prt2 = prt.clone();
                                        let gc2 = cancel.clone();
                                        let ca2 = ca.clone();
                                        let kp2 = kp.clone();
                                        let pptx2 = pptx.clone();
                                        let ppres2 = ppres.clone();
                                        tokio::spawn(async move {
                                            let _permit = permit; // held until task completes
                                            if let Some(ref tls) = tls {
                                                match tokio::time::timeout(
                                                    Duration::from_secs(5),
                                                    tls.acceptor.accept(stream),
                                                ).await {
                                                    Ok(Ok(tls_stream)) => {
                                                        handle_cluster_inbound_generic(
                                                            tls_stream, addr, &inst2,
                                                            int_tx2, conns2, subs2, met2, lrc2,
                                                            npt2, gct2, ci2, prt2, gc2,
                                                            ca2, kp2, pptx2, ppres2,
                                                        ).await;
                                                    }
                                                    Ok(Err(e)) => {
                                                        eprintln!("[WSE-Cluster] TLS accept failed from {addr}: {e}");
                                                    }
                                                    Err(_) => {
                                                        eprintln!("[WSE-Cluster] TLS accept timeout from {addr}");
                                                    }
                                                }
                                            } else {
                                                handle_cluster_inbound_generic(
                                                    stream, addr, &inst2,
                                                    int_tx2, conns2, subs2, met2, lrc2,
                                                    npt2, gct2, ci2, prt2, gc2,
                                                    ca2, kp2, pptx2, ppres2,
                                                ).await;
                                            }
                                        });
                                    }
                                    Err(e) => {
                                        eprintln!("[WSE-Cluster] Cluster accept error: {e}");
                                    }
                                }
                            }
                            () = cancel.cancelled() => break,
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("[WSE-Cluster] Failed to bind cluster listener on {bind_addr}: {e}");
            }
        }
    }

    eprintln!(
        "[WSE-Cluster] Manager started, {} peers configured",
        peers.len()
    );

    // Main loop: fan out commands to all peer channels
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ClusterCommand::Publish { topic, payload }) => {
                        // Pre-encode both compressed and uncompressed frames.
                        // Send compressed only to peers that negotiated CAP_COMPRESSION.
                        let mut compressed_data = BytesMut::new();
                        let has_compressed = encode_msg_compressed(&mut compressed_data, &topic, &payload);
                        let compressed_bytes = if has_compressed { Some(compressed_data.freeze()) } else { None };

                        // Uncompressed fallback for peers without CAP_COMPRESSION
                        let mut plain_data = BytesMut::new();
                        if !encode_msg(&mut plain_data, &topic, &payload) {
                            // Topic too long (>64KB) -- drop, don't send empty frame
                            eprintln!(
                                "[WSE-Cluster] Dropping publish: topic too long ({} bytes)",
                                topic.len()
                            );
                            continue;
                        }
                        let plain_bytes = plain_data.freeze();

                        for (peer_addr, tx, caps) in &peer_txs {
                            let interested = match remote_interest.get(peer_addr) {
                                Some(topics) => {
                                    topics.contains(&topic)
                                        || topics.iter().any(|pat| {
                                            (pat.contains('*') || pat.contains('?'))
                                                && super::server::glob_match(pat, &topic)
                                        })
                                }
                                None => true, // No RESYNC yet -> safe default: send to all
                            };
                            if interested {
                                let frame = if caps & CAP_COMPRESSION != 0 {
                                    compressed_bytes.as_ref().unwrap_or(&plain_bytes).clone()
                                } else {
                                    plain_bytes.clone()
                                };
                                match tx.try_send(frame) {
                                    Ok(()) => {
                                        metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                        eprintln!("[WSE-Cluster] Peer {peer_addr} backpressure, dropping message");
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                        if let Ok(mut dlq_guard) = dlq.lock() {
                                            dlq_guard.push(ClusterDlqEntry {
                                                topic: topic.clone(),
                                                payload: payload.clone(),
                                                peer_addr: peer_addr.clone(),
                                                error: "peer channel closed".to_string(),
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Sub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_sub(&mut frame_data, &topic) {
                            let frame_bytes = frame_data.freeze();
                            for (_, tx, _) in &peer_txs {
                                let _ = tx.try_send(frame_bytes.clone());
                            }
                        }
                    }
                    Some(ClusterCommand::Unsub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_unsub(&mut frame_data, &topic) {
                            let frame_bytes = frame_data.freeze();
                            for (_, tx, _) in &peer_txs {
                                let _ = tx.try_send(frame_bytes.clone());
                            }
                        }
                    }
                    Some(ClusterCommand::GossipPeerAnnounce { addr, exclude_peer }) => {
                        // Validate address before forwarding
                        if addr.parse::<std::net::SocketAddr>().is_err() {
                            eprintln!("[WSE-Cluster] Ignoring invalid gossip address from cmd_rx: {addr}");
                            continue;
                        }
                        // Forward only for newly discovered peers to prevent amplification
                        if !known_peers.contains(&addr) {
                            let mut frame_data = BytesMut::new();
                            encode_peer_announce(&mut frame_data, &addr);
                            let frame_bytes = frame_data.freeze();
                            for (peer_addr, tx, _) in &peer_txs {
                                if *peer_addr != exclude_peer {
                                    let _ = tx.try_send(frame_bytes.clone());
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::PresenceUpdate { topic, user_id, action, data, updated_at }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_presence_update(&mut frame_data, &topic, &user_id, action, &data, updated_at) {
                            let frame_bytes = frame_data.freeze();
                            for (peer_addr, tx, caps) in &peer_txs {
                                if caps & CAP_PRESENCE != 0 {
                                    match tx.try_send(frame_bytes.clone()) {
                                        Ok(()) => {}
                                        Err(mpsc::error::TrySendError::Full(_)) => {
                                            eprintln!("[WSE-Cluster] Peer {peer_addr} backpressure, dropping presence update");
                                        }
                                        Err(mpsc::error::TrySendError::Closed(_)) => {
                                            if let Ok(mut dlq_guard) = dlq.lock() {
                                                dlq_guard.push(ClusterDlqEntry {
                                                    topic: topic.clone(),
                                                    payload: data.clone(),
                                                    peer_addr: peer_addr.clone(),
                                                    error: "presence update failed".to_string(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Shutdown) | None => {
                        eprintln!("[WSE-Cluster] Manager shutting down");
                        let mut shutdown_data = BytesMut::new();
                        encode_shutdown(&mut shutdown_data);
                        let shutdown_bytes = shutdown_data.freeze();
                        for (peer_addr, tx, _) in &peer_txs {
                            if tx.try_send(shutdown_bytes.clone()).is_err() {
                                eprintln!("[WSE-Cluster] Could not send SHUTDOWN to {peer_addr}");
                            }
                        }
                        global_cancel.cancel();
                        break;
                    }
                }
            }
            // Dynamic peer discovery: spawn connection tasks for newly discovered peers
            Some(new_addr) = new_peer_rx.recv() => {
                // Validate address parses as a valid SocketAddr to prevent SSRF
                if new_addr.parse::<std::net::SocketAddr>().is_err() {
                    eprintln!("[WSE-Cluster] Ignoring invalid peer address from gossip: {new_addr}");
                    continue;
                }
                // Skip if we already know this peer or it's our own address
                let is_self = cluster_addr.as_deref() == Some(&new_addr);
                let already_known = !known_peers.insert(new_addr.clone());
                if !is_self && !already_known && !peer_txs.iter().any(|(a, _, _)| a == &new_addr) {
                    // Prune dead peer channels so they don't count toward capacity
                    peer_txs.retain(|(_, tx, _)| !tx.is_closed());
                    if peer_txs.len() >= MAX_CLUSTER_PEERS {
                        eprintln!(
                            "[WSE-Cluster] Ignoring discovered peer {new_addr}: at capacity ({MAX_CLUSTER_PEERS} peers)"
                        );
                    } else {
                        let (tx, rx) = mpsc::channel::<Bytes>(10_000);
                        peer_txs.push((new_addr.clone(), tx, LOCAL_CAPABILITIES));
                        peer_handles.push(spawn_ctx.spawn(new_addr.clone(), rx));
                        prune_handles(&mut peer_handles);
                        eprintln!("[WSE-Cluster] Discovered new peer: {new_addr}");
                    }
                }
            }
            // Handle gossip commands from peer tasks
            Some(gossip_cmd) = gossip_cmd_rx.recv() => {
                if let ClusterCommand::GossipPeerAnnounce { addr, exclude_peer } = gossip_cmd {
                    // Validate address before processing
                    if addr.parse::<std::net::SocketAddr>().is_err() {
                        eprintln!("[WSE-Cluster] Ignoring invalid gossip address: {addr}");
                        continue;
                    }
                    let is_self = cluster_addr.as_deref() == Some(&addr);
                    if is_self {
                        continue;
                    }
                    // Forward PEER_ANNOUNCE only for newly discovered peers
                    // to prevent gossip amplification in dense clusters
                    let is_new = !known_peers.contains(&addr);
                    if is_new {
                        let mut frame_data = BytesMut::new();
                        encode_peer_announce(&mut frame_data, &addr);
                        let frame_bytes = frame_data.freeze();
                        for (peer_addr, tx, _) in &peer_txs {
                            if *peer_addr != exclude_peer {
                                let _ = tx.try_send(frame_bytes.clone());
                            }
                        }
                    }
                    // Only spawn connection to this peer if we don't already know it
                    if known_peers.insert(addr.clone())
                        && !peer_txs.iter().any(|(a, _, _)| a == &addr)
                    {
                        peer_txs.retain(|(_, tx, _)| !tx.is_closed());
                        if peer_txs.len() >= MAX_CLUSTER_PEERS {
                            eprintln!(
                                "[WSE-Cluster] Ignoring gossip peer {addr}: at capacity ({MAX_CLUSTER_PEERS} peers)"
                            );
                        } else {
                            let (tx, rx) = mpsc::channel::<Bytes>(10_000);
                            peer_txs.push((addr.clone(), tx, LOCAL_CAPABILITIES));
                            peer_handles.push(spawn_ctx.spawn(addr.clone(), rx));
                            prune_handles(&mut peer_handles);
                            eprintln!("[WSE-Cluster] Discovered peer via gossip: {addr}");
                        }
                    }
                }
            }
            update = interest_rx.recv() => {
                match update {
                    Some(InterestUpdate::Sub { peer_addr, topic }) => {
                        remote_interest.entry(peer_addr).or_default().insert(topic);
                    }
                    Some(InterestUpdate::Unsub { peer_addr, topic }) => {
                        if let Some(topics) = remote_interest.get_mut(&peer_addr) {
                            topics.remove(&topic);
                        }
                    }
                    Some(InterestUpdate::Resync { peer_addr, topics }) => {
                        let set: ahash::AHashSet<String> = topics.into_iter().collect();
                        remote_interest.insert(peer_addr, set);
                    }
                    Some(InterestUpdate::PeerDisconnected { peer_addr }) => {
                        remote_interest.remove(&peer_addr);
                        // Allow gossip-discovered peers to be re-discovered
                        if !static_peers.contains(&peer_addr) {
                            known_peers.remove(&peer_addr);
                        }
                    }
                    None => {
                        // Interest channel closed, continue running
                    }
                }
            }
            // Handle inbound peer registrations/deregistrations
            Some(reg) = peer_reg_rx.recv() => {
                match reg {
                    PeerRegistration::Register { peer_addr, write_tx, negotiated_caps } => {
                        // Add inbound peer's write channel so it receives Publish/Sub/Unsub
                        if !peer_txs.iter().any(|(a, _, _)| a == &peer_addr) {
                            peer_txs.push((peer_addr.clone(), write_tx, negotiated_caps));
                            eprintln!("[WSE-Cluster] Registered inbound peer: {peer_addr} (caps=0x{negotiated_caps:08x})");
                        }
                    }
                    PeerRegistration::Deregister { peer_addr } => {
                        // Remove disconnected inbound peer
                        peer_txs.retain(|(a, _, _)| a != &peer_addr);
                        // Allow gossip-discovered peers to be re-discovered
                        if !static_peers.contains(&peer_addr) {
                            known_peers.remove(&peer_addr);
                        }
                    }
                }
            }
            // Handle presence frames received from peers
            Some(pf) = presence_peer_rx.recv() => {
                if let Some(ref pm) = presence {
                    match pf {
                        PresencePeerFrame::Update { topic, user_id, action, data, updated_at } => {
                            let data_val: serde_json::Value = serde_json::from_str(&data).unwrap_or(serde_json::Value::Null);
                            match action {
                                0 => {
                                    // Remote join: merge into local presence
                                    pm.merge_remote_join(&topic, &user_id, &data_val, updated_at);
                                    if let Some(ref tx) = server_cmd_tx {
                                        let msg = super::server::format_presence_msg("presence_join", &user_id, &data_val);
                                        let _ = tx.send(super::server::ServerCommand::BroadcastLocal {
                                            topic,
                                            data: msg,
                                            skip_recovery: true,
                                        });
                                    }
                                }
                                1 => {
                                    // Remote leave
                                    pm.merge_remote_leave(&topic, &user_id);
                                    if let Some(ref tx) = server_cmd_tx {
                                        let msg = super::server::format_presence_msg("presence_leave", &user_id, &data_val);
                                        let _ = tx.send(super::server::ServerCommand::BroadcastLocal {
                                            topic,
                                            data: msg,
                                            skip_recovery: true,
                                        });
                                    }
                                }
                                2 => {
                                    // Remote data update
                                    pm.merge_remote_update(&topic, &user_id, &data_val, updated_at);
                                    if let Some(ref tx) = server_cmd_tx {
                                        let msg = super::server::format_presence_msg("presence_update", &user_id, &data_val);
                                        let _ = tx.send(super::server::ServerCommand::BroadcastLocal {
                                            topic,
                                            data: msg,
                                            skip_recovery: true,
                                        });
                                    }
                                }
                                _ => {} // Ignore unknown actions
                            }
                        }
                        PresencePeerFrame::Full { entries } => {
                            pm.merge_full_state(&entries);
                        }
                    }
                }
            }
            () = global_cancel.cancelled() => break,
        }

        // Prune dead peer channels (receiver dropped = peer task exited)
        peer_txs.retain(|(addr, tx, _)| {
            if tx.is_closed() {
                eprintln!("[WSE-Cluster] Pruning dead peer channel: {addr}");
                false
            } else {
                true
            }
        });
    }

    // Wait for all peer tasks to finish
    for handle in peer_handles {
        let _ = handle.await;
    }
    eprintln!("[WSE-Cluster] Manager stopped");
}

// ---------------------------------------------------------------------------
// Inbound cluster connection handler (generic over transport)
// ---------------------------------------------------------------------------

/// Handle inbound cluster peer connection. Generic over stream type (TLS or plaintext).
#[allow(clippy::too_many_arguments)]
async fn handle_cluster_inbound_generic<S>(
    stream: S,
    addr: SocketAddr,
    instance_id: &str,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    new_peer_tx: mpsc::UnboundedSender<String>,
    cmd_tx: mpsc::UnboundedSender<ClusterCommand>,
    connected_instances: Arc<DashMap<String, String>>,
    peer_reg_tx: mpsc::UnboundedSender<PeerRegistration>,
    global_cancel: CancellationToken,
    cluster_addr: Option<String>,
    known_peers: Arc<DashSet<String>>,
    presence_tx: mpsc::UnboundedSender<PresencePeerFrame>,
    presence: Option<Arc<super::presence::PresenceManager>>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    // Read peer's HELLO frame
    let peer_hello = match tokio::time::timeout(Duration::from_secs(5), async {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
            return Ok::<Option<ClusterFrame>, std::io::Error>(None);
        }
        let mut frame_buf = BytesMut::zeroed(frame_len);
        reader.read_exact(&mut frame_buf).await?;
        Ok(decode_frame(frame_buf))
    })
    .await
    {
        Ok(Ok(frame)) => frame,
        _ => None,
    };

    let (peer_instance_id, _peer_version, peer_caps) = match peer_hello {
        Some(ClusterFrame::Hello {
            instance_id: peer_id,
            protocol_version,
            capabilities,
        }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
            Some(v) => (
                peer_id,
                v,
                negotiate_capabilities(LOCAL_CAPABILITIES, capabilities),
            ),
            None => {
                eprintln!(
                    "[WSE-Cluster] Version mismatch with inbound {addr}: local={PROTOCOL_VERSION}, remote={protocol_version}"
                );
                return;
            }
        },
        _ => {
            eprintln!("[WSE-Cluster] Invalid HELLO from inbound {addr}");
            return;
        }
    };

    // Duplicate connection prevention: atomic check-and-insert via entry() API
    {
        use dashmap::mapref::entry::Entry;
        match connected_instances.entry(peer_instance_id.clone()) {
            Entry::Occupied(_) => {
                eprintln!(
                    "[WSE-Cluster] Already connected to instance {}, rejecting inbound from {addr}",
                    peer_instance_id
                );
                return;
            }
            Entry::Vacant(e) => {
                e.insert(addr.to_string());
            }
        }
    }

    // Send our HELLO response
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() || writer.flush().await.is_err() {
        eprintln!("[WSE-Cluster] Failed to send HELLO to inbound {addr}");
        connected_instances.remove(&peer_instance_id);
        return;
    }

    // Send RESYNC with all current local topics (before spawning writer task)
    {
        let local_topics: Vec<String> = {
            let refcounts = local_topic_refcount
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            refcounts.keys().cloned().collect()
        };
        if !local_topics.is_empty() {
            let mut resync_buf = BytesMut::new();
            encode_resync(&mut resync_buf, &local_topics);
            let mut resync_frame = BytesMut::new();
            write_framed(&mut resync_frame, &resync_buf);
            if writer.write_all(&resync_frame).await.is_err() || writer.flush().await.is_err() {
                eprintln!("[WSE-Cluster] Failed to send RESYNC to inbound {addr}");
                connected_instances.remove(&peer_instance_id);
                return;
            }
        }
    }

    // Send PEER_ANNOUNCE with our cluster address (if in discovery mode)
    if let Some(ref our_addr) = cluster_addr {
        let mut announce_buf = BytesMut::new();
        encode_peer_announce(&mut announce_buf, our_addr);
        let mut announce_frame = BytesMut::new();
        write_framed(&mut announce_frame, &announce_buf);
        let _ = writer.write_all(&announce_frame).await;
        let _ = writer.flush().await;
    }

    // Send PEER_LIST with all known peers
    {
        let addrs: Vec<String> = known_peers.iter().map(|r| r.key().clone()).collect();
        if !addrs.is_empty() {
            let mut list_buf = BytesMut::new();
            encode_peer_list(&mut list_buf, &addrs);
            let mut list_frame = BytesMut::new();
            write_framed(&mut list_frame, &list_buf);
            let _ = writer.write_all(&list_frame).await;
            let _ = writer.flush().await;
        }
    }

    // Send full presence state to new peer (only if peer supports presence)
    if peer_caps & CAP_PRESENCE != 0
        && let Some(pm) = presence
    {
        let entries = pm.serialize_full_state();
        if !entries.is_empty() && entries != "{}" {
            let mut buf = BytesMut::new();
            if encode_presence_full(&mut buf, &entries) {
                let mut frame = BytesMut::new();
                write_framed(&mut frame, &buf);
                if let Err(e) = writer.write_all(&frame).await {
                    eprintln!("[WSE-Cluster] Failed to send PresenceFull to inbound {addr}: {e}");
                }
                let _ = writer.flush().await;
            }
        }
    }

    eprintln!(
        "[WSE-Cluster] Accepted inbound peer from {addr} (v{_peer_version}, caps=0x{peer_caps:08x})"
    );
    let peer_addr_str = addr.to_string();
    metrics.connected_peers.fetch_add(1, Ordering::Relaxed);

    let cancel = global_cancel.child_token();
    let (write_tx, write_rx) = mpsc::channel::<Bytes>(10_000);
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

    // Register this inbound peer's write channel with the cluster manager
    // so it receives Publish/Sub/Unsub frames (bidirectional routing)
    let _ = peer_reg_tx.send(PeerRegistration::Register {
        peer_addr: peer_addr_str.clone(),
        write_tx: write_tx.clone(),
        negotiated_caps: peer_caps,
    });

    let writer_handle = tokio::spawn(peer_writer(
        write_rx,
        writer,
        cancel.clone(),
        metrics.clone(),
    ));

    let reader_handle = tokio::spawn(peer_reader(
        reader,
        write_tx.clone(),
        peer_addr_str.clone(),
        interest_tx.clone(),
        connections,
        topic_subscribers,
        cancel.clone(),
        metrics.clone(),
        last_activity.clone(),
        new_peer_tx,
        cmd_tx,
        presence_tx,
    ));

    let heartbeat_handle = tokio::spawn(heartbeat_task(write_tx, cancel.clone(), last_activity));

    // Wait for any task to finish, then cancel + await all (prevents task leaks)
    tokio::pin!(reader_handle, writer_handle, heartbeat_handle);
    tokio::select! {
        _ = &mut reader_handle => {}
        _ = &mut writer_handle => {}
        _ = &mut heartbeat_handle => {}
    }
    cancel.cancel();
    let _ = reader_handle.await;
    let _ = writer_handle.await;
    let _ = heartbeat_handle.await;

    // Deregister inbound peer from the cluster manager's peer_txs
    let _ = peer_reg_tx.send(PeerRegistration::Deregister {
        peer_addr: peer_addr_str.clone(),
    });
    connected_instances.remove(&peer_instance_id);
    let _ = interest_tx.send(InterestUpdate::PeerDisconnected {
        peer_addr: peer_addr_str,
    });
    metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    eprintln!("[WSE-Cluster] Inbound peer {addr} disconnected");
}

/// Legacy inbound handler: used when cluster connections arrive on the main port (no separate cluster listener).
/// Kept for backwards compatibility when cluster_port is not configured.
pub(crate) async fn handle_cluster_inbound(
    stream: TcpStream,
    addr: SocketAddr,
    shared: &super::server::SharedState,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
) {
    let instance_id = match shared
        .cluster_instance_id
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
    {
        Some(id) => id,
        None => {
            eprintln!("[WSE-Cluster] Rejected inbound from {addr}: cluster not configured");
            return;
        }
    };

    let keepalive = TcpKeepalive::new()
        .with_time(Duration::from_secs(10))
        .with_interval(Duration::from_secs(5));
    let sock_ref = SockRef::from(&stream);
    let _ = sock_ref.set_tcp_keepalive(&keepalive);
    let _ = sock_ref.set_recv_buffer_size(262_144);
    let _ = sock_ref.set_send_buffer_size(262_144);

    // Legacy path: no cluster_manager, so create dummy channels for discovery and registration
    // (messages sent here are just dropped -- discovery/registration only works with cluster_port)
    let (new_peer_tx, _new_peer_rx) = mpsc::unbounded_channel::<String>();
    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<ClusterCommand>();
    let (peer_reg_tx, _peer_reg_rx) = mpsc::unbounded_channel::<PeerRegistration>();
    let (presence_tx, _presence_rx) = mpsc::unbounded_channel::<PresencePeerFrame>();
    // Shared across all legacy inbound calls so duplicate detection actually works
    static LEGACY_CONNECTED: std::sync::OnceLock<Arc<DashMap<String, String>>> =
        std::sync::OnceLock::new();
    let connected_instances = LEGACY_CONNECTED
        .get_or_init(|| Arc::new(DashMap::new()))
        .clone();
    // Legacy path has no global_cancel -- create a standalone token.
    // Cleanup happens via tokio runtime drop on server stop.
    let legacy_cancel = CancellationToken::new();

    // Legacy path has no gossip discovery
    let legacy_known_peers: Arc<DashSet<String>> = Arc::new(DashSet::new());

    handle_cluster_inbound_generic(
        stream,
        addr,
        &instance_id,
        interest_tx,
        shared.connections.clone(),
        shared.topic_subscribers.clone(),
        shared.cluster_metrics.clone(),
        shared.local_topic_refcount.clone(),
        new_peer_tx,
        cmd_tx,
        connected_instances,
        peer_reg_tx,
        legacy_cancel,
        None,
        legacy_known_peers,
        presence_tx,
        shared.presence.clone(),
    )
    .await;
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
    fn test_encode_decode_msg_compressed() {
        // Small payload: should NOT be compressed (below threshold)
        let mut buf = BytesMut::new();
        encode_msg_compressed(&mut buf, "chat", "hello");
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "chat".into(),
                payload: "hello".into(),
            }
        );

        // Large payload: should be compressed (must be >= 256 bytes)
        let big_payload = r#"{"user":"alice","message":"hello world, this is a longer message with repeated patterns for compression","timestamp":1234567890,"tags":["alpha","beta","gamma","delta","epsilon","zeta","eta","theta","iota","kappa"],"metadata":{"source":"api-gateway","version":"1.0.0","region":"us-east-1","cluster":"prod-main","environment":"production","extra":"additional padding data to ensure this payload exceeds the compression threshold of 256 bytes easily"}}"#;
        assert!(big_payload.len() >= COMPRESSION_THRESHOLD);
        let mut buf = BytesMut::new();
        encode_msg_compressed(&mut buf, "events.user", big_payload);
        // Verify the flags byte indicates compression
        assert_eq!(buf[0], MsgType::Msg as u8);
        assert_eq!(buf[1] & FLAG_COMPRESSED, FLAG_COMPRESSED);
        // Decode should produce the original payload
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "events.user".into(),
                payload: big_payload.into(),
            }
        );
    }

    #[test]
    fn test_decode_uncompressed_msg_with_zero_flags() {
        // Verify that uncompressed messages (flags=0) still decode correctly
        let mut buf = BytesMut::new();
        encode_msg(&mut buf, "topic", "payload");
        assert_eq!(buf[1], 0); // flags byte should be 0
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "topic".into(),
                payload: "payload".into(),
            }
        );
    }

    #[test]
    fn test_decode_rejects_decompression_bomb() {
        // Compress a payload larger than MAX_FRAME_SIZE (1MB)
        let big = vec![b'A'; MAX_FRAME_SIZE + 1];
        let compressed = zstd::bulk::compress(&big, 1).unwrap();
        // Repeated bytes compress very well, so this fits in a frame
        assert!(compressed.len() < MAX_FRAME_SIZE);

        let topic = b"bomb";
        let mut buf = BytesMut::new();
        buf.put_u8(MsgType::Msg as u8);
        buf.put_u8(FLAG_COMPRESSED);
        buf.put_u16_le(topic.len() as u16);
        buf.put_u32_le(compressed.len() as u32);
        buf.put_slice(topic);
        buf.put_slice(&compressed);

        // decode_frame should reject because decompressed size exceeds MAX_FRAME_SIZE
        assert!(decode_frame(buf).is_none());
    }

    #[test]
    fn test_decode_rejects_corrupt_compressed_payload() {
        // Random bytes with FLAG_COMPRESSED set should fail gracefully
        let topic = b"test";
        let garbage = b"this is not valid zstd data at all";
        let mut buf = BytesMut::new();
        buf.put_u8(MsgType::Msg as u8);
        buf.put_u8(FLAG_COMPRESSED);
        buf.put_u16_le(topic.len() as u16);
        buf.put_u32_le(garbage.len() as u32);
        buf.put_slice(topic);
        buf.put_slice(garbage.as_slice());

        assert!(decode_frame(buf).is_none());
    }

    #[test]
    fn test_compressed_flag_ignored_on_non_msg_frames() {
        // SUB frame with FLAG_COMPRESSED set -- flag should be ignored
        let mut buf = BytesMut::new();
        encode_sub(&mut buf, "chat.*");
        // Manually set the flags byte to FLAG_COMPRESSED
        buf[1] = FLAG_COMPRESSED;
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Sub {
                topic: "chat.*".into(),
            }
        );
    }

    #[test]
    fn test_incompressible_payload_falls_back_to_uncompressed() {
        // Random-looking data above threshold that doesn't compress well
        // Use bytes that look random to zstd (high entropy)
        let mut payload = String::with_capacity(512);
        for i in 0..512u32 {
            // Mix of chars to create high-entropy data
            payload.push(char::from((33 + (i * 7 + i * i) % 94) as u8));
        }
        assert!(payload.len() >= COMPRESSION_THRESHOLD);

        let mut buf = BytesMut::new();
        encode_msg_compressed(&mut buf, "test", &payload);
        // If compression didn't help, flags should be 0 (uncompressed fallback)
        // The function should still produce a valid frame either way
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Msg {
                topic: "test".into(),
                payload: payload,
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
        encode_hello(&mut buf, "instance-abc-123", CAP_INTEREST_ROUTING);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Hello {
                instance_id: "instance-abc-123".into(),
                protocol_version: 1,
                capabilities: CAP_INTEREST_ROUTING,
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
        assert_eq!(
            decode_frame(buf),
            Some(ClusterFrame::Unknown { msg_type: 0xFF })
        );
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
    fn test_encode_decode_sub() {
        let mut buf = BytesMut::new();
        assert!(encode_sub(&mut buf, "chat.general"));
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Sub {
                topic: "chat.general".into(),
            }
        );
    }

    #[test]
    fn test_encode_decode_unsub() {
        let mut buf = BytesMut::new();
        assert!(encode_unsub(&mut buf, "chat.general"));
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Unsub {
                topic: "chat.general".into(),
            }
        );
    }

    #[test]
    fn test_encode_decode_resync_multiple() {
        let topics = vec![
            "chat.general".to_string(),
            "chat.private".to_string(),
            "events.system".to_string(),
        ];
        let mut buf = BytesMut::new();
        encode_resync(&mut buf, &topics);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(frame, ClusterFrame::Resync { topics });
    }

    #[test]
    fn test_encode_decode_resync_empty() {
        let mut buf = BytesMut::new();
        encode_resync(&mut buf, &[]);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(frame, ClusterFrame::Resync { topics: Vec::new() });
    }

    #[test]
    fn test_encode_decode_peer_announce() {
        let mut buf = BytesMut::new();
        encode_peer_announce(&mut buf, "10.0.0.5:9222");
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::PeerAnnounce {
                addr: "10.0.0.5:9222".into()
            }
        );
    }

    #[test]
    fn test_encode_decode_peer_list() {
        let addrs = vec!["10.0.0.1:9222".to_string(), "10.0.0.2:9222".to_string()];
        let mut buf = BytesMut::new();
        encode_peer_list(&mut buf, &addrs);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(frame, ClusterFrame::PeerList { addrs });
    }

    #[test]
    fn test_encode_decode_peer_list_empty() {
        let mut buf = BytesMut::new();
        encode_peer_list(&mut buf, &[]);
        let frame = decode_frame(buf).unwrap();
        assert_eq!(frame, ClusterFrame::PeerList { addrs: Vec::new() });
    }

    #[test]
    fn test_encode_decode_sub_glob_pattern() {
        let mut buf = BytesMut::new();
        assert!(encode_sub(&mut buf, "user:*:events"));
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Sub {
                topic: "user:*:events".into(),
            }
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_two_peers_exchange_hello() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Peer A connects to listener
        let client_task = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (mut read, mut write) = stream.into_split();

            // Send HELLO
            let mut hello = BytesMut::new();
            encode_hello(&mut hello, "peer-a", LOCAL_CAPABILITIES);
            let mut frame = BytesMut::new();
            write_framed(&mut frame, &hello);
            write.write_all(&frame).await.unwrap();

            // Read HELLO from peer B
            let mut len_buf = [0u8; 4];
            read.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            read.read_exact(&mut frame_buf).await.unwrap();

            decode_frame(frame_buf)
        });

        // Peer B accepts connection
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read, mut write) = stream.into_split();

        // Read HELLO from peer A
        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        let hello_a = decode_frame(frame_buf).unwrap();

        assert!(
            matches!(hello_a, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "peer-a")
        );

        // Send HELLO back
        let mut hello = BytesMut::new();
        encode_hello(&mut hello, "peer-b", LOCAL_CAPABILITIES);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &hello);
        write.write_all(&frame).await.unwrap();

        // Verify peer A received peer B's HELLO
        let hello_b = client_task.await.unwrap().unwrap();
        assert!(
            matches!(hello_b, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "peer-b")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_msg_roundtrip() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let sender = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_, mut write) = stream.into_split();

            // Send a MSG frame
            let mut msg = BytesMut::new();
            encode_msg(&mut msg, "chat.general", r#"{"text":"hello"}"#);
            let mut frame = BytesMut::new();
            write_framed(&mut frame, &msg);
            write.write_all(&frame).await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (mut read, _) = stream.into_split();

        // Read MSG frame
        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();

        let msg = decode_frame(frame_buf).unwrap();
        assert_eq!(
            msg,
            ClusterFrame::Msg {
                topic: "chat.general".into(),
                payload: r#"{"text":"hello"}"#.into(),
            }
        );

        sender.await.unwrap();
    }

    #[test]
    fn test_negotiate_version_same() {
        assert_eq!(negotiate_version(1, 1), Some(1));
    }

    #[test]
    fn test_negotiate_version_different() {
        assert_eq!(negotiate_version(2, 1), Some(1));
        assert_eq!(negotiate_version(1, 2), Some(1));
    }

    #[test]
    fn test_negotiate_version_zero_rejected() {
        assert_eq!(negotiate_version(1, 0), None);
    }

    #[test]
    fn test_negotiate_capabilities() {
        assert_eq!(negotiate_capabilities(0b11, 0b10), 0b10);
        assert_eq!(negotiate_capabilities(0b01, 0b10), 0b00);
        assert_eq!(
            negotiate_capabilities(CAP_INTEREST_ROUTING, CAP_INTEREST_ROUTING | CAP_COMPRESSION),
            CAP_INTEREST_ROUTING
        );
    }

    #[test]
    fn test_encode_decode_hello_with_capabilities() {
        let mut buf = BytesMut::new();
        encode_hello(
            &mut buf,
            "test-node",
            CAP_INTEREST_ROUTING | CAP_COMPRESSION,
        );
        let frame = decode_frame(buf).unwrap();
        assert_eq!(
            frame,
            ClusterFrame::Hello {
                instance_id: "test-node".into(),
                protocol_version: 1,
                capabilities: CAP_INTEREST_ROUTING | CAP_COMPRESSION,
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sub_unsub_resync_roundtrip() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Peer A sends SUB, UNSUB, RESYNC in a single write
        let sender = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_, mut write) = stream.into_split();

            let mut wire = BytesMut::new();

            // SUB for "chat.general"
            let mut sub = BytesMut::new();
            encode_sub(&mut sub, "chat.general");
            write_framed(&mut wire, &sub);

            // UNSUB for "chat.private"
            let mut unsub = BytesMut::new();
            encode_unsub(&mut unsub, "chat.private");
            write_framed(&mut wire, &unsub);

            // RESYNC with 3 topics
            let topics = vec!["a".into(), "b".into(), "c".into()];
            let mut resync = BytesMut::new();
            encode_resync(&mut resync, &topics);
            write_framed(&mut wire, &resync);

            write.write_all(&wire).await.unwrap();
        });

        // Peer B reads and verifies each frame
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read, _) = stream.into_split();
        let mut len_buf = [0u8; 4];

        // Read SUB
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        assert_eq!(
            decode_frame(frame_buf).unwrap(),
            ClusterFrame::Sub {
                topic: "chat.general".into()
            }
        );

        // Read UNSUB
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        assert_eq!(
            decode_frame(frame_buf).unwrap(),
            ClusterFrame::Unsub {
                topic: "chat.private".into()
            }
        );

        // Read RESYNC
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        assert_eq!(
            decode_frame(frame_buf).unwrap(),
            ClusterFrame::Resync {
                topics: vec!["a".into(), "b".into(), "c".into()]
            }
        );

        sender.await.unwrap();
    }

    #[test]
    fn test_build_cluster_tls_missing_cert_file() {
        let result = build_cluster_tls(
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
            "/nonexistent/ca.pem",
        );
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(
            err.contains("cert file"),
            "Error should mention cert file, got: {err}"
        );
    }

    /// Generate a self-signed CA and a node cert signed by it.
    /// Returns (ca_cert_pem, node_cert_pem, node_key_pem).
    fn generate_test_certs() -> (String, String, String) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
        use std::net::IpAddr;

        // Generate CA
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "WSE Test CA");
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        // Generate node cert signed by CA with IP SAN for 127.0.0.1
        let mut node_params = CertificateParams::default();
        node_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-node");
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        node_params.subject_alt_names = vec![SanType::IpAddress(ip)];
        let node_key = KeyPair::generate().unwrap();
        let node_cert = node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();

        (ca_cert.pem(), node_cert.pem(), node_key.serialize_pem())
    }

    #[test]
    fn test_build_cluster_tls_valid_certs() {
        let (ca_pem, cert_pem, key_pem) = generate_test_certs();

        let dir = std::env::temp_dir().join(format!("wse_tls_test_valid_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ca.pem"), &ca_pem).unwrap();
        std::fs::write(dir.join("cert.pem"), &cert_pem).unwrap();
        std::fs::write(dir.join("key.pem"), &key_pem).unwrap();

        let result = build_cluster_tls(
            dir.join("cert.pem").to_str().unwrap(),
            dir.join("key.pem").to_str().unwrap(),
            dir.join("ca.pem").to_str().unwrap(),
        );
        assert!(
            result.is_ok(),
            "build_cluster_tls failed: {:?}",
            result.err()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_tls_cluster_hello_roundtrip() {
        let (ca_pem, cert_pem, key_pem) = generate_test_certs();

        let dir = std::env::temp_dir().join(format!("wse_tls_roundtrip_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ca.pem"), &ca_pem).unwrap();
        std::fs::write(dir.join("cert.pem"), &cert_pem).unwrap();
        std::fs::write(dir.join("key.pem"), &key_pem).unwrap();

        let tls = build_cluster_tls(
            dir.join("cert.pem").to_str().unwrap(),
            dir.join("key.pem").to_str().unwrap(),
            dir.join("ca.pem").to_str().unwrap(),
        )
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let tls_server = tls.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls_stream = tls_server.acceptor.accept(stream).await.unwrap();

            // Read HELLO from client
            let mut len_buf = [0u8; 4];
            tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut len_buf)
                .await
                .unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut frame_buf)
                .await
                .unwrap();
            let frame = decode_frame(frame_buf);
            assert!(
                matches!(frame, Some(ClusterFrame::Hello { .. })),
                "Expected Hello frame from client"
            );

            // Send HELLO back
            let mut hello_buf = BytesMut::new();
            encode_hello(&mut hello_buf, "server-id", LOCAL_CAPABILITIES);
            let mut framed = BytesMut::new();
            write_framed(&mut framed, &hello_buf);
            tokio::io::AsyncWriteExt::write_all(&mut tls_stream, &framed)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::flush(&mut tls_stream)
                .await
                .unwrap();
        });

        let tls_client = tls;
        let client = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let server_name = ServerName::from(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            let mut tls_stream = tls_client
                .connector
                .connect(server_name, stream)
                .await
                .unwrap();

            // Send HELLO
            let mut hello_buf = BytesMut::new();
            encode_hello(&mut hello_buf, "client-id", LOCAL_CAPABILITIES);
            let mut framed = BytesMut::new();
            write_framed(&mut framed, &hello_buf);
            tokio::io::AsyncWriteExt::write_all(&mut tls_stream, &framed)
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::flush(&mut tls_stream)
                .await
                .unwrap();

            // Read HELLO response
            let mut len_buf = [0u8; 4];
            tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut len_buf)
                .await
                .unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut frame_buf)
                .await
                .unwrap();
            let frame = decode_frame(frame_buf);
            assert!(
                matches!(frame, Some(ClusterFrame::Hello { .. })),
                "Expected Hello frame from server"
            );
        });

        server.await.unwrap();
        client.await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_tls_rejects_wrong_ca() {
        // Generate two separate CAs, each with their own node cert
        let (ca_pem_a, cert_pem_a, key_pem_a) = generate_test_certs();
        let (ca_pem_b, _cert_pem_b, _key_pem_b) = generate_test_certs();

        let dir = std::env::temp_dir().join(format!("wse_tls_wrong_ca_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Server uses CA A certs
        std::fs::write(dir.join("ca_a.pem"), &ca_pem_a).unwrap();
        std::fs::write(dir.join("cert_a.pem"), &cert_pem_a).unwrap();
        std::fs::write(dir.join("key_a.pem"), &key_pem_a).unwrap();

        // Client trusts CA B (different CA)
        std::fs::write(dir.join("ca_b.pem"), &ca_pem_b).unwrap();

        let server_tls = build_cluster_tls(
            dir.join("cert_a.pem").to_str().unwrap(),
            dir.join("key_a.pem").to_str().unwrap(),
            dir.join("ca_a.pem").to_str().unwrap(),
        )
        .unwrap();

        // Client uses cert from CA A but trusts CA B -- server should reject
        let client_tls = build_cluster_tls(
            dir.join("cert_a.pem").to_str().unwrap(),
            dir.join("key_a.pem").to_str().unwrap(),
            dir.join("ca_b.pem").to_str().unwrap(),
        )
        .unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // TLS handshake should fail (client doesn't trust our CA)
            let result = server_tls.acceptor.accept(stream).await;
            // Server may or may not fail depending on who detects mismatch first
            result.is_err()
        });

        let client = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let server_name = ServerName::from(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            let result = client_tls.connector.connect(server_name, stream).await;
            // Client should fail -- server cert not signed by CA B
            result.is_err()
        });

        let server_failed = server.await.unwrap();
        let client_failed = client.await.unwrap();
        // At least one side should detect the CA mismatch
        assert!(
            server_failed || client_failed,
            "Expected TLS handshake failure with wrong CA"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Test peer exchange: Node B sends PEER_LIST containing Node C's address to Node A.
    /// Verifies that PEER_ANNOUNCE and PEER_LIST are correctly exchanged during handshake.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_peer_exchange_discovery() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b_addr = listener.local_addr().unwrap();
        let c_addr = "192.168.1.100:9001"; // Node C's address (doesn't need to exist)

        // Node A: connect to B, exchange HELLO, then read PEER_ANNOUNCE + PEER_LIST
        let node_a = tokio::spawn(async move {
            let stream = TcpStream::connect(b_addr).await.unwrap();
            let (mut read, mut write) = stream.into_split();

            // Send HELLO
            let mut hello = BytesMut::new();
            encode_hello(&mut hello, "node-a", LOCAL_CAPABILITIES);
            let mut frame = BytesMut::new();
            write_framed(&mut frame, &hello);
            write.write_all(&frame).await.unwrap();

            // Read HELLO from B
            let mut len_buf = [0u8; 4];
            read.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            read.read_exact(&mut frame_buf).await.unwrap();
            let hello_b = decode_frame(frame_buf).unwrap();
            assert!(
                matches!(hello_b, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "node-b")
            );

            // Read PEER_ANNOUNCE from B (B announces its own cluster address)
            read.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            read.read_exact(&mut frame_buf).await.unwrap();
            let announce = decode_frame(frame_buf).unwrap();

            // Read PEER_LIST from B
            read.read_exact(&mut len_buf).await.unwrap();
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            let mut frame_buf = BytesMut::zeroed(frame_len);
            read.read_exact(&mut frame_buf).await.unwrap();
            let peer_list = decode_frame(frame_buf).unwrap();

            (announce, peer_list)
        });

        // Node B: accept connection, exchange HELLO, send PEER_ANNOUNCE + PEER_LIST
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read, mut write) = stream.into_split();

        // Read HELLO from A
        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf).await.unwrap();
        let frame_len = u32::from_be_bytes(len_buf) as usize;
        let mut frame_buf = BytesMut::zeroed(frame_len);
        read.read_exact(&mut frame_buf).await.unwrap();
        let hello_a = decode_frame(frame_buf).unwrap();
        assert!(
            matches!(hello_a, ClusterFrame::Hello { ref instance_id, .. } if instance_id == "node-a")
        );

        // Send HELLO back
        let mut hello = BytesMut::new();
        encode_hello(&mut hello, "node-b", LOCAL_CAPABILITIES);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &hello);
        write.write_all(&frame).await.unwrap();

        // Send PEER_ANNOUNCE (B's own cluster address)
        let b_cluster_addr = format!("127.0.0.1:{}", b_addr.port());
        let mut announce = BytesMut::new();
        encode_peer_announce(&mut announce, &b_cluster_addr);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &announce);
        write.write_all(&frame).await.unwrap();

        // Send PEER_LIST with C's address
        let addrs = vec![c_addr.to_string(), b_cluster_addr.clone()];
        let mut list = BytesMut::new();
        encode_peer_list(&mut list, &addrs);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &list);
        write.write_all(&frame).await.unwrap();

        // Verify what A received
        let (announce_frame, peer_list_frame) = node_a.await.unwrap();

        // Check PEER_ANNOUNCE
        assert_eq!(
            announce_frame,
            ClusterFrame::PeerAnnounce {
                addr: b_cluster_addr.clone()
            }
        );

        // Check PEER_LIST contains both C's address and B's address
        match peer_list_frame {
            ClusterFrame::PeerList { addrs } => {
                assert!(
                    addrs.contains(&c_addr.to_string()),
                    "PEER_LIST should contain Node C's address"
                );
                assert!(
                    addrs.contains(&b_cluster_addr),
                    "PEER_LIST should contain Node B's address"
                );
                assert_eq!(addrs.len(), 2);
            }
            other => panic!("Expected PeerList, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_presence_update() {
        let mut buf = BytesMut::new();
        let ok = encode_presence_update(
            &mut buf,
            "room:lobby",
            "alice",
            0, // join
            r#"{"status":"online"}"#,
            1709000000000,
        );
        assert!(ok);
        let frame = decode_frame(buf).unwrap();
        match frame {
            ClusterFrame::PresenceUpdate {
                topic,
                user_id,
                action,
                data,
                updated_at,
            } => {
                assert_eq!(topic, "room:lobby");
                assert_eq!(user_id, "alice");
                assert_eq!(action, 0);
                assert_eq!(data, r#"{"status":"online"}"#);
                assert_eq!(updated_at, 1709000000000);
            }
            other => panic!("Expected PresenceUpdate, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_presence_update_leave() {
        let mut buf = BytesMut::new();
        let ok = encode_presence_update(&mut buf, "chat:main", "bob", 1, "", 1709000001000);
        assert!(ok);
        let frame = decode_frame(buf).unwrap();
        match frame {
            ClusterFrame::PresenceUpdate {
                topic,
                user_id,
                action,
                data,
                updated_at,
            } => {
                assert_eq!(topic, "chat:main");
                assert_eq!(user_id, "bob");
                assert_eq!(action, 1);
                assert_eq!(data, "");
                assert_eq!(updated_at, 1709000001000);
            }
            other => panic!("Expected PresenceUpdate, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_presence_full() {
        let json = r#"{"room:lobby":{"alice":{"data":{"status":"online"},"updated_at":123}}}"#;
        let mut buf = BytesMut::new();
        let ok = encode_presence_full(&mut buf, json);
        assert!(ok);
        let frame = decode_frame(buf).unwrap();
        match frame {
            ClusterFrame::PresenceFull { entries } => {
                assert_eq!(entries, json);
            }
            other => panic!("Expected PresenceFull, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_presence_full_empty() {
        let mut buf = BytesMut::new();
        let ok = encode_presence_full(&mut buf, "{}");
        assert!(ok);
        let frame = decode_frame(buf).unwrap();
        match frame {
            ClusterFrame::PresenceFull { entries } => {
                assert_eq!(entries, "{}");
            }
            other => panic!("Expected PresenceFull, got {:?}", other),
        }
    }

    #[test]
    fn test_cap_presence_flag() {
        // Verify CAP_PRESENCE is included in LOCAL_CAPABILITIES
        assert_ne!(LOCAL_CAPABILITIES & CAP_PRESENCE, 0);
        // Verify it's a distinct flag
        assert_eq!(CAP_PRESENCE, 1 << 2);
        assert_ne!(CAP_PRESENCE, CAP_INTEREST_ROUTING);
        assert_ne!(CAP_PRESENCE, CAP_COMPRESSION);
    }
}
