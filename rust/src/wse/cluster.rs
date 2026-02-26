use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use dashmap::{DashMap, DashSet};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{RwLock, mpsc};
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
// All cluster channels are unbounded to prevent message drops.
// Memory growth only happens during benchmarks (publisher >> consumer rate).
// In production, message rates are bounded by real workload.

pub(crate) const PROTOCOL_VERSION_MIN: u16 = 1;

// Capability flags (negotiated via bitwise AND of both peers' values)
pub(crate) const CAP_INTEREST_ROUTING: u32 = 1 << 0; // Phase 2: SUB/UNSUB
#[allow(dead_code)]
pub(crate) const CAP_COMPRESSION: u32 = 1 << 1; // Phase 6: reserved for future

/// Our capabilities bitmask
pub(crate) const LOCAL_CAPABILITIES: u32 = CAP_INTEREST_ROUTING;

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
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands (Python -> cluster subsystem)
// ---------------------------------------------------------------------------

pub(crate) enum ClusterCommand {
    Publish { topic: String, payload: String },
    Sub { topic: String },
    Unsub { topic: String },
    Shutdown,
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
    #[allow(dead_code)]
    max_entries: usize,
}

impl ClusterDlq {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    #[allow(dead_code)]
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
    buf.reserve(8 + topic_bytes.len() + payload_bytes.len());
    buf.put_u8(MsgType::Msg as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(topic_bytes.len() as u16);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(topic_bytes);
    buf.put_slice(payload_bytes);
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
pub(crate) fn encode_resync(buf: &mut BytesMut, topics: &[String]) {
    let payload = topics.join("\n");
    let payload_bytes = payload.as_bytes();
    buf.reserve(8 + payload_bytes.len());
    buf.put_u8(MsgType::Resync as u8);
    buf.put_u8(0); // flags
    buf.put_u16_le(0); // no topic field
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(payload_bytes);
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
    let _flags = data.get_u8();
    let topic_len = data.get_u16_le() as usize;
    let payload_len = data.get_u32_le() as usize;

    match MsgType::from_u8(raw_type) {
        Some(MsgType::Msg) => {
            if data.remaining() < topic_len + payload_len {
                return None;
            }
            let topic = String::from_utf8(data.split_to(topic_len).to_vec()).ok()?;
            let payload = String::from_utf8(data.split_to(payload_len).to_vec()).ok()?;
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
                raw.split('\n').map(String::from).collect()
            };
            Some(ClusterFrame::Resync { topics })
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

async fn peer_writer(
    mut rx: mpsc::UnboundedReceiver<BytesMut>,
    mut writer: OwnedWriteHalf,
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
async fn peer_reader(
    reader: OwnedReadHalf,
    peer_write_tx: mpsc::UnboundedSender<BytesMut>,
    peer_addr: String,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
    last_activity: Arc<AtomicU64>,
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
                let _ = peer_write_tx.send(pong);
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
    peer_write_tx: mpsc::UnboundedSender<BytesMut>,
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
                    break;
                }
                let mut ping = BytesMut::new();
                encode_ping(&mut ping);
                if peer_write_tx.send(ping).is_err() {
                    break;
                }
            }
            () = cancel.cancelled() => break,
        }
    }
}

// ---------------------------------------------------------------------------
// Single peer connection task (connect + HELLO + spawn reader/writer/heartbeat)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn peer_connection_task(
    peer_addr: String,
    instance_id: String,
    mut data_rx: mpsc::UnboundedReceiver<BytesMut>,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    global_cancel: CancellationToken,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut breaker = CircuitBreaker::new();

    loop {
        if global_cancel.is_cancelled() {
            break;
        }

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

        // Split into owned halves for separate tasks
        let (reader, mut writer) = stream.into_split();

        // Send HELLO
        let mut hello_buf = BytesMut::new();
        encode_hello(&mut hello_buf, &instance_id, LOCAL_CAPABILITIES);
        let mut frame = BytesMut::new();
        write_framed(&mut frame, &hello_buf);
        if writer.write_all(&frame).await.is_err() {
            breaker.record_failure();
            let delay = backoff.next_delay();
            eprintln!(
                "[WSE-Cluster] Failed to send HELLO to {peer_addr}. Retry in {:.1}s",
                delay.as_secs_f64()
            );
            tokio::select! {
                () = tokio::time::sleep(delay) => continue,
                () = global_cancel.cancelled() => break,
            }
        }

        // Read HELLO response (full read under single timeout)
        let mut reader_temp = reader;
        let peer_hello = match tokio::time::timeout(Duration::from_secs(5), async {
            let mut len_buf = [0u8; 4];
            reader_temp.read_exact(&mut len_buf).await?;
            let frame_len = u32::from_be_bytes(len_buf) as usize;
            if frame_len == 0 || frame_len > MAX_FRAME_SIZE {
                return Ok::<Option<ClusterFrame>, std::io::Error>(None);
            }
            let mut frame_buf = BytesMut::zeroed(frame_len);
            reader_temp.read_exact(&mut frame_buf).await?;
            Ok(decode_frame(frame_buf))
        })
        .await
        {
            Ok(Ok(frame)) => frame,
            _ => None,
        };

        let (_peer_version, _peer_caps) = match peer_hello {
            Some(ClusterFrame::Hello {
                protocol_version,
                capabilities,
                ..
            }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
                Some(v) => (v, negotiate_capabilities(LOCAL_CAPABILITIES, capabilities)),
                None => {
                    eprintln!(
                        "[WSE-Cluster] Version mismatch with {peer_addr}: local={PROTOCOL_VERSION}, remote={protocol_version}"
                    );
                    breaker.record_failure();
                    let delay = backoff.next_delay();
                    tokio::select! {
                        () = tokio::time::sleep(delay) => continue,
                        () = global_cancel.cancelled() => break,
                    }
                }
            },
            _ => {
                eprintln!("[WSE-Cluster] Invalid HELLO from {peer_addr}");
                breaker.record_failure();
                let delay = backoff.next_delay();
                tokio::select! {
                    () = tokio::time::sleep(delay) => continue,
                    () = global_cancel.cancelled() => break,
                }
            }
        };

        // Connected successfully
        eprintln!(
            "[WSE-Cluster] Connected to {peer_addr} (v{_peer_version}, caps=0x{_peer_caps:08x})"
        );
        breaker.record_success();
        backoff.reset();
        metrics.connected_peers.fetch_add(1, Ordering::Relaxed);

        // Send RESYNC with all current local topics (before spawning writer task)
        {
            let local_topics: Vec<String> = {
                let refcounts = local_topic_refcount.lock().unwrap();
                refcounts.keys().cloned().collect()
            };
            if !local_topics.is_empty() {
                let mut resync_buf = BytesMut::new();
                encode_resync(&mut resync_buf, &local_topics);
                let mut resync_frame = BytesMut::new();
                write_framed(&mut resync_frame, &resync_buf);
                if writer.write_all(&resync_frame).await.is_err() {
                    eprintln!("[WSE-Cluster] Failed to send RESYNC to {peer_addr}");
                    metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
                    continue; // Will reconnect
                }
            }
        }

        // Spawn reader, writer, heartbeat with child cancellation token
        let peer_cancel = global_cancel.child_token();
        let (peer_write_tx, peer_write_rx) = mpsc::unbounded_channel::<BytesMut>();
        let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

        let writer_handle = tokio::spawn(peer_writer(
            peer_write_rx,
            writer,
            peer_cancel.clone(),
            metrics.clone(),
        ));

        let reader_handle = tokio::spawn(peer_reader(
            reader_temp,
            peer_write_tx.clone(),
            peer_addr.clone(),
            interest_tx.clone(),
            connections.clone(),
            topic_subscribers.clone(),
            peer_cancel.clone(),
            metrics.clone(),
            last_activity.clone(),
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
                    let _ = peer_write_tx.send(frame_data);
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
        let _ = interest_tx.send(InterestUpdate::PeerDisconnected {
            peer_addr: peer_addr.clone(),
        });
        eprintln!("[WSE-Cluster] Disconnected from {peer_addr}");

        if global_cancel.is_cancelled() {
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
    _dlq: Arc<std::sync::Mutex<ClusterDlq>>,
    local_topic_refcount: Arc<std::sync::Mutex<HashMap<String, usize>>>,
) {
    let global_cancel = CancellationToken::new();

    // Remote interest table: peer_addr -> set of topics that peer is interested in
    let mut remote_interest: HashMap<String, ahash::AHashSet<String>> = HashMap::new();

    // Per-peer channels: manager sends encoded BytesMut, peer task forwards to writer
    let mut peer_txs: Vec<(String, mpsc::UnboundedSender<BytesMut>)> = Vec::new();
    let mut peer_handles = Vec::new();

    for peer_addr in &peers {
        let (tx, rx) = mpsc::unbounded_channel::<BytesMut>();
        peer_txs.push((peer_addr.clone(), tx));

        let handle = tokio::spawn(peer_connection_task(
            peer_addr.clone(),
            instance_id.clone(),
            rx,
            interest_tx.clone(),
            connections.clone(),
            topic_subscribers.clone(),
            metrics.clone(),
            global_cancel.clone(),
            local_topic_refcount.clone(),
        ));
        peer_handles.push(handle);
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
                        let mut frame_data = BytesMut::new();
                        if !encode_msg(&mut frame_data, &topic, &payload) {
                            continue;
                        }

                        for (peer_addr, tx) in &peer_txs {
                            let interested = match remote_interest.get(peer_addr) {
                                Some(topics) => {
                                    topics.contains(&topic)
                                        || topics.iter().any(|pat| {
                                            (pat.contains('*') || pat.contains('?'))
                                                && super::redis_pubsub::glob_match(pat, &topic)
                                        })
                                }
                                None => true, // No RESYNC yet -> safe default: send to all
                            };
                            if interested {
                                if tx.send(frame_data.clone()).is_ok() {
                                    metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    Some(ClusterCommand::Sub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_sub(&mut frame_data, &topic) {
                            for (_peer_addr, tx) in &peer_txs {
                                let _ = tx.send(frame_data.clone());
                            }
                        }
                    }
                    Some(ClusterCommand::Unsub { topic }) => {
                        let mut frame_data = BytesMut::new();
                        if encode_unsub(&mut frame_data, &topic) {
                            for (_peer_addr, tx) in &peer_txs {
                                let _ = tx.send(frame_data.clone());
                            }
                        }
                    }
                    Some(ClusterCommand::Shutdown) | None => {
                        eprintln!("[WSE-Cluster] Manager shutting down");
                        let mut shutdown_data = BytesMut::new();
                        encode_shutdown(&mut shutdown_data);
                        for (peer_addr, tx) in &peer_txs {
                            if tx.send(shutdown_data.clone()).is_err() {
                                eprintln!("[WSE-Cluster] Could not send SHUTDOWN to {peer_addr}");
                            }
                        }
                        global_cancel.cancel();
                        break;
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
                    }
                    None => {
                        // Interest channel closed, continue running
                    }
                }
            }
            () = global_cancel.cancelled() => break,
        }
    }

    // Wait for all peer tasks to finish
    for handle in peer_handles {
        let _ = handle.await;
    }
    eprintln!("[WSE-Cluster] Manager stopped");
}

// ---------------------------------------------------------------------------
// Inbound cluster connection handler (accepted by main server listener)
// ---------------------------------------------------------------------------

pub(crate) async fn handle_cluster_inbound(
    stream: TcpStream,
    addr: SocketAddr,
    shared: &super::server::SharedState,
    interest_tx: mpsc::UnboundedSender<InterestUpdate>,
) {
    // Check if cluster mode is active
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

    let (mut reader, mut writer) = stream.into_split();

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

    let (_peer_version, _peer_caps) = match peer_hello {
        Some(ClusterFrame::Hello {
            protocol_version,
            capabilities,
            ..
        }) => match negotiate_version(PROTOCOL_VERSION, protocol_version) {
            Some(v) => (v, negotiate_capabilities(LOCAL_CAPABILITIES, capabilities)),
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

    // Send our HELLO response
    let mut hello_buf = BytesMut::new();
    encode_hello(&mut hello_buf, &instance_id, LOCAL_CAPABILITIES);
    let mut frame = BytesMut::new();
    write_framed(&mut frame, &hello_buf);
    if writer.write_all(&frame).await.is_err() {
        eprintln!("[WSE-Cluster] Failed to send HELLO to inbound {addr}");
        return;
    }

    // Send RESYNC with all current local topics (before spawning writer task)
    {
        let local_topics: Vec<String> = {
            let refcounts = shared.local_topic_refcount.lock().unwrap();
            refcounts.keys().cloned().collect()
        };
        if !local_topics.is_empty() {
            let mut resync_buf = BytesMut::new();
            encode_resync(&mut resync_buf, &local_topics);
            let mut resync_frame = BytesMut::new();
            write_framed(&mut resync_frame, &resync_buf);
            if writer.write_all(&resync_frame).await.is_err() {
                eprintln!("[WSE-Cluster] Failed to send RESYNC to inbound {addr}");
                return;
            }
        }
    }

    eprintln!(
        "[WSE-Cluster] Accepted inbound peer from {addr} (v{_peer_version}, caps=0x{_peer_caps:08x})"
    );
    let peer_addr_str = addr.to_string();
    let metrics = shared.cluster_metrics.clone();
    metrics.connected_peers.fetch_add(1, Ordering::Relaxed);

    let cancel = CancellationToken::new();
    let (write_tx, write_rx) = mpsc::unbounded_channel::<BytesMut>();
    let last_activity = Arc::new(AtomicU64::new(epoch_ms()));

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
        shared.connections.clone(),
        shared.topic_subscribers.clone(),
        cancel.clone(),
        metrics.clone(),
        last_activity.clone(),
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

    let _ = interest_tx.send(InterestUpdate::PeerDisconnected {
        peer_addr: peer_addr_str,
    });
    metrics.connected_peers.fetch_sub(1, Ordering::Relaxed);
    eprintln!("[WSE-Cluster] Inbound peer {addr} disconnected");
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
}
