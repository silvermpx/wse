// Types will be used in subsequent tasks (peer management, server integration)
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ahash::AHashSet;
use bytes::{Buf, BufMut, BytesMut};
use dashmap::{DashMap, DashSet};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_util::sync::CancellationToken;

use super::reliability::{CircuitBreaker, ExponentialBackoff};
use super::server::ConnectionHandle;

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
// Fan-out dispatch (with AHashSet dedup -- fixes existing BroadcastLocal bug)
// ---------------------------------------------------------------------------

pub(crate) async fn cluster_dispatch(
    connections: &RwLock<HashMap<String, ConnectionHandle>>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    topic: &str,
    payload: &str,
    metrics: &ClusterMetrics,
) {
    let conns = connections.read().await;
    let mut seen = AHashSet::new();

    // Exact topic match
    if let Some(conn_ids) = topic_subscribers.get(topic) {
        for conn_id_ref in conn_ids.iter() {
            let cid = conn_id_ref.key().clone();
            if seen.insert(cid.clone())
                && let Some(handle) = conns.get(&cid)
            {
                if handle
                    .tx
                    .send(Message::Text(payload.to_owned().into()))
                    .is_ok()
                {
                    metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    // Glob pattern match
    for entry in topic_subscribers.iter() {
        let pattern = entry.key();
        if (pattern.contains('*') || pattern.contains('?'))
            && super::redis_pubsub::glob_match(pattern, topic)
        {
            for conn_id_ref in entry.value().iter() {
                let cid = conn_id_ref.key().clone();
                if seen.insert(cid.clone())
                    && let Some(handle) = conns.get(&cid)
                {
                    if handle
                        .tx
                        .send(Message::Text(payload.to_owned().into()))
                        .is_ok()
                    {
                        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                    } else {
                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-peer writer task (write coalescing: recv + try_recv drain)
// ---------------------------------------------------------------------------

async fn peer_writer(
    mut rx: mpsc::Receiver<BytesMut>,
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
// Per-peer reader task
// ---------------------------------------------------------------------------

async fn peer_reader(
    mut reader: OwnedReadHalf,
    peer_write_tx: mpsc::Sender<BytesMut>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    cancel: CancellationToken,
    metrics: Arc<ClusterMetrics>,
) {
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
                cluster_dispatch(&connections, &topic_subscribers, &topic, &payload, &metrics)
                    .await;
            }
            Some(ClusterFrame::Ping) => {
                let mut pong = BytesMut::new();
                encode_pong(&mut pong);
                let _ = peer_write_tx.send(pong).await;
            }
            Some(ClusterFrame::Pong) => {
                // Liveness confirmed (heartbeat task tracks via channel activity)
            }
            Some(ClusterFrame::Shutdown) => {
                eprintln!("[WSE-Cluster] Peer sent SHUTDOWN");
                break;
            }
            Some(ClusterFrame::Hello { .. }) => {
                // Unexpected HELLO after handshake, ignore
            }
            None => {
                eprintln!("[WSE-Cluster] Failed to decode frame, disconnecting");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Heartbeat task
// ---------------------------------------------------------------------------

async fn heartbeat_task(peer_write_tx: mpsc::Sender<BytesMut>, cancel: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let mut ping = BytesMut::new();
                encode_ping(&mut ping);
                if peer_write_tx.send(ping).await.is_err() {
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

async fn peer_connection_task(
    peer_addr: String,
    instance_id: String,
    mut data_rx: mpsc::Receiver<BytesMut>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    global_cancel: CancellationToken,
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

        // TCP_NODELAY + keepalive
        let _ = stream.set_nodelay(true);
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(10))
            .with_interval(Duration::from_secs(5));
        let sock_ref = SockRef::from(&stream);
        let _ = sock_ref.set_tcp_keepalive(&keepalive);

        // Split into owned halves for separate tasks
        let (reader, mut writer) = stream.into_split();

        // Send HELLO
        let mut hello_buf = BytesMut::new();
        encode_hello(&mut hello_buf, &instance_id);
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

        // Read HELLO response
        let mut reader_temp = reader;
        let mut len_buf = [0u8; 4];
        let hello_ok = match tokio::time::timeout(
            Duration::from_secs(5),
            reader_temp.read_exact(&mut len_buf),
        )
        .await
        {
            Ok(Ok(_)) => {
                let frame_len = u32::from_be_bytes(len_buf) as usize;
                if frame_len > 0 && frame_len <= MAX_FRAME_SIZE {
                    let mut frame_buf = BytesMut::zeroed(frame_len);
                    if reader_temp.read_exact(&mut frame_buf).await.is_ok() {
                        matches!(decode_frame(frame_buf), Some(ClusterFrame::Hello { .. }))
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        };

        if !hello_ok {
            eprintln!("[WSE-Cluster] Invalid HELLO from {peer_addr}");
            breaker.record_failure();
            let delay = backoff.next_delay();
            tokio::select! {
                () = tokio::time::sleep(delay) => continue,
                () = global_cancel.cancelled() => break,
            }
        }

        // Connected successfully
        eprintln!("[WSE-Cluster] Connected to {peer_addr}");
        breaker.record_success();
        backoff.reset();
        metrics.connected_peers.fetch_add(1, Ordering::Relaxed);

        // Spawn reader, writer, heartbeat with child cancellation token
        let peer_cancel = global_cancel.child_token();
        let (peer_write_tx, peer_write_rx) = mpsc::channel::<BytesMut>(PER_PEER_CHANNEL_CAP);

        let writer_handle = tokio::spawn(peer_writer(
            peer_write_rx,
            writer,
            peer_cancel.clone(),
            metrics.clone(),
        ));

        let reader_handle = tokio::spawn(peer_reader(
            reader_temp,
            peer_write_tx.clone(),
            connections.clone(),
            topic_subscribers.clone(),
            peer_cancel.clone(),
            metrics.clone(),
        ));

        let heartbeat_handle =
            tokio::spawn(heartbeat_task(peer_write_tx.clone(), peer_cancel.clone()));

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
                    if peer_write_tx.try_send(frame_data).is_err() {
                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                    }
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

pub(crate) async fn cluster_manager(
    peers: Vec<String>,
    instance_id: String,
    mut cmd_rx: mpsc::Receiver<ClusterCommand>,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    metrics: Arc<ClusterMetrics>,
    dlq: Arc<std::sync::Mutex<ClusterDlq>>,
) {
    let global_cancel = CancellationToken::new();

    // Per-peer channels: manager sends encoded BytesMut, peer task forwards to writer
    let mut peer_txs: Vec<(String, mpsc::Sender<BytesMut>)> = Vec::new();
    let mut peer_handles = Vec::new();

    for peer_addr in &peers {
        let (tx, rx) = mpsc::channel::<BytesMut>(PER_PEER_CHANNEL_CAP);
        peer_txs.push((peer_addr.clone(), tx));

        let handle = tokio::spawn(peer_connection_task(
            peer_addr.clone(),
            instance_id.clone(),
            rx,
            connections.clone(),
            topic_subscribers.clone(),
            metrics.clone(),
            global_cancel.clone(),
        ));
        peer_handles.push(handle);
    }

    eprintln!(
        "[WSE-Cluster] Manager started, {} peers configured",
        peers.len()
    );

    // Main loop: fan out commands to all peer channels
    loop {
        let cmd = tokio::select! {
            cmd = cmd_rx.recv() => cmd,
            () = global_cancel.cancelled() => break,
        };

        match cmd {
            Some(ClusterCommand::Publish { topic, payload }) => {
                let mut frame_data = BytesMut::new();
                encode_msg(&mut frame_data, &topic, &payload);

                for (peer_addr, tx) in &peer_txs {
                    if tx.try_send(frame_data.clone()).is_err() {
                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                        if let Ok(mut guard) = dlq.lock() {
                            guard.push(ClusterDlqEntry {
                                topic: topic.clone(),
                                payload: payload.clone(),
                                peer_addr: peer_addr.clone(),
                                error: "Peer channel full or disconnected".into(),
                            });
                        }
                    } else {
                        metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Some(ClusterCommand::Shutdown) | None => {
                eprintln!("[WSE-Cluster] Manager shutting down");
                // Send SHUTDOWN frame to all peers before cancelling
                let mut shutdown_data = BytesMut::new();
                encode_shutdown(&mut shutdown_data);
                for (_, tx) in &peer_txs {
                    let _ = tx.try_send(shutdown_data.clone());
                }
                global_cancel.cancel();
                break;
            }
        }
    }

    // Wait for all peer tasks to finish
    for handle in peer_handles {
        let _ = handle.await;
    }
    eprintln!("[WSE-Cluster] Manager stopped");
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
