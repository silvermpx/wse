//! The cluster mesh: server instances forward topic messages, interest and
//! presence to each other over a framed TCP (optionally mTLS) protocol.
//!
//! # Module map
//!
//! - `mod.rs`: protocol constants and limits.
//! - `protocol`: the wire format: message types, frame encoding and decoding,
//!   the length-prefix framing, version / capability negotiation.
//! - `tls`: the cluster mTLS configuration.
//! - `types`: what the cluster tasks share and exchange: `ClusterContext`,
//!   commands, registrations, interest updates, metrics, the dead-letter queue.
//! - `peer`: one peer link: writer, reader, dispatch, heartbeat, the outbound
//!   session and its reconnect loop.
//! - `manager`: the cluster manager that owns every peer link.
//! - `inbound`: inbound peer links, on the cluster port or the main port.
//!
//! The whole module is engine code: it uses no PyO3. The crate-visible items
//! are re-exported below, so `cluster::X` paths stay as they were (`MsgType` is
//! used inside `protocol` only and is not re-exported).

mod inbound;
mod manager;
mod peer;
mod protocol;
mod tls;
mod types;

pub(crate) use inbound::handle_cluster_inbound;
pub(crate) use manager::cluster_manager;
pub(crate) use protocol::{
    ClusterFrame, cluster_link_survives, decode_frame, encode_drain, encode_hello, encode_msg,
    encode_msg_compressed, encode_msg_with_recovery, encode_peer_announce, encode_peer_list,
    encode_ping, encode_pong, encode_presence_full, encode_presence_update, encode_resync,
    encode_shutdown, encode_sub, encode_unsub, negotiate_capabilities, negotiate_version,
};
pub(crate) use tls::{ClusterTlsConfig, build_cluster_tls};
pub(crate) use types::{
    ClusterCommand, ClusterContext, ClusterDlq, ClusterDlqEntry, ClusterMetrics, InterestUpdate,
    PeerInfo, PeerRegistration, PresencePeerFrame,
};

use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

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
/// How often each peer link re-sends its full local interest (RESYNC). SUB/UNSUB
/// deltas are best-effort (try_send drops under back-pressure), so a dropped one
/// would otherwise leave the peer's view of our interest permanently wrong; the
/// periodic full set heals it.
pub(crate) const CLUSTER_RESYNC_INTERVAL_SECS: u64 = 60;
pub(crate) const MAX_BATCH_BYTES: usize = 65_536;

/// Monotonic generation counter for peer sessions. Prevents stale RESYNC
/// continuation frames from a prior connection corrupting the new session.
static PEER_SESSION_GENERATION: AtomicU64 = AtomicU64::new(0);
pub(crate) const MAX_CLUSTER_PEERS: usize = 256;
// Cluster peer channels are bounded (10K messages) to prevent OOM from slow peers.
// This applies to BOTH directions: the per-peer writer channel (peer_write_tx) AND
// the inbound MSG dispatch channel (dispatch_tx). When a peer publishes faster than
// local fan-out can drain, the bounded dispatch queue fills and excess messages are
// shed (try_send) with metrics.messages_dropped incremented, rather than growing an
// unbounded backlog. Shedding keeps the single reader loop responsive to control and
// heartbeat frames, and shed messages remain recoverable via the recovery buffer.
pub(crate) const PEER_DISPATCH_QUEUE_CAP: usize = 10_000;
/// Max distinct topics tracked in one peer's remote-interest set. Bounds memory
/// so a buggy or compromised (but authenticated) peer streaming distinct SUB
/// frames cannot grow the set without limit. Generous -- a peer aggregates the
/// interest of all its local connections.
pub(crate) const MAX_REMOTE_INTEREST_PER_PEER: usize = 1_000_000;

pub(crate) const PROTOCOL_VERSION_MIN: u16 = 1;

// Capability flags (negotiated via bitwise AND of both peers' values)
pub(crate) const CAP_INTEREST_ROUTING: u32 = 1 << 0; // SUB/UNSUB interest routing
pub(crate) const CAP_COMPRESSION: u32 = 1 << 1; // zstd frame compression
pub(crate) const CAP_PRESENCE: u32 = 1 << 2; // Presence sync
pub(crate) const CAP_RECOVERY: u32 = 1 << 3; // Cross-node recovery

/// Our capabilities bitmask
pub(crate) const LOCAL_CAPABILITIES: u32 =
    CAP_INTEREST_ROUTING | CAP_COMPRESSION | CAP_PRESENCE | CAP_RECOVERY;

// Per-frame flags (bit 0 of the flags byte in every frame header)
// bit 0: compressed (zstd), bit 1: has recovery trailer, bits 2-7: reserved
const FLAG_COMPRESSED: u8 = 0x01;
const FLAG_HAS_RECOVERY: u8 = 0x02;

/// Dispatch tuple: (topic, payload, optional origin recovery position).
type DispatchItem = (String, Bytes, Option<(u32, u64)>);

// Minimum payload size for compression to be worthwhile (bytes).
// Below this threshold, zstd overhead exceeds savings.
const COMPRESSION_THRESHOLD: usize = 256;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
