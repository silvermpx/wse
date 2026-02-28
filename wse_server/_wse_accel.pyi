from __future__ import annotations

from collections.abc import Callable
from typing import Any

# =============================================================================
# Compression
# =============================================================================

def rust_compress(data: bytes, level: int = 6) -> bytes:
    """Compress data using zlib at the given compression level."""
    ...

def rust_decompress(data: bytes) -> bytes:
    """Decompress zlib-compressed data."""
    ...

def rust_should_compress(data: bytes, threshold: int = 1024) -> bool:
    """Return True if data length exceeds the compression threshold."""
    ...

# =============================================================================
# Event Transform & Filter
# =============================================================================

def rust_transform_event(
    event: dict[str, Any], sequence: int, event_type_map: dict[str, Any]
) -> dict[str, Any]:
    """Transform a raw event dict into a WSE wire envelope."""
    ...

def rust_match_event(event: dict[str, Any], criteria: dict[str, Any]) -> bool:
    """Match an event against MongoDB-style filter criteria."""
    ...

# =============================================================================
# HMAC / SHA-256
# =============================================================================

def rust_hmac_sha256(key: bytes, data: bytes) -> bytes:
    """Compute HMAC-SHA256."""
    ...

def rust_sha256(data: bytes) -> str:
    """Compute SHA-256 hash, returned as a hex string."""
    ...

def rust_sign_message(payload_json: str, secret: bytes) -> str:
    """Sign a JSON payload string with HMAC-SHA256."""
    ...

# =============================================================================
# AES-GCM-256 Encryption
# =============================================================================

def rust_aes_gcm_encrypt(key: bytes, plaintext: bytes) -> bytes:
    """Encrypt with AES-GCM-256. Returns 12-byte IV + ciphertext + 16-byte tag."""
    ...

def rust_aes_gcm_decrypt(key: bytes, data: bytes) -> bytes:
    """Decrypt AES-GCM-256 ciphertext. Input: 12-byte IV + ciphertext + tag."""
    ...

# =============================================================================
# ECDH P-256 Key Exchange
# =============================================================================

def rust_ecdh_generate_keypair() -> tuple[bytes, bytes]:
    """Generate an ECDH P-256 keypair. Returns (private_key, public_key_sec1)."""
    ...

def rust_ecdh_derive_shared_secret(private_key: bytes, peer_public_key: bytes) -> bytes:
    """Derive a 32-byte AES key via ECDH + HKDF-SHA256."""
    ...

# =============================================================================
# JWT
# =============================================================================

def rust_jwt_encode(claims: dict[str, Any], secret: bytes, algorithm: str = "HS256") -> str:
    """Encode claims into a JWT (HS256)."""
    ...

def rust_jwt_decode(
    token: str,
    secret: bytes,
    algorithm: str = "HS256",
    issuer: str | None = None,
    audience: str | None = None,
) -> dict[str, Any] | None:
    """Decode and validate a JWT. Returns None on failure."""
    ...

# =============================================================================
# RustCompressionManager
# =============================================================================

class RustCompressionManager:
    """Zlib compression manager with optional msgpack support."""

    def __init__(self, threshold: int = 1024, compression_level: int = 6) -> None: ...
    def should_compress(self, data: bytes) -> bool:
        """Return True if data exceeds the compression threshold."""
        ...
    def compress(self, data: bytes, level: int | None = None) -> bytes:
        """Compress data with zlib."""
        ...
    def decompress(self, data: bytes) -> bytes:
        """Decompress zlib data."""
        ...
    def pack_msgpack(self, data: dict[str, Any]) -> bytes:
        """Serialize dict to msgpack bytes."""
        ...
    def unpack_msgpack(self, data: bytes) -> Any:
        """Deserialize msgpack bytes."""
        ...
    def pack_event(self, event: dict[str, Any], use_msgpack: bool = True) -> bytes:
        """Pack event as msgpack (with JSON fallback)."""
        ...
    def unpack_event(self, data: bytes, is_msgpack: bool = True) -> Any:
        """Unpack event from msgpack (with JSON fallback)."""
        ...
    def get_stats(self) -> dict[str, int | float]:
        """Return compression statistics.

        Keys: total_compressed, total_decompressed, compression_failures,
        decompression_failures, total_bytes_saved (int),
        compression_success_rate, decompression_success_rate,
        average_bytes_saved (float).
        """
        ...
    def reset_stats(self) -> None:
        """Reset all compression statistics to zero."""
        ...

# =============================================================================
# RustTokenBucket
# =============================================================================

class RustTokenBucket:
    """Token bucket rate limiter."""

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        initial_tokens: float | None = None,
    ) -> None: ...
    def acquire(self, tokens: float = 1.0) -> bool:
        """Try to acquire tokens. Returns True if successful."""
        ...
    @property
    def tokens(self) -> float:
        """Current token count after refill."""
        ...
    def reset(self) -> None:
        """Reset bucket to full capacity."""
        ...

# =============================================================================
# RustSequencer
# =============================================================================

class RustSequencer:
    """Monotonic sequence counter with duplicate detection."""

    def __init__(self, window_size: int) -> None: ...
    def next_seq(self) -> int:
        """Increment and return the next sequence number."""
        ...
    def get_current_seq(self) -> int:
        """Get current sequence number without incrementing."""
        ...
    def is_duplicate(self, event_id: str) -> bool:
        """Check if event ID has been seen."""
        ...
    def cleanup(self, max_age_secs: float) -> None:
        """Remove entries older than max_age_secs."""
        ...
    def seen_count(self) -> int:
        """Number of tracked event IDs."""
        ...

# =============================================================================
# RustEventSequencer
# =============================================================================

class RustEventSequencer:
    """Event sequencer with dedup and out-of-order buffering."""

    def __init__(self, window_size: int = 10000, max_out_of_order: int = 100) -> None: ...
    def next_seq(self) -> int:
        """Increment and return the next sequence number."""
        ...
    def get_current_sequence(self) -> int:
        """Get current sequence number."""
        ...
    def is_duplicate(self, event_id: str) -> bool:
        """Check if event ID has been seen."""
        ...
    def process_sequenced_event(self, topic: str, sequence: int, event: Any) -> list[Any] | None:
        """Process a sequenced event. Returns deliverable events or None if buffered."""
        ...
    def cleanup(self) -> None:
        """Clean up buffered events older than 5 minutes and trim seen_ids."""
        ...
    def get_buffer_stats(self) -> dict[str, Any]:
        """Get statistics about buffered events per topic."""
        ...
    def get_sequence_stats(self) -> dict[str, Any]:
        """Get detailed sequence statistics including per-topic info."""
        ...
    def reset_sequence(self, topic: str | None = None) -> None:
        """Reset sequence tracking for a specific topic or all topics."""
        ...

# =============================================================================
# RustPriorityQueue
# =============================================================================

class RustPriorityQueue:
    """Bounded priority queue (binary heap)."""

    def __init__(self, max_size: int) -> None: ...
    def push(self, priority: int, message: Any) -> bool:
        """Push a message. Returns False if queue is full."""
        ...
    def pop(self) -> Any | None:
        """Pop the highest-priority message."""
        ...
    def drain(self, max_count: int) -> list[Any]:
        """Drain up to max_count messages in priority order."""
        ...
    def len(self) -> int: ...
    def is_full(self) -> bool: ...
    def clear(self) -> None: ...

# =============================================================================
# RustPriorityMessageQueue
# =============================================================================

class RustPriorityMessageQueue:
    """Priority message queue with stats and batch dequeue."""

    def __init__(self, max_size: int = 1000, batch_size: int = 10) -> None: ...
    def enqueue(self, message: Any, priority: int = 5) -> bool:
        """Enqueue a message. Priorities: BACKGROUND=1, LOW=3, NORMAL=5, HIGH=8, CRITICAL=10."""
        ...
    def dequeue_batch(self) -> list[tuple[int, Any]]:
        """Dequeue a batch of (priority, message) tuples."""
        ...
    @property
    def size(self) -> int:
        """Current total message count."""
        ...
    def get_stats(self) -> dict[str, Any]:
        """Queue statistics.

        Keys: size (int), capacity (int), utilization_percent (float),
        priority_distribution (dict), priority_queue_depths (dict),
        dropped_by_priority (dict), total_dropped (int),
        backpressure (bool), oldest_message_age (float | None),
        processing_rate (float).
        """
        ...
    def clear(self) -> None: ...

# =============================================================================
# RustWSEServer
# =============================================================================

class RustWSEServer:
    """Standalone Rust WebSocket server controlled from Python.

    All transport (TCP accept, framing, JWT auth, compression, encryption,
    ping/pong) runs in Rust. Python handles application logic via drain mode
    or callbacks.
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_connections: int = 1000,
        jwt_secret: bytes | None = None,
        jwt_issuer: str | None = None,
        jwt_audience: str | None = None,
        max_inbound_queue_size: int = 131072,
        recovery_enabled: bool = False,
        recovery_buffer_size: int = 128,
        recovery_ttl: int = 300,
        recovery_max_messages: int = 500,
        recovery_memory_budget: int = 268435456,
        presence_enabled: bool = False,
        presence_max_data_size: int = 4096,
        presence_max_members: int = 0,
    ) -> None: ...

    # -- Lifecycle ------------------------------------------------------------

    def start(self) -> None:
        """Start the WebSocket server."""
        ...
    def stop(self) -> None:
        """Stop the server and close all connections."""
        ...
    def is_running(self) -> bool:
        """Return True if the server is running."""
        ...

    # -- Callbacks (alternative to drain mode) --------------------------------

    def set_callbacks(
        self,
        on_connect: Callable[[str, str], Any],
        on_message: Callable[[str, dict[str, Any]], Any],
        on_disconnect: Callable[[str], Any],
    ) -> None:
        """Set callback functions for connect, message, and disconnect events."""
        ...

    # -- Drain Mode -----------------------------------------------------------

    def enable_drain_mode(self) -> None:
        """Enable drain mode. Events are queued for batch retrieval."""
        ...
    def drain_inbound(
        self, max_count: int = 256, timeout_ms: int = 50
    ) -> list[tuple[str, str | None, Any]]:
        """Drain queued inbound events.

        Returns list of (event_type, conn_id, data) tuples.
        Event types: "connect", "auth_connect", "msg", "raw", "bin",
        "disconnect", "presence_join", "presence_leave".
        """
        ...
    def inbound_dropped_count(self) -> int:
        """Number of inbound events dropped due to queue overflow."""
        ...
    def inbound_queue_depth(self) -> int:
        """Current number of events in the inbound queue."""
        ...

    # -- Send -----------------------------------------------------------------

    def send(self, conn_id: str, data: str) -> None:
        """Send a text frame to a connection."""
        ...
    def send_bytes(self, conn_id: str, data: bytes) -> None:
        """Send a binary frame to a connection."""
        ...
    def send_event(
        self,
        conn_id: str,
        event: dict[str, Any],
        compression_threshold: int = 1024,
    ) -> int:
        """Send an event through the full outbound pipeline. Returns bytes sent."""
        ...

    # -- Broadcast ------------------------------------------------------------

    def broadcast_all(self, data: str) -> None:
        """Send text to all connected clients (no topic filtering)."""
        ...
    def broadcast_all_bytes(self, data: bytes) -> None:
        """Send binary to all connected clients."""
        ...
    def broadcast_local(self, topic: str, data: str) -> None:
        """Fan-out to local topic subscribers only (no cluster)."""
        ...
    def broadcast(self, topic: str, data: str) -> None:
        """Fan-out to topic subscribers locally and across cluster peers."""
        ...

    # -- Connection Management ------------------------------------------------

    def get_connection_count(self) -> int:
        """Number of active connections (lock-free AtomicUsize read)."""
        ...
    def get_connections(self) -> list[str]:
        """List of active connection IDs."""
        ...
    def disconnect(self, conn_id: str) -> None:
        """Force-disconnect a connection."""
        ...

    # -- Subscriptions --------------------------------------------------------

    def subscribe_connection(
        self,
        conn_id: str,
        topics: list[str],
        presence_data: dict[str, Any] | None = None,
    ) -> None:
        """Subscribe a connection to topics. Optionally set initial presence data."""
        ...
    def subscribe_with_recovery(
        self,
        conn_id: str,
        topics: list[str],
        recover: bool = False,
        epoch: str | None = None,
        offset: int | None = None,
    ) -> dict[str, Any]:
        """Subscribe with message recovery. Returns per-topic recovery status."""
        ...
    def unsubscribe_connection(self, conn_id: str, topics: list[str] | None = None) -> None:
        """Unsubscribe from topics. If topics is None, unsubscribe from all."""
        ...
    def get_topic_subscriber_count(self, topic: str) -> int:
        """Number of connections subscribed to a topic."""
        ...

    # -- Presence -------------------------------------------------------------

    def presence(self, topic: str) -> dict[str, dict[str, Any]]:
        """Get presence data for a topic. Returns {user_id: {data, connections}}."""
        ...
    def presence_stats(self, topic: str) -> dict[str, int]:
        """Get presence counts for a topic. Returns {num_users, num_connections}."""
        ...
    def update_presence(self, conn_id: str, data: dict[str, Any]) -> None:
        """Update presence metadata for a connection across all its topics."""
        ...

    # -- Cluster --------------------------------------------------------------

    def connect_cluster(
        self,
        peers: list[str],
        tls_cert: str | None = None,
        tls_key: str | None = None,
        tls_ca: str | None = None,
        cluster_port: int | None = None,
        seeds: list[str] | None = None,
        cluster_addr: str | None = None,
    ) -> None:
        """Connect to cluster peers via TCP mesh protocol."""
        ...
    def cluster_connected(self) -> bool:
        """Return True if at least one cluster peer is connected."""
        ...
    def cluster_peers_count(self) -> int:
        """Number of connected cluster peers."""
        ...

    # -- Health / Monitoring --------------------------------------------------

    def health_snapshot(self) -> dict[str, Any]:
        """Full server health metrics: connections, queue, cluster, recovery, presence, uptime."""
        ...
    def get_cluster_dlq_entries(self) -> list[dict[str, Any]]:
        """Drain and return dead-letter-queue entries from failed cluster sends."""
        ...
