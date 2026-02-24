# =============================================================================
# WSE Python Client -- Type Definitions
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any


class ConnectionState(str, Enum):
    """WebSocket connection lifecycle state.

    Typical flow: PENDING -> CONNECTING -> CONNECTED -> DISCONNECTED.
    RECONNECTING and DEGRADED are transient, ERROR is terminal.
    """

    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    PENDING = "pending"
    DEGRADED = "degraded"


class ConnectionQuality(str, Enum):
    """Network quality derived from latency, jitter, and packet loss.

    Thresholds: EXCELLENT (<=50ms, <=25ms jitter, <=0.1% loss),
    GOOD (<=150ms, <=50ms, <=1%), FAIR (<=300ms, <=100ms, <=3%),
    POOR (anything worse).
    """

    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNKNOWN = "unknown"


class MessagePriority(IntEnum):
    """Message priority for the server's 5-level queue.

    Higher values are sent first. Under backpressure the server
    drops lower-priority messages first.
    """

    CRITICAL = 10
    HIGH = 8
    NORMAL = 5
    LOW = 3
    BACKGROUND = 1


class CircuitBreakerState(str, Enum):
    """Circuit breaker state machine.

    CLOSED -- normal operation, failures counted.
    OPEN -- all calls rejected until reset timeout.
    HALF_OPEN -- one probe call allowed to test recovery.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class LoadBalancingStrategy(str, Enum):
    """Strategy for selecting among multiple server endpoints."""

    WEIGHTED_RANDOM = "weighted-random"
    LEAST_CONNECTIONS = "least-connections"
    ROUND_ROBIN = "round-robin"


class MessageCategory(str, Enum):
    """Wire protocol message category prefix."""

    SYSTEM = "WSE"
    SNAPSHOT = "S"
    UPDATE = "U"


class ReconnectMode(str, Enum):
    """Backoff strategy for auto-reconnection after a connection drop."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"
    ADAPTIVE = "adaptive"


@dataclass(frozen=True, slots=True)
class WSEEvent:
    """An event received from the WSE server.

    Attributes:
        type: Event type string, e.g. ``"notifications"``, ``"price_update"``.
        payload: Event data as a dict.
        id: Unique message UUID for deduplication.
        sequence: Monotonic sequence number for ordering.
        timestamp: ISO-8601 timestamp from the server.
        version: Wire protocol version (default 1).
        category: ``"WSE"`` (system), ``"S"`` (snapshot), ``"U"`` (update).
        priority: Numeric priority (see :class:`MessagePriority`).
        correlation_id: Links a request to its response.
        signature: HMAC-SHA256 hex signature when message signing is on.
    """

    type: str
    payload: dict[str, Any]
    id: str | None = None
    sequence: int | None = None
    timestamp: str | None = None
    version: int = 1
    category: str | None = None
    priority: int | None = None
    correlation_id: str | None = None
    signature: str | None = None


@dataclass
class ConnectionStats:
    """Counters and metrics for a single client connection."""

    messages_received: int = 0
    messages_sent: int = 0
    bytes_received: int = 0
    bytes_sent: int = 0
    reconnect_count: int = 0
    connected_since: float | None = None
    last_latency_ms: float | None = None
    avg_latency_ms: float | None = None
    min_latency_ms: float | None = None
    max_latency_ms: float | None = None
    connection_quality: ConnectionQuality = ConnectionQuality.UNKNOWN


@dataclass
class NetworkDiagnostics:
    """Snapshot of network quality analysis from :class:`NetworkMonitor`.

    Attributes:
        quality: Classified connection quality.
        stability: 1.0 = perfect, 0.0 = unusable.
        jitter: Average delta between consecutive RTT samples (ms).
        packet_loss: Estimated loss percentage (0--100).
        round_trip_time: Average RTT in milliseconds.
        suggestions: Hints for degraded connections.
        last_analysis: ``time.monotonic()`` of the last analysis run.
    """

    quality: ConnectionQuality
    stability: float
    jitter: float
    packet_loss: float
    round_trip_time: float
    suggestions: list[str] = field(default_factory=list)
    last_analysis: float | None = None


@dataclass
class ReconnectConfig:
    """Configuration for automatic reconnection.

    Attributes:
        mode: Backoff strategy (default: exponential).
        base_delay: Initial delay in seconds before first retry.
        max_delay: Maximum delay cap in seconds.
        max_attempts: Max retries, ``-1`` for infinite.
        factor: Multiplier per attempt for exponential backoff.
        jitter: Randomize delays to avoid thundering herd.
    """

    mode: ReconnectMode = ReconnectMode.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 30.0
    max_attempts: int = -1  # -1 = infinite (matches TS client)
    factor: float = 1.5
    jitter: bool = True
