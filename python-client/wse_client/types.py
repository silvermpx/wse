# =============================================================================
# WSE Python Client -- Type Definitions
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any


class ConnectionState(str, Enum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    PENDING = "pending"
    DEGRADED = "degraded"


class ConnectionQuality(str, Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNKNOWN = "unknown"


class MessagePriority(IntEnum):
    CRITICAL = 10
    HIGH = 8
    NORMAL = 5
    LOW = 3
    BACKGROUND = 1


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class LoadBalancingStrategy(str, Enum):
    WEIGHTED_RANDOM = "weighted-random"
    LEAST_CONNECTIONS = "least-connections"
    ROUND_ROBIN = "round-robin"


class MessageCategory(str, Enum):
    SYSTEM = "WSE"
    SNAPSHOT = "S"
    UPDATE = "U"


class ReconnectMode(str, Enum):
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"
    ADAPTIVE = "adaptive"


@dataclass(frozen=True, slots=True)
class WSEEvent:
    """An event received from the WSE server."""

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
    quality: ConnectionQuality
    stability: float
    jitter: float
    packet_loss: float
    round_trip_time: float
    suggestions: list[str] = field(default_factory=list)
    last_analysis: float | None = None


@dataclass
class ReconnectConfig:
    mode: ReconnectMode = ReconnectMode.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 30.0
    max_attempts: int = -1  # -1 = infinite (matches TS client)
    factor: float = 1.5
    jitter: bool = True
