# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger("wse.metrics")


# =============================================================================
# Metric Stubs (No-op for now, can add Prometheus later)
# =============================================================================

class MetricStub:
    """Stub metric that does nothing"""
    def labels(self, *args, **kwargs):
        return self
    def inc(self, value=1):
        pass
    def dec(self, value=1):
        pass
    def set(self, value):
        pass
    def observe(self, value):
        pass


# Prometheus metric stubs
ws_connections_active = MetricStub()
ws_connections_total = MetricStub()
ws_disconnections_total = MetricStub()
ws_messages_sent_total = MetricStub()
ws_messages_received_total = MetricStub()
ws_message_send_latency_seconds = MetricStub()
ws_bytes_sent_total = MetricStub()
ws_bytes_received_total = MetricStub()
ws_queue_size = MetricStub()
ws_queue_dropped_total = MetricStub()
ws_queue_backpressure = MetricStub()

# PubSub metrics (used by pubsub_bus.py)
pubsub_published_total = MetricStub()
pubsub_received_total = MetricStub()
pubsub_publish_latency_seconds = MetricStub()
pubsub_handler_errors_total = MetricStub()
pubsub_listener_errors_total = MetricStub()
dlq_messages_total = MetricStub()
dlq_size = MetricStub()
dlq_replayed_total = MetricStub()


# =============================================================================
# Connection Metrics
# =============================================================================

@dataclass
class ConnectionMetrics:
    """Metrics for a single WebSocket connection"""

    connection_id: str = ""
    connected_at: datetime | None = None
    connected_since: datetime | None = None
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    bytes_saved_by_compression: int = 0
    errors: int = 0
    connection_attempts: int = 0
    successful_connections: int = 0
    messages_dropped: int = 0
    protocol_errors: int = 0
    compression_hits: int = 0
    last_activity: datetime | None = None
    last_message_sent: datetime | None = None
    last_message_received: datetime | None = None
    last_error_message: str = ""
    compression_ratio: float = 1.0
    message_rate: float = 0.0
    bandwidth: float = 0.0
    latency_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    last_health_check: datetime | None = None

    @property
    def avg_latency(self) -> float:
        if not self.latency_samples:
            return 0.0
        return sum(self.latency_samples) / len(self.latency_samples)

    def record_message_sent(self, size: int = 0):
        self.messages_sent += 1
        self.bytes_sent += size
        self.last_activity = datetime.now(UTC)
        self.last_message_sent = datetime.now(UTC)

    def record_message_received(self, size: int = 0):
        self.messages_received += 1
        self.bytes_received += size
        self.last_activity = datetime.now(UTC)
        self.last_message_received = datetime.now(UTC)

    def record_error(self):
        self.errors += 1

    def record_latency(self, latency_ms: float):
        """Record a latency sample (deque auto-evicts oldest when maxlen reached)"""
        self.latency_samples.append(latency_ms)

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for logging/serialization"""
        return {
            "connection_id": self.connection_id,
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "connected_since": self.connected_since.isoformat() if self.connected_since else None,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "bytes_sent": self.bytes_sent,
            "bytes_received": self.bytes_received,
            "bytes_saved_by_compression": self.bytes_saved_by_compression,
            "errors": self.errors,
            "connection_attempts": self.connection_attempts,
            "successful_connections": self.successful_connections,
            "messages_dropped": self.messages_dropped,
            "protocol_errors": self.protocol_errors,
            "compression_hits": self.compression_hits,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "last_message_sent": self.last_message_sent.isoformat() if self.last_message_sent else None,
            "last_message_received": self.last_message_received.isoformat() if self.last_message_received else None,
            "compression_ratio": self.compression_ratio,
            "message_rate": self.message_rate,
            "bandwidth": self.bandwidth,
        }


# =============================================================================
# Network Quality Analyzer
# =============================================================================

@dataclass
class NetworkQualityAnalyzer:
    """Analyzes network quality for adaptive behavior"""

    latency_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    bytes_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    max_samples: int = 100
    total_bytes_sent: int = 0
    total_bytes_received: int = 0

    def record_latency(self, latency_ms: float):
        """Record a latency sample (deque auto-evicts oldest when maxlen reached)"""
        self.latency_samples.append(latency_ms)

    def record_bytes(self, bytes_count: int, direction: str = "sent"):
        """Record bytes sent/received for bandwidth analysis"""
        if direction == "sent":
            self.total_bytes_sent += bytes_count
        else:
            self.total_bytes_received += bytes_count
        self.bytes_samples.append(bytes_count)

    def record_packet_sent(self):
        """Record a packet was sent (for tracking purposes)"""
        pass

    def record_packet_received(self):
        """Record a packet was received (for tracking purposes)"""
        pass

    @property
    def average_latency(self) -> float:
        if not self.latency_samples:
            return 0.0
        return sum(self.latency_samples) / len(self.latency_samples)

    @property
    def quality_score(self) -> float:
        """Returns 0-1 quality score based on latency"""
        avg = self.average_latency
        if avg < 50:
            return 1.0
        elif avg < 100:
            return 0.8
        elif avg < 200:
            return 0.6
        elif avg < 500:
            return 0.4
        else:
            return 0.2

    def analyze(self) -> dict[str, Any]:
        """Analyze network quality and return diagnostics"""
        score = self.quality_score
        avg_latency = self.average_latency

        # Determine quality label (expected by WSE handlers)
        if score >= 0.8:
            quality = "excellent"
        elif score >= 0.6:
            quality = "good"
        elif score >= 0.4:
            quality = "fair"
        else:
            quality = "poor"

        # Calculate jitter (variance in latency)
        jitter = 0.0
        if len(self.latency_samples) > 1:
            mean = avg_latency
            variance = sum((x - mean) ** 2 for x in self.latency_samples) / len(self.latency_samples)
            jitter = variance ** 0.5

        # Generate suggestions based on quality
        suggestions = []
        if quality == "poor":
            suggestions = ["Check network connection", "Reduce message frequency", "Enable compression"]
        elif quality == "fair":
            suggestions = ["Consider enabling compression", "Monitor connection stability"]

        return {
            "quality": quality,
            "quality_score": score,
            "average_latency_ms": avg_latency,
            "jitter": jitter,
            "packet_loss": 0.0,  # Not tracked currently
            "latency_samples": len(self.latency_samples),
            "bytes_sent": self.total_bytes_sent,
            "bytes_received": self.total_bytes_received,
            "suggestions": suggestions,
            "status": "healthy" if score >= 0.6 else "degraded",
        }
