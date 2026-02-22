# =============================================================================
# WSE Python Client -- Network Monitor
# =============================================================================
#
# Latency / jitter / packet loss analysis -> connection quality.
# Port of client/services/NetworkMonitor.ts.
# =============================================================================

from __future__ import annotations

import time
from collections import deque

from .constants import (
    QUALITY_EXCELLENT_JITTER,
    QUALITY_EXCELLENT_LATENCY,
    QUALITY_EXCELLENT_LOSS,
    QUALITY_FAIR_JITTER,
    QUALITY_FAIR_LATENCY,
    QUALITY_FAIR_LOSS,
    QUALITY_GOOD_JITTER,
    QUALITY_GOOD_LATENCY,
    QUALITY_GOOD_LOSS,
)
from .types import ConnectionQuality, NetworkDiagnostics


class NetworkMonitor:
    """Track network quality from heartbeat round-trip times."""

    def __init__(self, window_size: int = 50) -> None:
        self._window_size = window_size
        self._latencies: deque[float] = deque(maxlen=window_size)
        self._pings_sent = 0
        self._pongs_received = 0
        self._last_analysis: float | None = None

    def record_ping(self) -> None:
        self._pings_sent += 1

    def record_pong(self, latency_ms: float) -> None:
        self._pongs_received += 1
        self._latencies.append(latency_ms)

    @property
    def quality(self) -> ConnectionQuality:
        diag = self.analyze()
        return diag.quality

    def analyze(self) -> NetworkDiagnostics:
        """Analyze current network conditions."""
        now = time.monotonic()

        if len(self._latencies) < 2:
            return NetworkDiagnostics(
                quality=ConnectionQuality.UNKNOWN,
                stability=1.0,
                jitter=0.0,
                packet_loss=0.0,
                round_trip_time=self._latencies[-1] if self._latencies else 0.0,
                last_analysis=now,
            )

        lats = list(self._latencies)
        avg_lat = sum(lats) / len(lats)

        # Jitter: average absolute difference between consecutive samples
        diffs = [abs(lats[i] - lats[i - 1]) for i in range(1, len(lats))]
        jitter = sum(diffs) / len(diffs) if diffs else 0.0

        # Packet loss
        loss = 0.0
        if self._pings_sent > 0:
            loss = max(
                0.0,
                (self._pings_sent - self._pongs_received) / self._pings_sent * 100,
            )

        # Quality classification
        quality = self._classify(avg_lat, jitter, loss)

        # Stability: 1.0 = perfect, 0.0 = terrible
        stability = max(0.0, 1.0 - (jitter / 200.0) - (loss / 20.0))

        self._last_analysis = now

        suggestions: list[str] = []
        if quality == ConnectionQuality.POOR:
            suggestions.append("High latency detected; consider reconnecting")
        if loss > 5.0:
            suggestions.append("Significant packet loss detected")

        return NetworkDiagnostics(
            quality=quality,
            stability=round(stability, 3),
            jitter=round(jitter, 2),
            packet_loss=round(loss, 2),
            round_trip_time=round(avg_lat, 2),
            suggestions=suggestions,
            last_analysis=now,
        )

    def _classify(
        self, latency: float, jitter: float, loss: float
    ) -> ConnectionQuality:
        if (
            latency <= QUALITY_EXCELLENT_LATENCY
            and jitter <= QUALITY_EXCELLENT_JITTER
            and loss <= QUALITY_EXCELLENT_LOSS
        ):
            return ConnectionQuality.EXCELLENT

        if (
            latency <= QUALITY_GOOD_LATENCY
            and jitter <= QUALITY_GOOD_JITTER
            and loss <= QUALITY_GOOD_LOSS
        ):
            return ConnectionQuality.GOOD

        if (
            latency <= QUALITY_FAIR_LATENCY
            and jitter <= QUALITY_FAIR_JITTER
            and loss <= QUALITY_FAIR_LOSS
        ):
            return ConnectionQuality.FAIR

        return ConnectionQuality.POOR

    def reset(self) -> None:
        self._latencies.clear()
        self._pings_sent = 0
        self._pongs_received = 0
        self._last_analysis = None
