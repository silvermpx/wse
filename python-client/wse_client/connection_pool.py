# =============================================================================
# WSE Python Client -- Connection Pool
# =============================================================================
#
# Multi-endpoint management with health scoring and load balancing.
# Port of client/services/ConnectionPool.ts.
# =============================================================================

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any

from .types import LoadBalancingStrategy


@dataclass
class EndpointHealth:
    """Health score and stats for a single endpoint."""

    url: str
    score: float = 100.0
    active_connections: int = 0
    total_connections: int = 0
    failures: int = 0
    successes: int = 0
    last_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    last_failure: float | None = None
    last_success: float | None = None


class ConnectionPool:
    """Multi-endpoint connection pool with health scoring.

    Strategies:
        - ``weighted-random``:  Probability proportional to health score.
        - ``least-connections``: Pick endpoint with fewest active connections.
        - ``round-robin``:      Cycle through endpoints in order.
    """

    # Scoring constants (match TS client)
    BASE_SCORE = 100.0
    FAILURE_PENALTY = 20.0
    SUCCESS_BONUS = 5.0
    LATENCY_PENALTY_FACTOR = 0.1  # per ms over 100
    RECOVERY_RATE = 2.0  # points per minute
    MIN_SCORE = 0.0
    MAX_SCORE = 100.0

    def __init__(
        self,
        endpoints: list[str],
        strategy: LoadBalancingStrategy = LoadBalancingStrategy.WEIGHTED_RANDOM,
    ) -> None:
        """Initialize the connection pool.

        Args:
            endpoints: WebSocket URLs to load-balance across.
            strategy: Load balancing strategy (default: weighted-random).
        """
        self._strategy = strategy
        self._endpoints: dict[str, EndpointHealth] = {
            url: EndpointHealth(url=url) for url in endpoints
        }
        self._rr_index = 0
        self._preferred_endpoint: str | None = None

    @property
    def endpoints(self) -> list[str]:
        return list(self._endpoints.keys())

    def select_endpoint(self) -> str:
        """Pick the best endpoint using the configured strategy."""
        if not self._endpoints:
            raise ValueError("No endpoints configured")

        self._apply_recovery()

        if self._strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._select_least_connections()
        elif self._strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._select_round_robin()
        else:
            return self._select_weighted_random()

    def record_success(self, url: str, latency_ms: float = 0.0) -> None:
        ep = self._endpoints.get(url)
        if not ep:
            return
        ep.successes += 1
        ep.last_success = time.monotonic()
        ep.last_latency_ms = latency_ms
        # EMA for average latency
        if ep.avg_latency_ms == 0.0:
            ep.avg_latency_ms = latency_ms
        else:
            ep.avg_latency_ms = ep.avg_latency_ms * 0.8 + latency_ms * 0.2

        ep.score = min(self.MAX_SCORE, ep.score + self.SUCCESS_BONUS)

        # Latency penalty for slow responses
        if latency_ms > 100:
            penalty = (latency_ms - 100) * self.LATENCY_PENALTY_FACTOR
            ep.score = max(self.MIN_SCORE, ep.score - penalty)

    def record_failure(self, url: str) -> None:
        ep = self._endpoints.get(url)
        if not ep:
            return
        ep.failures += 1
        ep.last_failure = time.monotonic()
        ep.score = max(self.MIN_SCORE, ep.score - self.FAILURE_PENALTY)

    def add_connection(self, url: str) -> None:
        ep = self._endpoints.get(url)
        if ep:
            ep.active_connections += 1
            ep.total_connections += 1

    def remove_connection(self, url: str) -> None:
        ep = self._endpoints.get(url)
        if ep and ep.active_connections > 0:
            ep.active_connections -= 1

    def get_stats(self) -> list[dict[str, Any]]:
        return [
            {
                "url": ep.url,
                "score": round(ep.score, 1),
                "active": ep.active_connections,
                "failures": ep.failures,
                "successes": ep.successes,
                "avg_latency_ms": round(ep.avg_latency_ms, 1),
            }
            for ep in self._endpoints.values()
        ]

    # -- Strategy implementations ---------------------------------------------

    def _select_weighted_random(self) -> str:
        eps = list(self._endpoints.values())
        total = sum(max(ep.score, 1.0) for ep in eps)
        r = random.random() * total
        cumulative = 0.0
        for ep in eps:
            cumulative += max(ep.score, 1.0)
            if r <= cumulative:
                return ep.url
        return eps[-1].url

    def _select_least_connections(self) -> str:
        return min(self._endpoints.values(), key=lambda ep: ep.active_connections).url

    def _select_round_robin(self) -> str:
        urls = list(self._endpoints.keys())
        url = urls[self._rr_index % len(urls)]
        self._rr_index += 1
        return url

    def has_endpoint(self, url: str) -> bool:
        return url in self._endpoints

    def add_endpoint(self, url: str) -> None:
        if url not in self._endpoints:
            self._endpoints[url] = EndpointHealth(url=url)

    def set_active_endpoint(self, url: str) -> None:
        """Mark an endpoint as the preferred active one."""
        self._preferred_endpoint = url

    def reset(self) -> None:
        """Reset all endpoint scores to default."""
        for ep in self._endpoints.values():
            ep.score = self.BASE_SCORE
            ep.failures = 0
            ep.successes = 0
            ep.active_connections = 0
            ep.last_failure = None
            ep.last_success = None

    def destroy(self) -> None:
        """Clear all endpoints."""
        self._endpoints.clear()

    def _apply_recovery(self) -> None:
        """Gradually recover scores over time."""
        now = time.monotonic()
        for ep in self._endpoints.values():
            if ep.score < self.MAX_SCORE and ep.last_failure:
                elapsed_min = (now - ep.last_failure) / 60.0
                recovery = elapsed_min * self.RECOVERY_RATE
                ep.score = min(self.MAX_SCORE, ep.score + recovery)
