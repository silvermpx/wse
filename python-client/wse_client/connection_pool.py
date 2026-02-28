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
from dataclasses import dataclass, field
from typing import Any

from .types import LoadBalancingStrategy


@dataclass
class EndpointHealth:
    """Health score and stats for a single endpoint."""

    url: str
    score: float = 100.0
    active_connections: int = 0
    total_connections: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    avg_latency_ms: float = 0.0
    latency_history: list[float] = field(default_factory=list)
    last_failure: float | None = None
    last_success: float | None = None
    last_checked: float = 0.0


class ConnectionPool:
    """Multi-endpoint connection pool with health scoring.

    Strategies:
        - ``weighted-random``:  Probability proportional to health score.
        - ``least-connections``: Pick endpoint with fewest active connections.
        - ``round-robin``:      Cycle through endpoints in order.
    """

    # Scoring constants (match TS client)
    MIN_SCORE = 10.0
    MAX_SCORE = 100.0
    HEALTH_CHECK_INTERVAL = 30.0  # seconds
    SCORE_DECAY_RATE = 0.1
    LATENCY_HISTORY_SIZE = 100

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
            url: EndpointHealth(url=url, last_checked=time.monotonic())
            for url in endpoints
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

        # Prefer sticky endpoint if set and healthy
        if self._preferred_endpoint:
            ep = self._endpoints.get(self._preferred_endpoint)
            if ep and ep.score > self.MIN_SCORE:
                return self._preferred_endpoint

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
        ep.last_success = time.monotonic()
        ep.consecutive_successes += 1
        ep.consecutive_failures = 0
        ep.total_requests += 1

        if latency_ms >= 0:
            ep.latency_history.append(latency_ms)
            if len(ep.latency_history) > self.LATENCY_HISTORY_SIZE:
                ep.latency_history.pop(0)
            ep.avg_latency_ms = (
                sum(ep.latency_history) / len(ep.latency_history)
                if ep.latency_history
                else 0.0
            )

        self._update_health_score(url)

    def record_failure(self, url: str) -> None:
        ep = self._endpoints.get(url)
        if not ep:
            return
        ep.last_failure = time.monotonic()
        ep.consecutive_failures += 1
        ep.consecutive_successes = 0
        ep.total_requests += 1
        ep.failed_requests += 1

        self._update_health_score(url, is_failure=True)

    def _update_health_score(self, url: str, *, is_failure: bool = False) -> None:
        """Compute health score matching TS ConnectionPool.updateHealthScore."""
        ep = self._endpoints.get(url)
        if not ep:
            return

        score = ep.score

        if is_failure:
            penalty = min(30.0, ep.consecutive_failures * 10.0)
            score = max(self.MIN_SCORE, score - penalty)
        else:
            base_increase = 5.0
            latency_bonus = self._calculate_latency_bonus(ep.avg_latency_ms)
            consistency_bonus = min(10.0, ep.consecutive_successes * 2.0)
            score = min(self.MAX_SCORE, score + base_increase + latency_bonus + consistency_bonus)

        # Time-based decay
        now = time.monotonic()
        time_since_check = now - ep.last_checked
        if time_since_check > self.HEALTH_CHECK_INTERVAL:
            decay_factor = 1.0 - (self.SCORE_DECAY_RATE * time_since_check / self.HEALTH_CHECK_INTERVAL)
            score = max(self.MIN_SCORE, score * decay_factor)

        ep.score = round(score)
        ep.last_checked = now

    @staticmethod
    def _calculate_latency_bonus(avg_latency: float) -> float:
        """Latency bonus matching TS calculateLatencyBonus."""
        if avg_latency == 0:
            return 0.0
        if avg_latency < 50:
            return 5.0
        if avg_latency < 100:
            return 3.0
        if avg_latency < 200:
            return 1.0
        return 0.0

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
                "score": ep.score,
                "active": ep.active_connections,
                "failures": ep.failed_requests,
                "successes": ep.total_requests - ep.failed_requests,
                "avg_latency_ms": round(ep.avg_latency_ms, 1),
                "consecutive_failures": ep.consecutive_failures,
                "consecutive_successes": ep.consecutive_successes,
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
            self._endpoints[url] = EndpointHealth(url=url, last_checked=time.monotonic())

    def set_active_endpoint(self, url: str) -> None:
        """Mark an endpoint as the preferred active one."""
        self._preferred_endpoint = url

    def reset(self) -> None:
        """Reset all endpoint scores to default."""
        now = time.monotonic()
        for ep in self._endpoints.values():
            ep.score = self.MAX_SCORE
            ep.consecutive_failures = 0
            ep.consecutive_successes = 0
            ep.total_requests = 0
            ep.failed_requests = 0
            ep.active_connections = 0
            ep.latency_history.clear()
            ep.avg_latency_ms = 0.0
            ep.last_failure = None
            ep.last_success = None
            ep.last_checked = now

    def destroy(self) -> None:
        """Clear all endpoints."""
        self._endpoints.clear()
