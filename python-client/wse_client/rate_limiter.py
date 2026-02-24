# =============================================================================
# WSE Python Client -- Token Bucket Rate Limiter
# =============================================================================

from __future__ import annotations

import time

from .constants import (
    RATE_LIMIT_CAPACITY,
    RATE_LIMIT_REFILL_INTERVAL,
    RATE_LIMIT_REFILL_RATE,
)


class TokenBucketRateLimiter:
    """Token bucket rate limiter for outbound messages.

    Args:
        capacity: Max tokens in the bucket (default 1000).
        refill_rate: Tokens added per refill interval (default 100).
        refill_interval: Seconds between refills (default 1.0).
    """

    def __init__(
        self,
        capacity: int = RATE_LIMIT_CAPACITY,
        refill_rate: int = RATE_LIMIT_REFILL_RATE,
        refill_interval: float = RATE_LIMIT_REFILL_INTERVAL,
    ) -> None:
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_rate = refill_rate
        self._refill_interval = refill_interval
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed >= self._refill_interval:
            intervals = elapsed / self._refill_interval
            self._tokens = min(
                self._capacity,
                self._tokens + intervals * self._refill_rate,
            )
            self._last_refill = now

    def can_send(self) -> bool:
        self._refill()
        return self._tokens >= 1.0

    def try_consume(self, tokens: int = 1) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def get_retry_after(self) -> float:
        """Seconds until at least 1 token is available."""
        self._refill()
        if self._tokens >= 1.0:
            return 0.0
        deficit = 1.0 - self._tokens
        return (deficit / self._refill_rate) * self._refill_interval

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def utilization(self) -> float:
        self._refill()
        return 1.0 - (self._tokens / self._capacity)

    def get_stats(self) -> dict:
        self._refill()
        return {
            "available_tokens": round(self._tokens, 1),
            "capacity": self._capacity,
            "utilization": round(self.utilization, 3),
            "refill_rate": self._refill_rate,
        }

    def reset(self) -> None:
        self._tokens = float(self._capacity)
        self._last_refill = time.monotonic()
