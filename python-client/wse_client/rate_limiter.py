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

    Only :meth:`try_consume` changes the bucket; the read paths
    (:meth:`can_send`, :attr:`available_tokens`, :meth:`get_stats`, ...)
    compute the refilled value without writing it, so reading stats from
    another thread never races the sending thread.

    Args:
        capacity: Max tokens in the bucket (default 1000).
        refill_rate: Tokens added per refill interval (default 100).
        refill_interval: Seconds between refills (default 1.0).
    """

    def __init__(
        self,
        capacity: float = RATE_LIMIT_CAPACITY,
        refill_rate: float = RATE_LIMIT_REFILL_RATE,
        refill_interval: float = RATE_LIMIT_REFILL_INTERVAL,
    ) -> None:
        self._capacity = capacity
        self._tokens = float(capacity)
        self._refill_rate = refill_rate
        self._refill_interval = refill_interval
        self._last_refill = time.monotonic()

    def _peek(self) -> tuple[float, float]:
        """Return ``(tokens, last_refill)`` as a refill now would set them."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed < self._refill_interval:
            return self._tokens, self._last_refill
        intervals = int(elapsed / self._refill_interval)
        tokens = min(self._capacity, self._tokens + intervals * self._refill_rate)
        # Snap back to preserve fractional interval remainder
        return tokens, now - (elapsed % self._refill_interval)

    def _refill(self) -> None:
        self._tokens, self._last_refill = self._peek()

    def can_send(self) -> bool:
        return self._peek()[0] >= 1.0

    def try_consume(self, tokens: int = 1) -> bool:
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def get_retry_after(self) -> float:
        """Seconds until at least 1 token is available."""
        available = self._peek()[0]
        if available >= 1.0:
            return 0.0
        deficit = 1.0 - available
        return (deficit / self._refill_rate) * self._refill_interval

    @property
    def available_tokens(self) -> float:
        return self._peek()[0]

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def refill_rate(self) -> float:
        return self._refill_rate

    @property
    def utilization(self) -> float:
        return 1.0 - (self._peek()[0] / self._capacity)

    def get_stats(self) -> dict:
        available = self._peek()[0]
        return {
            "available_tokens": round(available, 1),
            "capacity": self._capacity,
            "utilization": round(1.0 - available / self._capacity, 3),
            "refill_rate": self._refill_rate,
        }

    def reset(self) -> None:
        self._tokens = float(self._capacity)
        self._last_refill = time.monotonic()
