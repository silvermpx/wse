# =============================================================================
# WSE — WebSocket Engine
# Rust-accelerated rate limiter (delegates TokenBucket to RustTokenBucket)
# =============================================================================

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from wse_server._accel import RustTokenBucket

from .config import RateLimiterConfig

logger = logging.getLogger("wse.rate_limiter")


# --------------------------------------------------------------------- #
# Algorithm base
# --------------------------------------------------------------------- #


class RateLimiterAlgorithm(ABC):
    """Base class for rate limiting algorithms."""

    @abstractmethod
    async def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire *tokens*.  Return True on success."""

    @abstractmethod
    async def wait_and_acquire(self, tokens: int = 1) -> None:
        """Block until *tokens* are available, then acquire them."""


# --------------------------------------------------------------------- #
# Token bucket (Rust-accelerated)
# --------------------------------------------------------------------- #


class TokenBucket(RateLimiterAlgorithm):
    """Token bucket algorithm -- allows bursts up to *capacity*.

    Delegates to RustTokenBucket for the acquire logic.
    """

    def __init__(self, capacity: int, refill_rate: float, initial_tokens: int | None = None):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._rust = RustTokenBucket(
            capacity=float(capacity),
            refill_rate=refill_rate,
            initial_tokens=float(initial_tokens) if initial_tokens is not None else None,
        )
        self._lock = asyncio.Lock()

    @property
    def tokens(self) -> float:
        return self._rust.tokens

    async def acquire(self, tokens: int = 1) -> bool:
        async with self._lock:
            return self._rust.acquire(float(tokens))

    async def wait_and_acquire(self, tokens: int = 1) -> None:
        while not await self.acquire(tokens):
            async with self._lock:
                tokens_needed = tokens - self._rust.tokens
                wait_time = tokens_needed / self.refill_rate
            await asyncio.sleep(wait_time)


# --------------------------------------------------------------------- #
# Sliding window (stays in Python -- not a hot path)
# --------------------------------------------------------------------- #


class SlidingWindow(RateLimiterAlgorithm):
    """Sliding window rate limiter."""

    def __init__(self, capacity: int, window_seconds: int):
        self.capacity = capacity
        self.window_seconds = window_seconds
        self.requests: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        async with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            # Evict expired entries
            self.requests = [t for t in self.requests if t > cutoff]

            if len(self.requests) + tokens <= self.capacity:
                for _ in range(tokens):
                    self.requests.append(now)
                return True
            return False

    async def wait_and_acquire(self, tokens: int = 1) -> None:
        while not await self.acquire(tokens):
            await asyncio.sleep(0.1)


# --------------------------------------------------------------------- #
# Main facade
# --------------------------------------------------------------------- #


class RateLimiter:
    """Rate limiter with selectable algorithm (token bucket or sliding window)."""

    def __init__(
        self,
        name: str = "default",
        config: RateLimiterConfig | None = None,
        *,
        # Legacy compat parameters
        max_requests: int | None = None,
        time_window: int | None = None,
    ):
        self.name = name

        # Handle legacy initialisation
        if config is None:
            if max_requests is not None or time_window is not None:
                config = RateLimiterConfig(
                    algorithm="sliding_window" if time_window else "token_bucket",
                    capacity=max_requests or 100,
                    time_window=time_window,
                )
            else:
                config = RateLimiterConfig()

        # max_requests is an alias for capacity
        if config.max_requests is not None:
            config.capacity = config.max_requests

        self.config = config

        # Instantiate the chosen algorithm
        if config.algorithm == "sliding_window" and config.time_window:
            self.algorithm: RateLimiterAlgorithm = SlidingWindow(
                capacity=config.capacity,
                window_seconds=config.time_window,
            )
        else:
            self.algorithm = TokenBucket(
                capacity=config.capacity,
                refill_rate=config.refill_rate,
                initial_tokens=config.initial_tokens,
            )

    async def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire *tokens*.  Logs a warning on rejection."""
        acquired = await self.algorithm.acquire(tokens)
        if not acquired:
            logger.warning(
                "rate_limit_exceeded for %s, requested_tokens=%d",
                self.name,
                tokens,
            )
        return acquired

    async def wait_and_acquire(self, tokens: int = 1) -> None:
        """Block until *tokens* are available."""
        start_time = time.monotonic()
        await self.algorithm.wait_and_acquire(tokens)

        wait_time = time.monotonic() - start_time
        if wait_time > 0.1:
            logger.info(
                "rate_limit_wait for %s, wait_time=%.3f, tokens=%d",
                self.name,
                wait_time,
                tokens,
            )

    # Convenience / back-compat methods

    async def check_rate_limit(self, identifier: str = "default") -> bool:
        """Check rate limit (compatibility method)."""
        return await self.acquire()

    async def wait_if_needed(self, identifier: str = "default") -> None:
        """Wait if rate limited (compatibility method)."""
        await self.wait_and_acquire()

    def get_status(self) -> dict:
        """Return current rate limiter status."""
        status: dict[str, Any] = {
            "name": self.name,
            "algorithm": self.config.algorithm,
            "capacity": self.config.capacity,
        }

        if isinstance(self.algorithm, TokenBucket):
            status["available_tokens"] = self.algorithm.tokens
            status["refill_rate"] = self.algorithm.refill_rate
        elif isinstance(self.algorithm, SlidingWindow):
            status["current_requests"] = len(self.algorithm.requests)
            status["window_seconds"] = self.algorithm.window_seconds

        return status
