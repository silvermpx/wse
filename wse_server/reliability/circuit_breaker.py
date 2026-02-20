# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from datetime import UTC, datetime
from enum import Enum, auto
from typing import Any, TypeVar

from .config import CircuitBreakerConfig

logger = logging.getLogger("wse.circuit_breaker")

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker[T]:
    """Circuit Breaker implementation.

    State machine: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

    * **CLOSED** -- normal operation; failures are counted.
    * **OPEN** -- requests are rejected immediately until *reset_timeout_seconds*
      elapses.
    * **HALF_OPEN** -- a limited number of probe calls are allowed; if they
      succeed the breaker closes again, otherwise it re-opens.
    """

    def __init__(
        self,
        config: CircuitBreakerConfig,
        cache_manager: Any | None = None,
    ):
        self.name = config.name
        self.config = config
        self.cache_manager = cache_manager

        # Core state
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: datetime | None = None
        self._half_open_calls = 0

        # Sliding-window metrics (optional)
        self._call_metrics: deque | None = None
        if config.window_size:
            self._call_metrics = deque(maxlen=config.window_size)

        self._lock = asyncio.Lock()

        # Support both reset_timeout_seconds and timeout_seconds
        if config.timeout_seconds is not None:
            self.config.reset_timeout_seconds = int(config.timeout_seconds)

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #

    async def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute *func* with circuit breaker protection."""
        async with self._lock:
            if not self._can_execute():
                raise CircuitBreakerOpenError(
                    f"Circuit breaker {self.name} is OPEN"
                )

        start_time = time.monotonic()

        try:
            result = await func(*args, **kwargs)
            duration = time.monotonic() - start_time
            await self._on_success(duration)
            return result

        except Exception as e:
            duration = time.monotonic() - start_time

            # Certain exception types should not count as failures.
            if self._should_ignore_exception(e):
                logger.debug(
                    "Circuit breaker %s: ignoring %s "
                    "(client-side error, not counted as failure)",
                    self.name,
                    type(e).__name__,
                )
                raise

            await self._on_failure(duration, str(e))
            raise

    async def execute_async(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Alias for :meth:`call`."""
        return await self.call(func, *args, **kwargs)

    def can_execute(self) -> bool:
        """Check if an operation can be executed (sync convenience)."""
        return self._can_execute()

    def record_success(self) -> None:
        """Record a successful call (fire-and-forget)."""
        asyncio.create_task(self._on_success(0))

    def record_failure(self, error_details: str | None = None) -> None:
        """Record a failed call (fire-and-forget)."""
        asyncio.create_task(self._on_failure(0, error_details))

    async def get_state(self) -> dict[str, Any]:
        """Return current state and metrics."""
        async with self._lock:
            state_info: dict[str, Any] = {
                "name": self.name,
                "state": self._state.name,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": (
                    self._last_failure_time.isoformat()
                    if self._last_failure_time
                    else None
                ),
            }

            if self._call_metrics is not None:
                state_info.update(
                    {
                        "failure_rate": self._calculate_failure_rate(),
                        "metrics_window": len(self._call_metrics),
                    }
                )

            return state_info

    def get_metrics(self) -> dict[str, Any]:
        """Return lightweight metrics dict (sync)."""
        return {
            "name": self.name,
            "state": self._state.name,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
        }

    def get_state_sync(self) -> str:
        """Return the current state name (sync convenience)."""
        return self._state.name

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #

    def _should_ignore_exception(self, exception: Exception) -> bool:
        """Return True if *exception* should NOT count as a CB failure.

        Ignoring auth-related errors (401/403) is an industry best practice:
        they indicate a client-side problem, not service degradation.
        """
        # Configured ignored exception types
        if self.config.ignored_exception_types:
            if isinstance(exception, self.config.ignored_exception_types):
                return True

        # Heuristic: check error message for auth-related keywords
        error_msg = str(exception).lower()
        auth_keywords = (
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "auth failed",
            "authentication",
        )
        return bool(any(kw in error_msg for kw in auth_keywords))

    def _can_execute(self) -> bool:
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
                logger.info("circuit_breaker_half_open for %s", self.name)
                return True
            return False

        # HALF_OPEN
        if self._half_open_calls < self.config.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False

    async def _on_success(self, duration: float) -> None:
        async with self._lock:
            if self._call_metrics is not None:
                self._call_metrics.append((True, duration))

            self._failure_count = 0
            self._success_count += 1

            if self._state == CircuitState.HALF_OPEN:
                if self._success_count >= self.config.success_threshold:
                    await self._transition_to_closed()

    async def _on_failure(
        self, duration: float, error_details: str | None = None
    ) -> None:
        async with self._lock:
            if self._call_metrics is not None:
                self._call_metrics.append((False, duration))

            self._failure_count += 1
            self._last_failure_time = datetime.now(UTC)

            if error_details:
                logger.debug(
                    "Circuit breaker %s failure: %s", self.name, error_details
                )

            if self._state == CircuitState.HALF_OPEN:
                await self._transition_to_open()

            elif self._state == CircuitState.CLOSED:
                should_open = False

                if self._failure_count >= self.config.failure_threshold:
                    should_open = True

                elif (
                    self.config.failure_rate_threshold is not None
                    and self._call_metrics is not None
                    and len(self._call_metrics) >= self.config.window_size  # type: ignore[arg-type]
                ):
                    failure_rate = self._calculate_failure_rate()
                    if failure_rate >= self.config.failure_rate_threshold:
                        should_open = True

                if should_open:
                    await self._transition_to_open()

    def _calculate_failure_rate(self) -> float:
        if not self._call_metrics:
            return 0.0
        failures = sum(1 for success, _ in self._call_metrics if not success)
        return failures / len(self._call_metrics)

    def _should_attempt_reset(self) -> bool:
        if self._last_failure_time is None:
            return True
        elapsed = (
            datetime.now(UTC) - self._last_failure_time
        ).total_seconds()
        return elapsed >= self.config.reset_timeout_seconds

    async def _transition_to_closed(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0
        logger.info("circuit_breaker_closed for %s", self.name)

        if self.cache_manager:
            await self.cache_manager.set(
                f"circuit_breaker:{self.name}:state", "CLOSED", ttl=3600
            )

    async def _transition_to_open(self) -> None:
        self._state = CircuitState.OPEN
        self._success_count = 0
        logger.warning("circuit_breaker_opened for %s", self.name)

        if self.cache_manager:
            await self.cache_manager.set(
                f"circuit_breaker:{self.name}:state", "OPEN", ttl=3600
            )


# --------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------- #


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is OPEN and rejects a call."""

    def __init__(
        self,
        message: str,
        retry_after: int | None = None,
    ):
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreakerError(Exception):
    """Base circuit breaker error."""


# --------------------------------------------------------------------- #
# Global registry
# --------------------------------------------------------------------- #

_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(
    name: str,
    config: CircuitBreakerConfig | None = None,
    cache_manager: Any | None = None,
) -> CircuitBreaker:
    """Get or create a named circuit breaker instance."""
    if name not in _circuit_breakers:
        if config is None:
            config = CircuitBreakerConfig(name=name)
        _circuit_breakers[name] = CircuitBreaker(config, cache_manager)
    return _circuit_breakers[name]


def get_all_circuit_breaker_metrics() -> dict[str, dict[str, Any]]:
    """Return metrics for every registered circuit breaker."""
    metrics: dict[str, dict[str, Any]] = {}
    for name, breaker in _circuit_breakers.items():
        try:
            metrics[name] = breaker.get_metrics()
        except Exception as e:
            logger.error(
                "Error getting metrics for circuit breaker %s: %s", name, e
            )
            metrics[name] = {"error": str(e)}
    return metrics
