# =============================================================================
# WSE Python Client -- Circuit Breaker
# =============================================================================

from __future__ import annotations

import time
from typing import Awaitable, Callable, TypeVar

from .constants import (
    CIRCUIT_BREAKER_SUCCESS_THRESHOLD,
    CIRCUIT_BREAKER_THRESHOLD,
    CIRCUIT_BREAKER_TIMEOUT,
)
from .errors import WSECircuitBreakerError
from .types import CircuitBreakerState

T = TypeVar("T")


class CircuitBreaker:
    """Three-state circuit breaker to prevent reconnection storms.

    CLOSED -> normal, failures counted. OPEN -> all calls rejected
    until *reset_timeout*. HALF_OPEN -> one probe call allowed.

    Args:
        failure_threshold: Failures before the circuit opens (default 5).
        reset_timeout: Seconds before probing recovery (default 60).
        success_threshold: Successes in HALF_OPEN to close again (default 3).
    """

    def __init__(
        self,
        failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
        reset_timeout: float = CIRCUIT_BREAKER_TIMEOUT,
        success_threshold: int = CIRCUIT_BREAKER_SUCCESS_THRESHOLD,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._success_threshold = success_threshold

        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None

    @property
    def state(self) -> CircuitBreakerState:
        self._check_timeout()
        return self._state

    def can_execute(self) -> bool:
        self._check_timeout()
        return self._state != CircuitBreakerState.OPEN

    def record_success(self) -> None:
        if self._state == CircuitBreakerState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self._success_threshold:
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                self._success_count = 0
        elif self._state == CircuitBreakerState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitBreakerState.HALF_OPEN:
            self._state = CircuitBreakerState.OPEN
            self._success_count = 0
        elif self._failure_count >= self._failure_threshold:
            self._state = CircuitBreakerState.OPEN

    async def execute(self, fn: Callable[[], Awaitable[T]]) -> T:
        if not self.can_execute():
            raise WSECircuitBreakerError("Circuit breaker is open")
        try:
            result = await fn()
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def reset(self) -> None:
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None

    def _check_timeout(self) -> None:
        if (
            self._state == CircuitBreakerState.OPEN
            and self._last_failure_time is not None
        ):
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._reset_timeout:
                self._state = CircuitBreakerState.HALF_OPEN
                self._success_count = 0
