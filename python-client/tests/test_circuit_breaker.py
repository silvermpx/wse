"""Tests for circuit breaker."""

import time


from wse_client.circuit_breaker import CircuitBreaker
from wse_client.types import CircuitBreakerState


class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.can_execute() is True

    def test_opens_after_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.can_execute() is True
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN
        assert cb.can_execute() is False

    def test_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=0.1)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN

        time.sleep(0.15)
        assert cb.state == CircuitBreakerState.HALF_OPEN
        assert cb.can_execute() is True

    def test_closes_after_success_threshold(self):
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=0.1, success_threshold=2)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)

        # Access .state to trigger OPEN -> HALF_OPEN transition
        assert cb.state == CircuitBreakerState.HALF_OPEN

        cb.record_success()
        assert cb.state == CircuitBreakerState.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitBreakerState.CLOSED

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker(failure_threshold=2, reset_timeout=0.1)
        cb.record_failure()
        cb.record_failure()
        time.sleep(0.15)

        assert cb.state == CircuitBreakerState.HALF_OPEN
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN

    def test_reset(self):
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == CircuitBreakerState.OPEN

        cb.reset()
        assert cb.state == CircuitBreakerState.CLOSED
        assert cb.can_execute() is True

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        # failure_count was 2, now 0 after success (full reset, matches TS)
        cb.record_failure()
        cb.record_failure()
        # failure_count = 2, still below threshold 3
        assert cb.state == CircuitBreakerState.CLOSED
