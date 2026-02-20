"""Tests for WSE reliability infrastructure (CircuitBreaker + RateLimiter)."""

import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.reliability.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    _circuit_breakers,
    get_circuit_breaker,
)
from wse_server.reliability.config import CircuitBreakerConfig, RateLimiterConfig
from wse_server.reliability.rate_limiter import RateLimiter, SlidingWindow, TokenBucket

# =========================================================================
# CircuitBreakerConfig
# =========================================================================


class TestCircuitBreakerConfig:
    def test_default_values(self):
        config = CircuitBreakerConfig(name="test")
        assert config.name == "test"
        assert config.failure_threshold == 5
        assert config.success_threshold == 3
        assert config.reset_timeout_seconds == 30
        assert config.half_open_max_calls == 3
        assert config.window_size is None
        assert config.failure_rate_threshold is None
        assert config.timeout_seconds is None
        assert config.ignored_exception_types is None

    def test_custom_values(self):
        config = CircuitBreakerConfig(
            name="custom",
            failure_threshold=10,
            success_threshold=5,
            reset_timeout_seconds=60,
            half_open_max_calls=2,
            window_size=20,
            failure_rate_threshold=0.5,
        )
        assert config.failure_threshold == 10
        assert config.success_threshold == 5
        assert config.reset_timeout_seconds == 60
        assert config.window_size == 20
        assert config.failure_rate_threshold == 0.5

    def test_timeout_seconds_alias(self):
        config = CircuitBreakerConfig(name="t", timeout_seconds=45.0)
        assert config.timeout_seconds == 45.0


# =========================================================================
# CircuitBreaker
# =========================================================================


class TestCircuitBreaker:
    def test_initial_state_closed(self, circuit_breaker):
        assert circuit_breaker._state == CircuitState.CLOSED
        assert circuit_breaker.can_execute() is True

    @pytest.mark.asyncio
    async def test_success_keeps_closed(self, circuit_breaker):
        async def ok():
            return 42

        result = await circuit_breaker.call(ok)
        assert result == 42
        assert circuit_breaker._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_opens_after_failure_threshold(self):
        config = CircuitBreakerConfig(name="open_test", failure_threshold=3, window_size=10)
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("boom")

        for _ in range(3):
            with pytest.raises(ValueError):
                await cb.call(fail)

        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_rejects_when_open(self):
        config = CircuitBreakerConfig(name="reject", failure_threshold=1, window_size=10)
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("boom")

        with pytest.raises(ValueError):
            await cb.call(fail)

        assert cb._state == CircuitState.OPEN

        with pytest.raises(CircuitBreakerOpenError):
            await cb.call(fail)

    @pytest.mark.asyncio
    async def test_half_open_after_timeout(self):
        config = CircuitBreakerConfig(
            name="half_open",
            failure_threshold=1,
            reset_timeout_seconds=0,  # immediate reset for testing
            window_size=10,
        )
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("boom")

        with pytest.raises(ValueError):
            await cb.call(fail)

        assert cb._state == CircuitState.OPEN

        # With reset_timeout_seconds=0, _should_attempt_reset returns True
        # So the next call attempt transitions to HALF_OPEN and allows execution
        async def ok():
            return "recovered"

        result = await cb.call(ok)
        assert result == "recovered"

    @pytest.mark.asyncio
    async def test_half_open_to_closed_on_success(self):
        config = CircuitBreakerConfig(
            name="recovery",
            failure_threshold=1,
            success_threshold=2,
            reset_timeout_seconds=0,
            window_size=10,
        )
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("boom")

        async def ok():
            return "ok"

        # Trip the breaker
        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb._state == CircuitState.OPEN

        # First success -> HALF_OPEN, success_count = 1
        await cb.call(ok)
        # Second success -> success_count = 2 >= success_threshold -> CLOSED
        await cb.call(ok)
        assert cb._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens(self):
        config = CircuitBreakerConfig(
            name="reopen",
            failure_threshold=1,
            reset_timeout_seconds=0,
            window_size=10,
        )
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("boom")

        # Trip to OPEN
        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb._state == CircuitState.OPEN

        # Attempt in HALF_OPEN fails -> re-OPEN
        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_ignored_exception_types(self):
        config = CircuitBreakerConfig(
            name="ignore",
            failure_threshold=1,
            ignored_exception_types=(PermissionError,),
        )
        cb = CircuitBreaker(config)

        async def auth_fail():
            raise PermissionError("forbidden")

        # PermissionError is ignored -- should not trip the breaker
        with pytest.raises(PermissionError):
            await cb.call(auth_fail)
        assert cb._state == CircuitState.CLOSED
        assert cb._failure_count == 0

    @pytest.mark.asyncio
    async def test_auth_keyword_heuristic(self):
        """Exceptions with '401' or 'unauthorized' in message are auto-ignored."""
        config = CircuitBreakerConfig(name="heuristic", failure_threshold=1)
        cb = CircuitBreaker(config)

        async def auth_error():
            raise RuntimeError("HTTP 401 Unauthorized")

        with pytest.raises(RuntimeError):
            await cb.call(auth_error)
        assert cb._state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_failure_rate_threshold(self):
        config = CircuitBreakerConfig(
            name="rate",
            failure_threshold=100,  # high, so rate-based triggers first
            window_size=4,
            failure_rate_threshold=0.5,
        )
        cb = CircuitBreaker(config)

        async def ok():
            return True

        async def fail():
            raise ValueError("fail")

        # Fill the window: 2 successes, 2 failures = 50% failure rate
        await cb.call(ok)
        await cb.call(ok)
        with pytest.raises(ValueError):
            await cb.call(fail)
        with pytest.raises(ValueError):
            await cb.call(fail)

        assert cb._state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_get_state(self, circuit_breaker):
        state = await circuit_breaker.get_state()
        assert state["name"] == "test_cb"
        assert state["state"] == "CLOSED"
        assert state["failure_count"] == 0
        assert state["success_count"] == 0
        assert state["last_failure_time"] is None

    @pytest.mark.asyncio
    async def test_get_state_with_metrics(self):
        config = CircuitBreakerConfig(name="metrics", window_size=10)
        cb = CircuitBreaker(config)
        state = await cb.get_state()
        assert "failure_rate" in state
        assert "metrics_window" in state

    def test_get_metrics_sync(self, circuit_breaker):
        metrics = circuit_breaker.get_metrics()
        assert metrics["name"] == "test_cb"
        assert metrics["state"] == "CLOSED"

    def test_get_state_sync(self, circuit_breaker):
        assert circuit_breaker.get_state_sync() == "CLOSED"

    @pytest.mark.asyncio
    async def test_execute_async_alias(self, circuit_breaker):
        async def ok():
            return 99

        result = await circuit_breaker.execute_async(ok)
        assert result == 99

    @pytest.mark.asyncio
    async def test_timeout_seconds_overrides_reset_timeout(self):
        config = CircuitBreakerConfig(name="compat", timeout_seconds=120.0)
        cb = CircuitBreaker(config)
        assert cb.config.reset_timeout_seconds == 120

    @pytest.mark.asyncio
    async def test_success_resets_failure_count(self):
        config = CircuitBreakerConfig(name="reset_fc", failure_threshold=5, window_size=10)
        cb = CircuitBreaker(config)

        async def fail():
            raise ValueError("err")

        async def ok():
            return True

        # Accumulate 2 failures (below threshold)
        with pytest.raises(ValueError):
            await cb.call(fail)
        with pytest.raises(ValueError):
            await cb.call(fail)
        assert cb._failure_count == 2

        # Success resets failure_count to 0
        await cb.call(ok)
        assert cb._failure_count == 0

    @pytest.mark.asyncio
    async def test_circuit_breaker_open_error_retry_after(self):
        err = CircuitBreakerOpenError("open", retry_after=30)
        assert err.retry_after == 30
        assert str(err) == "open"


# =========================================================================
# Global registry
# =========================================================================


class TestCircuitBreakerRegistry:
    def test_get_circuit_breaker_creates_new(self):
        # Clear registry for isolation
        _circuit_breakers.clear()
        cb = get_circuit_breaker("registry_test")
        assert cb.name == "registry_test"
        assert "registry_test" in _circuit_breakers

    def test_get_circuit_breaker_returns_same(self):
        _circuit_breakers.clear()
        cb1 = get_circuit_breaker("same")
        cb2 = get_circuit_breaker("same")
        assert cb1 is cb2

    def test_get_circuit_breaker_with_config(self):
        _circuit_breakers.clear()
        config = CircuitBreakerConfig(name="custom_reg", failure_threshold=10)
        cb = get_circuit_breaker("custom_reg", config=config)
        assert cb.config.failure_threshold == 10


# =========================================================================
# RateLimiterConfig
# =========================================================================


class TestRateLimiterConfig:
    def test_default_values(self):
        config = RateLimiterConfig()
        assert config.algorithm == "token_bucket"
        assert config.capacity == 100
        assert config.refill_rate == 10.0
        assert config.initial_tokens is None
        assert config.max_requests is None
        assert config.time_window is None

    def test_custom_values(self):
        config = RateLimiterConfig(
            algorithm="sliding_window",
            capacity=50,
            refill_rate=5.0,
            initial_tokens=25,
            max_requests=50,
            time_window=60,
        )
        assert config.algorithm == "sliding_window"
        assert config.capacity == 50
        assert config.time_window == 60


# =========================================================================
# RateLimiter
# =========================================================================


class TestRateLimiter:
    @pytest.mark.asyncio
    async def test_allows_within_limit(self, rate_limiter):
        assert await rate_limiter.acquire() is True

    @pytest.mark.asyncio
    async def test_rejects_when_exhausted(self):
        config = RateLimiterConfig(capacity=2, refill_rate=0.001, initial_tokens=2)
        rl = RateLimiter(name="exhaust", config=config)
        assert await rl.acquire() is True
        assert await rl.acquire() is True
        assert await rl.acquire() is False

    @pytest.mark.asyncio
    async def test_refill_restores_tokens(self):
        config = RateLimiterConfig(capacity=1, refill_rate=10000.0, initial_tokens=1)
        rl = RateLimiter(name="refill", config=config)
        assert await rl.acquire() is True
        # refill_rate is very high, so after a tiny sleep tokens should be back
        await asyncio.sleep(0.01)
        assert await rl.acquire() is True

    @pytest.mark.asyncio
    async def test_acquire_multiple_tokens(self):
        config = RateLimiterConfig(capacity=5, refill_rate=0.001, initial_tokens=5)
        rl = RateLimiter(name="multi", config=config)
        assert await rl.acquire(tokens=3) is True
        # Only 2 left
        assert await rl.acquire(tokens=3) is False
        assert await rl.acquire(tokens=2) is True

    @pytest.mark.asyncio
    async def test_sliding_window_mode(self):
        config = RateLimiterConfig(
            algorithm="sliding_window",
            capacity=3,
            time_window=60,
        )
        rl = RateLimiter(name="sliding", config=config)
        assert isinstance(rl.algorithm, SlidingWindow)
        assert await rl.acquire() is True
        assert await rl.acquire() is True
        assert await rl.acquire() is True
        assert await rl.acquire() is False

    @pytest.mark.asyncio
    async def test_max_requests_alias(self):
        config = RateLimiterConfig(max_requests=5)
        rl = RateLimiter(name="alias", config=config)
        assert rl.config.capacity == 5

    @pytest.mark.asyncio
    async def test_legacy_init_max_requests(self):
        rl = RateLimiter(name="legacy", max_requests=5, time_window=60)
        assert isinstance(rl.algorithm, SlidingWindow)
        assert rl.config.capacity == 5

    @pytest.mark.asyncio
    async def test_legacy_init_default(self):
        rl = RateLimiter(name="default_legacy")
        assert isinstance(rl.algorithm, TokenBucket)

    @pytest.mark.asyncio
    async def test_check_rate_limit_compat(self, rate_limiter):
        result = await rate_limiter.check_rate_limit("user123")
        assert result is True

    @pytest.mark.asyncio
    async def test_wait_and_acquire(self):
        config = RateLimiterConfig(capacity=1, refill_rate=10000.0, initial_tokens=0)
        rl = RateLimiter(name="wait", config=config)
        # Should complete quickly since refill_rate is very high
        await asyncio.wait_for(rl.wait_and_acquire(), timeout=1.0)

    def test_get_status_token_bucket(self, rate_limiter):
        status = rate_limiter.get_status()
        assert status["name"] == "test_rl"
        assert status["algorithm"] == "token_bucket"
        assert status["capacity"] == 10
        assert "available_tokens" in status
        assert "refill_rate" in status

    def test_get_status_sliding_window(self):
        config = RateLimiterConfig(algorithm="sliding_window", capacity=10, time_window=60)
        rl = RateLimiter(name="sw_status", config=config)
        status = rl.get_status()
        assert status["algorithm"] == "sliding_window"
        assert "current_requests" in status
        assert "window_seconds" in status


# =========================================================================
# TokenBucket (internal algorithm)
# =========================================================================


class TestTokenBucket:
    @pytest.mark.asyncio
    async def test_initial_tokens_default(self):
        tb = TokenBucket(capacity=10, refill_rate=1.0)
        assert tb.tokens == 10.0

    @pytest.mark.asyncio
    async def test_initial_tokens_custom(self):
        tb = TokenBucket(capacity=10, refill_rate=1.0, initial_tokens=5)
        assert tb.tokens == pytest.approx(5.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_does_not_exceed_capacity(self):
        tb = TokenBucket(capacity=2, refill_rate=10000.0, initial_tokens=2)
        await asyncio.sleep(0.01)
        assert await tb.acquire() is True
        # After refill, tokens should be capped at capacity
        assert tb.tokens <= tb.capacity


# =========================================================================
# SlidingWindow (internal algorithm)
# =========================================================================


class TestSlidingWindow:
    @pytest.mark.asyncio
    async def test_within_window(self):
        sw = SlidingWindow(capacity=3, window_seconds=60)
        assert await sw.acquire() is True
        assert await sw.acquire() is True
        assert await sw.acquire() is True
        assert await sw.acquire() is False

    @pytest.mark.asyncio
    async def test_window_expiry(self):
        sw = SlidingWindow(capacity=1, window_seconds=0)
        assert await sw.acquire() is True
        # With window_seconds=0, all previous requests are expired
        assert await sw.acquire() is True
