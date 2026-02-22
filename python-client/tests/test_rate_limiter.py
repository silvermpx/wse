"""Tests for token bucket rate limiter."""

import time

from wse_client.rate_limiter import TokenBucketRateLimiter


class TestTokenBucketRateLimiter:
    def test_starts_full(self):
        rl = TokenBucketRateLimiter(capacity=10)
        assert rl.available_tokens == 10.0
        assert rl.can_send() is True

    def test_consume_reduces_tokens(self):
        rl = TokenBucketRateLimiter(capacity=5)
        assert rl.try_consume(3) is True
        assert rl.available_tokens == 2.0

    def test_cannot_consume_more_than_available(self):
        rl = TokenBucketRateLimiter(capacity=2)
        assert rl.try_consume(3) is False
        assert rl.available_tokens == 2.0

    def test_refill_over_time(self):
        rl = TokenBucketRateLimiter(
            capacity=10, refill_rate=100, refill_interval=0.1
        )
        rl.try_consume(10)
        assert rl.available_tokens == 0.0

        time.sleep(0.15)
        assert rl.available_tokens > 0

    def test_does_not_exceed_capacity(self):
        rl = TokenBucketRateLimiter(capacity=5, refill_rate=100, refill_interval=0.01)
        time.sleep(0.1)
        assert rl.available_tokens == 5.0  # Capped at capacity

    def test_retry_after(self):
        rl = TokenBucketRateLimiter(capacity=1, refill_rate=10, refill_interval=1.0)
        rl.try_consume(1)
        retry = rl.get_retry_after()
        assert retry > 0.0

    def test_utilization(self):
        rl = TokenBucketRateLimiter(capacity=10)
        assert rl.utilization == 0.0
        rl.try_consume(5)
        assert rl.utilization == 0.5

    def test_reset(self):
        rl = TokenBucketRateLimiter(capacity=10)
        rl.try_consume(10)
        rl.reset()
        assert rl.available_tokens == 10.0
