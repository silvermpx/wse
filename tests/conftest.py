"""Common fixtures for WSE standalone tests."""

import asyncio
import contextlib

import pytest

from wse_server.connection.compression import CompressionManager
from wse_server.connection.security import SecurityManager
from wse_server.connection.sequencer import EventSequencer
from wse_server.connection.transformer import EventTransformer
from wse_server.reliability.circuit_breaker import CircuitBreaker
from wse_server.reliability.config import CircuitBreakerConfig, RateLimiterConfig
from wse_server.reliability.rate_limiter import RateLimiter

# ---------------------------------------------------------------------------
# Circuit Breaker fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cb_config():
    """Default CircuitBreakerConfig for tests."""
    return CircuitBreakerConfig(name="test_cb", failure_threshold=3, window_size=10)


@pytest.fixture
def circuit_breaker(cb_config):
    """CircuitBreaker with low failure threshold for testing."""
    return CircuitBreaker(cb_config)


# ---------------------------------------------------------------------------
# Rate Limiter fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def rl_config():
    """Default RateLimiterConfig for tests."""
    return RateLimiterConfig(capacity=10, refill_rate=100.0)


@pytest.fixture
def rate_limiter(rl_config):
    """RateLimiter with generous limits for testing."""
    return RateLimiter(name="test_rl", config=rl_config)


# ---------------------------------------------------------------------------
# Compression fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def compression_manager():
    """Fresh CompressionManager instance."""
    return CompressionManager()


# ---------------------------------------------------------------------------
# Security fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def security_manager():
    """SecurityManager with defaults (no providers)."""
    return SecurityManager()


@pytest.fixture
async def signing_security_manager():
    """SecurityManager with message signing enabled."""
    sm = SecurityManager()
    await sm.initialize({"message_signing_enabled": True})
    return sm


@pytest.fixture
async def encryption_security_manager():
    """SecurityManager with encryption enabled but no provider (no-op)."""
    sm = SecurityManager()
    await sm.initialize({"encryption_enabled": True})
    return sm


# ---------------------------------------------------------------------------
# Transformer fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def transformer():
    """Default EventTransformer (pass-through)."""
    return EventTransformer()


@pytest.fixture
def mapped_transformer():
    """EventTransformer with custom type mappings."""
    return EventTransformer(
        event_type_map={
            "OrderPlaced": "order_update",
            "BalanceChanged": "balance_change",
            "PriceUpdate": "price",
        }
    )


# ---------------------------------------------------------------------------
# Sequencer fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def sequencer():
    """Fresh EventSequencer with small window for testing.

    Uses a small window_size and max_out_of_order to make tests deterministic
    and fast.  The background cleanup task is cancelled after the test.
    """
    seq = EventSequencer(window_size=100, max_out_of_order=10)
    yield seq
    # Cancel the background cleanup task to avoid warnings
    seq._cleanup_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await seq._cleanup_task
