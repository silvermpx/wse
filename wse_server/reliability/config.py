# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

from dataclasses import dataclass


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration.

    Attributes:
        name: Unique identifier for the circuit breaker instance.
        failure_threshold: Number of consecutive failures before opening.
        success_threshold: Successes required in HALF_OPEN to close.
        reset_timeout_seconds: Seconds to wait in OPEN before probing.
        half_open_max_calls: Max concurrent probe calls in HALF_OPEN.
        window_size: Sliding window size for failure-rate calculation.
            ``None`` disables rate-based tripping.
        failure_rate_threshold: Fraction (0..1) of failures in the window
            that triggers OPEN.  Only used when *window_size* is set.
        timeout_seconds: Optional alias for *reset_timeout_seconds*
            (kept for backward compatibility).
        ignored_exception_types: Tuple of exception classes that should
            NOT count as failures (e.g. auth errors).
    """

    name: str = "default"
    failure_threshold: int = 5
    success_threshold: int = 3
    reset_timeout_seconds: int = 30
    half_open_max_calls: int = 3
    window_size: int | None = None
    failure_rate_threshold: float | None = None
    timeout_seconds: float | None = None
    ignored_exception_types: tuple[type[BaseException], ...] | None = None


@dataclass
class RateLimiterConfig:
    """Token-bucket / sliding-window rate limiter configuration.

    Attributes:
        algorithm: ``"token_bucket"`` (default) or ``"sliding_window"``.
        capacity: Maximum burst size (bucket capacity or window limit).
        refill_rate: Tokens restored per second (token bucket only).
        initial_tokens: Starting token count.  Defaults to *capacity*.
        max_requests: Alias for *capacity* (sliding window compat).
        time_window: Window length in seconds (sliding window only).
    """

    algorithm: str = "token_bucket"
    capacity: int = 100
    refill_rate: float = 10.0
    initial_tokens: int | None = None
    max_requests: int | None = None
    time_window: int | None = None
