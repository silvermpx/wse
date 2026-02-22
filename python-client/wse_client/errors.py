# =============================================================================
# WSE Python Client -- Error Types
# =============================================================================


class WSEError(Exception):
    """Base exception for all WSE client errors."""


class WSEConnectionError(WSEError):
    """Connection-related errors (failed to connect, lost connection)."""


class WSEAuthError(WSEError):
    """Authentication errors (invalid token, expired, rejected)."""


class WSERateLimitError(WSEError):
    """Rate limit exceeded."""

    def __init__(self, retry_after: float = 0.0) -> None:
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded. Retry after {retry_after:.1f}s")


class WSECircuitBreakerError(WSEError):
    """Circuit breaker is open, refusing connection attempts."""


class WSEProtocolError(WSEError):
    """Wire protocol errors (malformed messages, unknown prefixes)."""


class WSEEncryptionError(WSEError):
    """Encryption/decryption failures."""


class WSETimeoutError(WSEError):
    """Operation timed out."""
