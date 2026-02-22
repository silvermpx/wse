# =============================================================================
# WSE Python Client
# =============================================================================

from ._version import __version__
from .client import AsyncWSEClient
from .connection_pool import ConnectionPool
from .errors import (
    WSEAuthError,
    WSECircuitBreakerError,
    WSEConnectionError,
    WSEEncryptionError,
    WSEError,
    WSEProtocolError,
    WSERateLimitError,
    WSETimeoutError,
)
from .sync_client import SyncWSEClient
from .types import (
    ConnectionQuality,
    ConnectionState,
    ConnectionStats,
    LoadBalancingStrategy,
    MessagePriority,
    ReconnectConfig,
    WSEEvent,
)


async def connect(
    url: str,
    **kwargs,
) -> AsyncWSEClient:
    """Connect to a WSE server and return a ready client.

    Usage::

        async with connect("ws://localhost:5006/wse", token="jwt") as client:
            await client.subscribe(["notifications"])
            async for event in client:
                print(event.type, event.payload)
    """
    client = AsyncWSEClient(url, **kwargs)
    await client.connect()
    return client


__all__ = [
    "__version__",
    "connect",
    "AsyncWSEClient",
    "SyncWSEClient",
    "ConnectionPool",
    "WSEEvent",
    "ConnectionState",
    "ConnectionQuality",
    "ConnectionStats",
    "MessagePriority",
    "ReconnectConfig",
    "LoadBalancingStrategy",
    "WSEError",
    "WSEConnectionError",
    "WSEAuthError",
    "WSERateLimitError",
    "WSECircuitBreakerError",
    "WSEProtocolError",
    "WSEEncryptionError",
    "WSETimeoutError",
]
