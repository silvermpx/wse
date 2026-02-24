"""WSE Python client for real-time WebSocket communication.

Async usage::

    from wse_client import connect

    async with connect("ws://localhost:5006/wse", token="your-jwt") as client:
        await client.subscribe(["notifications"])
        async for event in client:
            print(event.type, event.payload)

Sync usage::

    from wse_client import SyncWSEClient

    client = SyncWSEClient("ws://localhost:5006/wse", token="your-jwt")
    client.connect()
    event = client.recv(timeout=5.0)
    client.close()

Optional extras::

    pip install wse-client[crypto]   # ECDH/AES-GCM encryption
    pip install wse-client[all]      # crypto + msgpack + orjson
"""

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


def connect(
    url: str,
    **kwargs,
) -> AsyncWSEClient:
    """Create a WSE client connection.

    Use as an async context manager. Keyword arguments are forwarded
    to :class:`AsyncWSEClient` -- common ones: ``token``, ``topics``,
    ``reconnect``, ``extra_headers``, ``queue_size``.

    Args:
        url: WebSocket server URL, e.g. ``"ws://localhost:5006/wse"``.
        **kwargs: Passed to :class:`AsyncWSEClient`.

    Returns:
        An :class:`AsyncWSEClient` instance.

    Raises:
        WSEConnectionError: If the connection cannot be established.
        WSEAuthError: If authentication is rejected.

    Example::

        async with connect("ws://localhost:5006/wse", token="jwt") as client:
            await client.subscribe(["notifications"])
            async for event in client:
                print(event.type, event.payload)
    """
    return AsyncWSEClient(url, **kwargs)


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
