"""WSE Python client for real-time WebSocket communication.

Async usage::

    from wse_client import connect

    async with connect("ws://localhost:5007/wse", token="your-jwt") as client:
        await client.subscribe(["notifications"])
        async for event in client:
            print(event.type, event.payload)

Sync usage::

    from wse_client import SyncWSEClient

    client = SyncWSEClient("ws://localhost:5007/wse", token="your-jwt")
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
from .offline_queue import OfflineQueue
from .signing import (
    SignatureProof,
    generate_signing_key,
    public_key_base64,
    public_key_jwk,
)
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
    ReconnectMode,
    SendResult,
    TokenProvider,
    WSEEvent,
)


def connect(
    url: str,
    **kwargs,
) -> AsyncWSEClient:
    """Create a WSE client to use as an async context manager.

    Keyword arguments are forwarded to :class:`AsyncWSEClient` -- common
    ones: ``token``, ``token_provider``, ``topics``, ``reconnect``,
    ``extra_headers``, ``queue_size``, ``encryption``. Nothing connects until
    ``async with`` enters the client, which raises what
    :meth:`AsyncWSEClient.connect` raises (:class:`WSEAuthError` when the
    server rejects the credentials, :class:`WSEConnectionError` when it
    cannot be reached, ...).

    Args:
        url: WebSocket server URL, e.g. ``"ws://localhost:5007/wse"``.
        **kwargs: Passed to :class:`AsyncWSEClient`.

    Returns:
        An :class:`AsyncWSEClient` instance (not connected yet).

    Example::

        async with connect("ws://localhost:5007/wse", token="jwt") as client:
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
    "OfflineQueue",
    "WSEEvent",
    "SignatureProof",
    "generate_signing_key",
    "public_key_base64",
    "public_key_jwk",
    "ConnectionState",
    "ConnectionQuality",
    "ConnectionStats",
    "MessagePriority",
    "ReconnectConfig",
    "ReconnectMode",
    "LoadBalancingStrategy",
    "SendResult",
    "TokenProvider",
    "WSEError",
    "WSEConnectionError",
    "WSEAuthError",
    "WSERateLimitError",
    "WSECircuitBreakerError",
    "WSEProtocolError",
    "WSEEncryptionError",
    "WSETimeoutError",
]
