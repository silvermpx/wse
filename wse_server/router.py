# =============================================================================
# WSE — WebSocket Engine
# =============================================================================

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from fastapi import APIRouter, Query, Request, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

log = logging.getLogger("wse.router")

# Protocol version for the wire format
PROTOCOL_VERSION = 1


# =============================================================================
# Protocols
# =============================================================================


@runtime_checkable
class SnapshotProvider(Protocol):
    """Protocol that host applications implement to provide initial state snapshots.

    When a client connects and subscribes to topics the router can optionally
    deliver a snapshot of the current state so the client does not have to wait
    for the next publish cycle.
    """

    async def get_snapshot(self, user_id: str, topics: list[str]) -> dict[str, Any]:
        """Return a dict keyed by topic with the latest state for *user_id*."""
        ...


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class WSEConfig:
    """Configuration for the WSE router.

    All behavioural knobs are collected here so that the host application
    can customise the WebSocket endpoint without touching router internals.

    Attributes:
        auth_handler:
            Async callable ``(WebSocket) -> Optional[str]``.  Called after the
            WebSocket is accepted.  Must return a *user_id* string on success
            or ``None`` to reject the connection.  When ``None`` (the default),
            anonymous connections are allowed and user_id is set to a random
            UUID for each connection.
        snapshot_provider:
            Object implementing :class:`SnapshotProvider`.  When set, the
            router delivers an initial state snapshot after subscription.
        default_topics:
            Topics every new connection is automatically subscribed to when
            the client does not specify any.
        allowed_origins:
            List of allowed Origin header values for CSWSH protection.
            Empty list (default) disables origin checking (development mode).
        max_message_size:
            Maximum inbound WebSocket message size in bytes.
        heartbeat_interval:
            Server heartbeat interval in seconds.
        idle_timeout:
            Close the connection after this many seconds of inactivity.
        enable_compression:
            Whether per-message compression is offered during the handshake.
        enable_debug:
            Expose ``/wse/debug`` and ``/wse/compression-test`` endpoints.
        redis_client:
            Optional Redis client for cross-process PubSub fan-out.
            When ``None`` only in-process PubSubBus is used.
    """

    auth_handler: Callable[[WebSocket], Awaitable[str | None]] | None = None
    snapshot_provider: Any | None = None  # SnapshotProvider protocol
    default_topics: list[str] = field(default_factory=list)
    allowed_origins: list[str] = field(default_factory=list)
    max_message_size: int = 1_048_576  # 1 MB
    heartbeat_interval: float = 15.0
    idle_timeout: float = 90.0
    enable_compression: bool = True
    enable_debug: bool = False
    redis_client: Any | None = None  # For PubSub


# =============================================================================
# Router factory
# =============================================================================


def create_wse_router(config: WSEConfig) -> APIRouter:
    """Create a FastAPI :class:`APIRouter` with the WSE WebSocket endpoint.

    The returned router exposes:

    * ``/wse``               -- WebSocket endpoint (main)
    * ``/wse/health``        -- HTTP GET health check
    * ``/wse/debug``         -- HTTP GET debug info (only when *enable_debug*)
    * ``/wse/compression-test`` -- HTTP GET compression diagnostics (only when *enable_debug*)

    Parameters
    ----------
    config:
        A :class:`WSEConfig` instance that controls authentication, topics,
        origin policy, and optional services.

    Returns
    -------
    APIRouter
        A FastAPI router ready to be included via ``app.include_router()``.
    """

    router = APIRouter()

    # ------------------------------------------------------------------ #
    # WebSocket endpoint
    # ------------------------------------------------------------------ #

    @router.websocket("/wse")
    async def websocket_endpoint(
        websocket: WebSocket,
        client_version: str | None = Query("unknown", description="Client version"),
        protocol_version: int | None = Query(1, description="Protocol version"),
        topics: str | None = Query(None, description="Initial topics to subscribe"),
        compression: bool | None = Query(True, description="Enable compression"),
        encryption: bool | None = Query(False, description="Enable encryption"),
        message_signing: bool | None = Query(
            False,
            description="Selective signing for critical operations (Binance/Coinbase pattern)",
        ),
    ) -> None:
        """Enhanced WebSocket endpoint with protocol negotiation.

        Query Parameters
        ----------------
        client_version:
            Client application version string for diagnostics.
        protocol_version:
            Wire protocol version (default ``1``).
        topics:
            Comma-separated list of initial topics to subscribe.
        compression:
            Enable message compression (default ``true``).
        encryption:
            Enable message encryption (default ``false``).
        message_signing:
            Enable selective signing for critical operations.
        """

        # NOTE: Starlette/ASGI does not support closing a WebSocket before
        # accepting.  We must accept first, then close on auth failure.
        # See: https://github.com/encode/starlette/issues/2041
        await websocket.accept()

        # ----- Origin validation (CSWSH protection) -----
        origin = websocket.headers.get("origin", "")
        if config.allowed_origins and origin and origin not in config.allowed_origins:
            log.warning("Rejected WebSocket connection from disallowed origin: %s", origin)
            await websocket.send_json(
                {
                    "v": PROTOCOL_VERSION,
                    "t": "error",
                    "p": {"code": "ORIGIN_NOT_ALLOWED", "message": "Origin not allowed"},
                }
            )
            await websocket.close(code=4403, reason="Origin not allowed")
            return
        log.debug("WebSocket connection origin: %s", origin or "(none)")

        # ----- Logging -----
        client_ip = websocket.client.host if websocket.client else "unknown"
        log.info(
            "WebSocket connection attempt -- Client: %s, Protocol: %s, IP: %s",
            client_version,
            protocol_version,
            client_ip,
        )

        # ----- Authentication -----
        user_id: str | None = None

        if config.auth_handler is not None:
            try:
                user_id = await config.auth_handler(websocket)
            except Exception as exc:
                log.error("auth_handler raised %s: %s", type(exc).__name__, exc, exc_info=True)
                user_id = None

            if user_id is None:
                log.warning("WebSocket authentication failed (IP: %s)", client_ip)
                with contextlib.suppress(Exception):
                    await websocket.send_json(
                        {
                            "t": "error",
                            "p": {
                                "message": "Authentication failed",
                                "code": "AUTH_FAILED",
                                "recoverable": False,
                                "details": {
                                    "timestamp": datetime.now(UTC).isoformat(),
                                    "hint": "Please ensure you are logged in and try again",
                                },
                            },
                        }
                    )
                await websocket.close(code=4401, reason="Authentication required")
                return
        else:
            # Anonymous mode -- assign a random identifier
            user_id = uuid.uuid4().hex
            log.debug("Anonymous connection, assigned user_id=%s", user_id)

        # ----- Generate connection ID -----
        conn_id = f"ws_{user_id[:8]}_{uuid.uuid4().hex[:8]}"

        log.info(
            "WebSocket v%s connection authenticated -- "
            "User: %s, ConnID: %s, Client: %s, Compression: %s, Encryption: %s",
            protocol_version,
            user_id,
            conn_id,
            client_version,
            compression,
            encryption,
        )

        # ----- Obtain PubSubBus from application state -----
        app = websocket.scope.get("app")
        if not app:
            request = websocket.scope.get("request")
            if request and hasattr(request, "app"):
                app = request.app

        if not app:
            log.error("Cannot access app state from WebSocket scope")
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {
                        "t": "error",
                        "p": {
                            "message": "Server configuration error",
                            "code": "SERVER_ERROR",
                            "recoverable": False,
                            "details": {"timestamp": datetime.now(UTC).isoformat()},
                        },
                    }
                )
            await websocket.close(code=1011, reason="Server configuration error")
            return

        try:
            event_bus = getattr(app.state, "pubsub_bus", None)
            if not event_bus:
                raise RuntimeError("PubSubBus not available in app.state")
            log.debug("PubSubBus obtained successfully")
        except Exception as exc:
            log.error("Failed to get event bus: %s: %s", type(exc).__name__, exc, exc_info=True)
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {
                        "t": "error",
                        "p": {
                            "message": "Failed to initialize event bus",
                            "code": "INIT_ERROR",
                            "recoverable": False,
                            "details": {"timestamp": datetime.now(UTC).isoformat()},
                        },
                    }
                )
            await websocket.close(code=1011, reason="Event bus initialization failed")
            return

        # ----- Create WSEConnection -----
        # Import here to allow the connection module to be extracted separately.
        from .connection.connection import WSEConnection
        from .connection.handlers import WSEHandler
        from .connection.manager import get_ws_manager

        connection = WSEConnection(
            conn_id=conn_id,
            user_id=user_id,
            ws=websocket,
            event_bus=event_bus,
            protocol_version=protocol_version,
            compression_enabled=compression,
            encryption_enabled=encryption,
            message_signing_enabled=message_signing,
        )

        # Create message handler, optionally with snapshot provider
        handler_kwargs: dict[str, Any] = {}
        if config.snapshot_provider is not None:
            handler_kwargs["snapshot_provider"] = config.snapshot_provider
        message_handler = WSEHandler(connection, **handler_kwargs)

        try:
            # Initialize connection (starts heartbeat, metrics tasks)
            await connection.initialize()
            log.debug("Connection %s initialized", conn_id)

            # Register with WSEManager for connection tracking
            wse_manager = get_ws_manager()
            await wse_manager.add_connection(
                connection=connection,
                ip_address=client_ip,
                client_version=client_version,
                protocol_version=protocol_version,
            )
            log.debug("Connection %s registered with WSEManager", conn_id)

            # ----- Send server_ready -----
            # Generate ECDH keypair if encryption is enabled
            encryption_public_key = None
            if encryption:
                import base64

                raw_pk = connection.security_manager.generate_keypair(conn_id)
                encryption_public_key = base64.b64encode(raw_pk).decode("ascii")

            server_ready_details: dict[str, Any] = {
                "version": protocol_version,
                "client_version": client_version,
                "features": {
                    "compression": compression,
                    "encryption": encryption,
                    "batching": True,
                    "priority_queue": True,
                    "circuit_breaker": True,
                    "offline_queue": False,
                    "message_signing": connection.security_manager.message_signing_enabled,
                    "health_check": True,
                    "metrics": True,
                },
                "connection_id": conn_id,
                "server_time": datetime.now(UTC).isoformat(),
                "user_id": user_id,
                "endpoints": {
                    "primary": (
                        f"ws://{websocket.client.host}/ws/events" if websocket.client else None
                    ),
                    "health_check_interval": 30_000,
                    "heartbeat_interval": int(config.heartbeat_interval * 1000),
                },
            }
            if encryption_public_key:
                server_ready_details["encryption_public_key"] = encryption_public_key

            await connection.send_message(
                {
                    "t": "server_ready",
                    "p": {
                        "message": "Connection established",
                        "details": server_ready_details,
                    },
                },
                priority=10,
            )

            # ----- Initial topic subscription -----
            initial_topics: list[str] = []
            if topics:
                initial_topics = [t.strip() for t in topics.split(",") if t.strip()]
            if not initial_topics:
                initial_topics = list(config.default_topics)

            if initial_topics:
                log.info("Initial subscription topics for %s: %s", conn_id, initial_topics)
                await message_handler.handle_subscription(
                    {
                        "t": "subscription",
                        "p": {"action": "subscribe", "topics": initial_topics},
                    }
                )

            log.info("WebSocket %s connected and initialized successfully", conn_id)

            # ----- Main message loop -----
            while connection._running:
                try:
                    message = await asyncio.wait_for(
                        websocket.receive(),
                        timeout=1.0,
                    )

                    if message["type"] == "websocket.receive":
                        raw = message.get("text") or message.get("bytes")
                        if raw is not None:
                            parsed = await connection.handle_incoming(raw)
                            if parsed:
                                await message_handler.handle_message(parsed)

                    elif message["type"] == "websocket.disconnect":
                        log.info("WebSocket %s disconnect message received", conn_id)
                        break

                except TimeoutError:
                    # Normal -- allows checking _running flag
                    continue

                except WebSocketDisconnect:
                    log.info("WebSocket %s disconnected", conn_id)
                    break

                except Exception as exc:
                    error_str = str(exc).lower()

                    # Transport-closed means connection is dead
                    if "transport" in error_str and (
                        "closed" in error_str or "not initialized" in error_str
                    ):
                        log.warning(
                            "WebSocket %s transport closed, terminating connection", conn_id
                        )
                        break

                    log.error("WebSocket %s error: %s", conn_id, exc, exc_info=True)
                    connection.circuit_breaker.record_failure()

                    if connection.circuit_breaker.get_state_sync() == "OPEN":
                        log.warning("Circuit breaker OPEN for %s, closing connection", conn_id)
                        with contextlib.suppress(Exception):
                            await connection.send_message(
                                {
                                    "t": "error",
                                    "p": {
                                        "message": "Server error - circuit breaker activated",
                                        "code": "CIRCUIT_BREAKER_OPEN",
                                        "recoverable": False,
                                        "details": {"timestamp": datetime.now(UTC).isoformat()},
                                    },
                                },
                                priority=10,
                            )
                        await websocket.close(code=1011, reason="Server error")
                        break

        except Exception as exc:
            log.error(
                "WebSocket %s initialization error: %s: %s",
                conn_id,
                type(exc).__name__,
                exc,
                exc_info=True,
            )
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {
                        "t": "error",
                        "p": {
                            "message": "Connection initialization failed",
                            "code": "INIT_ERROR",
                            "recoverable": False,
                            "details": {"timestamp": datetime.now(UTC).isoformat()},
                        },
                    }
                )

        finally:
            # Unregister from WSEManager
            try:
                wse_manager = get_ws_manager()
                await wse_manager.remove_connection(conn_id)
                log.debug("Connection %s unregistered from WSEManager", conn_id)
            except Exception as exc:
                log.error("Error unregistering from WSEManager: %s", exc)

            # Clean up connection
            try:
                await connection.cleanup()
            except Exception as exc:
                log.error("Error during connection cleanup: %s", exc)

            # Close WebSocket if still open
            if websocket.client_state != WebSocketState.DISCONNECTED:
                with contextlib.suppress(Exception):
                    await websocket.close(code=1000, reason="Normal closure")

            # Log final metrics
            try:
                metrics = connection.metrics.to_dict()
                duration = (datetime.now(UTC) - connection.metrics.connected_since).total_seconds()
                log.info(
                    "WebSocket %s closed -- Duration: %.1fs, "
                    "Messages: %d/%d, Compression ratio: %.2f",
                    conn_id,
                    duration,
                    metrics["messages_sent"],
                    metrics["messages_received"],
                    metrics["compression_ratio"],
                )
            except Exception as exc:
                log.error("Error logging final metrics: %s", exc)

    # ------------------------------------------------------------------ #
    # Health check
    # ------------------------------------------------------------------ #

    @router.get("/wse/health")
    async def websocket_health_check(request: Request) -> dict[str, Any]:
        """Health check endpoint for the WebSocket service."""
        try:
            pubsub_bus = getattr(request.app.state, "pubsub_bus", None)
            if not pubsub_bus:
                return {
                    "status": "unhealthy",
                    "websocket_service": "error",
                    "error": "PubSubBus not available",
                    "timestamp": datetime.now(UTC).isoformat(),
                }

            metrics = pubsub_bus.get_metrics()
            is_healthy = metrics.get("running", False)

            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "websocket_service": "active",
                "pubsub_bus": metrics,
                "timestamp": datetime.now(UTC).isoformat(),
            }
        except Exception as exc:
            log.error("Health check failed: %s: %s", type(exc).__name__, exc, exc_info=True)
            return {
                "status": "unhealthy",
                "websocket_service": "error",
                "timestamp": datetime.now(UTC).isoformat(),
            }

    # ------------------------------------------------------------------ #
    # Debug endpoints (gated by config.enable_debug)
    # ------------------------------------------------------------------ #

    if config.enable_debug:

        @router.get("/wse/debug")
        async def websocket_debug_info(request: Request) -> dict[str, Any]:
            """Debug endpoint to inspect WebSocket service status."""
            try:
                pubsub_bus = getattr(request.app.state, "pubsub_bus", None)
                pubsub_metrics = pubsub_bus.get_metrics() if pubsub_bus else None

                return {
                    "status": "active",
                    "pubsub_bus": pubsub_metrics,
                    "protocol_version": PROTOCOL_VERSION,
                    "default_topics": config.default_topics,
                    "allowed_origins": config.allowed_origins,
                    "heartbeat_interval": config.heartbeat_interval,
                    "idle_timeout": config.idle_timeout,
                    "max_message_size": config.max_message_size,
                    "timestamp": datetime.now(UTC).isoformat(),
                }
            except Exception as exc:
                log.error("Debug endpoint error: %s: %s", type(exc).__name__, exc, exc_info=True)
                return {
                    "status": "error",
                    "timestamp": datetime.now(UTC).isoformat(),
                }

        @router.get("/wse/compression-test")
        async def compression_test() -> dict[str, Any]:
            """Diagnostics endpoint to verify compression behaviour."""
            from .connection.compression import CompressionManager

            cm = CompressionManager()

            test_cases = [
                {"size": 500, "data": "x" * 500},
                {"size": 1500, "data": "x" * 1500},
                {"size": 2000, "data": "".join(str(i % 10) for i in range(2000))},
                {
                    "size": 5000,
                    "data": json.dumps({"test": "data" * 100, "array": list(range(1000))}),
                },
            ]

            results = []
            for case in test_cases:
                data = case["data"].encode("utf-8")
                original_size = len(data)
                try:
                    compressed = cm.compress(data)
                    compressed_size = len(compressed)
                    ratio = compressed_size / original_size

                    decompressed = cm.decompress(compressed)
                    decompression_success = decompressed == data

                    results.append(
                        {
                            "original_size": original_size,
                            "compressed_size": compressed_size,
                            "compression_ratio": round(ratio, 3),
                            "saved_bytes": original_size - compressed_size,
                            "saved_percentage": round((1 - ratio) * 100, 1),
                            "decompression_success": decompression_success,
                            "should_compress": cm.should_compress(data),
                            "beneficial": ratio < 0.9,
                        }
                    )
                except Exception as exc:
                    results.append({"original_size": original_size, "error": str(exc)})

            return {
                "compression_threshold": cm.COMPRESSION_THRESHOLD,
                "compression_level": cm.COMPRESSION_LEVEL,
                "test_results": results,
                "stats": cm.get_stats(),
                "recommendation": (
                    "Compression is most effective for repetitive or structured data > 1KB"
                ),
            }

    return router
