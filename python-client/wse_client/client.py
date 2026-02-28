# =============================================================================
# WSE Python Client -- Async Client
# =============================================================================
#
# Primary public API.  Async context manager, async iterator, callbacks.
# Port of client/hooks/useWSE.ts + client/services/MessageProcessor.ts.
# =============================================================================

from __future__ import annotations

import asyncio
import time

from uuid import uuid4
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable

from ._logging import logger
from .circuit_breaker import CircuitBreaker
from .compression import CompressionHandler
from .connection import ConnectionManager
from .constants import (
    CLIENT_VERSION,
    PROTOCOL_VERSION,
    SEND_MAX_RETRIES,
    SEND_RETRY_BASE_DELAY,
    SEND_RETRY_MAX_DELAY,
    SNAPSHOT_MAX_RETRIES,
    SNAPSHOT_RETRY_DELAY,
)
from .errors import WSEConnectionError
from .event_sequencer import EventSequencer
from .network_monitor import NetworkMonitor
from .offline_queue import OfflineQueue
from .protocol import MessageCodec
from .rate_limiter import TokenBucketRateLimiter
from .types import (
    ConnectionQuality,
    ConnectionState,
    ConnectionStats,
    LoadBalancingStrategy,
    MessagePriority,
    ReconnectConfig,
    WSEEvent,
)

# Type alias for event handlers
EventHandler = Callable[[WSEEvent], Any]
AsyncEventHandler = Callable[[WSEEvent], Awaitable[Any]]

# System events consumed internally (not forwarded to user iterator)
_INTERNAL_ONLY_EVENTS = frozenset(
    {
        "server_ready",
        "server_hello",
        "subscription_update",
        "health_check",
        "config_update",
        "PONG",
        "pong",
        "heartbeat",
        "PING",
        "ping",
        "sync_request",
        "metrics_request",
        "priority_message",
    }
)


class AsyncWSEClient:
    """Async WSE client with context manager and async iterator support.

    Args:
        url: WebSocket server URL, e.g. ``"ws://localhost:5006/wse"``.
        token: JWT token for authentication (sent as Cookie + Authorization).
        topics: Topics to auto-subscribe after connecting.
        reconnect: Reconnection config. Defaults to exponential backoff,
            infinite retries.
        extra_headers: Additional HTTP headers for the handshake.
        queue_size: Max events buffered for the async iterator. When full,
            oldest events are dropped. Default 1000.

    Example::

        async with AsyncWSEClient("ws://localhost:5006/wse", token="jwt") as client:
            await client.subscribe(["notifications"])
            async for event in client:
                print(event.type, event.payload)
    """

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        topics: list[str] | None = None,
        reconnect: ReconnectConfig | None = None,
        extra_headers: dict[str, str] | None = None,
        queue_size: int = 1000,
        encryption: bool = False,
        endpoints: list[str] | None = None,
        load_balancing: LoadBalancingStrategy = LoadBalancingStrategy.WEIGHTED_RANDOM,
    ) -> None:
        self._url = url
        self._token = token
        self._initial_topics = list(topics) if topics else []
        self._subscribed_topics: set[str] = set()

        # E2E encryption (optional)
        self._security: SecurityManager | None = None
        if encryption:
            from .security import SecurityManager

            self._security = SecurityManager()

        # Services
        self._compression = CompressionHandler()
        msgpack_handler = None
        try:
            from .msgpack_handler import MsgPackHandler

            if MsgPackHandler.available():
                msgpack_handler = MsgPackHandler()
        except ImportError:
            pass
        self._codec = MessageCodec(
            self._compression, security=self._security, msgpack=msgpack_handler
        )
        self._rate_limiter = TokenBucketRateLimiter()
        self._circuit_breaker = CircuitBreaker()
        self._sequencer = EventSequencer()
        self._network_monitor = NetworkMonitor()
        self._offline_queue = OfflineQueue(max_size=1000, max_age=3600.0)

        # Event queue for async iteration
        self._event_queue: asyncio.Queue[WSEEvent | None] = asyncio.Queue(
            maxsize=queue_size
        )
        self._disconnecting = False

        # Callback handlers: type -> list of handlers
        self._handlers: dict[str, list[EventHandler | AsyncEventHandler]] = defaultdict(
            list
        )
        self._wildcard_handlers: list[EventHandler | AsyncEventHandler] = []

        # Stats
        self._stats = ConnectionStats()

        # Server-negotiated state
        self._server_features: dict[str, Any] = {}
        self._server_max_message_size: int | None = None
        self._server_rate_limit: int | None = None

        self._background_tasks: set[asyncio.Task[Any]] = set()

        # Snapshot dedup
        self._snapshot_requested = False

        # Recovery state: topic -> (epoch, offset)
        self._recovery_state: dict[str, tuple[str, int]] = {}
        self._server_recovery_enabled = False

        # Reconnect config for later use
        self._reconnect_cfg = reconnect

        # Connection manager
        self._connection = ConnectionManager(
            url,
            token=token,
            reconnect=reconnect,
            circuit_breaker=self._circuit_breaker,
            rate_limiter=self._rate_limiter,
            extra_headers=extra_headers,
            on_message=self._on_raw_message,
            on_state_change=self._on_state_change,
            security=self._security,
            endpoints=endpoints,
            load_balancing=load_balancing,
        )

        self._connection._on_pong = self._on_transport_pong
        self._connection._on_ping_sent = lambda: self._network_monitor.record_ping()
        self._server_ready_event = asyncio.Event()

        # System handler dispatch table (dict lookup = O(1))
        self._system_handlers: dict[str, Callable[[WSEEvent], None]] = {
            "server_ready": self._handle_server_ready,
            "server_hello": self._handle_server_hello,
            "subscription_update": self._handle_subscription_update,
            "error": self._handle_error,
            "health_check": self._handle_health_check,
            "health_check_response": self._handle_health_check_response,
            "config_update": self._handle_config_update,
            "snapshot_complete": self._handle_snapshot_complete,
            "rate_limit_warning": self._handle_rate_limit_warning,
            "connection_quality": self._handle_connection_quality,
            "connection_state_change": self._handle_connection_state_change,
            "sync_request": self._handle_sync_request,
            "metrics_request": self._handle_metrics_request,
            "priority_message": self._handle_priority_message,
            "PONG": self._handle_pong_event,
            "pong": self._handle_pong_event,
            "heartbeat": self._handle_pong_event,
            "PING": self._handle_ping_event,
            "ping": self._handle_ping_event,
            # Response handlers (forwarded to user)
            "debug_response": self._handle_forward_event,
            "sequence_stats_response": self._handle_forward_event,
            "config_response": self._handle_forward_event,
            "config_update_response": self._handle_forward_event,
            "encryption_response": self._handle_forward_event,
            "key_rotation_response": self._handle_forward_event,
            "batch_message_result": self._handle_forward_event,
            "metrics_response": self._handle_forward_event,
            "connection_state_response": self._handle_forward_event,
        }

    # -- Context manager ------------------------------------------------------

    async def __aenter__(self) -> AsyncWSEClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.disconnect()

    # -- Async iterator -------------------------------------------------------

    def __aiter__(self) -> AsyncWSEClient:
        return self

    async def __anext__(self) -> WSEEvent:
        event = await self._event_queue.get()
        if event is None:
            raise StopAsyncIteration
        return event

    # -- Connect / Disconnect -------------------------------------------------

    async def connect(self) -> None:
        """Connect and wait for server_ready."""
        await self._connection.connect(self._initial_topics)

        # Wait for server_ready with timeout
        try:
            await asyncio.wait_for(self._server_ready_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("server_ready not received within 10s, proceeding anyway")

        # Auto-subscribe to initial topics
        if self._initial_topics:
            await self.subscribe(self._initial_topics)

    async def disconnect(self) -> None:
        """Disconnect gracefully."""
        self._disconnecting = True

        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

        # Stop key rotation timer
        if self._security is not None:
            self._security.stop_key_rotation()

        # Clear offline queue (no point replaying after explicit disconnect)
        self._offline_queue.clear()

        # Signal iterator to stop (drop oldest if queue is full)
        try:
            self._event_queue.put_nowait(None)
        except asyncio.QueueFull:
            try:
                self._event_queue.get_nowait()
                self._event_queue.put_nowait(None)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
        try:
            await self._connection.destroy()
        finally:
            self._disconnecting = False

    async def close(self) -> None:
        """Alias for disconnect."""
        await self.disconnect()

    # -- Properties -----------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connection.is_connected

    @property
    def is_ready(self) -> bool:
        return self._connection.is_ready

    @property
    def is_fully_ready(self) -> bool:
        """Connected + server_ready + client_hello_sent (matches TS isFullyReady)."""
        return (
            self._connection.is_connected
            and self._connection.is_ready
            and self._connection._client_hello_sent
        )

    @property
    def state(self) -> ConnectionState:
        return self._connection.state

    @property
    def connection_quality(self) -> ConnectionQuality:
        return self._network_monitor.quality

    @property
    def subscribed_topics(self) -> set[str]:
        return set(self._subscribed_topics)

    @property
    def recovery_enabled(self) -> bool:
        """Whether the server supports message recovery."""
        return self._server_recovery_enabled

    @property
    def recovery_state(self) -> dict[str, tuple[str, int]]:
        """Per-topic recovery positions: ``{topic: (epoch, offset)}``."""
        return dict(self._recovery_state)

    @property
    def queue_size(self) -> int:
        """Number of events waiting in the iterator queue."""
        return self._event_queue.qsize()

    # -- Send / Subscribe / Unsubscribe ---------------------------------------

    async def send(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = MessagePriority.NORMAL,
        correlation_id: str | None = None,
    ) -> bool:
        """Send a message to the server.

        Args:
            type: Event type string, e.g. ``"update_settings"``.
            payload: Message data dict (default: empty).
            priority: See :class:`~wse_client.types.MessagePriority`.
            correlation_id: Optional ID to correlate request/response.

        Returns:
            True if sent, False if disconnected or rate-limited.
        """
        encoded = self._codec.encode(
            type,
            payload or {},
            priority=priority,
            correlation_id=correlation_id,
        )
        ok = await self._connection.send(encoded)
        if ok:
            self._stats.messages_sent += 1
            self._stats.bytes_sent += len(encoded.encode("utf-8"))
        elif self._offline_queue.enabled:
            self._offline_queue.enqueue(encoded, priority=priority, msg_type=type)
        return ok

    async def send_with_retry(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = MessagePriority.NORMAL,
        correlation_id: str | None = None,
        max_retries: int = SEND_MAX_RETRIES,
    ) -> bool:
        """Send with exponential backoff retry.

        Args:
            type: Event type string.
            payload: Message data dict.
            priority: See :class:`~wse_client.types.MessagePriority`.
            correlation_id: Optional correlation ID.
            max_retries: Max attempts (default 5).

        Returns:
            True if sent within the retry budget, False otherwise.
        """
        for attempt in range(max_retries):
            ok = await self.send(
                type, payload, priority=priority, correlation_id=correlation_id
            )
            if ok:
                return True

            if attempt < max_retries - 1:
                delay = min(
                    SEND_RETRY_BASE_DELAY * (2**attempt),
                    SEND_RETRY_MAX_DELAY,
                )
                logger.debug(
                    "Send retry %d/%d for '%s' in %.1fs",
                    attempt + 1,
                    max_retries,
                    type,
                    delay,
                )
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return False

        logger.warning("Send failed after %d retries for '%s'", max_retries, type)
        return False

    async def send_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> bool:
        """Send multiple messages in a single batch frame.

        Args:
            messages: List of ``(type, payload)`` tuples.

        Returns:
            True if the batch was sent.
        """
        if not messages:
            return True

        batch_items = []
        for msg_type, msg_payload in messages:
            item = {
                "t": msg_type,
                "p": msg_payload,
                "id": str(uuid4()),
                "seq": self._sequencer.get_next_sequence(),
                "v": PROTOCOL_VERSION,
                "ts": datetime.now(UTC).isoformat(),
            }
            batch_items.append(item)

        encoded = self._codec.encode(
            "batch",
            {"messages": batch_items, "count": len(batch_items)},
            priority=MessagePriority.NORMAL,
        )
        ok = await self._connection.send(encoded)
        if ok:
            self._stats.messages_sent += len(messages)
            self._stats.bytes_sent += len(encoded.encode("utf-8"))
        return ok

    async def subscribe(
        self,
        topics: list[str],
        *,
        recover: bool = False,
    ) -> bool:
        """Subscribe to one or more topics, optionally recovering missed messages.

        When *recover* is True and the server supports recovery, the client
        sends its last known epoch and offset for each topic so the server
        can replay missed messages before confirming the subscription.

        Args:
            topics: Topic names, e.g. ``["notifications", "live_data"]``.
            recover: If True, request message recovery using stored positions.

        Returns:
            True if the subscription message was sent.
        """
        new_topics = [t for t in topics if t not in self._subscribed_topics]
        if not new_topics:
            return True

        payload: dict[str, Any] = {"action": "subscribe", "topics": new_topics}

        # Attach recovery info when requested and server supports it
        if recover and self._server_recovery_enabled:
            recovery_info: dict[str, dict[str, Any]] = {}
            for topic in new_topics:
                state = self._recovery_state.get(topic)
                if state is not None:
                    recovery_info[topic] = {"epoch": state[0], "offset": state[1]}
            if recovery_info:
                payload["recover"] = True
                payload["recovery"] = recovery_info

        msg = self._codec.encode(
            "subscription_update",
            payload,
            priority=MessagePriority.HIGH,
        )
        ok = await self._connection.send(msg)
        if ok:
            self._subscribed_topics.update(new_topics)
            self._stats.messages_sent += 1
        return ok

    async def unsubscribe(self, topics: list[str]) -> bool:
        """Unsubscribe from one or more topics.

        Args:
            topics: Topic names to unsubscribe from.

        Returns:
            True if the unsubscribe message was sent.
        """
        msg = self._codec.encode(
            "subscription_update",
            {"action": "unsubscribe", "topics": topics},
            priority=MessagePriority.HIGH,
        )
        ok = await self._connection.send(msg)
        if ok:
            self._subscribed_topics -= set(topics)
            for t in topics:
                self._recovery_state.pop(t, None)
            self._stats.messages_sent += 1
        return ok

    async def request_snapshot(self, topics: list[str] | None = None) -> bool:
        """Request a state snapshot from the server.

        The server replies with current state for the given topics so the
        client doesn't have to wait for the next publish.

        Args:
            topics: Topics to snapshot. Defaults to subscribed topics.

        Returns:
            True if the request was sent.
        """
        if self._snapshot_requested:
            return True

        target = topics or list(self._subscribed_topics)

        for attempt in range(SNAPSHOT_MAX_RETRIES):
            msg = self._codec.encode(
                "sync_request",
                {
                    "topics": target,
                    "include_snapshots": True,
                    "include_history": False,
                    "last_sequence": self._sequencer.current_sequence,
                },
                priority=MessagePriority.HIGH,
            )
            ok = await self._connection.send(msg)
            if ok:
                self._snapshot_requested = True
                return True

            if attempt < SNAPSHOT_MAX_RETRIES - 1:
                delay = SNAPSHOT_RETRY_DELAY * (2**attempt)
                logger.debug(
                    "Snapshot request retry %d/%d in %.1fs",
                    attempt + 1,
                    SNAPSHOT_MAX_RETRIES,
                    delay,
                )
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    return False

        logger.warning("Snapshot request failed after %d retries", SNAPSHOT_MAX_RETRIES)
        return False

    async def recv(self, timeout: float | None = None) -> WSEEvent:
        """Receive a single event (alternative to async iteration).

        Args:
            timeout: Max seconds to wait. ``None`` blocks indefinitely.

        Returns:
            The next :class:`~wse_client.types.WSEEvent`.

        Raises:
            WSEConnectionError: If the connection is closed.
            asyncio.TimeoutError: If *timeout* expires.
        """
        if timeout is not None:
            event = await asyncio.wait_for(self._event_queue.get(), timeout=timeout)
        else:
            event = await self._event_queue.get()

        if event is None:
            # Re-queue sentinel so other callers also see the close signal
            try:
                self._event_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            raise WSEConnectionError("Connection closed")
        return event

    async def force_reconnect(self) -> None:
        """Force a reconnection (e.g. after detecting degraded quality)."""
        self._server_ready_event.clear()
        await self._connection.force_reconnect()

    async def change_endpoint(self, new_url: str) -> None:
        """Change the server endpoint and reconnect."""
        self._disconnecting = True
        try:
            await self._connection.disconnect()
        finally:
            self._disconnecting = False
        self._connection._url = new_url
        self._connection._active_endpoint = new_url
        self._connection._reconnect_attempts = 0
        self._server_ready_event.clear()

        # Register with pool if active
        if self._connection._pool is not None:
            self._connection._pool.add_endpoint(new_url)
            self._connection._pool.set_active_endpoint(new_url)

        # Merge dynamically subscribed topics with initial topics
        all_topics = list(set(self._initial_topics) | self._subscribed_topics)
        await self._connection.connect(all_topics)

    # -- Handler registration -------------------------------------------------

    def on(
        self, event_type: str
    ) -> Callable[[EventHandler | AsyncEventHandler], EventHandler | AsyncEventHandler]:
        """Decorator to register an event handler for a specific topic.

        Args:
            event_type: Event type to listen for, e.g. ``"notifications"``.

        Returns:
            Decorator that registers the function.

        Example::

            @client.on("notifications")
            async def handle(event: WSEEvent):
                print(event.payload)
        """

        def decorator(
            fn: EventHandler | AsyncEventHandler,
        ) -> EventHandler | AsyncEventHandler:
            self._handlers[event_type].append(fn)
            return fn

        return decorator

    def on_any(
        self, fn: EventHandler | AsyncEventHandler
    ) -> EventHandler | AsyncEventHandler:
        """Register a wildcard handler that receives all events."""
        self._wildcard_handlers.append(fn)
        return fn

    def off(self, event_type: str, fn: EventHandler | AsyncEventHandler) -> None:
        """Remove a specific handler."""
        handlers = self._handlers.get(event_type, [])
        if fn in handlers:
            handlers.remove(fn)

    # -- Stats ----------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return client statistics."""
        diag = self._network_monitor.analyze()
        return {
            "state": self._connection.state.value,
            "is_ready": self.is_ready,
            "is_fully_ready": self.is_fully_ready,
            "connection_id": self._connection.connection_id,
            "subscribed_topics": list(self._subscribed_topics),
            "messages_received": self._stats.messages_received,
            "messages_sent": self._stats.messages_sent,
            "bytes_received": self._stats.bytes_received,
            "bytes_sent": self._stats.bytes_sent,
            "reconnect_count": self._stats.reconnect_count,
            "sequencer": self._sequencer.get_stats(),
            "queue_size": self._event_queue.qsize(),
            "network": {
                "quality": diag.quality.value,
                "latency_ms": diag.round_trip_time,
                "jitter_ms": diag.jitter,
                "packet_loss": diag.packet_loss,
                "stability": diag.stability,
            },
            "circuit_breaker": {
                "state": self._circuit_breaker.state.value,
                "can_execute": self._circuit_breaker.can_execute(),
            },
            "rate_limiter": self._rate_limiter.get_stats(),
            "offline_queue": self._offline_queue.get_stats(),
            "connection_pool": (
                self._connection._pool.get_stats()
                if self._connection._pool is not None
                else None
            ),
        }

    # -- Internal: message handling -------------------------------------------

    def _on_raw_message(self, data: str | bytes) -> None:
        """Decode raw WebSocket frame and dispatch."""
        self._stats.messages_received += 1
        if isinstance(data, bytes):
            self._stats.bytes_received += len(data)
        else:
            self._stats.bytes_received += len(data.encode("utf-8"))

        result = self._codec.decode(data)
        if result is None:
            return

        events = result if isinstance(result, list) else [result]
        for event in events:
            self._process_event(event)

    def _process_event(self, event: WSEEvent) -> None:
        """Dedup, sequence, handle system events, then dispatch."""
        # Dedup
        if event.id and self._sequencer.is_duplicate(event.id):
            return

        # Sequence
        if event.sequence is not None:
            ordered = self._sequencer.process_sequenced_event(event.sequence, event)
            if ordered is None:
                return  # Buffered, not yet deliverable
            for evt in ordered:
                self._dispatch_event(evt)
            return

        self._dispatch_event(event)

    def _dispatch_event(self, event: WSEEvent) -> None:
        """Handle system events, invoke callbacks, enqueue for iterator."""
        # Fast system handler lookup (O(1) dict)
        handler = self._system_handlers.get(event.type)
        if handler is not None:
            handler(event)
            # Internal-only events are NOT forwarded to user queue/handlers
            if event.type in _INTERNAL_ONLY_EVENTS:
                return

        # Invoke registered handlers
        self._invoke_handlers(event)

        # Enqueue for async iterator
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            # Drop oldest to make room
            try:
                self._event_queue.get_nowait()
                self._event_queue.put_nowait(event)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    def _fire_task(self, coro: Any) -> None:
        """Schedule a coroutine with a strong reference to prevent GC."""
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _invoke_handlers(self, event: WSEEvent) -> None:
        """Call registered handlers for this event type."""
        handlers = self._handlers.get(event.type, []) + self._wildcard_handlers
        for handler in handlers:
            try:
                result = handler(event)
                if asyncio.iscoroutine(result):
                    self._fire_task(result)
            except Exception as exc:
                logger.error("Handler error for '%s': %s", event.type, exc)

    # -- System event handlers ------------------------------------------------

    def _handle_server_ready(self, event: WSEEvent) -> None:
        conn_id = event.payload.get("connection_id") or event.payload.get(
            "details", {}
        ).get("connection_id")
        self._connection.handle_server_ready(conn_id)
        self._server_ready_event.set()
        self._snapshot_requested = False

        # Check if server supports recovery (from features in details)
        details = event.payload.get("details", {})
        features = details.get("features", {})
        self._server_recovery_enabled = bool(features.get("recovery", False))

        logger.info(
            "Server ready (connection_id=%s, recovery=%s)",
            conn_id,
            self._server_recovery_enabled,
        )

        # Auto-resubscribe to previously subscribed topics on reconnect
        if self._subscribed_topics and self._stats.reconnect_count > 0:
            has_recovery_state = bool(self._recovery_state)
            self._fire_task(
                self._resubscribe_after_reconnect(
                    recover=self._server_recovery_enabled and has_recovery_state,
                )
            )

        # Flush offline queue after server is ready
        if self._offline_queue.size > 0:
            self._fire_task(self._flush_offline_queue())

    async def _resubscribe_after_reconnect(self, *, recover: bool) -> None:
        """Re-subscribe to all previously subscribed topics after reconnection."""
        topics = list(self._subscribed_topics)
        # Clear so subscribe() treats them as new
        self._subscribed_topics.clear()
        logger.info("Re-subscribing to %d topics (recover=%s)", len(topics), recover)
        try:
            ok = await self.subscribe(topics, recover=recover)
            if not ok:
                # Restore so the next reconnect can try again
                self._subscribed_topics.update(topics)
                logger.warning(
                    "Re-subscribe failed, topics preserved for next reconnect"
                )
        except BaseException:
            # Restore topics on cancellation or any exception
            self._subscribed_topics.update(topics)
            raise

    async def _flush_offline_queue(self) -> None:
        """Replay queued messages after reconnection."""
        messages = self._offline_queue.drain()
        if not messages:
            return
        logger.info("Flushing %d offline-queued messages", len(messages))
        for i, (encoded, priority) in enumerate(messages):
            ok = await self._connection.send(encoded)
            if not ok:
                remaining = messages[i:]
                logger.warning(
                    "Offline queue flush interrupted, re-enqueuing %d messages",
                    len(remaining),
                )
                for msg, pri in remaining:
                    self._offline_queue.enqueue(msg, priority=pri)
                break

    def _handle_server_hello(self, event: WSEEvent) -> None:
        """Handle server_hello with negotiated features and limits."""
        p = event.payload
        self._server_features = p.get("features", {})

        max_msg_size = p.get("max_message_size")
        if max_msg_size and isinstance(max_msg_size, int):
            self._server_max_message_size = max_msg_size

        rate_limit = p.get("rate_limit")
        if rate_limit and isinstance(rate_limit, (int, float)):
            self._server_rate_limit = int(rate_limit)

        # Complete ECDH key exchange if server confirmed encryption
        if (
            self._security is not None
            and p.get("features", {}).get("encryption")
            and p.get("encryption_public_key")
        ):
            import base64

            server_pubkey_bytes = base64.b64decode(p["encryption_public_key"])
            self._security.derive_shared_secret(server_pubkey_bytes)
            logger.info("E2E encryption enabled (ECDH P-256 + AES-GCM-256)")

        server_version = p.get("server_version", "unknown")
        logger.info(
            "Server hello: version=%s, features=%s",
            server_version,
            list(self._server_features.keys()),
        )

    def _handle_subscription_update(self, event: WSEEvent) -> None:
        p = event.payload
        success_topics = p.get("success_topics", [])
        failed_topics = p.get("failed_topics", [])
        if failed_topics:
            logger.warning("Subscription failed for: %s", failed_topics)
        if success_topics:
            logger.debug("Subscribed to: %s", success_topics)

        # Store recovery positions from server response
        recovery = p.get("recovery", {})
        for topic, info in recovery.items():
            if isinstance(info, dict):
                epoch = info.get("epoch")
                offset = info.get("offset")
                if epoch is not None and offset is not None:
                    self._recovery_state[topic] = (str(epoch), int(offset))
                    logger.debug(
                        "Recovery position for %s: epoch=%s offset=%d",
                        topic,
                        epoch,
                        offset,
                    )

    def _handle_error(self, event: WSEEvent) -> None:
        """Handle server error with code classification (matches TS error handler)."""
        p = event.payload
        code = p.get("code", "UNKNOWN_ERROR")
        message = p.get("message", "Unknown error")
        recoverable = p.get("recoverable", True)
        severity = p.get("severity", "error")

        if code == "AUTH_FAILED":
            logger.error("Auth failed: %s", message)
        elif code in ("RATE_LIMITED", "RATE_LIMIT_EXCEEDED") or "Rate limit" in message:
            retry_after = p.get("retry_after") or p.get("retryAfter", 0)
            logger.warning(
                "Rate limit exceeded (retry after %ss): %s", retry_after, message
            )
        elif code == "SUBSCRIPTION_FAILED":
            topics = p.get("details", {}).get("topics", [])
            logger.warning("Subscription failed for %s: %s", topics, message)
        elif code in ("PROTOCOL_ERROR", "INVALID_MESSAGE"):
            logger.error(
                "Critical error [%s]: %s (recoverable=%s)", code, message, recoverable
            )
        elif severity == "fatal":
            logger.error("Fatal server error [%s]: %s", code, message)
        else:
            logger.warning("Server error [%s]: %s", code, message)
        # User handlers invoked by _dispatch_event fall-through (not internal-only)

    def _handle_health_check(self, event: WSEEvent) -> None:
        self._fire_task(self._respond_health_check())

    async def _respond_health_check(self) -> None:
        stats = self.get_stats()
        diag = self._network_monitor.analyze()
        await self.send(
            "health_check_response",
            {
                "client_version": CLIENT_VERSION,
                "stats": stats,
                "diagnostics": {
                    "quality": diag.quality.value,
                    "latency_ms": diag.round_trip_time,
                    "jitter_ms": diag.jitter,
                },
                "queue_size": self._event_queue.qsize(),
            },
            priority=MessagePriority.CRITICAL,
        )

    def _handle_health_check_response(self, event: WSEEvent) -> None:
        """Handle health check response from server (update diagnostics)."""
        logger.debug("Health check response: %s", event.payload)

    def _handle_config_update(self, event: WSEEvent) -> None:
        """Handle dynamic config updates from server."""
        p = event.payload
        if "heartbeat_interval" in p:
            logger.debug("Config: heartbeat_interval=%s", p["heartbeat_interval"])
        if "rate_limit" in p:
            self._server_rate_limit = p["rate_limit"]
        logger.debug("Config update: %s", p)

    def _handle_snapshot_complete(self, event: WSEEvent) -> None:
        """Handle snapshot completion notification."""
        topics = event.payload.get("topics", [])
        count = event.payload.get("count", 0)
        self._snapshot_requested = False
        logger.debug("Snapshot complete: %d events for topics %s", count, topics)
        # User handlers invoked by _dispatch_event fall-through (not internal-only)

    def _handle_rate_limit_warning(self, event: WSEEvent) -> None:
        """Handle rate limit warning from server."""
        remaining = event.payload.get("remaining", 0)
        limit = event.payload.get("limit", 0)
        logger.warning("Rate limit warning: %d/%d remaining", remaining, limit)
        # User handlers invoked by _dispatch_event fall-through (not internal-only)

    def _handle_connection_quality(self, event: WSEEvent) -> None:
        """Handle connection quality update from server."""
        p = event.payload
        quality_str = p.get("quality", "unknown")
        latency = p.get("latency_ms", 0)

        try:
            self._stats.connection_quality = ConnectionQuality(quality_str)
        except ValueError:
            self._stats.connection_quality = ConnectionQuality.UNKNOWN
        self._stats.last_latency_ms = latency

    def _handle_connection_state_change(self, event: WSEEvent) -> None:
        """Handle connection state change notification from server."""
        logger.debug("Server connection state: %s", event.payload)
        # User handlers invoked by _dispatch_event fall-through (not internal-only)

    def _handle_sync_request(self, event: WSEEvent) -> None:
        """Respond to server sync request with current client state."""
        self._fire_task(self._respond_sync_request())

    async def _respond_sync_request(self) -> None:
        await self.send(
            "sync_response",
            {
                "client_version": CLIENT_VERSION,
                "protocol_version": PROTOCOL_VERSION,
                "sequence": self._sequencer.current_sequence,
                "subscriptions": list(self._subscribed_topics),
                "last_update": int(time.time() * 1000),
            },
            priority=MessagePriority.HIGH,
        )

    def _handle_metrics_request(self, event: WSEEvent) -> None:
        """Respond to server metrics request."""
        self._fire_task(self._respond_metrics_request())

    async def _respond_metrics_request(self) -> None:
        diag = self._network_monitor.analyze()
        await self.send(
            "metrics_response",
            {
                "client_version": CLIENT_VERSION,
                "connection_stats": {
                    "messages_received": self._stats.messages_received,
                    "messages_sent": self._stats.messages_sent,
                    "bytes_received": self._stats.bytes_received,
                    "bytes_sent": self._stats.bytes_sent,
                },
                "queue_stats": {
                    "size": self._event_queue.qsize(),
                    "max_size": self._event_queue.maxsize,
                },
                "diagnostics": {
                    "quality": diag.quality.value,
                    "latency_ms": diag.round_trip_time,
                    "jitter_ms": diag.jitter,
                    "packet_loss": diag.packet_loss,
                },
                "circuit_breaker": {
                    "state": self._circuit_breaker.state.value,
                },
                "subscriptions": list(self._subscribed_topics),
                "event_sequencer": self._sequencer.get_stats(),
                "timestamp": int(time.time() * 1000),
            },
            priority=MessagePriority.HIGH,
        )

    def _handle_priority_message(self, event: WSEEvent) -> None:
        """Unwrap priority_message and re-route the inner message."""
        inner = event.payload.get("message")
        if isinstance(inner, dict):
            inner_event = WSEEvent(
                type=inner.get("t", inner.get("type", "unknown")),
                payload=inner.get("p", inner.get("payload", {})),
                id=inner.get("id"),
                sequence=inner.get("seq"),
                timestamp=inner.get("ts"),
                version=inner.get("v", PROTOCOL_VERSION),
                priority=inner.get("pri"),
                correlation_id=inner.get("cid"),
            )
            self._dispatch_event(inner_event)

    def _handle_pong_event(self, event: WSEEvent) -> None:
        """Handle PONG events — record latency in NetworkMonitor."""
        ts = event.payload.get("client_timestamp") or event.payload.get("timestamp")
        if ts and isinstance(ts, (int, float)):
            latency = time.time() * 1000 - ts
            if 0 <= latency < 60000:  # Sanity: <60s
                self._network_monitor.record_pong(latency)
                self._stats.last_latency_ms = latency

    def _on_transport_pong(self, latency_ms: float) -> None:
        """Callback from ConnectionManager for plain-text PONG frames."""
        self._network_monitor.record_pong(latency_ms)
        self._stats.last_latency_ms = latency_ms

    def _handle_ping_event(self, event: WSEEvent) -> None:
        """Backup PING handler (primary is in ConnectionManager)."""
        pass

    def _handle_forward_event(self, event: WSEEvent) -> None:
        """Generic handler for response events (forwarded by _dispatch_event)."""
        pass  # User handlers invoked by _dispatch_event fall-through

    def _on_state_change(self, state: ConnectionState) -> None:
        if state == ConnectionState.RECONNECTING:
            self._stats.reconnect_count += 1
            self._server_ready_event.clear()
            self._snapshot_requested = False
            self._sequencer.reset()
        elif state == ConnectionState.CONNECTED:
            pass  # record_ping() is called per-heartbeat via on_ping_sent
        elif state == ConnectionState.DISCONNECTED:
            self._server_ready_event.clear()
            # Signal iterator to stop on terminal disconnect.
            # DISCONNECTED only fires for permanent closes (normal close,
            # going_away). Transient errors go through RECONNECTING, not here.
            # Skip if disconnect() already enqueued the sentinel.
            if not self._disconnecting:
                try:
                    self._event_queue.put_nowait(None)
                except asyncio.QueueFull:
                    try:
                        self._event_queue.get_nowait()
                        self._event_queue.put_nowait(None)
                    except (asyncio.QueueEmpty, asyncio.QueueFull):
                        pass
