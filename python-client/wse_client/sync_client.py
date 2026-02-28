# =============================================================================
# WSE Python Client -- Synchronous Wrapper
# =============================================================================
#
# Thread-based wrapper around AsyncWSEClient for blocking usage.
# =============================================================================

from __future__ import annotations

import asyncio
import queue
import threading
from typing import Any, Callable

from ._logging import logger
from .client import AsyncWSEClient
from .errors import WSEConnectionError, WSETimeoutError
from .types import (
    ConnectionQuality,
    ConnectionState,
    ReconnectConfig,
    WSEEvent,
)


class SyncWSEClient:
    """Blocking / thread-based WSE client.

    Runs an :class:`AsyncWSEClient` on a background thread. All public
    methods are thread-safe and block until complete.

    Args:
        url: WebSocket server URL, e.g. ``"ws://localhost:5006/wse"``.
        token: JWT token for authentication.
        topics: Topics to auto-subscribe after connecting.
        reconnect: Reconnection config.
        extra_headers: Additional HTTP headers for the handshake.
        queue_size: Max events buffered for ``recv()`` (default 1000).

    Example (pull)::

        client = SyncWSEClient("ws://localhost:5006/wse", token="jwt")
        client.connect()
        client.subscribe(["notifications"])
        event = client.recv(timeout=5.0)
        client.close()

    Example (callbacks)::

        @client.on("notifications")
        def handle(event):
            print(event.payload)

        client.run_forever()
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
    ) -> None:
        self._url = url
        self._token = token
        self._initial_topics = topics
        self._reconnect = reconnect
        self._extra_headers = extra_headers
        self._queue_size = queue_size

        self._event_queue: queue.Queue[WSEEvent | None] = queue.Queue(
            maxsize=queue_size
        )
        self._handlers: dict[str, list[Callable[[WSEEvent], Any]]] = {}
        self._wildcard_handlers: list[Callable[[WSEEvent], Any]] = []

        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: AsyncWSEClient | None = None
        self._running = False
        self._connected_event = threading.Event()
        self._connect_error: Exception | None = None

    # -- Lifecycle ------------------------------------------------------------

    def connect(self, timeout: float = 15.0) -> None:
        """Connect in a background thread.  Blocks until ready."""
        if self._running:
            return

        self._running = True
        self._connected_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="wse-client"
        )
        self._thread.start()

        if not self._connected_event.wait(timeout=timeout):
            self._running = False
            # Stop the background thread to prevent orphaned thread on retry
            if self._loop and self._client:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self._client.disconnect(), self._loop
                    ).result(timeout=3.0)
                except Exception:
                    pass
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=3.0)
            raise WSETimeoutError(f"Connection timed out after {timeout}s")

        # Check if connection failed in background thread
        err = getattr(self, "_connect_error", None)
        if err is not None:
            self._running = False
            raise WSEConnectionError(f"Connection failed: {err}") from err

    def close(self) -> None:
        """Disconnect and stop the background thread."""
        self._running = False
        if self._loop and self._client:
            future = asyncio.run_coroutine_threadsafe(
                self._client.disconnect(), self._loop
            )
            try:
                future.result(timeout=5.0)
            except Exception:
                pass
        # Signal queue consumers
        try:
            self._event_queue.put_nowait(None)
        except queue.Full:
            pass

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def disconnect(self) -> None:
        """Alias for close."""
        self.close()

    # -- Send / Subscribe -----------------------------------------------------

    def send(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 5,
    ) -> bool:
        """Send a message (blocks until sent)."""
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.send(type, payload, priority=priority), self._loop
            )
            return future.result(timeout=5.0)
        except Exception:
            return False

    def subscribe(self, topics: list[str], *, recover: bool = False) -> bool:
        """Subscribe to topics, optionally recovering missed messages.

        Args:
            topics: Topic names to subscribe to.
            recover: If True, request message recovery using stored positions.

        Returns:
            True if sent, False if not connected.
        """
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.subscribe(topics, recover=recover), self._loop
            )
            return future.result(timeout=5.0)
        except Exception:
            return False

    def unsubscribe(self, topics: list[str]) -> bool:
        """Unsubscribe from topics. Blocks until sent.

        Args:
            topics: Topic names to unsubscribe from.

        Returns:
            True if sent, False if not connected.
        """
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.unsubscribe(topics), self._loop
            )
            return future.result(timeout=5.0)
        except Exception:
            return False

    def send_with_retry(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 5,
        max_retries: int = 5,
    ) -> bool:
        """Send with exponential backoff retry."""
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.send_with_retry(
                    type, payload, priority=priority, max_retries=max_retries
                ),
                self._loop,
            )
            return future.result(timeout=30.0)
        except Exception:
            return False

    def send_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> bool:
        """Send multiple messages in a single batch frame."""
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.send_batch(messages), self._loop
            )
            return future.result(timeout=10.0)
        except Exception:
            return False

    def request_snapshot(self, topics: list[str] | None = None) -> bool:
        """Request a state snapshot. Blocks until sent.

        Args:
            topics: Topics to snapshot. Defaults to subscribed topics.

        Returns:
            True if sent, False if not connected.
        """
        if not self._loop or not self._client:
            return False
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.request_snapshot(topics), self._loop
            )
            return future.result(timeout=5.0)
        except Exception:
            return False

    def force_reconnect(self) -> None:
        """Force a reconnection."""
        if not self._loop or not self._client:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.force_reconnect(), self._loop
            )
            future.result(timeout=15.0)
        except Exception:
            pass

    def change_endpoint(self, new_url: str) -> None:
        """Change endpoint and reconnect."""
        if not self._loop or not self._client:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.change_endpoint(new_url), self._loop
            )
            future.result(timeout=15.0)
        except Exception:
            pass

    # -- Receive --------------------------------------------------------------

    def recv(self, timeout: float | None = None) -> WSEEvent:
        """Receive the next event. Blocks until available.

        Args:
            timeout: Max seconds to wait. ``None`` blocks indefinitely.

        Returns:
            The next :class:`~wse_client.types.WSEEvent`.

        Raises:
            WSETimeoutError: If *timeout* expires.
            WSEConnectionError: If the connection is closed.
        """
        try:
            event = self._event_queue.get(timeout=timeout)
        except queue.Empty:
            raise WSETimeoutError("recv() timed out")

        if event is None:
            raise WSEConnectionError("Connection closed")
        return event

    # -- Handler registration -------------------------------------------------

    def on(
        self, event_type: str
    ) -> Callable[[Callable[[WSEEvent], Any]], Callable[[WSEEvent], Any]]:
        """Decorator for event handlers (sync callbacks)."""

        def decorator(fn: Callable[[WSEEvent], Any]) -> Callable[[WSEEvent], Any]:
            self._handlers.setdefault(event_type, []).append(fn)
            return fn

        return decorator

    def on_any(self, fn: Callable[[WSEEvent], Any]) -> Callable[[WSEEvent], Any]:
        """Register wildcard handler."""
        self._wildcard_handlers.append(fn)
        return fn

    # -- Blocking run ---------------------------------------------------------

    def run_forever(self) -> None:
        """Block and dispatch events to registered handlers.

        Stops when ``close()`` is called or connection lost.
        """
        if not self._running:
            self.connect()

        while self._running:
            try:
                event = self._event_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if event is None:
                break

            self._dispatch_to_handlers(event)

    # -- Handler management ---------------------------------------------------

    def off(self, event_type: str, fn: Callable[[WSEEvent], Any]) -> None:
        """Remove a specific handler."""
        handlers = self._handlers.get(event_type, [])
        if fn in handlers:
            handlers.remove(fn)

    # -- Properties -----------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected

    @property
    def is_ready(self) -> bool:
        if self._client:
            return self._client.is_ready
        return False

    @property
    def is_fully_ready(self) -> bool:
        if self._client:
            return self._client.is_fully_ready
        return False

    @property
    def state(self) -> ConnectionState:
        if self._client:
            return self._client.state
        return ConnectionState.DISCONNECTED

    @property
    def connection_quality(self) -> ConnectionQuality:
        if self._client:
            return self._client.connection_quality
        return ConnectionQuality.UNKNOWN

    @property
    def queue_size(self) -> int:
        return self._event_queue.qsize()

    @property
    def subscribed_topics(self) -> set[str]:
        if self._client:
            return self._client.subscribed_topics
        return set()

    @property
    def recovery_enabled(self) -> bool:
        """Whether the server supports message recovery."""
        if self._client:
            return self._client.recovery_enabled
        return False

    @property
    def recovery_state(self) -> dict[str, tuple[str, int]]:
        """Per-topic recovery positions: ``{topic: (epoch, offset)}``."""
        if self._client:
            return self._client.recovery_state
        return {}

    def get_stats(self) -> dict[str, Any]:
        if self._client:
            return self._client.get_stats()
        return {}

    # -- Internal -------------------------------------------------------------

    def _run_loop(self) -> None:
        """Background thread: run the async event loop."""
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_main())
        except Exception as exc:
            logger.error("Background loop error: %s", exc)
        finally:
            self._loop.close()
            self._loop = None

    async def _async_main(self) -> None:
        """Async entry point in the background thread."""
        self._client = AsyncWSEClient(
            self._url,
            token=self._token,
            topics=self._initial_topics,
            reconnect=self._reconnect,
            extra_headers=self._extra_headers,
            queue_size=self._queue_size,
        )

        self._connect_error: Exception | None = None
        try:
            await self._client.connect()
            self._connected_event.set()

            async for event in self._client:
                if not self._running:
                    break
                try:
                    self._event_queue.put_nowait(event)
                except queue.Full:
                    # Drop oldest
                    try:
                        self._event_queue.get_nowait()
                        self._event_queue.put_nowait(event)
                    except (queue.Empty, queue.Full):
                        pass

        except Exception as exc:
            self._connect_error = exc
            logger.error("Client error: %s", exc)
        finally:
            await self._client.disconnect()
            self._connected_event.set()  # Unblock connect() if still waiting

    def _dispatch_to_handlers(self, event: WSEEvent) -> None:
        """Call sync handlers for the event."""
        handlers = self._handlers.get(event.type, []) + self._wildcard_handlers
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                logger.error("Handler error for '%s': %s", event.type, exc)
