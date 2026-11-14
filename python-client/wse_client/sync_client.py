# =============================================================================
# WSE Python Client -- Synchronous Wrapper
# =============================================================================
#
# Thread-based wrapper around AsyncWSEClient for blocking usage.
# =============================================================================

from __future__ import annotations

import asyncio
import concurrent.futures
import queue
import threading
from typing import Any, Callable, TypeVar

from ._logging import logger
from .client import AsyncWSEClient
from .constants import (
    DEFAULT_MAX_MESSAGE_SIZE,
    RATE_LIMIT_REFILL_RATE,
    RECOVERY_TIMEOUT,
)
from .errors import WSEConnectionError, WSEError, WSETimeoutError
from .types import (
    ConnectionQuality,
    ConnectionState,
    LoadBalancingStrategy,
    ReconnectConfig,
    SendResult,
    TokenProvider,
    WSEEvent,
)

T = TypeVar("T")


class _StreamEnd:
    """Queue marker: the background session ended (cleanly if error is None).

    ``seen`` turns True once a consumer got it; the marker stays queued so
    every later ``recv()`` reports the end too.
    """

    __slots__ = ("error", "seen")

    def __init__(self, error: BaseException | None) -> None:
        self.error = error
        self.seen = False


class _Session:
    """One connect() attempt: its own thread, event loop and client.

    A session that was abandoned (connect() timed out, close() was called)
    can still be finishing on its thread; it only ever touches its own
    fields, so it can never clobber the session that replaced it.
    """

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.thread: threading.Thread | None = None
        self.client: AsyncWSEClient | None = None
        self.main_task: asyncio.Task[None] | None = None
        self.ready = threading.Event()
        self.connected = False
        self.connect_error: BaseException | None = None


class SyncWSEClient:
    """Blocking / thread-based WSE client.

    Runs an :class:`AsyncWSEClient` on a background thread. All public
    methods are thread-safe and block until complete; stats and property
    reads that touch the client's state run on its event loop.

    Takes the same keyword arguments as :class:`AsyncWSEClient`.

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

    When the client ends for good (credentials rejected, E2E failure,
    reconnect attempts exhausted), ``recv()`` and ``run_forever()`` raise the
    reason, :attr:`background_error` holds it and the handler set with
    :meth:`set_error_handler` is called. Reconnects in between are invisible.
    """

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        topics: list[str] | None = None,
        reconnect: ReconnectConfig | None = None,
        extra_headers: dict[str, str] | None = None,
        queue_size: int = 1000,
        encryption: bool = False,
        e2e_server_keys: list[str] | None = None,
        e2e_unauthenticated: bool = False,
        e2e_legacy: bool = False,
        endpoints: list[str] | None = None,
        load_balancing: LoadBalancingStrategy = LoadBalancingStrategy.WEIGHTED_RANDOM,
        rate_limit: float | None = RATE_LIMIT_REFILL_RATE,
        rate_limit_burst: int | None = None,
        offline_queue: bool = False,
        max_message_size: int | None = DEFAULT_MAX_MESSAGE_SIZE,
        recovery_timeout: float = RECOVERY_TIMEOUT,
        wire_format: str = "json",
        signing_key: Any = None,
        server_signing_keys: list[str] | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._initial_topics = topics
        self._client_kwargs: dict[str, Any] = {
            "token": token,
            "token_provider": token_provider,
            "topics": topics,
            "reconnect": reconnect,
            "extra_headers": extra_headers,
            "queue_size": queue_size,
            "encryption": encryption,
            "e2e_server_keys": e2e_server_keys,
            "e2e_unauthenticated": e2e_unauthenticated,
            "e2e_legacy": e2e_legacy,
            "endpoints": endpoints,
            "load_balancing": load_balancing,
            "rate_limit": rate_limit,
            "rate_limit_burst": rate_limit_burst,
            "offline_queue": offline_queue,
            "max_message_size": max_message_size,
            "recovery_timeout": recovery_timeout,
            "wire_format": wire_format,
            "signing_key": signing_key,
            "server_signing_keys": server_signing_keys,
        }
        self._queue_size = queue_size

        self._event_queue: queue.Queue[WSEEvent | _StreamEnd] = queue.Queue(
            maxsize=queue_size
        )
        self._handlers: dict[str, list[Callable[[WSEEvent], Any]]] = {}
        self._wildcard_handlers: list[Callable[[WSEEvent], Any]] = []

        self._lock = threading.Lock()
        self._session: _Session | None = None
        self._running = False
        self._background_error: BaseException | None = None
        self._on_error: Callable[[Exception], Any] | None = None

    # -- Lifecycle ------------------------------------------------------------

    def connect(self, timeout: float = 15.0) -> None:
        """Connect in a background thread.  Blocks until ready.

        Raises:
            WSETimeoutError: Not connected within *timeout* seconds.
            WSEAuthError / WSEEncryptionError: As from ``AsyncWSEClient.connect()``.
            WSEConnectionError: Any other connection failure.
        """
        with self._lock:
            if self._running and self._session is not None:
                return
            session = _Session()
            self._session = session
            self._running = True
            self._background_error = None
            self._purge_end_markers()
            thread = threading.Thread(
                target=self._run_session,
                args=(session,),
                daemon=True,
                name="wse-client",
            )
            session.thread = thread
        thread.start()

        if not session.ready.wait(timeout=timeout):
            self._abandon(session)
            raise WSETimeoutError(f"Connection timed out after {timeout}s")

        err = session.connect_error
        if err is not None or not session.connected:
            self._abandon(session)
            if isinstance(err, WSEError):
                raise err
            raise WSEConnectionError(f"Connection failed: {err}") from err

    def set_error_handler(self, handler: Callable[[Exception], Any]) -> None:
        """Set a callback for the error that ends the client after connect().

        Called once from the background thread when the client ends for good
        (credentials rejected, E2E failure, reconnect attempts exhausted).
        Reconnects are not errors and do not call it.
        """
        self._on_error = handler

    @property
    def background_error(self) -> BaseException | None:
        """The error that ended the background client (None if healthy)."""
        return self._background_error

    def close(self) -> None:
        """Disconnect and stop the background thread."""
        with self._lock:
            session = self._session
            self._session = None
            self._running = False
        if session is not None:
            self._stop_session(session, join_timeout=5.0)
        # Signal queue consumers (recv / run_forever)
        self._put_end_marker(_StreamEnd(None))

    def disconnect(self) -> None:
        """Alias for close."""
        self.close()

    def set_token(self, token: str | None) -> None:
        """Use *token* for the next connect / reconnect (see
        :meth:`AsyncWSEClient.set_token`)."""
        self._token = token
        self._client_kwargs["token"] = token
        client = self._live_client()
        if client is not None:
            self._call_on_loop(lambda: client.set_token(token), timeout=5.0)

    # -- Send / Subscribe -----------------------------------------------------

    def send(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 5,
        signed: bool = False,
        nonce: str | None = None,
    ) -> SendResult:
        """Send a message (blocks until sent). See :meth:`AsyncWSEClient.send`."""
        result = self._run(
            lambda c: c.send(
                type, payload, priority=priority, signed=signed, nonce=nonce
            ),
            timeout=15.0,
            default=SendResult.FAILED,
        )
        return result

    def subscribe(self, topics: list[str], *, recover: bool = False) -> bool:
        """Subscribe to topics, optionally recovering missed messages.

        Args:
            topics: Topic names to subscribe to.
            recover: If True, request message recovery using stored positions.

        Returns:
            True if sent, False if not connected.
        """
        return self._run(
            lambda c: c.subscribe(topics, recover=recover), timeout=15.0, default=False
        )

    def unsubscribe(self, topics: list[str]) -> bool:
        """Unsubscribe from topics. Blocks until sent.

        Args:
            topics: Topic names to unsubscribe from.

        Returns:
            True if sent, False if not connected.
        """
        return self._run(lambda c: c.unsubscribe(topics), timeout=15.0, default=False)

    def send_with_retry(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 5,
        max_retries: int = 5,
    ) -> SendResult:
        """Send with exponential backoff retry."""
        return self._run(
            lambda c: c.send_with_retry(
                type, payload, priority=priority, max_retries=max_retries
            ),
            timeout=30.0,
            default=SendResult.FAILED,
        )

    def send_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> bool:
        """Send multiple messages in a single batch frame."""
        return self._run(lambda c: c.send_batch(messages), timeout=10.0, default=False)

    def request_snapshot(self, topics: list[str] | None = None) -> bool:
        """Request a state snapshot. Blocks until sent.

        Args:
            topics: Topics to snapshot. Defaults to subscribed topics.

        Returns:
            True if sent, False if not connected.
        """
        return self._run(
            lambda c: c.request_snapshot(topics), timeout=10.0, default=False
        )

    def force_reconnect(self) -> None:
        """Force a reconnection."""
        self._run(lambda c: c.force_reconnect(), timeout=15.0, default=None)

    def change_endpoint(self, new_url: str) -> None:
        """Change endpoint and reconnect."""
        self._run(lambda c: c.change_endpoint(new_url), timeout=25.0, default=None)

    # -- Receive --------------------------------------------------------------

    def recv(self, timeout: float | None = None) -> WSEEvent:
        """Receive the next event. Blocks until available.

        Registered ``on()`` handlers are invoked before the event is returned.

        Args:
            timeout: Max seconds to wait. ``None`` blocks indefinitely.

        Returns:
            The next :class:`~wse_client.types.WSEEvent`.

        Raises:
            WSETimeoutError: If *timeout* expires.
            WSEConnectionError: The client was closed (or failed).
            WSEAuthError / WSEEncryptionError: Why the client ended for good.
        """
        try:
            item = self._event_queue.get(timeout=timeout)
        except queue.Empty:
            raise WSETimeoutError("recv() timed out")

        if isinstance(item, _StreamEnd):
            item.seen = True
            self._requeue_end(item)
            if item.error is not None:
                if isinstance(item.error, WSEError):
                    raise item.error
                raise WSEConnectionError(
                    f"Connection failed: {item.error}"
                ) from item.error
            raise WSEConnectionError("Connection closed")

        self._dispatch_to_handlers(item)
        return item

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

        Returns after ``close()``. When the client ends for good it raises the
        reason (:class:`WSEAuthError`, :class:`WSEEncryptionError`,
        :class:`WSEConnectionError`) so a supervisor sees the failure.
        Reconnects in between are invisible.

        Connects first when not connected -- unless the client ended with an
        error nobody has seen yet: that error is raised first, and the next
        call connects again.
        """
        if not self._running and not self._pending_unseen_error():
            self.connect()

        while True:
            try:
                item = self._event_queue.get(timeout=1.0)
            except queue.Empty:
                if not self._running and self._event_queue.empty():
                    return  # ended without a marker (defensive)
                continue

            if isinstance(item, _StreamEnd):
                item.seen = True
                self._requeue_end(item)
                if item.error is not None:
                    if isinstance(item.error, WSEError):
                        raise item.error
                    raise WSEConnectionError(
                        f"Connection failed: {item.error}"
                    ) from item.error
                return

            self._dispatch_to_handlers(item)

    # -- Handler management ---------------------------------------------------

    def off(self, event_type: str, fn: Callable[[WSEEvent], Any]) -> None:
        """Remove a specific handler."""
        handlers = self._handlers.get(event_type, [])
        if fn in handlers:
            handlers.remove(fn)

    # -- Properties -----------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        client = self._live_client()
        return client is not None and client.is_connected

    @property
    def is_ready(self) -> bool:
        client = self._live_client()
        return client is not None and client.is_ready

    @property
    def is_fully_ready(self) -> bool:
        client = self._live_client()
        return client is not None and client.is_fully_ready

    @property
    def state(self) -> ConnectionState:
        client = self._live_client()
        if client is not None:
            return client.state
        return ConnectionState.DISCONNECTED

    @property
    def last_error(self) -> BaseException | None:
        """Why the client ended for good (same as :attr:`background_error`)."""
        return self._background_error

    @property
    def connection_quality(self) -> ConnectionQuality:
        client = self._live_client()
        if client is None:
            return ConnectionQuality.UNKNOWN
        return self._call_on_loop(
            lambda: client.connection_quality,
            timeout=5.0,
            default=ConnectionQuality.UNKNOWN,
        )

    @property
    def queue_size(self) -> int:
        return self._event_queue.qsize()

    @property
    def subscribed_topics(self) -> set[str]:
        client = self._live_client()
        if client is None:
            return set()
        return self._call_on_loop(
            lambda: client.subscribed_topics, timeout=5.0, default=set()
        )

    @property
    def recovery_enabled(self) -> bool:
        """Whether the server supports message recovery."""
        client = self._live_client()
        return client is not None and client.recovery_enabled

    @property
    def recovery_state(self) -> dict[str, tuple[str, int]]:
        """Per-topic recovery anchors: ``{topic: (epoch, offset)}``."""
        client = self._live_client()
        if client is None:
            return {}
        return self._call_on_loop(
            lambda: client.recovery_state, timeout=5.0, default={}
        )

    def get_stats(self) -> dict[str, Any]:
        """Client statistics, read on the client's event loop (thread-safe)."""
        client = self._live_client()
        if client is None:
            return {}
        return self._call_on_loop(client.get_stats, timeout=5.0, default={})

    # -- Internal -------------------------------------------------------------

    def _live_client(self) -> AsyncWSEClient | None:
        session = self._session
        if session is None or not session.connected:
            return None
        return session.client

    def _run(
        self,
        make: Callable[[AsyncWSEClient], Any],
        *,
        timeout: float,
        default: Any,
    ) -> Any:
        """Run a client coroutine on the session loop and wait for it."""
        session = self._session
        if session is None or not session.connected:
            return default
        loop, client = session.loop, session.client
        if loop is None or client is None or loop.is_closed():
            return default
        coro = make(client)
        try:
            future = asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError:
            coro.close()  # the loop closed meanwhile
            return default
        try:
            return future.result(timeout=timeout)
        except WSEError:
            raise
        except Exception:
            future.cancel()
            return default

    def _call_on_loop(
        self, fn: Callable[[], T], *, timeout: float, default: Any = None
    ) -> T:
        """Run a plain function on the session loop, so reads of the client's
        state never race the loop thread."""
        session = self._session
        loop = session.loop if session is not None else None
        if loop is None or loop.is_closed():
            return default
        if session is not None and session.thread is threading.current_thread():
            return fn()
        result: concurrent.futures.Future[T] = concurrent.futures.Future()

        def run() -> None:
            try:
                result.set_result(fn())
            except BaseException as exc:
                result.set_exception(exc)

        try:
            loop.call_soon_threadsafe(run)
            return result.result(timeout=timeout)
        except Exception:
            return default

    def _run_session(self, session: _Session) -> None:
        """Background thread: run the session's event loop."""
        loop = asyncio.new_event_loop()
        session.loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._session_main(session))
        except BaseException as exc:
            logger.error("Background loop error: %s", exc)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            loop.close()
            session.ready.set()  # unblock connect() if still waiting

    async def _session_main(self, session: _Session) -> None:
        """Async entry point in the background thread."""
        session.main_task = asyncio.current_task()
        client: AsyncWSEClient | None = None

        error: BaseException | None = None
        try:
            client = AsyncWSEClient(self._url, **self._client_kwargs)
            session.client = client
            await client.connect()
            session.connected = True
            session.ready.set()

            async for event in client:
                if self._session is not session:
                    break
                self._put_event(event)
        except asyncio.CancelledError:
            # close() or an abandoned connect(): finish the teardown below.
            current = asyncio.current_task()
            if current is not None:
                current.uncancel()
        except Exception as exc:
            error = exc
            if not session.connected:
                session.connect_error = exc
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except BaseException:
                    pass
            session.ready.set()
            self._session_ended(session, error)

    def _session_ended(self, session: _Session, error: BaseException | None) -> None:
        """Called on the session thread when its client stopped."""
        with self._lock:
            if self._session is not session or not session.connected:
                return  # stale session, or connect() reports it itself
            self._session = None
            self._running = False
            if error is not None:
                self._background_error = error
            self._put_end_marker(_StreamEnd(error))
        if error is None:
            return
        logger.error("Client stopped: %s", error)
        handler = self._on_error
        if handler is not None and isinstance(error, Exception):
            try:
                handler(error)
            except Exception:
                pass

    def _abandon(self, session: _Session) -> None:
        """Give up on a session whose connect() failed or timed out."""
        with self._lock:
            if self._session is session:
                self._session = None
                self._running = False
        self._stop_session(session, join_timeout=3.0)

    @staticmethod
    def _stop_session(session: _Session, *, join_timeout: float) -> None:
        """Cancel the session's main task (its finally disconnects) and join."""
        loop, task = session.loop, session.main_task
        if loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass  # loop already closed
        thread = session.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=join_timeout)

    def _put_event(self, event: WSEEvent) -> None:
        try:
            self._event_queue.put_nowait(event)
        except queue.Full:
            # Drop oldest
            try:
                self._event_queue.get_nowait()
                self._event_queue.put_nowait(event)
            except (queue.Empty, queue.Full):
                pass

    def _put_end_marker(self, end: _StreamEnd) -> None:
        try:
            self._event_queue.put_nowait(end)
        except queue.Full:
            try:
                self._event_queue.get_nowait()
                self._event_queue.put_nowait(end)
            except (queue.Empty, queue.Full):
                pass

    def _requeue_end(self, end: _StreamEnd) -> None:
        try:
            self._event_queue.put_nowait(end)
        except queue.Full:
            pass

    def _pending_unseen_error(self) -> bool:
        """An error end marker is queued that no consumer has got yet."""
        with self._event_queue.mutex:
            return any(
                isinstance(item, _StreamEnd)
                and item.error is not None
                and not item.seen
                for item in self._event_queue.queue
            )

    def _purge_end_markers(self) -> None:
        kept: list[WSEEvent | _StreamEnd] = []
        while True:
            try:
                kept.append(self._event_queue.get_nowait())
            except queue.Empty:
                break
        for item in kept:
            if not isinstance(item, _StreamEnd):
                self._event_queue.put_nowait(item)

    def _dispatch_to_handlers(self, event: WSEEvent) -> None:
        """Call sync handlers for the event."""
        handlers = self._handlers.get(event.type, []) + self._wildcard_handlers
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                logger.error("Handler error for '%s': %s", event.type, exc)
