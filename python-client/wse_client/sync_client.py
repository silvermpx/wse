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
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, TypeVar

from ._logging import logger
from .client import _IN_MEMORY, AsyncWSEClient
from .delivery import DeliveryConfig, TopicState
from .constants import (
    DEFAULT_MAX_MESSAGE_SIZE,
    RATE_LIMIT_REFILL_RATE,
    RECOVERY_TIMEOUT,
)
from .errors import WSEConnectionError, WSEError, WSETimeoutError
from .outbox import Offline, Outbox
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


@dataclass(eq=False, slots=True)
class _Outcome:
    """What became of the outbox's messages: for the handlers alone."""

    event: WSEEvent


class _Stream(queue.Queue):  # type: ignore[type-arg]
    """What recv() and run_forever() read: bounded for the server's events;
    an outcome and the end are never dropped, nor held back."""

    def put_always(self, item: WSEEvent | _StreamEnd | _Outcome) -> None:
        with self.not_full:
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def drop_ends(self) -> None:
        """The ends of earlier streams go; what came before them stays, in
        order, however much of it there is."""
        with self.not_full:
            kept = [item for item in self.queue if not isinstance(item, _StreamEnd)]
            gone = len(self.queue) - len(kept)
            if gone == 0:
                return
            self.queue.clear()
            self.queue.extend(kept)
            self.unfinished_tasks -= gone
            if self.unfinished_tasks == 0:
                self.all_tasks_done.notify_all()
            self.not_full.notify_all()

    def put_dropping(self, item: WSEEvent | _StreamEnd | _Outcome) -> None:
        """A full stream lets its oldest event go for *item*, never an
        outcome or the end."""
        with self.not_full:
            if 0 < self.maxsize <= self._qsize():
                for i, old in enumerate(self.queue):
                    if not isinstance(old, (_Outcome, _StreamEnd)):
                        del self.queue[i]
                        break
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()


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
        # connect() waits for the first connection; start() does not.
        self.wait = True


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
        offline: Offline | None = _IN_MEMORY,
        max_message_size: int | None = DEFAULT_MAX_MESSAGE_SIZE,
        recovery_timeout: float = RECOVERY_TIMEOUT,
        wire_format: str = "json",
        signing_key: Any = None,
        server_signing_keys: list[str] | None = None,
        e2e_bind_session: bool = True,
        cookie_name: str = "access_token",
        delivery: DeliveryConfig | None = None,
    ) -> None:
        self._url = url
        # recover-or-resync: each connect() takes up from where the last
        # session's consumer committed, not from the checkpoint given first.
        self._delivery = delivery
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
            "offline": offline,
            # One outbox across the sessions of connect() (made below, once the
            # stream its failures go to is there).
            "_outbox": None,
            "max_message_size": max_message_size,
            "recovery_timeout": recovery_timeout,
            "wire_format": wire_format,
            "signing_key": signing_key,
            "server_signing_keys": server_signing_keys,
            "e2e_bind_session": e2e_bind_session,
            "cookie_name": cookie_name,
            "delivery": delivery,
        }
        self._queue_size = queue_size

        self._event_queue = _Stream(maxsize=queue_size)
        self._handlers: dict[str, list[Callable[[WSEEvent], Any]]] = {}
        self._wildcard_handlers: list[Callable[[WSEEvent], Any]] = []
        # What one session did not get answered goes on the next.
        if offline is not None:
            self._client_kwargs["_outbox"] = Outbox(
                offline, report=self._storage_report
            )

        self._lock = threading.Lock()
        self._session: _Session | None = None
        # The application reads the stream (recv, run_forever): from then on
        # a full stream holds the client back instead of dropping.
        self._reading = False
        self._running = False
        # The client ended and its error handler runs: the end reaches the
        # stream once it returns.
        self._ending = False
        # Moves on every connect(): an end that a new connect() overtook
        # stays out of the new stream.
        self._generation = 0
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
        self._open(wait=True, timeout=timeout)

    def start(self) -> None:
        """Start in a background thread, and return at once: the connection
        is made there, a failed first attempt made again as any later one.
        What is sent meanwhile waits in the outbox."""
        self._open(wait=False, timeout=15.0)

    def _open(self, *, wait: bool, timeout: float) -> None:
        with self._lock:
            running = self._session if self._running else None
            if running is None:
                session = self._new_session(wait)
        if running is not None:
            # Another connect() is on its way: its outcome is this one's.
            if wait:
                self._outcome_of(running, timeout, abandon=False)
                # One started connects in the background: this call
                # returns once that connection is up.
                if not running.wait:
                    self._connected(running, timeout)
            return
        session.thread.start()  # type: ignore[union-attr]
        self._outcome_of(session, timeout, abandon=True)

    def _connected(self, session: _Session, timeout: float) -> None:
        """Waits for the connection *session* makes in the background:
        raises what ended it."""
        loop, client = session.loop, session.client
        if loop is None or client is None or loop.is_closed():
            raise WSEConnectionError("the client ended")
        try:
            future = asyncio.run_coroutine_threadsafe(client.connect(), loop)
        except RuntimeError:
            raise WSEConnectionError("the client ended") from None
        try:
            future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise WSETimeoutError(f"Connection timed out after {timeout}s") from None

    def _outcome_of(self, session: _Session, timeout: float, *, abandon: bool) -> None:
        """Waits for *session* to connect (or start): raises what it failed
        with; *abandon* lets go a session that does not make it."""
        if not session.ready.wait(timeout=timeout):
            if abandon:
                self._abandon(session)
            raise WSETimeoutError(f"Connection timed out after {timeout}s")

        err = session.connect_error
        if err is not None or not session.connected:
            if abandon:
                self._abandon(session)
            if isinstance(err, WSEError):
                raise err
            raise WSEConnectionError(f"Connection failed: {err}") from err

    def _new_session(self, wait: bool) -> _Session:
        """A session, made the current one, its thread not started yet
        (under the lock)."""
        session = _Session()
        session.wait = wait
        self._session = session
        self._running = True
        self._generation += 1
        self._background_error = None
        self._purge_end_markers()
        session.thread = threading.Thread(
            target=self._run_session,
            args=(session,),
            daemon=True,
            name="wse-client",
        )
        return session

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
            ending = self._ending
        if session is not None:
            self._stop_session(session, join_timeout=5.0)
        # Signal queue consumers (recv / run_forever), unless the client is
        # ending already: its end, with the error, is on its way.
        if not ending:
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
        live: bool = False,
        requires: list[str] | tuple[str, ...] | None = None,
        id: str | None = None,
        ttl: float | None = None,
    ) -> SendResult:
        """Send a message (blocks until sent, or until it waits in the
        outbox or for the topics it requires). See
        :meth:`AsyncWSEClient.send`."""
        result = self._run(
            lambda c: c.send(
                type,
                payload,
                priority=priority,
                signed=signed,
                nonce=nonce,
                live=live,
                requires=requires,
                id=id,
                ttl=ttl,
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

    def pause(self, reason: str) -> None:
        """Pause the connection for *reason*: it ends while any reason holds,
        and topics, positions and what waits in the outbox stay."""
        self._run(lambda c: c.pause(reason), timeout=15.0, default=None)

    def resume(self, reason: str) -> None:
        """Clear *reason*; once none is left the client connects again."""
        self._run(lambda c: c.resume(reason), timeout=15.0, default=None)

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
        until = None if timeout is None else time.monotonic() + timeout
        while True:
            try:
                self._reading = True
                left = None if until is None else max(0.0, until - time.monotonic())
                item = self._event_queue.get(timeout=left)
            except queue.Empty:
                raise WSETimeoutError("recv() timed out")
            if not isinstance(item, _Outcome):
                break
            # For the handlers alone: never what recv() returns.
            self._dispatch_to_handlers(item.event)

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
        """Decorator to register a handler (a sync callback) for one event
        type (the ``t`` of the messages, whatever topic they come on)."""

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
        if not self._running and not self._ending and not self._pending_unseen_error():
            self.connect()

        while True:
            try:
                self._reading = True
                item = self._event_queue.get(timeout=1.0)
            except queue.Empty:
                if not self._running and not self._ending and self._event_queue.empty():
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

            self._dispatch_to_handlers(
                item.event if isinstance(item, _Outcome) else item
            )

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

    @property
    def topic_states(self) -> dict[str, TopicState]:
        """Each topic held: whether the server took it, and whether its
        messages run without a break."""
        client = self._session_client()
        if client is None:
            return {}
        return self._call_on_loop(lambda: client.topic_states, timeout=5.0, default={})

    def commit(self, event: WSEEvent) -> None:
        """The consumer applied *event*; see :meth:`AsyncWSEClient.commit`.
        Taken in order with the calls after it."""
        self._later(lambda client: client.commit(event))

    def checkpoint(self) -> str:
        """What the consumer committed; see :meth:`AsyncWSEClient.checkpoint`."""
        client = self._session_client()
        if client is None:
            return ""
        return self._call_on_loop(client.checkpoint, timeout=5.0, default="")

    def resynced(self, round: int) -> None:
        """recover-or-resync: the application brought the topics of *round*
        up to date; see :meth:`AsyncWSEClient.resynced`."""
        self._later(lambda client: client.resynced(round))

    def resync(self, topics: list[str]) -> None:
        """Bring *topics* up to date; see :meth:`AsyncWSEClient.resync`."""
        wanted = list(topics)
        self._later(lambda client: client.resync(wanted))

    def get_stats(self) -> dict[str, Any]:
        """Client statistics, read on the client's event loop (thread-safe)."""
        client = self._live_client()
        if client is None:
            return {}
        return self._call_on_loop(client.get_stats, timeout=5.0, default={})

    # -- Internal -------------------------------------------------------------

    def _session_client(self) -> AsyncWSEClient | None:
        """The session's client, connected or not."""
        session = self._session
        return session.client if session is not None else None

    def _later(self, call: Callable[[AsyncWSEClient], Any]) -> None:
        """Runs *call* on the session loop, in order with what follows, without
        waiting for it."""
        session = self._session
        loop = session.loop if session is not None else None
        client = session.client if session is not None else None
        if loop is None or client is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(call, client)
        except RuntimeError:
            pass  # the loop closed meanwhile

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
            # What became of the outbox's messages reaches the handlers, in
            # order with the stream.
            client.on("delivered")(self._outcome)
            client.on("not_delivered")(self._outcome)
            if session.wait:
                await client.connect()
            else:
                await client.start()
            session.connected = True
            session.ready.set()

            async for event in client:
                if self._session is not session:
                    break
                await self._hand_over(event, session)
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
                if self._delivery is not None:
                    # The next session starts from what this one committed.
                    self._client_kwargs["delivery"] = replace(
                        self._delivery, checkpoint=client.checkpoint()
                    )
                try:
                    await client.disconnect()
                except BaseException:
                    pass
            session.ready.set()
            self._session_ended(session, error)

    def _session_ended(self, session: _Session, error: BaseException | None) -> None:
        """Called on the session thread when its client stopped. The error
        handler runs first: whoever sees the end in the stream (run_forever(),
        recv()) sees it after the handler."""
        with self._lock:
            if self._session is not session or not session.connected:
                return  # stale session, or connect() reports it itself
            self._session = None
            self._running = False
            self._ending = True
            generation = self._generation
            if error is not None:
                self._background_error = error
        try:
            if error is not None:
                logger.error("Client stopped: %s", error)
                handler = self._on_error
                if handler is not None and isinstance(error, Exception):
                    try:
                        handler(error)
                    except Exception:
                        pass
        finally:
            with self._lock:
                self._ending = False
                if self._generation == generation:
                    self._put_end_marker(_StreamEnd(error))

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

    async def _hand_over(self, event: WSEEvent, session: _Session) -> None:
        """Into the stream recv() and run_forever() read. Once the
        application reads it, a full stream holds the client back, and its
        socket with it: nothing is dropped. Before that the oldest goes."""
        while self._session is session:
            try:
                self._event_queue.put_nowait(event)
                return
            except queue.Full:
                if not self._reading:
                    self._put_event(event)
                    return
                await asyncio.sleep(0.005)

    def _put_event(self, event: WSEEvent | _Outcome) -> None:
        if isinstance(event, _Outcome):
            self._event_queue.put_always(event)
        else:
            # Nobody reads yet: the oldest event goes.
            self._event_queue.put_dropping(event)

    def _put_end_marker(self, end: _StreamEnd) -> None:
        # After what the stream holds: a reader gets all of it, then the end.
        self._event_queue.put_always(end)

    def _requeue_end(self, end: _StreamEnd) -> None:
        self._event_queue.put_always(end)

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
        self._event_queue.drop_ends()

    def _storage_report(self, operation: str, why: str) -> None:
        """The outbox's file could not be opened or written: said to the
        handlers of ``storage_failed`` (from any thread)."""
        logger.error("The outbox's file: %s failed: %s", operation, why)
        self._put_event(
            _Outcome(
                WSEEvent(
                    type="storage_failed",
                    payload={"operation": operation, "message": why},
                    category="WSE",
                )
            )
        )

    def _outcome(self, event: WSEEvent) -> None:
        """An outcome of the outbox, for the handlers (on the session's
        thread)."""
        self._put_event(_Outcome(event))

    def clear(self) -> None:
        """The user signs out: see :meth:`AsyncWSEClient.clear`."""
        client = self._session_client()
        if client is not None:
            self._later(lambda c: c.clear())
            return
        outbox = self._client_kwargs.get("_outbox")
        if outbox is not None:
            outcomes = outbox.clear()
            for reason, ids in outcomes.not_delivered.items():
                self._put_event(
                    _Outcome(
                        WSEEvent(
                            type="not_delivered",
                            payload={"reason": reason, "ids": ids},
                            category="WSE",
                        )
                    )
                )

    @property
    def kept(self) -> int:
        """How many messages wait in the outbox for the server's answer."""
        outbox = self._client_kwargs.get("_outbox")
        return outbox.size if outbox is not None else 0

    def _dispatch_to_handlers(self, event: WSEEvent) -> None:
        """Call sync handlers for the event."""
        handlers = self._handlers.get(event.type, []) + self._wildcard_handlers
        for handler in handlers:
            try:
                handler(event)
            except Exception as exc:
                logger.error("Handler error for '%s': %s", event.type, exc)
