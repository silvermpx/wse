# =============================================================================
# WSE Python Client -- Async Client
# =============================================================================
#
# Primary public API.  Async context manager, async iterator, callbacks.
# Port of ts-client/hooks/useWSE.ts + client/services/MessageProcessor.ts.
# =============================================================================

from __future__ import annotations

import asyncio
import json
import time

from uuid import uuid4
from collections import defaultdict, deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable

from ._logging import logger
from .circuit_breaker import CircuitBreaker
from .compression import CompressionHandler
from .connection import ConnectionManager
from .constants import (
    CLIENT_VERSION,
    DEFAULT_MAX_MESSAGE_SIZE,
    IDLE_TIMEOUT,
    KEY_EXCHANGE_TIMEOUT,
    PROTOCOL_VERSION,
    RATE_LIMIT_REFILL_RATE,
    RECOVERY_BUFFER_MAX,
    RECOVERY_MAX_UNANSWERED,
    RECOVERY_TIMEOUT,
    SEND_MAX_RETRIES,
    SEND_RETRY_BASE_DELAY,
    SEND_RETRY_MAX_DELAY,
    SERVER_READY_TIMEOUT,
    WS_CLOSE_ABNORMAL,
    SNAPSHOT_DEDUP_WINDOW,
    SNAPSHOT_MAX_RETRIES,
    SNAPSHOT_RETRY_DELAY,
)
from .errors import WSEAuthError, WSEConnectionError, WSEEncryptionError, WSEError
from .event_sequencer import EventSequencer
from .network_monitor import NetworkMonitor
from .offline_queue import OfflineQueue
from .protocol import MessageCodec, _json_loads
from .rate_limiter import TokenBucketRateLimiter
from .signing import MAX_NONCE_LEN, ClientSigner, ServerKeys
from .types import (
    ConnectionQuality,
    ConnectionState,
    ConnectionStats,
    LoadBalancingStrategy,
    MessagePriority,
    ReconnectConfig,
    SendResult,
    TokenProvider,
    WSEEvent,
)

# Type alias for event handlers
EventHandler = Callable[[WSEEvent], Any]
AsyncEventHandler = Callable[[WSEEvent], Awaitable[Any]]

# Event type of the synthetic event reporting messages a topic will never get.
RECOVERY_GAP_EVENT = "recovery_gap"

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

_WIRE_FORMATS = ("json", "msgpack")


class _StreamEnd:
    """Queue marker: the event stream ended (cleanly when ``error`` is None)."""

    __slots__ = ("error",)

    def __init__(self, error: WSEError | None) -> None:
        self.error = error


@dataclass(eq=False)
class _GapState:
    """A stream of a topic waiting for a recovery replay to fill an offset
    gap: one per stream with a hole (a topic published on several nodes has
    one stream per node)."""

    epoch: str
    # Messages that arrived past the hole, by offset (same epoch).
    buffer: dict[int, WSEEvent] = field(default_factory=dict)
    timer: asyncio.TimerHandle | None = None
    request: int | None = None  # its recovery request out, not answered yet
    requested_from: int | None = None  # anchor offset of the last request
    answered: bool = False  # an answer for it arrived


@dataclass(eq=False)
class _Request:
    """A recovery request out. The server answers a connection's requests in
    order, so each topic's answers settle its requests front to back."""

    id: int
    for_gap: bool
    #: The streams it named.
    named: tuple[str, ...] = ()


#: How long past the server's history_ttl a stream that heard nothing while
#: connected is still named: the clocks of a server and a client do not tick
#: together.
_QUIET_MARGIN = 5.0


def _listed_streams(value: Any) -> dict[str, int] | None:
    """A reply's streams (from 3.1): epoch -> the server's last offset."""
    if not isinstance(value, dict):
        return None
    return {
        str(epoch): offset
        for epoch, offset in value.items()
        if isinstance(offset, int) and not isinstance(offset, bool) and offset >= 0
    }


_HANDSHAKE_TYPES = frozenset({"server_hello", "error", "ping", "PING", "pong", "PONG"})

# The server's own messages: never a topic's, whatever stamp they carry.
_PROTOCOL_TYPES = frozenset(
    {
        "ping",
        "PING",
        "pong",
        "PONG",
        "server_ready",
        "server_hello",
        "subscription_update",
        "error",
    }
)


def _handshake_frame(data: str | bytes) -> bool:
    """A frame that may come unsealed before an E2E session: the answer to
    the offer, an error, a heartbeat."""
    if isinstance(data, bytes):
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError:
            return False
    if data.startswith(("PING:", "PONG:", "WSE:PING:", "WSE:PONG:")):
        return True
    text = data[3:] if data.startswith("WSE{") else data
    try:
        parsed = json.loads(text)
    except ValueError:
        return False
    return isinstance(parsed, dict) and parsed.get("t") in _HANDSHAKE_TYPES


def _sends_server_ready(version: object) -> bool:
    """A server of 3.0 or later: it always sends server_ready."""
    if not isinstance(version, str):
        return False
    major = version.split(".", 1)[0]
    return major.isdigit() and int(major) >= 3


def _resolve(fut: asyncio.Future[None] | None) -> None:
    if fut is not None and not fut.done():
        fut.set_result(None)


def _reject(fut: asyncio.Future[None] | None, exc: BaseException) -> None:
    if fut is not None and not fut.done():
        fut.set_exception(exc)
        fut.exception()  # mark retrieved: nobody may be waiting on it


class AsyncWSEClient:
    """Async WSE client with context manager and async iterator support.

    Args:
        url: WebSocket server URL, e.g. ``"ws://localhost:5006/wse"``.
        token: JWT token for authentication (sent as Cookie + Authorization).
        token_provider: Callable (sync or async) returning the token to use;
            called before every connect and reconnect attempt, so an expired
            token is replaced without restarting the client. Overrides
            *token*.
        topics: Topics to subscribe to after connecting.
        reconnect: Reconnection config. Defaults to exponential backoff,
            infinite retries.
        extra_headers: Additional HTTP headers for the handshake.
        queue_size: Max events buffered for the async iterator. When full,
            oldest events are dropped. Default 1000.
        encryption: Require E2E encryption (ECDH P-256 + AES-GCM-256, needs
            the ``[crypto]`` extra). Fails closed: application messages are
            held until the key exchange completed and never go out in
            plaintext; if the server does not complete the exchange the
            client ends with :class:`WSEEncryptionError`.
        endpoints: Additional endpoints for failover.
        load_balancing: Endpoint selection strategy when *endpoints* is set.
        rate_limit: Outbound limit for ``send()`` / ``send_with_retry()`` /
            ``send_batch()`` in messages per second (default 100). ``None``
            disables it. Protocol frames (heartbeat, PONG, client_hello,
            subscriptions, recovery requests) are never limited.
        rate_limit_burst: Messages that may go out at once before the rate
            applies (default 10 x *rate_limit*).
        offline_queue: Hold messages sent while disconnected and send them
            once after the next reconnect (``send()`` returns
            ``SendResult.QUEUED``). Off by default: a send that cannot go out
            returns ``SendResult.FAILED`` and is not replayed.
        max_message_size: Largest inbound message in bytes, checked on the
            frame and while inflating compressed frames (default 16 MiB).
            ``None`` disables the limit.
        recovery_timeout: Seconds a topic waits for a recovery replay after an
            offset gap before giving up (default 3). Messages that arrive
            meanwhile are held and delivered in order; on giving up a
            ``recovery_gap`` event reports the missing offsets. ``0`` never
            waits.
        wire_format: ``"json"`` (default) or ``"msgpack"`` (asks the server
            to send this connection's messages as MessagePack, and sends
            the client's in it too, except frames sealed for E2E; needs the
            ``[msgpack]`` extra).
        signing_key: This client's ECDSA P-256 private key (a key object or
            PKCS#8 PEM/DER) for ``send(..., signed=True)``. The server learns
            its public key from the token's ``cnf`` claim or from the
            application (``set_client_key``); see :mod:`wse_client.signing`.
            Needs the ``[crypto]`` extra.
        server_signing_keys: The server's signing public keys (base64, from
            ``server.signing_public_key()``) that the messages it signs are
            checked against; a signed message no pinned key verifies is
            dropped. Without them signed messages are delivered unchecked
            (``event.signature.verified`` is False).
        e2e_bind_session: With *signing_key* and *encryption*, the session
            is bound to the key (True, the default): the client proves the
            key inside the session, and goes on only once the server
            confirms the session is bound to it, as the token names it
            (``cnf``). A token copied on the way then opens nothing. False
            for a token that names no key, whose key the server application
            binds itself (``set_client_key``).
        cookie_name: The cookie the token goes in (``access_token``, the
            server's ``jwt_cookie_name`` default). Cookies already in
            *extra_headers* are kept, and an ``Authorization`` header there
            wins over the token's.

    Besides the server's messages, handlers (:meth:`on`) get ``connected``
    (``connection_id``, ``user_id``, ``reconnected``, ``recovery``) each
    time the server takes a connection, ``disconnected`` (``code``,
    ``reason``, ``reconnecting``) when one is lost, and the server's
    ``subscription_update`` replies (``success_topics``, ``failed_topics``,
    ``recovery``). The event stream carries the server's messages only.

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
        e2e_bind_session: bool = True,
        cookie_name: str = "access_token",
    ) -> None:
        if wire_format not in _WIRE_FORMATS:
            raise ValueError(f"wire_format must be one of {_WIRE_FORMATS}")
        if reconnect is not None and (
            reconnect.circuit_breaker_threshold < 0
            or reconnect.circuit_breaker_timeout < 0
        ):
            raise ValueError(
                "circuit_breaker_threshold and circuit_breaker_timeout must be >= 0"
            )
        if rate_limit is not None and rate_limit <= 0:
            raise ValueError("rate_limit must be > 0, or None to disable it")
        if max_message_size is not None and max_message_size <= 0:
            raise ValueError("max_message_size must be > 0, or None")

        self._url = url
        self._token = token
        self._initial_topics = list(topics) if topics else []
        self._subscribed_topics: set[str] = set()

        # E2E encryption (optional)
        self._security: SecurityManager | None = None
        if encryption:
            from .security import SecurityManager

            self._security = SecurityManager(
                server_keys=e2e_server_keys,
                unauthenticated=e2e_unauthenticated,
                legacy=e2e_legacy,
            )

        # Services
        self._compression = CompressionHandler()
        msgpack_handler = None
        try:
            from .msgpack_handler import MsgPackHandler

            if MsgPackHandler.available() or wire_format == "msgpack":
                # Raises ImportError with the install hint when missing.
                msgpack_handler = MsgPackHandler()
        except ImportError:
            if wire_format == "msgpack":
                raise
        self._codec = MessageCodec(
            self._compression,
            security=self._security,
            msgpack=msgpack_handler,
            max_message_size=max_message_size,
            server_keys=ServerKeys(server_signing_keys),
        )
        # Signed messages: this client's key, and the server's signing key id
        # from its server_hello (the audience of what the client signs).
        self._signer = ClientSigner(signing_key) if signing_key is not None else None
        self._server_signing_kid: str | None = None
        # The session is bound to the signing key: proved, and confirmed.
        self._bind_session = (
            e2e_bind_session
            and self._signer is not None
            and self._security is not None
            and self._security.session_mode
        )
        # Messages go out in msgpack too, except frames sealed for E2E.
        self._send_packed = (
            msgpack_handler
            if wire_format == "msgpack" and self._security is None
            else None
        )
        self._rate_limiter: TokenBucketRateLimiter | None = None
        if rate_limit is not None:
            burst = rate_limit_burst or max(1, round(rate_limit * 10))
            self._rate_limiter = TokenBucketRateLimiter(
                capacity=burst, refill_rate=rate_limit, refill_interval=1.0
            )
        self._rate_limited_total = 0
        self._last_rate_limit_log = 0.0
        # A pause after failed attempts in a row, only when asked for.
        breaker_after = reconnect.circuit_breaker_threshold if reconnect else 0
        self._circuit_breaker: CircuitBreaker | None = (
            CircuitBreaker(
                failure_threshold=breaker_after,
                reset_timeout=reconnect.circuit_breaker_timeout,
                success_threshold=1,
            )
            if reconnect is not None and breaker_after > 0
            else None
        )
        self._sequencer = EventSequencer()
        self._network_monitor = NetworkMonitor()
        self._offline_queue = OfflineQueue(
            max_size=1000, max_age=3600.0, enabled=offline_queue
        )

        # Event queue for async iteration (created lazily in connect() to avoid
        # RuntimeError when instantiated outside a running event loop)
        self._event_queue: asyncio.Queue[WSEEvent | _StreamEnd] | None = None
        self._queue_size = queue_size
        # Why the client ended for good (auth, encryption, attempts exhausted).
        self._terminal_error: WSEError | None = None

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
        self._server_ping_interval_ms: int | None = None

        self._background_tasks: set[asyncio.Task[Any]] = set()

        # Snapshot requests in flight: topic set -> monotonic expiry.
        self._snapshot_inflight: dict[frozenset[str], float] = {}

        # Recovery. The anchor of a topic is the sequencer's position: the
        # last offset delivered in order (or given up on). It only moves when
        # messages are delivered, never to a server-reported head.
        self._server_recovery_enabled = False
        # How long the server keeps a stream that receives nothing
        # (history_ttl, seconds): one quiet for longer while connected is
        # not named.
        self._history_ttl: float | None = None
        # Since when (time.time()) this connection has had each topic: from
        # then on whatever the server publishes on it arrives, so silence
        # then shows a quiet stream. Emptied when the connection goes: time
        # away proves nothing.
        self._live_since: dict[str, float] = {}
        # When this connection's last frame was read (it was surely alive
        # then), and when the run of frames ending there began. A pause
        # between two frames longer than the idle limit (a laptop asleep, a
        # stalled loop) starts a new run: what is read on waking may have
        # been sent before it, and proves nothing about the time between.
        self._last_frame_at: float | None = None
        self._frames_since: float | None = None
        self._recovery_timeout = recovery_timeout
        self._gaps: dict[tuple[str, str], _GapState] = {}
        # Topics with a recovery request out on this connection -> anchor sent.
        self._recovery_requests: dict[str, deque[_Request]] = {}
        self._next_request = 0
        # Topics the server said it cannot replay -> epoch of the anchor.
        self._unrecoverable: dict[str, str] = {}
        self._recovery_unanswered = 0
        self._gap_stats = {"gaps": 0, "missed": 0, "recovered": 0}

        # Per-connection handshake state (see _on_transport_open)
        self._server_ready_event: asyncio.Event | None = None
        self._server_hello_seen = False
        self._handshake: asyncio.Future[None] | None = None
        self._key_ready: asyncio.Future[None] | None = None
        self._encryption_error: WSEEncryptionError | None = None
        self._abort_task: asyncio.Future[None] | None = None
        self._resubscribe_pending: set[str] = set()
        # Some connection completed its handshake before: the next is a
        # reconnect.
        self._connected_before = False
        # The application reads events (iteration or recv()): while it is
        # behind, what comes next waits here and the socket is not read.
        self._reading = False
        self._overflow: deque[WSEEvent | _StreamEnd] = deque()
        self._room: asyncio.Event | None = None

        # Reconnect config for later use
        self._reconnect_cfg = reconnect

        # The upgrade asks for msgpack, and announces an E2E session: the
        # server then holds everything back until the session exists.
        query: dict[str, str] = {}
        if wire_format == "msgpack":
            query["format"] = "msgpack"
        if self._security is not None and self._security.session_mode:
            query["e2e"] = "1"

        # Connection manager
        self._connection = ConnectionManager(
            url,
            token=token,
            token_provider=token_provider,
            reconnect=reconnect,
            circuit_breaker=self._circuit_breaker,
            rate_limiter=None,  # applied to application sends here
            extra_headers=extra_headers,
            on_message=self._on_raw_message,
            on_state_change=self._on_state_change,
            security=self._security,
            endpoints=endpoints,
            load_balancing=load_balancing,
            max_message_size=max_message_size,
            query_params=query or None,
            cookie_name=cookie_name,
        )

        self._connection._on_pong = self._on_transport_pong
        self._connection._on_ping_sent = lambda: self._network_monitor.record_ping()
        self._connection._on_open = self._on_transport_open
        self._connection._on_close = self._on_transport_close
        self._connection._on_terminal = self._on_transport_terminal
        self._connection._before_read = self._event_room

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
        """Next event. Reconnects are invisible here: the stream continues
        across them. It ends after ``disconnect()``; when the client ends for
        good it raises the reason (:class:`WSEAuthError`,
        :class:`WSEEncryptionError`, :class:`WSEConnectionError`)."""
        queue = self._event_queue
        if queue is None:
            raise StopAsyncIteration
        self._reading = True
        item = await queue.get()
        self._refill()
        if isinstance(item, _StreamEnd):
            self._requeue_end(item)
            if item.error is not None:
                raise item.error
            raise StopAsyncIteration
        return item

    # -- Connect / Disconnect -------------------------------------------------

    async def connect(self) -> None:
        """Connect and complete the handshake.

        Returns once the server confirmed the connection with
        ``server_ready`` (from a server before 3.0, ``server_hello`` will do;
        with ``encryption=True``: once the key exchange completed too), then
        subscribes the initial *topics*. If neither arrives within 10 s the
        client proceeds anyway (for servers whose application never sends
        ``server_ready``) -- except with encryption, which then fails closed.

        Raises:
            WSEAuthError: The server rejected the credentials (close
                4401 / 4403 / 1008 or HTTP 401 / 403).
            WSEEncryptionError: ``encryption=True`` and the server did not
                complete the key exchange.
            WSEConnectionError: The connection could not be opened, or the
                server closed it during the handshake.
            WSETimeoutError: Opening the connection timed out.
        """
        self._ensure_primitives()
        state = self._connection.state
        if state in (ConnectionState.CONNECTING, ConnectionState.RECONNECTING):
            return  # already connecting in the background
        if state != ConnectionState.CONNECTED:
            self._reset_stream()
            await self._connection.connect()
        await self._await_handshake()
        self._connection.arm_reconnect()

        # Auto-subscribe to initial topics. If the connection dropped right
        # after the handshake, the next connection subscribes them.
        if self._initial_topics and not await self.subscribe(self._initial_topics):
            self._subscribed_topics.update(self._initial_topics)

    async def disconnect(self) -> None:
        """Disconnect gracefully. Iteration ends after the queued events;
        nothing reconnects until :meth:`connect` is called again."""
        # Never cancel the calling task: an async handler (a background task)
        # may await disconnect() itself, and cancelling it would abort the
        # teardown half way and leave the socket open.
        current = asyncio.current_task()
        for task in list(self._background_tasks):
            if task is not current:
                task.cancel()
        self._background_tasks.clear()

        # Clear offline queue (no point replaying after explicit disconnect)
        self._offline_queue.clear()

        # Messages held for a gap are delivered before the stream ends.
        for topic, epoch in list(self._gaps):
            self._give_up_gap(topic, epoch, "closed")

        # Signal iterator to stop (drop oldest if queue is full)
        self._push_stream_end(_StreamEnd(None))
        # Closed, not destroyed: connect() may open the client again.
        await self._connection.disconnect()

    async def close(self) -> None:
        """Alias for disconnect."""
        await self.disconnect()

    def set_token(self, token: str | None) -> None:
        """Use *token* for the next connect / reconnect.

        To apply it right away (e.g. after the client ended with
        :class:`WSEAuthError`), call :meth:`force_reconnect` afterwards.
        A configured ``token_provider`` still takes precedence.
        """
        self._token = token
        self._connection.set_token(token)

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
    def last_error(self) -> WSEError | None:
        """Why the client ended for good, or ``None`` while it is healthy."""
        return self._terminal_error

    @property
    def connection_quality(self) -> ConnectionQuality:
        return self._network_monitor.quality

    @property
    def subscribed_topics(self) -> set[str]:
        return set(self._subscribed_topics)

    @property
    def recovery_enabled(self) -> bool:
        """Whether the server announced message recovery (server_ready or
        server_hello of the current connection)."""
        return self._server_recovery_enabled

    @property
    def recovery_state(self) -> dict[str, tuple[str, int]]:
        """Per-topic recovery anchors: ``{topic: (epoch, offset)}`` of the last
        message delivered in order (the position recovery resumes from)."""
        return self._sequencer.topic_positions()

    @property
    def queue_size(self) -> int:
        """Number of events waiting in the iterator queue."""
        return self._event_queue.qsize() if self._event_queue else 0

    # -- Send / Subscribe / Unsubscribe ---------------------------------------

    async def send(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = MessagePriority.NORMAL,
        correlation_id: str | None = None,
        signed: bool = False,
        nonce: str | None = None,
        live: bool = False,
    ) -> SendResult:
        """Send a message to the server.

        Args:
            type: Event type string, e.g. ``"update_settings"``.
            payload: Message data dict (default: empty).
            priority: See :class:`~wse_client.types.MessagePriority`.
            correlation_id: Optional ID to correlate request/response.
            signed: Sign the message with ``signing_key``: the server checks
                it, answers with a signed ``receipt`` event and hands the
                application the proof. Needs a server with a signing key
                (its id comes in ``server_hello``).
            nonce: For a signed message, its unique id (an order id, say);
                the server takes each once. Random by default. The receipt
                and a ``SIGNATURE_REFUSED`` error carry it as ``n``.
            live: Only on the connection that is open now: never held by the
                offline queue for a later connection. For commands that must
                not run late (a valve, a door, an order).

        Returns:
            ``SendResult.SENT`` when it went out; ``SendResult.QUEUED`` when
            it is held by the offline queue (``offline_queue=True``) for the
            next reconnect; ``SendResult.FAILED`` (falsy) when it was neither
            sent nor queued (not connected, rate limited). A signed or live
            message is never queued: a signed one is signed for this server
            and valid for its signature window, so without a connection it
            fails, as a live one does.

        Raises:
            WSEEncryptionError: ``encryption=True`` and the key exchange of
                the current connection failed; nothing was sent.
            WSEError: ``signed=True`` without ``signing_key``, or before the
                server's ``server_hello`` named its signing key.
        """
        if signed:
            if self._signer is None:
                raise WSEError("signed=True needs AsyncWSEClient(signing_key=...)")
            if nonce is not None and not 1 <= len(nonce) <= MAX_NONCE_LEN:
                raise ValueError(f"nonce must be 1 to {MAX_NONCE_LEN} characters")
            if self._server_signing_kid is None:
                raise WSEError(
                    "the server has no signing key (none in its server_hello)"
                )
            encoded = self._signer.envelope(
                type, payload or {}, self._server_signing_kid, nonce=nonce
            )
        else:
            encoded = self._codec.encode(
                type,
                payload or {},
                priority=priority,
                correlation_id=correlation_id,
            )
        return await self._send_app_frame(
            encoded, priority=priority, msg_type=type, allow_queue=not (signed or live)
        )

    async def send_with_retry(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = MessagePriority.NORMAL,
        correlation_id: str | None = None,
        max_retries: int = SEND_MAX_RETRIES,
    ) -> SendResult:
        """Send with exponential backoff retry.

        The message is encoded once, so every attempt carries the same id and
        the server can tell retries apart from new messages. Failed attempts
        are never queued; with ``offline_queue=True`` a message that cannot
        go out because the client is disconnected is queued once.

        Args:
            type: Event type string.
            payload: Message data dict.
            priority: See :class:`~wse_client.types.MessagePriority`.
            correlation_id: Optional correlation ID.
            max_retries: Max attempts (default 5).

        Returns:
            ``SENT`` within the retry budget, ``QUEUED`` (see above), or
            ``FAILED``.
        """
        encoded = self._codec.encode(
            type, payload or {}, priority=priority, correlation_id=correlation_id
        )
        for attempt in range(max_retries):
            result = await self._send_app_frame(
                encoded, priority=priority, msg_type=type, allow_queue=False
            )
            if result:
                return result
            if not self._connection.is_connected and self._offline_queue.enabled:
                return self._queue_or_fail(encoded, priority, type, allow_queue=True)

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
                    return SendResult.FAILED

        logger.warning("Send failed after %d retries for '%s'", max_retries, type)
        return SendResult.FAILED

    async def send_batch(self, messages: list[tuple[str, dict[str, Any]]]) -> bool:
        """Send multiple messages in a single batch frame.

        Args:
            messages: List of ``(type, payload)`` tuples.

        Returns:
            True if the batch was sent (one frame against the rate limit).
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
        if not self._connection.is_connected:
            return False
        if self._rate_limiter is not None and not self._rate_limiter.try_consume():
            self._note_rate_limited("batch")
            return False
        ok = await self._transmit(encoded, strict=True)
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
        sends its last delivered epoch and offset for each topic so the
        server can replay missed messages before confirming the subscription.
        Subscribed topics are subscribed again automatically on every new
        connection (reconnect, ``change_endpoint()``).

        Args:
            topics: Topic names, e.g. ``["notifications", "live_data"]``.
            recover: If True, request message recovery using stored positions.

        Returns:
            True if the subscription message was sent.
        """
        new_topics = [
            t for t in dict.fromkeys(topics) if t not in self._subscribed_topics
        ]
        if not new_topics:
            return True

        payload, recovery_info = self._subscription_payload(new_topics, recover=recover)
        msg = self._codec.encode(
            "subscription_update",
            payload,
            priority=MessagePriority.HIGH,
        )
        ok = await self._transmit(msg, strict=True)
        # Sent or not, the topics are wanted: the next connection subscribes
        # them.
        self._subscribed_topics.update(new_topics)
        if ok:
            self._note_recovery_requested(recovery_info)
            self._stats.messages_sent += 1
        return ok

    async def unsubscribe(self, topics: list[str]) -> bool:
        """Unsubscribe from one or more topics.

        The topics' recovery positions are forgotten, so subscribing again
        later starts from the next live message instead of a gap.

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
        ok = await self._transmit(msg, strict=True)
        # Sent or not, they are no longer wanted: the next connection does
        # not subscribe them again.
        self._subscribed_topics -= set(topics)
        self._resubscribe_pending -= set(topics)
        for t in topics:
            self._forget_topic(t)
        if ok:
            self._stats.messages_sent += 1
        return ok

    async def request_snapshot(self, topics: list[str] | None = None) -> bool:
        """Request a state snapshot from the server.

        The server replies with current state for the given topics so the
        client doesn't have to wait for the next publish. A request for the
        same topics within a few seconds of an earlier one is not sent again
        (it returns True); requests for other topics always go out.

        Args:
            topics: Topics to snapshot. Defaults to subscribed topics.

        Returns:
            True if the request was sent (or an identical one is in flight).
        """
        target = sorted(set(topics)) if topics else sorted(self._subscribed_topics)
        key = frozenset(target)
        now = time.monotonic()
        self._snapshot_inflight = {
            k: expiry for k, expiry in self._snapshot_inflight.items() if expiry > now
        }
        if key in self._snapshot_inflight:
            return True

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
            if await self._transmit(msg, strict=True):
                self._snapshot_inflight[key] = time.monotonic() + SNAPSHOT_DEDUP_WINDOW
                self._stats.messages_sent += 1
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
            WSEConnectionError: The client was disconnected, or gave up
                reconnecting.
            WSEAuthError: The server rejected the credentials for good.
            WSEEncryptionError: The E2E key exchange failed.
            asyncio.TimeoutError: If *timeout* expires.
        """
        queue = self._event_queue
        if queue is None:
            raise WSEConnectionError("Not connected")
        self._reading = True
        if timeout is not None:
            item = await asyncio.wait_for(queue.get(), timeout=timeout)
        else:
            item = await queue.get()
        self._refill()

        if isinstance(item, _StreamEnd):
            # Re-queue the marker so other callers also see the close signal
            self._requeue_end(item)
            if item.error is not None:
                raise item.error
            raise WSEConnectionError("Connection closed")
        return item

    async def force_reconnect(self) -> None:
        """Force a reconnection (e.g. after detecting degraded quality).

        Also revives a client that ended (e.g. after :meth:`set_token`
        replaced a token the server rejected).
        """
        self._ensure_primitives()
        if self._server_ready_event is not None:
            self._server_ready_event.clear()
        if self._terminal_error is not None:
            self._reset_stream()
        await self._connection.force_reconnect()

    async def change_endpoint(self, new_url: str) -> None:
        """Switch to another server URL and reconnect.

        Every subscribed topic is subscribed again on the new server once its
        handshake completed.
        """
        self._ensure_primitives()
        conn = self._connection
        await conn.disconnect()
        conn._url = new_url
        conn._active_endpoint = new_url
        conn._reconnect_attempts = 0

        # Register with pool if active
        if conn._pool is not None:
            conn._pool.add_endpoint(new_url)
            conn._pool.set_active_endpoint(new_url)

        await conn.connect()
        await self._await_handshake()
        conn.arm_reconnect()

    # -- Handler registration -------------------------------------------------

    def on(
        self, event_type: str
    ) -> Callable[[EventHandler | AsyncEventHandler], EventHandler | AsyncEventHandler]:
        """Decorator to register a handler for one event type (the ``t``
        of the messages, whatever topic they come on).

        Args:
            event_type: Event type to listen for, e.g. ``"notification"``.

        Returns:
            Decorator that registers the function.

        Example::

            @client.on("notification")
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

    def _breaker_state(self) -> str:
        """The reconnect circuit breaker's state; closed when there is none."""
        if self._circuit_breaker is None:
            return "closed"
        return self._circuit_breaker.state.value

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
            "queue_size": self._event_queue.qsize() if self._event_queue else 0,
            "network": {
                "quality": diag.quality.value,
                "latency_ms": diag.round_trip_time,
                "jitter_ms": diag.jitter,
                "packet_loss": diag.packet_loss,
                "stability": diag.stability,
            },
            "circuit_breaker": {
                "enabled": self._circuit_breaker is not None,
                "state": self._breaker_state(),
                "can_execute": self._breaker_state() != "open",
            },
            "rate_limiter": (
                dict(
                    self._rate_limiter.get_stats(),
                    rate_limited=self._rate_limited_total,
                )
                if self._rate_limiter is not None
                else None
            ),
            "offline_queue": self._offline_queue.get_stats(),
            "recovery": {
                "enabled": self._server_recovery_enabled,
                "pending_gaps": len(self._gaps),
                "gaps": self._gap_stats["gaps"],
                "missed": self._gap_stats["missed"],
                "recovered": self._gap_stats["recovered"],
            },
            "last_error": str(self._terminal_error) if self._terminal_error else None,
            "connection_pool": (
                self._connection._pool.get_stats()
                if self._connection._pool is not None
                else None
            ),
        }

    # -- Internal: event stream -----------------------------------------------

    def _ensure_primitives(self) -> None:
        """Create the asyncio primitives (needs a running event loop)."""
        if self._event_queue is None:
            self._event_queue = asyncio.Queue(maxsize=self._queue_size)
        if self._server_ready_event is None:
            self._server_ready_event = asyncio.Event()
        if self._room is None:
            self._room = asyncio.Event()
            self._room.set()

    def _enqueue(self, item: WSEEvent | _StreamEnd) -> None:
        queue = self._event_queue
        if queue is None:
            return
        # Behind already: in order, after what waits.
        if self._overflow:
            self._overflow.append(item)
            return
        try:
            queue.put_nowait(item)
        except asyncio.QueueFull:
            if self._reading:
                # The application reads, and is behind: the event waits, and
                # the socket is not read until it has room (_event_room), so
                # nothing is lost here; the server holds back, or closes a
                # client that stays behind, which then recovers.
                self._overflow.append(item)
                if self._room is not None:
                    self._room.clear()
                return
            # Nobody reads the stream (handlers only): the oldest goes.
            try:
                queue.get_nowait()
                queue.put_nowait(item)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    def _refill(self) -> None:
        """Move what waits into the stream as the application reads it."""
        queue = self._event_queue
        if queue is None:
            return
        while self._overflow and not queue.full():
            queue.put_nowait(self._overflow.popleft())
        if not self._overflow and self._room is not None:
            self._room.set()

    async def _event_room(self) -> None:
        """The receive loop waits here before reading on while the
        application is behind."""
        room = self._room
        if room is not None and self._overflow:
            await room.wait()

    def _push_stream_end(self, end: _StreamEnd) -> None:
        # The end is never held back: the oldest event makes room for it.
        queue = self._event_queue
        if queue is None:
            return
        self._overflow.clear()
        if self._room is not None:
            self._room.set()
        try:
            queue.put_nowait(end)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
                queue.put_nowait(end)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    def _requeue_end(self, end: _StreamEnd) -> None:
        if self._event_queue is not None:
            try:
                self._event_queue.put_nowait(end)
            except asyncio.QueueFull:
                pass

    def _reset_stream(self) -> None:
        """Drop end markers of an earlier session (a new one is starting)."""
        self._terminal_error = None
        queue = self._event_queue
        if queue is None:
            return
        kept: list[WSEEvent | _StreamEnd] = []
        while True:
            try:
                kept.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        for item in kept:
            if not isinstance(item, _StreamEnd):
                queue.put_nowait(item)

    # -- Internal: transport callbacks ----------------------------------------

    def _on_transport_open(self) -> None:
        """A socket opened: fresh per-connection state, start the session."""
        self._ensure_primitives()
        loop = asyncio.get_running_loop()
        assert self._server_ready_event is not None
        self._server_ready_event.clear()
        self._server_hello_seen = False
        self._server_recovery_enabled = False
        self._history_ttl = None
        self._last_frame_at = None
        self._frames_since = None
        # The server of this connection names its own signing key, and the
        # user it takes the connection for.
        self._server_signing_kid = None
        self._codec.connection_user = None
        self._encryption_error = None
        self._recovery_requests.clear()
        self._unrecoverable.clear()
        self._recovery_unanswered = 0
        self._snapshot_inflight.clear()
        for gap in self._gaps.values():
            gap.request = None
        lost = WSEConnectionError("Connection replaced")
        _reject(self._handshake, lost)
        _reject(self._key_ready, lost)
        self._handshake = loop.create_future()
        self._key_ready = loop.create_future() if self._security is not None else None
        # Topics wanted before this connection existed are subscribed again
        # once it is ready; subscribe() sends the ones added from now on.
        self._resubscribe_pending = set(self._subscribed_topics)
        self._fire_task(
            self._session_start(self._handshake, self._connection.reconnect_armed)
        )

    def _on_transport_close(self) -> None:
        """A live socket went away (any reason)."""
        handshake = self._handshake
        was_up = (
            handshake is not None
            and handshake.done()
            and not handshake.cancelled()
            and handshake.exception() is None
        )
        if was_up:
            code, reason = self._connection.last_close or (WS_CLOSE_ABNORMAL, "")
            self._invoke_handlers(
                WSEEvent(
                    type="disconnected",
                    payload={
                        "code": code,
                        "reason": reason,
                        "reconnecting": self._connection.reconnect_armed,
                    },
                )
            )
        if self._server_ready_event is not None:
            self._server_ready_event.clear()
        # Sends waiting for this connection's key give up (and may queue).
        _reject(self._key_ready, WSEConnectionError("Connection lost"))
        self._recovery_requests.clear()
        self._snapshot_inflight.clear()
        for gap in self._gaps.values():
            gap.request = None
        # A stream that heard nothing for longer than the server keeps one,
        # while this connection had its topic, is gone there: naming it would
        # fail the recovery. Silence counts only within the run of frames
        # read up to the last one, never up to a close heard late (after a
        # sleep the server kept publishing), and that last frame may trail
        # its sending by up to the idle limit. Silence while away shows
        # nothing, so nothing is forgotten for it.
        last = self._last_frame_at
        frames_since = self._frames_since
        if self._history_ttl is not None and last is not None:
            before = last - self._history_ttl - _QUIET_MARGIN - IDLE_TIMEOUT
            if frames_since is not None and frames_since <= before:
                gaps = self._gaps
                for topic, since in self._live_since.items():
                    if since <= before:
                        self._sequencer.forget_quiet_streams(
                            topic, before, lambda epoch, t=topic: (t, epoch) in gaps
                        )
        self._live_since.clear()
        self._last_frame_at = None
        self._frames_since = None

    def _on_transport_terminal(self, error: WSEError) -> None:
        """The transport gave up for good: end the event stream with *error*."""
        self._terminal_error = error
        _reject(self._handshake, error)
        _reject(self._key_ready, error)
        for topic, epoch in list(self._gaps):
            self._give_up_gap(topic, epoch, "closed")
        self._push_stream_end(_StreamEnd(error))

    async def _await_handshake(self) -> None:
        """Wait for the handshake of the current connection (see connect())."""
        fut = self._handshake
        if fut is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout=SERVER_READY_TIMEOUT)
        except WSEError:
            # Let a fail-closed abort finish so the socket is closed when
            # connect() raises.
            abort = self._abort_task
            if abort is not None and not abort.done():
                await asyncio.wait([abort], timeout=5.0)
            raise
        except asyncio.TimeoutError:
            failure = self._handshake_failure()
            if failure is None:
                logger.warning(
                    "Neither server_ready nor server_hello received within %.0fs, "
                    "proceeding anyway",
                    SERVER_READY_TIMEOUT,
                )
                self._proceed_without_ready()
                return
            if isinstance(failure, WSEEncryptionError):
                self._encryption_error = failure
                _reject(self._key_ready, failure)
            await self._connection.abort(failure)
            raise failure from None

    async def _session_start(
        self, handshake: asyncio.Future[None], background: bool
    ) -> None:
        """After a connection's handshake: resubscribe, flush the queue."""
        try:
            await asyncio.wait_for(asyncio.shield(handshake), SERVER_READY_TIMEOUT)
        except asyncio.TimeoutError:
            if handshake is not self._handshake or not background:
                return  # connect() handles its own connection
            failure = self._handshake_failure()
            if isinstance(failure, WSEEncryptionError):
                self._start_encryption_failure(failure)
                return
            if failure is not None:
                logger.warning("%s; reconnecting", failure)
                await self._connection.force_reconnect()
                return
            logger.warning(
                "Neither server_ready nor server_hello within %.0fs after "
                "reconnecting, proceeding anyway",
                SERVER_READY_TIMEOUT,
            )
            self._proceed_without_ready()
        except WSEError:
            return  # the connection failed or was replaced
        if handshake is not self._handshake or not self._connection.is_connected:
            return
        await self._resubscribe()
        if self._offline_queue.size > 0:
            await self._flush_offline_queue()

    def _handshake_failure(self) -> WSEError | None:
        """Why a handshake that timed out failed; None for a server that
        sent neither server_hello nor server_ready (one older than 3.0
        without them), where the client proceeds."""
        security = self._security
        if security is not None:
            # Fail closed: without its key, or without the sealed
            # server_ready that confirms the session (and names the key the
            # session is bound to), nothing goes out.
            if not security.is_enabled:
                return WSEEncryptionError(
                    "E2E key exchange did not complete within "
                    f"{SERVER_READY_TIMEOUT:.0f}s (no server_hello with a key)"
                )
            return WSEEncryptionError(
                "the server did not confirm the E2E session within "
                f"{SERVER_READY_TIMEOUT:.0f}s (no sealed server_ready)"
            )
        if self._server_features or self._server_hello_seen:
            # A 3.x server answered: it sends server_ready once it takes the
            # connection, and takes nothing before.
            return WSEConnectionError(
                f"server_ready not received within {SERVER_READY_TIMEOUT:.0f}s"
            )
        return None

    def _proceed_without_ready(self) -> None:
        fut = self._handshake
        if fut is not None and not fut.done():
            fut.set_result(None)
            self._connection.mark_handshake_complete()

    def _maybe_complete_handshake(self) -> None:
        fut = self._handshake
        if fut is None or fut.done():
            return
        ready = self._server_hello_seen or (
            self._server_ready_event is not None and self._server_ready_event.is_set()
        )
        if not ready:
            return
        if self._security is not None and not self._security.is_enabled:
            return
        fut.set_result(None)
        self._connection.mark_handshake_complete()
        reconnected = self._connected_before
        self._connected_before = True
        self._invoke_handlers(
            WSEEvent(
                type="connected",
                payload={
                    "connection_id": self._connection.connection_id,
                    "user_id": self._codec.connection_user,
                    "reconnected": reconnected,
                    "recovery": self._server_recovery_enabled,
                },
            )
        )

    # -- Internal: sending ----------------------------------------------------

    async def _wait_for_key(self, *, strict: bool) -> bool:
        """With encryption: wait until this connection's key exists.

        Returns False when the connection went away meanwhile, or the key
        exchange failed and *strict* is False.

        Raises:
            WSEEncryptionError: The key exchange failed (only when *strict*).
        """
        security = self._security
        if security is None or security.is_enabled:
            return True
        if self._encryption_error is not None:
            if strict:
                raise self._encryption_error
            return False
        fut = self._key_ready
        if fut is None:
            return False
        try:
            await asyncio.wait_for(asyncio.shield(fut), KEY_EXCHANGE_TIMEOUT)
        except asyncio.TimeoutError:
            return False
        except WSEEncryptionError:
            if strict:
                raise
            return False
        except WSEError:
            return False
        return security.is_enabled

    async def _wait_for_handshake(self, *, strict: bool) -> bool:
        """Until the server took this connection (``server_ready``): nothing
        of the application's goes before it. False when the connection went
        away meanwhile or the handshake failed."""
        fut = self._handshake
        if fut is None:
            return self._connection.is_connected
        if not fut.done():
            try:
                await asyncio.wait_for(asyncio.shield(fut), SERVER_READY_TIMEOUT)
            except asyncio.TimeoutError:
                return False
            except WSEEncryptionError:
                if strict:
                    raise
                return False
            except WSEError:
                return False
        if fut.cancelled() or fut.exception() is not None:
            return False
        return fut is self._handshake and self._connection.is_connected

    async def _transmit(self, encoded: str, *, strict: bool = False) -> bool:
        """Send one frame on the current connection, once the server took it
        (``server_ready``); with encryption only after the key exchange
        (never plaintext). False if not connected.

        With *strict* (the public send / subscribe methods) a failed key
        exchange raises :class:`WSEEncryptionError` instead of returning
        False."""
        if not self._connection.is_connected:
            if strict and self._encryption_error is not None:
                raise self._encryption_error
            return False
        if not await self._wait_for_handshake(strict=strict):
            if strict and self._encryption_error is not None:
                raise self._encryption_error
            return False
        if self._security is not None and not await self._wait_for_key(strict=strict):
            return False
        if self._send_packed is not None:
            packed = b"M:" + self._send_packed.pack(_json_loads(encoded))
            return await self._connection.send_bytes(packed)
        return await self._connection.send(encoded)

    async def _send_app_frame(
        self,
        encoded: str,
        *,
        priority: int,
        msg_type: str,
        allow_queue: bool,
    ) -> SendResult:
        if self._encryption_error is not None:
            raise self._encryption_error
        if not self._connection.is_connected:
            return self._queue_or_fail(encoded, priority, msg_type, allow_queue)
        if self._rate_limiter is not None and not self._rate_limiter.try_consume():
            self._note_rate_limited(msg_type)
            return SendResult.FAILED
        if await self._transmit(encoded, strict=True):
            self._stats.messages_sent += 1
            self._stats.bytes_sent += len(encoded.encode("utf-8"))
            return SendResult.SENT
        return self._queue_or_fail(encoded, priority, msg_type, allow_queue)

    def _queue_or_fail(
        self, encoded: str, priority: int, msg_type: str, allow_queue: bool
    ) -> SendResult:
        if (
            allow_queue
            and self._offline_queue.enabled
            and self._offline_queue.enqueue(
                encoded, priority=priority, msg_type=msg_type
            )
        ):
            return SendResult.QUEUED
        return SendResult.FAILED

    def _note_rate_limited(self, msg_type: str) -> None:
        self._rate_limited_total += 1
        now = time.monotonic()
        if now - self._last_rate_limit_log >= 5.0:
            self._last_rate_limit_log = now
            logger.warning(
                "Outbound rate limit reached, '%s' not sent (%d dropped so far; "
                "AsyncWSEClient(rate_limit=...) sets the limit, None disables it)",
                msg_type,
                self._rate_limited_total,
            )

    async def _send_internal(
        self, type: str, payload: dict[str, Any], *, priority: int
    ) -> bool:
        """Protocol replies (health / sync / metrics): not rate limited, never
        queued for replay."""
        encoded = self._codec.encode(type, payload, priority=priority)
        ok = await self._transmit(encoded)
        if ok:
            self._stats.messages_sent += 1
            self._stats.bytes_sent += len(encoded.encode("utf-8"))
        return ok

    def _topic_streams(self, topic: str) -> dict[str, int]:
        """Where the client stands in each stream of *topic*."""
        return self._sequencer.topic_streams(topic)

    def _subscription_payload(
        self, topics: list[str], *, recover: bool
    ) -> tuple[dict[str, Any], dict[str, tuple[tuple[str, int], dict[str, int]]]]:
        payload: dict[str, Any] = {"action": "subscribe", "topics": topics}
        info: dict[str, tuple[tuple[str, int], dict[str, int]]] = {}
        if recover and self._server_recovery_enabled:
            for topic in topics:
                streams = self._topic_streams(topic)
                pos = self._sequencer.topic_position(topic)
                if pos is not None:
                    info[topic] = (pos, streams)
            if info:
                payload["recover"] = True
                payload["recovery"] = {
                    t: {"epoch": pos[0], "offset": pos[1], "streams": streams}
                    for t, (pos, streams) in info.items()
                }
        return payload, info

    def _note_recovery_requested(
        self,
        info: dict[str, tuple[tuple[str, int], dict[str, int]]],
        *,
        rearm: bool = False,
    ) -> None:
        """Requests sent for *info*'s topics: the gap of each stream asked
        for waits for its own answer. *rearm*: the request goes after a
        reconnect, and the gap's wait starts again with it."""
        for topic, (pos, streams) in info.items():
            request = _Request(
                id=self._next_request, for_gap=False, named=tuple(streams)
            )
            self._next_request += 1
            gap = self._gaps.get((topic, pos[0]))
            if gap is not None:
                gap.request = request.id
                gap.requested_from = pos[1]
                request.for_gap = True
                if rearm:
                    self._arm_gap(topic, gap)
            self._recovery_requests.setdefault(topic, deque()).append(request)

    def _arm_gap(self, topic: str, gap: _GapState) -> None:
        """The gap gives up recovery_timeout from now."""
        if gap.timer is not None:
            gap.timer.cancel()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            gap.timer = None
            return
        gap.timer = loop.call_later(
            self._recovery_timeout, self._on_gap_deadline, topic, gap
        )

    async def _resubscribe(self) -> None:
        """Subscribe this connection to the topics wanted before it opened."""
        topics = sorted(
            t for t in self._resubscribe_pending if t in self._subscribed_topics
        )
        self._resubscribe_pending = set()
        if not topics:
            return
        payload, info = self._subscription_payload(topics, recover=True)
        msg = self._codec.encode(
            "subscription_update", payload, priority=MessagePriority.HIGH
        )
        logger.info("Re-subscribing to %d topics (recover=%s)", len(topics), bool(info))
        if await self._transmit(msg):
            self._note_recovery_requested(info, rearm=True)
            self._stats.messages_sent += 1
            # Gaps in the topics' other streams ask for themselves.
            for (topic, _epoch), gap in list(self._gaps.items()):
                if topic in topics and gap.request is None:
                    self._fire_task(self._request_recovery(topic, gap))
        else:
            logger.warning("Re-subscribe failed; retried on the next connection")

    async def _flush_offline_queue(self) -> None:
        """Replay queued messages after reconnection."""
        messages = self._offline_queue.drain()
        if not messages:
            return
        logger.info("Flushing %d offline-queued messages", len(messages))
        for i, (encoded, priority) in enumerate(messages):
            ok = await self._transmit(encoded)
            if ok:
                self._stats.messages_sent += 1
                self._stats.bytes_sent += len(encoded.encode("utf-8"))
                continue
            remaining = messages[i:]
            logger.warning(
                "Offline queue flush interrupted, re-enqueuing %d messages",
                len(remaining),
            )
            for msg, pri in remaining:
                self._offline_queue.enqueue(msg, priority=pri)
            break

    def _fire_task(self, coro: Any) -> None:
        """Schedule a coroutine with a strong reference to prevent GC."""
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # -- Internal: message handling -------------------------------------------

    def _on_raw_message(self, data: str | bytes) -> None:
        """Decode raw WebSocket frame and dispatch."""
        now = time.time()
        last = self._last_frame_at
        if last is None or now - last > IDLE_TIMEOUT:
            self._frames_since = now
        self._last_frame_at = now
        self._stats.messages_received += 1
        if isinstance(data, bytes):
            self._stats.bytes_received += len(data)
        else:
            self._stats.bytes_received += len(data.encode("utf-8"))

        security = self._security
        # Unsealed before the session: only what the handshake needs.
        handshake_only = False
        if (
            security is not None
            and security.session_mode
            and not (isinstance(data, bytes) and data[:2] == b"E:")
        ):
            # An E2E session is opened first and then every frame is sealed:
            # unsealed come only the server's answer to the offer, an error
            # and heartbeats before it. Anything else was put in by whatever
            # sits between client and server.
            if security.is_enabled:
                reason = "a plaintext frame inside the session"
            elif _handshake_frame(data):
                reason = None
                handshake_only = True
            else:
                reason = "a plaintext frame before the session"
            if reason is not None:
                self._start_encryption_failure(
                    WSEEncryptionError(f"E2E frame refused: {reason}")
                )
                return
        result = self._codec.decode(data)
        if security is not None and security.broken is not None:
            # A frame out of order or forged: the session cannot go on.
            self._start_encryption_failure(
                WSEEncryptionError(f"E2E frame refused: {security.broken}")
            )
            return
        if result is None:
            return

        events = result if isinstance(result, list) else [result]
        for event in events:
            if handshake_only:
                # Never a topic's message nor the application's: nothing
                # unsealed reaches either.
                if event.type not in _HANDSHAKE_TYPES:
                    continue
                event = replace(event, topic=None, epoch=None, offset=None)
            try:
                self._process_event(event)
            except Exception as exc:
                logger.warning(
                    "Error processing '%s' event, dropped: %s", event.type, exc
                )

    def _process_event(self, event: WSEEvent) -> None:
        """Dedup + gap-detect, handle system events, then dispatch.

        Stamped topic messages (tp/e/o) are deduped and ordered by per-topic
        (epoch, offset) -- authoritative and reconnect-safe. The server's `seq`
        is a process-global counter, NOT a per-topic order, so it must not drive
        reordering: doing so buffered control messages such as server_ready
        (which carries seq=1 while the client expected 0) and stalled the
        handshake. Unstamped messages fall back to id-based dedup.
        """
        if (
            event.topic
            and event.epoch
            and event.offset is not None
            and event.type not in _PROTOCOL_TYPES
        ):
            self._process_stamped(event)
            return
        if event.id and self._sequencer.is_duplicate(event.id):
            return
        self._dispatch_event(event)

    # -- Internal: gap recovery -----------------------------------------------
    #
    # A topic never goes dark after an offset gap. With recovery available the
    # client asks the server to replay from the last delivered offset and holds
    # the messages that arrive past the hole (bounded) for recovery_timeout
    # seconds; replayed messages are delivered in order and the held ones
    # follow. If the replay does not fill the hole in time -- or recovery is
    # not available, the server says it cannot replay, or the buffer fills --
    # a recovery_gap event reports the missing offsets and the held messages
    # are delivered.

    def _process_stamped(self, event: WSEEvent) -> None:
        topic, epoch, offset = event.topic, event.epoch, event.offset
        assert topic is not None and epoch is not None and offset is not None
        self._live_since.setdefault(topic, time.time())
        # A hole in another stream of the topic does not hold this one.
        gap = self._gaps.get((topic, epoch))

        verdict = self._sequencer.check_topic_stamp(topic, epoch, offset)
        if verdict == "duplicate":
            return
        if verdict == "deliver":
            self._unrecoverable.pop(topic, None)
            self._dispatch_event(event)
            if gap is not None:
                self._drain_gap(topic, epoch)
            return

        # verdict == "gap"
        if gap is not None:
            gap.buffer.setdefault(offset, event)
            if len(gap.buffer) > RECOVERY_BUFFER_MAX:
                self._give_up_gap(topic, epoch, "overflow")
            return
        if self._unrecoverable.get(topic) == epoch:
            # The server already said it cannot replay this topic's history.
            del self._unrecoverable[topic]
            self._skip_gap(event, "not_recovered")
            return
        if not self._start_gap(event):
            self._skip_gap(event, "unavailable")

    def _recovery_available(self) -> bool:
        return (
            self._recovery_timeout > 0
            and self._server_recovery_enabled
            and self._recovery_unanswered < RECOVERY_MAX_UNANSWERED
            and self._connection.is_connected
        )

    def _start_gap(self, event: WSEEvent) -> bool:
        """Hold *event* and request recovery. False if recovery is not possible."""
        topic, epoch, offset = event.topic, event.epoch, event.offset
        assert topic is not None and epoch is not None and offset is not None
        if not self._recovery_available():
            return False
        if self._sequencer.stream_position(topic, epoch) is None:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        if (topic, epoch) in self._gaps:
            return False
        gap = _GapState(epoch=epoch)
        gap.buffer[offset] = event
        gap.timer = loop.call_later(
            self._recovery_timeout, self._on_gap_deadline, topic, gap
        )
        self._gaps[(topic, epoch)] = gap
        self._fire_task(self._request_recovery(topic, gap))
        return True

    async def _request_recovery(self, topic: str, gap: _GapState) -> None:
        """Ask the server to replay *topic* from its anchor (gap fill).

        subscribe() can't be reused here because it filters already-subscribed
        topics; this sends the recovery subscription_update directly."""
        if self._gaps.get((topic, gap.epoch)) is not gap or gap.request is not None:
            return
        # From the last message of the stream with the hole (where a server
        # before 3.1 replays from), with where the client stands in each.
        pos = self._sequencer.stream_position(topic, gap.epoch)
        if pos is None:
            return
        streams = self._topic_streams(topic)
        payload = {
            "action": "subscribe",
            "topics": [topic],
            "recover": True,
            "recovery": {
                topic: {"epoch": pos[0], "offset": pos[1], "streams": streams}
            },
        }
        msg = self._codec.encode(
            "subscription_update", payload, priority=MessagePriority.HIGH
        )
        self._note_recovery_requested({topic: (pos, streams)})
        request = gap.request
        try:
            ok = await self._transmit(msg)
        except WSEError:
            ok = False
        if not ok and self._gaps.get((topic, gap.epoch)) is gap:
            # Not sent: the resubscribe of the next connection or the
            # deadline takes over.
            gap.request = None
            queue = self._recovery_requests.get(topic)
            if queue is not None:
                for sent in [r for r in queue if r.id == request]:
                    queue.remove(sent)
                if not queue:
                    del self._recovery_requests[topic]
            logger.debug("Recovery request for '%s' not sent", topic)

    def _drain_gap(self, topic: str, epoch: str) -> None:
        """Deliver held messages that became in order; close a filled gap."""
        gap = self._gaps.get((topic, epoch))
        if gap is None:
            return
        while True:
            pos = self._sequencer.stream_position(topic, gap.epoch)
            if pos is None:
                break
            for off in [o for o in gap.buffer if o <= pos[1]]:
                del gap.buffer[off]
            nxt = gap.buffer.pop(pos[1] + 1, None)
            if nxt is None:
                break
            self._sequencer.set_topic_position(topic, gap.epoch, pos[1] + 1)
            self._dispatch_event(nxt)
        if not gap.buffer and self._gaps.get((topic, epoch)) is gap:
            self._close_gap(topic, gap)
            self._gap_stats["recovered"] += 1
            logger.info("Recovered offset gap on '%s'", topic)

    def _close_gap(self, topic: str, gap: _GapState) -> None:
        if gap.timer is not None:
            gap.timer.cancel()
            gap.timer = None
        if self._gaps.get((topic, gap.epoch)) is gap:
            del self._gaps[(topic, gap.epoch)]

    def _give_up_gap(self, topic: str, epoch: str, reason: str) -> None:
        """Stop waiting: report the holes, deliver the held messages."""
        gap = self._gaps.get((topic, epoch))
        if gap is None:
            return
        self._close_gap(topic, gap)
        pos = self._sequencer.stream_position(topic, gap.epoch)
        last = pos[1] if pos is not None else None
        for off in sorted(gap.buffer):
            if last is not None and off <= last:
                continue
            if last is not None and off > last + 1:
                self._report_gap(topic, gap.epoch, last + 1, off - 1, reason)
            self._sequencer.set_topic_position(topic, gap.epoch, off)
            self._dispatch_event(gap.buffer[off])
            last = off

    def _skip_gap(self, event: WSEEvent, reason: str) -> None:
        """Accept a gap at once: report it, re-baseline, deliver *event*."""
        topic, epoch, offset = event.topic, event.epoch, event.offset
        assert topic is not None and epoch is not None and offset is not None
        pos = self._sequencer.stream_position(topic, epoch)
        if pos is not None and offset > pos[1] + 1:
            self._report_gap(topic, epoch, pos[1] + 1, offset - 1, reason)
        self._sequencer.set_topic_position(topic, epoch, offset)
        self._dispatch_event(event)

    def _on_gap_deadline(self, topic: str, gap: _GapState) -> None:
        if self._gaps.get((topic, gap.epoch)) is not gap:
            return
        gap.timer = None
        if gap.request is not None and not gap.answered:
            # Nothing came back at all: the application may not answer
            # recovery requests. After a few, stop asking on this connection.
            self._recovery_unanswered += 1
        self._give_up_gap(topic, gap.epoch, "timeout")

    def _report_gap(
        self,
        topic: str,
        epoch: str,
        first: int | None,
        last: int | None,
        reason: str,
    ) -> None:
        """Surface messages a topic will not get as a ``recovery_gap`` event."""
        missed = last - first + 1 if first is not None and last is not None else None
        self._gap_stats["gaps"] += 1
        if missed:
            self._gap_stats["missed"] += missed
        logger.warning(
            "Topic '%s' lost offsets %s..%s (%s messages, %s)",
            topic,
            first,
            last,
            missed if missed is not None else "unknown",
            reason,
        )
        event = WSEEvent(
            type=RECOVERY_GAP_EVENT,
            payload={
                "topic": topic,
                "epoch": epoch,
                "first_missing": first,
                "last_missing": last,
                "missed": missed,
                "reason": reason,
            },
            category="WSE",
            topic=topic,
            epoch=epoch,
        )
        self._invoke_handlers(event)
        self._enqueue(event)

    def _on_recovery_reply(
        self, topic: str, info: dict[str, Any], listed_streams: bool = False
    ) -> _Request | None:
        """Handle the per-topic recovery info of a subscription_update; the
        request it answered."""
        recovered = bool(info.get("recovered", True))
        queue = self._recovery_requests.get(topic)
        if not queue:
            return None  # informational reply (plain subscribe): never moves the anchor
        request = queue.popleft()
        if not queue:
            del self._recovery_requests[topic]
        self._recovery_unanswered = 0
        gap = next(
            (
                g
                for (t, _epoch), g in self._gaps.items()
                if t == topic and g.request == request.id
            ),
            None,
        )

        if gap is not None:
            gap.request = None
            gap.answered = True
            self._drain_gap(topic, gap.epoch)  # the replay arrived before this reply
            if self._gaps.get((topic, gap.epoch)) is not gap:
                return request  # filled
            pos = self._sequencer.stream_position(topic, gap.epoch)
            progressed = (
                pos is not None
                and gap.requested_from is not None
                and pos[1] > gap.requested_from
            )
            # A replay the server's outbound budget cut short answers
            # recovered=False in the same history, past where it was asked
            # from: ask again from where it got to.
            if progressed and (recovered or info.get("epoch") == gap.epoch):
                self._fire_task(self._request_recovery(topic, gap))
            else:
                self._give_up_gap(
                    topic, gap.epoch, "incomplete" if recovered else "not_recovered"
                )
            return request

        if request.for_gap:
            return request  # the answer for a gap settled meanwhile
        # Replayed messages advanced the anchor as they arrived; a server that
        # lists its streams says with them what was missed.
        if recovered or listed_streams:
            return request

        # The server cannot replay after our anchor. Nothing moves the anchor
        # here -- messages still in flight would be dropped as duplicates; the
        # next gap on this topic is reported at once instead of retried.
        pos = self._sequencer.topic_position(topic)
        epoch = info.get("epoch")
        if pos is None:
            return request
        if epoch is None:
            # The server has no history of the topic any more (it
            # restarted): the stream this client was on is gone.
            self._report_gap(topic, pos[0], None, None, "epoch_changed")
            return request
        if str(epoch) != pos[0]:
            self._report_gap(topic, str(epoch), None, None, "epoch_changed")
            return request
        offset = info.get("offset")
        if isinstance(offset, int) and offset <= pos[1]:
            return request  # nothing was missed
        self._unrecoverable[topic] = pos[0]
        return request

    def _take_streams(
        self,
        topic: str,
        request: _Request | None,
        listed: dict[str, int],
        recovered: bool,
    ) -> None:
        """The streams the server holds of *topic*, as its reply lists them
        (from 3.1): where the client starts in each one it did not know; when
        a recovery did not recover, what each one it knew lost. A stream the
        request named that the server does not hold is forgotten: naming it
        again would fail every later recovery."""
        lost = not recovered and request is not None and not request.for_gap
        held = {epoch for (t, epoch) in self._gaps if t == topic}
        reports: list[tuple[str, int | None, int | None]] = []
        for epoch, offset in listed.items():
            if epoch in held:
                continue
            own = self._sequencer.stream_position(topic, epoch)
            if own is None:
                self._sequencer.raise_stream(topic, epoch, offset)
            elif lost and offset > own[1]:
                reports.append((epoch, own[1] + 1, offset))
                self._sequencer.raise_stream(topic, epoch, offset)
        for epoch in request.named if request is not None else ():
            if epoch in held or epoch in listed:
                continue
            own = self._sequencer.stream_position(topic, epoch)
            if lost and own is not None:
                reports.append((epoch, own[1] + 1, None))
            self._sequencer.forget_stream(topic, epoch)
        if lost and not reports:
            # Not recovered with nothing to show for it: the server cannot
            # show the client missed nothing.
            pos = self._sequencer.topic_position(topic)
            if pos is not None:
                reports.append((pos[0], None, None))
        for epoch, first, last in reports:
            self._report_gap(topic, epoch, first, last, "not_recovered")

    def _forget_topic(self, topic: str) -> None:
        """Drop a topic's recovery state (unsubscribed)."""
        self._sequencer.clear_topic(topic)
        for (t, _epoch), gap in list(self._gaps.items()):
            if t == topic:
                self._close_gap(topic, gap)
        self._recovery_requests.pop(topic, None)
        self._unrecoverable.pop(topic, None)
        self._live_since.pop(topic, None)

    # -- Internal: dispatch ---------------------------------------------------

    def _dispatch_event(self, event: WSEEvent) -> None:
        """Handle system events, invoke callbacks, enqueue for iterator."""
        # Fast system handler lookup (O(1) dict)
        handler = self._system_handlers.get(event.type)
        if handler is not None:
            try:
                handler(event)
            except Exception as exc:
                # A malformed system payload is dropped, never fatal.
                logger.warning("Malformed '%s' message ignored: %s", event.type, exc)
            # Internal-only events are NOT forwarded to user queue/handlers
            if event.type in _INTERNAL_ONLY_EVENTS:
                return

        # Invoke registered handlers
        self._invoke_handlers(event)

        # Enqueue for async iterator
        self._enqueue(event)

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
        payload = event.payload
        details = payload.get("details")
        if not isinstance(details, dict):
            details = {}
        if self._bind_session and self._signer is not None:
            # Sealed: the server says the session is bound to this client's
            # key. Otherwise the token at the upgrade was not this client's
            # (one put in its place on the way), or names no key.
            cnf = details.get("cnf")
            jkt = cnf.get("jkt") if isinstance(cnf, dict) else None
            if jkt != self._signer.thumbprint:
                self._start_binding_failure("server_ready names no key, or another")
                return
        conn_id = payload.get("connection_id") or details.get("connection_id")
        user_id = details.get("user_id")
        self._codec.connection_user = user_id if isinstance(user_id, str) else None
        self._take_signing_kid(details.get("signing"), sealed=True)
        self._connection.handle_server_ready(
            conn_id if isinstance(conn_id, str) else None
        )
        if self._server_ready_event is not None:
            self._server_ready_event.set()

        # Check if server supports recovery (from features in details)
        features = details.get("features")
        if isinstance(features, dict) and features.get("recovery"):
            self._server_recovery_enabled = True
        if isinstance(features, dict):
            self._take_history_ttl(features.get("history_ttl"))

        logger.info(
            "Server ready (connection_id=%s, recovery=%s)",
            conn_id,
            self._server_recovery_enabled,
        )
        self._maybe_complete_handshake()

    def _take_history_ttl(self, value: Any) -> None:
        """The server's history_ttl (seconds), when it names one."""
        if (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value > 0
        ):
            self._history_ttl = float(value)

    def _handle_server_hello(self, event: WSEEvent) -> None:
        """Handle server_hello with negotiated features and limits."""
        p = event.payload
        features = p.get("features")
        if not isinstance(features, dict):
            features = {}
        self._server_features = features

        # Server-enforced limits are nested under "limits" (max_message_size,
        # rate_limit_capacity, rate_limit_refill). They were read at the payload
        # top level before, so neither value was ever populated.
        limits = p.get("limits", {})
        if not isinstance(limits, dict):
            limits = {}

        max_msg_size = limits.get("max_message_size")
        if isinstance(max_msg_size, int):
            self._server_max_message_size = max_msg_size

        rate_cap = limits.get("rate_limit_capacity")
        if isinstance(rate_cap, (int, float)):
            self._server_rate_limit = int(rate_cap)

        ping_interval = p.get("ping_interval")
        if isinstance(ping_interval, (int, float)) and ping_interval > 0:
            self._server_ping_interval_ms = int(ping_interval)

        if features.get("recovery"):
            self._server_recovery_enabled = True
        self._take_history_ttl(features.get("history_ttl"))
        self._take_signing_kid(p.get("signing"), sealed=False)
        # A 3.x server always sends server_ready (at once with jwt_secret,
        # from accept_connection() without), and takes nothing before it:
        # only an older server's server_hello completes the handshake.
        self._server_hello_seen = not _sends_server_ready(p.get("server_version"))

        # Complete the ECDH key exchange -- or fail closed.
        security = self._security
        if security is not None and not security.is_enabled:
            try:
                security.accept_server_hello(p)
            except Exception as exc:
                self._start_encryption_failure(
                    WSEEncryptionError(f"E2E key exchange failed: {exc}")
                )
                return
            logger.info("E2E encryption enabled")
            th = security.transcript_hash
            if self._bind_session and self._signer is not None and th is not None:
                # Bound to this client's key: its proof is the first message
                # the session seals, before anything waiting for the key.
                self._fire_task(self._send_proof(self._signer.prove_session(th)))
            else:
                _resolve(self._key_ready)

        server_version = p.get("server_version", "unknown")
        logger.info(
            "Server hello: version=%s, features=%s",
            server_version,
            list(self._server_features.keys()),
        )
        self._maybe_complete_handshake()

    async def _send_proof(self, proof: str) -> None:
        """The session's first sealed message, then the rest may go."""
        try:
            sent = await self._connection.send(proof)
        except WSEError:
            sent = False
        if sent:
            _resolve(self._key_ready)
        # Not sent: the connection is going, and the next one proves again.

    def _start_binding_failure(self, reason: str) -> None:
        """The session is not bound to this client's key: end the client."""
        err = WSEAuthError(
            f"the server did not bind the session to this client's key ({reason}); "
            "for a token that names no key, pass e2e_bind_session=False"
        )
        logger.error("%s -- closing the connection", err)
        _reject(self._key_ready, err)
        _reject(self._handshake, err)
        self._abort_task = asyncio.ensure_future(self._connection.abort(err))
        self._background_tasks.add(self._abort_task)
        self._abort_task.add_done_callback(self._background_tasks.discard)

    def _take_signing_kid(self, signing: Any, *, sealed: bool) -> None:
        """The server's signing key id, the audience of what this client
        signs. In an E2E session only the sealed server_ready names it; with
        pinned server signing keys it must be one of them."""
        if not isinstance(signing, dict) or not isinstance(signing.get("kid"), str):
            return
        security = self._security
        if not sealed and security is not None and security.session_mode:
            return  # server_hello is not sealed: server_ready says it
        kid = signing["kid"]
        pinned = self._codec.server_keys
        if pinned and kid not in pinned:
            logger.warning(
                "The server names signing key %s, which is not pinned; not signing", kid
            )
            self._server_signing_kid = None
            return
        self._server_signing_kid = kid

    def _start_encryption_failure(self, err: WSEEncryptionError) -> None:
        """Fail closed: never fall back to plaintext, end the client."""
        if self._encryption_error is not None:
            return
        self._encryption_error = err
        logger.error("%s -- closing the connection (no plaintext fallback)", err)
        _reject(self._key_ready, err)
        _reject(self._handshake, err)
        self._abort_task = asyncio.ensure_future(self._connection.abort(err))
        self._background_tasks.add(self._abort_task)
        self._abort_task.add_done_callback(self._background_tasks.discard)

    def _handle_subscription_update(self, event: WSEEvent) -> None:
        p = event.payload
        failed_topics = p.get("failed_topics")
        if failed_topics:
            logger.warning("Subscription failed for: %s", failed_topics)
        success_topics = p.get("success_topics")
        if success_topics:
            logger.debug("Subscribed to: %s", success_topics)
            if isinstance(success_topics, list):
                now = time.time()
                for topic in success_topics:
                    if isinstance(topic, str):
                        self._live_since.setdefault(topic, now)

        recovery = p.get("recovery")
        if isinstance(recovery, dict):
            for topic, info in recovery.items():
                if isinstance(topic, str) and isinstance(info, dict):
                    listed = _listed_streams(info.get("streams"))
                    request = self._on_recovery_reply(topic, info, listed is not None)
                    if listed is not None:
                        self._take_streams(
                            topic, request, listed, bool(info.get("recovered", True))
                        )
        # What the server took and refused, for the application's handlers.
        self._invoke_handlers(event)

    def _handle_error(self, event: WSEEvent) -> None:
        """Handle server error with code classification (matches TS error handler)."""
        p = event.payload
        code = p.get("code", "UNKNOWN_ERROR")
        message = str(p.get("message", "Unknown error"))
        recoverable = p.get("recoverable", True)
        severity = p.get("severity", "error")

        if code == "KEY_EXCHANGE_FAILED" and self._security is not None:
            self._start_encryption_failure(
                WSEEncryptionError(f"Server-side key exchange failed: {message}")
            )
        elif code == "E2E_UNSUPPORTED" and self._security is not None:
            # Another attempt would be refused the same way (a server
            # without this E2E design): stop instead of reconnecting.
            self._start_encryption_failure(
                WSEEncryptionError(f"The server refused this E2E design: {message}")
            )
        elif code in ("AUTH_FAILED", "AUTH_REQUIRED"):
            logger.error("Auth failed: %s", message)
        elif code in ("RATE_LIMITED", "RATE_LIMIT_EXCEEDED") or "Rate limit" in message:
            retry_after = p.get("retry_after") or p.get("retryAfter", 0)
            logger.warning(
                "Rate limit exceeded (retry after %ss): %s", retry_after, message
            )
        elif code == "SUBSCRIPTION_FAILED":
            details = p.get("details")
            topics = details.get("topics", []) if isinstance(details, dict) else []
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
        await self._send_internal(
            "health_check_response",
            {
                "client_version": CLIENT_VERSION,
                "stats": stats,
                "diagnostics": {
                    "quality": diag.quality.value,
                    "latency_ms": diag.round_trip_time,
                    "jitter_ms": diag.jitter,
                },
                "queue_size": self._event_queue.qsize() if self._event_queue else 0,
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
        self._snapshot_inflight.clear()
        logger.debug("Snapshot complete: %s events for topics %s", count, topics)
        # User handlers invoked by _dispatch_event fall-through (not internal-only)

    def _handle_rate_limit_warning(self, event: WSEEvent) -> None:
        """Log the server's warning (its limit on messages it sends to this
        connection); the event also reaches user handlers."""
        remaining = event.payload.get("remaining", 0)
        limit = event.payload.get("limit", 0)
        logger.warning("Rate limit warning: %s/%s remaining", remaining, limit)
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
        await self._send_internal(
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
        await self._send_internal(
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
                    "size": self._event_queue.qsize() if self._event_queue else 0,
                    "max_size": self._event_queue.maxsize if self._event_queue else 0,
                },
                "diagnostics": {
                    "quality": diag.quality.value,
                    "latency_ms": diag.round_trip_time,
                    "jitter_ms": diag.jitter,
                    "packet_loss": diag.packet_loss,
                },
                "circuit_breaker": {
                    "state": self._breaker_state(),
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
            inner_payload = inner.get("p", inner.get("payload", {}))
            inner_event = WSEEvent(
                type=str(inner.get("t", inner.get("type", "unknown"))),
                payload=inner_payload
                if isinstance(inner_payload, dict)
                else {"data": inner_payload},
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
        """A server ping that came sealed (an E2E session): answered with a
        sealed PONG. Plaintext pings are answered by the connection itself."""
        self._fire_task(self._connection.answer_ping(event.payload))

    def _handle_forward_event(self, event: WSEEvent) -> None:
        """Generic handler for response events (forwarded by _dispatch_event)."""
        pass  # User handlers invoked by _dispatch_event fall-through

    def _on_state_change(self, state: ConnectionState) -> None:
        if state == ConnectionState.RECONNECTING:
            # A connection was lost and reconnecting started. The sequencer is
            # NOT reset: per-topic (epoch, offset) positions and the dedup window
            # must survive so replayed/duplicate messages are recognised.
            self._stats.reconnect_count += 1
