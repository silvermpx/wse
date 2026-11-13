# =============================================================================
# WSE Python Client -- Connection Manager
# =============================================================================
#
# WebSocket lifecycle management: connect, auth, heartbeat, reconnect.
# Port of ts-client/services/ConnectionManager.ts.
#
# Close-code contract (the TS client follows the same rules):
#   * A close the client initiates (disconnect() / close()) is final.
#   * The server rejecting the credentials -- close 4401 / 4403 / 1008, or
#     HTTP 401 / 403 on the upgrade -- ends the client with WSEAuthError.
#     With a token_provider the client first fetches a fresh token and
#     reconnects with backoff; AUTH_MAX_CONSECUTIVE_FAILURES rejections in a
#     row end it.
#   * Every other server close (1000, 1001 server stop, 1005 server kick,
#     1011, 1012, 1013, 4300 drain, 4429, ...) and every network loss is
#     reconnected with backoff. A failed reconnect attempt is never terminal;
#     only ReconnectConfig.max_attempts (when not -1) ends the client.
#   * Until the first handshake of connect() completes, a close fails
#     connect() instead (WSEAuthError for the auth codes, WSEConnectionError
#     otherwise) and nothing reconnects in the background.
#
# Heartbeat ping / PONG and client_hello are control frames: always plaintext
# (the server answers pings on its plaintext fast path) and never rate limited.
# With E2E encryption configured every other frame is encrypted; before the key
# exchange completes nothing else is sent (fail closed).
# =============================================================================

from __future__ import annotations

import asyncio
import inspect
import json as _json
import math
import random
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import quote
from uuid import uuid4

import websockets
import websockets.asyncio.client
from websockets.exceptions import ConnectionClosed, InvalidStatus

from ._logging import logger
from .connection_pool import ConnectionPool
from .constants import (
    AUTH_CLOSE_CODES,
    AUTH_MAX_CONSECUTIVE_FAILURES,
    CLIENT_VERSION,
    CONNECTION_TIMEOUT,
    DEFAULT_MAX_MESSAGE_SIZE,
    HEARTBEAT_INTERVAL,
    IDLE_TIMEOUT,
    PREFIX_ENCRYPTED,
    PROTOCOL_VERSION,
    RECONNECT_ABSOLUTE_CAP,
    WS_CLOSE_ABNORMAL,
    WS_CLOSE_GOING_AWAY,
    WS_CLOSE_NO_STATUS,
    WS_CLOSE_NORMAL,
)
from .errors import (
    WSEAuthError,
    WSECircuitBreakerError,
    WSEConnectionError,
    WSEError,
    WSETimeoutError,
)
from .types import (
    ConnectionState,
    LoadBalancingStrategy,
    MessagePriority,
    ReconnectConfig,
    ReconnectMode,
    TokenProvider,
)

if TYPE_CHECKING:
    from .circuit_breaker import CircuitBreaker
    from .rate_limiter import TokenBucketRateLimiter
    from .security import SecurityManager


class _TokenProviderError(WSEAuthError):
    """The token_provider raised or returned something that is not a token."""


def _current_task() -> asyncio.Task[Any] | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


async def _close_quietly(ws_cm: Any) -> None:
    try:
        await ws_cm.__aexit__(None, None, None)
    except Exception:
        pass


def _received_close(ws: Any) -> tuple[int, str]:
    """Close code and reason the peer sent (1005 when it sent none).

    websockets >= 15 exposes them on the connection, 13 / 14 only on its
    protocol object.
    """
    code = getattr(ws, "close_code", None)
    reason = getattr(ws, "close_reason", None)
    if code is None:
        protocol = getattr(ws, "protocol", None)
        code = getattr(protocol, "close_code", None)
        reason = getattr(protocol, "close_reason", None)
    if not isinstance(code, int):
        code = WS_CLOSE_NO_STATUS
    return code, reason if isinstance(reason, str) else ""


class ConnectionManager:
    """Manages the WebSocket lifecycle: auth, heartbeat, reconnection.

    This is the low-level transport layer.  ``AsyncWSEClient`` uses it
    for all network I/O.
    """

    def __init__(
        self,
        url: str,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        reconnect: ReconnectConfig | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        extra_headers: dict[str, str] | None = None,
        on_message: Callable[[str | bytes], Any] | None = None,
        on_state_change: Callable[[ConnectionState], Any] | None = None,
        security: SecurityManager | None = None,
        endpoints: list[str] | None = None,
        load_balancing: LoadBalancingStrategy = LoadBalancingStrategy.WEIGHTED_RANDOM,
        max_message_size: int | None = DEFAULT_MAX_MESSAGE_SIZE,
        query_params: dict[str, str] | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._token_provider = token_provider
        self._reconnect_cfg = reconnect or ReconnectConfig()
        self._circuit_breaker = circuit_breaker
        # Optional: AsyncWSEClient applies its limiter to application sends
        # itself and passes None here.
        self._rate_limiter = rate_limiter
        self._extra_headers = extra_headers or {}
        self._security = security
        self._max_message_size = max_message_size
        self._query_params = dict(query_params or {})

        # Connection pool for multi-endpoint failover
        all_endpoints = endpoints or []
        if url not in all_endpoints:
            all_endpoints = [url] + all_endpoints
        self._pool: ConnectionPool | None = None
        if len(all_endpoints) > 1:
            self._pool = ConnectionPool(all_endpoints, strategy=load_balancing)
        self._active_endpoint: str = url

        # Callbacks
        self._on_message = on_message
        self._on_state_change = on_state_change
        self._on_pong: Callable[[float], Any] | None = None  # latency callback
        self._on_ping_sent: Callable[[], Any] | None = None
        # Set by AsyncWSEClient: a socket opened (before any frame is read), a
        # live socket went away, the manager gave up for good.
        self._on_open: Callable[[], Any] | None = None
        self._on_close: Callable[[], Any] | None = None
        self._on_terminal: Callable[[WSEError], Any] | None = None

        # State
        self._ws_cm: Any | None = None  # websocket context manager
        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._state = ConnectionState.DISCONNECTED
        self._is_connecting = False
        self._reconnect_attempts = 0
        self._consecutive_failures = 0
        self._auth_failures = 0
        self._server_ready = False
        self._client_hello_sent = False
        self._connection_id: str | None = None
        self._connected_at: float | None = None
        self._last_pong: float | None = None
        self._destroyed = False
        # True while the client itself is closing (disconnect / abort): a
        # close it causes is never reconnected.
        self._intentional_close = False
        # False until the first handshake of connect() completed: a close
        # before that fails connect() instead of reconnecting.
        self._reconnect_armed = False
        self._last_error: WSEError | None = None
        self._last_close: tuple[int, str] | None = None

        # Tasks
        self._recv_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._background_tasks: set[asyncio.Task[Any]] = set()

    def _fire_task(self, coro: Any) -> None:
        """Schedule a coroutine with a strong reference to prevent GC."""
        task = asyncio.ensure_future(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    # -- Properties -----------------------------------------------------------

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return (
            self._ws is not None
            and self._state == ConnectionState.CONNECTED
            and not self._destroyed
        )

    @property
    def is_ready(self) -> bool:
        return self.is_connected and self._server_ready

    @property
    def connection_id(self) -> str | None:
        return self._connection_id

    @property
    def connected_since(self) -> float | None:
        return self._connected_at

    @property
    def last_error(self) -> WSEError | None:
        """The error of the last failed connect or of the terminal failure."""
        return self._last_error

    @property
    def last_close(self) -> tuple[int, str] | None:
        """``(code, reason)`` of the last close the server or network caused."""
        return self._last_close

    @property
    def reconnect_armed(self) -> bool:
        return self._reconnect_armed

    # -- Connect / Disconnect -------------------------------------------------

    async def connect(self, initial_topics: list[str] | None = None) -> None:
        """Open the WebSocket (one attempt) and send ``client_hello``.

        *initial_topics* is accepted for compatibility and ignored: the server
        does not read topics from the URL, ``AsyncWSEClient`` subscribes after
        the handshake.

        Until :meth:`arm_reconnect` is called a close is reported as a
        terminal failure instead of being reconnected, because the caller is
        still waiting for the handshake and gets the error.
        """
        if self._destroyed:
            raise WSEConnectionError("ConnectionManager has been destroyed")
        if self.is_connected or self._is_connecting:
            return

        self._cancel_reconnect()
        self._intentional_close = False
        self._reconnect_armed = False
        self._last_error = None
        self._reconnect_attempts = 0
        self._auth_failures = 0

        self._is_connecting = True
        self._set_state(ConnectionState.CONNECTING)
        try:
            await self._attempt()
        except WSEError as exc:
            self._last_error = exc
            self._set_state(ConnectionState.ERROR)
            raise
        finally:
            self._is_connecting = False

    def arm_reconnect(self) -> None:
        """From now on a lost connection is reconnected in the background."""
        self._reconnect_armed = True

    def mark_handshake_complete(self) -> None:
        """The server confirmed this connection: reset the backoff, and from
        now on reconnect a lost connection (also when connect() has not
        resumed yet)."""
        self._reconnect_attempts = 0
        self._consecutive_failures = 0
        self._reconnect_armed = True

    def set_token(self, token: str | None) -> None:
        """Use *token* for the next connect / reconnect."""
        self._token = token
        self._auth_failures = 0

    async def _attempt(self) -> None:
        """One connection attempt: token, socket, loops, client_hello.

        Raises a :class:`WSEError` on failure.
        """
        if self._circuit_breaker and not self._circuit_breaker.can_execute():
            raise WSECircuitBreakerError("Circuit breaker is open")

        # Select endpoint from pool (failover support)
        if self._pool is not None:
            selected = self._pool.select_endpoint()
            self._url = selected
            self._active_endpoint = selected

        token = await self._resolve_token()
        try:
            ws_cm, ws = await self._open_socket(token)
        except WSEError:
            self._consecutive_failures += 1
            if self._circuit_breaker:
                self._circuit_breaker.record_failure()
            if self._pool is not None:
                self._pool.record_failure(self._active_endpoint)
            raise

        self._consecutive_failures = 0
        if self._circuit_breaker:
            self._circuit_breaker.record_success()
        if self._pool is not None:
            self._pool.record_success(self._active_endpoint)
            self._pool.add_connection(self._active_endpoint)
        await self._activate(ws_cm, ws)

    async def _resolve_token(self) -> str | None:
        """The token for this attempt: from token_provider when configured."""
        provider = self._token_provider
        if provider is None:
            return self._token
        try:
            result = provider()
            if inspect.isawaitable(result):
                result = await result
        except Exception as exc:
            raise _TokenProviderError(f"token_provider failed: {exc}") from exc
        if result is not None and not isinstance(result, str):
            raise _TokenProviderError(
                f"token_provider returned {type(result).__name__}, expected str or None"
            )
        self._token = result
        return result

    async def _open_socket(self, token: str | None) -> tuple[Any, Any]:
        """Low-level WebSocket open with auth headers."""
        headers = dict(self._extra_headers)

        # Auth: cookie for Rust server + Authorization header as fallback
        if token and token != "cookie":
            headers["Cookie"] = f"access_token={token}"
            headers["Authorization"] = f"Bearer {token}"

        ws_cm = websockets.asyncio.client.connect(
            self._build_url(),
            additional_headers=headers,
            max_size=self._max_message_size,
            open_timeout=None,  # asyncio.wait_for handles timeout
        )
        try:
            ws = await asyncio.wait_for(ws_cm.__aenter__(), timeout=CONNECTION_TIMEOUT)
        except asyncio.CancelledError:
            # disconnect() during a reconnect attempt: leave no socket behind.
            await _close_quietly(ws_cm)
            raise
        except asyncio.TimeoutError:
            await _close_quietly(ws_cm)
            raise WSETimeoutError(f"Connection timed out after {CONNECTION_TIMEOUT}s")
        except InvalidStatus as exc:
            await _close_quietly(ws_cm)
            status = exc.response.status_code
            if status in (401, 403):
                raise WSEAuthError(
                    f"Server rejected the handshake with HTTP {status}"
                ) from exc
            raise WSEConnectionError(f"Failed to connect: {exc}") from exc
        except Exception as exc:
            await _close_quietly(ws_cm)
            raise WSEConnectionError(f"Failed to connect: {exc}") from exc
        return ws_cm, ws

    async def _activate(self, ws_cm: Any, ws: Any) -> None:
        """Install a freshly opened socket and send client_hello first."""
        self._ws_cm = ws_cm
        self._ws = ws
        self._connected_at = time.monotonic()
        self._last_pong = time.monotonic()
        self._server_ready = False
        self._client_hello_sent = False
        self._connection_id = None

        # The client sets up its per-connection state before any frame of
        # this socket is read (the receive loop starts below).
        if self._on_open:
            self._on_open()
        self._set_state(ConnectionState.CONNECTED)

        self._recv_task = asyncio.create_task(self._recv_loop(ws))
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
        await self._send_client_hello(ws)

    async def disconnect(self) -> None:
        """Graceful, final shutdown: nothing reconnects afterwards."""
        self._intentional_close = True
        self._reconnect_armed = False
        self._cancel_reconnect()
        await self._teardown(WS_CLOSE_NORMAL, "Client disconnect")
        self._set_state(ConnectionState.DISCONNECTED)

    async def destroy(self) -> None:
        """Disconnect and mark permanently destroyed."""
        self._destroyed = True
        await self.disconnect()
        if self._pool is not None:
            self._pool.destroy()
            self._pool = None

    async def abort(self, error: WSEError) -> None:
        """End the connection for good with *error*.

        Used for failures the server cannot cure by reconnecting, e.g. a
        failed E2E key exchange: the socket is closed, nothing reconnects,
        the state becomes ERROR and *error* is reported as terminal.
        """
        if self._destroyed or self._state == ConnectionState.ERROR:
            return
        self._intentional_close = True
        self._reconnect_armed = False
        self._cancel_reconnect()
        await self._teardown(WS_CLOSE_NORMAL, str(error)[:120])
        self._fail(error)

    # -- Send -----------------------------------------------------------------

    async def send(self, data: str) -> bool:
        """Send an application frame.  Returns True on success.

        With E2E configured the frame is always encrypted: until the key
        exchange of the current connection completed nothing is sent (fail
        closed, never plaintext).
        """
        ws = self._ws
        if ws is None or self._destroyed:
            return False

        if self._rate_limiter and not self._rate_limiter.try_consume():
            logger.debug("Rate limit exceeded, not sending")
            return False

        frame: str | bytes = data
        if self._security is not None:
            if not self._security.is_enabled:
                logger.debug("E2E key exchange not complete; not sending plaintext")
                return False
            try:
                frame = PREFIX_ENCRYPTED + self._security.encrypt(data)
            except Exception as exc:
                logger.warning("Encryption failed, not sending: %s", exc)
                return False
        return await self._ws_send(ws, frame)

    async def send_bytes(self, data: bytes) -> bool:
        """Send a binary frame (encrypted if E2E is configured, fail closed)."""
        ws = self._ws
        if ws is None or self._destroyed:
            return False
        if self._rate_limiter and not self._rate_limiter.try_consume():
            logger.debug("Rate limit exceeded, not sending binary message")
            return False
        frame = data
        if self._security is not None:
            if not self._security.is_enabled:
                logger.debug("E2E key exchange not complete; not sending plaintext")
                return False
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                logger.warning("send_bytes: non-UTF-8 data cannot be E2E encrypted")
                return False
            frame = PREFIX_ENCRYPTED + self._security.encrypt(text)
        return await self._ws_send(ws, frame)

    async def send_control(self, data: str) -> bool:
        """Send a protocol control frame (ping, PONG, client_hello).

        Never rate limited. In an E2E session they are sealed like every
        other frame once the key exists (the server accepts nothing else);
        with the first E2E design they stay plaintext.
        """
        ws = self._ws
        if ws is None or self._destroyed:
            return False
        security = self._security
        if security is not None and security.session_mode and security.is_enabled:
            try:
                return await self._ws_send(
                    ws, PREFIX_ENCRYPTED + security.encrypt(data)
                )
            except Exception as exc:
                logger.warning("Encryption failed, not sending: %s", exc)
                return False
        return await self._ws_send(ws, data)

    @staticmethod
    async def _ws_send(ws: Any, frame: str | bytes) -> bool:
        try:
            await ws.send(frame)
            return True
        except ConnectionClosed:
            logger.debug("Send failed: connection closed")
            return False
        except Exception as exc:
            logger.debug("Send failed: %s", exc)
            return False

    # -- Public reconnect -----------------------------------------------------

    async def force_reconnect(self) -> None:
        """Public: tear down the current connection and reconnect.

        Also revives a manager that ended (ERROR), e.g. after
        :meth:`set_token` replaced a rejected token.
        """
        if self._destroyed:
            return
        self._intentional_close = False
        self._reconnect_attempts = 0
        self._auth_failures = 0
        self._last_error = None
        await self._force_reconnect()

    # -- Internal: receive loop -----------------------------------------------

    async def _recv_loop(self, ws: Any) -> None:
        """Read messages from the WebSocket until it closes."""
        code, reason = WS_CLOSE_ABNORMAL, ""
        try:
            async for message in ws:
                try:
                    self._handle_raw_message(message)
                except Exception as exc:
                    # One bad frame never tears down the connection.
                    logger.warning("Error handling a frame, dropped: %s", exc)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed as exc:
            if exc.rcvd is not None:
                code, reason = exc.rcvd.code, exc.rcvd.reason
        except Exception as exc:
            logger.warning("Receive loop error: %s", exc)
            reason = str(exc)
        else:
            # websockets ends `async for` quietly on a 1000 / 1001 / 1005
            # close (ConnectionClosedOK); read the code it received.
            code, reason = _received_close(ws)
        await self._connection_lost(ws, code, reason)

    def _handle_raw_message(self, data: str | bytes) -> None:
        """Quick-check for PONG / PING at transport level, then forward."""
        if isinstance(data, str):
            # PONG handling (response to our PING)
            if data.startswith("WSE:PONG:"):
                self._last_pong = time.monotonic()
                self._record_pong_latency(data[9:])
                return
            if data.startswith("PONG:"):
                self._last_pong = time.monotonic()
                self._record_pong_latency(data[5:])
                return

            # Server-initiated PING -> respond with PONG immediately
            if data.startswith("WSE:PING:"):
                ts = data[9:]  # skip "WSE:PING:"
                self._fire_task(self._send_pong(ts))
                self._last_pong = time.monotonic()
                return
            if data.startswith("PING:"):
                ts = data[5:]  # skip "PING:"
                self._fire_task(self._send_pong(ts))
                self._last_pong = time.monotonic()
                return

            # PONG message (JSON with c field from Rust server)
            # Forward to client layer — _handle_pong_event records latency once
            # Only match top-level "t" field (first ~20 chars), not nested payloads
            data_start = data[:25]
            if '"t":"PONG"' in data_start or '"t": "PONG"' in data_start:
                self._last_pong = time.monotonic()
                if self._on_message:
                    self._on_message(data)
                return

            # PING message (JSON with c field, server sends lowercase)
            if (
                '"t":"PING"' in data_start
                or '"t": "PING"' in data_start
                or '"t":"ping"' in data_start
                or '"t": "ping"' in data_start
            ):
                self._fire_task(self._respond_ping_json(data))
                self._last_pong = time.monotonic()
                return

        # Forward to client
        if self._on_message:
            self._on_message(data)

    def _record_pong_latency(self, ts_str: str) -> None:
        """Extract latency from PONG timestamp and forward to callback."""
        if ts_str:
            try:
                ts = float(ts_str)
                latency = time.time() * 1000 - ts
                if 0 <= latency < 60000:
                    if self._on_pong:
                        self._on_pong(latency)
                    if self._pool is not None:
                        self._pool.record_success(self._active_endpoint, latency)
            except (ValueError, TypeError):
                pass

    async def _send_pong(self, timestamp: str) -> None:
        """Respond to server PING with PONG (control frame)."""
        await self.send_control(f"PONG:{timestamp}")

    async def _respond_ping_json(self, data: str) -> None:
        """Respond to a JSON-format PING from server (control frame)."""
        try:
            json_str = data[3:] if data.startswith("WSE") else data
            parsed = _json.loads(json_str)
            p = parsed.get("p", {})
            server_timestamp = (
                p.get("server_time") or p.get("timestamp") or int(time.time() * 1000)
            )
        except Exception:
            server_timestamp = int(time.time() * 1000)

        pong_msg = {
            "c": "WSE",
            "t": "PONG",
            "p": {
                "server_timestamp": server_timestamp,
                "client_timestamp": int(time.time() * 1000),
            },
            "v": PROTOCOL_VERSION,
        }
        await self.send_control(_json.dumps(pong_msg, separators=(",", ":")))

    def handle_server_ready(self, connection_id: str | None = None) -> None:
        """Called by the client layer when ``server_ready`` is decoded."""
        self._server_ready = True
        # server_ready follows a successful authentication.
        self._auth_failures = 0
        if connection_id:
            self._connection_id = connection_id

    async def _send_client_hello(self, ws: Any) -> None:
        """Send client_hello as the first frame of the connection.

        It is a plaintext control frame. With encryption it carries a fresh
        ECDH public key; the server answers with server_hello (and its key).
        Sent at open rather than at server_ready so the server_hello -- and
        the server's feature list -- also arrives when the application never
        sends server_ready.
        """
        # A fresh offer per connection; no key of an older connection
        # survives into this one.
        key_exchange = (
            self._security.hello_fields() if self._security is not None else {}
        )

        p: dict[str, Any] = {
            "client_version": CLIENT_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "features": {
                "compression": True,
                "encryption": self._security is not None,
                "batching": True,
                "priority_queue": True,
                "circuit_breaker": True,
                "offline_queue": True,
                "message_signing": False,
                "rate_limiting": True,
                "health_check": True,
                "metrics": True,
            },
            "connection_id": self._connection_id,
        }
        p.update(key_exchange)

        msg = {
            "c": "WSE",
            "t": "client_hello",
            "p": p,
            "id": str(uuid4()),
            "ts": datetime.now(UTC).isoformat(),
            "v": PROTOCOL_VERSION,
            "pri": MessagePriority.CRITICAL,
        }
        if ws is not self._ws:
            return
        if await self._ws_send(ws, _json.dumps(msg, separators=(",", ":"))):
            self._client_hello_sent = True
        else:
            logger.warning("client_hello could not be sent; the connection is closing")

    # -- Internal: heartbeat --------------------------------------------------

    async def _heartbeat_loop(self, ws: Any) -> None:
        """Send PING every HEARTBEAT_INTERVAL; detect idle timeout."""
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                return

            if ws is not self._ws:
                return

            # Check idle timeout
            if self._last_pong is not None:
                idle = time.monotonic() - self._last_pong
                if idle > IDLE_TIMEOUT:
                    logger.warning("Idle timeout (%.0fs), reconnecting", idle)
                    await self._force_reconnect()
                    return

            await self._send_ping()

    async def _send_ping(self) -> None:
        """Send one heartbeat PING (lowercase: the Rust server's fast path)."""
        ping_msg = {
            "c": "WSE",
            "t": "ping",
            "p": {"timestamp": int(time.time() * 1000)},
            "v": PROTOCOL_VERSION,
        }
        ok = await self.send_control(_json.dumps(ping_msg, separators=(",", ":")))
        if ok and self._on_ping_sent:
            self._on_ping_sent()
        elif not ok:
            logger.debug("PING send failed")

    # -- Internal: close handling and reconnection ----------------------------

    def _detach(self) -> tuple[Any, Any, asyncio.Task[None] | None]:
        """Drop the current socket from the manager and reset its state.

        Returns ``(ws, ws_cm, recv_task)`` for the caller to close / await.
        Never cancels the current task.
        """
        ws, ws_cm, recv_task = self._ws, self._ws_cm, self._recv_task
        had_socket = ws is not None
        self._ws = None
        self._ws_cm = None
        self._recv_task = None
        self._server_ready = False
        self._client_hello_sent = False
        self._connection_id = None

        heartbeat = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat is not None and heartbeat is not _current_task():
            heartbeat.cancel()

        if had_socket:
            if self._pool is not None:
                self._pool.remove_connection(self._active_endpoint)
            # Key state belongs to one connection: a send between the next
            # open and its key exchange must not use this connection's key.
            if self._security is not None:
                self._security.reset()
            if self._on_close:
                self._on_close()
        return ws, ws_cm, recv_task

    async def _teardown(self, code: int, reason: str) -> None:
        """Close the current socket on the client's initiative."""
        current = _current_task()
        ws, ws_cm, recv_task = self._detach()

        tasks: list[asyncio.Task[Any]] = []
        for task in (recv_task, *self._background_tasks):
            if task is not None and task is not current and not task.done():
                task.cancel()
                tasks.append(task)
        self._background_tasks.clear()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if ws is not None:
            try:
                await ws.close(code, reason)
            except Exception:
                pass
        if ws_cm is not None:
            await _close_quietly(ws_cm)

    async def _connection_lost(self, ws: Any, code: int, reason: str) -> None:
        """The server or the network closed *ws*."""
        if ws is not self._ws or self._intentional_close:
            return  # replaced, or the client is closing it itself
        self._last_close = (code, reason)
        logger.info("WebSocket closed: code=%s reason=%s", code, reason)
        _, ws_cm, _ = self._detach()
        if ws_cm is not None:
            await _close_quietly(ws_cm)
        if self._intentional_close or self._destroyed or self._ws is not None:
            return  # disconnect() / a new connection raced in meanwhile
        self._handle_close_code(code, reason)

    def _handle_close_code(self, code: int, reason: str) -> None:
        """React to a server- or network-caused close (see module header)."""
        detail = f"code {code}" + (f": {reason}" if reason else "")
        if code in AUTH_CLOSE_CODES:
            self._on_auth_rejected(
                WSEAuthError(f"Authentication rejected by the server ({detail})")
            )
            return
        if not self._reconnect_armed:
            self._fail(
                WSEConnectionError(
                    f"Connection closed by the server during the handshake ({detail})"
                )
            )
            return
        self._schedule_reconnect()

    def _on_auth_rejected(self, error: WSEAuthError) -> None:
        self._auth_failures += 1
        if (
            self._token_provider is not None
            and self._reconnect_armed
            and self._auth_failures < AUTH_MAX_CONSECUTIVE_FAILURES
        ):
            logger.warning("%s -- fetching a fresh token and reconnecting", error)
            self._schedule_reconnect()
            return
        self._fail(error)

    def _fail(self, error: WSEError) -> None:
        """Give up for good: ERROR state, report *error* as terminal."""
        self._cancel_reconnect()
        self._last_error = error
        logger.error("Connection ended: %s", error)
        self._set_state(ConnectionState.ERROR)
        if self._on_terminal:
            self._on_terminal(error)

    def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt with backoff."""
        if self._destroyed or self._intentional_close:
            return

        cfg = self._reconnect_cfg
        if cfg.max_attempts >= 0 and self._reconnect_attempts >= cfg.max_attempts:
            last = self._last_close
            closed = f"; last close code {last[0]}" if last else ""
            self._fail(
                WSEConnectionError(
                    f"Gave up reconnecting after {self._reconnect_attempts} "
                    f"attempt(s) (max_attempts={cfg.max_attempts}){closed}"
                )
            )
            return

        self._set_state(ConnectionState.RECONNECTING)
        delay = self._calculate_delay()
        logger.info(
            "Reconnecting in %.1fs (attempt %d/%s)",
            delay,
            self._reconnect_attempts + 1,
            cfg.max_attempts if cfg.max_attempts >= 0 else "inf",
        )
        self._cancel_reconnect()
        self._reconnect_task = asyncio.ensure_future(self._reconnect_after(delay))

    async def _reconnect_after(self, delay: float) -> None:
        """Wait, then try to reconnect. A failed attempt schedules the next."""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return
        if self._destroyed or self._intentional_close or self.is_connected:
            return

        self._reconnect_attempts += 1
        self._is_connecting = True
        try:
            await self._attempt()
        except asyncio.CancelledError:
            return
        except _TokenProviderError as exc:
            # Usually the auth service is down too: retry on the schedule.
            logger.warning("Reconnect attempt failed: %s", exc)
            self._schedule_reconnect()
        except WSEAuthError as exc:
            self._on_auth_rejected(exc)
        except Exception as exc:
            logger.info(
                "Reconnect attempt %d failed: %s", self._reconnect_attempts, exc
            )
            self._schedule_reconnect()
        finally:
            self._is_connecting = False

    async def _force_reconnect(self) -> None:
        """Tear down and reconnect."""
        if self._destroyed:
            return
        self._cancel_reconnect()
        # _teardown never cancels the current task: this runs inside the
        # heartbeat task on idle timeout, and cancelling it there would stop
        # the reconnect from ever being scheduled.
        await self._teardown(WS_CLOSE_GOING_AWAY, "Reconnecting")
        if self._destroyed or self._intentional_close:
            return
        # A forced reconnect always continues in the background.
        self._reconnect_armed = True
        self._schedule_reconnect()

    def _cancel_reconnect(self) -> None:
        task = self._reconnect_task
        self._reconnect_task = None
        if task is not None and task is not _current_task():
            task.cancel()

    def _calculate_delay(self) -> float:
        """Compute reconnect delay based on strategy. Never raises."""
        cfg = self._reconnect_cfg
        attempt = self._reconnect_attempts

        try:
            if cfg.mode == ReconnectMode.LINEAR:
                delay = cfg.base_delay + attempt * 1.0
            elif cfg.mode == ReconnectMode.FIBONACCI:
                delay = cfg.base_delay * _fib(min(attempt, 10))
            else:
                # Exponential / Adaptive (same curve)
                delay = cfg.base_delay * (cfg.factor**attempt)
        except OverflowError:
            # factor**attempt overflows after enough attempts (about 1750 at
            # factor 1.5, i.e. ~15 h of outage): the cap applies anyway.
            delay = cfg.max_delay
        if not math.isfinite(delay):
            delay = cfg.max_delay

        delay = min(delay, cfg.max_delay)

        # Absolute cap (5 minutes) matching TS scheduleReconnect
        delay = max(0.0, min(delay, RECONNECT_ABSOLUTE_CAP))

        if cfg.jitter:
            jitter_amount = delay * 0.2 * (random.random() - 0.5)
            delay = max(0.0, delay + jitter_amount)

        return delay

    # -- State management -----------------------------------------------------

    def _set_state(self, new_state: ConnectionState) -> None:
        if new_state == self._state:
            return
        old = self._state
        self._state = new_state
        logger.debug("State: %s -> %s", old.value, new_state.value)
        if self._on_state_change:
            self._on_state_change(new_state)

    # -- URL building ---------------------------------------------------------

    def _build_url(self, topics: list[str] | None = None) -> str:
        """Append query params to the base URL.

        *topics* is ignored (the server reads no topics from the URL).
        """
        sep = "&" if "?" in self._url else "?"

        enc = "true" if self._security is not None else "false"
        params: list[str] = [
            f"client_version={CLIENT_VERSION}",
            f"protocol_version={PROTOCOL_VERSION}",
            "compression=true",
            f"encryption={enc}",
        ]
        for key, value in self._query_params.items():
            params.append(f"{quote(key, safe='')}={quote(value, safe='')}")

        return self._url + sep + "&".join(params)


def _fib(n: int) -> int:
    """Fibonacci number for reconnect delay calculation."""
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a
