# =============================================================================
# WSE Python Client -- Connection Manager
# =============================================================================
#
# WebSocket lifecycle management: connect, auth, heartbeat, reconnect.
# Port of client/services/ConnectionManager.ts.
# =============================================================================

from __future__ import annotations

import asyncio
import json as _json
import random
import time

from uuid import uuid4
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable

import websockets
import websockets.asyncio.client
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
)

from ._logging import logger
from .constants import (
    CLIENT_HELLO_MAX_RETRIES,
    CLIENT_HELLO_RETRY_DELAY,
    CLIENT_VERSION,
    CONNECTION_TIMEOUT,
    HEARTBEAT_INTERVAL,
    IDLE_TIMEOUT,
    PROTOCOL_VERSION,
    RECONNECT_ABSOLUTE_CAP,
    WS_CLOSE_AUTH_EXPIRED,
    WS_CLOSE_AUTH_FAILED,
    WS_CLOSE_GOING_AWAY,
    WS_CLOSE_NORMAL,
    WS_CLOSE_POLICY_VIOLATION,
    WS_CLOSE_RATE_LIMITED,
)
from .errors import (
    WSEAuthError,
    WSECircuitBreakerError,
    WSEConnectionError,
    WSETimeoutError,
)
from .types import ConnectionState, MessagePriority, ReconnectConfig, ReconnectMode

if TYPE_CHECKING:
    from .circuit_breaker import CircuitBreaker
    from .rate_limiter import TokenBucketRateLimiter


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
        reconnect: ReconnectConfig | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        extra_headers: dict[str, str] | None = None,
        on_message: Callable[[str | bytes], Any] | None = None,
        on_state_change: Callable[[ConnectionState], Any] | None = None,
    ) -> None:
        self._url = url
        self._token = token
        self._reconnect_cfg = reconnect or ReconnectConfig()
        self._circuit_breaker = circuit_breaker
        self._rate_limiter = rate_limiter
        self._extra_headers = extra_headers or {}

        # Callbacks
        self._on_message = on_message
        self._on_state_change = on_state_change
        self._on_pong: Callable[[float], Any] | None = None  # latency callback
        self._on_ping_sent: Callable[[], Any] | None = None

        # State
        self._ws_cm: Any | None = None  # websocket context manager
        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._state = ConnectionState.DISCONNECTED
        self._is_connecting = False
        self._reconnect_attempts = 0
        self._consecutive_failures = 0
        self._server_ready = False
        self._client_hello_sent = False
        self._connection_id: str | None = None
        self._connected_at: float | None = None
        self._last_pong: float | None = None
        self._destroyed = False

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

    # -- Connect / Disconnect -------------------------------------------------

    async def connect(self, initial_topics: list[str] | None = None) -> None:
        """Open the WebSocket and complete the handshake."""
        if self._destroyed:
            raise WSEConnectionError("ConnectionManager has been destroyed")
        if self.is_connected or self._is_connecting:
            return

        if self._circuit_breaker and not self._circuit_breaker.can_execute():
            raise WSECircuitBreakerError("Circuit breaker is open")

        self._is_connecting = True
        self._set_state(ConnectionState.CONNECTING)

        try:
            await self._connect_ws(initial_topics)
            if self._circuit_breaker:
                self._circuit_breaker.record_success()
            self._consecutive_failures = 0
            self._reconnect_attempts = 0
        except Exception:
            self._consecutive_failures += 1
            if self._circuit_breaker:
                self._circuit_breaker.record_failure()
            raise
        finally:
            self._is_connecting = False

    async def _connect_ws(self, topics: list[str] | None = None) -> None:
        """Low-level WebSocket open with auth headers."""
        headers = dict(self._extra_headers)

        # Auth: cookie for Rust server + Authorization header as fallback
        if self._token and self._token != "cookie":
            headers["Cookie"] = f"access_token={self._token}"
            headers["Authorization"] = f"Bearer {self._token}"

        # Build URL with query params
        url = self._build_url(topics)

        try:
            self._ws_cm = websockets.asyncio.client.connect(
                url,
                additional_headers=headers,
                max_size=2**20,  # 1 MB
                open_timeout=None,  # asyncio.wait_for handles timeout
            )
            self._ws = await asyncio.wait_for(
                self._ws_cm.__aenter__(),
                timeout=CONNECTION_TIMEOUT,
            )
        except asyncio.TimeoutError:
            ws_cm = self._ws_cm
            self._ws_cm = None
            if ws_cm is not None:
                try:
                    await ws_cm.__aexit__(None, None, None)
                except Exception:
                    pass
            self._set_state(ConnectionState.ERROR)
            raise WSETimeoutError(f"Connection timed out after {CONNECTION_TIMEOUT}s")
        except Exception as exc:
            ws_cm = self._ws_cm
            self._ws_cm = None
            if ws_cm is not None:
                try:
                    await ws_cm.__aexit__(None, None, None)
                except Exception:
                    pass
            self._set_state(ConnectionState.ERROR)
            raise WSEConnectionError(f"Failed to connect: {exc}") from exc

        self._set_state(ConnectionState.CONNECTED)
        self._connected_at = time.monotonic()
        self._server_ready = False
        self._client_hello_sent = False

        # Start receive + heartbeat loops
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        """Graceful shutdown."""
        self._cancel_reconnect()
        self._server_ready = False
        self._client_hello_sent = False
        self._connection_id = None

        # Cancel tasks and await completion before closing the socket
        tasks_to_await: list[asyncio.Task[Any]] = []
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            tasks_to_await.append(self._heartbeat_task)
            self._heartbeat_task = None
        if self._recv_task:
            self._recv_task.cancel()
            tasks_to_await.append(self._recv_task)
            self._recv_task = None
        for task in self._background_tasks:
            task.cancel()
            tasks_to_await.append(task)
        self._background_tasks.clear()
        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        if self._ws_cm:
            try:
                await self._ws_cm.__aexit__(None, None, None)
            except Exception:
                pass
            self._ws_cm = None
        elif self._ws:
            try:
                await self._ws.close(WS_CLOSE_NORMAL, "Client disconnect")
            except Exception:
                pass
        self._ws = None

        self._set_state(ConnectionState.DISCONNECTED)

    async def destroy(self) -> None:
        """Disconnect and mark permanently destroyed."""
        self._destroyed = True
        await self.disconnect()

    # -- Send -----------------------------------------------------------------

    async def send(self, data: str) -> bool:
        """Send a text frame.  Returns True on success."""
        if not self._ws:
            return False

        if self._rate_limiter and not self._rate_limiter.try_consume():
            logger.warning("Rate limit exceeded, dropping message")
            return False

        try:
            await self._ws.send(data)
            return True
        except ConnectionClosed:
            logger.debug("Send failed: connection closed")
            return False
        except Exception as exc:
            logger.debug("Send failed: %s", exc)
            return False

    async def send_bytes(self, data: bytes) -> bool:
        """Send a binary frame."""
        if not self._ws:
            return False
        try:
            await self._ws.send(data)
            return True
        except Exception:
            return False

    # -- Public reconnect -----------------------------------------------------

    async def force_reconnect(self) -> None:
        """Public: tear down current connection and reconnect."""
        await self._force_reconnect()

    # -- Internal: receive loop -----------------------------------------------

    async def _recv_loop(self) -> None:
        """Read messages from the WebSocket until closed."""
        assert self._ws is not None
        try:
            async for message in self._ws:
                self._handle_raw_message(message)
        except ConnectionClosedOK:
            logger.debug("WebSocket closed normally")
            self._ws = None
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
            cm = self._ws_cm
            self._ws_cm = None
            if cm:
                try:
                    await cm.__aexit__(None, None, None)
                except Exception:
                    pass
            self._set_state(ConnectionState.DISCONNECTED)
        except ConnectionClosedError as exc:
            self._handle_close_code(exc.code, exc.reason)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Receive loop error: %s", exc)
            self._set_state(ConnectionState.ERROR)
            self._schedule_reconnect()

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

            # WSE-prefixed PONG message (JSON format from Rust server)
            # Forward to client layer — _handle_pong_event records latency once
            if '"t":"PONG"' in data or '"t": "PONG"' in data:
                self._last_pong = time.monotonic()
                if self._on_message:
                    self._on_message(data)
                return

            # WSE-prefixed PING message (JSON format)
            if '"t":"PING"' in data or '"t": "PING"' in data:
                self._fire_task(self._respond_ping_json(data))
                self._last_pong = time.monotonic()
                return

        # Forward to client
        if self._on_message:
            self._on_message(data)

    def _record_pong_latency(self, ts_str: str) -> None:
        """Extract latency from PONG timestamp and forward to callback."""
        if self._on_pong and ts_str:
            try:
                ts = float(ts_str)
                latency = time.time() * 1000 - ts
                if 0 <= latency < 60000:
                    self._on_pong(latency)
            except (ValueError, TypeError):
                pass

    async def _send_pong(self, timestamp: str) -> None:
        """Respond to server PING with PONG."""
        pong = f"PONG:{timestamp}"
        await self.send(pong)

    async def _respond_ping_json(self, data: str) -> None:
        """Respond to a JSON-format PING from server."""
        try:
            # Strip WSE prefix if present
            json_str = data[3:] if data.startswith("WSE") else data
            parsed = _json.loads(json_str)
            ts = parsed.get("p", {}).get("timestamp", int(time.time() * 1000))
        except Exception:
            ts = int(time.time() * 1000)

        pong_msg = {
            "t": "PONG",
            "p": {"timestamp": ts},
            "v": PROTOCOL_VERSION,
        }
        payload = f"WSE{_json.dumps(pong_msg, separators=(',', ':'))}"
        await self.send(payload)

    def handle_server_ready(self, connection_id: str | None = None) -> None:
        """Called by the client layer when ``server_ready`` is decoded."""
        self._server_ready = True
        if connection_id:
            self._connection_id = connection_id
        if not self._client_hello_sent:
            self._fire_task(self._send_client_hello())

    async def _send_client_hello(self) -> None:
        """Send the client_hello handshake message with retry."""
        for attempt in range(CLIENT_HELLO_MAX_RETRIES):
            msg = {
                "t": "client_hello",
                "p": {
                    "client_version": CLIENT_VERSION,
                    "protocol_version": PROTOCOL_VERSION,
                    "features": {
                        "compression": True,
                        "encryption": False,
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
                },
                "id": str(uuid4()),
                "ts": datetime.now(UTC).isoformat(),
                "v": PROTOCOL_VERSION,
                "pri": MessagePriority.CRITICAL,
            }
            payload = f"WSE{_json.dumps(msg, separators=(',', ':'))}"
            ok = await self.send(payload)
            if ok:
                self._client_hello_sent = True
                return

            logger.warning(
                "Failed to send client_hello (attempt %d/%d), retrying",
                attempt + 1,
                CLIENT_HELLO_MAX_RETRIES,
            )
            try:
                await asyncio.sleep(CLIENT_HELLO_RETRY_DELAY)
            except asyncio.CancelledError:
                return

        logger.error("client_hello failed after %d attempts", CLIENT_HELLO_MAX_RETRIES)

    # -- Internal: heartbeat --------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Send PING every HEARTBEAT_INTERVAL; detect idle timeout."""
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                return

            if not self._ws:
                return

            # Check idle timeout
            if self._last_pong is not None:
                idle = time.monotonic() - self._last_pong
                if idle > IDLE_TIMEOUT:
                    logger.warning("Idle timeout (%.0fs), reconnecting", idle)
                    await self._force_reconnect()
                    return

            # Send PING (lowercase -- Rust server fast-path checks "ping")
            ping_msg = {
                "t": "ping",
                "p": {"timestamp": int(time.time() * 1000)},
                "v": PROTOCOL_VERSION,
            }
            payload = f"WSE{_json.dumps(ping_msg, separators=(',', ':'))}"
            ok = await self.send(payload)
            if ok and self._on_ping_sent:
                self._on_ping_sent()
            elif not ok:
                logger.debug("PING send failed")

    # -- Internal: reconnection -----------------------------------------------

    def _handle_close_code(self, code: int, reason: str) -> None:
        """React to a WebSocket close code."""
        logger.debug("WebSocket closed: code=%d reason=%s", code, reason)

        self._ws = None
        self._server_ready = False
        self._client_hello_sent = False
        # Schedule ws_cm cleanup (async, but we're in sync context)
        if self._ws_cm:
            cm = self._ws_cm
            self._ws_cm = None
            self._fire_task(cm.__aexit__(None, None, None))

        # Cancel old tasks to prevent duplicate loops
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        # recv_task already exited (it raised ConnectionClosedError)
        self._recv_task = None

        # Normal close or going away
        if code in (WS_CLOSE_NORMAL, WS_CLOSE_GOING_AWAY):
            self._set_state(ConnectionState.DISCONNECTED)
            return

        # Auth failures -> no retry
        if code in (
            WS_CLOSE_AUTH_FAILED,
            WS_CLOSE_AUTH_EXPIRED,
            WS_CLOSE_POLICY_VIOLATION,
        ):
            self._set_state(ConnectionState.ERROR)
            self._token = None
            logger.error("Auth/policy failure (code %d): %s", code, reason)
            return

        # Rate limited -> no retry
        if code == WS_CLOSE_RATE_LIMITED:
            self._set_state(ConnectionState.ERROR)
            logger.error("Rate limited by server")
            return

        # Anything else -> reconnect
        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt with backoff."""
        if self._destroyed:
            return

        cfg = self._reconnect_cfg
        if cfg.max_attempts >= 0 and self._reconnect_attempts >= cfg.max_attempts:
            logger.error("Max reconnect attempts (%d) reached", cfg.max_attempts)
            self._set_state(ConnectionState.ERROR)
            return

        self._set_state(ConnectionState.RECONNECTING)
        delay = self._calculate_delay()
        logger.info(
            "Reconnecting in %.1fs (attempt %d/%s)",
            delay,
            self._reconnect_attempts + 1,
            cfg.max_attempts if cfg.max_attempts >= 0 else "inf",
        )

        self._reconnect_task = asyncio.ensure_future(self._reconnect_after(delay))

    async def _reconnect_after(self, delay: float) -> None:
        """Wait, then try to reconnect."""
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            return

        self._reconnect_attempts += 1
        try:
            await self.connect()
        except (WSEAuthError, WSECircuitBreakerError):
            # Don't retry on auth failures or circuit breaker
            self._set_state(ConnectionState.ERROR)
        except Exception as exc:
            logger.debug("Reconnect attempt failed: %s", exc)
            self._schedule_reconnect()

    async def _force_reconnect(self) -> None:
        """Tear down and reconnect immediately."""
        # Cancel tasks FIRST (before closing socket) to avoid spurious
        # exceptions in _recv_loop triggering unintended reconnect paths.
        tasks_to_await: list[asyncio.Task[None]] = []
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            tasks_to_await.append(self._heartbeat_task)
            self._heartbeat_task = None
        if self._recv_task:
            self._recv_task.cancel()
            tasks_to_await.append(self._recv_task)
            self._recv_task = None
        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        if self._ws_cm:
            try:
                await self._ws_cm.__aexit__(None, None, None)
            except Exception:
                pass
            self._ws_cm = None
            self._ws = None
        elif self._ws:
            try:
                await self._ws.close(WS_CLOSE_GOING_AWAY, "Reconnecting")
            except Exception:
                pass
            self._ws = None

        self._schedule_reconnect()

    def _cancel_reconnect(self) -> None:
        if self._reconnect_task:
            self._reconnect_task.cancel()
            self._reconnect_task = None

    def _calculate_delay(self) -> float:
        """Compute reconnect delay based on strategy."""
        cfg = self._reconnect_cfg
        attempt = self._reconnect_attempts

        if cfg.mode == ReconnectMode.LINEAR:
            delay = cfg.base_delay + attempt * 1.0
        elif cfg.mode == ReconnectMode.FIBONACCI:
            delay = cfg.base_delay * _fib(min(attempt + 1, 10))
        else:
            # Exponential / Adaptive
            delay = cfg.base_delay * (cfg.factor**attempt)

        delay = min(delay, cfg.max_delay)

        # Absolute cap (5 minutes) matching TS scheduleReconnect
        delay = min(delay, RECONNECT_ABSOLUTE_CAP)

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
        """Append query params to the base URL."""
        parts = [self._url]
        sep = "&" if "?" in self._url else "?"

        params: list[str] = [
            f"client_version={CLIENT_VERSION}",
            f"protocol_version={PROTOCOL_VERSION}",
            "compression=true",
            "encryption=false",
        ]
        if topics:
            params.append(f"topics={','.join(topics)}")

        return parts[0] + sep + "&".join(params)


def _fib(n: int) -> int:
    """Fibonacci number for reconnect delay calculation."""
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a
