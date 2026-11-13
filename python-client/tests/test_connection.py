# =============================================================================
# WSE Python Client -- Connection regression tests
# =============================================================================

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from wse_client.connection import ConnectionManager
from wse_client.types import ConnectionState, ReconnectConfig, ReconnectMode


@pytest.mark.asyncio
async def test_force_reconnect_from_heartbeat_does_not_zombie() -> None:
    """_force_reconnect is invoked from inside _heartbeat_loop on idle timeout.

    Regression: it used to cancel-and-await self._heartbeat_task -- i.e. the
    CURRENT task -- so a CancelledError fired inside _force_reconnect before
    _schedule_reconnect() ran, leaving the socket open and no reconnect
    scheduled (a permanent zombie). The current task must be skipped so the
    reconnect is always scheduled.
    """
    conn = ConnectionManager("ws://localhost:5006/wse", token="t")
    scheduled: list[bool] = []
    # Stub out the actual reconnect scheduling; we only assert it is reached.
    conn._schedule_reconnect = lambda: scheduled.append(True)  # type: ignore[method-assign]
    conn._ws = None
    conn._ws_cm = None

    async def fake_heartbeat() -> None:
        # Mirrors the real path: the heartbeat task itself calls _force_reconnect.
        await conn._force_reconnect()

    task = asyncio.ensure_future(fake_heartbeat())
    # The task IS the heartbeat task -- the exact self-cancel scenario.
    conn._heartbeat_task = task

    # Must complete promptly (no self-cancel hang) and schedule the reconnect.
    await asyncio.wait_for(task, timeout=1.0)
    assert scheduled == [True], "reconnect must be scheduled (no zombie)"
    assert conn._heartbeat_task is None


class FakeWS:
    """Minimal stand-in for a websockets ClientConnection."""

    def __init__(self, frames: list[Any] | None = None, close_code: int = 1000):
        self.frames = list(frames or [])
        self.sent: list[Any] = []
        self.close_code = close_code
        self.close_reason = ""

    def __aiter__(self):
        return self._iter()

    async def _iter(self):
        for frame in self.frames:
            yield frame
        # websockets ends `async for` quietly on 1000 / 1001 / 1005

    async def send(self, data: Any) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass


@pytest.mark.parametrize("code", [1000, 1001, 1005])
async def test_quiet_close_is_handled(code: int) -> None:
    """Websockets swallows 1000/1001/1005 inside `async for`; the receive
    loop just returned and the client kept reporting CONNECTED."""
    conn = ConnectionManager("ws://localhost:5006/wse")
    seen: list[tuple[int, str]] = []
    conn._handle_close_code = lambda c, r: seen.append((c, r))  # type: ignore[method-assign]
    ws = FakeWS(close_code=code)
    conn._ws = ws
    conn._state = ConnectionState.CONNECTED
    await conn._recv_loop(ws)
    assert seen == [(code, "")]
    assert conn._ws is None
    assert not conn.is_connected


async def test_server_close_schedules_reconnect_once_armed() -> None:
    conn = ConnectionManager(
        "ws://localhost:5006/wse", reconnect=ReconnectConfig(base_delay=60)
    )
    conn._reconnect_armed = True
    conn._handle_close_code(1001, "")
    assert conn.state == ConnectionState.RECONNECTING
    assert conn._reconnect_task is not None
    conn._cancel_reconnect()


async def test_close_before_handshake_fails_instead_of_reconnecting() -> None:
    conn = ConnectionManager("ws://localhost:5006/wse")
    errors: list[Exception] = []
    conn._on_terminal = errors.append
    conn._handle_close_code(1011, "boom")
    assert conn.state == ConnectionState.ERROR
    assert conn._reconnect_task is None
    assert len(errors) == 1


async def test_bad_frame_does_not_end_the_receive_loop() -> None:
    """An exception from frame handling tore the connection down."""
    handled: list[Any] = []

    def on_message(data: Any) -> None:
        if data == "bad":
            raise ValueError("malformed")
        handled.append(data)

    conn = ConnectionManager("ws://localhost:5006/wse", on_message=on_message)
    conn._handle_close_code = lambda c, r: None  # type: ignore[method-assign]
    ws = FakeWS(frames=["bad", '{"t":"ok"}'])
    conn._ws = ws
    await conn._recv_loop(ws)
    assert handled == ['{"t":"ok"}']


class TestBackoff:
    def test_exponential_never_overflows(self) -> None:
        """1.5 ** attempt overflowed after ~15 h of outage and killed
        the reconnect loop for good."""
        conn = ConnectionManager(
            "ws://x/wse", reconnect=ReconnectConfig(max_delay=30.0, jitter=False)
        )
        for attempt in (0, 10, 1751, 5000, 10**6):
            conn._reconnect_attempts = attempt
            delay = conn._calculate_delay()
            assert 0.0 <= delay <= 30.0

    def test_huge_factor(self) -> None:
        conn = ConnectionManager(
            "ws://x/wse",
            reconnect=ReconnectConfig(factor=1e300, max_delay=5.0, jitter=True),
        )
        conn._reconnect_attempts = 50
        assert 0.0 <= conn._calculate_delay() <= 6.0

    @pytest.mark.parametrize("mode", list(ReconnectMode))
    def test_all_modes_capped(self, mode: ReconnectMode) -> None:
        conn = ConnectionManager(
            "ws://x/wse",
            reconnect=ReconnectConfig(mode=mode, max_delay=7.0, jitter=False),
        )
        conn._reconnect_attempts = 100_000
        assert conn._calculate_delay() <= 7.0


class TestSendPaths:
    async def test_control_frames_bypass_limiter_and_encryption(self) -> None:
        """Ping / PONG went through encryption and the rate limiter."""
        pytest.importorskip("cryptography")
        from wse_client.rate_limiter import TokenBucketRateLimiter
        from wse_client.security import SecurityManager

        limiter = TokenBucketRateLimiter(capacity=1, refill_rate=1)
        limiter.try_consume()  # empty
        security = SecurityManager(legacy=True)
        peer = SecurityManager(legacy=True)
        security.generate_keypair()
        security.derive_shared_secret(peer.generate_keypair())

        conn = ConnectionManager("ws://x/wse", rate_limiter=limiter, security=security)
        ws = FakeWS()
        conn._ws = ws
        conn._state = ConnectionState.CONNECTED
        await conn._send_ping()
        await conn._send_pong("123")
        await conn._respond_ping_json('{"c":"WSE","t":"ping","p":{"server_time":"t"}}')
        assert len(ws.sent) == 3
        assert all(isinstance(frame, str) for frame in ws.sent)
        assert json.loads(ws.sent[0])["t"] == "ping"
        assert ws.sent[1] == "PONG:123"
        assert json.loads(ws.sent[2])["t"] == "PONG"

    async def test_session_control_frames_wait_for_the_handshake(self) -> None:
        """Once the server answered client_hello it takes sealed frames only:
        a heartbeat or PONG while the handshake is in flight is skipped, not
        sent in plaintext."""
        pytest.importorskip("cryptography")
        from wse_client.security import SecurityManager

        security = SecurityManager(unauthenticated=True)
        security.hello_fields()
        conn = ConnectionManager("ws://x/wse", security=security)
        ws = FakeWS()
        conn._ws = ws
        conn._state = ConnectionState.CONNECTED
        await conn._send_ping()
        await conn._respond_ping_json('{"c":"WSE","t":"ping","p":{"server_time":"t"}}')
        assert ws.sent == []

    async def test_app_frame_is_never_sent_in_plaintext(self) -> None:
        """Before the key exchange the transport fails closed."""
        pytest.importorskip("cryptography")
        from wse_client.security import SecurityManager

        conn = ConnectionManager("ws://x/wse", security=SecurityManager(legacy=True))
        ws = FakeWS()
        conn._ws = ws
        conn._state = ConnectionState.CONNECTED
        assert await conn.send('{"t":"order"}') is False
        assert await conn.send_bytes(b'{"t":"order"}') is False
        assert ws.sent == []


class TestUrl:
    def test_url_carries_package_version_and_no_topics(self) -> None:
        from wse_client import __version__

        conn = ConnectionManager("ws://x/wse")
        url = conn._build_url(["a", "b"])
        assert f"client_version={__version__}" in url
        assert "topics=" not in url  # the server reads no topics from the URL

    def test_msgpack_format_is_requested(self) -> None:
        """Format=msgpack was never sent, so the extra did nothing."""
        conn = ConnectionManager("ws://x/wse?a=1", query_params={"format": "msgpack"})
        url = conn._build_url()
        assert url.startswith("ws://x/wse?a=1&")
        assert "format=msgpack" in url

    def test_client_requests_msgpack(self) -> None:
        pytest.importorskip("msgpack")
        from wse_client import AsyncWSEClient

        client = AsyncWSEClient("ws://x/wse", wire_format="msgpack")
        assert "format=msgpack" in client._connection._build_url()
        assert "format=" not in AsyncWSEClient("ws://x/wse")._connection._build_url()

    def test_msgpack_without_the_extra_fails_early(self, monkeypatch) -> None:
        import wse_client.msgpack_handler as mp
        from wse_client import AsyncWSEClient

        monkeypatch.setattr(mp, "_HAS_MSGPACK", False)
        with pytest.raises(ImportError, match="wse-client\\[msgpack\\]"):
            AsyncWSEClient("ws://x/wse", wire_format="msgpack")
        with pytest.raises(ValueError):
            AsyncWSEClient("ws://x/wse", wire_format="xml")
