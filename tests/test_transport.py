"""Transport behaviour: closes the server starts, protocol pings and close
replies, the inbound rate limit, refusals at the upgrade and option checks."""

import asyncio
import base64
import json
import os
import socket
import time

import pytest
import websockets

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_integration import connect, drain_all, drain_until, ws_url
from wse_server._wse_accel import RustWSEServer


def start_server(port: int, **options) -> RustWSEServer:
    srv = RustWSEServer(
        "127.0.0.1",
        port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        **options,
    )
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)
    return srv


@pytest.fixture()
def idle_server(server_port):
    srv = start_server(server_port, ping_interval=1, idle_timeout=2)
    yield srv
    srv.stop()


async def connect_quiet(port: int, **options):
    """A client that sends nothing, not even protocol pings."""
    options.setdefault("ping_interval", None)
    return await websockets.connect(
        ws_url(port),
        additional_headers={"Authorization": f"Bearer {make_token()}"},
        open_timeout=2.0,
        **options,
    )


async def close_code(ws, timeout: float) -> int | None:
    """Read until the server closes the connection; the code it sent."""
    async with asyncio.timeout(timeout):
        try:
            async for _ in ws:
                pass
        except websockets.ConnectionClosed:
            pass
    return ws.close_code


async def texts_until_closed(ws, timeout: float) -> list[dict]:
    got = []
    async with asyncio.timeout(timeout):
        try:
            async for msg in ws:
                if isinstance(msg, str):
                    got.append(json.loads(msg))
        except websockets.ConnectionClosed:
            pass
    return got


def metric(srv: RustWSEServer, name: str) -> float:
    for line in srv.prometheus_metrics().splitlines():
        if line.startswith(name + " "):
            return float(line.split()[1])
    raise KeyError(name)


def raw_upgrade(port: int, token: str) -> socket.socket:
    """A WebSocket client that never reads after the upgrade and never answers
    a close frame."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall(
        (
            f"GET /wse HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
            "Upgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
            f"Authorization: Bearer {token}\r\n\r\n"
        ).encode()
    )
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = sock.recv(1)
        assert chunk, "the server closed during the upgrade"
        head += chunk
    assert head.startswith(b"HTTP/1.1 101"), head
    return sock


def seconds_until_eof(sock: socket.socket, timeout: float) -> float:
    started = time.monotonic()
    sock.settimeout(timeout)
    while sock.recv(65536):
        pass
    return time.monotonic() - started


class TestServerCloses:
    @pytest.mark.asyncio
    async def test_silent_clients_are_closed_one_after_another(self, idle_server, server_port):
        # Each close used to leave the ping task stuck on its own map lock, so
        # only the first silent client was ever closed.
        for _ in range(3):
            ws = await connect_quiet(server_port)
            assert await close_code(ws, timeout=10) == 4408
        async with await connect(server_port, make_token()) as ws:
            await ws.send(json.dumps({"t": "ping", "p": {"timestamp": 1}}))
            async with asyncio.timeout(5):
                while "PONG" not in str(await ws.recv()):
                    pass

    @pytest.mark.asyncio
    async def test_protocol_pings_keep_a_client_alive(self, idle_server, server_port):
        # A plain WebSocket client keeps the link with protocol pings alone;
        # the server's pongs must reach it, or the client gives up itself.
        ws = await connect_quiet(server_port, ping_interval=0.5, ping_timeout=3)
        try:
            with pytest.raises(TimeoutError):
                await close_code(ws, timeout=4.5)
            assert ws.close_code is None
        finally:
            await ws.close()

    @pytest.mark.asyncio
    async def test_disconnect_closes_with_the_normal_code(self, server, server_port):
        ws = await connect(server_port, make_token())
        _, conn_id, _ = drain_until(server, "auth_connect")
        server.disconnect(conn_id)
        assert await close_code(ws, timeout=5) == 1000

    def test_a_client_that_ignores_the_close_is_dropped(self, server, server_port):
        sock = raw_upgrade(server_port, make_token())
        try:
            _, conn_id, _ = drain_until(server, "auth_connect")
            assert server.get_connection_count() == 1
            server.disconnect(conn_id)
            waited = seconds_until_eof(sock, timeout=15)
            assert 3 < waited < 10, waited
            deadline = time.monotonic() + 5
            while server.get_connection_count() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert server.get_connection_count() == 0
            assert drain_until(server, "disconnect")[1] == conn_id
        finally:
            sock.close()

    @pytest.mark.asyncio
    async def test_messages_after_the_close_are_not_delivered(self, server, server_port):
        ws = await connect(server_port, make_token())
        _, conn_id, _ = drain_until(server, "auth_connect")
        drain_all(server)
        server.disconnect(conn_id)
        await asyncio.sleep(0.2)
        # The client has the close frame but has not read it yet.
        for i in range(5):
            try:
                await ws.send(json.dumps({"t": "late", "p": {"i": i}}))
            except websockets.ConnectionClosed:
                break
        assert await close_code(ws, timeout=5) == 1000
        events = drain_all(server, timeout_ms=300)
        assert not [ev for ev in events if ev[0] == "msg"]


class TestCloseHandshake:
    @pytest.mark.asyncio
    async def test_the_server_answers_with_the_clients_code(self, server, server_port):
        ws = await connect(server_port, make_token())
        drain_until(server, "auth_connect")
        await ws.close(code=4000, reason="done")
        assert ws.close_code == 4000

    @pytest.mark.asyncio
    async def test_stop_closes_clients_as_going_away(self, server_port):
        srv = start_server(server_port)
        ws = await connect(server_port, make_token())
        drain_until(srv, "auth_connect")
        srv.stop()
        assert await close_code(ws, timeout=5) == 1001


class TestUpgradeRefusals:
    @pytest.mark.asyncio
    async def test_a_full_server_answers_503(self, server_port):
        srv = start_server(server_port, max_connections=1)
        try:
            async with await connect(server_port, make_token()):
                drain_until(srv, "auth_connect")
                with pytest.raises(websockets.InvalidStatus) as refused:
                    await connect(server_port, make_token())
                assert refused.value.response.status_code == 503
                assert refused.value.response.headers["Retry-After"] == "5"
            assert metric(srv, "wse_connections_rejected_total") >= 1
        finally:
            srv.stop()


class TestInboundRateLimit:
    @pytest.mark.asyncio
    async def test_a_client_over_its_budget_is_told_and_dropped(self, server_port):
        srv = start_server(server_port, rate_limit_capacity=5, rate_limit_refill=1)
        try:
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                for i in range(20):
                    await ws.send(json.dumps({"t": "chat", "p": {"i": i}}))
                kinds = set()
                async with asyncio.timeout(5):
                    while "RATE_LIMITED" not in kinds:
                        msg = json.loads(await ws.recv())
                        kinds.add(msg.get("p", {}).get("code") or msg["t"])
                assert "rate_limit_warning" in kinds
                delivered = [ev for ev in drain_all(srv, timeout_ms=300) if ev[0] == "msg"]
                assert 5 <= len(delivered) <= 7
                assert metric(srv, "wse_rate_limited_total") >= 13
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_send_event_is_not_throttled(self, server_port):
        srv = start_server(server_port, rate_limit_capacity=1, rate_limit_refill=1)
        try:
            async with await connect(server_port, make_token()) as ws:
                _, conn_id, _ = drain_until(srv, "auth_connect")
                await asyncio.wait_for(ws.recv(), timeout=2)  # server_ready
                for i in range(50):
                    assert srv.send_event(conn_id, {"t": "tick", "p": {"i": i}}) > 0
                got = [json.loads(await asyncio.wait_for(ws.recv(), timeout=2)) for _ in range(50)]
                assert [m["p"]["i"] for m in got] == list(range(50))
        finally:
            srv.stop()


class TestHello:
    @pytest.mark.asyncio
    async def test_client_hello_is_answered_once(self, server, server_port):
        async with await connect(server_port, make_token()) as ws:
            drain_until(server, "auth_connect")
            hello = json.dumps({"t": "client_hello", "p": {"protocol_version": 1}})
            for _ in range(3):
                await ws.send(hello)
            await ws.send(json.dumps({"t": "ping", "p": {"timestamp": 1}}))
            kinds = []
            async with asyncio.timeout(5):
                while "PONG" not in kinds:
                    kinds.append(json.loads(await ws.recv())["t"])
            assert kinds.count("server_hello") == 1


class TestOptionChecks:
    @pytest.mark.parametrize(
        "options",
        [
            {"ping_interval": 0},
            {"max_connections": 0},
            {"max_inbound_queue_size": 0},
            {"max_message_size": 0},
            {"rate_limit_capacity": 0.5},
            {"rate_limit_capacity": float("nan")},
            {"rate_limit_refill": 0},
            {"rate_limit_refill": float("nan")},
            {"ping_interval": 60, "idle_timeout": 60},
        ],
    )
    def test_out_of_range_options_are_refused(self, options):
        with pytest.raises(ValueError):
            RustWSEServer("127.0.0.1", 0, **options)

    def test_an_unlimited_rate_is_allowed(self):
        RustWSEServer("127.0.0.1", 0, rate_limit_capacity=float("inf"))
