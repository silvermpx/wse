"""Server lifecycle: start and stop, drain, restarts, callback mode and logs."""

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import sys
import threading
import time

import pytest
import websockets

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, _next_port, make_token
from tests.test_integration import connect, drain_until, ws_url
from tests.test_transport import raw_upgrade, seconds_until_eof
from wse_server._wse_accel import RustWSEServer


def jwt_server(port: int, **options) -> RustWSEServer:
    return RustWSEServer(
        "127.0.0.1",
        port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        **options,
    )


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


class TestStart:
    def test_a_port_in_use_is_an_error_of_start(self, server_port):
        with socket.socket() as taken:
            taken.bind(("127.0.0.1", server_port))
            taken.listen()
            srv = RustWSEServer("127.0.0.1", server_port)
            with pytest.raises(OSError, match="cannot listen"):
                srv.start()
            assert not srv.is_running()

    def test_listening_when_start_returns(self, server_port):
        srv = RustWSEServer("127.0.0.1", server_port)
        srv.start()
        try:
            socket.create_connection(("127.0.0.1", server_port), timeout=1).close()
        finally:
            srv.stop()

    def test_context_manager_starts_and_stops(self, server_port):
        with RustWSEServer("127.0.0.1", server_port) as srv:
            assert srv.is_running()
        assert not srv.is_running()


class TestDrain:
    def test_drain_waits_for_clients_that_go(self, server_port):
        srv = jwt_server(server_port)
        srv.enable_drain_mode()
        srv.start()
        try:

            async def run():
                ws = await connect(server_port, make_token())
                drain_until(srv, "auth_connect")
                remaining = await asyncio.to_thread(srv.drain, 4300, "moving", 5)
                async with asyncio.timeout(5):
                    try:
                        async for _ in ws:
                            pass
                    except websockets.ConnectionClosed:
                        pass
                return remaining, ws.close_code

            remaining, code = asyncio.run(run())
            assert remaining == 0
            assert code == 4300
            assert srv.is_draining()
        finally:
            srv.stop()

    def test_drain_drops_clients_that_stay_at_the_timeout(self, server_port):
        srv = jwt_server(server_port)
        srv.enable_drain_mode()
        srv.start()
        sock = raw_upgrade(server_port, make_token())
        try:
            drain_until(srv, "auth_connect")
            started = time.monotonic()
            assert srv.drain(timeout=1) == 1
            assert time.monotonic() - started < 3
            assert seconds_until_eof(sock, timeout=5) < 3
            assert wait_for(lambda: srv.get_connection_count() == 0)
        finally:
            sock.close()
            srv.stop()

    @pytest.mark.asyncio
    async def test_restart_after_drain_takes_clients(self, server_port):
        srv = jwt_server(server_port)
        srv.enable_drain_mode()
        srv.start()
        ws = await connect(server_port, make_token())
        drain_until(srv, "auth_connect")
        await asyncio.to_thread(srv.drain, 4300, "", 2)
        srv.stop()
        await ws.close()
        srv.start()
        try:
            assert not srv.is_draining()
            async with await connect(server_port, make_token("again")):
                assert drain_until(srv, "auth_connect")[2] == "again"
                assert len(srv.get_connections()) == 1
        finally:
            srv.stop()


class TestStop:
    def test_stop_cleans_up_clients_that_do_not_answer(self, server_port):
        srv = jwt_server(server_port, presence_enabled=True)
        srv.enable_drain_mode()
        srv.start()
        sock = raw_upgrade(server_port, make_token("ghost"))
        _, conn_id, _ = drain_until(srv, "auth_connect")
        srv.subscribe_connection(conn_id, ["room"], {"here": True})
        started = time.monotonic()
        srv.stop()
        assert time.monotonic() - started < 5
        sock.close()
        # Its cleanup ran before the runtime went: nothing left for the next run.
        assert srv.get_connection_count() == 0
        assert srv.get_connections() == []
        assert srv.presence("room") == {}
        events = srv.drain_inbound(64, 100)
        assert ("disconnect", conn_id, None) in [tuple(ev) for ev in events]

    def test_stop_from_a_callback_does_not_hang(self, server_port):
        srv = jwt_server(server_port)
        stopped = threading.Event()

        def on_message(conn_id, data):
            srv.stop()
            stopped.set()

        srv.set_callbacks(lambda *a: None, on_message, lambda *a: None)
        srv.start()

        async def run():
            async with await connect(server_port, make_token()) as ws:
                await ws.send(json.dumps({"t": "stop"}))
                await asyncio.sleep(0.5)

        with contextlib.suppress(websockets.ConnectionClosed):
            asyncio.run(run())
        assert stopped.wait(5)
        assert wait_for(lambda: not srv.is_running())
        # A new start() is possible once the old thread has ended.
        assert wait_for(lambda: _try_start(srv), timeout=10)
        srv.stop()


def _try_start(srv) -> bool:
    try:
        srv.start()
    except (RuntimeError, OSError):
        return False
    return True


class TestCallbackMode:
    def test_callbacks_of_a_connection_run_in_order(self, server_port):
        srv = jwt_server(server_port)
        seen = []

        def on_connect(conn_id, user_id):
            time.sleep(0.2)  # slow on purpose: messages must still wait for it
            seen.append(("connect", user_id))

        def on_message(conn_id, data):
            seen.append(("msg", data["p"]["i"]))

        def on_disconnect(conn_id):
            seen.append(("disconnect", None))

        srv.set_callbacks(on_connect, on_message, on_disconnect)
        srv.start()
        try:

            async def run():
                async with await connect(server_port, make_token("carol")) as ws:
                    for i in range(50):
                        await ws.send(json.dumps({"t": "chat", "p": {"i": i}}))

            asyncio.run(run())
            assert wait_for(lambda: seen and seen[-1][0] == "disconnect")
            assert seen[0] == ("connect", "carol")
            assert [i for kind, i in seen if kind == "msg"] == list(range(50))
            assert len(seen) == 52
        finally:
            srv.stop()

    def test_on_connect_gets_the_cookies_without_jwt(self, server_port):
        srv = RustWSEServer("127.0.0.1", server_port)
        got = []
        srv.set_callbacks(lambda c, cookies: got.append(cookies), lambda *a: None, lambda *a: None)
        srv.start()
        try:

            async def run():
                async with websockets.connect(
                    ws_url(server_port), additional_headers={"Cookie": "session=abc"}
                ):
                    await asyncio.sleep(0.3)

            asyncio.run(run())
            assert got == ["session=abc"]
        finally:
            srv.stop()


class TestLogs:
    def test_warnings_are_printed_by_default(self):
        port = _next_port()
        code = f"""
import asyncio, time, websockets
from wse_server._wse_accel import RustWSEServer
srv = RustWSEServer("127.0.0.1", {port}, jwt_secret=b"x" * 32)
srv.start()
async def main():
    try:
        async with websockets.connect(
            "ws://127.0.0.1:{port}/wse",
            additional_headers={{"Authorization": "Bearer not-a-token"}},
        ) as ws:
            await ws.recv()
    except Exception:
        pass
asyncio.run(main())
time.sleep(0.2)
srv.stop()
"""
        out = subprocess.run(
            [sys.executable, "-I", "-c", code],
            capture_output=True,
            text=True,
            timeout=60,
            # Without RUST_LOG; the rest stays (Windows needs SYSTEMROOT).
            env={k: v for k, v in os.environ.items() if k != "RUST_LOG"},
        )
        assert "JWT validation failed" in out.stdout + out.stderr, out.stderr


class TestApi:
    def test_sending_to_a_gone_connection_does_nothing(self, server):
        assert server.send_event("no-such-connection", {"t": "x", "p": {}}) == 0
        assert server.set_topic_acl("no-such-connection", ["a"]) is False
        assert "command_queue_depth" in server.health_snapshot()

    @pytest.mark.asyncio
    async def test_jwt_keys_can_be_replaced(self, server, server_port):
        from wse_server._wse_accel import rust_jwt_encode

        new_secret = b"another-secret-for-the-rotation!"

        def token(secret):
            now = int(time.time())
            claims = {
                "sub": "dave",
                "iss": JWT_ISSUER,
                "aud": JWT_AUDIENCE,
                "exp": now + 600,
                "iat": now,
            }
            return rust_jwt_encode(claims, secret)

        async def accepted(tok) -> bool:
            async with await connect(server_port, tok) as ws:
                first = json.loads(await asyncio.wait_for(ws.recv(), timeout=2))
                return first["t"] == "server_ready"

        assert not await accepted(token(new_secret))
        server.reload_jwt_keys(new_secret, jwt_previous_secret=JWT_SECRET)
        assert await accepted(token(new_secret))
        assert await accepted(token(JWT_SECRET))
        server.reload_jwt_keys(new_secret)
        assert not await accepted(token(JWT_SECRET))
        with pytest.raises(ValueError):
            server.reload_jwt_keys(b"short")

    def test_reloading_keys_needs_a_jwt_server(self, server_no_auth):
        with pytest.raises(RuntimeError):
            server_no_auth.reload_jwt_keys(b"x" * 32)


class TestConversion:
    @pytest.mark.asyncio
    async def test_cyclic_or_too_deep_data_raises_value_error(self, server, server_port):
        async with await connect(server_port, make_token()):
            _, conn_id, _ = drain_until(server, "auth_connect")
            cyclic: dict = {"t": "x"}
            cyclic["p"] = cyclic
            with pytest.raises(ValueError, match="Circular"):
                server.send_event(conn_id, cyclic)
            deep: list = []
            for _ in range(200):
                deep = [deep]
            with pytest.raises(ValueError, match="deeper"):
                server.send_event(conn_id, {"t": "x", "p": deep})
            # Shared, not cyclic: fine.
            shared = {"k": 1}
            assert server.send_event(conn_id, {"t": "x", "p": {"a": shared, "b": shared}}) > 0


class TestIssueToken:
    @pytest.mark.asyncio
    async def test_tokens_the_server_issues_are_accepted(self, server, server_port):
        token = server.issue_token("erin", ttl=60, claims={"role": "admin"})
        async with await connect(server_port, token):
            assert drain_until(server, "auth_connect")[2] == "erin"

    def test_asymmetric_algorithms_need_the_private_key(self, server_port):
        ec = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ec")
        serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")
        private = ec.generate_private_key(ec.SECP256R1())
        public_pem = private.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        private_pem = private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        verify_only = RustWSEServer(
            "127.0.0.1", server_port, jwt_secret=public_pem, jwt_algorithm="ES256"
        )
        with pytest.raises(RuntimeError, match="jwt_private_key"):
            verify_only.issue_token("erin")
        signing = RustWSEServer(
            "127.0.0.1",
            server_port,
            jwt_secret=public_pem,
            jwt_algorithm="ES256",
            jwt_private_key=private_pem,
            jwt_issuer="me",
        )
        from wse_server._wse_accel import rust_jwt_decode

        claims = rust_jwt_decode(
            signing.issue_token("erin", ttl=60), public_pem, algorithm="ES256", issuer="me"
        )
        assert claims["sub"] == "erin" and claims["iss"] == "me"
