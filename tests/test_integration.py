"""Integration tests: Python wrapper + Rust WSE server.

Tests the full lifecycle: start server, connect via websocket,
send/receive messages, verify drain_inbound, subscriptions, etc.
"""

import asyncio
import json
import time

import pytest
import websockets

from tests.conftest import make_token
from wse_server._wse_accel import RustWSEServer

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ws_url(port: int) -> str:
    return f"ws://127.0.0.1:{port}/wse"


async def connect(port: int, token: str | None = None, timeout: float = 2.0):
    """Connect to WSE server with optional JWT token."""
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return await websockets.connect(
        ws_url(port),
        additional_headers=headers,
        open_timeout=timeout,
    )


_pending_events: list[tuple] = []


def drain_until(server, event_type: str, timeout: float = 2.0) -> tuple:
    """Drain events until we find one matching event_type.

    Preserves unmatched events in a pending buffer so back-to-back
    calls for the same event_type don't lose events that arrived
    in the same drain_inbound batch.
    """
    # Check pending buffer first
    for i, ev in enumerate(_pending_events):
        if ev[0] == event_type:
            return _pending_events.pop(i)

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        events = server.drain_inbound(64, 50)
        found = None
        for ev in events:
            if found is None and ev[0] == event_type:
                found = ev
            else:
                _pending_events.append(ev)
        if found is not None:
            return found
    raise TimeoutError(f"No '{event_type}' event within {timeout}s")


def drain_all(server, timeout_ms: int = 100) -> list[tuple]:
    """Drain all pending events."""
    return list(server.drain_inbound(256, timeout_ms))


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


class TestConnectionLifecycle:
    @pytest.mark.asyncio
    async def test_connect_with_jwt(self, server, server_port):
        """JWT-authenticated connection emits auth_connect with user_id."""
        token = make_token("alice")
        async with await connect(server_port, token) as ws:
            ev = drain_until(server, "auth_connect")
            assert ev[0] == "auth_connect"
            assert ev[2] == "alice"  # user_id

            # Should also receive server_ready from Rust
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert "server_ready" in str(msg)

    @pytest.mark.asyncio
    async def test_connect_without_jwt_rejected(self, server, server_port):
        """Connection without JWT to a JWT-required server sends AUTH_REQUIRED error."""
        async with await connect(server_port, None) as ws:
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert "AUTH_REQUIRED" in str(msg)

    @pytest.mark.asyncio
    async def test_connect_no_auth_server(self, server_no_auth, server_port):
        """Server without JWT accepts connections, emits 'connect'."""
        async with await connect(server_port, None):
            ev = drain_until(server_no_auth, "connect")
            assert ev[0] == "connect"
            assert isinstance(ev[1], str)  # conn_id
            assert isinstance(ev[2], str)  # cookies string

    @pytest.mark.asyncio
    async def test_disconnect_event(self, server, server_port):
        """Closing connection emits disconnect event."""
        token = make_token("bob")
        ws = await connect(server_port, token)
        drain_until(server, "auth_connect")
        conn_id = server.get_connections()[0]

        await ws.close()
        ev = drain_until(server, "disconnect")
        assert ev[0] == "disconnect"
        assert ev[1] == conn_id

    @pytest.mark.asyncio
    async def test_connection_count(self, server, server_port):
        """get_connection_count tracks active connections."""
        assert server.get_connection_count() == 0

        token = make_token("u1")
        ws1 = await connect(server_port, token)
        drain_until(server, "auth_connect")
        assert server.get_connection_count() == 1

        ws2 = await connect(server_port, make_token("u2"))
        drain_until(server, "auth_connect")
        assert server.get_connection_count() == 2

        await ws1.close()
        drain_until(server, "disconnect")
        assert server.get_connection_count() == 1

        await ws2.close()
        drain_until(server, "disconnect")
        assert server.get_connection_count() == 0


# ---------------------------------------------------------------------------
# Message send / receive
# ---------------------------------------------------------------------------


class TestMessaging:
    @pytest.mark.asyncio
    async def test_send_text(self, server, server_port):
        """server.send() delivers raw text to client."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            # consume server_ready
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")
            conn_id = server.get_connections()[0]

            server.send(conn_id, '{"c":"U","t":"hello","p":{"x":1},"v":1}')
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert "hello" in str(msg)

    @pytest.mark.asyncio
    async def test_send_event_dict(self, server, server_port):
        """server.send_event() serializes dict and delivers to client."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")
            conn_id = server.get_connections()[0]

            sent = server.send_event(conn_id, {"t": "test_event", "p": {"value": 42}})
            assert sent > 0  # byte count

            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            data = str(msg)
            assert "test_event" in data

    @pytest.mark.asyncio
    async def test_client_to_server_json(self, server, server_port):
        """Client sends JSON, server receives via drain_inbound as dict."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")

            await ws.send('{"c":"WSE","t":"chat","p":{"text":"hello"}}')
            ev = drain_until(server, "msg")
            assert ev[0] == "msg"
            assert isinstance(ev[2], dict)
            assert ev[2]["t"] == "chat"
            assert ev[2]["p"]["text"] == "hello"

    @pytest.mark.asyncio
    async def test_client_to_server_raw_text(self, server, server_port):
        """Non-JSON text goes through as raw event."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")

            await ws.send("not json at all")
            ev = drain_until(server, "raw")
            assert ev[0] == "raw"
            assert ev[2] == "not json at all"

    @pytest.mark.asyncio
    async def test_ping_pong(self, server, server_port):
        """Client sends ping, gets pong back from Rust."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")

            ts = int(time.time() * 1000)
            await ws.send(f'{{"c":"WSE","t":"ping","p":{{"timestamp":{ts}}}}}')
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert "PONG" in str(msg)
            assert str(ts) in str(msg)


# ---------------------------------------------------------------------------
# Broadcast
# ---------------------------------------------------------------------------


class TestBroadcast:
    @pytest.mark.asyncio
    async def test_broadcast_all(self, server, server_port):
        """broadcast_all sends to every connected client."""
        t1 = make_token("u1")
        t2 = make_token("u2")
        async with await connect(server_port, t1) as ws1, await connect(server_port, t2) as ws2:
            # consume server_ready
            await asyncio.wait_for(ws1.recv(), timeout=2.0)
            await asyncio.wait_for(ws2.recv(), timeout=2.0)
            drain_until(server, "auth_connect")
            drain_until(server, "auth_connect")

            server.broadcast_all('{"t":"global","p":{},"v":1}')

            m1 = await asyncio.wait_for(ws1.recv(), timeout=2.0)
            m2 = await asyncio.wait_for(ws2.recv(), timeout=2.0)
            assert "global" in str(m1)
            assert "global" in str(m2)

    @pytest.mark.asyncio
    async def test_broadcast_local_topic(self, server, server_port):
        """broadcast_local sends only to topic subscribers."""
        t1 = make_token("sub")
        t2 = make_token("nosub")
        async with await connect(server_port, t1) as ws1, await connect(server_port, t2) as ws2:
            await asyncio.wait_for(ws1.recv(), timeout=2.0)
            await asyncio.wait_for(ws2.recv(), timeout=2.0)
            ev1 = drain_until(server, "auth_connect")
            ev2 = drain_until(server, "auth_connect")

            # Correlate conn_id to user_id from auth_connect events
            sub_conn_id = ev1[1] if ev1[2] == "sub" else ev2[1]

            server.subscribe_connection(sub_conn_id, ["prices"])

            server.broadcast_local("prices", '{"t":"price","p":{"symbol":"AAPL"},"v":1}')

            # Subscriber gets it
            m1 = await asyncio.wait_for(ws1.recv(), timeout=2.0)
            assert "price" in str(m1)

            # Non-subscriber does NOT get it (timeout expected)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws2.recv(), timeout=0.3)


# ---------------------------------------------------------------------------
# Broadcast delivery under backlog
# ---------------------------------------------------------------------------


def bulk_fill(i: int) -> str:
    """Runs of small frames (copied per connection) between large ones (shared),
    with odd, growing sizes so frame boundaries never line up with what one
    write takes."""
    size = 100 + i if i % 4 else 65_537 + i
    return chr(97 + i % 26) * size


def bulk_message(i: int) -> str:
    return json.dumps({"t": "bulk", "p": {"i": i, "fill": bulk_fill(i)}})


def assert_bulk(messages: list, first: int, count: int) -> None:
    """Every bulk message from `first` on, each once, in order, intact."""
    got = [json.loads(m) for m in messages]
    assert [g["p"]["i"] for g in got] == list(range(first, first + count))
    for g in got:
        assert g["p"]["fill"] == bulk_fill(g["p"]["i"])


async def connect_slow_reader(port: int, token: str):
    """A client with a tiny receive buffer: the server's socket fills up, so
    its writes of a backlog come back partial."""
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    sock.connect(("127.0.0.1", port))
    sock.setblocking(False)
    return await websockets.connect(
        ws_url(port),
        sock=sock,
        additional_headers={"Authorization": f"Bearer {token}"},
        open_timeout=2.0,
        max_size=None,
    )


async def recv_n(ws, n: int, timeout: float = 20.0) -> list:
    return [await asyncio.wait_for(ws.recv(), timeout=timeout) for _ in range(n)]


def make_bulk_server(port: int) -> RustWSEServer:
    from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET

    srv = RustWSEServer(
        "127.0.0.1",
        port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        # Room for the whole backlog: a slow-consumer drop would fail the test
        # for the wrong reason.
        max_outbound_queue_bytes=64 * 1024 * 1024,
    )
    srv.enable_drain_mode()
    srv.start()
    time.sleep(0.05)
    return srv


class TestBroadcastDelivery:
    """A backlog of large broadcasts reaches every client complete and in
    order, whichever way it is sent, while the socket only takes part of it
    per write."""

    COUNT = 400  # ~6.5 MB per client

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["broadcast_all", "broadcast_local"])
    async def test_backlog_complete_and_in_order(self, server_port, method):
        srv = make_bulk_server(server_port)
        try:
            clients = [
                await connect_slow_reader(server_port, make_token(f"u{i}")) for i in range(3)
            ]
            for ws in clients:
                await asyncio.wait_for(ws.recv(), timeout=2.0)  # server_ready
            for _ in clients:
                conn_id = drain_until(srv, "auth_connect")[1]
                if method == "broadcast_local":
                    srv.subscribe_connection(conn_id, ["bulk"])

            for i in range(self.COUNT):
                if method == "broadcast_all":
                    srv.broadcast_all(bulk_message(i))
                else:
                    srv.broadcast_local("bulk", bulk_message(i))

            received = await asyncio.gather(*(recv_n(ws, self.COUNT) for ws in clients))
            for messages in received:
                assert_bulk(messages, 0, self.COUNT)
            for ws in clients:
                await ws.close()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_send_event_during_backlog(self, server_port):
        """A direct message (its own queue) still arrives while broadcasts
        are backed up, and does not disturb their order."""
        srv = make_bulk_server(server_port)
        try:
            ws = await connect_slow_reader(server_port, make_token())
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            conn_id = drain_until(srv, "auth_connect")[1]

            for i in range(self.COUNT):
                srv.broadcast_all(bulk_message(i))
                if i == self.COUNT // 2:
                    srv.send_event(conn_id, {"t": "direct", "p": {"n": 1}})

            messages = await recv_n(ws, self.COUNT + 1)
            direct = [m for m in messages if isinstance(m, bytes)]
            assert len(direct) == 1 and b'"direct"' in direct[0]
            assert_bulk([m for m in messages if isinstance(m, str)], 0, self.COUNT)
            await ws.close()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_mixed_encrypted_and_plain_subscribers(self, server_port):
        """With one E2E-encrypted connection on the server, broadcasts take the
        mixed path: the encrypted client gets its own E: frames, the plain one
        the shared frames; both complete and in order."""
        pytest.importorskip("cryptography")
        import base64

        from cryptography.hazmat.primitives.asymmetric.ec import (
            ECDH,
            SECP256R1,
            EllipticCurvePublicKey,
            generate_private_key,
        )
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        from cryptography.hazmat.primitives.hashes import SHA256
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF
        from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

        srv = make_bulk_server(server_port)
        try:
            enc_ws = await connect(server_port, make_token("enc"))
            await asyncio.wait_for(enc_ws.recv(), timeout=2.0)
            enc_conn = drain_until(srv, "auth_connect")[1]
            key = generate_private_key(SECP256R1())
            public = key.public_key().public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
            await enc_ws.send(
                json.dumps(
                    {
                        "c": "WSE",
                        "t": "client_hello",
                        "p": {
                            "protocol_version": 2,
                            "features": {"encryption": True},
                            "encryption_public_key": base64.b64encode(public).decode(),
                        },
                    }
                )
            )
            hello = json.loads(await asyncio.wait_for(enc_ws.recv(), timeout=2.0))
            assert hello["t"] == "server_hello"
            server_public = base64.b64decode(hello["p"]["encryption_public_key"])
            shared = key.exchange(
                ECDH(), EllipticCurvePublicKey.from_encoded_point(SECP256R1(), server_public)
            )
            aes = AESGCM(
                HKDF(SHA256(), 32, salt=None, info=b"wse-encryption/aes-gcm-key").derive(shared)
            )

            plain_ws = await connect_slow_reader(server_port, make_token("plain"))
            await asyncio.wait_for(plain_ws.recv(), timeout=2.0)
            plain_conn = drain_until(srv, "auth_connect")[1]
            srv.subscribe_connection(enc_conn, ["bulk"])
            srv.subscribe_connection(plain_conn, ["bulk"])

            for i in range(self.COUNT):
                srv.broadcast_local("bulk", bulk_message(i))

            plain, encrypted = await asyncio.gather(
                recv_n(plain_ws, self.COUNT), recv_n(enc_ws, self.COUNT)
            )
            assert_bulk(plain, 0, self.COUNT)
            assert all(isinstance(m, bytes) and m.startswith(b"E:") for m in encrypted)
            decrypted = [aes.decrypt(m[2:14], m[14:], None).decode() for m in encrypted]
            assert_bulk(decrypted, 0, self.COUNT)
            await enc_ws.close()
            await plain_ws.close()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_recovery_live_and_replayed_in_order(self, server_with_recovery, server_port):
        """A live subscriber gets the stamped broadcasts in order; a client that
        recovers from the first one gets the rest replayed in order."""
        srv = server_with_recovery
        count = 40  # within the fixture's recovery buffer (64)

        live = await connect_slow_reader(server_port, make_token("live"))
        await asyncio.wait_for(live.recv(), timeout=2.0)
        live_conn = drain_until(srv, "auth_connect")[1]
        srv.subscribe_with_recovery(live_conn, ["bulk"], recover=False)

        srv.broadcast_local("bulk", bulk_message(0))
        first = json.loads(await asyncio.wait_for(live.recv(), timeout=5.0))
        epoch, offset = first["e"], first["o"]
        for i in range(1, count):
            srv.broadcast_local("bulk", bulk_message(i))
        rest = await recv_n(live, count - 1)
        assert [json.loads(m)["o"] for m in rest] == list(range(offset + 1, offset + count))
        assert_bulk(rest, 1, count - 1)

        async with await connect(server_port, make_token("late")) as late:
            await asyncio.wait_for(late.recv(), timeout=2.0)
            late_conn = drain_until(srv, "auth_connect")[1]
            result = srv.subscribe_with_recovery(
                late_conn, ["bulk"], recover=True, epoch=epoch, offset=offset
            )
            assert result["topics"]["bulk"]["recovered"] is True
            assert result["topics"]["bulk"]["count"] == count - 1
            assert_bulk(await recv_n(late, count - 1), 1, count - 1)
        await live.close()


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


class TestSubscriptions:
    @pytest.mark.asyncio
    async def test_subscribe_unsubscribe(self, server, server_port):
        """Subscribe adds to topic, unsubscribe removes."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")
            conn_id = server.get_connections()[0]

            server.subscribe_connection(conn_id, ["news", "sports"])
            assert server.get_topic_subscriber_count("news") == 1
            assert server.get_topic_subscriber_count("sports") == 1

            server.unsubscribe_connection(conn_id, ["news"])
            assert server.get_topic_subscriber_count("news") == 0
            assert server.get_topic_subscriber_count("sports") == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_all(self, server, server_port):
        """Unsubscribe with None removes from all topics."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")
            conn_id = server.get_connections()[0]

            server.subscribe_connection(conn_id, ["a", "b", "c"])
            server.unsubscribe_connection(conn_id, None)
            assert server.get_topic_subscriber_count("a") == 0
            assert server.get_topic_subscriber_count("b") == 0
            assert server.get_topic_subscriber_count("c") == 0

    @pytest.mark.asyncio
    async def test_disconnect_cleans_subscriptions(self, server, server_port):
        """Disconnecting a client removes it from all subscribed topics."""
        token = make_token()
        ws = await connect(server_port, token)
        await asyncio.wait_for(ws.recv(), timeout=2.0)
        drain_until(server, "auth_connect")
        conn_id = server.get_connections()[0]

        server.subscribe_connection(conn_id, ["topic1"])
        assert server.get_topic_subscriber_count("topic1") == 1

        await ws.close()
        drain_until(server, "disconnect")
        # Give cleanup a moment
        time.sleep(0.05)
        assert server.get_topic_subscriber_count("topic1") == 0


# ---------------------------------------------------------------------------
# Client hello / Server hello
# ---------------------------------------------------------------------------


class TestProtocolNegotiation:
    @pytest.mark.asyncio
    async def test_client_hello(self, server, server_port):
        """Client sends client_hello, gets server_hello back."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            # Consume server_ready
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")

            hello = json.dumps(
                {
                    "t": "client_hello",
                    "p": {"client_version": "2.0.0", "protocol_version": 2},
                }
            )
            await ws.send(json.dumps({"c": "WSE", **json.loads(hello)}))

            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
            text = str(msg)
            assert "server_hello" in text
            assert "features" in text
            assert "connection_id" in text


# ---------------------------------------------------------------------------
# Health / diagnostics
# ---------------------------------------------------------------------------


class TestHealth:
    @pytest.mark.asyncio
    async def test_health_snapshot(self, server, server_port):
        """health_snapshot returns valid dict with expected keys."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server, "auth_connect")

            h = server.health_snapshot()
            assert isinstance(h, dict)
            assert h["connections"] == 1
            assert "inbound_queue_depth" in h
            assert "uptime_secs" in h
            assert "recovery_enabled" in h

    def test_health_empty(self, server):
        """Health works with no connections."""
        h = server.health_snapshot()
        assert h["connections"] == 0


# ---------------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------------


class TestRecovery:
    @pytest.mark.asyncio
    async def test_subscribe_with_recovery(self, server_with_recovery, server_port):
        """subscribe_with_recovery returns recovery metadata."""
        token = make_token()
        async with await connect(server_port, token) as ws:
            await asyncio.wait_for(ws.recv(), timeout=2.0)
            drain_until(server_with_recovery, "auth_connect")
            conn_id = server_with_recovery.get_connections()[0]

            result = server_with_recovery.subscribe_with_recovery(
                conn_id,
                ["prices"],
                recover=False,
            )
            assert isinstance(result, dict)
            assert "topics" in result

    @pytest.mark.asyncio
    async def test_recovery_health(self, server_with_recovery, server_port):
        """Recovery-enabled server shows recovery in health."""
        h = server_with_recovery.health_snapshot()
        assert h["recovery_enabled"] is True


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------


class TestServerLifecycle:
    def test_start_stop(self, server_port):
        """Server starts and stops cleanly."""
        srv = RustWSEServer("127.0.0.1", server_port, max_connections=10)
        srv.enable_drain_mode()
        assert not srv.is_running()
        srv.start()
        time.sleep(0.05)  # let tokio runtime spin up
        assert srv.is_running()
        srv.stop()
        time.sleep(0.05)
        assert not srv.is_running()

    def test_double_start_raises(self, server):
        """Starting an already-running server raises."""
        with pytest.raises(Exception, match="already running"):
            server.start()

    def test_disconnect_unknown_conn(self, server):
        """Disconnecting a non-existent connection doesn't crash."""
        server.disconnect("nonexistent-conn-id")  # should not raise

    def test_send_to_unknown_conn(self, server):
        """Sending to non-existent connection doesn't crash."""
        server.send("nonexistent", "hello")  # should not raise

    def test_broadcast_no_connections(self, server):
        """Broadcasting with zero connections doesn't crash."""
        server.broadcast_all("{}")
        server.broadcast_local("topic", "{}")


# ---------------------------------------------------------------------------
# Presence tracking
# ---------------------------------------------------------------------------


class TestPresence:
    @pytest.mark.asyncio
    async def test_presence_join_on_subscribe(self, server_with_presence, server_port):
        """Subscribing with presence_data emits presence_join event."""
        token = make_token("alice")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            conn_id = ev[1]

            server_with_presence.subscribe_connection(
                conn_id,
                ["chat-1"],
                {"status": "online"},
            )

            ev = drain_until(server_with_presence, "presence_join", timeout=1.0)
            assert ev[0] == "presence_join"
            assert ev[1] is None  # user-level event, no conn_id
            payload = ev[2]
            assert payload["topic"] == "chat-1"
            assert payload["user_id"] == "alice"
            assert payload["data"]["status"] == "online"

    @pytest.mark.asyncio
    async def test_presence_leave_on_disconnect(self, server_with_presence, server_port):
        """Disconnecting removes presence and emits presence_leave."""
        token = make_token("bob")
        ws = await connect(server_port, token)
        ev = drain_until(server_with_presence, "auth_connect")
        conn_id = ev[1]

        server_with_presence.subscribe_connection(
            conn_id,
            ["chat-1"],
            {"status": "online"},
        )
        drain_until(server_with_presence, "presence_join", timeout=1.0)

        await ws.close()
        ev = drain_until(server_with_presence, "presence_leave", timeout=2.0)
        assert ev[2]["topic"] == "chat-1"
        assert ev[2]["user_id"] == "bob"

    @pytest.mark.asyncio
    async def test_presence_multi_connection_single_user(self, server_with_presence, server_port):
        """Two connections from same user: one join, one leave (on last disconnect)."""
        token = make_token("carol")

        ws1 = await connect(server_port, token)
        ev1 = drain_until(server_with_presence, "auth_connect")
        conn_id1 = ev1[1]

        ws2 = await connect(server_port, token)
        ev2 = drain_until(server_with_presence, "auth_connect")
        conn_id2 = ev2[1]

        # First subscribe: join event
        server_with_presence.subscribe_connection(
            conn_id1,
            ["chat-1"],
            {"status": "online"},
        )
        ev = drain_until(server_with_presence, "presence_join", timeout=1.0)
        assert ev[2]["user_id"] == "carol"

        # Second subscribe: NO join event (user already present)
        server_with_presence.subscribe_connection(
            conn_id2,
            ["chat-1"],
            {"status": "online"},
        )
        # Small drain to check no presence_join
        time.sleep(0.1)
        events = drain_all(server_with_presence, timeout_ms=100)
        assert not any(e[0] == "presence_join" for e in events)

        # Close first connection: NO leave event (user still has second conn)
        await ws1.close()
        drain_until(server_with_presence, "disconnect", timeout=1.0)
        time.sleep(0.1)
        events = drain_all(server_with_presence, timeout_ms=100)
        assert not any(e[0] == "presence_leave" for e in events)

        # Close second connection: NOW leave fires
        await ws2.close()
        ev = drain_until(server_with_presence, "presence_leave", timeout=2.0)
        assert ev[2]["user_id"] == "carol"

    @pytest.mark.asyncio
    async def test_no_presence_without_data(self, server_with_presence, server_port):
        """subscribe_connection without presence_data does NOT track presence."""
        token = make_token("ghost")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            # Subscribe without presence_data
            server_with_presence.subscribe_connection(ev[1], ["room"])
            time.sleep(0.1)
            events = drain_all(server_with_presence, timeout_ms=100)
            assert not any(e[0] == "presence_join" for e in events)

    @pytest.mark.asyncio
    async def test_presence_leave_on_unsubscribe(self, server_with_presence, server_port):
        """Unsubscribing from topic removes presence for that topic."""
        token = make_token("helen")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            conn_id = ev[1]

            server_with_presence.subscribe_connection(
                conn_id,
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            server_with_presence.unsubscribe_connection(conn_id, ["room"])
            ev = drain_until(server_with_presence, "presence_leave", timeout=1.0)
            assert ev[2]["user_id"] == "helen"

    @pytest.mark.asyncio
    async def test_presence_query(self, server_with_presence, server_port):
        """presence() returns current members."""
        token1 = make_token("dave")
        token2 = make_token("eve")

        async with await connect(server_port, token1):
            ev = drain_until(server_with_presence, "auth_connect")
            server_with_presence.subscribe_connection(
                ev[1],
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            async with await connect(server_port, token2):
                ev = drain_until(server_with_presence, "auth_connect")
                server_with_presence.subscribe_connection(
                    ev[1],
                    ["room"],
                    {"status": "away"},
                )
                drain_until(server_with_presence, "presence_join", timeout=1.0)

                members = server_with_presence.presence("room")
                assert "dave" in members
                assert "eve" in members
                assert members["dave"]["data"]["status"] == "online"
                assert members["eve"]["data"]["status"] == "away"
                assert members["dave"]["connections"] == 1
                assert members["eve"]["connections"] == 1

    @pytest.mark.asyncio
    async def test_presence_stats(self, server_with_presence, server_port):
        """presence_stats() returns lightweight counters."""
        token = make_token("frank")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            server_with_presence.subscribe_connection(
                ev[1],
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            stats = server_with_presence.presence_stats("room")
            assert stats["num_users"] == 1
            assert stats["num_connections"] == 1

    @pytest.mark.asyncio
    async def test_update_presence(self, server_with_presence, server_port):
        """update_presence changes user data."""
        token = make_token("ivan")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            conn_id = ev[1]

            server_with_presence.subscribe_connection(
                conn_id,
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            server_with_presence.update_presence(conn_id, {"status": "away", "typing": True})

            members = server_with_presence.presence("room")
            assert members["ivan"]["data"]["status"] == "away"
            assert members["ivan"]["data"]["typing"] is True

    @pytest.mark.asyncio
    async def test_presence_health_snapshot(self, server_with_presence, server_port):
        """health_snapshot includes presence metrics."""
        token = make_token("jane")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            server_with_presence.subscribe_connection(
                ev[1],
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            health = server_with_presence.health_snapshot()
            assert health["presence_enabled"] is True
            assert health["presence_topics"] == 1
            assert health["presence_total_users"] == 1

    @pytest.mark.asyncio
    async def test_presence_empty_after_all_leave(self, server_with_presence, server_port):
        """After all users leave, presence and stats are empty."""
        token = make_token("kim")
        ws = await connect(server_port, token)
        ev = drain_until(server_with_presence, "auth_connect")
        conn_id = ev[1]

        server_with_presence.subscribe_connection(
            conn_id,
            ["room"],
            {"status": "online"},
        )
        drain_until(server_with_presence, "presence_join", timeout=1.0)

        await ws.close()
        drain_until(server_with_presence, "presence_leave", timeout=2.0)

        members = server_with_presence.presence("room")
        assert len(members) == 0

        stats = server_with_presence.presence_stats("room")
        assert stats["num_users"] == 0
        assert stats["num_connections"] == 0

    @pytest.mark.asyncio
    async def test_presence_broadcast_to_subscribers(self, server_with_presence, server_port):
        """Subscribers receive presence_join/leave as WebSocket messages."""
        token_alice = make_token("alice")
        token_bob = make_token("bob")

        def parse_wse_msg(raw):
            """Parse a WSE-prefixed message: strip 'WSE' prefix, then JSON decode."""
            text = raw if isinstance(raw, str) else raw.decode()
            if text.startswith("WSE"):
                text = text[3:]
            return json.loads(text)

        # Alice connects and subscribes to room (with presence)
        async with await connect(server_port, token_alice) as ws_alice:
            # Consume server_ready
            await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
            ev = drain_until(server_with_presence, "auth_connect")
            server_with_presence.subscribe_connection(
                ev[1],
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)

            # Alice receives her own join broadcast (she's subscribed to room)
            msg = await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
            data = parse_wse_msg(msg)
            assert data["t"] == "presence_join"
            assert data["p"]["user_id"] == "alice"

            # Bob joins the same room
            async with await connect(server_port, token_bob) as ws_bob:
                # Consume server_ready
                await asyncio.wait_for(ws_bob.recv(), timeout=2.0)
                ev = drain_until(server_with_presence, "auth_connect")
                server_with_presence.subscribe_connection(
                    ev[1],
                    ["room"],
                    {"status": "away"},
                )
                drain_until(server_with_presence, "presence_join", timeout=1.0)

                # Alice should receive Bob's join as a WebSocket message
                msg = await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
                data = parse_wse_msg(msg)
                assert data["t"] == "presence_join"
                assert data["p"]["user_id"] == "bob"

            # Bob disconnects (ws_bob closed by context manager)
            drain_until(server_with_presence, "presence_leave", timeout=2.0)

            # Alice should receive Bob's leave
            msg = await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
            data = parse_wse_msg(msg)
            assert data["t"] == "presence_leave"
            assert data["p"]["user_id"] == "bob"

    @pytest.mark.asyncio
    async def test_presence_update_broadcast(self, server_with_presence, server_port):
        """update_presence broadcasts presence_update to topic subscribers."""
        token_alice = make_token("alice")
        token_bob = make_token("bob")

        def parse_wse_msg(raw):
            text = raw if isinstance(raw, str) else raw.decode()
            if text.startswith("WSE"):
                text = text[3:]
            return json.loads(text)

        async with await connect(server_port, token_alice) as ws_alice:
            # Consume server_ready
            await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
            ev = drain_until(server_with_presence, "auth_connect")
            alice_conn_id = ev[1]
            server_with_presence.subscribe_connection(
                alice_conn_id,
                ["room"],
                {"status": "online"},
            )
            drain_until(server_with_presence, "presence_join", timeout=1.0)
            # Consume Alice's own join broadcast
            await asyncio.wait_for(ws_alice.recv(), timeout=2.0)

            async with await connect(server_port, token_bob) as ws_bob:
                # Consume server_ready
                await asyncio.wait_for(ws_bob.recv(), timeout=2.0)
                ev = drain_until(server_with_presence, "auth_connect")
                bob_conn_id = ev[1]
                server_with_presence.subscribe_connection(
                    bob_conn_id,
                    ["room"],
                    {"status": "online"},
                )
                drain_until(server_with_presence, "presence_join", timeout=1.0)
                # Consume Bob's join broadcast on Alice's ws
                await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
                # Consume Bob's own join broadcast
                await asyncio.wait_for(ws_bob.recv(), timeout=2.0)

                # Bob updates presence
                server_with_presence.update_presence(bob_conn_id, {"status": "away"})

                # Alice should receive the update
                msg = await asyncio.wait_for(ws_alice.recv(), timeout=2.0)
                data = parse_wse_msg(msg)
                assert data["t"] == "presence_update"
                assert data["p"]["user_id"] == "bob"
                assert data["p"]["data"]["status"] == "away"

    @pytest.mark.asyncio
    async def test_presence_data_size_limit(self, server_with_presence, server_port):
        """Presence data exceeding max size is silently skipped (no join event)."""
        token = make_token("bigdata")
        async with await connect(server_port, token):
            ev = drain_until(server_with_presence, "auth_connect")
            conn_id = ev[1]

            # Default max is 4096 bytes -- send data that exceeds it
            huge_data = {"payload": "x" * 5000}
            server_with_presence.subscribe_connection(
                conn_id,
                ["room"],
                huge_data,
            )
            # No presence_join event should fire (data too large)
            time.sleep(0.1)
            events = drain_all(server_with_presence, timeout_ms=100)
            presence_joins = [e for e in events if e[0] == "presence_join"]
            assert len(presence_joins) == 0

            # Presence query should show no members
            members = server_with_presence.presence("room")
            assert len(members) == 0
