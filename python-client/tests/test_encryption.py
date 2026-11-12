"""E2E encryption fails closed and heartbeats stay plaintext."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os

import pytest

from _fakes import CONTROL_TYPES, FakeWSEServer, until
from wse_client import (
    AsyncWSEClient,
    ConnectionState,
    ReconnectConfig,
    SendResult,
    WSEEncryptionError,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("cryptography") is None,
    reason="cryptography not installed",
)

FAST = ReconnectConfig(base_delay=0.05, max_delay=0.2, jitter=False)


def make_client(url: str, **kwargs) -> AsyncWSEClient:
    kwargs.setdefault("reconnect", FAST)
    client = AsyncWSEClient(url, encryption=True, **kwargs)
    client._circuit_breaker._failure_threshold = 10_000
    return client


def assert_no_plaintext_app_frames(conn) -> None:
    leaked = [t for t in conn.plain_types if t not in CONTROL_TYPES]
    assert leaked == [], f"application frames sent in plaintext: {leaked}"


class TestFailClosed:
    async def test_everything_but_control_frames_is_encrypted(self):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url, topics=["prices"])
            await client.connect()
            assert await client.send("order", {"qty": 1}) is SendResult.SENT
            conn = srv.connections[0]
            await until(lambda: conn.find("order"))
            assert "subscription_update" in conn.encrypted_types
            assert "order" in conn.encrypted_types
            assert conn.plain_types[0] == "client_hello"  # first frame
            assert_no_plaintext_app_frames(conn)
            await client.disconnect()

    async def test_send_before_the_key_exists_waits_for_it(self):
        """Sends between open and the key exchange went out in
        plaintext (or with the previous connection's key)."""
        async with FakeWSEServer(hello_delay=0.3) as srv:
            client = make_client(srv.url)
            connecting = asyncio.create_task(client.connect())
            await until(lambda: client.is_connected)
            assert not client._security.is_enabled
            result = await client.send("early", {"n": 1})
            assert result is SendResult.SENT
            await connecting
            conn = srv.connections[0]
            await until(lambda: conn.find("early"))
            assert conn.encrypted_types.count("early") == 1
            assert_no_plaintext_app_frames(conn)
            await client.disconnect()

    async def test_server_without_encryption_fails_closed(self):
        async with FakeWSEServer(encryption=False) as srv:
            client = make_client(srv.url, topics=["prices"])
            with pytest.raises(WSEEncryptionError):
                await client.connect()
            assert client.state == ConnectionState.ERROR
            await asyncio.sleep(0.3)
            assert len(srv.connections) == 1  # no reconnect loop
            assert_no_plaintext_app_frames(srv.connections[0])
            with pytest.raises(WSEEncryptionError):
                await client.send("order", {})
            await client.disconnect()

    async def test_key_exchange_failed_error_fails_closed(self):
        async def on_message(conn, msg):
            if msg.get("t") == "client_hello":
                await conn.ws.send(
                    json.dumps(
                        {
                            "c": "WSE",
                            "t": "error",
                            "p": {
                                "code": "KEY_EXCHANGE_FAILED",
                                "message": "bad key",
                                "recoverable": True,
                            },
                        }
                    )
                )

        async with FakeWSEServer(answer_hello=False, on_message=on_message) as srv:
            client = make_client(srv.url)
            with pytest.raises(WSEEncryptionError):
                await client.connect()
            assert client.state == ConnectionState.ERROR
            assert_no_plaintext_app_frames(srv.connections[0])
            await client.disconnect()

    async def test_no_server_hello_fails_closed_after_timeout(self, monkeypatch):
        import wse_client.client as client_mod

        monkeypatch.setattr(client_mod, "SERVER_READY_TIMEOUT", 0.3)
        async with FakeWSEServer(answer_hello=False) as srv:
            client = make_client(srv.url, topics=["prices"])
            with pytest.raises(WSEEncryptionError):
                await client.connect()
            assert_no_plaintext_app_frames(srv.connections[0])
            await client.disconnect()

    async def test_reconnect_uses_a_fresh_key(self):
        async with FakeWSEServer() as srv:
            client = make_client(srv.url, topics=["prices"])
            await client.connect()
            first_key = srv.connections[0].client_public_key
            await srv.connections[0].ws.close(1001)
            conn = await until(lambda: len(srv.connections) == 2 and srv.connections[1])
            # The resubscribe on the new connection is encrypted with the new
            # key (the fake server decrypts with the new connection's key).
            await until(lambda: "subscription_update" in conn.encrypted_types)
            assert conn.client_public_key is not None
            assert conn.client_public_key != first_key
            assert_no_plaintext_app_frames(conn)
            await client.disconnect()

    async def test_decrypts_server_messages(self):
        async def on_message(conn, msg):
            if msg.get("t") == "order":
                await conn.send_json({"c": "U", "t": "order_ack", "p": {"ok": True}})

        async with FakeWSEServer(on_message=on_message) as srv:
            client = make_client(srv.url)
            await client.connect()
            await client.send("order", {})
            event = await client.recv(timeout=2.0)
            assert event.type == "order_ack"
            await client.disconnect()


class TestHeartbeat:
    async def test_ping_and_pong_stay_plaintext_and_unlimited(self, monkeypatch):
        """The heartbeat went through encryption and the limiter; the
        server answers only plaintext pings, so no PONG ever came back."""
        import wse_client.connection as conn_mod

        monkeypatch.setattr(conn_mod, "HEARTBEAT_INTERVAL", 0.05)
        async with FakeWSEServer() as srv:
            client = make_client(srv.url, rate_limit=1, rate_limit_burst=1)
            await client.connect()
            await client.send("use", {})  # exhaust the limiter
            assert await client.send("use", {}) is SendResult.FAILED
            conn = srv.connections[0]
            await until(lambda: conn.plain_types.count("ping") >= 2)
            assert "ping" not in conn.encrypted_types
            await until(lambda: client._network_monitor._pongs_received >= 2)
            await client.disconnect()


class TestEncryptedMsgpack:
    def test_encrypted_msgpack_frame_decodes(self):
        """An encrypted M: (msgpack) frame was decoded as UTF-8 text."""
        msgpack = pytest.importorskip("msgpack")
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        from wse_client.compression import CompressionHandler
        from wse_client.msgpack_handler import MsgPackHandler
        from wse_client.protocol import MessageCodec
        from wse_client.security import SecurityManager

        client_sm, server_sm = SecurityManager(), SecurityManager()
        client_pub = client_sm.generate_keypair()
        server_pub = server_sm.generate_keypair()
        client_sm.derive_shared_secret(server_pub)
        server_sm.derive_shared_secret(client_pub)
        client_sm.stop_key_rotation()
        server_sm.stop_key_rotation()

        packed = b"M:" + msgpack.packb({"c": "U", "t": "tick", "p": {"px": 1.5}})
        iv = os.urandom(12)
        frame = b"E:" + iv + AESGCM(server_sm._aes_key).encrypt(iv, packed, None)

        codec = MessageCodec(
            CompressionHandler(), security=client_sm, msgpack=MsgPackHandler()
        )
        event = codec.decode(frame)
        assert event is not None and event.type == "tick"
        assert event.payload == {"px": 1.5}
