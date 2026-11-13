"""The Python client against the real server: subscriptions, recovery after a
reconnect, and both E2E designs."""

import asyncio
import importlib.util
import json

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_delivery import identity_key
from wse_server._wse_accel import RustWSEServer

wse_client = pytest.importorskip("wse_client")
needs_crypto = pytest.mark.skipif(
    importlib.util.find_spec("cryptography") is None, reason="cryptography not installed"
)


def start_server(port: int, **options) -> RustWSEServer:
    options.setdefault("client_subscriptions", True)
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
    return srv


def client_for(port: int, **options):
    return wse_client.AsyncWSEClient(
        f"ws://127.0.0.1:{port}/wse",
        token=make_token("pyclient"),
        reconnect=wse_client.ReconnectConfig(base_delay=0.05, max_delay=0.2, jitter=False),
        **options,
    )


async def next_of_type(client, kind: str, timeout: float = 5.0):
    async with asyncio.timeout(timeout):
        while True:
            event = await client.recv()
            if event.type == kind:
                return event


async def subscribed(srv: RustWSEServer, topic: str) -> None:
    async with asyncio.timeout(5):
        while srv.get_topic_subscriber_count(topic) == 0:
            await asyncio.sleep(0.02)


def publish(srv: RustWSEServer, topic: str, n: int) -> None:
    srv.broadcast_local(topic, json.dumps({"t": "tick", "p": {"n": n}}))


class TestPlain:
    @pytest.mark.asyncio
    async def test_the_client_subscribes_and_receives(self, server_port):
        srv = start_server(server_port)
        try:
            client = client_for(server_port, topics=["news"])
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 1)
            assert (await next_of_type(client, "tick")).payload == {"n": 1}
            await client.send("chat", {"text": "hi"})
            async with asyncio.timeout(5):
                while True:
                    events = [e for e in srv.drain_inbound(64, 50) if e[0] == "msg"]
                    if events:
                        break
            assert events[0][2]["p"] == {"text": "hi"}
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_missed_messages_come_back_after_a_reconnect(self, server_port):
        srv = start_server(server_port, recovery_enabled=True)
        try:
            client = client_for(server_port, topics=["prices"])
            await client.connect()
            await subscribed(srv, "prices")
            for n in range(3):
                publish(srv, "prices", n)
            for n in range(3):
                assert (await next_of_type(client, "tick")).payload == {"n": n}
            # Drop every connection; publish while the client is away.
            for conn_id in srv.get_connections():
                srv.disconnect(conn_id)
            async with asyncio.timeout(5):
                while srv.get_topic_subscriber_count("prices"):
                    await asyncio.sleep(0.02)
            for n in range(3, 6):
                publish(srv, "prices", n)
            got = [(await next_of_type(client, "tick", timeout=10)).payload["n"] for _ in range(3)]
            assert got == [3, 4, 5]
            await client.disconnect()
        finally:
            srv.stop()


@needs_crypto
class TestEncryption:
    @pytest.mark.asyncio
    async def test_a_pinned_session(self, server_port):
        srv = start_server(server_port, e2e_identity_key=identity_key())
        try:
            client = client_for(
                server_port,
                topics=["news"],
                encryption=True,
                e2e_server_keys=[srv.e2e_identity_public_key()],
            )
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 7)
            assert (await next_of_type(client, "tick")).payload == {"n": 7}
            await client.send("chat", {"text": "sealed"})
            async with asyncio.timeout(5):
                while True:
                    events = [e for e in srv.drain_inbound(64, 50) if e[0] == "msg"]
                    if events:
                        break
            assert events[0][2]["p"] == {"text": "sealed"}
            # Heartbeats in the session are sealed too: still connected.
            await asyncio.sleep(0.3)
            assert client.is_connected
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_a_wrong_pin_never_falls_back_to_plaintext(self, server_port):
        srv = start_server(server_port, e2e_identity_key=identity_key())
        other = RustWSEServer("127.0.0.1", 0, e2e_identity_key=identity_key())
        try:
            client = client_for(
                server_port,
                encryption=True,
                e2e_server_keys=[other.e2e_identity_public_key()],
            )
            with pytest.raises(wse_client.WSEEncryptionError):
                await client.connect()
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_the_first_design_still_works(self, server_port):
        srv = start_server(server_port)
        try:
            client = client_for(server_port, topics=["news"], encryption=True, e2e_legacy=True)
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 9)
            assert (await next_of_type(client, "tick")).payload == {"n": 9}
            await client.disconnect()
        finally:
            srv.stop()
