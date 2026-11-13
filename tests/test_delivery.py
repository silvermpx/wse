"""What reaches whom: E2E connections on every delivery path, topic ACLs with
pattern subscriptions, queue groups with recovery."""

import asyncio
import base64
import json

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_integration import connect, drain_until
from wse_server._wse_accel import (
    RustWSEServer,
    rust_aes_gcm_decrypt,
    rust_aes_gcm_encrypt,
    rust_ecdh_derive_shared_secret,
    rust_ecdh_generate_keypair,
)


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
    return srv


class EncryptedClient:
    """A plain WebSocket client doing the E2E key exchange by hand."""

    def __init__(self, ws):
        self.ws = ws
        self.key = None

    @classmethod
    async def open(cls, port: int, user: str = "eve"):
        client = cls(await connect(port, make_token(user)))
        private, public = rust_ecdh_generate_keypair()
        await client.ws.send(
            json.dumps(
                {
                    "t": "client_hello",
                    "p": {
                        "protocol_version": 1,
                        "features": {"encryption": True},
                        "encryption_public_key": base64.b64encode(public).decode(),
                    },
                }
            )
        )
        async with asyncio.timeout(5):
            while True:
                msg = json.loads(await client.ws.recv())
                if msg["t"] == "server_hello":
                    server_public = base64.b64decode(msg["p"]["encryption_public_key"])
                    client.key = rust_ecdh_derive_shared_secret(private, server_public)
                    return client

    async def recv(self, timeout: float = 3.0):
        """The next message, and whether it came encrypted."""
        frame = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
        if isinstance(frame, bytes) and frame.startswith(b"E:"):
            return json.loads(rust_aes_gcm_decrypt(self.key, frame[2:])), True
        return json.loads(frame), False

    async def send(self, message: dict):
        sealed = rust_aes_gcm_encrypt(self.key, json.dumps(message).encode())
        await self.ws.send(b"E:" + sealed)


async def next_of_type(client: EncryptedClient, kind: str):
    async with asyncio.timeout(5):
        while True:
            msg, encrypted = await client.recv()
            if msg.get("t") == kind:
                return msg, encrypted


class TestEncryptedDelivery:
    @pytest.mark.asyncio
    async def test_every_path_to_an_e2e_connection_is_encrypted(self, server_port):
        srv = start_server(server_port)
        try:
            client = await EncryptedClient.open(server_port)
            _, conn_id, _ = drain_until(srv, "auth_connect")
            srv.subscribe_connection(conn_id, ["room"])
            srv.subscribe_connection(conn_id, ["jobs"], queue_group="workers")
            await asyncio.sleep(0.1)

            srv.broadcast_local("room", json.dumps({"t": "topic", "p": {}}))
            srv.broadcast_all(json.dumps({"t": "everyone", "p": {}}))
            srv.send(conn_id, json.dumps({"t": "direct", "p": {}}))
            srv.broadcast_local("jobs", json.dumps({"t": "job", "p": {}}))
            # Direct sends and broadcasts travel separately: any order.
            seen = {}
            async with asyncio.timeout(5):
                while len(seen) < 4:
                    msg, encrypted = await client.recv()
                    if msg.get("t") in ("topic", "everyone", "direct", "job"):
                        seen[msg["t"]] = encrypted
            assert all(seen.values()), seen
            await client.ws.close()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_plain_and_encrypted_subscribers_share_a_topic(self, server_port):
        srv = start_server(server_port)
        try:
            sealed = await EncryptedClient.open(server_port, "eve")
            _, sealed_id, _ = drain_until(srv, "auth_connect")
            plain = await connect(server_port, make_token("pat"))
            _, plain_id, _ = drain_until(srv, "auth_connect")
            srv.subscribe_connection(sealed_id, ["news"])
            srv.subscribe_connection(plain_id, ["news"])
            await asyncio.sleep(0.1)
            srv.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 1}}))
            msg, encrypted = await next_of_type(sealed, "news")
            assert encrypted and msg["p"] == {"n": 1}
            async with asyncio.timeout(5):
                while True:
                    frame = json.loads(await plain.recv())
                    if frame.get("t") == "news":
                        break
            assert frame["p"] == {"n": 1}
            await plain.close()
            await sealed.ws.close()
        finally:
            srv.stop()


class TestAclWithPatterns:
    @pytest.mark.asyncio
    async def test_a_pattern_subscription_gets_only_allowed_topics(self, server_port):
        srv = start_server(server_port)
        try:
            ws = await connect(server_port, make_token())
            _, conn_id, _ = drain_until(srv, "auth_connect")
            assert srv.set_topic_acl(
                conn_id, allow=["tenant:acme:*"], deny=["tenant:acme:internal:*"]
            )
            srv.subscribe_connection(conn_id, ["tenant:acme:*"])
            await asyncio.sleep(0.1)
            srv.broadcast_local("tenant:acme:internal:logs", json.dumps({"t": "secret", "p": {}}))
            srv.broadcast_local("tenant:acme:orders", json.dumps({"t": "order", "p": {}}))
            seen = []
            async with asyncio.timeout(5):
                while "order" not in seen:
                    seen.append(json.loads(await ws.recv()).get("t"))
            assert "secret" not in seen
            await ws.close()
        finally:
            srv.stop()


class TestQueueGroups:
    @pytest.mark.asyncio
    async def test_members_get_messages_without_the_recovery_stamp(self, server_port):
        srv = start_server(server_port, recovery_enabled=True)
        try:
            ws = await connect(server_port, make_token())
            _, conn_id, _ = drain_until(srv, "auth_connect")
            srv.subscribe_connection(conn_id, ["tasks"], queue_group="workers")
            await asyncio.sleep(0.1)
            srv.broadcast_local("tasks", json.dumps({"t": "task", "p": {"n": 1}}))
            async with asyncio.timeout(5):
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("t") == "task":
                        break
            assert "tp" not in msg and "o" not in msg and "e" not in msg
            await ws.close()
        finally:
            srv.stop()
