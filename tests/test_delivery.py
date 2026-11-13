"""What reaches whom: E2E connections on every delivery path, topic ACLs with
pattern subscriptions, queue groups with recovery."""

import asyncio
import base64
import json

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_integration import connect, drain_all, drain_until
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


class TestClientCompression:
    async def received(self, ws) -> bytes:
        async with asyncio.timeout(5):
            while True:
                frame = await ws.recv()
                if isinstance(frame, bytes):
                    return frame

    @pytest.mark.asyncio
    async def test_the_client_decides_about_compression(self, server_port):
        srv = start_server(server_port)
        big = {"t": "big", "p": {"fill": "x" * 4000}}
        try:
            async with await connect(server_port, make_token()) as ws:
                _, conn_id, _ = drain_until(srv, "auth_connect")
                srv.send_event(conn_id, big)
                assert (await self.received(ws)).startswith(b"C:")
                await ws.send(
                    json.dumps({"t": "client_hello", "p": {"features": {"compression": False}}})
                )
                await asyncio.sleep(0.2)
                srv.send_event(conn_id, big)
                assert not (await self.received(ws)).startswith(b"C:")
                await ws.send(
                    json.dumps(
                        {
                            "c": "WSE",
                            "t": "client_config",
                            "p": {"compression": True, "compression_threshold": 10},
                        }
                    )
                )
                await asyncio.sleep(0.2)
                srv.send_event(conn_id, {"t": "small", "p": {"n": 12345}})
                assert (await self.received(ws)).startswith(b"C:")
                # The application's own 0 still turns compression off.
                srv.send_event(conn_id, big, compression_threshold=0)
                assert not (await self.received(ws)).startswith(b"C:")
                assert not [e for e in drain_all(srv) if e[0] == "msg"]
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_compression_false_in_the_url(self, server_port):
        import websockets

        srv = start_server(server_port)
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{server_port}/wse?compression=false",
                additional_headers={"Authorization": f"Bearer {make_token()}"},
            ) as ws:
                _, conn_id, _ = drain_until(srv, "auth_connect")
                srv.send_event(conn_id, {"t": "big", "p": {"fill": "x" * 4000}})
                assert not (await self.received(ws)).startswith(b"C:")
        finally:
            srv.stop()


def identity_key() -> bytes:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    return ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


class SessionClient:
    """The client side of an E2E session, by hand on a plain socket."""

    def __init__(self, ws, session):
        self.ws = ws
        self.session = session

    @classmethod
    async def open(cls, port: int, server_keys=None, unauthenticated=False):
        from wse_client.e2e_session import Offer

        ws = await connect(port, make_token())
        offer = Offer()
        await ws.send(
            json.dumps({"t": "client_hello", "p": {"protocol_version": 1, "e2e": offer.message()}})
        )
        async with asyncio.timeout(5):
            while True:
                msg = json.loads(await ws.recv())
                if msg["t"] == "server_hello":
                    p = msg["p"]
                    session = offer.accept(
                        p["e2e"], p["connection_id"], server_keys, unauthenticated
                    )
                    return cls(ws, session)

    async def send(self, message: dict):
        await self.ws.send(self.session.seal(json.dumps(message).encode()))

    async def recv(self) -> dict:
        frame = await asyncio.wait_for(self.ws.recv(), timeout=5)
        assert isinstance(frame, bytes) and frame.startswith(b"E:"), frame
        return json.loads(self.session.open(frame))

    async def next_of_type(self, kind: str) -> dict:
        async with asyncio.timeout(5):
            while True:
                msg = await self.recv()
                if msg.get("t") == kind:
                    return msg


class TestE2eSessions:
    @pytest.mark.asyncio
    async def test_a_pinned_session_carries_everything_sealed(self, server_port):
        srv = start_server(server_port, e2e_identity_key=identity_key())
        try:
            pin = srv.e2e_identity_public_key()
            client = await SessionClient.open(server_port, server_keys=[pin])
            _, conn_id, _ = drain_until(srv, "auth_connect")
            srv.subscribe_connection(conn_id, ["room"])
            await asyncio.sleep(0.1)
            srv.broadcast_local("room", json.dumps({"t": "topic", "p": {"n": 1}}))
            assert (await client.next_of_type("topic"))["p"] == {"n": 1}
            srv.send_event(conn_id, {"t": "direct", "p": {"fill": "x" * 3000}})
            assert (await client.next_of_type("direct"))["p"]["fill"] == "x" * 3000
            await client.send({"t": "ping", "p": {"timestamp": 1}})
            assert (await client.next_of_type("PONG"))["p"]["client_timestamp"] == 1
            await client.send({"t": "chat", "p": {"text": "hi"}})
            assert drain_until(srv, "msg")[2]["p"] == {"text": "hi"}
            await client.ws.close()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_plaintext_or_replayed_frames_end_the_session(self, server_port):
        import websockets

        srv = start_server(server_port, e2e_identity_key=identity_key())
        try:
            pin = srv.e2e_identity_public_key()
            client = await SessionClient.open(server_port, server_keys=[pin])
            await client.ws.send(json.dumps({"t": "chat", "p": {}}))
            with pytest.raises(websockets.ConnectionClosed):
                while True:
                    await asyncio.wait_for(client.ws.recv(), timeout=5)
            assert client.ws.close_code == 4802

            client = await SessionClient.open(server_port, server_keys=[pin])
            frame = client.session.seal(b'{"t":"chat","p":{}}')
            await client.ws.send(frame)
            await client.ws.send(frame)  # the same frame again
            with pytest.raises(websockets.ConnectionClosed):
                while True:
                    await asyncio.wait_for(client.ws.recv(), timeout=5)
            assert client.ws.close_code == 4802
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_the_client_refuses_an_unpinned_or_unsigned_server(self, server_port):
        from wse_client.e2e_session import E2eError

        srv = start_server(server_port, e2e_identity_key=identity_key())
        try:
            other = RustWSEServer("127.0.0.1", 0, e2e_identity_key=identity_key())
            with pytest.raises(E2eError) as wrong:
                await SessionClient.open(server_port, server_keys=[other.e2e_identity_public_key()])
            assert wrong.value.close_code == 4801
        finally:
            srv.stop()
        plain = start_server(server_port)
        try:
            assert plain.e2e_identity_public_key() is None
            with pytest.raises(E2eError):
                await SessionClient.open(server_port, server_keys=["BAAAAA=="])
            client = await SessionClient.open(server_port, unauthenticated=True)
            await client.send({"t": "ping", "p": {"timestamp": 2}})
            assert (await client.next_of_type("PONG"))["p"]["client_timestamp"] == 2
            await client.ws.close()
        finally:
            plain.stop()

    @pytest.mark.asyncio
    async def test_the_first_design_can_be_turned_off(self, server_port):
        srv = start_server(server_port, e2e_legacy=False)
        try:
            ws = await connect(server_port, make_token())
            _, public = rust_ecdh_generate_keypair()
            await ws.send(
                json.dumps(
                    {
                        "t": "client_hello",
                        "p": {
                            "features": {"encryption": True},
                            "encryption_public_key": base64.b64encode(public).decode(),
                        },
                    }
                )
            )
            codes = []
            async with asyncio.timeout(5):
                try:
                    async for frame in ws:
                        codes.append(json.loads(frame).get("p", {}).get("code"))
                except Exception:
                    pass
            assert "E2E_UNSUPPORTED" in codes
            assert ws.close_code == 4800
        finally:
            srv.stop()

    def test_a_malformed_identity_key_is_refused(self):
        with pytest.raises(ValueError, match="e2e_identity_key"):
            RustWSEServer("127.0.0.1", 0, e2e_identity_key=b"not a key")
