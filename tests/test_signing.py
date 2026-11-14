"""Signed messages through a running server: a client's signed message checked
and handed over with its proof, the server's receipts and signed messages, and
everything the server refuses."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.util
import json
import threading
import time

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET
from tests.test_delivery import start_server
from tests.test_integration import connect, drain_until
from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("cryptography") is None,
    reason="cryptography not installed",
)

CLIENT_LABEL = b"WSE-SIG client\0"
SERVER_LABEL = b"WSE-SIG server\0"


def new_key():
    from cryptography.hazmat.primitives.asymmetric import ec

    return ec.generate_private_key(ec.SECP256R1())


def pem(key) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def point(key) -> bytes:
    from cryptography.hazmat.primitives import serialization

    return key.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
    )


def kid(public: bytes) -> str:
    return hashlib.sha256(public).digest()[:8].hex()


def jwk(key) -> dict:
    p = point(key)

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    return {"kty": "EC", "crv": "P-256", "x": b64url(p[1:33]), "y": b64url(p[33:])}


def token(key=None, user: str = "trader") -> str:
    now = int(time.time())
    claims = {"sub": user, "iss": JWT_ISSUER, "aud": JWT_AUDIENCE, "exp": now + 3600, "iat": now}
    if key is not None:
        claims["cnf"] = {"jwk": jwk(key)}
    return rust_jwt_encode(claims, JWT_SECRET)


def signed(key, body: dict, *, text: str | None = None) -> str:
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

    text = text if text is not None else json.dumps(body, separators=(",", ":"))
    r, s = decode_dss_signature(key.sign(CLIENT_LABEL + text.encode(), ec.ECDSA(hashes.SHA256())))
    sig = base64.b64encode(r.to_bytes(32, "big") + s.to_bytes(32, "big")).decode()
    return json.dumps({"t": "signed", "sb": text, "sig": sig, "kid": kid(point(key))})


def order(audience: str, n: str, ts: int | None = None, **p) -> dict:
    return {
        "t": "order",
        "p": p or {"side": "buy", "qty": "1.25"},
        "aud": audience,
        "ts": int(time.time() * 1000) if ts is None else ts,
        "n": n,
    }


def server_body(server_public_b64: str, envelope: dict) -> dict:
    """The body of a server-signed message, checked against its key."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

    public = base64.b64decode(server_public_b64)
    assert envelope["t"] == "signed" and envelope["kid"] == kid(public)
    raw = base64.b64decode(envelope["sig"])
    der = encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big"))
    ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), public).verify(
        der, SERVER_LABEL + envelope["sb"].encode(), ec.ECDSA(hashes.SHA256())
    )
    return json.loads(envelope["sb"])


async def next_of(ws, wanted, timeout: float = 3.0) -> dict:
    async with asyncio.timeout(timeout):
        while True:
            frame = await ws.recv()
            if isinstance(frame, bytes):
                import msgpack

                msg = msgpack.unpackb(frame[2:] if frame.startswith(b"M:") else frame)
            else:
                msg = json.loads(frame)
            if wanted(msg):
                return msg


def is_type(t: str):
    return lambda msg: msg.get("t") == t


async def taken(srv, event: str):
    return await asyncio.to_thread(drain_until, srv, event, 3.0)


@pytest.fixture()
def server_key():
    return new_key()


@pytest.fixture()
def signing_server(server_port, server_key):
    srv = start_server(server_port, signing_key=pem(server_key), recovery_enabled=True)
    yield srv
    srv.stop()


class TestClientSigned:
    @pytest.mark.asyncio
    async def test_an_order_reaches_the_app_with_its_proof_and_a_signed_receipt(
        self, signing_server, server_port
    ):
        srv = signing_server
        public = srv.signing_public_key()
        audience = kid(base64.b64decode(public))
        client = new_key()
        async with await connect(server_port, token(client)) as ws:
            await taken(srv, "auth_connect")
            text = signed(client, order(audience, "o-1", note="café"))
            await ws.send(text)
            _, _, message = await taken(srv, "signed")
            assert (message["t"], message["n"]) == ("order", "o-1")
            assert message["p"] == {"note": "café"}
            proof = message["signature"]
            assert proof["body"] == json.loads(text)["sb"]
            assert proof["kid"] == kid(point(client))

            receipt = server_body(public, await next_of(ws, is_type("signed")))
            assert receipt["t"] == "receipt"
            body_hash = base64.b64encode(hashlib.sha256(proof["body"].encode()).digest()).decode()
            assert receipt["p"]["n"] == "o-1"
            assert receipt["p"]["kid"] == proof["kid"]
            assert receipt["p"]["sha256"] == body_hash

    @pytest.mark.asyncio
    async def test_every_refusal_is_an_error_and_nothing_reaches_the_app(
        self, signing_server, server_port
    ):
        srv = signing_server
        audience = kid(base64.b64decode(srv.signing_public_key()))
        client = new_key()
        stranger = new_key()
        refused = is_type("error")
        async with await connect(server_port, token(client)) as ws:
            await taken(srv, "auth_connect")

            async def reason(text: str) -> tuple[str, str | None]:
                await ws.send(text)
                p = (await next_of(ws, refused))["p"]
                assert p["code"] == "SIGNATURE_REFUSED"
                return p["reason"], p.get("n")

            good = order(audience, "o-1")
            text = json.dumps(good, separators=(",", ":"))
            tampered = json.loads(signed(client, good))
            tampered["sb"] = text.replace("1.25", "9.25")
            assert await reason(json.dumps(tampered)) == ("bad_signature", "o-1")
            # Signed by a key the connection does not have.
            assert await reason(signed(stranger, order(audience, "o-2"))) == (
                "unknown_key",
                "o-2",
            )
            assert await reason(signed(client, order("0000000000000000", "o-3"))) == (
                "wrong_audience",
                "o-3",
            )
            stale = int(time.time() * 1000) - 120_000
            assert await reason(signed(client, order(audience, "o-4", ts=stale))) == (
                "stale",
                "o-4",
            )
            no_n = {"t": "order", "aud": audience, "ts": int(time.time() * 1000)}
            assert (await reason(signed(client, no_n)))[0] == "malformed"
            # Taken once: the same n again, freshly signed, is a replay.
            await ws.send(signed(client, order(audience, "o-5")))
            await taken(srv, "signed")
            assert await reason(signed(client, order(audience, "o-5"))) == ("replayed", "o-5")
        assert not [e for e in srv.drain_inbound(256, 100) if e[0] == "signed"]

    @pytest.mark.asyncio
    async def test_the_application_binds_a_key_without_the_token(self, signing_server, server_port):
        srv = signing_server
        audience = kid(base64.b64decode(srv.signing_public_key()))
        client = new_key()
        async with await connect(server_port, token()) as ws:
            _, conn_id, _ = await taken(srv, "auth_connect")
            await ws.send(signed(client, order(audience, "a-1")))
            assert (await next_of(ws, is_type("error")))["p"]["reason"] == "unknown_key"

            assert srv.set_client_key(conn_id, base64.b64encode(point(client)).decode())
            await ws.send(signed(client, order(audience, "a-2")))
            _, _, message = await taken(srv, "signed")
            assert message["n"] == "a-2"

            assert srv.set_client_key(conn_id, None)
            await ws.send(signed(client, order(audience, "a-3")))
            assert (await next_of(ws, is_type("error")))["p"]["reason"] == "unknown_key"

        assert srv.set_client_key("no-such-connection", None) is False
        with pytest.raises(ValueError):
            srv.set_client_key(conn_id, "not-a-key")

    @pytest.mark.asyncio
    async def test_server_hello_names_the_signing_key(self, signing_server, server_port):
        srv = signing_server
        async with await connect(server_port, token()) as ws:
            await ws.send(json.dumps({"t": "client_hello", "p": {"protocol_version": 1}}))
            hello = (await next_of(ws, is_type("server_hello")))["p"]
            assert hello["features"]["signing"] is True
            assert hello["signing"]["public_key"] == srv.signing_public_key()
            assert hello["signing"]["kid"] == kid(base64.b64decode(srv.signing_public_key()))


class TestServerSigned:
    @pytest.mark.asyncio
    async def test_a_signed_topic_message_reaches_json_and_msgpack_clients_intact(
        self, signing_server, server_port
    ):
        pytest.importorskip("msgpack")
        import websockets

        srv = signing_server
        public = srv.signing_public_key()
        headers = {"Authorization": f"Bearer {token()}"}
        url = f"ws://127.0.0.1:{server_port}/wse"
        async with (
            websockets.connect(url, additional_headers=headers) as json_ws,
            websockets.connect(url + "?format=msgpack", additional_headers=headers) as packed_ws,
        ):
            for _ in range(2):
                _, conn_id, _ = await taken(srv, "auth_connect")
                srv.subscribe_connection(conn_id, ["orders.u1"])
            message = srv.sign("fill", {"px": "1.25", "qty": "3"}, topic="orders.u1")
            srv.broadcast_local("orders.u1", message)
            for ws in (json_ws, packed_ws):
                envelope = await next_of(ws, is_type("signed"))
                body = server_body(public, envelope)
                assert (body["t"], body["tp"]) == ("fill", "orders.u1")
                assert body["p"] == {"px": "1.25", "qty": "3"}
                # The recovery stamp is the server's, around the signed text.
                assert envelope["tp"] == "orders.u1" and "o" in envelope

    def test_without_a_signing_key_there_is_nothing_to_sign_or_check(self, server_port):
        srv = RustWSEServer("127.0.0.1", server_port, jwt_secret=JWT_SECRET)
        assert srv.signing_public_key() is None
        with pytest.raises(RuntimeError):
            srv.sign("fill", {})

    @pytest.mark.asyncio
    async def test_a_server_without_a_key_refuses_signed_messages(self, server_port):
        srv = start_server(server_port)
        try:
            client = new_key()
            async with await connect(server_port, token(client)) as ws:
                await taken(srv, "auth_connect")
                await ws.send(signed(client, order("0000000000000000", "x-1")))
                assert (await next_of(ws, is_type("error")))["p"]["reason"] == "not_enabled"
        finally:
            srv.stop()

    def test_options_are_checked(self, server_port):
        with pytest.raises(ValueError):
            RustWSEServer("127.0.0.1", server_port, signing_key=b"not a key")
        with pytest.raises(ValueError):
            RustWSEServer("127.0.0.1", server_port, signature_window=0)


@pytest.mark.asyncio
async def test_callback_mode_hands_the_proof_to_on_signed_message(server_port, server_key):
    srv = RustWSEServer(
        "127.0.0.1",
        server_port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        signing_key=pem(server_key),
    )
    got: list[dict] = []
    done = threading.Event()

    def on_signed(conn_id, message):
        got.append(message)
        done.set()

    srv.set_callbacks(lambda *a: None, lambda *a: None, lambda *a: None, on_signed)
    srv.start()
    try:
        audience = kid(point(server_key))
        client = new_key()
        async with await connect(server_port, token(client)) as ws:
            await ws.send(signed(client, order(audience, "cb-1")))
            assert await asyncio.to_thread(done.wait, 3.0)
        assert got[0]["n"] == "cb-1"
        assert got[0]["signature"]["kid"] == kid(point(client))
    finally:
        srv.stop()


@pytest.mark.asyncio
async def test_the_python_client_signs_and_checks(signing_server, server_port):
    pytest.importorskip("wse_client")
    from wse_client import AsyncWSEClient, generate_signing_key

    srv = signing_server
    key = generate_signing_key()
    url = f"ws://127.0.0.1:{server_port}/wse"
    async with AsyncWSEClient(
        url,
        token=token(key),
        signing_key=key,
        server_signing_keys=[srv.signing_public_key()],
        reconnect=None,
    ) as client:
        await client.send("order", {"side": "sell", "qty": "2"}, signed=True, nonce="py-1")
        _, conn_id, message = await taken(srv, "signed")
        assert (message["t"], message["n"]) == ("order", "py-1")

        receipt = await client.recv(timeout=3.0)
        while receipt.type != "receipt":
            receipt = await client.recv(timeout=3.0)
        assert receipt.payload["n"] == "py-1"
        assert receipt.signature is not None and receipt.signature.verified

        srv.send(conn_id, srv.sign("fill", {"qty": "2"}))
        fill = await client.recv(timeout=3.0)
        while fill.type != "fill":
            fill = await client.recv(timeout=3.0)
        assert fill.payload == {"qty": "2"} and fill.signature.verified
