"""Signed messages on the client side, against the shared test vectors (made
with an independent implementation, see tests/vectors in the server repository)."""

from __future__ import annotations

import base64
import importlib.util
import json
import pathlib

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("cryptography") is None,
    reason="cryptography not installed",
)

VECTORS = (
    pathlib.Path(__file__).resolve().parents[2] / "tests" / "vectors" / "sign.json"
)


@pytest.fixture(scope="module")
def v() -> dict:
    if not VECTORS.exists():
        pytest.skip("shared vectors not found")
    return json.loads(VECTORS.read_text())


def codec(keys: list[str] | None, msgpack=None):
    from wse_client.compression import CompressionHandler
    from wse_client.protocol import MessageCodec
    from wse_client.signing import ServerKeys

    return MessageCodec(
        CompressionHandler(), msgpack=msgpack, server_keys=ServerKeys(keys)
    )


def test_labels_and_key_ids_match_the_vectors(v):
    from wse_client.signing import CLIENT_LABEL, SERVER_LABEL, key_id

    assert CLIENT_LABEL == v["client_label"].encode()
    assert SERVER_LABEL == v["server_label"].encode()
    for side in ("client", "server"):
        assert key_id(base64.b64decode(v[side]["public_key"])) == v[side]["kid"]


def test_a_server_signed_message_is_delivered_with_its_proof(v):
    server = v["server"]
    envelope = json.loads(server["envelope"])
    envelope.update({"c": "U", "tp": "orders.u1", "e": "0000002a", "o": 7})
    event = codec([server["public_key"]]).decode(json.dumps(envelope))
    assert event.type == "fill"
    assert event.payload == {"px": "1.25", "qty": "3"}
    assert (event.topic, event.epoch, event.offset) == ("orders.u1", "0000002a", 7)
    assert event.signature.body == server["body"]
    assert event.signature.kid == server["kid"]
    assert event.signature.verified is True


def test_a_signed_message_that_does_not_check_is_dropped(v):
    server = v["server"]
    envelope = json.loads(server["envelope"])
    pinned = codec([server["public_key"]])
    tampered = dict(envelope, sb=server["tampered_body"])
    assert pinned.decode(json.dumps(tampered)) is None
    # Signed for another topic than the one it came on.
    assert pinned.decode(json.dumps(dict(envelope, tp="orders.u2"))) is None
    # A key the client does not pin.
    other = codec([v["client"]["public_key"]])
    assert other.decode(json.dumps(envelope)) is None


def test_without_pinned_keys_the_message_comes_unchecked(v):
    event = codec(None).decode(v["server"]["envelope"])
    assert event.type == "fill"
    assert event.signature.verified is False


def test_a_signed_message_in_msgpack_keeps_its_body(v):
    msgpack = pytest.importorskip("msgpack")
    from wse_client.msgpack_handler import MsgPackHandler

    server = v["server"]
    envelope = json.loads(server["envelope"])
    pinned = codec([server["public_key"]], msgpack=MsgPackHandler())
    event = pinned.decode(b"M:" + msgpack.packb(envelope))
    assert event.signature.verified is True
    assert event.signature.body == server["body"]


def test_what_the_client_signs_verifies_with_its_public_key():
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

    from wse_client.signing import (
        CLIENT_LABEL,
        ClientSigner,
        generate_signing_key,
        key_id,
        public_key_base64,
        public_key_jwk,
    )

    key = generate_signing_key()
    signer = ClientSigner(key)
    point = base64.b64decode(public_key_base64(key))
    assert signer.kid == key_id(point)
    jwk = public_key_jwk(key)
    assert (jwk["kty"], jwk["crv"]) == ("EC", "P-256")

    envelope = json.loads(
        signer.envelope(
            "order", {"qty": "1.5", "note": "café"}, "ed94c7318b47fe94", nonce="o-1"
        )
    )
    assert envelope["t"] == "signed" and envelope["kid"] == signer.kid
    body = json.loads(envelope["sb"])
    assert (
        body["t"] == "order"
        and body["aud"] == "ed94c7318b47fe94"
        and body["n"] == "o-1"
    )
    assert isinstance(body["ts"], int)
    raw = base64.b64decode(envelope["sig"])
    der = encode_dss_signature(
        int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big")
    )
    key.public_key().verify(
        der, CLIENT_LABEL + envelope["sb"].encode(), ec.ECDSA(hashes.SHA256())
    )


def test_keys_load_from_pem_and_bad_keys_are_refused():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    from wse_client.signing import ServerKeys, generate_signing_key, load_signing_key

    pem = generate_signing_key().private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    assert load_signing_key(pem).curve.name == "secp256r1"
    with pytest.raises(ValueError):
        load_signing_key(ec.generate_private_key(ec.SECP384R1()))
    with pytest.raises(ValueError):
        ServerKeys(["AAAA"])
