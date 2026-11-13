"""The client side of E2E sessions against the shared test vectors (made with
an independent implementation, see tests/vectors in the server repository)."""

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

VECTORS = pathlib.Path(__file__).resolve().parents[2] / "tests" / "vectors" / "e2e.json"


@pytest.fixture(scope="module")
def v() -> dict:
    if not VECTORS.exists():
        pytest.skip("shared vectors not found")
    return json.loads(VECTORS.read_text())


def test_transcript_key_id_and_signature(v):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

    from wse_client.e2e_session import key_id, transcript

    b = base64.b64decode
    identity = b(v["identity_public_key"])
    assert key_id(identity) == v["key_id"]
    t = transcript(
        v["suites"],
        v["suite"],
        b(v["client_random"]),
        b(v["client_public_key"]),
        b(v["server_public_key"]),
        v["connection_id"],
        bytes.fromhex(v["key_id"]),
    )
    assert t.hex() == v["transcript"]
    sig = b(v["signature"])
    der = encode_dss_signature(
        int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big")
    )
    public = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), identity)
    public.verify(der, t, ec.ECDSA(hashes.SHA256()))


@pytest.mark.parametrize(
    "name,direction", [("client_to_server", 1), ("server_to_client", 2)]
)
def test_frames_of_both_directions(v, name, direction):
    from wse_client.e2e_session import _REKEY_FRAMES, _Receiver, _Sender

    th = bytes.fromhex(v["th"])
    secret = bytes.fromhex(v[f"{name}_secret"])
    sender, receiver = _Sender(th, direction, secret), _Receiver(th, direction, secret)
    for i, frame in enumerate(v[name]):
        if i == 2:
            sender.epoch_frames = _REKEY_FRAMES  # the vectors rekey at the third frame
        text = frame["plaintext"].encode()
        sealed = sender.seal(text)
        assert sealed.hex() == frame["frame"]
        assert receiver.open(sealed) == text


def test_a_replayed_or_tampered_frame_is_refused(v):
    from wse_client.e2e_session import E2eError, _Receiver, _Sender

    th = bytes.fromhex(v["th"])
    secret = bytes.fromhex(v["server_to_client_secret"])
    sender, receiver = _Sender(th, 2, secret), _Receiver(th, 2, secret)
    one = sender.seal(b"one")
    assert receiver.open(one) == b"one"
    with pytest.raises(E2eError) as replay:
        receiver.open(one)
    assert replay.value.close_code == 4802
    two = bytearray(sender.seal(b"two"))
    two[-1] ^= 1
    with pytest.raises(E2eError):
        receiver.open(bytes(two))


def test_encryption_needs_a_pin_or_an_explicit_choice():
    from wse_client import AsyncWSEClient, WSEEncryptionError

    with pytest.raises(WSEEncryptionError):
        AsyncWSEClient("ws://127.0.0.1:1/wse", encryption=True)
    AsyncWSEClient("ws://127.0.0.1:1/wse", encryption=True, e2e_unauthenticated=True)
