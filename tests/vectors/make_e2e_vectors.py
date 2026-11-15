"""Make e2e.json: test vectors for the end-to-end encryption of WSE, computed
with the `cryptography` package, independently of the server and the clients.

    python tests/vectors/make_e2e_vectors.py > crates/wse-protocol/tests/vectors/e2e.json

ECDSA signatures are randomized: the vector holds one signature to verify.
"""

import base64
import hashlib
import json
import struct

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

SUITE = "p256-aes256gcm"


def key(n: int) -> ec.EllipticCurvePrivateKey:
    return ec.derive_private_key(n, ec.SECP256R1())


def point(k: ec.EllipticCurvePrivateKey) -> bytes:
    return k.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
    )


def h(th: bytes, ikm: bytes, label: bytes, n: int) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=n, salt=th, info=label).derive(ikm)


def transcript(suites, chosen, client_random, client_pub, server_pub, conn_id, kid) -> bytes:
    t = b"WSE-E2E server_hello\0" + bytes([len(suites)])
    for s in suites:
        t += bytes([len(s)]) + s.encode()
    t += bytes([len(chosen)]) + chosen.encode()
    t += client_random + client_pub + server_pub
    t += bytes([len(conn_id)]) + conn_id.encode() + kid
    return t


def seal(th, secret, direction, epoch, seq, text, fmt=1, frame_type=0) -> bytes:
    """A frame sealed with the keys of `secret`, whatever its header says."""
    k = h(th, secret, b"wse/e2e key", 32)
    iv = h(th, secret, b"wse/e2e iv", 4)
    hdr = struct.pack(">BBIQ", fmt, frame_type, epoch, seq)
    nonce = iv + struct.pack(">Q", seq)
    aad = bytes([direction]) + th + hdr
    return b"E:" + hdr + AESGCM(k).encrypt(nonce, text, aad)


def frames(th: bytes, secret: bytes, direction: int, texts: list[bytes], rekey_after: int):
    out = []
    epoch = 0
    for seq, text in enumerate(texts):
        if seq == rekey_after:
            secret = h(th, secret, b"wse/e2e next", 32)
            epoch += 1
        out.append(
            {
                "epoch": epoch,
                "seq": seq,
                "plaintext": text.decode(),
                "frame": seal(th, secret, direction, epoch, seq, text).hex(),
                "key": h(th, secret, b"wse/e2e key", 32).hex(),
                "iv": h(th, secret, b"wse/e2e iv", 4).hex(),
            }
        )
    return out


def changed(frame: bytes, at: int, value: int | None = None) -> bytes:
    out = bytearray(frame)
    out[at] = out[at] ^ 1 if value is None else value
    return bytes(out)


def refused(th: bytes, secrets: dict[str, bytes]) -> list[dict]:
    """Frames a receiver must refuse after opening the first `after` frames of
    its direction: each must end the session."""
    out = []
    for name, d in (("server_to_client", 2), ("client_to_server", 1)):
        e0 = secrets[name]
        e2 = h(th, h(th, e0, b"wse/e2e next", 32), b"wse/e2e next", 32)
        one = b'{"t":"one"}'
        first = seal(th, e0, d, 0, 0, one)
        tag_at, body_at, type_at = len(first) - 1, 2 + 14, 3
        cases = [
            ("tampered tag", 0, changed(first, tag_at)),
            ("tampered ciphertext", 0, changed(first, body_at)),
            ("header changed after sealing", 0, changed(first, type_at, 1)),
            ("replayed", 1, first),
            ("skipped ahead", 0, seal(th, e0, d, 0, 1, b'{"t":"two"}')),
            ("sealed for the other direction", 0, seal(th, e0, 3 - d, 0, 0, one)),
            ("next epoch sealed with the old key", 0, seal(th, e0, d, 1, 0, one)),
            ("an epoch skipped", 0, seal(th, e2, d, 2, 0, one)),
            ("earlier epoch after a rekey", 3, seal(th, e0, d, 0, 3, one)),
            ("unknown frame format", 0, seal(th, e0, d, 0, 0, one, fmt=2)),
            ("unknown frame type", 0, seal(th, e0, d, 0, 0, one, frame_type=7)),
            ("shorter than a tag", 0, first[: 2 + 14 + 15]),
            ("no frame prefix", 0, b"X:" + first[2:]),
        ]
        out += [
            {"case": case, "direction": name, "after": after, "frame": frame.hex()}
            for case, after, frame in cases
        ]
    return out


def main() -> None:
    identity = key(0x1D)
    client, server = key(0xC1), key(0x5E)
    client_pub, server_pub = point(client), point(server)
    identity_pub = point(identity)
    kid = hashlib.sha256(identity_pub).digest()[:8]
    suites = ["other-suite", SUITE]
    client_random = bytes(range(32))
    conn_id = "019a4f2e-0000-7000-8000-000000000001"
    t = transcript(suites, SUITE, client_random, client_pub, server_pub, conn_id, kid)
    th = hashlib.sha256(t).digest()
    z = client.exchange(ec.ECDH(), server.public_key())
    c2s = h(th, z, b"wse/e2e c2s", 32)
    s2c = h(th, z, b"wse/e2e s2c", 32)
    r, s = decode_dss_signature(identity.sign(t, ec.ECDSA(hashes.SHA256())))
    texts = [b'{"t":"one"}', b'{"t":"two"}', b'{"t":"three"}']
    b64 = lambda b: base64.b64encode(b).decode()  # noqa: E731
    vectors = {
        "suite": SUITE,
        "suites": suites,
        "client_random": b64(client_random),
        "client_public_key": b64(client_pub),
        "server_public_key": b64(server_pub),
        "connection_id": conn_id,
        "identity_public_key": b64(identity_pub),
        "key_id": kid.hex(),
        "transcript": t.hex(),
        "th": th.hex(),
        "shared_secret": z.hex(),
        "client_to_server_secret": c2s.hex(),
        "server_to_client_secret": s2c.hex(),
        "signature": b64(r.to_bytes(32, "big") + s.to_bytes(32, "big")),
        # Epoch 1 starts at the third frame of each direction.
        "server_to_client": frames(th, s2c, 2, texts, rekey_after=2),
        "client_to_server": frames(th, c2s, 1, texts, rekey_after=2),
        "refused": refused(th, {"server_to_client": s2c, "client_to_server": c2s}),
    }
    print(json.dumps(vectors, indent=2))


if __name__ == "__main__":
    main()
