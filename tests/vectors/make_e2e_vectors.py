"""Make e2e.json: test vectors for the end-to-end encryption of WSE, computed
with the `cryptography` package, independently of the server and the clients.

    python tests/vectors/make_e2e_vectors.py > tests/vectors/e2e.json

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


def frames(th: bytes, secret: bytes, direction: int, texts: list[bytes], rekey_after: int):
    out = []
    epoch = 0
    for seq, text in enumerate(texts):
        if seq == rekey_after:
            secret = h(th, secret, b"wse/e2e next", 32)
            epoch += 1
        k = h(th, secret, b"wse/e2e key", 32)
        iv = h(th, secret, b"wse/e2e iv", 4)
        hdr = struct.pack(">BBIQ", 1, 0, epoch, seq)
        nonce = iv + struct.pack(">Q", seq)
        aad = bytes([direction]) + th + hdr
        sealed = AESGCM(k).encrypt(nonce, text, aad)
        out.append(
            {
                "epoch": epoch,
                "seq": seq,
                "plaintext": text.decode(),
                "frame": (b"E:" + hdr + sealed).hex(),
                "key": k.hex(),
                "iv": iv.hex(),
            }
        )
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
    }
    print(json.dumps(vectors, indent=2))


if __name__ == "__main__":
    main()
