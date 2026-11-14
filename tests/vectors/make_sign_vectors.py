"""Make sign.json: test vectors for WSE's signed messages, computed with the
`cryptography` package, independently of the server and the clients.

    python tests/vectors/make_sign_vectors.py > tests/vectors/sign.json

ECDSA signatures are randomized: each vector holds one signature to verify.
"""

import base64
import hashlib
import json

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

CLIENT_LABEL = b"WSE-SIG client\0"
SERVER_LABEL = b"WSE-SIG server\0"


def key(n: int) -> ec.EllipticCurvePrivateKey:
    return ec.derive_private_key(n, ec.SECP256R1())


def point(k: ec.EllipticCurvePrivateKey) -> bytes:
    return k.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
    )


def kid(public: bytes) -> str:
    return hashlib.sha256(public).digest()[:8].hex()


def sign(k: ec.EllipticCurvePrivateKey, label: bytes, body: str) -> str:
    r, s = decode_dss_signature(k.sign(label + body.encode(), ec.ECDSA(hashes.SHA256())))
    return base64.b64encode(r.to_bytes(32, "big") + s.to_bytes(32, "big")).decode()


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def side(k: ec.EllipticCurvePrivateKey, label: bytes, body: str) -> dict:
    public = point(k)
    sig = sign(k, label, body)
    return {
        "public_key": base64.b64encode(public).decode(),
        "jwk": {"kty": "EC", "crv": "P-256", "x": b64url(public[1:33]), "y": b64url(public[33:])},
        "kid": kid(public),
        "body": body,
        "sig": sig,
        "envelope": json.dumps({"t": "signed", "sb": body, "sig": sig, "kid": kid(public)}),
        # The same signature over a body one character off: must not verify.
        "tampered_body": body.replace("1.25", "9.25"),
    }


def main() -> None:
    server = key(0x5E7E5)
    client = key(0xC11E7)
    server_kid = kid(point(server))
    # Escapes, non-ASCII text and a decimal as a string: the bytes as sent.
    order = (
        '{"t":"order","p":{"side":"buy","qty":"1.25","note":"caf\\u00e9 \\"q\\" ü"},'
        f'"aud":"{server_kid}","ts":1794600000000,"n":"order-7"}}'
    )
    fill = (
        '{"t":"fill","p":{"px":"1.25","qty":"3"},"ts":1794600000123,'
        '"n":"0192b0a0c0de7000a000000000000001","tp":"orders.u1"}'
    )
    out = {
        "client_label": CLIENT_LABEL.decode(),
        "server_label": SERVER_LABEL.decode(),
        "client": side(client, CLIENT_LABEL, order),
        "server": side(server, SERVER_LABEL, fill),
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
