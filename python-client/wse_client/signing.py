"""Signed messages: a message that travels as the exact text its sender signed,
with the signature and the id of the signing key, so whoever keeps it can show
later who sent what. See ``docs/PROTOCOL.md`` (Signed messages).

The client signs with its own ECDSA P-256 key; the server learns the public
key from the token (``cnf`` claim) or from the application
(``set_client_key``). The server signs with its signing key; the client checks
those messages against the keys it pins (``server_signing_keys``).

Needs the ``cryptography`` package (the ``[crypto]`` extra).
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

CLIENT_LABEL = b"WSE-SIG client\0"
SERVER_LABEL = b"WSE-SIG server\0"


def _crypto() -> Any:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec, utils
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise ImportError(
            "Signed messages need the cryptography package: pip install wse-client[crypto]"
        ) from exc
    return hashes, serialization, ec, utils


@dataclass(frozen=True)
class SignatureProof:
    """What proves who sent a signed message: keep all three.

    Attributes:
        body: The exact text that was signed (a JSON object).
        sig: The ECDSA P-256 signature, ``r || s`` in base64.
        kid: The id of the signing key: the first 8 bytes of SHA-256 of its
            public key, in hex.
        verified: The client checked the signature against a pinned server
            key. False when no ``server_signing_keys`` were given.
    """

    body: str
    sig: str
    kid: str
    verified: bool


class SignatureError(ValueError):
    """A signed message that fails its checks."""


def key_id(public_key: bytes) -> str:
    """The id of a public key (uncompressed point): first 8 bytes of SHA-256, hex."""
    return hashlib.sha256(public_key).digest()[:8].hex()


def generate_signing_key() -> Any:
    """A new ECDSA P-256 private key for this client's signed messages.

    Keep it on the device; register :func:`public_key_base64` (or
    :func:`public_key_jwk` in the token's ``cnf`` claim) with the server.
    """
    _, _, ec, _ = _crypto()
    return ec.generate_private_key(ec.SECP256R1())


def load_signing_key(key: Any) -> Any:
    """A private key object from a key object or PKCS#8 PEM / DER bytes."""
    _, serialization, ec, _ = _crypto()
    if isinstance(key, ec.EllipticCurvePrivateKey):
        loaded = key
    elif isinstance(key, bytes | bytearray):
        data = bytes(key)
        loaded = (
            serialization.load_pem_private_key(data, password=None)
            if data.lstrip().startswith(b"-----")
            else serialization.load_der_private_key(data, password=None)
        )
    else:
        raise TypeError("signing_key must be an EC private key or PKCS#8 PEM/DER bytes")
    if (
        not isinstance(loaded, ec.EllipticCurvePrivateKey)
        or loaded.curve.name != "secp256r1"
    ):
        raise ValueError("signing_key must be an ECDSA P-256 key")
    return loaded


def _point(private_key: Any) -> bytes:
    _, serialization, _, _ = _crypto()
    return private_key.public_key().public_bytes(
        serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
    )


def public_key_base64(private_key: Any) -> str:
    """The public key as the server takes it: base64 of the uncompressed point."""
    return base64.b64encode(_point(private_key)).decode()


def public_key_jwk(private_key: Any) -> dict[str, str]:
    """The public key as a JWK, for a token's ``cnf`` claim: ``{"jwk": ...}``."""
    point = _point(private_key)

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    return {
        "kty": "EC",
        "crv": "P-256",
        "x": b64url(point[1:33]),
        "y": b64url(point[33:]),
    }


def _sign(private_key: Any, label: bytes, body: str) -> str:
    hashes, _, ec, utils = _crypto()
    der = private_key.sign(label + body.encode(), ec.ECDSA(hashes.SHA256()))
    r, s = utils.decode_dss_signature(der)
    return base64.b64encode(r.to_bytes(32, "big") + s.to_bytes(32, "big")).decode()


def _verify(public_key: Any, label: bytes, body: str, sig: str) -> bool:
    hashes, _, ec, utils = _crypto()
    try:
        raw = base64.b64decode(sig, validate=True)
    except ValueError:
        return False
    if len(raw) != 64:
        return False
    der = utils.encode_dss_signature(
        int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big")
    )
    try:
        public_key.verify(der, label + body.encode(), ec.ECDSA(hashes.SHA256()))
    except Exception:
        return False
    return True


class ClientSigner:
    """Signs this client's messages for one server (its signing key id)."""

    def __init__(self, private_key: Any) -> None:
        self._key = load_signing_key(private_key)
        self.kid = key_id(_point(self._key))

    def envelope(
        self,
        type: str,
        payload: Any,
        audience: str,
        *,
        nonce: str | None = None,
        now_ms: int | None = None,
    ) -> str:
        """The signed message as JSON text: ``{"t": "signed", "sb", "sig", "kid"}``.

        ``audience`` is the server's signing key id; ``nonce`` is unique per
        message (an order id, say; a random one by default).
        """
        body: dict[str, Any] = {"t": type}
        if payload is not None:
            body["p"] = payload
        body["aud"] = audience
        body["ts"] = int(time.time() * 1000) if now_ms is None else now_ms
        body["n"] = nonce if nonce is not None else uuid.uuid4().hex
        text = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        return json.dumps(
            {
                "t": "signed",
                "sb": text,
                "sig": _sign(self._key, CLIENT_LABEL, text),
                "kid": self.kid,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )


class ServerKeys:
    """The server signing keys a client trusts, by key id."""

    def __init__(self, keys: list[str] | None) -> None:
        self._keys: dict[str, Any] = {}
        if not keys:
            return
        _, _, ec, _ = _crypto()
        for key in keys:
            point = base64.b64decode(key, validate=True)
            if len(point) != 65 or point[0] != 4:
                raise ValueError(
                    "a server signing key is base64 of an uncompressed P-256 point"
                )
            self._keys[key_id(point)] = ec.EllipticCurvePublicKey.from_encoded_point(
                ec.SECP256R1(), point
            )

    def __bool__(self) -> bool:
        return bool(self._keys)

    def open(self, envelope: dict[str, Any]) -> tuple[dict[str, Any], SignatureProof]:
        """The body of a server-signed message and its proof.

        Raises :class:`SignatureError` when it is malformed, or, with pinned
        keys, when no pinned key has its id or the signature does not verify.
        Without pinned keys the body is returned unchecked (``verified`` is
        False).
        """
        body, sig, kid = envelope.get("sb"), envelope.get("sig"), envelope.get("kid")
        if not (
            isinstance(body, str) and isinstance(sig, str) and isinstance(kid, str)
        ):
            raise SignatureError("signed message malformed")
        verified = False
        if self._keys:
            key = self._keys.get(kid)
            if key is None:
                raise SignatureError(f"no pinned server signing key {kid}")
            if not _verify(key, SERVER_LABEL, body, sig):
                raise SignatureError("server signature does not verify")
            verified = True
        try:
            message = json.loads(body)
        except ValueError as exc:
            raise SignatureError("signed body is not JSON") from exc
        if not isinstance(message, dict) or not isinstance(message.get("t"), str):
            raise SignatureError("signed body malformed")
        return message, SignatureProof(body=body, sig=sig, kid=kid, verified=verified)


__all__ = [
    "CLIENT_LABEL",
    "SERVER_LABEL",
    "ClientSigner",
    "ServerKeys",
    "SignatureError",
    "SignatureProof",
    "generate_signing_key",
    "key_id",
    "load_signing_key",
    "public_key_base64",
    "public_key_jwk",
]
