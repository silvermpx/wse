"""End-to-end encryption with an authenticated server: the client's side.

The client offers its suites, an ephemeral P-256 key and 32 random bytes in
``client_hello.p.e2e``. The server answers in ``server_hello.p.e2e`` with its
own ephemeral key and an ECDSA P-256 signature over the exchange, made with
its identity key; a client pinning that key knows no one in between answered.
Both sides then derive one key schedule per direction, bound to the exchange,
and every frame carries its direction's epoch and sequence number: only the
next frame opens, so replayed, reordered, dropped or foreign frames end the
connection.

Frame: ``E:`` | 0x01 | type | epoch u32be | seq u64be | ciphertext | tag.
"""

from __future__ import annotations

import base64
import hashlib
import os
import struct
import time
from typing import Any

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

SUITE = "p256-aes256gcm"
SIGNATURE_ALG = "ES256"

CLOSE_UNSUPPORTED = 4800
CLOSE_SERVER_REJECTED = 4801
CLOSE_BAD_FRAME = 4802
CLOSE_EXHAUSTED = 4803

_LABEL = b"WSE-E2E server_hello\0"
_FORMAT = 0x01
_DATA = 0
_CLOSE = 1
_HEADER = struct.Struct(">BBIQ")
_SEQ_MAX = (1 << 53) - 1
_REKEY_FRAMES = 1 << 20
_REKEY_AFTER = 600.0
_MAX_EPOCH_FRAMES = 1 << 24
_C2S = 0x01
_S2C = 0x02


class E2eError(Exception):
    """The session cannot go on; ``close_code`` says why (4800-4803)."""

    def __init__(self, message: str, close_code: int) -> None:
        super().__init__(message)
        self.close_code = close_code


def key_id(public_key: bytes) -> str:
    """The id of an identity public key (65-byte P-256 point): 8 bytes of
    its SHA-256, in hex."""
    return hashlib.sha256(public_key).digest()[:8].hex()


def transcript(
    suites: list[str],
    chosen: str,
    client_random: bytes,
    client_public: bytes,
    server_public: bytes,
    connection_id: str,
    kid: bytes,
) -> bytes:
    """The bytes the server signs and both sides hash into the session."""
    t = _LABEL + bytes([len(suites)])
    for suite in suites:
        t += bytes([len(suite)]) + suite.encode()
    t += bytes([len(chosen)]) + chosen.encode()
    conn = connection_id.encode()[:255]
    return t + client_random + client_public + server_public + bytes([len(conn)]) + conn + kid


def _derive(th: bytes, ikm: bytes, label: bytes, length: int) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=th, info=label).derive(ikm)


class _Direction:
    """One direction's key schedule: the current epoch's key and IV."""

    def __init__(self, th: bytes, direction: int, secret: bytes) -> None:
        self.th = th
        self.direction = direction
        self.secret = secret
        self.epoch = 0
        self._keys()

    def _keys(self) -> None:
        self.aead = AESGCM(_derive(self.th, self.secret, b"wse/e2e key", 32))
        self.iv = _derive(self.th, self.secret, b"wse/e2e iv", 4)

    def next_secret(self) -> bytes:
        return _derive(self.th, self.secret, b"wse/e2e next", 32)

    def aad(self, header: bytes) -> bytes:
        return bytes([self.direction]) + self.th + header


class _Sender(_Direction):
    def __init__(self, th: bytes, direction: int, secret: bytes) -> None:
        super().__init__(th, direction, secret)
        self.seq = 0
        self.epoch_frames = 0
        self.epoch_started = time.monotonic()

    def seal(self, plaintext: bytes, frame_type: int = _DATA) -> bytes:
        if self.seq > _SEQ_MAX:
            raise E2eError("e2e frame counter used up", CLOSE_EXHAUSTED)
        if (
            self.epoch_frames >= _REKEY_FRAMES
            or time.monotonic() - self.epoch_started >= _REKEY_AFTER
        ):
            self.secret = self.next_secret()
            self.epoch += 1
            self._keys()
            self.epoch_frames = 0
            self.epoch_started = time.monotonic()
        header = _HEADER.pack(_FORMAT, frame_type, self.epoch, self.seq)
        nonce = self.iv + struct.pack(">Q", self.seq)
        sealed = self.aead.encrypt(nonce, plaintext, self.aad(header))
        self.seq += 1
        self.epoch_frames += 1
        return b"E:" + header + sealed


class _Receiver(_Direction):
    def __init__(self, th: bytes, direction: int, secret: bytes) -> None:
        super().__init__(th, direction, secret)
        self.expected = 0
        self.epoch_frames = 0

    def open(self, frame: bytes) -> bytes | None:
        def bad() -> E2eError:
            return E2eError("e2e frame refused", CLOSE_BAD_FRAME)

        if not frame.startswith(b"E:") or len(frame) < 2 + _HEADER.size + 16:
            raise bad()
        header = frame[2 : 2 + _HEADER.size]
        fmt, frame_type, epoch, seq = _HEADER.unpack(header)
        if fmt != _FORMAT or frame_type not in (_DATA, _CLOSE) or seq != self.expected:
            raise bad()
        if epoch == self.epoch:
            if self.epoch_frames >= _MAX_EPOCH_FRAMES:
                raise bad()
            aead, iv, rekeyed = self.aead, self.iv, None
        elif epoch == self.epoch + 1:
            # Derived on trial: kept only if the tag holds.
            rekeyed = _Direction(self.th, self.direction, self.next_secret())
            aead, iv = rekeyed.aead, rekeyed.iv
        else:
            raise bad()
        nonce = iv + struct.pack(">Q", seq)
        try:
            plaintext = aead.decrypt(nonce, frame[2 + _HEADER.size :], self.aad(header))
        except InvalidTag:
            raise bad() from None
        if rekeyed is not None:
            self.secret, self.aead, self.iv = rekeyed.secret, rekeyed.aead, rekeyed.iv
            self.epoch = epoch
            self.epoch_frames = 0
        self.expected += 1
        self.epoch_frames += 1
        return plaintext if frame_type == _DATA else None


class Offer:
    """A fresh offer, made for each connection attempt."""

    def __init__(self) -> None:
        self.suites = [SUITE]
        self._private = ec.generate_private_key(ec.SECP256R1())
        self.public_key = self._private.public_key().public_bytes(
            serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint
        )
        self.random = os.urandom(32)

    def message(self) -> dict[str, Any]:
        """``client_hello.p.e2e``."""
        return {
            "suites": list(self.suites),
            "client_public_key": base64.b64encode(self.public_key).decode(),
            "client_random": base64.b64encode(self.random).decode(),
        }

    def accept(
        self,
        hello: dict[str, Any],
        connection_id: str,
        server_keys: list[str] | None = None,
        unauthenticated: bool = False,
    ) -> Session:
        """Check ``server_hello.p.e2e`` and start the session. With
        ``server_keys`` (base64 identity public keys) the signature must come
        from one of them; ``unauthenticated`` accepts an unsigned answer and
        then protects only against passive observers."""
        if hello.get("suite") not in self.suites:
            raise E2eError("no e2e suite in common", CLOSE_UNSUPPORTED)
        try:
            server_public = base64.b64decode(hello["server_public_key"])
            kid = bytes.fromhex(hello["key_id"])
        except (KeyError, ValueError, TypeError):
            raise E2eError("malformed server e2e answer", CLOSE_SERVER_REJECTED) from None
        if len(server_public) != 65 or len(kid) != 8:
            raise E2eError("malformed server e2e answer", CLOSE_SERVER_REJECTED)
        t = transcript(
            self.suites, SUITE, self.random, self.public_key, server_public, connection_id, kid
        )
        if server_keys:
            pinned = {key_id(base64.b64decode(k)): base64.b64decode(k) for k in server_keys}
            signer = pinned.get(kid.hex())
            signature = hello.get("signature")
            if signer is None or hello.get("alg") != SIGNATURE_ALG or not signature:
                raise E2eError("server identity not pinned", CLOSE_SERVER_REJECTED)
            sig = base64.b64decode(signature)
            der = encode_dss_signature(
                int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big")
            )
            public = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), signer)
            try:
                public.verify(der, t, ec.ECDSA(hashes.SHA256()))
            except InvalidSignature:
                raise E2eError("server identity signature invalid", CLOSE_SERVER_REJECTED) from None
        elif not unauthenticated:
            raise E2eError(
                "pin the server's e2e identity key (server_keys) or allow unauthenticated",
                CLOSE_SERVER_REJECTED,
            )
        peer = ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), server_public)
        z = self._private.exchange(ec.ECDH(), peer)
        th = hashlib.sha256(t).digest()
        return Session(
            _Sender(th, _C2S, _derive(th, z, b"wse/e2e c2s", 32)),
            _Receiver(th, _S2C, _derive(th, z, b"wse/e2e s2c", 32)),
        )


class Session:
    """An established session. Seal and open in the order of the wire."""

    def __init__(self, sender: _Sender, receiver: _Receiver) -> None:
        self._sender = sender
        self._receiver = receiver

    def seal(self, plaintext: bytes) -> bytes:
        """``plaintext`` as the body of a binary frame."""
        return self._sender.seal(plaintext)

    def open(self, frame: bytes) -> bytes | None:
        """The plaintext of the next frame (None for the peer's close
        notice); raises E2eError for anything else."""
        return self._receiver.open(frame)
