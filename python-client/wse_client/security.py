# =============================================================================
# WSE Python Client -- Security Manager
# =============================================================================
#
# ECDH P-256 key exchange, AES-GCM-256 encryption, HMAC-SHA256 signing.
# Wire-compatible with rust/src/wse/security.rs and client/utils/security.ts.
# =============================================================================

from __future__ import annotations

import hashlib
import hmac
import os

from ._logging import logger

try:
    from cryptography.hazmat.primitives.asymmetric.ec import (
        ECDH,
        SECP256R1,
        EllipticCurvePrivateKey,
        generate_private_key,
    )
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.hashes import SHA256
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        PublicFormat,
    )

    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False

from .errors import WSEEncryptionError

# Wire constants
HKDF_SALT = b"wse-encryption"
HKDF_INFO = b"aes-gcm-key"
AES_KEY_LEN = 32  # 256 bits
GCM_IV_LEN = 12  # 96 bits
GCM_TAG_LEN = 16  # 128 bits


class SecurityManager:
    """ECDH P-256 + AES-GCM-256 + HMAC-SHA256.

    Wire-compatible with the Rust server and TypeScript client.
    Requires ``cryptography`` package (``pip install wse-client[crypto]``).
    """

    def __init__(self) -> None:
        if not _HAS_CRYPTO:
            raise WSEEncryptionError(
                "cryptography package required: pip install wse-client[crypto]"
            )

        self._private_key: EllipticCurvePrivateKey | None = None
        self._aes_key: bytes | None = None
        self._signing_key: bytes | None = None
        self._enabled = False
        self._iv_cache: dict[bytes, None] = {}  # ordered dict for FIFO eviction

    @property
    def is_enabled(self) -> bool:
        return self._enabled and self._aes_key is not None

    # -- Key exchange ---------------------------------------------------------

    def generate_keypair(self) -> bytes:
        """Generate ECDH P-256 keypair.  Returns 65-byte uncompressed public key."""
        self._private_key = generate_private_key(SECP256R1())
        public_bytes = self._private_key.public_key().public_bytes(
            Encoding.X962, PublicFormat.UncompressedPoint
        )
        assert len(public_bytes) == 65  # 0x04 + 32 + 32
        return public_bytes

    def derive_shared_secret(self, peer_public_bytes: bytes) -> None:
        """Derive AES-256 key from ECDH shared secret via HKDF."""
        if self._private_key is None:
            raise WSEEncryptionError("No keypair generated")

        # Import peer public key from uncompressed SEC1
        from cryptography.hazmat.primitives.asymmetric.ec import (
            EllipticCurvePublicKey as ECPub,
        )

        peer_key = ECPub.from_encoded_point(SECP256R1(), peer_public_bytes)

        # ECDH
        shared_secret = self._private_key.exchange(ECDH(), peer_key)

        # HKDF -> AES key
        self._aes_key = HKDF(
            algorithm=SHA256(),
            length=AES_KEY_LEN,
            salt=HKDF_SALT,
            info=HKDF_INFO,
        ).derive(shared_secret)

        # Signing key = SHA256(shared_secret)
        self._signing_key = hashlib.sha256(shared_secret).digest()
        self._enabled = True
        self._iv_cache = {}
        logger.debug("Encryption enabled (AES-GCM-256)")

    # -- AES-GCM-256 encrypt / decrypt ----------------------------------------

    def encrypt(self, plaintext: str) -> bytes:
        """Encrypt plaintext -> bytes (without E: prefix)."""
        if self._aes_key is None:
            raise WSEEncryptionError("Encryption not initialised")

        iv = self._generate_iv()
        aes = AESGCM(self._aes_key)
        ciphertext_and_tag = aes.encrypt(iv, plaintext.encode("utf-8"), None)
        # Wire: IV (12) + ciphertext + tag (16)
        return iv + ciphertext_and_tag

    def decrypt(self, data: bytes) -> str:
        """Decrypt bytes (without E: prefix) -> plaintext string."""
        if self._aes_key is None:
            raise WSEEncryptionError("Encryption not initialised")

        if len(data) < GCM_IV_LEN + GCM_TAG_LEN:
            raise WSEEncryptionError("Encrypted payload too short")

        iv = data[:GCM_IV_LEN]
        ciphertext_and_tag = data[GCM_IV_LEN:]

        aes = AESGCM(self._aes_key)
        try:
            plaintext = aes.decrypt(iv, ciphertext_and_tag, None)
        except Exception as exc:
            raise WSEEncryptionError(f"Decryption failed: {exc}") from exc

        return plaintext.decode("utf-8")

    # -- HMAC-SHA256 signing ---------------------------------------------------

    def sign(self, payload_json: str) -> str:
        """Sign a JSON payload.  Returns hex-encoded HMAC-SHA256."""
        if self._signing_key is None:
            raise WSEEncryptionError("Signing key not available")

        # Match Rust: SHA256(payload) -> hex, then HMAC-SHA256(key, hex_hash)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        sig = hmac.new(
            self._signing_key, payload_hash.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return sig

    def verify(self, payload_json: str, signature_hex: str) -> bool:
        """Verify an HMAC-SHA256 signature."""
        expected = self.sign(payload_json)
        return hmac.compare_digest(expected, signature_hex)

    # -- Helpers --------------------------------------------------------------

    def _generate_iv(self) -> bytes:
        """Generate a unique 12-byte IV."""
        for _ in range(100):
            iv = os.urandom(GCM_IV_LEN)
            if iv not in self._iv_cache:
                self._iv_cache[iv] = None
                # Evict oldest half (dict preserves insertion order in Python 3.7+)
                if len(self._iv_cache) > 10_000:
                    keys = list(self._iv_cache)[:5_000]
                    for k in keys:
                        del self._iv_cache[k]
                return iv

        # Extremely unlikely fallback
        iv = os.urandom(GCM_IV_LEN)
        self._iv_cache[iv] = None
        return iv

    def reset(self) -> None:
        """Clear all keys and disable encryption."""
        self._private_key = None
        self._aes_key = None
        self._signing_key = None
        self._enabled = False
        self._iv_cache = {}
