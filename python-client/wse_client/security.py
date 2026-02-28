# =============================================================================
# WSE Python Client -- Security Manager
# =============================================================================
#
# ECDH P-256 key exchange, AES-GCM-256 encryption, HMAC-SHA256 signing.
# Wire-compatible with rust/src/wse/security.rs and client/utils/security.ts.
# =============================================================================

from __future__ import annotations

import asyncio
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
        self._signing_enabled_flag = False  # explicit opt-in via enable_signing()
        self._enabled = False
        self._iv_cache: dict[bytes, None] = {}  # ordered dict for FIFO eviction

        # Key rotation
        self._rotation_task: asyncio.Task[None] | None = None
        self._rotation_interval: float = 3600.0  # 1 hour default

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
        if len(public_bytes) != 65:
            raise WSEEncryptionError(
                f"Unexpected ECDH public key length: {len(public_bytes)}, expected 65"
            )
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

        # Signing key = random (matches TS client behavior; server uses
        # application-provided key, not derived from ECDH shared secret)
        self._signing_key = os.urandom(32)
        self._enabled = True
        self._iv_cache = {}
        self.start_key_rotation()
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

    @property
    def signing_enabled(self) -> bool:
        return self._signing_enabled_flag and self._signing_key is not None

    def enable_signing(self) -> None:
        """Explicitly enable HMAC signing for outgoing messages."""
        self._signing_enabled_flag = True

    def sign(self, payload_json: str) -> str:
        """Sign a JSON payload.  Returns base64-encoded HMAC-SHA256.

        Wire-compatible with TS client ``signMessage()``: direct HMAC-SHA256
        over raw UTF-8 bytes, base64-encoded output.
        """
        if self._signing_key is None:
            raise WSEEncryptionError("Signing key not available")

        import base64

        sig_bytes = hmac.new(
            self._signing_key,
            payload_json.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(sig_bytes).decode("ascii")

    def verify(self, payload_json: str, signature_b64: str) -> bool:
        """Verify a base64-encoded HMAC-SHA256 signature."""
        try:
            expected = self.sign(payload_json)
            return hmac.compare_digest(expected, signature_b64)
        except Exception:
            return False

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

    # -- Key rotation ---------------------------------------------------------

    def start_key_rotation(self, interval: float = 3600.0) -> None:
        """Start periodic key rotation (default: 1 hour).

        Matches TS ``scheduleKeyRotation()``. Rotation is skipped when an
        ECDH shared secret is active because replacing the ECDH-derived key
        with a random one would break encryption (server still holds the
        old shared secret).
        """
        if self._rotation_task is not None:
            return
        self._rotation_interval = interval
        try:
            loop = asyncio.get_running_loop()
            self._rotation_task = loop.create_task(self._rotation_loop())
        except RuntimeError:
            # No running event loop; caller must start rotation later
            pass

    def stop_key_rotation(self) -> None:
        """Cancel the key rotation timer."""
        if self._rotation_task is not None:
            self._rotation_task.cancel()
            self._rotation_task = None

    async def _rotation_loop(self) -> None:
        """Periodic key rotation coroutine."""
        failures = 0
        while True:
            try:
                await asyncio.sleep(self._rotation_interval)
            except asyncio.CancelledError:
                return

            # Skip rotation when ECDH session is active (matches TS behavior)
            if self._aes_key is not None and self._enabled:
                logger.debug("Skipping key rotation (ECDH session active)")
                continue

            try:
                self._signing_key = os.urandom(32)
                self._iv_cache = {}
                failures = 0
                logger.debug("Keys rotated successfully")
            except Exception as exc:
                failures += 1
                logger.error("Key rotation failed: %s", exc)
                if failures >= 3:
                    logger.error("Key rotation disabled after 3 consecutive failures")
                    return

    # -- Reset ----------------------------------------------------------------

    def reset(self) -> None:
        """Clear all keys, disable encryption, and cancel rotation."""
        self._private_key = None
        self._aes_key = None
        self._signing_key = None
        self._enabled = False
        self._iv_cache = {}
        self.stop_key_rotation()
