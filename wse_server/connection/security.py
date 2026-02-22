# =============================================================================
# WSE — WebSocket Engine
# Rust-accelerated security: AES-GCM-256, ECDH P-256, SHA-256, HMAC-SHA256
# =============================================================================

import base64
import json
import logging
import secrets
import time
from typing import Any, Protocol, runtime_checkable

from wse_server._accel import (
    rust_aes_gcm_decrypt,
    rust_aes_gcm_encrypt,
    rust_ecdh_derive_shared_secret,
    rust_ecdh_generate_keypair,
    rust_hmac_sha256,
    rust_sha256,
)

log = logging.getLogger("wse.security")


# =============================================================================
# Protocols for pluggable security providers
# =============================================================================


@runtime_checkable
class EncryptionProvider(Protocol):
    """Protocol for pluggable encryption.

    Implement this to provide custom encryption/decryption for WebSocket
    messages.  The default SecurityManager uses the built-in AesGcmProvider
    when encryption is enabled.
    """

    def encrypt(self, data: str, conn_id: str = "") -> str:
        """Encrypt plaintext data and return ciphertext (e.g. base64-encoded)."""
        ...

    def decrypt(self, encrypted_data: str, conn_id: str = "") -> str | None:
        """Decrypt ciphertext and return plaintext, or None on failure."""
        ...


@runtime_checkable
class TokenProvider(Protocol):
    """Protocol for pluggable token creation / verification.

    Implement this to provide JWT or custom token signing for WebSocket
    messages.  The default SecurityManager does HMAC-SHA256 signing
    with an auto-generated secret when no provider is set.
    """

    def create_token(
        self,
        subject: str,
        claims: dict[str, Any],
        expires_delta: int | None = None,
    ) -> str:
        """Create a signed token string."""
        ...

    def decode_token(self, token: str) -> dict[str, Any] | None:
        """Decode and verify a token.  Return claims dict or None."""
        ...


# =============================================================================
# Built-in AES-GCM-256 EncryptionProvider (Rust-accelerated)
# =============================================================================


class AesGcmProvider:
    """Built-in AES-GCM-256 encryption provider using Rust acceleration.

    Supports per-connection session keys derived from ECDH key exchange.
    Wire format: 12-byte IV + ciphertext + 16-byte auth tag (base64-encoded).
    Compatible with frontend security.ts AES-GCM-256 implementation.
    """

    def __init__(self):
        self._session_keys: dict[str, bytes] = {}  # conn_id -> 32-byte AES key

    def set_session_key(self, conn_id: str, key: bytes) -> None:
        """Store a derived AES-256 session key for a connection."""
        self._session_keys[conn_id] = key

    def remove_session_key(self, conn_id: str) -> None:
        """Remove session key when connection closes."""
        self._session_keys.pop(conn_id, None)

    def has_session_key(self, conn_id: str) -> bool:
        """Check if a connection has a session key."""
        return conn_id in self._session_keys

    def encrypt(self, data: str, conn_id: str = "") -> str:
        """Encrypt using AES-GCM-256 with per-connection key."""
        key = self._session_keys.get(conn_id)
        if not key:
            raise ValueError(f"No session key for connection {conn_id}")
        encrypted = rust_aes_gcm_encrypt(key, data.encode("utf-8"))
        return base64.b64encode(encrypted).decode("ascii")

    def decrypt(self, encrypted_data: str, conn_id: str = "") -> str | None:
        """Decrypt using AES-GCM-256 with per-connection key."""
        key = self._session_keys.get(conn_id)
        if not key:
            return None
        try:
            raw = base64.b64decode(encrypted_data)
            decrypted = rust_aes_gcm_decrypt(key, raw)
            return decrypted.decode("utf-8")
        except Exception:
            return None

    @property
    def active_connections(self) -> int:
        """Number of connections with active session keys."""
        return len(self._session_keys)


# =============================================================================
# SecurityManager
# =============================================================================


class SecurityManager:
    """Handles WebSocket message encryption and signing.

    Works in three modes:
    1. **No providers** (default): uses built-in AesGcmProvider for encryption,
       HMAC-SHA256 signing with auto-generated secret.
    2. **EncryptionProvider set**: delegates encrypt/decrypt to the provider.
    3. **TokenProvider set**: delegates token creation/verification to the
       provider (e.g. JWT).

    ECDH key exchange:
      1. Server calls generate_keypair(conn_id) -> public key bytes
      2. Server sends public key in server_ready message
      3. Client sends its public key in client_hello
      4. Server calls derive_session_key(conn_id, peer_pk) -> derives AES-256 key
      5. Both sides now share the same key via HKDF

    Crypto hot paths (AES-GCM, ECDH, SHA-256, HMAC-SHA256) delegate to Rust.
    """

    def __init__(
        self,
        encryption_provider: EncryptionProvider | None = None,
        token_provider: TokenProvider | None = None,
    ):
        self.encryption_enabled = False
        self.message_signing_enabled = False
        self.cipher_suite: str | None = "AES-GCM-256"
        self.key_rotation_interval: int | None = 3600  # seconds
        self.last_key_rotation: float | None = None

        # Pluggable providers
        self._encryption_provider = encryption_provider
        self._token_provider = token_provider

        # Built-in AES-GCM provider (used when no custom provider is set)
        self._builtin_aes_gcm = AesGcmProvider()

        # Per-connection ECDH state
        self._conn_private_keys: dict[str, bytes] = {}  # conn_id -> private key
        self._conn_public_keys: dict[str, bytes] = {}  # conn_id -> our public key

        # Fallback HMAC secret (auto-generated, used when no TokenProvider)
        self._hmac_secret: bytes = secrets.token_bytes(32)

    def configure(
        self,
        encryption_provider: EncryptionProvider | None = None,
        token_provider: TokenProvider | None = None,
    ) -> None:
        """Hot-swap security providers at runtime."""
        if encryption_provider is not None:
            self._encryption_provider = encryption_provider
        if token_provider is not None:
            self._token_provider = token_provider

    async def initialize(self, config: dict[str, Any]) -> None:
        """Initialize security with configuration"""
        self.encryption_enabled = config.get("encryption_enabled", False)
        self.message_signing_enabled = config.get("message_signing_enabled", False)

        if self.encryption_enabled:
            provider = self._encryption_provider or self._builtin_aes_gcm
            log.info(f"Encryption initialized with {type(provider).__name__}")

        if self.message_signing_enabled:
            if self._token_provider:
                log.info("Message signing initialized with custom TokenProvider")
            else:
                log.info("Message signing initialized with built-in HMAC-SHA256")

    # -----------------------------------------------------------------
    # ECDH P-256 key exchange
    # -----------------------------------------------------------------

    def generate_keypair(self, conn_id: str) -> bytes:
        """Generate ECDH P-256 keypair for a connection.

        Returns the public key (65 bytes, uncompressed SEC1 point: 04 || X || Y).
        The private key is stored until derive_session_key() is called.
        """
        private_key, public_key = rust_ecdh_generate_keypair()
        self._conn_private_keys[conn_id] = bytes(private_key)
        self._conn_public_keys[conn_id] = bytes(public_key)
        return bytes(public_key)

    def derive_session_key(self, conn_id: str, peer_public_key: bytes) -> bool:
        """Derive AES-256 session key from ECDH + HKDF-SHA256.

        HKDF: salt="wse-encryption", info="aes-gcm-key" (matches frontend).

        Returns True on success. The derived key is stored in the active
        encryption provider (built-in or custom AesGcmProvider).
        """
        private_key = self._conn_private_keys.pop(conn_id, None)
        if not private_key:
            log.warning(f"No ECDH private key for connection {conn_id}")
            return False
        try:
            aes_key = bytes(rust_ecdh_derive_shared_secret(private_key, peer_public_key))

            # Store in the active provider
            provider = self._encryption_provider or self._builtin_aes_gcm
            if hasattr(provider, "set_session_key"):
                provider.set_session_key(conn_id, aes_key)
            log.debug(f"ECDH session key derived for {conn_id}")
            return True
        except Exception as e:
            log.error(f"ECDH key derivation failed for {conn_id}: {e}")
            return False

    def has_session_key(self, conn_id: str) -> bool:
        """Check if a connection has a derived session key."""
        provider = self._encryption_provider or self._builtin_aes_gcm
        if hasattr(provider, "has_session_key"):
            return provider.has_session_key(conn_id)
        return False

    def remove_connection(self, conn_id: str) -> None:
        """Clean up all keys for a disconnected connection."""
        self._conn_private_keys.pop(conn_id, None)
        self._conn_public_keys.pop(conn_id, None)
        provider = self._encryption_provider or self._builtin_aes_gcm
        if hasattr(provider, "remove_session_key"):
            provider.remove_session_key(conn_id)

    # -----------------------------------------------------------------
    # Encryption
    # -----------------------------------------------------------------

    async def encrypt_message(self, data: str | bytes | dict, conn_id: str = "") -> str | None:
        """Encrypt message using the active encryption provider.

        Returns base64-encoded ciphertext, or None if disabled/no key.
        """
        if not self.encryption_enabled:
            return None

        provider = self._encryption_provider or self._builtin_aes_gcm

        try:
            if isinstance(data, dict):
                data = json.dumps(data, sort_keys=True)
            elif isinstance(data, bytes):
                data = data.decode("utf-8")

            return provider.encrypt(data, conn_id=conn_id)

        except Exception as e:
            log.error(f"Encryption failed: {e}")
            return None

    async def decrypt_message(
        self, encrypted_data: str, conn_id: str = ""
    ) -> str | dict[str, Any] | None:
        """Decrypt message using the active encryption provider."""
        if not self.encryption_enabled:
            return None

        provider = self._encryption_provider or self._builtin_aes_gcm

        try:
            decrypted = provider.decrypt(encrypted_data, conn_id=conn_id)
            if decrypted:
                try:
                    return json.loads(decrypted)
                except json.JSONDecodeError:
                    return decrypted
            return None

        except Exception as e:
            log.error(f"Decryption failed: {e}")
            return None

    # -----------------------------------------------------------------
    # Message signing  (Rust-accelerated SHA-256 + HMAC-SHA256)
    # -----------------------------------------------------------------

    async def sign_message(self, data: str | bytes | dict) -> str | None:
        """Sign a message for integrity verification.

        Uses TokenProvider if available, otherwise falls back to built-in
        HMAC-SHA256 signing (Rust-accelerated).

        Returns:
            Signature string, or None if signing is disabled/failed.
        """
        if not self.message_signing_enabled:
            return None

        try:
            # Serialize payload to JSON string for hashing
            if isinstance(data, dict):
                payload_str = json.dumps(data, sort_keys=True, default=str)
            elif isinstance(data, bytes):
                payload_str = data.decode("utf-8")
            else:
                payload_str = str(data)

            # Rust-accelerated SHA-256
            payload_hash = rust_sha256(payload_str.encode())

            # --- TokenProvider path ---
            if self._token_provider:
                token = self._token_provider.create_token(
                    subject="ws_message",
                    claims={
                        "hash": payload_hash,
                        "signed_at": time.time(),
                        "nonce": secrets.token_hex(8),
                    },
                )
                return token

            # --- Built-in HMAC-SHA256 fallback (Rust-accelerated) ---
            nonce = secrets.token_hex(8)
            signed_at = str(time.time())
            message_bytes = f"{payload_hash}:{signed_at}:{nonce}".encode()
            # Rust HMAC returns raw bytes; convert to hex
            signature = rust_hmac_sha256(self._hmac_secret, message_bytes).hex()
            # Encode as compact string: hash:ts:nonce:sig
            return f"{payload_hash}:{signed_at}:{nonce}:{signature}"

        except Exception as e:
            log.error(f"Message signing failed: {e}")
            return None

    async def verify_signature(self, signed_data: dict[str, Any]) -> dict[str, Any] | None:
        """Verify message signature.

        Supports both TokenProvider-issued tokens and built-in HMAC strings.
        """
        if not self.message_signing_enabled:
            return signed_data.get("payload", signed_data)

        try:
            signature = signed_data.get("signature") or signed_data.get("sig")
            payload = signed_data.get("payload") or signed_data.get("p")
            if not signature:
                log.warning("No signature found in message")
                return None

            # --- TokenProvider path ---
            if self._token_provider:
                decoded = self._token_provider.decode_token(signature)
                if not decoded:
                    log.warning("Invalid token signature")
                    return None

                # Verify payload hash
                expected_hash = decoded.get("hash")
                if expected_hash and payload is not None:
                    if isinstance(payload, dict):
                        payload_str = json.dumps(payload, sort_keys=True, default=str)
                    elif isinstance(payload, bytes):
                        payload_str = payload.decode("utf-8")
                    else:
                        payload_str = str(payload)
                    actual_hash = rust_sha256(payload_str.encode())
                    if expected_hash != actual_hash:
                        log.warning("Signature hash mismatch -- payload may have been tampered")
                        return None
                return decoded

            # --- Built-in HMAC verification (Rust-accelerated) ---
            parts = signature.split(":")
            if len(parts) != 4:
                log.warning("Invalid HMAC signature format")
                return None

            recv_hash, signed_at, nonce, recv_sig = parts
            message_bytes = f"{recv_hash}:{signed_at}:{nonce}".encode()
            expected_sig = rust_hmac_sha256(self._hmac_secret, message_bytes).hex()
            if recv_sig != expected_sig:
                log.warning("HMAC signature mismatch")
                return None

            # Verify payload hash if payload is provided
            if payload is not None:
                if isinstance(payload, dict):
                    payload_str = json.dumps(payload, sort_keys=True, default=str)
                elif isinstance(payload, bytes):
                    payload_str = payload.decode("utf-8")
                else:
                    payload_str = str(payload)
                actual_hash = rust_sha256(payload_str.encode())
                if recv_hash != actual_hash:
                    log.warning("Payload hash mismatch")
                    return None

            return {"hash": recv_hash, "signed_at": float(signed_at), "nonce": nonce}

        except Exception as e:
            log.error(f"Signature verification failed: {e}")
            return None

    # -----------------------------------------------------------------
    # Session tokens
    # -----------------------------------------------------------------

    async def create_session_token(self, user_id: str, conn_id: str) -> str:
        """Create a session token for WebSocket connection.

        Uses TokenProvider if available, otherwise generates an HMAC-signed
        opaque token (Rust-accelerated).
        """
        if self._token_provider:
            return self._token_provider.create_token(
                subject=user_id,
                claims={
                    "conn_id": conn_id,
                    "type": "ws_session",
                    "nonce": secrets.token_hex(8),
                },
                expires_delta=86400,  # 24 hours
            )

        # Built-in fallback: HMAC-signed token (Rust-accelerated)
        issued_at = str(time.time())
        nonce = secrets.token_hex(8)
        message = f"{user_id}:{conn_id}:{issued_at}:{nonce}".encode()
        sig = rust_hmac_sha256(self._hmac_secret, message).hex()
        return f"{user_id}:{conn_id}:{issued_at}:{nonce}:{sig}"

    async def verify_session_token(self, token: str) -> dict[str, Any] | None:
        """Verify and decode session token."""
        if self._token_provider:
            decoded = self._token_provider.decode_token(token)
            if decoded:
                return {
                    "user_id": decoded.get("sub", ""),
                    "conn_id": decoded.get("conn_id", ""),
                    "timestamp": decoded.get("iat", 0),
                    "nonce": decoded.get("nonce", ""),
                }
            return None

        # Built-in HMAC verification (Rust-accelerated)
        try:
            parts = token.split(":")
            if len(parts) != 5:
                return None
            user_id, conn_id, issued_at, nonce, recv_sig = parts

            message = f"{user_id}:{conn_id}:{issued_at}:{nonce}".encode()
            expected_sig = rust_hmac_sha256(self._hmac_secret, message).hex()
            if recv_sig != expected_sig:
                return None

            # Check expiry (24 hours)
            if time.time() - float(issued_at) > 86400:
                log.debug("Session token expired")
                return None

            return {
                "user_id": user_id,
                "conn_id": conn_id,
                "timestamp": float(issued_at),
                "nonce": nonce,
            }
        except Exception as e:
            log.error(f"Token verification failed: {e}")
            return None

    # -----------------------------------------------------------------
    # Key rotation & utilities
    # -----------------------------------------------------------------

    async def rotate_keys(self) -> None:
        """Rotate signing keys.

        For built-in HMAC: regenerates the secret.
        For providers: delegates to the provider if supported.
        """
        self._hmac_secret = secrets.token_bytes(32)
        self.last_key_rotation = time.time()
        log.info("Security keys rotated")

    def get_security_info(self) -> dict[str, Any]:
        """Get security configuration info"""
        provider = self._encryption_provider or self._builtin_aes_gcm
        active = 0
        if hasattr(provider, "active_connections"):
            active = provider.active_connections
        return {
            "encryption_enabled": self.encryption_enabled,
            "encryption_algorithm": self.cipher_suite if self.encryption_enabled else None,
            "encryption_provider": type(provider).__name__,
            "token_provider": type(self._token_provider).__name__
            if self._token_provider
            else "HMAC-SHA256",
            "message_signing_enabled": self.message_signing_enabled,
            "session_key_rotation": self.key_rotation_interval,
            "last_key_rotation": self.last_key_rotation,
            "active_encrypted_connections": active,
        }
