"""Tests for SecurityManager, AesGcmProvider, and ECDH key exchange."""

import base64
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server._accel import (
    rust_aes_gcm_decrypt,
    rust_aes_gcm_encrypt,
    rust_ecdh_derive_shared_secret,
    rust_ecdh_generate_keypair,
    rust_hmac_sha256,
    rust_jwt_decode,
    rust_jwt_encode,
    rust_sha256,
)
from wse_server.connection.security import (
    AesGcmProvider,
    EncryptionProvider,
    SecurityManager,
    TokenProvider,
)

# =========================================================================
# Stub providers for testing
# =========================================================================


class StubEncryptionProvider:
    """Simple reversible encryption for testing (base64)."""

    def encrypt(self, data: str, conn_id: str = "") -> str:
        return base64.b64encode(data.encode("utf-8")).decode("utf-8")

    def decrypt(self, encrypted_data: str, conn_id: str = "") -> str:
        return base64.b64decode(encrypted_data.encode("utf-8")).decode("utf-8")


class StubTokenProvider:
    """In-memory token store for testing (no real JWT)."""

    def __init__(self):
        self._tokens = {}

    def create_token(self, subject: str, claims: dict, expires_delta=None) -> str:
        token = f"tok_{subject}_{len(self._tokens)}"
        self._tokens[token] = {"sub": subject, **claims}
        return token

    def decode_token(self, token: str):
        return self._tokens.get(token)


class FailingEncryptionProvider:
    """Provider that always raises."""

    def encrypt(self, data: str, conn_id: str = "") -> str:
        raise RuntimeError("encryption failed")

    def decrypt(self, encrypted_data: str, conn_id: str = ""):
        raise RuntimeError("decryption failed")


# =========================================================================
# Protocol compliance
# =========================================================================


class TestProtocols:
    def test_stub_encryption_provider_satisfies_protocol(self):
        provider = StubEncryptionProvider()
        assert isinstance(provider, EncryptionProvider)

    def test_stub_token_provider_satisfies_protocol(self):
        provider = StubTokenProvider()
        assert isinstance(provider, TokenProvider)

    def test_aes_gcm_provider_satisfies_protocol(self):
        provider = AesGcmProvider()
        assert isinstance(provider, EncryptionProvider)


# =========================================================================
# Rust-level crypto functions
# =========================================================================


class TestRustCrypto:
    def test_sha256_basic(self):
        digest = rust_sha256(b"hello")
        assert isinstance(digest, str)
        assert len(digest) == 64  # hex-encoded SHA-256

    def test_sha256_deterministic(self):
        d1 = rust_sha256(b"test data")
        d2 = rust_sha256(b"test data")
        assert d1 == d2

    def test_sha256_different_input_different_hash(self):
        d1 = rust_sha256(b"hello")
        d2 = rust_sha256(b"world")
        assert d1 != d2

    def test_hmac_sha256_basic(self):
        mac = rust_hmac_sha256(b"secret", b"message")
        assert isinstance(mac, bytes)
        assert len(mac) == 32  # HMAC-SHA256 always 32 bytes

    def test_hmac_sha256_deterministic(self):
        m1 = rust_hmac_sha256(b"key", b"data")
        m2 = rust_hmac_sha256(b"key", b"data")
        assert m1 == m2

    def test_hmac_sha256_different_key_different_mac(self):
        m1 = rust_hmac_sha256(b"key1", b"data")
        m2 = rust_hmac_sha256(b"key2", b"data")
        assert m1 != m2

    def test_aes_gcm_encrypt_returns_bytes(self):
        key = os.urandom(32)
        result = rust_aes_gcm_encrypt(key, b"hello")
        assert isinstance(result, bytes)
        # 12 (IV) + 5 (plaintext) + 16 (tag) = 33
        assert len(result) == 33

    def test_aes_gcm_encrypt_decrypt_roundtrip(self):
        key = os.urandom(32)
        plaintext = b"The quick brown fox jumps over the lazy dog"
        encrypted = rust_aes_gcm_encrypt(key, plaintext)
        decrypted = rust_aes_gcm_decrypt(key, encrypted)
        assert decrypted == plaintext

    def test_aes_gcm_different_nonce_each_call(self):
        key = os.urandom(32)
        e1 = rust_aes_gcm_encrypt(key, b"same")
        e2 = rust_aes_gcm_encrypt(key, b"same")
        # IV (first 12 bytes) should differ
        assert e1[:12] != e2[:12]
        # But both decrypt to the same plaintext
        assert rust_aes_gcm_decrypt(key, e1) == rust_aes_gcm_decrypt(key, e2)

    def test_aes_gcm_wrong_key_fails(self):
        key1 = os.urandom(32)
        key2 = os.urandom(32)
        encrypted = rust_aes_gcm_encrypt(key1, b"secret")
        with pytest.raises(RuntimeError, match="Decrypt failed"):
            rust_aes_gcm_decrypt(key2, encrypted)

    def test_aes_gcm_tampered_ciphertext_fails(self):
        key = os.urandom(32)
        encrypted = bytearray(rust_aes_gcm_encrypt(key, b"secret"))
        # Flip a byte in the ciphertext
        encrypted[15] ^= 0xFF
        with pytest.raises(RuntimeError, match="Decrypt failed"):
            rust_aes_gcm_decrypt(key, bytes(encrypted))

    def test_aes_gcm_invalid_key_length(self):
        with pytest.raises(ValueError, match="32 bytes"):
            rust_aes_gcm_encrypt(b"short", b"data")

    def test_aes_gcm_data_too_short(self):
        key = os.urandom(32)
        with pytest.raises(ValueError, match="too short"):
            rust_aes_gcm_decrypt(key, b"short")

    def test_aes_gcm_empty_plaintext(self):
        key = os.urandom(32)
        encrypted = rust_aes_gcm_encrypt(key, b"")
        # 12 (IV) + 0 (plaintext) + 16 (tag) = 28
        assert len(encrypted) == 28
        decrypted = rust_aes_gcm_decrypt(key, encrypted)
        assert decrypted == b""


class TestRustECDH:
    def test_generate_keypair_sizes(self):
        private_key, public_key = rust_ecdh_generate_keypair()
        assert isinstance(private_key, bytes)
        assert isinstance(public_key, bytes)
        assert len(private_key) == 32  # P-256 scalar
        assert len(public_key) == 65  # uncompressed SEC1 point

    def test_public_key_uncompressed_prefix(self):
        _, public_key = rust_ecdh_generate_keypair()
        assert public_key[0] == 0x04  # uncompressed point indicator

    def test_keypairs_are_unique(self):
        sk1, pk1 = rust_ecdh_generate_keypair()
        sk2, pk2 = rust_ecdh_generate_keypair()
        assert sk1 != sk2
        assert pk1 != pk2

    def test_ecdh_shared_secret_symmetric(self):
        """Both sides derive the same AES key from cross-exchange."""
        sk_a, pk_a = rust_ecdh_generate_keypair()
        sk_b, pk_b = rust_ecdh_generate_keypair()

        key_ab = rust_ecdh_derive_shared_secret(sk_a, pk_b)
        key_ba = rust_ecdh_derive_shared_secret(sk_b, pk_a)

        assert key_ab == key_ba
        assert len(key_ab) == 32  # AES-256 key

    def test_ecdh_different_peers_different_keys(self):
        sk_a, pk_a = rust_ecdh_generate_keypair()
        _, pk_b = rust_ecdh_generate_keypair()
        _, pk_c = rust_ecdh_generate_keypair()

        key_ab = rust_ecdh_derive_shared_secret(sk_a, pk_b)
        key_ac = rust_ecdh_derive_shared_secret(sk_a, pk_c)
        assert key_ab != key_ac

    def test_ecdh_invalid_private_key(self):
        _, pk = rust_ecdh_generate_keypair()
        with pytest.raises(ValueError, match="Invalid private key"):
            rust_ecdh_derive_shared_secret(b"bad", pk)

    def test_ecdh_invalid_public_key(self):
        sk, _ = rust_ecdh_generate_keypair()
        with pytest.raises(ValueError, match="Invalid peer public key"):
            rust_ecdh_derive_shared_secret(sk, b"bad")

    def test_ecdh_then_aes_gcm_roundtrip(self):
        """Full key exchange -> encrypt -> decrypt flow at Rust level."""
        # Server generates keypair
        sk_server, pk_server = rust_ecdh_generate_keypair()
        # Client generates keypair
        sk_client, pk_client = rust_ecdh_generate_keypair()
        # Both derive the same AES key
        server_key = rust_ecdh_derive_shared_secret(sk_server, pk_client)
        client_key = rust_ecdh_derive_shared_secret(sk_client, pk_server)
        assert server_key == client_key

        # Server encrypts, client decrypts
        plaintext = b'U{"t":"order_placed","p":{"symbol":"AAPL","qty":100}}'
        encrypted = rust_aes_gcm_encrypt(server_key, plaintext)
        decrypted = rust_aes_gcm_decrypt(client_key, encrypted)
        assert decrypted == plaintext


# =========================================================================
# AesGcmProvider
# =========================================================================


class TestAesGcmProvider:
    def test_set_and_check_session_key(self):
        provider = AesGcmProvider()
        assert provider.has_session_key("conn1") is False
        provider.set_session_key("conn1", os.urandom(32))
        assert provider.has_session_key("conn1") is True

    def test_remove_session_key(self):
        provider = AesGcmProvider()
        provider.set_session_key("conn1", os.urandom(32))
        provider.remove_session_key("conn1")
        assert provider.has_session_key("conn1") is False

    def test_remove_nonexistent_key_no_error(self):
        provider = AesGcmProvider()
        provider.remove_session_key("ghost")  # should not raise

    def test_encrypt_decrypt_roundtrip(self):
        provider = AesGcmProvider()
        key = os.urandom(32)
        provider.set_session_key("c1", key)

        encrypted = provider.encrypt("hello world", conn_id="c1")
        assert isinstance(encrypted, str)
        assert encrypted != "hello world"

        decrypted = provider.decrypt(encrypted, conn_id="c1")
        assert decrypted == "hello world"

    def test_encrypt_no_key_raises(self):
        provider = AesGcmProvider()
        with pytest.raises(ValueError, match="No session key"):
            provider.encrypt("data", conn_id="missing")

    def test_decrypt_no_key_returns_none(self):
        provider = AesGcmProvider()
        result = provider.decrypt("some_base64_data", conn_id="missing")
        assert result is None

    def test_decrypt_invalid_data_returns_none(self):
        provider = AesGcmProvider()
        provider.set_session_key("c1", os.urandom(32))
        result = provider.decrypt("not_valid_base64!@#", conn_id="c1")
        assert result is None

    def test_active_connections_count(self):
        provider = AesGcmProvider()
        assert provider.active_connections == 0
        provider.set_session_key("c1", os.urandom(32))
        provider.set_session_key("c2", os.urandom(32))
        assert provider.active_connections == 2
        provider.remove_session_key("c1")
        assert provider.active_connections == 1

    def test_per_connection_isolation(self):
        """Different connections use different keys, can't cross-decrypt."""
        provider = AesGcmProvider()
        key1 = os.urandom(32)
        key2 = os.urandom(32)
        provider.set_session_key("c1", key1)
        provider.set_session_key("c2", key2)

        encrypted = provider.encrypt("secret for c1", conn_id="c1")
        # Decrypt with c2's key should fail
        result = provider.decrypt(encrypted, conn_id="c2")
        assert result is None

    def test_unicode_data(self):
        provider = AesGcmProvider()
        provider.set_session_key("c1", os.urandom(32))
        original = "Hello, World!"
        encrypted = provider.encrypt(original, conn_id="c1")
        decrypted = provider.decrypt(encrypted, conn_id="c1")
        assert decrypted == original


# =========================================================================
# Default SecurityManager (no providers)
# =========================================================================


class TestDefaultSecurityManager:
    def test_initial_state(self, security_manager):
        assert security_manager.encryption_enabled is False
        assert security_manager.message_signing_enabled is False

    @pytest.mark.asyncio
    async def test_signing_disabled_returns_none(self, security_manager):
        result = await security_manager.sign_message({"data": "test"})
        assert result is None

    @pytest.mark.asyncio
    async def test_encrypt_disabled_returns_none(self, security_manager):
        result = await security_manager.encrypt_message("hello")
        assert result is None

    @pytest.mark.asyncio
    async def test_decrypt_disabled_returns_none(self, security_manager):
        result = await security_manager.decrypt_message("encrypted")
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_signature_disabled_returns_payload(self, security_manager):
        msg = {"payload": {"key": "value"}, "signature": "abc"}
        result = await security_manager.verify_signature(msg)
        # When signing is disabled, verify_signature returns payload directly
        assert result == {"key": "value"}

    def test_security_info(self, security_manager):
        info = security_manager.get_security_info()
        assert info["encryption_enabled"] is False
        assert info["message_signing_enabled"] is False
        # Default uses built-in AesGcmProvider
        assert info["encryption_provider"] == "AesGcmProvider"
        assert info["token_provider"] == "HMAC-SHA256"


# =========================================================================
# Message signing (HMAC fallback)
# =========================================================================


class TestHMACSigning:
    @pytest.mark.asyncio
    async def test_sign_message_dict(self, signing_security_manager):
        sig = await signing_security_manager.sign_message({"key": "value"})
        assert sig is not None
        # HMAC format: hash:timestamp:nonce:signature
        parts = sig.split(":")
        assert len(parts) == 4

    @pytest.mark.asyncio
    async def test_sign_message_string(self, signing_security_manager):
        sig = await signing_security_manager.sign_message("hello world")
        assert sig is not None
        parts = sig.split(":")
        assert len(parts) == 4

    @pytest.mark.asyncio
    async def test_sign_message_bytes(self, signing_security_manager):
        sig = await signing_security_manager.sign_message(b"binary data")
        assert sig is not None
        parts = sig.split(":")
        assert len(parts) == 4

    @pytest.mark.asyncio
    async def test_sign_then_verify_roundtrip(self, signing_security_manager):
        payload = {"symbol": "AAPL", "price": 150.25}
        sig = await signing_security_manager.sign_message(payload)
        assert sig is not None

        signed_msg = {"payload": payload, "signature": sig}
        result = await signing_security_manager.verify_signature(signed_msg)
        assert result is not None
        assert "hash" in result
        assert "signed_at" in result
        assert "nonce" in result

    @pytest.mark.asyncio
    async def test_verify_invalid_signature_rejected(self, signing_security_manager):
        signed_msg = {
            "payload": {"data": "test"},
            "signature": "bad:bad:bad:bad",
        }
        result = await signing_security_manager.verify_signature(signed_msg)
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_no_signature_returns_none(self, signing_security_manager):
        msg = {"payload": {"data": "test"}}
        result = await signing_security_manager.verify_signature(msg)
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_wrong_format_returns_none(self, signing_security_manager):
        signed_msg = {
            "payload": {"data": "test"},
            "signature": "too_few_parts",
        }
        result = await signing_security_manager.verify_signature(signed_msg)
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_tampered_payload_rejected(self, signing_security_manager):
        payload = {"original": True}
        sig = await signing_security_manager.sign_message(payload)

        # Tamper with payload
        signed_msg = {
            "payload": {"original": False, "tampered": True},
            "signature": sig,
        }
        result = await signing_security_manager.verify_signature(signed_msg)
        assert result is None

    @pytest.mark.asyncio
    async def test_different_messages_different_signatures(self, signing_security_manager):
        sig1 = await signing_security_manager.sign_message({"a": 1})
        sig2 = await signing_security_manager.sign_message({"b": 2})
        assert sig1 != sig2


# =========================================================================
# Encryption with provider
# =========================================================================


class TestEncryptionWithProvider:
    @pytest.mark.asyncio
    async def test_encrypt_decrypt_roundtrip_string(self):
        sm = SecurityManager(encryption_provider=StubEncryptionProvider())
        await sm.initialize({"encryption_enabled": True})

        encrypted = await sm.encrypt_message("hello world")
        assert encrypted is not None
        assert encrypted != "hello world"

        decrypted = await sm.decrypt_message(encrypted)
        assert decrypted == "hello world"

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_roundtrip_dict(self):
        sm = SecurityManager(encryption_provider=StubEncryptionProvider())
        await sm.initialize({"encryption_enabled": True})

        original = {"t": "test", "p": {"symbol": "AAPL"}}
        encrypted = await sm.encrypt_message(original)
        assert encrypted is not None

        decrypted = await sm.decrypt_message(encrypted)
        # Dict gets serialized to sorted JSON, then encrypted, then decrypted
        # and parsed back to dict
        assert isinstance(decrypted, dict)
        assert decrypted["t"] == "test"

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_bytes(self):
        sm = SecurityManager(encryption_provider=StubEncryptionProvider())
        await sm.initialize({"encryption_enabled": True})

        encrypted = await sm.encrypt_message(b"binary")
        assert encrypted is not None
        decrypted = await sm.decrypt_message(encrypted)
        assert decrypted == "binary"

    @pytest.mark.asyncio
    async def test_encrypt_failure_returns_none(self):
        sm = SecurityManager(encryption_provider=FailingEncryptionProvider())
        await sm.initialize({"encryption_enabled": True})

        result = await sm.encrypt_message("hello")
        assert result is None

    @pytest.mark.asyncio
    async def test_decrypt_failure_returns_none(self):
        sm = SecurityManager(encryption_provider=FailingEncryptionProvider())
        await sm.initialize({"encryption_enabled": True})

        result = await sm.decrypt_message("some data")
        assert result is None


# =========================================================================
# Signing with TokenProvider
# =========================================================================


class TestSigningWithTokenProvider:
    @pytest.mark.asyncio
    async def test_sign_with_token_provider(self):
        tp = StubTokenProvider()
        sm = SecurityManager(token_provider=tp)
        await sm.initialize({"message_signing_enabled": True})

        sig = await sm.sign_message({"key": "value"})
        assert sig is not None
        assert sig.startswith("tok_ws_message_")

    @pytest.mark.asyncio
    async def test_verify_with_token_provider(self):
        tp = StubTokenProvider()
        sm = SecurityManager(token_provider=tp)
        await sm.initialize({"message_signing_enabled": True})

        payload = {"test": "data"}
        sig = await sm.sign_message(payload)

        result = await sm.verify_signature({"payload": payload, "signature": sig})
        assert result is not None
        assert "hash" in result

    @pytest.mark.asyncio
    async def test_verify_invalid_token_returns_none(self):
        tp = StubTokenProvider()
        sm = SecurityManager(token_provider=tp)
        await sm.initialize({"message_signing_enabled": True})

        result = await sm.verify_signature(
            {"payload": {"data": "test"}, "signature": "invalid_token"}
        )
        assert result is None


# =========================================================================
# Session tokens
# =========================================================================


class TestSessionTokens:
    @pytest.mark.asyncio
    async def test_create_session_token_hmac(self, security_manager):
        token = await security_manager.create_session_token("user123", "conn_abc")
        assert token is not None
        # HMAC format: user_id:conn_id:issued_at:nonce:sig
        parts = token.split(":")
        assert len(parts) == 5
        assert parts[0] == "user123"
        assert parts[1] == "conn_abc"

    @pytest.mark.asyncio
    async def test_verify_session_token_hmac(self, security_manager):
        token = await security_manager.create_session_token("user123", "conn_abc")
        result = await security_manager.verify_session_token(token)

        assert result is not None
        assert result["user_id"] == "user123"
        assert result["conn_id"] == "conn_abc"
        assert "timestamp" in result
        assert "nonce" in result

    @pytest.mark.asyncio
    async def test_verify_tampered_session_token_fails(self, security_manager):
        token = await security_manager.create_session_token("user123", "conn_abc")
        # Tamper with user_id
        parts = token.split(":")
        parts[0] = "hacker"
        tampered = ":".join(parts)

        result = await security_manager.verify_session_token(tampered)
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_invalid_format_session_token(self, security_manager):
        result = await security_manager.verify_session_token("not:enough:parts")
        assert result is None

    @pytest.mark.asyncio
    async def test_verify_completely_invalid_token(self, security_manager):
        result = await security_manager.verify_session_token("garbage")
        assert result is None

    @pytest.mark.asyncio
    async def test_session_token_with_token_provider(self):
        tp = StubTokenProvider()
        sm = SecurityManager(token_provider=tp)

        token = await sm.create_session_token("user456", "conn_xyz")
        assert token.startswith("tok_user456_")

        result = await sm.verify_session_token(token)
        assert result is not None
        assert result["user_id"] == "user456"
        assert result["conn_id"] == "conn_xyz"

    @pytest.mark.asyncio
    async def test_session_token_with_invalid_token_provider(self):
        tp = StubTokenProvider()
        sm = SecurityManager(token_provider=tp)

        result = await sm.verify_session_token("nonexistent_token")
        assert result is None


# =========================================================================
# Key rotation
# =========================================================================


class TestKeyRotation:
    @pytest.mark.asyncio
    async def test_rotate_keys(self, security_manager):
        old_secret = security_manager._hmac_secret
        await security_manager.rotate_keys()
        assert security_manager._hmac_secret != old_secret
        assert security_manager.last_key_rotation is not None

    @pytest.mark.asyncio
    async def test_old_token_invalid_after_rotation(self, security_manager):
        token = await security_manager.create_session_token("user", "conn")
        await security_manager.rotate_keys()
        result = await security_manager.verify_session_token(token)
        assert result is None  # old HMAC no longer valid

    @pytest.mark.asyncio
    async def test_old_signature_invalid_after_rotation(self):
        sm = SecurityManager()
        await sm.initialize({"message_signing_enabled": True})

        sig = await sm.sign_message({"data": "test"})
        await sm.rotate_keys()

        result = await sm.verify_signature({"payload": {"data": "test"}, "signature": sig})
        assert result is None


# =========================================================================
# Configure (hot-swap)
# =========================================================================


class TestConfigure:
    def test_configure_encryption_provider(self, security_manager):
        provider = StubEncryptionProvider()
        security_manager.configure(encryption_provider=provider)
        assert security_manager._encryption_provider is provider

    def test_configure_token_provider(self, security_manager):
        provider = StubTokenProvider()
        security_manager.configure(token_provider=provider)
        assert security_manager._token_provider is provider

    def test_configure_preserves_existing(self, security_manager):
        ep = StubEncryptionProvider()
        tp = StubTokenProvider()
        security_manager.configure(encryption_provider=ep)
        security_manager.configure(token_provider=tp)
        # Both should be set
        assert security_manager._encryption_provider is ep
        assert security_manager._token_provider is tp


# =========================================================================
# Utility methods
# =========================================================================


class TestUtilities:
    @pytest.mark.asyncio
    async def test_initialize_sets_flags(self):
        sm = SecurityManager()
        await sm.initialize(
            {
                "encryption_enabled": True,
                "message_signing_enabled": True,
            }
        )
        assert sm.encryption_enabled is True
        assert sm.message_signing_enabled is True

    @pytest.mark.asyncio
    async def test_initialize_defaults(self):
        sm = SecurityManager()
        await sm.initialize({})
        assert sm.encryption_enabled is False
        assert sm.message_signing_enabled is False

    def test_security_info_with_providers(self):
        sm = SecurityManager(
            encryption_provider=StubEncryptionProvider(),
            token_provider=StubTokenProvider(),
        )
        info = sm.get_security_info()
        assert info["encryption_provider"] == "StubEncryptionProvider"
        assert info["token_provider"] == "StubTokenProvider"

    def test_security_info_default(self):
        sm = SecurityManager()
        info = sm.get_security_info()
        assert info["encryption_provider"] == "AesGcmProvider"
        assert info["token_provider"] == "HMAC-SHA256"
        assert info["active_encrypted_connections"] == 0


# =========================================================================
# ECDH key exchange via SecurityManager
# =========================================================================


class TestSecurityManagerECDH:
    def test_generate_keypair_returns_public_key(self):
        sm = SecurityManager()
        pk = sm.generate_keypair("conn1")
        assert isinstance(pk, bytes)
        assert len(pk) == 65
        assert pk[0] == 0x04

    def test_generate_keypair_stores_private_key(self):
        sm = SecurityManager()
        sm.generate_keypair("conn1")
        assert "conn1" in sm._conn_private_keys
        assert "conn1" in sm._conn_public_keys

    def test_derive_session_key_success(self):
        sm = SecurityManager()
        # Server generates keypair
        sm.generate_keypair("conn1")
        # Client generates keypair (simulated)
        client_sk, client_pk = rust_ecdh_generate_keypair()
        # Server derives session key from client's public key
        result = sm.derive_session_key("conn1", bytes(client_pk))
        assert result is True
        assert sm.has_session_key("conn1")
        # Private key consumed after derivation
        assert "conn1" not in sm._conn_private_keys

    def test_derive_session_key_no_private_key(self):
        sm = SecurityManager()
        _, pk = rust_ecdh_generate_keypair()
        result = sm.derive_session_key("nonexistent", bytes(pk))
        assert result is False

    def test_derive_session_key_invalid_peer_pk(self):
        sm = SecurityManager()
        sm.generate_keypair("conn1")
        result = sm.derive_session_key("conn1", b"invalid")
        assert result is False

    def test_has_session_key_false_before_derivation(self):
        sm = SecurityManager()
        assert sm.has_session_key("conn1") is False

    def test_remove_connection_cleans_all_state(self):
        sm = SecurityManager()
        sm.generate_keypair("conn1")
        client_sk, client_pk = rust_ecdh_generate_keypair()
        sm.derive_session_key("conn1", bytes(client_pk))
        assert sm.has_session_key("conn1")

        sm.remove_connection("conn1")
        assert sm.has_session_key("conn1") is False
        assert "conn1" not in sm._conn_private_keys
        assert "conn1" not in sm._conn_public_keys

    def test_remove_nonexistent_connection_no_error(self):
        sm = SecurityManager()
        sm.remove_connection("ghost")  # should not raise


# =========================================================================
# E2E encryption via SecurityManager (ECDH + AES-GCM)
# =========================================================================


class TestE2EEncryption:
    @pytest.mark.asyncio
    async def test_full_handshake_encrypt_decrypt(self):
        """Simulate server_ready -> client_hello -> encrypt -> decrypt."""
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})

        # 1. Server generates ECDH keypair (server_ready)
        server_pk = sm.generate_keypair("conn1")

        # 2. Client generates ECDH keypair (client_hello)
        client_sk, client_pk = rust_ecdh_generate_keypair()

        # 3. Server derives session key from client's public key
        assert sm.derive_session_key("conn1", bytes(client_pk)) is True

        # 4. Server encrypts a message
        original = {"t": "order_placed", "p": {"symbol": "AAPL", "qty": 100}}
        encrypted = await sm.encrypt_message(original, conn_id="conn1")
        assert encrypted is not None

        # 5. Client decrypts (using Rust directly, simulating frontend)
        client_aes_key = rust_ecdh_derive_shared_secret(client_sk, server_pk)
        raw = base64.b64decode(encrypted)
        decrypted_bytes = rust_aes_gcm_decrypt(bytes(client_aes_key), raw)
        import json

        decrypted = json.loads(decrypted_bytes.decode("utf-8"))
        assert decrypted["t"] == "order_placed"
        assert decrypted["p"]["symbol"] == "AAPL"

    @pytest.mark.asyncio
    async def test_client_to_server_decrypt(self):
        """Client encrypts, server decrypts."""
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})

        # Handshake
        server_pk = sm.generate_keypair("conn1")
        client_sk, client_pk = rust_ecdh_generate_keypair()
        sm.derive_session_key("conn1", bytes(client_pk))

        # Client encrypts (using Rust directly)
        client_aes_key = rust_ecdh_derive_shared_secret(client_sk, server_pk)
        plaintext = b'{"t":"PONG","p":{"client_timestamp":1234567890}}'
        encrypted_raw = rust_aes_gcm_encrypt(bytes(client_aes_key), plaintext)
        encrypted_b64 = base64.b64encode(encrypted_raw).decode("ascii")

        # Server decrypts
        decrypted = await sm.decrypt_message(encrypted_b64, conn_id="conn1")
        assert isinstance(decrypted, dict)
        assert decrypted["t"] == "PONG"

    @pytest.mark.asyncio
    async def test_encrypt_without_session_key_returns_none(self):
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})
        # No ECDH handshake done
        result = await sm.encrypt_message("hello", conn_id="conn1")
        assert result is None

    @pytest.mark.asyncio
    async def test_multiple_connections_isolated(self):
        """Two connections with different keys cannot cross-decrypt."""
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})

        # Connection 1 handshake
        sm.generate_keypair("c1")
        c1_sk, c1_pk = rust_ecdh_generate_keypair()
        sm.derive_session_key("c1", bytes(c1_pk))

        # Connection 2 handshake
        sm.generate_keypair("c2")
        c2_sk, c2_pk = rust_ecdh_generate_keypair()
        sm.derive_session_key("c2", bytes(c2_pk))

        # Encrypt for c1
        encrypted = await sm.encrypt_message("for c1 only", conn_id="c1")
        assert encrypted is not None

        # Decrypt with c2 should fail (different key)
        decrypted = await sm.decrypt_message(encrypted, conn_id="c2")
        assert decrypted is None

    @pytest.mark.asyncio
    async def test_security_info_tracks_connections(self):
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})

        sm.generate_keypair("c1")
        c1_sk, c1_pk = rust_ecdh_generate_keypair()
        sm.derive_session_key("c1", bytes(c1_pk))

        info = sm.get_security_info()
        assert info["active_encrypted_connections"] == 1
        assert info["encryption_algorithm"] == "AES-GCM-256"

        sm.remove_connection("c1")
        info = sm.get_security_info()
        assert info["active_encrypted_connections"] == 0


# =========================================================================
# Rust JWT encode/decode
# =========================================================================


class TestRustJWT:
    """Tests for Rust-accelerated JWT encode and decode (HS256)."""

    def test_encode_returns_string(self):
        token = rust_jwt_encode({"sub": "user1", "exp": 9999999999}, b"secret")
        assert isinstance(token, str)
        # JWT has 3 dot-separated segments
        assert token.count(".") == 2

    def test_encode_decode_roundtrip(self):
        claims = {"sub": "user123", "exp": 9999999999, "iss": "wse", "aud": "wse-api"}
        secret = b"my-secret-key-for-testing"
        token = rust_jwt_encode(claims, secret)
        decoded = rust_jwt_decode(token, secret, issuer="wse", audience="wse-api")
        assert decoded is not None
        assert decoded["sub"] == "user123"
        assert decoded["iss"] == "wse"
        assert decoded["aud"] == "wse-api"

    def test_decode_bad_secret_returns_none(self):
        claims = {"sub": "user1", "exp": 9999999999}
        token = rust_jwt_encode(claims, b"correct-secret")
        result = rust_jwt_decode(token, b"wrong-secret")
        assert result is None

    def test_decode_expired_token_returns_none(self):
        claims = {"sub": "user1", "exp": 1000000000}  # expired in 2001
        token = rust_jwt_encode(claims, b"secret")
        result = rust_jwt_decode(token, b"secret")
        assert result is None

    def test_decode_wrong_issuer_returns_none(self):
        claims = {"sub": "user1", "exp": 9999999999, "iss": "real-issuer"}
        token = rust_jwt_encode(claims, b"secret")
        result = rust_jwt_decode(token, b"secret", issuer="wrong-issuer")
        assert result is None

    def test_decode_wrong_audience_returns_none(self):
        claims = {"sub": "user1", "exp": 9999999999, "aud": "real-audience"}
        token = rust_jwt_encode(claims, b"secret")
        result = rust_jwt_decode(token, b"secret", audience="wrong-audience")
        assert result is None

    def test_decode_no_issuer_check_when_not_specified(self):
        claims = {"sub": "user1", "exp": 9999999999, "iss": "anything"}
        token = rust_jwt_encode(claims, b"secret")
        result = rust_jwt_decode(token, b"secret")
        assert result is not None
        assert result["sub"] == "user1"

    def test_decode_invalid_token_format(self):
        result = rust_jwt_decode("not.a.jwt", b"secret")
        assert result is None

    def test_decode_empty_token(self):
        result = rust_jwt_decode("", b"secret")
        assert result is None

    def test_encode_preserves_custom_claims(self):
        claims = {
            "sub": "user1",
            "exp": 9999999999,
            "role": "admin",
            "permissions": ["read", "write"],
        }
        token = rust_jwt_encode(claims, b"secret")
        decoded = rust_jwt_decode(token, b"secret")
        assert decoded is not None
        assert decoded["role"] == "admin"
        assert decoded["permissions"] == ["read", "write"]

    def test_different_secrets_produce_different_tokens(self):
        claims = {"sub": "user1", "exp": 9999999999}
        t1 = rust_jwt_encode(claims, b"secret-one")
        t2 = rust_jwt_encode(claims, b"secret-two")
        assert t1 != t2

    def test_refresh_token_roundtrip(self):
        """Simulate refresh token flow with longer expiry."""
        claims = {
            "sub": "user456",
            "exp": 9999999999,
            "iss": "wse",
            "aud": "wse-api",
            "type": "refresh",
        }
        secret = b"refresh-secret-key"
        token = rust_jwt_encode(claims, secret)
        decoded = rust_jwt_decode(token, secret, issuer="wse", audience="wse-api")
        assert decoded is not None
        assert decoded["type"] == "refresh"
        assert decoded["sub"] == "user456"
