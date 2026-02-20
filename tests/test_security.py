"""Tests for SecurityManager."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.connection.security import (
    EncryptionProvider,
    SecurityManager,
    TokenProvider,
)

# =========================================================================
# Stub providers for testing
# =========================================================================


class StubEncryptionProvider:
    """Simple reversible encryption for testing (base64)."""

    def encrypt(self, data: str) -> str:
        import base64

        return base64.b64encode(data.encode("utf-8")).decode("utf-8")

    def decrypt(self, encrypted_data: str) -> str:
        import base64

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

    def encrypt(self, data: str) -> str:
        raise RuntimeError("encryption failed")

    def decrypt(self, encrypted_data: str):
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
        assert info["encryption_provider"] is None
        assert info["token_provider"] is None


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
    async def test_encrypt_no_provider_returns_none(self):
        sm = SecurityManager()
        await sm.initialize({"encryption_enabled": True})

        result = await sm.encrypt_message("hello")
        assert result is None

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

        result = await sm.verify_signature(
            {"payload": payload, "signature": sig}
        )
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

        result = await sm.verify_signature(
            {"payload": {"data": "test"}, "signature": sig}
        )
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
    async def test_get_public_key_returns_none(self, security_manager):
        result = await security_manager.get_public_key()
        assert result is None

    @pytest.mark.asyncio
    async def test_set_server_public_key_no_error(self, security_manager):
        # Should not raise
        await security_manager.set_server_public_key("fake_key")

    @pytest.mark.asyncio
    async def test_initialize_sets_flags(self):
        sm = SecurityManager()
        await sm.initialize({
            "encryption_enabled": True,
            "message_signing_enabled": True,
        })
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
