"""Tests for security manager (ECDH + AES-GCM + HMAC)."""

import pytest

try:
    from cryptography.hazmat.primitives.asymmetric.ec import generate_private_key, SECP256R1
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    HAS_CRYPTO = True
except ImportError:
    HAS_CRYPTO = False

from wse_client.security import SecurityManager


@pytest.mark.skipif(not HAS_CRYPTO, reason="cryptography not installed")
class TestSecurityManager:
    def test_generate_keypair_returns_65_bytes(self):
        sm = SecurityManager()
        pub = sm.generate_keypair()
        assert len(pub) == 65
        assert pub[0] == 0x04  # Uncompressed point prefix

    def test_ecdh_derive_shared_secret(self):
        # Simulate client + server key exchange
        client = SecurityManager()
        server = SecurityManager()

        client_pub = client.generate_keypair()
        server_pub = server.generate_keypair()

        client.derive_shared_secret(server_pub)
        server.derive_shared_secret(client_pub)

        assert client.is_enabled
        assert server.is_enabled

    def test_encrypt_decrypt_roundtrip(self):
        client = SecurityManager()
        server = SecurityManager()

        client_pub = client.generate_keypair()
        server_pub = server.generate_keypair()

        client.derive_shared_secret(server_pub)
        server.derive_shared_secret(client_pub)

        plaintext = '{"t":"secret","p":{"data":"classified"}}'
        encrypted = client.encrypt(plaintext)

        # Verify wire format: IV (12) + ciphertext + tag (16)
        assert len(encrypted) > 12 + 16

        decrypted = server.decrypt(encrypted)
        assert decrypted == plaintext

    def test_sign_verify_roundtrip(self):
        client = SecurityManager()
        server = SecurityManager()

        client_pub = client.generate_keypair()
        server_pub = server.generate_keypair()

        client.derive_shared_secret(server_pub)
        server.derive_shared_secret(client_pub)

        payload = '{"t":"test","p":{}}'
        sig = client.sign(payload)
        assert server.verify(payload, sig) is True

    def test_verify_rejects_tampered(self):
        client = SecurityManager()
        server = SecurityManager()

        client_pub = client.generate_keypair()
        server_pub = server.generate_keypair()

        client.derive_shared_secret(server_pub)
        server.derive_shared_secret(client_pub)

        sig = client.sign('{"original":true}')
        assert server.verify('{"tampered":true}', sig) is False

    def test_reset_disables(self):
        sm = SecurityManager()
        server = SecurityManager()

        sm_pub = sm.generate_keypair()
        server_pub = server.generate_keypair()

        sm.derive_shared_secret(server_pub)
        assert sm.is_enabled

        sm.reset()
        assert not sm.is_enabled

    def test_decrypt_wrong_key_fails(self):
        client = SecurityManager()
        server = SecurityManager()
        wrong = SecurityManager()

        client_pub = client.generate_keypair()
        server_pub = server.generate_keypair()
        wrong_pub = wrong.generate_keypair()

        client.derive_shared_secret(server_pub)
        wrong.derive_shared_secret(server_pub)  # Different shared secret

        encrypted = client.encrypt("secret message")

        from wse_client.errors import WSEEncryptionError
        with pytest.raises(WSEEncryptionError):
            wrong.decrypt(encrypted)
