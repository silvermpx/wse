//! End-to-end encryption: ECDH P-256 key exchange and AES-GCM-256.

use std::sync::Arc;

use zeroize::Zeroizing;

use crate::crypto::{self, Aes256Gcm};

/// HKDF info label of the E2E session key; the clients use the same label.
pub(crate) const E2E_KEY_INFO: &[u8] = b"wse-encryption/aes-gcm-key";

/// The E2E session key from an ECDH shared secret: HKDF-SHA256 with no salt
/// (RFC 5869's default) and `E2E_KEY_INFO`. The clients derive the same key.
fn e2e_key(shared_secret: &[u8]) -> Result<Aes256Gcm, crypto::Error> {
    let mut key = Zeroizing::new([0u8; 32]);
    crypto::hkdf_sha256(&[], shared_secret, E2E_KEY_INFO, &mut key[..])?;
    Aes256Gcm::new(&key[..])
}

/// ECDH with the client's public key and a fresh server key: returns the
/// server's public key (base64) and the session key.
pub(super) fn derive_connection_key(
    client_pubkey_b64: &str,
) -> Result<(String, Arc<Aes256Gcm>), String> {
    use base64::Engine;

    let client_public = base64::engine::general_purpose::STANDARD
        .decode(client_pubkey_b64)
        .map_err(|e| format!("Invalid base64: {e}"))?;
    if client_public.len() != crypto::P256_PUBLIC_LEN {
        return Err("Invalid ECDH public key: expected an uncompressed P-256 point".into());
    }
    let (server_public, key) = crypto::ecdh_ephemeral(&client_public, e2e_key)
        .map_err(|_| "Invalid ECDH public key".to_string())?;
    let server_public_b64 = base64::engine::general_purpose::STANDARD.encode(server_public);
    Ok((server_public_b64, Arc::new(key)))
}

/// Encrypt with AES-GCM-256: returns 12-byte IV + ciphertext + 16-byte tag.
pub(super) fn encrypt_outbound(cipher: &Aes256Gcm, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    cipher
        .seal_random_nonce(plaintext)
        .map_err(|e| format!("AES-GCM encrypt failed: {e}"))
}

/// Decrypt an AES-GCM-256 payload (12-byte IV + ciphertext + 16-byte tag).
pub(super) fn decrypt_inbound(cipher: &Aes256Gcm, data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < crypto::NONCE_LEN + crypto::TAG_LEN {
        return Err("Encrypted payload too short (min 28 bytes)".into());
    }
    cipher
        .open_nonce_prefixed(data)
        .map_err(|_| "AES-GCM decrypt failed".to_string())
}

/// `payload` sealed for one connection: `E:` followed by nonce, ciphertext
/// and tag, the body of a binary frame. None if the encryption failed.
fn e2e_body(cipher: &Aes256Gcm, payload: &[u8]) -> Option<Vec<u8>> {
    let sealed = encrypt_outbound(cipher, payload)
        .inspect_err(|e| tracing::error!("[WSE] {e}"))
        .ok()?;
    let mut body = Vec::with_capacity(2 + sealed.len());
    body.extend_from_slice(b"E:");
    body.extend_from_slice(&sealed);
    Some(body)
}

/// `payload` sealed for one connection, as a frame.
pub(super) fn e2e_frame(cipher: &Aes256Gcm, payload: &[u8]) -> Option<bytes::Bytes> {
    e2e_body(cipher, payload).map(|body| super::frames::encode_ws_frame(0x02, &body))
}

/// `payload` sealed for one connection, as a message.
pub(super) fn e2e_message(
    cipher: &Aes256Gcm,
    payload: &[u8],
) -> Option<tokio_tungstenite::tungstenite::protocol::Message> {
    e2e_body(cipher, payload)
        .map(|body| tokio_tungstenite::tungstenite::protocol::Message::Binary(body.into()))
}
