//! End-to-end encryption: ECDH P-256 key exchange and AES-GCM-256.

use zeroize::Zeroize;

// ---------------------------------------------------------------------------
// E2E encryption helpers (ECDH P-256 + AES-GCM-256)
// ---------------------------------------------------------------------------

/// Perform ECDH key exchange: generate server keypair, derive AES-256 key.
/// Returns (server_public_key_base64, Aes256Gcm cipher).
pub(super) fn derive_connection_key(
    client_pubkey_b64: &str,
) -> Result<(String, aes_gcm::Aes256Gcm), String> {
    use aes_gcm::KeyInit;
    use base64::Engine;
    use hkdf::Hkdf;
    use p256::ecdh::diffie_hellman;
    use p256::elliptic_curve::Generate;
    use p256::elliptic_curve::sec1::ToSec1Point;
    use p256::{PublicKey, SecretKey};
    use sha2::Sha256;

    let client_pubkey_bytes = base64::engine::general_purpose::STANDARD
        .decode(client_pubkey_b64)
        .map_err(|e| format!("Invalid base64: {e}"))?;

    let client_pk = PublicKey::from_sec1_bytes(&client_pubkey_bytes)
        .map_err(|e| format!("Invalid ECDH public key: {e}"))?;

    let server_sk = SecretKey::try_generate().map_err(|e| format!("System RNG failed: {e}"))?;
    let server_pk = server_sk.public_key();

    let shared = diffie_hellman(server_sk.to_nonzero_scalar(), client_pk.as_affine());

    // HKDF-SHA256 (RFC 5869): IKM = ECDH shared secret, salt = None (zero-salt),
    // info = context label for domain separation. All 4 components must match.
    let hkdf = Hkdf::<Sha256>::new(None, shared.raw_secret_bytes());
    let mut aes_key = [0u8; 32];
    hkdf.expand(b"wse-encryption/aes-gcm-key", &mut aes_key)
        .map_err(|e| format!("HKDF expand failed: {e}"))?;

    let cipher = aes_gcm::Aes256Gcm::new_from_slice(&aes_key)
        .map_err(|e| format!("AES key init failed: {e}"))?;
    // The cipher now owns the key material; wipe our stack copy.
    aes_key.zeroize();

    let server_pk_point = server_pk.to_sec1_point(false);
    let server_pk_b64 =
        base64::engine::general_purpose::STANDARD.encode(server_pk_point.as_bytes());

    Ok((server_pk_b64, cipher))
}

/// Encrypt plaintext with AES-GCM-256: returns 12-byte IV + ciphertext + 16-byte tag.
pub(super) fn encrypt_outbound(
    cipher: &aes_gcm::Aes256Gcm,
    plaintext: &[u8],
) -> Result<Vec<u8>, String> {
    use aes_gcm::aead::{Aead, Generate};

    let nonce = aes_gcm::Nonce::generate();
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| format!("AES-GCM encrypt failed: {e}"))?;
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt an AES-GCM-256 payload (12-byte IV + ciphertext + 16-byte tag).
pub(super) fn decrypt_inbound(cipher: &aes_gcm::Aes256Gcm, data: &[u8]) -> Result<Vec<u8>, String> {
    use aes_gcm::aead::Aead;

    if data.len() < 28 {
        return Err("Encrypted payload too short (min 28 bytes)".into());
    }
    // Non-panicking split of the 12-byte IV from attacker-controlled bytes
    // (unreachable Err given the >= 28 check, but no expect on the hot path).
    let Some((&nonce, ciphertext)) = data.split_first_chunk::<12>() else {
        return Err("Encrypted payload too short (min 28 bytes)".into());
    };
    cipher
        .decrypt(&nonce.into(), ciphertext)
        .map_err(|e| format!("AES-GCM decrypt failed: {e}"))
}
