use aes_gcm::{
    Aes256Gcm, KeyInit,
    aead::{Aead, AeadCore, OsRng},
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use p256::{PublicKey, SecretKey, ecdh::diffie_hellman, elliptic_curve::sec1::ToEncodedPoint};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// HMAC / SHA-256 signing
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256 of data using the given key.
/// Returns raw bytes (32 bytes).
#[pyfunction]
pub fn rust_hmac_sha256<'py>(py: Python<'py>, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let result = mac.finalize().into_bytes();
    Ok(PyBytes::new(py, &result).unbind())
}

/// Compute SHA-256 hash of data. Returns hex digest string.
#[pyfunction]
pub fn rust_sha256(_py: Python<'_>, data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Sign a JSON payload string using HMAC-SHA256.
///
/// 1. Compute SHA-256 hash of payload_json
/// 2. Compute HMAC-SHA256(secret, hash)
/// 3. Return hex-encoded HMAC
#[pyfunction]
pub fn rust_sign_message(_py: Python<'_>, payload_json: &str, secret: &[u8]) -> String {
    // Step 1: SHA-256 hash of payload
    let mut hasher = Sha256::new();
    hasher.update(payload_json.as_bytes());
    let payload_hash = hex::encode(hasher.finalize());

    // Step 2: HMAC-SHA256 of the hash
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload_hash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

// ---------------------------------------------------------------------------
// AES-GCM-256 encryption / decryption
// ---------------------------------------------------------------------------

/// Encrypt plaintext using AES-GCM-256.
///
/// key: 32-byte AES key.
/// Returns: 12-byte IV + ciphertext + 16-byte auth tag.
/// Wire format matches frontend security.ts.
#[pyfunction]
pub fn rust_aes_gcm_encrypt(py: Python, key: &[u8], plaintext: &[u8]) -> PyResult<Py<PyBytes>> {
    if key.len() != 32 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "AES-GCM-256 key must be 32 bytes",
        ));
    }
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid key: {e}")))?;
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Encrypt failed: {e}")))?;

    // [12-byte nonce][ciphertext + 16-byte tag]
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(PyBytes::new(py, &result).unbind())
}

/// Decrypt AES-GCM-256 ciphertext.
///
/// key: 32-byte AES key.
/// data: 12-byte IV + ciphertext + 16-byte auth tag.
/// Returns: decrypted plaintext.
#[pyfunction]
pub fn rust_aes_gcm_decrypt(py: Python, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    // Minimum: 12 (IV) + 16 (tag) = 28 bytes for empty plaintext
    if data.len() < 28 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Encrypted data too short (min 28 bytes: 12 IV + 16 tag)",
        ));
    }
    if key.len() != 32 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "AES-GCM-256 key must be 32 bytes",
        ));
    }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid key: {e}")))?;
    let nonce = aes_gcm::aead::generic_array::GenericArray::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("Decrypt failed: {e}")))?;
    Ok(PyBytes::new(py, &plaintext).unbind())
}

// ---------------------------------------------------------------------------
// ECDH P-256 key exchange
// ---------------------------------------------------------------------------

/// Generate an ECDH P-256 keypair.
///
/// Returns (private_key_32bytes, public_key_uncompressed_65bytes).
/// Public key format: 0x04 || X (32 bytes) || Y (32 bytes) — SEC1 uncompressed.
#[pyfunction]
pub fn rust_ecdh_generate_keypair(py: Python) -> PyResult<(Py<PyBytes>, Py<PyBytes>)> {
    let secret = SecretKey::random(&mut OsRng);
    let public = secret.public_key();

    // Private key: 32-byte scalar
    let sk_bytes = secret.to_bytes();

    // Public key: uncompressed SEC1 point (65 bytes)
    let pk_point = public.to_encoded_point(false);

    Ok((
        PyBytes::new(py, &sk_bytes).unbind(),
        PyBytes::new(py, pk_point.as_bytes()).unbind(),
    ))
}

/// Derive a 32-byte AES key from ECDH shared secret + HKDF-SHA256.
///
/// private_key: 32-byte raw scalar.
/// peer_public_key: 65-byte uncompressed SEC1 point (04 || X || Y).
///
/// HKDF parameters (matching frontend security.ts):
///   salt  = "wse-encryption"
///   info  = "aes-gcm-key"
///   hash  = SHA-256
///   output = 32 bytes (AES-256 key)
#[pyfunction]
pub fn rust_ecdh_derive_shared_secret(
    py: Python,
    private_key: &[u8],
    peer_public_key: &[u8],
) -> PyResult<Py<PyBytes>> {
    let secret = SecretKey::from_slice(private_key).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid private key: {e}"))
    })?;
    let peer_pk = PublicKey::from_sec1_bytes(peer_public_key).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Invalid peer public key: {e}"))
    })?;

    // ECDH key agreement
    let shared = diffie_hellman(secret.to_nonzero_scalar(), peer_pk.as_affine());

    // HKDF-SHA256: salt="wse-encryption", info="aes-gcm-key"
    let hkdf = Hkdf::<Sha256>::new(Some(b"wse-encryption"), shared.raw_secret_bytes());
    let mut aes_key = [0u8; 32];
    hkdf.expand(b"aes-gcm-key", &mut aes_key).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("HKDF expand failed: {e}"))
    })?;

    Ok(PyBytes::new(py, &aes_key).unbind())
}

// ---------------------------------------------------------------------------
// Rust-level unit tests (no Python required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes_gcm_roundtrip() {
        let key = [0xABu8; 32];
        let plaintext = b"Hello, WSE!";

        // Encrypt
        let cipher = Aes256Gcm::new_from_slice(&key).expect("key should be valid");
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ct = cipher.encrypt(&nonce, &plaintext[..]).expect("encrypt");

        let mut blob = Vec::with_capacity(12 + ct.len());
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ct);

        // Decrypt
        let (n, c) = blob.split_at(12);
        let nonce2 = aes_gcm::aead::generic_array::GenericArray::from_slice(n);
        let dec = cipher.decrypt(nonce2, c).expect("decrypt");
        assert_eq!(&dec, plaintext);
    }

    #[test]
    fn aes_gcm_wrong_key_fails() {
        let key1 = [0x01u8; 32];
        let key2 = [0x02u8; 32];

        let cipher1 = Aes256Gcm::new_from_slice(&key1).unwrap();
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ct = cipher1.encrypt(&nonce, b"secret".as_ref()).unwrap();

        let mut blob = Vec::new();
        blob.extend_from_slice(&nonce);
        blob.extend_from_slice(&ct);

        let cipher2 = Aes256Gcm::new_from_slice(&key2).unwrap();
        let (n, c) = blob.split_at(12);
        let nonce2 = aes_gcm::aead::generic_array::GenericArray::from_slice(n);
        assert!(cipher2.decrypt(nonce2, c).is_err());
    }

    #[test]
    fn ecdh_shared_secret_symmetric() {
        let sk_a = SecretKey::random(&mut OsRng);
        let pk_a = sk_a.public_key();
        let sk_b = SecretKey::random(&mut OsRng);
        let pk_b = sk_b.public_key();

        let shared_ab = diffie_hellman(sk_a.to_nonzero_scalar(), pk_b.as_affine());
        let shared_ba = diffie_hellman(sk_b.to_nonzero_scalar(), pk_a.as_affine());

        assert_eq!(
            shared_ab.raw_secret_bytes().as_slice(),
            shared_ba.raw_secret_bytes().as_slice()
        );
    }

    #[test]
    fn ecdh_hkdf_derives_32_byte_key() {
        let sk = SecretKey::random(&mut OsRng);
        let pk = sk.public_key();
        // Self-exchange just to test HKDF
        let shared = diffie_hellman(sk.to_nonzero_scalar(), pk.as_affine());
        let hkdf = Hkdf::<Sha256>::new(Some(b"wse-encryption"), shared.raw_secret_bytes());
        let mut key = [0u8; 32];
        hkdf.expand(b"aes-gcm-key", &mut key).unwrap();
        assert_eq!(key.len(), 32);
        // Key should not be all zeros
        assert!(key.iter().any(|&b| b != 0));
    }

    #[test]
    fn hmac_sha256_deterministic() {
        let key = b"test-key";
        let data = b"test-data";

        let mut mac1 = <HmacSha256 as Mac>::new_from_slice(key).unwrap();
        mac1.update(data);
        let r1 = mac1.finalize().into_bytes();

        let mut mac2 = <HmacSha256 as Mac>::new_from_slice(key).unwrap();
        mac2.update(data);
        let r2 = mac2.finalize().into_bytes();

        assert_eq!(r1, r2);
    }
}
