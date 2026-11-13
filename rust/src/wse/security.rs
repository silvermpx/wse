//! Cryptographic helpers exposed to Python: HMAC/SHA-256, AES-256-GCM and
//! ECDH P-256 with HKDF, on the crate's crypto library (see `crate::crypto`).

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use zeroize::Zeroizing;

use crate::crypto::{self, Aes256Gcm};
use crate::wse::server::E2E_KEY_INFO;

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(crypto::sha256(data))
}

// ---------------------------------------------------------------------------
// HMAC / SHA-256 signing
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256 of data using the given key.
/// Returns raw bytes (32 bytes).
#[pyfunction]
pub fn rust_hmac_sha256(py: Python<'_>, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    Ok(PyBytes::new(py, &crypto::hmac_sha256(key, data)).unbind())
}

/// Compute SHA-256 hash of data. Returns hex digest string.
#[pyfunction]
pub fn rust_sha256(_py: Python<'_>, data: &[u8]) -> String {
    sha256_hex(data)
}

/// Sign a JSON payload string using HMAC-SHA256.
///
/// 1. Compute SHA-256 hash of payload_json
/// 2. Compute HMAC-SHA256(secret, hash)
/// 3. Return hex-encoded HMAC
#[pyfunction]
pub fn rust_sign_message(_py: Python<'_>, payload_json: &str, secret: &[u8]) -> String {
    let payload_hash = sha256_hex(payload_json.as_bytes());
    hex::encode(crypto::hmac_sha256(secret, payload_hash.as_bytes()))
}

// ---------------------------------------------------------------------------
// AES-GCM-256 encryption / decryption
// ---------------------------------------------------------------------------

fn aes_256_gcm(key: &[u8]) -> PyResult<Aes256Gcm> {
    if key.len() != 32 {
        return Err(PyValueError::new_err("AES-GCM-256 key must be 32 bytes"));
    }
    Aes256Gcm::new(key).map_err(|_| PyValueError::new_err("Invalid AES-GCM-256 key"))
}

/// Encrypt plaintext using AES-GCM-256.
///
/// key: 32-byte AES key.
/// Returns: 12-byte IV + ciphertext + 16-byte auth tag.
/// Wire format matches frontend security.ts.
#[pyfunction]
pub fn rust_aes_gcm_encrypt(py: Python, key: &[u8], plaintext: &[u8]) -> PyResult<Py<PyBytes>> {
    let sealed = aes_256_gcm(key)?
        .seal_random_nonce(plaintext)
        .map_err(|_| PyRuntimeError::new_err("Encrypt failed"))?;
    Ok(PyBytes::new(py, &sealed).unbind())
}

/// Decrypt AES-GCM-256 ciphertext.
///
/// key: 32-byte AES key.
/// data: 12-byte IV + ciphertext + 16-byte auth tag.
/// Returns: decrypted plaintext.
#[pyfunction]
pub fn rust_aes_gcm_decrypt(py: Python, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    // Minimum: 12 (IV) + 16 (tag) = 28 bytes for empty plaintext
    if data.len() < crypto::NONCE_LEN + crypto::TAG_LEN {
        return Err(PyValueError::new_err(
            "Encrypted data too short (min 28 bytes: 12 IV + 16 tag)",
        ));
    }
    let plaintext = aes_256_gcm(key)?
        .open_nonce_prefixed(data)
        .map_err(|_| PyRuntimeError::new_err("Decrypt failed: wrong key or corrupted data"))?;
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
    let (scalar, public) =
        crypto::ecdh_generate().map_err(|_| PyRuntimeError::new_err("Key generation failed"))?;
    Ok((
        PyBytes::new(py, &scalar).unbind(),
        PyBytes::new(py, &public).unbind(),
    ))
}

/// Derive a 32-byte AES key from ECDH shared secret + HKDF-SHA256.
///
/// private_key: 32-byte raw scalar.
/// peer_public_key: 65-byte uncompressed SEC1 point (04 || X || Y).
///
/// HKDF parameters (RFC 5869, the same as the server and both clients):
///   salt   = None (zero-salt default, 32 zero bytes for SHA-256)
///   info   = "wse-encryption/aes-gcm-key"
///   hash   = SHA-256
///   output = 32 bytes (AES-256 key)
#[pyfunction]
pub fn rust_ecdh_derive_shared_secret(
    py: Python,
    private_key: &[u8],
    peer_public_key: &[u8],
) -> PyResult<Py<PyBytes>> {
    if private_key.len() != 32 {
        return Err(PyValueError::new_err(
            "Invalid private key: expected 32 bytes",
        ));
    }
    let key = crypto::ecdh_agree(private_key, peer_public_key, |shared| {
        let mut key = Zeroizing::new([0u8; 32]);
        crypto::hkdf_sha256(&[], shared, E2E_KEY_INFO, &mut key[..])?;
        Ok(key)
    })
    .map_err(|_| PyValueError::new_err("Invalid private key or peer public key"))?;
    // The bytes object is Python's copy; ours is wiped when `key` drops.
    Ok(PyBytes::new(py, &key[..]).unbind())
}
