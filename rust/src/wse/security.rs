use hmac::{Hmac, Mac};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Compute HMAC-SHA256 of data using the given key.
/// Returns raw bytes (32 bytes).
#[pyfunction]
pub fn rust_hmac_sha256<'py>(py: Python<'py>, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
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
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload_hash.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}
