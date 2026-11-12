//! Cryptographic helpers exposed to Python: HMAC/SHA-256, AES-256-GCM and
//! ECDH P-256 with HKDF. All of it runs on aws-lc-rs, the same library that
//! backs the cluster's TLS and JWT validation.

use aws_lc_rs::encoding::{AsBigEndian, EcPrivateKeyBin};
use aws_lc_rs::{aead, agreement, digest, hkdf, hmac, rand};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use zeroize::Zeroize;

/// HKDF info label of the E2E session key; the clients use the same label.
pub(crate) const E2E_KEY_INFO: &[u8] = b"wse-encryption/aes-gcm-key";

const NONCE_LEN: usize = aead::NONCE_LEN;
const TAG_LEN: usize = 16;

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(digest::digest(&digest::SHA256, data))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> hmac::Tag {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
}

// ---------------------------------------------------------------------------
// HMAC / SHA-256 signing
// ---------------------------------------------------------------------------

/// Compute HMAC-SHA256 of data using the given key.
/// Returns raw bytes (32 bytes).
#[pyfunction]
pub fn rust_hmac_sha256<'py>(py: Python<'py>, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    Ok(PyBytes::new(py, hmac_sha256(key, data).as_ref()).unbind())
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
    hex::encode(hmac_sha256(secret, payload_hash.as_bytes()))
}

// ---------------------------------------------------------------------------
// AES-GCM-256 encryption / decryption
// ---------------------------------------------------------------------------

fn aes_256_gcm(key: &[u8]) -> PyResult<aead::LessSafeKey> {
    if key.len() != 32 {
        return Err(PyValueError::new_err("AES-GCM-256 key must be 32 bytes"));
    }
    let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, key)
        .map_err(|_| PyValueError::new_err("Invalid AES-GCM-256 key"))?;
    Ok(aead::LessSafeKey::new(unbound))
}

/// Encrypt plaintext using AES-GCM-256.
///
/// key: 32-byte AES key.
/// Returns: 12-byte IV + ciphertext + 16-byte auth tag.
/// Wire format matches frontend security.ts.
#[pyfunction]
pub fn rust_aes_gcm_encrypt(py: Python, key: &[u8], plaintext: &[u8]) -> PyResult<Py<PyBytes>> {
    let cipher = aes_256_gcm(key)?;
    let mut nonce = [0u8; NONCE_LEN];
    rand::fill(&mut nonce).map_err(|_| PyRuntimeError::new_err("System RNG failed"))?;

    // [12-byte nonce][ciphertext + 16-byte tag]
    let mut out = Vec::with_capacity(NONCE_LEN + plaintext.len() + TAG_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(plaintext);
    let mut body = out.split_off(NONCE_LEN);
    cipher
        .seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::empty(),
            &mut body,
        )
        .map_err(|_| PyRuntimeError::new_err("Encrypt failed"))?;
    out.extend_from_slice(&body);
    Ok(PyBytes::new(py, &out).unbind())
}

/// Decrypt AES-GCM-256 ciphertext.
///
/// key: 32-byte AES key.
/// data: 12-byte IV + ciphertext + 16-byte auth tag.
/// Returns: decrypted plaintext.
#[pyfunction]
pub fn rust_aes_gcm_decrypt(py: Python, key: &[u8], data: &[u8]) -> PyResult<Py<PyBytes>> {
    // Minimum: 12 (IV) + 16 (tag) = 28 bytes for empty plaintext
    if data.len() < NONCE_LEN + TAG_LEN {
        return Err(PyValueError::new_err(
            "Encrypted data too short (min 28 bytes: 12 IV + 16 tag)",
        ));
    }
    let cipher = aes_256_gcm(key)?;
    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = aead::Nonce::try_assume_unique_for_key(nonce_bytes)
        .map_err(|_| PyValueError::new_err("Invalid nonce"))?;
    let mut buf = ciphertext.to_vec();
    let plaintext = cipher
        .open_in_place(nonce, aead::Aad::empty(), &mut buf)
        .map_err(|_| PyRuntimeError::new_err("Decrypt failed: wrong key or corrupted data"))?;
    Ok(PyBytes::new(py, plaintext).unbind())
}

// ---------------------------------------------------------------------------
// ECDH P-256 key exchange
// ---------------------------------------------------------------------------

/// HKDF output length of an AES-256 key.
struct AesKeyLen;

impl hkdf::KeyType for AesKeyLen {
    fn len(&self) -> usize {
        32
    }
}

/// HKDF-SHA256 over an ECDH shared secret into the 32-byte E2E key.
///
/// salt = none (RFC 5869's default of HashLen zero bytes; an empty HMAC key is
/// the same key), info = `E2E_KEY_INFO`. Clients derive the identical key.
pub(crate) fn e2e_key_from_shared_secret(shared_secret: &[u8], out: &mut [u8; 32]) -> bool {
    hkdf::Salt::new(hkdf::HKDF_SHA256, &[])
        .extract(shared_secret)
        .expand(&[E2E_KEY_INFO], AesKeyLen)
        .and_then(|okm| okm.fill(out))
        .is_ok()
}

/// Generate an ECDH P-256 keypair.
///
/// Returns (private_key_32bytes, public_key_uncompressed_65bytes).
/// Public key format: 0x04 || X (32 bytes) || Y (32 bytes) — SEC1 uncompressed.
#[pyfunction]
pub fn rust_ecdh_generate_keypair(py: Python) -> PyResult<(Py<PyBytes>, Py<PyBytes>)> {
    let private = agreement::PrivateKey::generate(&agreement::ECDH_P256)
        .map_err(|_| PyRuntimeError::new_err("System RNG failed"))?;
    let public = private
        .compute_public_key()
        .map_err(|_| PyRuntimeError::new_err("Key generation failed"))?;
    // The raw scalar's buffer is wiped when it is dropped.
    let scalar: EcPrivateKeyBin = private
        .as_be_bytes()
        .map_err(|_| PyRuntimeError::new_err("Key export failed"))?;
    Ok((
        PyBytes::new(py, scalar.as_ref()).unbind(),
        PyBytes::new(py, public.as_ref()).unbind(),
    ))
}

/// Derive a 32-byte AES key from ECDH shared secret + HKDF-SHA256.
///
/// private_key: 32-byte raw scalar.
/// peer_public_key: 65-byte uncompressed SEC1 point (04 || X || Y).
///
/// HKDF parameters (RFC 5869, matching server.rs / security.py / security.ts):
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
    let private = agreement::PrivateKey::from_private_key(&agreement::ECDH_P256, private_key)
        .map_err(|_| PyValueError::new_err("Invalid private key"))?;
    let peer = agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, peer_public_key);

    let mut aes_key = [0u8; 32];
    agreement::agree(&private, peer, (), |shared| {
        if e2e_key_from_shared_secret(shared, &mut aes_key) {
            Ok(())
        } else {
            Err(())
        }
    })
    .map_err(|()| PyValueError::new_err("Invalid peer public key"))?;

    // Hand the key to Python, then wipe our copy from the stack.
    let result = PyBytes::new(py, &aes_key).unbind();
    aes_key.zeroize();
    Ok(result)
}

// ---------------------------------------------------------------------------
// Rust-level unit tests (no Python required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn seal(key: &[u8], plaintext: &[u8]) -> Vec<u8> {
        let cipher =
            aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_256_GCM, key).unwrap());
        let mut nonce = [0u8; NONCE_LEN];
        rand::fill(&mut nonce).unwrap();
        let mut body = plaintext.to_vec();
        cipher
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::empty(),
                &mut body,
            )
            .unwrap();
        [nonce.as_slice(), &body].concat()
    }

    fn open(key: &[u8], blob: &[u8]) -> Option<Vec<u8>> {
        let cipher =
            aead::LessSafeKey::new(aead::UnboundKey::new(&aead::AES_256_GCM, key).unwrap());
        let (n, c) = blob.split_at(NONCE_LEN);
        let mut buf = c.to_vec();
        cipher
            .open_in_place(
                aead::Nonce::try_assume_unique_for_key(n).unwrap(),
                aead::Aad::empty(),
                &mut buf,
            )
            .ok()
            .map(|p| p.to_vec())
    }

    #[test]
    fn aes_gcm_roundtrip() {
        let key = [0xABu8; 32];
        let blob = seal(&key, b"Hello, WSE!");
        assert_eq!(open(&key, &blob).as_deref(), Some(&b"Hello, WSE!"[..]));
    }

    #[test]
    fn aes_gcm_wrong_key_fails() {
        let blob = seal(&[0x01u8; 32], b"secret");
        assert!(open(&[0x02u8; 32], &blob).is_none());
    }

    #[test]
    fn ecdh_shared_secret_symmetric() {
        let a = agreement::PrivateKey::generate(&agreement::ECDH_P256).unwrap();
        let b = agreement::PrivateKey::generate(&agreement::ECDH_P256).unwrap();
        let pub_a = a.compute_public_key().unwrap();
        let pub_b = b.compute_public_key().unwrap();
        let ab = agreement::agree(
            &a,
            agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, pub_b.as_ref()),
            (),
            |s| Ok(s.to_vec()),
        )
        .unwrap();
        let ba = agreement::agree(
            &b,
            agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, pub_a.as_ref()),
            (),
            |s| Ok(s.to_vec()),
        )
        .unwrap();
        assert_eq!(ab, ba);
        assert_eq!(pub_a.as_ref().len(), 65);
        assert_eq!(pub_a.as_ref()[0], 0x04);
    }

    #[test]
    fn hkdf_matches_rfc5869_zero_salt() {
        // An empty salt and HashLen zero bytes must give the same key: the
        // clients use the RFC default (salt=None), aws-lc-rs gets an empty one.
        let shared = [7u8; 32];
        let mut ours = [0u8; 32];
        assert!(e2e_key_from_shared_secret(&shared, &mut ours));
        let mut explicit = [0u8; 32];
        hkdf::Salt::new(hkdf::HKDF_SHA256, &[0u8; 32])
            .extract(&shared)
            .expand(&[E2E_KEY_INFO], AesKeyLen)
            .unwrap()
            .fill(&mut explicit)
            .unwrap();
        assert_eq!(ours, explicit);
        assert!(ours.iter().any(|&b| b != 0));
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_case_2() {
        let tag = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        assert_eq!(
            hex::encode(tag.as_ref()),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
