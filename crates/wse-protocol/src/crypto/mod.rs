//! The one crypto library behind end-to-end encryption, signed messages and
//! the server's crypto helpers: aws-lc-rs, or with the `fips` feature
//! aws-lc-rs built from its FIPS 140-3 validated module (building it needs
//! CMake and Go). In the browser, where aws-lc-rs does not build, the same
//! functions in pure Rust (RustCrypto).
//!
//! No other module names a crypto crate, so another library would be one more
//! implementation of the functions below.

#[cfg(not(any(target_arch = "wasm32", wse_rustcrypto)))]
mod aws_lc;
#[cfg(not(any(target_arch = "wasm32", wse_rustcrypto)))]
pub use aws_lc::*;

#[cfg(any(target_arch = "wasm32", wse_rustcrypto))]
mod rust;
#[cfg(any(target_arch = "wasm32", wse_rustcrypto))]
pub use rust::*;

/// A crypto operation failed. Deliberately without detail: the reason (a bad
/// tag, a point off the curve) is not for the peer to learn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("crypto operation failed")
    }
}

impl std::error::Error for Error {}

pub const NONCE_LEN: usize = 12;
pub const TAG_LEN: usize = 16;

/// Uncompressed SEC1 P-256 public key: `0x04 || X || Y`.
pub const P256_PUBLIC_LEN: usize = 65;

/// The DER inside a PEM block, if `text` is one.
fn pem_body(text: &[u8]) -> Option<Vec<u8>> {
    use base64::Engine;
    let text = std::str::from_utf8(text).ok()?;
    if !text.trim_start().starts_with("-----BEGIN") {
        return None;
    }
    let body: String = text
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .flat_map(|line| line.trim().chars())
        .collect();
    base64::engine::general_purpose::STANDARD.decode(body).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes_gcm_round_trip_and_tamper() {
        let key = Aes256Gcm::new(&[7u8; 32]).unwrap();
        let blob = key.seal_random_nonce(b"hello").unwrap();
        assert_eq!(key.open_nonce_prefixed(&blob).unwrap(), b"hello");
        let mut bad = blob.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(key.open_nonce_prefixed(&bad), Err(Error));
        assert!(Aes256Gcm::new(&[0u8; 16]).is_err());
    }

    #[test]
    fn aad_is_bound() {
        let mut sealer = CounterSealKey::new(&[1u8; 32]).unwrap();
        let key = Aes256Gcm::new(&[1u8; 32]).unwrap();
        let mut buf = b"frame".to_vec();
        sealer.seal([0u8; NONCE_LEN], b"header", &mut buf).unwrap();
        let mut copy = buf.clone();
        assert!(key.open([0u8; NONCE_LEN], b"other", &mut copy).is_err());
        assert_eq!(
            key.open([0u8; NONCE_LEN], b"header", &mut buf).unwrap(),
            b"frame"
        );
    }

    #[test]
    fn a_counter_nonce_must_grow() {
        let mut sealer = CounterSealKey::new(&[2u8; 32]).unwrap();
        let nonce = |seq: u64| {
            let mut n = [9u8; NONCE_LEN];
            n[4..].copy_from_slice(&seq.to_be_bytes());
            n
        };
        sealer.seal(nonce(0), b"", &mut b"a".to_vec()).unwrap();
        sealer.seal(nonce(1), b"", &mut b"b".to_vec()).unwrap();
        assert!(sealer.seal(nonce(1), b"", &mut b"c".to_vec()).is_err());
        assert!(sealer.seal(nonce(0), b"", &mut b"d".to_vec()).is_err());
        sealer.seal(nonce(5), b"", &mut b"e".to_vec()).unwrap();
    }

    #[test]
    fn stored_and_ephemeral_ecdh_agree() {
        let (scalar, public) = ecdh_generate().unwrap();
        assert_eq!(public.len(), P256_PUBLIC_LEN);
        let (ephemeral_public, ours) = ecdh_ephemeral(&public, |z| Ok(z.to_vec())).unwrap();
        let theirs = ecdh_agree(&scalar, &ephemeral_public, |z| Ok(z.to_vec())).unwrap();
        assert_eq!(ours, theirs);
        assert!(ecdh_agree(&scalar, &[4u8; 65], |_| Ok(())).is_err());
    }

    #[test]
    fn hkdf_and_hmac_known_answers() {
        // RFC 5869 test case 1.
        let ikm = [0x0bu8; 22];
        let salt: Vec<u8> = (0x00..=0x0c).collect();
        let info: Vec<u8> = (0xf0..=0xf9).collect();
        let mut okm = [0u8; 42];
        hkdf_sha256(&salt, &ikm, &info, &mut okm).unwrap();
        assert_eq!(
            hex::encode(okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        );
        // RFC 4231 test case 2.
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        assert_eq!(
            hex::encode(sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn identity_signatures_verify_and_bind_the_message() {
        let pkcs8 = generate_p256_pkcs8().unwrap();
        let key = IdentityKey::from_pkcs8(&pkcs8).unwrap();
        assert_eq!(key.public_key().len(), P256_PUBLIC_LEN);
        let sig = key.sign(b"transcript").unwrap();
        assert_eq!(sig.len(), 64);
        assert!(verify_p256(key.public_key(), b"transcript", &sig));
        assert!(!verify_p256(key.public_key(), b"transcripT", &sig));
        let parsed = VerifyingKey::from_sec1(key.public_key()).unwrap();
        assert!(parsed.verifies(b"transcript", &sig));
        assert!(!parsed.verifies(b"transcripT", &sig));
        assert!(VerifyingKey::from_sec1(&[4u8; 65]).is_none());
        assert!(IdentityKey::from_pkcs8(b"not a key").is_err());
    }

    #[test]
    fn the_library_is_reported() {
        assert!(LIBRARY.starts_with("aws-lc-rs") || LIBRARY == "RustCrypto");
    }
}
