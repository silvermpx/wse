//! The one crypto library behind cluster TLS, JWT validation, end-to-end
//! encryption and the Python crypto helpers, chosen when the extension is
//! built:
//!
//! - `aws-lc` (default, the published wheels): aws-lc-rs.
//! - `fips`: aws-lc-rs built from its FIPS 140-3 validated module. Building it
//!   needs CMake and Go:
//!   `MATURIN_PEP517_ARGS="--features fips" pip install wse-server --no-binary wse-server`.
//!
//! No other module names a crypto crate, so another library would be one more
//! implementation of the functions below. TLS gets its provider from here
//! explicitly; nothing is installed as the process-wide rustls default, so an
//! application's own TLS setup is left alone.

#[cfg(not(feature = "aws-lc"))]
compile_error!("choose a crypto library: the `aws-lc` feature (default) or `fips`");

use std::sync::Arc;

use aws_lc_rs::encoding::AsBigEndian;
use aws_lc_rs::{aead, agreement, digest, hkdf, hmac, rand};
use zeroize::Zeroizing;

/// The library this build uses, as reported by `health_snapshot()["crypto"]`.
pub(crate) const LIBRARY: &str = if cfg!(feature = "fips") {
    "aws-lc-rs (FIPS)"
} else {
    "aws-lc-rs"
};

/// A crypto operation failed. Deliberately without detail: the reason (a bad
/// tag, a point off the curve) is not for the peer to learn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("crypto operation failed")
    }
}

impl std::error::Error for Error {}

impl From<aws_lc_rs::error::Unspecified> for Error {
    fn from(_: aws_lc_rs::error::Unspecified) -> Self {
        Error
    }
}

impl From<aws_lc_rs::error::KeyRejected> for Error {
    fn from(_: aws_lc_rs::error::KeyRejected) -> Self {
        Error
    }
}

/// The rustls provider for cluster TLS.
pub(crate) fn tls_provider() -> Arc<rustls::crypto::CryptoProvider> {
    #[cfg(feature = "fips")]
    let provider = rustls::crypto::default_fips_provider();
    #[cfg(not(feature = "fips"))]
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    Arc::new(provider)
}

pub(crate) fn fill_random(buf: &mut [u8]) -> Result<(), Error> {
    Ok(rand::fill(buf)?)
}

pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, data).as_ref());
    out
}

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data).as_ref());
    out
}

/// HKDF-SHA256 (RFC 5869) filling `out`. An empty salt is the RFC's default
/// (HashLen zero bytes): the same key.
pub(crate) fn hkdf_sha256(
    salt: &[u8],
    ikm: &[u8],
    info: &[u8],
    out: &mut [u8],
) -> Result<(), Error> {
    struct Len(usize);
    impl hkdf::KeyType for Len {
        fn len(&self) -> usize {
            self.0
        }
    }
    hkdf::Salt::new(hkdf::HKDF_SHA256, salt)
        .extract(ikm)
        .expand(&[info], Len(out.len()))?
        .fill(out)?;
    Ok(())
}

pub(crate) const NONCE_LEN: usize = 12;
pub(crate) const TAG_LEN: usize = 16;

/// An AES-256-GCM key. The key schedule lives inside the library and is wiped
/// when the key is dropped; share one key with `Arc` rather than copying it.
pub(crate) struct Aes256Gcm(aead::LessSafeKey);

impl Aes256Gcm {
    pub(crate) fn new(key: &[u8]) -> Result<Self, Error> {
        if key.len() != 32 {
            return Err(Error);
        }
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, key)?;
        Ok(Self(aead::LessSafeKey::new(unbound)))
    }

    /// Encrypt `buf` in place and append the 16-byte tag. A nonce must never
    /// repeat under one key.
    pub(crate) fn seal(
        &self,
        nonce: [u8; NONCE_LEN],
        aad: &[u8],
        buf: &mut Vec<u8>,
    ) -> Result<(), Error> {
        Ok(self.0.seal_in_place_append_tag(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            buf,
        )?)
    }

    /// Check the tag and decrypt `buf` (ciphertext + tag) in place, returning
    /// the plaintext part of it.
    pub(crate) fn open<'a>(
        &self,
        nonce: [u8; NONCE_LEN],
        aad: &[u8],
        buf: &'a mut [u8],
    ) -> Result<&'a mut [u8], Error> {
        Ok(self.0.open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            buf,
        )?)
    }

    /// `nonce || ciphertext || tag` with a fresh random nonce, no AAD: the
    /// format of the current E2E frames and of the Python helpers.
    pub(crate) fn seal_random_nonce(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce = [0u8; NONCE_LEN];
        fill_random(&mut nonce)?;
        let mut body = Vec::with_capacity(plaintext.len() + TAG_LEN);
        body.extend_from_slice(plaintext);
        self.seal(nonce, &[], &mut body)?;
        let mut out = Vec::with_capacity(NONCE_LEN + body.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// The inverse of `seal_random_nonce`.
    pub(crate) fn open_nonce_prefixed(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        if data.len() < NONCE_LEN + TAG_LEN {
            return Err(Error);
        }
        let (nonce, sealed) = data.split_at(NONCE_LEN);
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(nonce);
        let mut buf = sealed.to_vec();
        let len = self.open(nonce_bytes, &[], &mut buf)?.len();
        buf.truncate(len);
        Ok(buf)
    }
}

/// Uncompressed SEC1 P-256 public key: `0x04 || X || Y`.
pub(crate) const P256_PUBLIC_LEN: usize = 65;

/// A new P-256 key pair for ECDH: the private scalar (wiped when dropped)
/// and the uncompressed public key.
pub(crate) fn ecdh_generate() -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), Error> {
    let private = agreement::PrivateKey::generate(&agreement::ECDH_P256)?;
    let public = private.compute_public_key()?;
    let scalar: aws_lc_rs::encoding::EcPrivateKeyBin = private.as_be_bytes()?;
    Ok((
        Zeroizing::new(scalar.as_ref().to_vec()),
        public.as_ref().to_vec(),
    ))
}

/// ECDH with a stored private scalar: `derive` gets the shared secret, which
/// never leaves the library otherwise.
pub(crate) fn ecdh_agree<R>(
    private_scalar: &[u8],
    peer_public: &[u8],
    derive: impl FnOnce(&[u8]) -> Result<R, Error>,
) -> Result<R, Error> {
    let private = agreement::PrivateKey::from_private_key(&agreement::ECDH_P256, private_scalar)?;
    let peer = agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, peer_public);
    agreement::agree(&private, peer, Error, derive)
}

/// ECDH with a fresh ephemeral key: returns our public key and what `derive`
/// made of the shared secret. The ephemeral key is gone afterwards.
pub(crate) fn ecdh_ephemeral<R>(
    peer_public: &[u8],
    derive: impl FnOnce(&[u8]) -> Result<R, Error>,
) -> Result<(Vec<u8>, R), Error> {
    let rng = rand::SystemRandom::new();
    let private = agreement::EphemeralPrivateKey::generate(&agreement::ECDH_P256, &rng)?;
    let public = private.compute_public_key()?.as_ref().to_vec();
    let peer = agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, peer_public);
    let derived = agreement::agree_ephemeral(private, peer, Error, derive)?;
    Ok((public, derived))
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
        let key = Aes256Gcm::new(&[1u8; 32]).unwrap();
        let mut buf = b"frame".to_vec();
        key.seal([0u8; NONCE_LEN], b"header", &mut buf).unwrap();
        let mut copy = buf.clone();
        assert!(key.open([0u8; NONCE_LEN], b"other", &mut copy).is_err());
        assert_eq!(
            key.open([0u8; NONCE_LEN], b"header", &mut buf).unwrap(),
            b"frame"
        );
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
    fn the_library_is_reported() {
        assert!(LIBRARY.starts_with("aws-lc-rs"));
        assert!(!tls_provider().cipher_suites.is_empty());
    }
}
