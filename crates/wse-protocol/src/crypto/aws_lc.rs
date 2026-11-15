//! The crypto on aws-lc-rs: the server's, and the Rust client's outside the
//! browser.

use aws_lc_rs::encoding::AsBigEndian;
use aws_lc_rs::{aead, agreement, digest, hkdf, hmac, rand};
use zeroize::Zeroizing;

use super::{Error, NONCE_LEN, TAG_LEN, pem_body};

/// The library this build uses, as the server's health snapshot reports it.
pub const LIBRARY: &str = if cfg!(feature = "fips") {
    "aws-lc-rs (FIPS)"
} else {
    "aws-lc-rs"
};

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

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(digest::digest(&digest::SHA256, data).as_ref());
    out
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data).as_ref());
    out
}

/// HKDF-SHA256 (RFC 5869) filling `out`. An empty salt is the RFC's default
/// (HashLen zero bytes): the same key.
pub fn hkdf_sha256(salt: &[u8], ikm: &[u8], info: &[u8], out: &mut [u8]) -> Result<(), Error> {
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

/// An AES-256-GCM key: it opens frames whatever their nonce, and seals with
/// a random nonce the library itself makes (the approved way in FIPS mode).
/// The key schedule lives inside the library and is wiped when the key is
/// dropped; share one key with `Arc` rather than copying it.
pub struct Aes256Gcm {
    open: aead::LessSafeKey,
    seal: aead::RandomizedNonceKey,
}

impl Aes256Gcm {
    pub fn new(key: &[u8]) -> Result<Self, Error> {
        if key.len() != 32 {
            return Err(Error);
        }
        let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, key)?;
        Ok(Self {
            open: aead::LessSafeKey::new(unbound),
            seal: aead::RandomizedNonceKey::new(&aead::AES_256_GCM, key)?,
        })
    }

    /// Check the tag and decrypt `buf` (ciphertext + tag) in place, returning
    /// the plaintext part of it.
    pub fn open<'a>(
        &self,
        nonce: [u8; NONCE_LEN],
        aad: &[u8],
        buf: &'a mut [u8],
    ) -> Result<&'a mut [u8], Error> {
        Ok(self.open.open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            buf,
        )?)
    }

    /// `nonce || ciphertext || tag` with a fresh random nonce, no AAD: the
    /// format of the first-design E2E frames and of the Python helpers.
    pub fn seal_random_nonce(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut body = Vec::with_capacity(plaintext.len() + TAG_LEN);
        body.extend_from_slice(plaintext);
        let nonce = self
            .seal
            .seal_in_place_append_tag(aead::Aad::empty(), &mut body)?;
        let mut out = Vec::with_capacity(NONCE_LEN + body.len());
        out.extend_from_slice(nonce.as_ref());
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// The inverse of `seal_random_nonce`.
    pub fn open_nonce_prefixed(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
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

/// A new P-256 key pair for ECDH: the private scalar (wiped when dropped)
/// and the uncompressed public key.
pub fn ecdh_generate() -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), Error> {
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
pub fn ecdh_agree<R>(
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
pub fn ecdh_ephemeral<R>(
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

/// An ephemeral P-256 key for ECDH, kept until the peer's key comes: used
/// once, then gone.
pub struct EphemeralKey(agreement::EphemeralPrivateKey);

impl EphemeralKey {
    /// A new key and its uncompressed public key.
    pub fn generate() -> Result<(Self, Vec<u8>), Error> {
        let rng = rand::SystemRandom::new();
        let private = agreement::EphemeralPrivateKey::generate(&agreement::ECDH_P256, &rng)?;
        let public = private.compute_public_key()?.as_ref().to_vec();
        Ok((Self(private), public))
    }

    /// ECDH with the peer's public key: `derive` gets the shared secret.
    pub fn agree<R>(
        self,
        peer_public: &[u8],
        derive: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let peer = agreement::UnparsedPublicKey::new(&agreement::ECDH_P256, peer_public);
        agreement::agree_ephemeral(self.0, peer, Error, derive)
    }
}

/// Fills `out` with random bytes from the system's generator.
pub fn fill_random(out: &mut [u8]) -> Result<(), Error> {
    use aws_lc_rs::rand::SecureRandom;
    Ok(rand::SystemRandom::new().fill(out)?)
}

/// The server's identity for end-to-end encryption: an ECDSA P-256 key that
/// signs each handshake, so a client pinning its public key knows it talks
/// to this server and not to whatever terminates TLS in between.
pub struct IdentityKey(aws_lc_rs::signature::EcdsaKeyPair);

impl IdentityKey {
    /// From a PKCS#8 private key, PEM (`-----BEGIN PRIVATE KEY-----`) or DER.
    pub fn from_pkcs8(key: &[u8]) -> Result<Self, Error> {
        let der = pem_body(key).unwrap_or_else(|| key.to_vec());
        aws_lc_rs::signature::EcdsaKeyPair::from_pkcs8(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &der,
        )
        .map(Self)
        .map_err(|_| Error)
    }

    /// The public key: uncompressed SEC1, `0x04 || X || Y`.
    pub fn public_key(&self) -> &[u8] {
        use aws_lc_rs::signature::KeyPair;
        self.0.public_key().as_ref()
    }

    /// An ECDSA P-256 / SHA-256 signature of `message`, `r || s` (64 bytes).
    pub fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let rng = rand::SystemRandom::new();
        Ok(self.0.sign(&rng, message)?.as_ref().to_vec())
    }
}

/// A new ECDSA P-256 private key, PKCS#8 DER.
pub fn generate_p256_pkcs8() -> Result<Vec<u8>, Error> {
    let rng = rand::SystemRandom::new();
    let pkcs8 = aws_lc_rs::signature::EcdsaKeyPair::generate_pkcs8(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
        &rng,
    )?;
    Ok(pkcs8.as_ref().to_vec())
}

/// Check an ECDSA P-256 / SHA-256 signature (`r || s`) by an uncompressed
/// SEC1 public key.
pub fn verify_p256(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    aws_lc_rs::signature::UnparsedPublicKey::new(
        &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED,
        public_key,
    )
    .verify(message, signature)
    .is_ok()
}

/// An ECDSA P-256 public key, parsed once, that checks signatures.
pub struct VerifyingKey(aws_lc_rs::signature::ParsedPublicKey);

impl VerifyingKey {
    /// From an uncompressed SEC1 point.
    pub fn from_sec1(public_key: &[u8]) -> Option<Self> {
        aws_lc_rs::signature::ParsedPublicKey::new(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED,
            public_key,
        )
        .ok()
        .map(Self)
    }

    /// Whether `signature` (`r || s`) signs `message` with this key.
    pub fn verifies(&self, message: &[u8], signature: &[u8]) -> bool {
        self.0.verify_sig(message, signature).is_ok()
    }
}

/// An AES-256-GCM key sealing with a counter nonce (a fixed 4-byte IV and
/// a 64-bit sequence number that only grows), as TLS 1.2 records do: the
/// library checks the counter, which makes it the approved way in FIPS mode.
pub struct CounterSealKey(aead::TlsRecordSealingKey);

impl CounterSealKey {
    pub fn new(key: &[u8]) -> Result<Self, Error> {
        Ok(Self(aead::TlsRecordSealingKey::new(
            &aead::AES_256_GCM,
            aead::TlsProtocolId::TLS12,
            key,
        )?))
    }

    /// Encrypt `buf` in place and append the 16-byte tag. Each nonce's last
    /// 8 bytes must be greater than the last one's.
    pub fn seal(
        &mut self,
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
}
