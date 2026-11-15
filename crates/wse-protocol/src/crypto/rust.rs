//! The crypto in pure Rust (RustCrypto), where aws-lc-rs does not build: the
//! Rust client in the browser. The same functions as on aws-lc-rs, checked
//! against the same vectors.

use aes_gcm::aead::inout::InOutBuf;
use aes_gcm::aead::{AeadInOut, KeyInit};
use hmac::Mac;
use p256::ecdsa::signature::{Signer, Verifier};
use p256::elliptic_curve::Generate;
use p256::elliptic_curve::sec1::ToSec1Point;
use p256::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use sha2::Digest;
use zeroize::Zeroizing;

use super::{Error, NONCE_LEN, TAG_LEN, pem_body};

/// The library this build uses.
pub const LIBRARY: &str = "RustCrypto";

type Gcm = aes_gcm::Aes256Gcm;
type Nonce = aes_gcm::aead::Nonce<Gcm>;
type Tag = aes_gcm::aead::Tag<Gcm>;

pub fn sha256(data: &[u8]) -> [u8; 32] {
    sha2::Sha256::digest(data).into()
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(key)
        .expect("HMAC takes a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// HKDF-SHA256 (RFC 5869) filling `out`. An empty salt is the RFC's default
/// (HashLen zero bytes): the same key.
pub fn hkdf_sha256(salt: &[u8], ikm: &[u8], info: &[u8], out: &mut [u8]) -> Result<(), Error> {
    hkdf::Hkdf::<sha2::Sha256>::new(Some(salt), ikm)
        .expand(info, out)
        .map_err(|_| Error)
}

/// An AES-256-GCM key: it opens frames whatever their nonce, and seals with
/// a random nonce.
pub struct Aes256Gcm(Gcm);

impl Aes256Gcm {
    pub fn new(key: &[u8]) -> Result<Self, Error> {
        if key.len() != 32 {
            return Err(Error);
        }
        Gcm::new_from_slice(key).map(Self).map_err(|_| Error)
    }

    /// Check the tag and decrypt `buf` (ciphertext + tag) in place, returning
    /// the plaintext part of it.
    pub fn open<'a>(
        &self,
        nonce: [u8; NONCE_LEN],
        aad: &[u8],
        buf: &'a mut [u8],
    ) -> Result<&'a mut [u8], Error> {
        let Some(len) = buf.len().checked_sub(TAG_LEN) else {
            return Err(Error);
        };
        let (text, tag) = buf.split_at_mut(len);
        let tag: [u8; TAG_LEN] = (&*tag).try_into().map_err(|_| Error)?;
        self.0
            .decrypt_inout_detached(
                &Nonce::from(nonce),
                aad,
                InOutBuf::from(text),
                &Tag::from(tag),
            )
            .map_err(|_| Error)?;
        Ok(&mut buf[..len])
    }

    /// `nonce || ciphertext || tag` with a fresh random nonce, no AAD: the
    /// format of the first-design E2E frames and of the Python helpers.
    pub fn seal_random_nonce(&self, plaintext: &[u8]) -> Result<Vec<u8>, Error> {
        let mut nonce = [0u8; NONCE_LEN];
        fill_random(&mut nonce)?;
        let mut out = Vec::with_capacity(NONCE_LEN + plaintext.len() + TAG_LEN);
        out.extend_from_slice(&nonce);
        out.extend_from_slice(plaintext);
        let tag = self
            .0
            .encrypt_inout_detached(
                &Nonce::from(nonce),
                &[],
                InOutBuf::from(&mut out[NONCE_LEN..]),
            )
            .map_err(|_| Error)?;
        out.extend_from_slice(&tag);
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
    let secret = p256::SecretKey::try_generate().map_err(|_| Error)?;
    let public = point(&secret.public_key());
    Ok((Zeroizing::new(secret.to_bytes().to_vec()), public))
}

/// ECDH with a stored private scalar: `derive` gets the shared secret.
pub fn ecdh_agree<R>(
    private_scalar: &[u8],
    peer_public: &[u8],
    derive: impl FnOnce(&[u8]) -> Result<R, Error>,
) -> Result<R, Error> {
    let secret = p256::SecretKey::from_slice(private_scalar).map_err(|_| Error)?;
    let peer = peer_key(peer_public)?;
    let shared = p256::ecdh::diffie_hellman(secret.to_nonzero_scalar(), peer.as_affine());
    derive(shared.raw_secret_bytes().as_slice())
}

/// ECDH with a fresh ephemeral key: returns our public key and what `derive`
/// made of the shared secret. The ephemeral key is gone afterwards.
pub fn ecdh_ephemeral<R>(
    peer_public: &[u8],
    derive: impl FnOnce(&[u8]) -> Result<R, Error>,
) -> Result<(Vec<u8>, R), Error> {
    let (key, public) = EphemeralKey::generate()?;
    let derived = key.agree(peer_public, derive)?;
    Ok((public, derived))
}

/// An ephemeral P-256 key for ECDH, kept until the peer's key comes: used
/// once, then gone.
pub struct EphemeralKey(p256::ecdh::EphemeralSecret);

impl EphemeralKey {
    /// A new key and its uncompressed public key.
    pub fn generate() -> Result<(Self, Vec<u8>), Error> {
        let secret = p256::ecdh::EphemeralSecret::try_generate().map_err(|_| Error)?;
        let public = point(&secret.public_key());
        Ok((Self(secret), public))
    }

    /// ECDH with the peer's public key: `derive` gets the shared secret.
    pub fn agree<R>(
        self,
        peer_public: &[u8],
        derive: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let peer = peer_key(peer_public)?;
        derive(self.0.diffie_hellman(&peer).raw_secret_bytes().as_slice())
    }
}

/// A peer's ECDH key: an uncompressed point on the curve.
fn peer_key(public_key: &[u8]) -> Result<p256::PublicKey, Error> {
    if public_key.len() != super::P256_PUBLIC_LEN {
        return Err(Error);
    }
    p256::PublicKey::from_sec1_bytes(public_key).map_err(|_| Error)
}

fn point(key: &p256::PublicKey) -> Vec<u8> {
    key.to_sec1_point(false).as_bytes().to_vec()
}

/// Fills `out` with random bytes from the system's generator.
pub fn fill_random(out: &mut [u8]) -> Result<(), Error> {
    getrandom::fill(out).map_err(|_| Error)
}

/// An ECDSA P-256 private key that signs: the server's identity for
/// end-to-end encryption, or a signing key.
pub struct IdentityKey {
    key: p256::ecdsa::SigningKey,
    public: Vec<u8>,
}

impl IdentityKey {
    /// From a PKCS#8 private key, PEM (`-----BEGIN PRIVATE KEY-----`) or DER.
    pub fn from_pkcs8(key: &[u8]) -> Result<Self, Error> {
        let der = pem_body(key).unwrap_or_else(|| key.to_vec());
        let key = p256::ecdsa::SigningKey::from_pkcs8_der(&der).map_err(|_| Error)?;
        let public = key.verifying_key().to_sec1_point(false).as_bytes().to_vec();
        Ok(Self { key, public })
    }

    /// The public key: uncompressed SEC1, `0x04 || X || Y`.
    pub fn public_key(&self) -> &[u8] {
        &self.public
    }

    /// An ECDSA P-256 / SHA-256 signature of `message`, `r || s` (64 bytes).
    pub fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        let signature: p256::ecdsa::Signature = self.key.sign(message);
        Ok(signature.to_bytes().to_vec())
    }
}

/// A new ECDSA P-256 private key, PKCS#8 DER.
pub fn generate_p256_pkcs8() -> Result<Vec<u8>, Error> {
    p256::SecretKey::try_generate()
        .map_err(|_| Error)?
        .to_pkcs8_der()
        .map(|der| der.as_bytes().to_vec())
        .map_err(|_| Error)
}

/// Check an ECDSA P-256 / SHA-256 signature (`r || s`) by an uncompressed
/// SEC1 public key.
pub fn verify_p256(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    VerifyingKey::from_sec1(public_key).is_some_and(|key| key.verifies(message, signature))
}

/// An ECDSA P-256 public key, parsed once, that checks signatures.
pub struct VerifyingKey(p256::ecdsa::VerifyingKey);

impl VerifyingKey {
    /// From an uncompressed SEC1 point.
    pub fn from_sec1(public_key: &[u8]) -> Option<Self> {
        if public_key.len() != super::P256_PUBLIC_LEN {
            return None;
        }
        p256::ecdsa::VerifyingKey::from_sec1_bytes(public_key)
            .ok()
            .map(Self)
    }

    /// Whether `signature` (`r || s`) signs `message` with this key.
    pub fn verifies(&self, message: &[u8], signature: &[u8]) -> bool {
        p256::ecdsa::Signature::from_slice(signature)
            .is_ok_and(|signature| self.0.verify(message, &signature).is_ok())
    }
}

/// An AES-256-GCM key sealing with a counter nonce (a fixed 4-byte IV and
/// a 64-bit sequence number that only grows), as TLS 1.2 records do.
pub struct CounterSealKey {
    key: Gcm,
    last: Option<u64>,
}

impl CounterSealKey {
    pub fn new(key: &[u8]) -> Result<Self, Error> {
        if key.len() != 32 {
            return Err(Error);
        }
        let key = Gcm::new_from_slice(key).map_err(|_| Error)?;
        Ok(Self { key, last: None })
    }

    /// Encrypt `buf` in place and append the 16-byte tag. Each nonce's last
    /// 8 bytes must be greater than the last one's.
    pub fn seal(
        &mut self,
        nonce: [u8; NONCE_LEN],
        aad: &[u8],
        buf: &mut Vec<u8>,
    ) -> Result<(), Error> {
        let mut counter = [0u8; 8];
        counter.copy_from_slice(&nonce[4..]);
        let counter = u64::from_be_bytes(counter);
        if self.last.is_some_and(|last| counter <= last) {
            return Err(Error);
        }
        let tag = self
            .key
            .encrypt_inout_detached(&Nonce::from(nonce), aad, InOutBuf::from(&mut buf[..]))
            .map_err(|_| Error)?;
        buf.extend_from_slice(&tag);
        self.last = Some(counter);
        Ok(())
    }
}
