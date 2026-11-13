//! End-to-end encryption with an authenticated server.
//!
//! The client offers suites, an ephemeral P-256 key and 32 random bytes in
//! `client_hello.p.e2e`. The server answers with its own ephemeral key and
//! signs the exchange (`transcript`) with its identity key, so a client that
//! pins that key knows no one in between (a TLS-terminating proxy) answered
//! in its place. Both sides then derive one key schedule per direction, bound
//! to the transcript, and every frame carries its direction's epoch and
//! sequence number: a receiver takes the next frame only, so replayed,
//! reordered, dropped, reflected or foreign frames end the connection.
//!
//! Frame: `E:` | format 0x01 | type | epoch u32be | seq u64be | ciphertext |
//! tag. Nonce: the epoch's 4-byte IV | seq. AAD: direction | TH | header.

use std::time::{Duration, Instant};

use base64::Engine;
use zeroize::Zeroizing;

use crate::crypto::{self, Aes256Gcm, IdentityKey};

/// The one suite: ECDH P-256, HKDF-SHA256, AES-256-GCM.
pub(crate) const SUITE: &str = "p256-aes256gcm";
/// The identity signature: ECDSA P-256 with SHA-256, `r || s`.
pub(crate) const SIGNATURE_ALG: &str = "ES256";

const TRANSCRIPT_LABEL: &[u8] = b"WSE-E2E server_hello\0";
const FRAME_FORMAT: u8 = 0x01;
pub(crate) const FRAME_DATA: u8 = 0;
const FRAME_CLOSE: u8 = 1;
const HEADER_LEN: usize = 14;
const TAG_LEN: usize = 16;
/// Frames per direction over the session: a JavaScript Number holds them.
const SEQ_MAX: u64 = (1 << 53) - 1;
/// A sender moves to the next epoch's key after this many frames...
const REKEY_FRAMES: u64 = 1 << 20;
/// ...or this long.
const REKEY_AFTER: Duration = Duration::from_secs(600);
/// A receiver refuses more frames than this under one epoch's key.
const MAX_EPOCH_FRAMES: u64 = 1 << 24;

const CLIENT_TO_SERVER: u8 = 0x01;
const SERVER_TO_CLIENT: u8 = 0x02;

/// Why a handshake or a frame was refused. The peer learns only the close
/// code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum E2eError {
    /// No suite in common, or a malformed offer.
    Unsupported,
    /// A frame that is not the next one, or does not open.
    BadFrame,
    /// The direction's frame counter is used up: reconnect.
    Exhausted,
}

/// What the client offered.
pub(crate) struct Offer {
    suites: Vec<String>,
    client_public: Vec<u8>,
    client_random: [u8; 32],
}

/// Read `client_hello.p.e2e`.
pub(crate) fn parse_offer(e2e: &serde_json::Value) -> Result<Offer, E2eError> {
    let b64 = base64::engine::general_purpose::STANDARD;
    let suites: Vec<String> = e2e
        .get("suites")
        .and_then(serde_json::Value::as_array)
        .ok_or(E2eError::Unsupported)?
        .iter()
        .filter_map(|s| s.as_str().map(str::to_owned))
        .filter(|s| s.len() <= 64)
        .take(16)
        .collect();
    let client_public = e2e
        .get("client_public_key")
        .and_then(serde_json::Value::as_str)
        .and_then(|k| b64.decode(k).ok())
        .filter(|k| k.len() == crypto::P256_PUBLIC_LEN)
        .ok_or(E2eError::Unsupported)?;
    let client_random: [u8; 32] = e2e
        .get("client_random")
        .and_then(serde_json::Value::as_str)
        .and_then(|r| b64.decode(r).ok())
        .and_then(|r| r.try_into().ok())
        .ok_or(E2eError::Unsupported)?;
    Ok(Offer {
        suites,
        client_public,
        client_random,
    })
}

/// The bytes the server signs, and both sides hash into TH.
pub(crate) fn transcript(
    suites: &[String],
    chosen: &str,
    client_random: &[u8; 32],
    client_public: &[u8],
    server_public: &[u8],
    connection_id: &str,
    key_id: &[u8; 8],
) -> Vec<u8> {
    let mut t = Vec::with_capacity(256);
    t.extend_from_slice(TRANSCRIPT_LABEL);
    t.push(suites.len() as u8);
    for suite in suites {
        t.push(suite.len() as u8);
        t.extend_from_slice(suite.as_bytes());
    }
    t.push(chosen.len() as u8);
    t.extend_from_slice(chosen.as_bytes());
    t.extend_from_slice(client_random);
    t.extend_from_slice(client_public);
    t.extend_from_slice(server_public);
    let id = &connection_id.as_bytes()[..connection_id.len().min(255)];
    t.push(id.len() as u8);
    t.extend_from_slice(id);
    t.extend_from_slice(key_id);
    t
}

/// The first 8 bytes of SHA-256 of an identity public key.
pub(crate) fn key_id(public_key: &[u8]) -> [u8; 8] {
    let mut id = [0u8; 8];
    id.copy_from_slice(&crypto::sha256(public_key)[..8]);
    id
}

/// HKDF-SHA256 with TH as the salt.
fn derive<const N: usize>(th: &[u8; 32], ikm: &[u8], label: &[u8]) -> Zeroizing<[u8; N]> {
    let mut out = Zeroizing::new([0u8; N]);
    crypto::hkdf_sha256(th, ikm, label, &mut out[..]).expect("HKDF output within limits");
    out
}

/// The key and IV of one epoch of one direction.
struct EpochKeys {
    key: Aes256Gcm,
    iv: [u8; 4],
}

impl EpochKeys {
    fn new(th: &[u8; 32], secret: &[u8; 32]) -> Self {
        let key = derive::<32>(th, secret, b"wse/e2e key");
        let iv = derive::<4>(th, secret, b"wse/e2e iv");
        Self {
            key: Aes256Gcm::new(&key[..]).expect("32-byte key"),
            iv: *iv,
        }
    }

    fn nonce(&self, seq: u64) -> [u8; crypto::NONCE_LEN] {
        let mut nonce = [0u8; crypto::NONCE_LEN];
        nonce[..4].copy_from_slice(&self.iv);
        nonce[4..].copy_from_slice(&seq.to_be_bytes());
        nonce
    }
}

fn next_secret(th: &[u8; 32], secret: &[u8; 32]) -> Zeroizing<[u8; 32]> {
    derive::<32>(th, secret, b"wse/e2e next")
}

fn header(frame_type: u8, epoch: u32, seq: u64) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0] = FRAME_FORMAT;
    h[1] = frame_type;
    h[2..6].copy_from_slice(&epoch.to_be_bytes());
    h[6..].copy_from_slice(&seq.to_be_bytes());
    h
}

fn aad(direction: u8, th: &[u8; 32], hdr: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(1 + 32 + HEADER_LEN);
    aad.push(direction);
    aad.extend_from_slice(th);
    aad.extend_from_slice(hdr);
    aad
}

/// Sealing for one direction. Owned by the connection's write task alone,
/// so sequence numbers follow the order on the wire.
pub(crate) struct SendState {
    th: [u8; 32],
    direction: u8,
    secret: Zeroizing<[u8; 32]>,
    keys: EpochKeys,
    epoch: u32,
    seq: u64,
    epoch_frames: u64,
    epoch_started: Instant,
}

impl SendState {
    fn new(th: [u8; 32], direction: u8, secret: Zeroizing<[u8; 32]>) -> Self {
        let keys = EpochKeys::new(&th, &secret);
        Self {
            th,
            direction,
            secret,
            keys,
            epoch: 0,
            seq: 0,
            epoch_frames: 0,
            epoch_started: Instant::now(),
        }
    }

    /// `plaintext` as the body of a binary frame (`E:` and all).
    pub(crate) fn seal(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, E2eError> {
        self.seal_typed(FRAME_DATA, plaintext)
    }

    fn seal_typed(&mut self, frame_type: u8, plaintext: &[u8]) -> Result<Vec<u8>, E2eError> {
        if self.seq > SEQ_MAX {
            return Err(E2eError::Exhausted);
        }
        if self.epoch_frames >= REKEY_FRAMES || self.epoch_started.elapsed() >= REKEY_AFTER {
            self.epoch = self.epoch.checked_add(1).ok_or(E2eError::Exhausted)?;
            self.secret = next_secret(&self.th, &self.secret);
            self.keys = EpochKeys::new(&self.th, &self.secret);
            self.epoch_frames = 0;
            self.epoch_started = Instant::now();
        }
        let hdr = header(frame_type, self.epoch, self.seq);
        let mut out = Vec::with_capacity(2 + HEADER_LEN + plaintext.len() + TAG_LEN);
        out.extend_from_slice(b"E:");
        out.extend_from_slice(&hdr);
        let mut body = plaintext.to_vec();
        self.keys
            .key
            .seal(
                self.keys.nonce(self.seq),
                &aad(self.direction, &self.th, &hdr),
                &mut body,
            )
            .map_err(|_| E2eError::BadFrame)?;
        out.extend_from_slice(&body);
        self.seq += 1;
        self.epoch_frames += 1;
        Ok(out)
    }
}

/// Opening for one direction, strictly in order. Owned by the read loop.
pub(crate) struct RecvState {
    th: [u8; 32],
    direction: u8,
    secret: Zeroizing<[u8; 32]>,
    keys: EpochKeys,
    epoch: u32,
    expected_seq: u64,
    epoch_frames: u64,
}

impl RecvState {
    fn new(th: [u8; 32], direction: u8, secret: Zeroizing<[u8; 32]>) -> Self {
        let keys = EpochKeys::new(&th, &secret);
        Self {
            th,
            direction,
            secret,
            keys,
            epoch: 0,
            expected_seq: 0,
            epoch_frames: 0,
        }
    }

    /// Open the body of a binary frame (`E:` and all). Only the next frame
    /// of the stream opens; anything else is an error that must end the
    /// connection. A close frame type opens to `None`.
    pub(crate) fn open(&mut self, frame: &[u8]) -> Result<Option<Vec<u8>>, E2eError> {
        let body = frame.strip_prefix(b"E:").ok_or(E2eError::BadFrame)?;
        if body.len() < HEADER_LEN + TAG_LEN {
            return Err(E2eError::BadFrame);
        }
        let (hdr, sealed) = body.split_at(HEADER_LEN);
        let frame_type = hdr[1];
        let epoch = u32::from_be_bytes(hdr[2..6].try_into().expect("4 bytes"));
        let seq = u64::from_be_bytes(hdr[6..].try_into().expect("8 bytes"));
        if hdr[0] != FRAME_FORMAT
            || !matches!(frame_type, FRAME_DATA | FRAME_CLOSE)
            || seq != self.expected_seq
            || seq > SEQ_MAX
        {
            return Err(E2eError::BadFrame);
        }
        // The next epoch is derived on trial and kept only if the tag holds.
        let next = if epoch == self.epoch {
            if self.epoch_frames >= MAX_EPOCH_FRAMES {
                return Err(E2eError::BadFrame);
            }
            None
        } else if Some(epoch) == self.epoch.checked_add(1) {
            let secret = next_secret(&self.th, &self.secret);
            let keys = EpochKeys::new(&self.th, &secret);
            Some((secret, keys))
        } else {
            return Err(E2eError::BadFrame);
        };
        let keys = next.as_ref().map_or(&self.keys, |(_, keys)| keys);
        let mut buf = sealed.to_vec();
        let len = keys
            .key
            .open(
                keys.nonce(seq),
                &aad(self.direction, &self.th, hdr),
                &mut buf,
            )
            .map_err(|_| E2eError::BadFrame)?
            .len();
        buf.truncate(len);
        if let Some((secret, keys)) = next {
            self.secret = secret;
            self.keys = keys;
            self.epoch = epoch;
            self.epoch_frames = 0;
        }
        self.expected_seq += 1;
        self.epoch_frames += 1;
        Ok((frame_type == FRAME_DATA).then_some(buf))
    }
}

/// The server's half of a handshake: `server_hello.p.e2e`, and the states of
/// both directions.
pub(crate) struct Accepted {
    pub(crate) hello: serde_json::Value,
    pub(crate) send: SendState,
    pub(crate) recv: RecvState,
}

/// Answer an offer: a fresh ephemeral key, the transcript signed with the
/// server's identity (when it has one), and the key schedule.
pub(crate) fn accept(
    offer: &Offer,
    connection_id: &str,
    identity: Option<&IdentityKey>,
) -> Result<Accepted, E2eError> {
    if !offer.suites.iter().any(|s| s == SUITE) {
        return Err(E2eError::Unsupported);
    }
    let (server_public, z) =
        crypto::ecdh_ephemeral(&offer.client_public, |z| Ok(Zeroizing::new(z.to_vec())))
            .map_err(|_| E2eError::Unsupported)?;
    let kid = identity.map_or([0u8; 8], |k| key_id(k.public_key()));
    let t = transcript(
        &offer.suites,
        SUITE,
        &offer.client_random,
        &offer.client_public,
        &server_public,
        connection_id,
        &kid,
    );
    let th = crypto::sha256(&t);
    let signature = identity
        .map(|k| k.sign(&t))
        .transpose()
        .map_err(|_| E2eError::Unsupported)?;
    let c2s = derive::<32>(&th, &z, b"wse/e2e c2s");
    let s2c = derive::<32>(&th, &z, b"wse/e2e s2c");
    let b64 = base64::engine::general_purpose::STANDARD;
    let hello = serde_json::json!({
        "suite": SUITE,
        "server_public_key": b64.encode(&server_public),
        "key_id": hex::encode(kid),
        "alg": identity.map(|_| SIGNATURE_ALG),
        "signature": signature.map(|s| b64.encode(s)),
    });
    Ok(Accepted {
        hello,
        send: SendState::new(th, SERVER_TO_CLIENT, s2c),
        recv: RecvState::new(th, CLIENT_TO_SERVER, c2s),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(th: [u8; 32], secret: [u8; 32]) -> (SendState, RecvState) {
        (
            SendState::new(th, SERVER_TO_CLIENT, Zeroizing::new(secret)),
            RecvState::new(th, SERVER_TO_CLIENT, Zeroizing::new(secret)),
        )
    }

    /// Vectors made with the `cryptography` package (tests/vectors), shared
    /// with both clients' tests.
    #[test]
    fn the_shared_vectors_hold() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let v: serde_json::Value =
            serde_json::from_str(include_str!("../../../../tests/vectors/e2e.json")).unwrap();
        let bytes = |k: &str| b64.decode(v[k].as_str().unwrap()).unwrap();
        let hexed = |k: &str| hex::decode(v[k].as_str().unwrap()).unwrap();
        let suites: Vec<String> = v["suites"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_owned())
            .collect();
        let kid = key_id(&bytes("identity_public_key"));
        assert_eq!(hex::encode(kid), v["key_id"].as_str().unwrap());
        let t = transcript(
            &suites,
            SUITE,
            &bytes("client_random").try_into().unwrap(),
            &bytes("client_public_key"),
            &bytes("server_public_key"),
            v["connection_id"].as_str().unwrap(),
            &kid,
        );
        assert_eq!(t, hexed("transcript"));
        assert!(crypto::verify_p256(
            &bytes("identity_public_key"),
            &t,
            &bytes("signature")
        ));
        let th = crypto::sha256(&t);
        assert_eq!(th.to_vec(), hexed("th"));
        let z = hexed("shared_secret");
        let c2s = derive::<32>(&th, &z, b"wse/e2e c2s");
        let s2c = derive::<32>(&th, &z, b"wse/e2e s2c");
        assert_eq!(c2s.to_vec(), hexed("client_to_server_secret"));
        assert_eq!(s2c.to_vec(), hexed("server_to_client_secret"));
        for (dir, name, secret) in [
            (SERVER_TO_CLIENT, "server_to_client", s2c),
            (CLIENT_TO_SERVER, "client_to_server", c2s),
        ] {
            let mut send = SendState::new(th, dir, secret.clone());
            let mut recv = RecvState::new(th, dir, secret);
            for (i, f) in v[name].as_array().unwrap().iter().enumerate() {
                if i == 2 {
                    send.epoch_frames = REKEY_FRAMES;
                }
                let text = f["plaintext"].as_str().unwrap().as_bytes();
                let frame = hex::decode(f["frame"].as_str().unwrap()).unwrap();
                assert_eq!(send.seal(text).unwrap(), frame, "{name} frame {i}");
                assert_eq!(recv.open(&frame).unwrap().unwrap(), text);
            }
        }
    }

    #[test]
    fn frames_open_in_order_only() {
        let (mut send, mut recv) = pair([7; 32], [9; 32]);
        let one = send.seal(b"one").unwrap();
        let two = send.seal(b"two").unwrap();
        assert_eq!(recv.open(&one).unwrap().unwrap(), b"one");
        // Replay, then skip: both refused.
        assert_eq!(recv.open(&one), Err(E2eError::BadFrame));
        let three = send.seal(b"three").unwrap();
        assert_eq!(recv.open(&three), Err(E2eError::BadFrame));
        assert_eq!(recv.open(&two).unwrap().unwrap(), b"two");
    }

    #[test]
    fn a_tampered_or_reflected_frame_is_refused() {
        let (mut send, mut recv) = pair([1; 32], [2; 32]);
        let mut frame = send.seal(b"hello").unwrap();
        *frame.last_mut().unwrap() ^= 1;
        assert_eq!(recv.open(&frame), Err(E2eError::BadFrame));
        // Same secret, other direction: the AAD differs.
        let mut other = SendState::new([1; 32], CLIENT_TO_SERVER, Zeroizing::new([2; 32]));
        let reflected = other.seal(b"hello").unwrap();
        let (_, mut fresh) = pair([1; 32], [2; 32]);
        assert_eq!(fresh.open(&reflected), Err(E2eError::BadFrame));
    }

    #[test]
    fn the_receiver_follows_a_rekey() {
        let (mut send, mut recv) = pair([3; 32], [4; 32]);
        for i in 0..3u8 {
            let frame = send.seal(&[i]).unwrap();
            assert_eq!(recv.open(&frame).unwrap().unwrap(), [i]);
        }
        // Force the next frame into epoch 1.
        send.epoch_frames = REKEY_FRAMES;
        let frame = send.seal(b"next epoch").unwrap();
        assert_eq!(u32::from_be_bytes(frame[4..8].try_into().unwrap()), 1);
        assert_eq!(recv.open(&frame).unwrap().unwrap(), b"next epoch");
        assert_eq!(recv.epoch, 1);
        let frame = send.seal(b"after").unwrap();
        assert_eq!(recv.open(&frame).unwrap().unwrap(), b"after");
    }

    #[test]
    fn the_handshake_is_signed_and_both_sides_agree() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let rng = aws_lc_rs::rand::SystemRandom::new();
        let pkcs8 = aws_lc_rs::signature::EcdsaKeyPair::generate_pkcs8(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &rng,
        )
        .unwrap();
        let identity = IdentityKey::from_pkcs8(pkcs8.as_ref()).unwrap();
        let (client_secret, client_public) = crypto::ecdh_generate().unwrap();
        let offer = serde_json::json!({
            "suites": ["other", SUITE],
            "client_public_key": b64.encode(&client_public),
            "client_random": b64.encode([5u8; 32]),
        });
        let accepted = accept(&parse_offer(&offer).unwrap(), "conn-1", Some(&identity)).unwrap();
        let hello = &accepted.hello;
        let server_public = b64
            .decode(hello["server_public_key"].as_str().unwrap())
            .unwrap();
        // The client's side: rebuild the transcript, check the signature.
        let kid = key_id(identity.public_key());
        assert_eq!(hello["key_id"], hex::encode(kid));
        let t = transcript(
            &["other".into(), SUITE.into()],
            SUITE,
            &[5u8; 32],
            &client_public,
            &server_public,
            "conn-1",
            &kid,
        );
        let sig = b64.decode(hello["signature"].as_str().unwrap()).unwrap();
        assert!(crypto::verify_p256(identity.public_key(), &t, &sig));
        // Same keys on both sides.
        let th = crypto::sha256(&t);
        let s2c = crypto::ecdh_agree(&client_secret, &server_public, |z| {
            Ok(derive::<32>(&th, z, b"wse/e2e s2c"))
        })
        .unwrap();
        let mut client_recv = RecvState::new(th, SERVER_TO_CLIENT, s2c);
        let mut server_send = accepted.send;
        let frame = server_send.seal(b"{\"t\":\"hi\"}").unwrap();
        assert_eq!(
            client_recv.open(&frame).unwrap().unwrap(),
            b"{\"t\":\"hi\"}"
        );
        // An offer without the suite is refused.
        let none = serde_json::json!({
            "suites": ["other"],
            "client_public_key": b64.encode(&client_public),
            "client_random": b64.encode([5u8; 32]),
        });
        assert!(accept(&parse_offer(&none).unwrap(), "c", None).is_err());
    }
}
