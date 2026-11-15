//! Signed messages: a message that travels as the exact text its sender signed,
//! next to the signature and the id of the signing key, so whoever keeps it can
//! show later who sent what.
//!
//! On the wire: `{"t": "signed", "sb": "<body>", "sig": "<signature>", "kid":
//! "<key id>"}`. The body is a JSON object as text, signed byte for byte and
//! never re-encoded: the server's conversions (msgpack, recovery stamps, the
//! category field) touch only the outer object. The signature is ECDSA P-256 /
//! SHA-256, `r || s` in base64, over a label naming the signer's side and the
//! body. A key id is the first 8 bytes of SHA-256 of the uncompressed public
//! key, in hex.
//!
//! A client's body: `t`, `p`, `aud` (the id of the server's signing key, so
//! the message is good for this deployment only), `ts` (milliseconds) and `n`
//! (unique per message). The server takes it once, within its time window.
//! The server's body: `t`, `p`, `ts`, `n`, and `tp` for a topic message.

use base64::Engine;
use serde_json::Value;

use crate::crypto::{self, IdentityKey};

/// What a client's signature covers before the body.
pub const CLIENT_LABEL: &[u8] = b"WSE-SIG client\0";
/// What the server's signature covers before the body.
pub const SERVER_LABEL: &[u8] = b"WSE-SIG server\0";

/// The longest `n` the server takes, in characters.
pub const MAX_NONCE_LEN: usize = 128;

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// A key id: the first 8 bytes of SHA-256 of an uncompressed public key, hex.
pub fn key_id(public_key: &[u8]) -> String {
    hex::encode(&crypto::sha256(public_key)[..8])
}

/// Now, in milliseconds since the Unix epoch: the clock of `ts`.
pub fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// What a signature covers: the signer's label, then the body.
pub fn signed_input(label: &[u8], body: &str) -> Vec<u8> {
    let mut input = Vec::with_capacity(label.len() + body.len());
    input.extend_from_slice(label);
    input.extend_from_slice(body.as_bytes());
    input
}

/// A public key that signed messages are checked against, parsed once: a
/// client's on the server, the server's on a client.
pub struct PublicKey {
    pub kid: String,
    key: crypto::VerifyingKey,
    point: Vec<u8>,
}

impl PublicKey {
    /// From an uncompressed SEC1 point, `0x04 || X || Y`.
    pub fn from_sec1(public_key: &[u8]) -> Option<Self> {
        if public_key.len() != crypto::P256_PUBLIC_LEN || public_key[0] != 0x04 {
            return None;
        }
        let key = crypto::VerifyingKey::from_sec1(public_key)?;
        Some(Self {
            kid: key_id(public_key),
            key,
            point: public_key.to_vec(),
        })
    }

    /// The uncompressed point, `0x04 || X || Y`.
    pub fn point(&self) -> &[u8] {
        &self.point
    }

    /// From the base64 of an uncompressed SEC1 point.
    pub fn from_base64(public_key: &str) -> Option<Self> {
        Self::from_sec1(&b64().decode(public_key.trim()).ok()?)
    }

    /// From a JWK (`{"kty": "EC", "crv": "P-256", "x": ..., "y": ...}`), the
    /// form a token's `cnf` claim carries (RFC 7800).
    pub fn from_jwk(jwk: &Value) -> Option<Self> {
        if jwk.get("kty")?.as_str()? != "EC" || jwk.get("crv")?.as_str()? != "P-256" {
            return None;
        }
        let coordinate = |name: &str| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(jwk.get(name)?.as_str()?)
                .ok()
                .filter(|bytes| bytes.len() == 32)
        };
        let mut point = Vec::with_capacity(crypto::P256_PUBLIC_LEN);
        point.push(0x04);
        point.extend_from_slice(&coordinate("x")?);
        point.extend_from_slice(&coordinate("y")?);
        Self::from_sec1(&point)
    }

    /// Whether `signature` (`r || s`) signs `label` and `body` with this key.
    pub fn verifies(&self, label: &[u8], body: &str, signature: &[u8]) -> bool {
        self.key.verifies(&signed_input(label, body), signature)
    }
}

/// The key a token binds its holder to (`cnf`, RFC 7800): the key itself
/// (`jwk`), or its thumbprint (`jkt`, RFC 7638) with the key coming in the
/// client's proof.
pub enum Binding {
    Key(PublicKey),
    Thumbprint(String),
}

impl Binding {
    /// A token's binding: none without `cnf`. A `cnf` that names no P-256
    /// key is an error: its holder could not be told, and the token must
    /// not pass as one that binds nothing.
    pub fn from_claims(claims: &Value) -> Result<Option<Self>, BadBinding> {
        let Some(cnf) = claims.get("cnf") else {
            return Ok(None);
        };
        if let Some(jwk) = cnf.get("jwk") {
            return PublicKey::from_jwk(jwk)
                .map(|key| Some(Self::Key(key)))
                .ok_or(BadBinding);
        }
        match cnf.get("jkt").and_then(Value::as_str) {
            Some(jkt) if is_thumbprint(jkt) => Ok(Some(Self::Thumbprint(jkt.to_owned()))),
            _ => Err(BadBinding),
        }
    }

    /// Whether `point` (uncompressed) is the bound key.
    pub fn holds(&self, point: &[u8]) -> bool {
        match self {
            Self::Key(key) => key.point() == point,
            Self::Thumbprint(jkt) => thumbprint(point).is_some_and(|t| t == *jkt),
        }
    }

    /// The bound key's thumbprint.
    pub fn thumbprint(&self) -> String {
        match self {
            Self::Key(key) => thumbprint(key.point()).unwrap_or_default(),
            Self::Thumbprint(jkt) => jkt.clone(),
        }
    }
}

/// A token's `cnf` that names no P-256 key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadBinding;

/// The RFC 7638 thumbprint of a P-256 public key (an uncompressed point):
/// base64url, unpadded, of SHA-256 of its JWK's required members in order.
pub fn thumbprint(point: &[u8]) -> Option<String> {
    if point.len() != crypto::P256_PUBLIC_LEN || point[0] != 0x04 {
        return None;
    }
    let b64u = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let jwk = format!(
        r#"{{"crv":"P-256","kty":"EC","x":"{}","y":"{}"}}"#,
        b64u.encode(&point[1..33]),
        b64u.encode(&point[33..])
    );
    Some(b64u.encode(crypto::sha256(jwk.as_bytes())))
}

fn is_thumbprint(jkt: &str) -> bool {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(jkt)
        .is_ok_and(|digest| digest.len() == 32)
}

/// A key that signs messages: the server's, or a client's.
pub struct Signer {
    key: IdentityKey,
    /// The key's id.
    pub kid: String,
    /// The public key, base64 of the uncompressed point.
    pub public_key: String,
}

impl Signer {
    /// The RFC 7638 thumbprint of this key: what a token names it by
    /// (`cnf.jkt`), and what the server confirms a session is bound to.
    pub fn thumbprint(&self) -> String {
        thumbprint(self.key.public_key()).unwrap_or_default()
    }

    /// The proof of this key over an end-to-end session (`client_auth`):
    /// what binds the session to a token naming the key.
    pub fn prove_session(&self, th: &[u8; 32]) -> Result<String, crate::e2e::E2eError> {
        crate::e2e::client_auth(th, &self.key)
    }

    /// From a PKCS#8 ECDSA P-256 private key, PEM or DER.
    pub fn from_pkcs8(key: &[u8]) -> Result<Self, crypto::Error> {
        let key = IdentityKey::from_pkcs8(key)?;
        let public = key.public_key();
        Ok(Self {
            kid: key_id(public),
            public_key: b64().encode(public),
            key,
        })
    }

    /// The signed message around `body`, a JSON object as text, signed under
    /// the signer's `label`.
    pub fn sign_envelope(&self, label: &[u8], body: &str) -> Result<String, crypto::Error> {
        let signature = self.key.sign(&signed_input(label, body))?;
        Ok(serde_json::json!({
            "t": "signed",
            "sb": body,
            "sig": b64().encode(signature),
            "kid": self.kid,
        })
        .to_string())
    }
}

/// A signed body as the application gets it, read so that it cannot say
/// anything but its text: no key twice in an object (readers differ on which
/// one counts) and every integer exact (past 64 bits it would come out
/// rounded here and exact elsewhere). None when it is not such JSON.
pub fn parse_exact(body: &str) -> Option<Value> {
    if !integers_exact(body) {
        return None;
    }
    let mut de = serde_json::Deserializer::from_str(body);
    let Exact(value) = serde::Deserialize::deserialize(&mut de).ok()?;
    de.end().ok()?;
    Some(value)
}

/// Every integer literal of a JSON text (outside its strings) fits 64 bits.
fn integers_exact(body: &str) -> bool {
    let bytes = body.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            match b {
                b'\\' => i += 1,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => {
                in_string = true;
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                let literal = &body[start..i];
                if !literal.contains(['.', 'e', 'E'])
                    && literal.parse::<i64>().is_err()
                    && literal.parse::<u64>().is_err()
                {
                    return false;
                }
            }
            _ => i += 1,
        }
    }
    true
}

/// JSON read into a `Value`, refusing an object with a key twice.
struct Exact(Value);

impl<'de> serde::Deserialize<'de> for Exact {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        de.deserialize_any(ExactVisitor).map(Exact)
    }
}

struct ExactVisitor;

impl<'de> serde::de::Visitor<'de> for ExactVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JSON")
    }
    fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }
    fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_u64<E>(self, v: u64) -> Result<Value, E> {
        Ok(v.into())
    }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Value, E> {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .ok_or_else(|| E::custom("not a finite number"))
    }
    fn visit_str<E>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_owned()))
    }
    fn visit_string<E>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }
    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(Exact(item)) = seq.next_element()? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut object = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(serde::de::Error::custom("a key twice"));
            }
            let Exact(value) = map.next_value()?;
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (IdentityKey, PublicKey) {
        let pair = IdentityKey::from_pkcs8(&crypto::generate_p256_pkcs8().unwrap()).unwrap();
        let key = PublicKey::from_sec1(pair.public_key()).unwrap();
        (pair, key)
    }

    #[test]
    fn a_jwk_names_the_same_key_as_its_point() {
        let (_, key) = pair();
        let (other, _) = pair();
        let point = other.public_key();
        let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let jwk = serde_json::json!({
            "kty": "EC", "crv": "P-256",
            "x": url.encode(&point[1..33]), "y": url.encode(&point[33..]),
        });
        let from_jwk = PublicKey::from_jwk(&jwk).unwrap();
        assert_eq!(from_jwk.kid, key_id(point));
        assert_ne!(from_jwk.kid, key.kid);
        let claims = serde_json::json!({"sub": "u", "cnf": {"jwk": jwk}});
        let Ok(Some(Binding::Key(bound))) = Binding::from_claims(&claims) else {
            panic!("bound by its jwk");
        };
        assert_eq!(bound.kid, from_jwk.kid);
        assert!(
            PublicKey::from_jwk(&serde_json::json!({"kty": "OKP", "crv": "Ed25519"})).is_none()
        );
        assert!(PublicKey::from_base64("not base64!").is_none());
    }

    #[test]
    fn a_thumbprint_is_rfc_7638s() {
        // The key and thumbprint of RFC 9449's examples.
        let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let mut point = vec![0x04];
        point.extend(
            url.decode("l8tFrhx-34tV3hRICRDY9zCkDlpBhF42UQUfWVAWBFs")
                .unwrap(),
        );
        point.extend(
            url.decode("9VE4jf_Ok_o64zbTTlcuNJajHmt6v9TDVrU0CdvGRDA")
                .unwrap(),
        );
        assert_eq!(
            thumbprint(&point).as_deref(),
            Some("0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I")
        );
        assert_eq!(thumbprint(&point[1..]), None);
    }

    #[test]
    fn a_token_binds_by_key_or_thumbprint_and_nothing_else() {
        let (pair, key) = pair();
        let point = pair.public_key();
        let jkt = thumbprint(point).unwrap();
        let claims = |cnf: Value| serde_json::json!({"sub": "u", "cnf": cnf});
        let by_jkt = Binding::from_claims(&claims(serde_json::json!({"jkt": jkt})))
            .unwrap()
            .unwrap();
        assert!(by_jkt.holds(point));
        assert_eq!(by_jkt.thumbprint(), jkt);
        let (other, _) = super::tests::pair();
        assert!(!by_jkt.holds(other.public_key()));
        let by_key = Binding::Key(key);
        assert!(by_key.holds(point));
        assert_eq!(by_key.thumbprint(), jkt);
        // No cnf: nothing bound. A cnf naming no P-256 key: refused.
        assert!(matches!(
            Binding::from_claims(&serde_json::json!({"sub": "u"})),
            Ok(None)
        ));
        for bad in [
            serde_json::json!({"jkt": "short"}),
            serde_json::json!({"jkt": 7}),
            serde_json::json!({"jwk": {"kty": "OKP", "crv": "Ed25519", "x": "AA"}}),
            serde_json::json!({"x5t#S256": "AAAA"}),
            serde_json::json!("a string"),
        ] {
            assert_eq!(
                Binding::from_claims(&claims(bad.clone())).err(),
                Some(BadBinding),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_session_proof_holds_for_its_session_and_key_only() {
        let pkcs8 = crypto::generate_p256_pkcs8().unwrap();
        let signer = Signer::from_pkcs8(&pkcs8).unwrap();
        let th = [7u8; 32];
        let message: Value = serde_json::from_str(&signer.prove_session(&th).unwrap()).unwrap();
        assert_eq!(message["t"], "client_auth");
        let proved = crate::e2e::check_client_auth(&th, &message["p"]).unwrap();
        assert_eq!(key_id(&proved), signer.kid);
        // Another session's transcript: refused.
        assert!(crate::e2e::check_client_auth(&[8u8; 32], &message["p"]).is_none());
        // A signature under another label for the same bytes: refused.
        let mut p = message["p"].clone();
        let input = crate::e2e::client_auth_input(&th, &proved);
        let mut relabeled = CLIENT_LABEL.to_vec();
        relabeled.extend_from_slice(&input[crate::e2e::CLIENT_AUTH_LABEL.len()..]);
        let other_label = IdentityKey::from_pkcs8(&pkcs8)
            .unwrap()
            .sign(&relabeled)
            .unwrap();
        p["sig"] = b64().encode(other_label).into();
        assert!(crate::e2e::check_client_auth(&th, &p).is_none());
        // Another key named: the signature is not its.
        let (other, _) = pair();
        let mut p = message["p"].clone();
        p["key"] = b64().encode(other.public_key()).into();
        assert!(crate::e2e::check_client_auth(&th, &p).is_none());
    }

    #[test]
    fn a_signer_signs_under_its_label_only() {
        let pkcs8 = crypto::generate_p256_pkcs8().unwrap();
        let signer = Signer::from_pkcs8(&pkcs8).unwrap();
        let body = r#"{"t":"order","ts":1,"n":"a"}"#;
        let env: Value =
            serde_json::from_str(&signer.sign_envelope(CLIENT_LABEL, body).unwrap()).unwrap();
        assert_eq!(
            (env["sb"].as_str(), env["kid"].as_str()),
            (Some(body), Some(signer.kid.as_str()))
        );
        let key = PublicKey::from_base64(&signer.public_key).unwrap();
        let sig = b64().decode(env["sig"].as_str().unwrap()).unwrap();
        assert!(key.verifies(CLIENT_LABEL, body, &sig));
        assert!(!key.verifies(SERVER_LABEL, body, &sig));
    }

    #[test]
    fn a_body_reads_one_way_only() {
        assert!(parse_exact(r#"{"a":1,"a":2}"#).is_none());
        assert!(parse_exact(r#"{"a":12345678901234567890123}"#).is_none());
        assert!(parse_exact(r#"{"a":"12345678901234567890123","b":-1.5e3}"#).is_some());
    }
}
