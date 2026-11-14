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

use base64::Engine;
use serde_json::{Map, Value};

use crate::crypto::{self, IdentityKey};

/// What a client's signature covers before the body.
pub(crate) const CLIENT_LABEL: &[u8] = b"WSE-SIG client\0";
/// What the server's signature covers before the body.
pub(crate) const SERVER_LABEL: &[u8] = b"WSE-SIG server\0";

/// The longest `n` the server takes.
const MAX_NONCE_LEN: usize = 128;

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// A key id: the first 8 bytes of SHA-256 of an uncompressed public key, hex.
pub(crate) fn key_id(public_key: &[u8]) -> String {
    hex::encode(&crypto::sha256(public_key)[..8])
}

/// Now, in milliseconds since the Unix epoch: the clock of `ts`.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn signed_input(label: &[u8], body: &str) -> Vec<u8> {
    let mut input = Vec::with_capacity(label.len() + body.len());
    input.extend_from_slice(label);
    input.extend_from_slice(body.as_bytes());
    input
}

/// A client's public key for its signed messages, parsed once.
pub(crate) struct ClientKey {
    pub(crate) kid: String,
    key: aws_lc_rs::signature::ParsedPublicKey,
}

impl ClientKey {
    /// From an uncompressed SEC1 point, `0x04 || X || Y`.
    pub(crate) fn from_sec1(public_key: &[u8]) -> Option<Self> {
        if public_key.len() != crypto::P256_PUBLIC_LEN || public_key[0] != 0x04 {
            return None;
        }
        let key = aws_lc_rs::signature::ParsedPublicKey::new(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED,
            public_key,
        )
        .ok()?;
        Some(Self {
            kid: key_id(public_key),
            key,
        })
    }

    /// From the base64 of an uncompressed SEC1 point.
    pub(crate) fn from_base64(public_key: &str) -> Option<Self> {
        Self::from_sec1(&b64().decode(public_key.trim()).ok()?)
    }

    /// From a JWK (`{"kty": "EC", "crv": "P-256", "x": ..., "y": ...}`), the
    /// form a token's `cnf` claim carries (RFC 7800).
    pub(crate) fn from_jwk(jwk: &Value) -> Option<Self> {
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

    /// The key a token binds to its holder: `cnf.jwk`. None when the claim is
    /// missing or not a P-256 key.
    pub(crate) fn from_claims(claims: &Value) -> Option<Self> {
        Self::from_jwk(claims.get("cnf")?.get("jwk")?)
    }

    fn verifies(&self, label: &[u8], body: &str, signature: &[u8]) -> bool {
        self.key
            .verify_sig(&signed_input(label, body), signature)
            .is_ok()
    }
}

/// The server's key for the messages it signs: its own key, apart from the
/// E2E identity key.
pub(crate) struct ServerSigner {
    key: IdentityKey,
    pub(crate) kid: String,
    /// The public key, base64 of the uncompressed point.
    pub(crate) public_key: String,
}

impl ServerSigner {
    /// From a PKCS#8 ECDSA P-256 private key, PEM or DER.
    pub(crate) fn from_pkcs8(key: &[u8]) -> Result<Self, crypto::Error> {
        let key = IdentityKey::from_pkcs8(key)?;
        let public = key.public_key();
        Ok(Self {
            kid: key_id(public),
            public_key: b64().encode(public),
            key,
        })
    }

    /// The signed message around `body`, a JSON object as text.
    pub(crate) fn envelope(&self, body: &str) -> Result<String, crypto::Error> {
        let signature = self.key.sign(&signed_input(SERVER_LABEL, body))?;
        Ok(serde_json::json!({
            "t": "signed",
            "sb": body,
            "sig": b64().encode(signature),
            "kid": self.kid,
        })
        .to_string())
    }

    /// A signed message of type `t` with payload `p`, stamped now with a new
    /// `n`; `tp` names the topic it is for, so it is good for that topic only.
    pub(crate) fn sign_event(
        &self,
        t: &str,
        p: Value,
        topic: Option<&str>,
        now_ms: u64,
    ) -> Result<String, crypto::Error> {
        let mut body = Map::new();
        body.insert("t".into(), Value::String(t.to_owned()));
        if !p.is_null() {
            body.insert("p".into(), p);
        }
        body.insert("ts".into(), Value::from(now_ms));
        body.insert(
            "n".into(),
            Value::String(uuid::Uuid::now_v7().simple().to_string()),
        );
        if let Some(topic) = topic {
            body.insert("tp".into(), Value::String(topic.to_owned()));
        }
        self.envelope(&Value::Object(body).to_string())
    }

    /// The server's receipt for a client's signed message: which key, which
    /// `n`, SHA-256 of the body it got, and when it got it.
    pub(crate) fn receipt(&self, signed: &Verified, now_ms: u64) -> Result<String, crypto::Error> {
        let digest = b64().encode(crypto::sha256(signed.body.as_bytes()));
        self.sign_event(
            "receipt",
            serde_json::json!({
                "kid": signed.kid,
                "n": signed.n,
                "sha256": digest,
                "received": now_ms,
            }),
            None,
            now_ms,
        )
    }
}

/// A client's signed message that passed every check.
pub(crate) struct Verified {
    /// The body, parsed: what the application gets, with the proof added.
    pub(crate) message: Map<String, Value>,
    pub(crate) body: String,
    pub(crate) sig: String,
    pub(crate) kid: String,
    pub(crate) n: String,
}

impl Verified {
    /// The message as the application gets it: the body's fields and
    /// `signature` (`body`, `sig`, `kid`), what it keeps as the proof.
    pub(crate) fn into_value(self) -> Value {
        let mut message = self.message;
        message.insert(
            "signature".into(),
            serde_json::json!({ "body": self.body, "sig": self.sig, "kid": self.kid }),
        );
        Value::Object(message)
    }
}

/// Why a signed message was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refused {
    /// Not a signed message the server can read.
    Malformed,
    /// No key of this connection has the message's key id.
    UnknownKey,
    BadSignature,
    /// Signed for another server.
    WrongAudience,
    /// Its time is outside the server's window.
    Stale,
    /// Its `n` was taken already.
    Replayed,
    /// The server has no signing key, so no audience to check.
    NotEnabled,
    /// Verified, but the application could not take it now (its queue of
    /// waiting messages is full): not received, so the same `n` may come again.
    Busy,
}

impl Refused {
    /// The reason, for the client's code to read.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::UnknownKey => "unknown_key",
            Self::BadSignature => "bad_signature",
            Self::WrongAudience => "wrong_audience",
            Self::Stale => "stale",
            Self::Replayed => "replayed",
            Self::NotEnabled => "not_enabled",
            Self::Busy => "busy",
        }
    }

    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::Malformed => "Signed message malformed",
            Self::UnknownKey => "No signing key with this id for the connection",
            Self::BadSignature => "Signature does not verify",
            Self::WrongAudience => "Signed for another server",
            Self::Stale => "Signed message outside the time window",
            Self::Replayed => "Signed message already received",
            Self::NotEnabled => "The server takes no signed messages",
            Self::Busy => "The application cannot take it now; send it again",
        }
    }
}

/// Check a client's signed message (`envelope`, its outer object) against the
/// connection's key, the server's key id (`audience`) and the time window.
/// The `n` is not checked against earlier messages here: that is the caller's.
pub(crate) fn check(
    envelope: &Value,
    key: Option<&ClientKey>,
    audience: &str,
    now_ms: u64,
    window_ms: u64,
) -> Result<Verified, Refused> {
    let field = |name: &str| envelope.get(name).and_then(Value::as_str);
    let (Some(body), Some(sig), Some(kid)) = (field("sb"), field("sig"), field("kid")) else {
        return Err(Refused::Malformed);
    };
    let key = key
        .filter(|key| key.kid == kid)
        .ok_or(Refused::UnknownKey)?;
    let signature = b64().decode(sig).map_err(|_| Refused::Malformed)?;
    if !key.verifies(CLIENT_LABEL, body, &signature) {
        return Err(Refused::BadSignature);
    }
    let Some(Value::Object(message)) = parse_exact(body) else {
        return Err(Refused::Malformed);
    };
    if !message.get("t").is_some_and(Value::is_string) {
        return Err(Refused::Malformed);
    }
    if message.get("aud").and_then(Value::as_str) != Some(audience) {
        return Err(Refused::WrongAudience);
    }
    let ts = message
        .get("ts")
        .and_then(Value::as_u64)
        .ok_or(Refused::Malformed)?;
    if ts.abs_diff(now_ms) > window_ms {
        return Err(Refused::Stale);
    }
    let n = message
        .get("n")
        .and_then(Value::as_str)
        .filter(|n| !n.is_empty() && n.chars().count() <= MAX_NONCE_LEN)
        .ok_or(Refused::Malformed)?
        .to_owned();
    Ok(Verified {
        message,
        body: body.to_owned(),
        sig: sig.to_owned(),
        kid: kid.to_owned(),
        n,
    })
}

/// A signed body as the application gets it, read so that it cannot say
/// anything but its text: no key twice in an object (readers differ on which
/// one counts) and every integer exact (past 64 bits it would come out
/// rounded here and exact elsewhere). None when it is not such JSON.
fn parse_exact(body: &str) -> Option<Value> {
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

    const WINDOW: u64 = 30_000;

    fn client_pair() -> (aws_lc_rs::signature::EcdsaKeyPair, ClientKey) {
        use aws_lc_rs::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair};
        let rng = aws_lc_rs::rand::SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let pair =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref()).unwrap();
        let key = ClientKey::from_sec1(pair.public_key().as_ref()).unwrap();
        (pair, key)
    }

    fn envelope(pair: &aws_lc_rs::signature::EcdsaKeyPair, kid: &str, body: &str) -> Value {
        let rng = aws_lc_rs::rand::SystemRandom::new();
        let sig = pair.sign(&rng, &signed_input(CLIENT_LABEL, body)).unwrap();
        serde_json::json!({"t": "signed", "sb": body, "sig": b64().encode(sig), "kid": kid})
    }

    fn body(aud: &str, ts: u64, n: &str) -> String {
        format!(r#"{{"t":"order","p":{{"qty":"1.5"}},"aud":"{aud}","ts":{ts},"n":"{n}"}}"#)
    }

    #[test]
    fn a_signed_message_is_taken_with_its_proof() {
        let (pair, key) = client_pair();
        let text = body("srv", 1_000_000, "a1");
        let env = envelope(&pair, &key.kid, &text);
        let ok = check(&env, Some(&key), "srv", 1_000_500, WINDOW).unwrap();
        assert_eq!(ok.n, "a1");
        let value = ok.into_value();
        assert_eq!(value["t"], "order");
        assert_eq!(value["p"]["qty"], "1.5");
        assert_eq!(value["signature"]["body"], text.as_str());
    }

    #[test]
    fn a_body_that_could_be_read_two_ways_is_refused() {
        let (pair, key) = client_pair();
        let at = 1_000_000;
        let take = |text: &str| {
            check(
                &envelope(&pair, &key.kid, text),
                Some(&key),
                "srv",
                at,
                WINDOW,
            )
        };
        // A key twice: readers differ on which one counts.
        let twice =
            r#"{"t":"order","p":{"qty":"1"},"p":{"qty":"1000"},"aud":"srv","ts":1000000,"n":"d1"}"#;
        assert_eq!(take(twice).err(), Some(Refused::Malformed));
        let nested =
            r#"{"t":"order","p":{"qty":"1","qty":"1000"},"aud":"srv","ts":1000000,"n":"d2"}"#;
        assert_eq!(take(nested).err(), Some(Refused::Malformed));
        // An integer past 64 bits would come out rounded here.
        let big = r#"{"t":"order","p":{"qty":12345678901234567890123},"aud":"srv","ts":1000000,"n":"d3"}"#;
        assert_eq!(take(big).err(), Some(Refused::Malformed));
        // Exact numbers, and digits inside strings, are fine.
        let fine = r#"{"t":"order","p":{"qty":18446744073709551615,"px":-12.5e-1,"id":"12345678901234567890123"},"aud":"srv","ts":1000000,"n":"d4"}"#;
        assert!(take(fine).is_ok());
        let escaped = r#"{"t":"order","p":{"note":"a \"quoted\" 99999999999999999999999"},"aud":"srv","ts":1000000,"n":"d5"}"#;
        assert!(take(escaped).is_ok());
    }

    #[test]
    fn a_nonce_is_counted_in_characters() {
        let (pair, key) = client_pair();
        let n: String = "ж".repeat(128);
        let text = body("srv", 1_000_000, &n);
        assert!(
            check(
                &envelope(&pair, &key.kid, &text),
                Some(&key),
                "srv",
                1_000_000,
                WINDOW
            )
            .is_ok()
        );
        let long = body("srv", 1_000_000, &"ж".repeat(129));
        assert_eq!(
            check(
                &envelope(&pair, &key.kid, &long),
                Some(&key),
                "srv",
                1_000_000,
                WINDOW
            )
            .err(),
            Some(Refused::Malformed)
        );
    }

    #[test]
    fn every_check_refuses_what_it_should() {
        let (pair, key) = client_pair();
        let (other, _) = client_pair();
        let good = body("srv", 1_000_000, "a1");
        let at = 1_000_000;
        let refused =
            |env: &Value, aud: &str, now: u64| check(env, Some(&key), aud, now, WINDOW).err();

        assert_eq!(
            refused(&serde_json::json!({"t": "signed"}), "srv", at),
            Some(Refused::Malformed)
        );
        assert_eq!(
            check(&envelope(&pair, &key.kid, &good), None, "srv", at, WINDOW).err(),
            Some(Refused::UnknownKey)
        );
        assert_eq!(
            refused(&envelope(&pair, "0000000000000000", &good), "srv", at),
            Some(Refused::UnknownKey)
        );
        // Signed by another key under this key's id.
        assert_eq!(
            refused(&envelope(&other, &key.kid, &good), "srv", at),
            Some(Refused::BadSignature)
        );
        // One byte of the body changed after signing.
        let mut tampered = envelope(&pair, &key.kid, &good);
        tampered["sb"] = Value::String(good.replace("1.5", "9.5"));
        assert_eq!(refused(&tampered, "srv", at), Some(Refused::BadSignature));
        assert_eq!(
            refused(&envelope(&pair, &key.kid, &good), "other", at),
            Some(Refused::WrongAudience)
        );
        assert_eq!(
            refused(&envelope(&pair, &key.kid, &good), "srv", at + WINDOW + 1),
            Some(Refused::Stale)
        );
        let no_n = r#"{"t":"order","aud":"srv","ts":1000000}"#;
        assert_eq!(
            refused(&envelope(&pair, &key.kid, no_n), "srv", at),
            Some(Refused::Malformed)
        );
        let not_object = r#"["order"]"#;
        assert_eq!(
            refused(&envelope(&pair, &key.kid, not_object), "srv", at),
            Some(Refused::Malformed)
        );
    }

    #[test]
    fn a_server_signature_verifies_with_the_server_label_only() {
        let rng = aws_lc_rs::rand::SystemRandom::new();
        let pkcs8 = aws_lc_rs::signature::EcdsaKeyPair::generate_pkcs8(
            &aws_lc_rs::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &rng,
        )
        .unwrap();
        let signer = ServerSigner::from_pkcs8(pkcs8.as_ref()).unwrap();
        let env: Value = serde_json::from_str(
            &signer
                .sign_event("fill", serde_json::json!({"px": "10"}), Some("orders"), 5)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(env["t"], "signed");
        assert_eq!(env["kid"], signer.kid.as_str());
        let body = env["sb"].as_str().unwrap();
        let inner: Value = serde_json::from_str(body).unwrap();
        assert_eq!(
            (inner["t"].as_str(), inner["tp"].as_str()),
            (Some("fill"), Some("orders"))
        );
        let key = ClientKey::from_base64(&signer.public_key).unwrap();
        let sig = b64().decode(env["sig"].as_str().unwrap()).unwrap();
        assert!(key.verifies(SERVER_LABEL, body, &sig));
        assert!(!key.verifies(CLIENT_LABEL, body, &sig));
    }

    /// The vectors made by tests/vectors/make_sign_vectors.py with the
    /// `cryptography` package: what the clients sign, the server takes.
    #[test]
    fn the_shared_vectors_verify() {
        let v: Value =
            serde_json::from_str(include_str!("../../../../tests/vectors/sign.json")).unwrap();
        assert_eq!(v["client_label"].as_str().unwrap().as_bytes(), CLIENT_LABEL);
        assert_eq!(v["server_label"].as_str().unwrap().as_bytes(), SERVER_LABEL);
        let client = &v["client"];
        let key = ClientKey::from_base64(client["public_key"].as_str().unwrap()).unwrap();
        assert_eq!(key.kid, client["kid"].as_str().unwrap());
        assert_eq!(ClientKey::from_jwk(&client["jwk"]).unwrap().kid, key.kid);
        let envelope: Value = serde_json::from_str(client["envelope"].as_str().unwrap()).unwrap();
        let audience = v["server"]["kid"].as_str().unwrap();
        let ok = check(&envelope, Some(&key), audience, 1_794_600_000_000, WINDOW).unwrap();
        assert_eq!(ok.n, "order-7");
        assert_eq!(ok.message["p"]["note"], "café \"q\" ü");
        let mut tampered = envelope.clone();
        tampered["sb"] = client["tampered_body"].clone();
        assert_eq!(
            check(&tampered, Some(&key), audience, 1_794_600_000_000, WINDOW).err(),
            Some(Refused::BadSignature)
        );
        // A signature one bit off, or made under the server's label.
        for bad in ["flipped_sig", "other_label_sig"] {
            let mut forged = envelope.clone();
            forged["sig"] = client[bad].clone();
            assert_eq!(
                check(&forged, Some(&key), audience, 1_794_600_000_000, WINDOW).err(),
                Some(Refused::BadSignature),
                "{bad}"
            );
        }

        let server = &v["server"];
        let key = ClientKey::from_base64(server["public_key"].as_str().unwrap()).unwrap();
        let body = server["body"].as_str().unwrap();
        let sig = |name: &str| b64().decode(server[name].as_str().unwrap()).unwrap();
        assert!(key.verifies(SERVER_LABEL, body, &sig("sig")));
        assert!(!key.verifies(
            SERVER_LABEL,
            server["tampered_body"].as_str().unwrap(),
            &sig("sig")
        ));
        assert!(!key.verifies(SERVER_LABEL, body, &sig("flipped_sig")));
        assert!(!key.verifies(SERVER_LABEL, body, &sig("other_label_sig")));
        assert!(key.verifies(CLIENT_LABEL, body, &sig("other_label_sig")));
    }

    #[test]
    fn a_jwk_names_the_same_key_as_its_point() {
        let (_, key) = client_pair();
        let (pair, _) = client_pair();
        use aws_lc_rs::signature::KeyPair;
        let point = pair.public_key().as_ref();
        let url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let jwk = serde_json::json!({
            "kty": "EC", "crv": "P-256",
            "x": url.encode(&point[1..33]), "y": url.encode(&point[33..]),
        });
        let from_jwk = ClientKey::from_jwk(&jwk).unwrap();
        assert_eq!(from_jwk.kid, key_id(point));
        assert_ne!(from_jwk.kid, key.kid);
        let claims = serde_json::json!({"sub": "u", "cnf": {"jwk": jwk}});
        assert_eq!(ClientKey::from_claims(&claims).unwrap().kid, from_jwk.kid);
        assert!(
            ClientKey::from_jwk(&serde_json::json!({"kty": "OKP", "crv": "Ed25519"})).is_none()
        );
        assert!(ClientKey::from_base64("not base64!").is_none());
    }
}
