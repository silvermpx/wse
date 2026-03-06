// JWT encode/decode — HS256, RS256, ES256 — powered by jsonwebtoken crate.
//
// Used in two ways:
//   1. Internally by server.rs during WebSocket handshake (zero GIL)
//   2. Exposed to Python via PyO3 for HTTP routes

use pyo3::prelude::*;
use pyo3::types::PyDict;

// ---------------------------------------------------------------------------
// Algorithm
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtAlgorithm {
    HS256,
    RS256,
    ES256,
}

impl JwtAlgorithm {
    pub fn from_str(s: &str) -> Result<Self, JwtError> {
        match s {
            "HS256" => Ok(Self::HS256),
            "RS256" => Ok(Self::RS256),
            "ES256" => Ok(Self::ES256),
            _ => Err(JwtError::UnsupportedAlgorithm),
        }
    }

    fn to_jw(self) -> jsonwebtoken::Algorithm {
        match self {
            Self::HS256 => jsonwebtoken::Algorithm::HS256,
            Self::RS256 => jsonwebtoken::Algorithm::RS256,
            Self::ES256 => jsonwebtoken::Algorithm::ES256,
        }
    }
}

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub enum JwtError {
    MalformedToken,
    InvalidHeader,
    UnsupportedAlgorithm,
    InvalidSignature,
    InvalidPayload,
    TokenExpired,
    TokenNotYetValid,
    InvalidIssuer,
    InvalidAudience,
    KeyTooShort,
    TokenTooLarge,
    InvalidKeyId,
    InvalidKey(String),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedToken => write!(f, "Malformed JWT token"),
            Self::InvalidHeader => write!(f, "Invalid JWT header"),
            Self::UnsupportedAlgorithm => {
                write!(f, "Unsupported algorithm (supported: HS256, RS256, ES256)")
            }
            Self::InvalidSignature => write!(f, "Invalid JWT signature"),
            Self::InvalidPayload => write!(f, "Invalid JWT payload"),
            Self::TokenExpired => write!(f, "Token expired"),
            Self::TokenNotYetValid => write!(f, "Token not yet valid (nbf)"),
            Self::InvalidIssuer => write!(f, "Invalid issuer"),
            Self::InvalidAudience => write!(f, "Invalid audience"),
            Self::KeyTooShort => {
                write!(f, "Secret key too short (minimum 32 bytes for HS256)")
            }
            Self::TokenTooLarge => write!(f, "Token exceeds maximum size"),
            Self::InvalidKeyId => write!(f, "Invalid or mismatched key ID (kid)"),
            Self::InvalidKey(detail) => write!(f, "Invalid key: {detail}"),
        }
    }
}

impl From<jsonwebtoken::errors::Error> for JwtError {
    fn from(e: jsonwebtoken::errors::Error) -> Self {
        use jsonwebtoken::errors::ErrorKind;
        match e.into_kind() {
            ErrorKind::InvalidToken | ErrorKind::Base64(_) | ErrorKind::Utf8(_) => {
                Self::MalformedToken
            }
            ErrorKind::InvalidSignature => Self::InvalidSignature,
            ErrorKind::InvalidAlgorithm
            | ErrorKind::InvalidAlgorithmName
            | ErrorKind::MissingAlgorithm => Self::UnsupportedAlgorithm,
            ErrorKind::ExpiredSignature => Self::TokenExpired,
            ErrorKind::ImmatureSignature => Self::TokenNotYetValid,
            ErrorKind::InvalidIssuer => Self::InvalidIssuer,
            ErrorKind::InvalidAudience => Self::InvalidAudience,
            ErrorKind::InvalidEcdsaKey => Self::InvalidKey("Invalid ECDSA key".into()),
            ErrorKind::InvalidEddsaKey => Self::InvalidKey("Invalid EdDSA key".into()),
            ErrorKind::InvalidRsaKey(msg) => Self::InvalidKey(format!("Invalid RSA key: {msg}")),
            ErrorKind::InvalidKeyFormat => Self::InvalidKey("Invalid key format".into()),
            ErrorKind::MissingRequiredClaim(_) | ErrorKind::InvalidClaimFormat(_) => {
                Self::InvalidPayload
            }
            ErrorKind::Json(_) => Self::InvalidPayload,
            ErrorKind::InvalidSubject => Self::InvalidPayload,
            ErrorKind::RsaFailedSigning => Self::InvalidKey("RSA signing failed".into()),
            ErrorKind::Signing(msg) => Self::InvalidKey(format!("Signing failed: {msg}")),
            ErrorKind::Provider(msg) => Self::InvalidKey(format!("Provider error: {msg}")),
            _ => Self::InvalidSignature,
        }
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct JwtConfig {
    pub algorithm: JwtAlgorithm,
    /// For HS256: shared secret (>= 32 bytes).
    /// For RS256/ES256: public key in PEM format (for verification).
    pub secret: Vec<u8>,
    /// For HS256: previous shared secret for key rotation.
    /// For RS256/ES256: previous public key in PEM format.
    pub previous_secret: Option<Vec<u8>>,
    // private_key validated at startup but not stored -- PyO3 rust_jwt_encode
    // takes the key directly as an argument. Keeping key material out of
    // long-lived SharedState reduces exposure in crash dumps.
    pub key_id: Option<String>,
    pub issuer: String,
    pub audience: String,
    pub cookie_name: String,
}

// ---------------------------------------------------------------------------
// Internal API (used by server.rs directly — no GIL)
// ---------------------------------------------------------------------------

// RFC 7518: HS256 key MUST be >= 256 bits (32 bytes)
const MIN_HS256_KEY_LEN: usize = 32;
// Reject tokens larger than 8KB to prevent DoS
const MAX_TOKEN_LEN: usize = 8192;
// Clock skew tolerance for exp/nbf (seconds) -- RFC 8725 recommendation
const CLOCK_SKEW_SECS: u64 = 30;

/// Build a DecodingKey for the given algorithm and key material.
fn make_decoding_key(
    algorithm: JwtAlgorithm,
    key: &[u8],
) -> Result<jsonwebtoken::DecodingKey, JwtError> {
    match algorithm {
        JwtAlgorithm::HS256 => {
            if key.len() < MIN_HS256_KEY_LEN {
                return Err(JwtError::KeyTooShort);
            }
            Ok(jsonwebtoken::DecodingKey::from_secret(key))
        }
        JwtAlgorithm::RS256 => Ok(jsonwebtoken::DecodingKey::from_rsa_pem(key)?),
        JwtAlgorithm::ES256 => Ok(jsonwebtoken::DecodingKey::from_ec_pem(key)?),
    }
}

/// Build an EncodingKey for the given algorithm and key material.
fn make_encoding_key(
    algorithm: JwtAlgorithm,
    key: &[u8],
) -> Result<jsonwebtoken::EncodingKey, JwtError> {
    match algorithm {
        JwtAlgorithm::HS256 => {
            if key.len() < MIN_HS256_KEY_LEN {
                return Err(JwtError::KeyTooShort);
            }
            Ok(jsonwebtoken::EncodingKey::from_secret(key))
        }
        JwtAlgorithm::RS256 => Ok(jsonwebtoken::EncodingKey::from_rsa_pem(key)?),
        JwtAlgorithm::ES256 => Ok(jsonwebtoken::EncodingKey::from_ec_pem(key)?),
    }
}

/// Build a Validation config for the given algorithm and claims requirements.
fn make_validation(
    algorithm: JwtAlgorithm,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> jsonwebtoken::Validation {
    let mut v = jsonwebtoken::Validation::new(algorithm.to_jw());
    v.leeway = CLOCK_SKEW_SECS;
    v.validate_exp = true;
    v.validate_nbf = true;
    v.set_required_spec_claims(&["exp"]);

    if let Some(iss) = issuer {
        v.set_issuer(&[iss]);
    }

    if let Some(aud) = audience {
        v.set_audience(&[aud]);
    } else {
        v.validate_aud = false;
    }

    v
}

/// Decode and validate a JWT token.
///
/// Returns the payload as a serde_json::Value on success.
/// Validates: signature, algorithm, expiration, issuer, audience, kid.
/// Supports key rotation: if the primary key fails, retries with previous_secret.
pub fn jwt_decode(token: &str, config: &JwtConfig) -> Result<serde_json::Value, JwtError> {
    let issuer = if config.issuer.is_empty() {
        None
    } else {
        Some(config.issuer.as_str())
    };
    let audience = if config.audience.is_empty() {
        None
    } else {
        Some(config.audience.as_str())
    };
    let kid = config.key_id.as_deref();

    match jwt_decode_inner(
        token,
        &config.secret,
        config.algorithm,
        issuer,
        audience,
        kid,
    ) {
        Err(JwtError::InvalidSignature | JwtError::InvalidKeyId)
            if config.previous_secret.is_some() =>
        {
            // Verify the token's alg header matches before trying the previous key.
            // This avoids double-verification on tokens with a completely wrong algorithm.
            if let Ok(hdr) = jsonwebtoken::decode_header(token)
                && hdr.alg != config.algorithm.to_jw()
            {
                return Err(JwtError::UnsupportedAlgorithm);
            }
            let prev = config.previous_secret.as_ref().unwrap();
            // kid not enforced on previous-key path: old key may have had a different kid
            jwt_decode_inner(token, prev, config.algorithm, issuer, audience, None)
        }
        other => other,
    }
}

/// Core decode logic, shared between internal and PyO3 paths.
fn jwt_decode_inner(
    token: &str,
    key: &[u8],
    algorithm: JwtAlgorithm,
    issuer: Option<&str>,
    audience: Option<&str>,
    expected_kid: Option<&str>,
) -> Result<serde_json::Value, JwtError> {
    // 0. Reject oversized tokens (DoS prevention)
    if token.len() > MAX_TOKEN_LEN {
        return Err(JwtError::TokenTooLarge);
    }

    // 1. Validate kid before full decode (cheap header-only parse, no signature check)
    if let Some(expected) = expected_kid {
        let header = jsonwebtoken::decode_header(token)?;
        if header.kid.as_deref() != Some(expected) {
            return Err(JwtError::InvalidKeyId);
        }
    }

    // 2. Build key and validation
    let decoding_key = make_decoding_key(algorithm, key)?;
    let validation = make_validation(algorithm, issuer, audience);

    // 3. Decode, verify signature, validate claims
    let token_data = jsonwebtoken::decode::<serde_json::Value>(token, &decoding_key, &validation)?;

    Ok(token_data.claims)
}

/// Encode claims into a JWT token.
///
/// For HS256: key is the shared secret (>= 32 bytes).
/// For RS256/ES256: key is the private key in PEM format.
/// The caller is responsible for including exp, iat, iss, aud, sub, etc.
pub fn jwt_encode(
    claims: &serde_json::Value,
    key: &[u8],
    algorithm: JwtAlgorithm,
    kid: Option<&str>,
) -> Result<String, JwtError> {
    let encoding_key = make_encoding_key(algorithm, key)?;

    let mut header = jsonwebtoken::Header::new(algorithm.to_jw());
    if let Some(kid) = kid {
        header.kid = Some(kid.to_string());
    }

    Ok(jsonwebtoken::encode(&header, claims, &encoding_key)?)
}

// ---------------------------------------------------------------------------
// Cookie parsing helper (used by server.rs)
// ---------------------------------------------------------------------------

/// Extract a named cookie value from a raw Cookie header string.
/// Returns None if the cookie is not found or is empty/"undefined"/"null".
pub fn parse_cookie_value<'a>(cookies: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{}=", name);
    for cookie in cookies.split(';') {
        let cookie = cookie.trim();
        if let Some(value) = cookie.strip_prefix(&prefix) {
            let value = value.trim();
            if !value.is_empty() && value != "undefined" && value != "null" {
                return Some(value);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// PyO3 wrappers (for Python consumers — HTTP routes, etc.)
// ---------------------------------------------------------------------------

/// Decode a JWT token. Returns a dict of claims on success, None on failure.
#[pyfunction]
#[pyo3(signature = (token, secret, algorithm="HS256", issuer=None, audience=None, previous_secret=None, key_id=None))]
#[allow(clippy::too_many_arguments)]
pub fn rust_jwt_decode<'py>(
    py: Python<'py>,
    token: &str,
    secret: &[u8],
    algorithm: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
    previous_secret: Option<&[u8]>,
    key_id: Option<&str>,
) -> PyResult<Option<Py<PyDict>>> {
    let alg = JwtAlgorithm::from_str(algorithm).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Unsupported algorithm: {algorithm}. Supported: HS256, RS256, ES256"
        ))
    })?;

    let result = match jwt_decode_inner(token, secret, alg, issuer, audience, key_id) {
        Err(JwtError::InvalidSignature | JwtError::InvalidKeyId) if previous_secret.is_some() => {
            if let Ok(hdr) = jsonwebtoken::decode_header(token)
                && hdr.alg != alg.to_jw()
            {
                return Ok(None);
            }
            jwt_decode_inner(token, previous_secret.unwrap(), alg, issuer, audience, None)
        }
        other => other,
    };

    match result {
        Ok(payload) => {
            let dict = PyDict::new(py);
            if let serde_json::Value::Object(map) = payload {
                for (k, v) in map {
                    let py_val = json_value_to_py(py, &v);
                    dict.set_item(&k, py_val)?;
                }
            }
            Ok(Some(dict.unbind()))
        }
        Err(_) => Ok(None),
    }
}

/// Encode claims into a JWT token string.
///
/// claims: dict with all JWT claims (sub, exp, iat, iss, aud, etc.)
/// secret: For HS256 -- signing key as bytes. For RS256/ES256 -- PEM private key as bytes.
#[pyfunction]
#[pyo3(signature = (claims, secret, algorithm="HS256", key_id=None))]
pub fn rust_jwt_encode(
    _py: Python<'_>,
    claims: &Bound<'_, PyDict>,
    secret: &[u8],
    algorithm: &str,
    key_id: Option<&str>,
) -> PyResult<String> {
    let alg = JwtAlgorithm::from_str(algorithm).map_err(|_| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "Unsupported algorithm: {algorithm}. Supported: HS256, RS256, ES256"
        ))
    })?;

    let json_val = pydict_to_json(claims);

    jwt_encode(&json_val, secret, alg, key_id)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("JWT encode error: {e}")))
}

// ---------------------------------------------------------------------------
// PyDict <-> serde_json conversion helpers
// ---------------------------------------------------------------------------

use pyo3::types::{PyBool, PyFloat, PyInt, PyList, PyString};

fn pydict_to_json(dict: &Bound<'_, PyDict>) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for (k, v) in dict.iter() {
        let key = k.extract::<String>().unwrap_or_else(|_| format!("{}", k));
        map.insert(key, pyany_to_json(&v));
    }
    serde_json::Value::Object(map)
}

fn pyany_to_json(obj: &Bound<'_, PyAny>) -> serde_json::Value {
    if obj.is_none() {
        return serde_json::Value::Null;
    }
    // Bool before int (bool is subclass of int in Python)
    if obj.is_instance_of::<PyBool>()
        && let Ok(b) = obj.extract::<bool>()
    {
        return serde_json::Value::Bool(b);
    }
    if obj.is_instance_of::<PyInt>()
        && let Ok(i) = obj.extract::<i64>()
    {
        return serde_json::Value::Number(i.into());
    }
    if obj.is_instance_of::<PyFloat>()
        && let Ok(f) = obj.extract::<f64>()
        && let Some(n) = serde_json::Number::from_f64(f)
    {
        return serde_json::Value::Number(n);
    }
    if obj.is_instance_of::<PyString>()
        && let Ok(s) = obj.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    if obj.is_instance_of::<PyDict>()
        && let Ok(dict) = obj.cast::<PyDict>()
    {
        return pydict_to_json(dict);
    }
    if obj.is_instance_of::<PyList>()
        && let Ok(list) = obj.cast::<PyList>()
    {
        let arr: Vec<serde_json::Value> = list.iter().map(|item| pyany_to_json(&item)).collect();
        return serde_json::Value::Array(arr);
    }
    // Datetime objects: try isoformat()
    if let Ok(iso) = obj.call_method0("isoformat")
        && let Ok(s) = iso.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    // Fallback: str(obj)
    if let Ok(s) = obj.str()
        && let Ok(s) = s.extract::<String>()
    {
        return serde_json::Value::String(s);
    }
    serde_json::Value::Null
}

fn json_value_to_py(py: Python<'_>, val: &serde_json::Value) -> Py<PyAny> {
    match val {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => (*b)
            .into_pyobject(py)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py).unwrap().into_any().unbind()
            } else if let Some(f) = n.as_f64() {
                f.into_pyobject(py).unwrap().into_any().unbind()
            } else {
                py.None()
            }
        }
        serde_json::Value::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        serde_json::Value::Array(arr) => {
            let list = PyList::empty(py);
            for item in arr {
                let _ = list.append(json_value_to_py(py, item));
            }
            list.into_any().unbind()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                let _ = dict.set_item(k, json_value_to_py(py, v));
            }
            dict.into_any().unbind()
        }
    }
}

// ---------------------------------------------------------------------------
// Rust-level unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_secret() -> Vec<u8> {
        b"test-secret-key-for-jwt-testing-32bytes!".to_vec()
    }

    fn hs256_config() -> JwtConfig {
        JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret: test_secret(),
            previous_secret: None,
            key_id: None,
            issuer: "sqv".to_string(),
            audience: "sqv-api".to_string(),
            cookie_name: "access_token".to_string(),
        }
    }

    fn future_exp() -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        now + 3600 // 1 hour from now
    }

    // --- HS256 tests ---

    #[test]
    fn hs256_encode_decode_roundtrip() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iat": 1700000000,
            "iss": "sqv",
            "aud": "sqv-api",
            "type": "access",
            "jti": "test-jti-001",
        });

        let token =
            jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).expect("encode ok");
        assert!(token.contains('.'));
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let decoded = jwt_decode(&token, &config).expect("decode ok");
        assert_eq!(decoded["sub"], "user-123");
        assert_eq!(decoded["iss"], "sqv");
        assert_eq!(decoded["aud"], "sqv-api");
        assert_eq!(decoded["type"], "access");
    }

    #[test]
    fn hs256_rejects_expired_token() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": 1000000000, // far in the past
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::TokenExpired)));
    }

    #[test]
    fn hs256_rejects_wrong_secret() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();

        let bad_config = JwtConfig {
            secret: b"wrong-secret-key-at-least-32bytes!".to_vec(),
            ..hs256_config()
        };
        let result = jwt_decode(&token, &bad_config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn hs256_rejects_wrong_issuer() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "other-service",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidIssuer)));
    }

    #[test]
    fn hs256_rejects_wrong_audience() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "other-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidAudience)));
    }

    #[test]
    fn hs256_rejects_malformed_token() {
        let config = hs256_config();
        assert!(jwt_decode("not.a.jwt.token", &config).is_err());
        assert!(matches!(
            jwt_decode("just-a-string", &config),
            Err(JwtError::MalformedToken)
        ));
        assert!(matches!(
            jwt_decode("two.parts", &config),
            Err(JwtError::MalformedToken)
        ));
        assert!(matches!(
            jwt_decode("", &config),
            Err(JwtError::MalformedToken)
        ));
    }

    #[test]
    fn hs256_rejects_tampered_payload() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();

        let parts: Vec<&str> = token.split('.').collect();
        let tampered_payload = format!("{}x", parts[1]);
        let tampered_token = format!("{}.{}.{}", parts[0], tampered_payload, parts[2]);

        let result = jwt_decode(&tampered_token, &config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn parse_cookie_value_works() {
        assert_eq!(
            parse_cookie_value("access_token=abc123; refresh_token=xyz", "access_token"),
            Some("abc123")
        );
        assert_eq!(
            parse_cookie_value("access_token=abc123; refresh_token=xyz", "refresh_token"),
            Some("xyz")
        );
        assert_eq!(parse_cookie_value("access_token=abc123", "missing"), None);
        assert_eq!(
            parse_cookie_value("access_token=undefined", "access_token"),
            None
        );
        assert_eq!(
            parse_cookie_value("access_token=null", "access_token"),
            None
        );
        assert_eq!(parse_cookie_value("access_token=", "access_token"), None);
    }

    #[test]
    fn hs256_decode_with_optional_issuer_audience() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, None).unwrap();

        let result = jwt_decode_inner(&token, &secret, JwtAlgorithm::HS256, None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn hs256_rejects_missing_exp() {
        let config = hs256_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        // jsonwebtoken returns MissingRequiredClaim which maps to InvalidPayload
        assert!(result.is_err());
    }

    #[test]
    fn hs256_skips_validation_when_issuer_audience_empty() {
        let secret = test_secret();
        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret: secret.clone(),
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "any-issuer",
            "aud": "any-audience",
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn hs256_preserves_all_claim_types() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iat": 1700000000,
            "roles": ["admin", "user"],
            "custom": {"nested": true},
            "count": 42,
            "active": true,
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, None).unwrap();
        let decoded =
            jwt_decode_inner(&token, &secret, JwtAlgorithm::HS256, None, None, None).unwrap();
        assert_eq!(decoded["sub"], "user-123");
        assert_eq!(decoded["roles"][0], "admin");
        assert_eq!(decoded["custom"]["nested"], true);
        assert_eq!(decoded["count"], 42);
        assert_eq!(decoded["active"], true);
    }

    #[test]
    fn hs256_rejects_short_secret() {
        let short_secret = b"too-short".to_vec();
        let claims = serde_json::json!({"sub": "x", "exp": future_exp()});
        let result = jwt_encode(&claims, &short_secret, JwtAlgorithm::HS256, None);
        assert!(matches!(result, Err(JwtError::KeyTooShort)));
    }

    #[test]
    fn hs256_rejects_oversized_token() {
        let config = hs256_config();
        let huge_token = format!("a.{}.b", "x".repeat(9000));
        let result = jwt_decode(&huge_token, &config);
        assert!(matches!(result, Err(JwtError::TokenTooLarge)));
    }

    #[test]
    fn hs256_exp_allows_clock_skew() {
        let config = hs256_config();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Token expired 10 seconds ago -- within 30s skew, should pass
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": now - 10,
            "iss": "sqv",
            "aud": "sqv-api",
        });
        let token = jwt_encode(&claims, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());

        // Token expired 60 seconds ago -- beyond skew, should fail
        let claims_old = serde_json::json!({
            "sub": "user-123",
            "exp": now - 60,
            "iss": "sqv",
            "aud": "sqv-api",
        });
        let token_old = jwt_encode(&claims_old, &config.secret, JwtAlgorithm::HS256, None).unwrap();
        let result_old = jwt_decode(&token_old, &config);
        assert!(matches!(result_old, Err(JwtError::TokenExpired)));
    }

    // --- HS256 key rotation tests ---

    #[test]
    fn hs256_key_rotation_accepts_old_secret() {
        let old_secret = b"old-secret-key-for-rotation-32b!!".to_vec();
        let new_secret = b"new-secret-key-for-rotation-32b!!".to_vec();

        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });
        let token = jwt_encode(&claims, &old_secret, JwtAlgorithm::HS256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret: new_secret,
            previous_secret: Some(old_secret),
            key_id: None,
            issuer: "sqv".to_string(),
            audience: "sqv-api".to_string(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["sub"], "user-123");
    }

    #[test]
    fn hs256_key_rotation_rejects_unknown_secret() {
        let unknown = b"unknown-secret-not-configured-32!".to_vec();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });
        let token = jwt_encode(&claims, &unknown, JwtAlgorithm::HS256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret: b"current-secret-key-32-bytes-ok!!".to_vec(),
            previous_secret: Some(b"previous-secret-key-32bytes-ok!!".to_vec()),
            key_id: None,
            issuer: "sqv".to_string(),
            audience: "sqv-api".to_string(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    // --- HS256 kid tests ---

    #[test]
    fn hs256_kid_in_header_roundtrip() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, Some("key-2024-03")).unwrap();

        // Verify kid is in the header
        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(header.kid.as_deref(), Some("key-2024-03"));

        // Decode with matching kid
        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret,
            previous_secret: None,
            key_id: Some("key-2024-03".to_string()),
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn hs256_kid_mismatch_rejected() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, Some("key-old")).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret,
            previous_secret: None,
            key_id: Some("key-new".to_string()),
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidKeyId)));
    }

    #[test]
    fn hs256_kid_missing_from_token_rejected() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret,
            previous_secret: None,
            key_id: Some("expected-kid".to_string()),
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidKeyId)));
    }

    #[test]
    fn hs256_kid_not_required_when_unconfigured() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret, JwtAlgorithm::HS256, Some("some-kid")).unwrap();
        let config = JwtConfig {
            algorithm: JwtAlgorithm::HS256,
            secret,
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
    }

    // --- RS256 tests ---

    const RSA_PRIVATE_KEY: &[u8] = include_bytes!("../../tests/keys/rsa_private.pem");
    const RSA_PUBLIC_KEY: &[u8] = include_bytes!("../../tests/keys/rsa_public.pem");
    const RSA2_PUBLIC_KEY: &[u8] = include_bytes!("../../tests/keys/rsa2_public.pem");

    #[test]
    fn rs256_encode_decode_roundtrip() {
        let claims = serde_json::json!({
            "sub": "user-rs256",
            "exp": future_exp(),
            "iss": "test",
            "aud": "test-api",
        });

        let token = jwt_encode(&claims, RSA_PRIVATE_KEY, JwtAlgorithm::RS256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::RS256,
            secret: RSA_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: "test".to_string(),
            audience: "test-api".to_string(),
            cookie_name: "access_token".to_string(),
        };
        let decoded = jwt_decode(&token, &config).unwrap();
        assert_eq!(decoded["sub"], "user-rs256");
    }

    #[test]
    fn rs256_wrong_key_rejected() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        // Sign with key 1
        let token = jwt_encode(&claims, RSA_PRIVATE_KEY, JwtAlgorithm::RS256, None).unwrap();

        // Verify with key 2 (different keypair)
        let config = JwtConfig {
            algorithm: JwtAlgorithm::RS256,
            secret: RSA2_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn rs256_key_rotation() {
        let claims = serde_json::json!({
            "sub": "user-rotation",
            "exp": future_exp(),
        });

        // Sign with old RSA key
        let token = jwt_encode(&claims, RSA_PRIVATE_KEY, JwtAlgorithm::RS256, None).unwrap();

        // Config: new key is primary, old key is previous
        let config = JwtConfig {
            algorithm: JwtAlgorithm::RS256,
            secret: RSA2_PUBLIC_KEY.to_vec(),
            previous_secret: Some(RSA_PUBLIC_KEY.to_vec()),
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["sub"], "user-rotation");
    }

    #[test]
    fn rs256_kid_support() {
        let claims = serde_json::json!({
            "sub": "user-kid",
            "exp": future_exp(),
        });

        let token = jwt_encode(
            &claims,
            RSA_PRIVATE_KEY,
            JwtAlgorithm::RS256,
            Some("rsa-key-1"),
        )
        .unwrap();

        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(header.kid.as_deref(), Some("rsa-key-1"));

        let config = JwtConfig {
            algorithm: JwtAlgorithm::RS256,
            secret: RSA_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: Some("rsa-key-1".to_string()),
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        assert!(jwt_decode(&token, &config).is_ok());
    }

    // --- ES256 tests ---

    const EC_PRIVATE_KEY: &[u8] = include_bytes!("../../tests/keys/ec_private.pem");
    const EC_PUBLIC_KEY: &[u8] = include_bytes!("../../tests/keys/ec_public.pem");
    const EC2_PUBLIC_KEY: &[u8] = include_bytes!("../../tests/keys/ec2_public.pem");

    #[test]
    fn es256_encode_decode_roundtrip() {
        let claims = serde_json::json!({
            "sub": "user-es256",
            "exp": future_exp(),
            "iss": "test",
            "aud": "test-api",
        });

        let token = jwt_encode(&claims, EC_PRIVATE_KEY, JwtAlgorithm::ES256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::ES256,
            secret: EC_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: "test".to_string(),
            audience: "test-api".to_string(),
            cookie_name: "access_token".to_string(),
        };
        let decoded = jwt_decode(&token, &config).unwrap();
        assert_eq!(decoded["sub"], "user-es256");
    }

    #[test]
    fn es256_wrong_key_rejected() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, EC_PRIVATE_KEY, JwtAlgorithm::ES256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::ES256,
            secret: EC2_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn es256_key_rotation() {
        let claims = serde_json::json!({
            "sub": "user-ec-rotation",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, EC_PRIVATE_KEY, JwtAlgorithm::ES256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::ES256,
            secret: EC2_PUBLIC_KEY.to_vec(),
            previous_secret: Some(EC_PUBLIC_KEY.to_vec()),
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
        assert_eq!(result.unwrap()["sub"], "user-ec-rotation");
    }

    #[test]
    fn es256_kid_support() {
        let claims = serde_json::json!({
            "sub": "user-kid",
            "exp": future_exp(),
        });

        let token = jwt_encode(
            &claims,
            EC_PRIVATE_KEY,
            JwtAlgorithm::ES256,
            Some("ec-key-1"),
        )
        .unwrap();

        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(header.kid.as_deref(), Some("ec-key-1"));

        let config = JwtConfig {
            algorithm: JwtAlgorithm::ES256,
            secret: EC_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: Some("ec-key-1".to_string()),
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        assert!(jwt_decode(&token, &config).is_ok());
    }

    // --- Cross-algorithm security tests ---

    #[test]
    fn cross_algorithm_hs256_token_rejected_by_rs256() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        // Encode with HS256
        let token = jwt_encode(&claims, &test_secret(), JwtAlgorithm::HS256, None).unwrap();

        // Try to decode with RS256 config
        let config = JwtConfig {
            algorithm: JwtAlgorithm::RS256,
            secret: RSA_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_err());
    }

    #[test]
    fn cross_algorithm_rs256_token_rejected_by_es256() {
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, RSA_PRIVATE_KEY, JwtAlgorithm::RS256, None).unwrap();

        let config = JwtConfig {
            algorithm: JwtAlgorithm::ES256,
            secret: EC_PUBLIC_KEY.to_vec(),
            previous_secret: None,
            key_id: None,
            issuer: String::new(),
            audience: String::new(),
            cookie_name: "access_token".to_string(),
        };
        let result = jwt_decode(&token, &config);
        assert!(result.is_err());
    }
}
