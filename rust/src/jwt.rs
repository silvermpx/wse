// JWT HS256 encode/decode — pure Rust, no Python dependency.
//
// Used in two ways:
//   1. Internally by server.rs during WebSocket handshake (zero GIL)
//   2. Exposed to Python via PyO3 for HTTP routes (FastAPI dependencies)

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug)]
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
    Base64Error(base64::DecodeError),
    JsonError(serde_json::Error),
}

impl std::fmt::Display for JwtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedToken => write!(f, "Malformed JWT token"),
            Self::InvalidHeader => write!(f, "Invalid JWT header"),
            Self::UnsupportedAlgorithm => write!(f, "Unsupported algorithm (only HS256)"),
            Self::InvalidSignature => write!(f, "Invalid JWT signature"),
            Self::InvalidPayload => write!(f, "Invalid JWT payload"),
            Self::TokenExpired => write!(f, "Token expired"),
            Self::TokenNotYetValid => write!(f, "Token not yet valid (nbf)"),
            Self::InvalidIssuer => write!(f, "Invalid issuer"),
            Self::InvalidAudience => write!(f, "Invalid audience"),
            Self::Base64Error(e) => write!(f, "Base64 decode error: {e}"),
            Self::JsonError(e) => write!(f, "JSON error: {e}"),
        }
    }
}

impl From<base64::DecodeError> for JwtError {
    fn from(e: base64::DecodeError) -> Self {
        Self::Base64Error(e)
    }
}

impl From<serde_json::Error> for JwtError {
    fn from(e: serde_json::Error) -> Self {
        Self::JsonError(e)
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct JwtConfig {
    pub secret: Vec<u8>,
    pub issuer: String,
    pub audience: String,
}

// ---------------------------------------------------------------------------
// Internal API (used by server.rs directly — no GIL)
// ---------------------------------------------------------------------------

/// Decode and validate a JWT token (HS256 only).
///
/// Returns the payload as a serde_json::Value on success.
/// Validates: signature, algorithm, expiration, issuer, audience.
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
    jwt_decode_inner(token, &config.secret, issuer, audience)
}

/// Core decode logic, shared between internal and PyO3 paths.
fn jwt_decode_inner(
    token: &str,
    secret: &[u8],
    issuer: Option<&str>,
    audience: Option<&str>,
) -> Result<serde_json::Value, JwtError> {
    // 1. Split into 3 parts
    let parts: Vec<&str> = token.splitn(3, '.').collect();
    if parts.len() != 3 {
        return Err(JwtError::MalformedToken);
    }
    let (header_b64, payload_b64, sig_b64) = (parts[0], parts[1], parts[2]);

    // 2. Verify signature (HMAC-SHA256)
    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let signature = URL_SAFE_NO_PAD.decode(sig_b64)?;

    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(signing_input.as_bytes());
    // Constant-time comparison via hmac crate's verify_slice
    mac.verify_slice(&signature)
        .map_err(|_| JwtError::InvalidSignature)?;

    // 3. Decode and validate header
    let header_bytes = URL_SAFE_NO_PAD.decode(header_b64)?;
    let header: serde_json::Value = serde_json::from_slice(&header_bytes)?;

    let alg = header
        .get("alg")
        .and_then(|a| a.as_str())
        .ok_or(JwtError::InvalidHeader)?;
    if alg != "HS256" {
        return Err(JwtError::UnsupportedAlgorithm);
    }

    // 4. Decode payload
    let payload_bytes = URL_SAFE_NO_PAD.decode(payload_b64)?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| JwtError::InvalidPayload)?;

    // 5. Validate claims
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // exp - required, reject tokens without expiration
    let exp = payload
        .get("exp")
        .and_then(|e| e.as_i64())
        .ok_or(JwtError::TokenExpired)?;
    if now >= exp {
        return Err(JwtError::TokenExpired);
    }

    // nbf - reject tokens not yet valid (RFC 7519 Section 4.1.5)
    // Allow 5 minutes of clock skew
    if let Some(nbf) = payload.get("nbf").and_then(|n| n.as_i64())
        && now < nbf - 300
    {
        return Err(JwtError::TokenNotYetValid);
    }

    // iss - validate if config specifies issuer
    if let Some(expected_iss) = issuer {
        let token_iss = payload.get("iss").and_then(|i| i.as_str());
        if token_iss != Some(expected_iss) {
            return Err(JwtError::InvalidIssuer);
        }
    }

    // aud - validate if config specifies audience
    if let Some(expected_aud) = audience {
        let token_aud = payload.get("aud").and_then(|a| a.as_str());
        if token_aud != Some(expected_aud) {
            return Err(JwtError::InvalidAudience);
        }
    }

    Ok(payload)
}

/// Encode claims into a JWT token (HS256).
///
/// The caller is responsible for including exp, iat, iss, aud, sub, etc.
/// This function just signs whatever claims are provided.
pub fn jwt_encode(claims: &serde_json::Value, secret: &[u8]) -> Result<String, JwtError> {
    // 1. Header
    let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});

    // 2. Base64url encode
    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
    let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims)?);

    // 3. Sign
    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature);

    Ok(format!("{}.{}.{}", header_b64, payload_b64, sig_b64))
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
///
/// Matches the behavior of JwtTokenManager.decode_token_payload() in Python:
/// returns None for any error (expired, bad signature, bad issuer, etc.)
#[pyfunction]
#[pyo3(signature = (token, secret, algorithm="HS256", issuer=None, audience=None))]
pub fn rust_jwt_decode<'py>(
    py: Python<'py>,
    token: &str,
    secret: &[u8],
    algorithm: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> PyResult<Option<Py<PyDict>>> {
    if algorithm != "HS256" {
        return Ok(None);
    }

    match jwt_decode_inner(token, secret, issuer, audience) {
        Ok(payload) => {
            // Convert serde_json::Value to PyDict
            let dict = PyDict::new(py);
            if let serde_json::Value::Object(map) = payload {
                for (k, v) in map {
                    let py_val = json_value_to_py(py, &v);
                    dict.set_item(&k, py_val)?;
                }
            }
            Ok(Some(dict.unbind()))
        }
        Err(e) => {
            // Log at debug level, return None (matches Python behavior)
            eprintln!("[RustJWT] decode error: {e}");
            Ok(None)
        }
    }
}

/// Encode claims into a JWT token string (HS256).
///
/// claims: dict with all JWT claims (sub, exp, iat, iss, aud, etc.)
/// secret: signing key as bytes
///
/// Raises PyRuntimeError on failure.
#[pyfunction]
#[pyo3(signature = (claims, secret, algorithm="HS256"))]
pub fn rust_jwt_encode(
    _py: Python<'_>,
    claims: &Bound<'_, PyDict>,
    secret: &[u8],
    algorithm: &str,
) -> PyResult<String> {
    if algorithm != "HS256" {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Only HS256 algorithm is supported",
        ));
    }

    // Convert PyDict to serde_json::Value
    let json_val = pydict_to_json(claims);

    jwt_encode(&json_val, secret)
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

    fn test_config() -> JwtConfig {
        JwtConfig {
            secret: test_secret(),
            issuer: "sqv".to_string(),
            audience: "sqv-api".to_string(),
        }
    }

    fn future_exp() -> i64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        now + 3600 // 1 hour from now
    }

    #[test]
    fn encode_decode_roundtrip() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iat": 1700000000,
            "iss": "sqv",
            "aud": "sqv-api",
            "type": "access",
            "jti": "test-jti-001",
        });

        let token = jwt_encode(&claims, &config.secret).expect("encode should succeed");
        assert!(token.contains('.'));
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let decoded = jwt_decode(&token, &config).expect("decode should succeed");
        assert_eq!(decoded["sub"], "user-123");
        assert_eq!(decoded["iss"], "sqv");
        assert_eq!(decoded["aud"], "sqv-api");
        assert_eq!(decoded["type"], "access");
    }

    #[test]
    fn decode_rejects_expired_token() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": 1000000000, // far in the past
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::TokenExpired)));
    }

    #[test]
    fn decode_rejects_wrong_secret() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();

        let bad_config = JwtConfig {
            secret: b"wrong-secret".to_vec(),
            ..test_config()
        };
        let result = jwt_decode(&token, &bad_config);
        assert!(matches!(result, Err(JwtError::InvalidSignature)));
    }

    #[test]
    fn decode_rejects_wrong_issuer() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "other-service",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidIssuer)));
    }

    #[test]
    fn decode_rejects_wrong_audience() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "other-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::InvalidAudience)));
    }

    #[test]
    fn decode_rejects_malformed_token() {
        let config = test_config();
        assert!(matches!(jwt_decode("not.a.jwt.token", &config), Err(_)));
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
    fn decode_rejects_tampered_payload() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();

        // Tamper with payload: change a character
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
    fn decode_with_optional_issuer_audience() {
        let secret = test_secret();
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
        });

        let token = jwt_encode(&claims, &secret).unwrap();

        // No issuer/audience validation
        let result = jwt_decode_inner(&token, &secret, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn decode_rejects_missing_exp() {
        let config = test_config();
        let claims = serde_json::json!({
            "sub": "user-123",
            "iss": "sqv",
            "aud": "sqv-api",
        });

        let token = jwt_encode(&claims, &config.secret).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(matches!(result, Err(JwtError::TokenExpired)));
    }

    #[test]
    fn decode_skips_validation_when_issuer_audience_empty() {
        let secret = test_secret();
        let config = JwtConfig {
            secret: secret.clone(),
            issuer: String::new(),
            audience: String::new(),
        };
        let claims = serde_json::json!({
            "sub": "user-123",
            "exp": future_exp(),
            "iss": "any-issuer",
            "aud": "any-audience",
        });

        let token = jwt_encode(&claims, &secret).unwrap();
        let result = jwt_decode(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn encode_preserves_all_claim_types() {
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

        let token = jwt_encode(&claims, &secret).unwrap();
        let decoded = jwt_decode_inner(&token, &secret, None, None).unwrap();
        assert_eq!(decoded["sub"], "user-123");
        assert_eq!(decoded["roles"][0], "admin");
        assert_eq!(decoded["custom"]["nested"], true);
        assert_eq!(decoded["count"], 42);
        assert_eq!(decoded["active"], true);
    }
}
