// Standalone HS256 JWT encode for benchmark client.
// Copied from rust/src/jwt.rs (encode path only, no PyO3).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn jwt_encode(claims: &serde_json::Value, secret: &[u8]) -> String {
    let header = serde_json::json!({"alg": "HS256", "typ": "JWT"});

    let header_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).unwrap());
    let payload_b64 = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());

    let signing_input = format!("{}.{}", header_b64, payload_b64);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();
    let sig_b64 = URL_SAFE_NO_PAD.encode(signature);

    format!("{}.{}.{}", header_b64, payload_b64, sig_b64)
}

/// Generate a benchmark JWT token matching bench_server.py:generate_token().
pub fn generate_bench_token(secret: &[u8], user_id: &str) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = serde_json::json!({
        "sub": user_id,
        "iss": "wse-bench",
        "aud": "wse-bench",
        "exp": now + 3600,
        "iat": now,
    });

    jwt_encode(&claims, secret)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_produces_valid_jwt() {
        let token = generate_bench_token(b"bench-secret-key-for-testing-only", "bench-user");
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        // Decode header
        let header_bytes = URL_SAFE_NO_PAD.decode(parts[0]).unwrap();
        let header: serde_json::Value = serde_json::from_slice(&header_bytes).unwrap();
        assert_eq!(header["alg"], "HS256");
        assert_eq!(header["typ"], "JWT");

        // Decode payload
        let payload_bytes = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).unwrap();
        assert_eq!(payload["sub"], "bench-user");
        assert_eq!(payload["iss"], "wse-bench");
        assert_eq!(payload["aud"], "wse-bench");
        assert!(payload["exp"].as_i64().unwrap() > payload["iat"].as_i64().unwrap());
    }

    #[test]
    fn encode_deterministic_with_same_claims() {
        let claims = serde_json::json!({
            "sub": "test",
            "exp": 9999999999_i64,
            "iss": "wse-bench",
            "aud": "wse-bench",
        });
        let secret = b"test-secret";
        let t1 = jwt_encode(&claims, secret);
        let t2 = jwt_encode(&claims, secret);
        assert_eq!(t1, t2);
    }
}
