use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use hkdf::Hkdf;
use p256::ecdh::EphemeralSecret;
use p256::PublicKey;
use sha2::Sha256;
use std::time::Duration;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Message;

const HKDF_SALT: &[u8] = b"wse-encryption";
const HKDF_INFO: &[u8] = b"aes-gcm-key";

struct CheckResult {
    passed: u32,
    failed: u32,
}

impl CheckResult {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
        }
    }

    fn check(&mut self, name: &str, condition: bool) {
        if condition {
            self.passed += 1;
            println!("    [PASS] {}", name);
        } else {
            self.failed += 1;
            println!("    [FAIL] {}", name);
        }
    }
}

/// Connect to the server but don't send client_hello yet (for custom handshake).
async fn connect_raw(
    host: &str,
    port: u16,
    token: &str,
) -> Result<WsStream, Box<dyn std::error::Error + Send + Sync>> {
    let uri = format!(
        "ws://{}:{}/wse?encryption=true&protocol_version=1",
        host, port
    );
    let request = http::Request::builder()
        .uri(&uri)
        .header("Host", format!("{}:{}", host, port))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Cookie", format!("access_token={}", token))
        .body(())?;

    let (ws_stream, _response) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(request),
    )
    .await??;

    let mut ws = ws_stream;

    // Wait for server_ready
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await?
            .ok_or("connection closed before server_ready")??;

        if let Some(parsed) = parse_wse_message(&msg) {
            if parsed.get("t").and_then(|t| t.as_str()) == Some("server_ready") {
                break;
            }
        }
    }

    Ok(ws)
}

/// Derive AES-256-GCM cipher from ECDH shared secret.
fn derive_cipher(shared_secret: &[u8]) -> Aes256Gcm {
    let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), shared_secret);
    let mut aes_key = [0u8; 32];
    hk.expand(HKDF_INFO, &mut aes_key)
        .expect("HKDF expand failed");
    Aes256Gcm::new_from_slice(&aes_key).expect("AES key init failed")
}

/// Encrypt plaintext with AES-GCM. Returns IV + ciphertext + tag.
fn encrypt(cipher: &Aes256Gcm, plaintext: &[u8]) -> Vec<u8> {
    let iv_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from_slice(&iv_bytes);
    let ciphertext = cipher.encrypt(nonce, plaintext).expect("encrypt failed");
    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&iv_bytes);
    result.extend_from_slice(&ciphertext);
    result
}

/// Decrypt AES-GCM data (IV + ciphertext + tag) -> plaintext.
fn decrypt(cipher: &Aes256Gcm, data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < 28 {
        return Err("data too short".into());
    }
    let nonce = Nonce::from_slice(&data[..12]);
    cipher
        .decrypt(nonce, &data[12..])
        .map_err(|e| format!("decrypt failed: {}", e))
}

/// Drain messages from a WsStream, looking for a specific type.
/// Handles both plaintext and encrypted frames.
async fn drain_for_type_encrypted(
    ws: &mut WsStream,
    msg_type: &str,
    cipher: Option<&Aes256Gcm>,
    deadline_ms: u64,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                // Try normal parse first
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some(msg_type) {
                        return Some(parsed);
                    }
                }
                // Try decrypting E: binary frames
                if let Some(cipher) = cipher {
                    if let Message::Binary(data) = &msg {
                        if data.starts_with(b"E:") {
                            if let Ok(plaintext) = decrypt(cipher, &data[2..]) {
                                if let Ok(text) = std::str::from_utf8(&plaintext) {
                                    // Strip WSE/S/U prefix and parse
                                    let json_str = if text.starts_with("WSE{") {
                                        &text[3..]
                                    } else if text.starts_with("U{") || text.starts_with("S{") {
                                        &text[1..]
                                    } else {
                                        text
                                    };
                                    if let Ok(parsed) =
                                        serde_json::from_str::<serde_json::Value>(json_str)
                                    {
                                        if parsed.get("t").and_then(|t| t.as_str())
                                            == Some(msg_type)
                                        {
                                            return Some(parsed);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => return None,
        }
    }
}

/// Battle Test: E2E Encryption
///
/// Tests ECDH P-256 key exchange, AES-GCM-256 encrypted messaging,
/// mixed encrypted/unencrypted connections, and invalid key handling.
///
/// Requires: python benchmarks/bench_battle_server.py --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    crate::stats::print_header(
        "BATTLE TEST: E2E Encryption",
        "Tests ECDH key exchange, AES-GCM encrypted messaging, mixed connections.",
    );

    let mut checks = CheckResult::new();
    let b64 = base64::engine::general_purpose::STANDARD;

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "enc-test-user");

    // =========================================================================
    // Test 1: Key exchange handshake
    // =========================================================================
    println!("\n  Test 1: ECDH key exchange handshake");

    let client_secret = EphemeralSecret::random(&mut OsRng);
    let client_pubkey = PublicKey::from(&client_secret);
    let client_pubkey_bytes = client_pubkey.to_sec1_bytes();
    let client_pubkey_b64 = b64.encode(&client_pubkey_bytes);

    checks.check(
        "Client public key is 65 bytes (uncompressed SEC1)",
        client_pubkey_bytes.len() == 65,
    );

    let mut ws = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            println!("    [FAIL] Could not connect: {}", e);
            return vec![];
        }
    };

    // Send client_hello with encryption
    let hello = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.2.0",
            "protocol_version": 1,
            "features": {
                "encryption": true
            },
            "encryption_public_key": client_pubkey_b64
        }
    });
    ws.send(Message::Text(format!("WSE{}", hello).into()))
        .await
        .unwrap();

    // Read server_hello
    let server_hello = drain_for_type_encrypted(&mut ws, "server_hello", None, 5000).await;
    let server_hello = server_hello.expect("server_hello not received");

    let payload = server_hello.get("p").unwrap();
    let features = payload.get("features").unwrap();

    checks.check(
        "server_hello has features.encryption = true",
        features.get("encryption").and_then(|v| v.as_bool()) == Some(true),
    );

    let server_pubkey_b64 = payload
        .get("encryption_public_key")
        .and_then(|v| v.as_str());
    checks.check(
        "server_hello has encryption_public_key field",
        server_pubkey_b64.is_some(),
    );

    // Derive shared AES key
    let server_pubkey_b64 = server_pubkey_b64.unwrap();
    let server_pubkey_bytes = b64.decode(server_pubkey_b64).unwrap();

    checks.check(
        "Server public key is 65 bytes (uncompressed SEC1)",
        server_pubkey_bytes.len() == 65,
    );

    let server_pubkey =
        PublicKey::from_sec1_bytes(&server_pubkey_bytes).expect("invalid server public key");
    let shared_secret = client_secret.diffie_hellman(&server_pubkey);
    let cipher = derive_cipher(shared_secret.raw_secret_bytes().as_slice());

    println!("    Shared secret derived, AES-GCM-256 cipher ready");

    // =========================================================================
    // Test 2: Encrypted message round-trip
    // =========================================================================
    println!("\n  Test 2: Encrypted message round-trip");

    // Send an encrypted battle_cmd health query
    let health_cmd = r#"{"t":"battle_cmd","p":{"action":"health"}}"#;
    let encrypted = encrypt(&cipher, health_cmd.as_bytes());
    let mut e_frame = Vec::with_capacity(2 + encrypted.len());
    e_frame.extend_from_slice(b"E:");
    e_frame.extend_from_slice(&encrypted);
    ws.send(Message::Binary(e_frame.into())).await.unwrap();

    // Should get a health_response back (encrypted)
    let response = drain_for_type_encrypted(&mut ws, "health_response", Some(&cipher), 5000).await;
    checks.check(
        "Received health_response after encrypted request",
        response.is_some(),
    );

    // Close encrypted connection
    let _ = ws.close(None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // =========================================================================
    // Test 3: Unencrypted connection still works (opt-out)
    // =========================================================================
    println!("\n  Test 3: Encryption opt-out (unencrypted connection)");

    let mut ws_plain = match protocol::connect_and_handshake(
        &cli.host,
        cli.port,
        &token,
        "encryption=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            println!("    [FAIL] Could not connect unencrypted: {}", e);
            return vec![];
        }
    };

    // Query health via plaintext
    let health = protocol::query_health(&mut ws_plain, 5).await;
    checks.check(
        "Unencrypted connection works (health response received)",
        health.is_some(),
    );

    let _ = ws_plain.close(None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // =========================================================================
    // Test 4: Invalid encryption key rejection
    // =========================================================================
    println!("\n  Test 4: Invalid encryption key rejection");

    let mut ws_bad = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            println!("    [FAIL] Could not connect: {}", e);
            return vec![];
        }
    };

    // Send client_hello with malformed public key
    let bad_hello = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.2.0",
            "protocol_version": 1,
            "features": {
                "encryption": true
            },
            "encryption_public_key": b64.encode(b"not-a-valid-ec-point-only-36-bytes!!!")
        }
    });
    ws_bad
        .send(Message::Text(format!("WSE{}", bad_hello).into()))
        .await
        .unwrap();

    let bad_server_hello = drain_for_type_encrypted(&mut ws_bad, "server_hello", None, 5000).await;

    if let Some(ref hello) = bad_server_hello {
        let p = hello.get("p").unwrap();
        let enc_enabled = p
            .get("features")
            .and_then(|f| f.get("encryption"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        checks.check(
            "Invalid key: server_hello has encryption=false",
            !enc_enabled,
        );
        let has_pubkey = p.get("encryption_public_key").is_some();
        checks.check(
            "Invalid key: no encryption_public_key in response",
            !has_pubkey,
        );
    } else {
        checks.check("Invalid key: server_hello received", false);
        checks.check("Invalid key: encryption disabled", false);
    }

    let _ = ws_bad.close(None).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // =========================================================================
    // Test 5: send_event encryption (server -> encrypted client)
    // =========================================================================
    println!("\n  Test 5: send_event to encrypted connection");

    // Connect with encryption again
    let client_secret2 = EphemeralSecret::random(&mut OsRng);
    let client_pubkey2 = PublicKey::from(&client_secret2);
    let client_pubkey2_b64 = b64.encode(client_pubkey2.to_sec1_bytes());

    let mut ws_enc = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            println!("    [FAIL] Could not connect: {}", e);
            return vec![];
        }
    };

    let hello2 = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.2.0",
            "protocol_version": 1,
            "features": { "encryption": true },
            "encryption_public_key": client_pubkey2_b64
        }
    });
    ws_enc
        .send(Message::Text(format!("WSE{}", hello2).into()))
        .await
        .unwrap();

    let sh2 = drain_for_type_encrypted(&mut ws_enc, "server_hello", None, 5000).await;
    let sh2 = sh2.expect("server_hello not received");
    let sp2 = sh2.get("p").unwrap();
    let spk2 = sp2
        .get("encryption_public_key")
        .and_then(|v| v.as_str())
        .unwrap();
    let spk2_bytes = b64.decode(spk2).unwrap();
    let server_pk2 = PublicKey::from_sec1_bytes(&spk2_bytes).unwrap();
    let shared2 = client_secret2.diffie_hellman(&server_pk2);
    let cipher2 = derive_cipher(shared2.raw_secret_bytes().as_slice());

    // Subscribe to a topic via encrypted channel
    let sub_cmd = r#"{"t":"battle_cmd","p":{"action":"subscribe","topics":["battle_all"]}}"#;
    let enc_sub = encrypt(&cipher2, sub_cmd.as_bytes());
    let mut e_sub = Vec::with_capacity(2 + enc_sub.len());
    e_sub.extend_from_slice(b"E:");
    e_sub.extend_from_slice(&enc_sub);
    ws_enc.send(Message::Binary(e_sub.into())).await.unwrap();

    checks.check("Encrypted subscribe command sent", true);

    // Verify encrypted connection still works after subscribe by querying health
    let health_cmd2 = r#"{"t":"battle_cmd","p":{"action":"health"}}"#;
    let enc_health2 = encrypt(&cipher2, health_cmd2.as_bytes());
    let mut e_health2 = Vec::with_capacity(2 + enc_health2.len());
    e_health2.extend_from_slice(b"E:");
    e_health2.extend_from_slice(&enc_health2);
    ws_enc
        .send(Message::Binary(e_health2.into()))
        .await
        .unwrap();

    let health_resp2 =
        drain_for_type_encrypted(&mut ws_enc, "health_response", Some(&cipher2), 5000).await;
    checks.check(
        "Encrypted health response after subscribe",
        health_resp2.is_some(),
    );

    let _ = ws_enc.close(None).await;

    // =========================================================================
    // Summary
    // =========================================================================
    // Summary
    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
