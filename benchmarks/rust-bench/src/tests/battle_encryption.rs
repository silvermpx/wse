use super::{Checks, EXTRA};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use hkdf::Hkdf;
use p256::ecdh::EphemeralSecret;
use p256::elliptic_curve::Generate;
use p256::PublicKey;
use sha2::Sha256;
use std::time::Duration;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Message;

// RFC 5869: zero-salt + context info. Must match all WSE components.
const HKDF_INFO: &[u8] = b"wse-encryption/aes-gcm-key";

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
    let hk = Hkdf::<Sha256>::new(None, shared_secret);
    let mut aes_key = [0u8; 32];
    hk.expand(HKDF_INFO, &mut aes_key)
        .expect("HKDF expand failed");
    Aes256Gcm::new_from_slice(&aes_key).expect("AES key init failed")
}

/// Encrypt plaintext with AES-GCM. Returns IV + ciphertext + tag.
fn encrypt(cipher: &Aes256Gcm, plaintext: &[u8]) -> Vec<u8> {
    let iv_bytes: [u8; 12] = rand::random();
    let nonce = Nonce::from(iv_bytes);
    let ciphertext = cipher.encrypt(&nonce, plaintext).expect("encrypt failed");
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
    let nonce = Nonce::try_from(&data[..12]).expect("IV is 12 bytes");
    cipher
        .decrypt(&nonce, &data[12..])
        .map_err(|e| format!("decrypt failed: {}", e))
}

/// The plaintext `server_hello` that answers a `client_hello`: its payload.
async fn server_hello(ws: &mut WsStream) -> Option<serde_json::Value> {
    super::reply(ws, "server_hello", Duration::from_secs(5)).await
}

/// The session key a `server_hello` gives with `secret`: None without a
/// valid server key.
fn session_key(secret: EphemeralSecret, hello: &serde_json::Value) -> Option<Aes256Gcm> {
    let b64 = base64::engine::general_purpose::STANDARD;
    let server_key = b64.decode(hello["encryption_public_key"].as_str()?).ok()?;
    let server_key = PublicKey::from_sec1_bytes(&server_key).ok()?;
    let shared = secret.diffie_hellman(&server_key);
    Some(derive_cipher(shared.raw_secret_bytes().as_slice()))
}

/// `json` sealed with the session key: an `E:` binary frame.
fn sealed(cipher: &Aes256Gcm, json: &str) -> Message {
    let encrypted = encrypt(cipher, json.as_bytes());
    let mut frame = Vec::with_capacity(2 + encrypted.len());
    frame.extend_from_slice(b"E:");
    frame.extend_from_slice(&encrypted);
    Message::Binary(frame.into())
}

/// A sealed frame opened with the session key, as JSON; None for anything
/// that is not one.
fn open(cipher: &Aes256Gcm, msg: &Message) -> Option<serde_json::Value> {
    let Message::Binary(data) = msg else {
        return None;
    };
    let plaintext = decrypt(cipher, data.strip_prefix(b"E:")?).ok()?;
    let text = std::str::from_utf8(&plaintext).ok()?;
    let json = text
        .strip_prefix("WSE")
        .or_else(|| text.strip_prefix('S'))
        .or_else(|| text.strip_prefix('U'))
        .unwrap_or(text);
    serde_json::from_str(json).ok()
}

/// What an encrypted connection got: the battle messages and the `reply`
/// payload that came sealed, and how many data frames came any other way.
struct Got {
    battle: u64,
    reply: Option<serde_json::Value>,
    unsealed: u64,
}

/// Reads `ws` until a sealed `reply` and `want` sealed battle messages have
/// come, or `within` passes, and then [`EXTRA`] more.
async fn read_sealed(
    ws: &mut WsStream,
    cipher: &Aes256Gcm,
    reply: &str,
    want: u64,
    within: Duration,
) -> Got {
    let mut got = Got {
        battle: 0,
        reply: None,
        unsealed: 0,
    };
    let mut until = tokio::time::Instant::now() + within;
    let mut done = false;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let Ok(Some(Ok(msg))) = tokio::time::timeout(left, ws.next()).await else {
            break;
        };
        if !matches!(msg, Message::Text(_) | Message::Binary(_)) {
            continue;
        }
        match open(cipher, &msg) {
            Some(parsed) if parsed["t"] == "battle" => got.battle += 1,
            Some(mut parsed) if parsed["t"] == reply && got.reply.is_none() => {
                got.reply = Some(parsed["p"].take());
            }
            Some(_) => {}
            None => got.unsealed += 1,
        }
        if !done && got.reply.is_some() && got.battle >= want {
            done = true;
            until = tokio::time::Instant::now() + EXTRA;
        }
    }
    got
}

fn client_hello(public_key_b64: &str) -> Message {
    let hello = serde_json::json!({
        "t": "client_hello",
        "p": {
            "client_version": "wse-bench/0.2.0",
            "protocol_version": 1,
            "features": {"encryption": true},
            "encryption_public_key": public_key_b64
        }
    });
    Message::Text(format!("WSE{hello}").into())
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

    let mut checks = Checks::default();
    let b64 = base64::engine::general_purpose::STANDARD;
    let within = Duration::from_secs(5);

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "enc-test-user");

    // =========================================================================
    // Test 1: Key exchange handshake
    // =========================================================================
    say!("\n  Test 1: ECDH key exchange handshake");

    let client_secret = EphemeralSecret::generate();
    let client_pubkey_bytes = PublicKey::from(&client_secret).to_sec1_bytes();

    checks.check(
        "Client public key is 65 bytes (uncompressed SEC1)",
        client_pubkey_bytes.len() == 65,
    );

    let mut ws = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            fail!("    [FAIL] Could not connect: {}", e);
            return vec![];
        }
    };
    ws.send(client_hello(&b64.encode(&client_pubkey_bytes)))
        .await
        .ok();

    let Some(hello) = server_hello(&mut ws).await else {
        checks.check("server_hello received", false);
        checks.finish();
        return vec![];
    };
    checks.check(
        "server_hello has features.encryption = true",
        hello["features"]["encryption"] == true,
    );
    let server_key = hello["encryption_public_key"]
        .as_str()
        .and_then(|key| b64.decode(key).ok());
    checks.check(
        &format!(
            "Server public key is 65 bytes (uncompressed SEC1) (got {:?})",
            server_key.as_ref().map(Vec::len)
        ),
        server_key.as_ref().is_some_and(|key| key.len() == 65),
    );
    let Some(cipher) = session_key(client_secret, &hello) else {
        checks.check("Session key derived from the server's key", false);
        checks.finish();
        return vec![];
    };
    say!("    Shared secret derived, AES-GCM-256 cipher ready");

    // =========================================================================
    // Test 2: Encrypted message round-trip
    // =========================================================================
    say!("\n  Test 2: Encrypted message round-trip");

    ws.send(sealed(
        &cipher,
        r#"{"t":"battle_cmd","p":{"action":"health"}}"#,
    ))
    .await
    .ok();
    // The answer counts only sealed with the session key: one in the clear
    // is the round trip failing.
    let got = read_sealed(&mut ws, &cipher, "health_response", 0, within).await;
    checks.check(
        &format!(
            "Sealed health_response to a sealed request (unsealed frames: {})",
            got.unsealed
        ),
        got.reply.is_some() && got.unsealed == 0,
    );

    let _ = ws.close(None).await;

    // =========================================================================
    // Test 3: Unencrypted connection still works (opt-out)
    // =========================================================================
    say!("\n  Test 3: Encryption opt-out (unencrypted connection)");

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
            fail!("    [FAIL] Could not connect unencrypted: {}", e);
            checks.finish();
            return vec![];
        }
    };

    let health = protocol::query_health(&mut ws_plain, 5).await;
    checks.check(
        "Unencrypted connection works (health response received)",
        health.is_some(),
    );

    let _ = ws_plain.close(None).await;

    // =========================================================================
    // Test 4: Invalid encryption key rejection
    // =========================================================================
    say!("\n  Test 4: Invalid encryption key rejection");

    let mut ws_bad = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            fail!("    [FAIL] Could not connect: {}", e);
            checks.finish();
            return vec![];
        }
    };
    ws_bad
        .send(client_hello(
            &b64.encode(b"not-a-valid-ec-point-only-36-bytes!!!"),
        ))
        .await
        .ok();

    match server_hello(&mut ws_bad).await {
        Some(hello) => {
            checks.check(
                "Invalid key: server_hello has encryption=false",
                hello["features"]["encryption"] != true,
            );
            checks.check(
                "Invalid key: no encryption_public_key in response",
                hello.get("encryption_public_key").is_none(),
            );
        }
        None => {
            checks.check("Invalid key: server_hello received", false);
        }
    }

    let _ = ws_bad.close(None).await;

    // =========================================================================
    // Test 5: Broadcasts to an encrypted connection
    // =========================================================================
    say!("\n  Test 5: Broadcasts to an encrypted connection");

    let secret = EphemeralSecret::generate();
    let public_key = b64.encode(PublicKey::from(&secret).to_sec1_bytes());
    let mut ws_enc = match connect_raw(&cli.host, cli.port, &token).await {
        Ok(ws) => ws,
        Err(e) => {
            fail!("    [FAIL] Could not connect: {}", e);
            checks.finish();
            return vec![];
        }
    };
    ws_enc.send(client_hello(&public_key)).await.ok();
    let cipher = match server_hello(&mut ws_enc).await {
        Some(hello) => session_key(secret, &hello),
        None => None,
    };
    let Some(cipher) = cipher else {
        checks.check("Second encrypted session set up", false);
        checks.finish();
        return vec![];
    };

    // Subscribed by a sealed command, then the server publishes to the
    // topic: every message has to reach the connection sealed.
    const PUBLISHED: u64 = 20;
    ws_enc
        .send(sealed(
            &cipher,
            r#"{"t":"battle_cmd","p":{"action":"subscribe","topics":["battle_all"]}}"#,
        ))
        .await
        .ok();
    ws_enc
        .send(sealed(
            &cipher,
            &format!(
                r#"{{"t":"battle_cmd","p":{{"action":"publish_messages","topic":"battle_all","count":{PUBLISHED}}}}}"#
            ),
        ))
        .await
        .ok();
    let got = read_sealed(&mut ws_enc, &cipher, "publish_ack", PUBLISHED, within).await;
    let acked = got.reply.as_ref().and_then(|p| p["count"].as_u64());
    checks.check(
        &format!(
            "Received all {PUBLISHED} broadcasts sealed (sealed: {}, ack: {acked:?}, unsealed frames: {})",
            got.battle, got.unsealed
        ),
        got.battle == PUBLISHED && acked == Some(PUBLISHED) && got.unsealed == 0,
    );

    let _ = ws_enc.close(None).await;

    checks.finish();

    Vec::new()
}
