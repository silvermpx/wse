use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Test 5: Format Comparison
/// JSON vs msgpack vs zlib-compressed throughput.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 5: Format Comparison (JSON vs MsgPack vs Compressed)",
        "Same payload, 3 wire formats. Measures serialization overhead.",
    );

    let tiers = cli.tiers_for(crate::config::TestName::FormatComparison);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = std::time::Duration::from_secs(cli.duration);

    // Build payloads in all formats
    let payload_json = protocol::build_payload(194);
    let payload_value: serde_json::Value = serde_json::from_str(&payload_json).unwrap();
    let payload_msgpack = build_msgpack_payload(&payload_value);
    let payload_compressed = build_compressed_payload(&payload_json);

    let json_size = payload_json.len();
    let msgpack_size = payload_msgpack.len();
    let compressed_size = payload_compressed.len();

    println!("  Payload sizes:");
    println!("    JSON:       {} bytes", json_size);
    println!(
        "    MsgPack:    {} bytes (M: prefix + msgpack)",
        msgpack_size
    );
    println!(
        "    Compressed: {} bytes (C: prefix + zlib)",
        compressed_size
    );

    let mut all_results = Vec::new();

    for &n in &tiers {
        println!("\n  === {} connections ===", n);

        // --- JSON ---
        let json_result = run_format(
            cli,
            &token,
            n,
            "JSON",
            &payload_json,
            json_size,
            "compression=false&protocol_version=1",
            duration,
        )
        .await;

        // --- MsgPack ---
        let msgpack_result = run_format_binary(
            cli,
            &token,
            n,
            "MsgPack",
            &payload_msgpack,
            msgpack_size,
            "format=msgpack&protocol_version=1",
            duration,
        )
        .await;

        // --- Compressed ---
        let compressed_result = run_format_binary(
            cli,
            &token,
            n,
            "Compressed",
            &payload_compressed,
            compressed_size,
            "compression=false&protocol_version=1",
            duration,
        )
        .await;

        // Summary
        if let (Some(j), Some(m), Some(c)) = (&json_result, &msgpack_result, &compressed_result) {
            let j_rate = j.msg_per_sec();
            let m_rate = m.msg_per_sec();
            let c_rate = c.msg_per_sec();
            println!("\n    Summary @ {} connections:", n);
            println!("      JSON:       {} msg/s", stats::fmt_rate(j_rate));
            println!(
                "      MsgPack:    {} msg/s ({:+.1}%)",
                stats::fmt_rate(m_rate),
                (m_rate / j_rate - 1.0) * 100.0
            );
            println!(
                "      Compressed: {} msg/s ({:+.1}%)",
                stats::fmt_rate(c_rate),
                (c_rate / j_rate - 1.0) * 100.0
            );
        }

        if let Some(r) = json_result {
            all_results.push(r);
        }
        if let Some(r) = msgpack_result {
            all_results.push(r);
        }
        if let Some(r) = compressed_result {
            all_results.push(r);
        }
    }

    all_results
}

#[allow(clippy::too_many_arguments)]
async fn run_format(
    cli: &Cli,
    token: &str,
    n: usize,
    format_name: &str,
    payload: &str,
    payload_len: usize,
    uri_params: &str,
    duration: std::time::Duration,
) -> Option<TierResult> {
    eprint!("    {}: ", format_name);

    let connections =
        protocol::connect_batch(&cli.host, cli.port, token, n, cli.batch_size, uri_params).await;

    if connections.is_empty() {
        eprintln!("FAILED");
        return None;
    }

    let actual = connections.len();
    let total_sent = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));

    let start = tokio::time::Instant::now();
    let mut handles = Vec::with_capacity(actual);

    for ws in connections {
        let sent = total_sent.clone();
        let bytes = total_bytes.clone();
        let msg = payload.to_string();
        let plen = payload_len as u64;
        handles.push(tokio::spawn(async move {
            text_send_loop(ws, &msg, plen, duration, sent, bytes).await
        }));
    }

    let ws_list: Vec<WsStream> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let elapsed = start.elapsed().as_secs_f64();
    let sent = total_sent.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let msg_s = sent as f64 / elapsed;

    eprintln!("{} msg/s", stats::fmt_rate(msg_s));

    let result = TierResult {
        tier: n,
        connected: actual,
        errors: n - actual,
        duration_secs: elapsed,
        messages_sent: sent,
        messages_received: 0,
        bytes_sent: bytes,
        latency: None,
        extra: serde_json::json!({
            "format": format_name,
            "payload_size": payload_len,
            "msg_per_sec": msg_s,
        }),
    };

    protocol::close_all(ws_list).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    Some(result)
}

#[allow(clippy::too_many_arguments)]
async fn run_format_binary(
    cli: &Cli,
    token: &str,
    n: usize,
    format_name: &str,
    payload: &[u8],
    payload_len: usize,
    uri_params: &str,
    duration: std::time::Duration,
) -> Option<TierResult> {
    eprint!("    {}: ", format_name);

    let connections =
        protocol::connect_batch(&cli.host, cli.port, token, n, cli.batch_size, uri_params).await;

    if connections.is_empty() {
        eprintln!("FAILED");
        return None;
    }

    let actual = connections.len();
    let total_sent = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));

    let start = tokio::time::Instant::now();
    let mut handles = Vec::with_capacity(actual);

    for ws in connections {
        let sent = total_sent.clone();
        let bytes = total_bytes.clone();
        let msg = payload.to_vec();
        let plen = payload_len as u64;
        handles.push(tokio::spawn(async move {
            binary_send_loop(ws, msg, plen, duration, sent, bytes).await
        }));
    }

    let ws_list: Vec<WsStream> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let elapsed = start.elapsed().as_secs_f64();
    let sent = total_sent.load(Ordering::Relaxed);
    let bytes = total_bytes.load(Ordering::Relaxed);
    let msg_s = sent as f64 / elapsed;

    eprintln!("{} msg/s", stats::fmt_rate(msg_s));

    let result = TierResult {
        tier: n,
        connected: actual,
        errors: n - actual,
        duration_secs: elapsed,
        messages_sent: sent,
        messages_received: 0,
        bytes_sent: bytes,
        latency: None,
        extra: serde_json::json!({
            "format": format_name,
            "payload_size": payload_len,
            "msg_per_sec": msg_s,
        }),
    };

    protocol::close_all(ws_list).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    Some(result)
}

async fn text_send_loop(
    mut ws: WsStream,
    payload: &str,
    payload_len: u64,
    duration: std::time::Duration,
    total_sent: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) -> WsStream {
    let deadline = tokio::time::Instant::now() + duration;
    let mut local_sent = 0u64;
    let mut local_bytes = 0u64;

    while tokio::time::Instant::now() < deadline {
        if ws
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
        local_sent += 1;
        local_bytes += payload_len;
        if local_sent.is_multiple_of(64) {
            let _ = ws.flush().await;
        }
    }

    total_sent.fetch_add(local_sent, Ordering::Relaxed);
    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    ws
}

async fn binary_send_loop(
    mut ws: WsStream,
    payload: Vec<u8>,
    payload_len: u64,
    duration: std::time::Duration,
    total_sent: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) -> WsStream {
    let deadline = tokio::time::Instant::now() + duration;
    let mut local_sent = 0u64;
    let mut local_bytes = 0u64;

    while tokio::time::Instant::now() < deadline {
        if ws
            .send(Message::Binary(payload.clone().into()))
            .await
            .is_err()
        {
            break;
        }
        local_sent += 1;
        local_bytes += payload_len;
        if local_sent.is_multiple_of(64) {
            let _ = ws.flush().await;
        }
    }

    total_sent.fetch_add(local_sent, Ordering::Relaxed);
    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    ws
}

fn build_msgpack_payload(value: &serde_json::Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(b"M:");
    buf.extend_from_slice(&rmp_serde::to_vec(value).unwrap());
    buf
}

fn build_compressed_payload(json: &str) -> Vec<u8> {
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(b"C:");
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(json.as_bytes()).unwrap();
    buf.extend_from_slice(&encoder.finish().unwrap());
    buf
}
