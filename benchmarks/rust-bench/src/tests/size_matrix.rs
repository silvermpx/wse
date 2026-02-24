use crate::config::{Cli, MATRIX_SIZES};
use crate::protocol::{self, WsStream};
use crate::report::{self, TierResult};
use crate::stats;
use futures_util::SinkExt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Test 4: Payload Size x Connections Matrix
/// Full throughput matrix across payload sizes and connection counts.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 4: Payload Size x Connections Matrix",
        "Throughput (msg/s) for each (size, connections) pair. 5s burst each.",
    );

    let tiers = cli.tiers_for(crate::config::TestName::SizeMatrix);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let sizes = MATRIX_SIZES;
    let burst_secs = 5u64; // Shorter than full test for matrix cells

    let mut results = Vec::new();
    let mut matrix: HashMap<(usize, usize), TierResult> = HashMap::new();

    for &size in sizes {
        let payload = protocol::build_payload(size);
        let actual_size = payload.len();
        println!(
            "\n  === Payload: {} ({} bytes actual) ===",
            format_size(size),
            actual_size
        );

        for &n in &tiers {
            eprint!("    {} conns: ", n);

            let connections = protocol::connect_batch(
                &cli.host,
                cli.port,
                &token,
                n,
                cli.batch_size,
                "compression=false&protocol_version=1",
            )
            .await;

            if connections.is_empty() {
                eprintln!("FAILED");
                continue;
            }

            let actual = connections.len();
            let total_sent = Arc::new(AtomicU64::new(0));
            let total_bytes = Arc::new(AtomicU64::new(0));
            let duration = std::time::Duration::from_secs(burst_secs);

            let start = tokio::time::Instant::now();
            let mut handles = Vec::with_capacity(actual);

            for ws in connections {
                let sent = total_sent.clone();
                let bytes = total_bytes.clone();
                let msg = payload.clone();
                let plen = actual_size as u64;

                handles.push(tokio::spawn(async move {
                    matrix_send_loop(ws, &msg, plen, duration, sent, bytes).await
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
            let mb_s = bytes as f64 / elapsed / 1_000_000.0;

            eprintln!("{} msg/s, {:.1} MB/s", stats::fmt_rate(msg_s), mb_s);

            let tier_result = TierResult {
                tier: n,
                connected: actual,
                errors: n - actual,
                duration_secs: elapsed,
                messages_sent: sent,
                messages_received: 0,
                bytes_sent: bytes,
                latency: None,
                extra: serde_json::json!({
                    "payload_size": size,
                    "actual_payload_size": actual_size,
                    "msg_per_sec": msg_s,
                    "mb_per_sec": mb_s,
                }),
            };

            matrix.insert((size, n), tier_result.clone());
            results.push(tier_result);

            protocol::close_all(ws_list).await;

            // Brief pause between cells
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    // Print matrix table
    report::print_matrix_table(sizes, &tiers, &matrix);

    results
}

async fn matrix_send_loop(
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
    let msg = Message::Text(payload.to_string().into());

    while tokio::time::Instant::now() < deadline {
        if ws.send(msg.clone()).await.is_err() {
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

fn format_size(n: usize) -> String {
    if n >= 1_048_576 {
        format!("{} MB", n / 1_048_576)
    } else if n >= 1_024 {
        format!("{} KB", n / 1_024)
    } else {
        format!("{} B", n)
    }
}
