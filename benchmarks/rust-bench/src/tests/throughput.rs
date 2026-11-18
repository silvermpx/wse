use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::{self, TierResult};
use crate::stats;
use futures_util::SinkExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Test 3: Throughput Saturation
/// All connections blast messages as fast as possible. Finds peak msg/s.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 3: Throughput Saturation",
        &format!(
            "All connections send as fast as possible for {}s. Finds peak msg/s.",
            cli.duration
        ),
    );

    let tiers = cli.tiers_for(crate::config::TestName::Throughput);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let payload = protocol::build_payload(194); // Standard 194-byte trading payload
    let payload_len = payload.len() as u64;
    let mut results = Vec::new();

    for &n in &tiers {
        say!("\n  --- {} connections ---", n);

        let connections = protocol::connect_batch(
            &cli.host,
            cli.port,
            &token,
            n,
            cli.batch_size,
            "compression=false&protocol_version=1",
        )
        .await;

        let actual = connections.len();
        crate::report::short("connections", actual as u64, n as u64);
        if actual == 0 {
            continue;
        }
        let total_sent = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));
        let duration = std::time::Duration::from_secs(cli.duration);

        // Start all senders simultaneously
        let start = tokio::time::Instant::now();
        let mut handles = Vec::with_capacity(actual);

        for ws in connections {
            let sent = total_sent.clone();
            let bytes = total_bytes.clone();
            let msg = payload.clone();
            let plen = payload_len;

            handles.push(tokio::spawn(async move {
                send_loop(ws, &msg, plen, duration, sent, bytes).await
            }));
        }

        // A sender whose connection failed (or whose task died) did not last
        // the run.
        let ended: Vec<(WsStream, bool)> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();
        let lasted = ended.iter().filter(|(_, lasted)| *lasted).count();
        let ws_list: Vec<WsStream> = ended.into_iter().map(|(ws, _)| ws).collect();

        let elapsed = start.elapsed().as_secs_f64();
        let sent = total_sent.load(Ordering::Relaxed);
        let bytes = total_bytes.load(Ordering::Relaxed);

        let msg_per_sec = sent as f64 / elapsed;
        let mb_per_sec = bytes as f64 / elapsed / 1_000_000.0;
        let per_conn = msg_per_sec / actual as f64;

        say!("    Duration: {:.2}s", elapsed);
        say!(
            "    Messages: {} ({}/s)",
            stats::fmt_num(sent),
            stats::fmt_rate(msg_per_sec)
        );
        say!(
            "    Bandwidth: {}",
            stats::fmt_bytes_per_sec(bytes as f64 / elapsed)
        );
        say!("    Per-connection: {:.0} msg/s", per_conn);
        crate::report::short("senders through the run", lasted as u64, n as u64);

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - lasted,
            duration_secs: elapsed,
            messages_sent: sent,
            messages_received: 0,
            bytes_sent: bytes,
            latency: None,
            extra: serde_json::json!({
                "msg_per_sec": msg_per_sec,
                "mb_per_sec": mb_per_sec,
                "per_conn_msg_s": per_conn,
            }),
        });

        protocol::close_all(ws_list).await;
        say!();
    }

    report::print_throughput_table(&results);
    results
}

async fn send_loop(
    mut ws: WsStream,
    payload: &str,
    payload_len: u64,
    duration: std::time::Duration,
    total_sent: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
) -> (WsStream, bool) {
    let deadline = tokio::time::Instant::now() + duration;
    let mut local_sent = 0u64;
    let mut local_bytes = 0u64;
    let mut lasted = true;
    let msg = Message::Text(payload.to_string().into());

    while tokio::time::Instant::now() < deadline {
        if ws.send(msg.clone()).await.is_err() {
            lasted = false;
            break;
        }
        local_sent += 1;
        local_bytes += payload_len;

        // Flush periodically to avoid buffer bloat
        if local_sent.is_multiple_of(64) {
            let _ = ws.flush().await;
        }
    }

    total_sent.fetch_add(local_sent, Ordering::Relaxed);
    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    (ws, lasted)
}
