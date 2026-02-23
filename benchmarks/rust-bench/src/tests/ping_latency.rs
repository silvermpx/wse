use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::{LatencySummary, TierResult};
use crate::stats::{self, LatencyHistogram};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::Message;

const PINGS_PER_CONNECTION: usize = 50;

/// Test 2: Ping/Pong Latency Under Load
/// N persistent connections, each pinging multiple times. Measures tail latency.
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 2: Ping/Pong Latency Under Concurrent Load",
        &format!(
            "N persistent connections, each pinging {}x. Measures tail latency.",
            PINGS_PER_CONNECTION
        ),
    );

    let tiers = cli.tiers_for(crate::config::TestName::PingLatency);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  --- {} concurrent connections ---", n);

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
            println!("    FAILED: no connections");
            continue;
        }

        let actual = connections.len();

        // Each connection pings PINGS_PER_CONNECTION times
        let mut handles = Vec::with_capacity(actual);
        for ws in connections {
            handles.push(tokio::spawn(ping_loop(ws)));
        }

        let task_results = futures_util::future::join_all(handles).await;

        // Merge all histograms
        let mut merged = LatencyHistogram::new();
        let mut ws_list = Vec::new();
        for (hist, ws) in task_results.into_iter().flatten() {
            merged.merge(&hist);
            ws_list.push(ws);
        }

        println!("    Total pings: {}", merged.len());
        merged.print_summary("    ");

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual,
            duration_secs: 0.0,
            messages_sent: merged.len(),
            messages_received: merged.len(),
            bytes_sent: 0,
            latency: Some(LatencySummary::from_histogram(&merged)),
            extra: serde_json::json!({}),
        });

        protocol::close_all(ws_list).await;
        println!();
    }

    results
}

async fn ping_loop(mut ws: WsStream) -> (LatencyHistogram, WsStream) {
    let mut hist = LatencyHistogram::new();

    for _ in 0..PINGS_PER_CONNECTION {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let ping_msg = serde_json::json!({
            "t": "ping",
            "p": { "client_timestamp": now_ms }
        });

        if ws
            .send(Message::Text(ping_msg.to_string().into()))
            .await
            .is_err()
        {
            break;
        }

        // Wait for PONG (with timeout)
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while let Ok(Some(Ok(msg))) = tokio::time::timeout_at(deadline, ws.next()).await {
            if let Some(parsed) = protocol::parse_wse_message(&msg) {
                if protocol::extract_pong_timestamp(&parsed).is_some() {
                    let actual_rtt = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let rtt = actual_rtt.saturating_sub(now_ms) as f64;
                    hist.record_ms(rtt);
                    break;
                }
            }
            // Not a pong, keep reading
        }
    }

    (hist, ws)
}
