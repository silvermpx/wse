use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::{LatencySummary, TierResult};
use crate::stats::{self, LatencyHistogram};
use futures_util::SinkExt;
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
        say!("\n  --- {} concurrent connections ---", n);

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

        // Each connection pings PINGS_PER_CONNECTION times
        let mut handles = Vec::with_capacity(actual);
        for ws in connections {
            handles.push(tokio::spawn(ping_loop(ws)));
        }

        let task_results = futures_util::future::join_all(handles).await;

        // Merge all histograms; a connection whose task died answered none.
        let mut merged = LatencyHistogram::new();
        let mut ws_list = Vec::new();
        let mut sent = 0u64;
        let mut cut_short = 0usize;
        for (hist, ws, pings) in task_results.into_iter().flatten() {
            merged.merge(&hist);
            sent += pings;
            if hist.len() < PINGS_PER_CONNECTION as u64 {
                cut_short += 1;
            }
            ws_list.push(ws);
        }
        cut_short += actual - ws_list.len();

        say!("    Total pings: {}", merged.len());
        crate::report::short(
            "pings answered",
            merged.len(),
            (n * PINGS_PER_CONNECTION) as u64,
        );
        if !merged.is_empty() {
            merged.print_summary("    ");
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual + cut_short,
            duration_secs: 0.0,
            messages_sent: sent,
            messages_received: merged.len(),
            bytes_sent: 0,
            latency: Some(LatencySummary::from_histogram(&merged)),
            extra: serde_json::json!({}),
        });

        protocol::close_all(ws_list).await;
        say!();
    }

    results
}

/// A connection's pings, until one goes unanswered: its latencies, the
/// connection, and how many pings it sent.
async fn ping_loop(mut ws: WsStream) -> (LatencyHistogram, WsStream, u64) {
    let mut hist = LatencyHistogram::new();
    let mut sent = 0u64;

    for _ in 0..PINGS_PER_CONNECTION {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let t0 = tokio::time::Instant::now();

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
        sent += 1;

        // Wait for PONG (with timeout): a ping it never answers ends the
        // connection's run, counted short.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut answered = false;
        while let Some(msg) = super::next_before(&mut ws, deadline).await {
            if let Some(parsed) = protocol::parse_wse_message(&msg) {
                if protocol::is_pong(&parsed) {
                    let rtt_us = t0.elapsed().as_micros() as u64;
                    hist.record_us(rtt_us.max(1));
                    answered = true;
                    break;
                }
            }
            // Not a pong, keep reading
        }
        if !answered {
            break;
        }
    }

    (hist, ws, sent)
}
