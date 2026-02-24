use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::{LatencySummary, TierResult};
use crate::stats::{self, LatencyHistogram};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::protocol::Message;

const PING_INTERVAL_SECS: u64 = 5;

/// Test 6: Sustained Hold
/// Can the server maintain N connections for extended time without degradation?
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let hold_secs = cli.duration.max(30); // At least 30s for sustained test

    stats::print_header(
        "TEST 6: Sustained Hold",
        &format!(
            "Hold N connections for {}s with periodic PING/PONG. Monitor stability.",
            hold_secs
        ),
    );

    let tiers = cli.tiers_for(crate::config::TestName::SustainedHold);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  --- {} connections for {}s ---", n, hold_secs);

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
        let disconnected = Arc::new(AtomicUsize::new(0));
        let duration = std::time::Duration::from_secs(hold_secs);

        let start = tokio::time::Instant::now();
        let mut handles = Vec::with_capacity(actual);

        for ws in connections {
            let disc = disconnected.clone();
            handles.push(tokio::spawn(
                async move { hold_loop(ws, duration, disc).await },
            ));
        }

        // Progress reporting
        let disc_ref = disconnected.clone();
        let progress = tokio::spawn(async move {
            let total = hold_secs;
            for elapsed in 1..=total {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let d = disc_ref.load(Ordering::Relaxed);
                eprint!(
                    "\r    Elapsed: {}s / {}s | Disconnected: {}",
                    elapsed, total, d
                );
            }
            eprintln!();
        });

        let task_results = futures_util::future::join_all(handles).await;
        let _ = progress.await;

        // Collect results
        let mut merged = LatencyHistogram::new();
        let mut ws_list = Vec::new();
        let mut total_pings = 0u64;

        for (hist, ws, pings) in task_results.into_iter().flatten() {
            merged.merge(&hist);
            total_pings += pings;
            ws_list.push(ws);
        }

        let elapsed = start.elapsed().as_secs_f64();
        let disc_count = disconnected.load(Ordering::Relaxed);
        let survival_rate = (actual - disc_count) as f64 / actual as f64 * 100.0;

        println!(
            "    Survival: {}/{} ({:.1}%)",
            actual - disc_count,
            actual,
            survival_rate
        );
        println!("    Total PINGs: {}", total_pings);
        if !merged.is_empty() {
            println!("    PING/PONG latency:");
            merged.print_summary("      ");
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: disc_count,
            duration_secs: elapsed,
            messages_sent: total_pings,
            messages_received: total_pings - disc_count as u64,
            bytes_sent: 0,
            latency: if merged.is_empty() {
                None
            } else {
                Some(LatencySummary::from_histogram(&merged))
            },
            extra: serde_json::json!({
                "survival_rate_pct": survival_rate,
                "disconnected": disc_count,
                "hold_seconds": hold_secs,
            }),
        });

        protocol::close_all(ws_list).await;
        println!();
    }

    results
}

async fn hold_loop(
    mut ws: WsStream,
    duration: std::time::Duration,
    disconnected: Arc<AtomicUsize>,
) -> (LatencyHistogram, WsStream, u64) {
    let mut hist = LatencyHistogram::new();
    let mut ping_count = 0u64;

    let deadline = tokio::time::Instant::now() + duration;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(PING_INTERVAL_SECS));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if tokio::time::Instant::now() >= deadline {
                    break;
                }

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                let ping = serde_json::json!({
                    "t": "ping",
                    "p": { "client_timestamp": now_ms }
                });

                if ws.send(Message::Text(ping.to_string().into())).await.is_err() {
                    disconnected.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                ping_count += 1;

                // Read pong (with timeout)
                let pong_deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(5);
                loop {
                    match tokio::time::timeout_at(pong_deadline, ws.next()).await {
                        Ok(Some(Ok(msg))) => {
                            if let Some(parsed) = protocol::parse_wse_message(&msg) {
                                if protocol::is_pong(&parsed) {
                                    let rtt_ms = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as u64;
                                    let rtt = rtt_ms.saturating_sub(now_ms) as f64;
                                    hist.record_ms(rtt);
                                    break;
                                }
                            }
                        }
                        Ok(Some(Err(_))) | Ok(None) => {
                            disconnected.fetch_add(1, Ordering::Relaxed);
                            return (hist, ws, ping_count);
                        }
                        Err(_) => break, // Timeout waiting for pong
                    }
                }
            }
        }
    }

    (hist, ws, ping_count)
}
