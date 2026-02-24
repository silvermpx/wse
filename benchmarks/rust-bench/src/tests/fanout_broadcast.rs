use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::{self, LatencySummary, TierResult};
use crate::stats::{self, LatencyHistogram};
use futures_util::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// Test 8: Fan-out Broadcast (no Redis)
///
/// Server broadcasts to ALL connected clients via server.broadcast().
/// Measures raw Rust tokio fan-out throughput — one message in, N deliveries out.
///
/// Server embeds seq + ts_us in each message for loss detection and latency.
///
/// Requires: bench_fanout_server.py --mode broadcast
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 8: Fan-out Broadcast (no Redis)",
        &format!(
            "Server broadcasts to N subscribers for {}s. Measures delivery throughput.",
            cli.duration
        ),
    );

    let tiers = cli.tiers_for(crate::config::TestName::FanoutBroadcast);
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  --- {} subscribers ---", n);

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
        let result = measure_fanout(connections, Duration::from_secs(cli.duration)).await;

        let msg_per_sec = result.total_received as f64 / result.elapsed;
        let mb_per_sec = result.total_bytes as f64 / result.elapsed / 1_000_000.0;
        let per_sub = msg_per_sec / actual as f64;
        let loss = result.total_gaps.load(Ordering::Relaxed);

        println!("    Duration:       {:.2}s", result.elapsed);
        println!(
            "    Received:       {} ({}/s fan-out)",
            stats::fmt_num(result.total_received),
            stats::fmt_rate(msg_per_sec)
        );
        println!(
            "    Bandwidth:      {}/s",
            stats::fmt_bytes_per_sec(result.total_bytes as f64 / result.elapsed)
        );
        println!("    Per-subscriber: {:.0} msg/s", per_sub);
        if loss > 0 {
            println!("    Seq gaps:       {}", loss);
        }

        if !result.latency.is_empty() {
            println!("    Delivery latency:");
            result.latency.print_summary("      ");
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual,
            duration_secs: result.elapsed,
            messages_sent: 0,
            messages_received: result.total_received,
            bytes_sent: 0,
            latency: if result.latency.is_empty() {
                None
            } else {
                Some(LatencySummary::from_histogram(&result.latency))
            },
            extra: serde_json::json!({
                "fanout_msg_per_sec": msg_per_sec,
                "per_subscriber_msg_s": per_sub,
                "mb_per_sec": mb_per_sec,
                "seq_gaps": loss,
            }),
        });

        protocol::close_all(result.connections).await;
        println!();
    }

    report::print_fanout_table(&results);
    results
}

pub(crate) struct FanoutResult {
    pub total_received: u64,
    pub total_bytes: u64,
    pub total_gaps: Arc<AtomicU64>,
    pub elapsed: f64,
    pub latency: LatencyHistogram,
    pub connections: Vec<WsStream>,
}

pub(crate) async fn measure_fanout(connections: Vec<WsStream>, duration: Duration) -> FanoutResult {
    let actual = connections.len();
    let total_received = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let total_gaps = Arc::new(AtomicU64::new(0));
    let latency = Arc::new(Mutex::new(LatencyHistogram::new()));

    // Warmup: drain queued messages (server_ready, early broadcasts).
    // 1 second lets the server stabilize and fill TCP buffers.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let start = tokio::time::Instant::now();
    let mut handles = Vec::with_capacity(actual);

    for ws in connections {
        let recv = total_received.clone();
        let bytes = total_bytes.clone();
        let gaps = total_gaps.clone();
        let lat = latency.clone();

        handles.push(tokio::spawn(receive_loop(
            ws, duration, recv, bytes, gaps, lat,
        )));
    }

    let ws_list: Vec<WsStream> = futures_util::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let elapsed = start.elapsed().as_secs_f64();

    let merged_latency = match Arc::try_unwrap(latency) {
        Ok(mutex) => mutex.into_inner(),
        Err(arc) => {
            // All tasks done, but Arc still shared — extract via merge
            let guard = arc.lock().await;
            let mut copy = LatencyHistogram::new();
            copy.merge(&guard);
            copy
        }
    };

    FanoutResult {
        total_received: total_received.load(Ordering::Relaxed),
        total_bytes: total_bytes.load(Ordering::Relaxed),
        total_gaps,
        elapsed,
        latency: merged_latency,
        connections: ws_list,
    }
}

pub(crate) async fn receive_loop(
    mut ws: WsStream,
    duration: Duration,
    total_received: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
    total_gaps: Arc<AtomicU64>,
    latency: Arc<Mutex<LatencyHistogram>>,
) -> WsStream {
    let deadline = tokio::time::Instant::now() + duration;
    let mut local_received = 0u64;
    let mut local_bytes = 0u64;
    let mut local_gaps = 0u64;
    let mut local_hist = LatencyHistogram::new();
    let mut last_seq: i64 = -1;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                local_bytes += msg.len() as u64;
                local_received += 1;

                if let Some(parsed) = parse_wse_message(&msg) {
                    // Latency: ts_us embedded by server
                    if let Some(ts_us) = parsed
                        .get("p")
                        .and_then(|p| p.get("ts_us"))
                        .and_then(|v| v.as_u64())
                        .or_else(|| parsed.get("ts_us").and_then(|v| v.as_u64()))
                    {
                        let now_us = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_micros() as u64;
                        let lat_us = now_us.saturating_sub(ts_us);
                        local_hist.record_us(lat_us.max(1));
                    }

                    // Sequence gap detection
                    if let Some(seq) = parsed
                        .get("p")
                        .and_then(|p| p.get("seq"))
                        .and_then(|v| v.as_i64())
                        .or_else(|| parsed.get("seq").and_then(|v| v.as_i64()))
                    {
                        if last_seq >= 0 && seq > last_seq + 1 {
                            local_gaps += (seq - last_seq - 1) as u64;
                        }
                        last_seq = seq;
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break, // duration ended
        }
    }

    total_received.fetch_add(local_received, Ordering::Relaxed);
    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    if local_gaps > 0 {
        total_gaps.fetch_add(local_gaps, Ordering::Relaxed);
    }

    if !local_hist.is_empty() {
        let mut hist = latency.lock().await;
        hist.merge(&local_hist);
    }

    ws
}
