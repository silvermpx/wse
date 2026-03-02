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
        if actual < n / 2 {
            println!(
                "    SKIPPED: only {}/{} connected (server max-connections too low?)",
                actual, n
            );
            protocol::close_all(connections).await;
            continue;
        }
        if actual < n {
            println!("    WARNING: {}/{} connected", actual, n);
        }
        // Capture drops counter before measurement for per-tier delta
        let drops_before =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(cli.port)).await;

        let result = measure_fanout(connections, Duration::from_secs(cli.duration)).await;

        let deliveries_per_sec = result.total_received as f64 / result.elapsed;
        let mb_per_sec = result.total_bytes as f64 / result.elapsed / 1_000_000.0;
        let published_per_sec = deliveries_per_sec / actual as f64;
        let raw_gaps = result.total_gaps.load(Ordering::Relaxed);
        // Gaps are counted per-subscriber; normalize to unique message gaps
        let unique_gaps = if actual > 0 {
            raw_gaps / actual as u64
        } else {
            raw_gaps
        };

        println!("    Duration:       {:.2}s", result.elapsed);
        println!(
            "    Published:      {}/s (unique messages from server)",
            stats::fmt_rate(published_per_sec)
        );
        println!(
            "    Deliveries:     {}/s (fan-out: {} x {} subs)",
            stats::fmt_rate(deliveries_per_sec),
            stats::fmt_rate(published_per_sec),
            actual
        );
        println!(
            "    Bandwidth:      {}",
            stats::fmt_bytes_per_sec(result.total_bytes as f64 / result.elapsed)
        );
        if unique_gaps > 0 {
            println!("    Seq gaps:       ~{} unique messages lost", unique_gaps);
        }

        if !result.latency.is_empty() {
            println!("    Delivery latency:");
            result.latency.print_summary("      ");
        }

        let drops_after =
            protocol::query_slow_consumer_drops(&cli.host, cli.metrics_port_for(cli.port)).await;
        let tier_drops = match (drops_before, drops_after) {
            (Some(b), Some(a)) => Some(a.saturating_sub(b)),
            _ => drops_after,
        };
        match tier_drops {
            Some(0) => println!("    Server drops:   0 (ok)"),
            Some(d) => println!("    Server drops:   {} (slow consumer drops)", d),
            None => {} // metrics endpoint not available, skip silently
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual,
            duration_secs: result.elapsed,
            messages_sent: (published_per_sec * result.elapsed) as u64,
            messages_received: result.total_received,
            bytes_sent: 0,
            latency: if result.latency.is_empty() {
                None
            } else {
                Some(LatencySummary::from_histogram(&result.latency))
            },
            extra: serde_json::json!({
                "published_per_sec": published_per_sec,
                "deliveries_per_sec": deliveries_per_sec,
                "mb_per_sec": mb_per_sec,
                "seq_gaps": unique_gaps,
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

    // Active drain warmup: read and discard all buffered messages for 2s.
    // This ensures measurement starts with a clean pipeline (no stale messages
    // that would show multi-second latency from inter-tier publishing).
    let warmup_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut drain_handles = Vec::with_capacity(actual);
    for ws in connections {
        drain_handles.push(tokio::spawn(async move {
            let mut ws = ws;
            loop {
                let remaining =
                    warmup_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, ws.next()).await {
                    Ok(Some(Ok(_))) => {} // discard
                    _ => break,
                }
            }
            ws
        }));
    }
    let connections: Vec<WsStream> = futures_util::future::join_all(drain_handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    // Timestamp gating: only measure latency for messages published after this point.
    let measurement_start_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;

    // All tasks share the same deadline so measurement window is identical.
    let deadline = tokio::time::Instant::now() + duration;
    let start = tokio::time::Instant::now();
    let mut handles = Vec::with_capacity(actual);

    for ws in connections {
        let recv = total_received.clone();
        let bytes = total_bytes.clone();
        let gaps = total_gaps.clone();
        let lat = latency.clone();

        handles.push(tokio::spawn(receive_loop(
            ws,
            deadline,
            recv,
            bytes,
            gaps,
            lat,
            measurement_start_us,
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
    deadline: tokio::time::Instant,
    total_received: Arc<AtomicU64>,
    total_bytes: Arc<AtomicU64>,
    total_gaps: Arc<AtomicU64>,
    latency: Arc<Mutex<LatencyHistogram>>,
    measurement_start_us: u64,
) -> WsStream {
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
                if let Some(parsed) = parse_wse_message(&msg) {
                    local_bytes += msg.len() as u64;
                    local_received += 1;

                    // Latency: only measure for messages published after measurement started
                    // (stale messages from pipeline backlog would show multi-second latency)
                    let ts_us = parsed
                        .get("p")
                        .and_then(|p| p.get("ts_us"))
                        .and_then(|v| v.as_u64())
                        .or_else(|| parsed.get("ts_us").and_then(|v| v.as_u64()));

                    if let Some(ts) = ts_us {
                        if ts >= measurement_start_us {
                            let now_us = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_micros() as u64;
                            let lat_us = now_us.saturating_sub(ts);
                            local_hist.record_us(lat_us.max(1));
                        }
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
