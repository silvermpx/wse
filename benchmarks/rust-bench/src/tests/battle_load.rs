use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats::{self, LatencyHistogram};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::Message;

const SUBSCRIBE_SETTLE_MS: u64 = 1000;

/// Shared counters for load receive tasks.
struct LoadCounters {
    received: AtomicU64,
    bytes: AtomicU64,
    broadcast_all: AtomicU64,
    topic: AtomicU64,
    gaps: AtomicU64,
}

impl LoadCounters {
    fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            broadcast_all: AtomicU64::new(0),
            topic: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
        }
    }
}

/// Load tiers for the 50K test.
const LOAD_TIERS: &[usize] = &[10, 50, 100, 500, 1_000, 5_000, 10_000, 20_000, 50_000];

/// Battle Load Test: 50K Subscriber Stress
///
/// Connects increasing numbers of subscribers (up to 50K), subscribes all to a
/// topic, and measures fan-out throughput and correctness under heavy load.
/// Tests both broadcast_all and topic-based delivery at scale.
///
/// Requires: python benchmarks/bench_battle_server.py
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE LOAD TEST: 50K Subscriber Stress",
        &format!(
            "Connect up to 50K subscribers, verify delivery at scale for {}s per tier.",
            cli.duration
        ),
    );

    let tiers = if let Some(ref custom) = cli.tiers {
        custom.clone()
    } else {
        LOAD_TIERS.to_vec()
    };

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = Duration::from_secs(cli.duration);
    let mut results = Vec::new();

    for &n in &tiers {
        println!("\n  === {} subscribers ===", n);

        // Connect
        let mut connections = protocol::connect_batch(
            &cli.host,
            cli.port,
            &token,
            n,
            cli.batch_size,
            "compression=false&protocol_version=1",
        )
        .await;

        let actual = connections.len();
        if actual < n / 2 {
            println!(
                "    SKIPPED: only {}/{} connected (ulimit or server max-connections?)",
                actual, n
            );
            protocol::close_all(connections).await;
            continue;
        }
        if actual < n {
            println!("    WARNING: {}/{} connected", actual, n);
        }

        // Subscribe all to "battle_all"
        print!("    Subscribing to topic... ");
        for ws in connections.iter_mut() {
            let cmd = serde_json::json!({
                "t": "battle_cmd",
                "p": {"action": "subscribe", "topics": ["battle_all"]}
            });
            let _ = ws.send(Message::Text(cmd.to_string().into())).await;
        }
        println!("done");

        // Settle + warmup
        tokio::time::sleep(Duration::from_millis(SUBSCRIBE_SETTLE_MS)).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Receive
        let counters = Arc::new(LoadCounters::new());
        let latency = Arc::new(Mutex::new(LatencyHistogram::new()));

        let deadline = tokio::time::Instant::now() + duration;
        let start = tokio::time::Instant::now();
        let mut handles = Vec::with_capacity(actual);

        for ws in connections {
            let ctrs = counters.clone();
            let lat = latency.clone();
            handles.push(tokio::spawn(load_receive_loop(ws, deadline, ctrs, lat)));
        }

        let ws_list: Vec<WsStream> = futures_util::future::join_all(handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .collect();

        let elapsed = start.elapsed().as_secs_f64();

        let received = counters.received.load(Ordering::Relaxed);
        let bytes = counters.bytes.load(Ordering::Relaxed);
        let ba_count = counters.broadcast_all.load(Ordering::Relaxed);
        let topic_count = counters.topic.load(Ordering::Relaxed);
        let gaps = counters.gaps.load(Ordering::Relaxed);

        let del_per_sec = received as f64 / elapsed;
        let unique_gaps = if actual > 0 {
            gaps / actual as u64
        } else {
            gaps
        };

        // Correctness checks
        let ba_ok = ba_count > 0;
        let topic_ok = topic_count > 0;

        println!("    Duration:       {:.2}s", elapsed);
        println!(
            "    Deliveries:     {}/s ({} total)",
            stats::fmt_rate(del_per_sec),
            received
        );
        println!(
            "    Bandwidth:      {}",
            stats::fmt_bytes_per_sec(bytes as f64 / elapsed)
        );
        println!(
            "    broadcast_all:  {} {}",
            ba_count,
            if ba_ok { "[OK]" } else { "[FAIL]" }
        );
        println!(
            "    topic msgs:     {} {}",
            topic_count,
            if topic_ok { "[OK]" } else { "[FAIL]" }
        );
        if unique_gaps > 0 {
            println!("    Seq gaps:       ~{} unique", unique_gaps);
        }

        let merged_latency = match Arc::try_unwrap(latency) {
            Ok(mutex) => mutex.into_inner(),
            Err(arc) => {
                let guard = arc.lock().await;
                let mut copy = LatencyHistogram::new();
                copy.merge(&guard);
                copy
            }
        };

        if !merged_latency.is_empty() {
            println!("    Delivery latency:");
            merged_latency.print_summary("      ");
        }

        results.push(TierResult {
            tier: n,
            connected: actual,
            errors: n - actual,
            duration_secs: elapsed,
            messages_sent: 0,
            messages_received: received,
            bytes_sent: 0,
            latency: if merged_latency.is_empty() {
                None
            } else {
                Some(crate::report::LatencySummary::from_histogram(&merged_latency))
            },
            extra: serde_json::json!({
                "deliveries_per_sec": del_per_sec,
                "broadcast_all_ok": ba_ok,
                "topic_ok": topic_ok,
                "seq_gaps": unique_gaps,
            }),
        });

        protocol::close_all(ws_list).await;
        println!();
    }

    // Summary table
    if !results.is_empty() {
        println!("\n  Load Test Summary:");
        println!(
            "  {:<12} {:<12} {:<15} {:<10} {:<10} {:<10} {:<8}",
            "Subscribers", "Connected", "Del/s", "p50", "p99", "Gaps", "Status"
        );
        println!("  {}", "-".repeat(75));
        for r in &results {
            let ok = r
                .extra
                .get("broadcast_all_ok")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                && r.extra
                    .get("topic_ok")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
            let del_per_sec = r
                .extra
                .get("deliveries_per_sec")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let gaps = r
                .extra
                .get("seq_gaps")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let (p50_str, p99_str) = match &r.latency {
                Some(lat) => (format!("{:.1}ms", lat.p50), format!("{:.1}ms", lat.p99)),
                None => ("-".to_string(), "-".to_string()),
            };
            println!(
                "  {:<12} {:<12} {:<15} {:<10} {:<10} {:<10} {:<8}",
                r.tier,
                r.connected,
                stats::fmt_rate(del_per_sec),
                p50_str,
                p99_str,
                gaps,
                if ok { "PASS" } else { "FAIL" }
            );
        }
    }

    results
}

async fn load_receive_loop(
    mut ws: WsStream,
    deadline: tokio::time::Instant,
    counters: Arc<LoadCounters>,
    latency: Arc<Mutex<LatencyHistogram>>,
) -> WsStream {
    let mut local_received = 0u64;
    let mut local_bytes = 0u64;
    let mut local_ba = 0u64;
    let mut local_topic = 0u64;
    let mut local_gaps = 0u64;
    let mut local_hist = LatencyHistogram::new();
    let mut last_seq_by_ch: HashMap<String, i64> = HashMap::new();

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
                    // Channel categorization
                    let ch_name = parsed
                        .get("p")
                        .and_then(|p| p.get("ch"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");

                    match ch_name {
                        "all" => local_ba += 1,
                        "unknown" => {}
                        _ => local_topic += 1,
                    }

                    // Latency
                    if let Some(ts_us) = parsed
                        .get("p")
                        .and_then(|p| p.get("ts_us"))
                        .and_then(|v| v.as_u64())
                    {
                        let now_us = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_micros() as u64;
                        let lat_us = now_us.saturating_sub(ts_us);
                        local_hist.record_us(lat_us.max(1));
                    }

                    // Per-channel sequence gap tracking
                    if let Some(seq) = parsed
                        .get("p")
                        .and_then(|p| p.get("seq"))
                        .and_then(|v| v.as_i64())
                    {
                        let last = last_seq_by_ch.entry(ch_name.to_string()).or_insert(-1);
                        if *last >= 0 && seq > *last + 1 {
                            local_gaps += (seq - *last - 1) as u64;
                        }
                        *last = seq;
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }

    counters.received.fetch_add(local_received, Ordering::Relaxed);
    counters.bytes.fetch_add(local_bytes, Ordering::Relaxed);
    counters.broadcast_all.fetch_add(local_ba, Ordering::Relaxed);
    counters.topic.fetch_add(local_topic, Ordering::Relaxed);
    if local_gaps > 0 {
        counters.gaps.fetch_add(local_gaps, Ordering::Relaxed);
    }
    if !local_hist.is_empty() {
        let mut hist = latency.lock().await;
        hist.merge(&local_hist);
    }

    ws
}
