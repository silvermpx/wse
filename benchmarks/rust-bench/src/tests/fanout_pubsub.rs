use crate::config::Cli;
use crate::protocol::{self};
use crate::report::{self, LatencySummary, TierResult};
use crate::stats::{self};
use std::sync::atomic::Ordering;

/// Test 9: Fan-out Pub/Sub (Redis, single server)
///
/// Server publishes via Redis, listeners receive and fan-out to WebSocket clients.
/// Measures Redis round-trip overhead on top of raw broadcast.
///
/// Path: Python publish() -> Rust cmd_tx -> Redis PUBLISH wse:bench_topic
///       -> Redis PSUBSCRIBE wse:* -> Rust listener_task -> dispatch_to_subscribers
///       -> WebSocket send to N clients
///
/// Server auto-subscribes each connecting client to "bench_topic".
///
/// Requires: bench_fanout_server.py --mode pubsub --redis-url redis://localhost:6379
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "TEST 9: Fan-out Pub/Sub (Redis, single server)",
        &format!(
            "Server publishes via Redis to N subscribers for {}s. Measures Redis overhead.",
            cli.duration
        ),
    );

    let tiers = cli.tiers_for(crate::config::TestName::FanoutPubsub);
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

        // Use the same receive measurement as fanout_broadcast
        let result = super::fanout_broadcast::measure_fanout(
            connections,
            std::time::Duration::from_secs(cli.duration),
        )
        .await;

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
            println!("    Delivery latency (includes Redis round-trip):");
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
