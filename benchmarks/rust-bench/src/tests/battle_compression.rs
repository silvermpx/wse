use crate::config::Cli;
use crate::protocol::{self, query_health};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

struct CheckResult {
    passed: u32,
    failed: u32,
}

impl CheckResult {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
        }
    }

    fn check(&mut self, name: &str, condition: bool) {
        if condition {
            self.passed += 1;
            println!("    [PASS] {}", name);
        } else {
            self.failed += 1;
            println!("    [FAIL] {}", name);
        }
    }
}

/// Battle Test: Inter-Peer zstd Compression
///
/// Verifies zstd compression on cluster wire by comparing compressed bytes
/// received vs message count. With ~1KB messages, compressed bytes/message
/// should be significantly less than 1024 if zstd is working.
///
/// Uses health_snapshot metrics only -- no high-throughput WebSocket clients
/// needed. Server A floods broadcast_all at 150K+/s with 1KB messages,
/// which would overwhelm subscriber clients. Instead, we:
///   1. Subscribe via a single health connection (triggers RESYNC)
///   2. Measure cluster_bytes_received and cluster_messages_received on Server B
///   3. Verify compressed bytes/message ratio shows compression
///
/// Note: Server A (publisher) floods broadcast_all to all connections,
/// making health queries unreliable on that side. All checks use Server B.
///
/// Requires: 2 cluster servers with --message-size 1024
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007 --message-size 1024
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "BATTLE TEST: Inter-Peer Compression",
        &format!(
            "Verify zstd compression on large cluster messages (:{} -> :{}).",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = Duration::from_secs(cli.duration);
    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect to Server B (subscriber side -- not flooded) and verify
    // =========================================================================
    println!(
        "\n  Phase 1: Connect to Server B (:{}) and verify cluster",
        port2
    );
    println!("    (Server A health skipped -- publisher floods 500K/s to all connections)");

    let mut ws_b = match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("    Failed to connect to Server B -- {}", e);
            return Vec::new();
        }
    };
    println!("    Connected to Server B (:{})", port2);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let health_b = query_health(&mut ws_b, 5).await;
    if let Some(ref h) = health_b {
        let connected = h
            .get("cluster_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let unknown = h
            .get("cluster_unknown_message_types")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);
        checks.check("Cluster connected (Server B)", connected);
        checks.check(
            &format!("No unknown message types (got {})", unknown),
            unknown == 0,
        );
    } else {
        println!("    WARNING: No health response from Server B");
        checks.check("Cluster connected (Server B)", false);
        checks.check("No unknown message types", false);
    }

    // =========================================================================
    // Phase 2: Subscribe and record initial metrics
    // =========================================================================
    println!("\n  Phase 2: Subscribe to battle_all and record initial metrics");

    // Subscribe this connection to battle_all (triggers RESYNC to Server A)
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_b.send(Message::Text(cmd.to_string().into())).await;
    println!("    Subscribed to battle_all");

    // Wait for RESYNC propagation
    println!("    Waiting 3s for RESYNC propagation...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Record initial metrics on Server B (bytes_received = compressed cluster wire bytes)
    let initial = query_health(&mut ws_b, 5).await;
    let initial_bytes = initial
        .as_ref()
        .and_then(|h| h.get("cluster_bytes_received").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let initial_delivered = initial
        .as_ref()
        .and_then(|h| h.get("cluster_messages_delivered").and_then(|v| v.as_u64()))
        .unwrap_or(0);

    println!(
        "    Initial: bytes_received={}, messages_delivered={}",
        initial_bytes, initial_delivered
    );

    // =========================================================================
    // Phase 3: Let messages flow for duration
    // =========================================================================
    println!(
        "\n  Phase 3: Let cluster messages flow for {}s",
        duration.as_secs()
    );
    tokio::time::sleep(duration).await;

    // =========================================================================
    // Phase 4: Verify compression via metrics
    // =========================================================================
    println!("\n  Phase 4: Verify compression ratio via health metrics");

    let final_health = query_health(&mut ws_b, 5).await;
    let final_bytes = final_health
        .as_ref()
        .and_then(|h| h.get("cluster_bytes_received").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let final_delivered = final_health
        .as_ref()
        .and_then(|h| h.get("cluster_messages_delivered").and_then(|v| v.as_u64()))
        .unwrap_or(0);

    let bytes_delta = final_bytes.saturating_sub(initial_bytes);
    let delivered_delta = final_delivered.saturating_sub(initial_delivered);

    println!(
        "    Final: bytes_received={}, messages_delivered={}",
        final_bytes, final_delivered
    );
    println!(
        "    Delta: bytes_received={}, messages_delivered={}",
        bytes_delta, delivered_delta
    );

    // Check 1: Cluster bytes are flowing
    checks.check(
        &format!("Cluster bytes received > 0 (got {})", bytes_delta),
        bytes_delta > 0,
    );

    // Check 2: Messages are being delivered to local subscribers
    checks.check(
        &format!("Cluster messages delivered > 0 (got {})", delivered_delta),
        delivered_delta > 0,
    );

    // Check 3: Compression ratio -- bytes_received / (messages_delivered * msg_size)
    // Server A sends ~1KB messages. With zstd on repetitive "xxx...xxx" padding,
    // compression should achieve 50%+ reduction.
    //
    // bytes_received = compressed bytes on cluster wire (includes all protocol overhead)
    // messages_delivered = application messages delivered to our subscriber
    //
    // With CAP_INTEREST_ROUTING, only subscribed topics + broadcast_all are forwarded.
    // bytes_received includes framing + protocol overhead, so the ratio overestimates
    // the per-message compressed size. Despite this, with good compression the ratio
    // should be well under 1024 bytes per delivered message.
    if delivered_delta > 0 && bytes_delta > 0 {
        let avg_bytes_per_delivered = bytes_delta as f64 / delivered_delta as f64;
        let compression_pct = (avg_bytes_per_delivered / 1024.0) * 100.0;
        println!(
            "    Avg cluster bytes / delivered message: {:.1} (uncompressed ~1024)",
            avg_bytes_per_delivered
        );
        println!("    Compression ratio: {:.1}% of original", compression_pct);

        // With zstd on repetitive data, avg should be well under 1024.
        // Use 800 as threshold (at least 20% compression).
        checks.check(
            &format!(
                "Compression active: {:.0} bytes/msg < 800 (uncompressed ~1024, {:.0}% ratio)",
                avg_bytes_per_delivered, compression_pct
            ),
            avg_bytes_per_delivered < 800.0,
        );
    } else {
        checks.check("Compression ratio measurable", false);
    }

    // Cleanup
    protocol::close_all(vec![ws_b]).await;

    // =========================================================================
    // Summary
    // =========================================================================
    println!(
        "\n  Result: {}/{} checks passed",
        checks.passed,
        checks.passed + checks.failed
    );
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
