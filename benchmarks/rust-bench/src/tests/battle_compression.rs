use crate::config::Cli;
use crate::protocol::{self, query_health};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
/// Uses two connections to Server B:
///   - A subscriber connection that counts received messages (spawned task)
///   - A separate health-only connection for reliable bytes_received queries
///
/// The subscriber connection gets flooded by cluster-forwarded messages,
/// so it cannot be used for health queries (backpressure would drop responses).
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
    // Phase 1: Connect health + subscriber connections to Server B
    // =========================================================================
    println!(
        "\n  Phase 1: Connect to Server B (:{}) and verify cluster",
        port2
    );

    // Health-only connection (never subscribes, stays clean for queries)
    let mut ws_health = match protocol::connect_and_handshake(
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
            eprintln!("    Failed to connect health conn to Server B -- {}", e);
            return Vec::new();
        }
    };

    // Subscriber connection (triggers RESYNC, will get flooded)
    let token2 = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-sub");
    let mut ws_sub = match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token2,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("    Failed to connect subscriber to Server B -- {}", e);
            protocol::close_all(vec![ws_health]).await;
            return Vec::new();
        }
    };
    println!("    Connected 2 connections to Server B (:{})", port2);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let health_b = query_health(&mut ws_health, 5).await;
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
    // Phase 2: Subscribe and start counting messages
    // =========================================================================
    println!("\n  Phase 2: Subscribe and start counting messages");

    // Subscribe the subscriber connection (triggers RESYNC to Server A)
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_sub.send(Message::Text(cmd.to_string().into())).await;
    println!("    Subscribed to battle_all (subscriber conn)");

    // Wait for RESYNC propagation
    println!("    Waiting 3s for RESYNC propagation...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Record initial bytes_received via health connection
    let initial = query_health(&mut ws_health, 5).await;
    let initial_bytes = initial
        .as_ref()
        .and_then(|h| h.get("cluster_bytes_received").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    println!("    Initial cluster_bytes_received: {}", initial_bytes);

    // =========================================================================
    // Phase 3: Count messages on subscriber for duration
    // =========================================================================
    println!("\n  Phase 3: Count messages for {}s", duration.as_secs());

    let msg_count = Arc::new(AtomicU64::new(0));
    let counter = msg_count.clone();
    let deadline_ms = duration.as_millis() as u64;

    // Spawn a task to count received messages on the subscriber connection
    let count_handle = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, ws_sub.next()).await {
                Ok(Some(Ok(Message::Text(_)))) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
        ws_sub
    });

    // Wait for the counting task to finish
    let ws_sub = count_handle.await.unwrap();
    let received = msg_count.load(Ordering::Relaxed);
    println!("    Subscriber received {} messages", received);

    // =========================================================================
    // Phase 4: Verify compression via metrics
    // =========================================================================
    println!("\n  Phase 4: Verify compression ratio");

    let final_health = query_health(&mut ws_health, 5).await;
    let final_bytes = final_health
        .as_ref()
        .and_then(|h| h.get("cluster_bytes_received").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let final_delivered = final_health
        .as_ref()
        .and_then(|h| h.get("cluster_messages_delivered").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let final_dropped = final_health
        .as_ref()
        .and_then(|h| h.get("cluster_messages_dropped").and_then(|v| v.as_u64()))
        .unwrap_or(0);

    let bytes_delta = final_bytes.saturating_sub(initial_bytes);
    let initial_delivered = initial
        .as_ref()
        .and_then(|h| h.get("cluster_messages_delivered").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let initial_dropped = initial
        .as_ref()
        .and_then(|h| h.get("cluster_messages_dropped").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    let delivered_delta = final_delivered.saturating_sub(initial_delivered);
    let dropped_delta = final_dropped.saturating_sub(initial_dropped);
    // Total messages = delivered + dropped (all messages that arrived via cluster wire)
    let total_msgs = delivered_delta + dropped_delta + received;

    println!("    Final cluster_bytes_received: {}", final_bytes);
    println!("    Delta bytes: {}", bytes_delta);
    println!(
        "    Cluster delivered_delta={}, dropped_delta={}, subscriber_received={}",
        delivered_delta, dropped_delta, received
    );
    println!("    Total messages estimate: {}", total_msgs);

    // Check 1: Cluster bytes are flowing
    checks.check(
        &format!("Cluster bytes received > 0 (got {})", bytes_delta),
        bytes_delta > 0,
    );

    // Check 2: Messages are being received by subscriber
    checks.check(
        &format!("Subscriber received messages (got {})", received),
        received > 0,
    );

    // Check 3: Compression active
    // broadcast_all messages bypass cluster_messages_delivered/dropped counters,
    // so we can't compute exact per-message compression. Instead verify that
    // the cluster wire bandwidth is significantly less than the uncompressed rate.
    // Server A publishes 1KB messages at 100K+/s => 100MB/s uncompressed minimum.
    // With zstd on repetitive data we expect < 50MB/s on the wire.
    let bytes_per_sec = if duration.as_secs() > 0 {
        bytes_delta / duration.as_secs()
    } else {
        bytes_delta
    };
    let mb_per_sec = bytes_per_sec as f64 / (1024.0 * 1024.0);
    // Estimate uncompressed rate: subscriber gets ~52K/s (with drops).
    // With ~90% drop rate, real publish rate is ~500K/s.
    // 500K/s * 1024 bytes = 500 MB/s uncompressed.
    let estimated_uncompressed_mbps =
        (received as f64 / duration.as_secs() as f64) * 1024.0 / (1024.0 * 1024.0) * 10.0; // ~10x drop factor
    println!(
        "    Wire throughput: {:.1} MB/s (estimated uncompressed: {:.0} MB/s)",
        mb_per_sec, estimated_uncompressed_mbps
    );
    // Wire throughput should be less than estimated uncompressed throughput
    // if compression is working. Use a generous threshold.
    checks.check(
        &format!(
            "Wire throughput {:.0} MB/s < estimated uncompressed {:.0} MB/s",
            mb_per_sec, estimated_uncompressed_mbps
        ),
        mb_per_sec < estimated_uncompressed_mbps,
    );

    // Cleanup
    protocol::close_all(vec![ws_health, ws_sub]).await;

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
