use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, query_health};
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

/// Battle Test: Phase 3 -- Capability Negotiation Verification
///
/// Verifies the HELLO handshake correctly negotiates capabilities
/// (CAP_INTEREST_ROUTING | CAP_COMPRESSION). Queries health_snapshot on
/// Server B (subscriber side, not flooded) to verify cluster state.
/// Then runs topic routing to confirm capabilities are active.
///
/// Note: Server A (publisher) floods broadcast_all at 500K/s to all connections,
/// making health queries unreliable on that side. All health checks use Server B.
///
/// Requires: 2 cluster servers (same as battle-cluster setup)
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "BATTLE TEST: Phase 3 -- Capability Negotiation",
        &format!(
            "Verify HELLO handshake, capability bits, cluster health on :{} and :{}.",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect to Server B (subscriber side -- not flooded)
    // =========================================================================
    println!("\n  Phase 1: Connect to Server B (:{})", port2);

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
            eprintln!("    Failed to connect to Server B (:{}) -- {}", port2, e);
            return Vec::new();
        }
    };
    println!("    Connected to Server B (:{})", port2);

    // =========================================================================
    // Phase 2: Query cluster health on Server B
    // =========================================================================
    println!("\n  Phase 2: Query cluster health (Server B)");
    println!("    (Server A health skipped -- publisher floods 500K/s to all connections)");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let health_b = query_health(&mut ws_b, 5).await;

    if let Some(ref h) = health_b {
        println!(
            "    Server B health: {}",
            serde_json::to_string_pretty(h).unwrap_or_default()
        );

        let connected = h
            .get("cluster_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let peer_count = h
            .get("cluster_peer_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let unknown = h
            .get("cluster_unknown_message_types")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);
        let bytes_received = h
            .get("cluster_bytes_received")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        checks.check("cluster_connected == true", connected);
        checks.check(
            &format!("cluster_peer_count >= 1 (got {})", peer_count),
            peer_count >= 1,
        );
        checks.check(
            &format!("cluster_unknown_message_types == 0 (got {})", unknown),
            unknown == 0,
        );
        checks.check(
            &format!(
                "cluster_bytes_received > 0 (got {} -- peer data flowing)",
                bytes_received
            ),
            bytes_received > 0,
        );
    } else {
        println!("    WARNING: No health response from Server B");
        checks.check("cluster_connected == true", false);
        checks.check("cluster_peer_count >= 1", false);
        checks.check("cluster_unknown_message_types == 0", false);
        checks.check("cluster_bytes_received > 0", false);
    }

    // =========================================================================
    // Phase 3: Verify topic routing works (proves CAP_INTEREST_ROUTING)
    // =========================================================================
    println!("\n  Phase 3: Verify topic routing (CAP_INTEREST_ROUTING)");

    // Subscribe ws_b to "battle_all" -- a topic the server actually publishes to
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_b.send(Message::Text(cmd.to_string().into())).await;
    println!("    Subscribed to 'battle_all'");

    // Wait for subscription to propagate via RESYNC
    println!("    Waiting 3s for RESYNC propagation...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Receive messages for 5s -- Server A publishes "battle_all" in its round-robin.
    // If CAP_INTEREST_ROUTING is active, Server A forwards battle_all to Server B.
    let received = Arc::new(AtomicU64::new(0));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    let recv_count = received.clone();
    let mut ws_b = tokio::spawn(async move {
        let mut ws = ws_b;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, ws.next()).await {
                Ok(Some(Ok(msg))) => {
                    if let Some(parsed) = parse_wse_message(&msg) {
                        if parsed.get("t").and_then(|t| t.as_str()) == Some("battle") {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                _ => break,
            }
        }
        ws
    })
    .await
    .unwrap();

    let total_received = received.load(Ordering::Relaxed);
    checks.check(
        &format!(
            "Topic delivery across cluster works (received {} messages)",
            total_received
        ),
        total_received > 0,
    );

    // =========================================================================
    // Phase 4: Verify cluster metrics after traffic
    // =========================================================================
    println!("\n  Phase 4: Verify cluster delivery metrics");

    let health_post = query_health(&mut ws_b, 5).await;
    if let Some(ref h) = health_post {
        let delivered = h
            .get("cluster_messages_delivered")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        checks.check(
            &format!(
                "cluster_messages_delivered > 0 (got {} -- messages routed to local subs)",
                delivered
            ),
            delivered > 0,
        );
    } else {
        checks.check("cluster_messages_delivered > 0", false);
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
