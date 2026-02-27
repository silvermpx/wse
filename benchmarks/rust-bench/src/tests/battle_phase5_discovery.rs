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

/// Battle Test: Phase 5 -- Gossip Peer Discovery
///
/// Tests 3-node gossip-based peer discovery:
///   Node A listens only (no --peers), has cluster_addr for gossip
///   Node B has --peers to A's cluster port, discovers A directly
///   Node C uses seeds=[B's cluster_addr] to discover the cluster via gossip
///
/// After gossip propagation:
///   - B gossips C's address to A (via B's outbound to A)
///   - A discovers C and creates outbound connection to C
///   - A can deliver messages to C (proves gossip discovery + mesh routing)
///
/// Note: Server A and B (publisher mode) flood broadcast_all at 500K/s to all
/// connections, making health queries unreliable. All health checks use Server C.
///
/// IMPORTANT: Start servers in this order:
///   1. Server A first (listener, no outbound peers)
///   2. Server B second (connects outbound to A's cluster port)
///   3. Server C last (connects to B's cluster port via seed)
///
/// Requires: 3 servers
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 \
///       --cluster-port 6006 --cluster-addr 127.0.0.1:6006
///   python benchmarks/bench_battle_server.py --mode cluster --port 5007 --peers 127.0.0.1:6006 \
///       --cluster-port 6007 --cluster-addr 127.0.0.1:6007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5008 \
///       --cluster-port 6008 --cluster-addr 127.0.0.1:6008 --seeds 127.0.0.1:6007
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);
    let port3 = cli.port3.unwrap_or(cli.port + 2);

    stats::print_header(
        "BATTLE TEST: Phase 5 -- Gossip Peer Discovery",
        &format!(
            "3-node gossip: A(:{}) <- B(:{}) direct, C(:{}) discovers via seeds.",
            cli.port, port2, port3
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect to Server C (subscriber side -- not flooded)
    // =========================================================================
    println!("\n  Phase 1: Connect to Server C (:{})", port3);
    println!("    (Server A/B health skipped -- publishers flood 500K/s to all connections)");

    let mut ws_c = match protocol::connect_and_handshake(
        &cli.host,
        port3,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            eprintln!("    Failed to connect to Server C (:{}) -- {}", port3, e);
            eprintln!(
                "\n  Start 3 servers (in this order):\n    \
                python benchmarks/bench_battle_server.py --mode cluster --port {} \
                --cluster-port 6006 --cluster-addr 127.0.0.1:6006\n    \
                python benchmarks/bench_battle_server.py --mode cluster --port {} --peers 127.0.0.1:6006 \
                --cluster-port 6007 --cluster-addr 127.0.0.1:6007\n    \
                python benchmarks/bench_battle_server.py --mode cluster-subscribe --port {} \
                --cluster-port 6008 --cluster-addr 127.0.0.1:6008 --seeds 127.0.0.1:6007",
                cli.port, port2, port3
            );
            return Vec::new();
        }
    };
    println!("    Connected to Server C (:{})", port3);

    // =========================================================================
    // Phase 2: Wait for gossip propagation and verify on Server C
    // =========================================================================
    println!("\n  Phase 2: Wait for gossip propagation (10s)");
    tokio::time::sleep(Duration::from_secs(10)).await;

    println!("    Querying health on Server C...");
    let health_c = query_health(&mut ws_c, 5).await;

    if let Some(ref h) = health_c {
        let peers = h
            .get("cluster_peer_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let connected = h
            .get("cluster_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let unknown = h
            .get("cluster_unknown_message_types")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);

        println!(
            "    Server C: peer_count={}, connected={}, unknown_types={}",
            peers, connected, unknown
        );

        checks.check(
            &format!(
                "Server C cluster_peer_count >= 2 (got {} -- discovered A via gossip)",
                peers
            ),
            peers >= 2,
        );
        checks.check("Server C cluster_connected == true", connected);
        checks.check(
            &format!("Server C cluster_unknown_message_types == 0 (got {})", unknown),
            unknown == 0,
        );
    } else {
        println!("    Server C: NO HEALTH RESPONSE");
        checks.check("Server C cluster_peer_count >= 2", false);
        checks.check("Server C cluster_connected == true", false);
        checks.check("Server C cluster_unknown_message_types == 0", false);
    }

    // =========================================================================
    // Phase 3: Verify message delivery A -> C (full mesh)
    // =========================================================================
    println!("\n  Phase 3: Verify A -> C message delivery via gossip mesh");

    // Subscribe ws_c to battle_all to receive messages from A's publish loop.
    // A publishes to battle_all in round-robin. With gossip discovery, A should
    // have discovered C and created an outbound connection. When C subscribes,
    // RESYNC propagates to A, and A starts forwarding battle_all to C.
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_c.send(Message::Text(cmd.to_string().into())).await;
    println!("    Subscribed to 'battle_all'");

    // Wait for RESYNC propagation through the gossip mesh
    println!("    Waiting 5s for RESYNC propagation...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Receive on ws_c for 5 seconds
    let received = Arc::new(AtomicU64::new(0));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    let recv_count = received.clone();
    let mut ws_c = tokio::spawn(async move {
        let mut ws = ws_c;
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
            "Topic delivery A -> C via gossip mesh (received {} messages)",
            total_received
        ),
        total_received > 0,
    );

    // =========================================================================
    // Phase 4: Verify delivery metrics on Server C
    // =========================================================================
    println!("\n  Phase 4: Verify delivery metrics on Server C");

    let health_post = query_health(&mut ws_c, 5).await;
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
    protocol::close_all(vec![ws_c]).await;

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
