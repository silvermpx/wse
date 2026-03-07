use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, query_health, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
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

/// Drain messages looking for a specific type.
async fn drain_for_type(
    ws: &mut WsStream,
    msg_type: &str,
    deadline_ms: u64,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some(msg_type) {
                        return Some(parsed);
                    }
                }
            }
            _ => return None,
        }
    }
}

/// Battle Test: Cluster Topology API
///
/// Verifies cluster_info() returns peer list with address and instance_id
/// for connected cluster peers.
///
/// Requires: 2 cluster servers (same as battle-cluster setup)
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "BATTLE TEST: Cluster Topology API",
        &format!(
            "Verify cluster_info() returns peer list on :{} and :{}.",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect to Server B (subscriber side)
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

    // Wait for cluster to settle
    tokio::time::sleep(Duration::from_secs(2)).await;

    // =========================================================================
    // Phase 2: Query health for basic cluster status
    // =========================================================================
    println!("\n  Phase 2: Query health snapshot");

    let health = query_health(&mut ws_b, 5).await;
    if let Some(ref h) = health {
        let connected = h
            .get("cluster_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let peer_count = h
            .get("cluster_peer_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        checks.check("cluster_connected == true", connected);
        checks.check(
            &format!("cluster_peer_count >= 1 (got {})", peer_count),
            peer_count >= 1,
        );
    } else {
        println!("    WARNING: No health response from Server B");
        checks.check("cluster_connected == true", false);
        checks.check("cluster_peer_count >= 1", false);
    }

    // =========================================================================
    // Phase 3: Query cluster_info for peer details
    // =========================================================================
    println!("\n  Phase 3: Query cluster_info");

    let info_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "cluster_info"}
    });
    ws_b.send(Message::Text(info_cmd.to_string().into()))
        .await
        .ok();

    let info_response = drain_for_type(&mut ws_b, "cluster_info_result", 5000).await;

    if let Some(ref parsed) = info_response {
        let peers = parsed
            .get("p")
            .and_then(|p| p.get("peers"))
            .and_then(|p| p.as_array());

        if let Some(peers) = peers {
            println!("    cluster_info returned {} peers", peers.len());

            checks.check(
                &format!("cluster_info has >= 1 peer (got {})", peers.len()),
                !peers.is_empty(),
            );

            if let Some(first_peer) = peers.first() {
                println!(
                    "    Peer details: {}",
                    serde_json::to_string_pretty(first_peer).unwrap_or_default()
                );

                let has_address = first_peer
                    .get("address")
                    .and_then(|v| v.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false);
                let has_instance_id = first_peer
                    .get("instance_id")
                    .and_then(|v| v.as_str())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false);

                checks.check("Peer has non-empty address", has_address);
                checks.check("Peer has non-empty instance_id", has_instance_id);

                // Check capabilities field exists
                let has_capabilities = first_peer.get("capabilities").is_some();
                checks.check("Peer has capabilities field", has_capabilities);

                // Check connected_at field exists
                let has_connected_at = first_peer.get("connected_at").is_some();
                checks.check("Peer has connected_at field", has_connected_at);
            }
        } else {
            checks.check("cluster_info returned peer array", false);
        }
    } else {
        println!("    WARNING: No cluster_info response");
        checks.check("cluster_info response received", false);
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
    protocol::close_all(vec![ws_b]).await;

    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
