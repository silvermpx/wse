use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const PUBLISH_COUNT: u64 = 200;

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

/// Drain messages, counting by channel. Returns (battle_all_count, battle_half_count).
async fn drain_battle_by_channel(ws: &mut WsStream, deadline_ms: u64) -> (u64, u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    let mut all_count = 0u64;
    let mut half_count = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some("battle") {
                        let ch = parsed
                            .get("p")
                            .and_then(|p| p.get("ch"))
                            .and_then(|c| c.as_str())
                            .unwrap_or("");
                        match ch {
                            "battle_all" => all_count += 1,
                            "battle_half" => half_count += 1,
                            _ => {}
                        }
                    }
                }
            }
            _ => break,
        }
    }
    (all_count, half_count)
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

/// Battle Test: Topic ACL (Access Control Lists)
///
/// Verifies that set_topic_acl correctly restricts topic subscriptions using
/// allow and deny glob patterns.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Topic ACL",
        "Tests topic access control with allow/deny glob patterns.",
    );

    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect 4 clients with different ACL rules
    // =========================================================================
    println!("\n  Phase 1: Connect 4 clients with different ACL rules");

    let mut clients: Vec<WsStream> = Vec::new();
    let labels = ["allow-exact", "allow-glob", "deny-half", "no-acl"];

    for (i, label) in labels.iter().enumerate() {
        let user_id = format!("acl-{}", label);
        let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), &user_id);
        match protocol::connect_and_handshake(
            &cli.host,
            cli.port,
            &token,
            "compression=false&protocol_version=1",
            10,
        )
        .await
        {
            Ok(ws) => clients.push(ws),
            Err(e) => {
                println!(
                    "    [FAIL] Failed to connect client {} ({}): {}",
                    i, label, e
                );
                checks.check("All clients connected", false);
                protocol::close_all(clients).await;
                return Vec::new();
            }
        }
    }
    checks.check(
        &format!("Connected {}/4 clients", clients.len()),
        clients.len() == 4,
    );

    // =========================================================================
    // Phase 2: Set ACL on first 3 clients
    // =========================================================================
    println!("\n  Phase 2: Set topic ACL rules");

    // Client A: allow=["battle_all"] (exact match only)
    let acl_a = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "allow": ["battle_all"]}
    });
    clients[0]
        .send(Message::Text(acl_a.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut clients[0], "acl_set", 3000).await;

    // Client B: allow=["battle_*"] (glob pattern)
    let acl_b = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "allow": ["battle_*"]}
    });
    clients[1]
        .send(Message::Text(acl_b.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut clients[1], "acl_set", 3000).await;

    // Client C: deny=["battle_half"]
    let acl_c = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "deny": ["battle_half"]}
    });
    clients[2]
        .send(Message::Text(acl_c.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut clients[2], "acl_set", 3000).await;

    // Client D: no ACL (unrestricted)
    println!("    ACL rules set on clients A, B, C (D = unrestricted)");

    // =========================================================================
    // Phase 3: Subscribe all clients to both topics
    // =========================================================================
    println!("\n  Phase 3: Subscribe all clients to battle_all + battle_half");

    for client in clients.iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {
                "action": "subscribe",
                "topics": ["battle_all", "battle_half"]
            }
        });
        client
            .send(Message::Text(cmd.to_string().into()))
            .await
            .ok();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // =========================================================================
    // Phase 4: Publish messages to both topics
    // =========================================================================
    println!(
        "\n  Phase 4: Publish {} messages to each topic",
        PUBLISH_COUNT
    );

    // Use a separate publisher connection
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "acl-publisher");
    let mut publisher = match protocol::connect_and_handshake(
        &cli.host,
        cli.port,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            println!("    [FAIL] Failed to connect publisher: {}", e);
            protocol::close_all(clients).await;
            return Vec::new();
        }
    };

    // Publish to battle_all
    let pub_all = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_all",
            "count": PUBLISH_COUNT
        }
    });
    publisher
        .send(Message::Text(pub_all.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut publisher, "publish_ack", 5000).await;

    // Publish to battle_half
    let pub_half = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_half",
            "count": PUBLISH_COUNT
        }
    });
    publisher
        .send(Message::Text(pub_half.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut publisher, "publish_ack", 5000).await;

    // =========================================================================
    // Phase 5: Collect messages from all clients
    // =========================================================================
    println!("\n  Phase 5: Collect messages (5s deadline)");

    let mut results: Vec<(u64, u64)> = Vec::new();
    for client in clients.iter_mut() {
        let (all_c, half_c) = drain_battle_by_channel(client, 5000).await;
        results.push((all_c, half_c));
    }

    for (i, (all_c, half_c)) in results.iter().enumerate() {
        println!(
            "    Client {} ({}): battle_all={}, battle_half={}",
            i, labels[i], all_c, half_c
        );
    }

    // =========================================================================
    // Phase 6: Verify ACL enforcement
    // =========================================================================
    println!("\n  Phase 6: Verify ACL enforcement");

    // Client A: allow=["battle_all"] -> receives battle_all only
    let (a_all, a_half) = results[0];
    checks.check(
        &format!("Client A (allow exact): battle_all={} > 0", a_all),
        a_all > 0,
    );
    checks.check(
        &format!("Client A (allow exact): battle_half={} == 0", a_half),
        a_half == 0,
    );

    // Client B: allow=["battle_*"] -> receives both (glob matches both)
    let (b_all, b_half) = results[1];
    checks.check(
        &format!("Client B (allow glob): battle_all={} > 0", b_all),
        b_all > 0,
    );
    checks.check(
        &format!("Client B (allow glob): battle_half={} > 0", b_half),
        b_half > 0,
    );

    // Client C: deny=["battle_half"] -> receives battle_all, NOT battle_half
    let (c_all, c_half) = results[2];
    checks.check(
        &format!("Client C (deny half): battle_all={} > 0", c_all),
        c_all > 0,
    );
    checks.check(
        &format!("Client C (deny half): battle_half={} == 0", c_half),
        c_half == 0,
    );

    // Client D: no ACL -> receives both
    let (d_all, d_half) = results[3];
    checks.check(
        &format!("Client D (no ACL): battle_all={} > 0", d_all),
        d_all > 0,
    );
    checks.check(
        &format!("Client D (no ACL): battle_half={} > 0", d_half),
        d_half > 0,
    );

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
    let _ = publisher.close(None).await;
    protocol::close_all(clients).await;
    println!("    All clients disconnected");

    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
