use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const RECOVERY_TOPIC: &str = "recovery_test";

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

/// Drain messages until we find a specific type or deadline expires.
/// Returns the parsed message if found.
async fn drain_for_type(
    ws: &mut WsStream,
    target_type: &str,
    deadline_ms: u64,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some(target_type) {
                        return Some(parsed);
                    }
                }
            }
            _ => break,
        }
    }
    None
}

/// Count messages of a given type within a deadline. Returns (count, last_seq).
async fn count_messages_with_seq(
    ws: &mut WsStream,
    target_type: &str,
    deadline_ms: u64,
) -> (u64, Option<u64>) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    let mut count = 0u64;
    let mut last_seq: Option<u64> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some(target_type) {
                        count += 1;
                        if let Some(seq) = parsed
                            .get("p")
                            .and_then(|p| p.get("seq"))
                            .and_then(|s| s.as_u64())
                        {
                            last_seq = Some(seq);
                        }
                    }
                }
            }
            _ => break,
        }
    }
    (count, last_seq)
}

/// Battle Test: Message Recovery
///
/// Tests subscribe_with_recovery, epoch+offset tracking, missed message replay.
/// Uses publish_messages command to populate recovery buffer on demand.
///
/// Requires: python benchmarks/bench_battle_server.py --recovery --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Message Recovery",
        "Tests subscribe_with_recovery, epoch+offset tracking, missed message replay.",
    );

    let mut checks = CheckResult::new();

    // =========================================================================
    // Step 1: Connect and seed the recovery buffer
    // =========================================================================
    println!("\n  Step 1: Connect and seed recovery buffer");

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "recovery-user");
    let mut ws = match protocol::connect_and_handshake(
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
            println!("    [FAIL] Failed to connect: {}", e);
            return Vec::new();
        }
    };
    checks.check("Connected to server", true);

    // Ask server to publish 50 messages to seed recovery buffer
    // (buffer_size=256, so keep total under 256 to avoid eviction)
    let seed_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": RECOVERY_TOPIC,
            "count": 50
        }
    });
    ws.send(Message::Text(seed_msg.to_string().into()))
        .await
        .ok();

    // Wait for publish_ack
    let ack = drain_for_type(&mut ws, "publish_ack", 5000).await;
    let seeded = ack
        .as_ref()
        .and_then(|a| a.get("p"))
        .and_then(|p| p.get("count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0);
    checks.check(&format!("Seeded recovery buffer ({} messages)", seeded), seeded > 0);

    if seeded == 0 {
        println!("    ABORT: Could not seed recovery buffer");
        let _ = ws.close(None).await;
        return Vec::new();
    }

    // =========================================================================
    // Step 2: Subscribe with recovery (initial, get epoch/offset)
    // =========================================================================
    println!("\n  Step 2: Subscribe with recovery (initial)");

    let subscribe_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "subscribe_recovery",
            "topics": [RECOVERY_TOPIC],
            "recover": false
        }
    });
    ws.send(Message::Text(subscribe_msg.to_string().into()))
        .await
        .ok();

    let sub_update = drain_for_type(&mut ws, "subscription_update", 5000).await;
    let (epoch, offset) = if let Some(ref parsed) = sub_update {
        let recovery = parsed
            .get("p")
            .and_then(|p| p.get("recovery"))
            .and_then(|r| r.get(RECOVERY_TOPIC));
        let ep = recovery
            .and_then(|r| r.get("epoch"))
            .and_then(|e| e.as_str())
            .map(String::from);
        let off = recovery
            .and_then(|r| r.get("offset"))
            .and_then(|o| o.as_u64());
        (ep, off)
    } else {
        (None, None)
    };

    let has_recovery_info = epoch.is_some() && offset.is_some();
    checks.check(
        &format!(
            "Got recovery info (epoch={:?}, offset={:?})",
            epoch, offset
        ),
        has_recovery_info,
    );

    if !has_recovery_info {
        println!("    ABORT: No recovery info from server. Is --recovery enabled?");
        let _ = ws.close(None).await;
        return Vec::new();
    }

    // =========================================================================
    // Step 3: Publish more messages while subscribed (we receive them)
    // =========================================================================
    println!("\n  Step 3: Publish 20 more messages (we receive these)");

    let more_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": RECOVERY_TOPIC,
            "count": 20
        }
    });
    ws.send(Message::Text(more_msg.to_string().into()))
        .await
        .ok();

    // Receive the battle messages + publish_ack
    let (received_before, _last_seq) = count_messages_with_seq(&mut ws, "battle", 3000).await;
    checks.check(
        &format!("Received {} messages before disconnect", received_before),
        received_before > 0,
    );

    // Re-query current recovery position after receiving messages
    let pos_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "subscribe_recovery",
            "topics": [RECOVERY_TOPIC],
            "recover": false
        }
    });
    ws.send(Message::Text(pos_msg.to_string().into()))
        .await
        .ok();
    let pos_update = drain_for_type(&mut ws, "subscription_update", 3000).await;
    let last_offset = pos_update
        .as_ref()
        .and_then(|p| p.get("p"))
        .and_then(|p| p.get("recovery"))
        .and_then(|r| r.get(RECOVERY_TOPIC))
        .and_then(|r| r.get("offset"))
        .and_then(|o| o.as_u64())
        .unwrap_or(offset.unwrap_or(0));
    println!("    Last known offset before disconnect: {}", last_offset);

    // =========================================================================
    // Step 4: Disconnect and publish messages we'll miss
    // =========================================================================
    println!("\n  Step 4: Disconnect");
    let _ = ws.close(None).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect a second helper connection to trigger publish while first is disconnected
    println!("    Publishing 100 messages while disconnected...");
    let mut helper = match protocol::connect_and_handshake(
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
            println!("    [FAIL] Failed to connect helper: {}", e);
            return Vec::new();
        }
    };

    let missed_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": RECOVERY_TOPIC,
            "count": 100
        }
    });
    helper
        .send(Message::Text(missed_msg.to_string().into()))
        .await
        .ok();

    // Wait for ack
    let missed_ack = drain_for_type(&mut helper, "publish_ack", 5000).await;
    let missed_count = missed_ack
        .as_ref()
        .and_then(|a| a.get("p"))
        .and_then(|p| p.get("count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0);
    checks.check(
        &format!("Published {} messages while disconnected", missed_count),
        missed_count > 0,
    );
    let _ = helper.close(None).await;

    // =========================================================================
    // Step 5: Reconnect with recovery
    // =========================================================================
    println!("\n  Step 5: Reconnect and subscribe with recover=true");

    let mut ws2 = match protocol::connect_and_handshake(
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
            println!("    [FAIL] Failed to reconnect: {}", e);
            return Vec::new();
        }
    };
    checks.check("Reconnected to server", true);

    // Subscribe with recovery (recover=true, with epoch + last_offset from step 3)
    let recover_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "subscribe_recovery",
            "topics": [RECOVERY_TOPIC],
            "recover": true,
            "recovery": {
                RECOVERY_TOPIC: {
                    "epoch": epoch.as_deref().unwrap_or(""),
                    "offset": last_offset
                }
            }
        }
    });
    ws2.send(Message::Text(recover_msg.to_string().into()))
        .await
        .ok();

    // =========================================================================
    // Step 6: Verify recovery response
    // =========================================================================
    println!("\n  Step 6: Verify recovery response");

    let recovery_response = drain_for_type(&mut ws2, "subscription_update", 5000).await;

    if let Some(ref parsed) = recovery_response {
        let p = parsed.get("p");
        let recovered = p
            .and_then(|p| p.get("recovered"))
            .and_then(|r| r.as_bool())
            .unwrap_or(false);
        let recovered_count = p
            .and_then(|p| p.get("recovery"))
            .and_then(|r| r.get(RECOVERY_TOPIC))
            .and_then(|r| r.get("count"))
            .and_then(|c| c.as_u64())
            .unwrap_or(0);

        checks.check(
            &format!("Recovery flag: {}", recovered),
            recovered,
        );
        checks.check(
            &format!("Recovered {} missed messages", recovered_count),
            recovered_count > 0,
        );
    } else {
        checks.check("Got recovery subscription_update", false);
        checks.check("Recovery count > 0", false);
    }

    // =========================================================================
    // Step 7: Verify health shows recovery stats
    // =========================================================================
    println!("\n  Step 7: Verify recovery health stats");

    if let Some(health) = protocol::query_health(&mut ws2, 3).await {
        let recovery_topics = health
            .get("recovery_topic_count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let recovery_bytes = health
            .get("recovery_total_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        checks.check(
            &format!("Recovery topics in health: {}", recovery_topics),
            recovery_topics > 0,
        );
        checks.check(
            &format!("Recovery bytes in health: {}", recovery_bytes),
            recovery_bytes > 0,
        );
    } else {
        checks.check("Health query returned data", false);
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
    let _ = ws2.close(None).await;
    println!("    Client disconnected");

    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
