use crate::config::Cli;
use crate::protocol::{self, parse_wse_message};
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

/// Battle Test: Message Recovery
///
/// Tests subscribe_with_recovery: connect, subscribe, disconnect, wait for
/// messages to accumulate in the ring buffer, reconnect with recover=true
/// and epoch+offset, verify missed messages are replayed.
///
/// Requires: python benchmarks/bench_battle_server.py --recovery
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Message Recovery",
        "Tests subscribe_with_recovery, epoch+offset tracking, missed message replay.",
    );

    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect and subscribe with recovery (initial)
    // =========================================================================
    println!("\n  Phase 1: Connect and subscribe with recovery");

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

    // Subscribe with recovery (initial, no recover)
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

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Read subscription response to get epoch/offset
    let mut epoch: Option<String> = None;
    let mut offset: Option<u64> = None;

    while let Ok(Some(Ok(msg))) =
        tokio::time::timeout(Duration::from_millis(500), ws.next()).await
    {
        if let Some(parsed) = parse_wse_message(&msg) {
            let msg_type = parsed.get("t").and_then(|t| t.as_str());
            if msg_type == Some("subscription_update") {
                if let Some(recovery) = parsed
                    .get("p")
                    .and_then(|p| p.get("recovery"))
                    .and_then(|r| r.get(RECOVERY_TOPIC))
                {
                    epoch = recovery
                        .get("epoch")
                        .and_then(|e| e.as_str())
                        .map(String::from);
                    offset = recovery.get("offset").and_then(|o| o.as_u64());
                }
            }
        }
    }

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
    // Phase 2: Receive some messages to advance offset
    // =========================================================================
    println!("\n  Phase 2: Receive messages for 3 seconds");

    let mut received_before = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut last_offset = offset;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some("battle") {
                        received_before += 1;
                        // Track sequence for offset
                        if let Some(seq) = parsed
                            .get("p")
                            .and_then(|p| p.get("seq"))
                            .and_then(|s| s.as_u64())
                        {
                            last_offset = Some(seq);
                        }
                    }
                }
            }
            _ => break,
        }
    }

    checks.check(
        &format!("Received {} messages before disconnect", received_before),
        received_before > 0,
    );

    // =========================================================================
    // Phase 3: Disconnect (messages accumulate in server buffer)
    // =========================================================================
    println!("\n  Phase 3: Disconnect and wait 3 seconds (messages accumulate)");

    let _ = ws.close(None).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // =========================================================================
    // Phase 4: Reconnect with recovery
    // =========================================================================
    println!("\n  Phase 4: Reconnect and subscribe with recover=true");

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

    // Subscribe with recovery (recover=true, with epoch+offset)
    let recover_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "subscribe_recovery",
            "topics": [RECOVERY_TOPIC],
            "recover": true,
            "recovery": {
                RECOVERY_TOPIC: {
                    "epoch": epoch.as_deref().unwrap_or(""),
                    "offset": last_offset.unwrap_or(0)
                }
            }
        }
    });
    ws2.send(Message::Text(recover_msg.to_string().into()))
        .await
        .ok();

    tokio::time::sleep(Duration::from_millis(500)).await;

    // =========================================================================
    // Phase 5: Verify recovery response
    // =========================================================================
    println!("\n  Phase 5: Verify recovery response and replayed messages");

    let mut recovered = false;
    let mut recovered_count = 0u64;
    let mut new_messages = 0u64;

    let read_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = read_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws2.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    let msg_type = parsed.get("t").and_then(|t| t.as_str());
                    match msg_type {
                        Some("subscription_update") => {
                            if let Some(p) = parsed.get("p") {
                                recovered = p
                                    .get("recovered")
                                    .and_then(|r| r.as_bool())
                                    .unwrap_or(false);
                                if let Some(recovery_info) = p
                                    .get("recovery")
                                    .and_then(|r| r.get(RECOVERY_TOPIC))
                                {
                                    recovered_count = recovery_info
                                        .get("count")
                                        .and_then(|c| c.as_u64())
                                        .unwrap_or(0);
                                }
                            }
                        }
                        Some("battle") => {
                            new_messages += 1;
                            // Stop after receiving some new messages
                            if new_messages >= 10 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => break,
        }
    }

    checks.check(
        &format!("Recovery flag in response: {}", recovered),
        recovered,
    );
    checks.check(
        &format!("Recovered {} missed messages", recovered_count),
        recovered_count > 0,
    );
    checks.check(
        &format!("Receiving new messages after recovery (got {})", new_messages),
        new_messages > 0,
    );

    // =========================================================================
    // Phase 6: Verify health shows recovery stats
    // =========================================================================
    println!("\n  Phase 6: Verify recovery health stats");

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
    // Phase 7: Clean up
    // =========================================================================
    println!("\n  Phase 7: Clean up");
    let _ = ws2.close(None).await;
    println!("    Client disconnected");

    // =========================================================================
    // Summary
    // =========================================================================
    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
