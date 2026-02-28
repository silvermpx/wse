use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const PRESENCE_CLIENTS: usize = 5;
const PRESENCE_TOPIC: &str = "presence_room";

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

/// Read messages from a WsStream until deadline, looking for messages with
/// a specific "t" type. Returns the count found. Handles the battle server
/// flooding broadcast messages by using a hard deadline instead of per-message timeout.
async fn drain_for_type(ws: &mut WsStream, msg_type: &str, deadline_ms: u64) -> u32 {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    let mut count = 0u32;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some(msg_type) {
                        count += 1;
                    }
                }
            }
            _ => break,
        }
    }
    count
}

/// Drain messages looking for a specific type and return the parsed JSON payload.
async fn drain_for_type_with_data(
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

/// Battle Test: Presence Tracking
///
/// Tests presence join/leave events, presence query, data updates, and
/// clean disconnect handling. Connects 5 clients with different user IDs,
/// subscribes 3 with presence data, verifies join/leave/update events.
///
/// Requires: python benchmarks/bench_battle_server.py (with presence_enabled)
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Presence Tracking",
        "Tests presence join/leave events, multi-user tracking, presence query, data updates.",
    );

    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect clients with DIFFERENT user IDs
    // =========================================================================
    println!(
        "\n  Phase 1: Connect {} clients (different users)",
        PRESENCE_CLIENTS
    );

    let mut clients: Vec<WsStream> = Vec::new();
    for i in 0..PRESENCE_CLIENTS {
        let user_id = format!("presence-user-{}", i);
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
                println!("    [FAIL] Failed to connect client {}: {}", i, e);
                checks.check("All clients connected", false);
                protocol::close_all(clients).await;
                return Vec::new();
            }
        }
    }
    checks.check(
        &format!("Connected {}/{} clients", clients.len(), PRESENCE_CLIENTS),
        clients.len() == PRESENCE_CLIENTS,
    );

    // =========================================================================
    // Phase 2: Subscribe first 3 clients with presence data
    // =========================================================================
    println!(
        "\n  Phase 2: Subscribe 3 clients to '{}' with presence",
        PRESENCE_TOPIC
    );

    for (i, client) in clients.iter_mut().enumerate().take(3) {
        let subscribe_msg = serde_json::json!({
            "t": "battle_cmd",
            "p": {
                "action": "subscribe_presence",
                "topics": [PRESENCE_TOPIC],
                "presence_data": {"status": "online", "name": format!("User {}", i)}
            }
        });
        client
            .send(Message::Text(subscribe_msg.to_string().into()))
            .await
            .ok();
    }

    // Wait for server to process subscriptions and broadcast presence events
    tokio::time::sleep(Duration::from_millis(800)).await;

    // =========================================================================
    // Phase 3: Verify presence_join events
    // =========================================================================
    println!("\n  Phase 3: Verify presence_join events");

    // Use hard deadline (3s) - battle server may be flooding, so per-message
    // timeout won't work. Drain all messages within deadline looking for joins.
    let mut join_counts = [0u32; 3];
    for (i, client) in clients.iter_mut().enumerate().take(3) {
        join_counts[i] = drain_for_type(client, "presence_join", 3000).await;
    }

    // Client 0 subscribed first, should see joins from client 1 and 2 (+ possibly self)
    checks.check(
        &format!("Client 0 received join events (got {})", join_counts[0]),
        join_counts[0] >= 2,
    );
    // Client 2 subscribed last, should see at least its own join
    checks.check(
        &format!("Client 2 received join events (got {})", join_counts[2]),
        join_counts[2] >= 1,
    );

    // =========================================================================
    // Phase 4: Query presence via battle_cmd
    // =========================================================================
    println!("\n  Phase 4: Query presence members");

    let query_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "presence_query",
            "topic": PRESENCE_TOPIC
        }
    });
    clients[0]
        .send(Message::Text(query_msg.to_string().into()))
        .await
        .ok();

    let mut query_members = 0usize;
    if let Some(parsed) = drain_for_type_with_data(&mut clients[0], "presence_result", 3000).await
    {
        if let Some(members) = parsed
            .get("p")
            .and_then(|p| p.get("members"))
            .and_then(|m| m.as_object())
        {
            query_members = members.len();
        }
    }
    checks.check(
        &format!("Presence query shows 3 members (got {})", query_members),
        query_members == 3,
    );

    // =========================================================================
    // Phase 5: Disconnect client 2, verify presence_leave
    // =========================================================================
    println!("\n  Phase 5: Disconnect client 2, verify presence_leave");

    let _ = clients[2].close(None).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let leave_count_0 = drain_for_type(&mut clients[0], "presence_leave", 3000).await;
    checks.check(
        &format!("Client 0 received presence_leave (got {})", leave_count_0),
        leave_count_0 >= 1,
    );

    // =========================================================================
    // Phase 6: Update presence data for client 1
    // =========================================================================
    println!("\n  Phase 6: Update presence data for client 1");

    let update_msg = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "update_presence",
            "data": {"status": "away", "name": "User 1"}
        }
    });
    clients[1]
        .send(Message::Text(update_msg.to_string().into()))
        .await
        .ok();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let update_count_0 = drain_for_type(&mut clients[0], "presence_update", 3000).await;
    checks.check(
        &format!(
            "Client 0 received presence_update (got {})",
            update_count_0
        ),
        update_count_0 >= 1,
    );

    // =========================================================================
    // Phase 7: Clean up
    // =========================================================================
    println!("\n  Phase 7: Clean up");

    for client in &mut clients {
        let _ = client.close(None).await;
    }
    println!("    All clients disconnected");

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
