use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const CLIENT_COUNT: usize = 10;

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

/// Wait for a WebSocket Close frame and return the close code.
async fn wait_for_close(ws: &mut WsStream, deadline_ms: u64) -> Option<u16> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Some(frame.map(|f| f.code.into()).unwrap_or(0));
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// Battle Test: Graceful Drain
///
/// Verifies that server.drain() sends Close frames with code 4300 to all
/// connected clients and rejects new connections after drain starts.
///
/// Uses --no-publish mode to avoid broadcast flood interfering with drain ack.
///
/// WARNING: This test is destructive -- it drains and shuts down the server.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Graceful Drain",
        "Tests drain() sends Close(4300) to all clients and rejects new connections.",
    );

    let mut checks = CheckResult::new();
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");

    // =========================================================================
    // Phase 1: Connect clients and subscribe
    // =========================================================================
    println!("\n  Phase 1: Connect {} clients", CLIENT_COUNT);

    let mut clients: Vec<WsStream> = Vec::new();
    for i in 0..CLIENT_COUNT {
        let user_id = format!("drain-user-{}", i);
        let t = crate::jwt::generate_bench_token(cli.secret.as_bytes(), &user_id);
        match protocol::connect_and_handshake(
            &cli.host,
            cli.port,
            &t,
            "compression=false&protocol_version=1",
            10,
        )
        .await
        {
            Ok(ws) => clients.push(ws),
            Err(e) => {
                println!("    [FAIL] Failed to connect client {}: {}", i, e);
            }
        }
    }
    checks.check(
        &format!("Connected {}/{} clients", clients.len(), CLIENT_COUNT),
        clients.len() == CLIENT_COUNT,
    );

    // Subscribe all to battle_all
    for client in clients.iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_all"]}
        });
        client
            .send(Message::Text(cmd.to_string().into()))
            .await
            .ok();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // =========================================================================
    // Phase 2: Verify server is handling messages
    // =========================================================================
    println!("\n  Phase 2: Verify server accepts messages");

    // Publish a few messages to confirm the server is alive
    let pub_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_all",
            "count": 10
        }
    });
    clients[0]
        .send(Message::Text(pub_cmd.to_string().into()))
        .await
        .ok();
    let ack = drain_for_type(&mut clients[0], "publish_ack", 5000).await;
    checks.check("Server accepting messages", ack.is_some());

    // Drain the published messages from all clients
    tokio::time::sleep(Duration::from_millis(300)).await;
    for client in clients.iter_mut() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(200);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, client.next()).await {
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
    }

    // =========================================================================
    // Phase 3: Trigger drain via battle_cmd
    // =========================================================================
    println!("\n  Phase 3: Trigger graceful drain (0.5s delay)");

    let drain_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "start_drain",
            "delay": 0.5
        }
    });
    clients[0]
        .send(Message::Text(drain_cmd.to_string().into()))
        .await
        .ok();

    let drain_ack = drain_for_type(&mut clients[0], "drain_started", 3000).await;
    checks.check("Drain started acknowledgment received", drain_ack.is_some());

    // =========================================================================
    // Phase 4: Wait for Close frames on all clients
    // =========================================================================
    println!("\n  Phase 4: Wait for Close frames (10s deadline)");

    let mut close_codes: Vec<Option<u16>> = Vec::new();
    let mut handles = Vec::new();
    for mut client in clients.into_iter() {
        handles.push(tokio::spawn(async move {
            let code = wait_for_close(&mut client, 10000).await;
            (code, client)
        }));
    }

    let mut clients_back = Vec::new();
    for h in handles {
        let (code, ws) = h.await.unwrap();
        close_codes.push(code);
        clients_back.push(ws);
    }

    let received_close = close_codes.iter().filter(|c| c.is_some()).count();
    let correct_code = close_codes.iter().filter(|c| **c == Some(4300)).count();

    println!(
        "    {}/{} received Close frame, {}/{} had code 4300",
        received_close, CLIENT_COUNT, correct_code, CLIENT_COUNT
    );

    checks.check(
        &format!(
            "All clients received Close frame ({}/{})",
            received_close, CLIENT_COUNT
        ),
        received_close == CLIENT_COUNT,
    );
    checks.check(
        &format!("Close code is 4300 ({}/{})", correct_code, CLIENT_COUNT),
        correct_code == CLIENT_COUNT,
    );

    // =========================================================================
    // Phase 5: Verify new connections are rejected
    // =========================================================================
    println!("\n  Phase 5: Verify new connections rejected after drain");

    tokio::time::sleep(Duration::from_secs(1)).await;

    let new_conn_result = protocol::connect_and_handshake(
        &cli.host,
        cli.port,
        &token,
        "compression=false&protocol_version=1",
        5,
    )
    .await;

    match new_conn_result {
        Ok(mut ws) => {
            let close_code = wait_for_close(&mut ws, 3000).await;
            if close_code.is_some() {
                checks.check("New connection rejected (got close after connect)", true);
            } else {
                checks.check("New connection rejected after drain", false);
            }
        }
        Err(_) => {
            checks.check("New connection rejected after drain", true);
        }
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
    protocol::close_all(clients_back).await;

    let total = checks.passed + checks.failed;
    println!("\n  Result: {}/{} checks passed", checks.passed, total);
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}
