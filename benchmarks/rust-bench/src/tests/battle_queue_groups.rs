use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const WORKER_COUNT: usize = 5;
const FANOUT_COUNT: usize = 2;
const PUBLISH_COUNT: u64 = 500;
const QUEUE_GROUP_TOPIC: &str = "battle_all";

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

/// Drain messages, counting those with t="battle". Returns count.
async fn drain_battle_count(ws: &mut WsStream, deadline_ms: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    let mut count = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(parsed) = parse_wse_message(&msg) {
                    if parsed.get("t").and_then(|t| t.as_str()) == Some("battle") {
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

/// Battle Test: Queue Groups (Round-Robin Dispatch)
///
/// Verifies that queue group members receive round-robin dispatched messages
/// while normal fanout subscribers receive all messages.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Queue Groups",
        "Tests round-robin dispatch to queue group members vs normal fanout subscribers.",
    );

    let mut checks = CheckResult::new();
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");

    // =========================================================================
    // Phase 1: Connect worker clients (queue group "workers")
    // =========================================================================
    println!("\n  Phase 1: Connect {} queue group workers", WORKER_COUNT);

    let mut workers: Vec<WsStream> = Vec::new();
    for i in 0..WORKER_COUNT {
        let user_id = format!("qg-worker-{}", i);
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
            Ok(ws) => workers.push(ws),
            Err(e) => {
                println!("    [FAIL] Failed to connect worker {}: {}", i, e);
                checks.check("All workers connected", false);
                protocol::close_all(workers).await;
                return Vec::new();
            }
        }
    }

    // Subscribe each worker to the topic with queue_group="workers"
    for worker in &mut workers {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {
                "action": "subscribe_queue_group",
                "topics": [QUEUE_GROUP_TOPIC],
                "queue_group": "workers"
            }
        });
        worker
            .send(Message::Text(cmd.to_string().into()))
            .await
            .ok();
    }

    // Wait for queue_group_subscribed acks
    for worker in &mut workers {
        let _ = drain_for_type(worker, "queue_group_subscribed", 3000).await;
    }
    checks.check(
        &format!("Connected {}/{} workers", workers.len(), WORKER_COUNT),
        workers.len() == WORKER_COUNT,
    );

    // =========================================================================
    // Phase 2: Connect fanout clients (normal subscribers)
    // =========================================================================
    println!("\n  Phase 2: Connect {} fanout subscribers", FANOUT_COUNT);

    let mut fanout_clients: Vec<WsStream> = Vec::new();
    for i in 0..FANOUT_COUNT {
        let user_id = format!("qg-fanout-{}", i);
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
            Ok(ws) => fanout_clients.push(ws),
            Err(e) => {
                println!("    [FAIL] Failed to connect fanout {}: {}", i, e);
                checks.check("All fanout clients connected", false);
                protocol::close_all(workers).await;
                protocol::close_all(fanout_clients).await;
                return Vec::new();
            }
        }
    }

    // Subscribe fanout clients normally (no queue group)
    for client in &mut fanout_clients {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {
                "action": "subscribe",
                "topics": [QUEUE_GROUP_TOPIC]
            }
        });
        client
            .send(Message::Text(cmd.to_string().into()))
            .await
            .ok();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    checks.check(
        &format!(
            "Connected {}/{} fanout clients",
            fanout_clients.len(),
            FANOUT_COUNT
        ),
        fanout_clients.len() == FANOUT_COUNT,
    );

    // =========================================================================
    // Phase 3: Publish messages via a separate connection
    // =========================================================================
    println!("\n  Phase 3: Publish {} messages", PUBLISH_COUNT);

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
            protocol::close_all(workers).await;
            protocol::close_all(fanout_clients).await;
            return Vec::new();
        }
    };

    let pub_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": QUEUE_GROUP_TOPIC,
            "count": PUBLISH_COUNT
        }
    });
    publisher
        .send(Message::Text(pub_cmd.to_string().into()))
        .await
        .ok();

    let ack = drain_for_type(&mut publisher, "publish_ack", 5000).await;
    let published = ack
        .as_ref()
        .and_then(|a| a.get("p"))
        .and_then(|p| p.get("count"))
        .and_then(|c| c.as_u64())
        .unwrap_or(0);
    checks.check(
        &format!("Published {} messages", published),
        published == PUBLISH_COUNT,
    );

    // =========================================================================
    // Phase 4: Collect messages from all clients
    // =========================================================================
    println!("\n  Phase 4: Collect messages (5s deadline)");

    // Collect worker counts in parallel using spawned tasks
    let worker_counts = Arc::new(
        (0..WORKER_COUNT)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );

    let mut worker_handles = Vec::new();
    for (i, mut ws) in workers.into_iter().enumerate() {
        let counts = worker_counts.clone();
        worker_handles.push(tokio::spawn(async move {
            let c = drain_battle_count(&mut ws, 5000).await;
            counts[i].store(c, Ordering::Relaxed);
            ws
        }));
    }

    let fanout_counts = Arc::new(
        (0..FANOUT_COUNT)
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>(),
    );
    let mut fanout_handles = Vec::new();
    for (i, mut ws) in fanout_clients.into_iter().enumerate() {
        let counts = fanout_counts.clone();
        fanout_handles.push(tokio::spawn(async move {
            let c = drain_battle_count(&mut ws, 5000).await;
            counts[i].store(c, Ordering::Relaxed);
            ws
        }));
    }

    // Await all
    let mut workers_back = Vec::new();
    for h in worker_handles {
        workers_back.push(h.await.unwrap());
    }
    let mut fanout_back = Vec::new();
    for h in fanout_handles {
        fanout_back.push(h.await.unwrap());
    }

    // =========================================================================
    // Phase 5: Verify results
    // =========================================================================
    println!("\n  Phase 5: Verify distribution");

    let worker_msg_counts: Vec<u64> = (0..WORKER_COUNT)
        .map(|i| worker_counts[i].load(Ordering::Relaxed))
        .collect();
    let fanout_msg_counts: Vec<u64> = (0..FANOUT_COUNT)
        .map(|i| fanout_counts[i].load(Ordering::Relaxed))
        .collect();

    let worker_total: u64 = worker_msg_counts.iter().sum();
    let _fanout_total: u64 = fanout_msg_counts.iter().sum();

    println!("    Worker counts: {:?}", worker_msg_counts);
    println!("    Fanout counts: {:?}", fanout_msg_counts);
    println!("    Worker aggregate: {}", worker_total);

    // Check that each fanout client received all messages
    for (i, &count) in fanout_msg_counts.iter().enumerate() {
        checks.check(
            &format!(
                "Fanout client {} received all messages ({}/{})",
                i, count, published
            ),
            count == published,
        );
    }

    // Check that worker aggregate equals what one fanout client received
    // (each message goes to exactly one worker)
    let expected_total = if !fanout_msg_counts.is_empty() {
        fanout_msg_counts[0]
    } else {
        published
    };
    checks.check(
        &format!(
            "Worker aggregate matches fanout total ({} vs {})",
            worker_total, expected_total
        ),
        worker_total == expected_total,
    );

    // Check round-robin distribution: each worker should get ~1/N of messages
    if worker_total > 0 {
        let expected_per_worker = worker_total as f64 / WORKER_COUNT as f64;
        let mut distribution_ok = true;
        for &count in &worker_msg_counts {
            let ratio = count as f64 / expected_per_worker;
            // Allow 50% deviation (0.5x to 1.5x) for round-robin
            if !(0.5..=1.5).contains(&ratio) {
                distribution_ok = false;
            }
        }
        checks.check(
            "Round-robin distribution within 50% tolerance",
            distribution_ok,
        );
    } else {
        checks.check("Workers received messages", false);
    }

    // =========================================================================
    // Phase 6: Query queue group info
    // =========================================================================
    println!("\n  Phase 6: Query queue group info");

    let info_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "queue_group_info",
            "topic": QUEUE_GROUP_TOPIC
        }
    });
    workers_back[0]
        .send(Message::Text(info_cmd.to_string().into()))
        .await
        .ok();

    let info_response = drain_for_type(&mut workers_back[0], "queue_group_info_result", 3000).await;
    if let Some(ref parsed) = info_response {
        let groups = parsed
            .get("p")
            .and_then(|p| p.get("groups"))
            .and_then(|g| g.as_object());
        if let Some(groups) = groups {
            let worker_group_size = groups.get("workers").and_then(|v| v.as_u64()).unwrap_or(0);
            checks.check(
                &format!(
                    "Queue group 'workers' has {} members (expected {})",
                    worker_group_size, WORKER_COUNT
                ),
                worker_group_size == WORKER_COUNT as u64,
            );
        } else {
            checks.check("Queue group info returned groups", false);
        }
    } else {
        checks.check("Queue group info response received", false);
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
    let _ = publisher.close(None).await;
    protocol::close_all(workers_back).await;
    protocol::close_all(fanout_back).await;
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
