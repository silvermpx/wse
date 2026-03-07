use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
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

/// Parse a Prometheus metric value from the metrics text.
fn parse_prometheus_value(metrics: &str, metric_name: &str) -> Option<f64> {
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(metric_name) {
            let parts: Vec<&str> = line.rsplitn(2, ' ').collect();
            if parts.len() == 2 {
                return parts[0].parse::<f64>().ok();
            }
        }
    }
    None
}

/// Parse a Prometheus metric with a specific label.
fn parse_prometheus_labeled(metrics: &str, metric_name: &str, label_value: &str) -> Option<f64> {
    let pattern = format!("{}{{topic=\"{}\"}}", metric_name, label_value);
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(&pattern) {
            let parts: Vec<&str> = line.rsplitn(2, ' ').collect();
            if parts.len() == 2 {
                return parts[0].parse::<f64>().ok();
            }
        }
    }
    None
}

/// Battle Test: Per-Topic Prometheus Metrics
///
/// Verifies that per-topic message counters appear in prometheus_metrics() output
/// along with standard gauges and counters.
///
/// Uses --no-publish mode with controlled publish_messages to avoid message flood.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Per-Topic Metrics",
        "Tests per-topic Prometheus counters and standard server metrics.",
    );

    let mut checks = CheckResult::new();

    // =========================================================================
    // Phase 1: Connect and subscribe
    // =========================================================================
    println!("\n  Phase 1: Connect 3 clients and subscribe");

    let mut clients: Vec<WsStream> = Vec::new();
    for i in 0..3 {
        let user_id = format!("metrics-user-{}", i);
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
        &format!("Connected {}/3 clients", clients.len()),
        clients.len() == 3,
    );

    // Subscribe all to battle_all and battle_half
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
    // Phase 2: Publish controlled traffic to both topics
    // =========================================================================
    println!("\n  Phase 2: Publish controlled traffic");

    // Publish 500 messages to battle_all
    let pub_all = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_all",
            "count": 500
        }
    });
    clients[0]
        .send(Message::Text(pub_all.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut clients[0], "publish_ack", 5000).await;

    // Publish 300 messages to battle_half
    let pub_half = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_half",
            "count": 300
        }
    });
    clients[0]
        .send(Message::Text(pub_half.to_string().into()))
        .await
        .ok();
    let _ = drain_for_type(&mut clients[0], "publish_ack", 5000).await;

    println!("    Published 500 to battle_all, 300 to battle_half");

    // Drain the received messages so they don't interfere
    tokio::time::sleep(Duration::from_millis(500)).await;
    for client in clients.iter_mut() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
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
    // Phase 3: Query Prometheus metrics via battle_cmd
    // =========================================================================
    println!("\n  Phase 3: Query Prometheus metrics");

    let metrics_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "prometheus"}
    });
    clients[0]
        .send(Message::Text(metrics_cmd.to_string().into()))
        .await
        .ok();

    let metrics_response = drain_for_type(&mut clients[0], "prometheus_result", 5000).await;

    if let Some(ref parsed) = metrics_response {
        let metrics_text = parsed
            .get("p")
            .and_then(|p| p.get("metrics"))
            .and_then(|m| m.as_str())
            .unwrap_or("");

        if metrics_text.is_empty() {
            checks.check("Prometheus metrics returned data", false);
            println!("    WARNING: Empty metrics response");
        } else {
            checks.check("Prometheus metrics returned data", true);

            let line_count = metrics_text.lines().count();
            println!("    Metrics response: {} lines", line_count);

            // =========================================================================
            // Phase 4: Verify standard metrics
            // =========================================================================
            println!("\n  Phase 4: Verify standard metrics");

            let connections = parse_prometheus_value(metrics_text, "wse_connections ");
            checks.check(
                &format!("wse_connections > 0 (got {})", connections.unwrap_or(0.0)),
                connections.map(|v| v > 0.0).unwrap_or(false),
            );

            let accepted = parse_prometheus_value(metrics_text, "wse_connections_accepted_total ");
            checks.check(
                &format!(
                    "wse_connections_accepted_total > 0 (got {})",
                    accepted.unwrap_or(0.0)
                ),
                accepted.map(|v| v > 0.0).unwrap_or(false),
            );

            let uptime = parse_prometheus_value(metrics_text, "wse_uptime_seconds ");
            checks.check(
                &format!("wse_uptime_seconds > 0 (got {})", uptime.unwrap_or(0.0)),
                uptime.map(|v| v > 0.0).unwrap_or(false),
            );

            let topics = parse_prometheus_value(metrics_text, "wse_topics ");
            checks.check(
                &format!("wse_topics > 0 (got {})", topics.unwrap_or(0.0)),
                topics.map(|v| v > 0.0).unwrap_or(false),
            );

            // =========================================================================
            // Phase 5: Verify per-topic metrics
            // =========================================================================
            println!("\n  Phase 5: Verify per-topic metrics");

            let battle_all_count =
                parse_prometheus_labeled(metrics_text, "wse_topic_messages_total", "battle_all");
            checks.check(
                &format!(
                    "Per-topic: battle_all > 0 (got {})",
                    battle_all_count.unwrap_or(0.0)
                ),
                battle_all_count.map(|v| v > 0.0).unwrap_or(false),
            );

            let battle_half_count =
                parse_prometheus_labeled(metrics_text, "wse_topic_messages_total", "battle_half");
            checks.check(
                &format!(
                    "Per-topic: battle_half > 0 (got {})",
                    battle_half_count.unwrap_or(0.0)
                ),
                battle_half_count.map(|v| v > 0.0).unwrap_or(false),
            );
        }
    } else {
        checks.check("Prometheus metrics response received", false);
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    println!("\n  Clean up");
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
