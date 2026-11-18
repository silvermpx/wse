use super::{collect, prometheus, reply, topic_count, Checks};
use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

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

const ALL: u64 = 500;
const HALF: u64 = 300;
const WITHIN: Duration = Duration::from_secs(5);

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

    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect and subscribe
    // =========================================================================
    say!("\n  Phase 1: Connect 3 clients and a publisher, and subscribe");

    // The publisher asks for the publications and the metrics; the clients
    // only receive.
    let mut clients: Vec<WsStream> = Vec::new();
    for name in [
        "metrics-user-0",
        "metrics-user-1",
        "metrics-user-2",
        "metrics-publisher",
    ] {
        let t = crate::jwt::generate_bench_token(cli.secret.as_bytes(), name);
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
            Err(e) => fail!("    [FAIL] Failed to connect {}: {}", name, e),
        }
    }
    if !checks.check(
        &format!("Connected {}/4 connections", clients.len()),
        clients.len() == 4,
    ) {
        protocol::close_all(clients).await;
        checks.finish();
        return Vec::new();
    }
    let Some(mut publisher) = clients.pop() else {
        return Vec::new();
    };

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
    // The server takes a connection's commands in order: once it has
    // answered the next one, the subscription is in place.
    let mut settled = 0;
    for client in clients.iter_mut() {
        settled += usize::from(protocol::query_health(client, 5).await.is_some());
    }
    checks.check(
        &format!("The server took all 3 subscriptions (answered {settled})"),
        settled == 3,
    );

    // The counters as they stand before: they count since the server
    // started, so what this run adds is what it compares.
    let before = prometheus(&mut publisher).await;
    if !checks.check("Prometheus metrics before publishing", before.is_some()) {
        let _ = publisher.close(None).await;
        protocol::close_all(clients).await;
        checks.finish();
        return Vec::new();
    }
    let before = before.unwrap_or_default();

    // =========================================================================
    // Phase 2: Publish controlled traffic to both topics
    // =========================================================================
    say!("\n  Phase 2: Publish {ALL} to battle_all, {HALF} to battle_half");

    for (topic, count) in [("battle_all", ALL), ("battle_half", HALF)] {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "publish_messages", "topic": topic, "count": count}
        });
        publisher
            .send(Message::Text(cmd.to_string().into()))
            .await
            .ok();
        let ack = reply(&mut publisher, "publish_ack", WITHIN).await;
        let acked = ack.as_ref().and_then(|p| p["count"].as_u64());
        checks.check(
            &format!("Published {count} to {topic} (ack: {acked:?})"),
            acked == Some(count),
        );
    }

    for (i, client) in clients.iter_mut().enumerate() {
        let got = collect(client, (ALL, HALF), WITHIN).await;
        checks.check(
            &format!(
                "Client {i} received battle_all={}, battle_half={} (want {ALL}, {HALF})",
                got.0, got.1
            ),
            got == (ALL, HALF),
        );
    }

    // =========================================================================
    // Phase 3: Query Prometheus metrics via battle_cmd
    // =========================================================================
    say!("\n  Phase 3: Query Prometheus metrics");

    match prometheus(&mut publisher).await {
        Some(metrics_text) => {
            checks.check("Prometheus metrics returned data", true);
            say!(
                "    Metrics response: {} lines",
                metrics_text.lines().count()
            );

            // =================================================================
            // Phase 4: Verify standard metrics
            // =================================================================
            say!("\n  Phase 4: Verify standard metrics");

            let at_least = |name: &str, least: f64| {
                let got = parse_prometheus_value(&metrics_text, &format!("{name} "));
                (
                    format!("{name} >= {least} (got {got:?})"),
                    got.is_some_and(|v| v >= least),
                )
            };
            // This run's own 4 connections are open, and 2 topics have its
            // subscribers.
            for (name, least) in [
                ("wse_connections", 4.0),
                ("wse_connections_accepted_total", 4.0),
                ("wse_topics", 2.0),
            ] {
                let (said, holds) = at_least(name, least);
                checks.check(&said, holds);
            }
            let uptime = parse_prometheus_value(&metrics_text, "wse_uptime_seconds ");
            checks.check(
                &format!("wse_uptime_seconds > 0 (got {uptime:?})"),
                uptime.is_some_and(|v| v > 0.0),
            );

            // =================================================================
            // Phase 5: Verify per-topic metrics
            // =================================================================
            say!("\n  Phase 5: Verify per-topic metrics");

            for (topic, count) in [("battle_all", ALL), ("battle_half", HALF)] {
                let added =
                    topic_count(&metrics_text, topic).checked_sub(topic_count(&before, topic));
                checks.check(
                    &format!("Per-topic: {topic} counted {added:?} more (want {count})"),
                    added == Some(count),
                );
            }
        }
        None => {
            checks.check("Prometheus metrics returned data", false);
        }
    }

    // =========================================================================
    // Clean up
    // =========================================================================
    say!("\n  Clean up");
    let _ = publisher.close(None).await;
    protocol::close_all(clients).await;
    say!("    All clients disconnected");

    checks.finish();

    Vec::new()
}
