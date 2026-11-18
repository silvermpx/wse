pub mod battle_caps;
pub mod battle_cluster;
pub mod battle_compression;
pub mod battle_discovery;
pub mod battle_drain;
pub mod battle_encryption;
pub mod battle_load;
pub mod battle_metrics;
pub mod battle_presence;
pub mod battle_queue_groups;
pub mod battle_recovery;
pub mod battle_standalone;
pub mod battle_tls;
pub mod battle_topic_acl;
pub mod battle_topology;
pub mod connection_limit;
pub mod connection_storm;
pub mod fanout_broadcast;
pub mod fanout_cluster;
pub mod fanout_cluster_tls;
pub mod format_comparison;
pub mod ping_latency;
pub mod size_matrix;
pub mod sustained_hold;
pub mod throughput;

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};

use crate::protocol::{parse_wse_message, WsStream};

/// The next message `ws` gets before `deadline`; None once it has passed.
/// `timeout_at` alone hands over a message that is ready however late it
/// is, so a reader of a flood would never stop.
pub(crate) async fn next_before(
    ws: &mut WsStream,
    deadline: tokio::time::Instant,
) -> Option<tokio_tungstenite::tungstenite::Message> {
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    match tokio::time::timeout_at(deadline, ws.next()).await {
        Ok(Some(Ok(msg))) => Some(msg),
        _ => None,
    }
}

/// After what a battle test waits for has come, how long it listens for one
/// more, so that one too many shows.
pub(crate) const EXTRA: Duration = Duration::from_millis(250);

/// The battle messages `ws` gets on battle_all and battle_half, until it has
/// `want` of each or `within` passes, and then [`EXTRA`] more.
pub(crate) async fn collect(ws: &mut WsStream, want: (u64, u64), within: Duration) -> (u64, u64) {
    let mut got = (0u64, 0u64);
    let mut until = tokio::time::Instant::now() + within;
    let mut done = false;
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        let Ok(Some(Ok(msg))) = tokio::time::timeout(left, ws.next()).await else {
            break;
        };
        let Some(parsed) = parse_wse_message(&msg) else {
            continue;
        };
        if parsed["t"] != "battle" {
            continue;
        }
        match parsed["p"]["ch"].as_str() {
            Some("battle_all") => got.0 += 1,
            Some("battle_half") => got.1 += 1,
            _ => {}
        }
        if !done && got.0 >= want.0 && got.1 >= want.1 {
            done = true;
            until = tokio::time::Instant::now() + EXTRA;
        }
    }
    got
}

/// The payload of the first `kind` message `ws` gets within `within`; what
/// comes before it is passed over.
pub(crate) async fn reply(
    ws: &mut WsStream,
    kind: &str,
    within: Duration,
) -> Option<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            return None;
        }
        match tokio::time::timeout(left, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                if let Some(mut parsed) = parse_wse_message(&msg) {
                    if parsed["t"] == kind {
                        return Some(parsed["p"].take());
                    }
                }
            }
            _ => return None,
        }
    }
}

/// The server's Prometheus text, asked for on `ws` with a battle command.
pub(crate) async fn prometheus(ws: &mut WsStream) -> Option<String> {
    let cmd = serde_json::json!({"t": "battle_cmd", "p": {"action": "prometheus"}});
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        cmd.to_string().into(),
    ))
    .await
    .ok()?;
    let mut p = reply(ws, "prometheus_result", Duration::from_secs(5)).await?;
    match p["metrics"].take() {
        serde_json::Value::String(text) if !text.is_empty() => Some(text),
        _ => None,
    }
}

/// How many messages the server's Prometheus text counts for `topic`;
/// none counted yet is 0.
pub(crate) fn topic_count(metrics: &str, topic: &str) -> u64 {
    let series = format!("wse_topic_messages_total{{topic=\"{topic}\"}} ");
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(&series))
        .and_then(|value| value.trim().parse::<f64>().ok())
        .map_or(0, |v| v as u64)
}

/// A battle test's checks, each said as it is made: one that fails fails
/// the run, however the test ends.
#[derive(Default)]
pub(crate) struct Checks {
    passed: u32,
    failed: u32,
}

impl Checks {
    /// Says `name` passed or failed as `holds` says, and returns `holds`.
    pub fn check(&mut self, name: &str, holds: bool) -> bool {
        if holds {
            self.passed += 1;
            say!("    [PASS] {}", name);
        } else {
            self.failed += 1;
            crate::report::failed();
            say!("    [FAIL] {}", name);
        }
        holds
    }

    /// The test's summary line.
    pub fn finish(&self) {
        say!(
            "\n  Result: {}/{} checks passed",
            self.passed,
            self.passed + self.failed
        );
        if self.failed > 0 {
            say!("  FAILED");
        } else {
            say!("  PASSED");
        }
    }
}

/// What one client got in a window of a battle test, by channel, and
/// whether its connection lasted the window: a check holds for every
/// client, never for a group's total alone.
#[derive(Clone, Copy, Default)]
pub(crate) struct Seen {
    pub lasted: bool,
    pub all: u64,
    pub battle_all: u64,
    pub battle_half: u64,
    pub battle_glob: u64,
    pub bytes: u64,
}

/// How many of `clients` hold for `holds`.
pub(crate) fn count(clients: &[Seen], holds: impl Fn(&Seen) -> bool) -> usize {
    clients.iter().filter(|seen| holds(seen)).count()
}

/// Receives until `deadline`, counting each channel's messages.
pub(crate) async fn receive_channels(
    mut ws: WsStream,
    deadline: tokio::time::Instant,
) -> (WsStream, Seen) {
    let mut seen = Seen {
        lasted: true,
        ..Seen::default()
    };
    let mut received = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                seen.bytes += msg.len() as u64;
                received += 1;
                // A reader whose socket is never empty yields now and then,
                // so the others are read too: without it some clients of a
                // flood were never read at all.
                if received.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
                if let Some(parsed) = parse_wse_message(&msg) {
                    let ch = parsed
                        .get("p")
                        .and_then(|p| p.get("ch"))
                        .and_then(|v| v.as_str());
                    match ch {
                        Some("all") => seen.all += 1,
                        Some("battle_all") => seen.battle_all += 1,
                        Some("battle_half") => seen.battle_half += 1,
                        Some("battle.glob_test") => seen.battle_glob += 1,
                        _ => {}
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => {
                seen.lasted = false;
                break;
            }
            Err(_) => break, // deadline reached
        }
    }
    (ws, seen)
}
