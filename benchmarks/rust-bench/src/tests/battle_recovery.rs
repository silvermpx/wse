use super::{Checks, EXTRA};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const RECOVERY_TOPIC: &str = "recovery_test";
/// The recovery buffer holds 256 messages: the test publishes fewer.
const SEEDED: u64 = 50;
const LIVE: u64 = 20;
const MISSED: u64 = 100;
const WITHIN: Duration = Duration::from_secs(5);

/// What `ws` gets until a `reply` message and `want` battle messages have
/// come, or `within` passes, and then a short while more, so that one too
/// many shows: the battle messages' seqs, in the order they came, and the
/// reply.
async fn read_until(
    ws: &mut WsStream,
    reply: &str,
    want: u64,
    within: Duration,
) -> (Vec<u64>, Option<serde_json::Value>) {
    let mut seqs = Vec::with_capacity(want as usize);
    let mut replied = None;
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
        let Some(mut parsed) = parse_wse_message(&msg) else {
            continue;
        };
        if parsed["t"] == "battle" {
            seqs.push(parsed["p"]["seq"].as_u64().unwrap_or(u64::MAX));
        } else if parsed["t"] == reply && replied.is_none() {
            replied = Some(parsed["p"].take());
        }
        if !done && replied.is_some() && seqs.len() as u64 >= want {
            done = true;
            until = tokio::time::Instant::now() + EXTRA;
        }
    }
    (seqs, replied)
}

/// The count a `publish_ack` says was published.
fn acked(ack: &Option<serde_json::Value>) -> Option<u64> {
    ack.as_ref()?["count"].as_u64()
}

/// The (epoch, offset) a `subscription_update` gives for the topic.
fn position(update: &Option<serde_json::Value>) -> Option<(String, u64)> {
    let at = &update.as_ref()?["recovery"][RECOVERY_TOPIC];
    Some((at["epoch"].as_str()?.to_owned(), at["offset"].as_u64()?))
}

fn publish(count: u64) -> Message {
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "publish_messages", "topic": RECOVERY_TOPIC, "count": count}
    });
    Message::Text(cmd.to_string().into())
}

fn subscribe(from: Option<(&str, u64)>) -> Message {
    let mut p = serde_json::json!({
        "action": "subscribe_recovery",
        "topics": [RECOVERY_TOPIC],
        "recover": from.is_some()
    });
    if let Some((epoch, offset)) = from {
        p["recovery"] = serde_json::json!({RECOVERY_TOPIC: {"epoch": epoch, "offset": offset}});
    }
    Message::Text(
        serde_json::json!({"t": "battle_cmd", "p": p})
            .to_string()
            .into(),
    )
}

/// Whether `seqs` are 0, 1, ... `n - 1`, in that order.
fn in_order(seqs: &[u64], n: u64) -> bool {
    seqs.len() as u64 == n && seqs.iter().copied().eq(0..n)
}

async fn connect(cli: &Cli, token: &str) -> Option<WsStream> {
    match protocol::connect_and_handshake(
        &cli.host,
        cli.port,
        token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => Some(ws),
        Err(e) => {
            fail!("    [FAIL] Failed to connect: {}", e);
            None
        }
    }
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

    let mut checks = Checks::default();

    // =========================================================================
    // Step 1: Connect and seed the recovery buffer
    // =========================================================================
    say!("\n  Step 1: Connect and seed recovery buffer");

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "recovery-user");
    let Some(mut ws) = connect(cli, &token).await else {
        return Vec::new();
    };

    // Not subscribed yet: the messages only fill the buffer.
    ws.send(publish(SEEDED)).await.ok();
    let (seqs, ack) = read_until(&mut ws, "publish_ack", 0, WITHIN).await;
    checks.check(
        &format!(
            "Seeded the buffer with {SEEDED} messages (ack: {:?}, received: {})",
            acked(&ack),
            seqs.len()
        ),
        acked(&ack) == Some(SEEDED) && seqs.is_empty(),
    );

    // =========================================================================
    // Step 2: Subscribe with recovery (initial, get epoch/offset)
    // =========================================================================
    say!("\n  Step 2: Subscribe with recovery (initial)");

    ws.send(subscribe(None)).await.ok();
    let (seqs, update) = read_until(&mut ws, "subscription_update", 0, WITHIN).await;
    let start = position(&update);
    if !checks.check(
        &format!("Got recovery info ({start:?}, {} messages)", seqs.len()),
        start.is_some() && seqs.is_empty(),
    ) {
        fail!("    ABORT: No recovery info from server. Is --recovery enabled?");
        let _ = ws.close(None).await;
        checks.finish();
        return Vec::new();
    }
    let (epoch, start) = start.unwrap_or_default();

    // =========================================================================
    // Step 3: Publish more messages while subscribed (we receive them)
    // =========================================================================
    say!("\n  Step 3: Publish {LIVE} more messages (we receive these)");

    ws.send(publish(LIVE)).await.ok();
    let (seqs, ack) = read_until(&mut ws, "publish_ack", LIVE, WITHIN).await;
    checks.check(
        &format!(
            "Received the {LIVE} published, in order (got {}, ack: {:?})",
            seqs.len(),
            acked(&ack)
        ),
        in_order(&seqs, LIVE) && acked(&ack) == Some(LIVE),
    );

    // Where the connection stands now: what it received moved it on by
    // exactly as many.
    ws.send(subscribe(None)).await.ok();
    let (seqs, update) = read_until(&mut ws, "subscription_update", 0, WITHIN).await;
    let last = position(&update);
    let want = (epoch.clone(), start + LIVE);
    if !checks.check(
        &format!("Position moved on by {LIVE} to {want:?} (got {last:?})"),
        last.as_ref() == Some(&want) && seqs.is_empty(),
    ) {
        let _ = ws.close(None).await;
        checks.finish();
        return Vec::new();
    }
    let last_offset = want.1;

    // =========================================================================
    // Step 4: Disconnect and publish messages we'll miss
    // =========================================================================
    say!("\n  Step 4: Disconnect");
    let _ = ws.close(None).await;

    say!("    Publishing {MISSED} messages while disconnected...");
    let Some(mut helper) = connect(cli, &token).await else {
        checks.finish();
        return Vec::new();
    };
    helper.send(publish(MISSED)).await.ok();
    let (seqs, ack) = read_until(&mut helper, "publish_ack", 0, WITHIN).await;
    checks.check(
        &format!(
            "Published {MISSED} messages while disconnected (ack: {:?}, helper received: {})",
            acked(&ack),
            seqs.len()
        ),
        acked(&ack) == Some(MISSED) && seqs.is_empty(),
    );
    let _ = helper.close(None).await;

    // =========================================================================
    // Step 5: Reconnect with recovery
    // =========================================================================
    say!("\n  Step 5: Reconnect and subscribe with recover=true");

    let Some(mut ws2) = connect(cli, &token).await else {
        checks.finish();
        return Vec::new();
    };
    ws2.send(subscribe(Some((&epoch, last_offset)))).await.ok();

    // =========================================================================
    // Step 6: Verify recovery response and the replay
    // =========================================================================
    say!("\n  Step 6: Verify recovery response and the replay");

    // The replay is queued ahead of the reply.
    let (replayed, update) = read_until(&mut ws2, "subscription_update", MISSED, WITHIN).await;
    let recovered = update.as_ref().and_then(|p| p["recovered"].as_bool());
    let count = update
        .as_ref()
        .and_then(|p| p["recovery"][RECOVERY_TOPIC]["count"].as_u64());
    checks.check(
        &format!("Recovered all {MISSED} missed (recovered: {recovered:?}, count: {count:?})"),
        recovered == Some(true) && count == Some(MISSED),
    );
    checks.check(
        &format!(
            "Replayed the {MISSED} missed, in order (got {})",
            replayed.len()
        ),
        in_order(&replayed, MISSED),
    );

    // =========================================================================
    // Step 7: Verify health shows recovery stats
    // =========================================================================
    say!("\n  Step 7: Verify recovery health stats");

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
    say!("\n  Clean up");
    let _ = ws2.close(None).await;
    say!("    Client disconnected");

    checks.finish();
    Vec::new()
}
