use super::{reply, Checks, EXTRA};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const PRESENCE_CLIENTS: usize = 5;
const PRESENCE_TOPIC: &str = "presence_room";

fn user(i: usize) -> String {
    format!("presence-user-{i}")
}

/// The payloads of the `kind` events `ws` gets: `want` of them, or fewer if
/// `within` passes first, and whatever else of the kind comes in a short
/// while after, so that one too many shows.
async fn events_of(
    ws: &mut WsStream,
    kind: &str,
    want: usize,
    within: Duration,
) -> Vec<serde_json::Value> {
    let mut got = Vec::with_capacity(want);
    let mut until = tokio::time::Instant::now() + if want == 0 { EXTRA } else { within };
    loop {
        let left = until.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        match tokio::time::timeout(left, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                let Some(mut parsed) = parse_wse_message(&msg) else {
                    continue;
                };
                if parsed["t"] == kind {
                    got.push(parsed["p"].take());
                    if got.len() == want {
                        until = tokio::time::Instant::now() + EXTRA;
                    }
                }
            }
            _ => break,
        }
    }
    got
}

/// The users the events are of, in the order they came.
fn users(events: &[serde_json::Value]) -> Vec<&str> {
    events
        .iter()
        .filter_map(|p| p["user_id"].as_str())
        .collect()
}

/// Battle Test: Presence Tracking
///
/// Tests presence join/leave events, presence query, data updates, and
/// clean disconnect handling. Connects 5 clients with different user IDs,
/// subscribes 3 with presence data, verifies join/leave/update events.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
/// (--no-publish avoids broadcast flood that would cause presence events to be
/// dropped by outbound backpressure)
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Presence Tracking",
        "Tests presence join/leave events, multi-user tracking, presence query, data updates.",
    );

    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect clients with DIFFERENT user IDs
    // =========================================================================
    say!(
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
                fail!("    [FAIL] Failed to connect client {}: {}", i, e);
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
    // Phase 2: Subscribe the first 3 clients with presence, one at a time
    // =========================================================================
    say!(
        "\n  Phase 2: Subscribe 3 clients to '{}' with presence",
        PRESENCE_TOPIC
    );

    // Each waits for its own join before the next subscribes, so the topic
    // has been told of it first: client k hears the joins of k and later.
    let within = Duration::from_secs(3);
    let mut joins: [Vec<serde_json::Value>; 3] = Default::default();
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
        joins[i] = events_of(client, "presence_join", 1, within).await;
    }

    // =========================================================================
    // Phase 3: Verify presence_join events
    // =========================================================================
    say!("\n  Phase 3: Verify presence_join events");

    for (i, client) in clients.iter_mut().enumerate().take(3) {
        let later = events_of(client, "presence_join", 2 - i, within).await;
        joins[i].extend(later);
        let want: Vec<String> = (i..3).map(user).collect();
        let got = users(&joins[i]);
        checks.check(
            &format!("Client {i} heard the joins of {want:?} (got {got:?})"),
            got == want,
        );
    }

    // =========================================================================
    // Phase 4: Query presence via battle_cmd
    // =========================================================================
    say!("\n  Phase 4: Query presence members");

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

    let mut members: Vec<String> = Vec::new();
    match reply(&mut clients[0], "presence_result", within).await {
        Some(p) => {
            if let Some(found) = p["members"].as_object() {
                members = found.keys().cloned().collect();
                members.sort_unstable();
            }
        }
        None => say!("    WARNING: No presence_result response received"),
    }
    let want: Vec<String> = (0..3).map(user).collect();
    checks.check(
        &format!("Presence query shows {want:?} (got {members:?})"),
        members == want,
    );

    // =========================================================================
    // Phase 5: Disconnect client 2, verify presence_leave
    // =========================================================================
    say!("\n  Phase 5: Disconnect client 2, verify presence_leave");

    let _ = clients[2].close(None).await;
    for (i, client) in clients.iter_mut().enumerate().take(2) {
        let left = events_of(client, "presence_leave", 1, within).await;
        let got = users(&left);
        checks.check(
            &format!("Client {i} heard the leave of {} (got {got:?})", user(2)),
            got == [user(2)],
        );
    }

    // =========================================================================
    // Phase 6: Update presence data for client 1
    // =========================================================================
    say!("\n  Phase 6: Update presence data for client 1");

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

    for (i, client) in clients.iter_mut().enumerate().take(2) {
        let updates = events_of(client, "presence_update", 1, within).await;
        let got = users(&updates);
        let away = updates.iter().all(|p| p["data"]["status"] == "away");
        checks.check(
            &format!(
                "Client {i} heard {} go away (got {got:?}, away: {away})",
                user(1)
            ),
            got == [user(1)] && away,
        );
    }

    // =========================================================================
    // Phase 7: Clean up
    // =========================================================================
    say!("\n  Phase 7: Clean up");

    for client in &mut clients {
        let _ = client.close(None).await;
    }
    say!("    All clients disconnected");

    // =========================================================================
    // Summary
    // =========================================================================
    checks.finish();

    Vec::new()
}
