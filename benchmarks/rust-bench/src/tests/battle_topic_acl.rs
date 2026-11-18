use super::{collect, reply, Checks};
use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const PUBLISH_COUNT: u64 = 200;

const WITHIN: Duration = Duration::from_secs(5);

/// Battle Test: Topic ACL (Access Control Lists)
///
/// Verifies that set_topic_acl correctly restricts topic subscriptions using
/// allow and deny glob patterns.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Topic ACL",
        "Tests topic access control with allow/deny glob patterns.",
    );

    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect 4 clients with different ACL rules
    // =========================================================================
    say!("\n  Phase 1: Connect 4 clients with different ACL rules");

    let mut clients: Vec<WsStream> = Vec::new();
    let labels = ["allow-exact", "allow-glob", "deny-half", "no-acl"];

    for (i, label) in labels.iter().enumerate() {
        let user_id = format!("acl-{}", label);
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
                fail!(
                    "    [FAIL] Failed to connect client {} ({}): {}",
                    i,
                    label,
                    e
                );
                checks.check("All clients connected", false);
                protocol::close_all(clients).await;
                return Vec::new();
            }
        }
    }
    checks.check(
        &format!("Connected {}/4 clients", clients.len()),
        clients.len() == 4,
    );

    // =========================================================================
    // Phase 2: Set ACL on first 3 clients
    // =========================================================================
    say!("\n  Phase 2: Set topic ACL rules");

    // Client A: allow=["battle_all"] (exact match only)
    let acl_a = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "allow": ["battle_all"]}
    });
    clients[0]
        .send(Message::Text(acl_a.to_string().into()))
        .await
        .ok();
    let set = reply(&mut clients[0], "acl_set", WITHIN).await;
    checks.check("Client A's ACL was set", set.is_some());

    // Client B: allow=["battle_*"] (glob pattern)
    let acl_b = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "allow": ["battle_*"]}
    });
    clients[1]
        .send(Message::Text(acl_b.to_string().into()))
        .await
        .ok();
    let set = reply(&mut clients[1], "acl_set", WITHIN).await;
    checks.check("Client B's ACL was set", set.is_some());

    // Client C: deny=["battle_half"]
    let acl_c = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "set_topic_acl", "deny": ["battle_half"]}
    });
    clients[2]
        .send(Message::Text(acl_c.to_string().into()))
        .await
        .ok();
    let set = reply(&mut clients[2], "acl_set", WITHIN).await;
    checks.check("Client C's ACL was set", set.is_some());

    // Client D: no ACL (unrestricted)
    say!("    ACL rules set on clients A, B, C (D = unrestricted)");

    // =========================================================================
    // Phase 3: Subscribe all clients to both topics
    // =========================================================================
    say!("\n  Phase 3: Subscribe all clients to battle_all + battle_half");

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
        &format!("The server took all 4 subscriptions (answered {settled})"),
        settled == 4,
    );

    // =========================================================================
    // Phase 4: Publish messages to both topics
    // =========================================================================
    say!(
        "\n  Phase 4: Publish {} messages to each topic",
        PUBLISH_COUNT
    );

    // Use a separate publisher connection
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "acl-publisher");
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
            fail!("    [FAIL] Failed to connect publisher: {}", e);
            protocol::close_all(clients).await;
            return Vec::new();
        }
    };

    // Publish to battle_all
    let pub_all = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_all",
            "count": PUBLISH_COUNT
        }
    });
    publisher
        .send(Message::Text(pub_all.to_string().into()))
        .await
        .ok();
    let ack = reply(&mut publisher, "publish_ack", WITHIN).await;
    let count = ack.as_ref().and_then(|p| p["count"].as_u64());
    checks.check(
        &format!("Published {PUBLISH_COUNT} to battle_all (ack: {count:?})"),
        count == Some(PUBLISH_COUNT),
    );

    // Publish to battle_half
    let pub_half = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_half",
            "count": PUBLISH_COUNT
        }
    });
    publisher
        .send(Message::Text(pub_half.to_string().into()))
        .await
        .ok();
    let ack = reply(&mut publisher, "publish_ack", WITHIN).await;
    let count = ack.as_ref().and_then(|p| p["count"].as_u64());
    checks.check(
        &format!("Published {PUBLISH_COUNT} to battle_half (ack: {count:?})"),
        count == Some(PUBLISH_COUNT),
    );

    // =========================================================================
    // Phase 5: Collect messages from all clients
    // =========================================================================
    say!("\n  Phase 5: Collect messages");

    // What each client may get of the 200 on each topic. The unrestricted
    // ones are read first: once they have every message, what reached the
    // restricted ones has reached them too.
    let n = PUBLISH_COUNT;
    let want = [(n, 0), (n, n), (n, 0), (n, n)];
    let mut results = [(0u64, 0u64); 4];
    for i in [3, 1, 0, 2] {
        results[i] = collect(&mut clients[i], want[i], WITHIN).await;
    }

    // =========================================================================
    // Phase 6: Verify ACL enforcement
    // =========================================================================
    say!("\n  Phase 6: Verify ACL enforcement");

    let rules = [
        "A (allow battle_all)",
        "B (allow battle_*)",
        "C (deny battle_half)",
        "D (no ACL)",
    ];
    for (i, rule) in rules.iter().enumerate() {
        let (all, half) = results[i];
        let (want_all, want_half) = want[i];
        checks.check(
            &format!(
                "Client {rule}: battle_all={all} (want {want_all}), battle_half={half} (want {want_half})"
            ),
            results[i] == want[i],
        );
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
