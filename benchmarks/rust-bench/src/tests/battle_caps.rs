use super::{next_before, prometheus, reply, topic_count, Checks};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, query_health, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Capability bits of the cluster HELLO, as docs/CLUSTER_PROTOCOL.md
/// numbers them.
const CAP_INTEREST_ROUTING: u64 = 1 << 0;
const CAP_COMPRESSION: u64 = 1 << 1;

/// The topics Server A publishes to, of which Server B's client wants one.
const TOPICS: [&str; 3] = ["battle_all", "battle_half", "battle.glob_test"];

/// What Server B has taken off the cluster link for each of [`TOPICS`].
async fn arrived(ws: &mut WsStream) -> Option<[u64; 3]> {
    let metrics = prometheus(ws).await?;
    Some(TOPICS.map(|topic| topic_count(&metrics, topic)))
}

/// Battle Test: Capability Negotiation Verification
///
/// Reads the capabilities Server B negotiated with each peer, then shows
/// interest routing at work: Server A publishes to three topics, and once a
/// client of Server B subscribes to one, only that one comes over the link.
/// All queries go to Server B; Server A floods its own connections.
///
/// Requires: 2 cluster servers (same as battle-cluster setup)
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2();

    stats::print_header(
        "BATTLE TEST: Capability Negotiation",
        &format!(
            "Verify HELLO handshake, capability bits, cluster health on :{} and :{}.",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect to Server B (subscriber side -- not flooded)
    // =========================================================================
    say!("\n  Phase 1: Connect to Server B (:{})", port2);

    // One connection asks, the other subscribes and gets the flood.
    let mut conns = Vec::with_capacity(2);
    for user in ["bench-user", "bench-sub"] {
        let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), user);
        match protocol::connect_and_handshake(
            &cli.host,
            port2,
            &token,
            "compression=false&protocol_version=1",
            10,
        )
        .await
        {
            Ok(ws) => conns.push(ws),
            Err(e) => {
                fail_err!("    Failed to connect to Server B (:{}) -- {}", port2, e);
                protocol::close_all(conns).await;
                return Vec::new();
            }
        }
    }
    let (Some(mut ws_sub), Some(mut ws_b)) = (conns.pop(), conns.pop()) else {
        return Vec::new();
    };
    drop(token);
    say!("    Connected to Server B (:{})", port2);

    // =========================================================================
    // Phase 2: Cluster health and negotiated capabilities on Server B
    // =========================================================================
    say!("\n  Phase 2: Cluster health and capabilities (Server B)");

    match query_health(&mut ws_b, 5).await {
        Some(h) => {
            let peer_count = h["cluster_peer_count"].as_u64().unwrap_or(0);
            let unknown = h["cluster_unknown_message_types"].as_u64();
            checks.check("cluster_connected == true", h["cluster_connected"] == true);
            checks.check(
                &format!("cluster_peer_count >= 1 (got {peer_count})"),
                peer_count >= 1,
            );
            checks.check(
                &format!("cluster_unknown_message_types == 0 (got {unknown:?})"),
                unknown == Some(0),
            );
        }
        None => {
            checks.check("Health response from Server B", false);
        }
    }

    let cmd = serde_json::json!({"t": "battle_cmd", "p": {"action": "cluster_info"}});
    let _ = ws_b.send(Message::Text(cmd.to_string().into())).await;
    let info = reply(&mut ws_b, "cluster_info_result", Duration::from_secs(5)).await;
    let peers = info
        .as_ref()
        .and_then(|p| p["peers"].as_array())
        .cloned()
        .unwrap_or_default();
    checks.check(
        &format!("Server B lists its peers (got {})", peers.len()),
        !peers.is_empty(),
    );
    for peer in &peers {
        let caps = peer["capabilities"].as_u64().unwrap_or(0);
        let address = peer["address"].as_str().unwrap_or("?");
        checks.check(
            &format!(
                "Peer {address}: interest routing and compression negotiated (caps {caps:#b})"
            ),
            caps & (CAP_INTEREST_ROUTING | CAP_COMPRESSION)
                == CAP_INTEREST_ROUTING | CAP_COMPRESSION,
        );
    }

    // =========================================================================
    // Phase 3: Interest routing: only the topic Server B wants comes over
    // =========================================================================
    say!("\n  Phase 3: Verify topic routing (CAP_INTEREST_ROUTING)");

    let before = arrived(&mut ws_b).await;
    if !checks.check(
        &format!("Server B's counters before subscribing ({before:?})"),
        before.is_some(),
    ) {
        protocol::close_all(vec![ws_b, ws_sub]).await;
        checks.finish();
        return Vec::new();
    }
    let before = before.unwrap_or_default();

    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_sub.send(Message::Text(cmd.to_string().into())).await;
    say!("    Subscribed to 'battle_all'");

    // Counted on its own task, from Server A's first message on.
    let reader = tokio::spawn(async move {
        let mut got = [0u64; 3];
        let mut until = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut started = false;
        while let Some(msg) = next_before(&mut ws_sub, until).await {
            let Some(parsed) = parse_wse_message(&msg) else {
                continue;
            };
            if parsed["t"] != "battle" {
                continue;
            }
            if let Some(i) = TOPICS.iter().position(|t| parsed["p"]["ch"] == *t) {
                got[i] += 1;
            }
            if !started {
                started = true;
                until = tokio::time::Instant::now() + Duration::from_secs(3);
            }
        }
        (ws_sub, got)
    });
    let (ws_sub, got) = match reader.await {
        Ok(read) => read,
        Err(e) => {
            fail!("    [FAIL] The subscriber's reader failed: {}", e);
            protocol::close_all(vec![ws_b]).await;
            checks.finish();
            return Vec::new();
        }
    };
    checks.check(
        &format!(
            "The client got battle_all only (battle_all={}, battle_half={}, battle.glob_test={})",
            got[0], got[1], got[2]
        ),
        got[0] > 0 && got[1] == 0 && got[2] == 0,
    );

    // =========================================================================
    // Phase 4: What came over the link
    // =========================================================================
    say!("\n  Phase 4: What Server A forwarded to Server B");

    match arrived(&mut ws_b).await {
        Some(after) => {
            let added: [u64; 3] = std::array::from_fn(|i| after[i].saturating_sub(before[i]));
            checks.check(
                &format!(
                    "Server A forwarded battle_all only (battle_all +{}, battle_half +{}, battle.glob_test +{})",
                    added[0], added[1], added[2]
                ),
                added[0] > 0 && added[1] == 0 && added[2] == 0,
            );
        }
        None => {
            checks.check("Server B's counters after the traffic", false);
        }
    }

    // Cleanup
    protocol::close_all(vec![ws_b, ws_sub]).await;

    checks.finish();

    Vec::new()
}
