use super::{next_before, Checks};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, query_health, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Topics only this test publishes, on Server A alone: the probe until
/// Server A forwards to Server C, then the counted ones.
const PROBE: &str = "discovery_probe";
const COUNTED: &str = "discovery_from_a";
const PUBLISHED: u64 = 100;

fn publish(topic: &str, count: u64) -> Message {
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "publish_messages", "topic": topic, "count": count}
    });
    Message::Text(cmd.to_string().into())
}

fn subscribe(topic: &str) -> Message {
    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": [topic]}
    });
    Message::Text(cmd.to_string().into())
}

/// The channel and seq of a battle message.
fn battle(msg: &Message) -> Option<(String, u64)> {
    let parsed = parse_wse_message(msg)?;
    if parsed["t"] != "battle" {
        return None;
    }
    Some((
        parsed["p"]["ch"].as_str()?.to_owned(),
        parsed["p"]["seq"].as_u64()?,
    ))
}

async fn connect(cli: &Cli, port: u16, token: &str) -> Option<WsStream> {
    match protocol::connect_and_handshake(
        &cli.host,
        port,
        token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => Some(ws),
        Err(e) => {
            fail_err!("    Failed to connect to :{} -- {}", port, e);
            None
        }
    }
}

/// Battle Test: Gossip Peer Discovery
///
/// Tests 3-node gossip-based peer discovery:
///   Node A listens only (no --peers), has cluster_addr for gossip
///   Node B has --peers to A's cluster port, discovers A directly
///   Node C uses seeds=[B's cluster_addr] to discover the cluster via gossip
///
/// After gossip propagation:
///   - B gossips C's address to A (via B's outbound to A)
///   - A discovers C and creates outbound connection to C
///   - A delivers messages to C: the cluster is a full mesh and relays
///     nothing, so what C gets of a topic only A publishes came over a link
///     between A and C, which only gossip made
///
/// Note: Server A and B (publisher mode) flood broadcast_all at 500K/s to all
/// connections, making health queries unreliable there. Health checks use
/// Server C; Server A is only told to publish.
///
/// IMPORTANT: Start servers in this order:
///   1. Server A first (listener, no outbound peers)
///   2. Server B second (connects outbound to A's cluster port)
///   3. Server C last (connects to B's cluster port via seed)
///
/// Requires: 3 servers
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 \
///       --cluster-port 6006 --cluster-addr 127.0.0.1:6006
///   python benchmarks/bench_battle_server.py --mode cluster --port 5007 --peers 127.0.0.1:6006 \
///       --cluster-port 6007 --cluster-addr 127.0.0.1:6007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5008 \
///       --cluster-port 6008 --cluster-addr 127.0.0.1:6008 --seeds 127.0.0.1:6007
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2();
    let port3 = cli.port3();

    stats::print_header(
        "BATTLE TEST: Gossip Peer Discovery",
        &format!(
            "3-node gossip: A(:{}) <- B(:{}) direct, C(:{}) discovers via seeds.",
            cli.port, port2, port3
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect to Server C (subscriber side -- not flooded)
    // =========================================================================
    say!("\n  Phase 1: Connect to Server C (:{})", port3);

    let Some(mut ws_c) = connect(cli, port3, &token).await else {
        eprintln!(
            "\n  Start 3 servers (in this order):\n    \
            python benchmarks/bench_battle_server.py --mode cluster --port {} \
            --cluster-port 6006 --cluster-addr 127.0.0.1:6006\n    \
            python benchmarks/bench_battle_server.py --mode cluster --port {} --peers 127.0.0.1:6006 \
            --cluster-port 6007 --cluster-addr 127.0.0.1:6007\n    \
            python benchmarks/bench_battle_server.py --mode cluster-subscribe --port {} \
            --cluster-port 6008 --cluster-addr 127.0.0.1:6008 --seeds 127.0.0.1:6007",
            cli.port, port2, port3
        );
        return Vec::new();
    };
    say!("    Connected to Server C (:{})", port3);

    // =========================================================================
    // Phase 2: Wait for gossip propagation and verify on Server C
    // =========================================================================
    say!("\n  Phase 2: Wait for gossip propagation (up to 20s)");

    // In a cluster of three, each node links to the other two.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut health_c = None;
    while tokio::time::Instant::now() < deadline {
        health_c = query_health(&mut ws_c, 5).await;
        if health_c
            .as_ref()
            .is_some_and(|h| h["cluster_peer_count"].as_u64() == Some(2))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let delivered_before = match health_c {
        Some(ref h) => {
            let peers = h["cluster_peer_count"].as_u64();
            let unknown = h["cluster_unknown_message_types"].as_u64();
            checks.check(
                &format!("Server C links to A and B: cluster_peer_count == 2 (got {peers:?})"),
                peers == Some(2),
            );
            checks.check(
                "Server C cluster_connected == true",
                h["cluster_connected"] == true,
            );
            checks.check(
                &format!("Server C cluster_unknown_message_types == 0 (got {unknown:?})"),
                unknown == Some(0),
            );
            h["cluster_messages_delivered"].as_u64()
        }
        None => {
            checks.check("Server C health response", false);
            None
        }
    };

    // =========================================================================
    // Phase 3: Verify message delivery A -> C (full mesh)
    // =========================================================================
    say!("\n  Phase 3: Verify A -> C message delivery via gossip mesh");

    // Server C's interest goes to Server A in the order it subscribed: once
    // the probe comes through, the counted topic does too.
    ws_c.send(subscribe(COUNTED)).await.ok();
    ws_c.send(subscribe(PROBE)).await.ok();

    let Some(mut ws_a) = connect(cli, cli.port, &token).await else {
        protocol::close_all(vec![ws_c]).await;
        checks.finish();
        return Vec::new();
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut probed = false;
    while !probed && tokio::time::Instant::now() < deadline {
        ws_a.send(publish(PROBE, 1)).await.ok();
        let wait = tokio::time::Instant::now() + Duration::from_millis(250);
        while let Some(msg) = next_before(&mut ws_c, wait).await {
            if battle(&msg).is_some_and(|(ch, _)| ch == PROBE) {
                probed = true;
                break;
            }
        }
    }
    checks.check("Server A forwards to Server C (probe arrived)", probed);

    let mut seqs = Vec::with_capacity(PUBLISHED as usize);
    if probed {
        ws_a.send(publish(COUNTED, PUBLISHED)).await.ok();
        // Every one, then a short while for one too many.
        let mut until = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut whole = false;
        while let Some(msg) = next_before(&mut ws_c, until).await {
            if let Some((ch, seq)) = battle(&msg) {
                if ch == COUNTED {
                    seqs.push(seq);
                }
            }
            if !whole && seqs.len() as u64 >= PUBLISHED {
                whole = true;
                until = tokio::time::Instant::now() + super::EXTRA;
            }
        }
    }
    checks.check(
        &format!(
            "Server C got all {PUBLISHED} of Server A's messages, in order (got {})",
            seqs.len()
        ),
        seqs.len() as u64 == PUBLISHED && seqs.iter().copied().eq(0..PUBLISHED),
    );
    // Server A floods its own connections: nothing to read there.
    drop(ws_a);

    // =========================================================================
    // Phase 4: Verify delivery metrics on Server C
    // =========================================================================
    say!("\n  Phase 4: Verify delivery metrics on Server C");

    let delivered_after = query_health(&mut ws_c, 5)
        .await
        .and_then(|h| h["cluster_messages_delivered"].as_u64());
    let added = delivered_after
        .zip(delivered_before)
        .map(|(after, before)| after.saturating_sub(before));
    checks.check(
        &format!("cluster_messages_delivered grew by at least {PUBLISHED} (got {added:?})"),
        added.is_some_and(|added| added >= PUBLISHED),
    );

    // Cleanup
    protocol::close_all(vec![ws_c]).await;

    checks.finish();

    Vec::new()
}
