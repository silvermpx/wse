use super::{reply, Checks};
use crate::config::Cli;
use crate::protocol::{self, query_health, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

/// The peers a server's cluster_info lists.
async fn peers(ws: &mut WsStream) -> Option<Vec<serde_json::Value>> {
    let cmd = serde_json::json!({"t": "battle_cmd", "p": {"action": "cluster_info"}});
    ws.send(Message::Text(cmd.to_string().into())).await.ok()?;
    // Server A floods its connections: the answer comes after what it
    // queued before it.
    let mut p = reply(ws, "cluster_info_result", Duration::from_secs(10)).await?;
    match p["peers"].take() {
        serde_json::Value::Array(peers) => Some(peers),
        _ => None,
    }
}

/// What is wrong with a peer entry, if anything: an address with a port, an
/// instance id, negotiated capabilities, and when its link came up, in
/// epoch milliseconds and not after now.
fn peer_faults(peer: &serde_json::Value, now_ms: u64) -> Vec<&'static str> {
    let mut faults = Vec::new();
    let address = peer["address"].as_str().unwrap_or("");
    if address
        .rsplit_once(':')
        .is_none_or(|(host, port)| host.is_empty() || port.parse::<u16>().is_err())
    {
        faults.push("address is not host:port");
    }
    if peer["instance_id"].as_str().is_none_or(str::is_empty) {
        faults.push("no instance_id");
    }
    if peer["capabilities"].as_u64().is_none_or(|caps| caps == 0) {
        faults.push("no capabilities");
    }
    // A minute of slack for the clocks of the bench and the server.
    if peer["connected_at"]
        .as_u64()
        .is_none_or(|at| at == 0 || at > now_ms + 60_000)
    {
        faults.push("connected_at is not a time up to now");
    }
    faults
}

/// Battle Test: Cluster Topology API
///
/// Verifies cluster_info() on both servers: each lists the other as its one
/// peer, with address, instance_id, capabilities and connected_at, and the
/// two name different instances.
///
/// Requires: 2 cluster servers (same as battle-cluster setup)
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2();

    stats::print_header(
        "BATTLE TEST: Cluster Topology API",
        &format!(
            "Verify cluster_info() returns peer list on :{} and :{}.",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect to both servers
    // =========================================================================
    say!(
        "\n  Phase 1: Connect to Server A (:{}) and Server B (:{})",
        cli.port,
        port2
    );

    let mut conns = Vec::with_capacity(2);
    for port in [cli.port, port2] {
        match protocol::connect_and_handshake(
            &cli.host,
            port,
            &token,
            "compression=false&protocol_version=1",
            10,
        )
        .await
        {
            Ok(ws) => conns.push(ws),
            Err(e) => {
                fail_err!("    Failed to connect to :{} -- {}", port, e);
                protocol::close_all(conns).await;
                return Vec::new();
            }
        }
    }

    // =========================================================================
    // Phase 2: Query health for basic cluster status (Server B)
    // =========================================================================
    say!("\n  Phase 2: Query health snapshot (Server B)");

    // Server A floods its connections: its health is read on Server B's side.
    match query_health(&mut conns[1], 5).await {
        Some(h) => {
            let peer_count = h["cluster_peer_count"].as_u64();
            checks.check("cluster_connected == true", h["cluster_connected"] == true);
            checks.check(
                &format!("cluster_peer_count == 1 (got {peer_count:?})"),
                peer_count == Some(1),
            );
        }
        None => {
            checks.check("Health response from Server B", false);
        }
    }

    // =========================================================================
    // Phase 3: cluster_info on both servers, every peer
    // =========================================================================
    say!("\n  Phase 3: Query cluster_info on both servers");

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    let mut seen = Vec::with_capacity(2);
    for (name, ws) in ["Server A", "Server B"].into_iter().zip(conns.iter_mut()) {
        let Some(peers) = peers(ws).await else {
            checks.check(&format!("{name}: cluster_info answered"), false);
            continue;
        };
        checks.check(
            &format!("{name}: cluster_info lists 1 peer (got {})", peers.len()),
            peers.len() == 1,
        );
        for peer in &peers {
            let faults = peer_faults(peer, now_ms);
            checks.check(
                &format!(
                    "{name}: peer {} is whole ({})",
                    peer["address"].as_str().unwrap_or("?"),
                    if faults.is_empty() {
                        "address, instance_id, capabilities, connected_at".to_owned()
                    } else {
                        faults.join(", ")
                    }
                ),
                faults.is_empty(),
            );
        }
        if let [peer] = peers.as_slice() {
            seen.push(peer["instance_id"].as_str().unwrap_or("").to_owned());
        }
    }
    // Each names the other: two instances, not one seen twice.
    checks.check(
        &format!("The servers name different instances ({seen:?})"),
        seen.len() == 2 && seen[0] != seen[1] && !seen[0].is_empty(),
    );

    // =========================================================================
    // Clean up
    // =========================================================================
    say!("\n  Clean up");
    protocol::close_all(conns).await;

    checks.finish();

    Vec::new()
}
