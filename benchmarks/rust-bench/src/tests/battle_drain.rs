use super::{collect, reply, Checks};
use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::protocol::Message;

const CLIENT_COUNT: usize = 10;

/// Wait for a WebSocket Close frame and return the close code.
async fn wait_for_close(ws: &mut WsStream, deadline_ms: u64) -> Option<u16> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(deadline_ms);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Some(frame.map(|f| f.code.into()).unwrap_or(0));
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return None,
        }
    }
}

/// How a new connection to a draining server ended: refused as the
/// server refuses while it drains (503 to the upgrade, or 1013 "try again
/// later" when the upgrade got in first), or anything else, said.
async fn refused_while_draining(cli: &Cli, token: &str) -> Result<String, String> {
    let request = http::Request::builder()
        .uri(format!(
            "ws://{}:{}/wse?protocol_version=1",
            cli.host, cli.port
        ))
        .header("Host", format!("{}:{}", cli.host, cli.port))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", generate_key())
        .header("Cookie", format!("access_token={token}"))
        .body(())
        .map_err(|e| e.to_string())?;
    let connected = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(request),
    )
    .await
    .map_err(|_| "no answer to the upgrade".to_owned())?;
    let mut ws = match connected {
        Ok((ws, _)) => ws,
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            return if response.status() == http::StatusCode::SERVICE_UNAVAILABLE {
                Ok("503 to the upgrade".to_owned())
            } else {
                Err(format!("HTTP {} to the upgrade", response.status()))
            };
        }
        Err(e) => return Err(format!("connect failed: {e}")),
    };
    loop {
        match tokio::time::timeout(Duration::from_secs(3), ws.next()).await {
            Ok(Some(Ok(Message::Close(Some(frame))))) if u16::from(frame.code) == 1013 => {
                return Ok("closed with 1013".to_owned());
            }
            Ok(Some(Ok(Message::Close(frame)))) => {
                return Err(format!("closed with {:?}", frame.map(|f| f.code)));
            }
            Ok(Some(Ok(msg))) => {
                if parse_wse_message(&msg).is_some_and(|p| p["t"] == "server_ready") {
                    return Err("let in: server_ready".to_owned());
                }
            }
            Ok(Some(Err(e))) => return Err(format!("read failed: {e}")),
            Ok(None) => return Err("ended without a close frame".to_owned()),
            Err(_) => return Err("let in, and nothing came".to_owned()),
        }
    }
}

/// Battle Test: Graceful Drain
///
/// Verifies that server.drain() sends Close frames with code 4300 to all
/// connected clients and rejects new connections after drain starts.
///
/// Uses --no-publish mode to avoid broadcast flood interfering with drain ack.
///
/// WARNING: This test is destructive -- it drains and shuts down the server.
///
/// Requires: python benchmarks/bench_battle_server.py --mode standalone --no-publish
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Graceful Drain",
        "Tests drain() sends Close(4300) to all clients and rejects new connections.",
    );

    let mut checks = Checks::default();
    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");

    // =========================================================================
    // Phase 1: Connect clients and subscribe
    // =========================================================================
    say!("\n  Phase 1: Connect {} clients", CLIENT_COUNT);

    let mut clients: Vec<WsStream> = Vec::new();
    for i in 0..CLIENT_COUNT {
        let user_id = format!("drain-user-{}", i);
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
                fail!("    [FAIL] Failed to connect client {}: {}", i, e);
            }
        }
    }
    checks.check(
        &format!("Connected {}/{} clients", clients.len(), CLIENT_COUNT),
        clients.len() == CLIENT_COUNT,
    );

    // Subscribe all to battle_all
    for client in clients.iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_all"]}
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
        &format!(
            "The server took all {} subscriptions (answered {settled})",
            clients.len()
        ),
        settled == clients.len(),
    );
    if clients.is_empty() {
        checks.finish();
        return Vec::new();
    }

    // =========================================================================
    // Phase 2: Verify server is handling messages
    // =========================================================================
    say!("\n  Phase 2: Verify server accepts messages");

    // Publish a few messages to confirm the server is alive
    let pub_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "publish_messages",
            "topic": "battle_all",
            "count": 10
        }
    });
    clients[0]
        .send(Message::Text(pub_cmd.to_string().into()))
        .await
        .ok();
    // Every client gets all 10 (the publish_ack is passed over).
    let mut whole = 0;
    for client in clients.iter_mut() {
        whole += usize::from(collect(client, (10, 0), Duration::from_secs(5)).await == (10, 0));
    }
    checks.check(
        &format!(
            "Server accepting messages: {whole}/{} clients got all 10",
            clients.len()
        ),
        whole == clients.len(),
    );

    // =========================================================================
    // Phase 3: Trigger drain via battle_cmd
    // =========================================================================
    say!("\n  Phase 3: Trigger graceful drain (0.5s delay)");

    let drain_cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {
            "action": "start_drain",
            "delay": 0.5
        }
    });
    clients[0]
        .send(Message::Text(drain_cmd.to_string().into()))
        .await
        .ok();

    let drain_ack = reply(&mut clients[0], "drain_started", Duration::from_secs(3)).await;
    checks.check("Drain started acknowledgment received", drain_ack.is_some());

    // =========================================================================
    // Phase 4: Wait for Close frames on all clients
    // =========================================================================
    say!("\n  Phase 4: Wait for Close frames (10s deadline)");

    let mut close_codes: Vec<Option<u16>> = Vec::new();
    let mut handles = Vec::new();
    for mut client in clients.into_iter() {
        handles.push(tokio::spawn(async move {
            let code = wait_for_close(&mut client, 10000).await;
            (code, client)
        }));
    }

    let mut clients_back = Vec::new();
    for h in handles {
        let (code, ws) = h.await.unwrap();
        close_codes.push(code);
        clients_back.push(ws);
    }

    let received_close = close_codes.iter().filter(|c| c.is_some()).count();
    let correct_code = close_codes.iter().filter(|c| **c == Some(4300)).count();

    say!(
        "    {}/{} received Close frame, {}/{} had code 4300",
        received_close,
        CLIENT_COUNT,
        correct_code,
        CLIENT_COUNT
    );

    checks.check(
        &format!(
            "All clients received Close frame ({}/{})",
            received_close, CLIENT_COUNT
        ),
        received_close == CLIENT_COUNT,
    );
    checks.check(
        &format!("Close code is 4300 ({}/{})", correct_code, CLIENT_COUNT),
        correct_code == CLIENT_COUNT,
    );

    // =========================================================================
    // Phase 5: Verify new connections are rejected
    // =========================================================================
    say!("\n  Phase 5: Verify new connections rejected after drain");

    match refused_while_draining(cli, &token).await {
        Ok(how) => checks.check(&format!("New connection refused: {how}"), true),
        Err(how) => checks.check(&format!("New connection refused (got: {how})"), false),
    };

    // =========================================================================
    // Clean up
    // =========================================================================
    say!("\n  Clean up");
    protocol::close_all(clients_back).await;

    checks.finish();

    Vec::new()
}
