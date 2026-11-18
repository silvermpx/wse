use super::{next_before, Checks};
use crate::config::Cli;
use crate::protocol::{self, query_health};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

/// What Server B has taken off the cluster link: the wire bytes, and the
/// messages they carried as its one subscriber took them, delivered or
/// dropped as too slow. What Server B publishes itself counts in neither.
/// Read from its metrics endpoint: a WebSocket of a node that publishes
/// itself is flooded.
async fn link_in(cli: &Cli, port: u16) -> Option<(u64, u64)> {
    let metrics = protocol::metrics_text(&cli.host, cli.metrics_port_for(port)).await?;
    let bytes = protocol::counter(&metrics, "wse_cluster_bytes_received_total")?;
    let messages = protocol::counter(&metrics, "wse_cluster_messages_delivered_total")?
        + protocol::counter(&metrics, "wse_cluster_messages_dropped_total")?;
    Some((bytes, messages))
}

/// The port of the server that published a battle message, as the battle
/// server names it (`"from":<port>`); read without parsing the whole
/// message, as the subscriber takes a flood.
fn origin(msg: &Message) -> Option<u16> {
    let Message::Text(text) = msg else {
        return None;
    };
    let at = text.find("\"from\":")? + 7;
    let digits = text[at..].bytes().take_while(u8::is_ascii_digit).count();
    text[at..at + digits].parse().ok()
}

/// Battle Test: Inter-Peer zstd Compression
///
/// Measures what the cluster link carries per message on Server B: the wire
/// bytes it took off the link over the messages that came in on it, against
/// the size of Server A's messages as Server B's subscriber gets them. Each
/// message names the server that published it, so one published on Server B
/// itself is never taken for one that crossed the link. The 1 KiB messages
/// of `--message-size 1024` are padded, so zstd takes them to a fraction of
/// their size; uncompressed, a message costs more than itself.
///
/// Server B's counters come from its metrics endpoint (port + 1000, as the
/// battle server serves it, or --metrics-port); its subscriber counts on a
/// task of its own.
///
/// Requires: 2 cluster servers with --message-size 1024
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:5007 --message-size 1024
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:5006
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2();

    stats::print_header(
        "BATTLE TEST: Inter-Peer Compression",
        &format!(
            "Verify zstd compression on large cluster messages (:{} -> :{}).",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = Duration::from_secs(cli.duration.max(1));
    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect health + subscriber connections to Server B
    // =========================================================================
    say!(
        "\n  Phase 1: Connect to Server B (:{}) and verify cluster",
        port2
    );

    // Health-only connection (never subscribes, stays clean for queries)
    let mut ws_health = match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            fail_err!("    Failed to connect health conn to Server B -- {}", e);
            return Vec::new();
        }
    };

    // Subscriber connection (triggers RESYNC, will get flooded)
    let token2 = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-sub");
    let mut ws_sub = match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token2,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => ws,
        Err(e) => {
            fail_err!("    Failed to connect subscriber to Server B -- {}", e);
            protocol::close_all(vec![ws_health]).await;
            return Vec::new();
        }
    };
    say!("    Connected 2 connections to Server B (:{})", port2);

    let health_b = query_health(&mut ws_health, 5).await;
    if let Some(ref h) = health_b {
        let connected = h
            .get("cluster_connected")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let unknown = h
            .get("cluster_unknown_message_types")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);
        checks.check("Cluster connected (Server B)", connected);
        checks.check(
            &format!("No unknown message types (got {})", unknown),
            unknown == 0,
        );
    } else {
        say!("    WARNING: No health response from Server B");
        checks.check("Cluster connected (Server B)", false);
    }
    // The counters come from the metrics endpoint from here on.
    protocol::close_all(vec![ws_health]).await;

    // =========================================================================
    // Phase 2: Subscribe, and wait for Server A's messages to arrive
    // =========================================================================
    say!("\n  Phase 2: Subscribe and wait for messages from Server A");

    let cmd = serde_json::json!({
        "t": "battle_cmd",
        "p": {"action": "subscribe", "topics": ["battle_all"]}
    });
    let _ = ws_sub.send(Message::Text(cmd.to_string().into())).await;

    // Server A forwards battle_all once it hears of the subscription.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut flowing = false;
    while let Some(msg) = next_before(&mut ws_sub, deadline).await {
        if origin(&msg) == Some(cli.port) {
            flowing = true;
            break;
        }
    }
    let before = if flowing {
        link_in(cli, port2).await
    } else {
        None
    };
    if !checks.check(
        &format!(
            "Messages from Server A (:{}) arrive, counters read ({before:?})",
            cli.port
        ),
        before.is_some(),
    ) {
        protocol::close_all(vec![ws_sub]).await;
        checks.finish();
        return Vec::new();
    }
    let (bytes_before, messages_before) = before.unwrap_or_default();

    // =========================================================================
    // Phase 3: Count messages on subscriber for duration
    // =========================================================================
    say!("\n  Phase 3: Count messages for {}s", duration.as_secs());

    // The subscriber is read on its own task: its flood never waits for the
    // counter queries. Only Server A's messages are measured.
    let from_a = cli.port;
    let count_handle = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + duration;
        let (mut received, mut size, mut elsewhere) = (0u64, 0u64, 0u64);
        while let Some(msg) = next_before(&mut ws_sub, deadline).await {
            if origin(&msg) == Some(from_a) {
                received += 1;
                size += msg.len() as u64;
            } else {
                elsewhere += 1;
            }
        }
        (ws_sub, received, size, elsewhere)
    });
    let (ws_sub, received, size, elsewhere) = match count_handle.await {
        Ok(counted) => counted,
        Err(e) => {
            fail!("    [FAIL] The subscriber's reader failed: {}", e);
            checks.finish();
            return Vec::new();
        }
    };
    say!(
        "    Subscriber received {} messages from Server A, {} from elsewhere",
        received,
        elsewhere
    );

    // =========================================================================
    // Phase 4: Verify compression on the wire
    // =========================================================================
    say!("\n  Phase 4: Verify compression on the wire");

    let after = link_in(cli, port2).await;
    checks.check(&format!("Counters read again ({after:?})"), after.is_some());
    let (bytes_after, messages_after) = after.unwrap_or((bytes_before, messages_before));
    let wire = bytes_after.saturating_sub(bytes_before);
    let carried = messages_after.saturating_sub(messages_before);
    let message_size = size.checked_div(received).unwrap_or(0);
    let per_message = wire.checked_div(carried).unwrap_or(u64::MAX);
    say!(
        "    {} wire bytes for {} messages over the link: {} per message",
        wire,
        carried,
        per_message
    );

    checks.check(
        &format!("The link carried messages (got {carried} in {wire} bytes)"),
        carried > 0 && wire > 0,
    );
    checks.check(
        &format!("Subscriber received Server A's messages (got {received})"),
        received > 0,
    );
    // The measure holds only for messages zstd has something to take off.
    checks.check(
        &format!("Server A's messages are the 1 KiB ones (got {message_size} bytes each)"),
        message_size >= 1000,
    );
    checks.check(
        &format!(
            "Compressed on the wire: {per_message} bytes per {message_size}-byte message (want under half)"
        ),
        per_message.saturating_mul(2) < message_size,
    );

    // Cleanup
    protocol::close_all(vec![ws_sub]).await;

    checks.finish();

    Vec::new()
}
