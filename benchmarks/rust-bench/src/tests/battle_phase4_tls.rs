use crate::config::Cli;
use crate::protocol::{self, parse_wse_message, query_health, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const BATTLE_CLIENTS: usize = 100;
const SUBSCRIBE_SETTLE_MS: u64 = 2000;

/// Per-channel delivery counters.
struct ChannelCounts {
    all: AtomicU64,
    battle_all: AtomicU64,
    battle_half: AtomicU64,
    battle_glob: AtomicU64,
    total_bytes: AtomicU64,
}

impl ChannelCounts {
    fn new() -> Self {
        Self {
            all: AtomicU64::new(0),
            battle_all: AtomicU64::new(0),
            battle_half: AtomicU64::new(0),
            battle_glob: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }

    fn total_messages(&self) -> u64 {
        self.all.load(Ordering::Relaxed)
            + self.battle_all.load(Ordering::Relaxed)
            + self.battle_half.load(Ordering::Relaxed)
            + self.battle_glob.load(Ordering::Relaxed)
    }
}

struct CheckResult {
    passed: u32,
    failed: u32,
}

impl CheckResult {
    fn new() -> Self {
        Self {
            passed: 0,
            failed: 0,
        }
    }

    fn check(&mut self, name: &str, condition: bool) {
        if condition {
            self.passed += 1;
            println!("    [PASS] {}", name);
        } else {
            self.failed += 1;
            println!("    [FAIL] {}", name);
        }
    }
}

/// Battle Test: Phase 4 -- TLS Cluster Verification
///
/// Same as battle_cluster (topic routing, isolation, glob patterns) but
/// with mTLS between cluster peers. Proves the cluster protocol works
/// identically over encrypted connections.
///
/// Note: Server A (publisher) floods broadcast_all at 500K/s to all connections,
/// making health queries unreliable on that side. All health checks use Server B.
///
/// Requires: 2 cluster servers with TLS and separate cluster ports
///   python benchmarks/bench_battle_server.py --mode cluster --port 5006 --peers 127.0.0.1:6007 \
///       --cluster-port 6006 --cluster-addr 127.0.0.1:6006 --generate-tls
///   python benchmarks/bench_battle_server.py --mode cluster-subscribe --port 5007 --peers 127.0.0.1:6006 \
///       --cluster-port 6007 --cluster-addr 127.0.0.1:6007 \
///       --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    let port2 = cli.port2.unwrap_or(cli.port + 1);

    stats::print_header(
        "BATTLE TEST: Phase 4 -- TLS Cluster Verification",
        &format!(
            "Verify topic routing, isolation, glob patterns over mTLS cluster (:{} -> :{}).",
            cli.port, port2
        ),
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = Duration::from_secs(cli.duration);
    let mut checks = CheckResult::new();

    // Verify connectivity to Server B
    print!("  Checking Server B (:{})... ", port2);
    match protocol::connect_and_handshake(
        &cli.host,
        port2,
        &token,
        "compression=false&protocol_version=1",
        10,
    )
    .await
    {
        Ok(ws) => {
            protocol::close_all(vec![ws]).await;
            println!("OK");
        }
        Err(e) => {
            eprintln!("FAILED: {e}");
            eprintln!(
                "\n  Start servers with TLS (separate cluster ports):\n    \
                python benchmarks/bench_battle_server.py --mode cluster --port {} --peers 127.0.0.1:6007 \
                --cluster-port 6006 --cluster-addr 127.0.0.1:6006 --generate-tls\n    \
                python benchmarks/bench_battle_server.py --mode cluster-subscribe --port {} --peers 127.0.0.1:6006 \
                --cluster-port 6007 --cluster-addr 127.0.0.1:6007 \
                --tls-cert /tmp/wse_tls_test/server.crt --tls-key /tmp/wse_tls_test/server.key --tls-ca /tmp/wse_tls_test/ca.crt",
                cli.port, port2
            );
            return Vec::new();
        }
    }

    // =========================================================================
    // Phase 1: Verify TLS cluster health (query Server B -- not flooded)
    // =========================================================================
    println!("\n  Phase 1: Verify TLS cluster health (Server B)");
    println!("    (Server A health skipped -- publisher floods 500K/s to all connections)");

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
            eprintln!("    Failed to connect to Server B for health -- {}", e);
            return Vec::new();
        }
    };

    tokio::time::sleep(Duration::from_secs(2)).await;

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
        checks.check("TLS cluster connected (Server B)", connected);
        checks.check(
            &format!("No unknown message types over TLS (got {})", unknown),
            unknown == 0,
        );
    } else {
        println!("    WARNING: No health response from Server B");
        checks.check("TLS cluster connected (Server B)", false);
        checks.check("No unknown message types over TLS", false);
    }

    protocol::close_all(vec![ws_health]).await;

    // =========================================================================
    // Phase 2: Connect clients to Server B and subscribe
    // =========================================================================
    println!(
        "\n  Phase 2: Connect {} clients to Server B (:{})",
        BATTLE_CLIENTS, port2
    );

    let mut connections = protocol::connect_batch(
        &cli.host,
        port2,
        &token,
        BATTLE_CLIENTS,
        cli.batch_size,
        "compression=false&protocol_version=1",
    )
    .await;

    checks.check(
        &format!(
            "Connected {}/{} to Server B",
            connections.len(),
            BATTLE_CLIENTS
        ),
        connections.len() == BATTLE_CLIENTS,
    );

    if connections.len() < BATTLE_CLIENTS / 2 {
        println!(
            "    ABORT: too few connections ({}/{})",
            connections.len(),
            BATTLE_CLIENTS
        );
        protocol::close_all(connections).await;
        return Vec::new();
    }

    let actual = connections.len();
    let half = actual / 2;

    // =========================================================================
    // Phase 3: Subscribe to topics on Server B
    // =========================================================================
    println!("\n  Phase 3: Subscribe to topics on Server B");

    // All subscribe to "battle_all"
    for ws in connections.iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_all"]}
        });
        let _ = ws.send(Message::Text(cmd.to_string().into())).await;
    }
    println!("    All {} subscribed to 'battle_all'", actual);

    // First half also subscribes to "battle_half" + "battle.*"
    for ws in connections[..half].iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_half", "battle.*"]}
        });
        let _ = ws.send(Message::Text(cmd.to_string().into())).await;
    }
    println!(
        "    First {} also subscribed to 'battle_half' + 'battle.*'",
        half
    );

    // Settle
    println!(
        "    Waiting {}ms for cluster subscription propagation...",
        SUBSCRIBE_SETTLE_MS
    );
    tokio::time::sleep(Duration::from_millis(SUBSCRIBE_SETTLE_MS)).await;

    // Warmup
    tokio::time::sleep(Duration::from_secs(2)).await;

    // =========================================================================
    // Phase 4: Receive and measure
    // =========================================================================
    println!(
        "\n  Phase 4: Receive for {}s over TLS cluster",
        duration.as_secs()
    );

    let group2_conns: Vec<WsStream> = connections.drain(half..).collect();
    let group1_conns: Vec<WsStream> = connections;

    let g1_counts = Arc::new(ChannelCounts::new());
    let g2_counts = Arc::new(ChannelCounts::new());

    let deadline = tokio::time::Instant::now() + duration;

    let mut g1_handles = Vec::with_capacity(group1_conns.len());
    for ws in group1_conns {
        let counts = g1_counts.clone();
        g1_handles.push(tokio::spawn(battle_receive_loop(ws, deadline, counts)));
    }

    let mut g2_handles = Vec::with_capacity(group2_conns.len());
    for ws in group2_conns {
        let counts = g2_counts.clone();
        g2_handles.push(tokio::spawn(battle_receive_loop(ws, deadline, counts)));
    }

    let g1_streams: Vec<WsStream> = futures_util::future::join_all(g1_handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    let g2_streams: Vec<WsStream> = futures_util::future::join_all(g2_handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    // =========================================================================
    // Phase 5: Verify cross-instance delivery over TLS
    // =========================================================================
    println!("\n  Phase 5: Verify cross-instance delivery over TLS");

    let g1_all = g1_counts.all.load(Ordering::Relaxed);
    let g1_ball = g1_counts.battle_all.load(Ordering::Relaxed);
    let g1_bhalf = g1_counts.battle_half.load(Ordering::Relaxed);
    let g1_bglob = g1_counts.battle_glob.load(Ordering::Relaxed);

    let g2_all = g2_counts.all.load(Ordering::Relaxed);
    let g2_ball = g2_counts.battle_all.load(Ordering::Relaxed);
    let g2_bhalf = g2_counts.battle_half.load(Ordering::Relaxed);
    let g2_bglob = g2_counts.battle_glob.load(Ordering::Relaxed);

    println!(
        "    Group 1 ({} clients, all topics): all={}, battle_all={}, battle_half={}, glob={}",
        half, g1_all, g1_ball, g1_bhalf, g1_bglob
    );
    println!(
        "    Group 2 ({} clients, battle_all only): all={}, battle_all={}, battle_half={}, glob={}",
        actual - half, g2_all, g2_ball, g2_bhalf, g2_bglob
    );

    checks.check(
        "Group 1 received 'battle_all' via TLS cluster",
        g1_ball > 0,
    );
    checks.check(
        "Group 2 received 'battle_all' via TLS cluster",
        g2_ball > 0,
    );
    checks.check(
        "Group 1 received 'battle_half' via TLS cluster",
        g1_bhalf > 0,
    );
    checks.check(
        "Group 2 isolated from 'battle_half' (TLS cluster)",
        g2_bhalf == 0,
    );
    checks.check(
        "Group 1 received glob via TLS cluster",
        g1_bglob > 0,
    );
    checks.check(
        "Group 2 isolated from glob (TLS cluster)",
        g2_bglob == 0,
    );

    // Throughput
    let total = g1_counts.total_messages() + g2_counts.total_messages();
    let total_bytes = g1_counts.total_bytes.load(Ordering::Relaxed)
        + g2_counts.total_bytes.load(Ordering::Relaxed);
    let del_per_sec = total as f64 / duration.as_secs_f64();
    println!(
        "\n    Total deliveries: {} ({}/s)",
        total,
        stats::fmt_rate(del_per_sec)
    );
    println!(
        "    Bandwidth: {}",
        stats::fmt_bytes_per_sec(total_bytes as f64 / duration.as_secs_f64())
    );

    // Cleanup
    protocol::close_all(g1_streams).await;
    protocol::close_all(g2_streams).await;

    // =========================================================================
    // Summary
    // =========================================================================
    println!(
        "\n  Result: {}/{} checks passed",
        checks.passed,
        checks.passed + checks.failed
    );
    if checks.failed > 0 {
        println!("  FAILED");
    } else {
        println!("  PASSED");
    }

    Vec::new()
}

async fn battle_receive_loop(
    mut ws: WsStream,
    deadline: tokio::time::Instant,
    counts: Arc<ChannelCounts>,
) -> WsStream {
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                counts
                    .total_bytes
                    .fetch_add(msg.len() as u64, Ordering::Relaxed);

                if let Some(parsed) = parse_wse_message(&msg) {
                    if let Some(ch) = parsed
                        .get("p")
                        .and_then(|p| p.get("ch"))
                        .and_then(|v| v.as_str())
                    {
                        match ch {
                            "all" => {
                                counts.all.fetch_add(1, Ordering::Relaxed);
                            }
                            "battle_all" => {
                                counts.battle_all.fetch_add(1, Ordering::Relaxed);
                            }
                            "battle_half" => {
                                counts.battle_half.fetch_add(1, Ordering::Relaxed);
                            }
                            "battle.glob_test" => {
                                counts.battle_glob.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => break,
        }
    }
    ws
}
