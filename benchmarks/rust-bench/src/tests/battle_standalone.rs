use super::Checks;
use crate::config::Cli;
use crate::protocol::{self, WsStream};
use crate::report::TierResult;
use crate::stats;
use futures_util::SinkExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

const BATTLE_CLIENTS: usize = 100;
const SUBSCRIBE_SETTLE_MS: u64 = 500;

/// Per-channel delivery counters shared across connections in a group.
struct ChannelCounts {
    all: AtomicU64,         // broadcast_all
    battle_all: AtomicU64,  // topic "battle_all"
    battle_half: AtomicU64, // topic "battle_half"
    battle_glob: AtomicU64, // topic "battle.glob_test" (via "battle.*" glob)
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

/// Battle Test: Standalone Feature Verification
///
/// Tests broadcast_all, topic delivery, topic isolation, glob pattern matching,
/// and disconnect handling. Connects 100 clients, subscribes them to various
/// topics, receives messages, and verifies correctness.
///
/// Requires: python benchmarks/bench_battle_server.py
pub async fn run(cli: &Cli) -> Vec<TierResult> {
    stats::print_header(
        "BATTLE TEST: Standalone Feature Verification",
        "Tests broadcast_all, topic delivery, topic isolation, glob patterns, disconnect handling.",
    );

    let token = crate::jwt::generate_bench_token(cli.secret.as_bytes(), "bench-user");
    let duration = Duration::from_secs(cli.duration);
    let mut checks = Checks::default();

    // =========================================================================
    // Phase 1: Connect
    // =========================================================================
    say!("\n  Phase 1: Connect {} clients", BATTLE_CLIENTS);

    let mut connections = protocol::connect_batch(
        &cli.host,
        cli.port,
        &token,
        BATTLE_CLIENTS,
        cli.batch_size,
        "compression=false&protocol_version=1",
    )
    .await;

    checks.check(
        &format!("Connected {}/{} clients", connections.len(), BATTLE_CLIENTS),
        connections.len() == BATTLE_CLIENTS,
    );

    if connections.len() < BATTLE_CLIENTS / 2 {
        fail!("    ABORT: too few connections, cannot continue");
        protocol::close_all(connections).await;
        return Vec::new();
    }

    let actual = connections.len();
    let half = actual / 2;

    // =========================================================================
    // Phase 2: Subscribe to topics
    // =========================================================================
    say!("\n  Phase 2: Subscribe to topics");

    // All clients subscribe to "battle_all"
    for ws in connections.iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_all"]}
        });
        let _ = ws.send(Message::Text(cmd.to_string().into())).await;
    }
    say!("    All {} subscribed to 'battle_all'", connections.len());

    // First half also subscribes to "battle_half" and "battle.*" (glob)
    for ws in connections[..half].iter_mut() {
        let cmd = serde_json::json!({
            "t": "battle_cmd",
            "p": {"action": "subscribe", "topics": ["battle_half", "battle.*"]}
        });
        let _ = ws.send(Message::Text(cmd.to_string().into())).await;
    }
    say!(
        "    First {} also subscribed to 'battle_half' + 'battle.*'",
        half
    );

    // Let subscriptions propagate
    tokio::time::sleep(Duration::from_millis(SUBSCRIBE_SETTLE_MS)).await;

    // Warmup: drain any messages that arrived during subscription setup
    tokio::time::sleep(Duration::from_secs(2)).await;

    // =========================================================================
    // Phase 3: Receive and measure
    // =========================================================================
    say!(
        "\n  Phase 3: Receive for {}s, verify delivery",
        duration.as_secs()
    );

    // Split connections into two groups
    let group2_conns: Vec<WsStream> = connections.drain(half..).collect();
    let group1_conns: Vec<WsStream> = connections;

    let g1_counts = Arc::new(ChannelCounts::new());
    let g2_counts = Arc::new(ChannelCounts::new());

    let deadline = tokio::time::Instant::now() + duration;

    // Spawn receive tasks for Group 1 (subscribed to battle_all + battle_half + battle.*)
    let mut g1_handles = Vec::with_capacity(group1_conns.len());
    for ws in group1_conns {
        let counts = g1_counts.clone();
        g1_handles.push(tokio::spawn(battle_receive_loop(ws, deadline, counts)));
    }

    // Spawn receive tasks for Group 2 (subscribed to battle_all only)
    let mut g2_handles = Vec::with_capacity(group2_conns.len());
    for ws in group2_conns {
        let counts = g2_counts.clone();
        g2_handles.push(tokio::spawn(battle_receive_loop(ws, deadline, counts)));
    }

    // Wait for all receive tasks to finish
    // A client whose task died lasted nothing and got nothing.
    let (g1_streams, g1_seen): (Vec<WsStream>, Vec<super::Seen>) =
        futures_util::future::join_all(g1_handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .unzip();
    let (g2_streams, g2_seen): (Vec<WsStream>, Vec<super::Seen>) =
        futures_util::future::join_all(g2_handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .unzip();
    let g1_size = half;
    let g2_size = actual - half;
    let every = |seen: &[super::Seen], size: usize, holds: fn(&super::Seen) -> bool| {
        super::count(seen, holds) == size
    };

    // =========================================================================
    // Phase 4: Verify correctness
    // =========================================================================
    say!("\n  Phase 4: Verify correctness");

    let g1_all = g1_counts.all.load(Ordering::Relaxed);
    let g1_ball = g1_counts.battle_all.load(Ordering::Relaxed);
    let g1_bhalf = g1_counts.battle_half.load(Ordering::Relaxed);
    let g1_bglob = g1_counts.battle_glob.load(Ordering::Relaxed);

    let g2_all = g2_counts.all.load(Ordering::Relaxed);
    let g2_ball = g2_counts.battle_all.load(Ordering::Relaxed);
    let g2_bhalf = g2_counts.battle_half.load(Ordering::Relaxed);
    let g2_bglob = g2_counts.battle_glob.load(Ordering::Relaxed);

    say!(
        "    Group 1 ({} clients, all topics): all={}, battle_all={}, battle_half={}, glob={}",
        half,
        g1_all,
        g1_ball,
        g1_bhalf,
        g1_bglob
    );
    say!(
        "    Group 2 ({} clients, battle_all only): all={}, battle_all={}, battle_half={}, glob={}",
        actual - half,
        g2_all,
        g2_ball,
        g2_bhalf,
        g2_bglob
    );

    // broadcast_all delivery
    // Every client lasted the window: isolation on a dead socket holds for nothing.
    checks.check(
        "Group 1: every client lasted the window",
        every(&g1_seen, g1_size, |s| s.lasted),
    );
    checks.check(
        "Group 2: every client lasted the window",
        every(&g2_seen, g2_size, |s| s.lasted),
    );
    checks.check(
        "Group 1: every client received broadcast_all",
        every(&g1_seen, g1_size, |s| s.all > 0),
    );
    checks.check(
        "Group 2: every client received broadcast_all",
        every(&g2_seen, g2_size, |s| s.all > 0),
    );

    // Topic delivery
    checks.check(
        "Group 1: every client received 'battle_all' topic",
        every(&g1_seen, g1_size, |s| s.battle_all > 0),
    );
    checks.check(
        "Group 2: every client received 'battle_all' topic",
        every(&g2_seen, g2_size, |s| s.battle_all > 0),
    );

    // Topic isolation -- battle_half
    checks.check(
        "Group 1: every client received 'battle_half' topic",
        every(&g1_seen, g1_size, |s| s.battle_half > 0),
    );
    checks.check("Group 2 isolated from 'battle_half'", g2_bhalf == 0);

    // Glob pattern matching -- battle.* matches battle.glob_test
    checks.check(
        "Group 1: every client received 'battle.glob_test' via glob 'battle.*'",
        every(&g1_seen, g1_size, |s| s.battle_glob > 0),
    );
    checks.check("Group 2 isolated from 'battle.glob_test'", g2_bglob == 0);

    // Throughput
    let total = g1_counts.total_messages() + g2_counts.total_messages();
    let total_bytes = g1_counts.total_bytes.load(Ordering::Relaxed)
        + g2_counts.total_bytes.load(Ordering::Relaxed);
    let del_per_sec = total as f64 / duration.as_secs_f64();
    say!(
        "\n    Total deliveries: {} ({}/s)",
        total,
        stats::fmt_rate(del_per_sec)
    );
    say!(
        "    Bandwidth: {}",
        stats::fmt_bytes_per_sec(total_bytes as f64 / duration.as_secs_f64())
    );

    // =========================================================================
    // Phase 5: Disconnect handling
    // =========================================================================
    say!("\n  Phase 5: Disconnect handling");

    // Close Group 1
    protocol::close_all(g1_streams).await;
    say!("    Dropped {} connections (Group 1)", half);

    // Reset counters and verify Group 2 still receives for 3s
    let g2_post = Arc::new(ChannelCounts::new());
    let post_deadline = tokio::time::Instant::now() + Duration::from_secs(3);

    let mut post_handles = Vec::new();
    for ws in g2_streams {
        let counts = g2_post.clone();
        post_handles.push(tokio::spawn(battle_receive_loop(ws, post_deadline, counts)));
    }
    let (remaining, post_seen): (Vec<WsStream>, Vec<super::Seen>) =
        futures_util::future::join_all(post_handles)
            .await
            .into_iter()
            .filter_map(|r| r.ok())
            .unzip();

    let post_all = g2_post.all.load(Ordering::Relaxed);
    let post_ball = g2_post.battle_all.load(Ordering::Relaxed);
    checks.check(
        "Every remaining client receives after peer disconnect",
        every(&post_seen, g2_size, |s| {
            s.lasted && s.all + s.battle_all > 0
        }),
    );
    say!(
        "    Remaining group (3s): all={}, battle_all={}",
        post_all,
        post_ball
    );

    // Cleanup
    protocol::close_all(remaining).await;

    // =========================================================================
    // Summary
    // =========================================================================
    checks.finish();

    Vec::new()
}

/// Receive messages until deadline, categorize by channel, accumulate counts:
/// what each client got, and the group's totals.
async fn battle_receive_loop(
    ws: WsStream,
    deadline: tokio::time::Instant,
    counts: Arc<ChannelCounts>,
) -> (WsStream, super::Seen) {
    let (ws, seen) = super::receive_channels(ws, deadline).await;
    counts.all.fetch_add(seen.all, Ordering::Relaxed);
    counts
        .battle_all
        .fetch_add(seen.battle_all, Ordering::Relaxed);
    counts
        .battle_half
        .fetch_add(seen.battle_half, Ordering::Relaxed);
    counts
        .battle_glob
        .fetch_add(seen.battle_glob, Ordering::Relaxed);
    counts.total_bytes.fetch_add(seen.bytes, Ordering::Relaxed);
    (ws, seen)
}
