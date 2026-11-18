pub mod battle_caps;
pub mod battle_cluster;
pub mod battle_compression;
pub mod battle_discovery;
pub mod battle_drain;
pub mod battle_encryption;
pub mod battle_load;
pub mod battle_metrics;
pub mod battle_presence;
pub mod battle_queue_groups;
pub mod battle_recovery;
pub mod battle_standalone;
pub mod battle_tls;
pub mod battle_topic_acl;
pub mod battle_topology;
pub mod connection_limit;
pub mod connection_storm;
pub mod fanout_broadcast;
pub mod fanout_cluster;
pub mod fanout_cluster_tls;
pub mod format_comparison;
pub mod ping_latency;
pub mod size_matrix;
pub mod sustained_hold;
pub mod throughput;

use futures_util::StreamExt;

use crate::protocol::{parse_wse_message, WsStream};

/// What one client got in a window of a battle test, by channel, and
/// whether its connection lasted the window: a check holds for every
/// client, never for a group's total alone.
#[derive(Clone, Copy, Default)]
pub(crate) struct Seen {
    pub lasted: bool,
    pub all: u64,
    pub battle_all: u64,
    pub battle_half: u64,
    pub battle_glob: u64,
    pub bytes: u64,
}

/// How many of `clients` hold for `holds`.
pub(crate) fn count(clients: &[Seen], holds: impl Fn(&Seen) -> bool) -> usize {
    clients.iter().filter(|seen| holds(seen)).count()
}

/// Receives until `deadline`, counting each channel's messages.
pub(crate) async fn receive_channels(
    mut ws: WsStream,
    deadline: tokio::time::Instant,
) -> (WsStream, Seen) {
    let mut seen = Seen {
        lasted: true,
        ..Seen::default()
    };
    let mut received = 0u64;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(msg))) => {
                seen.bytes += msg.len() as u64;
                received += 1;
                // A reader whose socket is never empty yields now and then,
                // so the others are read too: without it some clients of a
                // flood were never read at all.
                if received.is_multiple_of(64) {
                    tokio::task::yield_now().await;
                }
                if let Some(parsed) = parse_wse_message(&msg) {
                    let ch = parsed
                        .get("p")
                        .and_then(|p| p.get("ch"))
                        .and_then(|v| v.as_str());
                    match ch {
                        Some("all") => seen.all += 1,
                        Some("battle_all") => seen.battle_all += 1,
                        Some("battle_half") => seen.battle_half += 1,
                        Some("battle.glob_test") => seen.battle_glob += 1,
                        _ => {}
                    }
                }
            }
            Ok(Some(Err(_))) | Ok(None) => {
                seen.lasted = false;
                break;
            }
            Err(_) => break, // deadline reached
        }
    }
    (ws, seen)
}
