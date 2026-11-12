//! Topic glob matching, broadcast and topic fan-out, recovery stamping.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::AHashSet;
use bytes::Bytes;
use dashmap::DashMap;

use super::ConnectionHandle;

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pb = pattern.as_bytes();
    let tb = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);

    while ti < tb.len() {
        if pi < pb.len() && (pb[pi] == b'?' || pb[pi] == tb[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pb.len() && pb[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pb.len() && pb[pi] == b'*' {
        pi += 1;
    }
    pi == pb.len()
}

// ---------------------------------------------------------------------------
// Parallel fan-out helpers
// ---------------------------------------------------------------------------

/// NATS-style direct buffer fan-out for broadcast_all (BroadcastText/BroadcastBytes).
/// Appends pre-framed bytes to each connection's broadcast_buf under parking_lot::Mutex,
/// bypassing the mpsc channel entirely. Per-connection cost: ~15-20ns vs ~80-130ns for mpsc.
/// Only calls notify_one() when buffer transitions from empty to non-empty (skip when
/// write task is already awake draining data).
#[inline]
pub(super) fn fanout_broadcast_direct(
    conns: &HashMap<String, ConnectionHandle>,
    data: &Bytes,
    max_pending: usize,
    slow_drops: &AtomicU64,
) {
    let data_len = data.len();
    for h in conns.values() {
        // Skip dead connections (write task exited but cleanup hasn't run yet)
        if h.tx.is_closed() {
            continue;
        }
        if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        h.pending.fetch_add(data_len, Ordering::Relaxed);
        let was_empty = {
            let mut buf = h.broadcast_buf.lock();
            let empty = buf.is_empty();
            buf.push(data);
            empty
        };
        if was_empty {
            h.broadcast_notify.notify_one();
        }
    }
}

/// NATS-style direct buffer fan-out for topic-filtered connections (BroadcastLocal + cluster).
/// Iterates ConnectionHandle values stored directly in topic_subscribers DashMap,
/// eliminating the per-connection HashMap lookup that was the main bottleneck.
/// Returns (delivered, dropped) counts for metrics tracking.
#[inline]
pub(crate) fn fanout_topic_direct(
    topic_subscribers: &DashMap<String, DashMap<String, ConnectionHandle>>,
    topic: &str,
    data: &Bytes,
    max_pending: usize,
    slow_drops: &AtomicU64,
    glob_count: usize,
) -> (u64, u64) {
    let data_len = data.len();
    let mut delivered = 0u64;
    let mut dropped = 0u64;

    // Check if any glob patterns exist (rare -- most deployments use exact topics)
    let has_globs = glob_count > 0;

    if !has_globs {
        // Fast path: exact match only, iterate handles directly (no HashMap lookup)
        if let Some(subs) = topic_subscribers.get(topic) {
            for entry in subs.iter() {
                let h = entry.value();
                if h.tx.is_closed() {
                    continue;
                }
                if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                    slow_drops.fetch_add(1, Ordering::Relaxed);
                    dropped += 1;
                    continue;
                }
                h.pending.fetch_add(data_len, Ordering::Relaxed);
                let was_empty = {
                    let mut buf = h.broadcast_buf.lock();
                    let empty = buf.is_empty();
                    buf.push(data);
                    empty
                };
                if was_empty {
                    h.broadcast_notify.notify_one();
                }
                delivered += 1;
            }
        }
    } else {
        // Slow path: glob patterns exist, need AHashSet for dedup
        let mut seen = AHashSet::new();

        // Exact topic match
        if let Some(subs) = topic_subscribers.get(topic) {
            for entry in subs.iter() {
                if seen.insert(entry.key().clone()) {
                    let h = entry.value();
                    if h.tx.is_closed() {
                        continue;
                    }
                    if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                        slow_drops.fetch_add(1, Ordering::Relaxed);
                        dropped += 1;
                        continue;
                    }
                    h.pending.fetch_add(data_len, Ordering::Relaxed);
                    let was_empty = {
                        let mut buf = h.broadcast_buf.lock();
                        let empty = buf.is_empty();
                        buf.push(data);
                        empty
                    };
                    if was_empty {
                        h.broadcast_notify.notify_one();
                    }
                    delivered += 1;
                }
            }
        }

        // Glob pattern match
        for topic_entry in topic_subscribers.iter() {
            let pattern = topic_entry.key();
            if (pattern.contains('*') || pattern.contains('?')) && glob_match(pattern, topic) {
                for entry in topic_entry.value().iter() {
                    if seen.insert(entry.key().clone()) {
                        let h = entry.value();
                        if h.tx.is_closed() {
                            continue;
                        }
                        if max_pending > 0 && h.pending.load(Ordering::Relaxed) >= max_pending {
                            slow_drops.fetch_add(1, Ordering::Relaxed);
                            dropped += 1;
                            continue;
                        }
                        h.pending.fetch_add(data_len, Ordering::Relaxed);
                        let was_empty = {
                            let mut buf = h.broadcast_buf.lock();
                            let empty = buf.is_empty();
                            buf.push(data);
                            empty
                        };
                        if was_empty {
                            h.broadcast_notify.notify_one();
                        }
                        delivered += 1;
                    }
                }
            }
        }
    }

    (delivered, dropped)
}

/// Weave this message's recovery coordinates -- topic (`tp`), epoch (`e`, as an
/// 8-hex string) and offset (`o`) -- in as the first members of the JSON object,
/// right after the opening brace (the same position inject_category writes `c`).
/// Topic broadcasts are JSON objects produced by inject_category, so they always
/// start with `{`; anything else is returned unchanged. These fields let the
/// client dedup idempotently across reconnects within an epoch, detect gaps
/// (offset jumps), and request recovery from its last-seen (epoch, offset).
pub(crate) fn stamp_recovery_fields(json: &str, topic: &str, epoch: u32, offset: u64) -> String {
    let trimmed = json.trim_start();
    match trimmed.strip_prefix('{') {
        Some(rest) => {
            let sep = if rest.trim_start().starts_with('}') {
                ""
            } else {
                ","
            };
            let topic_json = serde_json::to_string(topic).unwrap_or_else(|_| "\"\"".to_string());
            format!("{{\"tp\":{topic_json},\"e\":\"{epoch:08x}\",\"o\":{offset}{sep}{rest}")
        }
        None => json.to_owned(),
    }
}
