use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::reliability::{CircuitBreaker, ExponentialBackoff};
use super::server::ConnectionHandle;
use dashmap::{DashMap, DashSet};
use futures_util::StreamExt;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

pub(crate) enum RedisCommand {
    Publish { channel: String, payload: String },
    Shutdown,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

pub(crate) struct RedisMetrics {
    pub messages_received: AtomicU64,
    pub messages_published: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub publish_errors: AtomicU64,
    pub reconnect_count: AtomicU64,
    pub connected: AtomicBool,
}

impl RedisMetrics {
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_published: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            publish_errors: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            connected: AtomicBool::new(false),
        }
    }
}

impl super::server::FanoutMetrics for RedisMetrics {
    fn add_delivered(&self, count: u64) {
        self.messages_delivered.fetch_add(count, Ordering::Relaxed);
    }
    fn add_dropped(&self, count: u64) {
        self.messages_dropped.fetch_add(count, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Dead Letter Queue
// ---------------------------------------------------------------------------

pub(crate) struct DlqEntry {
    pub channel: String,
    pub payload: String,
    pub error: String,
}

pub(crate) struct DeadLetterQueue {
    entries: VecDeque<DlqEntry>,
    max_entries: usize,
}

impl DeadLetterQueue {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries,
        }
    }

    fn push(&mut self, entry: DlqEntry) {
        if self.entries.len() >= self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn drain_all(&mut self) -> Vec<DlqEntry> {
        self.entries.drain(..).collect()
    }
}

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
// Fan-out dispatch
// ---------------------------------------------------------------------------

fn dispatch_to_subscribers(
    connections: &DashMap<String, ConnectionHandle>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    topic: &str,
    payload: &str,
    metrics: &Arc<RedisMetrics>,
) {
    let frame = super::server::WsFrame::PreFramed(super::server::pre_frame_text(payload));
    let senders = super::server::collect_topic_senders(connections, topic_subscribers, topic);
    super::server::fanout_to_senders_with_metrics(senders, frame, metrics);
}

// ---------------------------------------------------------------------------
// PUBLISH with pipelining + retry
// ---------------------------------------------------------------------------

const MAX_PIPELINE_SIZE: usize = 64;
const PUBLISH_MAX_ATTEMPTS: usize = 3;
const PUBLISH_RETRY_DELAYS: &[Duration] = &[Duration::from_millis(100), Duration::from_millis(200)];

async fn publish_pipeline(
    pub_conn: &mut redis::aio::MultiplexedConnection,
    batch: &[(String, String)],
    metrics: &RedisMetrics,
    dlq: &std::sync::Mutex<DeadLetterQueue>,
) -> bool {
    let mut last_err = String::new();
    let count = batch.len() as u64;

    for attempt in 0..PUBLISH_MAX_ATTEMPTS {
        let mut pipe = redis::pipe();
        for (channel, payload) in batch {
            pipe.cmd("PUBLISH").arg(channel).arg(payload).ignore();
        }

        match pipe.query_async::<()>(pub_conn).await {
            Ok(()) => {
                metrics
                    .messages_published
                    .fetch_add(count, Ordering::Relaxed);
                return true;
            }
            Err(e) => {
                last_err = e.to_string();
                if let Some(delay) = PUBLISH_RETRY_DELAYS.get(attempt) {
                    tokio::time::sleep(*delay).await;
                }
            }
        }
    }

    metrics.publish_errors.fetch_add(count, Ordering::Relaxed);
    eprintln!("[WSE-Redis] Pipeline publish failed after retries ({count} msgs): {last_err}");
    let mut guard = dlq.lock().unwrap();
    for (channel, payload) in batch {
        guard.push(DlqEntry {
            channel: channel.clone(),
            payload: payload.clone(),
            error: last_err.clone(),
        });
    }
    false
}

// ---------------------------------------------------------------------------
// Inner connection loop
// ---------------------------------------------------------------------------

/// Outcome from connect_and_run: either clean shutdown or error.
enum RunOutcome {
    Shutdown,
    Error(String),
}

async fn connect_and_run(
    url: &str,
    connections: &DashMap<String, ConnectionHandle>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<RedisCommand>,
    metrics: &Arc<RedisMetrics>,
    dlq: &std::sync::Mutex<DeadLetterQueue>,
) -> Result<(), String> {
    let client = redis::Client::open(url).map_err(|e| format!("Failed to open client: {e}"))?;

    let mut pub_conn = client
        .get_multiplexed_tokio_connection()
        .await
        .map_err(|e| format!("Failed to get publish connection: {e}"))?;

    let mut pubsub = client
        .get_async_pubsub()
        .await
        .map_err(|e| format!("Failed to get pubsub connection: {e}"))?;

    pubsub
        .psubscribe("wse:*")
        .await
        .map_err(|e| format!("Failed to psubscribe wse:*: {e}"))?;

    metrics.connected.store(true, Ordering::Relaxed);
    eprintln!("[WSE-Redis] Connected, listening on wse:*");

    // Use a shutdown signal to coordinate the two tasks.
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    // --- Publish task: batches commands from cmd_rx, pipelines to Redis ---
    let pub_metrics: &RedisMetrics = metrics;
    let pub_dlq = dlq;
    let pub_task = async {
        let mut breaker = CircuitBreaker::new();
        let mut batch: Vec<(String, String)> = Vec::with_capacity(MAX_PIPELINE_SIZE);

        loop {
            // Wait for first message (blocking)
            match cmd_rx.recv().await {
                Some(RedisCommand::Publish { channel, payload }) => {
                    batch.push((channel, payload));
                }
                Some(RedisCommand::Shutdown) | None => {
                    return RunOutcome::Shutdown;
                }
            }

            // Drain as many pending messages as available (non-blocking)
            while batch.len() < MAX_PIPELINE_SIZE {
                match cmd_rx.try_recv() {
                    Ok(RedisCommand::Publish { channel, payload }) => {
                        batch.push((channel, payload));
                    }
                    Ok(RedisCommand::Shutdown) => {
                        return RunOutcome::Shutdown;
                    }
                    Err(_) => break,
                }
            }

            // Pipeline the batch
            if breaker.can_execute() {
                if publish_pipeline(&mut pub_conn, &batch, pub_metrics, pub_dlq).await {
                    breaker.record_success();
                } else {
                    breaker.record_failure();
                }
            } else {
                let count = batch.len() as u64;
                pub_metrics
                    .publish_errors
                    .fetch_add(count, Ordering::Relaxed);
                let mut guard = pub_dlq.lock().unwrap();
                for (channel, payload) in &batch {
                    guard.push(DlqEntry {
                        channel: channel.clone(),
                        payload: payload.clone(),
                        error: "Circuit breaker open".to_string(),
                    });
                }
            }
            batch.clear();
        }
    };

    // --- Subscribe task: reads from Redis PSUBSCRIBE, dispatches to WebSocket ---
    let msg_stream = pubsub.into_on_message();
    tokio::pin!(msg_stream);
    let sub_metrics = metrics;

    let sub_task = async {
        loop {
            match msg_stream.next().await {
                Some(msg) => {
                    sub_metrics
                        .messages_received
                        .fetch_add(1, Ordering::Relaxed);
                    let channel_name = msg.get_channel_name().to_string();
                    let topic = channel_name.strip_prefix("wse:").unwrap_or(&channel_name);
                    if let Ok(payload) = msg.get_payload::<String>() {
                        dispatch_to_subscribers(
                            connections,
                            topic_subscribers,
                            topic,
                            &payload,
                            sub_metrics,
                        );
                    }
                }
                None => {
                    return RunOutcome::Error("Message stream ended".to_string());
                }
            }
        }
    };

    // Run both tasks concurrently. First one to finish determines outcome.
    // Drop shutdown_tx so _shutdown_rx in the other branch knows to stop.
    let outcome = tokio::select! {
        result = pub_task => {
            drop(shutdown_tx);
            result
        }
        result = sub_task => {
            drop(shutdown_tx);
            result
        }
        // If external shutdown arrives via the channel
        _ = shutdown_rx.recv() => {
            RunOutcome::Shutdown
        }
    };

    metrics.connected.store(false, Ordering::Relaxed);
    match outcome {
        RunOutcome::Shutdown => {
            eprintln!("[WSE-Redis] Shutting down");
            Ok(())
        }
        RunOutcome::Error(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Outer reconnection loop
// ---------------------------------------------------------------------------

pub(crate) async fn listener_task(
    url: String,
    connections: Arc<DashMap<String, ConnectionHandle>>,
    topic_subscribers: Arc<DashMap<String, DashSet<String>>>,
    mut cmd_rx: mpsc::UnboundedReceiver<RedisCommand>,
    metrics: Arc<RedisMetrics>,
    dlq: Arc<std::sync::Mutex<DeadLetterQueue>>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut conn_breaker = CircuitBreaker::new();

    loop {
        if !conn_breaker.can_execute() {
            eprintln!(
                "[WSE-Redis] Circuit breaker open, waiting {:.0}s",
                conn_breaker.reset_timeout.as_secs_f64()
            );
            let delay = conn_breaker.reset_timeout;
            if drain_during_backoff(&mut cmd_rx, delay, &metrics, &dlq).await {
                return;
            }
            continue;
        }

        match connect_and_run(
            &url,
            &connections,
            &topic_subscribers,
            &mut cmd_rx,
            &metrics,
            &dlq,
        )
        .await
        {
            Ok(()) => return, // Clean shutdown
            Err(e) => {
                // connected was set to true inside connect_and_run if connection succeeded,
                // then back to false before returning Err. We check was_connected_before
                // to see if it was false (meaning connect_and_run established a connection
                // that later dropped). Since connect_and_run sets connected=true on success
                // and false on error, the only way to detect a "was connected" state is
                // to check if connect_and_run set it to true at some point. We use a
                // separate flag returned from connect_and_run.
                //
                // Simpler: if the error message is about stream ending (not connection
                // failure), we were connected. But let's just always try to reset on
                // reconnectable errors that got past the initial connection phase.
                let was_connected = !e.starts_with("Failed to");
                if was_connected {
                    backoff.reset();
                    conn_breaker.record_success();
                } else {
                    conn_breaker.record_failure();
                }
                metrics.reconnect_count.fetch_add(1, Ordering::Relaxed);
                let delay = backoff.next_delay();
                eprintln!(
                    "[WSE-Redis] Connection lost: {e}. Reconnecting in {:.1}s",
                    delay.as_secs_f64()
                );
                if drain_during_backoff(&mut cmd_rx, delay, &metrics, &dlq).await {
                    return;
                }
            }
        }
    }
}

async fn drain_during_backoff(
    cmd_rx: &mut mpsc::UnboundedReceiver<RedisCommand>,
    delay: Duration,
    metrics: &RedisMetrics,
    dlq: &std::sync::Mutex<DeadLetterQueue>,
) -> bool {
    let deadline = tokio::time::Instant::now() + delay;
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => { return false; }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(RedisCommand::Shutdown) | None => {
                        metrics.connected.store(false, Ordering::Relaxed);
                        eprintln!("[WSE-Redis] Shutting down during backoff");
                        return true;
                    }
                    Some(RedisCommand::Publish { channel, payload }) => {
                        metrics.publish_errors.fetch_add(1, Ordering::Relaxed);
                        dlq.lock().unwrap().push(DlqEntry {
                            channel,
                            payload,
                            error: "Redis disconnected".to_string(),
                        });
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_exact() {
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "bar"));
    }

    #[test]
    fn test_glob_star() {
        assert!(glob_match("user:*:events", "user:abc:events"));
        assert!(glob_match("user:*", "user:abc:events"));
        assert!(!glob_match("user:*:events", "user:abc:other"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn test_glob_question() {
        assert!(glob_match("user:?:x", "user:a:x"));
        assert!(!glob_match("user:?:x", "user:ab:x"));
    }

    #[test]
    fn test_dlq_push_and_drain() {
        let mut dlq = DeadLetterQueue::new(3);
        dlq.push(DlqEntry {
            channel: "a".into(),
            payload: "1".into(),
            error: "err".into(),
        });
        dlq.push(DlqEntry {
            channel: "b".into(),
            payload: "2".into(),
            error: "err".into(),
        });
        assert_eq!(dlq.len(), 2);
        let entries = dlq.drain_all();
        assert_eq!(entries.len(), 2);
        assert_eq!(dlq.len(), 0);
    }

    #[test]
    fn test_dlq_evicts_oldest() {
        let mut dlq = DeadLetterQueue::new(2);
        dlq.push(DlqEntry {
            channel: "a".into(),
            payload: "1".into(),
            error: "e".into(),
        });
        dlq.push(DlqEntry {
            channel: "b".into(),
            payload: "2".into(),
            error: "e".into(),
        });
        dlq.push(DlqEntry {
            channel: "c".into(),
            payload: "3".into(),
            error: "e".into(),
        });
        assert_eq!(dlq.len(), 2);
        let entries = dlq.drain_all();
        assert_eq!(entries[0].channel, "b");
        assert_eq!(entries[1].channel, "c");
    }
}
