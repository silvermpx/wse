use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use dashmap::{DashMap, DashSet};
use futures_util::StreamExt;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::protocol::Message;

use super::reliability::{CircuitBreaker, ExponentialBackoff};
use super::server::ConnectionHandle;

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

fn glob_match(pattern: &str, text: &str) -> bool {
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

async fn dispatch_to_subscribers(
    connections: &RwLock<HashMap<String, ConnectionHandle>>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    topic: &str,
    payload: &str,
    metrics: &RedisMetrics,
) {
    let conns = connections.read().await;
    let mut delivered: u64 = 0;

    if let Some(conn_ids) = topic_subscribers.get(topic) {
        for conn_id_ref in conn_ids.iter() {
            if let Some(handle) = conns.get(conn_id_ref.key()) {
                if handle
                    .tx
                    .send(Message::Text(payload.to_owned().into()))
                    .is_ok()
                {
                    delivered += 1;
                } else {
                    metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    for entry in topic_subscribers.iter() {
        let pattern = entry.key();
        if (pattern.contains('*') || pattern.contains('?')) && glob_match(pattern, topic) {
            for conn_id_ref in entry.value().iter() {
                if let Some(handle) = conns.get(conn_id_ref.key()) {
                    if handle
                        .tx
                        .send(Message::Text(payload.to_owned().into()))
                        .is_ok()
                    {
                        delivered += 1;
                    } else {
                        metrics.messages_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    metrics
        .messages_delivered
        .fetch_add(delivered, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// PUBLISH with retry (3 attempts: 100ms, 200ms delays between retries)
// ---------------------------------------------------------------------------

const PUBLISH_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_millis(100), Duration::from_millis(200)];

async fn publish_with_retry(
    pub_conn: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &str,
    metrics: &RedisMetrics,
    dlq: &std::sync::Mutex<DeadLetterQueue>,
) {
    let mut last_err = String::new();

    for attempt in 0..3 {
        let result: Result<(), redis::RedisError> = redis::cmd("PUBLISH")
            .arg(channel)
            .arg(payload)
            .query_async(pub_conn)
            .await;

        match result {
            Ok(()) => {
                metrics.messages_published.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(e) => {
                last_err = e.to_string();
                if attempt < 2 {
                    tokio::time::sleep(PUBLISH_RETRY_DELAYS[attempt]).await;
                }
            }
        }
    }

    metrics.publish_errors.fetch_add(1, Ordering::Relaxed);
    eprintln!("[WSE-Redis] Publish failed after 3 attempts: {last_err}");
    dlq.lock().unwrap().push(DlqEntry {
        channel: channel.to_owned(),
        payload: payload.to_owned(),
        error: last_err,
    });
}

// ---------------------------------------------------------------------------
// Inner connection loop
// ---------------------------------------------------------------------------

async fn connect_and_run(
    url: &str,
    connections: &RwLock<HashMap<String, ConnectionHandle>>,
    topic_subscribers: &DashMap<String, DashSet<String>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<RedisCommand>,
    metrics: &RedisMetrics,
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

    let msg_stream = pubsub.into_on_message();
    tokio::pin!(msg_stream);

    let mut pub_breaker = CircuitBreaker::new();

    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(RedisCommand::Publish { channel, payload }) => {
                        if pub_breaker.can_execute() {
                            publish_with_retry(
                                &mut pub_conn, &channel, &payload, metrics, dlq,
                            ).await;
                            pub_breaker.record_success();
                        } else {
                            metrics.publish_errors.fetch_add(1, Ordering::Relaxed);
                            dlq.lock().unwrap().push(DlqEntry {
                                channel,
                                payload,
                                error: "Circuit breaker open".to_string(),
                            });
                        }
                    }
                    Some(RedisCommand::Shutdown) | None => {
                        eprintln!("[WSE-Redis] Shutting down");
                        metrics.connected.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                }
            }
            msg = msg_stream.next() => {
                match msg {
                    Some(msg) => {
                        metrics.messages_received.fetch_add(1, Ordering::Relaxed);
                        let channel_name = msg.get_channel_name().to_string();
                        let topic = channel_name.strip_prefix("wse:").unwrap_or(&channel_name);
                        if let Ok(payload) = msg.get_payload::<String>() {
                            dispatch_to_subscribers(
                                connections, topic_subscribers, topic, &payload, metrics,
                            ).await;
                        }
                    }
                    None => {
                        metrics.connected.store(false, Ordering::Relaxed);
                        return Err("Message stream ended".to_string());
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Outer reconnection loop
// ---------------------------------------------------------------------------

pub(crate) async fn listener_task(
    url: String,
    connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>,
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
            Ok(()) => return,
            Err(e) => {
                conn_breaker.record_failure();
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
