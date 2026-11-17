//! How fast the client sends with its outbox and without it: a measurement,
//! not a check. Run it built for release:
//!
//! ```text
//! cargo test --release -p wse-client --test outbox_speed -- --ignored --nocapture
//! ```

#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::Notify;
use wse_client::{Config, Error, Offline};

/// How many messages each run sends: `WSE_OUTBOX_SPEED_MESSAGES`, or
/// 200,000.
fn messages() -> u64 {
    std::env::var("WSE_OUTBOX_SPEED_MESSAGES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(200_000)
}
const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

/// Counts what it takes, and says when it took them all.
struct Count {
    taken: Arc<AtomicU64>,
    all: Arc<Notify>,
    count: u64,
}

impl wse::Handler for Count {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
        if self.taken.fetch_add(1, Ordering::AcqRel) + 1 == self.count {
            self.all.notify_one();
        }
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        true
    }

    fn disconnect(&self, _conn: ()) {}
}

/// Sends [`messages`] with `offline` and says how long the server's
/// application took to have them all, and the client to hear of them all.
async fn run(name: &str, offline: Option<Offline>) {
    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    // A measurement of the send path, not of the rate limit.
    config.rate_limit_capacity = 1e12;
    config.rate_limit_refill = 1e12;
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (taken, all) = (Arc::new(AtomicU64::new(0)), Arc::new(Notify::new()));
    server
        .start(
            listener,
            Count {
                taken: Arc::clone(&taken),
                all: Arc::clone(&all),
                count: messages(),
            },
        )
        .unwrap();
    let mut client_config = Config::new(url);
    client_config.token = Some(
        server
            .publisher()
            .issue_token("ann", Duration::from_secs(600), serde_json::Map::new())
            .unwrap(),
    );
    client_config.offline = offline;
    let (client, mut events) = wse_client::connect(client_config).await.unwrap();
    // The outcomes are read, as an application reads them; how many answers
    // the server gave is how many outcomes came.
    let answers = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&answers);
    let reader = tokio::spawn(async move {
        while let Some(incoming) = events.next().await {
            if let wse_client::Incoming::Event(wse_client::Event::Delivered(_)) = incoming {
                counted.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let count = messages();
    let start = Instant::now();
    let payload = serde_json::json!({"item": "a-typical-item", "qty": 3});
    let mut full = 0u64;
    for _ in 0..count {
        loop {
            match client.send("order", &payload).await {
                Ok(()) => break,
                Err(Error::Full) => {
                    full += 1;
                    tokio::task::yield_now().await;
                }
                Err(e) => panic!("{name}: {e}"),
            }
        }
    }
    let sent = start.elapsed();
    tokio::time::timeout(Duration::from_secs(120), all.notified())
        .await
        .unwrap_or_else(|_| panic!("{name}: {} taken", taken.load(Ordering::Acquire)));
    let taken_at = start.elapsed();
    while client.kept() > 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let answered = start.elapsed();
    let rate = count as f64 / taken_at.as_secs_f64();
    let answers = answers.load(Ordering::Relaxed).max(1);
    println!(
        "{name:>7}: {count} sent in {sent:.2?}, taken in {taken_at:.2?} ({rate:.0}/s), \
         all answered in {answered:.2?} by {answers} answers ({} a time); \
         waited for room {full} times",
        count / answers
    );
    client.close().await;
    reader.abort();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "a measurement: run it built for release"]
async fn sending_with_the_outbox_and_without() {
    let dir = std::env::temp_dir().join(format!("wse-outbox-speed-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    // One mode alone, for a profiler: `WSE_OUTBOX_SPEED_ONLY=memory`.
    if let Ok(only) = std::env::var("WSE_OUTBOX_SPEED_ONLY") {
        let offline = match only.as_str() {
            "none" => None,
            "file" => Some(Offline::in_file(dir.join("queue"))),
            window if window.parse::<usize>().is_ok() => {
                let mut wide = Offline::in_memory();
                wide.max_messages = window.parse().unwrap_or(1000);
                wide.max_bytes = 1 << 30;
                Some(wide)
            }
            _ => Some(Offline::in_memory()),
        };
        run(&only, offline).await;
        std::fs::remove_dir_all(&dir).unwrap();
        return;
    }
    for round in 0..2 {
        println!("round {round}");
        run("none", None).await;
        run("memory", Some(Offline::in_memory())).await;
        for window in [4_000, 10_000, 100_000] {
            let mut wide = Offline::in_memory();
            wide.max_messages = window;
            wide.max_bytes = 1 << 30;
            run(&format!("{window}"), Some(wide)).await;
        }
        let path = dir.join(format!("queue-{round}"));
        run("file", Some(Offline::in_file(&path))).await;
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
