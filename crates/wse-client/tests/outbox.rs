//! The outbox against a WSE server: every message waits until the server
//! answers for it, the application hears what became of each, what is sent
//! with no connection goes on the next one of the same user, from memory or
//! from a file that outlives the client, and the server's application takes
//! each message once, whatever the client went through.

#![cfg(not(target_arch = "wasm32"))]

use std::path::PathBuf;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as Frame;
use wse_client::{Config, Error, Event, Events, Incoming, NotDelivered, Offline, SendOptions};

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

/// What the server's application took: the type, the id, the number.
#[derive(Debug)]
struct Took {
    kind: String,
    id: Option<String>,
    s: Option<u64>,
}

/// Takes each message, but refuses those of type `refuse` (no room) and
/// takes its time over those of type `slow`, said before it is done.
struct App {
    took: mpsc::UnboundedSender<Took>,
}

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), message: wse::Inbound, _size: usize) -> bool {
        let wse::Inbound::Json(message) = message else {
            return true;
        };
        let kind = message["t"].as_str().unwrap_or_default().to_owned();
        if kind == "refuse" {
            return false;
        }
        let slow = kind == "slow";
        let _ = self.took.send(Took {
            kind,
            id: message["id"].as_str().map(str::to_owned),
            s: message["s"].as_u64(),
        });
        if slow {
            std::thread::sleep(Duration::from_millis(300));
        }
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        true
    }

    fn disconnect(&self, _conn: ()) {}
}

struct Stand {
    server: wse::Server,
    publisher: wse::Publisher,
    url: String,
    took: mpsc::UnboundedReceiver<Took>,
}

async fn stand() -> Stand {
    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (took_tx, took) = mpsc::unbounded_channel();
    server.start(listener, App { took: took_tx }).unwrap();
    let publisher = server.publisher();
    Stand {
        server,
        publisher,
        url,
        took,
    }
}

impl Stand {
    fn config(&self, user: &str, offline: Option<Offline>) -> Config {
        let mut config = Config::new(self.url.clone());
        config.token = Some(
            self.publisher
                .issue_token(user, Duration::from_secs(600), serde_json::Map::new())
                .unwrap(),
        );
        config.offline = offline;
        config
    }

    /// The next message the application took.
    async fn took(&mut self) -> Took {
        tokio::time::timeout(Duration::from_secs(5), self.took.recv())
            .await
            .expect("a message within 5 s")
            .expect("the server lives")
    }

    /// The application takes nothing for 300 ms.
    async fn quiet(&mut self) {
        let got = tokio::time::timeout(Duration::from_millis(300), self.took.recv()).await;
        assert!(got.is_err(), "nothing taken: {got:?}");
    }
}

/// A directory of its own for one test's files.
fn dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wse-outbox-test-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn connect(config: &Config) -> (wse_client::Client, Events) {
    wse_client::connect(config.clone()).await.unwrap()
}

async fn paused(client: &wse_client::Client) {
    client.pause("test").unwrap();
    for _ in 0..250 {
        if !client.is_connected() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the client did not pause");
}

/// The ids the next `Delivered` names.
async fn delivered(events: &mut Events) -> Vec<String> {
    outcome(events, |event| match event {
        Event::Delivered(ids) => Some(ids),
        _ => None,
    })
    .await
}

/// The next `NotDelivered`.
async fn not_delivered(events: &mut Events) -> NotDelivered {
    outcome(events, |event| match event {
        Event::NotDelivered(not) => Some(not),
        _ => None,
    })
    .await
}

async fn outcome<T>(events: &mut Events, mut pick: impl FnMut(Event) -> Option<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.next().await {
                Some(Incoming::Event(event)) => {
                    if let Some(found) = pick(event) {
                        return found;
                    }
                }
                Some(_) => {}
                None => panic!("the stream ended"),
            }
        }
    })
    .await
    .expect("an outcome within 5 s")
}

/// Every id delivered until `count` were.
async fn all_delivered(events: &mut Events, count: usize) -> Vec<String> {
    let mut ids = Vec::new();
    while ids.len() < count {
        ids.extend(delivered(events).await);
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_waits_for_the_server_and_the_application_hears_it_was_delivered() {
    let mut stand = stand().await;
    let (client, mut events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    let mut options = SendOptions::default();
    options.id = Some("order-17".to_owned());
    let id = client
        .send_with("order", &serde_json::json!({"n": 1}), &options)
        .await
        .unwrap();
    assert_eq!(id, "order-17");
    let took = stand.took().await;
    assert_eq!(
        (took.kind.as_str(), took.id.as_deref(), took.s),
        ("order", Some("order-17"), Some(1)),
        "numbered in the outbox, its id the application's"
    );
    assert_eq!(delivered(&mut events).await, ["order-17"]);
    assert_eq!(client.kept(), 0);

    // An id of the client's own: a UUID.
    let id = client
        .send_with("order", &serde_json::json!({}), &SendOptions::default())
        .await
        .unwrap();
    assert_eq!(id.len(), 36, "{id}");
    assert_eq!(stand.took().await.s, Some(2));
    assert_eq!(delivered(&mut events).await, [id]);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_the_application_refuses_is_said() {
    let mut stand = stand().await;
    let (client, mut events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    let id = client
        .send_with("refuse", &serde_json::json!({}), &SendOptions::default())
        .await
        .unwrap();
    let not = not_delivered(&mut events).await;
    assert_eq!((not.reason.as_str(), not.ids), ("busy", vec![id]));
    client.send("after", &serde_json::json!({})).await.unwrap();
    assert_eq!(stand.took().await.kind, "after");
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_is_sent_with_no_connection_goes_on_the_next() {
    let mut stand = stand().await;
    let (client, mut events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    paused(&client).await;
    client.send("one", &serde_json::json!({})).await.unwrap();
    client.try_send("two", &serde_json::json!({})).unwrap();
    // A live message never waits.
    assert_eq!(
        client.try_send_live("valve", &serde_json::json!({})),
        Err(Error::NotConnected)
    );
    for _ in 0..50 {
        if client.kept() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(client.kept(), 2);
    stand.quiet().await;

    client.resume("test").unwrap();
    assert_eq!(stand.took().await.kind, "one");
    assert_eq!(stand.took().await.kind, "two");
    assert_eq!(all_delivered(&mut events, 2).await.len(), 2);
    assert_eq!(client.kept(), 0);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_waits_no_longer_than_its_time_to_be_sent() {
    let stand = stand().await;
    let (client, mut events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    paused(&client).await;
    let mut options = SendOptions::default();
    options.ttl = Some(Duration::from_millis(100));
    let id = client
        .send_with("late", &serde_json::json!({}), &options)
        .await
        .unwrap();
    let not = not_delivered(&mut events).await;
    assert_eq!((not.reason.as_str(), not.ids), ("expired", vec![id]));
    assert_eq!(client.kept(), 0);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_clear_drops_what_waits() {
    let mut stand = stand().await;
    let (client, mut events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    paused(&client).await;
    let one = client
        .send_with("one", &serde_json::json!({}), &SendOptions::default())
        .await
        .unwrap();
    let two = client
        .send_with("two", &serde_json::json!({}), &SendOptions::default())
        .await
        .unwrap();
    client.clear().unwrap();
    let mut cleared = Vec::new();
    while cleared.len() < 2 {
        let not = not_delivered(&mut events).await;
        assert_eq!(not.reason, "cleared");
        cleared.extend(not.ids);
    }
    assert_eq!(cleared, [one, two]);
    assert_eq!(client.kept(), 0);
    client.resume("test").unwrap();
    stand.quiet().await;
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_waits_in_a_file_outlives_the_client() {
    let mut stand = stand().await;
    let dir = dir();
    let config = stand.config("ann", Some(Offline::in_file(dir.join("queue"))));
    {
        let (client, _events) = connect(&config).await;
        paused(&client).await;
        client.send("one", &serde_json::json!({})).await.unwrap();
        client.send("two", &serde_json::json!({})).await.unwrap();
        client.close().await;
    }
    stand.quiet().await;

    // The application starts again, as the same user.
    let (client, mut events) = connect(&config).await;
    assert_eq!(stand.took().await.kind, "one");
    assert_eq!(stand.took().await.kind, "two");
    assert_eq!(all_delivered(&mut events, 2).await.len(), 2);
    client.close().await;

    // Nothing comes twice.
    let (client, _events) = connect(&config).await;
    stand.quiet().await;
    assert_eq!(client.kept(), 0);
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A client that ends before the server's answer came: the next one sends
/// nothing again, and hears that the server took it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_message_taken_before_the_client_ended_is_not_taken_twice() {
    let mut stand = stand().await;
    let dir = dir();
    let config = stand.config("ann", Some(Offline::in_file(dir.join("queue"))));
    let id = {
        let (client, _events) = connect(&config).await;
        let id = client
            .send_with("slow", &serde_json::json!({}), &SendOptions::default())
            .await
            .unwrap();
        // The application has it, its answer not given yet: the client
        // ends where it stands.
        assert_eq!(stand.took().await.kind, "slow");
        client.close_within(Duration::ZERO).await;
        id
    };
    let (client, mut events) = connect(&config).await;
    assert_eq!(delivered(&mut events).await, [id]);
    stand.quiet().await;
    assert_eq!(client.kept(), 0);
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_users_connection_does_not_send_what_waits() {
    let mut stand = stand().await;
    let dir = dir();
    let offline = Offline::in_file(dir.join("queue"));
    let id = {
        let (client, _events) = connect(&stand.config("ann", Some(offline.clone()))).await;
        paused(&client).await;
        let id = client
            .send_with("anns", &serde_json::json!({}), &SendOptions::default())
            .await
            .unwrap();
        client.close().await;
        id
    };
    let (client, mut events) = connect(&stand.config("bob", Some(offline))).await;
    let not = not_delivered(&mut events).await;
    assert_eq!((not.reason.as_str(), not.ids), ("user_changed", vec![id]));
    stand.quiet().await;
    assert_eq!(client.kept(), 0);
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_outbox_refuses_at_once() {
    let stand = stand().await;
    let mut offline = Offline::in_memory();
    offline.max_messages = 2;
    let (client, _events) = connect(&stand.config("ann", Some(offline))).await;
    paused(&client).await;
    client.try_send("1", &serde_json::json!({})).unwrap();
    client.try_send("2", &serde_json::json!({})).unwrap();
    assert_eq!(
        client.try_send("3", &serde_json::json!({})),
        Err(Error::Full)
    );
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_an_outbox_a_send_with_no_connection_fails() {
    let stand = stand().await;
    let (client, _events) = connect(&stand.config("ann", None)).await;
    paused(&client).await;
    assert_eq!(
        client.try_send("x", &serde_json::json!({})),
        Err(Error::NotConnected)
    );
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_another_client_holds_is_said_and_the_outbox_kept_in_memory() {
    let mut stand = stand().await;
    let dir = dir();
    let config = stand.config("ann", Some(Offline::in_file(dir.join("queue"))));
    let (first, _first_events) = connect(&config).await;
    let (second, mut events) = connect(&config).await;
    let failed = outcome(&mut events, |event| match event {
        Event::StorageFailed(failed) => Some(failed),
        _ => None,
    })
    .await;
    assert_eq!(failed.operation, "open");
    second.send("still", &serde_json::json!({})).await.unwrap();
    assert_eq!(stand.took().await.kind, "still");
    first.close().await;
    second.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A server older than 3.2 does not answer: a message is gone once
/// written, as before, and goes without its number.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_that_does_not_answer_takes_what_was_written() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (heard_tx, mut heard) = mpsc::unbounded_channel::<serde_json::Value>();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        while let Some(Ok(frame)) = ws.next().await {
            let Frame::Text(text) = frame else {
                continue;
            };
            let message: serde_json::Value = serde_json::from_str(&text).unwrap();
            if message["t"] == "client_hello" {
                let ready = serde_json::json!({"c": "WSE", "t": "server_ready", "v": 1,
                    "p": {"connection_id": "c1", "details": {"user_id": "ann", "features": {}}}});
                ws.send(Frame::Text(ready.to_string().into()))
                    .await
                    .unwrap();
            } else {
                let _ = heard_tx.send(message);
            }
        }
    });
    let mut config = Config::new(url);
    config.offline = Some(Offline::in_memory());
    let (client, mut events) = connect(&config).await;
    let id = client
        .send_with("old", &serde_json::json!({}), &SendOptions::default())
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = heard.recv().await.unwrap();
            if message["t"] == "old" {
                return message;
            }
        }
    })
    .await
    .unwrap();
    assert!(message.get("s").is_none(), "not numbered: {message}");
    assert_eq!(delivered(&mut events).await, [id]);
    client.close().await;
}
