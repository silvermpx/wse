//! The offline queue against a WSE server: what is sent with no connection
//! goes on the next one of the same user, from memory or from a file that
//! outlives the client.

#![cfg(not(target_arch = "wasm32"))]

use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use wse_client::{Config, Error, Event, Incoming, Offline};

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

/// Says the type and the id of each message a client sends.
struct App {
    inbound: mpsc::UnboundedSender<(String, Option<String>)>,
}

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), message: wse::Inbound, _size: usize) -> bool {
        if let wse::Inbound::Json(message) = message
            && let Some(kind) = message["t"].as_str()
        {
            let id = message["id"].as_str().map(str::to_owned);
            let _ = self.inbound.send((kind.to_owned(), id));
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
    inbound: mpsc::UnboundedReceiver<(String, Option<String>)>,
}

async fn stand() -> Stand {
    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    config.client_subscriptions = true;
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (inbound_tx, inbound) = mpsc::unbounded_channel();
    server
        .start(
            listener,
            App {
                inbound: inbound_tx,
            },
        )
        .unwrap();
    let publisher = server.publisher();
    Stand {
        server,
        publisher,
        url,
        inbound,
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

    /// The type and id of the next message a client sent.
    async fn next(&mut self) -> (String, Option<String>) {
        tokio::time::timeout(Duration::from_secs(5), self.inbound.recv())
            .await
            .expect("a message within 5 s")
            .expect("the server lives")
    }

    /// No client sends anything for 300 ms.
    async fn quiet(&mut self) {
        let got = tokio::time::timeout(Duration::from_millis(300), self.inbound.recv()).await;
        assert!(got.is_err(), "nothing sent: {got:?}");
    }
}

/// A directory of its own for one test's files.
fn dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "wse-offline-test-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Connects with `config`, again while the file is still the last client's
/// (its task ends right after its close).
async fn connect(config: &Config) -> (wse_client::Client, wse_client::Events) {
    for _ in 0..100 {
        match wse_client::connect(config.clone()).await {
            Err(Error::Config(why)) if why.contains("another client's") => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            connected => return connected.unwrap(),
        }
    }
    panic!("the offline queue's file stayed the last client's");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_is_sent_with_no_connection_goes_on_the_next() {
    let mut stand = stand().await;
    let (client, _events) = connect(&stand.config("ann", Some(Offline::in_memory()))).await;
    client.send("first", &serde_json::json!({})).await.unwrap();
    let (kind, id) = stand.next().await;
    assert_eq!(kind, "first");
    let id = id.expect("every message has an id");
    assert_eq!(id.len(), 36, "a UUID: {id}");

    paused(&client).await;
    client.send("one", &serde_json::json!({})).await.unwrap();
    client.try_send("two", &serde_json::json!({})).unwrap();
    assert_eq!(client.kept(), 2);
    // A live message is never kept.
    assert_eq!(
        client.try_send_live("valve", &serde_json::json!({})),
        Err(Error::NotConnected)
    );
    stand.quiet().await;

    client.resume("test").unwrap();
    assert_eq!(stand.next().await.0, "one");
    assert_eq!(stand.next().await.0, "two");
    for _ in 0..50 {
        if client.kept() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(client.kept(), 0, "written: no longer kept");
    client.send("after", &serde_json::json!({})).await.unwrap();
    assert_eq!(stand.next().await.0, "after");
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_is_kept_in_a_file_outlives_the_client() {
    let mut stand = stand().await;
    let dir = dir();
    let offline = Offline::in_file(dir.join("queue"));
    let config = stand.config("ann", Some(offline));
    {
        let (client, _events) = connect(&config).await;
        paused(&client).await;
        client.send("one", &serde_json::json!({})).await.unwrap();
        client.send("two", &serde_json::json!({})).await.unwrap();
        client.close().await;
    }
    stand.quiet().await;

    // The application starts again, as the same user.
    let (client, _events) = connect(&config).await;
    assert_eq!(stand.next().await.0, "one");
    assert_eq!(stand.next().await.0, "two");
    client.close().await;
    drop(client);

    // Nothing comes twice.
    let (client, _events) = connect(&config).await;
    stand.quiet().await;
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn another_users_connection_does_not_send_what_was_kept() {
    let mut stand = stand().await;
    let dir = dir();
    let offline = Offline::in_file(dir.join("queue"));
    {
        let (client, _events) = connect(&stand.config("ann", Some(offline.clone()))).await;
        paused(&client).await;
        client.send("anns", &serde_json::json!({})).await.unwrap();
        client.close().await;
    }
    let (client, _events) = connect(&stand.config("bob", Some(offline))).await;
    stand.quiet().await;
    assert_eq!(client.kept(), 0, "dropped, not kept for later");
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn it_keeps_as_many_as_it_may() {
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
async fn without_an_offline_queue_a_send_with_no_connection_fails() {
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

/// A file that can no longer be written over says so, and what is kept
/// still goes.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_file_that_cannot_be_written_says_so() {
    use std::os::unix::fs::PermissionsExt;

    let mut stand = stand().await;
    let dir = dir();
    let (client, mut events) =
        connect(&stand.config("ann", Some(Offline::in_file(dir.join("queue"))))).await;
    paused(&client).await;
    // The directory takes no new file: writing the file over fails (not for
    // root, who writes anywhere).
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    if std::fs::write(dir.join("probe"), b"").is_ok() {
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        client.close().await;
        stand.server.stop().await;
        std::fs::remove_dir_all(&dir).unwrap();
        return;
    }
    for n in 0..70 {
        client
            .send(&format!("m{n}"), &serde_json::json!({}))
            .await
            .unwrap();
    }
    client.resume("test").unwrap();
    for n in 0..70 {
        assert_eq!(stand.next().await.0, format!("m{n}"));
    }
    let failed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match events.next().await {
                Some(Incoming::Event(Event::StorageFailed(failed))) => return failed,
                Some(_) => {}
                None => panic!("the stream ended"),
            }
        }
    })
    .await
    .expect("a storage failure within 5 s");
    assert_eq!(failed.operation, "write");
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    client.close().await;
    stand.server.stop().await;
    std::fs::remove_dir_all(&dir).unwrap();
}
