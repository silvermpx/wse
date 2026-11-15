//! The client for mobile bindings (the `native` feature) against the WSE
//! server, both in this process.

#![cfg(all(feature = "native", not(target_arch = "wasm32")))]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use wse_client::native::{
    BatchKind, CancelToken, ErrorKind, Item, NativeClient, NativeConfig, NativeStatus, TokenSource,
};

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

struct App;

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
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
}

async fn stand() -> Stand {
    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    config.client_subscriptions = true;
    config.recovery = Some(wse::RecoverySettings::default());
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    server.start(listener, App).unwrap();
    let publisher = server.publisher();
    Stand {
        server,
        publisher,
        url,
    }
}

impl Stand {
    fn token(&self, user: &str) -> String {
        self.publisher
            .issue_token(user, Duration::from_secs(600), serde_json::Map::new())
            .unwrap()
    }

    fn config(&self, user: &str) -> NativeConfig {
        NativeConfig {
            url: self.url.clone(),
            token: Some(self.token(user)),
            topics: vec!["prices".into()],
            ..NativeConfig::default()
        }
    }
}

/// Batches until `want` messages came, acknowledging each.
async fn messages(client: &NativeClient, want: usize) -> Vec<String> {
    let mut ack = 0;
    let mut out = Vec::new();
    let cancel = CancelToken::new();
    while out.len() < want {
        let batch = tokio::time::timeout(
            Duration::from_secs(5),
            client.next_batch(ack, 64, 1 << 20, &cancel),
        )
        .await
        .expect("a batch within 5 s")
        .unwrap();
        ack = batch.id;
        for item in batch.items {
            if let Item::Message(m) = item {
                out.push(m.json.unwrap_or_default());
            }
        }
    }
    out
}

async fn subscribed(client: &NativeClient) -> u64 {
    let cancel = CancelToken::new();
    let mut ack = 0;
    loop {
        let batch = tokio::time::timeout(
            Duration::from_secs(5),
            client.next_batch(ack, 64, 1 << 20, &cancel),
        )
        .await
        .expect("a batch within 5 s")
        .unwrap();
        ack = batch.id;
        if batch
            .items
            .iter()
            .any(|i| matches!(i, Item::Subscribed { .. }))
        {
            return ack;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_comes_again_until_it_is_acknowledged() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("ana"), None, &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(client.status(), NativeStatus::Connected);
    let ack = subscribed(&client).await;
    stand
        .publisher
        .publish_local("prices", r#"{"t":"tick","p":{"n":1}}"#)
        .unwrap();
    let cancel = CancelToken::new();
    let first = client.next_batch(ack, 64, 1 << 20, &cancel).await.unwrap();
    assert_eq!(first.kind, BatchKind::Items);
    // Not acknowledged: the same batch again.
    let again = client.next_batch(ack, 64, 1 << 20, &cancel).await.unwrap();
    assert_eq!(again, first);
    let Item::Message(message) = &first.items[0] else {
        panic!("a message: {:?}", first.items);
    };
    assert_eq!(message.kind, "tick");
    assert_eq!(message.topic.as_deref(), Some("prices"));
    assert_eq!(message.json.as_deref(), Some(r#"{"n":1}"#));
    assert!(message.offset.is_some());
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_reader_at_a_time_and_disconnect_ends_the_wait() {
    let stand = stand().await;
    let client = Arc::new(
        NativeClient::connect(stand.config("bo"), None, &CancelToken::new())
            .await
            .unwrap(),
    );
    let ack = subscribed(&client).await;
    let waiting = {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            client
                .next_batch(ack, 64, 1 << 20, &CancelToken::new())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let second = client
        .next_batch(ack, 64, 1 << 20, &CancelToken::new())
        .await;
    assert_eq!(second.unwrap_err().kind, ErrorKind::Busy);
    client.disconnect(1_000).await;
    let ended = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .expect("the waiting reader wakes")
        .unwrap()
        .unwrap();
    assert_eq!(ended.kind, BatchKind::End);
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_read_loses_nothing() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("cy"), None, &CancelToken::new())
        .await
        .unwrap();
    let ack = subscribed(&client).await;
    let cancel = CancelToken::new();
    cancel.cancel();
    let cancelled = client.next_batch(ack, 64, 1 << 20, &cancel).await;
    assert_eq!(cancelled.unwrap_err().kind, ErrorKind::Cancelled);
    for n in 0..3 {
        stand
            .publisher
            .publish_local("prices", &format!(r#"{{"t":"tick","p":{{"n":{n}}}}}"#))
            .unwrap();
    }
    assert_eq!(
        messages(&client, 3).await,
        [r#"{"n":0}"#, r#"{"n":1}"#, r#"{"n":2}"#]
    );
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_connect_gives_up() {
    // Nothing listens there: the connect waits for its timeout.
    let config = NativeConfig {
        url: "ws://10.255.255.1:9/wse".into(),
        ..NativeConfig::default()
    };
    let cancel = CancelToken::new();
    let canceller = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        canceller.cancel();
    });
    let started = std::time::Instant::now();
    let error = NativeClient::connect(config, None, &cancel)
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Cancelled);
    assert!(started.elapsed() < Duration::from_secs(5));
}

struct Tokens {
    token: String,
    asked: AtomicUsize,
}

impl TokenSource for Tokens {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>> {
        self.asked.fetch_add(1, Ordering::Relaxed);
        let token = self.token.clone();
        Box::pin(async move { Ok(token) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_token_comes_from_the_application_and_payloads_are_json() {
    let stand = stand().await;
    let tokens = Arc::new(Tokens {
        token: stand.token("di"),
        asked: AtomicUsize::new(0),
    });
    let config = NativeConfig {
        token: None,
        ..stand.config("di")
    };
    let client = NativeClient::connect(config, Some(tokens.clone()), &CancelToken::new())
        .await
        .unwrap();
    assert_eq!(tokens.asked.load(Ordering::Relaxed), 1);
    assert_eq!(client.user_id().as_deref(), Some("di"));
    assert_eq!(
        client.try_send("order", "not json").unwrap_err().kind,
        ErrorKind::Encode
    );
    client
        .send("order".into(), r#"{"qty":1}"#.into())
        .await
        .unwrap();
    client.disconnect(1_000).await;
    stand.server.stop().await;
}
