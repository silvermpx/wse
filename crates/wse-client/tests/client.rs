//! The Rust client against the WSE server, both in this process.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use wse_client::{Config, Error, Event, Events, Format, Incoming, OwnedMessage, Status};

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

/// What the server application heard.
#[derive(Debug)]
enum Heard {
    Connect(String),
    Message(serde_json::Value),
    #[cfg_attr(not(feature = "aws-lc"), allow(dead_code))]
    Signed(serde_json::Value),
    Disconnect(String),
}

struct App {
    heard: mpsc::UnboundedSender<Heard>,
}

impl wse::Handler for App {
    type Connection = String;

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<String> {
        let conn_id = (**connect.conn_id).clone();
        let _ = self.heard.send(Heard::Connect(conn_id.clone()));
        Some(conn_id)
    }

    fn message(&self, _conn: &mut String, message: wse::Inbound, _size: usize) -> bool {
        if let wse::Inbound::Json(value) = message {
            let _ = self.heard.send(Heard::Message(value));
        }
        true
    }

    fn signed(&self, _conn: &mut String, message: serde_json::Value, _size: usize) -> bool {
        let _ = self.heard.send(Heard::Signed(message));
        true
    }

    fn disconnect(&self, conn: String) {
        let _ = self.heard.send(Heard::Disconnect(conn));
    }
}

struct Stand {
    server: wse::Server,
    publisher: wse::Publisher,
    url: String,
    heard: mpsc::UnboundedReceiver<Heard>,
}

/// A server that validates tokens, answers subscriptions and keeps topic
/// histories.
async fn stand() -> Stand {
    stand_with(wse::Config::default()).await
}

async fn stand_with(mut config: wse::Config) -> Stand {
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    config.client_subscriptions = true;
    config.recovery = Some(wse::RecoverySettings::default());
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (tx, heard) = mpsc::unbounded_channel();
    server.start(listener, App { heard: tx }).unwrap();
    let publisher = server.publisher();
    Stand {
        server,
        publisher,
        url,
        heard,
    }
}

impl Stand {
    fn config(&self, user: &str) -> Config {
        let token = self
            .publisher
            .issue_token(user, Duration::from_secs(600), serde_json::Map::new())
            .unwrap();
        let mut config = Config::new(&self.url);
        config.token = Some(token);
        config.reconnect.base_delay = Duration::from_millis(50);
        config
    }

    async fn heard(&mut self) -> Heard {
        tokio::time::timeout(Duration::from_secs(5), self.heard.recv())
            .await
            .expect("the application hears within 5 s")
            .expect("the application alive")
    }

    async fn connect_id(&mut self) -> String {
        loop {
            if let Heard::Connect(conn_id) = self.heard().await {
                return conn_id;
            }
        }
    }
}

async fn next(events: &mut Events) -> Incoming {
    tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("something within 5 s")
        .expect("the stream open")
}

async fn next_message(events: &mut Events) -> OwnedMessage {
    loop {
        if let Incoming::Message(message) = next(events).await {
            return message;
        }
    }
}

async fn next_event(events: &mut Events) -> Event {
    loop {
        if let Incoming::Event(event) = next(events).await {
            return event;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_connects_subscribes_and_hears_its_topics() {
    let mut stand = stand().await;
    let mut config = stand.config("alice");
    config.topics = vec!["prices".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    assert_eq!(client.status(), Status::Connected);
    assert_eq!(client.user_id().as_deref(), Some("alice"));
    let conn_id = stand.connect_id().await;
    let client_conn = client.connection_id();
    assert_eq!(client_conn.as_deref(), Some(conn_id.as_str()));

    let Event::Connected(connected) = next_event(&mut events).await else {
        panic!("connected first");
    };
    assert_eq!(connected.user_id.as_deref(), Some("alice"));
    assert!(connected.recovery);
    assert!(!connected.reconnected);
    let Event::Subscribed(subscribed) = next_event(&mut events).await else {
        panic!("then subscribed");
    };
    assert_eq!(subscribed.topics, ["prices"]);

    for px in [1, 2, 3] {
        stand
            .publisher
            .publish_local("prices", &json!({"t": "tick", "p": {"px": px}}).to_string())
            .unwrap();
    }
    let mut last = None;
    for px in [1, 2, 3] {
        let tick = next_message(&mut events).await;
        assert_eq!(tick.kind(), "tick");
        assert_eq!(tick.topic(), Some("prices"));
        assert_eq!(tick.payload_value().unwrap()["px"], px);
        let at = tick.position().expect("a stamped topic message");
        if let Some(last) = last {
            assert_eq!(at.offset, last + 1);
        }
        last = Some(at.offset);
    }
    client.close().await;
    assert_eq!(client.status(), Status::Closed(None));
    let gone = loop {
        if let Heard::Disconnect(conn) = stand.heard().await {
            break conn;
        }
    };
    assert_eq!(Some(gone), client_conn);
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_server_application_hears_what_a_client_sends() {
    let mut stand = stand().await;
    let (client, _events) = wse_client::connect(stand.config("bob")).await.unwrap();
    client
        .send("order", &json!({"id": 7, "side": "buy"}))
        .await
        .unwrap();
    client.try_send("cancel", &json!({"id": 7})).unwrap();
    let mut got = Vec::new();
    while got.len() < 2 {
        if let Heard::Message(message) = stand.heard().await {
            got.push(message);
        }
    }
    assert_eq!(got[0]["t"], "order");
    assert_eq!(got[0]["p"]["side"], "buy");
    assert_eq!(got[1]["t"], "cancel");
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_large_message_comes_compressed_and_reads_the_same() {
    let mut stand = stand().await;
    let (client, mut events) = wse_client::connect(stand.config("carol")).await.unwrap();
    let conn_id = stand.connect_id().await;
    let big = "x".repeat(4000);
    let mut event = serde_json::Map::new();
    event.insert("t".into(), "report".into());
    event.insert("p".into(), json!({"text": big}));
    let queued = stand.publisher.send_event(&conn_id, event, 1024).unwrap();
    assert!(queued < 1000, "compressed: {queued} bytes");
    let report = next_message(&mut events).await;
    assert_eq!(report.kind(), "report");
    assert_eq!(report.payload_value().unwrap()["text"], big);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_comes_back_and_gets_what_it_missed() {
    let mut stand = stand().await;
    let mut config = stand.config("dave");
    config.topics = vec!["chat".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    let conn_id = stand.connect_id().await;
    let say = |n: u32| json!({"t": "said", "p": {"n": n}}).to_string();
    // Subscribed before the first message goes out.
    while !matches!(next_event(&mut events).await, Event::Subscribed(_)) {}
    stand.publisher.publish_local("chat", &say(1)).unwrap();
    assert_eq!(
        next_message(&mut events).await.payload_value().unwrap()["n"],
        1
    );

    // The server drops the connection; the topic goes on without it.
    stand.publisher.disconnect(&conn_id).unwrap();
    let Event::Disconnected(lost) = next_event(&mut events).await else {
        panic!("disconnected");
    };
    assert_eq!(lost.code, Some(1000));
    for n in 2..=4 {
        stand.publisher.publish_local("chat", &say(n)).unwrap();
    }

    // Back: what it missed comes first, in order, then live messages.
    let Event::Connected(again) = next_event(&mut events).await else {
        panic!("connected again");
    };
    assert!(again.reconnected);
    for n in 2..=4 {
        assert_eq!(
            next_message(&mut events).await.payload_value().unwrap()["n"],
            n
        );
    }
    let Event::Subscribed(subscribed) = next_event(&mut events).await else {
        panic!("the subscription's answer");
    };
    assert_eq!(subscribed.recovery.len(), 1);
    assert!(subscribed.recovery[0].recovered);
    assert_eq!(subscribed.recovery[0].count, 3);
    stand.publisher.publish_local("chat", &say(5)).unwrap();
    assert_eq!(
        next_message(&mut events).await.payload_value().unwrap()["n"],
        5
    );
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn msgpack_reads_the_same_as_json() {
    let stand = stand().await;
    let mut config = stand.config("erin");
    config.format = Format::Msgpack;
    config.topics = vec!["prices".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    while !matches!(next_event(&mut events).await, Event::Subscribed(_)) {}
    stand
        .publisher
        .publish_local("prices", r#"{"t":"tick","p":{"px":1.25,"sym":"BTC"}}"#)
        .unwrap();
    let tick = next_message(&mut events).await;
    assert_eq!(tick.kind(), "tick");
    assert_eq!(tick.payload_json(), None, "msgpack, not JSON");
    #[derive(serde::Deserialize)]
    struct Tick<'a> {
        px: f64,
        sym: &'a str,
    }
    let decoded: Tick<'_> = tick.payload().unwrap();
    assert_eq!((decoded.px, decoded.sym), (1.25, "BTC"));
    assert!(tick.position().is_some());
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_token_is_an_error_of_connect() {
    let stand = stand().await;
    let mut config = stand.config("mallory");
    config.token = Some("not-a-token".into());
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, Error::Auth(_)), "{error}");
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_hears_messages_in_place() {
    let stand = stand().await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut config = stand.config("frank");
    config.topics = vec!["prices".into()];
    let client = wse_client::Client::connect(config, move |message: &wse_client::Message<'_>| {
        #[derive(serde::Deserialize)]
        struct Tick {
            px: u32,
        }
        if message.kind() == "tick" {
            let tick: Tick = message.payload().unwrap();
            let _ = tx.send(tick.px);
        }
    })
    .await
    .unwrap();
    // The subscription goes out right after the server took the connection.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for px in 0..100 {
        stand
            .publisher
            .publish_local("prices", &json!({"t": "tick", "p": {"px": px}}).to_string())
            .unwrap();
    }
    for px in 0..100 {
        let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, px);
    }
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_server_is_waited_for_and_the_client_comes_back() {
    let mut stand = stand().await;
    let (client, mut events) = wse_client::connect(stand.config("grace")).await.unwrap();
    stand.connect_id().await;
    let addr = stand
        .url
        .trim_start_matches("ws://")
        .trim_end_matches("/wse")
        .to_owned();
    stand.server.stop().await;
    let lost = loop {
        if let Event::Disconnected(lost) = next_event(&mut events).await {
            break lost;
        }
    };
    assert_eq!(lost.code, Some(1001));
    assert!(!client.is_connected());
    assert_eq!(client.send("x", &json!({})).await, Err(Error::NotConnected));
    // The same server again, on the same port.
    let listener = TcpListener::bind(&addr).await.unwrap();
    let (tx, _heard) = mpsc::unbounded_channel();
    stand.server.start(listener, App { heard: tx }).unwrap();
    loop {
        if let Event::Connected(again) = next_event(&mut events).await {
            assert!(again.reconnected);
            break;
        }
    }
    assert!(client.is_connected());
    client.close().await;
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
/// A server with an E2E identity key and a signing key.
async fn secure_stand() -> Stand {
    let mut config = wse::Config::default();
    config.e2e_identity_key = Some(wse_client::generate_key());
    config.signing_key = Some(wse_client::generate_key());
    stand_with(config).await
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_session_with_the_pinned_server() {
    let mut stand = secure_stand().await;
    let identity = stand.publisher.e2e_identity_public_key().unwrap();
    let mut config = stand.config("hana");
    config.encryption = Some(wse_client::Encryption::pinned([identity]));
    config.topics = vec!["secrets".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    while !matches!(next_event(&mut events).await, Event::Subscribed(_)) {}

    stand
        .publisher
        .publish_local("secrets", r#"{"t":"secret","p":{"code":42}}"#)
        .unwrap();
    let secret = next_message(&mut events).await;
    assert_eq!(secret.kind(), "secret");
    assert_eq!(secret.payload_value().unwrap()["code"], 42);
    assert!(secret.position().is_some());

    client.send("answer", &json!({"code": 43})).await.unwrap();
    loop {
        if let Heard::Message(message) = stand.heard().await {
            assert_eq!(message["t"], "answer");
            assert_eq!(message["p"]["code"], 43);
            break;
        }
    }
    client.close().await;
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_without_the_pinned_key_is_refused() {
    let stand = secure_stand().await;
    let other = wse_client::public_key(&wse_client::generate_key()).unwrap();
    let mut config = stand.config("ivan");
    config.encryption = Some(wse_client::Encryption::pinned([other]));
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, Error::Encryption(_)), "{error}");

    // Without a pin, only a client that allows it goes on.
    let mut config = stand.config("ivan");
    config.encryption = Some(wse_client::Encryption::default());
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, Error::Config(_)), "{error}");
    let mut config = stand.config("ivan");
    config.encryption = Some(wse_client::Encryption::unauthenticated());
    let (client, _events) = wse_client::connect(config).await.unwrap();
    client.close().await;
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_messages_both_ways() {
    let mut stand = secure_stand().await;
    let signing_key = wse_client::generate_key();
    // The token binds the client's key: the server takes what it signs.
    let mut claims = serde_json::Map::new();
    claims.insert(
        "cnf".into(),
        json!({"jwk": wse_client::public_key_jwk(&signing_key).unwrap()}),
    );
    let token = stand
        .publisher
        .issue_token("judy", Duration::from_secs(600), claims)
        .unwrap();
    let mut config = Config::new(&stand.url);
    config.token = Some(token);
    config.signing_key = Some(signing_key);
    config.server_signing_keys = vec![stand.publisher.signing_public_key().unwrap()];
    config.topics = vec!["fills".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    while !matches!(next_event(&mut events).await, Event::Subscribed(_)) {}

    // The client's order: the server application gets it with the proof,
    // and the client a signed receipt.
    client
        .send_signed(
            "order",
            &json!({"side": "buy", "qty": "1.5"}),
            Some("order-1"),
        )
        .await
        .unwrap();
    loop {
        if let Heard::Signed(order) = stand.heard().await {
            assert_eq!(order["t"], "order");
            assert_eq!(order["p"]["qty"], "1.5");
            assert_eq!(order["n"], "order-1");
            assert!(order["signature"]["body"].is_string());
            break;
        }
    }
    let receipt = next_message(&mut events).await;
    assert_eq!(receipt.kind(), "receipt");
    assert_eq!(receipt.payload_value().unwrap()["n"], "order-1");
    assert!(receipt.signature().unwrap().verified);

    // A fill the server signs for the topic.
    let fill = stand
        .publisher
        .sign("fill", json!({"px": "100.5"}), Some("fills"))
        .unwrap();
    stand.publisher.publish_local("fills", &fill).unwrap();
    let fill = next_message(&mut events).await;
    assert_eq!(fill.kind(), "fill");
    assert_eq!(fill.topic(), Some("fills"));
    assert!(fill.position().is_some());
    let proof = fill.signature().unwrap();
    assert!(proof.verified);
    assert!(proof.body.contains("100.5"));

    // Signed for another topic than it came on: dropped.
    let misplaced = stand
        .publisher
        .sign("fill", json!({"px": "1"}), Some("other"))
        .unwrap();
    stand.publisher.publish_local("fills", &misplaced).unwrap();
    stand
        .publisher
        .publish_local("fills", r#"{"t":"after"}"#)
        .unwrap();
    assert_eq!(next_message(&mut events).await.kind(), "after");
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_was_sent_before_close_goes_out() {
    let mut stand = stand().await;
    let (client, _events) = wse_client::connect(stand.config("alice")).await.unwrap();
    stand.connect_id().await;
    for n in 0..20 {
        client.send("bye", &json!({"n": n})).await.unwrap();
    }
    client.close().await;
    for n in 0..20 {
        loop {
            if let Heard::Message(message) = stand.heard().await {
                assert_eq!(message["t"], "bye");
                assert_eq!(message["p"]["n"], n);
                break;
            }
        }
    }
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_asked_for_happens_at_once_without_reconnects() {
    let mut stand = stand().await;
    let mut config = stand.config("alice");
    config.reconnect.enabled = false;
    config.reconnect.base_delay = Duration::from_secs(30);
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    let first = stand.connect_id().await;
    client.reconnect().unwrap();
    let second = stand.connect_id().await;
    assert_ne!(first, second);
    loop {
        if let Event::Connected(connected) = next_event(&mut events).await
            && connected.reconnected
        {
            break;
        }
    }
    assert_eq!(client.status(), Status::Connected);
    client.close().await;
    stand.server.stop().await;
}

/// Panics on the message `boom`.
struct Fragile;

impl wse_client::Handler for Fragile {
    fn message(&mut self, message: &wse_client::Message<'_>) {
        assert_ne!(message.kind(), "boom", "the handler's own bug");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_panics_ends_the_client_and_says_so() {
    let mut stand = stand().await;
    let client = wse_client::Client::connect(stand.config("alice"), Fragile)
        .await
        .unwrap();
    let conn_id = stand.connect_id().await;
    stand.publisher.send(&conn_id, r#"{"t":"boom"}"#).unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), client.closed())
        .await
        .expect("closed within 5 s");
    assert_eq!(closed, Some(Error::Panicked));
    assert!(!client.is_connected());
    assert!(matches!(
        client.status(),
        Status::Closed(Some(Error::Panicked))
    ));
    stand.server.stop().await;
}

/// Keeps the client it was given, as a bot that answers from its handler.
struct Keeper {
    _client: wse_client::Client,
}

impl wse_client::Handler for Keeper {
    fn message(&mut self, _message: &wse_client::Message<'_>) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_client_a_handler_keeps_does_not_keep_the_connection() {
    let mut stand = stand().await;
    let client = wse_client::Client::connect_with(stand.config("alice"), |client| Keeper {
        _client: client,
    })
    .await
    .unwrap();
    let conn_id = stand.connect_id().await;
    drop(client);
    loop {
        if let Heard::Disconnect(gone) = stand.heard().await {
            assert_eq!(gone, conn_id);
            break;
        }
    }
    stand.server.stop().await;
}
