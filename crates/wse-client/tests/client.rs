//! The Rust client against the WSE server, both in this process.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use wse_client::{
    Config, Error, Event, Events, Format, Incoming, Offline, OwnedMessage, SendOptions, Status,
    TokenProvider,
};

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
    fn token(&self, user: &str) -> String {
        self.publisher
            .issue_token(user, Duration::from_secs(600), serde_json::Map::new())
            .unwrap()
    }

    fn config(&self, user: &str) -> Config {
        let mut config = Config::new(&self.url);
        config.token = Some(self.token(user));
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

/// The network between a client and the server: it can stop taking what the
/// client writes, and it can be cut.
struct Link {
    url: String,
    held: Arc<AtomicBool>,
    cut: broadcast::Sender<()>,
}

impl Link {
    async fn to(server_url: &str) -> Self {
        let server = server_url
            .trim_start_matches("ws://")
            .trim_end_matches("/wse")
            .to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/wse", listener.local_addr().unwrap());
        let held = Arc::new(AtomicBool::new(false));
        let (cut, _) = broadcast::channel(1);
        let (link_held, link_cut) = (Arc::clone(&held), cut.clone());
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(&server).await else {
                    continue;
                };
                let held = Arc::clone(&link_held);
                let mut cut = link_cut.subscribe();
                tokio::spawn(async move {
                    let (mut from_client, mut to_client) = client.into_split();
                    let (mut from_server, mut to_server) = server.into_split();
                    let up = async {
                        let mut buf = vec![0u8; 16 * 1024];
                        loop {
                            while held.load(Ordering::Acquire) {
                                tokio::time::sleep(Duration::from_millis(5)).await;
                            }
                            let n = from_client.read(&mut buf).await?;
                            if n == 0 {
                                return Ok::<_, std::io::Error>(());
                            }
                            to_server.write_all(&buf[..n]).await?;
                        }
                    };
                    let down = tokio::io::copy(&mut from_server, &mut to_client);
                    // Whichever ends first, both sockets go with this task.
                    tokio::select! {
                        _ = up => {}
                        _ = down => {}
                        _ = cut.recv() => {}
                    }
                });
            }
        });
        Self { url, held, cut }
    }

    fn hold(&self, held: bool) {
        self.held.store(held, Ordering::Release);
    }

    fn cut(&self) {
        let _ = self.cut.send(());
    }
}

/// `len` characters no compression shrinks much.
fn noise(len: usize) -> String {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

async fn next_connected(events: &mut Events) -> wse_client::Connected {
    loop {
        if let Event::Connected(connected) = next_event(events).await {
            return connected;
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
async fn a_request_names_its_correlation_id_and_priority_kept_or_live() {
    let mut stand = stand().await;
    let (client, _events) = wse_client::connect(stand.config("erin")).await.unwrap();
    let mut options = SendOptions::default();
    options.correlation_id = Some("order-7".to_owned());
    options.priority = Some(8);
    client
        .send_with("ask", &json!({"n": 1}), &options)
        .await
        .unwrap();
    options.correlation_id = Some("order-8".to_owned());
    options.priority = None;
    options.live = true;
    let id = client
        .try_send_with("ask", &json!({"n": 2}), &options)
        .unwrap();
    // A live message goes straight to the connection, a kept one through
    // the outbox: either may come first.
    let mut got = std::collections::HashMap::new();
    while got.len() < 2 {
        if let Heard::Message(message) = stand.heard().await {
            got.insert(
                message["cid"].as_str().unwrap_or_default().to_owned(),
                message,
            );
        }
    }
    assert_eq!(got["order-7"]["pri"], 8);
    assert!(got["order-8"].get("pri").is_none());
    assert_eq!(got["order-8"]["id"], id.as_str());
    // A live message waits for nothing: no ttl, no topics to wait for.
    options.ttl = Some(Duration::from_secs(1));
    assert!(matches!(
        client.try_send_with("ask", &json!({}), &options),
        Err(Error::Config(_))
    ));
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
    // Sent meanwhile: it waits in the outbox for the next connection.
    client.send("meanwhile", &json!({})).await.unwrap();
    // The same server again, on the same port.
    let listener = TcpListener::bind(&addr).await.unwrap();
    let (tx, mut heard) = mpsc::unbounded_channel();
    stand.server.start(listener, App { heard: tx }).unwrap();
    loop {
        if let Event::Connected(again) = next_event(&mut events).await {
            assert!(again.reconnected);
            break;
        }
    }
    assert!(client.is_connected());
    loop {
        let got = tokio::time::timeout(Duration::from_secs(5), heard.recv())
            .await
            .expect("the application hears within 5 s")
            .expect("the application alive");
        if let Heard::Message(message) = got {
            assert_eq!(message["t"], "meanwhile");
            break;
        }
    }
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
    // A token naming the key binds the session to it: opened end to end.
    config.encryption = Some(wse_client::Encryption::pinned([stand
        .publisher
        .e2e_identity_public_key()
        .unwrap()]));
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

    // Signed for one user: taken by that user's client only.
    let theirs = stand
        .publisher
        .sign_to("mallory", "fill", json!({"px": "2"}))
        .unwrap();
    stand.publisher.send_to_user("judy", &theirs).unwrap();
    let mine = stand
        .publisher
        .sign_to("judy", "fill", json!({"px": "3"}))
        .unwrap();
    stand.publisher.send_to_user("judy", &mine).unwrap();
    let fill = next_message(&mut events).await;
    assert_eq!(fill.payload_value().unwrap()["px"], "3");
    assert!(fill.signature().unwrap().body.contains(r#""to":"judy""#));
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

/// A token for `user` naming `cnf`, and a client config with an end-to-end
/// session pinned to the stand's identity and `signing_key`.
#[cfg(feature = "aws-lc")]
fn bound_config(stand: &Stand, user: &str, cnf: serde_json::Value, signing_key: &[u8]) -> Config {
    let mut claims = serde_json::Map::new();
    claims.insert("cnf".into(), cnf);
    let token = stand
        .publisher
        .issue_token(user, Duration::from_secs(600), claims)
        .unwrap();
    let mut config = Config::new(&stand.url);
    config.token = Some(token);
    config.signing_key = Some(signing_key.to_vec());
    config.encryption = Some(wse_client::Encryption::pinned([stand
        .publisher
        .e2e_identity_public_key()
        .unwrap()]));
    config.reconnect.enabled = false;
    config
}

#[cfg(feature = "aws-lc")]
fn thumbprint(signing_key: &[u8]) -> String {
    use base64::Engine;
    let point = base64::engine::general_purpose::STANDARD
        .decode(wse_client::public_key(signing_key).unwrap())
        .unwrap();
    wse::protocol::signing::thumbprint(&point).unwrap()
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_is_bound_to_the_key_its_token_names() {
    let mut stand = secure_stand().await;
    let key = wse_client::generate_key();
    // Named by the key itself, then by its thumbprint.
    for cnf in [
        json!({"jwk": wse_client::public_key_jwk(&key).unwrap()}),
        json!({"jkt": thumbprint(&key)}),
    ] {
        let config = bound_config(&stand, "kate", cnf.clone(), &key);
        let (client, _events) = wse_client::connect(config)
            .await
            .expect("bound and admitted");
        stand.connect_id().await;
        client.send("hello", &json!({})).await.unwrap();
        loop {
            if let Heard::Message(message) = stand.heard().await {
                assert_eq!(message["t"], "hello", "{cnf}");
                break;
            }
        }
        client.close().await;
    }
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_copied_without_its_key_opens_nothing() {
    let mut stand = secure_stand().await;
    let owner = wse_client::generate_key();
    let thief = wse_client::generate_key();
    let cnf = json!({"jwk": wse_client::public_key_jwk(&owner).unwrap()});
    // The token, with another key: no proof of the one it names.
    let config = bound_config(&stand, "leo", cnf.clone(), &thief);
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, Error::Auth(_)), "{error}");
    // The token over a connection without a session: refused at once.
    let mut config = bound_config(&stand, "leo", cnf, &thief);
    config.encryption = None;
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, Error::Auth(_)), "{error}");
    // The application never heard of either.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), stand.heard.recv())
            .await
            .is_err()
    );
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cnf_that_names_no_key_is_refused() {
    let stand = secure_stand().await;
    let key = wse_client::generate_key();
    for cnf in [json!({"jkt": "short"}), json!({"x5t#S256": "AAAA"})] {
        let config = bound_config(&stand, "mia", cnf.clone(), &key);
        let error = wse_client::connect(config).await.err().unwrap();
        assert!(matches!(error, Error::Auth(_)), "{cnf}: {error}");
    }
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_bound_to_its_key_needs_a_token_that_names_it() {
    let mut stand = secure_stand().await;
    let key = wse_client::generate_key();
    // A token naming no key, as one put in place of the client's own would.
    let mut config = stand.config("noor");
    config.signing_key = Some(key.clone());
    config.encryption = Some(wse_client::Encryption::pinned([stand
        .publisher
        .e2e_identity_public_key()
        .unwrap()]));
    config.reconnect.enabled = false;
    let error = wse_client::connect(config.clone()).await.err().unwrap();
    assert!(matches!(error, Error::Auth(_)), "{error}");
    // Keys the application binds itself: the session is not bound.
    if let Some(encryption) = config.encryption.as_mut() {
        encryption.bind_session = false;
    }
    let (client, _events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;
    client.close().await;
    stand.server.stop().await;
}

/// An application that checks its connections itself and knows its user's
/// key: it binds each connection to that key.
#[cfg(feature = "aws-lc")]
struct KeyedApp {
    publisher: wse::Publisher,
    key: String,
    heard: mpsc::UnboundedSender<Heard>,
}

#[cfg(feature = "aws-lc")]
impl wse::Handler for KeyedApp {
    type Connection = String;

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<String> {
        let conn_id = (**connect.conn_id).clone();
        let _ = self.publisher.accept_bound(&conn_id, "olga", &self.key);
        let _ = self.heard.send(Heard::Connect(conn_id.clone()));
        Some(conn_id)
    }

    fn message(&self, _conn: &mut String, message: wse::Inbound, _size: usize) -> bool {
        if let wse::Inbound::Json(value) = message {
            let _ = self.heard.send(Heard::Message(value));
        }
        true
    }

    fn signed(&self, _conn: &mut String, _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, conn: String) {
        let _ = self.heard.send(Heard::Disconnect(conn));
    }
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_application_binds_a_connection_to_the_key_it_knows() {
    let key = wse_client::generate_key();
    let mut config = wse::Config::default();
    config.e2e_identity_key = Some(wse_client::generate_key());
    let server = wse::Server::new(config).unwrap();
    let publisher = server.publisher();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    let (tx, mut heard) = mpsc::unbounded_channel();
    server
        .start(
            listener,
            KeyedApp {
                publisher: publisher.clone(),
                key: wse_client::public_key(&key).unwrap(),
                heard: tx,
            },
        )
        .unwrap();
    let identity = publisher.e2e_identity_public_key().unwrap();
    let client_config = |signing_key: &[u8]| {
        let mut config = Config::new(&url);
        config.signing_key = Some(signing_key.to_vec());
        config.encryption = Some(wse_client::Encryption::pinned([identity.clone()]));
        config.reconnect.enabled = false;
        config
    };

    // The key the application knows: admitted, its messages heard.
    let (client, _events) = wse_client::connect(client_config(&key)).await.unwrap();
    client.send("hello", &json!({})).await.unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(5), heard.recv())
            .await
            .unwrap()
            .unwrap();
        if let Heard::Message(message) = event {
            assert_eq!(message["t"], "hello");
            break;
        }
    }
    client.close().await;

    // Another key: refused.
    let other = wse_client::generate_key();
    let error = wse_client::connect(client_config(&other))
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Auth(_)), "{error}");
    server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_session_outlives_a_short_burst_over_the_rate_limit() {
    let mut config = wse::Config::default();
    config.e2e_identity_key = Some(wse_client::generate_key());
    config.rate_limit_capacity = 4.0;
    config.rate_limit_refill = 2.0;
    let mut stand = stand_with(config).await;
    let identity = stand.publisher.e2e_identity_public_key().unwrap();
    let mut client_config = stand.config("lena");
    client_config.encryption = Some(wse_client::Encryption::pinned([identity]));
    let (client, _events) = wse_client::connect(client_config).await.unwrap();
    stand.connect_id().await;

    // More than the bucket holds at once: the server refuses some of them.
    for n in 0..8 {
        client.send("burst", &json!({ "n": n })).await.unwrap();
    }
    // Once it refilled, the same session goes on.
    tokio::time::sleep(Duration::from_millis(3_000)).await;
    client.send("after", &json!({ "ok": true })).await.unwrap();
    loop {
        match stand.heard().await {
            Heard::Message(message) if message["t"] == "after" => break,
            Heard::Disconnect(_) => panic!("the server dropped the session after the burst"),
            _ => {}
        }
    }
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_writing_flat_out_hears_why_it_was_closed() {
    let mut config = wse::Config::default();
    config.rate_limit_capacity = 4.0;
    config.rate_limit_refill = 2.0;
    let stand = stand_with(config).await;
    let mut client_config = stand.config("mira");
    client_config.reconnect.base_delay = Duration::from_secs(30);
    let (client, mut events) = wse_client::connect(client_config).await.unwrap();

    // Writing when the server closes: its close frame still names why. A
    // full outbox (its messages not answered yet) is waited out.
    let flooder = client.clone();
    let flood = tokio::spawn(async move {
        let mut n = 0u64;
        loop {
            match flooder.send("flood", &json!({ "n": n })).await {
                Ok(()) => n += 1,
                Err(Error::Full) => tokio::time::sleep(Duration::from_millis(1)).await,
                Err(_) => break,
            }
        }
    });
    let disconnected = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(incoming) = events.next().await {
            if let Incoming::Event(Event::Disconnected(disconnected)) = incoming {
                return disconnected;
            }
        }
        panic!("the events ended before the client was disconnected");
    })
    .await
    .expect("closed within 15 s");
    assert_eq!(disconnected.code, Some(4429), "{disconnected:?}");
    flood.abort();
    client.close().await;
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_encrypted_flood_is_still_closed_for_flooding() {
    let mut config = wse::Config::default();
    config.e2e_identity_key = Some(wse_client::generate_key());
    config.rate_limit_capacity = 4.0;
    config.rate_limit_refill = 2.0;
    let stand = stand_with(config).await;
    let identity = stand.publisher.e2e_identity_public_key().unwrap();
    let mut client_config = stand.config("mira");
    client_config.encryption = Some(wse_client::Encryption::pinned([identity]));
    client_config.reconnect.base_delay = Duration::from_secs(30);
    let (client, mut events) = wse_client::connect(client_config).await.unwrap();

    let started = std::time::Instant::now();
    let flooder = client.clone();
    let flood = tokio::spawn(async move {
        for n in 0u64.. {
            if flooder.try_send("flood", &json!({ "n": n })).is_err() && !flooder.is_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    // The server closes it after ten seconds over the limit, as any flood
    // (4429), not on the first frame after a refused one (4802); the client
    // says so though it was writing when the server closed.
    let disconnected = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(incoming) = events.next().await {
            if let Incoming::Event(Event::Disconnected(disconnected)) = incoming {
                return disconnected;
            }
        }
        panic!("the events ended before the client was disconnected");
    })
    .await
    .expect("closed within 15 s");
    let after = started.elapsed();
    assert!(after >= Duration::from_secs(9), "closed after {after:?}");
    assert_eq!(disconnected.code, Some(4429), "{disconnected:?}");
    flood.abort();
    client.close().await;
    stand.server.stop().await;
}

/// Sends until the client's queue is full: the link has stopped taking what
/// the client writes, and the client stopped taking it from the queue.
fn fill_queue(client: &wse_client::Client, kind: &str, payload: &serde_json::Value) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match client.try_send(kind, payload) {
            Ok(()) => {}
            Err(Error::Full) => return,
            Err(error) => panic!("{error}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the queue never filled"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_one_user_queued_does_not_go_out_for_the_next() {
    let mut stand = stand().await;
    let link = Link::to(&stand.url).await;
    let token = Arc::new(Mutex::new(stand.token("alice")));
    let current = Arc::clone(&token);
    let mut config = Config::new(&link.url);
    config.token_provider = Some(TokenProvider::new(move || {
        let token = current.lock().unwrap().clone();
        async move { Ok::<_, String>(token) }
    }));
    config.reconnect.base_delay = Duration::from_millis(50);
    config.send_queue = 4;
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;

    // Alice's orders back up in the client while the network holds them.
    link.hold(true);
    fill_queue(&client, "alice_order", &json!({ "pad": noise(64 * 1024) }));

    // The network goes; the next connection is Bob's.
    *token.lock().unwrap() = stand.token("bob");
    link.cut();
    while !matches!(next_event(&mut events).await, Event::Disconnected(_)) {}
    link.hold(false);
    let again = next_connected(&mut events).await;
    assert_eq!(again.user_id.as_deref(), Some("bob"));
    client.send("bob_hello", &json!({})).await.unwrap();
    loop {
        if let Heard::Message(message) = stand.heard().await {
            assert_eq!(message["t"], "bob_hello", "Alice's order went out for Bob");
            break;
        }
    }
    client.close().await;
    stand.server.stop().await;
}

#[cfg(feature = "aws-lc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_message_goes_on_its_own_connection_or_not_at_all() {
    let mut stand = secure_stand().await;
    let link = Link::to(&stand.url).await;
    let signing_key = wse_client::generate_key();
    let cnf = json!({ "jwk": wse_client::public_key_jwk(&signing_key).unwrap() });
    let mut config = bound_config(&stand, "judy", cnf, &signing_key);
    config.url = link.url.clone();
    config.reconnect.enabled = true;
    config.reconnect.base_delay = Duration::from_millis(50);
    config.send_queue = 4;
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;

    // Orders signed for this connection back up while the network holds
    // them.
    link.hold(true);
    let order = json!({ "pad": noise(64 * 1024) });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    for n in 0.. {
        let nonce = format!("order-{n}");
        let sent = tokio::time::timeout(
            Duration::from_millis(300),
            client.send_signed("order", &order, Some(&nonce)),
        )
        .await;
        match sent {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("{error}"),
            // The queue is full: the client stopped taking from it.
            Err(_) => break,
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the queue never filled"
        );
    }

    // The network goes and comes back: the same user, another connection.
    link.cut();
    while !matches!(next_event(&mut events).await, Event::Disconnected(_)) {}
    link.hold(false);
    next_connected(&mut events).await;
    client.send("after", &json!({})).await.unwrap();
    loop {
        match stand.heard().await {
            Heard::Signed(order) => {
                panic!("{} went out on the next connection", order["n"])
            }
            Heard::Message(message) => {
                assert_eq!(message["t"], "after");
                break;
            }
            _ => {}
        }
    }
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_message_goes_on_its_own_connection_or_not_at_all() {
    let mut stand = stand().await;
    let link = Link::to(&stand.url).await;
    let mut config = Config::new(&link.url);
    config.token = Some(stand.token("vera"));
    config.reconnect.base_delay = Duration::from_millis(50);
    config.send_queue = 4;
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;

    // Commands for this connection back up while the network holds them.
    link.hold(true);
    let command = json!({ "pad": noise(64 * 1024) });
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match client.try_send_live("valve", &command) {
            Ok(()) => {}
            Err(Error::Full) => break,
            Err(error) => panic!("{error}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the queue never filled"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    // The network goes and comes back: none of them goes out late.
    link.cut();
    while !matches!(next_event(&mut events).await, Event::Disconnected(_)) {}
    assert!(matches!(
        client.try_send_live("valve", &command),
        Err(Error::NotConnected)
    ));
    link.hold(false);
    next_connected(&mut events).await;
    client.send("after", &json!({})).await.unwrap();
    loop {
        if let Heard::Message(message) = stand.heard().await {
            assert_eq!(
                message["t"], "after",
                "a command went out on the next connection"
            );
            break;
        }
    }
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_close_past_its_deadline_drops_a_stuck_connection() {
    let mut stand = stand().await;
    let link = Link::to(&stand.url).await;
    let mut config = Config::new(&link.url);
    config.token = Some(stand.token("dee"));
    config.send_queue = 4;
    let (client, _events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;
    // The network takes nothing: what is queued cannot go out.
    link.hold(true);
    fill_queue(&client, "stuck", &json!({ "pad": noise(64 * 1024) }));
    let started = std::time::Instant::now();
    client.close_within(Duration::from_millis(300)).await;
    let took = started.elapsed();
    assert!(took < Duration::from_secs(2), "closed after {took:?}");
    assert!(matches!(client.status(), wse_client::Status::Closed(_)));
    assert!(!client.is_connected());
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_client_comes_back_with_what_it_missed() {
    let mut stand = stand().await;
    let mut config = stand.config("paula");
    config.topics = vec!["chat".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    stand.connect_id().await;
    let say = |n: u32| json!({"t": "said", "p": {"n": n}}).to_string();
    while !matches!(next_event(&mut events).await, Event::Subscribed(_)) {}
    stand.publisher.publish_local("chat", &say(1)).unwrap();
    assert_eq!(
        next_message(&mut events).await.payload_value().unwrap()["n"],
        1
    );

    // Two reasons: the connection ends and stays down until both are gone.
    client.pause("background").unwrap();
    client.pause("manual").unwrap();
    let Event::Paused(paused) = next_event(&mut events).await else {
        panic!("paused");
    };
    assert_eq!(paused.reasons, ["background"]);
    assert_eq!(client.status(), wse_client::Status::Paused);
    assert!(!client.is_connected());
    for n in 2..=3 {
        stand.publisher.publish_local("chat", &say(n)).unwrap();
    }
    client.resume("background").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(client.status(), wse_client::Status::Paused);

    // The last reason gone: back at once, with what it missed.
    client.resume("manual").unwrap();
    let Event::Connected(again) = next_event(&mut events).await else {
        panic!("connected again");
    };
    assert!(again.reconnected);
    for n in 2..=3 {
        assert_eq!(
            next_message(&mut events).await.payload_value().unwrap()["n"],
            n
        );
    }
    let Event::Subscribed(subscribed) = next_event(&mut events).await else {
        panic!("the subscription's answer");
    };
    assert!(subscribed.recovery[0].recovered);
    client.close().await;
    stand.server.stop().await;
}

/// A client whose first connection never comes: its token never does.
fn never_connected() -> Config {
    let mut config = Config::new("ws://127.0.0.1:9/wse");
    config.token_provider = Some(TokenProvider::new(|| {
        std::future::pending::<Result<String, String>>()
    }));
    config
}

/// A send cancelled while it waits for room in the queue gives back the
/// place it took in the outbox.
#[tokio::test]
async fn a_send_cancelled_while_it_waits_gives_its_place_back() {
    let mut config = never_connected();
    config.send_queue = 1;
    let mut offline = Offline::in_memory();
    offline.max_messages = 2;
    config.offline = Some(offline);
    let (client, _events) = wse_client::start(config).await.unwrap();
    // One thread: the driver has not taken the first yet, the queue is full.
    client.try_send("first", &()).unwrap();
    {
        let send = client.send("cancelled", &());
        tokio::pin!(send);
        std::future::poll_fn(|cx| {
            assert!(send.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert_eq!(client.kept(), 2);
    }
    client.clear().unwrap();
    for _ in 0..100 {
        if client.kept() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(client.kept(), 0, "nothing kept for the cancelled send");
    client.close().await;
}

/// A send that waits for its topics counts its time from its send: it runs
/// out while it waits, and the application hears so.
#[tokio::test]
async fn a_send_that_waits_for_its_topics_runs_out_in_its_time() {
    let mut config = never_connected();
    config.offline = Some(Offline::in_memory());
    let (client, mut events) = wse_client::start(config).await.unwrap();
    let mut options = SendOptions::default();
    options.id = Some("late-order".into());
    options.requires = vec!["prices".into()];
    options.ttl = Some(Duration::from_millis(5));
    client.send_with("order", &(), &options).await.unwrap();
    let expired = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(Incoming::Event(Event::NotDelivered(n))) = events.next().await {
                return n;
            }
        }
    })
    .await
    .expect("not delivered within 5 s");
    assert_eq!(
        (expired.reason.as_str(), expired.ids.as_slice()),
        ("expired", &["late-order".to_owned()][..])
    );
    assert_eq!(client.kept(), 0);
    client.close().await;
}
