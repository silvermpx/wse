//! A Rust application serving its clients: its own handler, the server on
//! its runtime, a WebSocket client on the other side.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
enum Event {
    Connect(String),
    Message(serde_json::Value),
    Disconnect(String),
}

/// Tells the test what the server told it.
struct Recorder {
    events: mpsc::UnboundedSender<Event>,
}

impl wse::Handler for Recorder {
    type Connection = String;

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<String> {
        let conn_id = (**connect.conn_id).clone();
        let _ = self.events.send(Event::Connect(conn_id.clone()));
        Some(conn_id)
    }

    fn message(&self, _conn: &mut String, message: wse::Inbound, _size: usize) -> bool {
        if let wse::Inbound::Json(value) = message {
            let _ = self.events.send(Event::Message(value));
        }
        true
    }

    fn signed(&self, _conn: &mut String, _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, conn: String) {
        let _ = self.events.send(Event::Disconnect(conn));
    }
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The next text the server sent, its pings aside.
async fn next_text(ws: &mut Client) -> String {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("a frame within 5 s")
            .expect("the connection open")
            .expect("a frame");
        match frame {
            Message::Text(text) => {
                if !text.contains("\"t\":\"ping\"") {
                    return text.to_string();
                }
            }
            Message::Binary(data) => return String::from_utf8_lossy(&data).into_owned(),
            Message::Close(frame) => panic!("closed: {frame:?}"),
            _ => {}
        }
    }
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("an event within 5 s")
        .expect("the handler alive")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rust_application_serves_its_clients() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut events) = mpsc::unbounded_channel();
    server.start(listener, Recorder { events: tx }).unwrap();
    assert!(server.is_running());
    let publisher = server.publisher();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
        .await
        .unwrap();
    // Without a JWT configuration the application accepts the connection,
    // and only then does the client get server_ready.
    let Event::Connect(conn_id) = next_event(&mut events).await else {
        panic!("a connect event first");
    };
    assert!(publisher.accept(&conn_id, "alice").unwrap());
    let ready = next_text(&mut ws).await;
    assert!(ready.contains("server_ready"), "{ready}");

    // The client's message reaches the handler.
    ws.send(Message::Text(r#"{"t":"hello","p":{"n":1}}"#.into()))
        .await
        .unwrap();
    let Event::Message(message) = next_event(&mut events).await else {
        panic!("its message");
    };
    assert_eq!(message["t"], "hello");
    assert_eq!(message["p"]["n"], 1);

    // To the connection, then to everyone.
    publisher.send(&conn_id, r#"{"t":"direct"}"#).unwrap();
    assert!(next_text(&mut ws).await.contains("direct"));
    publisher.broadcast(r#"{"t":"everyone"}"#).unwrap();
    assert!(next_text(&mut ws).await.contains("everyone"));

    // Stopped: the client is told the server goes away, the handler hears
    // of its end, and sends fail.
    server.stop().await;
    assert!(!server.is_running());
    let mut close_code = None;
    while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        if let Ok(Message::Close(Some(frame))) = frame {
            close_code = Some(u16::from(frame.code));
            break;
        }
    }
    assert_eq!(close_code, Some(1001));
    let Event::Disconnect(gone) = next_event(&mut events).await else {
        panic!("its disconnect");
    };
    assert_eq!(gone, conn_id);
    assert_eq!(publisher.broadcast("{}"), Err(wse::Error::Stopped));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_starts_once_and_again_after_stop() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let (tx, _events) = mpsc::unbounded_channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    server
        .start(listener, Recorder { events: tx.clone() })
        .unwrap();
    let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(
        server.start(second, Recorder { events: tx.clone() }),
        Err(wse::Error::Running)
    );
    server.stop().await;
    let again = TcpListener::bind("127.0.0.1:0").await.unwrap();
    server.start(again, Recorder { events: tx }).unwrap();
    assert!(server.publisher().broadcast("{}").is_ok());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_ends_the_run_while_connections_keep_coming() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let (tx, _events) = mpsc::unbounded_channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    server.start(listener, Recorder { events: tx }).unwrap();
    // A connection every few milliseconds, never a pause of a tenth of a
    // second: the stop does not wait for them to stop coming.
    let knocking = tokio::spawn(async move {
        loop {
            let _ = tokio::net::TcpStream::connect(addr).await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let stopped = tokio::time::timeout(Duration::from_secs(2), server.stop()).await;
    knocking.abort();
    assert!(
        stopped.is_ok(),
        "the stop waited for the connections to stop coming"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drained_server_takes_no_one_once_drain_returns() {
    for _ in 0..10 {
        let server = wse::Server::new(wse::Config::default()).unwrap();
        let (tx, _events) = mpsc::unbounded_channel();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        server.start(listener, Recorder { events: tx }).unwrap();
        let dropped = server
            .drain(1001, "moving", Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(dropped, 0);
        assert!(server.publisher().is_draining());
        server.stop().await;
    }
}

#[test]
fn a_configuration_is_checked_as_a_whole() {
    let mut config = wse::Config::default();
    config.ping_interval = 60;
    config.idle_timeout = 30;
    let error = wse::Server::new(config).err().unwrap();
    assert_eq!(
        error.to_string(),
        "ping_interval (60s) must be less than idle_timeout (30s)"
    );
}

/// Subscribes each connection to its user's topic as it comes in.
struct UserTopics;

impl wse::Handler for UserTopics {
    type Connection = ();

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<()> {
        let topics = vec![format!("user:{}", connect.user_id.unwrap_or("anonymous"))];
        let taken = connect.subscribe(topics, &wse::Subscribe::default()).ok()?;
        assert_eq!(taken.len(), 1);
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, _conn: ()) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_subscribes_its_connections_as_they_come() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    server.start(listener, UserTopics).unwrap();
    let publisher = server.publisher();

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
        .await
        .unwrap();
    let conn_id = loop {
        if let Some(conn_id) = publisher.connections().pop() {
            break conn_id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(publisher.accept(&conn_id, "anonymous").unwrap());
    assert!(next_text(&mut ws).await.contains("server_ready"));
    assert_eq!(publisher.topic_subscriber_count("user:anonymous"), 1);

    // Its own topic reaches it, another one does not.
    publisher
        .publish_local("user:someone-else", r#"{"t":"not-yours"}"#)
        .unwrap();
    publisher
        .publish_local("user:anonymous", r#"{"t":"yours"}"#)
        .unwrap();
    assert!(next_text(&mut ws).await.contains("yours"));

    // Subscribed later from the application's own code.
    let taken = publisher
        .subscribe(&conn_id, vec!["prices".into()], &wse::Subscribe::default())
        .unwrap();
    assert_eq!(taken, vec!["prices".to_string()]);
    publisher
        .publish_local("prices", r#"{"t":"tick"}"#)
        .unwrap();
    assert!(next_text(&mut ws).await.contains("tick"));

    publisher.unsubscribe(&conn_id, None);
    assert_eq!(publisher.topic_subscriber_count("prices"), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_user_is_reached_on_every_connection_it_has() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut events) = mpsc::unbounded_channel();
    server.start(listener, Recorder { events: tx }).unwrap();
    let publisher = server.publisher();

    // Two tabs of alice, one of bob.
    let mut tabs = Vec::new();
    for user in ["alice", "alice", "bob"] {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
            .await
            .unwrap();
        let Event::Connect(conn_id) = next_event(&mut events).await else {
            panic!("a connect event");
        };
        assert!(publisher.accept(&conn_id, user).unwrap());
        assert!(next_text(&mut ws).await.contains("server_ready"));
        tabs.push(ws);
    }
    assert_eq!(publisher.user_connections("alice").len(), 2);

    assert_eq!(
        publisher
            .send_to_user("alice", r#"{"t":"for-alice"}"#)
            .unwrap(),
        2
    );
    assert!(next_text(&mut tabs[0]).await.contains("for-alice"));
    assert!(next_text(&mut tabs[1]).await.contains("for-alice"));
    publisher.send_to_user("bob", r#"{"t":"for-bob"}"#).unwrap();
    assert!(next_text(&mut tabs[2]).await.contains("for-bob"));

    // Logged out: both of alice's tabs close, bob's stays.
    assert_eq!(publisher.disconnect_user("alice").unwrap(), 2);
    for ws in &mut tabs[..2] {
        let mut code = None;
        while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            if let Ok(Message::Close(Some(frame))) = frame {
                code = Some(u16::from(frame.code));
                break;
            }
        }
        assert_eq!(code, Some(1000));
        // Read on: the client answers the close, and the server lets go.
        while let Ok(Some(_)) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await {}
    }
    for _ in 0..2 {
        assert!(matches!(
            next_event(&mut events).await,
            Event::Disconnect(_)
        ));
    }
    assert!(publisher.user_connections("alice").is_empty());
    assert_eq!(publisher.user_connections("bob").len(), 1);
    server.stop().await;
}

/// Panics on the message `boom`, and tells the test of everything else.
struct Fragile {
    events: mpsc::UnboundedSender<Event>,
}

impl wse::Handler for Fragile {
    type Connection = String;

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<String> {
        let conn_id = (**connect.conn_id).clone();
        let _ = self.events.send(Event::Connect(conn_id.clone()));
        Some(conn_id)
    }

    fn message(&self, _conn: &mut String, message: wse::Inbound, _size: usize) -> bool {
        if let wse::Inbound::Json(value) = message {
            assert_ne!(value["t"], "boom", "the handler's own bug");
            let _ = self.events.send(Event::Message(value));
        }
        true
    }

    fn signed(&self, _conn: &mut String, _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, conn: String) {
        let _ = self.events.send(Event::Disconnect(conn));
    }
}

/// A client the application accepted: its connection id and its socket.
async fn accepted_client(
    addr: std::net::SocketAddr,
    publisher: &wse::Publisher,
    events: &mut mpsc::UnboundedReceiver<Event>,
) -> (String, Client) {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
        .await
        .unwrap();
    let Event::Connect(conn_id) = next_event(events).await else {
        panic!("a connect event first");
    };
    assert!(publisher.accept(&conn_id, "alice").unwrap());
    assert!(next_text(&mut ws).await.contains("server_ready"));
    (conn_id, ws)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_that_panics_closes_its_connection_alone() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut events) = mpsc::unbounded_channel();
    server.start(listener, Fragile { events: tx }).unwrap();
    let publisher = server.publisher();
    let (doomed_id, mut doomed) = accepted_client(addr, &publisher, &mut events).await;
    let (_, mut other) = accepted_client(addr, &publisher, &mut events).await;

    doomed
        .send(Message::Text(r#"{"t":"boom"}"#.into()))
        .await
        .unwrap();
    let mut close_code = None;
    while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(5), doomed.next()).await {
        if let Ok(Message::Close(Some(frame))) = frame {
            close_code = Some(u16::from(frame.code));
            break;
        }
    }
    assert_eq!(close_code, Some(1011));
    // The client answers the close (reading on sends its reply).
    while let Ok(Some(_)) = tokio::time::timeout(Duration::from_secs(5), doomed.next()).await {}
    // Cleaned up, and the application still hears of its end.
    let Event::Disconnect(gone) = next_event(&mut events).await else {
        panic!("its disconnect");
    };
    assert_eq!(gone, doomed_id);
    assert_eq!(publisher.connection_count(), 1);

    // The other connection goes on, and new ones come in.
    other
        .send(Message::Text(r#"{"t":"fine"}"#.into()))
        .await
        .unwrap();
    let Event::Message(message) = next_event(&mut events).await else {
        panic!("its message");
    };
    assert_eq!(message["t"], "fine");
    let (_, _again) = accepted_client(addr, &publisher, &mut events).await;
    assert_eq!(publisher.connection_count(), 2);
    server.stop().await;
}

/// Puts every connection on `room` with presence, and tells the test of the
/// presence changes.
struct Present {
    changes: mpsc::UnboundedSender<wse::Presence>,
}

impl wse::Handler for Present {
    type Connection = ();

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<()> {
        let presence = wse::Subscribe::presence(serde_json::json!({"status": "here"}));
        connect.subscribe(vec!["room".into()], &presence).ok()?;
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, _conn: ()) {}

    fn presence(&self, event: wse::Presence) {
        let _ = self.changes.send(event);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn presence_asked_for_in_connect_is_tracked() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(
        "a-secret-of-at-least-thirty-two-bytes",
    ));
    config.presence = Some(wse::PresenceSettings::default());
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, mut changes) = mpsc::unbounded_channel();
    server.start(listener, Present { changes: tx }).unwrap();
    let publisher = server.publisher();
    let token = publisher
        .issue_token("alice", Duration::from_secs(60), serde_json::Map::new())
        .unwrap();
    let mut request = format!("ws://{addr}/wse").into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert!(next_text(&mut ws).await.contains("server_ready"));

    let change = tokio::time::timeout(Duration::from_secs(5), changes.recv())
        .await
        .expect("a presence change within 5 s")
        .expect("the handler alive");
    let wse::Presence::Join { topic, user_id, .. } = change else {
        panic!("a join: {change:?}");
    };
    assert_eq!((topic.as_str(), user_id.as_str()), ("room", "alice"));
    let members = publisher.presence("room");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].user_id, "alice");
    assert_eq!(members[0].data["status"], "here");
    server.stop().await;
}

/// Takes its time over each disconnect, then notes it.
struct SlowGoodbye {
    said: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl wse::Handler for SlowGoodbye {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, _conn: ()) {
        std::thread::sleep(Duration::from_millis(200));
        self.said.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_returns_once_every_disconnect_was_heard() {
    let server = wse::Server::new(wse::Config::default()).unwrap();
    let said = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for _ in 0..2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        server
            .start(
                listener,
                SlowGoodbye {
                    said: std::sync::Arc::clone(&said),
                },
            )
            .unwrap();
        let mut clients = Vec::new();
        for _ in 0..3 {
            clients.push(
                tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
                    .await
                    .unwrap()
                    .0,
            );
        }
        while server.connection_count() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let before = said.load(std::sync::atomic::Ordering::SeqCst);
        server.stop().await;
        assert_eq!(said.load(std::sync::atomic::Ordering::SeqCst), before + 3);
        assert_eq!(server.connection_count(), 0);
    }
}

#[test]
fn keys_stay_out_of_debug_prints() {
    let key = b"-----BEGIN PRIVATE KEY----- not a real one".to_vec();
    let mut config = wse::Config::default();
    config.signing_key = Some(key.clone());
    config.e2e_identity_key = Some(key.clone());
    let mut jwt = wse::JwtSettings::new(key.clone());
    jwt.private_key = Some(key.clone());
    config.jwt = Some(jwt);
    let printed = format!("{config:?}");
    assert!(!printed.contains(&format!("{key:?}")), "{printed}");
    assert!(
        printed.contains("signing_key: Some(<redacted>)"),
        "{printed}"
    );
    let keys = format!("{:?}", wse::JwtKeys::new(key.clone()));
    assert!(!keys.contains(&format!("{key:?}")), "{keys}");
}

#[test]
fn timers_beyond_a_week_are_refused() {
    let mut config = wse::Config::default();
    config.idle_timeout = 8 * 86_400;
    let error = wse::Server::new(config).err().unwrap();
    assert!(error.message().contains("idle_timeout"), "{error}");
}
