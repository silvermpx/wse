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
        .await
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
