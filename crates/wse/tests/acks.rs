//! The clients' outboxes on the server: a numbered message is confirmed once
//! the application took it, a repeat is confirmed again and not handed on,
//! a refused one is said, and said again to a client that comes back without
//! having heard, what the server cannot tell is marked, and a client that
//! moves to another node of the cluster is not handed on twice.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// What the application took: the type, the number and the mark.
#[derive(Debug, PartialEq)]
struct Taken {
    kind: String,
    s: u64,
    possible_repeat: bool,
}

/// Takes every message, refuses those of type `refuse` (no room); each
/// takes it `pace` (a slow application).
struct App {
    taken: mpsc::UnboundedSender<Taken>,
    connected: mpsc::UnboundedSender<String>,
    pace: Duration,
}

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<()> {
        let _ = self.connected.send((**connect.conn_id).clone());
        Some(())
    }

    fn message(&self, _conn: &mut (), message: wse::Inbound, _size: usize) -> bool {
        let wse::Inbound::Json(value) = message else {
            return true;
        };
        if !self.pace.is_zero() {
            std::thread::sleep(self.pace);
        }
        let kind = value["t"].as_str().unwrap_or_default().to_owned();
        if kind == "refuse" {
            return false;
        }
        let _ = self.taken.send(Taken {
            kind,
            s: value["s"].as_u64().unwrap_or(0),
            possible_repeat: value["possible_repeat"] == true,
        });
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, _conn: ()) {}
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Node {
    server: wse::Server,
    addr: std::net::SocketAddr,
    taken: mpsc::UnboundedReceiver<Taken>,
    connected: mpsc::UnboundedReceiver<String>,
}

async fn node() -> Node {
    node_with(wse::Config::default()).await
}

async fn node_with(config: wse::Config) -> Node {
    node_paced(config, Duration::ZERO).await
}

async fn node_paced(config: wse::Config, pace: Duration) -> Node {
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (taken_tx, taken) = mpsc::unbounded_channel();
    let (connected_tx, connected) = mpsc::unbounded_channel();
    server
        .start(
            listener,
            App {
                taken: taken_tx,
                connected: connected_tx,
                pace,
            },
        )
        .unwrap();
    Node {
        server,
        addr,
        taken,
        connected,
    }
}

impl Node {
    /// A client of `user` with the outbox `outbox`, accepted: the server's
    /// answer to its hello (what was decided, and the node's name).
    async fn client(&mut self, user: &str, outbox: serde_json::Value) -> (Client, u64, String) {
        let (ws, reply) = self.answered(user, outbox).await;
        let acked = reply["acked"].as_u64().unwrap();
        let node = reply["node"].as_str().unwrap().to_owned();
        (ws, acked, node)
    }

    /// A client of `user` with the outbox `outbox`, accepted, and the
    /// server's whole answer to its hello.
    async fn answered(
        &mut self,
        user: &str,
        outbox: serde_json::Value,
    ) -> (Client, serde_json::Value) {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/wse", self.addr))
            .await
            .unwrap();
        let hello = serde_json::json!({"t": "client_hello", "p": {"protocol_version": 2, "outbox": outbox}});
        ws.send(Message::Text(hello.to_string().into()))
            .await
            .unwrap();
        let conn_id = tokio::time::timeout(Duration::from_secs(5), self.connected.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(self.server.publisher().accept(&conn_id, user).unwrap());
        let reply = next_of(&mut ws, "outbox").await;
        (ws, reply["p"].clone())
    }

    async fn took(&mut self) -> Taken {
        tokio::time::timeout(Duration::from_secs(5), self.taken.recv())
            .await
            .expect("taken within 5 s")
            .expect("the application alive")
    }

    /// The application took nothing for 300 ms.
    async fn took_nothing(&mut self) {
        let got = tokio::time::timeout(Duration::from_millis(300), self.taken.recv()).await;
        assert!(got.is_err(), "nothing taken: {got:?}");
    }
}

/// The next server message of type `kind`.
async fn next_of(ws: &mut Client, kind: &str) -> serde_json::Value {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap_or_else(|_| panic!("a {kind} within 5 s"))
            .expect("the connection open")
            .expect("a frame");
        if let Message::Text(text) = frame {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["t"] == kind {
                return value;
            }
        }
    }
}

/// A numbered message, its number first, as a client sends it.
async fn send(ws: &mut Client, kind: &str, s: u64) {
    let message = format!(r#"{{"s":{s},"t":"{kind}","p":{{}},"id":"m{s}"}}"#);
    ws.send(Message::Text(message.into())).await.unwrap();
}

/// Reads acks until one confirms `s`.
async fn acked(ws: &mut Client, s: u64) {
    loop {
        if next_of(ws, "ack").await["p"]["s"].as_u64() >= Some(s) {
            return;
        }
    }
}

fn taken(kind: &str, s: u64) -> Taken {
    Taken {
        kind: kind.to_owned(),
        s,
        possible_repeat: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_is_taken_once_and_the_client_hears_of_each() {
    let mut node = node().await;
    let (mut ws, acked_at, _) = node
        .client("ann", serde_json::json!({"producer": "p1"}))
        .await;
    assert_eq!(acked_at, 0);

    send(&mut ws, "a", 1).await;
    send(&mut ws, "b", 2).await;
    assert_eq!(node.took().await, taken("a", 1));
    assert_eq!(node.took().await, taken("b", 2));
    acked(&mut ws, 2).await;

    // A repeat is confirmed again, not handed on.
    send(&mut ws, "b", 2).await;
    acked(&mut ws, 2).await;
    node.took_nothing().await;

    // No room: refused, and said so.
    send(&mut ws, "refuse", 3).await;
    let nack = next_of(&mut ws, "nack").await;
    assert_eq!(
        (nack["p"]["s"].as_u64(), nack["p"]["code"].as_str()),
        (Some(3), Some("busy"))
    );

    // A number skipped on the connection is refused as missing.
    send(&mut ws, "d", 5).await;
    let nack = next_of(&mut ws, "nack").await;
    assert_eq!(
        (nack["p"]["s"].as_u64(), nack["p"]["code"].as_str()),
        (Some(4), Some("missing"))
    );
    assert_eq!(node.took().await, taken("d", 5));
    acked(&mut ws, 5).await;
    node.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_back_on_the_same_node_goes_on_from_what_was_taken() {
    let mut node = node().await;
    let (mut ws, _, name) = node
        .client("ann", serde_json::json!({"producer": "p2"}))
        .await;
    for s in 1..=3 {
        send(&mut ws, "m", s).await;
        assert_eq!(node.took().await, taken("m", s));
    }
    acked(&mut ws, 3).await;
    drop(ws);

    let outbox = serde_json::json!({"producer": "p2", "node": name, "written": 3});
    let (mut ws, acked_at, _) = node.client("ann", outbox).await;
    assert_eq!(acked_at, 3, "what was taken");
    // Sent again all the same: a repeat.
    send(&mut ws, "m", 3).await;
    acked(&mut ws, 3).await;
    node.took_nothing().await;
    send(&mut ws, "m", 4).await;
    assert_eq!(node.took().await, taken("m", 4));

    // The same producer of another user is another one.
    let outbox = serde_json::json!({"producer": "p2", "node": name, "written": 3});
    let (_bob, acked_at, _) = node.client("bob", outbox).await;
    assert_eq!(acked_at, 0);
    node.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_the_server_cannot_tell_reaches_the_application_marked() {
    let mut node = node().await;
    // From a node that is gone, with two messages it may have taken.
    let outbox = serde_json::json!({"producer": "p3", "node": "gone", "written": 2});
    let (mut ws, acked_at, _) = node.client("ann", outbox).await;
    assert_eq!(acked_at, 0);
    for s in 1..=3 {
        send(&mut ws, "m", s).await;
    }
    let marked = |s, possible_repeat| Taken {
        kind: "m".to_owned(),
        s,
        possible_repeat,
    };
    assert_eq!(node.took().await, marked(1, true));
    assert_eq!(node.took().await, marked(2, true));
    assert_eq!(node.took().await, marked(3, false));
    acked(&mut ws, 3).await;
    node.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_the_client_did_not_hear_of_is_told_when_it_comes_back() {
    let mut node = node().await;
    let (mut ws, _, name) = node
        .client("ann", serde_json::json!({"producer": "p6"}))
        .await;
    send(&mut ws, "m", 1).await;
    assert_eq!(node.took().await, taken("m", 1));
    send(&mut ws, "refuse", 2).await;
    send(&mut ws, "m", 3).await;
    assert_eq!(node.took().await, taken("m", 3));
    // Gone before the answers came.
    drop(ws);

    // Back, still waiting to hear of 2 and 3.
    let outbox = serde_json::json!({"producer": "p6", "node": name, "written": 3, "from": 2});
    let (mut ws, reply) = node.answered("ann", outbox).await;
    assert_eq!(reply["acked"], 3);
    assert_eq!(
        reply["refused"],
        serde_json::json!([{"s": 2, "code": "busy"}]),
        "2 was refused, 3 taken"
    );
    // Sent again all the same: refused again, never handed on.
    send(&mut ws, "m", 2).await;
    let nack = next_of(&mut ws, "nack").await;
    assert_eq!(
        (nack["p"]["s"].as_u64(), nack["p"]["code"].as_str()),
        (Some(2), Some("busy"))
    );
    node.took_nothing().await;
    drop(ws);

    // A client that heard of it is not told again.
    let outbox = serde_json::json!({"producer": "p6", "node": name, "written": 3, "from": 4});
    let (_ws, reply) = node.answered("ann", outbox).await;
    assert_eq!(reply["acked"], 3);
    assert!(reply.get("refused").is_none(), "{reply}");
    node.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_over_the_rate_limit_is_refused_at_once() {
    let mut config = wse::Config::default();
    config.rate_limit_capacity = 2.0;
    config.rate_limit_refill = 0.01;
    let mut node = node_with(config).await;
    let (mut ws, _, _) = node
        .client("ann", serde_json::json!({"producer": "p8"}))
        .await;
    for s in 1..=4 {
        send(&mut ws, "m", s).await;
    }
    // Nothing comes after 4: its refusal does not wait for a later message.
    let mut refused = Vec::new();
    while refused.last() != Some(&4) {
        let nack = next_of(&mut ws, "nack").await;
        assert_eq!(nack["p"]["code"], "rate_limited", "{nack}");
        let to = nack["p"]["s"].as_u64().unwrap();
        let from = nack["p"]["from"].as_u64().unwrap_or(to);
        refused.extend(from..=to);
    }
    // What the bucket held was taken, the rest refused, in order.
    let first = refused[0];
    assert_eq!(refused, (first..=4).collect::<Vec<_>>());
    for s in 1..first {
        assert_eq!(node.took().await, taken("m", s));
    }
    node.took_nothing().await;
    node.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_an_outbox_a_number_is_the_applications_own() {
    let mut node = node().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/wse", node.addr))
        .await
        .unwrap();
    let hello = serde_json::json!({"t": "client_hello", "p": {"protocol_version": 2}});
    ws.send(Message::Text(hello.to_string().into()))
        .await
        .unwrap();
    let conn_id = tokio::time::timeout(Duration::from_secs(5), node.connected.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(node.server.publisher().accept(&conn_id, "ann").unwrap());
    send(&mut ws, "score", 3).await;
    send(&mut ws, "score", 3).await;
    // Both taken as they are, and no answer the client did not ask for.
    assert_eq!(node.took().await, taken("score", 3));
    assert_eq!(node.took().await, taken("score", 3));
    let answered = tokio::time::timeout(Duration::from_millis(300), next_of(&mut ws, "ack")).await;
    assert!(answered.is_err(), "no ack: {answered:?}");
    node.server.stop().await;
}

/// Links `a` and `b` (on their WebSocket ports) and waits for the link.
async fn link(a: &Node, b: &Node) {
    let secret = b"a-cluster-secret-of-at-least-32-bytes!".to_vec();
    for (node, peer) in [(a, b), (b, a)] {
        let mut settings = wse::ClusterSettings::default();
        settings.peers = vec![peer.addr.to_string()];
        settings.single_port = true;
        settings.secret = Some(secret.clone());
        node.server.connect_cluster(settings).unwrap();
    }
    for _ in 0..250 {
        if !a.server.publisher().cluster_peers().is_empty()
            && !b.server.publisher().cluster_peers().is_empty()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the nodes did not link");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_moves_to_another_node_is_not_handed_on_twice() {
    let mut a = node().await;
    let mut b = node().await;
    link(&a, &b).await;

    let (mut on_a, _, name_a) = a.client("ann", serde_json::json!({"producer": "p4"})).await;
    for s in 1..=3 {
        send(&mut on_a, "m", s).await;
        assert_eq!(a.took().await, taken("m", s));
    }
    // 3 was taken on A, its ack not heard: the client goes to B with it.
    let outbox = serde_json::json!({"producer": "p4", "node": name_a, "written": 3});
    let (mut on_b, acked_at, _) = b.client("ann", outbox).await;
    assert_eq!(acked_at, 3, "B asked A");
    send(&mut on_b, "m", 3).await;
    acked(&mut on_b, 3).await;
    b.took_nothing().await;
    send(&mut on_b, "m", 4).await;
    assert_eq!(b.took().await, taken("m", 4));

    // A handed the producer over: its stale connection takes nothing more.
    send(&mut on_a, "m", 5).await;
    a.took_nothing().await;
    a.server.stop().await;
    b.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_goes_with_the_client_to_another_node() {
    let mut a = node().await;
    let mut b = node().await;
    link(&a, &b).await;

    let (mut on_a, _, name_a) = a.client("ann", serde_json::json!({"producer": "p7"})).await;
    send(&mut on_a, "refuse", 1).await;
    send(&mut on_a, "m", 2).await;
    assert_eq!(a.took().await, taken("m", 2));
    // The answers not heard: the client goes to B with both.
    let outbox = serde_json::json!({"producer": "p7", "node": name_a, "written": 2, "from": 1});
    let (_on_b, reply) = b.answered("ann", outbox).await;
    assert_eq!(reply["acked"], 2, "B asked A");
    assert_eq!(
        reply["refused"],
        serde_json::json!([{"s": 1, "code": "busy"}])
    );
    b.took_nothing().await;
    a.server.stop().await;
    b.server.stop().await;
}

/// A drained server answers what it took before it closes: the client
/// hears of every message the application took.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_answers_what_was_taken_before_the_close() {
    let mut node = node().await;
    let (mut ws, _, _) = node
        .client("ann", serde_json::json!({"producer": "p5"}))
        .await;
    for s in 1..=50 {
        send(&mut ws, "m", s).await;
    }
    let mut taken = 0;
    while taken < 50 {
        node.took().await;
        taken += 1;
    }
    let server = node.server;
    let drained = tokio::spawn(async move {
        let _ = server.drain(4300, "draining", Duration::from_secs(5)).await;
        server
    });
    let mut highest = 0;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("a frame within 5 s");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["t"] == "ack" {
                    highest = highest.max(value["p"]["s"].as_u64().unwrap());
                }
            }
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
            Some(Ok(_)) => {}
        }
    }
    assert_eq!(
        highest, 50,
        "every message taken was confirmed before the close"
    );
    drained.await.unwrap().stop().await;
}

/// A drained server stops taking the messages of a client that keeps
/// sending, and answers what it took: the client hears of each one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drain_answers_a_client_that_keeps_sending() {
    // An application slower than the client: frames wait for it.
    let mut node = node_paced(wse::Config::default(), Duration::from_micros(20)).await;
    let (ws, _, _) = node
        .client("ann", serde_json::json!({"producer": "p6"}))
        .await;
    let (mut sink, mut stream) = ws.split();
    // It sends with no pause until its connection goes, 64 at a time: the
    // server always has a frame waiting.
    let sender = tokio::spawn(async move {
        for s in 1u64.. {
            let message = format!(r#"{{"s":{s},"t":"m","p":{{}},"id":"m{s}"}}"#);
            let sent = if s % 64 == 0 {
                sink.send(Message::Text(message.into())).await
            } else {
                sink.feed(Message::Text(message.into())).await
            };
            if sent.is_err() {
                return;
            }
        }
    });
    let mut last_taken = 0;
    while last_taken < 200 {
        last_taken = node.took().await.s;
    }
    let server = node.server;
    let drained = tokio::spawn(async move {
        let _ = server.drain(4300, "draining", Duration::from_secs(5)).await;
        server
    });
    let mut highest = 0;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a frame within 5 s");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                if value["t"] == "ack" {
                    highest = highest.max(value["p"]["s"].as_u64().unwrap());
                }
            }
            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
            Some(Ok(_)) => {}
        }
    }
    sender.abort();
    let _ = sender.await;
    drop(stream);
    while let Ok(Some(taken)) =
        tokio::time::timeout(Duration::from_millis(300), node.taken.recv()).await
    {
        last_taken = last_taken.max(taken.s);
    }
    assert!(
        highest >= last_taken,
        "taken up to {last_taken}, confirmed up to {highest}"
    );
    drained.await.unwrap().stop().await;
}
