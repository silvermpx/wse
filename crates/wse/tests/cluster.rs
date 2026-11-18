//! Nodes of a cluster that stop: what a node published before its stop
//! reaches the clients of the other nodes, and they know it left on
//! purpose, so a node that comes up in its place takes nothing from their
//! clients' recovery. The node that stops runs on a runtime of its own,
//! which ends with the stop, as the Python server's does.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::StreamExt;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Takes every connection.
struct Everyone;

impl wse::Handler for Everyone {
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

    fn disconnect(&self, _conn: ()) {}
}

fn config(recovery: bool) -> wse::Config {
    let mut config = wse::Config::default();
    if recovery {
        config.recovery = Some(wse::RecoverySettings::default());
    }
    config
}

/// Links on the WebSocket port, to `peers`.
fn cluster(peers: &[SocketAddr]) -> wse::ClusterSettings {
    let mut settings = wse::ClusterSettings::default();
    settings.peers = peers.iter().map(ToString::to_string).collect();
    settings.single_port = true;
    settings.secret = Some(b"a-cluster-secret-of-at-least-32-bytes!".to_vec());
    settings
}

/// A node on this test's runtime.
struct Node {
    server: wse::Server,
    addr: SocketAddr,
}

async fn node(recovery: bool) -> Node {
    let server = wse::Server::new(config(recovery)).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    server.start(listener, Everyone).unwrap();
    Node { server, addr }
}

/// A node on a runtime of its own: the runtime ends right after the stop,
/// and with it whatever the node had not finished by then.
struct Apart {
    addr: SocketAddr,
    publisher: wse::Publisher,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Apart {
    fn start(addr: SocketAddr, recovery: bool, peers: Vec<SocketAddr>) -> Apart {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = wse::Server::new(config(recovery)).unwrap();
                let listener = TcpListener::bind(addr).await.unwrap();
                let addr = listener.local_addr().unwrap();
                server.start(listener, Everyone).unwrap();
                server.connect_cluster(cluster(&peers)).unwrap();
                ready_tx.send((addr, server.publisher())).unwrap();
                let _ = stop_rx.await;
                server.stop().await;
            });
            // Whatever still runs on it is dropped here.
            drop(runtime);
        });
        let (addr, publisher) = ready_rx.recv().unwrap();
        Apart {
            addr,
            publisher,
            stop: Some(stop_tx),
            thread: Some(thread),
        }
    }

    /// Stop the node, and its runtime with it.
    async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join().unwrap())
                .await
                .unwrap();
        }
    }
}

impl Drop for Apart {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// A client of `addr` the application took, past its server_ready, and its
/// connection's id.
async fn accepted(addr: SocketAddr, publisher: &wse::Publisher) -> (Client, String) {
    let before = publisher.connections();
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/wse"))
        .await
        .unwrap();
    let conn_id = loop {
        if let Some(id) = publisher
            .connections()
            .into_iter()
            .find(|id| !before.contains(id))
        {
            break id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(publisher.accept(&conn_id, "ann").unwrap());
    next_of(&mut ws, "server_ready").await;
    (ws, conn_id)
}

/// The next message of type `kind` the client gets.
async fn next_of(ws: &mut Client, kind: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(5), next_message(ws, kind))
        .await
        .unwrap_or_else(|_| panic!("a {kind} within 5 s"))
}

async fn next_message(ws: &mut Client, kind: &str) -> serde_json::Value {
    loop {
        let frame = ws
            .next()
            .await
            .expect("the connection open")
            .expect("a frame");
        if let Message::Text(text) = frame {
            let json = &text[text.find('{').expect("a JSON message")..];
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            if value["t"] == kind {
                return value;
            }
        }
    }
}

/// `kind` published by `publisher` until one reaches the client: the
/// client's interest is known at the publishing node then.
async fn through(publisher: &wse::Publisher, ws: &mut Client, kind: &str) -> serde_json::Value {
    let message = format!(r#"{{"t":"{kind}","p":{{}}}}"#);
    for _ in 0..100 {
        publisher.publish(kind, &message).unwrap();
        let next = next_message(ws, kind);
        if let Ok(got) = tokio::time::timeout(Duration::from_millis(200), next).await {
            return got;
        }
    }
    panic!("no {kind} came through in 20 s");
}

async fn until(what: &str, check: impl Fn() -> bool) {
    for _ in 0..1000 {
        if check() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{what} within 20 s");
}

/// A node publishes, then stops at once: a client of the other node gets
/// every message, the last ones too, whichever node made the link.
async fn published_before_the_stop_reaches_the_other_node(dialed_by_the_stopping_node: bool) {
    let b = node(false).await;
    let peers = if dialed_by_the_stopping_node {
        vec![b.addr]
    } else {
        Vec::new()
    };
    let mut a = Apart::start("127.0.0.1:0".parse().unwrap(), false, peers);
    if !dialed_by_the_stopping_node {
        b.server.connect_cluster(cluster(&[a.addr])).unwrap();
    } else {
        b.server.connect_cluster(cluster(&[])).unwrap();
    }
    let publisher = b.server.publisher();
    until("the link", || {
        !publisher.cluster_peers().is_empty() && !a.publisher.cluster_peers().is_empty()
    })
    .await;
    let (mut ws, conn_id) = accepted(b.addr, &publisher).await;
    let topics = vec!["news".to_owned(), "probe".to_owned()];
    publisher
        .subscribe(&conn_id, topics, &wse::Subscribe::default())
        .unwrap();
    through(&a.publisher, &mut ws, "probe").await;

    const SENT: u64 = 2000;
    for n in 1..=SENT {
        let message = format!(r#"{{"t":"news","p":{{"n":{n}}}}}"#);
        a.publisher.publish("news", &message).unwrap();
    }
    a.stop().await;
    for n in 1..=SENT {
        let next = next_message(&mut ws, "news");
        let got = tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap_or_else(|_| panic!("{} of the {SENT} messages came", n - 1));
        assert_eq!(got["p"]["n"], n, "message {n} of {SENT}");
    }
    b.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_node_published_before_its_stop_reaches_the_node_that_dialed_it() {
    published_before_the_stop_reaches_the_other_node(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn what_a_node_published_before_its_stop_reaches_the_node_it_dialed() {
    published_before_the_stop_reaches_the_other_node(true).await;
}

/// A node stops while linked, and a new one comes up at its address: a
/// client of the other node recovers from its last message of the node that
/// stopped as before, since that node said it left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_stopped_and_one_anew_in_its_place_keep_the_others_recovery() {
    let b = node(true).await;
    let mut a = Apart::start("127.0.0.1:0".parse().unwrap(), true, Vec::new());
    let addr = a.addr;
    b.server.connect_cluster(cluster(&[addr])).unwrap();
    let publisher = b.server.publisher();
    until("the link", || publisher.cluster_peers().len() == 1).await;

    let (mut live, conn_id) = accepted(b.addr, &publisher).await;
    let topics = vec!["news".to_owned(), "barrier".to_owned()];
    publisher
        .subscribe(&conn_id, topics, &wse::Subscribe::default())
        .unwrap();
    // Once a barrier came through, the interest in news is known at A too:
    // the next news is the client's last message from A.
    through(&a.publisher, &mut live, "barrier").await;
    a.publisher
        .publish("news", r#"{"t":"news","p":{"n":1}}"#)
        .unwrap();
    let last = next_of(&mut live, "news").await;
    let epoch = u32::from_str_radix(last["e"].as_str().unwrap(), 16).unwrap();
    let offset = last["o"].as_u64().unwrap();

    a.stop().await;
    until("the link's end", || publisher.cluster_peers().is_empty()).await;
    let mut anew = Apart::start(addr, true, Vec::new());
    until("the link to the node anew", || {
        publisher.cluster_peers().len() == 1
    })
    .await;
    through(&anew.publisher, &mut live, "barrier").await;

    let (_other, other_id) = accepted(b.addr, &publisher).await;
    let positions = HashMap::from([("news".to_owned(), (epoch, offset))]);
    let answer = publisher
        .subscribe_with_recovery(&other_id, vec!["news".to_owned()], &positions)
        .unwrap();
    let news = &answer["news"];
    assert!(news.recovered, "{news:?}");
    assert_eq!(news.count, 0, "{news:?}");
    anew.stop().await;
    b.server.stop().await;
}
