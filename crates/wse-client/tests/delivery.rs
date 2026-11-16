//! The recover-or-resync delivery on the plain client, against a WSE server
//! with recovery: through the events stream, where it stands where the
//! stream is read, and through a handler, where it stands where the session
//! hands out.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use wse_client::{
    Config, Continuity, Delivery, Event, Events, Handler, Incoming, Message, StreamPosition,
    Subscription,
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
    fn config(&self, user: &str, delivery: Option<Delivery>) -> Config {
        let mut config = Config::new(self.url.clone());
        config.token = Some(
            self.publisher
                .issue_token(user, Duration::from_secs(600), serde_json::Map::new())
                .unwrap(),
        );
        config.topics = vec!["prices".into()];
        config.delivery = delivery;
        config
    }

    fn publish(&self, n: u64) {
        self.publisher
            .publish_local("prices", &format!(r#"{{"t":"tick","p":{{"n":{n}}}}}"#))
            .unwrap();
    }
}

fn board() -> Option<Delivery> {
    Some(Delivery::new("board"))
}

/// Reads until `done` says so of an item: every item read, in order.
async fn read_until(events: &mut Events, done: impl Fn(&Incoming) -> bool) -> Vec<Incoming> {
    let mut out = Vec::new();
    loop {
        let item = tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .unwrap_or_else(|_| panic!("an item within 5 s; read so far: {out:?}"))
            .expect("the stream ended");
        let finished = done(&item);
        out.push(item);
        if finished {
            return out;
        }
    }
}

fn number(item: &Incoming) -> Option<u64> {
    match item {
        Incoming::Message(m) if m.topic() == Some("prices") => {
            m.payload_value().ok()?["n"].as_u64()
        }
        _ => None,
    }
}

fn numbers(items: &[Incoming]) -> Vec<u64> {
    items.iter().filter_map(number).collect()
}

fn resync_of(items: &[Incoming]) -> wse_client::Resync {
    items
        .iter()
        .find_map(|i| match i {
            Incoming::Event(Event::Resync(r)) => Some(r.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a resync round: {items:?}"))
}

fn is_resync(item: &Incoming) -> bool {
    matches!(item, Incoming::Event(Event::Resync(_)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stream_holds_a_topic_until_the_application_resynced_it() {
    let stand = stand().await;
    let (client, mut events) = wse_client::connect(stand.config("gil", board()))
        .await
        .unwrap();
    let resync = resync_of(&read_until(&mut events, is_resync).await);
    assert_eq!(resync.topics, ["prices"]);
    assert_eq!(resync.reasons, ["cold_start"]);
    assert!(resync.committed.is_empty());
    let state = &client.topic_states()[0];
    assert_eq!(
        (state.subscription, state.continuity),
        (Subscription::Active, Continuity::Resyncing)
    );

    // Published while the application loads its state: held.
    stand.publish(1);
    stand.publish(2);
    let quiet = tokio::time::timeout(Duration::from_millis(300), events.next()).await;
    assert!(quiet.is_err(), "nothing comes before the resync: {quiet:?}");
    client.resynced(resync.round).unwrap();
    let items = read_until(&mut events, |i| number(i) == Some(2)).await;
    assert_eq!(numbers(&items), [1, 2]);
    assert!(items.iter().any(|i| matches!(
        i,
        Incoming::Event(Event::Continuity(c)) if c.continuity == Continuity::Continuous
    )));
    stand.publish(3);
    let items = read_until(&mut events, |i| matches!(i, Incoming::Message(_))).await;
    assert_eq!(numbers(&items), [3]);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_resyncs_and_recovers_from_what_was_committed() {
    let stand = stand().await;
    let (client, mut events) = wse_client::connect(stand.config("hal", board()))
        .await
        .unwrap();
    let resync = resync_of(&read_until(&mut events, is_resync).await);
    client.resynced(resync.round).unwrap();
    for n in 1..=4 {
        stand.publish(n);
    }
    let items = read_until(&mut events, |i| number(i) == Some(4)).await;
    let messages: Vec<_> = items
        .iter()
        .filter_map(|i| match i {
            Incoming::Message(m) => Some(m.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(messages.len(), 4);
    let epoch = messages[0].position().unwrap().epoch;
    let at: Vec<u64> = messages
        .iter()
        .map(|m| m.position().unwrap().offset)
        .collect();
    // Nothing applied yet: nothing in the checkpoint.
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert!(saved["topics"]["prices"].is_null(), "{saved}");
    // Applied out of order: the checkpoint moves only through what has no hole.
    client.commit(&messages[0].as_message());
    client.commit(&messages[2].as_message());
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(saved["topics"]["prices"][format!("{epoch:08x}")], at[0]);
    client.commit(&messages[1].as_message());
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(saved["topics"]["prices"][format!("{epoch:08x}")], at[2]);
    assert_eq!(saved["user"], "hal");
    assert_eq!(saved["consumer"], "board");

    // 4 came but was not applied: after the reconnect it comes again.
    client.reconnect().unwrap();
    let resync = resync_of(&read_until(&mut events, is_resync).await);
    assert_eq!(resync.reasons, ["reconnected"]);
    let position = &resync.committed[0];
    assert_eq!(
        (position.topic.as_str(), position.epoch, position.offset),
        ("prices", epoch, at[2])
    );
    client.resynced(resync.round).unwrap();
    let items = read_until(&mut events, |i| number(i) == Some(4)).await;
    assert_eq!(numbers(&items), [4]);
    client.close().await;
    stand.server.stop().await;
}

/// A handler that applies (and commits) each tick and says what it heard;
/// asked for a resync, it resyncs itself, or says the round.
struct Board {
    client: wse_client::Client,
    heard: mpsc::UnboundedSender<String>,
    resyncs_itself: bool,
}

impl Handler for Board {
    fn message(&mut self, message: &Message<'_>) {
        let n = message.payload_value().ok().and_then(|p| p["n"].as_u64());
        if let Some(n) = n {
            self.client.commit(message);
            let _ = self.heard.send(format!("tick {n}"));
        }
    }

    fn event(&mut self, event: Event) {
        if let Event::Resync(resync) = event {
            if self.resyncs_itself {
                // What was held comes once the handler returns.
                self.client.resynced(resync.round).unwrap();
                let _ = self.heard.send("resynced".to_owned());
            } else {
                let _ = self.heard.send(format!("round {}", resync.round));
            }
        }
    }
}

async fn board_client(
    stand: &Stand,
    user: &str,
    resyncs_itself: bool,
) -> (wse_client::Client, mpsc::UnboundedReceiver<String>) {
    let (heard_tx, heard) = mpsc::unbounded_channel();
    let client = wse_client::Client::connect_with(stand.config(user, board()), |client| Board {
        client,
        heard: heard_tx,
        resyncs_itself,
    })
    .await
    .unwrap();
    (client, heard)
}

async fn next(heard: &mut mpsc::UnboundedReceiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(5), heard.recv())
        .await
        .expect("heard within 5 s")
        .expect("the handler lives")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_hears_what_was_held_in_order_once_the_application_resynced() {
    let stand = stand().await;
    let (client, mut heard) = board_client(&stand, "ivy", false).await;
    let round: u64 = next(&mut heard).await["round ".len()..].parse().unwrap();
    for n in 1..=3 {
        stand.publish(n);
    }
    let quiet = tokio::time::timeout(Duration::from_millis(300), heard.recv()).await;
    assert!(quiet.is_err(), "nothing comes before the resync: {quiet:?}");
    // From another task than the handler's.
    client.resynced(round).unwrap();
    for n in 1..=3 {
        assert_eq!(next(&mut heard).await, format!("tick {n}"));
    }
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(
        saved["topics"]["prices"].as_object().map(|s| s.len()),
        Some(1),
        "{saved}"
    );
    assert_eq!(client.topic_states()[0].continuity, Continuity::Continuous);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handler_resyncs_from_itself() {
    let stand = stand().await;
    let (client, mut heard) = board_client(&stand, "jon", true).await;
    assert_eq!(next(&mut heard).await, "resynced");
    for n in 1..=3 {
        stand.publish(n);
    }
    for n in 1..=3 {
        assert_eq!(next(&mut heard).await, format!("tick {n}"));
    }
    assert_eq!(client.topic_states()[0].continuity, Continuity::Continuous);
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_of_another_consumer_is_refused_aloud() {
    let stand = stand().await;
    let mut delivery = Delivery::new("board");
    delivery.checkpoint = Some(format!(
        r#"{{"v":1,"realm":"{}","user":"kim","consumer":"other","schema":null,"topics":{{"prices":{{"0000000a":9}}}}}}"#,
        stand.url
    ));
    let (client, mut events) = wse_client::connect(stand.config("kim", Some(delivery)))
        .await
        .unwrap();
    let items = read_until(&mut events, is_resync).await;
    assert!(
        items.iter().any(|i| matches!(
            i,
            Incoming::Event(Event::CheckpointRejected(c)) if c.reason == "consumer"
        )),
        "{items:?}"
    );
    // Nothing of it is taken: the resync starts from nothing committed.
    assert_eq!(resync_of(&items).committed, Vec::<StreamPosition>::new());
    client.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn best_effort_says_what_the_server_took_and_lets_the_app_say_it_resynced() {
    let stand = stand().await;
    let (client, mut events) = wse_client::connect(stand.config("lee", None))
        .await
        .unwrap();
    read_until(&mut events, |i| {
        matches!(i, Incoming::Event(Event::Subscribed(_)))
    })
    .await;
    let state = &client.topic_states()[0];
    assert_eq!(
        (state.topic.as_str(), state.subscription, state.continuity),
        ("prices", Subscription::Active, Continuity::Continuous)
    );
    client.resync(["prices"]).unwrap();
    assert_eq!(client.topic_states()[0].continuity, Continuity::Continuous);
    // Nothing is held in best effort: a tick comes at once.
    stand.publish(1);
    let items = read_until(&mut events, |i| number(i).is_some()).await;
    assert_eq!(numbers(&items), [1]);
    client.close().await;
    stand.server.stop().await;
}
