//! The window bridge's owner against a WSE server, with windows of the
//! test's own that speak the protocol frame by frame, as the TypeScript
//! window does.

#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use wse_client::Config;
use wse_client::bridge::{Limits, Options, Owner, Window};

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

/// The server's application: it takes every message, and tells the test
/// the type of each when asked to.
struct App {
    seen: Option<mpsc::UnboundedSender<String>>,
}

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), message: wse::Inbound, _size: usize) -> bool {
        if let (Some(seen), wse::Inbound::Json(message)) = (&self.seen, message) {
            let _ = seen.send(message["t"].as_str().unwrap_or_default().to_owned());
        }
        true
    }

    fn signed(&self, _conn: &mut (), _message: Value, _size: usize) -> bool {
        true
    }

    fn disconnect(&self, _conn: ()) {}
}

struct Stand {
    server: wse::Server,
    publisher: wse::Publisher,
    url: String,
}

/// A server that validates tokens, answers subscriptions and keeps topic
/// histories (so it stamps each topic message with its position).
async fn stand() -> Stand {
    stand_seeing(None).await
}

/// [`stand`], telling `seen` the type of each message it takes.
async fn stand_seeing(seen: Option<mpsc::UnboundedSender<String>>) -> Stand {
    let mut config = wse::Config::default();
    config.jwt = Some(wse::JwtSettings::new(SECRET));
    config.client_subscriptions = true;
    config.recovery = Some(wse::RecoverySettings::default());
    let server = wse::Server::new(config).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/wse", listener.local_addr().unwrap());
    server.start(listener, App { seen }).unwrap();
    let publisher = server.publisher();
    Stand {
        server,
        publisher,
        url,
    }
}

impl Stand {
    fn config(&self) -> Config {
        let mut config = Config::new(&self.url);
        config.token = Some(
            self.publisher
                .issue_token("ann", Duration::from_secs(600), serde_json::Map::new())
                .unwrap(),
        );
        config.reconnect.base_delay = Duration::from_millis(50);
        config.reconnect.max_delay = Duration::from_millis(200);
        config.reconnect.jitter = false;
        config
    }

    async fn owner(&self, options: Options) -> Owner {
        Owner::start(self.config(), options).await.unwrap()
    }

    fn publish(&self, topic: &str, n: u64) {
        let text = json!({"t": "price", "p": {"n": n}}).to_string();
        self.publisher.publish_local(topic, &text).unwrap();
    }
}

/// The owner's end of a test window: each frame it posts, read.
struct Probe(mpsc::UnboundedSender<Value>);

impl Window for Probe {
    fn post(&self, frame: String) {
        let _ = self
            .0
            .send(serde_json::from_str(&frame).expect("a frame is JSON"));
    }

    fn close(&self) {
        let _ = self.0.send(json!({"k": "closed"}));
    }
}

/// A window of the test's: it says hello, acknowledges each batch it takes
/// unless told to hold back, and keeps what came.
struct TestWindow {
    owner: Owner,
    att: u64,
    rx: mpsc::UnboundedReceiver<Value>,
    /// Frames other than batches, and the items of the batches taken.
    frames: Vec<Value>,
    items: Vec<Value>,
    held: Vec<Value>,
    holding: bool,
}

impl TestWindow {
    fn new(owner: &Owner) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let att = owner.attach(Probe(tx));
        let window = Self {
            owner: owner.clone(),
            att,
            rx,
            frames: Vec::new(),
            items: Vec::new(),
            held: Vec::new(),
            holding: false,
        };
        window.send(json!({"k": "hello", "v": 1, "agent": "test"}));
        window
    }

    fn send(&self, frame: Value) {
        self.owner.frame(self.att, &frame.to_string());
    }

    fn take(&mut self, frame: Value) {
        if frame["k"] != "b" {
            self.frames.push(frame);
            return;
        }
        if self.holding {
            self.held.push(frame);
            return;
        }
        self.items
            .extend(frame["items"].as_array().cloned().unwrap_or_default());
        self.send(json!({"k": "ack", "s": frame["s"]}));
    }

    /// Takes what comes until `test` holds, or fails after 5 s.
    async fn until(&mut self, what: &str, test: impl Fn(&Self) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !test(self) {
            match tokio::time::timeout_at(deadline, self.rx.recv()).await {
                Ok(Some(frame)) => self.take(frame),
                _ => panic!(
                    "{what}: not within 5 s; frames {:?}, items {:?}",
                    self.frames, self.items
                ),
            }
        }
    }

    /// Takes what comes for `ms`.
    async fn settle(&mut self, ms: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(ms);
        while let Ok(Some(frame)) = tokio::time::timeout_at(deadline, self.rx.recv()).await {
            self.take(frame);
        }
    }

    fn messages(&self, topic: &str) -> Vec<u64> {
        self.items
            .iter()
            .filter(|item| item["m"]["tp"] == topic)
            .filter_map(|item| item["m"]["p"]["n"].as_u64())
            .collect()
    }

    fn has_item(&self, test: impl Fn(&Value) -> bool) -> bool {
        self.items.iter().any(test)
    }

    fn has_frame(&self, test: impl Fn(&Value) -> bool) -> bool {
        self.frames.iter().any(test)
    }

    fn subscribed(&self, topic: &str) -> bool {
        self.has_item(|item| {
            item["ev"] == "subscribed"
                && item["taken"]
                    .as_array()
                    .is_some_and(|taken| taken.iter().any(|t| t == topic))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_windows_share_one_connection_and_each_hears_what_it_holds() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    let mut b = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    b.send(json!({"k": "share", "on": true}));
    a.send(json!({"k": "hold", "topics": ["prices"]}));
    b.send(json!({"k": "hold", "topics": ["prices", "news"]}));
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    a.until("prices taken", |w| w.subscribed("prices")).await;
    b.until("news taken", |w| w.subscribed("news")).await;
    b.until("prices taken", |w| w.subscribed("prices")).await;
    stand.publish("prices", 1);
    stand.publish("news", 2);
    b.until("both topics", |w| {
        w.messages("news") == [2] && w.messages("prices") == [1]
    })
    .await;
    a.until("prices", |w| w.messages("prices") == [1]).await;
    a.settle(200).await;
    assert!(a.messages("news").is_empty());
    // The message as the TypeScript client shows it: its topic and position.
    let price = a.items.iter().find(|i| i["m"]["tp"] == "prices").unwrap();
    assert_eq!(price["m"]["t"], "price");
    assert_eq!(price["m"]["e"].as_str().map(str::len), Some(8));
    assert!(price["m"]["o"].is_u64());
    // The window's view of the connection: ready, and no secret in it.
    let conn = a.frames.iter().rev().find(|f| f["k"] == "conn").unwrap();
    assert_eq!(conn["state"]["ready"], true);
    assert_eq!(conn["state"]["endpoint"], stand.url);
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_send_goes_through_the_outbox_and_its_outcome_to_its_window_alone() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    let mut b = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    a.send(json!({"k": "send", "n": 1, "t": "order", "p": {"n": 1}, "o": {"id": "from-a"}, "c": 1, "a": 0}));
    a.until("sent", |w| w.has_frame(|f| f["k"] == "sent" && f["n"] == 1))
        .await;
    let sent = a.frames.iter().find(|f| f["k"] == "sent").unwrap();
    assert_eq!(
        (sent["status"].as_str(), sent["id"].as_str()),
        (Some("sent"), Some("from-a"))
    );
    a.until("delivered", |w| {
        w.has_item(|i| {
            i["ev"] == "event"
                && i["name"] == "wseDelivered"
                && i["detail"]["ids"] == json!(["from-a"])
        })
    })
    .await;
    b.settle(200).await;
    assert!(!b.has_item(|i| i["ev"] == "event"));
    // A live send on a connection the window did not see goes nowhere.
    a.send(json!({"k": "send", "n": 2, "t": "nudge", "p": {}, "o": {"id": "late", "live": true}, "c": 7, "a": 0}));
    a.until("dropped", |w| {
        w.has_frame(|f| f["k"] == "sent" && f["n"] == 2)
    })
    .await;
    let late = a
        .frames
        .iter()
        .find(|f| f["k"] == "sent" && f["n"] == 2)
        .unwrap();
    assert_eq!(late["status"], "dropped");
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_connection_is_made_while_a_window_shares_it() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    a.settle(300).await;
    assert!(!owner.client().is_connected());
    let first = a.frames.iter().find(|f| f["k"] == "welcome").unwrap();
    assert_eq!(first["conn"]["connection"], "pending");
    a.send(json!({"k": "share", "on": true}));
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    assert!(owner.client().is_connected());
    // Its share goes: so does the connection; the window hears it at once.
    a.send(json!({"k": "share", "on": false}));
    a.until("lost", |w| w.has_item(|i| i["ev"] == "lost")).await;
    a.until("pending", |w| {
        w.frames
            .last()
            .is_some_and(|f| f["k"] == "conn" && f["state"]["connection"] == "pending")
    })
    .await;
    assert!(!owner.client().is_connected());
    // The application's own share holds it too.
    let share = owner.retain();
    a.until("connected again", |w| {
        w.items.iter().filter(|i| i["ev"] == "connected").count() == 2
    })
    .await;
    assert_eq!(
        a.items.iter().rfind(|i| i["ev"] == "connected").unwrap()["gen"],
        2
    );
    drop(share);
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_that_falls_behind_loses_its_queue_alone() {
    let stand = stand().await;
    let mut options = Options::default();
    let mut limits = Limits::default();
    limits.queue_items = 20;
    options.limits = limits;
    let owner = stand.owner(options).await;
    let mut slow = TestWindow::new(&owner);
    let mut quick = TestWindow::new(&owner);
    slow.send(json!({"k": "share", "on": true}));
    slow.send(json!({"k": "hold", "topics": ["prices"]}));
    quick.send(json!({"k": "hold", "topics": ["prices"]}));
    slow.until("taken", |w| w.subscribed("prices")).await;
    quick.until("taken", |w| w.subscribed("prices")).await;
    slow.holding = true;
    // In tens, each taken by the quick window before the next: its queue
    // stays below its limit however slowly the machine runs it.
    for ten in 0..6 {
        for n in ten * 10 + 1..=ten * 10 + 10 {
            stand.publish("prices", n);
        }
        let seen = usize::try_from(ten * 10 + 10).unwrap();
        quick
            .until("each ten", |w| w.messages("prices").len() == seen)
            .await;
    }
    assert_eq!(quick.messages("prices"), (1..=60).collect::<Vec<_>>());
    slow.settle(200).await;
    // Two batches in hand, the rest past its limit: dropped, and said so.
    assert_eq!(slow.held.len(), 2);
    slow.holding = false;
    for batch in std::mem::take(&mut slow.held) {
        slow.take(batch);
    }
    slow.until("overflow", |w| {
        w.has_item(|i| i["ev"] == "overflow" && i["topics"] == json!(["prices"]))
    })
    .await;
    assert!(slow.messages("prices").len() < 60);
    assert!(slow.subscribed("prices"));
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_behind_hears_what_became_of_its_send_though_its_queue_overflowed() {
    let stand = stand().await;
    let mut options = Options::default();
    let mut limits = Limits::default();
    limits.queue_items = 20;
    options.limits = limits;
    let owner = stand.owner(options).await;
    let mut slow = TestWindow::new(&owner);
    let mut quick = TestWindow::new(&owner);
    slow.send(json!({"k": "share", "on": true}));
    slow.send(json!({"k": "hold", "topics": ["prices"]}));
    quick.send(json!({"k": "hold", "topics": ["prices"]}));
    slow.until("taken", |w| w.subscribed("prices")).await;
    quick.until("taken", |w| w.subscribed("prices")).await;
    // Behind, with batches in hand: its send's outcome waits at the owner.
    slow.holding = true;
    for n in 1..=4 {
        stand.publish("prices", n);
    }
    quick
        .until("the first four", |w| w.messages("prices").len() == 4)
        .await;
    slow.send(json!({"k": "send", "n": 1, "t": "order", "p": {"n": 1}, "o": {"id": "kept"}, "c": 1, "a": 0}));
    slow.until("sent", |w| w.has_frame(|f| f["k"] == "sent" && f["n"] == 1))
        .await;
    // What comes next outgrows its queue.
    for ten in 0..6 {
        for n in ten * 10 + 5..=ten * 10 + 14 {
            stand.publish("prices", n);
        }
        let seen = usize::try_from(ten * 10 + 14).unwrap();
        quick
            .until("each ten", |w| w.messages("prices").len() == seen)
            .await;
    }
    slow.settle(200).await;
    slow.holding = false;
    for batch in std::mem::take(&mut slow.held) {
        slow.take(batch);
    }
    slow.until("delivered", |w| {
        w.has_item(|i| {
            i["ev"] == "event"
                && i["name"] == "wseDelivered"
                && i["detail"]["ids"] == json!(["kept"])
        })
    })
    .await;
    assert!(slow.has_item(|i| i["ev"] == "overflow"));
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_of_another_protocol_is_refused_and_one_that_breaks_it_let_go() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let att = owner.attach(Probe(tx));
    owner.frame(att, r#"{"k":"hello","v":2,"agent":"next"}"#);
    let refused = rx.recv().await.unwrap();
    assert_eq!(
        (refused["k"].as_str(), refused["reason"].as_str()),
        (Some("refused"), Some("version"))
    );
    assert_eq!(rx.recv().await.unwrap()["k"], "closed");

    let mut early = TestWindow::new(&owner);
    early.send(json!({"k": "ack", "s": 3}));
    early
        .until("revoked", |w| {
            w.has_frame(|f| f["k"] == "revoked" && f["reason"] == "protocol")
        })
        .await;
    early
        .until("closed", |w| w.has_frame(|f| f["k"] == "closed"))
        .await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let att = owner.attach(Probe(tx));
    owner.frame(att, r#"{"k":"share","on":true}"#);
    assert_eq!(rx.recv().await.unwrap()["reason"], "protocol");
    assert_eq!(owner.attached(), 0);
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_signs_every_window_out() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    let mut b = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    b.until("welcome", |w| w.has_frame(|f| f["k"] == "welcome"))
        .await;
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    a.send(json!({"k": "signOut"}));
    b.until("revoked", |w| {
        w.has_frame(|f| f["k"] == "revoked" && f["reason"] == "signed_out")
    })
    .await;
    assert_eq!(owner.attached(), 0);
    // The owner is done: the next user gets a new one.
    let (tx, mut rx) = mpsc::unbounded_channel();
    owner.attach(Probe(tx));
    assert_eq!(rx.recv().await.unwrap()["reason"], "closing");
    assert!(owner.client().closed().await.is_none());
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_never_hears_the_credentials_of_the_endpoint() {
    let mut config = Config::new("ws://ann:password@127.0.0.1:9/wse?access_token=secret");
    config.reconnect.jitter = false;
    let owner = Owner::start(config, Options::default()).await.unwrap();
    let mut window = TestWindow::new(&owner);
    window
        .until("welcome", |w| w.has_frame(|f| f["k"] == "welcome"))
        .await;
    let heard = serde_json::to_string(&window.frames).unwrap();
    for secret in ["password", "access_token", "secret", "ann:"] {
        assert!(!heard.contains(secret), "{secret} in {heard}");
    }
    owner.close().await;
}

/// What the window heard of its sends delivered: the ids of each event.
fn delivered(w: &TestWindow) -> Vec<Value> {
    w.items
        .iter()
        .filter(|i| i["ev"] == "event" && i["name"] == "wseDelivered")
        .map(|i| i["detail"]["ids"].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_that_chose_the_same_id_each_hear_what_became_of_their_own_send() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    let mut b = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    b.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    a.send(json!({"k": "send", "n": 1, "t": "order", "p": {"n": 1}, "o": {"id": "order-1"}, "c": 1, "a": 0}));
    b.send(json!({"k": "send", "n": 1, "t": "order", "p": {"n": 2}, "o": {"id": "order-1"}, "c": 1, "a": 0}));
    a.until("its outcome", |w| !delivered(w).is_empty()).await;
    b.until("its outcome", |w| !delivered(w).is_empty()).await;
    a.settle(200).await;
    b.settle(200).await;
    assert_eq!(delivered(&a), [json!(["order-1"])]);
    assert_eq!(delivered(&b), [json!(["order-1"])]);
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_send_that_requires_topics_waits_after_a_reconnect_until_its_window_says_so_of_that_connection()
 {
    let (seen_tx, mut seen) = mpsc::unbounded_channel();
    let stand = stand_seeing(Some(seen_tx)).await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    a.send(json!({"k": "hold", "topics": ["prices"]}));
    a.until("prices taken", |w| w.subscribed("prices")).await;
    a.send(json!({"k": "continuous", "gen": 1, "topics": ["prices"]}));
    // The connection goes; the send comes while it is made again.
    a.send(json!({"k": "reconnect"}));
    a.until("lost", |w| w.has_item(|i| i["ev"] == "lost")).await;
    a.send(json!({"k": "send", "n": 1, "t": "cancel", "p": {"id": 9}, "o": {"id": "cancel-9", "requires": ["prices"]}, "c": 1, "a": 0}));
    // Said before the window heard of the loss, it comes after it.
    a.send(json!({"k": "continuous", "gen": 1, "topics": ["prices"]}));
    a.until("prices taken again", |w| {
        w.items.iter().filter(|i| i["ev"] == "subscribed").count() == 2
    })
    .await;
    a.settle(300).await;
    let mut types = Vec::new();
    while let Ok(kind) = seen.try_recv() {
        types.push(kind);
    }
    assert!(
        !types.iter().any(|t| t == "cancel"),
        "it went before the window said so of this connection: {types:?}"
    );
    a.send(json!({"k": "continuous", "gen": 2, "topics": ["prices"]}));
    let went = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(kind) = seen.recv().await {
            if kind == "cancel" {
                return true;
            }
        }
        false
    })
    .await;
    assert_eq!(went, Ok(true), "it never went once the window said so");
    a.until("delivered", |w| delivered(w) == [json!(["cancel-9"])])
        .await;
    owner.close().await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_send_that_runs_out_waiting_for_its_topics_is_said_so_to_its_window() {
    let stand = stand().await;
    let owner = stand.owner(Options::default()).await;
    let mut a = TestWindow::new(&owner);
    a.send(json!({"k": "share", "on": true}));
    a.until("connected", |w| w.has_item(|i| i["ev"] == "connected"))
        .await;
    a.send(json!({"k": "send", "n": 1, "t": "cancel", "p": {}, "o": {"id": "late", "requires": ["orders"], "ttl": 100}, "c": 1, "a": 0}));
    a.until("expired", |w| {
        w.has_item(|i| {
            i["name"] == "wseNotDelivered"
                && i["detail"]["reason"] == "expired"
                && i["detail"]["ids"] == json!(["late"])
        })
    })
    .await;
    assert!(delivered(&a).is_empty());
    owner.close().await;
    stand.server.stop().await;
}
