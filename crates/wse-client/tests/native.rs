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
    BatchKind, CancelToken, ErrorKind, Item, NativeClient, NativeConfig, NativeContinuity,
    NativeDelivery, NativeStatus, NativeSubscription, NativeTopicState, NativeTrust,
    StreamPosition, TokenSource,
};

#[path = "common/tls.rs"]
mod tls;

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
            insecure: true,
            ..NativeConfig::default()
        }
    }

    /// The same server behind a wss:// door: its URL and the door's CA.
    async fn tls(&self) -> (String, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let plain = self.url["ws://".len()..self.url.len() - "/wse".len()]
            .parse()
            .unwrap();
        let ca = tls::open(listener, plain);
        (format!("wss://localhost:{port}/wse"), ca)
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
        insecure: true,
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
        .send("order".into(), r#"{"qty":1}"#.into(), &CancelToken::new())
        .await
        .unwrap();
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_send_goes_on_the_ready_connection_or_fails() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("li"), None, &CancelToken::new())
        .await
        .unwrap();
    client.try_send_live("valve", r#"{"open":true}"#).unwrap();
    client
        .send_live(
            "valve".into(),
            r#"{"open":false}"#.into(),
            &CancelToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        client.try_send_live("valve", "not json").unwrap_err().kind,
        ErrorKind::Encode
    );
    client.disconnect(1_000).await;
    assert_eq!(
        client.try_send_live("valve", "{}").unwrap_err().kind,
        ErrorKind::Closed
    );
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_send_does_not_go_out() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("cy"), None, &CancelToken::new())
        .await
        .unwrap();
    let cancelled = CancelToken::new();
    cancelled.cancel();
    let refused = [
        client
            .send("order".into(), "{}".into(), &cancelled)
            .await
            .unwrap_err(),
        client
            .send_live("order".into(), "{}".into(), &cancelled)
            .await
            .unwrap_err(),
    ];
    for error in refused {
        assert_eq!(error.kind, ErrorKind::Cancelled);
    }
    client
        .send("after".into(), "{}".into(), &CancelToken::new())
        .await
        .unwrap();
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_client_says_so_and_comes_back() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("pi"), None, &CancelToken::new())
        .await
        .unwrap();
    client.pause("background".into()).unwrap();
    let mut seen = Vec::new();
    let mut ack = 0;
    while !seen.iter().any(|item| matches!(item, Item::Paused { .. })) {
        let batch = tokio::time::timeout(
            Duration::from_secs(5),
            client.next_batch(ack, 64, 1 << 20, &CancelToken::new()),
        )
        .await
        .expect("an item within 5 s")
        .unwrap();
        ack = batch.id;
        seen.extend(batch.items);
    }
    assert!(seen.contains(&Item::Paused {
        reasons: vec!["background".into()]
    }));
    assert_eq!(client.status(), NativeStatus::Paused);
    client.resume("background".into()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.status() != NativeStatus::Connected {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("connected again");
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_goes_in_the_clear_only_when_asked() {
    let stand = stand().await;
    let config = NativeConfig {
        insecure: false,
        ..stand.config("eve")
    };
    let error = NativeClient::connect(config, None, &CancelToken::new())
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ErrorKind::Config, "{error}");
    assert!(error.message.contains("insecure"), "{error}");
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_private_ca_is_trusted_where_it_is_named() {
    let stand = stand().await;
    let (url, ca) = stand.tls().await;
    let config = |trust| NativeConfig {
        url: url.clone(),
        trust,
        insecure: false,
        ..stand.config("fay")
    };
    // That CA alone: PEM, and the same certificate in DER.
    let der = ca
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    let der = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, der).unwrap();
    for roots in [vec![ca.clone().into_bytes()], vec![der]] {
        let client =
            NativeClient::connect(config(NativeTrust::Roots(roots)), None, &CancelToken::new())
                .await
                .expect("the CA named");
        assert_eq!(client.user_id().as_deref(), Some("fay"));
        client.disconnect(1_000).await;
    }
    // The system's verifier and the Mozilla roots do not know it.
    for trust in [NativeTrust::Platform, NativeTrust::Mozilla] {
        let error = NativeClient::connect(config(trust.clone()), None, &CancelToken::new())
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, ErrorKind::Connect, "{trust:?}: {error}");
        assert!(error.message.contains("certificate"), "{trust:?}: {error}");
    }
    // Roots that are no certificate: said at once.
    let error = NativeClient::connect(
        config(NativeTrust::Roots(vec![b"not a certificate".to_vec()])),
        None,
        &CancelToken::new(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind, ErrorKind::Config, "{error}");
    stand.server.stop().await;
}

/// Reads batches, acknowledging each, until `done` says so of an item; every
/// item read, in order.
async fn read_until(
    client: &NativeClient,
    ack: &mut u64,
    done: impl Fn(&Item) -> bool,
) -> Vec<Item> {
    let cancel = CancelToken::new();
    let mut out = Vec::new();
    loop {
        let batch = tokio::time::timeout(
            Duration::from_secs(5),
            client.next_batch(*ack, 64, 1 << 20, &cancel),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "a batch within 5 s; read so far: {out:?}; states {:?}",
                client.topic_states()
            )
        })
        .unwrap();
        *ack = batch.id;
        let finished = batch.items.iter().any(&done);
        out.extend(batch.items);
        if finished {
            return out;
        }
    }
}

/// The `n` of each tick read.
fn numbers(items: &[Item]) -> Vec<u64> {
    items.iter().filter_map(number).collect()
}

fn number(item: &Item) -> Option<u64> {
    match item {
        Item::Message(m) if m.topic.as_deref() == Some("prices") => {
            let p: serde_json::Value = serde_json::from_str(m.json.as_deref()?).ok()?;
            p["n"].as_u64()
        }
        _ => None,
    }
}

fn recover_or_resync(stand: &Stand, user: &str) -> NativeConfig {
    NativeConfig {
        delivery: Some(NativeDelivery {
            consumer: "board".into(),
            ..NativeDelivery::default()
        }),
        ..stand.config(user)
    }
}

fn publish(stand: &Stand, n: u64) {
    stand
        .publisher
        .publish_local("prices", &format!(r#"{{"t":"tick","p":{{"n":{n}}}}}"#))
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_or_resync_holds_a_topic_until_the_application_resynced_it() {
    let stand = stand().await;
    let client = NativeClient::connect(recover_or_resync(&stand, "gil"), None, &CancelToken::new())
        .await
        .unwrap();
    let mut ack = 0;
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    let Some(Item::Resync {
        round,
        topics,
        reasons,
        committed,
    }) = items.last().cloned()
    else {
        panic!("{items:?}");
    };
    assert_eq!(topics, ["prices"]);
    assert_eq!(reasons, ["cold_start"]);
    assert!(committed.is_empty());
    assert_eq!(
        client.topic_states()[0].continuity,
        NativeContinuity::Resyncing
    );
    assert_eq!(
        client.topic_states()[0].subscription,
        NativeSubscription::Active
    );

    // Published while the application loads its state: held.
    publish(&stand, 1);
    publish(&stand, 2);
    let cancel = CancelToken::new();
    let quiet = tokio::time::timeout(
        Duration::from_millis(300),
        client.next_batch(ack, 64, 1 << 20, &cancel),
    )
    .await;
    assert!(quiet.is_err(), "nothing comes before the resync: {quiet:?}");
    client.resynced(round);
    let items = read_until(&client, &mut ack, |i| number(i) == Some(2)).await;
    assert_eq!(numbers(&items), [1, 2]);
    assert!(items.iter().any(|i| matches!(
        i,
        Item::Continuity {
            continuity: NativeContinuity::Continuous,
            ..
        }
    )));
    publish(&stand, 3);
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Message(_))).await;
    assert_eq!(numbers(&items), [3]);
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_resyncs_and_recovers_from_what_was_committed() {
    let stand = stand().await;
    let client = NativeClient::connect(recover_or_resync(&stand, "hal"), None, &CancelToken::new())
        .await
        .unwrap();
    let mut ack = 0;
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    let Some(Item::Resync { round, .. }) = items.last().cloned() else {
        panic!("{items:?}");
    };
    client.resynced(round);
    for n in 1..=4 {
        publish(&stand, n);
    }
    let items = read_until(&client, &mut ack, |i| number(i) == Some(4)).await;
    let messages: Vec<_> = items
        .iter()
        .filter_map(|i| match i {
            Item::Message(m) => Some(m.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(messages.len(), 4);
    let epoch = messages[0].epoch.unwrap();
    let at: Vec<u64> = messages.iter().map(|m| m.offset.unwrap()).collect();
    // Nothing applied yet: nothing in the checkpoint.
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert!(saved["topics"]["prices"].is_null(), "{saved}");
    // Applied out of order: the checkpoint moves only through what has no hole.
    client.commit("prices", epoch, at[0]);
    client.commit("prices", epoch, at[2]);
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(saved["topics"]["prices"][format!("{epoch:08x}")], at[0]);
    client.commit("prices", epoch, at[1]);
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(saved["topics"]["prices"][format!("{epoch:08x}")], at[2]);
    assert_eq!(saved["user"], "hal");
    assert_eq!(saved["consumer"], "board");

    // 4 came but was not applied: after the reconnect it comes again.
    client.reconnect().unwrap();
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    let Some(Item::Resync {
        round,
        reasons,
        committed,
        ..
    }) = items.last().cloned()
    else {
        panic!("{items:?}");
    };
    assert_eq!(reasons, ["reconnected"]);
    assert_eq!(
        committed,
        [StreamPosition {
            topic: "prices".into(),
            epoch,
            offset: at[2]
        }]
    );
    client.resynced(round);
    let items = read_until(&client, &mut ack, |i| number(i) == Some(4)).await;
    assert_eq!(numbers(&items), [4]);
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_checkpoint_of_another_consumer_is_refused_aloud() {
    let stand = stand().await;
    let mut config = recover_or_resync(&stand, "ida");
    config.delivery.as_mut().unwrap().checkpoint = Some(format!(
        r#"{{"v":1,"realm":"{}","user":"ida","consumer":"audit","schema":null,"topics":{{"prices":{{"0000000a":9}}}}}}"#,
        stand.url
    ));
    let client = NativeClient::connect(config, None, &CancelToken::new())
        .await
        .unwrap();
    let mut ack = 0;
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    assert!(
        items.contains(&Item::CheckpointRejected {
            reason: "consumer".into()
        }),
        "{items:?}"
    );
    let Some(Item::Resync { committed, .. }) = items.last() else {
        panic!("{items:?}");
    };
    assert!(committed.is_empty());
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn messages_held_past_the_limit_start_the_resync_over() {
    let stand = stand().await;
    let mut config = recover_or_resync(&stand, "jo");
    config.delivery.as_mut().unwrap().max_held = 2;
    let client = NativeClient::connect(config, None, &CancelToken::new())
        .await
        .unwrap();
    let mut ack = 0;
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    let Some(Item::Resync { round, .. }) = items.last().cloned() else {
        panic!("{items:?}");
    };
    for n in 1..=3 {
        publish(&stand, n);
    }
    // Let the three arrive (and be held) before the resync ends.
    let cancel = CancelToken::new();
    let _ = tokio::time::timeout(
        Duration::from_millis(300),
        client.next_batch(ack, 64, 1 << 20, &cancel),
    )
    .await;
    client.resynced(round);
    let items = read_until(&client, &mut ack, |i| matches!(i, Item::Resync { .. })).await;
    assert!(numbers(&items).is_empty(), "{items:?}");
    let Some(Item::Resync { reasons, .. }) = items.last() else {
        panic!("{items:?}");
    };
    assert_eq!(reasons, &["overflow"]);
    client.disconnect(1_000).await;
    stand.server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn best_effort_says_what_the_server_took_and_lets_the_app_say_it_resynced() {
    let stand = stand().await;
    let client = NativeClient::connect(stand.config("kay"), None, &CancelToken::new())
        .await
        .unwrap();
    subscribed(&client).await;
    assert_eq!(
        client.topic_states(),
        [NativeTopicState {
            topic: "prices".into(),
            subscription: NativeSubscription::Active,
            continuity: NativeContinuity::Continuous,
            reason: None,
        }]
    );
    client.resync(vec!["prices".into()]);
    assert_eq!(
        client.topic_states()[0].continuity,
        NativeContinuity::Continuous
    );
    client.disconnect(1_000).await;
    stand.server.stop().await;
}
