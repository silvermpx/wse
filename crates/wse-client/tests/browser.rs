//! The client compiled for the browser, against a running server: the
//! `server` example, whose URL and keys come in at build time.
//!
//!     cargo run -p wse-client --example server &
//!     WSE_TEST_URL=ws://127.0.0.1:5007/wse WSE_TEST_IDENTITY=<e2e key> \
//!     WSE_TEST_SIGNING=<signing key> \
//!     CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!     cargo test -p wse-client --target wasm32-unknown-unknown --test browser
//!
//! The runner runs it in Node, whose WebSocket is the browser's. Built
//! without `WSE_TEST_URL`, the tests fail: they need the server.

#![cfg(target_arch = "wasm32")]

use std::time::Duration;

use serde_json::json;
use wasm_bindgen_test::wasm_bindgen_test;
#[cfg(feature = "aws-lc")]
use wse_client::Encryption;
use wse_client::{Config, Continuity, Delivery, Event, Events, Incoming, OwnedMessage, Resync};

const URL: Option<&str> = option_env!("WSE_TEST_URL");
#[cfg(feature = "aws-lc")]
const IDENTITY: Option<&str> = option_env!("WSE_TEST_IDENTITY");
#[cfg(feature = "aws-lc")]
const SIGNING: Option<&str> = option_env!("WSE_TEST_SIGNING");

/// A setting of the test server, given at build time (see the top).
fn given(value: Option<&'static str>, name: &str) -> &'static str {
    value.unwrap_or_else(|| panic!("{name} is not set: these tests need the example server"))
}

/// `future`, or a panic after `secs`.
async fn within<F: std::future::Future>(secs: u64, future: F) -> F::Output {
    let timeout = gloo_timers::future::sleep(Duration::from_secs(secs));
    futures_util::pin_mut!(future);
    futures_util::pin_mut!(timeout);
    match futures_util::future::select(future, timeout).await {
        futures_util::future::Either::Left((output, _)) => output,
        futures_util::future::Either::Right(_) => panic!("nothing within {secs} s"),
    }
}

async fn next(events: &mut Events) -> Incoming {
    within(5, events.next()).await.expect("the stream open")
}

async fn message(events: &mut Events, kind: &str) -> OwnedMessage {
    loop {
        if let Incoming::Message(message) = next(events).await
            && message.kind() == kind
        {
            return message;
        }
    }
}

async fn resync(events: &mut Events) -> Resync {
    loop {
        if let Incoming::Event(Event::Resync(resync)) = next(events).await {
            return resync;
        }
    }
}

#[wasm_bindgen_test]
async fn recover_or_resync_in_the_browser() {
    let url = given(URL, "WSE_TEST_URL");
    let mut config = Config::new(url);
    config.topics = vec!["ticks".into()];
    config.delivery = Some(Delivery::new("board"));
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    // A cold start: the topic is held until the application resynced it.
    let cold = resync(&mut events).await;
    assert_eq!(cold.reasons, ["cold_start"]);
    // Ticks come ten times a second; none reaches the application while it
    // loads its state.
    gloo_timers::future::sleep(Duration::from_millis(300)).await;
    while let Some(item) = events.try_next() {
        assert!(!matches!(item, Incoming::Message(_)), "held: {item:?}");
    }
    client.resynced(cold.round).unwrap();
    let first = message(&mut events, "tick").await;
    let second = message(&mut events, "tick").await;
    let (a, b) = (first.position().unwrap(), second.position().unwrap());
    assert_eq!(b.offset, a.offset + 1, "what was held, in order");
    assert_eq!(client.topic_states()[0].continuity, Continuity::Continuous);
    // Applied in order: the checkpoint moves through both.
    client.commit(&first.as_message());
    client.commit(&second.as_message());
    let saved: serde_json::Value = serde_json::from_str(&client.checkpoint()).unwrap();
    assert_eq!(
        saved["topics"]["ticks"][format!("{:08x}", b.epoch)],
        b.offset
    );

    // A reconnect resyncs, and recovery starts from what was committed: the
    // tick after it comes again.
    client.reconnect().unwrap();
    let again = resync(&mut events).await;
    assert_eq!(again.reasons, ["reconnected"]);
    assert_eq!(again.committed[0].offset, b.offset);
    client.resynced(again.round).unwrap();
    let next_one = message(&mut events, "tick").await;
    assert_eq!(next_one.position().unwrap().offset, b.offset + 1);
    client.close().await;
}

/// A send that requires a topic waits for its resync; one that requires
/// nothing goes past it.
#[wasm_bindgen_test]
async fn a_send_in_the_browser_waits_for_its_topic() {
    let url = given(URL, "WSE_TEST_URL");
    let mut config = Config::new(url);
    config.topics = vec!["ticks".into()];
    config.delivery = Some(Delivery::new("board"));
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    let cold = resync(&mut events).await;
    client
        .send_requiring("echo", &serde_json::json!({"n": 1}), ["ticks"])
        .await
        .unwrap();
    client
        .send("echo", &serde_json::json!({"n": 2}))
        .await
        .unwrap();
    let first = message(&mut events, "echoed").await;
    assert_eq!(first.payload_value().unwrap()["n"], 2);
    client.resynced(cold.round).unwrap();
    let second = message(&mut events, "echoed").await;
    assert_eq!(second.payload_value().unwrap()["n"], 1);
    client.close().await;
}

/// What is sent with no connection waits in the outbox, in memory, and goes
/// on the next.
#[wasm_bindgen_test]
async fn a_send_in_the_browser_with_no_connection_waits_for_the_next() {
    let url = given(URL, "WSE_TEST_URL");
    let config = Config::new(url);
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    client.pause("test").unwrap();
    while client.is_connected() {
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
    client
        .send("echo", &serde_json::json!({"n": 1}))
        .await
        .unwrap();
    assert_eq!(client.kept(), 1);
    client.resume("test").unwrap();
    let echoed = message(&mut events, "echoed").await;
    assert_eq!(echoed.payload_value().unwrap()["n"], 1);
    // It leaves the outbox once the server confirmed it.
    for _ in 0..250 {
        if client.kept() == 0 {
            break;
        }
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(client.kept(), 0);
    client.close().await;
}

#[wasm_bindgen_test]
async fn a_browser_client_hears_its_topic_and_is_heard() {
    let url = given(URL, "WSE_TEST_URL");
    let mut config = Config::new(url);
    config.topics = vec!["ticks".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    assert_eq!(client.user_id().as_deref(), Some("guest"));
    let Incoming::Event(Event::Connected(connected)) = next(&mut events).await else {
        panic!("connected first");
    };
    assert!(connected.recovery);

    // Ticks in order, with their positions.
    let first = message(&mut events, "tick").await;
    let second = message(&mut events, "tick").await;
    let (a, b) = (first.position().unwrap(), second.position().unwrap());
    assert_eq!(b.offset, a.offset + 1);
    assert_eq!(
        second.payload_value().unwrap()["n"].as_u64(),
        first.payload_value().unwrap()["n"].as_u64().map(|n| n + 1)
    );

    client.send("echo", &json!({"said": "hi"})).await.unwrap();
    let echoed = message(&mut events, "echoed").await;
    assert_eq!(echoed.payload_value().unwrap()["said"], "hi");
    client.close().await;
}

#[wasm_bindgen_test]
async fn msgpack_in_the_browser() {
    let url = given(URL, "WSE_TEST_URL");
    let mut config = Config::new(url);
    config.format = wse_client::Format::Msgpack;
    config.topics = vec!["ticks".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    let tick = message(&mut events, "tick").await;
    assert_eq!(tick.payload_json(), None, "msgpack, not JSON");
    assert!(tick.payload_value().unwrap()["n"].as_u64().is_some());
    client.close().await;
}

#[cfg(feature = "aws-lc")]
#[wasm_bindgen_test]
async fn an_encrypted_session_from_the_browser() {
    let (url, identity) = (
        given(URL, "WSE_TEST_URL"),
        given(IDENTITY, "WSE_TEST_IDENTITY"),
    );
    let mut config = Config::new(url);
    config.encryption = Some(Encryption::pinned([identity]));
    config.topics = vec!["ticks".into()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    message(&mut events, "tick").await;
    client.send("echo", &json!({"sealed": true})).await.unwrap();
    let echoed = message(&mut events, "echoed").await;
    assert_eq!(echoed.payload_value().unwrap()["sealed"], true);
    client.close().await;

    // Another server's key: refused.
    let other = wse_client::public_key(&wse_client::generate_key()).unwrap();
    let mut config = Config::new(url);
    config.encryption = Some(Encryption::pinned([other]));
    let error = wse_client::connect(config).await.err().unwrap();
    assert!(matches!(error, wse_client::Error::Encryption(_)), "{error}");
}

#[cfg(feature = "aws-lc")]
#[wasm_bindgen_test]
async fn signed_messages_from_the_browser() {
    let (url, signing) = (
        given(URL, "WSE_TEST_URL"),
        given(SIGNING, "WSE_TEST_SIGNING"),
    );
    let key = wse_client::generate_key();
    let mut config = Config::new(url);
    config.signing_key = Some(key.clone());
    config.server_signing_keys = vec![signing.to_owned()];
    let (client, mut events) = wse_client::connect(config).await.unwrap();
    client
        .send(
            "bind_key",
            &json!({"public_key": wse_client::public_key(&key).unwrap()}),
        )
        .await
        .unwrap();
    let bound = message(&mut events, "key_bound").await;
    assert_eq!(bound.payload_value().unwrap()["bound"], true);

    client
        .send_signed("order", &json!({"side": "buy"}), Some("web-1"))
        .await
        .unwrap();
    let receipt = message(&mut events, "receipt").await;
    assert_eq!(receipt.payload_value().unwrap()["n"], "web-1");
    assert!(receipt.signature().unwrap().verified);
    client.close().await;
}
