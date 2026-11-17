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
use wse_client::{
    Config, Continuity, Delivery, Event, Events, Incoming, Offline, OwnedMessage, Resync,
};

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

// -- The outbox in IndexedDB ---------------------------------------------------
//
// Node has neither IndexedDB nor Web Locks: fake-indexeddb (the
// repository's development dependency: `npm ci` first) stands for the one,
// and locks kept as a browser keeps them for the other.

/// IndexedDB and Web Locks in this Node, once.
fn browser_storage() {
    let install = js_sys::Function::new_with_args(
        "root",
        r#"
        if (!globalThis.indexedDB) {
          const { createRequire } = process.getBuiltinModule('node:module');
          createRequire(root)('fake-indexeddb/auto');
        }
        if (globalThis.navigator.locks) return;
        const held = new Set();
        const waiting = new Map();
        const locks = {
          async request(name, options, callback) {
            if (typeof options === 'function') { callback = options; options = {}; }
            if (held.has(name)) {
              if (options.ifAvailable) return callback(null);
              await new Promise((resolve) => waiting.set(name, [...(waiting.get(name) ?? []), resolve]));
            }
            held.add(name);
            try { return await callback({ name, mode: 'exclusive' }); }
            finally { held.delete(name); waiting.get(name)?.shift()?.(); }
          },
        };
        Object.defineProperty(globalThis.navigator, 'locks', { value: locks, configurable: true });
        "#,
    );
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/../../package.json");
    install
        .call1(&wasm_bindgen::JsValue::NULL, &root.into())
        .expect("fake-indexeddb from the repository's node_modules");
}

/// Each producer in the database `name` and the ids of its messages.
async fn stored(name: &str) -> Vec<Vec<String>> {
    let read = js_sys::Function::new_with_args(
        "name",
        r#"
        return new Promise((resolve, reject) => {
          const open = indexedDB.open(name);
          open.onerror = () => reject(open.error);
          open.onsuccess = () => {
            const db = open.result;
            if (!db.objectStoreNames.contains('outbox')) { db.close(); resolve([]); return; }
            const all = db.transaction(['outbox']).objectStore('outbox').getAll();
            all.onerror = () => reject(all.error);
            all.onsuccess = () => {
              db.close();
              const by = new Map();
              for (const row of all.result) by.set(row.producer, [...(by.get(row.producer) ?? []), row.id]);
              resolve([...by.values()]);
            };
          };
        });
        "#,
    );
    let promise: js_sys::Promise = read
        .call1(&wasm_bindgen::JsValue::NULL, &name.into())
        .unwrap()
        .into();
    let rows = wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
    js_sys::Array::from(&rows)
        .iter()
        .map(|ids| {
            js_sys::Array::from(&ids)
                .iter()
                .filter_map(|id| id.as_string())
                .collect()
        })
        .collect()
}

async fn paused(client: &wse_client::Client) {
    client.pause("test").unwrap();
    while client.is_connected() {
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
}

#[wasm_bindgen_test]
async fn what_waits_in_indexed_db_goes_with_the_next_client_once() {
    browser_storage();
    let mut config = Config::new(given(URL, "WSE_TEST_URL"));
    config.offline = Some(Offline::in_indexed_db("wse-test-outbox-next"));
    let (first, _events) = wse_client::connect(config.clone()).await.unwrap();
    paused(&first).await;
    let id = first
        .send_with("echo", &json!({"n": 41}), &Default::default())
        .await
        .unwrap();
    assert_eq!(first.kept(), 1);
    first.close().await;
    for _ in 0..250 {
        if stored("wse-test-outbox-next").await == vec![vec![id.clone()]] {
            break;
        }
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(stored("wse-test-outbox-next").await, vec![vec![id.clone()]]);

    // The next client takes the producer over: the server takes it once.
    let (second, mut events) = wse_client::connect(config).await.unwrap();
    let echoed = message(&mut events, "echoed").await;
    assert_eq!(echoed.payload_value().unwrap()["n"], 41);
    for _ in 0..250 {
        if second.kept() == 0
            && stored("wse-test-outbox-next")
                .await
                .iter()
                .all(Vec::is_empty)
        {
            break;
        }
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(second.kept(), 0);
    assert!(
        stored("wse-test-outbox-next")
            .await
            .iter()
            .all(Vec::is_empty)
    );
    second.close().await;
}

#[wasm_bindgen_test]
async fn two_clients_of_one_database_keep_a_producer_each() {
    browser_storage();
    let mut config = Config::new(given(URL, "WSE_TEST_URL"));
    config.offline = Some(Offline::in_indexed_db("wse-test-outbox-two"));
    let (a, _a_events) = wse_client::connect(config.clone()).await.unwrap();
    let (b, _b_events) = wse_client::connect(config).await.unwrap();
    paused(&a).await;
    paused(&b).await;
    let from_a = a
        .send_with("echo", &json!({"n": 1}), &Default::default())
        .await
        .unwrap();
    let from_b = b
        .send_with("echo", &json!({"n": 2}), &Default::default())
        .await
        .unwrap();
    a.close().await;
    b.close().await;
    let mut held = Vec::new();
    for _ in 0..250 {
        held = stored("wse-test-outbox-two").await;
        held.sort();
        if held.len() == 2 {
            break;
        }
        gloo_timers::future::sleep(Duration::from_millis(20)).await;
    }
    let mut expected = vec![vec![from_a], vec![from_b]];
    expected.sort();
    assert_eq!(held, expected);
}

#[wasm_bindgen_test]
async fn without_web_locks_the_outbox_stays_in_memory_and_says_so() {
    browser_storage();
    let navigator =
        js_sys::Object::from(js_sys::Reflect::get(&js_sys::global(), &"navigator".into()).unwrap());
    // Hidden by a property of its own that says there are none, whether the
    // runtime's are the object's own or its prototype's (Node 24's), and
    // afterwards back as they were.
    let own = js_sys::Object::get_own_property_descriptor(&navigator, &"locks".into());
    let none = js_sys::Object::new();
    js_sys::Reflect::set(&none, &"value".into(), &wasm_bindgen::JsValue::UNDEFINED).unwrap();
    js_sys::Reflect::set(&none, &"configurable".into(), &true.into()).unwrap();
    js_sys::Object::define_property(&navigator, &"locks".into(), &none);
    let mut config = Config::new(given(URL, "WSE_TEST_URL"));
    config.offline = Some(Offline::in_indexed_db("wse-test-outbox-nolocks"));
    let connected = wse_client::connect(config).await;
    if own.is_undefined() {
        js_sys::Reflect::delete_property(&navigator, &"locks".into()).unwrap();
    } else {
        js_sys::Object::define_property(&navigator, &"locks".into(), &own.into());
    }
    let (client, mut events) = connected.unwrap();
    let failed = loop {
        if let Incoming::Event(Event::StorageFailed(failed)) = next(&mut events).await {
            break failed;
        }
    };
    assert_eq!(failed.operation, "open");
    assert!(failed.message.contains("Web Locks"), "{}", failed.message);
    // In memory, as before.
    client.send("echo", &json!({"n": 7})).await.unwrap();
    let echoed = message(&mut events, "echoed").await;
    assert_eq!(echoed.payload_value().unwrap()["n"], 7);
    client.close().await;
}
