//! The Rust client compiled for the browser, against the conformance server
//! (crates/wse-conformance): the Rust driver's cases, run on the browser's
//! connection (the client's browser module, on Node's WebSocket, which is
//! the browser's). The control URL comes in at build time:
//!
//!     cargo run -p wse-conformance -- serve        # prints the control URL
//!     WSE_CONFORMANCE=ws://127.0.0.1:P/control \
//!     CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!     cargo test -p wse-client --target wasm32-unknown-unknown --test conformance_browser
//!
//! A page sends its cookie with the upgrade; Node sends none, so the driver
//! hands the case's token to the WebSocket as a bearer (the server takes
//! either), in the one place a page would have its cookie.
// It claims end-to-end encryption and signing: the default build's.
#![cfg(all(target_arch = "wasm32", feature = "aws-lc"))]

use std::collections::HashMap;
use std::time::Duration;

use futures_util::future::{Either, select};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use wasm_bindgen::prelude::{Closure, wasm_bindgen};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;
use wse_client::{
    Client, Config, Continuity, Delivery, Encryption, Event, Events, Format, Incoming, SendOptions,
};

const CONTROL: Option<&str> = option_env!("WSE_CONFORMANCE");

/// What the Rust client in the browser has, by the catalog's names.
const FEATURES: [&str; 7] = [
    "outbox",
    "start",
    "live",
    "msgpack",
    "e2e",
    "signing",
    "recover_or_resync",
];

#[wasm_bindgen(inline_js = r#"
// Each WebSocket made from here on carries `token` as its bearer.
export function bearer(token) {
  const Base = globalThis.__wseWebSocket ?? globalThis.WebSocket;
  globalThis.__wseWebSocket = Base;
  globalThis.__wseBearer = token;
  globalThis.WebSocket = class extends Base {
    constructor(url, protocols) {
      const headers = { Authorization: `Bearer ${globalThis.__wseBearer}` };
      super(url, protocols === undefined ? { headers } : { protocols, headers });
    }
  };
}
"#)]
extern "C" {
    fn bearer(token: &str);
}

/// Milliseconds since the epoch, the browser's clock.
fn now() -> f64 {
    js_sys::Date::now()
}

async fn sleep(ms: f64) {
    gloo_timers::future::sleep(Duration::from_millis(ms.max(0.0) as u64)).await;
}

/// The control channel: requests and their answers, by id.
struct Control {
    socket: web_sys::WebSocket,
    answers: mpsc::UnboundedReceiver<String>,
    next: u64,
    // Kept for the socket's life.
    _onmessage: Closure<dyn FnMut(web_sys::MessageEvent)>,
    _onopen: Closure<dyn FnMut(JsValue)>,
}

impl Control {
    async fn open(url: &str) -> Self {
        let socket = web_sys::WebSocket::new(url).expect("a control socket");
        let (tx, answers) = mpsc::unbounded_channel();
        let (opened_tx, mut opened) = mpsc::unbounded_channel();
        let onmessage = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(
            move |event: web_sys::MessageEvent| {
                if let Some(text) = event.data().as_string() {
                    let _ = tx.send(text);
                }
            },
        );
        let onopen = Closure::<dyn FnMut(JsValue)>::new(move |_| {
            let _ = opened_tx.send(());
        });
        socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        socket.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        opened.recv().await.expect("the conformance server");
        Self {
            socket,
            answers,
            next: 0,
            _onmessage: onmessage,
            _onopen: onopen,
        }
    }

    async fn call(&mut self, op: &str, body: Value) -> Result<Value, String> {
        self.next += 1;
        let id = self.next;
        let mut request = body;
        request["id"] = json!(id);
        request["op"] = json!(op);
        self.socket
            .send_with_str(&request.to_string())
            .map_err(|e| format!("{op}: {e:?}"))?;
        while let Some(text) = self.answers.recv().await {
            let answer: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if answer["id"] == id {
                return if answer["ok"] == true {
                    Ok(answer)
                } else {
                    Err(format!("{op}: {}", answer["error"]))
                };
            }
        }
        Err(format!("{op}: the conformance server went"))
    }
}

/// One case: the client, what it saw, and the steps' doings.
struct Run {
    begun: Value,
    trace: Vec<Value>,
    client: Option<Client>,
    events: Option<Events>,
    ready: bool,
    rr: bool,
    published: HashMap<String, Vec<u64>>,
    /// Resync rounds waiting for a step to say the application resynced.
    resyncs: Vec<(u64, Vec<String>)>,
    key: Option<Vec<u8>>,
    bound: bool,
    began: f64,
}

impl Run {
    fn new(begun: Value) -> Self {
        Self {
            begun,
            trace: Vec::new(),
            client: None,
            events: None,
            ready: false,
            rr: false,
            published: HashMap::new(),
            resyncs: Vec::new(),
            key: None,
            bound: false,
            began: now(),
        }
    }

    fn record(&mut self, mut event: Value) {
        event["t"] = json!((now() - self.began) as u64);
        self.trace.push(event);
    }

    async fn go(&mut self, control: &mut Control) {
        let steps = self.begun["steps"].as_array().cloned().unwrap_or_default();
        for (at, step) in steps.iter().enumerate() {
            self.drain();
            if step["server"].is_string() {
                self.record(json!({"ev": "step", "server": step["server"]}));
                match control.call("act", json!({"step": step})).await {
                    Ok(done) => {
                        if step["server"] == "publish" {
                            let topic = done["topic"].as_str().unwrap_or_default().to_owned();
                            let numbers = done["n"].as_array().cloned().unwrap_or_default();
                            self.published
                                .entry(topic)
                                .or_default()
                                .extend(numbers.iter().filter_map(Value::as_u64));
                        }
                    }
                    Err(why) => {
                        self.record(json!({"ev": "error", "message": format!("step {at}: {why}")}));
                        break;
                    }
                }
                continue;
            }
            match self.client_step(step).await {
                Ok(true) => {}
                Ok(false) => {
                    self.record(json!({"ev": "timeout", "step": at, "waiting": step}));
                    break;
                }
                Err(why) => {
                    self.record(json!({"ev": "error", "message": format!("step {at}: {why}")}));
                    break;
                }
            }
        }
        self.close().await;
    }

    /// A step of the client's: false when what it waits for did not come.
    async fn client_step(&mut self, step: &Value) -> Result<bool, String> {
        match step["client"].as_str().unwrap_or_default() {
            "connect" => self.connect(step).await.map(|()| true),
            "wait" => {
                let deadline = now() + step["timeout_ms"].as_f64().unwrap_or(10_000.0);
                Ok(self.until(step, deadline).await)
            }
            "send" => self.send(step).await.map(|()| true),
            "pause" => {
                self.client()?.pause("test").map_err(|e| e.to_string())?;
                Ok(true)
            }
            "resume" => {
                self.client()?.resume("test").map_err(|e| e.to_string())?;
                Ok(true)
            }
            "resynced" => {
                let topics: Vec<String> =
                    serde_json::from_value(step["topics"].clone()).unwrap_or_default();
                self.record(json!({"ev": "resynced", "topics": topics}));
                let (done, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.resyncs)
                    .into_iter()
                    .partition(|(_, of)| of.iter().any(|t| topics.contains(t)));
                self.resyncs = rest;
                for (round, _) in done {
                    self.client()?.resynced(round).map_err(|e| e.to_string())?;
                }
                Ok(true)
            }
            "sleep" => {
                let until = now() + step["ms"].as_f64().unwrap_or(0.0);
                while now() < until {
                    self.pump(until).await;
                }
                Ok(true)
            }
            "close" => {
                self.close().await;
                Ok(true)
            }
            other => Err(format!("no such client step: {other}")),
        }
    }

    fn client(&self) -> Result<&Client, String> {
        self.client.as_ref().ok_or_else(|| "no client".to_owned())
    }

    async fn connect(&mut self, step: &Value) -> Result<(), String> {
        // A client made again: the last one goes, as a dropped handle's does.
        if self.client.is_some() {
            self.close().await;
        }
        let user = step["user"].as_str().unwrap_or("ann");
        bearer(self.begun["tokens"][user].as_str().unwrap_or_default());
        let mut config = Config::new(self.begun["url"].as_str().unwrap_or_default());
        config.topics = serde_json::from_value(step["topics"].clone()).unwrap_or_default();
        config.reconnect.base_delay = Duration::from_millis(50);
        config.reconnect.max_delay = Duration::from_millis(300);
        config.reconnect.jitter = false;
        if step["format"] == "msgpack" {
            config.format = Format::Msgpack;
        }
        if step["e2e"] == true {
            config.encryption = Some(Encryption::pinned([self.begun["identity_key"]
                .as_str()
                .unwrap_or_default()]));
        }
        if step["signing"] == true {
            let key = wse_client::generate_key();
            config.signing_key = Some(key.clone());
            config.server_signing_keys = vec![
                self.begun["signing_key"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            ];
            self.key = Some(key);
        }
        self.rr = step["recover_or_resync"] == true;
        if self.rr {
            config.delivery = Some(Delivery::new("conformance"));
        }
        let (client, events) = if step["start"] == true {
            wse_client::start(config).await
        } else {
            wse_client::connect(config).await
        }
        .map_err(|e| e.to_string())?;
        self.client = Some(client);
        self.events = Some(events);
        Ok(())
    }

    async fn send(&mut self, step: &Value) -> Result<(), String> {
        let kind = step["type"].as_str().unwrap_or_default().to_owned();
        let count = step["count"].as_u64().unwrap_or(1);
        for k in 1..=count {
            let id = step["id"]
                .as_str()
                .map_or_else(|| format!("{kind}-{k}"), str::to_owned);
            let payload = if step["payload"].is_null() {
                json!({"n": k})
            } else {
                step["payload"].clone()
            };
            if step["signed"] == true {
                if !self.bound {
                    let public = wse_client::public_key(self.key.as_ref().ok_or("no signing key")?)
                        .map_err(|e| e.to_string())?;
                    let mut options = SendOptions::default();
                    options.id = Some("bind-key".to_owned());
                    let bind = self
                        .client()?
                        .send_with("bind_key", &json!({"public_key": public}), &options)
                        .await;
                    self.record(json!({"ev": "sent", "id": "bind-key", "type": "bind_key", "live": false, "ok": bind.is_ok()}));
                    bind.map_err(|e| e.to_string())?;
                    let deadline = now() + 10_000.0;
                    if !self
                        .until(&json!({"for": "message", "type": "key_bound"}), deadline)
                        .await
                    {
                        return Err("the server never bound the key".to_owned());
                    }
                    self.bound = true;
                }
                let sent = self.client()?.send_signed(&kind, &payload, Some(&id)).await;
                self.record(json!({"ev": "sent", "id": id, "type": kind, "signed": true, "live": false, "ok": sent.is_ok()}));
                continue;
            }
            let mut options = SendOptions::default();
            options.id = Some(id.clone());
            options.correlation_id = step["correlation_id"].as_str().map(str::to_owned);
            options.priority = step["priority"].as_u64().and_then(|p| u8::try_from(p).ok());
            if step["live"] == true {
                options.live = true;
                let sent = self.client()?.try_send_with(&kind, &payload, &options);
                self.record(
                    json!({"ev": "sent", "id": id, "type": kind, "live": true, "ok": sent.is_ok()}),
                );
                continue;
            }
            let sent = self.client()?.send_with(&kind, &payload, &options).await;
            self.record(
                json!({"ev": "sent", "id": id, "type": kind, "live": false, "ok": sent.is_ok()}),
            );
        }
        Ok(())
    }

    /// Takes what the client has for the application now.
    fn drain(&mut self) {
        while let Some(item) = self.events.as_mut().and_then(Events::try_next) {
            self.take(item);
        }
    }

    /// Takes what comes until `deadline`, or the next item.
    async fn pump(&mut self, deadline: f64) {
        self.drain();
        let wait = (deadline - now()).clamp(0.0, 20.0);
        let Some(events) = self.events.as_mut() else {
            sleep(wait).await;
            return;
        };
        let item = {
            let next = events.next();
            let timer = sleep(wait);
            futures_util::pin_mut!(next);
            futures_util::pin_mut!(timer);
            match select(next, timer).await {
                Either::Left((item, _)) => item,
                Either::Right(_) => None,
            }
        };
        if let Some(item) = item {
            self.take(item);
        }
    }

    async fn until(&mut self, step: &Value, deadline: f64) -> bool {
        loop {
            self.drain();
            if self.holds(step) {
                return true;
            }
            if now() >= deadline {
                return false;
            }
            self.pump(deadline).await;
        }
    }

    fn take(&mut self, item: Incoming) {
        match item {
            Incoming::Message(message) => {
                let payload = message.payload_value().unwrap_or(Value::Null);
                let topic = message.topic().map(str::to_owned);
                self.record(json!({
                    "ev": "message",
                    "type": message.kind(),
                    "topic": topic,
                    "n": payload["n"].as_u64(),
                    "payload": payload,
                    "cid": message.correlation_id(),
                }));
                // Applied: the checkpoint moves on.
                if self.rr
                    && topic.is_some()
                    && let Some(client) = &self.client
                {
                    client.commit(&message.as_message());
                }
            }
            Incoming::Event(Event::Connected(_)) => {
                self.ready = true;
                self.record(json!({"ev": "ready"}));
            }
            Incoming::Event(Event::Disconnected(_) | Event::Paused(_)) => self.ready = false,
            Incoming::Event(Event::Gap(gap)) => {
                self.record(json!({"ev": "break", "topic": gap.topic, "kind": "gap"}));
            }
            Incoming::Event(Event::Continuity(change)) => {
                if change.continuity != Continuity::Continuous {
                    self.record(json!({"ev": "break", "topic": change.topic, "kind": "continuity", "reason": change.reason}));
                }
            }
            Incoming::Event(Event::Resync(resync)) => {
                let cold = resync.reasons.iter().all(|r| r == "cold_start");
                self.record(json!({"ev": "resync", "topics": resync.topics, "auto": cold, "reasons": resync.reasons}));
                // At a cold start the application has its state at once.
                if cold {
                    self.record(json!({"ev": "resynced", "topics": resync.topics, "auto": true}));
                    if let Some(client) = &self.client {
                        let _ = client.resynced(resync.round);
                    }
                } else {
                    self.resyncs.push((resync.round, resync.topics));
                }
            }
            Incoming::Event(Event::Delivered(ids)) => {
                self.record(json!({"ev": "delivered", "ids": ids}))
            }
            Incoming::Event(Event::NotDelivered(not)) => {
                self.record(json!({"ev": "not_delivered", "reason": not.reason, "ids": not.ids}));
            }
            _ => {}
        }
    }

    fn holds(&self, step: &Value) -> bool {
        let messages = |pick: &dyn Fn(&Value) -> bool| {
            self.trace
                .iter()
                .filter(|e| e["ev"] == "message" && pick(e))
                .count()
        };
        let topic = &step["topic"];
        let count = step["count"].as_u64().unwrap_or(1) as usize;
        match step["for"].as_str().unwrap_or_default() {
            "ready" => self.ready,
            "not_ready" => !self.ready,
            "messages" => messages(&|e| e["topic"] == *topic) >= count,
            "message" => messages(&|e| e["type"] == step["type"]) >= count,
            "published" => {
                let last = step["last"].as_u64().unwrap_or(1) as usize;
                let numbers = self
                    .published
                    .get(topic.as_str().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let wanted = &numbers[numbers.len().saturating_sub(last)..];
                wanted.len() == last
                    && wanted.iter().all(|n| {
                        self.trace
                            .iter()
                            .any(|e| e["ev"] == "message" && e["topic"] == *topic && e["n"] == *n)
                    })
            }
            "break" => self.trace.iter().any(|e| {
                (e["ev"] == "break" && e["topic"] == *topic)
                    || (e["ev"] == "resync"
                        && e["topics"].as_array().is_some_and(|t| t.contains(topic)))
            }),
            "resync" => self.trace.iter().any(|e| {
                e["ev"] == "resync"
                    && e["auto"] != true
                    && e["topics"].as_array().is_some_and(|t| t.contains(topic))
            }),
            "delivered" => {
                let delivered: Vec<&Value> = self
                    .trace
                    .iter()
                    .filter(|e| e["ev"] == "delivered")
                    .flat_map(|e| e["ids"].as_array().into_iter().flatten())
                    .collect();
                let ids: Vec<Value> = if step["all"] == true {
                    self.trace
                        .iter()
                        .filter(|e| {
                            e["ev"] == "sent"
                                && e["ok"] == true
                                && e["live"] != true
                                && e["signed"] != true
                        })
                        .map(|e| e["id"].clone())
                        .collect()
                } else {
                    step["ids"].as_array().cloned().unwrap_or_default()
                };
                ids.iter().all(|id| delivered.contains(&id))
            }
            _ => false,
        }
    }

    async fn close(&mut self) {
        self.drain();
        if let Some(client) = self.client.take() {
            client.close().await;
        }
        self.drain();
        self.events = None;
        self.ready = false;
    }
}

#[wasm_bindgen_test]
async fn the_rust_client_in_the_browser_passes_every_case_it_can() {
    let Some(url) = CONTROL else {
        assert!(
            option_env!("WSE_CONFORMANCE_REQUIRED") != Some("1"),
            "WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not"
        );
        return;
    };
    let mut control = Control::open(url).await;
    let hello = control
        .call(
            "hello",
            json!({
                "agent": "rust-browser",
                "version": env!("CARGO_PKG_VERSION"),
                "runtime": "wasm32 (the browser's WebSocket, in Node)",
                "os": "browser",
                "arch": "wasm32",
                "features": FEATURES,
            }),
        )
        .await
        .unwrap();
    let mut failed = Vec::new();
    for case in hello["cases"].as_array().cloned().unwrap_or_default() {
        let name = case["case"].as_str().unwrap_or_default();
        let begun = control.call("begin", json!({"case": name})).await.unwrap();
        let mut run = Run::new(begun);
        run.go(&mut control).await;
        for chunk in run.trace.chunks(500) {
            control
                .call("trace", json!({"events": chunk}))
                .await
                .unwrap();
        }
        let judged = control.call("end", json!({})).await.unwrap();
        if judged["verdict"] != "pass" {
            failed.push(format!("{name}: {}", judged["failures"]));
        }
    }
    control.call("bye", json!({})).await.unwrap();
    assert!(failed.is_empty(), "{failed:#?}");
}
