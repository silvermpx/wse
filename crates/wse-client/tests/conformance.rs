//! The Rust client's driver for the conformance server
//! (crates/wse-conformance): it runs each case it can, step by step, and
//! records what its client saw and did; the server judges. Runs when
//! WSE_CONFORMANCE holds the server's control URL:
//!
//!     cargo run -p wse-conformance -- serve        # prints the control URL
//!     WSE_CONFORMANCE=ws://127.0.0.1:P/control cargo test -p wse-client --test conformance

// It claims end-to-end encryption and signing: the default build's.
#![cfg(all(not(target_arch = "wasm32"), feature = "aws-lc"))]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wse_client::{
    Client, Config, Continuity, Delivery, Encryption, Event, Events, Format, Incoming, SendOptions,
};

/// What the Rust client has, by the catalog's names.
const FEATURES: [&str; 7] = [
    "outbox",
    "start",
    "live",
    "msgpack",
    "e2e",
    "signing",
    "recover_or_resync",
];

/// The control channel: requests and their answers, by id.
struct Control {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next: u64,
}

impl Control {
    async fn call(&mut self, op: &str, body: Value) -> Result<Value, String> {
        self.next += 1;
        let id = self.next;
        let mut request = body;
        request["id"] = json!(id);
        request["op"] = json!(op);
        self.ws
            .send(Frame::text(request.to_string()))
            .await
            .map_err(|e| e.to_string())?;
        while let Some(frame) = self.ws.next().await {
            let Ok(Frame::Text(text)) = frame else {
                continue;
            };
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
    began: Instant,
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
            began: Instant::now(),
        }
    }

    fn record(&mut self, mut event: Value) {
        event["t"] = json!(u64::try_from(self.began.elapsed().as_millis()).unwrap_or(u64::MAX));
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
                let deadline = Instant::now()
                    + Duration::from_millis(step["timeout_ms"].as_u64().unwrap_or(10_000));
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
                let until =
                    Instant::now() + Duration::from_millis(step["ms"].as_u64().unwrap_or(0));
                while Instant::now() < until {
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
        let user = step["user"].as_str().unwrap_or("ann");
        let mut config = Config::new(self.begun["url"].as_str().unwrap_or_default());
        config.token = self.begun["tokens"][user].as_str().map(str::to_owned);
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
                    let deadline = Instant::now() + Duration::from_secs(10);
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
    async fn pump(&mut self, deadline: Instant) {
        self.drain();
        let Some(events) = self.events.as_mut() else {
            tokio::time::sleep_until(deadline.into()).await;
            return;
        };
        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(20));
        if let Ok(Some(item)) = tokio::time::timeout(wait, events.next()).await {
            self.take(item);
        }
    }

    async fn until(&mut self, step: &Value, deadline: Instant) -> bool {
        loop {
            self.drain();
            if self.holds(step) {
                return true;
            }
            if Instant::now() >= deadline {
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
        match step["for"].as_str().unwrap_or_default() {
            "ready" => self.ready,
            "not_ready" => !self.ready,
            "messages" => {
                messages(&|e| e["topic"] == *topic) as u64 >= step["count"].as_u64().unwrap_or(1)
            }
            "message" => {
                messages(&|e| e["type"] == step["type"])
                    >= step["count"].as_u64().unwrap_or(1) as usize
            }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rust_client_passes_every_case_it_can() {
    let Ok(url) = std::env::var("WSE_CONFORMANCE") else {
        assert!(
            std::env::var("WSE_CONFORMANCE_REQUIRED").as_deref() != Ok("1"),
            "WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not"
        );
        return;
    };
    let (ws, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .expect("the conformance server");
    let mut control = Control { ws, next: 0 };
    let hello = control
        .call(
            "hello",
            json!({
                "agent": "rust-client",
                "version": env!("CARGO_PKG_VERSION"),
                "runtime": "tokio",
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
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
