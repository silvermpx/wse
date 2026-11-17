//! One connection for every window of an application: the window bridge's
//! owner.
//!
//! An [`Owner`] holds a [`Client`], and so one connection, for the windows
//! it serves (the webviews of a Tauri application, say): their shares of it,
//! the union of their topics, their sends into one outbox. Each window holds
//! a client of `wse-client/bridge` (TypeScript) over a channel the
//! application carries; the owner hands each window what it holds and
//! listens to, in batches the window acknowledges as it takes them, and
//! routes back what became of the window's own sends. What depends on the
//! application stays in the window: its listeners, the delivery's barrier,
//! resync, commit and checkpoint. The frames are JSON text, the TypeScript
//! owner's protocol (`ts-client/bridge/protocol.ts`).
//!
//! ```no_run
//! # async fn run() -> Result<(), wse_client::Error> {
//! use wse_client::bridge::{Owner, Options, Window};
//!
//! struct Webview; // the application's channel to one window
//! impl Window for Webview {
//!     fn post(&self, frame: String) { /* hand the frame to the window */ }
//!     fn close(&self) {}
//! }
//!
//! let mut config = wse_client::Config::new("wss://example.com/wse");
//! config.token = Some("...".to_owned());
//! let owner = Owner::start(config, Options::default()).await?;
//! let window = owner.attach(Webview);
//! owner.frame(window, r#"{"k":"hello","v":1,"agent":"main"}"#); // each frame the window sends
//! # Ok(()) }
//! ```

use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};
use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::handler::{Event, GapReason, Handler};
use crate::message::Message;
use crate::{Client, Config, Error, SendOptions};

/// The protocol's major version: an owner and a window of others do not
/// speak.
pub const PROTOCOL: u32 = 1;

/// The reason the owner's client is paused for while no one shares it.
const IDLE: &str = "wse-bridge-idle";

/// A window asks for a new connection at most this often.
const RECONNECT_EVERY: Duration = Duration::from_secs(5);

/// How much the owner hands a window at once, and holds for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Limits {
    /// Items in one batch at most.
    pub batch_items: usize,
    /// Bytes of one batch's items at most; a larger item goes alone.
    pub batch_bytes: usize,
    /// Batches handed and not acknowledged at most.
    pub in_flight: u64,
    /// Items waiting for a window at most: past it, or `queue_bytes`, its
    /// queue is dropped and its topics said to have broken off.
    pub queue_items: usize,
    /// Bytes waiting for a window at most.
    pub queue_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            batch_items: 256,
            batch_bytes: 1 << 20,
            in_flight: 2,
            queue_items: 10_000,
            queue_bytes: 64 << 20,
        }
    }
}

/// The owner's end of one window's channel.
pub trait Window: Send + Sync + 'static {
    /// A frame for the window, JSON text, in order. Called while the owner
    /// holds its lock: hand the frame on, never wait for the window.
    fn post(&self, frame: String);
    /// The window is let go: its channel ends.
    fn close(&self);
}

/// Decides whether a window may sign a message of a type with the owner's key.
pub type AllowSign = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// How the owner serves its windows.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Options {
    pub limits: Limits,
    /// Whether a window may sign a message of a type with the owner's key
    /// (default: every type).
    pub allow_sign: Option<AllowSign>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Options")
            .field("limits", &self.limits)
            .field("allow_sign", &self.allow_sign.is_some())
            .finish()
    }
}

/// Holds one connection for the windows it serves. Clones share it.
#[derive(Clone)]
pub struct Owner {
    client: Client,
    shared: Arc<Shared>,
}

impl fmt::Debug for Owner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Owner").finish_non_exhaustive()
    }
}

/// A share of the connection the application holds itself: it is made while
/// any share is, a window's or the application's.
#[derive(Debug)]
pub struct Share {
    owner: Owner,
}

impl Drop for Share {
    fn drop(&mut self) {
        let mut state = self.owner.shared.lock();
        self.owner.unshare(&mut state);
    }
}

/// What the owner and its client's handler share.
struct Shared {
    state: Mutex<State>,
    limits: Limits,
    allow_sign: Option<AllowSign>,
    encrypts: bool,
    realm: String,
    runtime: tokio::runtime::Handle,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The owner's connection, as a window shows it.
struct Conn {
    connection: &'static str,
    paused: Vec<String>,
    endpoint: Option<String>,
    last_error: Option<String>,
    storage: &'static str,
}

impl Conn {
    fn json(&self, ready: bool) -> String {
        let mut out = String::with_capacity(128);
        out.push_str("{\"connection\":\"");
        out.push_str(self.connection);
        let _ = write!(out, "\",\"ready\":{ready},\"paused\":");
        push_strings(&mut out, &self.paused);
        out.push_str(",\"endpoint\":");
        push_opt(&mut out, self.endpoint.as_deref());
        out.push_str(",\"lastError\":");
        push_opt(&mut out, self.last_error.as_deref());
        out.push_str(",\"storage\":\"");
        out.push_str(self.storage);
        out.push_str("\"}");
        out
    }
}

/// A window the owner serves.
struct Win {
    window: Arc<dyn Window>,
    welcomed: bool,
    share: bool,
    holds: HashSet<String>,
    listens: HashSet<String>,
    queue: VecDeque<Arc<str>>,
    bytes: usize,
    /// The last batch handed, and the last the window took.
    handed: u64,
    took: u64,
    reconnected: Option<Instant>,
}

struct State {
    windows: HashMap<u64, Win>,
    /// Each topic the windows hold: how many hold it.
    topics: HashMap<String, usize>,
    /// The topics the server took on this connection.
    active: HashSet<String>,
    /// Whose send each outbox message is, by its id: its outcome goes there.
    sender: HashMap<String, u64>,
    shares: usize,
    next: u64,
    generation: u64,
    user: Option<String>,
    ready: bool,
    conn: Conn,
    ended: bool,
}

impl Owner {
    /// Starts the owner's client, paused until a window (or the
    /// application, [`retain`](Self::retain)) shares it. Runs on the Tokio
    /// runtime it is called on. The windows keep their own delivery: a
    /// `config.delivery` is refused.
    pub async fn start(mut config: Config, options: Options) -> Result<Self, Error> {
        if config.delivery.is_some() {
            return Err(Error::Config(
                "the bridge's windows keep their own delivery: the owner's config has none"
                    .to_owned(),
            ));
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Config("the bridge's owner runs on a Tokio runtime".to_owned()))?;
        config.paused = vec![IDLE.to_owned()];
        let durable = config.offline.as_ref().is_some_and(|o| o.path.is_some());
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                windows: HashMap::new(),
                topics: HashMap::new(),
                active: HashSet::new(),
                sender: HashMap::new(),
                shares: 0,
                next: 0,
                generation: 0,
                user: None,
                ready: false,
                conn: Conn {
                    connection: "pending",
                    paused: Vec::new(),
                    endpoint: bare(&config.url),
                    last_error: None,
                    storage: if durable { "durable" } else { "memory" },
                },
                ended: false,
            }),
            limits: options.limits,
            allow_sign: options.allow_sign,
            encrypts: config.encryption.is_some(),
            realm: config.url.clone(),
            runtime,
        });
        let client = Client::start(
            config,
            Routes {
                shared: Arc::clone(&shared),
            },
        )
        .await?;
        Ok(Self { client, shared })
    }

    /// The owner's own client: the application sends on it as on any.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// A share of the connection for the application itself.
    pub fn retain(&self) -> Share {
        let mut state = self.shared.lock();
        self.share(&mut state);
        Share {
            owner: self.clone(),
        }
    }

    /// The windows served now.
    pub fn attached(&self) -> usize {
        self.shared.lock().windows.len()
    }

    /// Serves a window over `window`: the number its frames are given with.
    /// An owner that ended refuses it.
    pub fn attach(&self, window: impl Window) -> u64 {
        let window: Arc<dyn Window> = Arc::new(window);
        let mut state = self.shared.lock();
        if state.ended {
            window.post(r#"{"k":"refused","reason":"closing"}"#.to_owned());
            window.close();
            return 0;
        }
        state.next += 1;
        let att = state.next;
        state.windows.insert(
            att,
            Win {
                window,
                welcomed: false,
                share: false,
                holds: HashSet::new(),
                listens: HashSet::new(),
                queue: VecDeque::new(),
                bytes: 0,
                handed: 0,
                took: 0,
                reconnected: None,
            },
        );
        att
    }

    /// Lets window `att` go: its share, its topics, its channel.
    pub fn detach(&self, att: u64) {
        let mut state = self.shared.lock();
        self.detach_in(&mut state, att);
    }

    /// A frame window `att` sent, JSON text.
    pub fn frame(&self, att: u64, text: &str) {
        let frame: Frame<'_> = match serde_json::from_str(text) {
            Ok(frame) => frame,
            Err(error) => {
                let mut state = self.shared.lock();
                self.revoke(&mut state, att, "protocol", Some(&error.to_string()));
                return;
            }
        };
        let mut state = self.shared.lock();
        let Some(win) = state.windows.get_mut(&att) else {
            return;
        };
        let kind = frame.k.clone();
        if !win.welcomed && kind != "hello" {
            let detail = format!("{kind} before hello");
            self.revoke(&mut state, att, "protocol", Some(&detail));
            return;
        }
        match &*kind {
            "hello" => {
                if frame.v != Some(PROTOCOL) {
                    let mut out = String::from(r#"{"k":"refused","reason":"version","detail":"#);
                    push_str(
                        &mut out,
                        &format!("the owner speaks {PROTOCOL}, not {:?}", frame.v),
                    );
                    out.push('}');
                    win.window.post(out);
                    self.detach_in(&mut state, att);
                    return;
                }
                win.welcomed = true;
                let welcome = self.welcome(&state, att);
                if let Some(win) = state.windows.get(&att) {
                    win.window.post(welcome);
                }
            }
            "share" => {
                let on = frame.on == Some(true);
                if win.share != on {
                    win.share = on;
                    if on {
                        self.share(&mut state);
                    } else {
                        self.unshare(&mut state);
                    }
                }
            }
            "hold" => {
                for topic in frame.topics.unwrap_or_default() {
                    self.hold(&mut state, att, topic);
                }
            }
            "drop" => {
                for topic in frame.topics.unwrap_or_default() {
                    self.drop_topic(&mut state, att, &topic);
                }
            }
            "listen" => win.listens.extend(frame.types.unwrap_or_default()),
            "unlisten" => {
                for kind in frame.types.unwrap_or_default() {
                    win.listens.remove(&kind);
                }
            }
            "ack" => {
                let s = frame.s.unwrap_or(u64::MAX);
                if s > win.handed || s < win.took {
                    let detail = format!("ack {s} of batches {}..{}", win.took, win.handed);
                    self.revoke(&mut state, att, "protocol", Some(&detail));
                    return;
                }
                win.took = s;
                pump(win, &self.shared.limits);
            }
            "send" => self.send(&mut state, att, &frame),
            "sign" => self.sign(&state, att, frame),
            "reconnect" => {
                // A window asks at most once in five seconds.
                let now = Instant::now();
                if win
                    .reconnected
                    .is_some_and(|at| now.duration_since(at) < RECONNECT_EVERY)
                {
                    return;
                }
                win.reconnected = Some(now);
                let _ = self.client.reconnect();
            }
            "wake" => {
                if !state.ready && state.shares > 0 {
                    let _ = self.client.reconnect();
                }
            }
            "signOut" => self.sign_out_in(&mut state),
            "bye" => self.detach_in(&mut state, att),
            other => {
                let detail = format!("no frame {other}");
                self.revoke(&mut state, att, "protocol", Some(&detail));
            }
        }
    }

    /// The user signs out: every window is let go, and the client with
    /// what it kept for the user. The next user gets a new owner.
    pub fn sign_out(&self) {
        let mut state = self.shared.lock();
        self.sign_out_in(&mut state);
    }

    /// The owner ends: every window is let go, then the client closes; what
    /// it keeps in a file stays.
    pub async fn close(&self) {
        {
            let mut state = self.shared.lock();
            if !state.ended {
                state.ended = true;
                self.revoke_all(&mut state, "owner_closed");
            }
        }
        self.client.close().await;
    }

    fn sign_out_in(&self, state: &mut State) {
        if state.ended {
            return;
        }
        state.ended = true;
        self.revoke_all(state, "signed_out");
        let _ = self.client.clear();
        let client = self.client.clone();
        self.shared
            .runtime
            .spawn(async move { client.close().await });
    }

    fn welcome(&self, state: &State, att: u64) -> String {
        let limits = &self.shared.limits;
        let mut out = String::with_capacity(320);
        let _ = write!(
            out,
            r#"{{"k":"welcome","v":{PROTOCOL},"att":{att},"limits":{{"batchItems":{},"batchBytes":{},"inFlight":{},"queueItems":{},"queueBytes":{}}},"realm":"#,
            limits.batch_items,
            limits.batch_bytes,
            limits.in_flight,
            limits.queue_items,
            limits.queue_bytes,
        );
        push_str(&mut out, &self.shared.realm);
        out.push_str(r#","account":{"epoch":0,"user":"#);
        push_opt(&mut out, state.user.as_deref());
        out.push_str(r#"},"conn":"#);
        out.push_str(&state.conn.json(state.ready));
        let generation = if state.ready { state.generation } else { 0 };
        let _ = write!(out, r#","gen":{generation}}}"#);
        out
    }

    fn share(&self, state: &mut State) {
        state.shares += 1;
        if state.shares == 1 {
            let _ = self.client.resume(IDLE);
            if !state.ready && state.conn.paused.is_empty() {
                state.conn.connection = "connecting";
                conn_changed(state);
            }
        }
    }

    fn unshare(&self, state: &mut State) {
        state.shares = state.shares.saturating_sub(1);
        if state.shares == 0 && !state.ended {
            let _ = self.client.pause(IDLE);
        }
    }

    fn hold(&self, state: &mut State, att: u64, topic: String) {
        let State {
            windows,
            topics,
            active,
            ..
        } = state;
        let Some(win) = windows.get_mut(&att) else {
            return;
        };
        if !win.holds.insert(topic.clone()) {
            return;
        }
        match topics.entry(topic) {
            Entry::Occupied(mut held) => {
                *held.get_mut() += 1;
                // The server took it for another window already: this one hears so now.
                if active.contains(held.key()) {
                    let mut item = String::from(r#"{"ev":"subscribed","taken":["#);
                    push_str(&mut item, held.key());
                    item.push_str(r#"],"refused":[]}"#);
                    enqueue(win, Arc::from(item), &self.shared.limits, active);
                }
            }
            Entry::Vacant(free) => {
                let _ = self.client.subscribe([free.key().as_str()]);
                free.insert(1);
            }
        }
    }

    fn drop_topic(&self, state: &mut State, att: u64, topic: &str) {
        let Some(win) = state.windows.get_mut(&att) else {
            return;
        };
        if !win.holds.remove(topic) {
            return;
        }
        if let Some(count) = state.topics.get_mut(topic) {
            *count -= 1;
            if *count == 0 {
                state.topics.remove(topic);
                state.active.remove(topic);
                let _ = self.client.unsubscribe([topic]);
            }
        }
    }

    fn send(&self, state: &mut State, att: u64, frame: &Frame<'_>) {
        let n = frame.n.unwrap_or(0);
        let options = frame.o.as_ref();
        let id = options.and_then(|o| o.id.as_deref());
        let live = options.and_then(|o| o.live) == Some(true);
        let payload = frame.p.unwrap_or(NULL);
        let kind = frame.t.as_deref().unwrap_or_default();
        let status = if kind.is_empty()
            // A live one goes on the connection the window saw ready, or not at all.
            || (live && !(state.ready && frame.c == Some(state.generation)))
            // Asked to go encrypted on a connection that is not: it does not go.
            || (options.and_then(|o| o.encrypted) == Some(true) && !self.shared.encrypts)
        {
            "dropped"
        } else {
            let send = SendOptions {
                id: id.map(str::to_owned),
                ttl: options
                    .and_then(|o| o.ttl)
                    .filter(|ttl| !live && ttl.is_finite() && *ttl >= 0.0)
                    .map(|ttl| Duration::from_millis(ttl as u64)),
                requires: Vec::new(),
                correlation_id: options.and_then(|o| o.correlation_id.clone()),
                // The TypeScript client's priorities run 1 to 10.
                priority: options
                    .and_then(|o| o.priority)
                    .filter(|p| p.is_finite())
                    .map(|p| p.clamp(0.0, 255.0) as u8),
                live,
            };
            match self.client.try_send_with(kind, payload, &send) {
                Ok(id) if live => {
                    let _ = id;
                    "sent"
                }
                Ok(id) => {
                    state.sender.insert(id, att);
                    if self.client.is_connected() {
                        "sent"
                    } else {
                        "queued"
                    }
                }
                Err(_) => "dropped",
            }
        };
        if let Some(win) = state.windows.get(&att) {
            win.window.post(sent_frame(n, status, id, None));
        }
    }

    fn sign(&self, state: &State, att: u64, frame: Frame<'_>) {
        let Some(win) = state.windows.get(&att) else {
            return;
        };
        let n = frame.n.unwrap_or(0);
        let nonce = frame.o.and_then(|o| o.nonce);
        let kind = frame.t.unwrap_or_default();
        let allowed = !kind.is_empty()
            && self
                .shared
                .allow_sign
                .as_ref()
                .is_none_or(|allow| allow(&kind));
        if !allowed {
            win.window
                .post(sent_frame(n, "dropped", None, nonce.as_deref()));
            return;
        }
        let payload: Box<RawValue> = frame.p.unwrap_or(NULL).to_owned();
        let client = self.client.clone();
        let window = Arc::clone(&win.window);
        self.shared.runtime.spawn(async move {
            let status = match client.send_signed(&kind, &*payload, nonce.as_deref()).await {
                Ok(_) => "sent",
                Err(_) => "dropped",
            };
            window.post(sent_frame(n, status, None, nonce.as_deref()));
        });
    }

    fn revoke(&self, state: &mut State, att: u64, reason: &str, detail: Option<&str>) {
        if let Some(win) = state.windows.get(&att) {
            win.window.post(revoked_frame(reason, detail));
        }
        self.detach_in(state, att);
    }

    fn revoke_all(&self, state: &mut State, reason: &str) {
        let atts: Vec<u64> = state.windows.keys().copied().collect();
        for att in atts {
            self.revoke(state, att, reason, None);
        }
    }

    fn detach_in(&self, state: &mut State, att: u64) {
        let Some(win) = state.windows.remove(&att) else {
            return;
        };
        if win.share {
            self.unshare(state);
        }
        for topic in &win.holds {
            if let Some(count) = state.topics.get_mut(topic) {
                *count -= 1;
                if *count == 0 {
                    state.topics.remove(topic);
                    state.active.remove(topic);
                    let _ = self.client.unsubscribe([topic.as_str()]);
                }
            }
        }
        state.sender.retain(|_, sender| *sender != att);
        win.window.close();
    }
}

/// The client's handler: what the server sends, routed to the windows.
struct Routes {
    shared: Arc<Shared>,
}

impl Handler for Routes {
    fn message(&mut self, message: &Message<'_>) {
        let mut state = self.shared.lock();
        let State {
            windows, active, ..
        } = &mut *state;
        let mut item: Option<Arc<str>> = None;
        for win in windows.values_mut() {
            if !win.welcomed {
                continue;
            }
            let wanted = match message.topic() {
                Some(topic) => win.holds.contains(topic),
                None => win.listens.contains(message.kind()) || win.listens.contains("*"),
            };
            if !wanted {
                continue;
            }
            let item = item.get_or_insert_with(|| message_item(message)).clone();
            enqueue(win, item, &self.shared.limits, active);
        }
    }

    fn event(&mut self, event: Event) {
        let limits = &self.shared.limits;
        let mut guard = self.shared.lock();
        let state = &mut *guard;
        match event {
            Event::Connected(connected) => {
                state.generation += 1;
                state.ready = true;
                state.user = connected.user_id;
                state.conn.connection = "connected";
                state.conn.last_error = None;
                let mut item = format!(r#"{{"ev":"connected","gen":{},"user":"#, state.generation);
                push_opt(&mut item, state.user.as_deref());
                item.push('}');
                to_all(state, Arc::from(item), limits);
                conn_changed(state);
            }
            Event::Disconnected(disconnected) => {
                lost(state, limits);
                state.conn.connection = "reconnecting";
                state.conn.last_error = Some(disconnected.reason);
                conn_changed(state);
            }
            Event::Paused(paused) => {
                lost(state, limits);
                let reasons: Vec<String> =
                    paused.reasons.into_iter().filter(|r| r != IDLE).collect();
                state.conn.connection = if reasons.is_empty() {
                    "pending"
                } else {
                    "disconnected"
                };
                state.conn.paused = reasons;
                conn_changed(state);
            }
            Event::Subscribed(subscribed) => {
                let State {
                    windows, active, ..
                } = state;
                for topic in &subscribed.topics {
                    active.insert(topic.clone());
                }
                for topic in &subscribed.failed {
                    active.remove(topic);
                }
                for win in windows.values_mut().filter(|w| w.welcomed) {
                    let taken: Vec<&String> = subscribed
                        .topics
                        .iter()
                        .filter(|t| win.holds.contains(*t))
                        .collect();
                    let refused: Vec<&String> = subscribed
                        .failed
                        .iter()
                        .filter(|t| win.holds.contains(*t))
                        .collect();
                    if taken.is_empty() && refused.is_empty() {
                        continue;
                    }
                    let mut item = String::from(r#"{"ev":"subscribed","taken":"#);
                    push_strings(&mut item, &taken);
                    item.push_str(r#","refused":"#);
                    push_strings(&mut item, &refused);
                    item.push('}');
                    enqueue(win, Arc::from(item), limits, active);
                }
            }
            Event::Gap(gap) => {
                let mut item = String::from(r#"{"ev":"gap","detail":{"topic":"#);
                push_str(&mut item, &gap.topic);
                let _ = write!(item, r#","epoch":"{:08x}","fromOffset":"#, gap.epoch);
                push_number(&mut item, gap.first);
                item.push_str(r#","toOffset":"#);
                push_number(&mut item, gap.last);
                item.push_str(r#","missed":"#);
                push_number(&mut item, gap.missed());
                let reason = match gap.reason {
                    GapReason::NotRecovered | GapReason::Unavailable => "not_recovered",
                    GapReason::EpochChanged => "epoch_changed",
                    _ => "offset_gap",
                };
                let _ = write!(item, r#","reason":"{reason}"}}}}"#);
                let item: Arc<str> = Arc::from(item);
                let State {
                    windows, active, ..
                } = state;
                for win in windows
                    .values_mut()
                    .filter(|w| w.welcomed && w.holds.contains(&gap.topic))
                {
                    enqueue(win, Arc::clone(&item), limits, active);
                }
            }
            Event::ServerError(error) => {
                let mut item =
                    String::from(r#"{"ev":"event","name":"wseServerError","detail":{"message":"#);
                push_str(&mut item, &error.message);
                item.push_str(r#","code":"#);
                push_str(&mut item, &error.code);
                item.push_str(r#","details":"#);
                item.push_str(&error.details.to_string());
                item.push_str("}}");
                to_all(state, Arc::from(item), limits);
            }
            Event::Delivered(ids) => outcome(state, "wseDelivered", None, ids, limits),
            Event::NotDelivered(not) => {
                outcome(state, "wseNotDelivered", Some(&not.reason), not.ids, limits);
            }
            Event::StorageFailed(failed) => {
                if failed.operation == "open" {
                    state.conn.storage = "unavailable";
                    conn_changed(state);
                }
                let mut item = String::from(
                    r#"{"ev":"event","name":"wseStorageError","detail":{"operation":"#,
                );
                push_str(&mut item, &failed.operation);
                item.push_str(r#","name":"Error","message":"#);
                push_str(&mut item, &failed.message);
                item.push_str(r#","quota":false}}"#);
                to_all(state, Arc::from(item), limits);
            }
            Event::Closed(error) => {
                if let Some(error) = &error {
                    state.conn.last_error = Some(error.to_string());
                }
                state.ready = false;
                state.ended = true;
                let windows: Vec<Win> = state.windows.drain().map(|(_, win)| win).collect();
                for win in windows {
                    win.window.post(revoked_frame("owner_closed", None));
                    win.window.close();
                }
                state.topics.clear();
                state.active.clear();
                state.sender.clear();
            }
            _ => {}
        }
    }
}

/// The connection went: each window hears it, in order with its messages.
fn lost(state: &mut State, limits: &Limits) {
    if !state.ready {
        return;
    }
    state.ready = false;
    state.active.clear();
    to_all(state, Arc::from(r#"{"ev":"lost"}"#), limits);
}

/// The connection as it is now, to every window at once, beside the
/// batches: a window that holds its batches (paused, or behind) still shows it.
fn conn_changed(state: &State) {
    let mut frame = String::from(r#"{"k":"conn","state":"#);
    frame.push_str(&state.conn.json(state.ready));
    frame.push('}');
    for win in state.windows.values().filter(|w| w.welcomed) {
        win.window.post(frame.clone());
    }
}

fn to_all(state: &mut State, item: Arc<str>, limits: &Limits) {
    let State {
        windows, active, ..
    } = state;
    for win in windows.values_mut().filter(|w| w.welcomed) {
        enqueue(win, Arc::clone(&item), limits, active);
    }
}

/// What became of windows' sends goes to the window that sent each.
fn outcome(state: &mut State, name: &str, reason: Option<&str>, ids: Vec<String>, limits: &Limits) {
    let mut by_window: HashMap<u64, Vec<String>> = HashMap::new();
    for id in ids {
        if let Some(att) = state.sender.remove(&id) {
            by_window.entry(att).or_default().push(id);
        }
    }
    let State {
        windows, active, ..
    } = state;
    for (att, ids) in by_window {
        let Some(win) = windows.get_mut(&att) else {
            continue;
        };
        let mut item = String::from(r#"{"ev":"event","name":"#);
        push_str(&mut item, name);
        item.push_str(r#","detail":{"#);
        if let Some(reason) = reason {
            item.push_str(r#""reason":"#);
            push_str(&mut item, reason);
            item.push(',');
        }
        item.push_str(r#""ids":"#);
        push_strings(&mut item, &ids);
        item.push_str("}}");
        enqueue(win, Arc::from(item), limits, active);
    }
}

/// An item for a window: past its limits its queue goes, and what it holds
/// broke off; the connection and the other windows go on.
fn enqueue(win: &mut Win, item: Arc<str>, limits: &Limits, active: &HashSet<String>) {
    win.bytes += item.len();
    win.queue.push_back(item);
    if win.queue.len() > limits.queue_items || win.bytes > limits.queue_bytes {
        win.queue.clear();
        win.bytes = 0;
        let holds: Vec<&String> = win.holds.iter().collect();
        let mut overflow = String::from(r#"{"ev":"overflow","topics":"#);
        push_strings(&mut overflow, &holds);
        overflow.push('}');
        win.queue.push_back(Arc::from(overflow));
        let taken: Vec<&String> = holds.into_iter().filter(|t| active.contains(*t)).collect();
        if !taken.is_empty() {
            let mut subscribed = String::from(r#"{"ev":"subscribed","taken":"#);
            push_strings(&mut subscribed, &taken);
            subscribed.push_str(r#","refused":[]}"#);
            win.queue.push_back(Arc::from(subscribed));
        }
        win.bytes = win.queue.iter().map(|i| i.len()).sum();
    }
    pump(win, limits);
}

/// Batches for the window, as many as it may have in hand.
fn pump(win: &mut Win, limits: &Limits) {
    while !win.queue.is_empty() && win.handed - win.took < limits.in_flight {
        let mut count = 0;
        let mut bytes = 0;
        for item in &win.queue {
            if count == limits.batch_items || (count > 0 && bytes + item.len() > limits.batch_bytes)
            {
                break;
            }
            bytes += item.len();
            count += 1;
        }
        win.handed += 1;
        let mut frame = String::with_capacity(bytes + count + 32);
        let _ = write!(frame, r#"{{"k":"b","s":{},"items":["#, win.handed);
        for (at, item) in win.queue.drain(..count).enumerate() {
            if at > 0 {
                frame.push(',');
            }
            frame.push_str(&item);
        }
        frame.push_str("]}");
        win.bytes -= bytes;
        win.window.post(frame);
    }
}

/// A message as the owner's client hands it, as the TypeScript client
/// shows it (`WSMessage`): `{"m": {...}}`.
fn message_item(message: &Message<'_>) -> Arc<str> {
    let converted;
    let payload = match message.payload_json() {
        Some(json) => json,
        None => {
            converted = message
                .payload_value()
                .map_or_else(|_| "null".to_owned(), |value| value.to_string());
            &converted
        }
    };
    let mut out = String::with_capacity(payload.len() + 128);
    out.push_str(r#"{"m":{"t":"#);
    push_str(&mut out, message.kind());
    out.push_str(r#","p":"#);
    out.push_str(payload);
    if let Some(category) = message.category() {
        out.push_str(r#","c":"#);
        push_str(&mut out, category);
    }
    if let Some(topic) = message.topic() {
        out.push_str(r#","tp":"#);
        push_str(&mut out, topic);
    }
    if let Some(position) = message.position() {
        let _ = write!(
            out,
            r#","e":"{:08x}","o":{}"#,
            position.epoch, position.offset
        );
    }
    if let Some(id) = message.id() {
        out.push_str(r#","id":"#);
        push_str(&mut out, id);
    }
    if let Some(cid) = message.correlation_id() {
        out.push_str(r#","cid":"#);
        push_str(&mut out, cid);
    }
    if let Some(ts) = message.timestamp() {
        out.push_str(r#","ts":"#);
        push_str(&mut out, ts);
    }
    if let Some(priority) = message.priority() {
        let _ = write!(out, r#","pri":{priority}"#);
    }
    if let Some(signature) = message.signature() {
        out.push_str(r#","signature":{"body":"#);
        push_str(&mut out, &signature.body);
        out.push_str(r#","sig":"#);
        push_str(&mut out, &signature.sig);
        out.push_str(r#","kid":"#);
        push_str(&mut out, &signature.kid);
        let _ = write!(out, r#","verified":{}}}"#, signature.verified);
    }
    out.push_str("}}");
    Arc::from(out)
}

fn sent_frame(n: u64, status: &str, id: Option<&str>, nonce: Option<&str>) -> String {
    let mut out = format!(r#"{{"k":"sent","n":{n},"status":"{status}""#);
    if let Some(id) = id {
        out.push_str(r#","id":"#);
        push_str(&mut out, id);
    }
    if let Some(nonce) = nonce {
        out.push_str(r#","nonce":"#);
        push_str(&mut out, nonce);
    }
    out.push('}');
    out
}

fn revoked_frame(reason: &str, detail: Option<&str>) -> String {
    let mut out = String::from(r#"{"k":"revoked","reason":"#);
    push_str(&mut out, reason);
    if let Some(detail) = detail {
        out.push_str(r#","detail":"#);
        push_str(&mut out, detail);
    }
    out.push('}');
    out
}

/// The endpoint without its query, fragment or user: no secret leaves the owner.
fn bare(url: &str) -> Option<String> {
    let url = url.split(['?', '#']).next().unwrap_or_default();
    let (scheme, rest) = url.split_once("://")?;
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let path = if path.is_empty() { "/" } else { path };
    Some(format!("{scheme}://{host}{path}"))
}

/// `s` as a JSON string.
fn push_str(out: &mut String, s: &str) {
    out.push('"');
    let mut start = 0;
    for (at, byte) in s.bytes().enumerate() {
        let escaped = match byte {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0..=0x1f => {
                out.push_str(&s[start..at]);
                let _ = write!(out, "\\u{byte:04x}");
                start = at + 1;
                continue;
            }
            _ => continue,
        };
        out.push_str(&s[start..at]);
        out.push_str(escaped);
        start = at + 1;
    }
    out.push_str(&s[start..]);
    out.push('"');
}

fn push_opt(out: &mut String, s: Option<&str>) {
    match s {
        Some(s) => push_str(out, s),
        None => out.push_str("null"),
    }
}

fn push_number(out: &mut String, n: Option<u64>) {
    match n {
        Some(n) => {
            let _ = write!(out, "{n}");
        }
        None => out.push_str("null"),
    }
}

fn push_strings<S: AsRef<str>>(out: &mut String, strings: &[S]) {
    out.push('[');
    for (at, s) in strings.iter().enumerate() {
        if at > 0 {
            out.push(',');
        }
        push_str(out, s.as_ref());
    }
    out.push(']');
}

/// The JSON `null`, a send's payload when it has none.
const NULL: &RawValue = RawValue::NULL;

/// A window's frame: one shape for every kind (`k`), read in one pass.
#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow)]
    k: std::borrow::Cow<'a, str>,
    v: Option<u32>,
    on: Option<bool>,
    topics: Option<Vec<String>>,
    types: Option<Vec<String>>,
    s: Option<u64>,
    n: Option<u64>,
    t: Option<String>,
    #[serde(borrow)]
    p: Option<&'a RawValue>,
    o: Option<FrameOptions>,
    c: Option<u64>,
}

/// A send's or a signed send's options, as they cross.
#[derive(Deserialize)]
struct FrameOptions {
    id: Option<String>,
    ttl: Option<f64>,
    live: Option<bool>,
    encrypted: Option<bool>,
    priority: Option<f64>,
    correlation_id: Option<String>,
    nonce: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strings_go_as_json_strings() {
        let mut out = String::new();
        push_str(&mut out, "a\"b\\c\nd\u{1}é");
        assert_eq!(out, r#""a\"b\\c\nd\u0001é""#);
        let parsed: String = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed, "a\"b\\c\nd\u{1}é");
    }

    #[test]
    fn the_endpoint_goes_without_its_query_or_user() {
        assert_eq!(
            bare("wss://ann:secret@example.com:8443/wse?token=x#f").as_deref(),
            Some("wss://example.com:8443/wse")
        );
        assert_eq!(
            bare("ws://localhost:5006").as_deref(),
            Some("ws://localhost:5006/")
        );
        assert_eq!(bare("not a url"), None);
    }

    #[test]
    fn a_frame_of_every_kind_reads_in_one_pass() {
        let frame: Frame<'_> = serde_json::from_str(
            r#"{"k":"send","n":3,"t":"order","p":{"id":1,"x":[1,2]},"o":{"id":"o-1","ttl":500,"live":false},"c":2,"a":0}"#,
        )
        .unwrap();
        assert_eq!(frame.k, "send");
        assert_eq!(frame.p.map(RawValue::get), Some(r#"{"id":1,"x":[1,2]}"#));
        assert_eq!(frame.o.as_ref().and_then(|o| o.id.as_deref()), Some("o-1"));
        assert_eq!(frame.c, Some(2));
        let hello: Frame<'_> =
            serde_json::from_str(r#"{"k":"hello","v":1,"agent":"main"}"#).unwrap();
        assert_eq!((hello.k.as_ref(), hello.v), ("hello", Some(1)));
    }
}
