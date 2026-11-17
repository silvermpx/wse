//! What Dart sees. The types are flat (an item is one struct with a kind)
//! so the Dart side needs no code generation of its own. Every waiting call
//! is async; a call Dart gave up still runs in Rust, so the facade's cancel
//! object and `disconnect` end the waits.

use flutter_rust_bridge::frb;
use wse_client::native;

/// What went wrong; `kind` names it (config, connect, auth, timeout,
/// encryption, signing, gave_up, not_connected, full, closed, encode,
/// decode, cancelled, busy, internal, unknown).
#[derive(Debug)]
pub struct WseError {
    pub kind: String,
    pub message: String,
}

impl From<native::NativeError> for WseError {
    fn from(error: native::NativeError) -> Self {
        let kind = match error.kind {
            native::ErrorKind::Config => "config",
            native::ErrorKind::Connect => "connect",
            native::ErrorKind::Auth => "auth",
            native::ErrorKind::Timeout => "timeout",
            native::ErrorKind::Encryption => "encryption",
            native::ErrorKind::Signing => "signing",
            native::ErrorKind::GaveUp => "gave_up",
            native::ErrorKind::NotConnected => "not_connected",
            native::ErrorKind::Full => "full",
            native::ErrorKind::Closed => "closed",
            native::ErrorKind::Encode => "encode",
            native::ErrorKind::Decode => "decode",
            native::ErrorKind::Cancelled => "cancelled",
            native::ErrorKind::Busy => "busy",
            native::ErrorKind::Internal => "internal",
            native::ErrorKind::Unknown => "unknown",
        };
        Self {
            kind: kind.to_owned(),
            message: error.message,
        }
    }
}

/// Where the client connects and how; durations in milliseconds, zero for the default.
pub struct WseConfig {
    pub url: String,
    pub token: Option<String>,
    pub topics: Vec<String>,
    pub msgpack: bool,
    pub header_names: Vec<String>,
    pub header_values: Vec<String>,
    pub send_queue: u32,
    pub reconnect: bool,
    pub connect_timeout_ms: u64,
    pub recovery_timeout_ms: u64,
    pub server_identity_keys: Vec<String>,
    pub signing_key: Option<Vec<u8>>,
    pub server_signing_keys: Vec<String>,
    /// The certificates a wss:// server's chain must lead to.
    pub trust: WseTrustKind,
    /// The CA certificates (PEM or DER) of [`WseTrustKind::Roots`].
    pub trusted_roots: Vec<Vec<u8>>,
    /// Allows ws://, which sends everything in the clear: for development.
    pub insecure: bool,
    /// recover-or-resync; None: best effort.
    pub delivery: Option<WseDelivery>,
    /// The outbox, where every message waits until the server answers for
    /// it; None: in memory, with its defaults.
    pub offline: Option<WseOffline>,
}

/// The outbox: every message sent, except a live or signed one, waits in it
/// until the server answers for it, goes again after a reconnect, and
/// reaches the server's application once; the delivered and notDelivered
/// items say what became of each.
pub struct WseOffline {
    /// No outbox: a message goes on the connection that is ready, or the
    /// send fails as not connected, as before 3.2.
    pub disabled: bool,
    /// The file they wait in, so that they outlive the app (in its own
    /// storage); the client's alone while it runs. None keeps them in memory.
    pub path: Option<String>,
    /// How many wait at most, those not answered yet included (0: 100,000);
    /// past it `send` waits for room and `trySend` fails as `full`.
    pub max_messages: u32,
    /// How many bytes of messages wait at most (0: 16 MiB), as
    /// `max_messages`.
    pub max_bytes: u64,
    /// How long one waits to be written first, in milliseconds (0: an
    /// hour); one not written by then is dropped (`expired`).
    pub max_age_ms: u64,
}

/// How a message goes (`sendWith`).
pub struct WseSendOptions {
    /// Its id: the app's own, or a random UUID; its outcome names it.
    pub id: Option<String>,
    /// How long it may wait to be written first, in milliseconds (0: the
    /// outbox's `max_age_ms`).
    pub ttl_ms: u64,
    /// Topics that must each be continuous when it goes.
    pub requires: Vec<String>,
}

impl From<WseSendOptions> for native::NativeSendOptions {
    fn from(options: WseSendOptions) -> Self {
        Self {
            id: options.id,
            ttl_ms: options.ttl_ms,
            requires: options.requires,
        }
    }
}

/// The recover-or-resync delivery: a topic that is not continuous (a cold
/// start, every reconnect, a break) holds its messages until the
/// application brought its state up to date (a resync item, then
/// `resynced(round)`); the checkpoint is what it committed it applied, and
/// recovery starts from it.
pub struct WseDelivery {
    /// The one consumer that commits, by a stable name.
    pub consumer: String,
    /// The application's data schema: a checkpoint of another is refused.
    pub schema: Option<String>,
    /// The server's realm (default: the URL): a checkpoint of another is refused.
    pub realm: Option<String>,
    /// The checkpoint the application saved last (`checkpoint()`).
    pub checkpoint: Option<String>,
    /// Messages a topic holds while it resyncs (0: 10,000); more starts the resync over.
    pub max_held: u32,
}

/// Whether the server took a topic.
pub enum WseSubscription {
    Requested,
    Active,
    Denied,
}

/// Whether a topic's messages run without a break.
pub enum WseContinuity {
    Continuous,
    ResyncRequired,
    Resyncing,
}

pub struct WseTopicState {
    pub topic: String,
    pub subscription: WseSubscription,
    pub continuity: WseContinuity,
    /// Why it is not continuous: cold_start, reconnected, offset_gap, ...
    pub reason: Option<String>,
}

/// Where a stream of a topic stands.
pub struct WseStreamPosition {
    pub topic: String,
    pub epoch: u32,
    pub offset: u64,
}

fn continuity_of(c: native::NativeContinuity) -> WseContinuity {
    match c {
        native::NativeContinuity::Continuous => WseContinuity::Continuous,
        native::NativeContinuity::ResyncRequired => WseContinuity::ResyncRequired,
        native::NativeContinuity::Resyncing => WseContinuity::Resyncing,
    }
}

/// Which certificates a wss:// server's chain must lead to.
pub enum WseTrustKind {
    /// The system's own verifier: the roots the device trusts and its
    /// revocation checks.
    Platform,
    /// The Mozilla root certificates built into the library.
    Mozilla,
    /// Only the CA certificates in `trusted_roots`: a private CA.
    Roots,
}

/// `dev.wse.flutter.WseClientPlugin.initNative(context)`: Android's
/// certificate verifier gets the JVM and the application's context; true once
/// it has them.
///
/// # Safety
///
/// Called by the JVM with its JNIEnv, the class and a Context.
#[cfg(target_os = "android")]
#[frb(ignore)]
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_dev_wse_flutter_WseClientPlugin_initNative(
    env: *mut std::ffi::c_void,
    _class: *mut std::ffi::c_void,
    context: *mut std::ffi::c_void,
) -> u8 {
    // Safety: the JVM passes its own env and a valid Context.
    u8::from(unsafe { native::init_android(env, context) }.is_ok())
}

/// A new ECDSA P-256 private key (PKCS#8 DER) for signed messages: keep it
/// in the platform's keystore.
#[frb(sync)]
pub fn generate_signing_key() -> Vec<u8> {
    native::generate_signing_key()
}

/// The public key of a PKCS#8 private key (base64 of the uncompressed
/// point): what the server binds to a connection for its signed messages.
#[frb(sync)]
pub fn signing_public_key(key: Vec<u8>) -> Result<String, WseError> {
    Ok(native::signing_public_key(&key)?)
}

/// Cancels the calls it is given to.
#[frb(opaque)]
pub struct WseCancel(native::CancelToken);

impl WseCancel {
    #[frb(sync)]
    pub fn new() -> WseCancel {
        WseCancel(native::CancelToken::new())
    }

    #[frb(sync)]
    pub fn cancel(&self) {
        self.0.cancel();
    }
}

/// The kinds of [`WseItem`].
pub enum WseItemKind {
    Message,
    Binary,
    Connected,
    Disconnected,
    Paused,
    Subscribed,
    Gap,
    ServerError,
    Closed,
    /// recover-or-resync: bring the state of `topics` up to date (`reasons`
    /// for each, `committed`), then call `resynced(round)`.
    Resync,
    /// A topic's continuity changed (`topic`, `continuity`, `reason`).
    Continuity,
    /// The saved checkpoint is not this client's (`reason`).
    CheckpointRejected,
    /// The server took these messages of the outbox (`ids`).
    Delivered,
    /// These messages of the outbox will not be delivered (`ids`), and why
    /// (`reason`): the server's refusal (`busy`, `rate_limited`, `missing`,
    /// `not_ready`), `expired`, `user_changed`, `cleared` or `closed`.
    NotDelivered,
    /// The outbox's file could not be opened or written (`name`: what failed,
    /// `reason`: why): what is kept stays in memory, and may not outlive the
    /// app.
    StorageFailed,
    Unknown,
}

/// What a subscription replayed of a topic asked for with a position.
pub struct WseTopicRecovery {
    pub topic: String,
    /// Every missed message was replayed.
    pub recovered: bool,
    pub count: u64,
}

/// One thing the client heard; the fields its kind has are set.
pub struct WseItem {
    pub kind: WseItemKind,
    /// Message: its type; ServerError: its code; Unknown: its name.
    pub name: Option<String>,
    pub topic: Option<String>,
    pub epoch: Option<u32>,
    pub offset: Option<u64>,
    pub id: Option<String>,
    /// Message: the payload as JSON text (None without one, absent or null,
    /// or when it could not be decoded); ServerError: its details.
    pub json: Option<String>,
    /// Message: why its payload could not be decoded (msgpack JSON cannot
    /// show).
    pub payload_error: Option<String>,
    /// Message: the user generation it was handed out in (a new one starts
    /// where the events show another user); `commit` takes it back.
    pub generation: u32,
    pub bytes: Option<Vec<u8>>,
    /// Signed messages: the exact signed body, the signature, the key id.
    pub signed_body: Option<String>,
    pub signature: Option<String>,
    pub signature_kid: Option<String>,
    pub verified: bool,
    /// Connected: the connection's id and user, and whether the server keeps
    /// topic histories (missed messages are replayed).
    pub connection_id: Option<String>,
    pub user_id: Option<String>,
    pub reconnected: bool,
    pub recovery: bool,
    /// Disconnected: the close code; Gap and Closed: the reason.
    pub code: Option<u16>,
    pub reason: Option<String>,
    /// Disconnected: when the next attempt is made, and attempts so far.
    pub retry_in_ms: Option<u64>,
    pub attempt: Option<u32>,
    /// Paused: the reasons the client is paused for.
    pub reasons: Vec<String>,
    /// Delivered and NotDelivered: the messages' ids.
    pub ids: Vec<String>,
    /// Subscribed: the topics taken and refused, and per topic asked for with
    /// a position what was replayed.
    pub topics: Vec<String>,
    pub failed: Vec<String>,
    pub recoveries: Vec<WseTopicRecovery>,
    /// Gap: the first and last offsets missing, when known.
    pub first: Option<u64>,
    pub last: Option<u64>,
    /// Resync: the round to say `resynced` for, and what the consumer
    /// committed of the topics (their reasons in `reasons`).
    pub round: Option<u64>,
    pub committed: Vec<WseStreamPosition>,
    /// Continuity: the topic's (in `topic`), why not continuous in `reason`.
    pub continuity: Option<WseContinuity>,
}

impl WseItem {
    fn of(kind: WseItemKind) -> Self {
        WseItem {
            kind,
            name: None,
            topic: None,
            epoch: None,
            offset: None,
            id: None,
            json: None,
            payload_error: None,
            generation: 0,
            bytes: None,
            signed_body: None,
            signature: None,
            signature_kid: None,
            verified: false,
            connection_id: None,
            user_id: None,
            reconnected: false,
            recovery: false,
            code: None,
            reason: None,
            retry_in_ms: None,
            attempt: None,
            reasons: Vec::new(),
            ids: Vec::new(),
            topics: Vec::new(),
            failed: Vec::new(),
            recoveries: Vec::new(),
            first: None,
            last: None,
            round: None,
            committed: Vec::new(),
            continuity: None,
        }
    }
}

impl From<native::Item> for WseItem {
    fn from(item: native::Item) -> Self {
        match item {
            native::Item::Message(m) => {
                let signature = m.signature;
                WseItem {
                    name: Some(m.kind),
                    topic: m.topic,
                    epoch: m.epoch,
                    offset: m.offset,
                    id: m.id,
                    json: m.json,
                    payload_error: m.payload_error,
                    generation: m.generation,
                    verified: signature.as_ref().is_some_and(|s| s.verified),
                    signed_body: signature.as_ref().map(|s| s.body.clone()),
                    signature: signature.as_ref().map(|s| s.sig.clone()),
                    signature_kid: signature.map(|s| s.kid),
                    ..WseItem::of(WseItemKind::Message)
                }
            }
            native::Item::Binary(bytes) => WseItem {
                bytes: Some(bytes),
                ..WseItem::of(WseItemKind::Binary)
            },
            native::Item::Connected {
                connection_id,
                user_id,
                reconnected,
                recovery,
            } => WseItem {
                connection_id: Some(connection_id),
                user_id,
                reconnected,
                recovery,
                ..WseItem::of(WseItemKind::Connected)
            },
            native::Item::Disconnected {
                code,
                reason,
                retry_in_ms,
                attempt,
            } => WseItem {
                code,
                reason: Some(reason),
                retry_in_ms: Some(retry_in_ms),
                attempt: Some(attempt),
                ..WseItem::of(WseItemKind::Disconnected)
            },
            native::Item::Paused { reasons } => WseItem {
                reasons,
                ..WseItem::of(WseItemKind::Paused)
            },
            native::Item::Subscribed {
                topics,
                failed,
                recovery,
            } => WseItem {
                topics,
                failed,
                recoveries: recovery
                    .into_iter()
                    .map(|r| WseTopicRecovery {
                        topic: r.topic,
                        recovered: r.recovered,
                        count: r.count,
                    })
                    .collect(),
                ..WseItem::of(WseItemKind::Subscribed)
            },
            native::Item::Gap {
                topic,
                epoch,
                first,
                last,
                reason,
            } => WseItem {
                topic: Some(topic),
                epoch: Some(epoch),
                first,
                last,
                reason: Some(reason),
                ..WseItem::of(WseItemKind::Gap)
            },
            native::Item::ServerError {
                code,
                message,
                details_json,
            } => WseItem {
                name: Some(code),
                reason: Some(message),
                json: Some(details_json),
                ..WseItem::of(WseItemKind::ServerError)
            },
            native::Item::Closed { error } => WseItem {
                reason: error.map(|e| e.message),
                ..WseItem::of(WseItemKind::Closed)
            },
            native::Item::Resync {
                round,
                topics,
                reasons,
                committed,
            } => WseItem {
                round: Some(round),
                topics,
                reasons,
                committed: committed
                    .into_iter()
                    .map(|p| WseStreamPosition {
                        topic: p.topic,
                        epoch: p.epoch,
                        offset: p.offset,
                    })
                    .collect(),
                ..WseItem::of(WseItemKind::Resync)
            },
            native::Item::Continuity {
                topic,
                continuity,
                reason,
            } => WseItem {
                topic: Some(topic),
                continuity: Some(continuity_of(continuity)),
                reason,
                ..WseItem::of(WseItemKind::Continuity)
            },
            native::Item::CheckpointRejected { reason } => WseItem {
                reason: Some(reason),
                ..WseItem::of(WseItemKind::CheckpointRejected)
            },
            native::Item::Delivered { ids } => WseItem {
                ids,
                ..WseItem::of(WseItemKind::Delivered)
            },
            native::Item::NotDelivered { reason, ids } => WseItem {
                reason: Some(reason),
                ids,
                ..WseItem::of(WseItemKind::NotDelivered)
            },
            native::Item::StorageFailed { operation, message } => WseItem {
                name: Some(operation),
                reason: Some(message),
                ..WseItem::of(WseItemKind::StorageFailed)
            },
            native::Item::Unknown { name } => WseItem {
                name: Some(name),
                ..WseItem::of(WseItemKind::Unknown)
            },
        }
    }
}

/// A batch of items; `end` once nothing more will come.
pub struct WseBatch {
    pub id: u64,
    pub end: bool,
    pub items: Vec<WseItem>,
}

/// The native client's settings for `config`.
fn native_config(config: WseConfig) -> native::NativeConfig {
    let headers = config
        .header_names
        .into_iter()
        .zip(config.header_values)
        .collect();
    native::NativeConfig {
        url: config.url,
        token: config.token,
        topics: config.topics,
        msgpack: config.msgpack,
        headers,
        send_queue: config.send_queue,
        reconnect: Some(config.reconnect),
        connect_timeout_ms: config.connect_timeout_ms,
        recovery_timeout_ms: config.recovery_timeout_ms,
        server_identity_keys: config.server_identity_keys,
        signing_key: config.signing_key,
        server_signing_keys: config.server_signing_keys,
        trust: match config.trust {
            WseTrustKind::Platform => native::NativeTrust::Platform,
            WseTrustKind::Mozilla => native::NativeTrust::Mozilla,
            WseTrustKind::Roots => native::NativeTrust::Roots(config.trusted_roots),
        },
        insecure: config.insecure,
        delivery: config.delivery.map(|d| native::NativeDelivery {
            consumer: d.consumer,
            schema: d.schema,
            realm: d.realm,
            checkpoint: d.checkpoint,
            max_held: d.max_held,
        }),
        offline: config.offline.map(|o| native::NativeOffline {
            disabled: o.disabled,
            path: o.path,
            max_messages: o.max_messages,
            max_bytes: o.max_bytes,
            max_age_ms: o.max_age_ms,
        }),
        ..native::NativeConfig::default()
    }
}

#[frb(opaque)]
pub struct WseNativeClient(native::NativeClient);

impl WseNativeClient {
    /// Connects, and returns once the server took the connection.
    pub async fn connect(
        config: WseConfig,
        cancel: &WseCancel,
    ) -> Result<WseNativeClient, WseError> {
        let client = native::NativeClient::connect(native_config(config), None, &cancel.0).await?;
        Ok(WseNativeClient(client))
    }

    /// Starts, and returns at once: the connection is made in the
    /// background, a failed first attempt made again as any later one. What
    /// is sent meanwhile waits in the outbox: for an app that may start
    /// without a network.
    pub async fn start(config: WseConfig) -> Result<WseNativeClient, WseError> {
        let client = native::NativeClient::start(native_config(config), None).await?;
        Ok(WseNativeClient(client))
    }

    #[frb(sync)]
    pub fn try_send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send(&kind, &json)?)
    }

    pub async fn send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.send(kind, json, &native::CancelToken::new()).await?)
    }

    /// Sends `{"t": kind, "p": json}` with `options`: its own id, how long it
    /// may wait, the topics it requires. Its id, which its outcome names.
    /// Fails at once when the outbox or the queue is full.
    #[frb(sync)]
    pub fn try_send_with(
        &self,
        kind: String,
        json: String,
        options: WseSendOptions,
    ) -> Result<String, WseError> {
        Ok(self.0.try_send_with(&kind, &json, options.into())?)
    }

    /// `try_send_with`, waiting for room.
    pub async fn send_with(
        &self,
        kind: String,
        json: String,
        options: WseSendOptions,
    ) -> Result<String, WseError> {
        Ok(self
            .0
            .send_with(kind, json, options.into(), &native::CancelToken::new())
            .await?)
    }

    /// The user signs out: every message in the outbox, and every one sent
    /// before this call, goes (`notDelivered`, `cleared`), and the user is
    /// forgotten.
    #[frb(sync)]
    pub fn clear(&self) -> Result<(), WseError> {
        Ok(self.0.clear()?)
    }

    /// Sends `{"t": kind, "p": json}` once each of `topics` is continuous on the
    /// connection; until then it waits, in order with the others that wait.
    /// Fails at once when the queue is full.
    #[frb(sync)]
    pub fn try_send_requiring(
        &self,
        kind: String,
        json: String,
        topics: Vec<String>,
    ) -> Result<(), WseError> {
        Ok(self.0.try_send_requiring(&kind, &json, topics)?)
    }

    /// `try_send_requiring`, waiting for room in the queue.
    pub async fn send_requiring(
        &self,
        kind: String,
        json: String,
        topics: Vec<String>,
    ) -> Result<(), WseError> {
        Ok(self
            .0
            .send_requiring(kind, json, topics, &native::CancelToken::new())
            .await?)
    }

    #[frb(sync)]
    pub fn try_send_live(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send_live(&kind, &json)?)
    }

    pub async fn send_live(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self
            .0
            .send_live(kind, json, &native::CancelToken::new())
            .await?)
    }

    pub async fn send_signed(
        &self,
        kind: String,
        json: String,
        nonce: Option<String>,
    ) -> Result<String, WseError> {
        Ok(self
            .0
            .send_signed(kind, json, nonce, &native::CancelToken::new())
            .await?)
    }

    #[frb(sync)]
    pub fn subscribe(&self, topics: Vec<String>) -> Result<(), WseError> {
        Ok(self.0.subscribe(topics)?)
    }

    #[frb(sync)]
    pub fn unsubscribe(&self, topics: Vec<String>) -> Result<(), WseError> {
        Ok(self.0.unsubscribe(topics)?)
    }

    #[frb(sync)]
    pub fn reconnect(&self) -> Result<(), WseError> {
        Ok(self.0.reconnect()?)
    }

    #[frb(sync)]
    pub fn pause(&self, reason: String) -> Result<(), WseError> {
        Ok(self.0.pause(reason)?)
    }

    #[frb(sync)]
    pub fn resume(&self, reason: String) -> Result<(), WseError> {
        Ok(self.0.resume(reason)?)
    }

    /// Each topic held: whether the server took it, and whether its
    /// messages run without a break.
    #[frb(sync)]
    pub fn topic_states(&self) -> Vec<WseTopicState> {
        self.0
            .topic_states()
            .into_iter()
            .map(|s| WseTopicState {
                topic: s.topic,
                subscription: match s.subscription {
                    native::NativeSubscription::Requested => WseSubscription::Requested,
                    native::NativeSubscription::Active => WseSubscription::Active,
                    native::NativeSubscription::Denied => WseSubscription::Denied,
                },
                continuity: continuity_of(s.continuity),
                reason: s.reason,
            })
            .collect()
    }

    /// The consumer applied the message at `epoch`, `offset` of `topic`,
    /// handed out in `generation`: one of an earlier user counts for nobody.
    #[frb(sync)]
    pub fn commit(&self, topic: String, epoch: u32, offset: u64, generation: u32) {
        self.0.commit(&topic, epoch, offset, generation);
    }

    /// What the consumer committed, to save with the application's own
    /// state and give back as `WseDelivery.checkpoint`.
    #[frb(sync)]
    pub fn checkpoint(&self) -> String {
        self.0.checkpoint()
    }

    #[frb(sync)]
    /// How many messages wait in the outbox for the server's answer.
    pub fn kept(&self) -> u32 {
        self.0.kept()
    }

    /// recover-or-resync: the application brought the topics of `round` up
    /// to date; what they held comes next.
    #[frb(sync)]
    pub fn resynced(&self, round: u64) {
        self.0.resynced(round);
    }

    /// Bring `topics` up to date: in recover-or-resync a resync round asks
    /// for it; in best effort the application did so itself.
    #[frb(sync)]
    pub fn resync(&self, topics: Vec<String>) {
        self.0.resync(topics);
    }

    /// connecting, connected, reconnecting, paused or closed.
    #[frb(sync)]
    pub fn status(&self) -> String {
        match self.0.status() {
            native::NativeStatus::Connecting => "connecting",
            native::NativeStatus::Connected => "connected",
            native::NativeStatus::Reconnecting => "reconnecting",
            native::NativeStatus::Paused => "paused",
            native::NativeStatus::Closed => "closed",
        }
        .to_owned()
    }

    #[frb(sync)]
    pub fn user_id(&self) -> Option<String> {
        self.0.user_id()
    }

    /// The next batch: `ack` acknowledges the last one (0: none yet); one not
    /// acknowledged comes again.
    pub async fn next_batch(
        &self,
        ack: u64,
        max_items: u32,
        max_bytes: u32,
        cancel: &WseCancel,
    ) -> Result<WseBatch, WseError> {
        let batch = self
            .0
            .next_batch(ack, max_items, max_bytes, &cancel.0)
            .await?;
        Ok(WseBatch {
            id: batch.id,
            end: batch.kind == native::BatchKind::End,
            items: batch.items.into_iter().map(WseItem::from).collect(),
        })
    }

    /// Closes the client: what was sent goes out first, within `deadline_ms`;
    /// a waiting next_batch gets the end.
    pub async fn disconnect(&self, deadline_ms: u64) {
        self.0.disconnect(deadline_ms).await;
    }
}
