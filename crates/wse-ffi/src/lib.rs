//! The WSE client for Swift and Kotlin: the Rust client's native module through UniFFI.
//!
//! Every call that waits is async and returns a `Result`; a panic is an
//! error, never an unwind into Swift or Kotlin. Swift does not cancel Rust
//! futures by itself: the Swift package wraps each call in a task
//! cancellation handler that cancels its [`WseCancel`]. Kotlin's
//! cancellation drops the Rust future, which [`WseClient::next_batch`] is
//! built to survive (the batch comes again until it is acknowledged).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use wse_client::native;

uniffi::setup_scaffolding!();

// -- errors ---------------------------------------------------------------------

/// What went wrong. A kind added later reads as `Unknown`.
#[derive(Debug, uniffi::Error)]
pub enum WseError {
    Config { reason: String },
    Connect { reason: String },
    Auth { reason: String },
    Timeout { reason: String },
    Encryption { reason: String },
    Signing { reason: String },
    GaveUp { reason: String },
    NotConnected,
    Full,
    Closed,
    Encode { reason: String },
    Decode { reason: String },
    Cancelled,
    Busy,
    Internal { reason: String },
    Unknown { reason: String },
}

impl std::fmt::Display for WseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConnected => f.write_str("not connected"),
            Self::Full => f.write_str("the send queue is full"),
            Self::Closed => f.write_str("the client was closed"),
            Self::Cancelled => f.write_str("cancelled"),
            Self::Busy => f.write_str("another call reads the events"),
            Self::Config { reason }
            | Self::Connect { reason }
            | Self::Auth { reason }
            | Self::Timeout { reason }
            | Self::Encryption { reason }
            | Self::Signing { reason }
            | Self::GaveUp { reason }
            | Self::Encode { reason }
            | Self::Decode { reason }
            | Self::Internal { reason }
            | Self::Unknown { reason } => f.write_str(reason),
        }
    }
}

impl std::error::Error for WseError {}

impl From<native::NativeError> for WseError {
    fn from(error: native::NativeError) -> Self {
        let reason = error.message;
        match error.kind {
            native::ErrorKind::Config => Self::Config { reason },
            native::ErrorKind::Connect => Self::Connect { reason },
            native::ErrorKind::Auth => Self::Auth { reason },
            native::ErrorKind::Timeout => Self::Timeout { reason },
            native::ErrorKind::Encryption => Self::Encryption { reason },
            native::ErrorKind::Signing => Self::Signing { reason },
            native::ErrorKind::GaveUp => Self::GaveUp { reason },
            native::ErrorKind::NotConnected => Self::NotConnected,
            native::ErrorKind::Full => Self::Full,
            native::ErrorKind::Closed => Self::Closed,
            native::ErrorKind::Encode => Self::Encode { reason },
            native::ErrorKind::Decode => Self::Decode { reason },
            native::ErrorKind::Cancelled => Self::Cancelled,
            native::ErrorKind::Busy => Self::Busy,
            native::ErrorKind::Internal => Self::Internal { reason },
            native::ErrorKind::Unknown => Self::Unknown { reason },
        }
    }
}

// -- configuration ----------------------------------------------------------------

#[derive(uniffi::Record)]
pub struct WseHeader {
    pub name: String,
    pub value: String,
}

/// Where the client connects and how; durations in milliseconds, zero for the default.
#[derive(uniffi::Record)]
pub struct WseConfig {
    /// ws://host:port/wse or wss://...
    pub url: String,
    #[uniffi(default = None)]
    pub token: Option<String>,
    #[uniffi(default = [])]
    pub topics: Vec<String>,
    #[uniffi(default = false)]
    pub msgpack: bool,
    #[uniffi(default = [])]
    pub headers: Vec<WseHeader>,
    #[uniffi(default = 0)]
    pub send_queue: u32,
    #[uniffi(default = true)]
    pub reconnect: bool,
    #[uniffi(default = 0)]
    pub connect_timeout_ms: u64,
    #[uniffi(default = 0)]
    pub ready_timeout_ms: u64,
    #[uniffi(default = 0)]
    pub idle_timeout_ms: u64,
    #[uniffi(default = 0)]
    pub recovery_timeout_ms: u64,
    /// End-to-end encryption pinned to these server identity keys (base64).
    #[uniffi(default = [])]
    pub server_identity_keys: Vec<String>,
    /// This client's ECDSA P-256 private key (PKCS#8, PEM or DER), for signed messages.
    #[uniffi(default = None)]
    pub signing_key: Option<Vec<u8>>,
    #[uniffi(default = [])]
    pub server_signing_keys: Vec<String>,
    /// The certificates a wss:// server's chain must lead to; none: the
    /// system's verifier.
    #[uniffi(default = None)]
    pub trust: Option<WseTrust>,
    /// Allows ws://, which sends everything in the clear: for development.
    #[uniffi(default = false)]
    pub insecure: bool,
    /// recover-or-resync; none: best effort.
    #[uniffi(default = None)]
    pub delivery: Option<WseDelivery>,
    /// The outbox, where every message waits until the server answers for it; none: in
    /// memory, with its defaults.
    #[uniffi(default = None)]
    pub offline: Option<WseOffline>,
}

/// The outbox: every message sent, except a live or signed one, waits in it until the
/// server answers for it, goes again after a reconnect, and reaches the server's
/// application once; `Delivered` and `NotDelivered` say what became of each.
#[derive(uniffi::Record)]
pub struct WseOffline {
    /// No outbox: a message goes on the connection that is ready, or the send fails as
    /// not connected, as before 3.2.
    #[uniffi(default = false)]
    pub disabled: bool,
    /// The file they wait in, so that they outlive the app (in its own storage); the
    /// client's alone while it runs. None keeps them in memory.
    #[uniffi(default = None)]
    pub path: Option<String>,
    /// How many wait at most, those not answered yet included (0: 100,000); past it
    /// `send` waits for room and `try_send` fails as `Full`.
    #[uniffi(default = 0)]
    pub max_messages: u32,
    /// How many bytes of messages wait at most (0: 16 MiB), as `max_messages`.
    #[uniffi(default = 0)]
    pub max_bytes: u64,
    /// How long one waits to be written first, in milliseconds (0: an hour); one not
    /// written by then is dropped (`NotDelivered`, `expired`).
    #[uniffi(default = 0)]
    pub max_age_ms: u64,
}

/// How a message goes (`send_with`).
#[derive(uniffi::Record)]
pub struct WseSendOptions {
    /// Its id: the app's own (one it keeps itself, say), or a random UUID; its outcome
    /// names it.
    #[uniffi(default = None)]
    pub id: Option<String>,
    /// How long it may wait to be written first, in milliseconds (0: the outbox's
    /// `max_age_ms`).
    #[uniffi(default = 0)]
    pub ttl_ms: u64,
    /// Topics that must each be continuous when it goes.
    #[uniffi(default = [])]
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
/// application brought its state up to date (a `Resync` item, then
/// `resynced(round)`); the checkpoint is what it committed it applied, and
/// recovery starts from it.
#[derive(uniffi::Record)]
pub struct WseDelivery {
    /// The one consumer that commits, by a stable name.
    pub consumer: String,
    /// The application's data schema: a checkpoint of another is refused.
    #[uniffi(default = None)]
    pub schema: Option<String>,
    /// The server's realm (default: the URL): a checkpoint of another is refused.
    #[uniffi(default = None)]
    pub realm: Option<String>,
    /// The checkpoint the application saved last (`checkpoint()`).
    #[uniffi(default = None)]
    pub checkpoint: Option<String>,
    /// Messages a topic holds while it resyncs (0: 10,000); more starts the resync over.
    #[uniffi(default = 0)]
    pub max_held: u32,
}

/// Whether the server took a topic.
#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WseSubscription {
    Requested,
    Active,
    Denied,
}

/// Whether a topic's messages run without a break.
#[derive(uniffi::Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum WseContinuity {
    Continuous,
    ResyncRequired,
    Resyncing,
}

#[derive(uniffi::Record)]
pub struct WseTopicState {
    pub topic: String,
    pub subscription: WseSubscription,
    pub continuity: WseContinuity,
    /// Why it is not continuous: cold_start, reconnected, requested, overflow, or the reason
    /// of the gap (timeout, not_recovered, epoch_changed, ...).
    pub reason: Option<String>,
}

/// Where a stream of a topic stands.
#[derive(uniffi::Record)]
pub struct WseStreamPosition {
    pub topic: String,
    pub epoch: u32,
    pub offset: u64,
}

fn subscription_of(s: native::NativeSubscription) -> WseSubscription {
    match s {
        native::NativeSubscription::Requested => WseSubscription::Requested,
        native::NativeSubscription::Active => WseSubscription::Active,
        native::NativeSubscription::Denied => WseSubscription::Denied,
    }
}

fn continuity_of(c: native::NativeContinuity) -> WseContinuity {
    match c {
        native::NativeContinuity::Continuous => WseContinuity::Continuous,
        native::NativeContinuity::ResyncRequired => WseContinuity::ResyncRequired,
        native::NativeContinuity::Resyncing => WseContinuity::Resyncing,
    }
}

/// Which certificates a wss:// server's chain must lead to.
#[derive(Debug, Clone, uniffi::Enum)]
pub enum WseTrust {
    /// The system's own verifier: the roots the device trusts and its
    /// revocation checks. On Android the library sets it up as the app
    /// starts (`WseTls.init`).
    Platform,
    /// The Mozilla root certificates built into the library: the same on
    /// every device.
    Mozilla,
    /// Only these CA certificates, PEM or DER: a private CA.
    Roots { certificates: Vec<Vec<u8>> },
}

impl From<WseTrust> for native::NativeTrust {
    fn from(trust: WseTrust) -> Self {
        match trust {
            WseTrust::Platform => Self::Platform,
            WseTrust::Mozilla => Self::Mozilla,
            WseTrust::Roots { certificates } => Self::Roots(certificates),
        }
    }
}

/// `dev.wse.ffi.WseTls.initNative(context)`: Android's certificate verifier
/// gets the JVM and the application's context; true once it has them.
///
/// # Safety
///
/// Called by the JVM with its JNIEnv, the class and a Context.
#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
pub unsafe extern "system" fn Java_dev_wse_ffi_WseTls_initNative(
    env: *mut std::ffi::c_void,
    _class: *mut std::ffi::c_void,
    context: *mut std::ffi::c_void,
) -> u8 {
    // Safety: the JVM passes its own env and a valid Context.
    u8::from(unsafe { native::init_android(env, context) }.is_ok())
}

/// A new ECDSA P-256 private key (PKCS#8 DER) for signed messages: keep it
/// in the platform's keystore.
#[uniffi::export]
pub fn generate_signing_key() -> Vec<u8> {
    native::generate_signing_key()
}

/// The public key of a PKCS#8 private key (base64 of the uncompressed
/// point): what the server binds to a connection for its signed messages.
#[uniffi::export]
pub fn signing_public_key(key: Vec<u8>) -> Result<String, WseError> {
    Ok(native::signing_public_key(&key)?)
}

impl From<WseConfig> for native::NativeConfig {
    fn from(c: WseConfig) -> Self {
        native::NativeConfig {
            url: c.url,
            token: c.token,
            topics: c.topics,
            msgpack: c.msgpack,
            headers: c.headers.into_iter().map(|h| (h.name, h.value)).collect(),
            send_queue: c.send_queue,
            reconnect: Some(c.reconnect),
            connect_timeout_ms: c.connect_timeout_ms,
            ready_timeout_ms: c.ready_timeout_ms,
            idle_timeout_ms: c.idle_timeout_ms,
            recovery_timeout_ms: c.recovery_timeout_ms,
            server_identity_keys: c.server_identity_keys,
            signing_key: c.signing_key,
            server_signing_keys: c.server_signing_keys,
            trust: c.trust.map(Into::into).unwrap_or_default(),
            insecure: c.insecure,
            delivery: c.delivery.map(|d| native::NativeDelivery {
                consumer: d.consumer,
                schema: d.schema,
                realm: d.realm,
                checkpoint: d.checkpoint,
                max_held: d.max_held,
            }),
            offline: c.offline.map(|o| native::NativeOffline {
                disabled: o.disabled,
                path: o.path,
                max_messages: o.max_messages,
                max_bytes: o.max_bytes,
                max_age_ms: o.max_age_ms,
            }),
        }
    }
}

// -- the token, from the application ---------------------------------------------------

#[derive(Debug, uniffi::Error)]
pub enum WseTokenError {
    Failed { reason: String },
}

impl std::fmt::Display for WseTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self::Failed { reason } = self;
        f.write_str(reason)
    }
}

impl std::error::Error for WseTokenError {}

impl From<uniffi::UnexpectedUniFFICallbackError> for WseTokenError {
    fn from(error: uniffi::UnexpectedUniFFICallbackError) -> Self {
        Self::Failed {
            reason: error.reason,
        }
    }
}

/// Asked for the token before each connection attempt.
#[uniffi::export(foreign)]
#[async_trait::async_trait]
pub trait WseTokenSource: Send + Sync {
    async fn token(&self) -> Result<String, WseTokenError>;
}

struct Tokens(Arc<dyn WseTokenSource>);

impl native::TokenSource for Tokens {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>> {
        let source = Arc::clone(&self.0);
        Box::pin(async move { source.token().await.map_err(|e| e.to_string()) })
    }
}

// -- cancellation -------------------------------------------------------------------

/// Cancels the calls it is given to.
#[derive(uniffi::Object, Default)]
pub struct WseCancel(native::CancelToken);

#[uniffi::export]
impl WseCancel {
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn cancel(&self) {
        self.0.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

// -- what the client hears -------------------------------------------------------------

#[derive(uniffi::Record)]
pub struct WseSignature {
    /// The exact text that was signed: keep it as it is.
    pub body: String,
    pub sig: String,
    pub kid: String,
    pub verified: bool,
}

#[derive(uniffi::Record)]
pub struct WseMessage {
    pub kind: String,
    pub topic: Option<String>,
    pub epoch: Option<u32>,
    pub offset: Option<u64>,
    pub id: Option<String>,
    /// The payload as JSON text (from msgpack too); None without one
    /// (absent or null) or when it could not be decoded.
    pub json: Option<String>,
    /// Why the payload could not be decoded (msgpack JSON cannot show).
    pub payload_error: Option<String>,
    pub signature: Option<WseSignature>,
    /// The user generation it was handed out in: a new one starts where
    /// the events show another user. `commit` takes it back.
    pub generation: u32,
}

#[derive(uniffi::Record)]
pub struct WseTopicRecovery {
    pub topic: String,
    pub recovered: bool,
    pub count: u64,
}

/// One thing the client heard, in order.
#[derive(uniffi::Enum)]
pub enum WseItem {
    Message {
        message: WseMessage,
    },
    Binary {
        bytes: Vec<u8>,
    },
    Connected {
        connection_id: String,
        user_id: Option<String>,
        reconnected: bool,
        recovery: bool,
    },
    Disconnected {
        code: Option<u16>,
        reason: String,
        retry_in_ms: u64,
        attempt: u32,
    },
    /// Paused by the application: no connection until the last reason is
    /// cleared; topics and positions stay.
    Paused {
        reasons: Vec<String>,
    },
    Subscribed {
        topics: Vec<String>,
        failed: Vec<String>,
        recovery: Vec<WseTopicRecovery>,
    },
    /// Messages of a topic that will not come: reload what depends on them.
    Gap {
        topic: String,
        epoch: u32,
        first: Option<u64>,
        last: Option<u64>,
        reason: String,
    },
    ServerError {
        code: String,
        text: String,
        details_json: String,
    },
    /// The client stopped for good; `reason` when it was not closed by the application.
    Closed {
        reason: Option<String>,
    },
    /// recover-or-resync: bring the state of `topics` up to date (the
    /// reason of each, what the consumer committed), then call
    /// `resynced(round)`; their messages wait till then.
    Resync {
        round: u64,
        topics: Vec<String>,
        reasons: Vec<String>,
        committed: Vec<WseStreamPosition>,
    },
    /// A topic's continuity changed, and why it is not continuous.
    Continuity {
        topic: String,
        continuity: WseContinuity,
        reason: Option<String>,
    },
    /// The saved checkpoint is not this client's: its realm, user,
    /// consumer, schema or version, or it is malformed.
    CheckpointRejected {
        reason: String,
    },
    /// The server took these messages of the outbox (their ids).
    Delivered {
        ids: Vec<String>,
    },
    /// These messages of the outbox will not be delivered, and why: the server's refusal
    /// (`busy`, `rate_limited`, `missing`, `not_ready`), `expired`, `user_changed`,
    /// `cleared` or `closed`.
    NotDelivered {
        reason: String,
        ids: Vec<String>,
    },
    /// The outbox's file could not be opened or written (`operation`): what is kept stays in
    /// memory, and may not outlive the app.
    StorageFailed {
        operation: String,
        message: String,
    },
    /// Something this version does not know.
    Unknown {
        name: String,
    },
}

impl From<native::Item> for WseItem {
    fn from(item: native::Item) -> Self {
        match item {
            native::Item::Message(m) => Self::Message {
                message: WseMessage {
                    kind: m.kind,
                    topic: m.topic,
                    epoch: m.epoch,
                    offset: m.offset,
                    id: m.id,
                    json: m.json,
                    payload_error: m.payload_error,
                    signature: m.signature.map(|s| WseSignature {
                        body: s.body,
                        sig: s.sig,
                        kid: s.kid,
                        verified: s.verified,
                    }),
                    generation: m.generation,
                },
            },
            native::Item::Binary(bytes) => Self::Binary { bytes },
            native::Item::Connected {
                connection_id,
                user_id,
                reconnected,
                recovery,
            } => Self::Connected {
                connection_id,
                user_id,
                reconnected,
                recovery,
            },
            native::Item::Disconnected {
                code,
                reason,
                retry_in_ms,
                attempt,
            } => Self::Disconnected {
                code,
                reason,
                retry_in_ms,
                attempt,
            },
            native::Item::Paused { reasons } => Self::Paused { reasons },
            native::Item::Subscribed {
                topics,
                failed,
                recovery,
            } => Self::Subscribed {
                topics,
                failed,
                recovery: recovery
                    .into_iter()
                    .map(|r| WseTopicRecovery {
                        topic: r.topic,
                        recovered: r.recovered,
                        count: r.count,
                    })
                    .collect(),
            },
            native::Item::Gap {
                topic,
                epoch,
                first,
                last,
                reason,
            } => Self::Gap {
                topic,
                epoch,
                first,
                last,
                reason,
            },
            native::Item::ServerError {
                code,
                message,
                details_json,
            } => Self::ServerError {
                code,
                text: message,
                details_json,
            },
            native::Item::Closed { error } => Self::Closed {
                reason: error.map(|e| e.message),
            },
            native::Item::Resync {
                round,
                topics,
                reasons,
                committed,
            } => Self::Resync {
                round,
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
            },
            native::Item::Continuity {
                topic,
                continuity,
                reason,
            } => Self::Continuity {
                topic,
                continuity: continuity_of(continuity),
                reason,
            },
            native::Item::CheckpointRejected { reason } => Self::CheckpointRejected { reason },
            native::Item::Delivered { ids } => Self::Delivered { ids },
            native::Item::NotDelivered { reason, ids } => Self::NotDelivered { reason, ids },
            native::Item::StorageFailed { operation, message } => {
                Self::StorageFailed { operation, message }
            }
            native::Item::Unknown { name } => Self::Unknown { name },
        }
    }
}

#[derive(uniffi::Enum)]
pub enum WseBatchKind {
    Items,
    /// Nothing more will come: the client closed.
    End,
}

#[derive(uniffi::Record)]
pub struct WseBatch {
    /// Acknowledge it with the next call; until then it comes again.
    pub id: u64,
    pub kind: WseBatchKind,
    pub items: Vec<WseItem>,
}

#[derive(uniffi::Enum)]
pub enum WseStatus {
    Connecting,
    Connected,
    Reconnecting,
    /// Paused by the application: no connection until it resumes.
    Paused,
    Closed,
}

// -- the client ---------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct WseClient(native::NativeClient);

/// Connects, and returns once the server took the connection; gives up
/// when `cancel` is cancelled.
#[uniffi::export]
pub async fn connect(
    config: WseConfig,
    tokens: Option<Arc<dyn WseTokenSource>>,
    cancel: Arc<WseCancel>,
) -> Result<Arc<WseClient>, WseError> {
    let tokens = tokens.map(|source| Arc::new(Tokens(source)) as Arc<dyn native::TokenSource>);
    let client = native::NativeClient::connect(config.into(), tokens, &cancel.0).await?;
    Ok(Arc::new(WseClient(client)))
}

/// Starts, and returns at once: the connection is made in the background, a
/// failed first attempt made again as any later one. What is sent meanwhile
/// waits in the outbox: for an app that may start without a network.
#[uniffi::export]
pub async fn start(
    config: WseConfig,
    tokens: Option<Arc<dyn WseTokenSource>>,
) -> Result<Arc<WseClient>, WseError> {
    let tokens = tokens.map(|source| Arc::new(Tokens(source)) as Arc<dyn native::TokenSource>);
    let client = native::NativeClient::start(config.into(), tokens).await?;
    Ok(Arc::new(WseClient(client)))
}

#[uniffi::export]
impl WseClient {
    /// Sends `{"t": kind, "p": json}`; fails at once when the outbox or the queue is
    /// full.
    pub fn try_send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send(&kind, &json)?)
    }

    /// Sends `{"t": kind, "p": json}` with `options`: its own id, how long it may wait,
    /// the topics it requires. Its id, which its outcome names. Fails at once when the
    /// outbox or the queue is full.
    pub fn try_send_with(
        &self,
        kind: String,
        json: String,
        options: WseSendOptions,
    ) -> Result<String, WseError> {
        Ok(self.0.try_send_with(&kind, &json, options.into())?)
    }

    /// `try_send_with`, waiting for room; `cancel` gives up the wait (a message already
    /// queued still goes).
    pub async fn send_with(
        &self,
        kind: String,
        json: String,
        options: WseSendOptions,
        cancel: Arc<WseCancel>,
    ) -> Result<String, WseError> {
        Ok(self
            .0
            .send_with(kind, json, options.into(), &cancel.0)
            .await?)
    }

    /// The user signs out: every message in the outbox, and every one sent before this
    /// call, goes (`NotDelivered`, `cleared`), and the user is forgotten.
    pub fn clear(&self) -> Result<(), WseError> {
        Ok(self.0.clear()?)
    }

    /// Sends `{"t": kind, "p": json}`, waiting for room in the queue;
    /// `cancel` gives up the wait (a message already queued still goes).
    pub async fn send(
        &self,
        kind: String,
        json: String,
        cancel: Arc<WseCancel>,
    ) -> Result<(), WseError> {
        Ok(self.0.send(kind, json, &cancel.0).await?)
    }

    /// Sends `{"t": kind, "p": json}` once each of `topics` is continuous on the connection
    /// (see `topic_states`); until then it waits, in order with the others that wait, while
    /// sends that require nothing go past it. It belongs to the user who sent it and goes
    /// nowhere once the server takes a connection for another. Fails at once when the queue
    /// is full.
    pub fn try_send_requiring(
        &self,
        kind: String,
        json: String,
        topics: Vec<String>,
    ) -> Result<(), WseError> {
        Ok(self.0.try_send_requiring(&kind, &json, topics)?)
    }

    /// `try_send_requiring`, waiting for room in the queue; `cancel` gives up the wait (a
    /// message already queued still goes).
    pub async fn send_requiring(
        &self,
        kind: String,
        json: String,
        topics: Vec<String>,
        cancel: Arc<WseCancel>,
    ) -> Result<(), WseError> {
        Ok(self.0.send_requiring(kind, json, topics, &cancel.0).await?)
    }

    /// Sends `{"t": kind, "p": json}` on the connection that is ready now, or not at all:
    /// never on a later one. For commands that must not run late. Fails at once when the
    /// queue is full.
    pub fn try_send_live(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send_live(&kind, &json)?)
    }

    /// `try_send_live`, waiting for room in the queue; `cancel` gives up
    /// the wait.
    pub async fn send_live(
        &self,
        kind: String,
        json: String,
        cancel: Arc<WseCancel>,
    ) -> Result<(), WseError> {
        Ok(self.0.send_live(kind, json, &cancel.0).await?)
    }

    /// Sends a signed message on this connection, or not at all; the nonce it went with.
    pub async fn send_signed(
        &self,
        kind: String,
        json: String,
        nonce: Option<String>,
        cancel: Arc<WseCancel>,
    ) -> Result<String, WseError> {
        Ok(self.0.send_signed(kind, json, nonce, &cancel.0).await?)
    }

    pub fn subscribe(&self, topics: Vec<String>) -> Result<(), WseError> {
        Ok(self.0.subscribe(topics)?)
    }

    pub fn unsubscribe(&self, topics: Vec<String>) -> Result<(), WseError> {
        Ok(self.0.unsubscribe(topics)?)
    }

    pub fn reconnect(&self) -> Result<(), WseError> {
        Ok(self.0.reconnect()?)
    }

    /// Ends the connection for `reason` ("background", "offline", ...) and
    /// makes none while any reason holds: topics and positions stay.
    pub fn pause(&self, reason: String) -> Result<(), WseError> {
        Ok(self.0.pause(reason)?)
    }

    /// Clears `reason`; once none is left the client connects again at once
    /// and recovers what it missed.
    pub fn resume(&self, reason: String) -> Result<(), WseError> {
        Ok(self.0.resume(reason)?)
    }

    /// Each topic held: whether the server took it, and whether its
    /// messages run without a break.
    pub fn topic_states(&self) -> Vec<WseTopicState> {
        self.0
            .topic_states()
            .into_iter()
            .map(|s| WseTopicState {
                topic: s.topic,
                subscription: subscription_of(s.subscription),
                continuity: continuity_of(s.continuity),
                reason: s.reason,
            })
            .collect()
    }

    /// The consumer applied the message at `epoch`, `offset` of `topic`,
    /// handed out in `generation`: one of an earlier user counts for
    /// nobody. `commit(message)` passes all four.
    pub fn commit(&self, topic: String, epoch: u32, offset: u64, generation: u32) {
        self.0.commit(&topic, epoch, offset, generation);
    }

    /// What the consumer committed, to save with the application's own
    /// state and give back as `WseDelivery.checkpoint`.
    pub fn checkpoint(&self) -> String {
        self.0.checkpoint()
    }

    /// How many messages wait in the outbox for the server's answer.
    pub fn kept(&self) -> u32 {
        self.0.kept()
    }

    /// recover-or-resync: the application brought the topics of `round` up
    /// to date; what they held comes next.
    pub fn resynced(&self, round: u64) {
        self.0.resynced(round);
    }

    /// Bring `topics` up to date: in recover-or-resync a resync round asks
    /// for it; in best effort the application did so itself.
    pub fn resync(&self, topics: Vec<String>) {
        self.0.resync(topics);
    }

    pub fn status(&self) -> WseStatus {
        match self.0.status() {
            native::NativeStatus::Connecting => WseStatus::Connecting,
            native::NativeStatus::Connected => WseStatus::Connected,
            native::NativeStatus::Reconnecting => WseStatus::Reconnecting,
            native::NativeStatus::Paused => WseStatus::Paused,
            native::NativeStatus::Closed => WseStatus::Closed,
        }
    }

    pub fn connection_id(&self) -> Option<String> {
        self.0.connection_id()
    }

    pub fn user_id(&self) -> Option<String> {
        self.0.user_id()
    }

    /// The next batch of what the client heard: `ack` acknowledges the last
    /// one (0: none yet); one not acknowledged comes again.
    pub async fn next_batch(
        &self,
        ack: u64,
        max_items: u32,
        max_bytes: u32,
        cancel: Arc<WseCancel>,
    ) -> Result<WseBatch, WseError> {
        let batch = self
            .0
            .next_batch(ack, max_items, max_bytes, &cancel.0)
            .await?;
        Ok(WseBatch {
            id: batch.id,
            kind: match batch.kind {
                native::BatchKind::Items => WseBatchKind::Items,
                native::BatchKind::End => WseBatchKind::End,
            },
            items: batch.items.into_iter().map(WseItem::from).collect(),
        })
    }

    /// Closes the client: what was sent goes out first, within `deadline_ms`.
    pub async fn disconnect(&self, deadline_ms: u64) {
        self.0.disconnect(deadline_ms).await;
    }
}
