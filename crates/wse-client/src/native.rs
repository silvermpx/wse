//! The client as mobile bindings need it (the `native` feature): plain types that any
//! binding generator can carry (UniFFI for Swift and Kotlin,
//! flutter_rust_bridge for Dart), one tokio runtime for every client of the
//! process, and calls that can be cancelled without losing anything.
//!
//! - [`NativeClient::connect`] runs on the shared [`runtime`] and gives up at
//!   once when its [`CancelToken`] is cancelled.
//! - What the server sends is read in batches, at most `max_items` and about
//!   `max_bytes` each, with [`NativeClient::next_batch`]. A batch is handed
//!   out again until the next call acknowledges its id, so a binding whose
//!   call was cancelled after the batch was taken from the client loses
//!   nothing: it asks again with the old id and gets the same batch.
//! - Errors have a kind a binding can match on and a message; kinds added
//!   later read as [`ErrorKind::Unknown`] in old code of the binding.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::{
    Config, Error, Event, Events, GapReason, Incoming, OwnedMessage, Status, TokenProvider,
};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

// -- the runtime --------------------------------------------------------------

/// Worker threads of the shared runtime: a client spends most of its time
/// waiting on its socket, and every client of the process shares them.
const WORKER_THREADS: usize = 2;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static ON_THREAD_START: OnceLock<fn()> = OnceLock::new();

/// Called on each thread of the runtime as it starts (on Android, to attach
/// it to the JVM once). Takes effect only before the runtime's first use.
pub fn set_on_thread_start(hook: fn()) {
    let _ = ON_THREAD_START.set(hook);
}

/// The runtime every client of the process runs on.
pub fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder
            .worker_threads(WORKER_THREADS)
            .thread_name("wse")
            .enable_all();
        if let Some(hook) = ON_THREAD_START.get() {
            builder.on_thread_start(*hook);
        }
        builder.build().expect("the WSE runtime starts")
    })
}

/// Runs `work` on the shared runtime: dropping the returned future (a
/// binding's call cancelled) stops the work too. A panic is an
/// [`ErrorKind::Internal`] error, not one that unwinds into the binding.
async fn on_runtime<T: Send + 'static>(
    work: impl Future<Output = Result<T, NativeError>> + Send + 'static,
) -> Result<T, NativeError> {
    struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
    impl<T> Drop for AbortOnDrop<T> {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let mut task = AbortOnDrop(runtime().spawn(work));
    match (&mut task.0).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => {
            Err(NativeError::new(ErrorKind::Internal, "the call panicked"))
        }
        Err(_) => Err(NativeError::new(
            ErrorKind::Cancelled,
            "the call was cancelled",
        )),
    }
}

// -- errors -------------------------------------------------------------------

/// What went wrong, for a binding to match on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The configuration cannot work: a URL that is not ws:// or wss://, a header that cannot be sent.
    Config,
    /// The server could not be reached or did not upgrade the connection.
    Connect,
    /// The server refused the credentials.
    Auth,
    /// The server did not confirm the connection in time.
    Timeout,
    /// The end-to-end session could not be opened or went wrong.
    Encryption,
    /// A signed message cannot be sent: no signing key, or no server key to sign for.
    Signing,
    /// The client gave up reconnecting.
    GaveUp,
    /// Not connected now: the client is reconnecting.
    NotConnected,
    /// The send queue is full.
    Full,
    /// The client was closed.
    Closed,
    /// A message could not be encoded (not JSON, say).
    Encode,
    /// A payload could not be decoded.
    Decode,
    /// The call was cancelled.
    Cancelled,
    /// Another call reads the events: one reader at a time.
    Busy,
    /// A panic inside the client.
    Internal,
    /// A kind this version does not know.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeError {
    pub kind: ErrorKind,
    pub message: String,
}

impl NativeError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NativeError {}

impl From<Error> for NativeError {
    fn from(error: Error) -> Self {
        let kind = match &error {
            Error::Config(_) => ErrorKind::Config,
            Error::Connect(_) => ErrorKind::Connect,
            Error::Auth(_) => ErrorKind::Auth,
            Error::Timeout(_) => ErrorKind::Timeout,
            Error::Encryption(_) => ErrorKind::Encryption,
            Error::Signing(_) => ErrorKind::Signing,
            Error::GaveUp(_) => ErrorKind::GaveUp,
            Error::NotConnected => ErrorKind::NotConnected,
            Error::Full => ErrorKind::Full,
            Error::Closed => ErrorKind::Closed,
            Error::Panicked => ErrorKind::Internal,
            Error::Encode(_) => ErrorKind::Encode,
            Error::Decode(_) => ErrorKind::Decode,
        };
        Self::new(kind, error.to_string())
    }
}

// -- configuration ------------------------------------------------------------

/// Where the client connects and how. Durations in milliseconds; zero means
/// the Rust client's default.
#[derive(Clone, Debug, Default)]
pub struct NativeConfig {
    /// ws://host:port/wse or wss://...
    pub url: String,
    /// The JWT (cookie and Authorization header); a token source takes its place.
    pub token: Option<String>,
    /// Topics subscribed to on every connection.
    pub topics: Vec<String>,
    /// The server sends msgpack instead of JSON.
    pub msgpack: bool,
    /// More headers for the upgrade request.
    pub headers: Vec<(String, String)>,
    /// Messages waiting to be sent at most (default 1024).
    pub send_queue: u32,
    /// Reconnect after a lost connection (default true).
    pub reconnect: Option<bool>,
    pub connect_timeout_ms: u64,
    pub ready_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub recovery_timeout_ms: u64,
    /// End-to-end encryption, pinned to these identity keys of the server (base64).
    pub server_identity_keys: Vec<String>,
    /// This client's ECDSA P-256 private key (PKCS#8, PEM or DER), for send_signed.
    pub signing_key: Option<Vec<u8>>,
    /// The server's signing public keys the messages it signs are checked against.
    pub server_signing_keys: Vec<String>,
    /// The certificates a wss:// server's chain must lead to.
    pub trust: NativeTrust,
    /// Allows ws://, which sends everything in the clear: for development.
    pub insecure: bool,
    /// recover-or-resync; None: best effort.
    pub delivery: Option<NativeDelivery>,
}

/// The recover-or-resync delivery: a topic that is not continuous (a cold
/// start, every reconnect, a break) holds its messages until the
/// application brought its state up to date ([`Item::Resync`], then
/// [`NativeClient::resynced`]); the checkpoint is what it committed it
/// applied, and recovery starts from it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NativeDelivery {
    /// The one consumer that commits, by a stable name.
    pub consumer: String,
    /// The application's data schema: a checkpoint of another is refused.
    pub schema: Option<String>,
    /// The server's realm (default: the URL): a checkpoint of another is refused.
    pub realm: Option<String>,
    /// The checkpoint the application saved last ([`NativeClient::checkpoint`]).
    pub checkpoint: Option<String>,
    /// Messages a topic holds while it resyncs (0: 10,000); more starts the resync over.
    pub max_held: u32,
}

/// Whether the server took a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeSubscription {
    Requested,
    Active,
    Denied,
}

/// Whether a topic's messages run without a break.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeContinuity {
    Continuous,
    ResyncRequired,
    Resyncing,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTopicState {
    pub topic: String,
    pub subscription: NativeSubscription,
    pub continuity: NativeContinuity,
    /// Why it is not continuous: cold_start, reconnected, offset_gap,
    /// not_recovered, epoch_changed, overflow, requested, ...
    pub reason: Option<String>,
}

/// Where a stream of a topic stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPosition {
    pub topic: String,
    pub epoch: u32,
    pub offset: u64,
}

/// Which certificates a wss:// server's chain must lead to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum NativeTrust {
    /// The system's own verifier: the roots the device trusts (with those
    /// its owner or an administrator added, where the system counts them)
    /// and its revocation checks. On Android it needs [`init_android`] first;
    /// the Kotlin and Flutter packages call it as the app starts.
    #[default]
    Platform,
    /// The Mozilla root certificates built into the library: the same on
    /// every device.
    Mozilla,
    /// Only these CA certificates, PEM (one or more each) or DER: a private
    /// CA.
    Roots(Vec<Vec<u8>>),
}

/// The token for each connection attempt, asked of the application.
pub trait TokenSource: Send + Sync + 'static {
    fn token(&self) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
}

fn config_of(
    native: NativeConfig,
    tokens: Option<Arc<dyn TokenSource>>,
    delivery: &Arc<std::sync::Mutex<crate::delivery::Delivery>>,
) -> Result<Config, NativeError> {
    if native.url.starts_with("ws://") && !native.insecure {
        return Err(NativeError::new(
            ErrorKind::Config,
            "ws:// sends everything in the clear: use wss://, or set insecure (for development)",
        ));
    }
    let tls = native
        .url
        .starts_with("wss://")
        .then(|| tls_of(native.trust))
        .transpose()?;
    let mut config = Config::new(native.url);
    config.tls = tls;
    if native.delivery.is_some() {
        // Recovery starts from what the application committed.
        let delivery = Arc::clone(delivery);
        config.committed = Some(Arc::new(move |user| {
            delivery.lock().ok().and_then(|mut d| d.committed(user))
        }));
    }
    config.token = native.token;
    if let Some(source) = tokens {
        config.token_provider = Some(TokenProvider::new(move || source.token()));
    }
    config.topics = native.topics;
    if native.msgpack {
        config.format = crate::Format::Msgpack;
    }
    config.headers = native.headers;
    if native.send_queue > 0 {
        config.send_queue = native.send_queue as usize;
    }
    if let Some(reconnect) = native.reconnect {
        config.reconnect.enabled = reconnect;
    }
    let ms = |value: u64, default: Duration| {
        if value == 0 {
            default
        } else {
            Duration::from_millis(value)
        }
    };
    config.connect_timeout = ms(native.connect_timeout_ms, config.connect_timeout);
    config.ready_timeout = ms(native.ready_timeout_ms, config.ready_timeout);
    config.idle_timeout = ms(native.idle_timeout_ms, config.idle_timeout);
    if native.recovery_timeout_ms > 0 {
        config.recovery_timeout = Duration::from_millis(native.recovery_timeout_ms);
    }
    if !native.server_identity_keys.is_empty() {
        config.encryption = Some(crate::Encryption::pinned(native.server_identity_keys));
    }
    config.signing_key = native.signing_key;
    config.server_signing_keys = native.server_signing_keys;
    Ok(config)
}

/// The TLS configuration of `trust`: the system's verifier and the Mozilla
/// roots are made once for the process.
fn tls_of(trust: NativeTrust) -> Result<Arc<dyn std::any::Any + Send + Sync>, NativeError> {
    let config = match trust {
        NativeTrust::Platform => {
            #[cfg(target_os = "android")]
            if !ANDROID_VERIFIER.load(std::sync::atomic::Ordering::Acquire) {
                return Err(NativeError::new(
                    ErrorKind::Config,
                    "Android's certificate verifier is not set up: the Kotlin library and the \
                     Flutter package set it up as the app starts (WseTls.init(context) by hand)",
                ));
            }
            static PLATFORM: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
            match PLATFORM.get() {
                Some(config) => config.clone(),
                None => {
                    let builder = crate::connection::tls_builder();
                    let verifier =
                        rustls_platform_verifier::Verifier::new(builder.crypto_provider().clone())
                            .map_err(|e| {
                                NativeError::new(
                                    ErrorKind::Config,
                                    format!("the system's certificate verifier: {e}"),
                                )
                            })?;
                    let config = builder
                        .dangerous()
                        .with_custom_certificate_verifier(Arc::new(verifier))
                        .with_no_client_auth();
                    PLATFORM.get_or_init(|| Arc::new(config)).clone()
                }
            }
        }
        NativeTrust::Mozilla => crate::connection::default_tls(),
        NativeTrust::Roots(certificates) => {
            use rustls::pki_types::CertificateDer;
            use rustls::pki_types::pem::PemObject;
            let bad = |e: &dyn std::fmt::Display| {
                NativeError::new(ErrorKind::Config, format!("a trusted root: {e}"))
            };
            let mut roots = rustls::RootCertStore::empty();
            for certificate in certificates {
                if memchr::memmem::find(&certificate, b"-----BEGIN").is_some() {
                    for der in CertificateDer::pem_slice_iter(&certificate) {
                        roots.add(der.map_err(|e| bad(&e))?).map_err(|e| bad(&e))?;
                    }
                } else {
                    roots
                        .add(CertificateDer::from(certificate))
                        .map_err(|e| bad(&e))?;
                }
            }
            if roots.is_empty() {
                return Err(bad(&"no certificate given"));
            }
            Arc::new(
                crate::connection::tls_builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        }
    };
    Ok(config)
}

/// Android's verifier was given the JVM and the app's context.
#[cfg(target_os = "android")]
static ANDROID_VERIFIER: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Gives Android's certificate verifier the JVM and the application's
/// context, once per process: [`NativeTrust::Platform`] needs it. A
/// binding's JNI method calls it with the arguments the JVM gave it.
///
/// # Safety
///
/// `env` is the JNIEnv of the calling thread and `context` a reference to
/// an `android.content.Context`, both valid for the call.
#[cfg(target_os = "android")]
pub unsafe fn init_android(
    env: *mut std::ffi::c_void,
    context: *mut std::ffi::c_void,
) -> Result<(), NativeError> {
    // Safety: the caller passes the thread's JNIEnv.
    let mut unowned = unsafe { jni::EnvUnowned::from_raw(env.cast()) };
    let outcome = unowned
        .with_env(|env| -> Result<(), jni::errors::Error> {
            // Safety: the caller passes a valid Context reference.
            let context = unsafe { jni::objects::JObject::from_raw(env, context.cast()) };
            rustls_platform_verifier::android::init_with_env(env, context)
        })
        .into_outcome();
    match outcome {
        jni::Outcome::Ok(()) => {
            ANDROID_VERIFIER.store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }
        jni::Outcome::Err(e) => Err(NativeError::new(
            ErrorKind::Config,
            format!("Android's certificate verifier: {e}"),
        )),
        jni::Outcome::Panic(_) => Err(NativeError::new(
            ErrorKind::Internal,
            "Android's certificate verifier panicked as it was set up",
        )),
    }
}

// -- signing keys -------------------------------------------------------------

/// A new ECDSA P-256 private key (PKCS#8 DER), for signed messages: the
/// application keeps it in the platform's keystore.
pub fn generate_signing_key() -> Vec<u8> {
    crate::generate_key()
}

/// The public key of a PKCS#8 private key, base64 of the uncompressed
/// point: what the server binds to a connection for its signed messages.
pub fn signing_public_key(pkcs8: &[u8]) -> Result<String, NativeError> {
    Ok(crate::public_key(pkcs8)?)
}

// -- cancellation ---------------------------------------------------------------

/// Cancels the calls it is given to (connect, next_batch): a binding's own
/// cancellation (a Swift Task, say) cancels it.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(CancellationToken);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
}

// -- what the client hears -----------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeSignature {
    /// The exact text that was signed (a JSON object): keep it as it is.
    pub body: String,
    /// r || s in base64.
    pub sig: String,
    pub kid: String,
    /// Checked against a pinned server signing key.
    pub verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeMessage {
    pub kind: String,
    pub topic: Option<String>,
    /// The message's stream and offset in its topic, when it has them.
    pub epoch: Option<u32>,
    pub offset: Option<u64>,
    pub id: Option<String>,
    /// The payload as JSON text, from msgpack too; None without one (absent
    /// or null, in either format) or when it could not be decoded.
    pub json: Option<String>,
    /// Why the payload could not be decoded: msgpack that JSON cannot show
    /// (a map keyed by a number, say). None when `json` holds it.
    pub payload_error: Option<String>,
    pub signature: Option<NativeSignature>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRecovery {
    pub topic: String,
    pub recovered: bool,
    pub count: u64,
}

/// One thing the client heard, in the order it came.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Item {
    Message(NativeMessage),
    /// Bytes the server application sent as they are.
    Binary(Vec<u8>),
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
        recovery: Vec<TopicRecovery>,
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
        message: String,
        details_json: String,
    },
    /// The client stopped for good: closed by the application, or the reason.
    Closed {
        error: Option<NativeError>,
    },
    /// recover-or-resync: bring the state of `topics` up to date (the
    /// reason of each, what the consumer committed of each), then call
    /// [`NativeClient::resynced`] with `round`; their messages wait till then.
    Resync {
        round: u64,
        topics: Vec<String>,
        reasons: Vec<String>,
        committed: Vec<StreamPosition>,
    },
    /// A topic's continuity changed, and why it is not continuous.
    Continuity {
        topic: String,
        continuity: NativeContinuity,
        reason: Option<String>,
    },
    /// The saved checkpoint is not this client's: its realm, user, consumer,
    /// schema or version, or it is malformed.
    CheckpointRejected {
        reason: String,
    },
    /// Something this version does not know.
    Unknown {
        name: String,
    },
}

impl Item {
    /// About what it costs a binding to carry.
    fn size(&self) -> usize {
        64 + match self {
            Item::Message(m) => {
                m.kind.len()
                    + m.topic.as_ref().map_or(0, String::len)
                    + m.json.as_ref().map_or(0, String::len)
                    + m.signature
                        .as_ref()
                        .map_or(0, |s| s.body.len() + s.sig.len())
            }
            Item::Binary(bytes) => bytes.len(),
            Item::ServerError { details_json, .. } => details_json.len(),
            _ => 0,
        }
    }
}

fn gap_reason(reason: GapReason) -> &'static str {
    match reason {
        GapReason::Timeout => "timeout",
        GapReason::Overflow => "overflow",
        GapReason::NotRecovered => "not_recovered",
        GapReason::Unavailable => "unavailable",
        GapReason::Incomplete => "incomplete",
        GapReason::EpochChanged => "epoch_changed",
        GapReason::Closed => "closed",
    }
}

fn message_of(message: &OwnedMessage) -> NativeMessage {
    let position = message.position();
    // The same payload reads the same in either format: none (absent or
    // null: JSON reads `"p": null` as none), or its JSON; msgpack JSON cannot
    // show says why instead.
    let (json, payload_error) = match message.payload_json() {
        Some(text) => (Some(text.to_owned()), None),
        None if !message.has_payload() => (None, None),
        None => match message.payload_value() {
            Ok(serde_json::Value::Null) => (None, None),
            Ok(value) => (Some(value.to_string()), None),
            Err(error) => (None, Some(error.to_string())),
        },
    };
    NativeMessage {
        kind: message.kind().to_owned(),
        topic: message.topic().map(str::to_owned),
        epoch: position.map(|p| p.epoch),
        offset: position.map(|p| p.offset),
        id: message.id().map(str::to_owned),
        json,
        payload_error,
        signature: message.signature().map(|s| NativeSignature {
            body: s.body.clone(),
            sig: s.sig.clone(),
            kid: s.kid.clone(),
            verified: s.verified,
        }),
    }
}

fn item_of(incoming: Incoming) -> Item {
    match incoming {
        Incoming::Message(message) => Item::Message(message_of(&message)),
        Incoming::Binary(bytes) => Item::Binary(bytes.to_vec()),
        Incoming::Event(event) => match event {
            Event::Connected(c) => Item::Connected {
                connection_id: c.connection_id,
                user_id: c.user_id,
                reconnected: c.reconnected,
                recovery: c.recovery,
            },
            Event::Disconnected(d) => Item::Disconnected {
                code: d.code,
                reason: d.reason,
                retry_in_ms: u64::try_from(d.retry_in.as_millis()).unwrap_or(u64::MAX),
                attempt: d.attempt,
            },
            Event::Paused(p) => Item::Paused { reasons: p.reasons },
            Event::Subscribed(s) => Item::Subscribed {
                topics: s.topics,
                failed: s.failed,
                recovery: s
                    .recovery
                    .into_iter()
                    .map(|r| TopicRecovery {
                        topic: r.topic,
                        recovered: r.recovered,
                        count: r.count,
                    })
                    .collect(),
            },
            Event::Gap(g) => Item::Gap {
                reason: gap_reason(g.reason).to_owned(),
                topic: g.topic,
                epoch: g.epoch,
                first: g.first,
                last: g.last,
            },
            Event::ServerError(e) => Item::ServerError {
                code: e.code,
                message: e.message,
                details_json: e.details.to_string(),
            },
            Event::Closed(error) => Item::Closed {
                error: error.map(NativeError::from),
            },
        },
    }
}

/// Whether a batch carries items, or the end of the events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKind {
    Items,
    /// Nothing more will come: the client closed.
    End,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// Acknowledge it with the next call; until then it is handed out again.
    pub id: u64,
    pub kind: BatchKind,
    pub items: Vec<Item>,
}

/// The events and the batch handed out but not acknowledged yet.
struct Reader {
    events: Events,
    pending: Option<Batch>,
    next_id: u64,
    ended: bool,
}

// -- the client ---------------------------------------------------------------

/// Statuses a binding shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStatus {
    Connecting,
    Connected,
    Reconnecting,
    Paused,
    Closed,
}

pub struct NativeClient {
    client: crate::Client,
    reader: Mutex<Reader>,
    closing: CancellationToken,
    delivery: Arc<std::sync::Mutex<crate::delivery::Delivery>>,
    /// Items the delivery made outside the events wake a waiting reader.
    woken: Arc<tokio::sync::Notify>,
}

impl NativeClient {
    /// Connects, and returns once the server took the connection, or fails;
    /// gives up at once when `cancel` is cancelled.
    pub async fn connect(
        config: NativeConfig,
        tokens: Option<Arc<dyn TokenSource>>,
        cancel: &CancelToken,
    ) -> Result<Self, NativeError> {
        let delivery =
            crate::delivery::Delivery::new(config.delivery.clone(), &config.url, &config.topics)?;
        let delivery = Arc::new(std::sync::Mutex::new(delivery));
        let config = config_of(config, tokens, &delivery)?;
        let cancel = cancel.0.clone();
        let (client, events) = on_runtime(async move {
            tokio::select! {
                connected = crate::connect(config) => connected.map_err(NativeError::from),
                () = cancel.cancelled() => Err(NativeError::new(ErrorKind::Cancelled, "connect was cancelled")),
            }
        })
        .await?;
        Ok(Self {
            client,
            reader: Mutex::new(Reader {
                events,
                pending: None,
                next_id: 1,
                ended: false,
            }),
            closing: CancellationToken::new(),
            delivery,
            woken: Arc::new(tokio::sync::Notify::new()),
        })
    }

    fn delivery(&self) -> std::sync::MutexGuard<'_, crate::delivery::Delivery> {
        self.delivery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Each topic held: whether the server took it, and whether its
    /// messages run without a break.
    pub fn topic_states(&self) -> Vec<NativeTopicState> {
        self.delivery().topic_states()
    }

    /// The consumer applied the message at `epoch`, `offset` of `topic`: the
    /// checkpoint moves on through what was applied without a hole.
    pub fn commit(&self, topic: &str, epoch: u32, offset: u64) {
        self.delivery().commit(topic, epoch, offset);
    }

    /// What the consumer committed, bound to the realm, the user, the
    /// consumer and the schema: to save with the application's own state and
    /// give back as [`NativeDelivery::checkpoint`].
    pub fn checkpoint(&self) -> String {
        self.delivery().checkpoint()
    }

    /// recover-or-resync: the application brought the topics of `round` up
    /// to date; what they held comes next.
    pub fn resynced(&self, round: u64) {
        self.delivery().resynced(round);
        self.woken.notify_one();
    }

    /// Bring `topics` up to date: in recover-or-resync a resync round asks
    /// for it; in best effort the application did so itself, and they read
    /// continuous again.
    pub fn resync(&self, topics: Vec<String>) {
        self.delivery().resync(&topics);
        self.woken.notify_one();
    }

    /// Sends `{"t": kind, "p": json}`; fails at once when the queue is full.
    pub fn try_send(&self, kind: &str, json: &str) -> Result<(), NativeError> {
        let payload = raw_json(json)?;
        Ok(self.client.try_send(kind, &*payload)?)
    }

    /// Sends `{"t": kind, "p": json}`, waiting for room in the queue;
    /// `cancel` gives up the wait (a message already queued still goes).
    pub async fn send(
        &self,
        kind: String,
        json: String,
        cancel: &CancelToken,
    ) -> Result<(), NativeError> {
        let payload = raw_json(&json)?;
        let client = self.client.clone();
        let cancel = cancel.0.clone();
        on_runtime(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled("send")),
                sent = client.send(&kind, &*payload) => sent.map_err(NativeError::from),
            }
        })
        .await
    }

    /// Sends `{"t": kind, "p": json}` on the connection that is ready now,
    /// or not at all: never on a later one. For commands that must not run
    /// late. Fails at once when the queue is full.
    pub fn try_send_live(&self, kind: &str, json: &str) -> Result<(), NativeError> {
        let payload = raw_json(json)?;
        Ok(self.client.try_send_live(kind, &*payload)?)
    }

    /// [`NativeClient::try_send_live`], waiting for room in the queue;
    /// `cancel` gives up the wait.
    pub async fn send_live(
        &self,
        kind: String,
        json: String,
        cancel: &CancelToken,
    ) -> Result<(), NativeError> {
        let payload = raw_json(&json)?;
        let client = self.client.clone();
        let cancel = cancel.0.clone();
        on_runtime(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled("send_live")),
                sent = client.send_live(&kind, &*payload) => sent.map_err(NativeError::from),
            }
        })
        .await
    }

    /// Sends a signed message on this connection, or not at all; the nonce
    /// it went with.
    pub async fn send_signed(
        &self,
        kind: String,
        json: String,
        nonce: Option<String>,
        cancel: &CancelToken,
    ) -> Result<String, NativeError> {
        let payload = raw_json(&json)?;
        let client = self.client.clone();
        let cancel = cancel.0.clone();
        on_runtime(async move {
            tokio::select! {
                biased;
                () = cancel.cancelled() => Err(cancelled("send_signed")),
                sent = client.send_signed(&kind, &*payload, nonce.as_deref()) => {
                    sent.map_err(NativeError::from)
                }
            }
        })
        .await
    }

    pub fn subscribe(&self, topics: Vec<String>) -> Result<(), NativeError> {
        self.delivery().track(&topics);
        Ok(self.client.subscribe(topics)?)
    }

    pub fn unsubscribe(&self, topics: Vec<String>) -> Result<(), NativeError> {
        self.delivery().untrack(&topics);
        Ok(self.client.unsubscribe(topics)?)
    }

    /// Drops the connection and makes it again at once.
    pub fn reconnect(&self) -> Result<(), NativeError> {
        Ok(self.client.reconnect()?)
    }

    /// Ends the connection for `reason` (`"background"`, `"offline"`, ...)
    /// and makes none while any reason holds: topics and positions stay.
    pub fn pause(&self, reason: String) -> Result<(), NativeError> {
        Ok(self.client.pause(reason)?)
    }

    /// Clears `reason`; once none is left the client connects again at once
    /// and recovers what it missed.
    pub fn resume(&self, reason: String) -> Result<(), NativeError> {
        Ok(self.client.resume(reason)?)
    }

    pub fn status(&self) -> NativeStatus {
        match self.client.status() {
            Status::Connecting => NativeStatus::Connecting,
            Status::Connected => NativeStatus::Connected,
            Status::Reconnecting => NativeStatus::Reconnecting,
            Status::Paused => NativeStatus::Paused,
            _ => NativeStatus::Closed,
        }
    }

    pub fn connection_id(&self) -> Option<String> {
        self.client.connection_id()
    }

    pub fn user_id(&self) -> Option<String> {
        self.client.user_id()
    }

    /// The next batch of what the client heard: the one with id `ack` is
    /// acknowledged (0: none yet), the one handed out and not acknowledged is
    /// handed out again. Waits for at least one item (or the end), then takes
    /// what came with it, up to `max_items` and about `max_bytes`. One
    /// reader at a time; `cancel` (or [`disconnect`](Self::disconnect)) ends
    /// the wait.
    pub async fn next_batch(
        &self,
        ack: u64,
        max_items: u32,
        max_bytes: u32,
        cancel: &CancelToken,
    ) -> Result<Batch, NativeError> {
        let mut reader = self
            .reader
            .try_lock()
            .map_err(|_| NativeError::new(ErrorKind::Busy, "another call reads the events"))?;
        if let Some(pending) = &reader.pending {
            if pending.id != ack {
                return Ok(pending.clone());
            }
            reader.pending = None;
        }
        if reader.ended {
            let id = reader.next_id;
            return Ok(Batch {
                id,
                kind: BatchKind::End,
                items: Vec::new(),
            });
        }
        let max_items = max_items.max(1) as usize;
        let max_bytes = max_bytes.max(1) as usize;
        let id = reader.next_id;
        reader.next_id += 1;
        // What the delivery holds is safe with it; what it hands out goes into
        // this batch before anything awaits again.
        let mut items = Vec::new();
        loop {
            self.delivery_out(&mut items, max_items);
            if !items.is_empty() {
                break;
            }
            let first = tokio::select! {
                first = reader.events.next() => first,
                () = self.woken.notified() => continue,
                () = cancel.0.cancelled() => return Err(NativeError::new(ErrorKind::Cancelled, "next_batch was cancelled")),
                () = self.closing.cancelled() => None,
            };
            let Some(first) = first else {
                reader.ended = true;
                let batch = Batch {
                    id,
                    kind: BatchKind::End,
                    items: Vec::new(),
                };
                reader.pending = Some(batch.clone());
                return Ok(batch);
            };
            self.delivery().take(item_of(first), &mut items);
        }
        let mut bytes: usize = items.iter().map(Item::size).sum();
        while items.len() < max_items && bytes < max_bytes {
            let Some(incoming) = reader.events.try_next() else {
                break;
            };
            let before = items.len();
            self.delivery().take(item_of(incoming), &mut items);
            bytes += items[before..].iter().map(Item::size).sum::<usize>();
        }
        let batch = Batch {
            id,
            kind: BatchKind::Items,
            items,
        };
        reader.pending = Some(batch.clone());
        Ok(batch)
    }

    /// Items the delivery made outside the events, up to `max_items`.
    fn delivery_out(&self, items: &mut Vec<Item>, max_items: usize) {
        let mut delivery = self.delivery();
        while items.len() < max_items {
            match delivery.take_out() {
                Some(item) => items.push(item),
                None => break,
            }
        }
    }

    /// Closes the client: what was sent goes out first, within `deadline_ms`;
    /// a reader waiting in [`next_batch`](Self::next_batch) gets the end.
    pub async fn disconnect(&self, deadline_ms: u64) {
        self.closing.cancel();
        let client = self.client.clone();
        let deadline = Duration::from_millis(deadline_ms.max(1));
        let _ = on_runtime(async move {
            client.close_within(deadline).await;
            Ok(())
        })
        .await;
    }
}

fn cancelled(call: &str) -> NativeError {
    NativeError::new(ErrorKind::Cancelled, format!("{call} was cancelled"))
}

fn raw_json(json: &str) -> Result<Box<serde_json::value::RawValue>, NativeError> {
    serde_json::value::RawValue::from_string(json.to_owned())
        .map_err(|e| NativeError::new(ErrorKind::Encode, format!("the payload is not JSON: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{json_envelope, msgpack_envelope};

    fn from_json(text: &str) -> NativeMessage {
        message_of(&json_envelope(text).unwrap().into_message(None).to_owned())
    }

    fn from_msgpack(bytes: &[u8]) -> NativeMessage {
        message_of(
            &msgpack_envelope(bytes)
                .unwrap()
                .into_message(None)
                .to_owned(),
        )
    }

    fn packed(value: &serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(value).unwrap()
    }

    /// The same message in either format: its payload as the app sees it.
    #[test]
    fn a_payload_reads_the_same_in_json_and_msgpack() {
        let cases = [
            (serde_json::json!({"t": "m"}), None),
            (serde_json::json!({"t": "m", "p": null}), None),
            (
                serde_json::json!({"t": "m", "p": 18446744073709551615u64}),
                Some("18446744073709551615"),
            ),
            (serde_json::json!({"t": "m", "p": -2.5}), Some("-2.5")),
            (
                serde_json::json!({"t": "m", "p": {"a": [1, null]}}),
                Some(r#"{"a":[1,null]}"#),
            ),
        ];
        for (message, payload) in cases {
            let text = message.to_string();
            let json = from_json(&text);
            let msgpack = from_msgpack(&packed(&message));
            assert_eq!(json.json.as_deref(), payload, "json {text}");
            assert_eq!(msgpack.json.as_deref(), payload, "msgpack {text}");
            assert_eq!(json.payload_error, None);
            assert_eq!(msgpack.payload_error, None);
        }
    }

    #[test]
    fn a_signing_key_and_its_public_key() {
        let key = generate_signing_key();
        let public = signing_public_key(&key).unwrap();
        // base64 of 0x04 || X || Y: 65 bytes.
        assert_eq!(public.len(), 88);
        assert_ne!(generate_signing_key(), key);
        assert_eq!(
            signing_public_key(b"not a key").unwrap_err().kind,
            ErrorKind::Config
        );
    }

    #[test]
    fn a_msgpack_payload_json_cannot_show_is_an_error() {
        // {"t": "m", "p": {1: 2}}: a map keyed by a number.
        let bytes = [0x82, 0xa1, b't', 0xa1, b'm', 0xa1, b'p', 0x81, 0x01, 0x02];
        let message = from_msgpack(&bytes);
        assert_eq!(message.json, None);
        assert!(message.payload_error.is_some());
    }
}
