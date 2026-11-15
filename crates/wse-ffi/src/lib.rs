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
    /// The payload as JSON text (from msgpack too).
    pub json: Option<String>,
    pub signature: Option<WseSignature>,
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
                    signature: m.signature.map(|s| WseSignature {
                        body: s.body,
                        sig: s.sig,
                        kid: s.kid,
                        verified: s.verified,
                    }),
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

#[uniffi::export]
impl WseClient {
    /// Sends `{"t": kind, "p": json}`; fails at once when the queue is full.
    pub fn try_send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send(&kind, &json)?)
    }

    /// Sends `{"t": kind, "p": json}`, waiting for room in the queue.
    pub async fn send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.send(kind, json).await?)
    }

    /// Sends a signed message on this connection, or not at all; the nonce it went with.
    pub async fn send_signed(
        &self,
        kind: String,
        json: String,
        nonce: Option<String>,
    ) -> Result<String, WseError> {
        Ok(self.0.send_signed(kind, json, nonce).await?)
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

    pub fn status(&self) -> WseStatus {
        match self.0.status() {
            native::NativeStatus::Connecting => WseStatus::Connecting,
            native::NativeStatus::Connected => WseStatus::Connected,
            native::NativeStatus::Reconnecting => WseStatus::Reconnecting,
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
