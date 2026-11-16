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
    /// Subscribed: the topics taken and refused, and per topic asked for with
    /// a position what was replayed.
    pub topics: Vec<String>,
    pub failed: Vec<String>,
    pub recoveries: Vec<WseTopicRecovery>,
    /// Gap: the first and last offsets missing, when known.
    pub first: Option<u64>,
    pub last: Option<u64>,
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
            topics: Vec::new(),
            failed: Vec::new(),
            recoveries: Vec::new(),
            first: None,
            last: None,
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

#[frb(opaque)]
pub struct WseNativeClient(native::NativeClient);

impl WseNativeClient {
    /// Connects, and returns once the server took the connection.
    pub async fn connect(
        config: WseConfig,
        cancel: &WseCancel,
    ) -> Result<WseNativeClient, WseError> {
        let headers = config
            .header_names
            .into_iter()
            .zip(config.header_values)
            .collect();
        let native_config = native::NativeConfig {
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
            ..native::NativeConfig::default()
        };
        let client = native::NativeClient::connect(native_config, None, &cancel.0).await?;
        Ok(WseNativeClient(client))
    }

    #[frb(sync)]
    pub fn try_send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.try_send(&kind, &json)?)
    }

    pub async fn send(&self, kind: String, json: String) -> Result<(), WseError> {
        Ok(self.0.send(kind, json, &native::CancelToken::new()).await?)
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

    /// connecting, connected, reconnecting or closed.
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
