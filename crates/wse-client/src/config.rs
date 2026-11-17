//! What a client connects to and how it behaves.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::handshake::client::Request;
#[cfg(not(target_arch = "wasm32"))]
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::error::Error;

/// The wire format the server sends this client.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Format {
    /// JSON (the default).
    #[default]
    Json,
    /// MessagePack: 24-41% fewer bytes on typical messages.
    Msgpack,
}

/// How a lost connection is made again.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Reconnect {
    /// Reconnect at all; without it a lost connection ends the client.
    pub enabled: bool,
    /// The wait before the first attempt.
    pub base_delay: Duration,
    /// The longest wait between attempts.
    pub max_delay: Duration,
    /// Each attempt waits this many times longer than the one before.
    pub factor: f64,
    /// Spread waits by ±10%, so clients of a restarted server do not all
    /// come back at once.
    pub jitter: bool,
    /// Attempts in a row before giving up; `None` never gives up.
    pub max_attempts: Option<u32>,
}

impl Default for Reconnect {
    fn default() -> Self {
        Self {
            enabled: true,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            factor: 1.5,
            jitter: true,
            max_attempts: None,
        }
    }
}

impl Reconnect {
    /// The wait before attempt `attempt` (0 for the first).
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        let base = self.base_delay.as_secs_f64();
        let exp = self.factor.max(1.0).powi(attempt.min(1000) as i32);
        let mut secs = (base * exp).min(self.max_delay.as_secs_f64());
        if !secs.is_finite() {
            secs = self.max_delay.as_secs_f64();
        }
        if self.jitter {
            secs += secs * 0.2 * (random() - 0.5);
        }
        Duration::from_secs_f64(secs.clamp(0.0, 300.0))
    }
}

/// End-to-end encryption: a session with the server inside the
/// connection, so whatever terminates TLS in between (a proxy, a load
/// balancer) sees nothing.
#[cfg(feature = "aws-lc")]
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Encryption {
    /// The server's identity public keys (base64 of the uncompressed
    /// point, from the server's `e2e_identity_public_key`): its answer must
    /// be signed by one of them.
    pub server_keys: Vec<String>,
    /// Without `server_keys`: go on with a server that does not sign.
    /// Protects from passive observers only.
    pub unauthenticated: bool,
    /// With [`Config::signing_key`], the session is bound to it (true, the
    /// default): the client proves the key inside the session, and goes on
    /// only once the server confirms the session is bound to that key, as
    /// its token names it (`cnf`). A token copied on the way then opens
    /// nothing. False for a token that names no key, whose key the server
    /// application binds itself (`set_client_key`).
    pub bind_session: bool,
}

#[cfg(feature = "aws-lc")]
impl Default for Encryption {
    fn default() -> Self {
        Self {
            server_keys: Vec::new(),
            unauthenticated: false,
            bind_session: true,
        }
    }
}

#[cfg(feature = "aws-lc")]
impl Encryption {
    /// A session with a server holding one of `server_keys`.
    pub fn pinned<I, K>(server_keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        Self {
            server_keys: server_keys.into_iter().map(Into::into).collect(),
            ..Self::default()
        }
    }

    /// A session with whatever server answers.
    pub fn unauthenticated() -> Self {
        Self {
            unauthenticated: true,
            ..Self::default()
        }
    }
}

/// A number in [0, 1), for the jitter of the waits.
#[cfg(not(target_arch = "wasm32"))]
fn random() -> f64 {
    fastrand::f64()
}

#[cfg(target_arch = "wasm32")]
fn random() -> f64 {
    js_sys::Math::random()
}

#[cfg(not(target_arch = "wasm32"))]
type TokenFuture = Pin<Box<dyn Future<Output = Result<String, String>> + Send>>;
#[cfg(target_arch = "wasm32")]
type TokenFuture = Pin<Box<dyn Future<Output = Result<String, String>>>>;

/// Fetches the token for each connection attempt, so an expired one is
/// replaced without restarting the client. In the browser, which sends the
/// page's cookies itself and no header, it is asked before each attempt (to
/// refresh the cookie, say) and what it returns is not sent.
#[derive(Clone)]
#[cfg(not(target_arch = "wasm32"))]
pub struct TokenProvider(Arc<dyn Fn() -> TokenFuture + Send + Sync>);

/// Fetches the token for each connection attempt, so an expired one is
/// replaced without restarting the client. In the browser, which sends the
/// page's cookies itself and no header, it is asked before each attempt (to
/// refresh the cookie, say) and what it returns is not sent.
#[derive(Clone)]
#[cfg(target_arch = "wasm32")]
pub struct TokenProvider(std::rc::Rc<dyn Fn() -> TokenFuture>);

#[cfg(not(target_arch = "wasm32"))]
impl TokenProvider {
    pub fn new<F, Fut, E>(provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, E>> + Send + 'static,
        E: fmt::Display,
    {
        Self(Arc::new(move || {
            let token = provider();
            Box::pin(async move { token.await.map_err(|e| e.to_string()) })
        }))
    }
}

#[cfg(target_arch = "wasm32")]
impl TokenProvider {
    pub fn new<F, Fut, E>(provider: F) -> Self
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<String, E>> + 'static,
        E: fmt::Display,
    {
        Self(std::rc::Rc::new(move || {
            let token = provider();
            Box::pin(async move { token.await.map_err(|e| e.to_string()) })
        }))
    }
}

impl TokenProvider {
    pub(crate) async fn token(&self) -> Result<String, String> {
        (self.0)().await
    }
}

impl fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenProvider")
    }
}

/// What the application committed it applied: asked with the user of a
/// connection about to subscribe, each topic's position in each of its
/// streams. A topic left out starts with none; None leaves delivery where it
/// is.
pub(crate) type Committed = std::sync::Arc<
    dyn Fn(
            Option<&str>,
        ) -> Option<std::collections::BTreeMap<String, Vec<crate::message::Position>>>
        + Send
        + Sync,
>;

/// What a client connects to and how it behaves. The defaults are those of
/// the Python and TypeScript clients. Its `Debug` print leaves the token,
/// the headers' values and the signing key out.
#[derive(Clone)]
#[non_exhaustive]
pub struct Config {
    /// `ws://host:port/wse` or `wss://...`.
    pub url: String,
    /// The JWT, sent in the `access_token` cookie and as
    /// `Authorization: Bearer`. Not in the browser: it sends the page's
    /// cookies itself, and no header.
    pub token: Option<String>,
    /// Asked for the token before each connection attempt (within
    /// `connect_timeout`); takes the place of `token`. A refused token is
    /// then fetched again instead of ending the client, three times at most
    /// between two connections the server took.
    pub token_provider: Option<TokenProvider>,
    /// The cookie the token goes in: the server's `jwt_cookie_name`.
    pub cookie_name: String,
    /// Topics to subscribe to once connected, and again after every
    /// reconnect from where the client left off.
    pub topics: Vec<String>,
    /// How the server sends this client its messages.
    pub format: Format,
    /// Let the server compress large messages (zlib, above 1 KiB).
    pub compression: bool,
    /// The largest message taken, also after inflating (16 MiB).
    pub max_message_size: usize,
    /// For the socket, TLS and the upgrade (10 s).
    pub connect_timeout: Duration,
    /// For the server to confirm the connection after the upgrade (10 s).
    pub ready_timeout: Duration,
    /// Between the client's heartbeats (15 s).
    pub heartbeat: Duration,
    /// Silence from the server after which the connection is taken for
    /// dead and made again (40 s; the server pings every 25 s).
    pub idle_timeout: Duration,
    /// How long a topic waits for the server to replay messages it missed
    /// before reporting the gap (3 s); zero reports it at once.
    pub recovery_timeout: Duration,
    pub reconnect: Reconnect,
    /// More headers for the upgrade request (not in the browser, which
    /// sends none).
    pub headers: Vec<(String, String)>,
    /// Messages waiting to be sent at most; past it `try_send` fails and
    /// `send` waits (1024). What a lost connection did not write goes out
    /// on the next one, once the server takes it. Sends that wait for their
    /// topics ([`Client::send_requiring`](crate::Client::send_requiring))
    /// are held besides, as many at most: while that many wait, so does the
    /// queue.
    pub send_queue: usize,
    /// The TLS configuration for `wss://`: an `Arc` of a
    /// `rustls::ClientConfig` of rustls 0.23, which this crate builds on,
    /// held as `Any` so that rustls's own versions are not this crate's.
    /// By default the Mozilla root certificates on aws-lc-rs. In the browser
    /// TLS is the browser's.
    ///
    /// ```ignore
    /// config.tls = Some(std::sync::Arc::new(my_rustls_client_config));
    /// ```
    #[cfg(all(feature = "aws-lc", not(target_arch = "wasm32")))]
    pub tls: Option<Arc<dyn std::any::Any + Send + Sync>>,
    /// End-to-end encryption; none by default.
    #[cfg(feature = "aws-lc")]
    pub encryption: Option<Encryption>,
    /// The recover-or-resync delivery: a topic that is not continuous holds
    /// its messages until the application brought its state up to date, and
    /// recovery starts from what it committed. None: best effort, where the
    /// topics' states and the checkpoint are kept all the same.
    pub delivery: Option<crate::Delivery>,
    /// The outbox: every message sent, except a live or signed one, waits
    /// in it until the server answers for it, and goes again after a
    /// reconnect; a server that answers (3.2) takes each once. In memory by
    /// default; [`Offline::in_file`](crate::Offline::in_file) keeps it past
    /// the application's end. None: no outbox, and a send with no
    /// connection fails with [`Error::NotConnected`](crate::Error::NotConnected).
    pub offline: Option<crate::Offline>,
    /// Where each recovery starts: what the application committed, as the
    /// delivery keeps it.
    pub(crate) committed: Option<Committed>,
    /// This client's ECDSA P-256 private key (PKCS#8, PEM or DER) for
    /// [`Client::send_signed`](crate::Client::send_signed). The server
    /// learns its public key from the token's `cnf` claim or from its
    /// application.
    #[cfg(feature = "aws-lc")]
    pub signing_key: Option<Vec<u8>>,
    /// The server's signing public keys (base64 of the uncompressed point,
    /// from the server's `signing_public_key`) that the messages it signs
    /// are checked against; one failing the check is dropped. Without them
    /// signed messages are delivered unchecked.
    #[cfg(feature = "aws-lc")]
    pub server_signing_keys: Vec<String>,
}

/// Messages waiting to be sent at most.
const MAX_SEND_QUEUE: usize = 1 << 20;
/// The longest wait a client is configured with.
const MAX_WAIT: Duration = Duration::from_secs(7 * 86_400);

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = |set: bool| if set { "Some(<redacted>)" } else { "None" };
        let headers: Vec<(&str, &str)> = self
            .headers
            .iter()
            .map(|(name, _)| (name.as_str(), "<redacted>"))
            .collect();
        let mut f = f.debug_struct("Config");
        f.field("url", &self.url)
            .field("token", &format_args!("{}", redacted(self.token.is_some())))
            .field("token_provider", &self.token_provider)
            .field("cookie_name", &self.cookie_name)
            .field("topics", &self.topics)
            .field("format", &self.format)
            .field("compression", &self.compression)
            .field("max_message_size", &self.max_message_size)
            .field("connect_timeout", &self.connect_timeout)
            .field("ready_timeout", &self.ready_timeout)
            .field("heartbeat", &self.heartbeat)
            .field("idle_timeout", &self.idle_timeout)
            .field("recovery_timeout", &self.recovery_timeout)
            .field("reconnect", &self.reconnect)
            .field("headers", &headers)
            .field("send_queue", &self.send_queue)
            .field("delivery", &self.delivery)
            .field("offline", &self.offline);
        #[cfg(all(feature = "aws-lc", not(target_arch = "wasm32")))]
        f.field(
            "tls",
            &format_args!(
                "{}",
                if self.tls.is_some() {
                    "Some(..)"
                } else {
                    "None"
                }
            ),
        );
        #[cfg(feature = "aws-lc")]
        f.field("encryption", &self.encryption)
            .field(
                "signing_key",
                &format_args!("{}", redacted(self.signing_key.is_some())),
            )
            .field("server_signing_keys", &self.server_signing_keys);
        f.finish()
    }
}

impl Config {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token: None,
            token_provider: None,
            cookie_name: "access_token".to_owned(),
            topics: Vec::new(),
            format: Format::Json,
            compression: true,
            max_message_size: 16 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            ready_timeout: Duration::from_secs(10),
            heartbeat: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(40),
            recovery_timeout: Duration::from_secs(3),
            reconnect: Reconnect::default(),
            headers: Vec::new(),
            send_queue: 1024,
            #[cfg(all(feature = "aws-lc", not(target_arch = "wasm32")))]
            tls: None,
            #[cfg(feature = "aws-lc")]
            encryption: None,
            delivery: None,
            offline: Some(crate::Offline::in_memory()),
            committed: None,
            #[cfg(feature = "aws-lc")]
            signing_key: None,
            #[cfg(feature = "aws-lc")]
            server_signing_keys: Vec::new(),
        }
    }

    /// Checks what can be checked before connecting.
    pub(crate) fn check(&self) -> Result<(), Error> {
        let scheme = self.url.split_once("://").map(|(scheme, _)| scheme);
        match scheme {
            Some("ws") => {}
            Some("wss") if cfg!(any(feature = "aws-lc", target_arch = "wasm32")) => {}
            Some("wss") => {
                return Err(Error::Config(
                    "wss:// needs the aws-lc feature (TLS)".to_owned(),
                ));
            }
            _ => {
                return Err(Error::Config(format!(
                    "{} is not a ws:// or wss:// URL",
                    self.url
                )));
            }
        }
        if self.heartbeat.is_zero() || self.idle_timeout <= self.heartbeat {
            return Err(Error::Config(
                "heartbeat must be above zero and below idle_timeout".to_owned(),
            ));
        }
        if !(1..=MAX_SEND_QUEUE).contains(&self.send_queue) {
            return Err(Error::Config(format!(
                "send_queue must be 1 to {MAX_SEND_QUEUE}"
            )));
        }
        if self.max_message_size == 0 {
            return Err(Error::Config(
                "max_message_size must be above zero".to_owned(),
            ));
        }
        // A week at most: further out, the timers' instants overflow, and the
        // browser's cannot count past 24.8 days.
        for (name, wait) in [
            ("connect_timeout", self.connect_timeout),
            ("ready_timeout", self.ready_timeout),
            ("heartbeat", self.heartbeat),
            ("idle_timeout", self.idle_timeout),
            ("recovery_timeout", self.recovery_timeout),
        ] {
            if wait > MAX_WAIT {
                return Err(Error::Config(format!("{name} must be at most a week")));
            }
        }
        #[cfg(all(feature = "aws-lc", not(target_arch = "wasm32")))]
        if self
            .tls
            .as_ref()
            .is_some_and(|tls| !tls.is::<rustls::ClientConfig>())
        {
            return Err(Error::Config(
                "tls must be an Arc of a rustls::ClientConfig (rustls 0.23)".to_owned(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        self.request(None, false)?;
        Ok(())
    }

    /// The URL, with what the client asks of the server in the query.
    pub(crate) fn url(&self, e2e: bool) -> String {
        let mut url = self.url.clone();
        let mut param = |kv: &str| {
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str(kv);
        };
        param(concat!("client_version=", env!("CARGO_PKG_VERSION")));
        param("protocol_version=1");
        if self.format == Format::Msgpack {
            param("format=msgpack");
        }
        if !self.compression {
            param("compression=false");
        }
        if e2e {
            param("e2e=1");
        }
        url
    }

    /// The upgrade request, with the token and what the client asks of the
    /// server in the query.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn request(&self, token: Option<&str>, e2e: bool) -> Result<Request, Error> {
        let mut request = self
            .url(e2e)
            .into_client_request()
            .map_err(|e| Error::Config(format!("{}: {e}", self.url)))?;
        let headers = request.headers_mut();
        for (name, value) in &self.headers {
            let name = name
                .parse::<tokio_tungstenite::tungstenite::http::HeaderName>()
                .map_err(|e| Error::Config(format!("header {name}: {e}")))?;
            let value = HeaderValue::from_str(value)
                .map_err(|e| Error::Config(format!("header {name}: {e}")))?;
            headers.append(name, value);
        }
        if let Some(token) = token.filter(|t| !t.is_empty()) {
            let cookie = match headers.get("cookie").and_then(|v| v.to_str().ok()) {
                Some(cookies) => format!("{cookies}; {}={token}", self.cookie_name),
                None => format!("{}={token}", self.cookie_name),
            };
            let cookie = HeaderValue::from_str(&cookie)
                .map_err(|_| Error::Config("the token cannot go in a cookie".to_owned()))?;
            headers.insert("cookie", cookie);
            if !headers.contains_key("authorization") {
                let bearer = HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| Error::Config("the token cannot go in a header".to_owned()))?;
                headers.insert("authorization", bearer);
            }
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn the_request_carries_the_token_and_the_wishes() {
        let mut config = Config::new("ws://127.0.0.1:5006/wse?room=1");
        config.format = Format::Msgpack;
        config.compression = false;
        config.headers = vec![("Cookie".into(), "theme=dark".into())];
        let request = config.request(Some("jwt"), true).unwrap();
        let query = request.uri().query().unwrap();
        assert!(query.starts_with("room=1&client_version="), "{query}");
        assert!(
            query.contains("&format=msgpack&compression=false&e2e=1"),
            "{query}"
        );
        assert_eq!(request.headers()["cookie"], "theme=dark; access_token=jwt");
        assert_eq!(request.headers()["authorization"], "Bearer jwt");
    }

    #[test]
    fn a_configuration_that_cannot_work_is_refused() {
        assert!(Config::new("http://example.com").check().is_err());
        assert!(Config::new("example.com").check().is_err());
        let mut config = Config::new("ws://example.com/wse");
        config.idle_timeout = config.heartbeat;
        assert!(config.check().is_err());
        assert!(Config::new("ws://example.com/wse").check().is_ok());
    }

    #[test]
    fn waits_grow_to_the_cap() {
        let mut reconnect = Reconnect {
            jitter: false,
            ..Reconnect::default()
        };
        assert_eq!(reconnect.delay(0), Duration::from_secs(1));
        assert_eq!(reconnect.delay(1), Duration::from_millis(1500));
        assert_eq!(reconnect.delay(50), Duration::from_secs(30));
        assert_eq!(reconnect.delay(u32::MAX), Duration::from_secs(30));
        reconnect.jitter = true;
        let d = reconnect.delay(0).as_secs_f64();
        assert!((0.9..=1.1).contains(&d), "{d}");
    }
}
