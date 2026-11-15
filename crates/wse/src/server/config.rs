//! A server's configuration, checked as a whole before the server starts.

use std::sync::Arc;

use ahash::AHashSet;
use zeroize::Zeroizing;

use super::state::{SharedState, SharedStateConfig};
use crate::jwt::{self, JwtConfig};

/// What a server is configured with. Start from `Config::default()` and set
/// the fields that differ: the defaults are those of the Python server. Its
/// `Debug` print shows whether a key is set, never the key.
#[derive(Clone)]
#[non_exhaustive]
pub struct Config {
    /// Registered connections at most; more are refused (1013).
    pub max_connections: usize,
    /// The Python package's queue of client messages waiting for the
    /// application: a Rust `Handler` is called directly and never queued.
    #[doc(hidden)]
    pub max_inbound_queue_size: usize,
    /// The same queue's bytes at most.
    #[doc(hidden)]
    pub max_inbound_queue_bytes: usize,
    /// Origins a browser may connect from; None: any.
    pub allowed_origins: Option<Vec<String>>,
    /// Whether the server answers the clients' own subscribe and
    /// unsubscribe requests.
    pub client_subscriptions: bool,
    /// The identity key that signs each E2E session handshake: an ECDSA
    /// P-256 private key in PKCS#8 (PEM or DER). None: no E2E sessions.
    pub e2e_identity_key: Option<Vec<u8>>,
    /// Whether clients may still use the first E2E design.
    pub e2e_legacy: bool,
    /// The server's key for signed messages: an ECDSA P-256 private key in
    /// PKCS#8 (PEM or DER).
    pub signing_key: Option<Vec<u8>>,
    /// How far, in seconds, a signed message's time may be from the server's.
    pub signature_window: u64,
    /// A token that names the client's key (`cnf`) binds the connection to
    /// it (true, the default): the client must open an end-to-end session
    /// and prove the key inside it before it is admitted, so a token copied
    /// on the way opens nothing. False: the key only checks the client's
    /// signed messages, and the token is a bearer token like any other.
    pub bind_sessions: bool,
    /// A client's message is at most this many bytes.
    pub max_message_size: usize,
    /// Seconds between the server's pings: at least 1, below `idle_timeout`.
    pub ping_interval: u64,
    /// A connection silent for this many seconds is closed: at most a week.
    pub idle_timeout: u64,
    /// Bytes waiting to be written to one connection at most; past them it
    /// misses messages, and a connection over them for two pings is closed
    /// (1013). 0: no limit.
    pub max_outbound_queue_bytes: usize,
    /// Topics one connection may subscribe to at most; 0: no limit.
    pub max_subscriptions_per_connection: usize,
    /// A connection's messages: a token bucket of this many.
    pub rate_limit_capacity: f64,
    /// ... refilled at this many per second.
    pub rate_limit_refill: f64,
    /// Tokens the server validates itself; None: the application accepts
    /// each connection (`Publisher::accept`).
    pub jwt: Option<JwtSettings>,
    /// Message history for clients that come back; None: no recovery.
    pub recovery: Option<RecoverySettings>,
    /// Who is on a topic; None: no presence.
    pub presence: Option<PresenceSettings>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_inbound_queue_size: 131_072,
            max_inbound_queue_bytes: 64 * 1024 * 1024,
            allowed_origins: None,
            client_subscriptions: false,
            e2e_identity_key: None,
            e2e_legacy: true,
            signing_key: None,
            signature_window: 30,
            bind_sessions: true,
            max_message_size: 1024 * 1024,
            ping_interval: 25,
            idle_timeout: 60,
            max_outbound_queue_bytes: 16 * 1024 * 1024,
            max_subscriptions_per_connection: 0,
            rate_limit_capacity: 100_000.0,
            rate_limit_refill: 10_000.0,
            jwt: None,
            recovery: None,
            presence: None,
        }
    }
}

/// Tokens the server validates itself, from the cookie or the
/// Authorization header. Its `Debug` print leaves the keys out.
#[derive(Clone)]
#[non_exhaustive]
pub struct JwtSettings {
    /// The HMAC secret, or the public key (PEM) of the asymmetric algorithms.
    pub secret: Vec<u8>,
    /// The algorithm tokens must use, whatever their own header says:
    /// `HS256` (the default), `HS384`, `HS512`, `RS256`, `RS384`, `RS512`,
    /// `PS256`, `PS384`, `PS512`, `ES256`, `ES384` or `EdDSA`.
    pub algorithm: Option<String>,
    /// The `iss` a token must carry; None: any.
    pub issuer: Option<String>,
    /// The `aud` a token must carry; None: any.
    pub audience: Option<String>,
    /// The cookie the token comes in; `access_token` by default.
    pub cookie_name: Option<String>,
    /// The key before the last rotation, still accepted.
    pub previous_secret: Option<Vec<u8>>,
    /// The `kid` of the tokens the server issues.
    pub key_id: Option<String>,
    /// The private key the server issues tokens with (asymmetric algorithms).
    pub private_key: Option<Vec<u8>>,
}

impl JwtSettings {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            algorithm: None,
            issuer: None,
            audience: None,
            cookie_name: None,
            previous_secret: None,
            key_id: None,
            private_key: None,
        }
    }
}

/// Each topic keeps its last messages for clients that come back.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RecoverySettings {
    /// Messages kept per topic: rounded up to a power of two, from 16 to
    /// 65,536.
    pub buffer_size: usize,
    /// Seconds an idle topic's history lives.
    pub ttl: u64,
    /// Messages one recovery replays at most.
    pub max_messages: usize,
    /// Bytes of history across every topic at most.
    pub memory_budget: usize,
}

impl Default for RecoverySettings {
    fn default() -> Self {
        Self {
            buffer_size: 128,
            ttl: 300,
            max_messages: 500,
            memory_budget: 256 * 1024 * 1024,
        }
    }
}

/// Who is on a topic, for presence.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PresenceSettings {
    /// Bytes of a user's presence data at most.
    pub max_data_size: usize,
    /// Users on one topic at most; 0: no limit.
    pub max_members: usize,
}

impl Default for PresenceSettings {
    fn default() -> Self {
        Self {
            max_data_size: 4096,
            max_members: 0,
        }
    }
}

/// A configuration that cannot run, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(String);

impl ConfigError {
    /// What is wrong with the configuration.
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn invalid(message: impl Into<String>) -> ConfigError {
    ConfigError(message.into())
}

impl Config {
    /// The server's state, every option checked first. The private keys'
    /// bytes are wiped once read.
    pub(crate) fn into_state(mut self) -> Result<SharedState, ConfigError> {
        let state = self.build();
        for key in [&mut self.e2e_identity_key, &mut self.signing_key]
            .into_iter()
            .flatten()
        {
            zeroize::Zeroize::zeroize(key);
        }
        state
    }

    fn build(&mut self) -> Result<SharedState, ConfigError> {
        let at_least_one = [
            ("max_connections", self.max_connections),
            ("max_inbound_queue_size", self.max_inbound_queue_size),
            ("max_inbound_queue_bytes", self.max_inbound_queue_bytes),
            ("max_message_size", self.max_message_size),
        ];
        for (name, value) in at_least_one {
            if value == 0 {
                return Err(invalid(format!("{name} must be at least 1")));
            }
        }
        if self.ping_interval == 0 {
            return Err(invalid("ping_interval must be at least 1 second"));
        }
        // NaN is refused; infinity means no limit.
        if self.rate_limit_capacity.is_nan() || self.rate_limit_capacity < 1.0 {
            return Err(invalid(format!(
                "rate_limit_capacity must be at least 1, got {}",
                self.rate_limit_capacity
            )));
        }
        if self.rate_limit_refill.is_nan() || self.rate_limit_refill <= 0.0 {
            return Err(invalid(format!(
                "rate_limit_refill must be above 0, got {}",
                self.rate_limit_refill
            )));
        }
        let allowed_origins = self
            .allowed_origins
            .take()
            .map(|origins| {
                origins
                    .iter()
                    .map(|origin| super::normalize_origin(origin))
                    .collect::<Result<AHashSet<String>, String>>()
            })
            .transpose()
            .map_err(invalid)?;
        let e2e_identity = self
            .e2e_identity_key
            .as_deref()
            .map(crate::crypto::IdentityKey::from_pkcs8)
            .transpose()
            .map_err(|_| {
                invalid(
                    "e2e_identity_key must be an ECDSA P-256 private key in PKCS#8 (PEM or DER)",
                )
            })?;
        let signer = self
            .signing_key
            .as_deref()
            .map(super::signing::ServerSigner::from_pkcs8)
            .transpose()
            .map_err(|_| {
                invalid("signing_key must be an ECDSA P-256 private key in PKCS#8 (PEM or DER)")
            })?;
        if self.signature_window == 0 {
            return Err(invalid("signature_window must be at least 1 second"));
        }
        // A week at most: further out, the timers' instants overflow.
        const MAX_IDLE_SECS: u64 = 7 * 86_400;
        if self.idle_timeout > MAX_IDLE_SECS {
            return Err(invalid(format!(
                "idle_timeout must be at most {MAX_IDLE_SECS} seconds, got {}",
                self.idle_timeout
            )));
        }
        if self.ping_interval >= self.idle_timeout {
            return Err(invalid(format!(
                "ping_interval ({}s) must be less than idle_timeout ({}s)",
                self.ping_interval, self.idle_timeout
            )));
        }
        let jwt_config = self.jwt.take().map(JwtSettings::into_config).transpose()?;
        let recovery = self.recovery.take().map(|recovery| {
            let clamped = recovery.buffer_size.max(16);
            let bits = clamped.next_power_of_two().trailing_zeros();
            Arc::new(crate::recovery::RecoveryManager::new(
                crate::recovery::RecoveryConfig {
                    buffer_size_bits: bits,
                    history_ttl_secs: recovery.ttl,
                    max_recovery_messages: recovery.max_messages,
                    global_memory_budget: recovery.memory_budget,
                },
            ))
        });
        let presence = self.presence.as_ref();
        Ok(SharedState::new(SharedStateConfig {
            max_connections: self.max_connections,
            jwt_config,
            max_inbound_queue_size: self.max_inbound_queue_size,
            max_inbound_queue_bytes: self.max_inbound_queue_bytes,
            allowed_origins,
            client_subscriptions: self.client_subscriptions,
            e2e_identity,
            e2e_legacy: self.e2e_legacy,
            recovery,
            presence_enabled: presence.is_some(),
            presence_max_data_size: presence.map_or(4096, |p| p.max_data_size),
            presence_max_members: presence.map_or(0, |p| p.max_members),
            rate_capacity: self.rate_limit_capacity,
            rate_refill: self.rate_limit_refill,
            max_message_size: self.max_message_size,
            ping_interval_secs: self.ping_interval,
            idle_timeout_secs: self.idle_timeout,
            max_outbound_queue_bytes: self.max_outbound_queue_bytes,
            max_subscriptions_per_connection: self.max_subscriptions_per_connection,
            signer,
            signature_window_secs: self.signature_window,
            bind_sessions: self.bind_sessions,
        }))
    }
}

impl JwtSettings {
    /// The keys checked against the algorithm now, not at the first
    /// connection.
    fn into_config(self) -> Result<JwtConfig, ConfigError> {
        let algorithm = match self.algorithm.as_deref() {
            None => jwt::JwtAlgorithm::HS256,
            Some(name) => jwt::JwtAlgorithm::from_str(name).map_err(|_| {
                invalid(format!(
                    "Unsupported jwt_algorithm: {name}. Supported: {}",
                    jwt::JwtAlgorithm::supported()
                ))
            })?,
        };
        let key_error =
            |option: &str, e: jwt::JwtError| invalid(format!("{option} for {algorithm:?}: {e}"));
        jwt::check_verification_key(algorithm, &self.secret)
            .map_err(|e| key_error("jwt_secret", e))?;
        if let Some(ref key) = self.previous_secret {
            jwt::check_verification_key(algorithm, key)
                .map_err(|e| key_error("jwt_previous_secret", e))?;
        }
        if let Some(ref key) = self.private_key {
            jwt::check_signing_key(algorithm, key).map_err(|e| key_error("jwt_private_key", e))?;
        }
        Ok(JwtConfig {
            algorithm,
            secret: Zeroizing::new(self.secret),
            previous_secret: self.previous_secret.map(Zeroizing::new),
            signing_key: self.private_key.map(Zeroizing::new),
            key_id: self.key_id,
            issuer: self.issuer.unwrap_or_default(),
            audience: self.audience.unwrap_or_default(),
            cookie_name: self
                .cookie_name
                .unwrap_or_else(|| "access_token".to_string()),
        })
    }
}

/// A secret in a `Debug` print: whether it is there, never what it is.
struct Redacted(bool);

impl std::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0 { "Some(<redacted>)" } else { "None" })
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("max_connections", &self.max_connections)
            .field("max_inbound_queue_size", &self.max_inbound_queue_size)
            .field("max_inbound_queue_bytes", &self.max_inbound_queue_bytes)
            .field("allowed_origins", &self.allowed_origins)
            .field("client_subscriptions", &self.client_subscriptions)
            .field(
                "e2e_identity_key",
                &Redacted(self.e2e_identity_key.is_some()),
            )
            .field("e2e_legacy", &self.e2e_legacy)
            .field("signing_key", &Redacted(self.signing_key.is_some()))
            .field("signature_window", &self.signature_window)
            .field("bind_sessions", &self.bind_sessions)
            .field("max_message_size", &self.max_message_size)
            .field("ping_interval", &self.ping_interval)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_outbound_queue_bytes", &self.max_outbound_queue_bytes)
            .field(
                "max_subscriptions_per_connection",
                &self.max_subscriptions_per_connection,
            )
            .field("rate_limit_capacity", &self.rate_limit_capacity)
            .field("rate_limit_refill", &self.rate_limit_refill)
            .field("jwt", &self.jwt)
            .field("recovery", &self.recovery)
            .field("presence", &self.presence)
            .finish()
    }
}

impl std::fmt::Debug for JwtSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtSettings")
            .field("secret", &Redacted(true))
            .field("algorithm", &self.algorithm)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("cookie_name", &self.cookie_name)
            .field("previous_secret", &Redacted(self.previous_secret.is_some()))
            .field("key_id", &self.key_id)
            .field("private_key", &Redacted(self.private_key.is_some()))
            .finish()
    }
}
