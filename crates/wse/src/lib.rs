//! WSE: a WebSocket engine for applications that publish to many clients at
//! once. Fan-out on every CPU core, topics with recovery for clients that come
//! back, presence, a native cluster protocol, JWT, end-to-end encryption and
//! signed messages.
//!
//! A server is a `Config`, a `Handler` (what the application hears of its
//! clients) and a `Publisher` (what it sends them). The Python package
//! `wse-server` runs this same engine.

mod cluster;
mod crypto;
mod jwt;
mod msgpack;
mod presence;
mod recovery;
mod reliability;
mod server;

pub use server::{
    ClusterPeer, ClusterSettings, ClusterTls, Config, ConfigError, Connect, DeadLetter, Error,
    Handler, Health, Inbound, JwtKeys, JwtSettings, Member, Presence, PresenceSettings, Publisher,
    Recovered, RecoverySettings, Server, Subscribe,
};

// The Python module and its helpers: built into the `wse-server` package.
#[cfg(feature = "python")]
mod compression;
#[cfg(feature = "python")]
mod filters;
#[cfg(feature = "python")]
mod jwt_py;
#[cfg(feature = "python")]
mod pylock;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "python")]
mod queue;
#[cfg(feature = "python")]
mod rate_limiter;
#[cfg(feature = "python")]
mod security;
#[cfg(feature = "python")]
mod sequencer;
#[cfg(feature = "python")]
mod transformer;
