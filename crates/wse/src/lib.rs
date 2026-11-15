#![cfg_attr(feature = "server", doc = include_str!("../README.md"))]

// The server runs on an operating system; in the browser the client does.
#[cfg(all(feature = "server", target_arch = "wasm32"))]
compile_error!(
    "the WSE server does not build for the browser: depend on wse-client there \
     (or on wse with default-features = false, for the protocol alone)"
);

pub mod protocol;

#[cfg(feature = "server")]
mod cluster;
#[cfg(feature = "server")]
mod crypto;
#[cfg(feature = "server")]
mod jwt;
#[cfg(feature = "server")]
mod msgpack;
#[cfg(feature = "server")]
mod presence;
#[cfg(feature = "server")]
mod recovery;
#[cfg(feature = "server")]
mod reliability;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "server")]
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
// Public for crates/wse-py only: no stable API.
#[cfg(feature = "python")]
#[doc(hidden)]
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
