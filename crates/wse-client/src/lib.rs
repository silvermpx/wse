// The README shows encryption and signing: the crate documentation with them.
#![cfg_attr(feature = "aws-lc", doc = include_str!("../README.md"))]

// In the browser the crypto is RustCrypto: there is no FIPS module there.
#[cfg(all(feature = "fips", target_arch = "wasm32"))]
compile_error!("the fips feature needs aws-lc-rs, which does not build for the browser");
// WebAssembly outside the browser has no WebSocket of the page to drive.
#[cfg(all(target_arch = "wasm32", not(target_os = "unknown")))]
compile_error!("wse-client runs natively, or in the browser (wasm32-unknown-unknown)");

#[cfg(target_arch = "wasm32")]
mod browser;
mod client;
mod config;
#[cfg(not(target_arch = "wasm32"))]
mod connection;
mod decode;
mod error;
mod events;
mod frames;
mod handler;
mod message;
/// The client as mobile bindings (Swift, Kotlin, Dart) need it.
#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub mod native;
mod policy;
mod scan;
mod security;
mod sequencer;
mod session;

pub use client::{Client, Status};
#[cfg(feature = "aws-lc")]
pub use config::Encryption;
pub use config::{Config, Format, Reconnect, TokenProvider};
pub use error::Error;
pub use events::{Events, Incoming, connect};
pub use handler::{
    Connected, Disconnected, Event, Gap, GapReason, Handler, MaybeSend, Paused, Recovery,
    ServerError, Subscribed,
};
pub use message::{Message, OwnedMessage, Position, Signature};
#[cfg(feature = "aws-lc")]
pub use security::with_crypto::{generate_key, public_key, public_key_jwk};
