// The README shows encryption and signing: the crate documentation with them.
#![cfg_attr(feature = "aws-lc", doc = include_str!("../README.md"))]

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
    Connected, Disconnected, Event, Gap, GapReason, Handler, MaybeSend, Recovery, ServerError,
    Subscribed,
};
pub use message::{Message, OwnedMessage, Position, Signature};
#[cfg(feature = "aws-lc")]
pub use security::with_crypto::{generate_key, public_key, public_key_jwk};
