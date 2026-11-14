#![doc = include_str!("../README.md")]

mod client;
mod config;
mod connection;
mod decode;
mod error;
mod events;
mod handler;
mod message;
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
    Connected, Disconnected, Event, Gap, GapReason, Handler, Recovery, ServerError, Subscribed,
};
pub use message::{Message, OwnedMessage, Position, Signature};
#[cfg(feature = "aws-lc")]
pub use security::with_crypto::{generate_key, public_key, public_key_jwk};
