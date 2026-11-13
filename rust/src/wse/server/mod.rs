// =============================================================================
// WSE Rust WebSocket server: tokio + tungstenite + PyO3.
//
// Key optimizations:
//   - send_event(): full outbound pipeline in Rust (envelope+serialize+compress+send)
//   - Drain mode: Python polls Rust for batched inbound events (1 GIL per batch)
//   - Ping/pong handled entirely in Rust (zero Python round-trips)
//   - Write coalescing: feed() + batch try_recv() + single flush()
//   - TCP_NODELAY on accept
// =============================================================================

//! The WebSocket server.
//!
//! # Module map
//!
//! Engine, plain Rust (the part a native `wse-rs` library would keep):
//!
//! - `frames`: WebSocket frame types, pre-framed encoding, the per-connection
//!   broadcast queue and its vectored writer.
//! - `state`: what every task shares: connection handles, queue groups, inbound
//!   events, server commands and `SharedState`.
//! - `messages`: protocol messages the server builds itself (server_hello,
//!   server_ready, errors, presence events) and the category helpers.
//! - `handshake`: WebSocket upgrade helpers: the auth rejection, the
//!   pre-handshake cap and its guard.
//! - `e2e`: end-to-end encryption (ECDH P-256 key exchange, AES-GCM-256).
//! - `connection`: one client connection, from the upgrade to the cleanup.
//! - `fanout`: topic glob matching, broadcast and topic fan-out, recovery
//!   stamping.
//! - `commands`: the command processor that executes `ServerCommand`s.
//!
//! Python layer, PyO3:
//!
//! - `py`: the `RustWSEServer` class. `py/mod.rs` holds the class, its
//!   constructor parameters and the single `#[pymethods]` block, so every
//!   Python-visible signature, default and docstring is in one place. Apart
//!   from the constructor and the short accessors, each method body is an
//!   `*_impl` helper in a topic file: `py/lifecycle.rs`, `messaging.rs`,
//!   `inbound.rs`, `subscriptions.rs`, `presence.rs`, `cluster.rs` and
//!   `metrics.rs`; `convert.rs` converts between Python objects and JSON,
//!   `callbacks.rs` fires the callback-mode callbacks.
//!
//! The crate-visible items are re-exported below, so `server::X` paths stay as
//! they were. Sibling modules import them through these re-exports and import
//! module-internal helpers (`pub(super)`) from their own module.
//!
//! # Engine vs Python layer
//!
//! PyO3 is still used inside the engine here, to untangle before the engine can
//! stand alone:
//!
//! - `state`: `SharedState::{on_connect, on_message, on_disconnect}` hold the
//!   callback-mode Python callbacks as `Py<PyAny>`.
//! - `connection`: `handle_connection` runs callback mode itself. It clones the
//!   callbacks under `Python::try_attach`, calls `on_message` with
//!   `py::json_to_pyobj` / `PyBytes` arguments through `spawn_bounded_callback`,
//!   and fires `py::fire_on_connect` / `py::fire_on_disconnect`.
//! - Modules outside `server` that the engine calls also define PyO3 functions:
//!   `crate::jwt` (JWT validation, topic ACLs) and `crate::wse::compression`
//!   (msgpack helpers). `crate::wse::cluster` has no PyO3.
//!
//! Engine code that still lives in the Python layer:
//!
//! - `py::lifecycle::start_impl` builds the runtime: the accept loop with
//!   cluster protocol detection, the recovery cleanup, the presence TTL sweep
//!   and the ping / zombie / slow-consumer task, which also fires
//!   `fire_on_disconnect`. `start_impl` and `stop_impl` take the lifecycle
//!   lock through the Python-aware `crate::wse::pylock::lock`.
//! - `py::messaging::send_event_impl` holds the outbound pipeline (dedup, rate
//!   limit, envelope, msgpack / zlib / E2E) next to the `PyDict` conversion.
//! - `py::subscriptions` and `py::presence` hold subscribe / unsubscribe, queue
//!   groups, cluster interest refcounts, recovery replay and presence tracking.
//! - `RustWSEServer::build` (`py/mod.rs`) validates the JWT keys and builds the
//!   `SharedState` configuration, reporting errors as Python exceptions.

mod commands;
mod connection;
mod e2e;
mod fanout;
mod frames;
mod handshake;
mod messages;
mod py;
mod state;

pub use py::RustWSEServer;

pub(crate) use e2e::E2E_KEY_INFO;
pub(crate) use fanout::{fanout_topic_direct, glob_match, stamp_recovery_fields};
pub(crate) use frames::{
    BroadcastQueue, WsFrame, encode_ws_frame, pre_frame_binary, pre_frame_text,
};
pub(crate) use handshake::normalize_origin;
pub(crate) use messages::format_presence_msg;
pub(crate) use state::{
    CLOSE_GOING_AWAY, CLOSE_GRACE, CLOSE_IDLE, CLOSE_NORMAL, CLOSE_RATE_LIMITED,
    CLOSE_TOKEN_EXPIRED, CLOSE_TRY_AGAIN_LATER, Closer, ConnectionHandle, QueueGroup,
    ServerCommand, SharedState, SharedStateConfig,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
