//! The application's side of the server: what it hears of its clients.

use std::sync::Arc;

/// A connection, as the application first hears of it. It may subscribe the
/// connection right away (`subscribe`, `set_topic_acl`).
pub struct Connect<'a> {
    /// The server's id for the connection.
    pub conn_id: &'a Arc<String>,
    /// The user the server validated from the client's token. None without
    /// a JWT configuration: the application checks `cookies` itself, then
    /// accepts the connection.
    pub user_id: Option<&'a str>,
    /// The cookies of the upgrade request.
    pub cookies: &'a str,
    pub(crate) state: &'a super::state::SharedState,
    pub(crate) handle: &'a super::state::ConnectionHandle,
}

/// A client's message, as the server read it.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Inbound {
    /// JSON text, or the message of a msgpack client, decoded; `type` and
    /// `payload` read as `t` and `p`.
    Json(serde_json::Value),
    /// Text that is not JSON.
    Text(String),
    /// Binary from a client that does not speak msgpack.
    Binary(Vec<u8>),
}

/// What the server tells the application. Each connection's calls come in
/// order on the connection's own task: connect, its messages, disconnect.
/// They run on the runtime's worker threads and should return fast; a
/// handler hands long work to a task of its own.
///
/// A call that panics closes its connection (1011) and nothing else: the
/// server cleans the connection up, and `disconnect` still comes for one
/// that `connect` took.
pub trait Handler: Send + Sync + 'static {
    /// What the handler keeps for one connection, from `connect` to
    /// `disconnect`.
    type Connection: Send + 'static;

    /// The application learns of a connection. None when it has no room for
    /// it now: the client is told to come back later (1013).
    fn connect(&self, connect: &Connect<'_>) -> Option<Self::Connection>;

    /// A client's message; `size` is its size on the wire. False when there
    /// is no room for it: it is dropped, and one of a 3.2 client's outbox is
    /// refused (`busy`), which its client hears.
    fn message(&self, conn: &mut Self::Connection, message: Inbound, size: usize) -> bool;

    /// A signed message that passed every check: its body, as the client
    /// signed it. False when there is no room for it: the client hears
    /// `busy` and may send it again with the same nonce.
    fn signed(&self, conn: &mut Self::Connection, message: serde_json::Value, size: usize) -> bool;

    /// The connection ended. Only a connection `connect` took gets here.
    fn disconnect(&self, conn: Self::Connection);

    /// A user's first connection joined a topic with presence, or its last
    /// one left. Called on whichever thread made the change: a connection's
    /// own task (from inside `connect` when it subscribes with presence, and
    /// from its cleanup), the caller of `Publisher::subscribe`,
    /// `unsubscribe` or `update_presence`, the presence sweep, or a cluster
    /// link. A lock held across those calls must not be taken here too.
    fn presence(&self, event: Presence) {
        let _ = event;
    }
}

/// A handler call that panicked: caught, and its connection closed.
pub(crate) struct Panicked;

/// Run a call into the application's handler; a panic in it is caught (the
/// panic hook has reported it), so the server goes on and cleans up after it.
pub(crate) fn guarded<R>(f: impl FnOnce() -> R) -> Result<R, Panicked> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).map_err(|_| Panicked)
}

/// A user's presence on a topic changed.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Presence {
    Join {
        topic: String,
        user_id: String,
        data: serde_json::Value,
    },
    Leave {
        topic: String,
        user_id: String,
        data: serde_json::Value,
    },
}
