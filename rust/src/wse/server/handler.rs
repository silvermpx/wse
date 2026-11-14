//! The application's side of the server: what it hears of its clients.

use std::sync::Arc;

/// A connection, as the application first hears of it.
pub struct Connect<'a> {
    /// The server's id for the connection.
    pub conn_id: &'a Arc<String>,
    /// The user the server validated from the client's token. None without
    /// a JWT configuration: the application checks `cookies` itself, then
    /// accepts the connection.
    pub user_id: Option<&'a str>,
    /// The cookies of the upgrade request.
    pub cookies: &'a str,
}

/// A client's message, as the server read it.
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
/// They should return fast; a handler hands long work to a task of its own.
pub trait Handler: Send + Sync + 'static {
    /// What the handler keeps for one connection, from `connect` to
    /// `disconnect`.
    type Connection: Send + 'static;

    /// The application learns of a connection. None when it has no room for
    /// it now: the client is told to come back later (1013).
    fn connect(&self, connect: &Connect<'_>) -> Option<Self::Connection>;

    /// A client's message; `size` is its size on the wire. False when there
    /// is no room for it: it is dropped.
    fn message(&self, conn: &mut Self::Connection, message: Inbound, size: usize) -> bool;

    /// A signed message that passed every check: its body, as the client
    /// signed it. False when there is no room for it: the client hears
    /// `busy` and may send it again with the same nonce.
    fn signed(&self, conn: &mut Self::Connection, message: serde_json::Value, size: usize) -> bool;

    /// The connection ended. Only a connection `connect` took gets here.
    fn disconnect(&self, conn: Self::Connection);
}
