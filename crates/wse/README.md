# wse

A WebSocket engine for reactive user interfaces, run inside your Rust
application: topics with recovery for clients that come back, presence,
servers that form a cluster without a broker, JWT validation, end-to-end
encryption and signed messages, delivered on every CPU core. The same
engine runs the Python package
[`wse-server`](https://pypi.org/project/wse-server/); the clients are
[`wse-client`](https://crates.io/crates/wse-client) for Rust,
[`wse-client`](https://www.npmjs.com/package/wse-client) for TypeScript and
React, and [`wse-client`](https://pypi.org/project/wse-client/) for Python.

```toml
[dependencies]
wse = "3"
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "signal"] }
```

## A server

A server is a `Config`, a `Handler` (what the application hears of its
clients) and a `Publisher` (what it sends them). It runs on the
application's own tokio runtime.

```rust,no_run
use wse::{Config, Connect, Handler, Inbound, JwtSettings, Server, Subscribe};

/// Every connection joins its user's topic.
struct App;

impl Handler for App {
    type Connection = String;

    fn connect(&self, connect: &Connect<'_>) -> Option<String> {
        let user = connect.user_id?.to_owned();
        connect.subscribe(vec![format!("user:{user}")], &Subscribe::default()).ok()?;
        Some(user)
    }

    fn message(&self, user: &mut String, message: Inbound, _size: usize) -> bool {
        if let Inbound::Json(message) = message {
            println!("{user}: {message}");
        }
        true
    }

    fn signed(&self, _user: &mut String, _message: serde_json::Value, _size: usize) -> bool {
        false
    }

    fn disconnect(&self, _user: String) {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    config.jwt = Some(JwtSettings::new(std::env::var("JWT_SECRET")?));
    let server = Server::new(config)?;
    server.start(tokio::net::TcpListener::bind("0.0.0.0:5006").await?, App)?;

    let publisher = server.publisher();
    publisher.publish("user:42", r#"{"t":"order_filled","p":{"id":7}}"#)?;
    publisher.send_to_user("42", r#"{"t":"notice","p":{"text":"hello"}}"#)?;

    tokio::signal::ctrl_c().await?;
    server.stop().await;
    Ok(())
}
```

`examples/chat.rs` is a complete chat room; it prints a token to try it
with, and the `wse-client` crate's `chat_client` example talks to it.

## What a server does

- **Fan-out on every core.** Each connection belongs to a delivery lane, one
  per core; a broadcast to 10,000 connections is delivered by every core at
  once. A publisher faster than its clients waits for room
  (`Publisher::ready`) instead of filling memory: past 65,536 queued sends,
  a send fails with `Error::Full`.
- **Topics.** Exact names and glob patterns, queue groups (one member of a
  group gets each message), per-connection ACLs, from the JWT `wse_topics`
  claim or set by the application.
- **Recovery.** Topics keep their last messages; a client that comes back
  with its position gets what it missed, or learns that it must reload.
- **Presence.** Who is on a topic, with their data, across the cluster.
- **Users.** `send_to_user` reaches every tab and device of a user,
  `disconnect_user` closes them all.
- **Cluster.** Nodes link over a binary protocol in TLS, from the cluster's
  secret (`ClusterSettings::secret`) or from certificates, and
  forward each topic only where it has subscribers. With recovery on, a
  link that drops and comes back loses nothing: each node gets what it
  missed from the other, in order, before anything new.
- **Security.** JWT validation in the handshake (HS*, RS*, PS*, ES*, EdDSA,
  key rotation), origin checks, rate limits, end-to-end encryption with the
  server's identity key, signed messages both ways. A token that names the
  client's key (`cnf`) opens a session only for the client that proves the
  key (`Config::bind_sessions`; `Publisher::accept_bound` when the
  application authenticates connections itself).
- **Operations.** Graceful drain, health snapshots and Prometheus metrics.

## Features

- `server` (default): the server. Without it the crate is its `protocol`
  module alone: the crypto, end-to-end sessions and signed messages that
  the server and `wse-client` share, which is how the client depends on it,
  in the browser too.
- `fips`: aws-lc-rs built from its FIPS 140-3 validated module (needs CMake
  and Go). The crypto is aws-lc-rs otherwise, for TLS, JWT, end-to-end
  encryption and signing.

The protocol, the close codes and the cluster wire format are documented in
the [repository](https://github.com/silvermpx/wse/tree/main/docs).

## License

MIT
