# wse

A WebSocket engine for applications that publish to many clients at once:
fan-out on every CPU core, topics with recovery for clients that come back,
presence, a native cluster protocol, JWT validation, end-to-end encryption
and signed messages. The same engine runs the Python package
[`wse-server`](https://pypi.org/project/wse-server/); the clients are
[`wse-client`](https://crates.io/crates/wse-client) for Rust,
[`wse-client`](https://www.npmjs.com/package/wse-client) for TypeScript and
React, and [`wse-client`](https://pypi.org/project/wse-client/) for Python.

```toml
[dependencies]
wse = "3"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net"] }
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
  once, and a publisher faster than its clients is held back instead of
  filling memory.
- **Topics.** Exact names and glob patterns, queue groups (one member of a
  group gets each message), per-connection ACLs, from the JWT `wse_topics`
  claim or set by the application.
- **Recovery.** Topics keep their last messages; a client that comes back
  with its position gets what it missed, or learns that it must reload.
- **Presence.** Who is on a topic, with their data, across the cluster.
- **Users.** `send_to_user` reaches every tab and device of a user,
  `disconnect_user` closes them all.
- **Cluster.** Nodes link over a binary protocol (optionally mutual TLS) and
  forward each topic only where it has subscribers.
- **Security.** JWT validation in the handshake (HS*, RS*, PS*, ES*, EdDSA,
  key rotation), origin checks, rate limits, end-to-end encryption with the
  server's identity key, signed messages both ways.
- **Operations.** Graceful drain, health snapshots and Prometheus metrics.

## Features

- `aws-lc` (default): aws-lc-rs for TLS, JWT and the E2E and signing crypto.
- `fips`: aws-lc-rs built from its FIPS 140-3 validated module (needs CMake
  and Go).

The protocol, the close codes and the cluster wire format are documented in
the [repository](https://github.com/silvermpx/wse/tree/main/docs).

## License

MIT
