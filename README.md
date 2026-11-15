# WSE

[![PyPI - Server](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![PyPI - Client](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![crates.io - Server](https://img.shields.io/crates/v/wse?label=crates.io%20wse)](https://crates.io/crates/wse)
[![crates.io - Client](https://img.shields.io/crates/v/wse-client?label=crates.io%20wse-client)](https://crates.io/crates/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/silvermpx/wse/blob/main/LICENSE)

WSE is a WebSocket server for reactive user interfaces: trading terminals, dashboards, monitoring, chats, notifications. Your backend publishes events to topics, and WSE pushes each event to every client subscribed to it, so the screen changes the moment the data does, without polling.

WSE runs inside your backend rather than as a separate service. A Python application starts it with the `wse-server` package; a Rust application embeds the `wse` crate. Both are the same engine, written in Rust. Several servers form a cluster by themselves, with no Redis or other broker in between.

In the browser, a React hook keeps one connection per page, with its subscriptions and recovery; interfaces written in Rust (Leptos, Yew, Dioxus) use the Rust client compiled to WebAssembly. Services and bots connect with the Python or the Rust client.

## What it does

- **Topics.** The backend publishes to topics such as `prices:BTC`, `orders:user-42` or `chat:room-7`, and clients subscribe by name or by pattern (`prices:*`). A message can also go to a single connection, or to one worker of a queue group. Each client chooses JSON or MessagePack.
- **Nothing lost on reconnect.** Topics keep their latest messages, and every message carries its position. A client that loses its connection comes back with that position and receives what it missed, in order and without duplicates; if the gap is too old, it is told so and reloads. The clients track positions, drop duplicates and ask for what is missing by themselves.
- **Clients that look after themselves.** They reconnect with backoff, get a fresh token, and report anything they could not recover. The TypeScript and Python clients can also keep what the user sends while offline and deliver it once the connection is back.
- **Presence.** Who is on a topic right now, with each user's status or other data, and an event when someone joins or leaves. Several tabs of one user count as one.
- **Several instances.** In production a backend usually runs as several copies behind a load balancer, each with its own WSE server. The servers link to each other directly: an event published on one copy reaches the subscribers connected to any other, presence covers all of them, and one copy can be drained for a deploy while its clients move to the others. No Redis or other broker sits in between, and each topic goes only to the servers that have subscribers for it.
- **Access.** Users sign in with a JWT checked during the WebSocket handshake: from a cookie in browsers, from the `Authorization` header elsewhere. Each connection can be limited to its own topics, by its token or by the application. The server can refuse pages from other sites, and it limits how much and how fast each client may send.
- **End-to-end encryption.** Clients pin the server's public key and every frame is sealed, so a proxy or load balancer that terminates TLS can neither read nor change the traffic. A token that names the client's key opens a connection only for the client that holds the key, so a token seen on the way is of no use.
- **Signed messages.** Orders, fills and other messages that may be disputed later carry their sender's signature, which can still be checked long after they arrived. The server confirms each one with a signed receipt.
- **Speed.** The engine delivers on every CPU core: on 12 cores of an Intel Xeon it sends 7.4 to 9.4 million messages a second to 1,000-10,000 subscribers. A publisher faster than its clients is slowed down instead of filling memory.
- **Operations.** Graceful drain for rolling deploys, health snapshots, Prometheus metrics, limits on connections, message sizes, send rates and memory, and guides for Docker and Kubernetes.

## Packages

| Package | Install | Use it for |
|---------|---------|------------|
| [`wse-server`](https://pypi.org/project/wse-server/) | `pip install wse-server` | The server, run from Python |
| [`wse`](https://crates.io/crates/wse) | `cargo add wse` | The server, inside a Rust application (tokio) |
| [`wse-client`](https://www.npmjs.com/package/wse-client) | `npm install wse-client` | Web interfaces: React and TypeScript |
| [`wse-client`](https://crates.io/crates/wse-client) | `cargo add wse-client` | Interfaces in Rust (WebAssembly), and Rust services on tokio |
| [`wse-client`](https://pypi.org/project/wse-client/) | `pip install wse-client` | Python services and scripts, with or without asyncio |

## A server in Python

```bash
pip install wse-server
```

```python
import json
import threading
import time

from wse_server import RustWSEServer

server = RustWSEServer("0.0.0.0", 5007, jwt_secret=b"replace-with-a-strong-secret-key!")
server.enable_drain_mode()
server.start()
print("token:", server.issue_token("user-1"))


def handle_events():
    while server.is_running():
        for kind, conn_id, data in server.drain_inbound(256, 50):
            if kind == "auth_connect":  # data: the user id from the token
                server.subscribe_connection(conn_id, ["prices"])
            elif kind == "msg":  # data: the client's message, a dict
                print(conn_id, data)


threading.Thread(target=handle_events, daemon=True).start()

price = 100.0
while True:
    price += 0.25
    server.broadcast("prices", json.dumps({"t": "price", "p": {"symbol": "ACME", "price": price}}))
    time.sleep(1)
```

Clients connect to `ws://localhost:5007/wse` with the printed token: a browser sends it as the `access_token` cookie, other clients in the `Authorization: Bearer` header. Messages are JSON objects with their type in `t` and their data in `p`. The rest of the Python API is further down, and runnable examples are in [`examples/`](https://github.com/silvermpx/wse/tree/main/examples).

## A server in Rust

```bash
cargo add wse serde_json tokio --features tokio/full
```

```rust
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
    server.start(tokio::net::TcpListener::bind("0.0.0.0:5007").await?, App)?;

    let publisher = server.publisher();
    publisher.publish("user:42", r#"{"t":"order_filled","p":{"id":7}}"#)?;
    publisher.send_to_user("42", r#"{"t":"notice","p":{"text":"hello"}}"#)?;

    tokio::signal::ctrl_c().await?;
    server.stop().await;
    Ok(())
}
```

The server runs on the application's own tokio runtime. The `Handler` hears connections and their messages; the `Publisher` sends to topics, to single connections and to every connection of a user. A complete chat room is in [`crates/wse/examples/chat.rs`](https://github.com/silvermpx/wse/blob/main/crates/wse/examples/chat.rs), and the crate's own [README](https://github.com/silvermpx/wse/blob/main/crates/wse/README.md) has the rest.

## Clients

### TypeScript and React

```bash
npm install wse-client
```

```tsx
import { useMemo, useState } from 'react';
import { useWSE, type UseWSEConfig } from 'wse-client';

export function Price() {
  const [price, setPrice] = useState<number>();
  const config = useMemo<UseWSEConfig>(() => ({
    endpoints: ['wss://example.com/wse'],
    registerHandlers: (processor) => {
      processor.registerHandler('price', (msg) => setPrice(msg.p.price));
    },
  }), []);
  const { connectionHealth } = useWSE('cookie', ['prices'], config);

  return <p>{connectionHealth}: {price ?? '-'}</p>;
}
```

A browser cannot put headers on a WebSocket, so it signs in with the cookie your login endpoint sets (`access_token`); `'cookie'` tells the hook to connect. Every `useWSE()` on a page shares one connection. More in the [TypeScript client's README](https://github.com/silvermpx/wse/blob/main/ts-client/README.md).

### Python

```bash
pip install wse-client
```

```python
import asyncio

from wse_client import connect


async def main():
    async with connect("ws://localhost:5007/wse", token="<token>", topics=["prices"]) as client:
        async for event in client:
            print(event.type, event.payload)


asyncio.run(main())
```

`SyncWSEClient` does the same without asyncio. More in the [Python client's README](https://github.com/silvermpx/wse/blob/main/python-client/README.md).

### Rust

```bash
cargo add wse-client tokio --features tokio/full
```

```rust
#[tokio::main]
async fn main() -> Result<(), wse_client::Error> {
    let mut config = wse_client::Config::new("ws://localhost:5007/wse");
    config.token = Some("<token>".into());
    config.topics = vec!["prices".into()];
    let (_client, mut events) = wse_client::connect(config).await?;
    while let Some(incoming) = events.next().await {
        println!("{incoming:?}");
    }
    Ok(())
}
```

The same crate builds for the browser (`wasm32-unknown-unknown`), for Leptos, Yew or Dioxus apps, with the same API. A `Handler` can read each message straight from its frame into your own types, without copying it. More in the [Rust client's README](https://github.com/silvermpx/wse/blob/main/crates/wse-client/README.md).

## The Python server

Everything here is a method or an option of `RustWSEServer`. The [integration guide](https://github.com/silvermpx/wse/blob/main/docs/INTEGRATION.md) has the complete reference.

### Options

Sizes are in bytes, times in seconds.

| Option | Default | |
|--------|---------|---|
| `host`, `port` | required | The address to listen on |
| `max_connections` | 1000 | Connections at once |
| `jwt_secret` | `None` | The key that checks tokens: the shared secret for HS256/384/512 (at least 32, 48 or 64 bytes), or the PEM public key for the RSA, ECDSA and EdDSA algorithms. Without it the application checks each connection itself and calls `accept_connection()` |
| `jwt_algorithm` | `"HS256"` | The algorithm tokens must use; a token's own `alg` header never overrides it |
| `jwt_issuer`, `jwt_audience` | `None` | The `iss` and `aud` a token must carry |
| `jwt_key_id` | `None` | The `kid` a token must carry |
| `jwt_cookie_name` | `"access_token"` | The cookie a browser's token is read from |
| `jwt_previous_secret` | `None` | The previous key, still accepted while clients move to the new one |
| `jwt_private_key` | `None` | The PEM private key `issue_token()` signs with, for the RSA, ECDSA and EdDSA algorithms |
| `allowed_origins` | `None` | The origins browsers may connect from (`"https://app.example.com"`); others get HTTP 403 |
| `client_subscriptions` | `False` | Let clients subscribe, unsubscribe and recover by themselves, within their topic ACL. Otherwise their requests reach the application as messages |
| `max_subscriptions_per_connection` | 0 | Topics per connection (0: no limit) |
| `recovery_enabled` | `False` | Keep each topic's recent messages for clients that reconnect |
| `recovery_buffer_size` | 128 | Messages kept per topic, rounded up to a power of two |
| `recovery_ttl` | 300 | How long a quiet topic's messages are kept |
| `recovery_max_messages` | 500 | Messages replayed for one request at most |
| `recovery_memory_budget` | 256 MB | Memory for the kept messages of all topics |
| `presence_enabled` | `False` | Track who is on each topic |
| `presence_max_data_size` | 4096 | A user's presence data at most |
| `presence_max_members` | 0 | Members per topic (0: no limit) |
| `max_message_size` | 1 MB | The largest frame a client may send |
| `rate_limit_capacity` | 100000 | Messages a client may send in a burst (`float("inf")`: no limit) |
| `rate_limit_refill` | 10000 | Messages per second a client may send after the burst |
| `max_outbound_queue_bytes` | 16 MB | Data waiting to be written to one client. Past it the client misses messages, and one over it for two pings is closed (code 1013) |
| `max_inbound_queue_size` | 131072 | Client messages waiting for the application before new ones are dropped. Connect, disconnect and presence events are never dropped |
| `max_inbound_queue_bytes` | 64 MB | The same limit in bytes |
| `ping_interval` | 25 | Time between the server's pings |
| `idle_timeout` | 60 | A client silent this long is closed (code 4408) |
| `e2e_identity_key` | `None` | PEM or DER (PKCS#8) P-256 private key the server proves itself with in end-to-end encryption |
| `e2e_legacy` | `True` | Also serve 2.x clients' encryption, which has no server identity; `False` refuses it (code 4800) |
| `signing_key` | `None` | PEM or DER (PKCS#8) P-256 private key for signed messages |
| `signature_window` | 30 | How far a client's signed message may be off the server's clock |
| `bind_sessions` | `True` | A token that names the client's key (`cnf`) is taken only on an end-to-end session in which the client proves that key; `False` keeps the key for signed messages only |

### Events

The application reads what clients do in batches:

```python
server.enable_drain_mode()                # before start()
events = server.drain_inbound(256, 50)    # up to 256 events; waits up to 50 ms for the first
```

Each event is a tuple `(kind, conn_id, data)`:

| Kind | When | `data` |
|------|------|--------|
| `"auth_connect"` | A client connected with a valid token | its user id (the token's `sub`) |
| `"connect"` | A client connected to a server without `jwt_secret` | its Cookie header: check it, then call `accept_connection(conn_id, user_id)` |
| `"msg"` | A JSON message | the message, a dict |
| `"signed"` | A signed message that passed every check | its fields, and `signature` (`body`, `sig`, `kid`): the proof to keep |
| `"raw"` | Text that is not JSON | the string |
| `"bin"` | A binary frame | the bytes |
| `"disconnect"` | The connection closed | `None` |
| `"presence_join"` | A user's first connection joined a topic | `user_id`, `topic` and `data` |
| `"presence_leave"` | A user's last connection left a topic | `user_id`, `topic` and `data` |

Instead of polling, `set_callbacks(on_connect, on_message, on_disconnect, on_signed_message=None)` has the server call the application on its worker threads, each connection's callbacks in the order of its events.

### Sending

```python
server.send(conn_id, text)                  # to one connection
server.send_bytes(conn_id, data)            # binary, to one connection
server.send_event(conn_id, {"t": "notice", "p": {"text": "hi"}})  # a dict, compressed when large
server.broadcast(topic, text)               # to a topic's subscribers on every server of the cluster
server.broadcast_local(topic, text)         # to a topic's subscribers on this server
server.broadcast_all(text)                  # to every connection on this server
server.broadcast_all_bytes(data)
```

A client that asked for MessagePack gets MessagePack from all of these except the two `_bytes` methods, which send their bytes as they are. When the server has 65,536 commands queued, these calls wait (without holding the GIL), so a publisher faster than its clients cannot run the server out of memory.

### Topics

```python
server.set_topic_acl(conn_id, allow=["user:42:*"], deny=["user:42:admin"])
server.subscribe_connection(conn_id, ["user:42:orders", "user:42:alerts:*"])  # returns the topics it took
server.subscribe_connection(conn_id, ["chat"], {"status": "online"})         # with presence data
server.subscribe_connection(conn_id, ["jobs"], queue_group="workers")        # one of "workers" gets each message
server.unsubscribe_connection(conn_id, ["chat"])                             # or None: every topic
server.get_topic_subscriber_count("chat")
```

The ACL takes `*` and `?` wildcards, and a deny wins over an allow. Set it before subscribing; a token can carry the same rules in its `wse_topics` claim. A pattern subscription gets only the topics the ACL allows.

### Presence

With `presence_enabled=True`:

```python
server.presence("chat")         # {"alice": {"data": {"status": "online"}, "connections": 2}, ...}
server.presence_stats("chat")   # {"num_users": 2, "num_connections": 3}
server.update_presence(conn_id, {"status": "away"})
```

A user is the token's `sub`, so two tabs of one user are one member.

### Recovery

With `recovery_enabled=True`, every message published to a topic carries its position: `tp`, `e` and `o` (topic, epoch, offset). The clients in this repository track it, drop duplicates, notice gaps and ask for what they missed. With `client_subscriptions=True` the server answers them itself; otherwise the application does:

```python
server.subscribe_with_recovery(conn_id, ["prices"], recover=True, epoch=epoch, offset=offset)
# {"recovered": True, "topics": {"prices": {"epoch": "0000007b", "offset": 456, "recovered": True, "count": 12, ...}}}
```

When the missed messages are no longer kept, `recovered` is `False` and the client has to reload.

### Cluster

```python
server.connect_cluster(
    peers=["10.0.0.2:9999", "10.0.0.3:9999"],
    cluster_port=9999,
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
)
server.broadcast("prices", text)   # reaches the subscribers on every server
```

A new server can also join through one seed address (`seeds=[...]`, `cluster_addr=...`). On one host or a closed network the links can share the WebSocket port instead (`single_port=True`, without TLS). `cluster_info()` lists the linked peers. The protocol is described in [CLUSTER_PROTOCOL.md](https://github.com/silvermpx/wse/blob/main/docs/CLUSTER_PROTOCOL.md).

### Running it

```python
server.drain(4300, "deploy", timeout=30)   # refuse new clients, close the rest, wait; returns how many it forced
server.stop()
server.health_snapshot()                   # connections, queues, recovery, cluster and presence
server.prometheus_metrics()                # text for a /metrics endpoint
```

`with RustWSEServer(...) as server:` starts the server and stops it at the end. Logs go to standard output at level `info`; `RUST_LOG=wse=debug` shows more.

## Security

- **Tokens.** `issue_token(sub, ttl=900, claims=None)` signs tokens with the server's own settings, for a login endpoint in the same process, and `reload_jwt_keys()` replaces the keys of a running server. A connection is closed when its token expires (code 4403), and the clients reconnect with a fresh one.
- **TLS.** Serve `wss://` through a reverse proxy or a load balancer (nginx, Caddy); the server itself speaks plain WebSocket. Cluster links have their own mutual TLS.
- **Browsers.** `allowed_origins` refuses pages from other sites, which could otherwise connect with your users' cookies.
- **Limits.** Clients are rate-limited on what they send, and one that keeps going over is closed (code 4429). Frame sizes, subscriptions, queues and connections waiting in the handshake are all bounded.

### End-to-end encryption

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out e2e-identity.pem
```

```python
server = RustWSEServer(
    "0.0.0.0", 5007,
    jwt_secret=secret,
    e2e_identity_key=open("e2e-identity.pem", "rb").read(),
)
print(server.e2e_identity_public_key())   # the key clients pin
```

Clients pin that key: `encryption=True, e2e_server_keys=[key]` in Python, `security: { encryptionEnabled: true, serverKeys: [key] }` in TypeScript, `Encryption::pinned([key])` in Rust. Each direction then has its own AES-256-GCM key; frames are numbered, and a replayed, reordered or forged frame ends the connection.

The token still passes whatever terminates TLS. A token that names the client's key in its `cnf` claim is of no use there: the client signs the session's handshake with that key, and the server admits the connection only when the signature matches the key in the token. The clients do it by themselves when they have a `signing_key`, and stop if the server does not confirm their key.

### Signed messages

```python
fill = server.sign("fill", {"order": "o-17", "qty": "2"}, topic="orders:u1")
server.broadcast("orders:u1", fill)   # signed once, sent to any number of clients
```

Clients check such messages against the server's `signing_public_key()`, which they pin. They sign their own with a key that stays on the device: `send(..., signed=True)` in Python, `sendSigned()` in TypeScript, `send_signed()` in Rust. The server accepts each one once, within `signature_window`, and only with the key bound to that user, by the token's `cnf` claim or by `set_client_key()`.

Setup, key rotation and what each guarantees: [SECURITY.md](https://github.com/silvermpx/wse/blob/main/docs/SECURITY.md).

## Performance

A broadcast is encoded once and written out by every CPU core in parallel, each core serving its share of the connections. A publisher faster than its clients is slowed down rather than allowed to fill memory.

3.0 against 2.5.1 on the same machine (Intel Xeon Gold 5412U, 24 cores), with the server and the benchmark client on separate cores:

| Test | 2.5.1 | 3.0 |
|------|-------|-----|
| Broadcast to 1,000 / 5,000 / 10,000 subscribers | 5.4 / 6.0 / 7.4M deliveries/s | 9.4 / 8.2 / 7.4M |
| A topic through a cluster peer, 1,000 / 10,000 subscribers | 5.9 / 5.7M deliveries/s | 6.8 / 8.4M |
| Messages from clients, 100 / 1,000 connections | 8.3 / 7.7M msg/s | 8.3 / 7.7M |
| Peak memory under a publisher that never pauses | 35-39 GB | 19-21 GB |

More results: [benchmarks](https://github.com/silvermpx/wse/blob/main/docs/BENCHMARKS.md), [fan-out](https://github.com/silvermpx/wse/blob/main/docs/BENCHMARKS_FANOUT.md), and the [Rust](https://github.com/silvermpx/wse/blob/main/docs/BENCHMARKS_RUST_CLIENT.md), [Python](https://github.com/silvermpx/wse/blob/main/docs/BENCHMARKS_PYTHON_CLIENT.md) and [TypeScript](https://github.com/silvermpx/wse/blob/main/docs/BENCHMARKS_TS_CLIENT.md) clients.

## Upgrading from 2.x

The Python API keeps its names, but some behaviour changed. What most applications notice:

- `connect_cluster()` needs `cluster_port`, or `single_port=True`.
- `start()` raises `OSError` when the port is taken; `drain()` waits for the clients and returns how many it had to drop.
- JWT settings are checked at startup, and a token without the configured `iss` or `aud` is refused.
- End-to-end encryption now authenticates the server, so clients pin its key. 2.x clients keep working while `e2e_legacy=True`, the default.
- The rate limit applies to what clients send; `send_event()` is no longer throttled.

The full list, with the client changes: [MIGRATION.md](https://github.com/silvermpx/wse/blob/main/docs/MIGRATION.md#migrating-from-v2x-to-v30).

## Documentation

- [Integration guide](https://github.com/silvermpx/wse/blob/main/docs/INTEGRATION.md): setup and the complete API
- [Wire protocol](https://github.com/silvermpx/wse/blob/main/docs/PROTOCOL.md): messages, close codes, end-to-end encryption
- [Cluster protocol](https://github.com/silvermpx/wse/blob/main/docs/CLUSTER_PROTOCOL.md)
- [Security](https://github.com/silvermpx/wse/blob/main/docs/SECURITY.md)
- [Deployment](https://github.com/silvermpx/wse/blob/main/docs/DEPLOYMENT.md): production setup, Docker, Kubernetes
- [Architecture](https://github.com/silvermpx/wse/blob/main/docs/ARCHITECTURE.md): how the server works inside
- [Rust utilities](https://github.com/silvermpx/wse/blob/main/docs/RUST_UTILITIES.md): queues, rate limiters, compression, crypto and JWT helpers the Python package also exports
- [Changelog](https://github.com/silvermpx/wse/blob/main/CHANGELOG.md)
- [Contributing](https://github.com/silvermpx/wse/blob/main/CONTRIBUTING.md)

## License

MIT
