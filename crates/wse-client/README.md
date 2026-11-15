# wse-client

The Rust client of [WSE](https://github.com/silvermpx/wse): topics with
recovery, reconnects, msgpack, end-to-end encryption and signed messages,
on tokio and in the browser (WebAssembly).
It speaks the protocol of the TypeScript and Python clients to the same
servers: [`wse`](https://crates.io/crates/wse) in Rust and
[`wse-server`](https://pypi.org/project/wse-server/) in Python.

```toml
[dependencies]
wse-client = "3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## A stream of what the server sends

```rust,no_run
use wse_client::{Config, Event, Incoming};

#[tokio::main]
async fn main() -> Result<(), wse_client::Error> {
    let mut config = Config::new("wss://example.com/wse");
    config.token = std::env::var("WSE_TOKEN").ok();
    config.topics = vec!["prices".into()];
    let (client, mut events) = wse_client::connect(config).await?;

    client
        .send("hello", &serde_json::json!({"from": "a bot"}))
        .await?;
    while let Some(incoming) = events.next().await {
        match incoming {
            Incoming::Message(message) => {
                println!("{} {:?}: {}", message.kind(), message.topic(), message.payload_value()?);
            }
            // Messages that will not come: reload what depends on them.
            Incoming::Event(Event::Gap(gap)) => println!("{} lost {:?}", gap.topic, gap.missed()),
            Incoming::Event(event) => println!("{event:?}"),
            _ => {}
        }
    }
    Ok(())
}
```

## A handler, without copies

A `Handler` is called on the connection's own task with each message read
in place from its frame; its payload decodes into the application's types,
borrowing from the frame.

```rust,no_run
use wse_client::{Client, Config, Event, Handler, Message};

#[derive(serde::Deserialize)]
struct Tick<'a> {
    symbol: &'a str,
    price: f64,
}

struct Prices;

impl Handler for Prices {
    fn message(&mut self, message: &Message<'_>) {
        if message.kind() == "tick" {
            if let Ok(tick) = message.payload::<Tick>() {
                println!("{} {}", tick.symbol, tick.price);
            }
        }
    }

    fn event(&mut self, event: Event) {
        if let Event::Gap(gap) = event {
            println!("{} lost {:?} messages", gap.topic, gap.missed());
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), wse_client::Error> {
    let mut config = Config::new("ws://127.0.0.1:5006/wse");
    config.topics = vec!["prices".into()];
    let client = Client::connect(config, Prices).await?;
    client.closed().await;
    Ok(())
}
```

`examples/chat_client.rs` talks to the chat server of the `wse` crate.

## What a client does

- **Topics with recovery.** Each topic message carries its position; the
  client drops duplicates, holds what comes past a gap while it asks the
  server for what is missing, and after a reconnect subscribes again from
  where it left off. What cannot be recovered is reported as a `Gap`.
- **Reconnects** with backoff and jitter, a fresh token from a
  `TokenProvider` each time; an expired or refused token, a server that
  refuses the session, and the application's own close end the client.
- **Heartbeats** both ways, and a connection silent for too long is made
  again.
- **Wire formats.** JSON in text or binary frames, compressed frames, and
  msgpack (`Format::Msgpack`, 24-41% fewer bytes).
- **End-to-end encryption** with the server's identity key pinned
  (`Encryption`): whatever terminates TLS in between sees nothing.
- **Signed messages.** `Client::send_signed` signs with the client's key
  for the server's; the server's signed messages are checked against the
  keys the client pins, and each carries its proof. With encryption, the
  client also proves its key inside the session, which binds a token that
  names the key to this client (`Encryption::bind_session`, on by default),
  and stops unless the server confirms the key.
- **Back-pressure.** A handler that waits in `ready` holds the server back;
  the stream of `connect` does the same while nobody reads it.

```rust,no_run
# async fn run() -> Result<(), wse_client::Error> {
// Kept by the client. The backend names its public key in the client's
// token (`cnf`: `wse_client::public_key_jwk(&key)`).
let key = wse_client::generate_key();
let mut config = wse_client::Config::new("wss://example.com/wse");
config.token = Some("<a token whose cnf names the key>".into());
config.encryption = Some(wse_client::Encryption::pinned([
    "<the server's e2e_identity_public_key>",
]));
config.signing_key = Some(key);
config.server_signing_keys = vec!["<the server's signing_public_key>".into()];
let (client, _events) = wse_client::connect(config).await?;
client
    .send_signed("order", &serde_json::json!({"side": "buy", "qty": "1.5"}), Some("order-7"))
    .await?;
# Ok(())
# }
```

## In the browser

The same crate builds for `wasm32-unknown-unknown`, for applications in
Leptos, Yew, Dioxus and the like, with the same API. There the connection
is the browser's WebSocket, on the page's task: a handler need not be
`Send`, and `Client::connect` is awaited from the page's own futures.

- The browser sends the page's cookies with the upgrade and no header, so
  the server takes the client by its cookie (`jwt_cookie_name`); `token`,
  `headers` and `tls` do not apply, and a `TokenProvider` is asked before
  each attempt only to refresh what it must.
- The browser cannot hold a WebSocket back: what comes waits in memory
  until the handler or the stream takes it (`Handler::ready` is not called
  there), and the client's own sends wait while the browser holds a
  megabyte unsent.
- End-to-end encryption and signed messages run on RustCrypto there,
  checked against the same vectors as aws-lc-rs.

## Features

- `aws-lc` (default): `wss://` with rustls and the Mozilla root
  certificates, end-to-end encryption and signed messages, on aws-lc-rs (in
  the browser: encryption and signing on RustCrypto, TLS the browser's).
  Without it there is no end-to-end encryption or signing, and natively no
  `wss://`; in the browser `wss://` is the browser's own TLS.
- `fips`: aws-lc-rs built from its FIPS 140-3 validated module (needs CMake
  and Go).

The protocol is documented in the
[repository](https://github.com/silvermpx/wse/tree/main/docs).

## License

MIT
