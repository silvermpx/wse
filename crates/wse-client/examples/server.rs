//! A server to try the client against: it takes every connection, answers
//! the clients' subscriptions, keeps topic histories, publishes a tick on
//! `ticks` ten times a second, answers `echo` with `echoed`, binds the key a
//! client names in `bind_key` for its signed messages, and has an E2E
//! identity and a signing key.
//!
//!     cargo run --example server
//!
//! It prints its URL and the keys a client pins; `WSE_PORT` sets the port.

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::mpsc;

    struct Lab {
        publisher: wse::Publisher,
        accept: mpsc::UnboundedSender<String>,
    }

    impl wse::Handler for Lab {
        type Connection = String;

        fn connect(&self, connect: &wse::Connect<'_>) -> Option<String> {
            let conn_id = (**connect.conn_id).clone();
            let _ = self.accept.send(conn_id.clone());
            Some(conn_id)
        }

        fn message(&self, conn: &mut String, message: wse::Inbound, _size: usize) -> bool {
            let wse::Inbound::Json(message) = message else {
                return true;
            };
            match message["t"].as_str() {
                Some("echo") => {
                    let echoed = json!({"t": "echoed", "p": message["p"]});
                    let _ = self.publisher.send(conn, echoed.to_string());
                }
                Some("bind_key") => {
                    let key = message["p"]["public_key"].as_str();
                    let bound = self.publisher.set_client_key(conn, key).unwrap_or(false);
                    let answer = json!({"t": "key_bound", "p": {"bound": bound}});
                    let _ = self.publisher.send(conn, answer.to_string());
                }
                _ => {}
            }
            true
        }

        fn signed(&self, _conn: &mut String, _message: serde_json::Value, _size: usize) -> bool {
            // Taken: the client gets a signed receipt.
            true
        }

        fn disconnect(&self, _conn: String) {}
    }

    let mut config = wse::Config::default();
    config.client_subscriptions = true;
    config.recovery = Some(wse::RecoverySettings::default());
    // The keys come from the client's own helper, which needs its crypto.
    #[cfg(feature = "aws-lc")]
    {
        config.e2e_identity_key = Some(wse_client::generate_key());
        config.signing_key = Some(wse_client::generate_key());
    }
    let server = wse::Server::new(config)?;
    let publisher = server.publisher();
    let port: u16 = std::env::var("WSE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5007);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    let (accept, mut accepted) = mpsc::unbounded_channel();
    server.start(
        listener,
        Lab {
            publisher: publisher.clone(),
            accept,
        },
    )?;
    println!("url ws://127.0.0.1:{port}/wse");
    println!(
        "e2e_identity_public_key {}",
        publisher.e2e_identity_public_key().unwrap_or_default()
    );
    println!(
        "signing_public_key {}",
        publisher.signing_public_key().unwrap_or_default()
    );

    // Every connection is taken, as a guest.
    let taker = publisher.clone();
    tokio::spawn(async move {
        while let Some(conn_id) = accepted.recv().await {
            let _ = taker.accept(&conn_id, "guest");
        }
    });
    let ticker = publisher.clone();
    tokio::spawn(async move {
        let mut n = 0u64;
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            n += 1;
            let tick = json!({"t": "tick", "p": {"n": n}}).to_string();
            if ticker.publish_local("ticks", &tick).is_err() {
                return;
            }
        }
    });
    tokio::signal::ctrl_c().await?;
    server.stop().await;
    Ok(())
}
