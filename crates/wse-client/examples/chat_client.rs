//! A chat client for the `wse` crate's chat server: what it says comes from
//! the terminal, what the room says is printed.
//!
//!     WSE_TOKEN=<the token the server printed> cargo run --example chat_client
//!
//! Other servers: `WSE_URL=wss://...`.

#[cfg(not(target_arch = "wasm32"))]
use tokio::io::{AsyncBufReadExt, BufReader};
#[cfg(not(target_arch = "wasm32"))]
use wse_client::{Config, Event, Incoming};

#[cfg(not(target_arch = "wasm32"))]
#[derive(serde::Deserialize)]
struct Said<'a> {
    from: &'a str,
    text: &'a str,
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("WSE_URL").unwrap_or_else(|_| "ws://127.0.0.1:5006/wse".into());
    let mut config = Config::new(url);
    config.token = Some(std::env::var("WSE_TOKEN")?);
    let (client, mut events) = wse_client::connect(config).await?;
    println!("connected as {:?}; type to talk", client.user_id());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        tokio::select! {
            line = lines.next_line() => match line? {
                Some(text) => client.send("say", &serde_json::json!({"text": text})).await?,
                None => break,
            },
            incoming = events.next() => match incoming {
                Some(Incoming::Message(message)) if message.kind() == "said" => {
                    let said: Said<'_> = message.payload()?;
                    println!("{}: {}", said.from, said.text);
                }
                Some(Incoming::Event(Event::Disconnected(lost))) => {
                    println!("lost the connection ({}); again in {:?}", lost.reason, lost.retry_in);
                }
                Some(Incoming::Event(Event::Closed(why))) => {
                    println!("closed: {why:?}");
                    break;
                }
                Some(_) => {}
                None => break,
            },
        }
    }
    client.close().await;
    Ok(())
}
