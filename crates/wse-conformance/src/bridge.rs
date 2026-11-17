//! A Rust owner of the window bridge, for a driver's window (`/bridge`): the
//! TypeScript client runs the cases as a window over this WebSocket, and the
//! owner holds the connection to the case's server, as a Tauri
//! application's Rust side holds it.
//!
//! The driver's first message names the owner's connection, `{"url",
//! "token", "format", "e2e_key", "signing", "server_signing_key"}`, and is
//! answered `{"ready": true, "public_key"}` (the public key of the signing
//! key made for it, when asked for one) or `{"ready": false, "error"}`.
//! Then each message is a frame: the window's to the owner, the owner's to
//! the window.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use wse_client::bridge::{Options, Owner, Window};
use wse_client::{Config, Encryption, Format};

/// The owner's end of the driver's window: frames into the socket's writer;
/// `None` ends it.
struct Pipe(mpsc::UnboundedSender<Option<String>>);

impl Window for Pipe {
    fn post(&self, frame: String) {
        let _ = self.0.send(Some(frame));
    }

    fn close(&self) {
        let _ = self.0.send(None);
    }
}

/// The owner's connection, as the driver names it; and the signing key's
/// public key, when it asked for one.
fn config(asked: &Value) -> Result<(Config, Option<String>), String> {
    let mut config = Config::new(asked["url"].as_str().ok_or("no url")?);
    config.token = asked["token"].as_str().map(str::to_owned);
    config.reconnect.base_delay = Duration::from_millis(50);
    config.reconnect.max_delay = Duration::from_millis(300);
    config.reconnect.jitter = false;
    if asked["format"] == "msgpack" {
        config.format = Format::Msgpack;
    }
    if let Some(key) = asked["e2e_key"].as_str() {
        config.encryption = Some(Encryption::pinned([key]));
    }
    let mut public = None;
    if asked["signing"] == true {
        let key = wse_client::generate_key();
        public = Some(wse_client::public_key(&key).map_err(|e| e.to_string())?);
        config.signing_key = Some(key);
        config.server_signing_keys = asked["server_signing_key"]
            .as_str()
            .map(|key| vec![key.to_owned()])
            .unwrap_or_default();
    }
    Ok((config, public))
}

/// Serves one driver's window until either end goes.
pub async fn host(ws: WebSocketStream<TcpStream>) -> Result<(), String> {
    let (mut sink, mut stream) = ws.split();
    let asked = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => {
                break serde_json::from_str::<Value>(&text).map_err(|e| e.to_string())?;
            }
            Some(Ok(_)) => {}
            _ => return Ok(()),
        }
    };
    let started = match config(&asked) {
        Ok((config, public)) => Owner::start(config, Options::default())
            .await
            .map(|owner| (owner, public))
            .map_err(|e| e.to_string()),
        Err(why) => Err(why),
    };
    let (owner, public) = match started {
        Ok(started) => started,
        Err(why) => {
            let answer = json!({"ready": false, "error": why}).to_string();
            let _ = sink.send(Message::text(answer)).await;
            return Err(why);
        }
    };
    let answer = json!({"ready": true, "public_key": public}).to_string();
    sink.send(Message::text(answer))
        .await
        .map_err(|e| e.to_string())?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let att = owner.attach(Pipe(tx));
    loop {
        tokio::select! {
            out = rx.recv() => match out {
                Some(Some(frame)) => {
                    if sink.send(Message::text(frame)).await.is_err() {
                        break;
                    }
                }
                // The owner let the window go.
                _ => break,
            },
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(text))) => owner.frame(att, &text),
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
        }
    }
    owner.detach(att);
    let _ = sink.close().await;
    owner.close().await;
    Ok(())
}
