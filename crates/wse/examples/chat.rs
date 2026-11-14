//! A chat room: the server validates each client's token, puts it in the
//! lobby, and sends what one says to everyone there.
//!
//!     JWT_SECRET=$(openssl rand -hex 32) cargo run --example chat
//!
//! Clients connect to ws://127.0.0.1:5006/wse with their token in the
//! `access_token` cookie or an `Authorization: Bearer` header, and send
//! `{"t": "say", "p": {"text": "..."}}`.

use std::time::Duration;

use tokio::net::TcpListener;
use wse::{Config, Connect, Handler, Inbound, JwtSettings, Publisher, Server, Subscribe};

const LOBBY: &str = "room:lobby";

struct Chat {
    publisher: Publisher,
}

impl Handler for Chat {
    /// Who the connection is.
    type Connection = String;

    fn connect(&self, connect: &Connect<'_>) -> Option<String> {
        // The server validated the token: every connection here has a user.
        let user = connect.user_id?.to_owned();
        connect
            .subscribe(vec![LOBBY.to_owned()], &Subscribe::default())
            .ok()?;
        Some(user)
    }

    fn message(&self, user: &mut String, message: Inbound, _size: usize) -> bool {
        let Inbound::Json(message) = message else {
            return true;
        };
        if message["t"] == "say" {
            let said = serde_json::json!({
                "t": "said",
                "p": {"from": user, "text": message["p"]["text"]},
            });
            // A full queue holds a fast talker back instead of filling memory.
            if self.publisher.is_full() {
                return false;
            }
            let _ = self.publisher.publish(LOBBY, &said.to_string());
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
    let chat = Chat {
        publisher: server.publisher(),
    };
    server.start(TcpListener::bind("127.0.0.1:5006").await?, chat)?;
    println!("chat on ws://127.0.0.1:5006/wse");

    tokio::signal::ctrl_c().await?;
    // Clients are told the server restarts and come back to another one.
    let dropped = server
        .drain(1012, "restarting", Duration::from_secs(10))
        .await?;
    println!("drained, {dropped} connection(s) dropped");
    server.stop().await;
    Ok(())
}
