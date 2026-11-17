//! One case's server: a WSE server of its own on a port of its own, kept
//! across a restart (a new server, so a new history and epoch, on the same
//! port and with the same keys), and the log of what it published and took.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

use crate::cases::ServerSettings;

const SECRET: &str = "the-conformance-server-secret-of-32-bytes-or-more";

/// What a server took: a message, by the connection it came on (the first
/// connection of the case is 1, across restarts too).
#[derive(Clone, Debug, Serialize)]
pub struct Taken {
    pub conn: u64,
    pub user: Option<String>,
    #[serde(rename = "type")]
    pub kind: String,
    pub id: Option<String>,
    pub s: Option<u64>,
    pub possible_repeat: bool,
    pub signed: bool,
}

/// A message the case published on a topic: its number (`p.n`), counted on
/// for the whole case.
#[derive(Clone, Debug, Serialize)]
pub struct Published {
    pub topic: String,
    pub n: u64,
}

/// What the case's servers did.
#[derive(Default, Debug, Serialize)]
pub struct Log {
    pub connections: u64,
    pub taken: Vec<Taken>,
    pub published: Vec<Published>,
}

/// The connection, as the handler keeps it.
struct Conn {
    id: String,
    index: u64,
    user: Option<String>,
}

/// Takes every connection, logs what it takes, answers `echo` with
/// `echoed` and a signed message with `signed_taken`, and binds the key a
/// client names in `bind_key` (`key_bound`).
struct Lab {
    publisher: wse::Publisher,
    log: Arc<Mutex<Log>>,
}

impl Lab {
    fn take(&self, conn: &Conn, message: &Value, signed: bool) {
        let taken = Taken {
            conn: conn.index,
            user: conn.user.clone(),
            kind: message["t"].as_str().unwrap_or_default().to_owned(),
            id: message["id"].as_str().map(str::to_owned),
            s: message["s"].as_u64(),
            possible_repeat: message["possible_repeat"].as_bool() == Some(true),
            signed,
        };
        lock(&self.log).taken.push(taken);
    }
}

fn lock(log: &Mutex<Log>) -> std::sync::MutexGuard<'_, Log> {
    log.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The log, to read.
pub fn lock_log(log: &Mutex<Log>) -> std::sync::MutexGuard<'_, Log> {
    lock(log)
}

impl wse::Handler for Lab {
    type Connection = Conn;

    fn connect(&self, connect: &wse::Connect<'_>) -> Option<Conn> {
        let mut log = lock(&self.log);
        log.connections += 1;
        Some(Conn {
            id: (**connect.conn_id).clone(),
            index: log.connections,
            user: connect.user_id.map(str::to_owned),
        })
    }

    fn message(&self, conn: &mut Conn, message: wse::Inbound, _size: usize) -> bool {
        let wse::Inbound::Json(message) = message else {
            return true;
        };
        self.take(conn, &message, false);
        match message["t"].as_str() {
            Some("echo") => {
                let echoed = json!({"t": "echoed", "p": message["p"]});
                let _ = self.publisher.send(&conn.id, echoed.to_string());
            }
            // The application binds the key the client signs with.
            Some("bind_key") => {
                let key = message["p"]["public_key"].as_str();
                let bound = self
                    .publisher
                    .set_client_key(&conn.id, key)
                    .unwrap_or(false);
                let answer = json!({"t": "key_bound", "p": {"bound": bound}});
                let _ = self.publisher.send(&conn.id, answer.to_string());
            }
            _ => {}
        }
        true
    }

    fn signed(&self, conn: &mut Conn, message: Value, _size: usize) -> bool {
        self.take(conn, &message, true);
        let answer = json!({"t": "signed_taken", "p": {"type": message["t"]}});
        let _ = self.publisher.send(&conn.id, answer.to_string());
        true
    }

    fn disconnect(&self, _conn: Conn) {}
}

/// A case's server.
pub struct Stand {
    settings: ServerSettings,
    addr: SocketAddr,
    server: Option<wse::Server>,
    publisher: Option<wse::Publisher>,
    e2e_key: Option<Vec<u8>>,
    signing_key: Option<Vec<u8>>,
    pub log: Arc<Mutex<Log>>,
    /// The next number of each topic.
    next: HashMap<String, u64>,
}

impl Stand {
    /// A server for `settings`, listening.
    pub async fn open(settings: ServerSettings) -> Result<Self, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        let key = || wse::protocol::crypto::generate_p256_pkcs8().map_err(|e| e.to_string());
        let mut stand = Self {
            e2e_key: settings.e2e.then(key).transpose()?,
            signing_key: settings.signing.then(key).transpose()?,
            settings,
            addr,
            server: None,
            publisher: None,
            log: Arc::default(),
            next: HashMap::new(),
        };
        stand.start(listener)?;
        Ok(stand)
    }

    fn start(&mut self, listener: TcpListener) -> Result<(), String> {
        let mut config = wse::Config::default();
        config.jwt = Some(wse::JwtSettings::new(SECRET));
        config.client_subscriptions = true;
        config.recovery = self.settings.recovery.map(|size| {
            let mut recovery = wse::RecoverySettings::default();
            recovery.buffer_size = size;
            recovery
        });
        config.e2e_identity_key.clone_from(&self.e2e_key);
        config.signing_key.clone_from(&self.signing_key);
        let server = wse::Server::new(config).map_err(|e| e.to_string())?;
        let publisher = server.publisher();
        let lab = Lab {
            publisher: publisher.clone(),
            log: Arc::clone(&self.log),
        };
        server.start(listener, lab).map_err(|e| e.to_string())?;
        self.server = Some(server);
        self.publisher = Some(publisher);
        Ok(())
    }

    fn publisher(&self) -> Result<&wse::Publisher, String> {
        self.publisher
            .as_ref()
            .ok_or_else(|| "the server is stopped".to_owned())
    }

    /// What a driver needs for the case: where, its users' tokens, the keys
    /// it pins.
    pub fn about(&self, users: &[&str]) -> Result<Value, String> {
        let publisher = self.publisher()?;
        let mut tokens = serde_json::Map::new();
        for user in users {
            let token = publisher
                .issue_token(user, Duration::from_secs(3600), serde_json::Map::new())
                .map_err(|e| e.to_string())?;
            tokens.insert((*user).to_owned(), Value::String(token));
        }
        Ok(json!({
            "url": format!("ws://{}/wse", self.addr),
            "tokens": tokens,
            "identity_key": publisher.e2e_identity_public_key(),
            "signing_key": publisher.signing_public_key(),
        }))
    }

    /// One of the server's steps.
    pub async fn act(&mut self, step: &Value) -> Result<Value, String> {
        let topic = || {
            step["topic"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "a topic".to_owned())
        };
        match step["server"].as_str().unwrap_or_default() {
            "publish" => {
                let topic = topic()?;
                let count = step["count"].as_u64().unwrap_or(1);
                let mut numbers = Vec::new();
                for _ in 0..count {
                    let next = self.next.entry(topic.clone()).or_insert(1);
                    let n = *next;
                    *next += 1;
                    let text = json!({"t": "tick", "p": {"n": n}}).to_string();
                    self.publisher()?
                        .publish(&topic, &text)
                        .map_err(|e| e.to_string())?;
                    lock(&self.log).published.push(Published {
                        topic: topic.clone(),
                        n,
                    });
                    numbers.push(n);
                }
                Ok(json!({"topic": topic, "n": numbers}))
            }
            "await_subscribers" => {
                let topic = topic()?;
                let count = step["count"].as_u64().unwrap_or(1);
                for _ in 0..1000 {
                    if self.publisher()?.topic_subscriber_count(&topic) as u64 >= count {
                        return Ok(json!({}));
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(format!("no {count} subscriber(s) of {topic} within 10 s"))
            }
            "kick" => {
                let publisher = self.publisher()?;
                for conn in publisher.connections() {
                    let _ = publisher.disconnect(&conn);
                }
                Ok(json!({}))
            }
            "stop" => {
                self.stop().await;
                Ok(json!({}))
            }
            "restart" => {
                self.stop().await;
                // The same port: the clients come back where they were.
                let listener = TcpListener::bind(self.addr)
                    .await
                    .map_err(|e| e.to_string())?;
                self.start(listener)?;
                Ok(json!({}))
            }
            "sleep" => {
                let ms = step["ms"].as_u64().unwrap_or(0);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(json!({}))
            }
            other => Err(format!("no such server step: {other}")),
        }
    }

    /// The server goes; its port with it until a restart.
    pub async fn stop(&mut self) {
        self.publisher = None;
        if let Some(server) = self.server.take() {
            server.stop().await;
        }
    }
}
