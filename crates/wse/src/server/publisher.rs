//! What the application reaches a running server with: sending to one
//! connection, to a topic, to every connection and to the cluster.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

use super::messages::{build_server_ready, inject_category, message_category};
use super::signing;
use super::state::{ServerCommand, SharedState};
use crate::msgpack::serde_json_to_rmpv;

/// Why the server did not take a message or a request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The server stopped: it takes nothing more.
    Stopped,
    /// The server is running already.
    Running,
    /// The server validates tokens itself and accepts its connections on
    /// its own.
    AcceptsByToken,
    /// The server has no signing key.
    NoSigningKey,
    /// Not a client key: base64 of an uncompressed P-256 point (65 bytes).
    BadKey,
    /// The message could not be encoded or signed.
    Encode(String),
    /// A connection over its subscription limit (its id, the limit).
    TooManyTopics(String, usize),
    /// The server validates no tokens (no JWT configuration).
    NoJwt,
    /// A token could not be issued.
    Jwt(String),
    /// A key does not fit the configured algorithm.
    BadJwtKey(String),
    /// The cluster could not be joined, or its TLS reloaded.
    Cluster(String),
    /// Cluster settings that exclude each other.
    ClusterConfig(String),
    /// A port could not be bound.
    Io(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => f.write_str("the server stopped"),
            Self::Running => f.write_str("the server is running already"),
            Self::AcceptsByToken => f.write_str(
                "this server validates tokens itself (jwt_secret) and accepts connections on \
                 its own; accept_connection() is for applications that authenticate them",
            ),
            Self::NoSigningKey => f.write_str("signing needs the server's signing key"),
            Self::BadKey => {
                f.write_str("public_key must be base64 of an uncompressed P-256 point (65 bytes)")
            }
            Self::Encode(what)
            | Self::Jwt(what)
            | Self::BadJwtKey(what)
            | Self::Cluster(what)
            | Self::ClusterConfig(what)
            | Self::Io(what) => f.write_str(what),
            Self::TooManyTopics(conn_id, max) => {
                write!(f, "Connection {conn_id} reached max subscriptions ({max})")
            }
            Self::NoJwt => f.write_str("this server does not validate tokens (no jwt_secret)"),
        }
    }
}

impl std::error::Error for Error {}

/// What the application reaches its server with: send, publish, subscribe,
/// close. Cheap to clone; every clone reaches the same server, across
/// restarts. While the server is stopped, sends fail with `Error::Stopped`.
/// Sends and broadcasts go through the server's command queue in the order
/// they are made: when the queue is full, a fast publisher waits for room
/// (`ready`, `wait_ready`) instead of filling memory.
#[derive(Clone)]
pub struct Publisher {
    pub(crate) shared: Arc<SharedState>,
}

impl Publisher {
    pub(crate) fn new(shared: Arc<SharedState>) -> Self {
        Self { shared }
    }

    /// Queue a command for the running server.
    pub(crate) fn command(&self, cmd: ServerCommand) -> Result<(), Error> {
        match &*self.shared.commands.load() {
            Some(commands) => commands.send(cmd).map_err(|_| Error::Stopped),
            None => Err(Error::Stopped),
        }
    }

    fn queue(&self, cmd: ServerCommand) -> Result<(), Error> {
        self.command(cmd)
    }

    /// Whether the server is running: it takes sends and broadcasts.
    pub fn is_running(&self) -> bool {
        self.shared
            .commands
            .load()
            .as_ref()
            .is_some_and(|commands| !commands.is_closed())
    }

    /// Whether the command queue is full: sends and broadcasts wait for room
    /// before theirs.
    pub fn is_full(&self) -> bool {
        self.shared
            .commands
            .load()
            .as_ref()
            .is_some_and(|commands| commands.is_full())
    }

    /// Wait for room in the command queue.
    pub async fn ready(&self) {
        while self
            .shared
            .commands
            .load()
            .as_ref()
            .is_some_and(|commands| commands.is_full() && !commands.is_closed())
        {
            tokio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
    }

    /// Block the calling thread until the command queue has room: for a
    /// caller outside the runtime.
    pub fn wait_ready(&self) {
        if let Some(commands) = self.shared.commands.load_full() {
            commands.wait_for_room();
        }
    }

    /// Accept a connection the application authenticated itself (a server
    /// without a JWT configuration): it gets server_ready for `user_id`, then
    /// broadcasts reach it and it may subscribe on its own. False when the
    /// connection is gone.
    pub fn accept(&self, conn_id: &str, user_id: &str) -> Result<bool, Error> {
        let shared = &self.shared;
        if shared.jwt_config.is_some() {
            return Err(Error::AcceptsByToken);
        }
        // Its activity entry lives from registration to cleanup.
        if !shared.conn_last_activity.contains_key(conn_id) {
            return Ok(false);
        }
        shared.register_user(conn_id, user_id);
        if let Some(ref pm) = shared.presence {
            pm.register_connection(conn_id, user_id);
        }
        // Gone meanwhile: its cleanup may have run before the register.
        if !shared.conn_last_activity.contains_key(conn_id) {
            shared.unregister_user(conn_id);
            if let Some(ref pm) = shared.presence {
                pm.remove_connection(conn_id);
            }
            return Ok(false);
        }
        let ready = build_server_ready(conn_id, user_id, shared);
        self.queue(ServerCommand::Accept {
            conn_id: conn_id.to_owned(),
            ready,
        })?;
        Ok(true)
    }

    /// Send `text` to one connection, as it is.
    pub fn send(&self, conn_id: &str, text: impl Into<String>) -> Result<(), Error> {
        self.queue(ServerCommand::SendText {
            conn_id: conn_id.to_owned(),
            data: text.into(),
        })
    }

    /// Send `data` to one connection as a binary frame, as it is.
    pub fn send_bytes(&self, conn_id: &str, data: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.queue(ServerCommand::SendBytes {
            conn_id: conn_id.to_owned(),
            data: data.into(),
        })
    }

    /// Send an event to one connection in its envelope (`c`, `t`, the
    /// event's fields, `id`, `seq`, `ts`, `v`), in the connection's format:
    /// msgpack, compressed JSON past `compression_threshold` bytes (the
    /// client's own threshold first; 0 compresses nothing), or JSON; sealed
    /// in an E2E session, and never compressed then. The same `id` goes to a
    /// connection once. Returns the bytes queued, 0 when nothing was sent.
    pub fn send_event(
        &self,
        conn_id: &str,
        event: serde_json::Map<String, serde_json::Value>,
        compression_threshold: usize,
    ) -> Result<usize, Error> {
        let shared = &self.shared;
        if !self.is_running() {
            return Err(Error::Stopped);
        }
        // A connection that is gone (or never was) gets nothing.
        if !shared.conn_last_activity.contains_key(conn_id) {
            return Ok(0);
        }
        // Once to each connection: the same id to another connection is a
        // send of its own.
        if let Some(id) = event.get("id").and_then(serde_json::Value::as_str) {
            let key = format!("{conn_id}\0{id}");
            let mut dedup = shared.dedup.lock().unwrap_or_else(|e| e.into_inner());
            if !dedup.seen.insert(key.clone()) {
                return Ok(0);
            }
            dedup.queue.push_back(key);
            while dedup.queue.len() > dedup.max_entries {
                if let Some(old) = dedup.queue.pop_front() {
                    dedup.seen.remove(&old);
                }
            }
        }

        // c first, t second, v last.
        let msg_type = event
            .get("t")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let mut map = serde_json::Map::new();
        map.insert(
            "c".to_owned(),
            serde_json::Value::String(message_category(&msg_type).to_owned()),
        );
        if !msg_type.is_empty() {
            map.insert("t".to_owned(), serde_json::Value::String(msg_type));
        }
        for (key, value) in event {
            if key != "t" && key != "v" && key != "c" {
                map.insert(key, value);
            }
        }
        if !map.contains_key("id") {
            map.insert(
                "id".to_owned(),
                serde_json::Value::String(Uuid::now_v7().to_string()),
            );
        }
        static GLOBAL_SEQ: AtomicU64 = AtomicU64::new(0);
        if !map.contains_key("seq") {
            map.insert(
                "seq".to_owned(),
                serde_json::Value::Number(GLOBAL_SEQ.fetch_add(1, Ordering::Relaxed).into()),
            );
        }
        if !map.contains_key("ts") {
            let ts = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
            map.insert("ts".to_owned(), serde_json::Value::String(ts));
        }
        map.insert("v".to_owned(), serde_json::Value::Number(1.into()));

        let use_msgpack = shared.conn_formats.get(conn_id).is_some_and(|v| *v);
        // Encrypted connections are sealed by the server on the way out, and
        // never compressed: compression would leak through the length.
        let e2e =
            shared.conn_encryption.contains_key(conn_id) || shared.conn_sealed.contains(conn_id);
        // The client's own compression wish, unless the call turns it off.
        let client_threshold = shared.conn_compression.get(conn_id).map(|t| *t);
        let body: Vec<u8> = if use_msgpack {
            let value = serde_json_to_rmpv(&serde_json::Value::Object(map));
            let mut buf = Vec::with_capacity(256);
            buf.extend_from_slice(b"M:");
            rmpv::encode::write_value(&mut buf, &value)
                .map_err(|e| Error::Encode(format!("Msgpack error: {e}")))?;
            buf
        } else {
            let json = serde_json::to_vec(&serde_json::Value::Object(map))
                .map_err(|e| Error::Encode(format!("JSON error: {e}")))?;
            let threshold = client_threshold.unwrap_or(compression_threshold);
            if !e2e && compression_threshold > 0 && threshold > 0 && json.len() > threshold {
                let mut encoder = ZlibEncoder::new(Vec::from(&b"C:"[..]), Compression::fast());
                encoder
                    .write_all(&json)
                    .map_err(|e| Error::Encode(format!("Compress error: {e}")))?;
                encoder
                    .finish()
                    .map_err(|e| Error::Encode(format!("Compress finish: {e}")))?
            } else {
                // JSON as a binary frame (skips UTF-8 validation overhead)
                json
            }
        };
        let byte_count = body.len();
        self.queue(ServerCommand::SendPrebuilt {
            conn_id: conn_id.to_owned(),
            message: Message::Binary(body.into()),
        })?;
        Ok(byte_count)
    }

    /// Send `text` to every connection.
    pub fn broadcast(&self, text: &str) -> Result<(), Error> {
        self.queue(ServerCommand::BroadcastText {
            data: inject_category(text),
        })
    }

    /// Send `data` to every connection as a binary frame.
    pub fn broadcast_bytes(&self, data: impl Into<Vec<u8>>) -> Result<(), Error> {
        self.queue(ServerCommand::BroadcastBytes { data: data.into() })
    }

    /// Publish `text` on `topic` to this server's subscribers only.
    pub fn publish_local(&self, topic: &str, text: &str) -> Result<(), Error> {
        self.queue(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data: inject_category(text),
            skip_recovery: false,
            publish: false,
        })
    }

    /// Publish `text` on `topic` to its subscribers here and on every
    /// cluster node. The command task stamps it (its recovery position),
    /// delivers it here, then hands the same stamped bytes to the cluster:
    /// every node delivers one form of a message, at one position.
    pub fn publish(&self, topic: &str, text: &str) -> Result<(), Error> {
        self.queue(ServerCommand::BroadcastLocal {
            topic: topic.to_owned(),
            data: inject_category(text),
            skip_recovery: false,
            publish: true,
        })
    }
}

impl SharedState {
    /// A server-signed event (`signing_key`): the `signed` envelope a
    /// client checks with the server's public key.
    pub(crate) fn sign(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        topic: Option<&str>,
    ) -> Result<String, Error> {
        let signer = self.signer.as_ref().ok_or(Error::NoSigningKey)?;
        signer
            .sign_event(event_type, payload, topic, signing::now_ms())
            .map_err(|_| Error::Encode("signing failed".to_owned()))
    }

    /// A connection's public key for its signed messages (base64 of an
    /// uncompressed P-256 point), or none. False when the connection is gone.
    pub(crate) fn set_client_key(
        &self,
        conn_id: &str,
        public_key: Option<&str>,
    ) -> Result<bool, Error> {
        let key = public_key
            .map(|public_key| signing::ClientKey::from_base64(public_key).ok_or(Error::BadKey))
            .transpose()?;
        // Its activity entry lives from registration to cleanup.
        if !self.conn_last_activity.contains_key(conn_id) {
            return Ok(false);
        }
        match key {
            None => {
                self.client_keys.remove(conn_id);
            }
            Some(key) => {
                self.client_keys.insert(conn_id.to_owned(), Arc::new(key));
            }
        }
        // Gone meanwhile: its cleanup may have run before the insert.
        if !self.conn_last_activity.contains_key(conn_id) {
            self.client_keys.remove(conn_id);
            return Ok(false);
        }
        Ok(true)
    }
}
