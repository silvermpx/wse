//! What the application reaches a running server with: sending to one
//! connection, to a topic, to every connection and to the cluster.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use flate2::Compression;
use flate2::write::ZlibEncoder;
use tokio_tungstenite::tungstenite::protocol::Message;
use uuid::Uuid;

use super::CLOSE_AUTH_FAILED;
use super::messages::{build_auth_failed, build_server_ready, inject_category, message_category};
use super::signing;
use super::state::{Bind, ServerCommand, SharedState};
use crate::msgpack::serde_json_to_rmpv;

/// Why the server did not take a message or a request.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The server stopped: it takes nothing more.
    Stopped,
    /// The server's command queue is full: the publisher is faster than the
    /// server delivers. Await `Publisher::ready` and send again.
    Full,
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
    /// The connection has no end-to-end session: it cannot be bound to a
    /// key.
    NoSession,
    /// The cluster could not be joined, or its TLS reloaded.
    Cluster(String),
    /// Cluster settings that exclude each other.
    ClusterConfig(String),
    /// A port could not be bound: why, and the message.
    Io(std::io::ErrorKind, String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stopped => f.write_str("the server stopped"),
            Self::Full => f.write_str(
                "the server's command queue is full: await Publisher::ready() and send again",
            ),
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
            | Self::Io(_, what) => f.write_str(what),
            Self::TooManyTopics(conn_id, max) => {
                write!(f, "Connection {conn_id} reached max subscriptions ({max})")
            }
            Self::NoJwt => f.write_str("this server does not validate tokens (no jwt_secret)"),
            Self::NoSession => {
                f.write_str("the connection has no end-to-end session: it cannot be bound to a key")
            }
        }
    }
}

impl std::error::Error for Error {}

/// What the application reaches its server with: send, publish, subscribe,
/// close. Cheap to clone; every clone reaches the same server, across
/// restarts. While the server is stopped, sends fail with `Error::Stopped`.
///
/// Sends and broadcasts go through the server's command queue in the order
/// they are made, and never wait. The queue holds 65,536 commands: past
/// that, they fail with `Error::Full` until the server has caught up. A
/// publisher that may outrun its clients awaits `ready` before each send,
/// and is held back to the speed the server delivers at.
#[derive(Clone)]
pub struct Publisher {
    pub(crate) shared: Arc<SharedState>,
    /// Whether a send is refused while the command queue is full; the
    /// Python layer waits for room itself and then queues.
    refuse_full: bool,
}

impl Publisher {
    pub(crate) fn new(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            refuse_full: true,
        }
    }

    /// A publisher whose caller waits for room before each send (`wait_ready`)
    /// and then queues even if others filled the queue meanwhile.
    #[cfg(feature = "python")]
    pub(crate) fn waiting(shared: Arc<SharedState>) -> Self {
        Self {
            shared,
            refuse_full: false,
        }
    }

    /// Queue a command for the running server.
    pub(crate) fn command(&self, cmd: ServerCommand) -> Result<(), Error> {
        match &*self.shared.commands.load() {
            Some(commands) => commands.send(cmd).map_err(|_| Error::Stopped),
            None => Err(Error::Stopped),
        }
    }

    /// Queue the application's send or broadcast: refused while the
    /// command queue is full.
    fn queue(&self, cmd: ServerCommand) -> Result<(), Error> {
        match &*self.shared.commands.load() {
            Some(commands) if self.refuse_full && commands.is_full() => Err(Error::Full),
            Some(commands) => commands.send(cmd).map_err(|_| Error::Stopped),
            None => Err(Error::Stopped),
        }
    }

    /// Whether the server is running: it takes sends and broadcasts.
    pub fn is_running(&self) -> bool {
        self.shared
            .commands
            .load()
            .as_ref()
            .is_some_and(|commands| !commands.is_closed())
    }

    /// Whether the command queue is full: sends and broadcasts fail with
    /// `Error::Full` until it has room.
    pub fn is_full(&self) -> bool {
        self.shared
            .commands
            .load()
            .as_ref()
            .is_some_and(|commands| commands.is_full())
    }

    /// Wait for room in the command queue: a publisher awaits this before
    /// its sends to be held back instead of refused.
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

    /// Block the calling thread until the command queue has room, for a
    /// thread of the application's own. On the runtime, a `Handler`
    /// included, await `ready` instead: blocking a worker thread holds
    /// back the server's own tasks.
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
        let ready = build_server_ready(conn_id, user_id, shared, None);
        self.command(ServerCommand::Accept {
            conn_id: conn_id.to_owned(),
            ready,
        })?;
        Ok(true)
    }

    /// `accept`, with the connection bound to `client_key` (base64 of an
    /// uncompressed P-256 point): the key the application knows for
    /// `user_id`. The connection is admitted once the client has proved
    /// that key inside its end-to-end session (it does so by itself, with
    /// its signing key), and refused (4401) when it proves another key, or
    /// none by the admission deadline; whoever copied its cookie on the way
    /// cannot. `Error::NoSession` for a connection without a session.
    pub fn accept_bound(
        &self,
        conn_id: &str,
        user_id: &str,
        client_key: &str,
    ) -> Result<bool, Error> {
        use dashmap::mapref::entry::Entry;
        let shared = &self.shared;
        if shared.jwt_config.is_some() {
            return Err(Error::AcceptsByToken);
        }
        let key = signing::ClientKey::from_base64(client_key).ok_or(Error::BadKey)?;
        if !shared.conn_last_activity.contains_key(conn_id) {
            return Ok(false);
        }
        if !shared.conn_sealed.contains(conn_id) {
            return Err(Error::NoSession);
        }
        let proved = match shared.binds.entry(conn_id.to_owned()) {
            Entry::Occupied(entry) if matches!(entry.get(), Bind::Expected { .. }) => {
                return Ok(true); // accepted already
            }
            Entry::Occupied(entry) => entry.remove(),
            Entry::Vacant(entry) => {
                // The proof comes later: it admits the connection then.
                entry.insert(Bind::Expected {
                    point: key.point().to_vec(),
                    user_id: user_id.to_owned(),
                });
                return Ok(true);
            }
        };
        if !matches!(&proved, Bind::Proved(point) if point.as_slice() == key.point()) {
            let reason = if matches!(proved, Bind::Proved(_)) {
                "proof_invalid"
            } else {
                "proof_missing"
            };
            self.command(ServerCommand::Refuse {
                conn_id: conn_id.to_owned(),
                message: build_auth_failed(reason),
                code: CLOSE_AUTH_FAILED,
            })?;
            return Ok(false);
        }
        shared.register_user(conn_id, user_id);
        if let Some(ref pm) = shared.presence {
            pm.register_connection(conn_id, user_id);
        }
        let jkt = crate::protocol::signing::thumbprint(key.point()).unwrap_or_default();
        shared.client_keys.insert(conn_id.to_owned(), Arc::new(key));
        let ready = build_server_ready(conn_id, user_id, shared, Some(&jkt));
        self.command(ServerCommand::Accept {
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
        // send of its own. Its place is taken now, so that two sends of it at
        // once go once, and given back when it does not go.
        let taken = match event.get("id").and_then(serde_json::Value::as_str) {
            Some(id) => {
                let key = format!("{conn_id}\0{id}");
                let mut dedup = shared.dedup.lock().unwrap_or_else(|e| e.into_inner());
                if !dedup.seen.insert(key.clone()) {
                    return Ok(0);
                }
                dedup.queue.push_back(key.clone());
                while dedup.queue.len() > dedup.max_entries {
                    if let Some(old) = dedup.queue.pop_front() {
                        dedup.seen.remove(&old);
                    }
                }
                Some(key)
            }
            None => None,
        };
        let sent = self.queue_event(conn_id, event, compression_threshold);
        if sent.is_err()
            && let Some(key) = taken
        {
            let mut dedup = shared.dedup.lock().unwrap_or_else(|e| e.into_inner());
            if dedup.seen.remove(&key) {
                if dedup.queue.back() == Some(&key) {
                    dedup.queue.pop_back();
                } else {
                    dedup.queue.retain(|k| *k != key);
                }
            }
        }
        sent
    }

    /// The event in its envelope, in the connection's format, queued.
    fn queue_event(
        &self,
        conn_id: &str,
        event: serde_json::Map<String, serde_json::Value>,
        compression_threshold: usize,
    ) -> Result<usize, Error> {
        let shared = &self.shared;
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
            let ts = crate::rfc3339::now(3);
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
        to: Option<&str>,
    ) -> Result<String, Error> {
        let signer = self.signer.as_ref().ok_or(Error::NoSigningKey)?;
        if to.is_some_and(str::is_empty) {
            return Err(Error::Encode(
                "a signed message's user (to) is empty".to_owned(),
            ));
        }
        signer
            .sign_event(event_type, payload, topic, to, signing::now_ms())
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
