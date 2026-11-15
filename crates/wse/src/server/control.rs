//! What an application asks of its server besides sending: subscriptions,
//! topic rights, its connections, presence, signing and tokens.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use zeroize::Zeroizing;

use super::TopicAcl;
use super::handler::Connect;
use super::publisher::{Error, Publisher};
use super::state::{ConnectionHandle, ServerCommand, SharedState};
use super::subscribe::TopicRecovery;
use crate::jwt::{self, JwtConfig};

/// How a connection subscribes to its topics.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Subscribe {
    /// One member of the group gets each message, in turn.
    pub queue_group: Option<String>,
    /// The connection's user is present on the topics, with this data.
    pub presence: Option<serde_json::Value>,
}

impl Subscribe {
    pub fn queue_group(group: impl Into<String>) -> Self {
        Self {
            queue_group: Some(group.into()),
            presence: None,
        }
    }

    pub fn presence(data: serde_json::Value) -> Self {
        Self {
            queue_group: None,
            presence: Some(data),
        }
    }
}

/// Where a topic a connection subscribed to with recovery stands.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Recovered {
    /// The topic's history epoch (8 hex digits), when it has one.
    pub epoch: Option<String>,
    /// The topic's head offset: the client's position from now on.
    pub offset: u64,
    /// Whether the topic keeps a history at all.
    pub recoverable: bool,
    /// Whether every message after the client's position was replayed.
    pub recovered: bool,
    /// Messages replayed.
    pub count: usize,
    /// Whether the connection was subscribed (its ACL allows the topic).
    pub subscribed: bool,
}

/// A user present on a topic.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Member {
    pub user_id: String,
    pub data: serde_json::Value,
    /// The user's connections on the topic.
    pub connections: usize,
}

/// New keys for the tokens a server validates (key rotation). Its `Debug`
/// print leaves the keys out.
#[derive(Clone)]
#[non_exhaustive]
pub struct JwtKeys {
    pub secret: Vec<u8>,
    pub previous_secret: Option<Vec<u8>>,
    pub key_id: Option<String>,
    pub private_key: Option<Vec<u8>>,
}

impl std::fmt::Debug for JwtKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set = |key: bool| if key { "Some(<redacted>)" } else { "None" };
        f.debug_struct("JwtKeys")
            .field("secret", &format_args!("<redacted>"))
            .field(
                "previous_secret",
                &format_args!("{}", set(self.previous_secret.is_some())),
            )
            .field("key_id", &self.key_id)
            .field(
                "private_key",
                &format_args!("{}", set(self.private_key.is_some())),
            )
            .finish()
    }
}

impl JwtKeys {
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            previous_secret: None,
            key_id: None,
            private_key: None,
        }
    }
}

/// Subscribe `conn_id`, whose `handle` the caller has, to `topics`.
fn subscribe(
    shared: &SharedState,
    conn_id: &str,
    handle: &ConnectionHandle,
    topics: Vec<String>,
    options: &Subscribe,
) -> Result<Vec<String>, Error> {
    let accepted = shared
        .admit_topics(conn_id, topics)
        .map_err(|max| Error::TooManyTopics(conn_id.to_owned(), max))?;
    shared.subscribe_topics(conn_id, handle, &accepted, options.queue_group.as_deref());
    // Gone meanwhile: its cleanup (it removes the activity entry first) may
    // have run before the subscribe, which would outlive it.
    if !shared.conn_last_activity.contains_key(conn_id) {
        shared.unsubscribe_topics(conn_id, None);
        return Ok(Vec::new());
    }
    if let Some(data) = &options.presence {
        shared.track_presence(conn_id, &accepted, data);
    }
    Ok(accepted)
}

fn recover(
    shared: &SharedState,
    conn_id: &str,
    handle: Option<&ConnectionHandle>,
    topics: Vec<String>,
    positions: &HashMap<String, (u32, u64)>,
) -> Result<HashMap<String, Recovered>, Error> {
    let (accepted, recovered) = match handle {
        Some(handle) => shared
            .subscribe_and_recover(conn_id, handle, topics.clone(), positions)
            .map_err(|max| Error::TooManyTopics(conn_id.to_owned(), max))?,
        None => (Vec::new(), HashMap::new()),
    };
    // Gone meanwhile: what it subscribed would outlive it.
    let (accepted, recovered) = if shared.conn_last_activity.contains_key(conn_id) {
        (accepted, recovered)
    } else {
        shared.unsubscribe_topics(conn_id, None);
        (Vec::new(), HashMap::new())
    };
    Ok(topics
        .into_iter()
        .map(|topic| {
            let r = recovered.get(&topic).copied().unwrap_or(TopicRecovery {
                epoch: None,
                offset: 0,
                recovered: false,
                recoverable: false,
                count: 0,
            });
            let subscribed = accepted.contains(&topic);
            let result = Recovered {
                epoch: r.epoch_hex(),
                offset: r.offset,
                recoverable: r.recoverable,
                recovered: r.recovered,
                count: r.count,
                subscribed,
            };
            (topic, result)
        })
        .collect())
}

impl Connect<'_> {
    /// Subscribe the connection to `topics` as the application learns of
    /// it: the topics its ACL allows; the ones it took.
    pub fn subscribe(
        &self,
        topics: Vec<String>,
        options: &Subscribe,
    ) -> Result<Vec<String>, Error> {
        subscribe(self.state, self.conn_id, self.handle, topics, options)
    }

    /// Set the connection's topic ACL before it subscribes to anything.
    pub fn set_topic_acl(&self, allow: Option<Vec<String>>, deny: Option<Vec<String>>) {
        let acl = match allow {
            Some(allow) if allow.is_empty() => TopicAcl::deny_all(),
            allow => TopicAcl {
                allow: allow.unwrap_or_default(),
                deny: deny.unwrap_or_default(),
            },
        };
        self.state.topics.acls.insert((**self.conn_id).clone(), acl);
    }
}

impl Publisher {
    /// The connections registered now.
    pub fn connections(&self) -> Vec<String> {
        // An activity entry lives from a connection's registration to its
        // cleanup.
        self.shared
            .conn_last_activity
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn connection_count(&self) -> usize {
        self.shared.connection_count.load(Ordering::Relaxed)
    }

    /// Whether the server is draining: it closes its connections and takes
    /// no new one.
    pub fn is_draining(&self) -> bool {
        self.shared.draining.load(Ordering::Relaxed)
    }

    /// Close a connection (1000). Its messages after that are not delivered,
    /// and a client that does not answer within 5 seconds is dropped.
    pub fn disconnect(&self, conn_id: &str) -> Result<(), Error> {
        self.command(ServerCommand::Disconnect {
            conn_id: conn_id.to_owned(),
        })
    }

    /// The connections of a user: every tab and device it has open here.
    pub fn user_connections(&self, user_id: &str) -> Vec<String> {
        self.shared
            .user_conns
            .get(user_id)
            .map_or_else(Vec::new, |conns| conns.iter().cloned().collect())
    }

    /// Send `text` to every connection of a user. Returns how many it went to.
    pub fn send_to_user(&self, user_id: &str, text: &str) -> Result<usize, Error> {
        let conns = self.user_connections(user_id);
        for conn_id in &conns {
            self.send(conn_id, text)?;
        }
        Ok(conns.len())
    }

    /// Send an event (`send_event`) to every connection of a user, each in
    /// its own format. Returns how many it went to.
    pub fn send_event_to_user(
        &self,
        user_id: &str,
        event: &serde_json::Map<String, serde_json::Value>,
        compression_threshold: usize,
    ) -> Result<usize, Error> {
        let conns = self.user_connections(user_id);
        let mut sent = 0;
        for conn_id in &conns {
            if self.send_event(conn_id, event.clone(), compression_threshold)? > 0 {
                sent += 1;
            }
        }
        Ok(sent)
    }

    /// Close every connection of a user (1000): a logout, a revoked
    /// session, a banned account. Returns how many it closed.
    pub fn disconnect_user(&self, user_id: &str) -> Result<usize, Error> {
        let conns = self.user_connections(user_id);
        for conn_id in &conns {
            self.disconnect(conn_id)?;
        }
        Ok(conns.len())
    }

    /// Subscribe a connection to `topics`: the ones its ACL allows, within
    /// its limit. Returns the topics it took; none when it is gone. It does
    /// not wait: a `Handler` may call it for its own connection (keep
    /// `Connect::conn_id` in the handler's `Connection`).
    pub fn subscribe(
        &self,
        conn_id: &str,
        topics: Vec<String>,
        options: &Subscribe,
    ) -> Result<Vec<String>, Error> {
        let handle = self.shared.connections.read().get(conn_id).cloned();
        match handle {
            Some(handle) => subscribe(&self.shared, conn_id, &handle, topics, options),
            None => Ok(Vec::new()),
        }
    }

    /// Subscribe a connection to `topics`, each replaying what the
    /// connection missed after its position in `positions` (topic ->
    /// (epoch, offset)); a topic without a position only subscribes.
    pub fn subscribe_with_recovery(
        &self,
        conn_id: &str,
        topics: Vec<String>,
        positions: &HashMap<String, (u32, u64)>,
    ) -> Result<HashMap<String, Recovered>, Error> {
        let handle = self.shared.connections.read().get(conn_id).cloned();
        recover(&self.shared, conn_id, handle.as_ref(), topics, positions)
    }

    /// Unsubscribe a connection from `topics`, or from every topic.
    pub fn unsubscribe(&self, conn_id: &str, topics: Option<&[String]>) {
        self.shared.unsubscribe_topics(conn_id, topics);
    }

    /// A connection's topic ACL: the topics it may subscribe to (`allow`
    /// globs; none: every topic; an empty list: no topic) less `deny`.
    /// False when the connection is gone.
    pub fn set_topic_acl(
        &self,
        conn_id: &str,
        allow: Option<Vec<String>>,
        deny: Option<Vec<String>>,
    ) -> bool {
        let shared = &self.shared;
        // Only for a registered connection: its cleanup removes the entry.
        if !shared.conn_last_activity.contains_key(conn_id) {
            return false;
        }
        let acl = match allow {
            Some(allow) if allow.is_empty() => TopicAcl::deny_all(),
            allow => TopicAcl {
                allow: allow.unwrap_or_default(),
                deny: deny.unwrap_or_default(),
            },
        };
        shared.topics.acls.insert(conn_id.to_owned(), acl);
        // Gone meanwhile: its cleanup may have run before the insert.
        if !shared.conn_last_activity.contains_key(conn_id) {
            shared.topics.acls.remove(conn_id);
            return false;
        }
        true
    }

    /// This server's subscribers of `topic`.
    pub fn topic_subscriber_count(&self, topic: &str) -> usize {
        self.shared
            .topics
            .subscribers
            .get(topic)
            .map_or(0, |subscribers| subscribers.len())
    }

    /// The queue groups of `topic`, each with its members.
    pub fn queue_groups(&self, topic: &str) -> Vec<(String, usize)> {
        match self.shared.queue_groups.get(topic) {
            Some(groups) => groups
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().members.len()))
                .collect(),
            None => Vec::new(),
        }
    }

    /// Who is present on `topic`.
    pub fn presence(&self, topic: &str) -> Vec<Member> {
        match &self.shared.presence {
            Some(pm) => pm
                .query(topic)
                .into_iter()
                .map(|(user_id, data, connections)| Member {
                    user_id,
                    data,
                    connections,
                })
                .collect(),
            None => Vec::new(),
        }
    }

    /// Users and connections present on `topic`.
    pub fn presence_stats(&self, topic: &str) -> (usize, usize) {
        self.shared
            .presence
            .as_ref()
            .map_or((0, 0), |pm| pm.stats(topic))
    }

    /// New presence data for a connection's user, on every topic it is
    /// present on. False over the size limit or for a connection without
    /// presence.
    pub fn update_presence(&self, conn_id: &str, data: &serde_json::Value) -> bool {
        self.shared.update_presence(conn_id, data)
    }

    /// A server-signed event: the `signed` envelope a client checks with
    /// `signing_public_key`. With a `topic`, the signature covers it, and
    /// clients take the message on that topic only. Sign once, send to any
    /// number of connections.
    pub fn sign(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        topic: Option<&str>,
    ) -> Result<String, Error> {
        self.shared.sign(event_type, payload, topic, None)
    }

    /// `sign`, for one user: the signature covers `user_id` (`to`), so the
    /// message proves whom it was for, and only that user's clients take
    /// it. Send it with `send_to_user` or `send_event`.
    pub fn sign_to(
        &self,
        user_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<String, Error> {
        self.shared.sign(event_type, payload, None, Some(user_id))
    }

    /// The server's public key for signed messages (base64 of an
    /// uncompressed P-256 point), when it has a signing key.
    pub fn signing_public_key(&self) -> Option<String> {
        self.shared.signer.as_ref().map(|s| s.public_key.clone())
    }

    /// The public key clients pin for end-to-end encryption: base64 of the
    /// uncompressed P-256 point of `e2e_identity_key`, None without one.
    pub fn e2e_identity_public_key(&self) -> Option<String> {
        use base64::Engine;
        self.shared
            .e2e_identity
            .as_ref()
            .map(|k| base64::engine::general_purpose::STANDARD.encode(k.public_key()))
    }

    /// A connection's public key for its signed messages, or none. False
    /// when the connection is gone.
    pub fn set_client_key(&self, conn_id: &str, public_key: Option<&str>) -> Result<bool, Error> {
        self.shared.set_client_key(conn_id, public_key)
    }

    /// A token this server validates, for `sub`, valid for `ttl`, with the
    /// configured issuer and audience and `claims` besides.
    pub fn issue_token(
        &self,
        sub: &str,
        ttl: Duration,
        claims: serde_json::Map<String, serde_json::Value>,
    ) -> Result<String, Error> {
        let holder = self.shared.jwt_config.as_ref().ok_or(Error::NoJwt)?;
        let cfg = holder.load();
        let key: &[u8] = match cfg.algorithm.key_kind() {
            jwt::KeyKind::Secret { .. } => &cfg.secret,
            _ => cfg.signing_key.as_deref().ok_or_else(|| {
                Error::Jwt(format!(
                    "{:?} signs with jwt_private_key, which this server was not given",
                    cfg.algorithm
                ))
            })?,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut token = serde_json::Map::new();
        token.insert("iat".into(), now.into());
        token.insert("exp".into(), now.saturating_add(ttl.as_secs()).into());
        if !cfg.issuer.is_empty() {
            token.insert("iss".into(), cfg.issuer.clone().into());
        }
        if !cfg.audience.is_empty() {
            token.insert("aud".into(), cfg.audience.clone().into());
        }
        token.extend(claims);
        token.insert("sub".into(), sub.into());
        jwt::jwt_encode(
            &serde_json::Value::Object(token),
            key,
            cfg.algorithm,
            cfg.key_id.as_deref(),
        )
        .map_err(|e| Error::Jwt(format!("JWT encode error: {e}")))
    }

    /// Rotate the keys of the tokens this server validates; connections
    /// already open keep going. The private key it issues tokens with stays
    /// unless a new one is given (a new public key needs it).
    pub fn reload_jwt_keys(&self, keys: JwtKeys) -> Result<(), Error> {
        let holder = self.shared.jwt_config.as_ref().ok_or(Error::NoJwt)?;
        let current = holder.load();
        let alg = current.algorithm;
        let key_error =
            |option: &str, e: jwt::JwtError| Error::BadJwtKey(format!("{option} for {alg:?}: {e}"));
        jwt::check_verification_key(alg, &keys.secret).map_err(|e| key_error("jwt_secret", e))?;
        if let Some(ref key) = keys.previous_secret {
            jwt::check_verification_key(alg, key)
                .map_err(|e| key_error("jwt_previous_secret", e))?;
        }
        if let Some(ref key) = keys.private_key {
            jwt::check_signing_key(alg, key).map_err(|e| key_error("jwt_private_key", e))?;
        } else if current.signing_key.is_some()
            && !matches!(alg.key_kind(), jwt::KeyKind::Secret { .. })
            && keys.secret.as_slice() != current.secret.as_slice()
        {
            // The private key it has belongs to the old public key: tokens
            // issued with it would not verify any more.
            return Err(Error::BadJwtKey(
                "jwt_private_key: a new jwt_secret needs the private key that goes with it, \
                 for issue_token()"
                    .to_owned(),
            ));
        }
        holder.store(Arc::new(JwtConfig {
            algorithm: alg,
            secret: Zeroizing::new(keys.secret),
            previous_secret: keys.previous_secret.map(Zeroizing::new),
            signing_key: keys
                .private_key
                .map(Zeroizing::new)
                .or_else(|| current.signing_key.clone()),
            key_id: keys.key_id.or_else(|| current.key_id.clone()),
            issuer: current.issuer.clone(),
            audience: current.audience.clone(),
            cookie_name: current.cookie_name.clone(),
        }));
        tracing::info!("[WSE] JWT keys reloaded");
        Ok(())
    }
}
