//! Delivery of one message to many connections (broadcast, topics, queue
//! groups), topic patterns and recovery stamping.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use ahash::AHashSet;
use bytes::Bytes;
use dashmap::{DashMap, DashSet};

use tokio_tungstenite::tungstenite::protocol::Message;

use super::e2e::{e2e_frame, e2e_message};
use super::frames::{encode_ws_frame, ws_frame_size};
use super::state::Seal;
use super::{ConnectionHandle, QueueGroup, WsFrame};
use crate::wse::msgpack::serde_json_to_rmpv;

// ---------------------------------------------------------------------------
// Glob matching
// ---------------------------------------------------------------------------

pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pb = pattern.as_bytes();
    let tb = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star_p, mut star_t) = (usize::MAX, 0usize);

    while ti < tb.len() {
        if pi < pb.len() && (pb[pi] == b'?' || pb[pi] == tb[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pb.len() && pb[pi] == b'*' {
            star_p = pi;
            star_t = ti;
            pi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_t += 1;
            ti = star_t;
        } else {
            return false;
        }
    }
    while pi < pb.len() && pb[pi] == b'*' {
        pi += 1;
    }
    pi == pb.len()
}

// ---------------------------------------------------------------------------
// Topic ACL
// ---------------------------------------------------------------------------

/// A connection's topic access control list, from the JWT `wse_topics`
/// claim or `set_topic_acl()`.
///
/// Format in JWT: `{"wse_topics": {"allow": ["chat.*"], "deny": ["admin.*"]}}`
///
/// Rules:
/// - Deny takes precedence over allow
/// - Empty allow list = allow all (backward compatible)
/// - Patterns use glob matching (*, ?)
#[derive(Debug, Clone)]
pub(crate) struct TopicAcl {
    pub(crate) allow: Vec<String>,
    pub(crate) deny: Vec<String>,
}

impl TopicAcl {
    /// An ACL that allows no topic at all.
    pub(crate) fn deny_all() -> Self {
        Self {
            allow: Vec::new(),
            deny: vec!["*".to_string()],
        }
    }

    /// Check if a topic is allowed by this ACL.
    ///
    /// Deny rules take precedence. Empty allow = allow all.
    pub(crate) fn is_allowed(&self, topic: &str) -> bool {
        // Check deny first (deny takes precedence)
        for pattern in &self.deny {
            if glob_match(pattern, topic) {
                return false;
            }
        }

        // Empty allow = allow all
        if self.allow.is_empty() {
            return true;
        }

        // Check allow patterns
        for pattern in &self.allow {
            if glob_match(pattern, topic) {
                return true;
            }
        }

        false
    }
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

/// A message going to many connections: what the application wrote, and its
/// frame, built once and shared by every connection that gets it as is.
pub(crate) struct Outgoing {
    payload: Bytes,
    frame: Bytes,
    /// JSON text, which the server may send as msgpack.
    text: bool,
    /// The message as `M:` msgpack and its frame, for the connections that
    /// asked for msgpack: made on first use, then shared by all of them.
    /// None when the text is not JSON: they get it as is then.
    packed: OnceLock<Option<(Bytes, Bytes)>>,
}

impl Outgoing {
    pub(crate) fn text(payload: Bytes) -> Self {
        let frame = encode_ws_frame(0x01, &payload);
        Self {
            payload,
            frame,
            text: true,
            packed: OnceLock::new(),
        }
    }

    pub(crate) fn binary(payload: Bytes) -> Self {
        let frame = encode_ws_frame(0x02, &payload);
        Self {
            payload,
            frame,
            text: false,
            packed: OnceLock::new(),
        }
    }

    /// The payload and frame `h` gets: msgpack when it asked for msgpack.
    fn for_handle(&self, h: &ConnectionHandle) -> (&Bytes, &Bytes) {
        if h.msgpack
            && self.text
            && let Some((payload, frame)) = self.packed.get_or_init(|| {
                let payload = to_msgpack(&self.payload)?;
                let frame = encode_ws_frame(0x02, &payload);
                Some((payload, frame))
            })
        {
            return (payload, frame);
        }
        (&self.payload, &self.frame)
    }
}

/// JSON text as an `M:` msgpack message, what a client that asked for msgpack
/// reads; None when the text is not JSON.
pub(crate) fn to_msgpack(json: &[u8]) -> Option<Bytes> {
    let value: serde_json::Value = serde_json::from_slice(json).ok()?;
    let mut buf = Vec::with_capacity(json.len());
    buf.extend_from_slice(b"M:");
    rmpv::encode::write_value(&mut buf, &serde_json_to_rmpv(&value)).ok()?;
    Some(Bytes::from(buf))
}

impl ConnectionHandle {
    /// Deliver `msg` to this connection: sealed with its E2E key when it has
    /// one (an `E:` frame of its own), else the shared frame. Every path that
    /// sends one message to many connections comes through here, so none
    /// skips a connection's encryption. A connection over its outbound budget
    /// drops it (counted in `slow_drops`). True when queued.
    pub(crate) fn deliver(
        &self,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> bool {
        if self.tx.is_closed() {
            return false;
        }
        if max_pending > 0 && self.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let (payload, frame) = msg.for_handle(self);
        match self.cipher.get() {
            None => self.push_frame(frame),
            Some(Seal::Legacy(cipher)) => match e2e_frame(cipher, payload) {
                Some(frame) => self.push_frame(&frame),
                None => return false,
            },
            Some(Seal::Session) => return self.queue(WsFrame::Seal(payload.clone())),
        }
        true
    }

    /// Queue a frame on the control queue, counted in the outbound budget.
    fn queue(&self, frame: WsFrame) -> bool {
        let bytes = ws_frame_size(&frame);
        self.pending.fetch_add(bytes, Ordering::Relaxed);
        if self.tx.send(frame).is_err() {
            self.pending.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Queue a message the server makes on its own (a ping, a reply, an
    /// error): sealed in an E2E session, where the client accepts nothing
    /// else, as is otherwise (older E2E clients take these in plaintext).
    pub(crate) fn send_control(&self, msg: Message) -> bool {
        match (self.cipher.get(), msg) {
            (Some(Seal::Session), Message::Text(text)) => {
                self.queue(WsFrame::Seal(Bytes::from(text)))
            }
            (Some(Seal::Session), Message::Binary(data)) => self.queue(WsFrame::Seal(data)),
            (_, msg) => self.queue(WsFrame::Msg(msg)),
        }
    }

    /// Send one message to this connection alone, in order with its other
    /// direct sends: sealed with its E2E key when it has one. A connection
    /// over its outbound budget drops it (counted in `slow_drops`). True
    /// when queued.
    pub(crate) fn send_one(
        &self,
        msg: Message,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> bool {
        if max_pending > 0 && self.pending.load(Ordering::Relaxed) >= max_pending {
            slow_drops.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        // JSON text in msgpack, for a connection that asked for msgpack.
        let msg = match msg {
            Message::Text(text) if self.msgpack => match to_msgpack(text.as_bytes()) {
                Some(packed) => Message::Binary(packed),
                None => Message::Text(text),
            },
            msg => msg,
        };
        let frame = match (self.cipher.get(), msg) {
            (Some(Seal::Legacy(cipher)), Message::Text(text)) => {
                match e2e_message(cipher, text.as_bytes()) {
                    Some(sealed) => WsFrame::Msg(sealed),
                    None => return false,
                }
            }
            (Some(Seal::Legacy(cipher)), Message::Binary(data)) => match e2e_message(cipher, &data)
            {
                Some(sealed) => WsFrame::Msg(sealed),
                None => return false,
            },
            (Some(Seal::Session), Message::Text(text)) => WsFrame::Seal(Bytes::from(text)),
            (Some(Seal::Session), Message::Binary(data)) => WsFrame::Seal(data),
            (_, msg) => WsFrame::Msg(msg),
        };
        self.queue(frame)
    }

    /// Append a frame to the broadcast buffer; the write task is woken when
    /// the buffer was empty.
    fn push_frame(&self, frame: &Bytes) {
        self.pending.fetch_add(frame.len(), Ordering::Relaxed);
        let was_empty = {
            let mut buf = self.broadcast_buf.lock();
            let empty = buf.is_empty();
            buf.push(frame);
            empty
        };
        if was_empty {
            self.broadcast_notify.notify_one();
        }
    }
}

/// Connections from which a broadcast is split across the runtime's threads.
pub(super) const PARALLEL_FANOUT_AT: usize = 4096;
/// Connections each part of a split broadcast delivers to, at least.
pub(super) const FANOUT_PART: usize = 1024;

/// Deliver `msg` to every connection of `list` in `parts` on the runtime's
/// own threads (their wakes stay on the runtime's queues), the first part
/// on this task; returns when every part is done, so a next message starts
/// after it and each connection gets messages in order.
pub(super) async fn deliver_in_parts(
    list: Arc<Vec<ConnectionHandle>>,
    msg: Arc<Outgoing>,
    parts: usize,
    max_pending: usize,
    slow_drops: Arc<AtomicU64>,
) {
    let size = list.len().div_ceil(parts.max(1));
    let mut others = Vec::with_capacity(parts.saturating_sub(1));
    for part in 1..parts {
        let (list, msg, slow_drops) = (list.clone(), msg.clone(), slow_drops.clone());
        others.push(tokio::spawn(async move {
            let start = (part * size).min(list.len());
            let end = ((part + 1) * size).min(list.len());
            for h in &list[start..end] {
                h.deliver(&msg, max_pending, &slow_drops);
            }
        }));
    }
    for h in &list[..size.min(list.len())] {
        h.deliver(&msg, max_pending, &slow_drops);
    }
    for part in others {
        let _ = part.await;
    }
}

/// Deliver `msg` to every connection.
pub(super) fn fanout_broadcast_direct(
    conns: &HashMap<String, ConnectionHandle>,
    msg: &Outgoing,
    max_pending: usize,
    slow_drops: &AtomicU64,
) {
    for h in conns.values() {
        h.deliver(msg, max_pending, slow_drops);
    }
}

/// Who gets a topic's messages: its subscribers, and the subscribers of the
/// patterns (names with `*` or `?`) matching it. Shared by the server and
/// the cluster's inbound links.
#[derive(Clone, Default)]
pub(crate) struct TopicIndex {
    pub(crate) subscribers: Arc<DashMap<String, DashMap<String, ConnectionHandle>>>,
    /// The subscribed names that are patterns. Changed only under the shard
    /// lock of the pattern's `subscribers` entry, so the two never disagree.
    pub(crate) patterns: Arc<DashSet<String>>,
    /// Per-connection topic ACLs: a pattern subscription gets only the topics
    /// its connection's ACL allows.
    pub(crate) acls: Arc<DashMap<String, TopicAcl>>,
}

pub(crate) fn is_pattern(topic: &str) -> bool {
    topic.contains(['*', '?'])
}

impl TopicIndex {
    /// Add `conn_id` to `topic`; true when it was not subscribed yet.
    pub(crate) fn subscribe(&self, topic: &str, conn_id: &str, handle: &ConnectionHandle) -> bool {
        let subs = self.subscribers.entry(topic.to_owned()).or_default();
        let added = subs.insert(conn_id.to_owned(), handle.clone()).is_none();
        if added && subs.len() == 1 && is_pattern(topic) {
            self.patterns.insert(topic.to_owned());
        }
        added
    }

    /// Remove `conn_id` from `topic`; true when the topic has no subscriber
    /// left.
    pub(crate) fn unsubscribe(&self, topic: &str, conn_id: &str) -> bool {
        if let dashmap::Entry::Occupied(subs) = self.subscribers.entry(topic.to_owned()) {
            subs.get().remove(conn_id);
            if subs.get().is_empty() {
                subs.remove();
                if is_pattern(topic) {
                    self.patterns.remove(topic);
                }
                return true;
            }
        }
        false
    }

    /// Deliver `msg` on `topic`. A connection subscribed several ways gets it
    /// once; a pattern subscriber only when its ACL allows `topic`. Returns
    /// (delivered, dropped).
    pub(crate) fn fanout(
        &self,
        topic: &str,
        msg: &Outgoing,
        max_pending: usize,
        slow_drops: &AtomicU64,
    ) -> (u64, u64) {
        let mut delivered = 0u64;
        let mut dropped = 0u64;
        let mut count = |sent: bool| {
            if sent {
                delivered += 1;
            } else {
                dropped += 1;
            }
        };
        if self.patterns.is_empty() {
            // Exact topics only: the common case, no bookkeeping.
            if let Some(subs) = self.subscribers.get(topic) {
                for entry in subs.iter() {
                    count(entry.value().deliver(msg, max_pending, slow_drops));
                }
            }
            return (delivered, dropped);
        }
        // Connections already served, by the identity of their queue.
        let mut served: AHashSet<usize> = AHashSet::new();
        let id = |h: &ConnectionHandle| Arc::as_ptr(&h.pending) as usize;
        if let Some(subs) = self.subscribers.get(topic) {
            for entry in subs.iter() {
                let h = entry.value();
                if served.insert(id(h)) {
                    count(h.deliver(msg, max_pending, slow_drops));
                }
            }
        }
        let matching: Vec<String> = self
            .patterns
            .iter()
            .filter(|pattern| pattern.as_str() != topic && glob_match(pattern, topic))
            .map(|pattern| pattern.key().clone())
            .collect();
        for pattern in matching {
            let Some(subs) = self.subscribers.get(&pattern) else {
                continue;
            };
            for entry in subs.iter() {
                let h = entry.value();
                let allowed = self
                    .acls
                    .get(entry.key())
                    .is_none_or(|acl| acl.is_allowed(topic));
                if allowed && served.insert(id(h)) {
                    count(h.deliver(msg, max_pending, slow_drops));
                }
            }
        }
        (delivered, dropped)
    }
}

/// Deliver `msg` to one member of each queue group of `topic`, round-robin;
/// a member that is gone or over its budget is passed over. Returns
/// (delivered, dropped).
pub(crate) fn fanout_queue_groups(
    queue_groups: &DashMap<String, DashMap<String, QueueGroup>>,
    topic: &str,
    msg: &Outgoing,
    max_pending: usize,
    slow_drops: &AtomicU64,
) -> (u64, u64) {
    let mut delivered = 0u64;
    let mut dropped = 0u64;
    let Some(topic_groups) = queue_groups.get(topic) else {
        return (0, 0);
    };
    for group_entry in topic_groups.iter() {
        let group = group_entry.value();
        let len = group.members.len();
        if len == 0 {
            continue;
        }
        let base = group.next.fetch_add(1, Ordering::Relaxed);
        let served = (0..len).any(|attempt| {
            let (_, ref h) = group.members[(base + attempt) % len];
            h.deliver(msg, max_pending, slow_drops)
        });
        if served {
            delivered += 1;
        } else {
            dropped += 1;
        }
    }
    (delivered, dropped)
}

/// A recovery-stamped message without its stamp: what queue-group members
/// get, since each sees only some of the topic's offsets and would take the
/// rest for gaps. Messages without a stamp come back unchanged.
pub(crate) fn unstamped(payload: &Bytes) -> Bytes {
    let Ok(text) = std::str::from_utf8(payload) else {
        return payload.clone();
    };
    let Some(rest) = text.strip_prefix("{\"tp\":") else {
        return payload.clone();
    };
    // The topic is a JSON string: skip it, escapes included.
    let mut chars = rest.char_indices();
    if !matches!(chars.next(), Some((_, '"'))) {
        return payload.clone();
    }
    let mut after_topic = None;
    let mut escaped = false;
    for (i, c) in chars {
        match (escaped, c) {
            (true, _) => escaped = false,
            (false, '\\') => escaped = true,
            (false, '"') => {
                after_topic = Some(i + 1);
                break;
            }
            _ => {}
        }
    }
    let Some(after_topic) = after_topic else {
        return payload.clone();
    };
    // ,"e":"xxxxxxxx","o":<digits> then `,` or `}`
    let rest = &rest[after_topic..];
    let Some(rest) = rest.strip_prefix(",\"e\":\"") else {
        return payload.clone();
    };
    if rest.len() < 8 || !rest.as_bytes()[..8].iter().all(u8::is_ascii_hexdigit) {
        return payload.clone();
    }
    let Some(rest) = rest[8..].strip_prefix("\",\"o\":") else {
        return payload.clone();
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return payload.clone();
    }
    let rest = &rest[digits..];
    let body = rest.strip_prefix(',').unwrap_or(rest);
    let mut out = String::with_capacity(1 + body.len());
    out.push('{');
    out.push_str(body);
    Bytes::from(out)
}

/// Weave this message's recovery coordinates -- topic (`tp`), epoch (`e`, as an
/// 8-hex string) and offset (`o`) -- in as the first members of the JSON object,
/// right after the opening brace (the same position inject_category writes `c`).
/// Topic broadcasts are JSON objects produced by inject_category, so they always
/// start with `{`; anything else is returned unchanged. These fields let the
/// client dedup idempotently across reconnects within an epoch, detect gaps
/// (offset jumps), and request recovery from its last-seen (epoch, offset).
pub(crate) fn stamp_recovery_fields(json: &str, topic: &str, epoch: u32, offset: u64) -> String {
    let trimmed = json.trim_start();
    match trimmed.strip_prefix('{') {
        Some(rest) => {
            let sep = if rest.trim_start().starts_with('}') {
                ""
            } else {
                ","
            };
            let topic_json = serde_json::to_string(topic).unwrap_or_else(|_| "\"\"".to_string());
            format!("{{\"tp\":{topic_json},\"e\":\"{epoch:08x}\",\"o\":{offset}{sep}{rest}")
        }
        None => json.to_owned(),
    }
}
