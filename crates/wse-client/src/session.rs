//! The protocol on one client: what the server's messages mean, which reach
//! the application, the topics subscribed and where each stands, and the
//! gaps being recovered. No I/O: what it sends goes in its outbox.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use web_time::Instant;

use crate::client::Shared;
use crate::decode::{self, Envelope, epoch_of};
use crate::error::Error;
use crate::handler::{
    Connected, Event, Gap, GapReason, Handler, Recovery, ServerError, Subscribed,
};
use crate::message::{Message, OwnedMessage, Payload, Position, Signature};
use crate::security::{Keys, Sealing};
use crate::sequencer::{Streams, Verdict};

/// Messages held per topic while a gap is recovered; past it the client
/// stops waiting.
const HELD_MAX: usize = 1000;

/// Recovery requests in a row that got no answer at all before the client
/// stops asking on this connection: the server application does not
/// answer them.
const UNANSWERED_MAX: u32 = 2;

type Map<V> = HashMap<Box<str>, V, foldhash::fast::RandomState>;

/// A stream of a topic waiting for the server to replay what it missed.
struct Hole {
    epoch: u32,
    /// What came past the hole, by offset.
    held: BTreeMap<u64, OwnedMessage>,
    deadline: Instant,
    /// The request out for it, not answered yet.
    request: Option<u64>,
    /// The offset the last request asked from.
    requested_from: Option<u64>,
    /// An answer came for it.
    answered: bool,
}

#[derive(Default)]
struct Topic {
    streams: Streams,
    /// One per stream with a gap: usually none, at most one per node that
    /// publishes on the topic.
    holes: Vec<Hole>,
}

/// A recovery request out. The server answers a connection's requests in
/// order, so each topic's answers settle its requests front to back.
struct Request {
    id: u64,
    /// Asked for a hole, not by a subscription.
    for_hole: bool,
}

/// What the session asks of the connection.
pub(crate) enum Outgoing {
    /// Sealed on a connection with an end-to-end session; never sent there
    /// in plaintext.
    Text(String),
    /// The handshake's own (client_hello, the answer to a ping): as it is
    /// before the session opens, sealed after.
    Handshake(String),
}

pub(crate) struct Session<H> {
    pub(crate) handler: H,
    shared: Arc<Shared>,
    recovery_timeout: Duration,
    /// Topics the client subscribed to: subscribed again on every
    /// connection.
    wanted: BTreeSet<String>,
    topics: Map<Topic>,
    /// The recovery requests out, by topic, oldest first.
    requests: Map<VecDeque<Request>>,
    next_request: u64,
    /// Topics whose history the server said it cannot replay, by epoch.
    unrecoverable: Map<u32>,
    unanswered: u32,
    holes: usize,
    next_deadline: Option<Instant>,
    /// What to send.
    pub(crate) outbox: Vec<Outgoing>,

    keys: Arc<Keys>,

    // The connection's own.
    /// Its end-to-end session.
    pub(crate) sealing: Option<Sealing>,
    /// The connection cannot go on, nor the client.
    pub(crate) fatal: Option<Error>,
    /// The server took the connection.
    pub(crate) ready: bool,
    /// Became ready on the last frame.
    pub(crate) became_ready: bool,
    pub(crate) connected_before: bool,
    server_recovery: bool,
}

impl<H: Handler> Session<H> {
    pub(crate) fn new(
        handler: H,
        shared: Arc<Shared>,
        keys: Arc<Keys>,
        topics: &[String],
        recovery_timeout: Duration,
    ) -> Self {
        Self {
            handler,
            shared,
            keys,
            sealing: None,
            fatal: None,
            recovery_timeout,
            wanted: topics.iter().cloned().collect(),
            topics: Map::default(),
            requests: Map::default(),
            next_request: 0,
            unrecoverable: Map::default(),
            unanswered: 0,
            holes: 0,
            next_deadline: None,
            outbox: Vec::new(),
            ready: false,
            became_ready: false,
            connected_before: false,
            server_recovery: false,
        }
    }

    /// A new connection: nothing of the last one carries over but the
    /// topics and their positions.
    pub(crate) fn open(&mut self, sealing: Option<Sealing>) {
        self.sealing = sealing;
        self.fatal = None;
        self.ready = false;
        self.became_ready = false;
        self.server_recovery = false;
        self.requests.clear();
        self.unrecoverable.clear();
        self.unanswered = 0;
        self.outbox.clear();
        for topic in self.topics.values_mut() {
            for hole in &mut topic.holes {
                hole.request = None;
            }
        }
        // The new connection's server names its own key and id.
        if let Ok(mut server_kid) = self.shared.server_kid.lock() {
            *server_kid = None;
        }
        if let Ok(mut connection) = self.shared.connection.lock() {
            *connection = None;
        }
    }

    /// The connection went away.
    pub(crate) fn close(&mut self) {
        // Keys belong to one connection.
        self.sealing = None;
        self.ready = false;
        self.shared.ready.store(false, Ordering::Release);
        self.requests.clear();
        for topic in self.topics.values_mut() {
            for hole in &mut topic.holes {
                hole.request = None;
            }
        }
    }

    /// The client stops: whatever is held is delivered, its gaps reported.
    pub(crate) fn finish(&mut self) {
        for (topic, epoch) in self.holes_where(|_| true) {
            self.give_up(&topic, epoch, GapReason::Closed);
        }
    }

    /// The holes `pick` picks, by topic and stream.
    fn holes_where(&self, pick: impl Fn(&Hole) -> bool) -> Vec<(Box<str>, u32)> {
        self.topics
            .iter()
            .flat_map(|(name, topic)| {
                topic
                    .holes
                    .iter()
                    .filter(|hole| pick(hole))
                    .map(move |hole| (name.clone(), hole.epoch))
            })
            .collect()
    }

    // -- Frames ------------------------------------------------------------

    /// A JSON message; false when `text` is not one.
    pub(crate) fn on_json(&mut self, text: &str, frame: Option<&Bytes>) -> bool {
        match decode::json_envelope(text) {
            Ok(envelope) => {
                self.on_envelope(envelope, frame);
                true
            }
            Err(e) => {
                tracing::debug!("[WSE] not a message: {e}");
                false
            }
        }
    }

    /// A msgpack message; false when `data` is not one.
    pub(crate) fn on_msgpack(&mut self, data: &[u8], frame: Option<&Bytes>) -> bool {
        match decode::msgpack_envelope(data) {
            Ok(envelope) => {
                self.on_envelope(envelope, frame);
                true
            }
            Err(e) => {
                tracing::debug!("[WSE] not a message: {e}");
                false
            }
        }
    }

    /// A frame that came unsealed on a connection with an end-to-end
    /// session: before the session only the server's answer, an error and
    /// heartbeats may; inside it nothing may.
    pub(crate) fn on_unsealed(&mut self, text: Option<&str>) {
        let envelope = text.and_then(|text| decode::json_envelope(text).ok());
        let open = self.sealing.as_ref().is_some_and(Sealing::is_open);
        let handshake = envelope.as_ref().is_some_and(|e| {
            matches!(
                e.t.as_deref(),
                Some("server_hello" | "error" | "ping" | "PING" | "pong" | "PONG")
            )
        });
        match envelope {
            Some(mut envelope) if handshake && !open => {
                // What the handshake needs, never a topic's message nor the
                // application's: nothing unsealed reaches either.
                envelope.tp = None;
                envelope.e = None;
                envelope.o = None;
                self.dispatch(&envelope.into_message(None));
            }
            _ => {
                let why = if open { "inside" } else { "before" };
                self.fail(Error::Encryption(format!(
                    "a plaintext frame {why} the session"
                )));
            }
        }
    }

    /// The connection and the client end with `error`.
    pub(crate) fn fail(&mut self, error: Error) {
        if self.fatal.is_none() {
            tracing::error!("[WSE] {error}: closing, without falling back to plaintext");
            self.fatal = Some(error);
        }
    }

    fn on_envelope<'a>(&mut self, envelope: Envelope<'a>, frame: Option<&'a Bytes>) {
        if envelope.t.as_deref() == Some("signed") {
            self.on_signed(envelope, frame);
            return;
        }
        if envelope.t.as_deref() == Some("batch") {
            match envelope.p {
                Some(Payload::Json(p)) => {
                    for message in decode::json_batch(p).unwrap_or_default() {
                        let _ = self.on_json(message, frame);
                    }
                }
                Some(Payload::Msgpack(p)) => {
                    for message in decode::msgpack_batch(p).unwrap_or_default() {
                        let _ = self.on_msgpack(message, frame);
                    }
                }
                _ => {}
            }
            return;
        }
        let message = envelope.into_message(frame);
        match (message.position, &message.topic) {
            (Some(at), Some(_)) if !is_protocol(message.kind()) => self.on_stamped(message, at),
            _ => self.dispatch(&message),
        }
    }

    /// A message the server signed: checked against the pinned keys, then
    /// delivered as its signed body says, with the proof. Dropped when the
    /// check fails, or when the body names another topic than the one it
    /// came on.
    fn on_signed<'a>(&mut self, outer: Envelope<'a>, frame: Option<&'a Bytes>) {
        let (Some(body), Some(sig), Some(kid)) = (outer.sb, outer.sig, outer.kid) else {
            tracing::warn!("[WSE] malformed signed message dropped");
            return;
        };
        let verified = match self.keys.check_server(&kid, &body, &sig) {
            Ok(verified) => verified,
            Err(why) => {
                tracing::warn!("[WSE] signed message dropped: {why}");
                return;
            }
        };
        let Ok(signed) = decode::json_envelope(&body) else {
            tracing::warn!("[WSE] signed message dropped: its body is not a JSON object");
            return;
        };
        if signed.t.is_none() {
            tracing::warn!("[WSE] signed message dropped: its body has no type");
            return;
        }
        match (&outer.tp, &signed.tp) {
            (Some(came_on), Some(signed_for)) if came_on != signed_for => {
                tracing::warn!(
                    "[WSE] message signed for topic {signed_for} that came on {came_on} dropped"
                );
                return;
            }
            // Signed for one connection, not for a topic's subscribers.
            (Some(came_on), None) => {
                tracing::warn!("[WSE] message signed for no topic that came on {came_on} dropped");
                return;
            }
            _ => {}
        }
        // The time the server signed, in milliseconds: the one a reader can
        // trust, written as the other clients write it.
        #[derive(serde::Deserialize)]
        struct SignedAt {
            ts: Option<u64>,
        }
        let signed_at = serde_json::from_str::<SignedAt>(&body)
            .ok()
            .and_then(|at| at.ts)
            .map(|ms| Cow::Owned(rfc3339_ms(ms)));
        let signature = Signature {
            body: body.to_string(),
            sig: sig.into_owned(),
            kid: kid.into_owned(),
            verified,
        };
        let id = signed.n;
        let message = Envelope {
            t: signed.t,
            p: signed.p,
            c: outer.c,
            ts: signed_at,
            tp: outer.tp.or(signed.tp),
            e: outer.e,
            o: outer.o,
            ..Envelope::default()
        };
        let mut message = message.into_message(frame);
        message.id = id;
        message.signature = Some(Arc::new(signature));
        match (message.position, &message.topic) {
            (Some(at), Some(_)) if !is_protocol(message.kind()) => self.on_stamped(message, at),
            _ => self.dispatch(&message),
        }
    }

    /// What reaches the application, the server's protocol messages aside.
    fn dispatch(&mut self, message: &Message<'_>) {
        match message.kind() {
            "ping" | "PING" => self.answer_ping(message),
            "PONG" | "pong" => {}
            "server_ready" => self.on_server_ready(message),
            "server_hello" => self.on_server_hello(message),
            "subscription_update" => self.on_subscription_reply(message),
            "error" => self.on_error(message),
            _ => self.handler.message(message),
        }
    }

    // -- Topic messages in order --------------------------------------------

    fn on_stamped(&mut self, message: Message<'_>, at: Position) {
        let name = message.topic.as_deref().unwrap_or_default();
        let topic = match self.topics.get_mut(name) {
            Some(topic) => topic,
            None => self.topics.entry(name.into()).or_default(),
        };
        // A hole in another stream of the topic does not hold this one.
        let held_here = topic.holes.iter().any(|h| h.epoch == at.epoch);
        match topic.streams.check(at) {
            Verdict::Duplicate => {}
            Verdict::Deliver => {
                if !self.unrecoverable.is_empty() {
                    self.unrecoverable.remove(name);
                }
                self.handler.message(&message);
                if held_here {
                    self.drain(name, at.epoch);
                }
            }
            Verdict::Gap if held_here => {
                let Some(hole) = topic.holes.iter_mut().find(|h| h.epoch == at.epoch) else {
                    return;
                };
                hole.held
                    .entry(at.offset)
                    .or_insert_with(|| message.to_owned());
                if hole.held.len() > HELD_MAX {
                    self.give_up(name, at.epoch, GapReason::Overflow);
                }
            }
            Verdict::Gap => {
                if self.unrecoverable.get(name) == Some(&at.epoch) {
                    self.unrecoverable.remove(name);
                    self.skip(name, &message, at, GapReason::NotRecovered);
                } else if !self.start_hole(name, &message, at) {
                    self.skip(name, &message, at, GapReason::Unavailable);
                }
            }
        }
    }

    fn recovery_available(&self) -> bool {
        !self.recovery_timeout.is_zero()
            && self.server_recovery
            && self.unanswered < UNANSWERED_MAX
            && self.ready
    }

    /// Holds `message` and asks the server for what is missing before it.
    fn start_hole(&mut self, name: &str, message: &Message<'_>, at: Position) -> bool {
        if !self.recovery_available() {
            return false;
        }
        let Some(topic) = self.topics.get_mut(name) else {
            return false;
        };
        if topic.holes.iter().any(|h| h.epoch == at.epoch)
            || topic.streams.stream(at.epoch).is_none()
        {
            return false;
        }
        let deadline = Instant::now() + self.recovery_timeout;
        let mut held = BTreeMap::new();
        held.insert(at.offset, message.to_owned());
        topic.holes.push(Hole {
            epoch: at.epoch,
            held,
            deadline,
            request: None,
            requested_from: None,
            answered: false,
        });
        self.holes += 1;
        self.next_deadline = Some(self.next_deadline.map_or(deadline, |d| d.min(deadline)));
        self.request_recovery(name, at.epoch);
        true
    }

    /// Asks the server to replay a topic from the last message of the
    /// stream with the hole: it replays that stream, and every other
    /// stream of the topic, from that point.
    fn request_recovery(&mut self, name: &str, epoch: u32) {
        let Some(topic) = self.topics.get_mut(name) else {
            return;
        };
        let Some(hole) = topic.holes.iter_mut().find(|h| h.epoch == epoch) else {
            return;
        };
        if hole.request.is_some() {
            return;
        }
        let Some(from) = topic.streams.stream(epoch) else {
            return;
        };
        let id = self.next_request;
        self.next_request += 1;
        hole.request = Some(id);
        hole.requested_from = Some(from.offset);
        self.requests
            .entry(name.into())
            .or_default()
            .push_back(Request { id, for_hole: true });
        let request = subscription(&[name], &[(name, from)]);
        self.outbox.push(Outgoing::Text(request));
    }

    /// Delivers what became in order; closes a filled hole.
    fn drain(&mut self, name: &str, epoch: u32) {
        loop {
            let Some(topic) = self.topics.get_mut(name) else {
                return;
            };
            let Some(hole) = topic.holes.iter_mut().find(|h| h.epoch == epoch) else {
                return;
            };
            let Some(next_offset) = topic
                .streams
                .stream(epoch)
                .and_then(|last| last.offset.checked_add(1))
            else {
                break;
            };
            hole.held = hole.held.split_off(&next_offset);
            let Some(next) = hole.held.remove(&next_offset) else {
                break;
            };
            topic.streams.advance(Position {
                epoch,
                offset: next_offset,
            });
            self.handler.message(&next.as_message());
        }
        let filled = self
            .topics
            .get(name)
            .and_then(|t| t.holes.iter().find(|h| h.epoch == epoch))
            .is_some_and(|h| h.held.is_empty());
        if filled {
            self.close_hole(name, epoch);
            tracing::info!("[WSE] recovered the gap on {name}");
        }
    }

    fn close_hole(&mut self, name: &str, epoch: u32) -> Option<Hole> {
        let holes = &mut self.topics.get_mut(name)?.holes;
        let index = holes.iter().position(|h| h.epoch == epoch)?;
        let hole = holes.swap_remove(index);
        self.holes -= 1;
        if self.next_deadline == Some(hole.deadline) || self.holes == 0 {
            self.next_deadline = self.first_deadline();
        }
        Some(hole)
    }

    /// The deadline of the hole that gives up first.
    fn first_deadline(&self) -> Option<Instant> {
        self.topics
            .values()
            .flat_map(|t| t.holes.iter().map(|h| h.deadline))
            .min()
    }

    /// Stops waiting: reports what is missing, delivers what is held.
    fn give_up(&mut self, name: &str, epoch: u32, reason: GapReason) {
        let Some(hole) = self.close_hole(name, epoch) else {
            return;
        };
        let mut last = self
            .topics
            .get(name)
            .and_then(|t| t.streams.stream(epoch))
            .map(|p| p.offset);
        for (offset, message) in hole.held {
            if last.is_some_and(|last| offset <= last) {
                continue;
            }
            if let Some(first) = last.and_then(|last| last.checked_add(1))
                && offset > first
            {
                self.report(name, epoch, Some(first), Some(offset - 1), reason);
            }
            if let Some(topic) = self.topics.get_mut(name) {
                topic.streams.advance(Position { epoch, offset });
            }
            self.handler.message(&message.as_message());
            last = Some(offset);
        }
    }

    /// Takes a gap at once: reports it, delivers `message`.
    fn skip(&mut self, name: &str, message: &Message<'_>, at: Position, reason: GapReason) {
        let Some(topic) = self.topics.get_mut(name) else {
            return;
        };
        let last = topic.streams.stream(at.epoch);
        topic.streams.advance(at);
        if let Some(first) = last.and_then(|last| last.offset.checked_add(1))
            && at.offset > first
        {
            self.report(name, at.epoch, Some(first), Some(at.offset - 1), reason);
        }
        self.handler.message(message);
    }

    fn report(
        &mut self,
        topic: &str,
        epoch: u32,
        first: Option<u64>,
        last: Option<u64>,
        reason: GapReason,
    ) {
        let gap = Gap {
            topic: topic.to_owned(),
            epoch,
            first,
            last,
            reason,
        };
        tracing::warn!(
            "[WSE] {topic} lost offsets {first:?}..{last:?} ({:?} messages, {reason:?})",
            gap.missed()
        );
        self.handler.event(Event::Gap(gap));
    }

    /// When the next hole gives up waiting.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.next_deadline
    }

    /// Holes whose wait is over give up.
    pub(crate) fn on_deadline(&mut self, now: Instant) {
        // Nothing came back for these: the server application may not
        // answer recovery requests. After a few, the client stops asking.
        let unanswered = self
            .holes_where(|h| h.deadline <= now && h.request.is_some() && !h.answered)
            .len();
        self.unanswered = self
            .unanswered
            .saturating_add(u32::try_from(unanswered).unwrap_or(u32::MAX));
        for (name, epoch) in self.holes_where(|h| h.deadline <= now) {
            self.give_up(&name, epoch, GapReason::Timeout);
        }
    }

    // -- Subscriptions -----------------------------------------------------

    /// Subscribes to topics not subscribed yet, from where the client
    /// stands in them.
    pub(crate) fn subscribe(&mut self, topics: Vec<String>) {
        let new: Vec<String> = topics
            .into_iter()
            .filter(|t| !t.is_empty())
            .filter(|t| self.wanted.insert(t.clone()))
            .collect();
        if self.ready && !new.is_empty() {
            self.send_subscription(&new);
        }
    }

    /// Unsubscribes, and forgets where the client stood in the topics: a
    /// later subscription starts from the next message.
    pub(crate) fn unsubscribe(&mut self, topics: Vec<String>) {
        for topic in &topics {
            self.wanted.remove(topic);
            if let Some(state) = self.topics.remove(topic.as_str())
                && !state.holes.is_empty()
            {
                self.holes -= state.holes.len();
                self.next_deadline = self.first_deadline();
            }
            self.requests.remove(topic.as_str());
            self.unrecoverable.remove(topic.as_str());
        }
        if self.ready && !topics.is_empty() {
            let mut text = String::from(
                r#"{"c":"WSE","t":"subscription_update","p":{"action":"unsubscribe","topics":"#,
            );
            push_json(&mut text, &topics);
            text.push_str("},\"v\":1}");
            self.outbox.push(Outgoing::Text(text));
        }
    }

    /// A subscription request for `topics`, with the positions the client
    /// has of them.
    fn send_subscription(&mut self, topics: &[String]) {
        let mut positions = Vec::new();
        if self.server_recovery {
            for topic in topics {
                if let Some(at) = self
                    .topics
                    .get(topic.as_str())
                    .and_then(|t| t.streams.position())
                {
                    positions.push((topic.as_str(), at));
                }
            }
        }
        let now = Instant::now();
        for &(topic, at) in &positions {
            let id = self.next_request;
            self.next_request += 1;
            let hole = self
                .topics
                .get_mut(topic)
                .and_then(|t| t.holes.iter_mut().find(|h| h.epoch == at.epoch));
            let for_hole = hole.is_some();
            if let Some(hole) = hole {
                hole.request = Some(id);
                hole.requested_from = Some(at.offset);
                // The wait starts again with the request: the replay comes
                // after it, not before.
                hole.deadline = now + self.recovery_timeout;
            }
            self.requests
                .entry(topic.into())
                .or_default()
                .push_back(Request { id, for_hole });
        }
        if self.holes > 0 {
            self.next_deadline = self.first_deadline();
        }
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.outbox
            .push(Outgoing::Text(subscription(&names, &positions)));
        // Holes in the topics' other streams ask for themselves.
        for (topic, epoch) in self.holes_where(|h| h.request.is_none()) {
            if topics.iter().any(|t| **t == *topic) {
                self.request_recovery(&topic, epoch);
            }
        }
    }

    fn on_subscription_reply(&mut self, message: &Message<'_>) {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Reply {
            success_topics: Vec<String>,
            failed_topics: Vec<String>,
            recovery: BTreeMap<String, Info>,
        }
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Info {
            epoch: Option<String>,
            offset: Option<u64>,
            recovered: Option<bool>,
            count: u64,
        }
        let Ok(reply) = message.payload::<Reply>() else {
            tracing::debug!("[WSE] unreadable subscription reply dropped");
            return;
        };
        if !reply.failed_topics.is_empty() {
            tracing::warn!("[WSE] subscription refused for {:?}", reply.failed_topics);
        }
        let mut recovery = Vec::with_capacity(reply.recovery.len());
        for (topic, info) in &reply.recovery {
            let epoch = info.epoch.as_deref().and_then(epoch_of);
            let recovered = info.recovered.unwrap_or(true);
            self.on_recovery_reply(topic, recovered, epoch, info.epoch.is_some(), info.offset);
            recovery.push(Recovery {
                topic: topic.clone(),
                recovered,
                count: info.count,
                position: epoch
                    .zip(info.offset)
                    .map(|(epoch, offset)| Position { epoch, offset }),
            });
        }
        self.handler.event(Event::Subscribed(Subscribed {
            topics: reply.success_topics,
            failed: reply.failed_topics,
            recovery,
        }));
    }

    fn on_recovery_reply(
        &mut self,
        name: &str,
        recovered: bool,
        epoch: Option<u32>,
        has_epoch: bool,
        offset: Option<u64>,
    ) {
        let Some(request) = self.requests.get_mut(name).and_then(VecDeque::pop_front) else {
            return; // an answer to a plain subscription: nothing moves
        };
        if self.requests.get(name).is_some_and(VecDeque::is_empty) {
            self.requests.remove(name);
        }
        self.unanswered = 0;
        let hole = self
            .topics
            .get_mut(name)
            .and_then(|t| t.holes.iter_mut().find(|h| h.request == Some(request.id)));
        if let Some(hole) = hole {
            hole.request = None;
            hole.answered = true;
            let (hole_epoch, requested_from) = (hole.epoch, hole.requested_from);
            // The replay came before this answer.
            self.drain(name, hole_epoch);
            let Some(topic) = self
                .topics
                .get(name)
                .filter(|t| t.holes.iter().any(|h| h.epoch == hole_epoch))
            else {
                return; // filled
            };
            let progressed = topic
                .streams
                .stream(hole_epoch)
                .zip(requested_from)
                .is_some_and(|(at, from)| at.offset > from);
            // A replay the server's outbound budget cut short answers
            // `recovered: false` in the same history, past where it was
            // asked from: ask again from where it got to.
            if progressed && (recovered || epoch == Some(hole_epoch)) {
                self.request_recovery(name, hole_epoch);
            } else {
                let reason = if recovered {
                    GapReason::Incomplete
                } else {
                    GapReason::NotRecovered
                };
                self.give_up(name, hole_epoch, reason);
            }
            return;
        }
        if request.for_hole {
            return; // the answer for a hole settled meanwhile
        }
        if recovered {
            return; // the replay moved the position as it came
        }
        let Some(at) = self.topics.get(name).and_then(|t| t.streams.position()) else {
            return;
        };
        match epoch {
            // The server has no history of the topic any more, or a new one:
            // it restarted.
            None if !has_epoch => self.report(name, at.epoch, None, None, GapReason::EpochChanged),
            Some(epoch) if epoch != at.epoch => {
                self.report(name, epoch, None, None, GapReason::EpochChanged)
            }
            _ => {
                if offset.is_some_and(|offset| offset <= at.offset) {
                    return; // nothing was missed
                }
                // The next gap of this stream is reported at once instead of
                // asked for: the server cannot replay it.
                self.unrecoverable.insert(name.into(), at.epoch);
            }
        }
    }

    // -- The server's protocol messages --------------------------------------

    fn answer_ping(&mut self, ping: &Message<'_>) {
        #[derive(serde::Deserialize)]
        struct Ping<'a> {
            #[serde(borrow)]
            server_time: Option<&'a serde_json::value::RawValue>,
            #[serde(borrow)]
            timestamp: Option<&'a serde_json::value::RawValue>,
        }
        let now = now_ms();
        let echoed = ping
            .payload::<Ping<'_>>()
            .ok()
            .and_then(|p| {
                p.server_time
                    .or(p.timestamp)
                    .map(|raw| raw.get().to_owned())
            })
            .unwrap_or_else(|| now.to_string());
        let pong = format!(
            r#"{{"c":"WSE","t":"PONG","p":{{"client_timestamp":{now},"server_timestamp":{echoed}}},"v":1}}"#
        );
        self.outbox.push(Outgoing::Handshake(pong));
    }

    fn on_server_ready(&mut self, message: &Message<'_>) {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Ready {
            connection_id: Option<String>,
            details: Details,
        }
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Details {
            connection_id: Option<String>,
            user_id: Option<String>,
            features: Features,
            signing: Option<SigningKey>,
        }
        let ready = message.payload::<Ready>().unwrap_or_default();
        // Sealed in an end-to-end session: what it names can be trusted.
        if let Some(signing) = ready.details.signing {
            self.take_signing_key(signing.kid);
        }
        let connection_id = ready
            .connection_id
            .or(ready.details.connection_id)
            .unwrap_or_default();
        if ready.details.features.recovery {
            self.server_recovery = true;
        }
        self.take_connection(connection_id, ready.details.user_id);
    }

    fn on_server_hello(&mut self, message: &Message<'_>) {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Hello {
            server_version: Option<String>,
            connection_id: Option<String>,
            features: Features,
            signing: Option<SigningKey>,
            e2e: Option<serde_json::Value>,
        }
        let hello = message.payload::<Hello>().unwrap_or_default();
        if hello.features.recovery {
            self.server_recovery = true;
        }
        if let Some(sealing) = &mut self.sealing {
            let answer = hello
                .e2e
                .as_ref()
                .ok_or_else(|| Error::Encryption("the server did not answer the offer".to_owned()))
                .and_then(|e2e| {
                    sealing.accept(
                        &self.keys,
                        e2e,
                        hello.connection_id.as_deref().unwrap_or(""),
                    )
                });
            match answer {
                Ok(()) => tracing::info!("[WSE] end-to-end session open"),
                Err(error) => self.fail(error),
            }
            // server_hello comes unsealed, and anyone in between may have
            // written it: only the sealed server_ready takes the connection
            // and names the signing key. Nothing goes before it.
            return;
        }
        if let Some(signing) = hello.signing {
            self.take_signing_key(signing.kid);
        }
        // A 3.x server sends server_ready, at once or when its application
        // takes the connection; an older one may never send it.
        let older = hello
            .server_version
            .as_deref()
            .and_then(|v| v.split('.').next())
            .and_then(|major| major.parse::<u32>().ok())
            .is_some_and(|major| major < 3);
        if older && !self.ready {
            self.take_connection(hello.connection_id.unwrap_or_default(), None);
        }
    }

    /// The server's signing key: what this client's signed messages are
    /// for. One it pins, when it pins any.
    fn take_signing_key(&mut self, kid: String) {
        let kid = if self.keys.signs_for(&kid) {
            Some(kid)
        } else {
            tracing::warn!("[WSE] the server names signing key {kid}, which is not pinned");
            None
        };
        if let Ok(mut server_kid) = self.shared.server_kid.lock() {
            *server_kid = kid;
        }
    }

    /// The server took the connection: subscribe again, tell the
    /// application.
    fn take_connection(&mut self, connection_id: String, user_id: Option<String>) {
        if self.ready {
            return;
        }
        self.ready = true;
        self.became_ready = true;
        if let Ok(mut connection) = self.shared.connection.lock() {
            *connection = Some((connection_id.clone(), user_id.clone()));
        }
        self.shared.ready.store(true, Ordering::Release);
        let topics: Vec<String> = self.wanted.iter().cloned().collect();
        if !topics.is_empty() {
            self.send_subscription(&topics);
        }
        tracing::info!(
            "[WSE] connected as {connection_id} (recovery: {})",
            self.server_recovery
        );
        let reconnected = std::mem::replace(&mut self.connected_before, true);
        self.handler.event(Event::Connected(Connected {
            connection_id,
            user_id,
            reconnected,
            recovery: self.server_recovery,
        }));
    }

    fn on_error(&mut self, message: &Message<'_>) {
        let details = message.payload_value().unwrap_or_default();
        let field = |name: &str| {
            details
                .get(name)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let (code, text) = (field("code"), field("message"));
        tracing::warn!("[WSE] server error {code}: {text}");
        if self.sealing.is_some()
            && matches!(code.as_str(), "E2E_UNSUPPORTED" | "KEY_EXCHANGE_FAILED")
        {
            // Another attempt would be refused the same way.
            self.fail(Error::Encryption(format!(
                "the server refused the session: {text}"
            )));
        }
        self.handler.event(Event::ServerError(ServerError {
            code,
            message: text,
            details,
        }));
    }
}

/// Milliseconds since the Unix epoch in RFC 3339, UTC, as the TypeScript
/// client's `toISOString()` writes them.
fn rfc3339_ms(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, of_day) = (secs / 86_400, secs % 86_400);
    // Days since 1970-01-01 to a civil date (H. Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let (era, doe) = (z.div_euclid(146_097), z.rem_euclid(146_097));
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60,
        ms % 1000
    )
}

/// The server's own messages: never a topic's, whatever they carry.
fn is_protocol(kind: &str) -> bool {
    matches!(
        kind,
        "ping"
            | "PING"
            | "pong"
            | "PONG"
            | "server_ready"
            | "server_hello"
            | "subscription_update"
            | "error"
    )
}

#[derive(serde::Deserialize, Default)]
#[serde(default)]
struct Features {
    recovery: bool,
}

#[derive(serde::Deserialize)]
struct SigningKey {
    kid: String,
}

/// A `subscription_update` subscribing to `topics`, recovering from
/// `positions`.
fn subscription(topics: &[&str], positions: &[(&str, Position)]) -> String {
    let mut text =
        String::from(r#"{"c":"WSE","t":"subscription_update","p":{"action":"subscribe","topics":"#);
    push_json(&mut text, topics);
    if !positions.is_empty() {
        text.push_str(r#","recover":true,"recovery":{"#);
        for (i, (topic, at)) in positions.iter().enumerate() {
            if i > 0 {
                text.push(',');
            }
            push_json(&mut text, topic);
            let _ = write!(
                text,
                r#":{{"epoch":"{:08x}","offset":{}}}"#,
                at.epoch, at.offset
            );
        }
        text.push('}');
    }
    text.push_str("},\"v\":1}");
    text
}

fn push_json<T: serde::Serialize + ?Sized>(text: &mut String, value: &T) {
    // Strings and lists of strings: always encodable.
    text.push_str(&serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()));
}

pub(crate) fn now_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The client's heartbeat as a message: where the WebSocket has no pings of
/// its own to send (the browser's), and in an end-to-end session, where only
/// a sealed answer shows the server is alive.
pub(crate) fn ping() -> String {
    format!(
        r#"{{"c":"WSE","t":"ping","p":{{"timestamp":{}}},"v":1}}"#,
        now_ms()
    )
}

/// The first frame of a connection, with the end-to-end offer when it
/// opens a session.
pub(crate) fn client_hello(compression: bool, e2e: Option<serde_json::Value>) -> String {
    let mut p = serde_json::json!({
        "client_version": env!("CARGO_PKG_VERSION"),
        "protocol_version": 1,
        "features": {
            "compression": compression,
            "encryption": e2e.is_some(),
            "batching": true,
        },
    });
    if let Some(e2e) = e2e {
        p["e2e"] = e2e;
    }
    serde_json::json!({"c": "WSE", "t": "client_hello", "p": p, "v": 1}).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// What reached the application.
    #[derive(Default)]
    struct Seen {
        offsets: Vec<u64>,
        events: Vec<Event>,
    }

    impl Handler for Seen {
        fn message(&mut self, message: &Message<'_>) {
            if let Some(at) = message.position() {
                self.offsets.push(at.offset);
            }
        }
        fn event(&mut self, event: Event) {
            self.events.push(event);
        }
    }

    fn session(recovery_timeout: Duration) -> Session<Seen> {
        let shared = Arc::new(Shared {
            ready: AtomicBool::new(false),
            connection: Mutex::new(None),
            token: Mutex::new(None),
            server_kid: Mutex::new(None),
        });
        let keys = Arc::new(Keys::from_config(&crate::Config::new("ws://x")).unwrap());
        let mut session = Session::new(
            Seen::default(),
            shared,
            keys,
            &["t".to_owned()],
            recovery_timeout,
        );
        session.open(None);
        session
    }

    fn ready(session: &mut Session<Seen>, recovery: bool) {
        session.on_json(
            &format!(
                r#"{{"c":"WSE","t":"server_ready","p":{{"details":{{"connection_id":"c1","user_id":"u","features":{{"recovery":{recovery}}}}}}}}}"#
            ),
            None,
        );
    }

    fn feed(session: &mut Session<Seen>, epoch: &str, offsets: &[u64]) {
        for o in offsets {
            session.on_json(
                &format!(r#"{{"tp":"t","e":"{epoch}","o":{o},"c":"U","t":"m","p":{{}}}}"#),
                None,
            );
        }
    }

    /// The texts in the outbox, taken out.
    fn sent(session: &mut Session<Seen>) -> Vec<serde_json::Value> {
        session
            .outbox
            .drain(..)
            .map(|(Outgoing::Text(text) | Outgoing::Handshake(text))| {
                serde_json::from_str(text.as_str()).unwrap()
            })
            .collect()
    }

    fn gaps(session: &Session<Seen>) -> Vec<(Option<u64>, Option<u64>, GapReason)> {
        session
            .handler
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Gap(g) => Some((g.first, g.last, g.reason)),
                _ => None,
            })
            .collect()
    }

    const E: &str = "0000000a";

    /// The server's answer to a recovery request for `t`.
    fn reply(s: &mut Session<Seen>, epoch: &str, offset: u64, recovered: bool, count: u64) {
        s.on_json(
            &format!(
                r#"{{"c":"WSE","t":"subscription_update","p":{{"success_topics":["t"],"recovery":{{"t":{{"epoch":"{epoch}","offset":{offset},"recovered":{recovered},"count":{count}}}}}}}}}"#
            ),
            None,
        );
    }

    #[test]
    fn an_unsealed_handshake_frame_never_reaches_a_topic() {
        let mut s = session(Duration::from_secs(3));
        // Before a session: a ping with a topic's stamp, written by whoever
        // sits in between.
        s.on_unsealed(Some(
            r#"{"t":"ping","tp":"t","e":"0000000a","o":18446744073709551615,"p":{}}"#,
        ));
        assert!(s.handler.offsets.is_empty());
        assert!(
            s.topics
                .get("t")
                .and_then(|t| t.streams.position())
                .is_none()
        );
        // Answered as a ping, by the handshake's own kind of message.
        assert!(matches!(
            s.outbox.as_slice(),
            [Outgoing::Handshake(pong)] if pong.contains("PONG")
        ));
        // A stamped protocol message moves no topic either.
        ready(&mut s, true);
        s.on_json(
            r#"{"t":"error","tp":"t","e":"0000000a","o":99,"p":{"code":"X","message":"y"}}"#,
            None,
        );
        feed(&mut s, E, &[1]);
        assert_eq!(s.handler.offsets, [1]);
    }

    #[test]
    fn a_late_answer_settles_only_its_own_hole() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        sent(&mut s);
        feed(&mut s, E, &[1, 3]);
        // The replay fills the hole before its answer comes.
        feed(&mut s, E, &[2]);
        assert_eq!(s.handler.offsets, [1, 2, 3]);
        // Another hole, another request.
        feed(&mut s, E, &[5]);
        assert_eq!(sent(&mut s).len(), 2);
        // The first request's answer is not the second's.
        reply(&mut s, E, 3, true, 1);
        assert!(gaps(&s).is_empty());
        feed(&mut s, E, &[4]);
        reply(&mut s, E, 5, true, 1);
        assert_eq!(s.handler.offsets, [1, 2, 3, 4, 5]);
        assert!(gaps(&s).is_empty());
        assert_eq!(s.next_deadline(), None);
    }

    #[test]
    fn gaps_in_two_streams_of_a_topic_are_recovered_each() {
        const F: &str = "0000000b";
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        sent(&mut s);
        feed(&mut s, E, &[1]);
        feed(&mut s, F, &[1]);
        feed(&mut s, E, &[3]);
        // A gap in the other stream is held too, not skipped.
        feed(&mut s, F, &[3]);
        assert_eq!(sent(&mut s).len(), 2);
        feed(&mut s, E, &[2]);
        feed(&mut s, F, &[2]);
        reply(&mut s, E, 3, true, 1);
        reply(&mut s, F, 3, true, 1);
        assert_eq!(s.handler.offsets, [1, 1, 2, 3, 2, 3]);
        assert!(gaps(&s).is_empty());
        assert_eq!(s.next_deadline(), None);
    }

    #[test]
    fn a_replay_the_server_cut_short_is_asked_for_again() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        sent(&mut s);
        feed(&mut s, E, &[1, 5]);
        sent(&mut s);
        // The server's outbound budget let one through.
        feed(&mut s, E, &[2]);
        reply(&mut s, E, 2, false, 1);
        assert!(gaps(&s).is_empty());
        let again = sent(&mut s);
        assert_eq!(
            again[0]["p"]["recovery"],
            serde_json::json!({"t": {"epoch": E, "offset": 2}})
        );
        feed(&mut s, E, &[3, 4]);
        reply(&mut s, E, 4, true, 2);
        assert_eq!(s.handler.offsets, [1, 2, 3, 4, 5]);
        assert!(gaps(&s).is_empty());
    }

    #[test]
    fn a_reconnect_gives_a_hole_its_whole_wait_again() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        feed(&mut s, E, &[1, 3]);
        let first = s.next_deadline().unwrap();
        s.close();
        std::thread::sleep(Duration::from_millis(20));
        s.open(None);
        ready(&mut s, true);
        assert!(s.next_deadline().unwrap() > first);
        assert!(gaps(&s).is_empty());
    }

    #[test]
    fn signed_times_read_as_the_other_clients_write_them() {
        assert_eq!(rfc3339_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339_ms(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(rfc3339_ms(1_700_000_000_123), "2023-11-14T22:13:20.123Z");
    }

    #[test]
    fn a_message_signed_for_no_topic_is_not_taken_on_one() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        s.on_json(
            r#"{"t":"signed","tp":"t","e":"0000000a","o":1,"sb":"{\"t\":\"fill\",\"p\":{}}","sig":"AA","kid":"k"}"#,
            None,
        );
        assert!(s.handler.offsets.is_empty());
        // Signed for the topic it came on: taken.
        s.on_json(
            r#"{"t":"signed","tp":"t","e":"0000000a","o":1,"sb":"{\"t\":\"fill\",\"tp\":\"t\",\"p\":{}}","sig":"AA","kid":"k"}"#,
            None,
        );
        assert_eq!(s.handler.offsets, [1]);
    }

    #[test]
    fn a_gap_is_filled_by_the_replay_and_what_came_past_it_follows() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        assert_eq!(sent(&mut s)[0]["p"]["topics"], serde_json::json!(["t"]));
        feed(&mut s, E, &[1, 2, 4, 5]);
        assert_eq!(s.handler.offsets, [1, 2]);
        // The client asks from the last one it has.
        let request = sent(&mut s);
        assert_eq!(
            request[0]["p"]["recovery"],
            serde_json::json!({"t": {"epoch": E, "offset": 2}})
        );
        // The replay: 3, then 4 and 5 again.
        feed(&mut s, E, &[3, 4, 5]);
        s.on_json(
            r#"{"c":"WSE","t":"subscription_update","p":{"success_topics":["t"],"failed_topics":[],"recovery":{"t":{"epoch":"0000000a","offset":5,"recovered":true,"count":3}}}}"#,
            None,
        );
        assert_eq!(s.handler.offsets, [1, 2, 3, 4, 5]);
        assert!(gaps(&s).is_empty());
        assert_eq!(s.next_deadline(), None);
        feed(&mut s, E, &[6]);
        assert_eq!(s.handler.offsets, [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn a_gap_not_filled_in_time_is_reported_and_what_is_held_delivered() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        feed(&mut s, E, &[1, 3, 4]);
        assert_eq!(s.handler.offsets, [1]);
        s.on_deadline(Instant::now() + Duration::from_secs(4));
        assert_eq!(s.handler.offsets, [1, 3, 4]);
        assert_eq!(gaps(&s), [(Some(2), Some(2), GapReason::Timeout)]);
        feed(&mut s, E, &[5]);
        assert_eq!(s.handler.offsets, [1, 3, 4, 5]);
    }

    #[test]
    fn without_recovery_a_gap_is_reported_at_once() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, false);
        feed(&mut s, E, &[1, 4, 4, 5]);
        assert_eq!(s.handler.offsets, [1, 4, 5]);
        assert_eq!(gaps(&s), [(Some(2), Some(3), GapReason::Unavailable)]);
    }

    #[test]
    fn a_new_connection_asks_for_its_topics_from_where_it_left_off() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        feed(&mut s, E, &[1, 2]);
        sent(&mut s);
        s.close();
        s.open(None);
        ready(&mut s, true);
        let request = sent(&mut s);
        assert_eq!(
            request[0]["p"],
            serde_json::json!({
                "action": "subscribe",
                "topics": ["t"],
                "recover": true,
                "recovery": {"t": {"epoch": E, "offset": 2}},
            })
        );
        // The server restarted meanwhile: its history began again.
        s.on_json(
            r#"{"c":"WSE","t":"subscription_update","p":{"success_topics":["t"],"recovery":{"t":{"epoch":"0000000b","offset":0,"recovered":false,"count":0}}}}"#,
            None,
        );
        assert_eq!(gaps(&s), [(None, None, GapReason::EpochChanged)]);
        let Some(Event::Connected(again)) = s
            .handler
            .events
            .iter()
            .rev()
            .find(|e| matches!(e, Event::Connected(_)))
        else {
            panic!("connected");
        };
        assert!(again.reconnected);
    }

    #[test]
    fn a_topic_left_starts_over_when_joined_again() {
        let mut s = session(Duration::from_secs(3));
        ready(&mut s, true);
        feed(&mut s, E, &[1, 2]);
        s.unsubscribe(vec!["t".to_owned()]);
        s.subscribe(vec!["t".to_owned()]);
        let requests = sent(&mut s);
        assert_eq!(requests.last().unwrap()["p"].get("recovery"), None);
        feed(&mut s, E, &[9]);
        assert_eq!(s.handler.offsets, [1, 2, 9]);
        assert!(gaps(&s).is_empty());
    }

    #[test]
    fn requests_are_json() {
        let text = subscription(
            &["a\"b", "c"],
            &[(
                "c",
                Position {
                    epoch: 0xab,
                    offset: 7,
                },
            )],
        );
        let value: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(
            value["p"],
            serde_json::json!({
                "action": "subscribe",
                "topics": ["a\"b", "c"],
                "recover": true,
                "recovery": {"c": {"epoch": "000000ab", "offset": 7}},
            })
        );
        let hello: serde_json::Value =
            serde_json::from_str(client_hello(true, None).as_str()).unwrap();
        assert_eq!(hello["t"], "client_hello");
        assert_eq!(hello["p"]["features"]["compression"], true);
    }

    /// What reading a topic message costs, frames apart: run with
    /// `cargo test --release -p wse-client --lib -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn the_cost_of_a_message() {
        #[derive(serde::Deserialize)]
        struct Tick<'a> {
            #[serde(borrow)]
            symbol: &'a str,
            price: f64,
        }
        struct Decode(f64);
        impl Handler for Decode {
            fn message(&mut self, message: &Message<'_>) {
                if message.kind() == "tick" {
                    let tick: Tick<'_> = message.payload().unwrap();
                    self.0 += tick.price + tick.symbol.len() as f64;
                }
            }
        }
        let n = 1_000_000u64;
        let frames: Vec<String> = (1..=n)
            .map(|o| {
                format!(
                    r#"{{"tp":"bench","e":"0000abcd","o":{o},"t":"tick","p":{{"symbol":"BTCUSDT","price":67012.5,"qty":0.0125,"side":"buy","ts":1794600000000}}}}"#
                )
            })
            .collect();
        let shared = Arc::new(Shared {
            ready: AtomicBool::new(false),
            connection: Mutex::new(None),
            token: Mutex::new(None),
            server_kid: Mutex::new(None),
        });
        let keys = Arc::new(Keys::from_config(&crate::Config::new("ws://x")).unwrap());
        let mut s = Session::new(Decode(0.0), shared, keys, &[], Duration::from_secs(3));
        s.open(None);
        let started = std::time::Instant::now();
        for frame in &frames {
            s.on_json(frame, None);
        }
        let all = started.elapsed().as_nanos() as f64 / n as f64;
        // The envelope alone.
        let started = std::time::Instant::now();
        let mut kinds = 0usize;
        for frame in &frames {
            kinds += decode::json_envelope(frame).unwrap().t.unwrap().len();
        }
        let envelope = started.elapsed().as_nanos() as f64 / n as f64;
        // A whole serde_json::Value, as a typical client reads it.
        let started = std::time::Instant::now();
        for frame in &frames {
            let value: serde_json::Value = serde_json::from_str(frame).unwrap();
            kinds += value["t"].as_str().unwrap().len();
        }
        let value = started.elapsed().as_nanos() as f64 / n as f64;
        println!(
            "per message: session {all:.0} ns (envelope {envelope:.0} ns); a serde_json::Value {value:.0} ns [{kinds} {}]",
            s.handler.0
        );
    }
}
