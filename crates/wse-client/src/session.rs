//! The protocol on one client: what the server's messages mean, which reach
//! the application, the topics subscribed and where each stands, and the
//! gaps being recovered. No I/O: what it sends goes in its outbox.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Utf8Bytes;

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

type Map<V> = HashMap<Box<str>, V, ahash::RandomState>;

/// A topic waiting for the server to replay what it missed.
struct Hole {
    epoch: u32,
    /// What came past the hole, by offset.
    held: BTreeMap<u64, OwnedMessage>,
    deadline: Instant,
    /// A request is out, not answered yet.
    in_flight: bool,
    /// The offset the last request asked from.
    requested_from: Option<u64>,
    /// Some answer came for this topic.
    answered: bool,
}

#[derive(Default)]
struct Topic {
    streams: Streams,
    hole: Option<Box<Hole>>,
}

/// What the session asks of the connection.
pub(crate) enum Outgoing {
    Text(Utf8Bytes),
}

pub(crate) struct Session<H> {
    pub(crate) handler: H,
    shared: Arc<Shared>,
    recovery_timeout: Duration,
    /// Topics the client subscribed to: subscribed again on every
    /// connection.
    wanted: BTreeSet<String>,
    topics: Map<Topic>,
    /// Positions recoveries were asked from, by topic, until answered.
    requests: Map<Position>,
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
            if let Some(hole) = &mut topic.hole {
                hole.in_flight = false;
            }
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
            if let Some(hole) = &mut topic.hole {
                hole.in_flight = false;
            }
        }
    }

    /// The client stops: whatever is held is delivered, its gaps reported.
    pub(crate) fn finish(&mut self) {
        let held: Vec<Box<str>> = self
            .topics
            .iter()
            .filter(|(_, t)| t.hole.is_some())
            .map(|(name, _)| name.clone())
            .collect();
        for topic in held {
            self.give_up(&topic, GapReason::Closed);
        }
    }

    // -- Frames ------------------------------------------------------------

    pub(crate) fn on_json(&mut self, text: &str, frame: Option<&Bytes>) {
        match decode::json_envelope(text) {
            Ok(envelope) => self.on_envelope(envelope, frame),
            Err(e) => tracing::debug!("[WSE] frame dropped: {e}"),
        }
    }

    pub(crate) fn on_msgpack(&mut self, data: &[u8], frame: Option<&Bytes>) {
        match decode::msgpack_envelope(data) {
            Ok(envelope) => self.on_envelope(envelope, frame),
            Err(e) => tracing::debug!("[WSE] frame dropped: {e}"),
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
            Some(envelope) if handshake && !open => self.on_envelope(envelope, None),
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
                        self.on_json(message, frame);
                    }
                }
                Some(Payload::Msgpack(p)) => {
                    for message in decode::msgpack_batch(p).unwrap_or_default() {
                        self.on_msgpack(message, frame);
                    }
                }
                _ => {}
            }
            return;
        }
        let message = envelope.into_message(frame);
        match (message.position, &message.topic) {
            (Some(at), Some(_)) => self.on_stamped(message, at),
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
        if let (Some(came_on), Some(signed_for)) = (&outer.tp, &signed.tp)
            && came_on != signed_for
        {
            tracing::warn!(
                "[WSE] message signed for topic {signed_for} that came on {came_on} dropped"
            );
            return;
        }
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
            tp: outer.tp.or(signed.tp),
            e: outer.e,
            o: outer.o,
            ..Envelope::default()
        };
        let mut message = message.into_message(frame);
        message.id = id;
        message.signature = Some(Arc::new(signature));
        match (message.position, &message.topic) {
            (Some(at), Some(_)) => self.on_stamped(message, at),
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
        let held_here = topic.hole.as_ref().is_some_and(|h| h.epoch == at.epoch);
        match topic.streams.check(at) {
            Verdict::Duplicate => {}
            Verdict::Deliver => {
                if !self.unrecoverable.is_empty() {
                    self.unrecoverable.remove(name);
                }
                self.handler.message(&message);
                if held_here {
                    let name: Box<str> = name.into();
                    self.drain(&name);
                }
            }
            Verdict::Gap if held_here => {
                let hole = topic.hole.as_mut().expect("held_here");
                hole.held
                    .entry(at.offset)
                    .or_insert_with(|| message.to_owned());
                if hole.held.len() > HELD_MAX {
                    let name: Box<str> = name.into();
                    self.give_up(&name, GapReason::Overflow);
                }
            }
            Verdict::Gap => {
                let name: Box<str> = name.into();
                if self.unrecoverable.get(&name) == Some(&at.epoch) {
                    self.unrecoverable.remove(&name);
                    self.skip(&name, &message, at, GapReason::NotRecovered);
                } else if !self.start_hole(&name, &message, at) {
                    self.skip(&name, &message, at, GapReason::Unavailable);
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
        if topic.hole.is_some() || topic.streams.stream(at.epoch).is_none() {
            return false;
        }
        let deadline = Instant::now() + self.recovery_timeout;
        let mut held = BTreeMap::new();
        held.insert(at.offset, message.to_owned());
        topic.hole = Some(Box::new(Hole {
            epoch: at.epoch,
            held,
            deadline,
            in_flight: false,
            requested_from: None,
            answered: false,
        }));
        self.holes += 1;
        self.next_deadline = Some(self.next_deadline.map_or(deadline, |d| d.min(deadline)));
        self.request_recovery(name);
        true
    }

    /// Asks the server to replay a topic from the last message of the
    /// stream with the hole: it replays that stream, and every other
    /// stream of the topic, from that point.
    fn request_recovery(&mut self, name: &str) {
        let Some(topic) = self.topics.get_mut(name) else {
            return;
        };
        let Some(hole) = topic.hole.as_mut() else {
            return;
        };
        if hole.in_flight {
            return;
        }
        let Some(from) = topic.streams.stream(hole.epoch) else {
            return;
        };
        hole.in_flight = true;
        hole.requested_from = Some(from.offset);
        self.requests.insert(name.into(), from);
        let request = subscription(&[name], &[(name, from)]);
        self.outbox.push(Outgoing::Text(request));
    }

    /// Delivers what became in order; closes a filled hole.
    fn drain(&mut self, name: &str) {
        loop {
            let Some(topic) = self.topics.get_mut(name) else {
                return;
            };
            let Some(hole) = topic.hole.as_mut() else {
                return;
            };
            let Some(last) = topic.streams.stream(hole.epoch) else {
                break;
            };
            hole.held = hole.held.split_off(&(last.offset + 1));
            let Some(next) = hole.held.remove(&(last.offset + 1)) else {
                break;
            };
            topic.streams.advance(Position {
                epoch: hole.epoch,
                offset: last.offset + 1,
            });
            self.handler.message(&next.as_message());
        }
        let filled = self
            .topics
            .get(name)
            .and_then(|t| t.hole.as_ref())
            .is_some_and(|h| h.held.is_empty());
        if filled {
            self.close_hole(name);
            tracing::info!("[WSE] recovered the gap on {name}");
        }
    }

    fn close_hole(&mut self, name: &str) -> Option<Box<Hole>> {
        let hole = self.topics.get_mut(name)?.hole.take()?;
        self.holes -= 1;
        if self.next_deadline == Some(hole.deadline) || self.holes == 0 {
            self.next_deadline = self
                .topics
                .values()
                .filter_map(|t| t.hole.as_ref().map(|h| h.deadline))
                .min();
        }
        Some(hole)
    }

    /// Stops waiting: reports what is missing, delivers what is held.
    fn give_up(&mut self, name: &str, reason: GapReason) {
        let Some(hole) = self.close_hole(name) else {
            return;
        };
        let mut last = self
            .topics
            .get(name)
            .and_then(|t| t.streams.stream(hole.epoch))
            .map(|p| p.offset);
        for (offset, message) in hole.held {
            if last.is_some_and(|last| offset <= last) {
                continue;
            }
            if let Some(last) = last.filter(|&last| offset > last + 1) {
                self.report(name, hole.epoch, Some(last + 1), Some(offset - 1), reason);
            }
            if let Some(topic) = self.topics.get_mut(name) {
                topic.streams.advance(Position {
                    epoch: hole.epoch,
                    offset,
                });
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
        if let Some(last) = last.filter(|last| at.offset > last.offset + 1) {
            self.report(
                name,
                at.epoch,
                Some(last.offset + 1),
                Some(at.offset - 1),
                reason,
            );
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
        let expired: Vec<Box<str>> = self
            .topics
            .iter()
            .filter(|(_, t)| t.hole.as_ref().is_some_and(|h| h.deadline <= now))
            .map(|(name, _)| name.clone())
            .collect();
        for name in expired {
            let unanswered = self
                .topics
                .get(&name)
                .and_then(|t| t.hole.as_ref())
                .is_some_and(|h| h.in_flight && !h.answered);
            if unanswered {
                // Nothing came back: the server application may not answer
                // recovery requests. After a few, stop asking.
                self.unanswered += 1;
            }
            self.give_up(&name, GapReason::Timeout);
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
                && state.hole.is_some()
            {
                self.holes -= 1;
                self.next_deadline = self
                    .topics
                    .values()
                    .filter_map(|t| t.hole.as_ref().map(|h| h.deadline))
                    .min();
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
            self.outbox.push(Outgoing::Text(text.into()));
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
        for &(topic, at) in &positions {
            self.requests.insert(topic.into(), at);
            if let Some(hole) = self.topics.get_mut(topic).and_then(|t| t.hole.as_mut())
                && hole.epoch == at.epoch
            {
                hole.in_flight = true;
                hole.requested_from = Some(at.offset);
            }
        }
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.outbox
            .push(Outgoing::Text(subscription(&names, &positions)));
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
        let requested = self.requests.remove(name);
        let hole = self.topics.get_mut(name).and_then(|t| t.hole.as_mut());
        if let Some(hole) = hole {
            if !hole.in_flight {
                return; // not the answer to a request of this client's
            }
            hole.in_flight = false;
            hole.answered = true;
            let (hole_epoch, requested_from) = (hole.epoch, hole.requested_from);
            self.unanswered = 0;
            // The replay came before this answer.
            self.drain(name);
            let Some(topic) = self.topics.get(name).filter(|t| t.hole.is_some()) else {
                return; // filled
            };
            let progressed = topic
                .streams
                .stream(hole_epoch)
                .zip(requested_from)
                .is_some_and(|(at, from)| at.offset > from);
            if recovered && progressed {
                // A replay cut short by back-pressure: ask again from where
                // it got to.
                self.request_recovery(name);
            } else {
                let reason = if recovered {
                    GapReason::Incomplete
                } else {
                    GapReason::NotRecovered
                };
                self.give_up(name, reason);
            }
            return;
        }
        if requested.is_none() {
            return; // an answer to a plain subscription: nothing moves
        }
        self.unanswered = 0;
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
        self.outbox.push(Outgoing::Text(pong.into()));
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
            // server_hello comes unsealed: the sealed server_ready names
            // the signing key.
        } else if let Some(signing) = hello.signing {
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
fn subscription(topics: &[&str], positions: &[(&str, Position)]) -> Utf8Bytes {
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
    text.into()
}

fn push_json<T: serde::Serialize + ?Sized>(text: &mut String, value: &T) {
    // Strings and lists of strings: always encodable.
    text.push_str(&serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()));
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The first frame of a connection, with the end-to-end offer when it
/// opens a session.
pub(crate) fn client_hello(compression: bool, e2e: Option<serde_json::Value>) -> Utf8Bytes {
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
    serde_json::json!({"c": "WSE", "t": "client_hello", "p": p, "v": 1})
        .to_string()
        .into()
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
            .map(|Outgoing::Text(text)| serde_json::from_str(text.as_str()).unwrap())
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
