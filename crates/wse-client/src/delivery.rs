//! The delivery, as the TypeScript client's `core/delivery.ts`: each topic's
//! subscription (requested, active, denied) and continuity (continuous,
//! resync required, resyncing), what the consumer committed it applied, and
//! in the recover-or-resync mode ([`Delivery`]) the barrier.
//!
//! A topic that is not continuous -- from a cold start, after every
//! reconnect, after a break -- holds its messages. Once the server took it,
//! an [`Event::Resync`] asks the application to bring the topic's state up
//! to date; [`Client::resynced`] then hands on what was held (the
//! application applies a repeat safely). A topic that lost messages to the
//! holding limit, or broke off again while it resynced, is asked for once
//! more instead of passing for complete.
//!
//! The checkpoint is what the application committed it applied: per stream
//! it moves only through what was applied without a hole. It is bound to the
//! realm, the user, the consumer and the schema, and one of anything else is
//! refused aloud. A recovery starts from it, so a message that came but was
//! not applied comes again.
//!
//! The gate sees what the application hears in the order the application
//! hears it: a [`Handler`] where the session hands it out, the [`Events`]
//! stream where it is read.
//!
//! [`Client::resynced`]: crate::Client::resynced
//! [`Events`]: crate::Events

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::error::Error;
use crate::events::Incoming;
use crate::handler::{CheckpointRejected, Event, Handler, MaybeSend, Resync, TopicContinuity};
use crate::message::{Message, OwnedMessage, Position};

const DEFAULT_MAX_HELD: usize = 10_000;
/// Commits held beyond a hole, per stream; more are not tracked.
const MAX_AHEAD: usize = 10_000;
/// Reasons that concern what came before the topic's subscription: the
/// state the application is loading covers them.
const BEFORE_SUBSCRIPTION: [&str; 2] = ["not_recovered", "epoch_changed"];

/// The recover-or-resync delivery, for an application where every fact
/// counts: a topic that is not continuous (a cold start, every reconnect, a
/// break) holds its messages until the application brought its state up to
/// date ([`Event::Resync`], then [`Client::resynced`]); the checkpoint is
/// what it committed it applied, and recovery starts from it.
///
/// [`Client::resynced`]: crate::Client::resynced
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Delivery {
    /// The one consumer that commits, by a stable name.
    pub consumer: String,
    /// The application's data schema: a checkpoint of another is refused.
    pub schema: Option<String>,
    /// The server's realm (by default the URL as given): a checkpoint of
    /// another is refused.
    pub realm: Option<String>,
    /// The checkpoint the application saved last
    /// ([`Client::checkpoint`](crate::Client::checkpoint)).
    pub checkpoint: Option<String>,
    /// Messages a topic holds while it resyncs (0: 10,000); more starts the
    /// resync over.
    pub max_held: usize,
}

// The checkpoint names the user: left out of a print.
impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Delivery")
            .field("consumer", &self.consumer)
            .field("schema", &self.schema)
            .field("realm", &self.realm)
            .field(
                "checkpoint",
                &format_args!(
                    "{}",
                    if self.checkpoint.is_some() {
                        "Some(<redacted>)"
                    } else {
                        "None"
                    }
                ),
            )
            .field("max_held", &self.max_held)
            .finish()
    }
}

impl Delivery {
    pub fn new(consumer: impl Into<String>) -> Self {
        Self {
            consumer: consumer.into(),
            schema: None,
            realm: None,
            checkpoint: None,
            max_held: 0,
        }
    }
}

/// Whether the server took a topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Subscription {
    Requested,
    Active,
    Denied,
}

/// Whether a topic's messages run without a break.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Continuity {
    Continuous,
    /// Its messages broke off (or none came yet): the application's state of
    /// it is to be brought up to date.
    ResyncRequired,
    /// A resync round asks the application for it.
    Resyncing,
}

/// A topic held: whether the server took it, and whether its messages run
/// without a break.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicState {
    pub topic: String,
    pub subscription: Subscription,
    pub continuity: Continuity,
    /// Why it is not continuous: `cold_start`, `reconnected`, `requested`,
    /// `overflow`, or the reason of the gap.
    pub reason: Option<String>,
}

/// Where a consumer stands in one stream of a topic: the last offset it
/// committed it applied.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamPosition {
    pub topic: String,
    pub epoch: u32,
    pub offset: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    BestEffort,
    RecoverOrResync,
}

#[derive(Clone)]
struct State {
    subscription: Subscription,
    continuity: Continuity,
    reason: Option<String>,
    /// The resync round it is in.
    round: Option<u64>,
}

/// What a consumer applied of one stream: everything before `next`, and
/// some beyond.
#[derive(Default)]
struct Progress {
    next: u64,
    ahead: BTreeSet<u64>,
}

impl Progress {
    fn from(next: u64) -> Self {
        Self {
            next,
            ahead: BTreeSet::new(),
        }
    }

    /// The last offset applied, when any is.
    fn last(&self) -> Option<u64> {
        self.next.checked_sub(1)
    }
}

#[derive(Default)]
struct Held {
    messages: Vec<OwnedMessage>,
    /// Messages were lost to the limit.
    incomplete: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Saved {
    v: u32,
    realm: Option<String>,
    user: Option<String>,
    consumer: Option<String>,
    schema: Option<String>,
    topics: BTreeMap<String, BTreeMap<String, u64>>,
}

/// The delivery's state: what the application heard, held and committed.
pub(crate) struct Gate {
    mode: Mode,
    consumer: Option<String>,
    schema: Option<String>,
    realm: Option<String>,
    max_held: usize,
    /// The saved checkpoint, until the first connection says whose it is.
    pending: Option<String>,
    /// The user of the last connection in the order the application hears
    /// the events, once there was one: what came before the events show
    /// another user is still the last one's.
    user: Option<Option<String>>,
    /// The user generation: a new one starts where the events show another
    /// user. Each message handed out carries it, and a commit counts only in
    /// the generation it was handed out in.
    generation: u32,
    states: BTreeMap<String, State>,
    held: HashMap<String, Held>,
    progress: HashMap<String, BTreeMap<u32, Progress>>,
    /// The last offset received of each stream.
    received: HashMap<String, BTreeMap<u32, u64>>,
    /// Topics that broke off while they resynced, and why.
    again: HashMap<String, String>,
    next_round: u64,
    /// What the gate made outside what the application hears (held messages
    /// handed on, a refused checkpoint, what an event leads to), handed out
    /// before what comes next.
    out: VecDeque<Incoming>,
}

/// The gate, shared by the client's handles and where the application hears.
pub(crate) type Shared = Arc<Mutex<Gate>>;

/// The gate, whatever a thread that panicked holding it left.
pub(crate) fn lock(gate: &Shared) -> MutexGuard<'_, Gate> {
    gate.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Gate {
    pub(crate) fn new(
        config: Option<Delivery>,
        realm: &str,
        topics: &[String],
    ) -> Result<Self, Error> {
        let mut gate = match config {
            None => Self::with(Mode::BestEffort, None, None, None, DEFAULT_MAX_HELD, None),
            Some(config) => {
                if config.consumer.is_empty() {
                    return Err(Error::Config(
                        "recover-or-resync needs a consumer name".to_owned(),
                    ));
                }
                let max_held = if config.max_held == 0 {
                    DEFAULT_MAX_HELD
                } else {
                    config.max_held
                };
                Self::with(
                    Mode::RecoverOrResync,
                    Some(config.consumer),
                    config.schema,
                    Some(config.realm.unwrap_or_else(|| realm.to_owned())),
                    max_held,
                    config.checkpoint,
                )
            }
        };
        gate.track(topics);
        Ok(gate)
    }

    fn with(
        mode: Mode,
        consumer: Option<String>,
        schema: Option<String>,
        realm: Option<String>,
        max_held: usize,
        pending: Option<String>,
    ) -> Self {
        Self {
            mode,
            consumer,
            schema,
            realm,
            max_held,
            pending,
            user: None,
            generation: 0,
            states: BTreeMap::new(),
            held: HashMap::new(),
            progress: HashMap::new(),
            received: HashMap::new(),
            again: HashMap::new(),
            next_round: 1,
            out: VecDeque::new(),
        }
    }

    fn barrier(&self) -> bool {
        self.mode == Mode::RecoverOrResync
    }

    pub(crate) fn topic_states(&self) -> Vec<TopicState> {
        self.states
            .iter()
            .map(|(topic, state)| TopicState {
                topic: topic.clone(),
                subscription: state.subscription,
                continuity: state.continuity,
                reason: state.reason.clone(),
            })
            .collect()
    }

    /// What the gate made outside what the application hears, next.
    pub(crate) fn take_out(&mut self) -> Option<Incoming> {
        self.out.pop_front()
    }

    // -- topics --------------------------------------------------------------

    pub(crate) fn track(&mut self, topics: &[String]) {
        for topic in topics {
            if topic.is_empty() || self.states.contains_key(topic) {
                continue;
            }
            let state = if self.barrier() {
                self.held.insert(topic.clone(), Held::default());
                State {
                    subscription: Subscription::Requested,
                    continuity: Continuity::ResyncRequired,
                    reason: Some("cold_start".to_owned()),
                    round: None,
                }
            } else {
                State {
                    subscription: Subscription::Requested,
                    continuity: Continuity::Continuous,
                    reason: None,
                    round: None,
                }
            };
            self.states.insert(topic.clone(), state);
        }
    }

    pub(crate) fn untrack(&mut self, topics: &[String]) {
        for topic in topics {
            self.states.remove(topic);
            self.held.remove(topic);
            self.progress.remove(topic);
            self.received.remove(topic);
            self.again.remove(topic);
        }
    }

    // -- what the application hears ------------------------------------------

    /// A message: the generation to hand it on with now, or None when the
    /// barrier holds it (a copy of it).
    pub(crate) fn message(&mut self, message: &Message<'_>) -> Option<u32> {
        match self.admit(message.topic(), message.position()) {
            Admit::Pass => Some(self.generation),
            Admit::Hold => {
                let mut owned = message.to_owned();
                owned.generation = self.generation;
                self.hold(owned);
                None
            }
        }
    }

    /// [`message`](Self::message) for one already owned: itself stamped, or
    /// None when held.
    pub(crate) fn owned(&mut self, mut message: OwnedMessage) -> Option<OwnedMessage> {
        message.generation = self.generation;
        match self.admit(message.topic(), message.position()) {
            Admit::Pass => Some(message),
            Admit::Hold => {
                self.hold(message);
                None
            }
        }
    }

    fn admit(&mut self, topic: Option<&str>, position: Option<Position>) -> Admit {
        let Some(topic) = topic else {
            return Admit::Pass;
        };
        if let Some(at) = position {
            match self.received.get_mut(topic) {
                Some(streams) => {
                    let last = streams.entry(at.epoch).or_insert(at.offset);
                    *last = (*last).max(at.offset);
                }
                None => {
                    self.received
                        .insert(topic.to_owned(), BTreeMap::from([(at.epoch, at.offset)]));
                }
            }
        }
        let Some(state) = self.states.get(topic) else {
            return Admit::Pass;
        };
        if self.barrier() && state.continuity != Continuity::Continuous {
            return Admit::Hold;
        }
        if let Some(at) = position {
            self.handed_on(topic, at);
        }
        Admit::Pass
    }

    fn hold(&mut self, message: OwnedMessage) {
        let Some(topic) = message.topic() else {
            return;
        };
        let held = self.held.entry(topic.to_owned()).or_default();
        if held.messages.len() < self.max_held {
            held.messages.push(message);
        } else {
            held.incomplete = true;
        }
    }

    /// An event the application hears; what follows from it goes out after
    /// it.
    pub(crate) fn event(&mut self, event: &Event) {
        match event {
            Event::Connected(connected) => self.user_is(connected.user_id.as_deref()),
            Event::Subscribed(subscribed) => {
                for topic in &subscribed.topics {
                    self.set_subscription(topic, Subscription::Active);
                }
                for topic in &subscribed.failed {
                    self.set_subscription(topic, Subscription::Denied);
                }
                self.start_resyncs();
            }
            Event::Disconnected(_) | Event::Paused(_) => self.connection_lost(),
            Event::Gap(gap) => self.broke_off(&gap.topic, gap.reason.as_str()),
            _ => {}
        }
    }

    fn set_subscription(&mut self, topic: &str, subscription: Subscription) {
        if let Some(state) = self.states.get_mut(topic) {
            state.subscription = subscription;
        }
    }

    /// A message handed on: a stream first seen starts its checkpoint just
    /// before it.
    fn handed_on(&mut self, topic: &str, at: Position) {
        match self.progress.get_mut(topic) {
            Some(streams) => {
                streams
                    .entry(at.epoch)
                    .or_insert_with(|| Progress::from(at.offset));
            }
            None => {
                self.progress.insert(
                    topic.to_owned(),
                    BTreeMap::from([(at.epoch, Progress::from(at.offset))]),
                );
            }
        }
    }

    fn connection_lost(&mut self) {
        let barrier = self.barrier();
        let mut changes = Vec::new();
        for (topic, state) in &mut self.states {
            state.subscription = Subscription::Requested;
            state.round = None;
            self.again.remove(topic);
            let before = state.continuity;
            if barrier {
                self.held.insert(topic.clone(), Held::default());
                if state.continuity != Continuity::ResyncRequired {
                    state.continuity = Continuity::ResyncRequired;
                    state.reason = Some("reconnected".to_owned());
                }
            } else if state.continuity == Continuity::Resyncing {
                state.continuity = Continuity::ResyncRequired;
            }
            if state.continuity != before {
                changes.push(continuity_event(topic, state));
            }
        }
        self.out.extend(changes);
    }

    fn broke_off(&mut self, topic: &str, reason: &str) {
        let Some(continuity) = self.states.get(topic).map(|s| s.continuity) else {
            return;
        };
        match continuity {
            Continuity::Resyncing => {
                // What came before the subscription is in the state being
                // loaded; a message lost since may not be.
                if !BEFORE_SUBSCRIPTION.contains(&reason) {
                    self.again.insert(topic.to_owned(), reason.to_owned());
                }
            }
            Continuity::ResyncRequired => {}
            Continuity::Continuous => {
                if self.barrier() {
                    self.held.insert(topic.to_owned(), Held::default());
                }
                self.set_continuity(topic, Continuity::ResyncRequired, Some(reason));
                self.start_resyncs();
            }
        }
    }

    /// The topics the server took that wait for a resync, in one round.
    fn start_resyncs(&mut self) {
        if !self.barrier() {
            return;
        }
        let topics: Vec<String> = self
            .states
            .iter()
            .filter(|(_, s)| {
                s.subscription == Subscription::Active && s.continuity == Continuity::ResyncRequired
            })
            .map(|(topic, _)| topic.clone())
            .collect();
        if topics.is_empty() {
            return;
        }
        let round = self.next_round;
        self.next_round += 1;
        let mut reasons = Vec::with_capacity(topics.len());
        let mut committed = Vec::new();
        for topic in &topics {
            let state = self.states.get_mut(topic).expect("listed");
            state.continuity = Continuity::Resyncing;
            state.round = Some(round);
            reasons.push(
                state
                    .reason
                    .clone()
                    .unwrap_or_else(|| "requested".to_owned()),
            );
            self.out.push_back(continuity_event(topic, state));
            for (epoch, progress) in self.progress.get(topic).into_iter().flatten() {
                if let Some(offset) = progress.last() {
                    committed.push(StreamPosition {
                        topic: topic.clone(),
                        epoch: *epoch,
                        offset,
                    });
                }
            }
        }
        self.out.push_back(Incoming::Event(Event::Resync(Resync {
            round,
            topics,
            reasons,
            committed,
        })));
    }

    // -- what the application says -------------------------------------------

    /// The application brought the topics of `round` up to date: what they
    /// held goes on, or a topic that lost some is asked for again.
    pub(crate) fn resynced(&mut self, round: u64) {
        let topics: Vec<String> = self
            .states
            .iter()
            .filter(|(_, s)| s.round == Some(round) && s.continuity == Continuity::Resyncing)
            .map(|(topic, _)| topic.clone())
            .collect();
        for topic in &topics {
            let held = self.held.remove(topic).unwrap_or_default();
            let broke = if held.incomplete {
                Some("overflow".to_owned())
            } else {
                self.again.remove(topic)
            };
            self.again.remove(topic);
            if let Some(reason) = broke {
                self.held.insert(topic.clone(), Held::default());
                if let Some(state) = self.states.get_mut(topic) {
                    state.round = None;
                }
                self.set_continuity(topic, Continuity::ResyncRequired, Some(&reason));
                continue;
            }
            self.restart(topic, &held.messages);
            if let Some(state) = self.states.get_mut(topic) {
                state.round = None;
            }
            self.set_continuity(topic, Continuity::Continuous, None);
            for message in held.messages {
                if let Some(at) = message.position() {
                    self.handed_on(topic, at);
                }
                self.out.push_back(Incoming::Message(message));
            }
        }
        self.start_resyncs();
    }

    /// The state the application loaded covers each stream up to the first
    /// message handed on after it, or up to what was received when none is.
    fn restart(&mut self, topic: &str, onward: &[OwnedMessage]) {
        let mut streams: BTreeMap<u32, Progress> = self
            .received
            .get(topic)
            .into_iter()
            .flatten()
            .map(|(epoch, offset)| (*epoch, Progress::from(offset + 1)))
            .collect();
        let mut first = BTreeSet::new();
        for message in onward {
            if let Some(at) = message.position()
                && first.insert(at.epoch)
            {
                streams.insert(at.epoch, Progress::from(at.offset));
            }
        }
        self.progress.insert(topic.to_owned(), streams);
    }

    /// The application asks for `topics` to be brought up to date. Best
    /// effort: it did so itself, and they read continuous again.
    pub(crate) fn resync(&mut self, topics: &[String]) {
        for topic in topics {
            let Some(state) = self.states.get(topic) else {
                continue;
            };
            if !self.barrier() {
                if state.continuity != Continuity::Continuous {
                    self.set_continuity(topic, Continuity::Continuous, None);
                }
                continue;
            }
            self.held.insert(topic.clone(), Held::default());
            if let Some(state) = self.states.get_mut(topic) {
                state.round = None;
            }
            if self.states[topic].continuity != Continuity::ResyncRequired {
                self.set_continuity(topic, Continuity::ResyncRequired, Some("requested"));
            }
        }
        self.start_resyncs();
    }

    /// The consumer applied the message at `epoch`, `offset` of `topic`,
    /// handed out in `generation`: one of an earlier user counts for nobody.
    pub(crate) fn commit(&mut self, topic: &str, epoch: u32, offset: u64, generation: u32) {
        if generation != self.generation {
            return;
        }
        let Some(progress) = self
            .progress
            .get_mut(topic)
            .and_then(|streams| streams.get_mut(&epoch))
        else {
            return;
        };
        if offset < progress.next {
            return;
        }
        if offset == progress.next {
            progress.next += 1;
            while progress.ahead.remove(&progress.next) {
                progress.next += 1;
            }
        } else if progress.ahead.len() < MAX_AHEAD {
            progress.ahead.insert(offset);
        }
    }

    /// The checkpoint, to save with the application's own state.
    pub(crate) fn checkpoint(&self) -> String {
        // A stream nothing of which was applied is left out, and a topic
        // with none.
        let topics = self
            .progress
            .iter()
            .filter_map(|(topic, streams)| {
                let applied: BTreeMap<String, u64> = streams
                    .iter()
                    .filter_map(|(epoch, progress)| {
                        Some((format!("{epoch:08x}"), progress.last()?))
                    })
                    .collect();
                (!applied.is_empty()).then(|| (topic.clone(), applied))
            })
            .collect();
        serde_json::to_string(&Saved {
            v: 1,
            realm: self.realm.clone(),
            user: self.user.clone().flatten(),
            consumer: self.consumer.clone(),
            schema: self.schema.clone(),
            topics,
        })
        .unwrap_or_default()
    }

    /// recover-or-resync: where each topic's recovery starts, for a
    /// connection of `user` about to subscribe (None in best effort). The
    /// saved checkpoint is taken here, once a connection named its user.
    pub(crate) fn committed(
        &mut self,
        user: Option<&str>,
    ) -> Option<BTreeMap<String, Vec<Position>>> {
        if !self.barrier() {
            return None;
        }
        match &self.user {
            // The first connection: nothing was heard before it.
            None => self.user_is(user),
            // Another user's: nothing of what the last one applied. The
            // change itself comes where the events show it, after what the
            // last user still has to hear.
            Some(last) if last.as_deref() != user => return Some(BTreeMap::new()),
            Some(_) => {}
        }
        self.take_saved();
        Some(
            self.progress
                .iter()
                .filter(|(topic, _)| self.states.contains_key(*topic))
                .map(|(topic, streams)| {
                    (
                        topic.clone(),
                        streams
                            .iter()
                            .filter_map(|(epoch, progress)| {
                                Some(Position {
                                    epoch: *epoch,
                                    offset: progress.last()?,
                                })
                            })
                            .collect(),
                    )
                })
                .collect(),
        )
    }

    /// A connection of `user`, where the events show it: what another user
    /// applied, or holds, is not this one's.
    fn user_is(&mut self, user: Option<&str>) {
        let user = user.map(str::to_owned);
        if self.user.as_ref().is_some_and(|last| *last != user) {
            self.progress.clear();
            self.received.clear();
            self.again.clear();
            for held in self.held.values_mut() {
                *held = Held::default();
            }
            self.generation = self.generation.wrapping_add(1);
        }
        self.user = Some(user);
    }

    fn take_saved(&mut self) {
        let Some(text) = self.pending.take() else {
            return;
        };
        let Ok(saved) = serde_json::from_str::<Saved>(&text) else {
            self.reject("malformed");
            return;
        };
        let mismatch = if saved.v != 1 {
            Some("version")
        } else if saved.realm != self.realm {
            Some("realm")
        } else if saved.user != self.user.clone().flatten() {
            Some("user")
        } else if saved.consumer != self.consumer {
            Some("consumer")
        } else if saved.schema != self.schema {
            Some("schema")
        } else {
            None
        };
        if let Some(reason) = mismatch {
            self.reject(reason);
            return;
        }
        for (topic, streams) in saved.topics {
            if !self.states.contains_key(&topic) || self.progress.contains_key(&topic) {
                continue;
            }
            let streams: BTreeMap<u32, Progress> = streams
                .into_iter()
                .filter_map(|(epoch, last)| {
                    let epoch = u32::from_str_radix(&epoch, 16).ok()?;
                    Some((epoch, Progress::from(last.checked_add(1)?)))
                })
                .collect();
            if !streams.is_empty() {
                self.progress.insert(topic, streams);
            }
        }
    }

    fn reject(&mut self, reason: &str) {
        tracing::warn!(
            "[WSE] the saved checkpoint is not taken: its {reason} is not this client's"
        );
        self.out
            .push_back(Incoming::Event(Event::CheckpointRejected(
                CheckpointRejected {
                    reason: reason.to_owned(),
                },
            )));
    }

    fn set_continuity(&mut self, topic: &str, continuity: Continuity, reason: Option<&str>) {
        let Some(state) = self.states.get_mut(topic) else {
            return;
        };
        if state.continuity == continuity && state.reason.as_deref() == reason {
            return;
        }
        state.continuity = continuity;
        state.reason = reason.map(str::to_owned);
        let event = continuity_event(topic, state);
        self.out.push_back(event);
    }
}

enum Admit {
    Pass,
    Hold,
}

fn continuity_event(topic: &str, state: &State) -> Incoming {
    Incoming::Event(Event::Continuity(TopicContinuity {
        topic: topic.to_owned(),
        continuity: state.continuity,
        reason: state.reason.clone(),
    }))
}

/// The application's handler behind the gate, for a client made with one:
/// what the session hands out passes the gate in the order the handler hears
/// it. Without a gate (the [`Events`](crate::Events) stream's queue, read
/// behind one of its own) everything passes as it is.
pub(crate) struct Delivered<H> {
    pub(crate) inner: H,
    gate: Option<Shared>,
}

impl<H: Handler> Delivered<H> {
    pub(crate) fn new(inner: H, gate: Option<Shared>) -> Self {
        Self { inner, gate }
    }

    /// What the gate made outside the session's hand-out (held messages
    /// handed on, a refused checkpoint, what an event led to), to the handler
    /// now, the gate let go while it runs.
    pub(crate) fn drain(&mut self) {
        let Some(gate) = &self.gate else {
            return;
        };
        loop {
            let next = lock(gate).take_out();
            match next {
                Some(Incoming::Message(message)) => self.inner.message(&message.as_message()),
                Some(Incoming::Event(event)) => self.inner.event(event),
                Some(Incoming::Binary(data)) => self.inner.binary(&data),
                None => return,
            }
        }
    }
}

impl<H: Handler> Handler for Delivered<H> {
    fn message(&mut self, message: &Message<'_>) {
        let Some(gate) = &self.gate else {
            self.inner.message(message);
            return;
        };
        // The gate is let go before the handler runs: it may commit.
        let generation = lock(gate).message(message);
        match generation {
            Some(generation) if generation == message.generation => self.inner.message(message),
            Some(generation) => {
                let mut stamped = message.clone();
                stamped.generation = generation;
                self.inner.message(&stamped);
            }
            None => {}
        }
        self.drain();
    }

    fn event(&mut self, event: Event) {
        if let Some(gate) = &self.gate {
            lock(gate).event(&event);
        }
        self.inner.event(event);
        self.drain();
    }

    fn binary(&mut self, data: &[u8]) {
        self.inner.binary(data);
    }

    fn ready(&mut self) -> impl std::future::Future<Output = ()> + MaybeSend {
        self.inner.ready()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{Connected, Disconnected, Subscribed};

    fn message(topic: &str, epoch: u32, offset: u64) -> OwnedMessage {
        OwnedMessage::for_test("m", Some(topic), Some(Position { epoch, offset }))
    }

    fn connected(user: &str) -> Event {
        Event::Connected(Connected {
            connection_id: "c".into(),
            user_id: Some(user.into()),
            reconnected: false,
            recovery: true,
        })
    }

    fn disconnected() -> Event {
        Event::Disconnected(Disconnected {
            code: Some(1006),
            reason: "gone".into(),
            retry_in: std::time::Duration::ZERO,
            attempt: 1,
        })
    }

    fn subscribed(topics: &[&str]) -> Event {
        Event::Subscribed(Subscribed {
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            failed: Vec::new(),
            recovery: Vec::new(),
        })
    }

    fn out(gate: &mut Gate) -> Vec<Incoming> {
        std::iter::from_fn(|| gate.take_out()).collect()
    }

    fn round_of(items: &[Incoming]) -> u64 {
        items
            .iter()
            .find_map(|i| match i {
                Incoming::Event(Event::Resync(r)) => Some(r.round),
                _ => None,
            })
            .expect("a resync round")
    }

    fn board() -> Gate {
        Gate::new(
            Some(Delivery::new("board")),
            "ws://x/wse",
            &["t".to_owned()],
        )
        .unwrap()
    }

    /// A connection of `user` that took topic t and resynced it.
    fn resynced(gate: &mut Gate, user: &str) {
        gate.committed(Some(user));
        gate.event(&connected(user));
        gate.event(&subscribed(&["t"]));
        let round = round_of(&out(gate));
        gate.resynced(round);
        out(gate);
    }

    #[test]
    fn a_checkpoint_is_never_another_users() {
        let mut gate = board();
        resynced(&mut gate, "alice");
        let alices = gate.owned(message("t", 1, 99)).expect("handed on");
        gate.commit("t", 1, 99, alices.generation);
        assert_eq!(
            gate.committed(Some("alice")).unwrap()["t"],
            [Position {
                epoch: 1,
                offset: 99
            }]
        );

        // The next connection is Bob's: it recovers nothing of what Alice
        // applied.
        gate.event(&disconnected());
        let positions = gate.committed(Some("bob")).unwrap();
        assert!(
            positions.get("t").is_none_or(Vec::is_empty),
            "{positions:?}"
        );
        // Where the events show his connection, the checkpoint is his, and
        // Alice's message applied late counts for nobody.
        gate.event(&connected("bob"));
        gate.commit("t", 1, 99, alices.generation);
        gate.commit("t", 1, 100, alices.generation);
        let saved: serde_json::Value = serde_json::from_str(&gate.checkpoint()).unwrap();
        assert_eq!(saved["user"], "bob");
        assert_eq!(saved["topics"], serde_json::json!({}));
    }

    #[test]
    fn a_cold_start_holds_the_topic_until_the_application_resynced_it() {
        let mut gate = board();
        gate.committed(Some("ann"));
        gate.event(&connected("ann"));
        assert!(
            gate.owned(message("t", 1, 5)).is_none(),
            "held before the server took it"
        );
        gate.event(&subscribed(&["t"]));
        let items = out(&mut gate);
        let round = round_of(&items);
        assert!(
            gate.owned(message("t", 1, 6)).is_none(),
            "held while it resyncs"
        );
        gate.resynced(round);
        let released: Vec<Position> = out(&mut gate)
            .into_iter()
            .filter_map(|i| match i {
                Incoming::Message(m) => m.position(),
                _ => None,
            })
            .collect();
        assert_eq!(
            released,
            [
                Position {
                    epoch: 1,
                    offset: 5
                },
                Position {
                    epoch: 1,
                    offset: 6
                }
            ]
        );
        assert!(gate.owned(message("t", 1, 7)).is_some(), "continuous now");
    }

    #[test]
    fn a_break_while_it_resyncs_asks_for_the_topic_again() {
        let mut gate = board();
        gate.committed(Some("ann"));
        gate.event(&connected("ann"));
        gate.event(&subscribed(&["t"]));
        let round = round_of(&out(&mut gate));
        gate.event(&Event::Gap(crate::handler::Gap {
            topic: "t".into(),
            epoch: 1,
            first: Some(3),
            last: Some(4),
            reason: crate::handler::GapReason::Timeout,
        }));
        gate.resynced(round);
        let items = out(&mut gate);
        assert!(
            items.iter().any(
                |i| matches!(i, Incoming::Event(Event::Resync(r)) if r.reasons == ["timeout"])
            ),
            "{items:?}"
        );
    }

    #[test]
    fn best_effort_holds_nothing_and_counts_commits() {
        let mut gate = Gate::new(None, "ws://x/wse", &["t".to_owned()]).unwrap();
        gate.event(&connected("ann"));
        gate.event(&subscribed(&["t"]));
        let m = gate.owned(message("t", 1, 1)).expect("never held");
        gate.commit("t", 1, 1, m.generation);
        let saved: serde_json::Value = serde_json::from_str(&gate.checkpoint()).unwrap();
        assert_eq!(saved["topics"]["t"]["00000001"], 1, "{saved}");
        assert!(out(&mut gate).is_empty());
    }
}
