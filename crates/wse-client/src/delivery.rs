//! The `native` module's delivery, as the TypeScript client's
//! `core/delivery.ts`: each topic's subscription (requested, active, denied)
//! and continuity (continuous, resync required, resyncing), and in the
//! recover-or-resync mode the barrier and the checkpoint.
//!
//! A topic that is not continuous -- from a cold start, after every
//! reconnect, after a break -- holds its messages. Once the server took it,
//! a [`Item::Resync`] asks the application to bring the topic's state up to
//! date; [`Delivery::resynced`] then hands on what was held (the application
//! applies a repeat safely). A topic that lost messages to the holding
//! limit, or broke off again while it resynced, is asked for once more
//! instead of passing for complete.
//!
//! The checkpoint is what the application committed it applied: per stream
//! it moves only through what was applied without a hole. It is bound to the
//! realm, the user, the consumer and the schema, and one of anything else is
//! refused aloud. A recovery starts from it, so a message that came but was
//! not applied comes again.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use crate::message::Position;
use crate::native::{
    ErrorKind, Item, NativeContinuity, NativeDelivery, NativeError, NativeMessage,
    NativeSubscription, NativeTopicState, StreamPosition,
};

const DEFAULT_MAX_HELD: usize = 10_000;
/// Commits held beyond a hole, per stream; more are not tracked.
const MAX_AHEAD: usize = 10_000;
/// Reasons that concern what came before the topic's subscription: the
/// state the application is loading covers them.
const BEFORE_SUBSCRIPTION: [&str; 2] = ["not_recovered", "epoch_changed"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    BestEffort,
    RecoverOrResync,
}

#[derive(Clone)]
struct State {
    subscription: NativeSubscription,
    continuity: NativeContinuity,
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
    items: Vec<Item>,
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

pub(crate) struct Delivery {
    mode: Mode,
    consumer: Option<String>,
    schema: Option<String>,
    realm: Option<String>,
    max_held: usize,
    /// The saved checkpoint, until the first connection says whose it is.
    pending: Option<String>,
    user: Option<String>,
    states: BTreeMap<String, State>,
    held: HashMap<String, Held>,
    progress: HashMap<String, BTreeMap<u32, Progress>>,
    /// The last offset received of each stream.
    received: HashMap<String, BTreeMap<u32, u64>>,
    /// Topics that broke off while they resynced, and why.
    again: HashMap<String, String>,
    next_round: u64,
    /// Items made outside the events (held messages handed on, a refused
    /// checkpoint), handed out before what comes next.
    out: VecDeque<Item>,
}

impl Delivery {
    pub(crate) fn new(
        config: Option<NativeDelivery>,
        realm: &str,
        topics: &[String],
    ) -> Result<Self, NativeError> {
        let mut delivery = match config {
            None => Self::with(Mode::BestEffort, None, None, None, DEFAULT_MAX_HELD, None),
            Some(config) => {
                if config.consumer.is_empty() {
                    return Err(NativeError::new(
                        ErrorKind::Config,
                        "recover-or-resync needs a consumer name",
                    ));
                }
                let max_held = if config.max_held == 0 {
                    DEFAULT_MAX_HELD
                } else {
                    config.max_held as usize
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
        delivery.track(topics);
        Ok(delivery)
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

    pub(crate) fn topic_states(&self) -> Vec<NativeTopicState> {
        self.states
            .iter()
            .map(|(topic, state)| NativeTopicState {
                topic: topic.clone(),
                subscription: state.subscription,
                continuity: state.continuity,
                reason: state.reason.clone(),
            })
            .collect()
    }

    /// Items made outside the events, for the next batch.
    pub(crate) fn take_out(&mut self) -> Option<Item> {
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
                    subscription: NativeSubscription::Requested,
                    continuity: NativeContinuity::ResyncRequired,
                    reason: Some("cold_start".to_owned()),
                    round: None,
                }
            } else {
                State {
                    subscription: NativeSubscription::Requested,
                    continuity: NativeContinuity::Continuous,
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

    // -- what the client hears -------------------------------------------------

    /// One item of the events: none when the barrier holds it; the items
    /// that follow from it go out with it.
    pub(crate) fn take(&mut self, item: Item, items: &mut Vec<Item>) {
        match item {
            Item::Message(message) => self.take_message(message, items),
            Item::Connected { ref user_id, .. } => {
                self.user.clone_from(user_id);
                items.push(item);
            }
            Item::Subscribed {
                ref topics,
                ref failed,
                ..
            } => {
                for topic in topics {
                    self.set_subscription(topic, NativeSubscription::Active);
                }
                for topic in failed {
                    self.set_subscription(topic, NativeSubscription::Denied);
                }
                items.push(item);
                self.start_resyncs(items);
            }
            Item::Disconnected { .. } | Item::Paused { .. } => {
                items.push(item);
                self.connection_lost(items);
            }
            Item::Gap {
                ref topic,
                ref reason,
                ..
            } => {
                let (topic, reason) = (topic.clone(), reason.clone());
                items.push(item);
                self.broke_off(&topic, &reason, items);
            }
            item => items.push(item),
        }
    }

    fn take_message(&mut self, message: NativeMessage, items: &mut Vec<Item>) {
        if let (Some(topic), Some(epoch), Some(offset)) =
            (&message.topic, message.epoch, message.offset)
        {
            let last = self
                .received
                .entry(topic.clone())
                .or_default()
                .entry(epoch)
                .or_insert(offset);
            *last = (*last).max(offset);
        }
        if let Some(topic) = &message.topic
            && let Some(state) = self.states.get(topic)
        {
            if self.barrier() && state.continuity != NativeContinuity::Continuous {
                let held = self.held.entry(topic.clone()).or_default();
                if held.items.len() < self.max_held {
                    held.items.push(Item::Message(message));
                } else {
                    held.incomplete = true;
                }
                return;
            }
            let topic = topic.clone();
            self.handed_on(&topic, message.epoch, message.offset);
        }
        items.push(Item::Message(message));
    }

    fn set_subscription(&mut self, topic: &str, subscription: NativeSubscription) {
        if let Some(state) = self.states.get_mut(topic) {
            state.subscription = subscription;
        }
    }

    /// A message handed on: a stream first seen starts its checkpoint just
    /// before it.
    fn handed_on(&mut self, topic: &str, epoch: Option<u32>, offset: Option<u64>) {
        let (Some(epoch), Some(offset)) = (epoch, offset) else {
            return;
        };
        self.progress
            .entry(topic.to_owned())
            .or_default()
            .entry(epoch)
            .or_insert_with(|| Progress::from(offset));
    }

    fn connection_lost(&mut self, items: &mut Vec<Item>) {
        let barrier = self.barrier();
        let mut changes = Vec::new();
        for (topic, state) in &mut self.states {
            state.subscription = NativeSubscription::Requested;
            state.round = None;
            self.again.remove(topic);
            let before = state.continuity;
            if barrier {
                self.held.insert(topic.clone(), Held::default());
                if state.continuity != NativeContinuity::ResyncRequired {
                    state.continuity = NativeContinuity::ResyncRequired;
                    state.reason = Some("reconnected".to_owned());
                }
            } else if state.continuity == NativeContinuity::Resyncing {
                state.continuity = NativeContinuity::ResyncRequired;
            }
            if state.continuity != before {
                changes.push(continuity_item(topic, state));
            }
        }
        items.extend(changes);
    }

    fn broke_off(&mut self, topic: &str, reason: &str, items: &mut Vec<Item>) {
        let Some(continuity) = self.states.get(topic).map(|s| s.continuity) else {
            return;
        };
        match continuity {
            NativeContinuity::Resyncing => {
                // What came before the subscription is in the state being
                // loaded; a message lost since may not be.
                if !BEFORE_SUBSCRIPTION.contains(&reason) {
                    self.again.insert(topic.to_owned(), reason.to_owned());
                }
            }
            NativeContinuity::ResyncRequired => {}
            NativeContinuity::Continuous => {
                if self.barrier() {
                    self.held.insert(topic.to_owned(), Held::default());
                }
                self.set_continuity(topic, NativeContinuity::ResyncRequired, Some(reason), items);
                self.start_resyncs(items);
            }
        }
    }

    /// The topics the server took that wait for a resync, in one round.
    fn start_resyncs(&mut self, items: &mut Vec<Item>) {
        if !self.barrier() {
            return;
        }
        let topics: Vec<String> = self
            .states
            .iter()
            .filter(|(_, s)| {
                s.subscription == NativeSubscription::Active
                    && s.continuity == NativeContinuity::ResyncRequired
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
            state.continuity = NativeContinuity::Resyncing;
            state.round = Some(round);
            reasons.push(
                state
                    .reason
                    .clone()
                    .unwrap_or_else(|| "requested".to_owned()),
            );
            items.push(continuity_item(topic, state));
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
        items.push(Item::Resync {
            round,
            topics,
            reasons,
            committed,
        });
    }

    // -- what the application says ---------------------------------------------

    /// The application brought the topics of `round` up to date: what they
    /// held goes on, or a topic that lost some is asked for again.
    pub(crate) fn resynced(&mut self, round: u64) {
        let topics: Vec<String> = self
            .states
            .iter()
            .filter(|(_, s)| s.round == Some(round) && s.continuity == NativeContinuity::Resyncing)
            .map(|(topic, _)| topic.clone())
            .collect();
        let mut items = Vec::new();
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
                self.set_continuity(
                    topic,
                    NativeContinuity::ResyncRequired,
                    Some(&reason),
                    &mut items,
                );
                continue;
            }
            self.restart(topic, &held.items);
            if let Some(state) = self.states.get_mut(topic) {
                state.round = None;
            }
            self.set_continuity(topic, NativeContinuity::Continuous, None, &mut items);
            for item in held.items {
                if let Item::Message(message) = &item {
                    self.handed_on(topic, message.epoch, message.offset);
                }
                items.push(item);
            }
        }
        self.start_resyncs(&mut items);
        self.out.extend(items);
    }

    /// The state the application loaded covers each stream up to the first
    /// message handed on after it, or up to what was received when none is.
    fn restart(&mut self, topic: &str, onward: &[Item]) {
        let mut streams: BTreeMap<u32, Progress> = self
            .received
            .get(topic)
            .into_iter()
            .flatten()
            .map(|(epoch, offset)| (*epoch, Progress::from(offset + 1)))
            .collect();
        let mut first = BTreeSet::new();
        for item in onward {
            if let Item::Message(message) = item
                && let (Some(epoch), Some(offset)) = (message.epoch, message.offset)
                && first.insert(epoch)
            {
                streams.insert(epoch, Progress::from(offset));
            }
        }
        self.progress.insert(topic.to_owned(), streams);
    }

    /// The application asks for `topics` to be brought up to date. Best
    /// effort: it did so itself, and they read continuous again.
    pub(crate) fn resync(&mut self, topics: &[String]) {
        let mut items = Vec::new();
        for topic in topics {
            let Some(state) = self.states.get(topic) else {
                continue;
            };
            if !self.barrier() {
                if state.continuity != NativeContinuity::Continuous {
                    self.set_continuity(topic, NativeContinuity::Continuous, None, &mut items);
                }
                continue;
            }
            self.held.insert(topic.clone(), Held::default());
            if let Some(state) = self.states.get_mut(topic) {
                state.round = None;
            }
            if self.states[topic].continuity != NativeContinuity::ResyncRequired {
                self.set_continuity(
                    topic,
                    NativeContinuity::ResyncRequired,
                    Some("requested"),
                    &mut items,
                );
            }
        }
        self.start_resyncs(&mut items);
        self.out.extend(items);
    }

    /// The consumer applied the message at `epoch`, `offset` of `topic`.
    pub(crate) fn commit(&mut self, topic: &str, epoch: u32, offset: u64) {
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
            user: self.user.clone(),
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
        self.user = user.map(str::to_owned);
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
        } else if saved.user != self.user {
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
        self.out.push_back(Item::CheckpointRejected {
            reason: reason.to_owned(),
        });
    }

    fn set_continuity(
        &mut self,
        topic: &str,
        continuity: NativeContinuity,
        reason: Option<&str>,
        items: &mut Vec<Item>,
    ) {
        let Some(state) = self.states.get_mut(topic) else {
            return;
        };
        if state.continuity == continuity && state.reason.as_deref() == reason {
            return;
        }
        state.continuity = continuity;
        state.reason = reason.map(str::to_owned);
        items.push(continuity_item(topic, state));
    }
}

fn continuity_item(topic: &str, state: &State) -> Item {
    Item::Continuity {
        topic: topic.to_owned(),
        continuity: state.continuity,
        reason: state.reason.clone(),
    }
}
