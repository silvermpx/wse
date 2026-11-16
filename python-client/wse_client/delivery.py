"""The delivery, as the TypeScript and Rust clients have it.

Each topic's subscription (requested, active, denied) and continuity
(continuous, resync required, resyncing), what the consumer committed it
applied, and in the recover-or-resync mode (:class:`DeliveryConfig`) the
barrier.

A topic that is not continuous -- from a cold start, after every reconnect,
after a break -- holds its messages. Once the server took it, a ``resync``
event asks the application to bring the topic's state up to date;
:meth:`~wse_client.AsyncWSEClient.resynced` then hands on what was held (the
application applies a repeat safely). A topic that lost messages to the
holding limit, or broke off again while it resynced, is asked for once more
instead of passing for complete.

The checkpoint is what the application committed it applied: per stream it
moves only through what was applied without a hole. It is bound to the
realm, the user, the consumer and the schema, and one of anything else is
refused aloud. A recovery starts from it, so a message that came but was not
applied comes again.

No I/O: the client feeds it what it hears and hands out what it makes.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass
from enum import Enum

from .types import WSEEvent

logger = logging.getLogger("wse_client.delivery")

DEFAULT_MAX_HELD = 10_000
# Commits held beyond a hole, per stream; more are not tracked.
MAX_AHEAD = 10_000
# Reasons that concern what came before the topic's subscription: the state
# the application is loading covers them.
BEFORE_SUBSCRIPTION = frozenset({"not_recovered", "epoch_changed"})

RESYNC_EVENT = "resync"
CONTINUITY_EVENT = "continuity"
CHECKPOINT_REJECTED_EVENT = "checkpoint_rejected"


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    """The recover-or-resync delivery, for an application where every fact counts.

    Attributes:
        consumer: The one consumer that commits, by a stable name.
        schema: The application's data schema: a checkpoint of another is refused.
        realm: The server's realm (by default the URL): a checkpoint of another
            is refused.
        checkpoint: The checkpoint the application saved last
            (:meth:`~wse_client.AsyncWSEClient.checkpoint`).
        max_held: Messages a topic holds while it resyncs (0: 10,000); more
            starts the resync over.
    """

    consumer: str
    schema: str | None = None
    realm: str | None = None
    checkpoint: str | None = None
    max_held: int = 0

    def __repr__(self) -> str:
        # The checkpoint names the user: left out of a print.
        held = "<redacted>" if self.checkpoint is not None else None
        return (
            f"DeliveryConfig(consumer={self.consumer!r}, schema={self.schema!r}, "
            f"realm={self.realm!r}, checkpoint={held}, max_held={self.max_held})"
        )


class Subscription(str, Enum):
    """Whether the server took a topic."""

    REQUESTED = "requested"
    ACTIVE = "active"
    DENIED = "denied"


class Continuity(str, Enum):
    """Whether a topic's messages run without a break."""

    CONTINUOUS = "continuous"
    RESYNC_REQUIRED = "resync_required"
    RESYNCING = "resyncing"


@dataclass(frozen=True, slots=True)
class TopicState:
    """A topic held: whether the server took it, and whether its messages run
    without a break. *reason* says why it is not continuous: ``cold_start``,
    ``reconnected``, ``requested``, ``overflow``, or the reason of the gap."""

    topic: str
    subscription: Subscription
    continuity: Continuity
    reason: str | None = None


@dataclass(slots=True)
class _State:
    subscription: Subscription
    continuity: Continuity
    reason: str | None = None
    # The resync round it is in.
    round: int | None = None


class _Progress:
    """What a consumer applied of one stream: everything before *next*, and
    some beyond."""

    __slots__ = ("ahead", "next")

    def __init__(self, next_offset: int) -> None:
        self.next = next_offset
        self.ahead: set[int] = set()

    def last(self) -> int | None:
        return self.next - 1 if self.next > 0 else None


class _Held:
    __slots__ = ("incomplete", "messages")

    def __init__(self) -> None:
        self.messages: list[WSEEvent] = []
        # Messages were lost to the limit.
        self.incomplete = False


def _epoch_number(epoch: str | None) -> int | None:
    if epoch is None:
        return None
    try:
        return int(epoch, 16)
    except ValueError:
        return None


class Delivery:
    """The delivery's state: what the application heard, held and committed."""

    def __init__(
        self, config: DeliveryConfig | None, realm: str, topics: list[str]
    ) -> None:
        self._barrier = config is not None
        if config is not None:
            if not config.consumer:
                raise ValueError("recover-or-resync needs a consumer name")
            if config.max_held < 0:
                raise ValueError("max_held must be >= 0")
        self._consumer = config.consumer if config else None
        self._schema = config.schema if config else None
        self._realm = (config.realm or realm) if config else None
        self._max_held = (
            (config.max_held or DEFAULT_MAX_HELD) if config else DEFAULT_MAX_HELD
        )
        # The saved checkpoint, until the first connection says whose it is.
        self._pending = config.checkpoint if config else None
        # The user of the last connection, once there was one (a one-item
        # tuple, so that "no user" and "not yet" differ).
        self._user: tuple[str | None] | None = None
        # The user generation: a new one starts where another user's
        # connection comes. A commit counts only in the generation it was
        # handed out in.
        self.generation = 0
        self._states: dict[str, _State] = {}
        self._held: dict[str, _Held] = {}
        self._progress: dict[str, dict[int, _Progress]] = {}
        # The last offset received of each stream.
        self._received: dict[str, dict[int, int]] = {}
        # Topics that broke off while they resynced, and why.
        self._again: dict[str, str] = {}
        self._next_round = 1
        # What the delivery made outside what the application hears (held
        # messages handed on, a refused checkpoint, what an event led to),
        # handed out before what comes next.
        self._out: deque[WSEEvent] = deque()
        self.track(topics)

    @property
    def barrier(self) -> bool:
        return self._barrier

    def topic_states(self) -> dict[str, TopicState]:
        return {
            topic: TopicState(topic, s.subscription, s.continuity, s.reason)
            for topic, s in sorted(self._states.items())
        }

    def take_out(self) -> WSEEvent | None:
        return self._out.popleft() if self._out else None

    # -- topics ----------------------------------------------------------------

    def track(self, topics: list[str]) -> None:
        for topic in topics:
            if not topic or topic in self._states:
                continue
            if self._barrier:
                self._held[topic] = _Held()
                self._states[topic] = _State(
                    Subscription.REQUESTED, Continuity.RESYNC_REQUIRED, "cold_start"
                )
            else:
                self._states[topic] = _State(
                    Subscription.REQUESTED, Continuity.CONTINUOUS
                )

    def untrack(self, topics: list[str]) -> None:
        for topic in topics:
            self._states.pop(topic, None)
            self._held.pop(topic, None)
            self._progress.pop(topic, None)
            self._received.pop(topic, None)
            self._again.pop(topic, None)

    # -- what the application hears ----------------------------------------------

    def message(self, event: WSEEvent) -> bool:
        """A topic message: True to hand it on now, False when the barrier
        holds it."""
        topic = event.topic
        if topic is None:
            return True
        epoch = _epoch_number(event.epoch)
        offset = event.offset
        if epoch is not None and offset is not None:
            streams = self._received.setdefault(topic, {})
            last = streams.get(epoch)
            streams[epoch] = offset if last is None else max(last, offset)
        state = self._states.get(topic)
        if state is None:
            return True
        if self._barrier and state.continuity is not Continuity.CONTINUOUS:
            held = self._held.setdefault(topic, _Held())
            if len(held.messages) < self._max_held:
                held.messages.append(event)
            else:
                held.incomplete = True
            return False
        if epoch is not None and offset is not None:
            self._handed_on(topic, epoch, offset)
        return True

    def connected(self, user: str | None) -> None:
        """A connection of *user*: what another user applied, or holds, is
        not this one's."""
        if self._user is not None and self._user[0] != user:
            self._progress.clear()
            self._received.clear()
            self._again.clear()
            for topic in self._held:
                self._held[topic] = _Held()
            self.generation += 1
        self._user = (user,)

    def subscribed(self, taken: list[str], refused: list[str]) -> None:
        for topic in taken:
            state = self._states.get(topic)
            if state is not None:
                state.subscription = Subscription.ACTIVE
        for topic in refused:
            state = self._states.get(topic)
            if state is not None:
                state.subscription = Subscription.DENIED
        self._start_resyncs()

    def connection_lost(self) -> None:
        for topic, state in self._states.items():
            state.subscription = Subscription.REQUESTED
            state.round = None
            self._again.pop(topic, None)
            before = state.continuity
            if self._barrier:
                self._held[topic] = _Held()
                if state.continuity is not Continuity.RESYNC_REQUIRED:
                    state.continuity = Continuity.RESYNC_REQUIRED
                    state.reason = "reconnected"
            elif state.continuity is Continuity.RESYNCING:
                state.continuity = Continuity.RESYNC_REQUIRED
            if state.continuity is not before:
                self._out.append(self._continuity_event(topic, state))

    def broke_off(self, topic: str, reason: str) -> None:
        state = self._states.get(topic)
        if state is None:
            return
        if state.continuity is Continuity.RESYNCING:
            # What came before the subscription is in the state being
            # loaded; a message lost since may not be.
            if reason not in BEFORE_SUBSCRIPTION:
                self._again[topic] = reason
        elif state.continuity is Continuity.CONTINUOUS:
            if self._barrier:
                self._held[topic] = _Held()
            self._set_continuity(topic, Continuity.RESYNC_REQUIRED, reason)
            self._start_resyncs()

    def _handed_on(self, topic: str, epoch: int, offset: int) -> None:
        """A message handed on: a stream first seen starts its checkpoint
        just before it."""
        streams = self._progress.setdefault(topic, {})
        if epoch not in streams:
            streams[epoch] = _Progress(offset)

    def _start_resyncs(self) -> None:
        """The topics the server took that wait for a resync, in one round."""
        if not self._barrier:
            return
        topics = [
            topic
            for topic, s in sorted(self._states.items())
            if s.subscription is Subscription.ACTIVE
            and s.continuity is Continuity.RESYNC_REQUIRED
        ]
        if not topics:
            return
        round_ = self._next_round
        self._next_round += 1
        reasons: dict[str, str] = {}
        committed: dict[str, dict[str, int]] = {}
        for topic in topics:
            state = self._states[topic]
            state.continuity = Continuity.RESYNCING
            state.round = round_
            reasons[topic] = state.reason or "requested"
            self._out.append(self._continuity_event(topic, state))
            for epoch, progress in sorted(self._progress.get(topic, {}).items()):
                last = progress.last()
                if last is not None:
                    committed.setdefault(topic, {})[f"{epoch:08x}"] = last
        self._out.append(
            WSEEvent(
                type=RESYNC_EVENT,
                payload={
                    "round": round_,
                    "topics": topics,
                    "reasons": reasons,
                    "committed": committed,
                },
                category="WSE",
            )
        )

    # -- what the application says -------------------------------------------------

    def resynced(self, round_: int) -> None:
        """The application brought the topics of *round_* up to date: what
        they held goes on, or a topic that lost some is asked for again."""
        topics = [
            topic
            for topic, s in sorted(self._states.items())
            if s.round == round_ and s.continuity is Continuity.RESYNCING
        ]
        for topic in topics:
            held = self._held.pop(topic, None) or _Held()
            again = self._again.pop(topic, None)
            broke = "overflow" if held.incomplete else again
            state = self._states[topic]
            if broke is not None:
                self._held[topic] = _Held()
                state.round = None
                self._set_continuity(topic, Continuity.RESYNC_REQUIRED, broke)
                continue
            self._restart(topic, held.messages)
            state.round = None
            self._set_continuity(topic, Continuity.CONTINUOUS, None)
            for message in held.messages:
                epoch = _epoch_number(message.epoch)
                if epoch is not None and message.offset is not None:
                    self._handed_on(topic, epoch, message.offset)
                self._out.append(message)
        self._start_resyncs()

    def _restart(self, topic: str, onward: list[WSEEvent]) -> None:
        """The state the application loaded covers each stream up to the
        first message handed on after it, or up to what was received when
        none is."""
        streams = {
            epoch: _Progress(offset + 1)
            for epoch, offset in self._received.get(topic, {}).items()
        }
        first: set[int] = set()
        for message in onward:
            epoch = _epoch_number(message.epoch)
            if epoch is None or message.offset is None or epoch in first:
                continue
            first.add(epoch)
            streams[epoch] = _Progress(message.offset)
        self._progress[topic] = streams

    def resync(self, topics: list[str]) -> None:
        """The application asks for *topics* to be brought up to date. Best
        effort: it did so itself, and they read continuous again."""
        for topic in topics:
            state = self._states.get(topic)
            if state is None:
                continue
            if not self._barrier:
                if state.continuity is not Continuity.CONTINUOUS:
                    self._set_continuity(topic, Continuity.CONTINUOUS, None)
                continue
            self._held[topic] = _Held()
            state.round = None
            if state.continuity is not Continuity.RESYNC_REQUIRED:
                self._set_continuity(topic, Continuity.RESYNC_REQUIRED, "requested")
        self._start_resyncs()

    def commit(self, topic: str, epoch: int, offset: int, generation: int) -> None:
        """The consumer applied the message at *epoch*, *offset* of *topic*,
        handed out in *generation*: one of an earlier user counts for nobody."""
        if generation != self.generation:
            return
        progress = self._progress.get(topic, {}).get(epoch)
        if progress is None or offset < progress.next:
            return
        if offset == progress.next:
            progress.next += 1
            while progress.next in progress.ahead:
                progress.ahead.discard(progress.next)
                progress.next += 1
        elif len(progress.ahead) < MAX_AHEAD:
            progress.ahead.add(offset)

    def checkpoint(self) -> str:
        """The checkpoint, to save with the application's own state."""
        topics: dict[str, dict[str, int]] = {}
        for topic in sorted(self._progress):
            applied = {
                f"{epoch:08x}": last
                for epoch, progress in sorted(self._progress[topic].items())
                if (last := progress.last()) is not None
            }
            if applied:
                topics[topic] = applied
        return json.dumps(
            {
                "v": 1,
                "realm": self._realm,
                "user": self._user[0] if self._user else None,
                "consumer": self._consumer,
                "schema": self._schema,
                "topics": topics,
            },
            separators=(",", ":"),
        )

    def committed(self, user: str | None) -> dict[str, dict[int, int]] | None:
        """recover-or-resync: where each topic's recovery starts, for a
        connection of *user* about to subscribe (None in best effort). The
        saved checkpoint is taken here, once a connection named its user."""
        if not self._barrier:
            return None
        if self._user is None:
            self.connected(user)
        elif self._user[0] != user:
            return {}
        self._take_saved()
        return {
            topic: {
                epoch: last
                for epoch, progress in streams.items()
                if (last := progress.last()) is not None
            }
            for topic, streams in self._progress.items()
            if topic in self._states
        }

    def _take_saved(self) -> None:
        text, self._pending = self._pending, None
        if text is None:
            return
        try:
            saved = json.loads(text)
            if not isinstance(saved, dict) or not isinstance(saved.get("topics"), dict):
                raise ValueError("no topics")
            topics: dict[str, dict[int, _Progress]] = {}
            for topic, streams in saved["topics"].items():
                if not isinstance(streams, dict):
                    raise ValueError("a topic of no streams")
                parsed: dict[int, _Progress] = {}
                for epoch, last in streams.items():
                    if (
                        not isinstance(last, int)
                        or isinstance(last, bool)
                        or last < 0
                        or last >= 2**64 - 1
                    ):
                        raise ValueError("an offset that is not one")
                    number = _epoch_number(epoch)
                    if number is None or not 0 <= number < 2**32:
                        continue
                    parsed[number] = _Progress(last + 1)
                topics[topic] = parsed
        except (ValueError, TypeError):
            self._reject("malformed")
            return
        user = self._user[0] if self._user else None
        mismatch = (
            "version"
            if saved.get("v") != 1
            else "realm"
            if saved.get("realm") != self._realm
            else "user"
            if saved.get("user") != user
            else "consumer"
            if saved.get("consumer") != self._consumer
            else "schema"
            if saved.get("schema") != self._schema
            else None
        )
        if mismatch is not None:
            self._reject(mismatch)
            return
        for topic, streams in topics.items():
            if topic not in self._states or topic in self._progress or not streams:
                continue
            self._progress[topic] = streams

    def _reject(self, reason: str) -> None:
        logger.warning(
            "The saved checkpoint is not taken: its %s is not this client's", reason
        )
        self._out.append(
            WSEEvent(
                type=CHECKPOINT_REJECTED_EVENT,
                payload={"reason": reason},
                category="WSE",
            )
        )

    def _set_continuity(
        self, topic: str, continuity: Continuity, reason: str | None
    ) -> None:
        state = self._states.get(topic)
        if state is None or (state.continuity is continuity and state.reason == reason):
            return
        state.continuity = continuity
        state.reason = reason
        self._out.append(self._continuity_event(topic, state))

    @staticmethod
    def _continuity_event(topic: str, state: _State) -> WSEEvent:
        return WSEEvent(
            type=CONTINUITY_EVENT,
            payload={
                "topic": topic,
                "continuity": state.continuity.value,
                "reason": state.reason,
            },
            category="WSE",
            topic=topic,
        )


__all__ = [
    "CHECKPOINT_REJECTED_EVENT",
    "CONTINUITY_EVENT",
    "RESYNC_EVENT",
    "Continuity",
    "Delivery",
    "DeliveryConfig",
    "Subscription",
    "TopicState",
]
