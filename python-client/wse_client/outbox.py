# =============================================================================
# WSE Python Client -- Outbox
# =============================================================================
#
# Every message the client sends, except a live or signed one, waits here
# until the server answers for it, and goes again after a reconnect. A server
# that answers (``acks``, from 3.2) confirms what its application took
# (``ack``, up to and with a number) and says what it refused (``nack``). The
# messages go out in order, numbered (``s``, first in the object) per
# producer, one producer per outbox, and the server drops a repeat, so its
# application takes each message once. A server that does not answer takes a
# message once it is written, as before.
# =============================================================================

from __future__ import annotations

import secrets
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Offline:
    """The outbox: where messages wait until the server answers for them.

    Args:
        max_messages: How many wait at most, those written and not answered
            yet included (default 100,000). Past it a send fails.
        max_bytes: How many bytes of messages (their JSON) wait at most
            (default 16 MiB).
        max_age: Seconds a message may wait to be written first (default an
            hour); one not written by then is dropped and the application
            told. Once written it waits for the server's answer whatever its
            age.
    """

    max_messages: int = 100_000
    max_bytes: int = 16 * 1024 * 1024
    max_age: float = 3600.0


@dataclass
class Outcomes:
    """What became of messages: delivered, or not and why (reason -> ids)."""

    delivered: list[str] = field(default_factory=list)
    not_delivered: dict[str, list[str]] = field(default_factory=dict)

    def refused(self, why: str, message_id: str) -> None:
        self.not_delivered.setdefault(why, []).append(message_id)

    def merge(self, other: Outcomes) -> None:
        self.delivered.extend(other.delivered)
        for why, ids in other.not_delivered.items():
            self.not_delivered.setdefault(why, []).extend(ids)

    def __bool__(self) -> bool:
        return bool(self.delivered or self.not_delivered)


class _Kept:
    """A message kept."""

    __slots__ = ("id", "owned", "s", "size", "text", "until")

    def __init__(
        self, s: int, message_id: str, text: str, until: float, owned: bool
    ) -> None:
        self.s = s
        self.id = message_id
        # The JSON object as it was made: written after its number.
        self.text = text
        self.until = until
        self.owned = owned
        self.size = len(text)


def numbered(s: int, text: str) -> str:
    """The message with its number first: ``{"s":N,`` before the rest."""
    rest = text.lstrip()[1:]
    if rest.lstrip().startswith("}"):
        return '{"s":%d%s' % (s, rest)
    return '{"s":%d,%s' % (s, rest)


class Outbox:
    """The outbox of one client: see the module's docstring."""

    def __init__(self, offline: Offline, *, producer: str | None = None) -> None:
        self._limits = offline
        self.producer = producer or secrets.token_hex(16)
        # The records, oldest first, from ``_head`` on; ``_cursor`` (an
        # absolute index) is where those not handed to this connection start.
        self._records: list[_Kept | None] = []
        self._head = 0
        self._cursor = 0
        self._next = 1
        self._node: str | None = None
        # The highest number handed to a connection: one above it never
        # reached a server.
        self._written = 0
        # The highest number written to this connection's socket.
        self._flushed = 0
        # Written on this connection and cleared before the server answered.
        self._limbo: list[tuple[int, str]] = []
        # The user of the last connection; _NONE before the first.
        self._user: object = _NONE
        self._ready = False
        self._answered = False
        self._plain = False
        self._count = 0
        self._bytes = 0
        # When the first message not written yet runs out (time.time()).
        self._earliest = float("inf")

    # -- What it holds ---------------------------------------------------------

    @property
    def size(self) -> int:
        """How many wait for the server's answer."""
        return self._count

    @property
    def connection_ready(self) -> bool:
        return self._ready

    @property
    def flushed(self) -> int:
        """The highest number written to this connection's socket."""
        return self._flushed

    def hello(self) -> dict[str, object]:
        """What the client's hello says of it."""
        first = self._first()
        return {
            "producer": self.producer,
            "node": self._node,
            "written": self._written,
            "from": first.s if first is not None else self._next,
        }

    def deadline(self) -> float | None:
        """When the first message not written yet runs out, or None."""
        return None if self._earliest == float("inf") else self._earliest

    def _first(self) -> _Kept | None:
        return self._records[self._head] if self._head < len(self._records) else None

    # -- What the application and the connection do ----------------------------

    def add(self, message_id: str, text: str, ttl: float | None, now: float) -> int:
        """Keeps a message (its JSON object): its number, or 0 when full."""
        size = len(text)
        if (
            self._count + 1 > self._limits.max_messages
            or self._bytes + size > self._limits.max_bytes
        ):
            return 0
        s = self._next
        self._next += 1
        wait = ttl if ttl is not None and ttl >= 0 else self._limits.max_age
        until = now + wait
        self._records.append(
            _Kept(s, message_id, text, until, owned=isinstance(self._user, str))
        )
        self._count += 1
        self._bytes += size
        self._earliest = min(self._earliest, until)
        return s

    def connected(self, user: str | None, now: float) -> Outcomes:
        """The server took a connection of *user*: another user's messages go,
        and those that waited too long."""
        outcomes = Outcomes()
        self._ready = True
        if self._user is not _NONE and self._user != user:
            # What was sent before any connection goes on any.
            self._remove_where("user_changed", outcomes, lambda _, r: r.owned)
        self._user = user
        if self._earliest <= now:
            self.expire(now, outcomes)
        return outcomes

    def server_answers(self, answers: bool) -> None:
        """The server of this connection answers numbered messages, or not."""
        self._plain = not answers

    def answered(
        self, acked: int, refused: list[tuple[int, int, str]], node: str
    ) -> Outcomes:
        """The server's answer to the hello: what it decided, the refusals
        among that, and its name. It may come before the server took the
        connection or after."""
        outcomes = Outcomes()
        for start, end, why in refused:
            outcomes.merge(self.refused(start, end, why))
        outcomes.merge(self.acked(acked))
        self._answered = True
        self._node = node
        return outcomes

    def acked(self, s: int) -> Outcomes:
        """The server took every message up to and with *s*."""
        outcomes = Outcomes()
        records = self._records
        while self._head < len(records):
            record = records[self._head]
            assert record is not None
            if record.s > s:
                break
            records[self._head] = None
            self._head += 1
            self._count -= 1
            self._bytes -= record.size
            outcomes.delivered.append(record.id)
        # The answer to a hello may take what this connection did not write.
        self._cursor = max(self._cursor, self._head)
        self._compact()
        while self._limbo and self._limbo[0][0] <= s:
            outcomes.delivered.append(self._limbo.pop(0)[1])
        return outcomes

    def refused(self, start: int, end: int, why: str) -> Outcomes:
        """The server refused the messages *start* to *end* for good."""
        outcomes = Outcomes()
        self._remove_where(why, outcomes, lambda _, r: start <= r.s <= end)
        kept = []
        for n, message_id in self._limbo:
            if start <= n <= end:
                outcomes.refused(why, message_id)
            else:
                kept.append((n, message_id))
        self._limbo = kept
        return outcomes

    def due(self, now: float, outcomes: Outcomes) -> list[tuple[int, str]]:
        """The messages for this connection's writer, in order, each with its
        number and its text as it goes: all it did not take yet, once the
        server took the connection and its mode is known. What waited too
        long to be sent goes instead."""
        if not self._ready or not (self._answered or self._plain):
            return []
        if self._earliest <= now:
            self.expire(now, outcomes)
        out: list[tuple[int, str]] = []
        records = self._records
        numbered_now = self._answered
        for i in range(self._cursor, len(records)):
            record = records[i]
            assert record is not None
            text = numbered(record.s, record.text) if numbered_now else record.text
            out.append((record.s, text))
        self._cursor = len(records)
        if out and numbered_now:
            self._written = max(self._written, out[-1][0])
        return out

    def wrote(self, last: int) -> Outcomes:
        """The connection wrote the messages up to and with *last*. A server
        that does not answer took them."""
        self._flushed = max(self._flushed, last)
        if self._plain and not self._answered:
            return self.acked(last)
        return Outcomes()

    def owed(self) -> bool:
        """The server owes an answer on this connection: for messages it has
        in hand, or to the hello while messages wait."""
        if self._answered:
            first = self._first()
            return bool(self._limbo) or (first is not None and first.s <= self._flushed)
        return self._ready and not self._plain and self._count > 0

    def disconnected(self) -> Outcomes:
        """The connection went: what waits goes on the next one. What was
        cleared while the server had it in hand is not delivered."""
        outcomes = Outcomes()
        self._ready = False
        self._answered = False
        self._plain = False
        self._cursor = self._head
        self._flushed = 0
        for _, message_id in self._limbo:
            outcomes.refused("cleared", message_id)
        self._limbo = []
        return outcomes

    def close(self) -> Outcomes:
        """The client ends: what waits goes with it (``closed``)."""
        outcomes = self.disconnected()
        for record in self._records[self._head :]:
            assert record is not None
            outcomes.refused("closed", record.id)
        self._reset()
        return outcomes

    def clear(self) -> Outcomes:
        """The user signed out: every message goes. One the server has in hand
        on this connection is delivered when it confirms it on it, and not
        when the connection ends first."""
        outcomes = Outcomes()
        in_hand = self._flushed if self._answered else 0
        for record in self._records[self._head :]:
            assert record is not None
            if record.s <= in_hand:
                self._limbo.append((record.s, record.id))
            else:
                outcomes.refused("cleared", record.id)
        self._reset()
        self._user = _NONE
        return outcomes

    def expire(self, now: float, outcomes: Outcomes) -> None:
        """Drops what waited longer than it may to be written."""
        written, cursor = self._written, self._cursor
        self._remove_where(
            "expired",
            outcomes,
            lambda at, r: at >= cursor and r.s > written and r.until < now,
        )
        earliest = float("inf")
        for record in self._records[self._head :]:
            assert record is not None
            if record.s > written and record.until < earliest:
                earliest = record.until
        self._earliest = earliest

    # -- Inside -----------------------------------------------------------------

    def _reset(self) -> None:
        self._records = []
        self._head = 0
        self._cursor = 0
        self._count = 0
        self._bytes = 0
        self._earliest = float("inf")

    def _remove_where(self, why: str, outcomes: Outcomes, gone) -> None:  # noqa: ANN001
        kept: list[_Kept | None] = []
        cursor = self._cursor - self._head
        for i in range(self._head, len(self._records)):
            record = self._records[i]
            assert record is not None
            if gone(i, record):
                self._count -= 1
                self._bytes -= record.size
                outcomes.refused(why, record.id)
                if i < self._cursor:
                    cursor -= 1
            else:
                kept.append(record)
        self._records = kept
        self._head = 0
        self._cursor = cursor

    def _compact(self) -> None:
        if self._head > 1024 and self._head * 2 > len(self._records):
            del self._records[: self._head]
            self._cursor -= self._head
            self._head = 0


class _NoneYet:
    """No connection yet: no user to bind messages to."""


_NONE = _NoneYet()
