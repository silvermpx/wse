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
#
# In a file the outbox outlives the process, in the format of the Rust
# client's (crates/wse-client/tests/vectors/outbox.json holds it): a log of
# lines, each ``<crc32 of the salt and the JSON, 8 hex digits> <JSON>``,
# read up to the first that does not check. A thread of its own writes it,
# the lines of a batch at once and to the disk right after.
# =============================================================================

from __future__ import annotations

import atexit
import errno
import hashlib
import json
import os
import queue
import secrets
import socket
import sys
import threading
import time
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field

from ._logging import logger


@dataclass(frozen=True)
class Offline:
    """The outbox: where messages wait until the server answers for them.

    Args:
        path: The file they wait in, so that they outlive the process; the
            client's alone while it runs (a lock file beside it, ``.lock``).
            ``None`` (default) keeps them in memory.
        max_messages: How many wait at most, those written and not answered
            yet included (default 100,000). Past it a send fails.
        max_bytes: How many bytes of messages (their JSON) wait at most
            (default 16 MiB).
        max_age: Seconds a message may wait to be written first (default an
            hour); one not written by then is dropped and the application
            told. Once written it waits for the server's answer whatever its
            age.
    """

    path: str | None = None
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


#: Says what failed in the file: the operation (``open``, ``write``), why.
Report = Callable[[str, str], None]


class _Kept:
    """A message kept."""

    __slots__ = ("at", "id", "owned", "requires", "s", "size", "text", "until")

    def __init__(
        self,
        s: int,
        message_id: str,
        text: str,
        at: float,
        until: float,
        owned: bool,
        size: int | None = None,
        requires: tuple[str, ...] | None = None,
    ) -> None:
        self.s = s
        self.id = message_id
        # The message as it goes, numbered: ``{"s":N,`` before the rest.
        self.text = text
        self.at = at
        self.until = until
        self.owned = owned
        # Topics each continuous right before it goes, every time it goes:
        # once more after a reconnect too.
        self.requires = requires
        # Its place in the room: the size of the message as it was sent.
        self.size = len(text) if size is None else size


def numbered(s: int, text: str) -> str:
    """The message with its number first: ``{"s":N,`` before the rest."""
    rest = text.lstrip()[1:]
    if rest.lstrip().startswith("}"):
        return '{"s":%d%s' % (s, rest)
    return '{"s":%d,%s' % (s, rest)


def unnumbered(text: str) -> str:
    """The message without its number, for a server that does not answer."""
    if not text.startswith('{"s":'):
        return text
    rest = text[5:].lstrip("0123456789")
    if rest.startswith(","):
        rest = rest[1:]
    return "{" + rest


class Outbox:
    """The outbox of one client: see the module's docstring."""

    def __init__(
        self,
        offline: Offline,
        *,
        producer: str | None = None,
        report: Report | None = None,
    ) -> None:
        self._limits = offline
        self.producer = producer or secrets.token_hex(16)
        self._report = report or (lambda operation, why: None)
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
        # Written ahead: the highest number the file was handed the mark of
        # (that up to it, messages may have gone). A numbered message goes
        # once the file wrote its mark, so that after a crash its number is
        # never another's.
        self._promised = 0
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
        # Places held for sends that wait for their topics: each joins the
        # outbox once they are continuous, its place taken from its send on.
        self._held = 0
        self._held_bytes = 0
        # When the first message not written yet runs out (time.time()).
        self._earliest = float("inf")
        # The file, when there is one, and whether a write of it failed (it
        # is written anew then, at most once a second).
        self._log: _Log | None = None
        self._owed = False
        self._heal_at = 0.0
        # Called with the seconds until a rewrite owed can go (one a second
        # at most): the client's loop calls flush() then, whether or not
        # anything changes meanwhile.
        self.wake: Callable[[float], None] | None = None
        # Called, from the file's own thread, once the file wrote what
        # messages waited for: the client's loop writes the outbox then.
        self.writable: Callable[[], None] | None = None
        # Counts the clears: a batch handed out before one goes no further.
        self.generation = 0
        if offline.path is not None:
            try:
                self._log, read = _Log.open(offline.path, self._report, self._wrote)
            except OSError as exc:
                self._report("open", f"{offline.path}: {exc}")
            else:
                if read is not None:
                    self._restore(read)
                self._rewrite()

    def _wrote(self) -> None:
        writable = self.writable
        if writable is not None:
            writable()

    def _restore(self, read: _Read) -> None:
        self.producer = read.producer
        self._node = read.node
        self._written = read.written
        self._promised = read.written
        self._next = read.next
        self._user = read.user if read.user is not None else _NONE
        for record in read.records:
            record.size = len(unnumbered(record.text))
            self._records.append(record)
            self._count += 1
            self._bytes += record.size
            self._earliest = min(self._earliest, record.until)

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

    def hold(self, size: int) -> bool:
        """Holds a place for a message of *size* the client keeps later (a
        send that waits for its topics); False when there is none."""
        if (
            self._count + self._held + 1 > self._limits.max_messages
            or self._bytes + self._held_bytes + size > self._limits.max_bytes
        ):
            return False
        self._held += 1
        self._held_bytes += size
        return True

    def unhold(self, size: int) -> None:
        """A place held for a message of *size* is not needed any more."""
        self._held -= 1
        self._held_bytes -= size

    def add(
        self,
        message_id: str,
        text: str,
        ttl: float | None,
        now: float,
        requires: tuple[str, ...] | None = None,
        held: bool = False,
    ) -> int:
        """Keeps a message (its JSON object): its number, or 0 when full.
        One that requires topics goes, each time, only while they are
        continuous (:meth:`due`). *held*: its place was held (:meth:`hold`),
        and it takes it."""
        size = len(text)
        if held:
            self.unhold(size)
        elif (
            self._count + self._held + 1 > self._limits.max_messages
            or self._bytes + self._held_bytes + size > self._limits.max_bytes
        ):
            return 0
        s = self._next
        self._next += 1
        wait = ttl if ttl is not None and ttl >= 0 else self._limits.max_age
        record = _Kept(
            s,
            message_id,
            numbered(s, text),
            now,
            now + wait,
            owned=isinstance(self._user, str),
            size=size,
            requires=requires or None,
        )
        self._records.append(record)
        self._count += 1
        self._bytes += size
        self._earliest = min(self._earliest, record.until)
        if self._log is not None:
            self._log.push_add(record)
        self._changed()
        return s

    def connected(self, user: str | None, now: float) -> Outcomes:
        """The server took a connection of *user*: another user's messages go,
        and those that waited too long."""
        outcomes = Outcomes()
        self._ready = True
        if self._user is _NONE or self._user != user:
            if self._user is not _NONE:
                # What was sent before any connection goes on any, until a
                # connection wrote it: then it was that connection's user's
                # (the server may have taken it as theirs), and it never goes
                # as another's.
                written = self._written
                self._remove_where(
                    "user_changed", outcomes, lambda _, r: r.owned or r.s <= written
                )
            self._user = user
            self._put({"user": user})
        if self._earliest <= now:
            self.expire(now, outcomes)
        self._changed()
        return outcomes

    def server_answers(self, answers: bool) -> None:
        """The server of this connection answers numbered messages, or not."""
        self._plain = not answers

    def answered(
        self,
        acked: int,
        refused: list[tuple[int, int, str]],
        node: str,
        unsure: int = 0,
    ) -> Outcomes:
        """The server's answer to the hello: what it decided, the refusals
        among that, its name, and how far past what it decided it cannot tell
        whether it took a message (*unsure*, 0 for nothing). It may come
        before the server took the connection or after."""
        outcomes = Outcomes()
        for start, end, why in refused:
            outcomes.merge(self.refused(start, end, why))
        outcomes.merge(self.acked(acked))
        # Past both, the server took none of what may have gone: those are
        # like any not written yet, and what ran out goes as expired (the
        # next time they are due).
        may_have_gone = max(acked, min(self._written, unsure))
        if may_have_gone < self._written:
            self._written = may_have_gone
            self._rearm()
        self._answered = True
        if self._node != node:
            self._node = node
            self._put({"node": node})
        self._changed()
        return outcomes

    def acked(self, s: int) -> Outcomes:
        """The server took every message up to and with *s*."""
        outcomes = Outcomes()
        gone = self._take_front(s, outcomes)
        while self._limbo and self._limbo[0][0] <= s:
            outcomes.delivered.append(self._limbo.pop(0)[1])
        if gone:
            self._put({"ack": s})
            self._changed()
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
        self._changed()
        return outcomes

    def due(
        self,
        now: float,
        outcomes: Outcomes,
        continuous: Callable[[tuple[str, ...]], bool] | None = None,
    ) -> list[tuple[int, str]]:
        """The messages for this connection's writer, in order, each with its
        number and its text as it goes: all it did not take yet, once the
        server took the connection and its mode is known, up to one whose
        topics are not *continuous* now (it waits in its place, and what
        comes after it with it); then the file gets the batch's lines. What
        waited too long to be sent goes instead."""
        out: list[tuple[int, str]] = []
        if self._ready and (self._answered or self._plain):
            if self._earliest <= now:
                self.expire(now, outcomes)
            records = self._records
            numbered_now = self._answered
            # What the file wrote that messages up to may have gone; None:
            # no file to wait for (none, or one that fails).
            log = self._log
            stored = log.written if numbered_now and log and not log.behind else None
            ahead = False
            for i in range(self._cursor, len(records)):
                record = records[i]
                assert record is not None
                if (
                    record.requires
                    and continuous is not None
                    and not continuous(record.requires)
                ):
                    break
                # Written ahead: it goes once the file wrote the mark.
                if stored is not None and record.s > stored:
                    ahead = True
                    break
                text = record.text if numbered_now else unnumbered(record.text)
                out.append((record.s, text))
                self._cursor = i + 1
            if out and numbered_now and out[-1][0] > self._written:
                self._written = out[-1][0]
            # The mark for every message kept now, in this batch: they go
            # once the file wrote it.
            last = records[-1] if records else None
            if ahead and last is not None and last.s > self._promised:
                self._promised = last.s
                self._put({"written": last.s})
        self.flush()
        return out

    def writing(self, s: int) -> None:
        """The connection hands message *s* to the server now: a clear from
        here on waits for its answer (or, from a server that does not
        answer, for its write)."""
        self._flushed = max(self._flushed, s)

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
        self.flush()
        return outcomes

    def close(self) -> Outcomes:
        """The client ends: in memory, what waits goes with it (``closed``);
        in a file, it waits for the next client."""
        outcomes = self.disconnected()
        if self._log is None:
            for record in self._records[self._head :]:
                assert record is not None
                outcomes.refused("closed", record.id)
            self._reset()
        return outcomes

    def close_file(self) -> None:
        """The file is written and let go: the next client may take it."""
        log = self._log
        if log is None:
            return
        if self._owed or log.take_broken():
            self._rewrite()
        else:
            log.flush()
        log.close()
        self._log = None

    def clear(self) -> Outcomes:
        """The user signed out: every message goes. One the server has in hand
        on this connection is delivered when it confirms it on it, and not
        when the connection ends first."""
        outcomes = Outcomes()
        # A batch the connection's writer holds goes no further.
        self.generation += 1
        in_hand = self._flushed
        numbers = []
        for record in self._records[self._head :]:
            assert record is not None
            numbers.append(record.s)
            if record.s <= in_hand:
                self._limbo.append((record.s, record.id))
            else:
                outcomes.refused("cleared", record.id)
        self._reset()
        if numbers:
            self._put({"drop": numbers})
        self._user = _NONE
        self._put({"user": None})
        self._changed()
        return outcomes

    def expire(self, now: float, outcomes: Outcomes) -> None:
        """Drops what waited longer than it may to be written."""
        written, cursor = self._written, self._cursor
        self._remove_where(
            "expired",
            outcomes,
            lambda at, r: at >= cursor and r.s > written and r.until < now,
        )
        self._rearm()
        self._changed()

    def _rearm(self) -> None:
        """When the first message not written yet runs out."""
        earliest = float("inf")
        for record in self._records[self._head :]:
            assert record is not None
            if record.s > self._written and record.until < earliest:
                earliest = record.until
        self._earliest = earliest

    # -- Inside -----------------------------------------------------------------

    def _take_front(self, s: int, outcomes: Outcomes) -> int:
        gone = 0
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
            gone += 1
        # The answer to a hello may take what this connection did not write.
        self._cursor = max(self._cursor, self._head)
        if self._head > 1024 and self._head * 2 > len(records):
            del records[: self._head]
            self._cursor -= self._head
            self._head = 0
        return gone

    def _reset(self) -> None:
        self._records = []
        self._head = 0
        self._cursor = 0
        self._count = 0
        self._bytes = 0
        self._earliest = float("inf")

    def _remove_where(self, why: str, outcomes: Outcomes, gone) -> None:  # noqa: ANN001
        kept: list[_Kept | None] = []
        numbers = []
        cursor = self._cursor - self._head
        for i in range(self._head, len(self._records)):
            record = self._records[i]
            assert record is not None
            if gone(i, record):
                self._count -= 1
                self._bytes -= record.size
                outcomes.refused(why, record.id)
                numbers.append(record.s)
                if i < self._cursor:
                    cursor -= 1
            else:
                kept.append(record)
        self._records = kept
        self._head = 0
        self._cursor = cursor
        if numbers:
            self._put({"drop": numbers})

    def _put(self, line: dict[str, object]) -> None:
        """A line for the file's batch."""
        if self._log is not None:
            self._log.push(line)

    def _changed(self) -> None:
        """With no connection, the file gets what changed now; with one, at
        the connection's next write (``due``), a batch at once."""
        if not self._ready:
            self.flush()

    def flush(self) -> None:
        """Hands the batch's lines to the file's thread; writes the file anew
        instead once what went outweighs what is kept, or after a write
        failed (the lines are in what is written anew)."""
        log = self._log
        if log is None:
            return
        self._owed = self._owed or log.take_broken()
        if self._owed:
            now = time.monotonic()
            if now >= self._heal_at:
                self._owed = False
                self._heal_at = now + 1.0
                self._rewrite()
            else:
                log.forget()
                if self.wake is not None:
                    self.wake(self._heal_at - now)
        elif log.lines > 2 * self._count + 64:
            self._rewrite()
        else:
            log.flush(self._promised)

    def _rewrite(self) -> None:
        """Writes the file anew, with what is kept now alone, under a new
        salt."""
        log = self._log
        if log is None:
            return
        salt = secrets.token_hex(16)
        head = _dumps(
            {
                "wse_outbox": 1,
                "salt": salt,
                "producer": self.producer,
                "next": self._next,
            }
        )
        lines = [_line(b"", head)]
        salted = salt.encode()
        if self._node is not None:
            lines.append(_line(salted, _dumps({"node": self._node})))
        # What went meanwhile, while the file failed, is in it.
        mark = max(self._written, self._promised)
        self._promised = mark
        if mark > 0:
            lines.append(_line(salted, _dumps({"written": mark})))
        if isinstance(self._user, str):
            lines.append(_line(salted, _dumps({"user": self._user})))
        for record in self._records[self._head :]:
            assert record is not None
            lines.append(_line(salted, _add_json(record)))
        log.rewrite(b"".join(lines), salt, len(lines), mark)


class _NoneYet:
    """No connection yet: no user to bind messages to."""


_NONE = _NoneYet()


# -- The file ---------------------------------------------------------------------


def _dumps(value: object) -> str:
    """JSON as the Rust client writes it: compact, UTF-8 as it is."""
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _line(salt: bytes, body: str) -> bytes:
    """A line: its check, a space, its JSON, a newline."""
    data = body.encode()
    return b"%08x %s\n" % (zlib.crc32(data, zlib.crc32(salt)) & 0xFFFFFFFF, data)


def _add_prefix(
    s: int,
    message_id: str,
    at: int,
    until: int,
    owned: bool,
    requires: tuple[str, ...] | None,
) -> str:
    """A kept message's line up to its text, as every client writes it."""
    return '{"add":{"s":%d,"id":%s,"at":%d,"until":%d,"owned":%s,%s"text":' % (
        s,
        _dumps(message_id),
        at,
        until,
        "true" if owned else "false",
        '"requires":%s,' % _dumps(list(requires)) if requires else "",
    )


def _add_json(record: _Kept) -> str:
    """A message kept, its text as it is (a JSON object)."""
    prefix = _add_prefix(
        record.s,
        record.id,
        round(record.at * 1000),
        min(round(record.until * 1000), 2**63 - 1),
        record.owned,
        record.requires,
    )
    return "%s%s}}" % (prefix, record.text)


@dataclass
class _Read:
    """What a file held when it was opened."""

    producer: str
    node: str | None
    written: int
    next: int
    user: str | None
    records: list[_Kept]


def _checked(line: bytes, salt: bytes) -> bytes | None:
    """One line's JSON, when its check holds."""
    if len(line) < 10 or line[8:9] != b" ":
        return None
    try:
        crc = int(line[:8], 16)
    except ValueError:
        return None
    body = line[9:]
    return body if zlib.crc32(body, zlib.crc32(salt)) & 0xFFFFFFFF == crc else None


def _read(data: bytes) -> _Read | None:
    """What a file holds, read up to the first line that does not check: a
    tail cut short, or left from an older file, is not read. None: not a
    file of this format."""
    lines = data.split(b"\n")
    body = _checked(lines[0], b"")
    if body is None:
        return None
    try:
        head = json.loads(body)
    except ValueError:
        return None
    if not isinstance(head, dict) or head.get("wse_outbox") != 1:
        return None
    salt = str(head.get("salt", "")).encode()
    read = _Read(
        producer=str(head.get("producer", "")),
        node=None,
        written=0,
        next=max(1, int(head.get("next", 1))),
        user=None,
        records=[],
    )
    # The records are in the order of their numbers: what an answer took is
    # at the front, before ``first``.
    records: list[_Kept] = []
    first = 0
    for line in lines[1:]:
        if not line:
            continue
        body = _checked(line, salt)
        try:
            item = json.loads(body) if body is not None else None
        except ValueError:
            item = None
        if not isinstance(item, dict) or len(item) != 1:
            logger.warning("The outbox's file ends in a line that does not check")
            break
        ((kind, value),) = item.items()
        try:
            if kind == "add":
                record = _record(body.decode(), value)
                if record is None:
                    raise ValueError(kind)
                read.next = max(read.next, record.s + 1)
                records.append(record)
            elif kind == "written":
                written = _number(value)
                read.written = max(read.written, written)
                read.next = max(read.next, written + 1)
            elif kind == "ack":
                acked = _number(value)
                while first < len(records) and records[first].s <= acked:
                    first += 1
            elif kind == "drop":
                if not isinstance(value, list):
                    raise ValueError(kind)
                gone = {_number(n) for n in value}
                records = [r for r in records[first:] if r.s not in gone]
                first = 0
            elif kind == "user":
                if value is not None and not isinstance(value, str):
                    raise ValueError(kind)
                read.user = value
            elif kind == "node":
                if not isinstance(value, str):
                    raise ValueError(kind)
                read.node = value
            # A line of a kind a later version writes is skipped: a change it
            # must not skip is a new format (``wse_outbox``).
        except (TypeError, ValueError):
            logger.warning("The outbox's file ends in a line that does not check")
            break
    read.records = records[first:]
    return read


def _number(value: object) -> int:
    """A number of the file: a whole number, not below zero."""
    if type(value) is not int or value < 0:
        raise ValueError(value)
    return value


def _record(body: str, value: object) -> _Kept | None:
    """A message kept, as read: its text taken as it was written."""
    try:
        if not isinstance(value, dict) or not isinstance(value["id"], str):
            return None
        s, message_id = _number(value["s"]), value["id"]
        at, until, owned = _number(value["at"]), _number(value["until"]), value["owned"]
        if not isinstance(owned, bool) or not isinstance(value["text"], dict):
            return None
        topics = value.get("requires")
        if topics is not None and not (
            isinstance(topics, list)
            and topics
            and all(isinstance(t, str) and t for t in topics)
        ):
            return None
        requires = tuple(topics) if topics else None
    except (KeyError, TypeError, ValueError):
        return None
    prefix = _add_prefix(s, message_id, at, until, owned, requires)
    if not (body.startswith(prefix) and body.endswith("}}")):
        return None
    return _Kept(
        s,
        message_id,
        body[len(prefix) : -2],
        at / 1000,
        until / 1000,
        owned,
        requires=requires,
    )


# Bytes as they are: Windows opens a file as text otherwise, a newline
# written as two bytes, and no line of it checks.
_BINARY = getattr(os, "O_BINARY", 0)
_APPEND = os.O_WRONLY | os.O_CREAT | os.O_APPEND | _BINARY
_CREATE = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _BINARY


def _sync(fd: int) -> None:
    """To the disk: on Apple's systems past the drive's own cache."""
    if sys.platform == "darwin":
        import fcntl

        fcntl.fcntl(fd, fcntl.F_FULLFSYNC)
    else:
        os.fsync(fd)


def _write_all(fd: int, data: bytes) -> None:
    """Writes all of *data*: one write may take only part of it."""
    view = memoryview(data)
    while view:
        taken = os.write(fd, view)
        if taken <= 0:
            raise OSError("the write took nothing")
        view = view[taken:]


def _lock(fd: int) -> bool:
    """The file the client's alone; False when another client has it."""
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _lock_name(path: str) -> str:
    """The name of the lock every client takes of the file at *path* on
    Linux: ``wse-outbox-`` and 40 hex digits of the SHA-256 of its path,
    its directory's resolved."""
    where = os.path.join(
        os.path.realpath(os.path.dirname(path) or "."), os.path.basename(path)
    )
    return "wse-outbox-" + hashlib.sha256(os.fsencode(where)).hexdigest()[:40]


def _named_lock(path: str) -> socket.socket | None:
    """On Linux, the lock beside the lock file that the TypeScript client
    in Node takes (it has no file locks): a socket in the abstract namespace,
    named for the file, gone with the process. None elsewhere."""
    if not (sys.platform.startswith("linux") or sys.platform == "android"):
        return None
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind("\0" + _lock_name(path))
    except OSError as exc:
        sock.close()
        if exc.errno == errno.EADDRINUSE:
            raise OSError("it is another client's") from None
        raise
    return sock


class _Log:
    """The file, as the client's loop sees it: a batch of lines, handed to
    the thread that writes them."""

    def __init__(
        self,
        path: str,
        lock_fd: int,
        report: Report,
        wrote: Callable[[], None],
        named: socket.socket | None = None,
    ) -> None:
        self.path = path
        self._salt = b""
        self._batch: list[bytes] = []
        # Lines in the file, the batch's included.
        self.lines = 0
        self._broken = threading.Event()
        # The highest mark the file holds, past a crash of the process; and
        # whether it is behind (a write failed: messages do not wait for it
        # until it is written anew). The file's thread sets both.
        self.written = 0
        self.behind = False
        self._jobs: queue.SimpleQueue[tuple[str, bytes, int] | None] = (
            queue.SimpleQueue()
        )
        self._thread = threading.Thread(
            target=_Writer(path, lock_fd, report, self._broken, self, wrote, named).run,
            args=(self._jobs,),
            name="wse-outbox",
            daemon=True,
        )
        self._thread.start()
        # A process that ends still writes what it handed over.
        atexit.register(self.close)
        self._closed = False

    @staticmethod
    def open(
        path: str, report: Report, wrote: Callable[[], None]
    ) -> tuple[_Log, _Read | None]:
        """The file at *path*, the client's alone from now on, and what it
        holds (None: a new file, or not one of this format)."""
        lock_fd = os.open(path + ".lock", os.O_RDWR | os.O_CREAT | _BINARY, 0o600)
        if not _lock(lock_fd):
            os.close(lock_fd)
            raise OSError("it is another client's")
        named: socket.socket | None = None
        try:
            named = _named_lock(path)
            try:
                with open(path, "rb") as f:
                    read = _read(f.read())
            except FileNotFoundError:
                read = None
            log = _Log(path, lock_fd, report, wrote, named)
            log.written = read.written if read is not None else 0
            return log, read
        except OSError:
            if named is not None:
                named.close()
            os.close(lock_fd)
            raise

    def push(self, line: dict[str, object]) -> None:
        self._batch.append(_line(self._salt, _dumps(line)))
        self.lines += 1

    def push_add(self, record: _Kept) -> None:
        self._batch.append(_line(self._salt, _add_json(record)))
        self.lines += 1

    def flush(self, mark: int = 0) -> None:
        """Hands the batch to the thread, with *mark*, the highest mark in
        the file once it is written."""
        if self._batch:
            self._jobs.put(("append", b"".join(self._batch), mark))
            self._batch = []

    def forget(self) -> None:
        """The batch is in what is written anew."""
        self._batch = []

    def take_broken(self) -> bool:
        broken = self._broken.is_set()
        self._broken.clear()
        return broken

    def rewrite(self, data: bytes, salt: str, lines: int, mark: int) -> None:
        self._batch = []
        self._salt = salt.encode()
        self.lines = lines
        self._jobs.put(("rewrite", data, mark))

    def close(self) -> None:
        """The thread writes what it was handed and lets the file go."""
        if self._closed:
            return
        self._closed = True
        atexit.unregister(self.close)
        self.flush()
        self._jobs.put(None)
        self._thread.join(timeout=10)


class _Writer:
    """The thread that writes the file."""

    def __init__(
        self,
        path: str,
        lock_fd: int,
        report: Report,
        broken: threading.Event,
        log: _Log,
        wrote: Callable[[], None],
        named: socket.socket | None = None,
    ) -> None:
        self.path = path
        self.tmp = path + ".tmp"
        self.lock_fd = lock_fd
        self.named = named
        self.report = report
        self.broken = broken
        # The log whose mark it raises, and who hears it rose.
        self.log = log
        self.wrote = wrote
        self.fd: int | None = os.open(path, _APPEND, 0o600)
        # A write failed: what is appended waits for the file anew.
        self.skipping = False
        # The last failure was said; the next is said after a success.
        self.failing = False

    def run(self, jobs: queue.SimpleQueue[tuple[str, bytes, int] | None]) -> None:
        try:
            while True:
                job = jobs.get()
                done = job is None
                mark = 0
                dirty = False
                if job is not None:
                    dirty, mark = self.take(job, mark)
                while not done:
                    try:
                        job = jobs.get_nowait()
                    except queue.Empty:
                        break
                    if job is None:
                        done = True
                    else:
                        appended, mark = self.take(job, mark)
                        dirty = appended or dirty
                # Written, past a crash of the process: what waited for the
                # mark may go; the disk has it once synced, at once after.
                if mark > self.log.written:
                    self.log.written = mark
                    self.wrote()
                if dirty and self.fd is not None:
                    try:
                        _sync(self.fd)
                    except OSError as exc:
                        self.fail(f"cannot sync it to the disk: {exc}")
                if done:
                    return
        finally:
            if self.fd is not None:
                os.close(self.fd)
            os.close(self.lock_fd)
            if self.named is not None:
                self.named.close()

    def take(self, job: tuple[str, bytes, int], mark: int) -> tuple[bool, int]:
        """Whether it appended (to be synced), and the mark of what was
        written."""
        kind, data, holds = job
        if kind == "append":
            if self.skipping:
                return False, mark
            try:
                if self.fd is None:
                    raise OSError("no file")
                _write_all(self.fd, data)
            except OSError as exc:
                self.fail(f"cannot write it: {exc}")
                return False, mark
            return True, max(mark, holds)
        try:
            self.rewrite(data)
        except OSError as exc:
            self.fail(f"cannot write it anew: {exc}")
            return False, mark
        # Synced as it was written.
        self.skipping = False
        self.failing = False
        self.log.behind = False
        return False, max(mark, holds)

    def fail(self, why: str) -> None:
        self.skipping = True
        self.log.behind = True
        self.broken.set()
        if not self.failing:
            self.failing = True
            self.report("write", f"{self.path}: {why}")

    def rewrite(self, data: bytes) -> None:
        """The new file beside the old, on the disk, then in its place."""
        try:
            fd = os.open(self.tmp, _CREATE, 0o600)
            try:
                _write_all(fd, data)
                _sync(fd)
            finally:
                os.close(fd)
        except OSError:
            try:
                os.remove(self.tmp)
            except OSError:
                pass
            raise
        # Closed before it is replaced: Windows does not replace an open file.
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            os.replace(self.tmp, self.path)
        except OSError:
            try:
                os.remove(self.tmp)
            except OSError:
                pass
            self.fd = os.open(self.path, _APPEND, 0o600)
            raise
        if sys.platform != "win32":
            directory = os.path.dirname(self.path) or "."
            try:
                dfd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(dfd)
                finally:
                    os.close(dfd)
            except OSError:
                pass
        self.fd = os.open(self.path, _APPEND, 0o600)
