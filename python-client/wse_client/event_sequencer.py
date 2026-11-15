# =============================================================================
# WSE Python Client -- Event Sequencer
# =============================================================================
#
# Idempotent delivery: id dedup + per-topic (epoch, offset) dedup/gap detection.
# Port of ts-client/services/EventSequencer.ts.
# =============================================================================

from __future__ import annotations

import time

from .constants import DUPLICATE_MAX_AGE, SEQUENCE_WINDOW_SIZE

#: Streams (origin epochs) remembered per topic: a cluster's nodes, plus the
#: epochs a restart leaves behind.
MAX_STREAMS_PER_TOPIC = 32


class EventSequencer:
    """Idempotent delivery: id dedup + per-topic (epoch, offset) dedup/gap detection.

    Tracks seen message IDs in a sliding window (for unstamped messages) and the
    last-seen (epoch, offset) per topic (the authoritative dedup/ordering for
    stamped messages). Also owns the client's outbound send sequence counter.

    Args:
        window_size: Dedup window size (default 10 000).
        max_age: Seconds before expiring old entries (default 300).
    """

    def __init__(
        self,
        window_size: int = SEQUENCE_WINDOW_SIZE,
        max_age: float = DUPLICATE_MAX_AGE,
    ) -> None:
        self._window_size = window_size
        self._max_age = max_age

        self._seen_ids: dict[str, float] = {}
        self._sequence = 0
        self._cleanup_counter = 0
        self._duplicates_detected = 0
        # Per-topic last delivered position: topic -> (epoch, offset), the
        # anchor a recovery asks from. Survives reconnects.
        self._topic_positions: dict[str, tuple[str, int]] = {}
        # Per-topic, per-stream last offset: topic -> {epoch: offset}. A topic
        # published on several cluster nodes interleaves their streams (one
        # epoch each), each in order on its own: the authoritative dedup and
        # gap detection for stamped messages.
        self._streams: dict[str, dict[str, int]] = {}
        # When each stream last moved or became known (time.time(): a laptop's
        # sleep counts, as it does for the server's history).
        self._seen: dict[str, dict[str, float]] = {}

    def get_next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    @property
    def current_sequence(self) -> int:
        return self._sequence

    def is_duplicate(self, event_id: str) -> bool:
        """Check if event ID was already seen. Registers it if new."""
        self._cleanup_counter += 1
        if self._cleanup_counter >= 100:
            self._cleanup_old_entries()
            self._cleanup_counter = 0

        if event_id in self._seen_ids:
            self._duplicates_detected += 1
            return True

        self._seen_ids[event_id] = time.monotonic()

        # Evict oldest if window full. Entries are never re-inserted, so dict
        # insertion order is age order and the first key is the oldest: O(1)
        # instead of scanning the whole window on every message.
        if len(self._seen_ids) > self._window_size:
            del self._seen_ids[next(iter(self._seen_ids))]

        return False

    def check_topic_stamp(self, topic: str, epoch: str, offset: int) -> str:
        """Idempotent dedup + gap detection by ``(epoch, offset)`` within the
        topic's stream of that epoch.

        Returns one of:
            ``"duplicate"`` -- already delivered (offset <= last seen in its
                stream); the caller should drop it. This is what makes replay /
                reconnect overlap safe independently of the id window.
            ``"gap"`` -- the offset jumped ahead of the expected next in its
                stream. The position does not move; the caller holds the
                message while it recovers the hole, or re-baselines with
                :meth:`set_topic_position` when it gives up.
            ``"deliver"`` -- the next in order, or the first message of a stream
                not seen yet (another node publishing on the topic, or one that
                restarted).
        """
        streams = self._streams.setdefault(topic, {})
        last = streams.get(epoch)
        if last is None:
            self._advance(topic, epoch, offset)
            return "deliver"
        if offset <= last:
            return "duplicate"
        if offset == last + 1:
            self._advance(topic, epoch, offset)
            return "deliver"
        # Gap: do NOT advance. The caller recovers from `last`, which replays the
        # whole missed range (last+1 .. head) in order; advancing here would make
        # those replayed messages look like duplicates and they'd be dropped.
        return "gap"

    def _advance(self, topic: str, epoch: str, offset: int) -> None:
        streams = self._streams.setdefault(topic, {})
        streams.pop(epoch, None)  # most recent last
        streams[epoch] = offset
        if len(streams) > MAX_STREAMS_PER_TOPIC:
            oldest = next(iter(streams))
            del streams[oldest]
            self._seen.get(topic, {}).pop(oldest, None)
        self._seen.setdefault(topic, {})[epoch] = time.time()
        self._topic_positions[topic] = (epoch, offset)

    def topic_streams(self, topic: str) -> dict[str, int]:
        """The last offset of each stream of *topic*: epoch -> offset."""
        return dict(self._streams.get(topic, {}))

    def raise_stream(self, topic: str, epoch: str, offset: int) -> None:
        """Take *offset* as where the stream of *epoch* stands without a
        message delivered (a server's reply lists its streams): a stream not
        known yet starts there, as the oldest; a known one moves up to it. The
        last message delivered stays the topic's anchor."""
        streams = self._streams.setdefault(topic, {})
        own = streams.get(epoch)
        if own is not None:
            if offset > own:
                streams[epoch] = offset
                self._seen.setdefault(topic, {})[epoch] = time.time()
            return
        if len(streams) >= MAX_STREAMS_PER_TOPIC:
            return
        self._streams[topic] = {epoch: offset, **streams}
        self._seen.setdefault(topic, {})[epoch] = time.time()
        self._topic_positions.setdefault(topic, (epoch, offset))

    def forget_stream(self, topic: str, epoch: str) -> None:
        """Forget the stream of *epoch*; the anchor moves to the last one left."""
        streams = self._streams.get(topic)
        if not streams or streams.pop(epoch, None) is None:
            return
        self._seen.get(topic, {}).pop(epoch, None)
        position = self._topic_positions.get(topic)
        if position is not None and position[0] == epoch:
            if streams:
                last = next(reversed(streams))
                self._topic_positions[topic] = (last, streams[last])
            else:
                del self._topic_positions[topic]

    def forget_quiet_streams(self, topic: str, before: float, keep) -> None:
        """Forget the streams of *topic* that did not move since *before*
        (time.time()), but those *keep* keeps."""
        seen = self._seen.get(topic, {})
        for epoch in list(self._streams.get(topic, {})):
            if seen.get(epoch, 0.0) < before and not keep(epoch):
                self.forget_stream(topic, epoch)

    def topic_position(self, topic: str) -> tuple[str, int] | None:
        """Last delivered ``(epoch, offset)`` of a topic, for recovery requests."""
        return self._topic_positions.get(topic)

    def stream_position(self, topic: str, epoch: str) -> tuple[str, int] | None:
        """Last delivered ``(epoch, offset)`` of a topic's stream of *epoch*."""
        offset = self._streams.get(topic, {}).get(epoch)
        return None if offset is None else (epoch, offset)

    def topic_positions(self) -> dict[str, tuple[str, int]]:
        """Copy of every topic's last-seen ``(epoch, offset)``."""
        return dict(self._topic_positions)

    def set_topic_position(self, topic: str, epoch: str, offset: int) -> None:
        """Set a topic's position (re-baseline after an unrecoverable gap)."""
        self._advance(topic, epoch, offset)

    def clear_topic(self, topic: str) -> None:
        """Forget a topic's position (on unsubscribe), so a later subscribe
        re-baselines on its first message instead of seeing a gap."""
        self._topic_positions.pop(topic, None)
        self._streams.pop(topic, None)
        self._seen.pop(topic, None)

    def _cleanup_old_entries(self) -> None:
        # Age order == insertion order: drop the expired prefix only.
        cutoff = time.monotonic() - self._max_age
        seen = self._seen_ids
        while seen:
            oldest = next(iter(seen))
            if seen[oldest] >= cutoff:
                break
            del seen[oldest]

    def get_stats(self) -> dict:
        return {
            "current_sequence": self._sequence,
            "duplicate_window_size": len(self._seen_ids),
            "topic_positions": len(self._topic_positions),
            "duplicates_detected": self._duplicates_detected,
        }

    def reset(self) -> None:
        """Full reset for a NEW session (e.g. token change) -- clears per-topic
        positions too. NOT to be called on a transient reconnect, which must keep
        positions so (epoch, offset) dedup spans the reconnect."""
        self._seen_ids.clear()
        self._sequence = 0
        self._duplicates_detected = 0
        self._cleanup_counter = 0
        self._topic_positions.clear()
        self._streams.clear()
        self._seen.clear()
