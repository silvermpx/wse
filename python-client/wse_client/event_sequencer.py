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
        # Per-topic last-seen recovery position: topic -> (epoch, offset). The
        # AUTHORITATIVE dedup/ordering for stamped messages; survives reconnects
        # within an epoch (not cleared on reconnect). Replaces the old seq-based
        # reorder buffer, which the server's global seq made unsound.
        self._topic_positions: dict[str, tuple[str, int]] = {}

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

        # Evict oldest if window full
        if len(self._seen_ids) > self._window_size:
            oldest_key = min(self._seen_ids, key=self._seen_ids.get)  # type: ignore[arg-type]
            del self._seen_ids[oldest_key]

        return False

    def check_topic_stamp(self, topic: str, epoch: str, offset: int) -> str:
        """Idempotent dedup + gap detection by per-topic ``(epoch, offset)``.

        Returns one of:
            ``"duplicate"`` -- already delivered (offset <= last seen on the same
                epoch); the caller should drop it. This is what makes replay /
                reconnect overlap safe independently of the id window.
            ``"gap"`` -- the offset jumped ahead of the expected next; the caller
                should still deliver it but trigger recovery to fill the hole.
            ``"deliver"`` -- the next in order, or the first message on a new or
                changed epoch (server restart -- old offsets are meaningless).
        """
        prev = self._topic_positions.get(topic)
        if prev is None or prev[0] != epoch:
            # New topic, or epoch change (server restart): accept and (re)baseline.
            self._topic_positions[topic] = (epoch, offset)
            return "deliver"
        last = prev[1]
        if offset <= last:
            return "duplicate"
        if offset == last + 1:
            self._topic_positions[topic] = (epoch, offset)
            return "deliver"
        # Gap: do NOT advance. The caller recovers from `last`, which replays the
        # whole missed range (last+1 .. head) in order; advancing here would make
        # those replayed messages look like duplicates and they'd be dropped.
        return "gap"

    def topic_position(self, topic: str) -> tuple[str, int] | None:
        """Last-seen ``(epoch, offset)`` for a topic, for recovery requests."""
        return self._topic_positions.get(topic)

    def set_topic_position(self, topic: str, epoch: str, offset: int) -> None:
        """Set a topic's position (e.g. from a subscription_update reply)."""
        self._topic_positions[topic] = (epoch, offset)

    def _cleanup_old_entries(self) -> None:
        now = time.monotonic()
        cutoff = now - self._max_age
        expired = [k for k, v in self._seen_ids.items() if v < cutoff]
        for k in expired:
            del self._seen_ids[k]

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
