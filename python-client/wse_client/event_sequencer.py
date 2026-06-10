# =============================================================================
# WSE Python Client -- Event Sequencer
# =============================================================================
#
# Duplicate detection and out-of-order event buffering.
# Port of client/services/EventSequencer.ts.
# =============================================================================

from __future__ import annotations

import time

from .constants import DUPLICATE_MAX_AGE, MAX_OUT_OF_ORDER, SEQUENCE_WINDOW_SIZE
from .types import WSEEvent


class EventSequencer:
    """Duplicate detection and out-of-order event reordering.

    Tracks seen message IDs in a sliding window and buffers events
    that arrive ahead of their expected sequence number.

    Args:
        window_size: Dedup window size (default 10 000).
        max_out_of_order: Max sequence gap before resetting (default 100).
        max_age: Seconds before expiring old entries (default 300).
    """

    def __init__(
        self,
        window_size: int = SEQUENCE_WINDOW_SIZE,
        max_out_of_order: int = MAX_OUT_OF_ORDER,
        max_age: float = DUPLICATE_MAX_AGE,
    ) -> None:
        self._window_size = window_size
        self._max_out_of_order = max_out_of_order
        self._max_age = max_age

        self._seen_ids: dict[str, float] = {}
        self._expected_sequence = 0
        self._out_of_order_buffer: dict[int, tuple[WSEEvent, float]] = {}
        self._sequence = 0
        self._cleanup_counter = 0
        self._duplicates_detected = 0
        # Per-topic last-seen recovery position: topic -> (epoch, offset).
        # This is the AUTHORITATIVE dedup/ordering for stamped messages and must
        # survive reconnects within an epoch (do not clear on reconnect), unlike
        # the seq-based reorder buffer which the server's global seq makes unsound.
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

    def record_sequence(self, seq: int) -> None:
        if seq >= self._expected_sequence:
            self._expected_sequence = seq + 1

    def process_sequenced_event(
        self, seq: int, event: WSEEvent
    ) -> list[WSEEvent] | None:
        """Process event with sequence number. Returns ordered events or None."""
        # If sequence matches expected, deliver it plus any buffered followers
        if seq == self._expected_sequence:
            result = [event]
            self._expected_sequence = seq + 1

            # Drain consecutive buffered events
            while self._expected_sequence in self._out_of_order_buffer:
                buffered, _ts = self._out_of_order_buffer.pop(self._expected_sequence)
                result.append(buffered)
                self._expected_sequence += 1

            return result

        # Future sequence: buffer if gap is within limit
        gap = seq - self._expected_sequence
        if 0 < gap <= self._max_out_of_order:
            self._out_of_order_buffer[seq] = (event, time.monotonic())
            self._cleanup_stale_buffer()
            return None

        # Gap too large: reset and deliver
        if gap > self._max_out_of_order:
            self._out_of_order_buffer.clear()
            self._expected_sequence = seq + 1
            return [event]

        # Old sequence (already delivered): skip
        return None

    def _cleanup_old_entries(self) -> None:
        now = time.monotonic()
        cutoff = now - self._max_age
        expired = [k for k, v in self._seen_ids.items() if v < cutoff]
        for k in expired:
            del self._seen_ids[k]

    def _cleanup_stale_buffer(self) -> None:
        """Remove out-of-order buffered events older than max_age."""
        now = time.monotonic()
        cutoff = now - self._max_age
        expired = [
            seq for seq, (_, ts) in self._out_of_order_buffer.items() if ts < cutoff
        ]
        for seq in expired:
            del self._out_of_order_buffer[seq]

    def get_stats(self) -> dict:
        return {
            "current_sequence": self._sequence,
            "expected_sequence": self._expected_sequence,
            "duplicate_window_size": len(self._seen_ids),
            "out_of_order_buffer_size": len(self._out_of_order_buffer),
            "duplicates_detected": self._duplicates_detected,
        }

    def reset(self) -> None:
        """Full reset for a NEW session (e.g. token change) -- clears per-topic
        positions too. NOT to be called on a transient reconnect, which must keep
        positions so (epoch, offset) dedup spans the reconnect."""
        self._seen_ids.clear()
        self._out_of_order_buffer.clear()
        self._expected_sequence = 0
        self._sequence = 0
        self._duplicates_detected = 0
        self._cleanup_counter = 0
        self._topic_positions.clear()
