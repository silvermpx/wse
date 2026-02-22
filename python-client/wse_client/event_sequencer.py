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
    """Duplicate detection and out-of-order event buffering."""

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
        self._seen_ids.clear()
        self._out_of_order_buffer.clear()
        self._expected_sequence = 0
        self._sequence = 0
        self._duplicates_detected = 0
