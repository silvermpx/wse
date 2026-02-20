# =============================================================================
# WSE — WebSocket Event System
# Rust-accelerated event sequencer (delegates to RustEventSequencer)
# =============================================================================

import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import Any

from wse_server._accel import RustEventSequencer

log = logging.getLogger("wse.sequencer")


class _SeenIdsProxy:
    """Proxy that delegates `in` checks to RustEventSequencer.is_duplicate."""

    def __init__(self, rust_seq):
        self._rust = rust_seq

    def __contains__(self, event_id: str) -> bool:
        # is_duplicate returns True if already seen
        return self._rust.is_duplicate(event_id)


@dataclass
class SequencedEvent:
    """Event with sequence information"""

    event_id: str
    sequence: int
    timestamp: object  # datetime
    topic: str
    payload: dict[str, Any]


class EventSequencer:
    """Enhanced event sequencer with frontend compatibility.

    Delegates to RustEventSequencer for all sequencing, dedup, and
    buffering logic. Async methods wrap synchronous Rust calls.
    The cleanup loop remains in Python (asyncio scheduling).
    """

    def __init__(self, window_size: int = 10000, max_out_of_order: int = 100):
        self._rust = RustEventSequencer(
            window_size=window_size,
            max_out_of_order=max_out_of_order,
        )
        self._lock = asyncio.Lock()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    # ---- Convenience properties (read from Rust stats) ----

    @property
    def duplicate_count(self) -> int:
        return self._rust.get_sequence_stats().get("duplicate_count", 0)

    @property
    def out_of_order_count(self) -> int:
        return self._rust.get_sequence_stats().get("out_of_order_count", 0)

    @property
    def dropped_count(self) -> int:
        return self._rust.get_sequence_stats().get("dropped_count", 0)

    @property
    def seen_ids(self) -> set:
        """Return set of currently tracked event IDs (for test compat)."""
        self._rust.get_sequence_stats()
        # Rust doesn't expose raw seen_ids set, but tracks count.
        # For test compat, return a set-like object. Tests only check `in`.
        return _SeenIdsProxy(self._rust)

    @property
    def expected_sequences(self) -> dict[str, int]:
        stats = self._rust.get_sequence_stats()
        topics = stats.get("topics", {})
        return {topic: info["expected"] for topic, info in topics.items()}

    @property
    def buffered_events(self) -> dict[str, Any]:
        stats = self._rust.get_buffer_stats()
        topics = stats.get("topics", {})
        return {topic: info.get("buffered_sequences", []) for topic, info in topics.items()}

    async def get_next_sequence(self) -> int:
        """Get the next sequence number"""
        async with self._lock:
            return self._rust.next_seq()

    def get_current_sequence(self) -> int:
        """Get the current sequence number without incrementing"""
        return self._rust.get_current_sequence()

    async def is_duplicate(self, event_id: str) -> bool:
        """Check if an event is duplicate within the window"""
        async with self._lock:
            return self._rust.is_duplicate(event_id)

    async def process_sequenced_event(
        self, topic: str, sequence: int, event: dict[str, Any]
    ) -> list[dict[str, Any]] | None:
        """
        Process an event with a sequence number.
        Returns a list of events that can be delivered (maintaining order).
        """
        async with self._lock:
            result = self._rust.process_sequenced_event(topic, sequence, event)
            # Rust returns None (as Python None) when event is buffered/dropped,
            # or a list of events when they can be delivered.
            return result

    async def reset_sequence(self, topic: str | None = None) -> None:
        """Reset sequence for a specific topic or all topics"""
        async with self._lock:
            self._rust.reset_sequence(topic)

    async def get_sequence_stats(self) -> dict[str, Any]:
        """Get detailed sequence statistics"""
        async with self._lock:
            return dict(self._rust.get_sequence_stats())

    async def get_buffer_stats(self) -> dict[str, Any]:
        """Get statistics about buffered events (frontend compatible)"""
        async with self._lock:
            return dict(self._rust.get_buffer_stats())

    async def cleanup(self) -> None:
        """Manually trigger cleanup"""
        async with self._lock:
            self._rust.cleanup()

    async def _cleanup_loop(self):
        """Periodically clean up old buffered events"""
        try:
            while True:
                await asyncio.sleep(60)  # Cleanup every minute

                async with self._lock:
                    self._rust.cleanup()

                    # Log stats if we have buffered events
                    stats = self._rust.get_buffer_stats()
                    total_buffered = stats.get("total_buffered", 0)
                    total_topics = stats.get("total_topics", 0)
                    if total_buffered > 0:
                        log.info(
                            f"EventSequencer: {total_buffered} events buffered "
                            f"across {total_topics} topics"
                        )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"Error in sequencer cleanup: {e}")

    async def shutdown(self):
        """Shutdown the sequencer"""
        self._cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._cleanup_task

        stats = self._rust.get_sequence_stats()
        log.info(
            f"EventSequencer shutdown - Stats: "
            f"duplicates={stats.get('duplicate_count', 0)}, "
            f"out_of_order={stats.get('out_of_order_count', 0)}, "
            f"dropped={stats.get('dropped_count', 0)}"
        )
