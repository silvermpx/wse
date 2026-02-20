"""Tests for EventSequencer."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import UTC

from wse_server.connection.sequencer import EventSequencer, SequencedEvent

# =========================================================================
# SequencedEvent dataclass
# =========================================================================


class TestSequencedEvent:
    def test_creation(self):
        from datetime import datetime

        now = datetime.now(UTC)
        evt = SequencedEvent(
            event_id="e1",
            sequence=1,
            timestamp=now,
            topic="prices",
            payload={"t": "price", "p": {"symbol": "AAPL"}},
        )
        assert evt.event_id == "e1"
        assert evt.sequence == 1
        assert evt.topic == "prices"
        assert evt.payload["t"] == "price"


# =========================================================================
# Sequential message processing
# =========================================================================


class TestSequentialProcessing:
    @pytest.mark.asyncio
    async def test_get_next_sequence_increments(self, sequencer):
        seq1 = await sequencer.get_next_sequence()
        seq2 = await sequencer.get_next_sequence()
        seq3 = await sequencer.get_next_sequence()
        assert seq1 == 1
        assert seq2 == 2
        assert seq3 == 3

    def test_get_current_sequence_no_side_effect(self, sequencer):
        initial = sequencer.get_current_sequence()
        again = sequencer.get_current_sequence()
        assert initial == again == 0

    @pytest.mark.asyncio
    async def test_first_event_on_topic_delivered_immediately(self, sequencer):
        event = {"t": "test", "id": "e1"}
        result = await sequencer.process_sequenced_event("topic_a", 1, event)
        assert result is not None
        assert len(result) == 1
        assert result[0] == event

    @pytest.mark.asyncio
    async def test_sequential_events_delivered_in_order(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e2 = {"t": "test", "id": "e2"}
        e3 = {"t": "test", "id": "e3"}

        r1 = await sequencer.process_sequenced_event("topic", 1, e1)
        r2 = await sequencer.process_sequenced_event("topic", 2, e2)
        r3 = await sequencer.process_sequenced_event("topic", 3, e3)

        assert r1 == [e1]
        assert r2 == [e2]
        assert r3 == [e3]

    @pytest.mark.asyncio
    async def test_independent_topics_have_separate_sequences(self, sequencer):
        ea = {"t": "a"}
        eb = {"t": "b"}

        # Both start at seq 1 on different topics
        ra = await sequencer.process_sequenced_event("topic_a", 1, ea)
        rb = await sequencer.process_sequenced_event("topic_b", 1, eb)

        assert ra == [ea]
        assert rb == [eb]


# =========================================================================
# Duplicate detection
# =========================================================================


class TestDuplicateDetection:
    @pytest.mark.asyncio
    async def test_first_occurrence_not_duplicate(self, sequencer):
        assert await sequencer.is_duplicate("evt_001") is False

    @pytest.mark.asyncio
    async def test_second_occurrence_is_duplicate(self, sequencer):
        await sequencer.is_duplicate("evt_001")
        assert await sequencer.is_duplicate("evt_001") is True

    @pytest.mark.asyncio
    async def test_different_ids_not_duplicate(self, sequencer):
        await sequencer.is_duplicate("evt_001")
        assert await sequencer.is_duplicate("evt_002") is False

    @pytest.mark.asyncio
    async def test_duplicate_increments_counter(self, sequencer):
        await sequencer.is_duplicate("evt_001")
        await sequencer.is_duplicate("evt_001")
        await sequencer.is_duplicate("evt_001")
        assert sequencer.duplicate_count == 2  # first call is not a duplicate

    @pytest.mark.asyncio
    async def test_seen_ids_tracked_in_set(self, sequencer):
        await sequencer.is_duplicate("x")
        assert "x" in sequencer.seen_ids

    @pytest.mark.asyncio
    async def test_window_eviction(self):
        """IDs older than window_size are eventually evicted."""
        seq = EventSequencer(window_size=5, max_out_of_order=10)
        try:
            # Fill beyond window
            for i in range(10):
                await seq.is_duplicate(f"evt_{i}")

            # Rust tracks seen_ids_count via stats
            stats = await seq.get_sequence_stats()
            assert stats["seen_ids_count"] <= 5
        finally:
            seq._cleanup_task.cancel()


# =========================================================================
# Gap detection and out-of-order handling
# =========================================================================


class TestGapDetection:
    @pytest.mark.asyncio
    async def test_out_of_order_event_buffered(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e3 = {"t": "test", "id": "e3"}

        # Deliver event 1 (expected=1)
        r1 = await sequencer.process_sequenced_event("topic", 1, e1)
        assert r1 == [e1]

        # Skip event 2, deliver event 3 -> should be buffered
        r3 = await sequencer.process_sequenced_event("topic", 3, e3)
        assert r3 is None  # buffered, not delivered
        assert sequencer.out_of_order_count == 1

    @pytest.mark.asyncio
    async def test_gap_filled_delivers_buffered(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e2 = {"t": "test", "id": "e2"}
        e3 = {"t": "test", "id": "e3"}

        await sequencer.process_sequenced_event("topic", 1, e1)

        # Event 3 arrives first (out of order)
        r3 = await sequencer.process_sequenced_event("topic", 3, e3)
        assert r3 is None

        # Event 2 fills the gap -> delivers both e2 and buffered e3
        r2 = await sequencer.process_sequenced_event("topic", 2, e2)
        assert r2 is not None
        assert len(r2) == 2
        assert r2[0] == e2
        assert r2[1] == e3

    @pytest.mark.asyncio
    async def test_old_event_dropped(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e2 = {"t": "test", "id": "e2"}

        await sequencer.process_sequenced_event("topic", 1, e1)
        await sequencer.process_sequenced_event("topic", 2, e2)

        # Re-deliver event 1 (old) -> should be dropped
        old = {"t": "test", "id": "e1_old"}
        result = await sequencer.process_sequenced_event("topic", 1, old)
        assert result is None
        assert sequencer.dropped_count == 1

    @pytest.mark.asyncio
    async def test_too_far_ahead_resets_sequence(self, sequencer):
        """When a sequence is too far ahead (> max_out_of_order), the
        sequencer resets to the new sequence and drops intermediate events."""
        e1 = {"t": "test", "id": "e1"}
        await sequencer.process_sequenced_event("topic", 1, e1)

        # Jump far ahead (max_out_of_order=10, so 50 is way beyond)
        e50 = {"t": "test", "id": "e50"}
        result = await sequencer.process_sequenced_event("topic", 50, e50)

        # Should deliver immediately after reset
        assert result is not None
        assert result == [e50]
        # Intermediate events (2-49) are counted as dropped
        assert sequencer.dropped_count > 0

    @pytest.mark.asyncio
    async def test_multiple_buffered_events_flush_in_order(self, sequencer):
        """When a gap is filled, all consecutive buffered events are flushed."""
        e1 = {"t": "test", "id": "e1"}
        e2 = {"t": "test", "id": "e2"}
        e3 = {"t": "test", "id": "e3"}
        e4 = {"t": "test", "id": "e4"}
        e5 = {"t": "test", "id": "e5"}

        await sequencer.process_sequenced_event("topic", 1, e1)

        # Buffer 3, 4, 5 (all out of order)
        assert await sequencer.process_sequenced_event("topic", 3, e3) is None
        assert await sequencer.process_sequenced_event("topic", 4, e4) is None
        assert await sequencer.process_sequenced_event("topic", 5, e5) is None

        # Fill gap with event 2 -> flush 2, 3, 4, 5
        result = await sequencer.process_sequenced_event("topic", 2, e2)
        assert result is not None
        assert len(result) == 4
        assert result[0] == e2
        assert result[1] == e3
        assert result[2] == e4
        assert result[3] == e5


# =========================================================================
# Reset and stats
# =========================================================================


class TestResetAndStats:
    @pytest.mark.asyncio
    async def test_reset_specific_topic(self, sequencer):
        e1 = {"t": "test"}
        await sequencer.process_sequenced_event("topic_a", 1, e1)
        await sequencer.process_sequenced_event("topic_b", 1, e1)

        await sequencer.reset_sequence("topic_a")

        assert "topic_a" not in sequencer.expected_sequences
        assert "topic_b" in sequencer.expected_sequences

    @pytest.mark.asyncio
    async def test_reset_all_topics(self, sequencer):
        e1 = {"t": "test"}
        await sequencer.process_sequenced_event("topic_a", 1, e1)
        await sequencer.process_sequenced_event("topic_b", 1, e1)

        await sequencer.reset_sequence()

        assert len(sequencer.expected_sequences) == 0
        assert len(sequencer.buffered_events) == 0

    @pytest.mark.asyncio
    async def test_reset_nonexistent_topic(self, sequencer):
        """Reset on a topic that was never used should not raise."""
        await sequencer.reset_sequence("nonexistent")

    @pytest.mark.asyncio
    async def test_get_sequence_stats(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        await sequencer.process_sequenced_event("topic", 1, e1)

        # Create a duplicate to increment counter
        await sequencer.is_duplicate("dup")
        await sequencer.is_duplicate("dup")

        stats = await sequencer.get_sequence_stats()
        assert "current_sequence" in stats
        assert "duplicate_count" in stats
        assert stats["duplicate_count"] == 1
        assert "out_of_order_count" in stats
        assert "dropped_count" in stats
        assert "topics" in stats
        assert "topic" in stats["topics"]
        assert "total_topics" in stats
        assert stats["total_topics"] == 1
        assert "seen_ids_count" in stats

    @pytest.mark.asyncio
    async def test_get_sequence_stats_with_gaps(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e5 = {"t": "test", "id": "e5"}

        await sequencer.process_sequenced_event("topic", 1, e1)
        # Buffer event 5 (creates gap of 2,3,4)
        await sequencer.process_sequenced_event("topic", 5, e5)

        stats = await sequencer.get_sequence_stats()
        topic_stats = stats["topics"]["topic"]
        assert topic_stats["buffered"] == 1
        assert len(topic_stats["gaps"]) > 0
        assert topic_stats["buffered_sequences"] == [5]

    @pytest.mark.asyncio
    async def test_get_buffer_stats(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        e3 = {"t": "test", "id": "e3"}

        await sequencer.process_sequenced_event("topic", 1, e1)
        await sequencer.process_sequenced_event("topic", 3, e3)

        stats = await sequencer.get_buffer_stats()
        assert "total_topics" in stats
        assert "total_buffered" in stats
        assert stats["total_buffered"] == 1
        assert "topic" in stats["topics"]
        assert stats["topics"]["topic"]["buffered_count"] == 1

    @pytest.mark.asyncio
    async def test_buffer_stats_empty_when_no_gaps(self, sequencer):
        e1 = {"t": "test", "id": "e1"}
        await sequencer.process_sequenced_event("topic", 1, e1)

        stats = await sequencer.get_buffer_stats()
        # topic exists but has no buffered events, so not in topics dict
        assert stats["total_buffered"] == 0


# =========================================================================
# Cleanup
# =========================================================================


class TestCleanup:
    @pytest.mark.asyncio
    async def test_manual_cleanup(self, sequencer):
        """Manual cleanup should not raise."""
        await sequencer.cleanup()

    @pytest.mark.asyncio
    async def test_shutdown(self, sequencer):
        """Shutdown cancels cleanup task."""
        await sequencer.shutdown()
        assert sequencer._cleanup_task.cancelled()

    @pytest.mark.asyncio
    async def test_cleanup_does_not_crash(self):
        """Cleanup can be called safely even with buffered events."""
        seq = EventSequencer(window_size=100, max_out_of_order=50)
        try:
            e1 = {"t": "test", "id": "e1"}
            await seq.process_sequenced_event("topic", 1, e1)
            # Buffer an out-of-order event
            await seq.process_sequenced_event("topic", 5, {"t": "future"})

            # Cleanup should not crash
            await seq.cleanup()

            # Stats still accessible after cleanup
            stats = await seq.get_buffer_stats()
            assert isinstance(stats, dict)
        finally:
            seq._cleanup_task.cancel()
