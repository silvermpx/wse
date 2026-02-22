"""Tests for event sequencer."""

from wse_client.event_sequencer import EventSequencer
from wse_client.types import WSEEvent


def _evt(type: str = "test", seq: int | None = None, id: str | None = None) -> WSEEvent:
    return WSEEvent(type=type, payload={}, id=id, sequence=seq)


class TestDuplicateDetection:
    def test_new_event_not_duplicate(self):
        seq = EventSequencer()
        assert seq.is_duplicate("evt-1") is False

    def test_same_id_is_duplicate(self):
        seq = EventSequencer()
        seq.is_duplicate("evt-1")
        assert seq.is_duplicate("evt-1") is True

    def test_different_ids_not_duplicate(self):
        seq = EventSequencer()
        seq.is_duplicate("evt-1")
        assert seq.is_duplicate("evt-2") is False

    def test_window_eviction(self):
        seq = EventSequencer(window_size=3)
        seq.is_duplicate("a")
        seq.is_duplicate("b")
        seq.is_duplicate("c")
        seq.is_duplicate("d")  # "a" should be evicted
        # "a" was evicted, so it looks new
        assert seq.is_duplicate("a") is False

    def test_stats_track_duplicates(self):
        seq = EventSequencer()
        seq.is_duplicate("x")
        seq.is_duplicate("x")
        seq.is_duplicate("x")
        stats = seq.get_stats()
        assert stats["duplicates_detected"] == 2


class TestSequenceOrdering:
    def test_in_order_delivery(self):
        seq = EventSequencer()
        e1 = _evt(seq=0)
        result = seq.process_sequenced_event(0, e1)
        assert result is not None
        assert len(result) == 1
        assert result[0] is e1

    def test_out_of_order_buffered(self):
        seq = EventSequencer()
        e2 = _evt("second", seq=1)
        # Seq 1 arrives before seq 0
        result = seq.process_sequenced_event(1, e2)
        assert result is None  # Buffered

    def test_buffered_events_delivered_in_order(self):
        seq = EventSequencer()
        e1 = _evt("second", seq=1)
        e0 = _evt("first", seq=0)

        seq.process_sequenced_event(1, e1)  # Buffered
        result = seq.process_sequenced_event(0, e0)

        assert result is not None
        assert len(result) == 2
        assert result[0].type == "first"
        assert result[1].type == "second"

    def test_large_gap_resets(self):
        seq = EventSequencer(max_out_of_order=5)
        e100 = _evt(seq=100)
        result = seq.process_sequenced_event(100, e100)
        # Gap > 5, so it resets and delivers
        assert result is not None
        assert len(result) == 1

    def test_old_sequence_skipped(self):
        seq = EventSequencer()
        seq.process_sequenced_event(0, _evt(seq=0))
        seq.process_sequenced_event(1, _evt(seq=1))
        # Seq 0 again (already delivered)
        result = seq.process_sequenced_event(0, _evt(seq=0))
        assert result is None

    def test_reset(self):
        seq = EventSequencer()
        seq.is_duplicate("x")
        seq.process_sequenced_event(0, _evt(seq=0))
        seq.reset()
        stats = seq.get_stats()
        assert stats["current_sequence"] == 0
        assert stats["expected_sequence"] == 0
        assert stats["duplicate_window_size"] == 0
