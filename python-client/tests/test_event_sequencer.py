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


class TestTopicStamp:
    """Per-topic (epoch, offset) idempotent dedup + gap detection."""

    def test_new_topic_delivers_and_baselines(self):
        seq = EventSequencer()
        assert seq.check_topic_stamp("room", "0000000a", 5) == "deliver"
        assert seq.topic_position("room") == ("0000000a", 5)

    def test_in_order_advances(self):
        seq = EventSequencer()
        seq.check_topic_stamp("room", "0000000a", 5)
        assert seq.check_topic_stamp("room", "0000000a", 6) == "deliver"
        assert seq.topic_position("room") == ("0000000a", 6)

    def test_duplicate_dropped(self):
        seq = EventSequencer()
        seq.check_topic_stamp("room", "0000000a", 5)
        seq.check_topic_stamp("room", "0000000a", 6)
        # Re-seeing an already-delivered offset (e.g. replay overlap) is a dup.
        assert seq.check_topic_stamp("room", "0000000a", 6) == "duplicate"
        assert seq.check_topic_stamp("room", "0000000a", 3) == "duplicate"
        assert seq.topic_position("room") == ("0000000a", 6)

    def test_gap_does_not_advance(self):
        seq = EventSequencer()
        seq.check_topic_stamp("room", "0000000a", 5)
        # Jump from 5 to 9 -> gap; position must stay at 5 so recovery from 5
        # replays 6..9 in order without them being deduped.
        assert seq.check_topic_stamp("room", "0000000a", 9) == "gap"
        assert seq.topic_position("room") == ("0000000a", 5)
        # The replayed in-order messages then advance contiguously.
        assert seq.check_topic_stamp("room", "0000000a", 6) == "deliver"

    def test_epoch_change_rebaselines(self):
        seq = EventSequencer()
        seq.check_topic_stamp("room", "0000000a", 100)
        # New epoch (server restart): accept and reset, regardless of offset.
        assert seq.check_topic_stamp("room", "0000000b", 0) == "deliver"
        assert seq.topic_position("room") == ("0000000b", 0)

    def test_positions_survive_full_reset_only(self):
        seq = EventSequencer()
        seq.check_topic_stamp("room", "0000000a", 5)
        seq.reset()
        assert seq.topic_position("room") is None
