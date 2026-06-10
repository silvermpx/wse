"""Tests for event sequencer."""

from wse_client.event_sequencer import EventSequencer


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


class TestReset:
    def test_reset_clears_state(self):
        seq = EventSequencer()
        seq.is_duplicate("x")
        seq.check_topic_stamp("room", "0000000a", 5)
        seq.reset()
        stats = seq.get_stats()
        assert stats["current_sequence"] == 0
        assert stats["duplicate_window_size"] == 0
        assert stats["topic_positions"] == 0
        assert seq.topic_position("room") is None


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
