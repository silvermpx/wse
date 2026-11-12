"""Regression tests for the Rust utility classes and functions in wse_server."""

import datetime
import decimal
import uuid

import pytest

from wse_server import (
    EventFilter,
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityMessageQueue,
    RustPriorityQueue,
    RustSequencer,
    rust_jwt_decode,
    rust_jwt_encode,
    rust_match_event,
    rust_transform_event,
)

# ---------------------------------------------------------------------------
# Event filters
# ---------------------------------------------------------------------------


class TestFilters:
    def test_or_does_not_hide_sibling_conditions(self):
        """{"account": "A", "$or": [...]} requires both the account and the $or."""
        criteria = {"account": "A", "$or": [{"t": "fill"}, {"t": "cancel"}]}
        assert rust_match_event({"account": "A", "t": "fill"}, criteria)
        assert not rust_match_event({"account": "B", "t": "fill"}, criteria)
        assert not EventFilter.matches({"account": "B", "t": "fill"}, criteria)

    def test_and_and_or_both_apply(self):
        criteria = {"$and": [{"a": 1}], "$or": [{"b": 1}, {"b": 2}]}
        assert rust_match_event({"a": 1, "b": 2}, criteria)
        assert not rust_match_event({"a": 1, "b": 3}, criteria)
        assert not rust_match_event({"a": 2, "b": 1}, criteria)

    def test_tuple_operand_is_accepted(self):
        assert rust_match_event({"a": 1}, {"$or": ({"a": 1},)})
        assert not rust_match_event({"a": 2}, {"$or": ({"a": 1},)})

    def test_malformed_logical_operand_raises(self):
        with pytest.raises(TypeError):
            rust_match_event({"a": 1}, {"$or": {"a": 1}})
        with pytest.raises(TypeError):
            rust_match_event({"a": 1}, {"$and": ["not a dict"]})

    def test_regex_still_anchored_and_cached(self):
        for _ in range(3):
            assert rust_match_event({"s": "AAPL"}, {"s": {"$regex": "AA"}})
            assert not rust_match_event({"s": "XAAPL"}, {"s": {"$regex": "AA"}})


# ---------------------------------------------------------------------------
# Compression and msgpack
# ---------------------------------------------------------------------------


class TestCompression:
    def test_cyclic_payload_raises_instead_of_aborting(self):
        cm = RustCompressionManager()
        d = {}
        d["self"] = d
        with pytest.raises(ValueError, match="cycle"):
            cm.pack_msgpack(d)

    def test_too_deep_payload_raises(self):
        cm = RustCompressionManager()
        deep = {}
        cur = deep
        for _ in range(200):
            cur["x"] = {}
            cur = cur["x"]
        with pytest.raises(ValueError, match="deeper"):
            cm.pack_msgpack(deep)

    def test_shared_but_acyclic_values_are_fine(self):
        cm = RustCompressionManager()
        shared = {"k": 1}
        packed = cm.pack_msgpack({"a": shared, "b": shared})
        assert cm.unpack_msgpack(packed) == {"a": {"k": 1}, "b": {"k": 1}}

    def test_unpack_event_falls_back_to_json(self):
        cm = RustCompressionManager()
        assert cm.unpack_event(b'{"t":"x"}') == {"t": "x"}
        assert cm.unpack_event(b"[1, 2]", is_msgpack=False) == [1, 2]

    def test_unpack_msgpack_rejects_trailing_bytes(self):
        cm = RustCompressionManager()
        packed = cm.pack_msgpack({"a": 1})
        with pytest.raises(RuntimeError):
            cm.unpack_msgpack(packed + b"junk")

    def test_invalid_compression_level_is_a_value_error(self):
        with pytest.raises(ValueError):
            RustCompressionManager(compression_level=10)
        cm = RustCompressionManager()
        with pytest.raises(ValueError):
            cm.compress(b"x" * 2000, level=0)

    def test_large_payload_roundtrip(self):
        cm = RustCompressionManager()
        data = b"wse " * 100_000  # above the detach threshold
        assert cm.decompress(cm.compress(data)) == data


# ---------------------------------------------------------------------------
# Event transformer
# ---------------------------------------------------------------------------


class TestTransformer:
    def test_converts_values_at_any_depth(self):
        uid = uuid.uuid4()
        event = {"event_type": "OrderPlaced", "order": {"customer": {"address": {"id": uid}}}}
        out = rust_transform_event(event, 1, {})
        assert out["p"]["order"]["customer"]["address"]["id"] == str(uid)

    def test_decimal_becomes_exact_string(self):
        out = rust_transform_event({"event_type": "x", "price": decimal.Decimal("10.10")}, 1, {})
        assert out["p"]["price"] == "10.10"

    def test_already_transformed_event_is_not_mutated(self):
        event = {"v": 1, "t": "x", "p": {}, "id": "1", "ts": "2026-01-01T00:00:00Z"}
        out = rust_transform_event(event, 7, {})
        assert out["seq"] == 7
        assert "seq" not in event

    def test_cycle_raises(self):
        payload = {"event_type": "x", "items": []}
        payload["items"].append(payload["items"])
        payload["items"].append(uuid.uuid4())  # forces conversion
        with pytest.raises(ValueError, match="cycle"):
            rust_transform_event(payload, 1, {})


# ---------------------------------------------------------------------------
# Priority queues and sequencers
# ---------------------------------------------------------------------------


class TestQueues:
    def test_low_priority_burst_cannot_evict_normal_work(self):
        q = RustPriorityMessageQueue(max_size=3, batch_size=10)
        for i in range(3):
            assert q.enqueue({"i": i}, 5)
        assert not q.enqueue({"bg": 1}, 1)  # BACKGROUND rejected, NORMAL kept
        stats = q.get_stats()
        assert stats["priority_distribution"][5] == 3
        assert stats["dropped_by_priority"][1] == 1
        assert stats["dropped_by_priority"][5] == 0

    def test_higher_priority_evicts_the_lowest_first(self):
        q = RustPriorityMessageQueue(max_size=2, batch_size=10)
        assert q.enqueue({"n": 1}, 5)
        assert q.enqueue({"bg": 1}, 1)
        assert q.enqueue({"hi": 1}, 8)  # evicts BACKGROUND, not NORMAL
        batch = q.dequeue_batch()
        assert [p for p, _ in batch] == [8, 5]

    def test_critical_never_evicts_high(self):
        q = RustPriorityMessageQueue(max_size=1, batch_size=10)
        assert q.enqueue({"h": 1}, 8)
        assert not q.enqueue({"c": 1}, 10)
        assert q.get_stats()["dropped_by_priority"][10] == 1

    def test_clear_resets_drop_statistics(self):
        q = RustPriorityMessageQueue(max_size=1, batch_size=10)
        q.enqueue({"a": 1}, 5)
        q.enqueue({"b": 1}, 3)
        q.clear()
        assert q.get_stats()["total_dropped"] == 0
        assert q.size == 0

    def test_sizes_are_validated_and_not_preallocated(self):
        with pytest.raises(ValueError):
            RustPriorityQueue(0)
        with pytest.raises(ValueError):
            RustPriorityMessageQueue(max_size=0)
        with pytest.raises(ValueError):
            RustSequencer(0)
        with pytest.raises(ValueError):
            RustEventSequencer(window_size=0)
        # A huge bound must not reserve its capacity up front.
        q = RustPriorityQueue(10**12)
        assert q.push(1, "x")
        assert RustSequencer(10**12).next_seq() == 1


# ---------------------------------------------------------------------------
# JWT claim encoding
# ---------------------------------------------------------------------------

SECRET = b"a-test-secret-that-is-long-enough!!"


class TestJwtEncode:
    def test_datetime_claims_become_timestamps(self):
        exp = datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC)
        token = rust_jwt_encode({"sub": "u", "exp": exp}, SECRET)
        claims = rust_jwt_decode(token, SECRET)
        assert claims["exp"] == int(exp.timestamp())

    def test_tuples_and_sets_become_arrays(self):
        exp = int(datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC).timestamp())
        claims = {"sub": "u", "exp": exp, "wse_topics": {"allow": ("chat.*",)}, "roles": {"a"}}
        decoded = rust_jwt_decode(rust_jwt_encode(claims, SECRET), SECRET)
        assert decoded["wse_topics"] == {"allow": ["chat.*"]}
        assert decoded["roles"] == ["a"]

    def test_unsupported_values_raise(self):
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "obj": object()}, SECRET)
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "big": 2**70}, SECRET)
        with pytest.raises(TypeError):
            rust_jwt_encode({"sub": "u", "x": float("nan")}, SECRET)
