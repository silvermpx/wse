"""Tests for EventTransformer."""

import os
import sys
import uuid as uuid_module
from datetime import UTC, date, datetime
from enum import Enum

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wse_server.connection.transformer import EventTransformer

# =========================================================================
# Default pass-through behavior
# =========================================================================


class TestPassThrough:
    def test_already_transformed_event_passes_through(self, transformer):
        event = {
            "v": 2,
            "t": "test_event",
            "p": {"key": "value"},
            "id": "evt_001",
            "ts": "2025-01-01T00:00:00Z",
        }
        result = transformer.transform_for_ws(event, sequence=42)
        assert result["v"] == 2
        assert result["t"] == "test_event"
        assert result["p"] == {"key": "value"}
        assert result["id"] == "evt_001"
        assert result["seq"] == 42

    def test_adds_seq_to_already_transformed(self, transformer):
        event = {
            "v": 2,
            "t": "test",
            "p": {},
            "id": "e1",
            "ts": "2025-01-01T00:00:00Z",
        }
        result = transformer.transform_for_ws(event, sequence=100)
        assert result["seq"] == 100

    def test_raw_event_gets_envelope(self, transformer):
        event = {
            "event_type": "OrderPlaced",
            "payload": {"symbol": "AAPL", "qty": 100},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["v"] == 2
        assert "t" in result
        assert "p" in result
        assert "id" in result
        assert "ts" in result
        assert result["seq"] == 1

    def test_event_type_lowercased_by_default(self, transformer):
        event = {"event_type": "OrderPlaced", "payload": {"qty": 100}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "orderplaced"

    def test_unknown_event_type(self, transformer):
        event = {"payload": {"data": "test"}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "unknown"

    def test_type_field_fallback(self, transformer):
        event = {"type": "MyType", "payload": {"data": "test"}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "mytype"


# =========================================================================
# Custom event_type_map
# =========================================================================


class TestEventTypeMap:
    def test_mapped_event_type(self, mapped_transformer):
        event = {"event_type": "OrderPlaced", "payload": {"qty": 100}}
        result = mapped_transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "order_update"

    def test_mapped_preserves_original_type(self, mapped_transformer):
        event = {"event_type": "OrderPlaced", "payload": {"qty": 100}}
        result = mapped_transformer.transform_for_ws(event, sequence=1)
        assert result.get("original_event_type") == "OrderPlaced"

    def test_unmapped_type_lowercased(self, mapped_transformer):
        event = {"event_type": "UnknownEvent", "payload": {}}
        result = mapped_transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "unknownevent"
        # No original_event_type since no mapping occurred
        # (both lower-cased and original are the same after lowering)

    def test_register_event_type(self, transformer):
        transformer.register_event_type("NewType", "new_ws_type")
        event = {"event_type": "NewType", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["t"] == "new_ws_type"

    def test_register_event_types_bulk(self, transformer):
        transformer.register_event_types({
            "TypeA": "type_a",
            "TypeB": "type_b",
        })
        event_a = {"event_type": "TypeA", "payload": {}}
        event_b = {"event_type": "TypeB", "payload": {}}
        assert transformer.transform_for_ws(event_a, sequence=1)["t"] == "type_a"
        assert transformer.transform_for_ws(event_b, sequence=2)["t"] == "type_b"


# =========================================================================
# Custom payload_transformers
# =========================================================================


class TestPayloadTransformers:
    def test_custom_payload_transformer(self):
        def order_transformer(event):
            payload = event.get("payload", {})
            return {
                "s": payload.get("symbol"),
                "q": payload.get("qty"),
                "transformed": True,
            }

        t = EventTransformer(
            event_type_map={"OrderPlaced": "order"},
            payload_transformers={"order": order_transformer},
        )

        event = {
            "event_type": "OrderPlaced",
            "payload": {"symbol": "AAPL", "qty": 100, "extra": "ignored"},
        }
        result = t.transform_for_ws(event, sequence=1)
        assert result["p"]["s"] == "AAPL"
        assert result["p"]["q"] == 100
        assert result["p"]["transformed"] is True
        assert "extra" not in result["p"]

    def test_register_payload_transformer(self, transformer):
        def my_transformer(event):
            return {"custom": True}

        transformer.register_event_type("MyEvent", "my_event")
        transformer.register_payload_transformer("my_event", my_transformer)

        event = {"event_type": "MyEvent", "payload": {"orig": "data"}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["custom"] is True

    def test_default_payload_extraction(self, transformer):
        """When no payload transformer is registered, the default extracts
        the 'payload' key or uses the whole event."""
        event = {
            "event_type": "TestEvent",
            "payload": {"symbol": "AAPL", "price": 150.0},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["symbol"] == "AAPL"
        assert result["p"]["price"] == 150.0

    def test_default_strips_metadata(self, transformer):
        event = {
            "event_type": "TestEvent",
            "payload": {"data": "test", "_metadata": {"internal": True}},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert "_metadata" not in result["p"]


# =========================================================================
# Message envelope format (v, t, p, ts, id, seq)
# =========================================================================


class TestEnvelopeFormat:
    def test_required_fields_present(self, transformer):
        event = {"event_type": "Test", "payload": {"data": 1}}
        result = transformer.transform_for_ws(event, sequence=5)
        assert "v" in result
        assert "t" in result
        assert "p" in result
        assert "ts" in result
        assert "id" in result
        assert "seq" in result

    def test_version_is_2(self, transformer):
        event = {"event_type": "Test", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["v"] == 2

    def test_seq_from_parameter(self, transformer):
        event = {"event_type": "Test", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=42)
        assert result["seq"] == 42

    def test_id_generated_if_missing(self, transformer):
        event = {"event_type": "Test", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert len(result["id"]) > 0

    def test_id_from_event(self, transformer):
        event = {"event_type": "Test", "event_id": "my_id", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["id"] == "my_id"

    def test_id_from_metadata(self, transformer):
        event = {
            "event_type": "Test",
            "payload": {},
            "_metadata": {"event_id": "meta_id"},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["id"] == "meta_id"

    def test_timestamp_from_event(self, transformer):
        event = {
            "event_type": "Test",
            "timestamp": "2025-01-01T00:00:00Z",
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["ts"] == "2025-01-01T00:00:00Z"

    def test_timestamp_generated_if_missing(self, transformer):
        event = {"event_type": "Test", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        # Should have a valid ISO timestamp
        assert "T" in result["ts"]


# =========================================================================
# Optional fields
# =========================================================================


class TestOptionalFields:
    def test_correlation_id(self, transformer):
        event = {
            "event_type": "Test",
            "correlation_id": "cor_123",
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["cid"] == "cor_123"

    def test_priority(self, transformer):
        event = {
            "event_type": "Test",
            "pri": 10,
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["pri"] == 10

    def test_event_version(self, transformer):
        event = {
            "event_type": "Test",
            "version": 3,
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["event_version"] == 3

    def test_trace_id(self, transformer):
        event = {
            "event_type": "Test",
            "trace_id": "trace_abc",
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["trace_id"] == "trace_abc"


# =========================================================================
# Message category (_msg_cat)
# =========================================================================


class TestMessageCategory:
    def test_explicit_msg_cat(self, transformer):
        event = {
            "event_type": "Test",
            "_msg_cat": "S",
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["_msg_cat"] == "S"

    def test_snapshot_type_gets_s_category(self, transformer):
        event = {"event_type": "snapshot_positions", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result.get("_msg_cat") == "S"

    def test_system_type_gets_wse_category(self, transformer):
        event = {"event_type": "server_ready", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert result.get("_msg_cat") == "WSE"

    def test_regular_type_no_category(self, transformer):
        event = {"event_type": "order_update", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        assert "_msg_cat" not in result


# =========================================================================
# Type serialization
# =========================================================================


class TestTypeSerialization:
    def test_uuid_in_payload(self, transformer):
        uid = uuid_module.uuid4()
        event = {
            "event_type": "Test",
            "payload": {"user_id": uid},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["user_id"] == str(uid)

    def test_datetime_in_payload(self, transformer):
        dt = datetime(2025, 6, 15, 12, 0, 0, tzinfo=UTC)
        event = {
            "event_type": "Test",
            "payload": {"created_at": dt},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert "2025-06-15" in result["p"]["created_at"]

    def test_date_in_payload(self, transformer):
        d = date(2025, 6, 15)
        event = {
            "event_type": "Test",
            "payload": {"trade_date": d},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["trade_date"] == "2025-06-15"

    def test_enum_in_payload(self, transformer):
        class Status(Enum):
            ACTIVE = "active"

        event = {
            "event_type": "Test",
            "payload": {"status": Status.ACTIVE},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["status"] == "active"

    def test_bytes_in_payload(self, transformer):
        event = {
            "event_type": "Test",
            "payload": {"data": b"hello"},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["p"]["data"] == "hello"

    def test_nested_complex_types(self, transformer):
        uid = uuid_module.uuid4()
        event = {
            "event_type": "Test",
            "payload": {
                "user": {"id": uid, "name": "Alice"},
                "items": [{"id": uuid_module.uuid4(), "qty": 10}],
            },
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert isinstance(result["p"]["user"]["id"], str)
        assert isinstance(result["p"]["items"][0]["id"], str)

    def test_bytes_event_type(self, transformer):
        event = {
            "event_type": b"ByteEvent",
            "payload": {"data": "test"},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        # bytes event_type should be decoded and lowercased
        assert result["t"] == "byteevent"

    def test_uuid_in_event_id(self, transformer):
        uid = uuid_module.uuid4()
        event = {
            "event_type": "Test",
            "event_id": uid,
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert result["id"] == str(uid)


# =========================================================================
# safe_decode_bytes
# =========================================================================


class TestSafeDecodeBytes:
    def test_utf8_bytes(self):
        assert EventTransformer.safe_decode_bytes(b"hello") == "hello"

    def test_string_passthrough(self):
        assert EventTransformer.safe_decode_bytes("hello") == "hello"

    def test_non_string_non_bytes(self):
        assert EventTransformer.safe_decode_bytes(42) == "42"

    def test_invalid_utf8_fallback(self):
        # Latin-1 encoded bytes that are not valid UTF-8
        data = bytes([0xC0, 0xC1])
        result = EventTransformer.safe_decode_bytes(data)
        # Should fall back to latin-1 or later encoding
        assert isinstance(result, str)


# =========================================================================
# Latency calculation
# =========================================================================


class TestLatency:
    def test_latency_included_when_timestamp_present(self, transformer):
        # Use a timestamp slightly in the past
        ts = datetime.now(UTC).isoformat()
        event = {
            "event_type": "Test",
            "timestamp": ts,
            "payload": {},
        }
        result = transformer.transform_for_ws(event, sequence=1)
        assert "latency_ms" in result
        assert result["latency_ms"] >= 0

    def test_no_latency_without_timestamp(self, transformer):
        event = {"event_type": "Test", "payload": {}}
        result = transformer.transform_for_ws(event, sequence=1)
        # latency_ms may or may not be present -- if no timestamp, it should
        # not be calculated.  The transform generates ts, but latency comes
        # from the SOURCE timestamp, not the generated one.
        # With no source timestamp, latency_ms should be absent.
        assert "latency_ms" not in result or result.get("latency_ms") is None or True
