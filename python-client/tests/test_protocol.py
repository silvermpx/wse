"""Tests for wire protocol codec."""

import json

from wse_client.compression import CompressionHandler
from wse_client.protocol import MessageCodec
from wse_client.types import WSEEvent


def _make_codec() -> MessageCodec:
    return MessageCodec(CompressionHandler())


class TestTextDecoding:
    def test_system_prefix(self):
        codec = _make_codec()
        data = 'WSE{"t":"server_ready","p":{"connection_id":"abc"}}'
        event = codec.decode(data)
        assert isinstance(event, WSEEvent)
        assert event.type == "server_ready"
        assert event.payload["connection_id"] == "abc"
        assert event.category == "WSE"

    def test_snapshot_prefix(self):
        codec = _make_codec()
        data = 'S{"t":"market_data","p":{"symbol":"AAPL"}}'
        event = codec.decode(data)
        assert isinstance(event, WSEEvent)
        assert event.type == "market_data"
        assert event.category == "S"

    def test_update_prefix(self):
        codec = _make_codec()
        data = 'U{"t":"price_update","p":{"price":150.5}}'
        event = codec.decode(data)
        assert isinstance(event, WSEEvent)
        assert event.type == "price_update"
        assert event.category == "U"

    def test_plain_json(self):
        codec = _make_codec()
        data = '{"t":"hello","p":{"msg":"world"}}'
        event = codec.decode(data)
        assert isinstance(event, WSEEvent)
        assert event.type == "hello"
        assert event.payload == {"msg": "world"}

    def test_full_envelope(self):
        codec = _make_codec()
        data = json.dumps(
            {
                "t": "test",
                "p": {"key": "value"},
                "id": "evt-123",
                "seq": 42,
                "ts": "2024-01-01T00:00:00Z",
                "v": 1,
                "pri": 8,
                "cid": "corr-1",
            }
        )
        event = codec.decode(data)
        assert event.type == "test"
        assert event.id == "evt-123"
        assert event.sequence == 42
        assert event.priority == 8
        assert event.correlation_id == "corr-1"

    def test_batch_message(self):
        codec = _make_codec()
        data = json.dumps(
            {
                "t": "batch",
                "p": {
                    "messages": [
                        {"t": "a", "p": {"n": 1}},
                        {"t": "b", "p": {"n": 2}},
                    ]
                },
            }
        )
        result = codec.decode(data)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0].type == "a"
        assert result[1].type == "b"

    def test_invalid_json_returns_none(self):
        codec = _make_codec()
        assert codec.decode("not json at all") is None

    def test_oversized_message_dropped(self):
        codec = MessageCodec(CompressionHandler(), max_message_size=1_048_576)
        data = '{"t":"big","p":{}}' + "x" * 2_000_000
        assert codec.decode(data) is None

    def test_long_form_keys(self):
        codec = _make_codec()
        data = '{"type":"test","payload":{"k":"v"}}'
        event = codec.decode(data)
        assert event.type == "test"
        assert event.payload == {"k": "v"}


class TestBinaryDecoding:
    def test_compressed_frame(self):
        import zlib

        codec = _make_codec()
        inner = '{"t":"compressed","p":{"ok":true}}'
        compressed = zlib.compress(inner.encode("utf-8"))
        frame = b"C:" + compressed
        event = codec.decode(frame)
        assert isinstance(event, WSEEvent)
        assert event.type == "compressed"

    def test_raw_zlib_frame(self):
        import zlib

        codec = _make_codec()
        inner = '{"t":"raw_zlib","p":{}}'
        compressed = zlib.compress(inner.encode("utf-8"))
        # Raw zlib starts with 0x78
        assert compressed[0] == 0x78
        event = codec.decode(compressed)
        assert isinstance(event, WSEEvent)
        assert event.type == "raw_zlib"

    def test_plain_json_in_binary(self):
        codec = _make_codec()
        data = b'{"t":"binary_json","p":{"val":42}}'
        event = codec.decode(data)
        assert isinstance(event, WSEEvent)
        assert event.type == "binary_json"


class TestEncoding:
    def test_basic_encode(self):
        codec = _make_codec()
        encoded = codec.encode("test_event", {"key": "value"})
        parsed = json.loads(encoded)
        assert parsed["c"] == "U"
        assert parsed["t"] == "test_event"
        assert parsed["p"] == {"key": "value"}
        assert "id" in parsed
        assert "seq" in parsed
        assert "ts" in parsed
        assert parsed["v"] == 1

    def test_system_message_gets_wse_category(self):
        codec = _make_codec()
        encoded = codec.encode("client_hello", {"version": "1.0"})
        parsed = json.loads(encoded)
        assert parsed["c"] == "WSE"

    def test_sync_message_gets_s_category(self):
        codec = _make_codec()
        encoded = codec.encode("sync_request", {"topics": ["prices"]})
        parsed = json.loads(encoded)
        assert parsed["c"] == "S"

    def test_encode_with_category(self):
        codec = _make_codec()
        encoded = codec.encode("price_update", {"price": 100}, category="U")
        parsed = json.loads(encoded)
        assert parsed["c"] == "U"
        assert parsed["t"] == "price_update"

    def test_sequence_increments(self):
        codec = _make_codec()
        e1 = json.loads(codec.encode("a", {}))
        e2 = json.loads(codec.encode("b", {}))
        assert e2["seq"] == e1["seq"] + 1

    def test_priority_and_correlation(self):
        codec = _make_codec()
        encoded = codec.encode("cmd", {"x": 1}, priority=10, correlation_id="req-1")
        parsed = json.loads(encoded)
        assert parsed["pri"] == 10
        assert parsed["cid"] == "req-1"

    def test_roundtrip_encode_decode(self):
        codec = _make_codec()
        encoded = codec.encode("roundtrip", {"hello": "world"})
        event = codec.decode(encoded)
        assert isinstance(event, WSEEvent)
        assert event.type == "roundtrip"
        assert event.payload == {"hello": "world"}


class TestSizeLimit:
    """The inbound limit is configurable (default 16 MiB) and
    applies to the inflated size of compressed frames."""

    def test_default_accepts_messages_above_1mib(self):
        codec = _make_codec()
        data = json.dumps({"t": "snapshot", "p": {"blob": "x" * 1_500_000}})
        event = codec.decode(data.encode())
        assert isinstance(event, WSEEvent)
        assert event.type == "snapshot"

    def test_compressed_frame_inflating_above_1mib_is_accepted(self):
        import zlib

        codec = _make_codec()
        inner = json.dumps({"t": "big", "p": {"blob": "y" * 3_000_000}}).encode()
        frame = b"C:" + zlib.compress(inner)
        assert len(frame) < 1_048_576
        event = codec.decode(frame)
        assert isinstance(event, WSEEvent) and event.type == "big"

    def test_compressed_bomb_is_dropped(self):
        import zlib

        codec = MessageCodec(CompressionHandler(), max_message_size=1_048_576)
        frame = b"C:" + zlib.compress(b"\0" * (64 * 1024 * 1024), 9)
        assert codec.decode(frame) is None
        raw = zlib.compress(b" " * (64 * 1024 * 1024), 9)
        assert codec.decode(raw) is None

    def test_custom_limit_drops_oversized_frames(self):
        codec = MessageCodec(CompressionHandler(), max_message_size=100)
        assert codec.decode('{"t":"big","p":{"x":"' + "y" * 200 + '"}}') is None
        assert codec.decode(b'{"t":"ok","p":{}}').type == "ok"

    def test_no_limit(self):
        import zlib

        codec = MessageCodec(CompressionHandler(), max_message_size=None)
        inner = json.dumps({"t": "big", "p": {"blob": "z" * 20_000_000}}).encode()
        assert codec.decode(b"C:" + zlib.compress(inner)).type == "big"


class TestMalformedFrames:
    """Decode never raises."""

    def test_garbage_msgpack_frame(self):
        import pytest

        pytest.importorskip("msgpack")
        from wse_client.msgpack_handler import MsgPackHandler

        codec = MessageCodec(CompressionHandler(), msgpack=MsgPackHandler())
        assert codec.decode(b"M:\xc1\xc1\xc1") is None

    def test_batch_with_bad_messages(self):
        codec = _make_codec()
        assert codec.decode('{"t":"batch","p":{"messages":"nope"}}') is not None
        result = codec.decode('{"t":"batch","p":{"messages":[1,{"t":"a","p":{}}]}}')
        assert [e.type for e in result] == ["a"]

    def test_invalid_utf8_after_inflate(self):
        import zlib

        codec = _make_codec()
        assert codec.decode(b"C:" + zlib.compress(b"\xff\xfe\xfd")) is None
