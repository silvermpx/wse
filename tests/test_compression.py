"""Tests for CompressionManager."""

import json
import os
import sys
import zlib

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import UTC

from wse_server.connection.compression import CompressionManager

# =========================================================================
# Compress / Decompress roundtrip
# =========================================================================


class TestCompressDecompress:
    def test_roundtrip_basic(self, compression_manager):
        data = b"Hello, World! " * 200  # large enough to be worth compressing
        compressed = compression_manager.compress(data)
        decompressed = compression_manager.decompress(compressed)
        assert decompressed == data

    def test_roundtrip_json_payload(self, compression_manager):
        payload = json.dumps(
            {
                "t": "order_update",
                "p": {"symbol": "AAPL", "price": 150.25, "qty": 100},
                "ts": "2025-01-01T00:00:00Z",
            }
        ).encode("utf-8")
        # Pad to exceed threshold
        padded = payload + b" " * 2000
        compressed = compression_manager.compress(padded)
        decompressed = compression_manager.decompress(compressed)
        assert decompressed == padded

    def test_compress_actually_shrinks_repetitive_data(self, compression_manager):
        data = b"AAAA" * 1000  # highly repetitive
        compressed = compression_manager.compress(data)
        assert len(compressed) < len(data)

    def test_decompress_invalid_data(self, compression_manager):
        with pytest.raises((zlib.error, RuntimeError)):
            compression_manager.decompress(b"this is not zlib data")

    def test_compress_empty_data(self, compression_manager):
        compressed = compression_manager.compress(b"")
        decompressed = compression_manager.decompress(compressed)
        assert decompressed == b""


# =========================================================================
# Async compress
# =========================================================================


class TestAsyncCompress:
    @pytest.mark.asyncio
    async def test_compress_async_roundtrip(self, compression_manager):
        data = b"Async test data " * 200
        compressed = await compression_manager.compress_async(data)
        decompressed = compression_manager.decompress(compressed)
        assert decompressed == data

    @pytest.mark.asyncio
    async def test_compress_async_updates_stats(self, compression_manager):
        data = b"x" * 2000
        await compression_manager.compress_async(data)
        assert compression_manager.stats["total_compressed"] == 1


# =========================================================================
# Threshold behavior
# =========================================================================


class TestThreshold:
    def test_small_message_not_compressed(self, compression_manager):
        small_data = b"tiny"
        assert compression_manager.should_compress(small_data) is False

    def test_large_message_should_compress(self, compression_manager):
        large_data = b"x" * 2000
        assert compression_manager.should_compress(large_data) is True

    def test_threshold_boundary_below(self, compression_manager):
        data = b"x" * CompressionManager.COMPRESSION_THRESHOLD
        assert compression_manager.should_compress(data) is False

    def test_threshold_boundary_above(self, compression_manager):
        data = b"x" * (CompressionManager.COMPRESSION_THRESHOLD + 1)
        assert compression_manager.should_compress(data) is True

    def test_threshold_value(self):
        assert CompressionManager.COMPRESSION_THRESHOLD == 1024


# =========================================================================
# Stats tracking
# =========================================================================


class TestStats:
    def test_initial_stats_zeroed(self, compression_manager):
        stats = compression_manager.get_stats()
        assert stats["total_compressed"] == 0
        assert stats["total_decompressed"] == 0
        assert stats["compression_failures"] == 0
        assert stats["decompression_failures"] == 0
        assert stats["total_bytes_saved"] == 0

    def test_compress_increments_counter(self, compression_manager):
        data = b"x" * 2000
        compression_manager.compress(data)
        assert compression_manager.stats["total_compressed"] == 1

    def test_decompress_increments_counter(self, compression_manager):
        data = b"x" * 2000
        compressed = compression_manager.compress(data)
        compression_manager.decompress(compressed)
        assert compression_manager.stats["total_decompressed"] == 1

    def test_decompression_failure_increments_counter(self, compression_manager):
        with pytest.raises((zlib.error, RuntimeError)):
            compression_manager.decompress(b"invalid")
        assert compression_manager.stats["decompression_failures"] == 1

    def test_bytes_saved_tracked(self, compression_manager):
        data = b"A" * 5000  # very compressible
        compression_manager.compress(data)
        assert compression_manager.stats["total_bytes_saved"] > 0

    def test_get_stats_computed_rates(self, compression_manager):
        data = b"x" * 2000
        compression_manager.compress(data)
        compressed = compression_manager.compress(data)
        compression_manager.decompress(compressed)

        stats = compression_manager.get_stats()
        assert stats["compression_success_rate"] == 1.0
        assert stats["decompression_success_rate"] == 1.0
        assert stats["average_bytes_saved"] > 0

    def test_reset_stats(self, compression_manager):
        data = b"x" * 2000
        compression_manager.compress(data)
        assert compression_manager.stats["total_compressed"] == 1

        compression_manager.reset_stats()
        assert compression_manager.stats["total_compressed"] == 0
        assert compression_manager.stats["total_bytes_saved"] == 0

    def test_success_rate_zero_when_no_ops(self, compression_manager):
        stats = compression_manager.get_stats()
        assert stats["compression_success_rate"] == 0
        assert stats["decompression_success_rate"] == 0


# =========================================================================
# pack_event / unpack_event (JSON fallback)
# =========================================================================


class TestPackUnpack:
    def test_json_pack_unpack_roundtrip(self, compression_manager):
        event = {"t": "test", "p": {"key": "value"}, "v": 2}
        packed = compression_manager.pack_event(event, use_msgpack=False)
        unpacked = compression_manager.unpack_event(packed, is_msgpack=False)
        assert unpacked == event

    def test_pack_event_returns_bytes(self, compression_manager):
        event = {"t": "test"}
        packed = compression_manager.pack_event(event, use_msgpack=False)
        assert isinstance(packed, bytes)

    def test_unpack_invalid_json(self, compression_manager):
        # Rust returns an error dict instead of raising
        result = compression_manager.unpack_event(b"\xff\xfe", is_msgpack=False)
        assert result.get("_decode_error") is True


# =========================================================================
# Serialization helpers
# =========================================================================


class TestSerializeForMsgpack:
    def test_uuid_serialization(self):
        import uuid

        u = uuid.uuid4()
        result = CompressionManager._serialize_for_msgpack(u)
        assert result == str(u)

    def test_datetime_serialization(self):
        from datetime import datetime

        dt = datetime(2025, 1, 1, tzinfo=UTC)
        result = CompressionManager._serialize_for_msgpack(dt)
        assert "2025-01-01" in result

    def test_date_serialization(self):
        from datetime import date

        d = date(2025, 6, 15)
        result = CompressionManager._serialize_for_msgpack(d)
        assert result == "2025-06-15"

    def test_enum_serialization(self):
        from enum import Enum

        class Color(Enum):
            RED = "red"

        result = CompressionManager._serialize_for_msgpack(Color.RED)
        assert result == "red"

    def test_object_serialization(self):
        class Obj:
            def __init__(self):
                self.x = 1

        result = CompressionManager._serialize_for_msgpack(Obj())
        assert result == {"x": 1}

    def test_fallback_to_str(self):
        result = CompressionManager._serialize_for_msgpack(42)
        assert result == "42"
