"""Tests for compression handler."""

import zlib

from wse_client.compression import CompressionHandler


class TestCompressionHandler:
    def test_roundtrip(self):
        handler = CompressionHandler()
        data = b"Hello, WSE!" * 100
        compressed = handler.compress(data)
        decompressed = handler.decompress(compressed)
        assert decompressed == data

    def test_should_compress_above_threshold(self):
        handler = CompressionHandler(threshold=100)
        assert handler.should_compress(b"x" * 101) is True
        assert handler.should_compress(b"x" * 50) is False

    def test_decompress_raw_deflate(self):
        handler = CompressionHandler()
        data = b"test data for raw deflate"
        # Compress with raw deflate (no zlib header)
        compress_obj = zlib.compressobj(6, zlib.DEFLATED, -zlib.MAX_WBITS)
        raw = compress_obj.compress(data) + compress_obj.flush()
        decompressed = handler.decompress(raw)
        assert decompressed == data
