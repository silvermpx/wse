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


class TestBoundedDecompression:
    """Frames were fully inflated before the size check (decompression
    bomb: a 64 KiB frame inflating to gigabytes)."""

    def test_bomb_is_rejected_without_inflating_it(self):
        import pytest

        from wse_client.compression import DecompressedSizeError

        bomb = zlib.compress(b"\0" * (256 * 1024 * 1024), 9)  # ~256 KiB
        assert len(bomb) < 1024 * 1024
        handler = CompressionHandler()
        with pytest.raises(DecompressedSizeError):
            handler.decompress(bomb, max_size=1024 * 1024)

    def test_output_at_the_limit_is_accepted(self):
        handler = CompressionHandler()
        data = b"a" * 1000
        assert handler.decompress(zlib.compress(data), max_size=1000) == data

    def test_one_byte_over_the_limit_is_rejected(self):
        import pytest

        from wse_client.compression import DecompressedSizeError

        handler = CompressionHandler()
        with pytest.raises(DecompressedSizeError):
            handler.decompress(zlib.compress(b"a" * 1001), max_size=1000)

    def test_raw_deflate_is_bounded_too(self):
        import pytest

        from wse_client.compression import DecompressedSizeError

        obj = zlib.compressobj(9, zlib.DEFLATED, -zlib.MAX_WBITS)
        raw = obj.compress(b"\0" * (8 * 1024 * 1024)) + obj.flush()
        with pytest.raises(DecompressedSizeError):
            CompressionHandler().decompress(raw, max_size=1024)

    def test_truncated_stream_is_an_error(self):
        import pytest

        data = zlib.compress(b"hello world" * 100)
        with pytest.raises(zlib.error):
            CompressionHandler().decompress(data[:-6])
