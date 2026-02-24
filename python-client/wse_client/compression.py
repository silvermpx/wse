# =============================================================================
# WSE Python Client -- Compression Handler
# =============================================================================

from __future__ import annotations

import zlib

from .constants import COMPRESSION_LEVEL, COMPRESSION_THRESHOLD


class CompressionHandler:
    """Zlib compression/decompression, wire-compatible with the server's Rust flate2.

    Args:
        threshold: Minimum payload size in bytes to compress (default 1024).
        level: Zlib compression level 1--9 (default 6).
    """

    def __init__(
        self,
        threshold: int = COMPRESSION_THRESHOLD,
        level: int = COMPRESSION_LEVEL,
    ) -> None:
        self.threshold = threshold
        self.level = level

    def should_compress(self, data: bytes) -> bool:
        return len(data) > self.threshold

    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data, self.level)

    def decompress(self, data: bytes) -> bytes:
        try:
            return zlib.decompress(data)
        except zlib.error:
            # Try raw deflate (no zlib header)
            return zlib.decompress(data, -zlib.MAX_WBITS)
