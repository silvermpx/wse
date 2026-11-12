# =============================================================================
# WSE Python Client -- Compression Handler
# =============================================================================

from __future__ import annotations

import zlib

from .constants import COMPRESSION_LEVEL, COMPRESSION_THRESHOLD


class DecompressedSizeError(ValueError):
    """A compressed frame inflates past the allowed size (decompression bomb)."""


class CompressionHandler:
    """Zlib compression/decompression, wire-compatible with the server's Rust flate2.

    The client uses it to inflate compressed frames from the server (``C:``
    prefix or raw zlib). It does not compress outbound messages: the server
    reads client frames as plain JSON.

    Args:
        threshold: Minimum payload size in bytes for :meth:`should_compress`
            (default 1024).
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

    def decompress(self, data: bytes, max_size: int | None = None) -> bytes:
        """Inflate zlib (or raw deflate) data.

        With *max_size*, inflation stops as soon as the output would pass
        *max_size* bytes and :class:`DecompressedSizeError` is raised, so a
        small frame that inflates to gigabytes never gets allocated.

        Raises:
            zlib.error: The data is not a complete zlib/deflate stream.
            DecompressedSizeError: The output would exceed *max_size*.
        """
        try:
            return _inflate(data, zlib.MAX_WBITS, max_size)
        except zlib.error:
            # Try raw deflate (no zlib header)
            return _inflate(data, -zlib.MAX_WBITS, max_size)


def _inflate(data: bytes, wbits: int, max_size: int | None) -> bytes:
    inflater = zlib.decompressobj(wbits)
    if max_size is None:
        out = inflater.decompress(data)
    else:
        # Ask for one byte more than allowed: getting it (or leftover input in
        # unconsumed_tail) means the stream inflates past the limit.
        out = inflater.decompress(data, max_size + 1)
        if len(out) > max_size or inflater.unconsumed_tail:
            raise DecompressedSizeError(
                f"compressed frame inflates past {max_size} bytes"
            )
    out += inflater.flush()
    if max_size is not None and len(out) > max_size:
        raise DecompressedSizeError(f"compressed frame inflates past {max_size} bytes")
    if not inflater.eof:
        raise zlib.error("incomplete or truncated stream")
    return out
