# =============================================================================
# WSE — WebSocket Event System
# Rust-accelerated compression (delegates to RustCompressionManager)
# =============================================================================

import asyncio
import logging
from datetime import date, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from wse_server._accel import RustCompressionManager

log = logging.getLogger("wse.compression")


class CompressionManager:
    """Handle message compression/decompression via Rust acceleration.

    Public API is identical to the original pure-Python implementation.
    All heavy lifting (zlib, msgpack, stats) is done in Rust.
    """

    COMPRESSION_THRESHOLD = 1024  # bytes
    COMPRESSION_LEVEL = 6  # zlib compression level (1-9)

    def __init__(self):
        self._rust = RustCompressionManager(
            threshold=self.COMPRESSION_THRESHOLD,
            compression_level=self.COMPRESSION_LEVEL,
        )

    @property
    def stats(self) -> dict[str, Any]:
        """Direct access to stats dict (for backward compat)."""
        return dict(self._rust.get_stats())

    def should_compress(self, data: bytes) -> bool:
        """Check if data should be compressed"""
        return self._rust.should_compress(data)

    def compress(self, data: bytes) -> bytes:
        """Compress data using zlib with validation"""
        return self._rust.compress(data)

    async def compress_async(self, data: bytes) -> bytes:
        """Compress data using zlib asynchronously (non-blocking)

        PERFORMANCE OPTIMIZATION: Offload CPU-intensive compression to executor
        This prevents blocking the event loop for large events (>10KB)
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._rust.compress, data)

    def decompress(self, data: bytes) -> bytes:
        """Decompress data with better error handling"""
        return self._rust.decompress(data)

    def pack_msgpack(self, data: dict[str, Any]) -> bytes:
        """Pack data using msgpack with custom serializer for UUID/datetime/Enum support"""
        return self._rust.pack_msgpack(data)

    def unpack_msgpack(self, data: bytes) -> dict[str, Any]:
        """Unpack msgpack data"""
        return self._rust.unpack_msgpack(data)

    def pack_event(self, event: dict[str, Any], use_msgpack: bool = True) -> bytes:
        """Pack event to bytes with proper serialization (msgpack with JSON fallback)

        Args:
            event: Event dictionary to pack
            use_msgpack: Use msgpack if True, otherwise use JSON

        Returns:
            Serialized event as bytes
        """
        return self._rust.pack_event(event, use_msgpack)

    def unpack_event(self, data: bytes, is_msgpack: bool = True) -> dict[str, Any]:
        """Unpack event from bytes with robust error handling

        Args:
            data: Serialized event bytes
            is_msgpack: If True, try msgpack first, then JSON fallback

        Returns:
            Deserialized event dictionary
        """
        return self._rust.unpack_event(data, is_msgpack)

    def get_stats(self) -> dict[str, Any]:
        """Get compression statistics"""
        return dict(self._rust.get_stats())

    def reset_stats(self):
        """Reset compression statistics"""
        self._rust.reset_stats()

    @staticmethod
    def _serialize_for_msgpack(obj: Any) -> Any:
        """Serialize Python objects for msgpack compatibility.

        Kept in Python for backward-compat; Rust handles this internally
        during pack_msgpack, but external callers may use this utility.
        """
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return str(obj)
