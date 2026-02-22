# =============================================================================
# WSE Python Client -- MessagePack Handler
# =============================================================================
#
# Optional msgpack encode/decode.  Requires ``msgpack`` package.
# =============================================================================

from __future__ import annotations

from typing import Any


try:
    import msgpack

    _HAS_MSGPACK = True
except ImportError:
    _HAS_MSGPACK = False


class MsgPackHandler:
    """MessagePack encode/decode for binary frames with ``M:`` prefix."""

    def __init__(self) -> None:
        if not _HAS_MSGPACK:
            raise ImportError(
                "msgpack package required: pip install wse-client[msgpack]"
            )

    @staticmethod
    def available() -> bool:
        return _HAS_MSGPACK

    def pack(self, obj: Any) -> bytes:
        """Encode an object to msgpack bytes."""
        return msgpack.packb(obj, use_bin_type=True)

    def unpack(self, data: bytes) -> Any:
        """Decode msgpack bytes to a Python object."""
        return msgpack.unpackb(data, raw=False)
