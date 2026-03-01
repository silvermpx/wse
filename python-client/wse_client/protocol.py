# =============================================================================
# WSE Python Client -- Wire Protocol Codec
# =============================================================================
#
# Handles all message encoding/decoding per docs/PROTOCOL.md:
#
# Incoming (server -> client):
#   Text:   JSON with "c" field for category, or legacy prefix (WSE{, S{, U{)
#   Binary: C: (zlib), M: (msgpack), E: (encrypted), raw zlib (0x78), plain JSON
#
# Outgoing (client -> server):
#   JSON with "c" field for category
# =============================================================================

from __future__ import annotations

import json

from uuid import uuid4
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ._logging import logger
from .constants import (
    MAX_MESSAGE_SIZE,
    PREFIX_COMPRESSED,
    PREFIX_ENCRYPTED,
    PREFIX_MSGPACK,
    PREFIX_SNAPSHOT,
    PREFIX_SYSTEM,
    PREFIX_UPDATE,
    PROTOCOL_VERSION,
    ZLIB_MAGIC,
    ZLIB_METHODS,
)
from .types import MessageCategory, WSEEvent

if TYPE_CHECKING:
    from .compression import CompressionHandler
    from .msgpack_handler import MsgPackHandler
    from .security import SecurityManager

# System message types -> WSE prefix
_SYSTEM_TYPES = frozenset(
    {
        "client_hello",
        "subscription_update",
        "PING",
        "PONG",
        "health_check_response",
        "sync_response",
        "metrics_response",
        "debug_request",
        "config_request",
        "config_update_request",
        "encryption_request",
        "key_rotation_request",
        "batch",
    }
)

# Sync/snapshot message types -> S prefix
_SYNC_TYPES = frozenset(
    {
        "sync_request",
        "request_snapshot",
        "snapshot_request",
    }
)

try:
    import orjson

    def _json_loads(data: str | bytes) -> Any:
        return orjson.loads(data)

    def _json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode()

except ImportError:

    def _json_loads(data: str | bytes) -> Any:
        return json.loads(data)

    def _json_dumps(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))


class MessageCodec:
    """Encode and decode WSE wire protocol messages.

    Handles JSON text frames, zlib-compressed frames, msgpack binary,
    and AES-GCM encrypted frames. See ``docs/PROTOCOL.md`` for the
    wire format specification.

    Args:
        compression: Zlib compression handler.
        security: AES-GCM encryption handler, or ``None`` to disable.
        msgpack: MsgPack handler, or ``None`` to disable binary protocol.
    """

    def __init__(
        self,
        compression: CompressionHandler,
        security: SecurityManager | None = None,
        msgpack: MsgPackHandler | None = None,
    ) -> None:
        self._compression = compression
        self._security = security
        self._msgpack = msgpack
        self._sequence = 0

    def decode(self, data: str | bytes) -> WSEEvent | list[WSEEvent] | None:
        """Decode an incoming message from any wire format."""
        if isinstance(data, str):
            return self._decode_text(data)
        return self._decode_binary(data)

    def encode(
        self,
        type: str,
        payload: dict[str, Any],
        *,
        priority: int | None = None,
        correlation_id: str | None = None,
        category: str | None = None,
    ) -> str:
        """Encode an outgoing message as JSON with category field.

        Category is auto-detected from message type if not specified:
        - WSE for system messages (client_hello, subscription_update, etc.)
        - S for sync/snapshot messages (sync_request, etc.)
        - U for all other user messages
        """
        if category is None:
            if type in _SYSTEM_TYPES:
                category = PREFIX_SYSTEM
            elif type in _SYNC_TYPES:
                category = PREFIX_SNAPSHOT
            else:
                category = PREFIX_UPDATE
        self._sequence += 1
        message: dict[str, Any] = {
            "c": category,
            "t": type,
            "p": payload,
            "id": str(uuid4()),
            "seq": self._sequence,
            "ts": datetime.now(UTC).isoformat(),
            "v": PROTOCOL_VERSION,
        }
        if priority is not None:
            message["pri"] = priority
        if correlation_id is not None:
            message["cid"] = correlation_id

        # HMAC signing (when enabled)
        if self._security is not None and self._security.signing_enabled:
            json_str = _json_dumps(message)
            message["sig"] = self._security.sign(json_str)

        return _json_dumps(message)

    # -- Text decoding ---------------------------------------------------------

    def _decode_text(self, data: str) -> WSEEvent | None:
        if len(data) > MAX_MESSAGE_SIZE:
            logger.warning("Message exceeds max size (%d bytes), dropping", len(data))
            return None

        category, json_str = self._strip_prefix(data)
        try:
            parsed = _json_loads(json_str)
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Failed to parse JSON: %s", e)
            return None

        return self._parsed_to_event(parsed, category)

    def _strip_prefix(self, data: str) -> tuple[MessageCategory | None, str]:
        if data.startswith("WSE{"):
            return MessageCategory.SYSTEM, data[3:]
        if data.startswith("S{"):
            return MessageCategory.SNAPSHOT, data[1:]
        if data.startswith("U{"):
            return MessageCategory.UPDATE, data[1:]
        return None, data

    # -- Binary decoding -------------------------------------------------------

    def _decode_binary(self, data: bytes) -> WSEEvent | list[WSEEvent] | None:
        if len(data) > MAX_MESSAGE_SIZE:
            logger.warning("Binary message exceeds max size (%d bytes)", len(data))
            return None

        # Compressed: C: prefix
        if data[:2] == PREFIX_COMPRESSED:
            try:
                decompressed = self._compression.decompress(data[2:])
            except Exception:
                logger.warning(
                    "Corrupt compressed frame (%d bytes), dropping", len(data)
                )
                return None
            if len(decompressed) > MAX_MESSAGE_SIZE:
                logger.warning(
                    "Decompressed message exceeds max size (%d bytes), dropping",
                    len(decompressed),
                )
                return None
            text = decompressed.decode("utf-8")
            return self._decode_text(text)

        # MessagePack: M: prefix
        if data[:2] == PREFIX_MSGPACK:
            if self._msgpack is None:
                logger.warning("Received msgpack frame but msgpack not available")
                return None
            parsed = self._msgpack.unpack(data[2:])
            return self._parsed_to_event(parsed, None)

        # Encrypted: E: prefix
        if data[:2] == PREFIX_ENCRYPTED:
            if self._security is None or not self._security.is_enabled:
                logger.warning("Received encrypted frame but encryption not configured")
                return None
            try:
                plaintext = self._security.decrypt(data[2:])
            except Exception:
                logger.warning(
                    "Decryption failed for E: frame (%d bytes), dropping", len(data)
                )
                return None
            return self._decode_text(plaintext)

        # Raw zlib (magic byte 0x78)
        if len(data) >= 2 and data[0] == ZLIB_MAGIC and data[1] in ZLIB_METHODS:
            try:
                decompressed = self._compression.decompress(data)
            except Exception:
                logger.warning("Corrupt zlib frame (%d bytes), dropping", len(data))
                return None
            if len(decompressed) > MAX_MESSAGE_SIZE:
                logger.warning(
                    "Decompressed message exceeds max size (%d bytes), dropping",
                    len(decompressed),
                )
                return None
            text = decompressed.decode("utf-8")
            return self._decode_text(text)

        # Plain JSON in binary frame (server default since v1.1)
        try:
            text = data.decode("utf-8")
            return self._decode_text(text)
        except UnicodeDecodeError:
            pass

        # Raw msgpack (no M: prefix)
        if self._msgpack is not None:
            try:
                parsed = self._msgpack.unpack(data)
                return self._parsed_to_event(parsed, None)
            except Exception:
                pass

        logger.debug("Unable to decode binary frame (%d bytes)", len(data))
        return None

    # -- Helpers ---------------------------------------------------------------

    def _parsed_to_event(
        self,
        parsed: dict[str, Any],
        category: MessageCategory | None,
    ) -> WSEEvent | list[WSEEvent] | None:
        if not isinstance(parsed, dict):
            return None

        # Normalize keys: "type" -> "t", "payload" -> "p"
        t = parsed.get("t") or parsed.get("type", "unknown")
        p = parsed.get("p") or parsed.get("payload") or {}

        # Handle batch messages
        if t == "batch" and isinstance(p, dict) and "messages" in p:
            events = []
            for msg in p["messages"]:
                evt = self._parsed_to_event(msg, category)
                if isinstance(evt, WSEEvent):
                    events.append(evt)
            return events if events else None

        # Category: prefer wire prefix, fall back to "c" field in JSON
        cat = category.value if category else parsed.get("c")

        return WSEEvent(
            type=t,
            payload=p if isinstance(p, dict) else {"data": p},
            id=parsed.get("id"),
            sequence=parsed.get("seq"),
            timestamp=parsed.get("ts"),
            version=parsed.get("v", PROTOCOL_VERSION),
            category=cat,
            priority=parsed.get("pri"),
            correlation_id=parsed.get("cid"),
            signature=parsed.get("sig"),
        )
