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
from .compression import DecompressedSizeError
from .constants import (
    DEFAULT_MAX_MESSAGE_SIZE,
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
from .signing import ServerKeys, SignatureError
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
        max_message_size: Largest accepted message in bytes, checked on the
            frame and -- for compressed frames -- while inflating. ``None``
            disables the check.
        server_keys: The server signing keys to check signed messages
            against; without any, signed messages are delivered unchecked.
    """

    def __init__(
        self,
        compression: CompressionHandler,
        security: SecurityManager | None = None,
        msgpack: MsgPackHandler | None = None,
        *,
        max_message_size: int | None = DEFAULT_MAX_MESSAGE_SIZE,
        server_keys: ServerKeys | None = None,
    ) -> None:
        self._compression = compression
        self._security = security
        self._msgpack = msgpack
        self._max_size = max_message_size
        self._server_keys = server_keys or ServerKeys(None)
        self._sequence = 0

    def decode(self, data: str | bytes) -> WSEEvent | list[WSEEvent] | None:
        """Decode an incoming message from any wire format.

        Never raises: a frame that cannot be decoded is logged and dropped
        (``None``), so one malformed frame never tears down the connection.
        """
        try:
            if isinstance(data, str):
                return self._decode_text(data)
            return self._decode_binary(data)
        except Exception as exc:
            logger.warning("Dropping undecodable frame: %s", exc)
            return None

    def _too_big(self, size: int) -> bool:
        return self._max_size is not None and size > self._max_size

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

        return _json_dumps(message)

    # -- Text decoding ---------------------------------------------------------

    def _decode_text(self, data: str) -> WSEEvent | list[WSEEvent] | None:
        if self._too_big(len(data)):
            logger.warning("Message exceeds max size (%d chars), dropping", len(data))
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

    def _inflate(self, data: bytes, frame_len: int) -> bytes | None:
        """Inflate a compressed payload, bounded by the size limit."""
        try:
            return self._compression.decompress(data, self._max_size)
        except DecompressedSizeError:
            logger.warning(
                "Compressed frame (%d bytes) inflates past max size %s, dropping",
                frame_len,
                self._max_size,
            )
        except Exception:
            logger.warning("Corrupt compressed frame (%d bytes), dropping", frame_len)
        return None

    def _decode_binary(
        self, data: bytes, *, decrypted: bool = False
    ) -> WSEEvent | list[WSEEvent] | None:
        if self._too_big(len(data)):
            logger.warning("Binary message exceeds max size (%d bytes)", len(data))
            return None

        # Compressed: C: prefix
        if data[:2] == PREFIX_COMPRESSED:
            decompressed = self._inflate(data[2:], len(data))
            if decompressed is None:
                return None
            return self._decode_text(decompressed.decode("utf-8"))

        # MessagePack: M: prefix
        if data[:2] == PREFIX_MSGPACK:
            if self._msgpack is None:
                logger.warning("Received msgpack frame but msgpack not available")
                return None
            parsed = self._msgpack.unpack(data[2:])
            return self._parsed_to_event(parsed, None)

        # Encrypted: E: prefix (never nested inside another encrypted frame)
        if data[:2] == PREFIX_ENCRYPTED and not decrypted:
            if self._security is None or not self._security.is_enabled:
                logger.warning("Received encrypted frame but encryption not configured")
                return None
            try:
                plaintext = self._security.decrypt_bytes(data[2:])
            except Exception:
                logger.warning(
                    "Decryption failed for E: frame (%d bytes), dropping", len(data)
                )
                return None
            # The plaintext is JSON, or an M:-prefixed msgpack payload on a
            # connection that negotiated wire_format="msgpack".
            return self._decode_binary(plaintext, decrypted=True)

        # Raw zlib (magic byte 0x78)
        if len(data) >= 2 and data[0] == ZLIB_MAGIC and data[1] in ZLIB_METHODS:
            decompressed = self._inflate(data, len(data))
            if decompressed is None:
                return None
            return self._decode_text(decompressed.decode("utf-8"))

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
        if t == "signed":
            return self._signed_to_event(parsed, category)
        p = parsed.get("p") or parsed.get("payload") or {}

        # Handle batch messages
        if t == "batch" and isinstance(p, dict) and isinstance(p.get("messages"), list):
            events = []
            for msg in p["messages"]:
                evt = self._parsed_to_event(msg, category)
                if isinstance(evt, WSEEvent):
                    events.append(evt)
            return events if events else None

        # Category: prefer wire prefix, fall back to "c" field in JSON
        cat = category.value if category else parsed.get("c")

        # Recovery stamp (present on stamped topic publications): topic / epoch /
        # offset, accepted under the short wire keys with long-name fallbacks.
        topic = parsed.get("tp") or parsed.get("topic")
        epoch = parsed.get("e") or parsed.get("epoch")
        offset = parsed.get("o")
        if offset is None:
            offset = parsed.get("offset")

        return WSEEvent(
            type=t if isinstance(t, str) else str(t),
            payload=p if isinstance(p, dict) else {"data": p},
            id=parsed.get("id"),
            sequence=parsed.get("seq"),
            timestamp=parsed.get("ts"),
            version=parsed.get("v", PROTOCOL_VERSION),
            category=cat,
            priority=parsed.get("pri"),
            correlation_id=parsed.get("cid"),
            topic=topic if isinstance(topic, str) else None,
            epoch=str(epoch) if epoch is not None else None,
            offset=int(offset) if isinstance(offset, int) else None,
        )

    def _signed_to_event(
        self, envelope: dict[str, Any], category: MessageCategory | None
    ) -> WSEEvent | None:
        """A message the server signed: checked against the pinned keys, then
        delivered as the signed text says, with the proof. Dropped when the
        check fails, or when the text names another topic than the one it
        came on."""
        try:
            message, proof = self._server_keys.open(envelope)
        except SignatureError as exc:
            logger.warning("Dropping a signed message: %s", exc)
            return None
        topic = envelope.get("tp")
        signed_topic = message.get("tp")
        if (
            isinstance(topic, str)
            and signed_topic is not None
            and signed_topic != topic
        ):
            logger.warning(
                "Dropping a message signed for topic %r that came on %r",
                signed_topic,
                topic,
            )
            return None
        epoch = envelope.get("e")
        offset = envelope.get("o")
        ts = message.get("ts")
        p = message.get("p", {})
        return WSEEvent(
            type=message["t"],
            payload=p if isinstance(p, dict) else {"data": p},
            id=message.get("n") if isinstance(message.get("n"), str) else None,
            timestamp=(
                datetime.fromtimestamp(ts / 1000, UTC).isoformat()
                if isinstance(ts, int)
                else None
            ),
            category=category.value if category else envelope.get("c"),
            topic=topic
            if isinstance(topic, str)
            else (signed_topic if isinstance(signed_topic, str) else None),
            epoch=str(epoch) if epoch is not None else None,
            offset=int(offset) if isinstance(offset, int) else None,
            signature=proof,
        )
