# =============================================================================
# WSE — WebSocket Engine
# Rust-accelerated event transformer (delegates to rust_transform_event)
# =============================================================================

"""
EventTransformer for WSE (standalone)

Transforms internal events to WebSocket-compatible format.

Architecture:
    Internal Event --> EventTransformer --> WebSocket Event (v1)

WebSocket Event Format (v1):
    {
        "v": 1,                          # Protocol version
        "id": "evt_123",                 # Event ID
        "t": "order_update",             # Event type (mapped from internal)
        "ts": "2025-01-13T...",          # Timestamp (ISO 8601)
        "seq": 1234,                     # Sequence number
        "p": {...},                      # Payload (transformed)
        "cid": "cor_123",                # Correlation ID (optional)
        "pri": 5,                        # Priority (optional)
        "event_version": 1,              # Schema version (optional)
        "trace_id": "trace_123",         # Distributed tracing (optional)
        "latency_ms": 15                 # Processing latency (optional)
    }

Usage:
    # Default (pass-through):
    transformer = EventTransformer()
    ws_event = transformer.transform_for_ws(raw_event, sequence=42)

    # Custom mappings:
    transformer = EventTransformer(
        event_type_map={"OrderPlaced": "order_update", ...},
        payload_transformers={"order_update": my_order_transformer, ...},
    )
"""

import logging
from collections.abc import Callable
from typing import Any

from wse_server._accel import rust_transform_event

log = logging.getLogger("wse.transformer")

# Type alias for payload transformer functions
PayloadTransformer = Callable[[dict[str, Any]], dict[str, Any]]


class EventTransformer:
    """Transform events between internal and WebSocket formats.

    The core transform_for_ws delegates to Rust for the generic path.
    Python payload transformers are still supported: if a registered
    Python transformer matches the WebSocket event type, it runs
    *after* the Rust envelope is built (replacing the generic payload).

    Args:
        event_type_map: Mapping from internal event type names to WebSocket
            event type names.  When ``None``, event types are lower-cased
            as a default.
        payload_transformers: Mapping from *WebSocket* event type to a
            callable that transforms the raw event dict into a payload dict.
            When a WebSocket event type has no registered transformer the
            default generic extraction is used.
    """

    def __init__(
        self,
        event_type_map: dict[str, str] | None = None,
        payload_transformers: dict[str, PayloadTransformer] | None = None,
    ):
        self._event_type_map: dict[str, str] = event_type_map or {}
        self._payload_transformers: dict[str, PayloadTransformer] = payload_transformers or {}

    # -----------------------------------------------------------------
    # Public helpers for registering custom mappings at runtime
    # -----------------------------------------------------------------

    def register_event_type(self, internal_type: str, ws_type: str) -> None:
        """Register a single internal -> WebSocket event type mapping."""
        self._event_type_map[internal_type] = ws_type

    def register_event_types(self, mapping: dict[str, str]) -> None:
        """Bulk-register internal -> WebSocket event type mappings."""
        self._event_type_map.update(mapping)

    def register_payload_transformer(
        self, ws_event_type: str, transformer: PayloadTransformer
    ) -> None:
        """Register a payload transformer for a WebSocket event type."""
        self._payload_transformers[ws_event_type] = transformer

    # -----------------------------------------------------------------
    # Core transformation
    # -----------------------------------------------------------------

    def transform_for_ws(self, event: dict[str, Any], sequence: int = 0) -> dict[str, Any]:
        """Transform internal event to WebSocket format.

        Delegates the heavy lifting to Rust (type mapping, value
        conversion, envelope construction, latency calculation).
        If a Python payload transformer is registered for the
        resulting WebSocket event type, it replaces the generic payload.
        """
        ws_event = rust_transform_event(event, sequence, self._event_type_map)

        # Apply Python payload transformer if registered
        ws_type = ws_event.get("t", "")
        transformer = self._payload_transformers.get(ws_type)
        if transformer:
            ws_event["p"] = transformer(event)

        return ws_event

    # -----------------------------------------------------------------
    # Utility helpers (kept in Python for backward compat)
    # -----------------------------------------------------------------

    @staticmethod
    def safe_decode_bytes(data: str | bytes | Any) -> str:
        """Safely decode bytes to string with fallback encodings."""
        if isinstance(data, str):
            return data
        if isinstance(data, bytes):
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data.decode("latin-1")
        return str(data)
