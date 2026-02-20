"""Example: Domain event mapping configuration for WSE.

WSE transforms internal event types to wire-format event types.
Configure your mapping when creating the router.
"""

# Map internal event names to client-facing wire names.
# Keys: your internal event type strings
# Values: what the client receives as the "t" field
EVENT_TYPE_MAP = {
    # Chat application example
    "ChatMessageCreated": "chat_message",
    "ChatMessageEdited": "chat_message_edit",
    "ChatMessageDeleted": "chat_message_delete",
    "UserTypingStarted": "typing_start",
    "UserTypingStopped": "typing_stop",
    "UserPresenceChanged": "presence_update",
    "ChannelCreated": "channel_created",
    "ChannelArchived": "channel_archived",
    # IoT / monitoring example
    "SensorReadingReceived": "sensor_reading",
    "DeviceStatusChanged": "device_status",
    "AlertTriggered": "alert",
    "AlertResolved": "alert_resolved",
    "ThresholdExceeded": "threshold_warning",
}


def example_payload_transformer(event_type: str, payload: dict) -> dict:
    """
    Optional: transform payload before sending to client.

    Useful for:
    - Stripping internal fields (database IDs, audit trails)
    - Renaming fields for the client API contract
    - Adding computed fields
    """
    # Strip internal fields
    payload.pop("_internal_trace_id", None)
    payload.pop("_created_by_service", None)

    return payload
