"""Example: Publishing events from your application to WSE clients."""


async def publish_to_user(bus, user_id: str, event_type: str, payload: dict):
    """
    Publish an event to a specific user's WSE connection.

    Args:
        bus: PubSubBus instance (from app.state.pubsub_bus)
        user_id: Target user ID
        event_type: Event type string (mapped via EVENT_TYPE_MAP)
        payload: Event payload dict
    """
    await bus.publish(
        topic=f"user:{user_id}:events",
        event={
            "event_type": event_type,
            **payload,
        },
    )


async def publish_to_channel(bus, channel: str, event_type: str, payload: dict):
    """Publish an event to all subscribers of a channel."""
    await bus.publish(
        topic=channel,
        event={
            "event_type": event_type,
            **payload,
        },
    )


# --- Usage in a FastAPI endpoint ---
#
# from fastapi import Request
#
# @router.post("/messages")
# async def create_message(request: Request, body: CreateMessageRequest):
#     bus = request.app.state.pubsub_bus
#     message = save_message(body)
#     await publish_to_channel(
#         bus,
#         channel=f"chat:{body.channel_id}",
#         event_type="ChatMessageCreated",
#         payload={
#             "id": str(message.id),
#             "text": message.text,
#             "author": message.author,
#         },
#     )
#     return message
