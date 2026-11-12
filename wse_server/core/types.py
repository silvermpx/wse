# =============================================================================
# WSE -- WebSocket Engine
# =============================================================================

"""
WSE Core Types

Type definitions for WebSocket Engine:
- EventPriority: Priority levels for events
- DeliveryGuarantee: Delivery semantics
"""

from enum import Enum, IntEnum


class EventPriority(IntEnum):
    """Event priority levels.

    The members are ints, so they can be passed wherever the queues take a
    priority: ``queue.enqueue(msg, EventPriority.HIGH)``.
    """

    CRITICAL = 10
    HIGH = 8
    NORMAL = 5
    LOW = 3
    BACKGROUND = 1


class DeliveryGuarantee(Enum):
    """Event delivery guarantees.

    WSE provides at-most-once delivery. The recovery system allows
    clients to catch up on missed messages within a time window,
    but does not guarantee redelivery.
    """

    AT_MOST_ONCE = "at_most_once"
