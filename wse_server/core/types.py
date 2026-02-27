# =============================================================================
# WSE -- WebSocket Engine
# =============================================================================

"""
WSE Core Types

Type definitions for WebSocket Engine:
- EventPriority: Priority levels for events
- DeliveryGuarantee: Delivery semantics
"""

from enum import Enum


class EventPriority(Enum):
    """Event priority levels"""

    CRITICAL = 10
    HIGH = 8
    NORMAL = 5
    LOW = 3
    BACKGROUND = 1


class DeliveryGuarantee(Enum):
    """Event delivery guarantees"""

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
