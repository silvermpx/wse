# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

"""
WSE Core Types

Type definitions for WebSocket Event System:
- EventPriority: Priority levels for events
- DeliveryGuarantee: Delivery semantics
- EventHandler: Protocol for event handlers
- EventMetadata: Event metadata
- Subscription: Subscription configuration
- SubscriptionStats: Subscription statistics
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


class EventHandler(Protocol):
    """Protocol for event handlers"""

    async def __call__(self, event: dict[str, Any]) -> None: ...


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class EventMetadata:
    """Metadata for events"""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    version: int = 1
    priority: EventPriority = EventPriority.NORMAL
    ttl: int | None = None  # seconds
    correlation_id: str | None = None
    causation_id: str | None = None
    source: str | None = None
    compressed: bool = False
    encrypted: bool = False
    user_id: str | None = None
    connection_id: str | None = None


@dataclass
class Subscription:
    """Represents an event subscription"""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    subscriber_id: str = ""
    topics: set[str] = field(default_factory=set)
    handler: EventHandler = None
    filters: dict[str, Any] = field(default_factory=dict)
    transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    active: bool = True
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    max_retries: int = 3
    retry_delay: float = 1.0
    dead_letter_topic: str | None = None
    batch_size: int = 100
    batch_timeout: float = 0.1
    # CRITICAL: Track last delivered to prevent duplicates
    last_delivered_id: str | None = None
    last_delivery_time: datetime | None = None


@dataclass
class SubscriptionStats:
    """Statistics for a subscription"""

    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    messages_filtered: int = 0
    messages_transformed: int = 0
    messages_duplicate: int = 0
    last_message_at: datetime | None = None
    average_processing_time: float = 0.0
    total_processing_time: float = 0.0


# =============================================================================
# EOF
# =============================================================================
