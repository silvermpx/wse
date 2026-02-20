# =============================================================================
# WSE — WebSocket Engine
# Rust-accelerated priority message queue (delegates to RustPriorityMessageQueue)
# =============================================================================

import asyncio
from dataclasses import dataclass
from typing import Any

from wse_server._accel import RustPriorityMessageQueue


@dataclass
class QueuedMessage:
    """Message in the queue"""

    message: dict[str, Any]
    priority: int
    timestamp: float
    retry_count: int = 0


class PriorityMessageQueue:
    """Priority-based message queue with batching support.

    Delegates to RustPriorityMessageQueue for all queue operations.
    Async methods wrap synchronous Rust calls.
    """

    def __init__(self, max_size: int = 1000, batch_size: int = 10):
        self.max_size = max_size
        self.batch_size = batch_size
        self._rust = RustPriorityMessageQueue(max_size=max_size, batch_size=batch_size)
        self._lock = asyncio.Lock()

    @property
    def size(self) -> int:
        return self._rust.size

    @property
    def priority_distribution(self) -> dict[int, int]:
        """Counts of messages at each priority level."""
        return dict(self._rust.get_stats().get("priority_distribution", {}))

    @property
    def oldest_message_timestamp(self) -> float | None:
        """Age of the oldest message in seconds, or None if queue is empty."""
        if self._rust.size == 0:
            return None
        return self._rust.get_stats().get("oldest_message_age", 0.0)

    @property
    def dropped_count(self) -> dict[int, int]:
        """Counts of dropped messages keyed by priority level."""
        return dict(self._rust.get_stats().get("dropped_by_priority", {}))

    async def enqueue(self, message: dict[str, Any], priority: int = 5) -> bool:
        """
        Add a message to the queue with priority-based dropping.

        When queue is full:
        1. Try to drop BACKGROUND (1) priority messages
        2. Try to drop LOW (3) priority messages
        3. Try to drop NORMAL (5) priority messages
        4. If new message is LOW/NORMAL and still full, drop it
        5. For HIGH/CRITICAL, drop oldest NORMAL as last resort
        6. If absolutely full with only HIGH/CRITICAL, drop new message
        """
        async with self._lock:
            return self._rust.enqueue(message, priority)

    async def dequeue_batch(self) -> list[tuple[int, dict[str, Any]]]:
        """Get a batch of messages ordered by priority"""
        async with self._lock:
            # Rust returns list of (priority, message) tuples
            return self._rust.dequeue_batch()

    def clear(self) -> None:
        """Clear all queues (synchronous - for emergency shutdown)"""
        self._rust.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get queue statistics"""
        return dict(self._rust.get_stats())
