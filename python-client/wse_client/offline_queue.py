# =============================================================================
# WSE Python Client -- Offline Queue
# =============================================================================
#
# Buffers outgoing messages when the connection is down and flushes them
# (in priority order) on reconnect.  Port of client/services/OfflineQueue.ts.
#
# In-memory implementation - Python processes don't need persistence across
# restarts the way browser tabs need IndexedDB.
# =============================================================================

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from ._logging import logger


@dataclass(order=True)
class QueuedMessage:
    """A message waiting to be sent when the connection is restored."""

    _sort_priority: int = field(init=False, repr=False)  # negated for descending sort
    priority: int = field(compare=False)
    timestamp: float = field(compare=False)
    encoded: str = field(compare=False)
    retries: int = field(default=0, compare=False)
    msg_type: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        self._sort_priority = -self.priority  # higher priority sorts first


class OfflineQueue:
    """In-memory queue for messages that could not be sent while offline.

    Messages are stored with their priority and timestamp. On flush,
    they are returned in priority order (highest priority first, i.e.
    lowest numeric value). Expired messages are dropped automatically.

    Args:
        max_size: Maximum number of buffered messages. Default 1000.
        max_age: Maximum age in seconds before a message is discarded.
            Default 300 (5 minutes).
        enabled: Whether the queue is active. Default True.
    """

    def __init__(
        self,
        *,
        max_size: int = 1000,
        max_age: float = 300.0,
        enabled: bool = True,
    ) -> None:
        self._max_size = max_size
        self._max_age = max_age
        self._enabled = enabled
        self._queue: deque[QueuedMessage] = deque()

    @property
    def size(self) -> int:
        return len(self._queue)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enqueue(
        self,
        encoded: str,
        *,
        priority: int = 5,
        msg_type: str = "",
    ) -> bool:
        """Add a message to the offline queue.

        Returns True if enqueued, False if the queue is full or disabled.
        """
        if not self._enabled:
            return False

        if len(self._queue) >= self._max_size:
            logger.debug("Offline queue full (%d), dropping message", self._max_size)
            return False

        self._queue.append(
            QueuedMessage(
                priority=priority,
                timestamp=time.monotonic(),
                encoded=encoded,
                msg_type=msg_type,
            )
        )
        return True

    def drain(self) -> list[tuple[str, int]]:
        """Return all queued messages in priority order and clear the queue.

        Returns a list of ``(encoded, priority)`` tuples so callers can
        preserve priority when re-enqueuing on partial flush failure.
        Expired messages (older than *max_age*) are dropped silently.
        """
        if not self._queue:
            return []

        now = time.monotonic()
        cutoff = now - self._max_age

        # Filter expired, sort by priority
        valid = [m for m in self._queue if m.timestamp >= cutoff]
        valid.sort()  # QueuedMessage is ordered by priority

        expired = len(self._queue) - len(valid)
        if expired:
            logger.debug("Dropped %d expired messages from offline queue", expired)

        self._queue.clear()
        return [(m.encoded, m.priority) for m in valid]

    def clear(self) -> None:
        """Discard all queued messages."""
        self._queue.clear()

    def get_stats(self) -> dict[str, Any]:
        return {
            "size": len(self._queue),
            "capacity": self._max_size,
            "enabled": self._enabled,
            "max_age_seconds": self._max_age,
        }
