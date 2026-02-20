# =============================================================================
# WSE — WebSocket Engine
# Rust-accelerated event filters (delegates to rust_match_event)
# =============================================================================

"""
EventFilter for WSE

Advanced event filtering with MongoDB-like query operators.

Supported Operators:
- $in: Value must be in list
- $regex: Regular expression match
- $gt, $lt, $gte, $lte: Comparison operators
- $ne: Not equal
- $exists: Field existence check
- $contains: Substring match
- $startswith, $endswith: String prefix/suffix match

Example:
    ```python
    criteria = {
        "event_type": {"$in": ["OrderPlaced", "OrderFilled"]},
        "symbol": {"$regex": "^AAPL"},
        "quantity": {"$gte": 100}
    }

    if EventFilter.matches(event, criteria):
        # Process event
    ```

Nested Fields:
    Use dot notation for nested field access:
    ```python
    criteria = {
        "metadata.user_id": "user123",
        "payload.price": {"$gt": 100}
    }
    ```
"""

import logging
from typing import Any

from wse_server._accel import rust_match_event

log = logging.getLogger("wse.filters")


class EventFilter:
    """Advanced event filtering with multiple strategies.

    Delegates entirely to Rust for matching logic (regex, operators,
    nested field access, $and/$or).
    """

    @staticmethod
    def matches(event: dict[str, Any], criteria: dict[str, Any]) -> bool:
        """Check if an event matches filter criteria"""
        return rust_match_event(event, criteria)


# =============================================================================
# EOF
# =============================================================================
