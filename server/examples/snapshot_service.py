"""Example: Snapshot provider for WSE initial state delivery.

When a client connects and subscribes to topics, WSE can send an initial
snapshot of the current state. Implement the SnapshotProvider protocol.
"""
from typing import Any, Dict, List


class ExampleSnapshotProvider:
    """
    Provides initial state snapshots when clients subscribe.

    Implement get_snapshot() to return current state for the user's topics.
    WSE sends this as a 'snapshot' message immediately after subscription.
    """

    def __init__(self, db=None):
        self.db = db

    async def get_snapshot(self, user_id: str, topics: List[str]) -> Dict[str, Any]:
        """
        Return initial state for the given topics.

        Args:
            user_id: Authenticated user ID
            topics: List of topic strings the user subscribed to

        Returns:
            Dict with topic keys and their current state as values.
            Only include topics that have snapshot data.
        """
        snapshot = {}

        for topic in topics:
            if topic.startswith("chat:"):
                channel_id = topic.split(":", 1)[1]
                # Fetch recent messages for the channel
                snapshot[topic] = {
                    "messages": await self._get_recent_messages(channel_id),
                    "members_online": await self._get_online_members(channel_id),
                }
            elif topic == "notifications":
                snapshot[topic] = {
                    "unread_count": await self._get_unread_count(user_id),
                }

        return snapshot

    async def _get_recent_messages(self, channel_id: str) -> list:
        """Fetch last 50 messages from database."""
        if not self.db:
            return []
        # Your database query here
        return []

    async def _get_online_members(self, channel_id: str) -> list:
        return []

    async def _get_unread_count(self, user_id: str) -> int:
        return 0
