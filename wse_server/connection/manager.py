# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..reliability.config import RateLimiterConfig
from ..reliability.rate_limiter import RateLimiter
from .connection import WSEConnection

log = logging.getLogger("wse.manager")


@dataclass
class ConnectionInfo:
    """Information about a WebSocket connection"""
    connection: WSEConnection
    user_id: str
    ip_address: str
    connected_at: datetime
    last_activity: datetime
    client_version: str
    protocol_version: int


class WSEManager:
    """
    Manages multiple WebSocket connections with reliability infrastructure.

    Industry Standard (2025): 1 connection per user using multiplexing pattern.
    All events routed through topics over single connection for optimal performance.
    """

    def __init__(self, max_connections_per_user: int = 1):
        self.connections: dict[str, ConnectionInfo] = {}
        self.user_connections: dict[str, set[str]] = {}
        self.ip_connections: dict[str, set[str]] = {}

        self.max_connections_per_user = max_connections_per_user

        # Create rate limiters for users and IPs
        self.user_rate_limiters: dict[str, RateLimiter] = {}
        self.ip_rate_limiters: dict[str, RateLimiter] = {}

        # Metrics
        self.total_connections = 0
        self.total_messages_sent = 0
        self.total_messages_received = 0

        self._lock = asyncio.Lock()
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    def _remove_connection_unlocked(self, conn_id: str) -> WSEConnection | None:
        """Remove a connection from tracking without acquiring the lock.
        MUST be called while holding self._lock.
        Returns the removed WSEConnection (for cleanup outside lock), or None."""
        if conn_id not in self.connections:
            return None

        info = self.connections[conn_id]
        user_id = info.user_id
        ip_address = info.ip_address

        # Update metrics
        self.total_messages_sent += info.connection.metrics.messages_sent
        self.total_messages_received += info.connection.metrics.messages_received

        # Remove from tracking
        del self.connections[conn_id]

        if user_id in self.user_connections:
            self.user_connections[user_id].discard(conn_id)
            if not self.user_connections[user_id]:
                del self.user_connections[user_id]
                if user_id in self.user_rate_limiters:
                    del self.user_rate_limiters[user_id]

        if ip_address in self.ip_connections:
            self.ip_connections[ip_address].discard(conn_id)
            if not self.ip_connections[ip_address]:
                del self.ip_connections[ip_address]
                if ip_address in self.ip_rate_limiters:
                    del self.ip_rate_limiters[ip_address]

        log.info(f"Removed connection {conn_id} for user {user_id}")
        return info.connection

    async def add_connection(
            self,
            connection: WSEConnection,
            ip_address: str,
            client_version: str,
            protocol_version: int
    ) -> bool:
        """Add a new connection.

        If the user is at the connection limit, removes the oldest connection
        from tracking and adds the new one in a SINGLE lock acquisition.
        The old connection's WebSocket is closed AFTER releasing the lock
        with a timeout to prevent hangs on half-closed sockets.
        """
        old_connection = None
        old_conn_id = None

        async with self._lock:
            conn_id = connection.conn_id
            user_id = connection.user_id

            # Check user connection limit
            user_conns = self.user_connections.get(user_id, set())
            if len(user_conns) >= self.max_connections_per_user:
                # Find oldest connection to replace
                oldest_conn_id = None
                oldest_time = None

                for existing_conn_id in user_conns:
                    if existing_conn_id in self.connections:
                        conn_info = self.connections[existing_conn_id]
                        if oldest_time is None or conn_info.connected_at < oldest_time:
                            oldest_time = conn_info.connected_at
                            oldest_conn_id = existing_conn_id

                if oldest_conn_id and oldest_conn_id in self.connections:
                    log.info(
                        f"User {user_id} at connection limit ({self.max_connections_per_user}). "
                        f"Replacing oldest connection {oldest_conn_id} with {conn_id}"
                    )
                    old_connection = self._remove_connection_unlocked(oldest_conn_id)
                    old_conn_id = oldest_conn_id
                else:
                    log.warning(f"User {user_id} exceeded max connections but no old connection found")
                    return False

            # Add the new connection (same lock acquisition -- atomic swap)
            info = ConnectionInfo(
                connection=connection,
                user_id=user_id,
                ip_address=ip_address,
                connected_at=datetime.now(UTC),
                last_activity=datetime.now(UTC),
                client_version=client_version,
                protocol_version=protocol_version
            )

            self.connections[conn_id] = info

            if user_id not in self.user_connections:
                self.user_connections[user_id] = set()
            self.user_connections[user_id].add(conn_id)

            if ip_address not in self.ip_connections:
                self.ip_connections[ip_address] = set()
            self.ip_connections[ip_address].add(conn_id)

            self.total_connections += 1

            log.info(
                f"Added connection {conn_id} for user {user_id} from {ip_address}. "
                f"User now has {len(self.user_connections[user_id])} connections."
            )

        # Signal old connection to stop and close its WebSocket OUTSIDE the lock.
        if old_connection:
            old_connection._running = False
            try:
                await asyncio.wait_for(
                    old_connection.ws.close(code=4000, reason="Replaced by new connection"),
                    timeout=3.0,
                )
                log.info(f"Closed old connection {old_conn_id} WebSocket")
            except TimeoutError:
                log.warning(
                    f"Timeout closing old connection {old_conn_id} WebSocket "
                    f"(client likely already disconnected)"
                )
            except Exception as e:
                log.warning(f"Error closing old connection {old_conn_id}: {e}")

        return True

    async def remove_connection(self, conn_id: str) -> None:
        """Remove a connection (acquires lock, safe to call from outside)"""
        async with self._lock:
            self._remove_connection_unlocked(conn_id)

    async def get_user_connections(self, user_id: str) -> list[WSEConnection]:
        """Get all connections for a user"""
        async with self._lock:
            conn_ids = self.user_connections.get(user_id, set())
            connections = []

            for conn_id in conn_ids:
                if conn_id in self.connections:
                    connections.append(self.connections[conn_id].connection)

            return connections

    async def broadcast_to_user(self, user_id: str, message: dict[str, Any]) -> int:
        """Broadcast message to all user connections"""
        connections = await self.get_user_connections(user_id)
        sent_count = 0

        for connection in connections:
            try:
                await connection.send_message(message)
                sent_count += 1
            except Exception as e:
                log.error(f"Failed to send to connection {connection.conn_id}: {e}")

        return sent_count

    async def get_stats(self) -> dict[str, Any]:
        """Get manager statistics"""
        async with self._lock:
            active_users = len(self.user_connections)
            active_ips = len(self.ip_connections)
            active_connections = len(self.connections)

            avg_connections_per_user = 0
            if active_users > 0:
                total_user_connections = sum(
                    len(conns) for conns in self.user_connections.values()
                )
                avg_connections_per_user = total_user_connections / active_users

            version_distribution = {}
            for info in self.connections.values():
                version = info.client_version
                version_distribution[version] = version_distribution.get(version, 0) + 1

            protocol_distribution = {}
            for info in self.connections.values():
                protocol = info.protocol_version
                protocol_distribution[protocol] = protocol_distribution.get(protocol, 0) + 1

            return {
                'active_connections': active_connections,
                'active_users': active_users,
                'active_ips': active_ips,
                'total_connections': self.total_connections,
                'total_messages_sent': self.total_messages_sent,
                'total_messages_received': self.total_messages_received,
                'avg_connections_per_user': round(avg_connections_per_user, 2),
                'max_connections_per_user': self.max_connections_per_user,
                'client_versions': version_distribution,
                'protocol_versions': protocol_distribution
            }

    async def check_rate_limits(self, user_id: str, ip_address: str, is_premium: bool = False) -> bool:
        """Check if the connection is rate-limited"""
        if user_id not in self.user_rate_limiters:
            if is_premium:
                config = RateLimiterConfig(
                    algorithm="token_bucket",
                    capacity=5000,
                    refill_rate=500.0,
                )
            else:
                config = RateLimiterConfig(
                    algorithm="token_bucket",
                    capacity=1000,
                    refill_rate=100.0,
                )

            self.user_rate_limiters[user_id] = RateLimiter(
                name=f"wse_user_{user_id}",
                config=config
            )

        if ip_address not in self.ip_rate_limiters:
            config = RateLimiterConfig(
                algorithm="token_bucket",
                capacity=10000,
                refill_rate=1000.0,
            )

            self.ip_rate_limiters[ip_address] = RateLimiter(
                name=f"wse_ip_{ip_address}",
                config=config
            )

        user_allowed = await self.user_rate_limiters[user_id].acquire()
        if not user_allowed:
            return False

        ip_allowed = await self.ip_rate_limiters[ip_address].acquire()
        return ip_allowed

    async def _cleanup_loop(self):
        """Periodically clean up stale connections"""
        while True:
            try:
                await asyncio.sleep(30)

                async with self._lock:
                    now_ts = time.time()
                    stale_connections = []

                    for conn_id, info in self.connections.items():
                        last_active = info.connection._last_activity
                        if (now_ts - last_active) > 120:
                            stale_connections.append(conn_id)

                    for conn_id in stale_connections:
                        log.warning(f"Removing stale connection {conn_id}")
                        self._remove_connection_unlocked(conn_id)

                    user_ids_to_remove = [
                        uid for uid in self.user_rate_limiters
                        if uid not in self.user_connections
                    ]
                    for uid in user_ids_to_remove:
                        del self.user_rate_limiters[uid]

                    ip_addresses_to_remove = [
                        ip for ip in self.ip_rate_limiters
                        if ip not in self.ip_connections
                    ]
                    for ip in ip_addresses_to_remove:
                        del self.ip_rate_limiters[ip]

                    if user_ids_to_remove or ip_addresses_to_remove:
                        log.info(f"Cleaned up {len(user_ids_to_remove)} user rate limiters "
                                 f"and {len(ip_addresses_to_remove)} IP rate limiters")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in connection cleanup: {e}")

    async def shutdown(self):
        """Shutdown the manager"""
        self._cleanup_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._cleanup_task


# Global instance
_ws_manager: WSEManager | None = None


def get_ws_manager() -> WSEManager:
    """Get the global WebSocket manager instance"""
    global _ws_manager
    if _ws_manager is None:
        _ws_manager = WSEManager()
    return _ws_manager


async def reset_ws_manager():
    """Reset the WebSocket manager (useful for testing)"""
    global _ws_manager
    if _ws_manager:
        await _ws_manager.shutdown()
    _ws_manager = None
