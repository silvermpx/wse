# =============================================================================
# WSE — WebSocket Event System
# =============================================================================

"""
PubSubBus - Redis Pub/Sub for WebSocket Coordination

Industry Pattern: redis-py official async pub/sub
- Shared connection pool (redis-py best practice)
- PSUBSCRIBE for pattern matching
- Broadcast to ALL instances
- Non-blocking handler invocation

Architecture:
    Domain Event -> Publisher -> PubSubBus.publish()
                                       |
                           Redis Pub/Sub (broadcast)
                                       |
                        ALL WSE Instances subscribe
                                       |
                   Each instance checks: "user connected to me?"
                                       |
                           Yes -> Send to WebSocket
                           No  -> Ignore

Best Practices:
- Fire-and-forget delivery (ephemeral)
- <1ms latency (vs 5-10ms Streams)
- Handles 10,000+ events/sec
- Simple coordination (no consumer groups, ACK, offset tracking)
- Reuses global Redis connection pool (saves connections)

Sources:
- redis-py official asyncio examples (2024-2025)
- FastAPI + Redis Pub/Sub tutorials (2024)
- geOps production multiplexer pattern (2024)
"""

import asyncio
import json
import logging
import random
import time
import uuid
from typing import Callable, Dict, Set, Any, Optional
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False

from ..reliability.circuit_breaker import get_circuit_breaker, CircuitBreakerConfig
from ..metrics.connection_metrics import (
    pubsub_published_total,
    pubsub_received_total,
    pubsub_publish_latency_seconds,
    pubsub_handler_errors_total,
    pubsub_listener_errors_total,
    dlq_messages_total,
    dlq_size,
    dlq_replayed_total,
)

log = logging.getLogger("wse.pubsub")

# Metrics are always available via stubs from connection_metrics
METRICS_AVAILABLE = True


class WSEJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for WSE events that handles UUID, datetime, Decimal"""

    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def _orjson_default(obj):
    """orjson default for Decimal (orjson handles UUID/datetime natively)."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


class PubSubBus:
    """
    Redis Pub/Sub Bus for Multi-Instance WebSocket Coordination

    This class implements the redis-py official async pub/sub pattern:
    - Shared connection pool (reuses global Redis client)
    - Pattern-based subscriptions (PSUBSCRIBE)
    - Broadcast to all server instances
    - Non-blocking handler invocation

    Example Usage:
        ```python
        bus = PubSubBus(redis_client=redis_client)
        await bus.initialize()

        # Subscribe to pattern
        await bus.subscribe("user:123:*", handler)

        # Publish event (broadcasts to ALL instances)
        await bus.publish("user:123:events", {"type": "order_placed"})
        ```
    """

    def __init__(self, redis_client: Any):
        """
        Initialize PubSubBus

        Args:
            redis_client: Redis client instance (from global pool)
        """
        if not redis_client:
            raise ValueError("redis_client is required")

        self.redis_client = redis_client
        self.pubsub: Optional[Any] = None  # redis.asyncio.client.PubSub

        # Subscriptions: pattern -> {subscription_id: handler}
        # Multiple handlers per channel (one per WSE connection)
        self.subscriptions: Dict[str, Dict[str, Callable]] = {}

        # Connection tracking: connection_id -> set of subscription_ids
        # Used for bulk cleanup on disconnect (prevents orphaned handlers)
        self._connection_handlers: Dict[str, Set[str]] = {}

        # State
        self._running = False
        self._listener_task: Optional[asyncio.Task] = None

        # Metrics
        self._messages_published = 0
        self._messages_received = 0
        self._handler_errors = 0

        # Dead Letter Queue configuration
        self._dlq_enabled = True
        self._dlq_prefix = "wse:dlq:"
        self._dlq_ttl = 86400 * 7  # 7 days

        # Circuit Breaker for Redis Pub/Sub listener
        self._circuit_breaker = get_circuit_breaker(
            "pubsub_listener",
            CircuitBreakerConfig(
                name="pubsub_listener",
                failure_threshold=10,      # Open after 10 failures
                reset_timeout_seconds=60,  # Try recovery after 60s
                half_open_max_calls=3,     # Test with 3 calls
                success_threshold=3        # Close after 3 successes
            )
        )

        # Exponential backoff state
        self._consecutive_errors = 0
        self._backoff_delay = 1.0
        self._max_backoff = 60.0
        self._backoff_factor = 1.5

    async def initialize(self) -> None:
        """
        Initialize Redis Pub/Sub

        Pattern (redis-py official):
        - PUBLISH: Use redis_client directly (from shared pool)
        - SUBSCRIBE: Use redis_client.pubsub() (gets connection from SAME pool)

        This ensures all connections come from the global Redis connection pool.
        """
        try:
            log.info("[PUBSUB_BUS] Initializing PubSubBus...")

            # Create PubSub from redis_client (uses same connection pool)
            self.pubsub = self.redis_client.pubsub()
            log.info(f"[PUBSUB_BUS] Created pubsub instance: {self.pubsub is not None}")

            # Test connection
            await self.redis_client.ping()
            log.info("[PUBSUB_BUS] Redis ping successful")

            self._running = True
            log.info("[PUBSUB_BUS] Set _running = True")

            # NOTE: Listener task will be started on first subscription
            # (redis-py listen() terminates immediately if no subscriptions exist)
            log.info("[PUBSUB_BUS] PubSubBus initialized - listener will start on first subscription")

        except Exception as e:
            log.error(f"Failed to initialize PubSubBus: {e}", exc_info=True)
            raise

    async def publish(self, topic: str, event: Dict[str, Any], priority: Any = None, ttl: int = None) -> None:
        """
        Publish event to Redis Pub/Sub (broadcast to ALL instances)

        Args:
            topic: Topic pattern (e.g., "user:123:events")
            event: Event dictionary to publish
            priority: Optional EventPriority (added to event metadata for priority queue)
            ttl: Optional TTL in seconds (ignored - Redis Pub/Sub is ephemeral)

        Note:
            - Adds "wse:" prefix for namespacing
            - Fire-and-forget delivery (at-most-once)
            - <1ms latency (industry benchmark)
            - Priority parameter added to event metadata (for priority queue)
            - TTL parameter ignored (Redis Pub/Sub doesn't support TTL)
        """
        if not self.redis_client:
            log.warning("PubSubBus not initialized, cannot publish")
            return

        start_time = time.time()

        # Generate or extract event ID for tracking
        event_id = event.get('id') or event.get('event_id') or str(uuid.uuid4())
        event_type = event.get('event_type') or event.get('t') or 'unknown'

        log.debug(f"[PUBSUB_PUBLISH_START] Event: {event_type}, ID: {event_id}, Topic: {topic}")

        try:
            # Add wse: prefix for namespacing
            channel = f"wse:{topic}"

            # Add priority to event metadata if provided (for priority queue)
            priority_str = "default"
            if priority is not None:
                if 'metadata' not in event:
                    event['metadata'] = {}
                event['metadata']['priority'] = priority.value if hasattr(priority, 'value') else priority
                priority_str = priority.name if hasattr(priority, 'name') else str(priority)

            # Add event ID to metadata for tracking
            if 'metadata' not in event:
                event['metadata'] = {}
            event['metadata']['event_id'] = event_id
            event['metadata']['published_at'] = datetime.now(timezone.utc).isoformat()

            # Serialize event to JSON
            if ORJSON_AVAILABLE:
                message = orjson.dumps(event, default=_orjson_default).decode()
            else:
                import json
                message = json.dumps(event, default=str)
            message_size = len(message)

            await self.redis_client.publish(channel, message)

            self._messages_published += 1
            publish_latency_ms = (time.time() - start_time) * 1000

            # Prometheus metrics (stubs unless real Prometheus is configured)
            pubsub_published_total.labels(topic=topic, priority=priority_str).inc()
            latency = time.time() - start_time
            pubsub_publish_latency_seconds.labels(topic=topic).observe(latency)

            log.debug(
                f"[PUBSUB_PUBLISH_SUCCESS] Event: {event_type}, ID: {event_id}, "
                f"Topic: {topic}, Priority: {priority_str}, "
                f"Size: {message_size}B, Latency: {publish_latency_ms:.2f}ms, "
                f"Total: {self._messages_published}"
            )

        except Exception as e:
            log.error(f"Failed to publish to {topic}: {e}", exc_info=True)

    async def subscribe(self, pattern: str, handler: Callable, connection_id: str | None = None) -> str:
        """
        Subscribe to topic using SUBSCRIBE (exact) or PSUBSCRIBE (wildcard)

        Args:
            pattern: Topic or pattern (e.g., "user:123:events" or "user:*:events")
            handler: Async callback function(event: Dict) -> None
            connection_id: Optional WSE connection ID for bulk cleanup on disconnect

        Returns:
            Subscription ID

        Example:
            ```python
            async def my_handler(event: Dict):
                print(f"Received: {event}")

            sub_id = await bus.subscribe("user:123:*", my_handler, connection_id="ws_abc123")
            ```

        Note:
            - Uses PSUBSCRIBE for patterns with wildcards (* or ?)
            - Uses SUBSCRIBE for exact topics (no wildcards)
            - Handler invoked in background (non-blocking)
            - Each instance receives ALL messages (broadcast)
        """
        if not self.pubsub:
            raise RuntimeError("PubSubBus not initialized")

        try:
            # Add wse: prefix
            channel_pattern = f"wse:{pattern}"

            # Generate subscription ID
            subscription_id = f"{pattern}::{id(handler)}"

            # Only subscribe to Redis if this is the FIRST handler for this channel
            already_subscribed = channel_pattern in self.subscriptions
            if not already_subscribed:
                is_pattern = '*' in pattern or '?' in pattern
                if is_pattern:
                    await self.pubsub.psubscribe(channel_pattern)
                    log.debug(f"Subscribed to pattern (PSUBSCRIBE): {channel_pattern}")
                else:
                    await self.pubsub.subscribe(channel_pattern)
                    log.debug(f"Subscribed to exact topic (SUBSCRIBE): {channel_pattern}")
                self.subscriptions[channel_pattern] = {}
            else:
                log.debug(f"Adding handler to existing channel: {channel_pattern} (now {len(self.subscriptions[channel_pattern]) + 1} handlers)")

            # Store handler (multiple handlers per channel supported)
            self.subscriptions[channel_pattern][subscription_id] = handler

            # Track handler by connection_id for bulk cleanup
            if connection_id:
                if connection_id not in self._connection_handlers:
                    self._connection_handlers[connection_id] = set()
                self._connection_handlers[connection_id].add(subscription_id)

            # Start listener task if not already running
            if self._listener_task is None or self._listener_task.done():
                # Reset state in case listener was stopped by consecutive errors
                self._running = True
                self._consecutive_errors = 0
                self._backoff_delay = 1.0
                self._listener_task = asyncio.create_task(self._listen_loop())
                log.info("[PUBSUB_BUS] Started listener task after first subscription")

            return subscription_id

        except Exception as e:
            log.error(f"Failed to subscribe to {pattern}: {e}", exc_info=True)
            raise

    async def unsubscribe(self, subscription_id: str) -> None:
        """
        Unsubscribe a specific handler from a topic.

        Only removes the handler matching this subscription_id.
        Only unsubscribes from Redis when the LAST handler for a channel is removed.
        This prevents a race condition where connection A's cleanup removes
        connection B's handler when both subscribe to the same channel.

        Args:
            subscription_id: ID returned from subscribe()
        """
        if not self.pubsub:
            return

        # Extract pattern from subscription_id
        pattern = subscription_id.split("::")[0]
        channel_pattern = f"wse:{pattern}"

        try:
            handlers = self.subscriptions.get(channel_pattern)
            if not handlers:
                return

            # Remove only this specific handler
            handlers.pop(subscription_id, None)

            # Only unsubscribe from Redis when NO handlers remain
            if not handlers:
                del self.subscriptions[channel_pattern]

                is_pattern = '*' in pattern or '?' in pattern
                if is_pattern:
                    await self.pubsub.punsubscribe(channel_pattern)
                    log.info(f"Unsubscribed from pattern (PUNSUBSCRIBE): {channel_pattern} (last handler)")
                else:
                    await self.pubsub.unsubscribe(channel_pattern)
                    log.info(f"Unsubscribed from exact topic (UNSUBSCRIBE): {channel_pattern} (last handler)")
            else:
                log.info(f"Removed handler from {channel_pattern}, {len(handlers)} remaining")

        except Exception as e:
            log.error(f"Failed to unsubscribe from {pattern}: {e}", exc_info=True)

    async def unsubscribe_connection(self, connection_id: str) -> int:
        """
        Bulk unsubscribe ALL handlers registered by a specific WSE connection.

        Called during connection cleanup to prevent orphaned handlers that
        continue receiving messages after WebSocket disconnect/reconnect.

        Args:
            connection_id: WSE connection ID (e.g., "ws_f186cd9d_019c53c4")

        Returns:
            Number of handlers removed
        """
        sub_ids = self._connection_handlers.pop(connection_id, set())
        if not sub_ids:
            return 0

        removed = 0
        for sub_id in sub_ids:
            try:
                await self.unsubscribe(sub_id)
                removed += 1
            except Exception as e:
                log.error(f"Error bulk-unsubscribing {sub_id} for connection {connection_id}: {e}")

        log.info(f"[PUBSUB_BUS] Bulk unsubscribed {removed}/{len(sub_ids)} handlers for connection {connection_id}")
        return removed

    async def _listen_loop(self) -> None:
        """
        Background listener for Pub/Sub messages with circuit breaker and exponential backoff

        Features:
        - Circuit breaker (Google SRE Ch. 22 pattern)
        - Exponential backoff with jitter (AWS best practices)
        - Self-healing on Redis outages

        Pattern:
            for message in pubsub.listen():
                if message['type'] == 'pmessage':  # PSUBSCRIBE (patterns)
                    asyncio.create_task(handler(event))
                elif message['type'] == 'message':  # SUBSCRIBE (exact)
                    asyncio.create_task(handler(event))
        """
        log.info("[PUBSUB_BUS] PubSub listener loop started with circuit breaker")

        while self._running:
            try:
                # Execute through circuit breaker
                message = await self._circuit_breaker.call(self._get_message_with_timeout)

                if message and message.get('type') in ['pmessage', 'message']:
                    # Process message
                    await self._process_message(message)

                    # Reset backoff on success
                    self._consecutive_errors = 0
                    self._backoff_delay = 1.0

            except asyncio.CancelledError:
                log.info("Listener loop cancelled")
                break

            except Exception as e:
                self._consecutive_errors += 1

                # Circuit breaker: Stop after too many errors
                if self._consecutive_errors >= 20:
                    log.critical("Too many consecutive errors, stopping listener")
                    self._running = False
                    break

                # Exponential backoff with jitter
                delay = min(
                    self._backoff_delay * (self._backoff_factor ** (self._consecutive_errors - 1)),
                    self._max_backoff
                )

                # Add jitter (+/-20%)
                jitter = delay * 0.2 * (2 * random.random() - 1)
                delay = max(0.1, delay + jitter)

                log.error(
                    f"PubSubBus listener error (attempt {self._consecutive_errors}): {e}. "
                    f"Retrying in {delay:.2f}s...",
                    exc_info=True
                )

                await asyncio.sleep(delay)

    async def _get_message_with_timeout(self):
        """Get message with timeout (for circuit breaker wrapping)"""
        return await self.pubsub.get_message(timeout=1.0)

    async def _process_message(self, message: Dict) -> None:
        """Process a single Pub/Sub message"""
        # Handle pattern messages (PSUBSCRIBE)
        if message['type'] == 'pmessage':
            # Handle both bytes and str (redis-py async returns str in newer versions)
            pattern_data = message['pattern']
            pattern = pattern_data.decode('utf-8') if isinstance(pattern_data, bytes) else pattern_data

            data_bytes = message['data']
            data_str = data_bytes.decode('utf-8') if isinstance(data_bytes, bytes) else data_bytes

            try:
                event = orjson.loads(data_str) if ORJSON_AVAILABLE else __import__('json').loads(data_str)
            except Exception as e:
                log.error(f"Failed to deserialize pmessage: {e}")
                return

            # Get handlers for this pattern (multiple connections may subscribe)
            handlers = self.subscriptions.get(pattern)

            if handlers:
                self._messages_received += 1

                # Prometheus metrics
                pubsub_received_total.labels(pattern=pattern).inc()

                # Invoke ALL handlers in background (one per subscribed connection)
                for handler in list(handlers.values()):
                    asyncio.create_task(self._safe_invoke_handler(handler, event))
            else:
                log.warning(f"No handler for pattern: {pattern}")

        # Handle exact topic messages (SUBSCRIBE)
        elif message['type'] == 'message':
            # Handle both bytes and str (redis-py async returns str in newer versions)
            channel_data = message['channel']
            channel = channel_data.decode('utf-8') if isinstance(channel_data, bytes) else channel_data

            data_bytes = message['data']
            data_str = data_bytes.decode('utf-8') if isinstance(data_bytes, bytes) else data_bytes

            log.debug(f"[PUBSUB_BUS] Received message on channel: {channel}")

            try:
                event = orjson.loads(data_str) if ORJSON_AVAILABLE else __import__('json').loads(data_str)
                event_type = event.get('event_type') or event.get('t') or 'unknown'
                log.debug(f"PubSubBus message event_type: {event_type}")
            except Exception as e:
                log.error(f"Failed to deserialize message: {e}")
                return

            # Get handlers for this exact topic (multiple connections may subscribe)
            handlers = self.subscriptions.get(channel)

            if handlers:
                self._messages_received += 1
                log.debug(f"PubSubBus invoking {len(handlers)} handler(s) for {channel}, total received: {self._messages_received}")

                # Prometheus metrics
                pubsub_received_total.labels(pattern=channel).inc()

                # Invoke ALL handlers in background (one per subscribed connection)
                for handler in list(handlers.values()):
                    asyncio.create_task(self._safe_invoke_handler(handler, event))
            else:
                log.warning(f"No handler for channel: {channel}, available channels: {list(self.subscriptions.keys())}")

    async def _safe_invoke_handler(self, handler: Callable, event: Dict) -> None:
        """
        Safely invoke handler with error handling

        Args:
            handler: Async callback function
            event: Event dictionary

        Note:
            - Catches all exceptions to prevent handler errors from crashing listener
            - Logs errors for debugging
        """
        event_type = event.get('event_type', event.get('type', 'unknown'))
        event_id = event.get('event_id', event.get('_metadata', {}).get('event_id', 'unknown'))

        # Extract event ID from metadata if available
        if 'metadata' in event and 'event_id' in event['metadata']:
            event_id = event['metadata']['event_id']

        handler_start_time = time.time()

        try:
            log.debug(f"[HANDLER_START] Event: {event_type}, ID: {event_id}")
            await handler(event)

            handler_latency_ms = (time.time() - handler_start_time) * 1000
            log.debug(f"[HANDLER_SUCCESS] Event: {event_type}, ID: {event_id}, Latency: {handler_latency_ms:.2f}ms")
        except Exception as e:
            self._handler_errors += 1
            log.error(
                f"[PUBSUB_ERROR] Handler error (total: {self._handler_errors}) for {event_type}: {e}",
                exc_info=True
            )

            # Prometheus metrics
            error_type_str = type(e).__name__
            pubsub_handler_errors_total.labels(
                pattern=event.get('channel', 'unknown'),
                error_type=error_type_str
            ).inc()

            # Send to Dead Letter Queue
            await self._send_to_dlq(
                channel=event.get('channel', 'unknown'),
                message=event,
                error_type="handler_exception",
                error_details=str(e),
                message_id=event_id
            )

    async def _send_to_dlq(
        self,
        channel: str,
        message: Any,
        error_type: str,
        error_details: str,
        message_id: Optional[str] = None
    ) -> None:
        """Send failed message to Dead Letter Queue"""
        if not self._dlq_enabled:
            return

        try:
            dlq_key = f"{self._dlq_prefix}{channel}"
            now = datetime.now(timezone.utc)

            dlq_entry = {
                "channel": channel,
                "message": message if isinstance(message, dict) else str(message),
                "message_id": message_id or f"dlq_{now.timestamp()}",
                "error_type": error_type,
                "error_details": error_details,
                "timestamp": now.isoformat(),
                "retry_count": 0
            }

            # Store in Redis List
            await self.redis_client.lpush(dlq_key, json.dumps(dlq_entry, cls=WSEJSONEncoder))
            await self.redis_client.expire(dlq_key, self._dlq_ttl)
            await self.redis_client.ltrim(dlq_key, 0, 999)  # Keep latest 1000

            # Prometheus metrics
            dlq_messages_total.labels(channel=channel, error_type=error_type).inc()
            # Update DLQ size gauge
            dlq_len = await self.redis_client.llen(dlq_key)
            dlq_size.labels(channel=channel).set(dlq_len)

            log.info(f"[DLQ] Message sent to DLQ: {dlq_key}, message_id={message_id}")

        except Exception as e:
            log.error(f"[DLQ] Failed to send to DLQ: {e}", exc_info=True)

    async def get_dlq_messages(self, channel: str, limit: int = 100) -> list:
        """Retrieve messages from DLQ"""
        dlq_key = f"{self._dlq_prefix}{channel}"

        try:
            messages = await self.redis_client.lrange(dlq_key, 0, limit - 1)
            return [json.loads(msg) for msg in messages]
        except Exception as e:
            log.error(f"[DLQ] Failed to get DLQ messages: {e}")
            return []

    async def replay_dlq_message(self, channel: str, message_id: str) -> bool:
        """Replay a single DLQ message (manual recovery)"""
        dlq_key = f"{self._dlq_prefix}{channel}"

        try:
            # Find message in DLQ
            entries = await self.redis_client.lrange(dlq_key, 0, -1)
            for i, entry_json in enumerate(entries):
                entry = json.loads(entry_json)
                if entry.get('message_id') == message_id:
                    # Remove from DLQ
                    await self.redis_client.lrem(dlq_key, 1, entry_json)

                    # Republish to original channel
                    original_message = entry['message']
                    channel_to_publish = f"wse:{channel}" if not channel.startswith('wse:') else channel
                    await self.redis_client.publish(channel_to_publish, json.dumps(original_message, cls=WSEJSONEncoder))

                    # Prometheus metrics
                    dlq_replayed_total.labels(channel=channel).inc()
                    # Update DLQ size gauge
                    dlq_len = await self.redis_client.llen(dlq_key)
                    dlq_size.labels(channel=channel).set(dlq_len)

                    log.info(f"[DLQ] Replayed DLQ message: {message_id} to {channel_to_publish}")
                    return True

            log.warning(f"[DLQ] Message not found: {message_id}")
            return False

        except Exception as e:
            log.error(f"[DLQ] Failed to replay DLQ message: {e}")
            return False

    async def shutdown(self) -> None:
        """
        Cleanup resources and close connections

        Steps:
        1. Stop listener loop
        2. Unsubscribe from all patterns
        3. Close PubSub connection

        Note: redis_client is NOT closed (managed by global pool)
        """
        log.info("Shutting down PubSubBus...")

        self._running = False

        # Cancel listener task
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        # Unsubscribe all channels/patterns and close PubSub
        if self.pubsub:
            try:
                all_channels = list(self.subscriptions.keys())
                # Separate glob patterns (PSUBSCRIBE) from exact channels (SUBSCRIBE)
                patterns_to_unsub = [p for p in all_channels if '*' in p or '?' in p]
                channels_to_unsub = [p for p in all_channels if '*' not in p and '?' not in p]
                if patterns_to_unsub:
                    await self.pubsub.punsubscribe(*patterns_to_unsub)
                if channels_to_unsub:
                    await self.pubsub.unsubscribe(*channels_to_unsub)
                await self.pubsub.close()
                log.debug("Closed PubSub connection (returned to pool)")
            except Exception as e:
                log.error(f"Error closing pubsub: {e}")

        log.info(
            f"PubSubBus shut down - "
            f"Published: {self._messages_published}, "
            f"Received: {self._messages_received}, "
            f"Handler errors: {self._handler_errors}"
        )

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get PubSubBus metrics

        Returns:
            Dictionary with metrics
        """
        return {
            "running": self._running,
            "subscriptions": sum(len(h) for h in self.subscriptions.values()),
            "channels": len(self.subscriptions),
            "messages_published": self._messages_published,
            "messages_received": self._messages_received,
            "handler_errors": self._handler_errors,
            "redis_pool": "shared"  # Using global Redis pool
        }


# =============================================================================
# EOF
# =============================================================================
