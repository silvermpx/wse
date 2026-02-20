# =============================================================================
# WSE — WebSocket Engine
# =============================================================================

import asyncio
import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from .connection import ConnectionState

log = logging.getLogger("wse.handlers")

# Message priority constants (standalone, no external dep)
PRIORITY_CRITICAL = 10
PRIORITY_HIGH = 8
PRIORITY_NORMAL = 5
PRIORITY_LOW = 3
PRIORITY_BACKGROUND = 1


# =============================================================================
# Handler type alias
# =============================================================================

# A handler is an async callable that takes a message dict
HandlerFunc = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


# =============================================================================
# Subscription hook protocol
# =============================================================================


class SubscriptionHook:
    """Hook called when topics are subscribed / unsubscribed.

    Override ``on_subscribe`` and ``on_unsubscribe`` in a subclass, then
    register with ``WSEHandler.add_subscription_hook(hook)``.
    """

    async def on_subscribe(
        self,
        connection: Any,
        topics: list[str],
        success_topics: list[str],
        failed_topics: list[dict[str, Any]],
    ) -> None:
        """Called after successful subscribe action."""
        pass

    async def on_unsubscribe(
        self,
        connection: Any,
        topics: list[str],
        success_topics: list[str],
        failed_topics: list[dict[str, Any]],
    ) -> None:
        """Called after successful unsubscribe action."""
        pass


# =============================================================================
# WSEHandler
# =============================================================================


class WSEHandler:
    """Generic WebSocket message handler framework.

    Provides the protocol scaffolding (ping/pong, subscriptions, config,
    metrics, debug, encryption, batching) without any domain-specific logic.

    To add domain-specific behaviour:
        handler = WSEHandler(connection)
        handler.register("my_custom_event", my_async_handler)

    Or in bulk:
        handler.register_many({
            "order_request": handle_order,
            "market_subscribe": handle_market_subscribe,
        })
    """

    def __init__(
        self,
        connection,
        topic_normalizer: Callable[[str, str], str] | None = None,
        snapshot_provider: Any | None = None,
    ):
        """
        Args:
            connection: WSEConnection instance
            topic_normalizer: Optional callable(topic, user_id) -> normalized_topic.
                If None, topics are used as-is.
            snapshot_provider: Optional SnapshotProvider for delivering initial state.
        """
        self.connection = connection
        self._topic_normalizer = topic_normalizer
        self._snapshot_provider = snapshot_provider
        self._subscription_hooks: list[SubscriptionHook] = []

        # Core message type handlers
        self.handlers: dict[str, HandlerFunc] = {
            # Ping / Pong
            "ping": self.handle_ping,
            "PING": self.handle_ping,
            "pong": self.handle_pong,
            "PONG": self.handle_pong,
            # Connection management
            "client_hello": self.handle_client_hello,
            "connection_state_request": self.handle_connection_state_request,
            # Subscription management
            "subscription": self.handle_subscription,
            "subscription_update": self.handle_subscription,
            # Sync
            "sync_request": self.handle_sync_request,
            # Health and metrics
            "health_check": self.handle_health_check,
            "health_check_request": self.handle_health_check,
            "health_check_response": self.handle_health_check_response,
            "metrics_request": self.handle_metrics_request,
            "metrics_response": self.handle_metrics_response,
            # Configuration
            "config_update": self.handle_config_update,
            "config_request": self.handle_config_request,
            # Message handling
            "priority_message": self.handle_priority_message,
            "batch_message": self.handle_batch_message,
            # Debug and diagnostics
            "debug_handlers": self.handle_debug_request,
            "debug_request": self.handle_debug_request,
            "sequence_stats_request": self.handle_sequence_stats_request,
            # Security
            "encryption_request": self.handle_encryption_request,
            "key_rotation_request": self.handle_key_rotation_request,
        }

    # -----------------------------------------------------------------
    # Registration API
    # -----------------------------------------------------------------

    def register(self, message_type: str, handler: HandlerFunc) -> None:
        """Register a handler for a message type.

        Overwrites any existing handler for the same type.
        """
        self.handlers[message_type] = handler
        log.debug(f"Registered handler for message type: {message_type}")

    def register_many(self, handlers: dict[str, HandlerFunc]) -> None:
        """Bulk-register handlers."""
        self.handlers.update(handlers)
        log.debug(f"Registered {len(handlers)} handlers: {list(handlers.keys())}")

    def unregister(self, message_type: str) -> None:
        """Remove a handler for a message type."""
        self.handlers.pop(message_type, None)

    def add_subscription_hook(self, hook: SubscriptionHook) -> None:
        """Register a hook that fires on subscribe/unsubscribe events."""
        self._subscription_hooks.append(hook)

    # -----------------------------------------------------------------
    # Message routing
    # -----------------------------------------------------------------

    async def handle_message(self, message_data: dict[str, Any]) -> None:
        """Route message to appropriate handler with enhanced error handling"""
        if not message_data:
            return

        message_data = self._normalize_message_format(message_data)

        # Check for duplicate messages
        msg_id = message_data.get("id")
        if msg_id and self.connection.is_duplicate_message(msg_id):
            self.connection.metrics.messages_dropped += 1
            log.debug(f"Dropped duplicate message: {msg_id}")
            return

        msg_type = message_data.get("t") or message_data.get("type")
        log.debug(f"Handling message type: {msg_type}")

        handler = self.handlers.get(msg_type)
        if handler:
            try:
                await handler(message_data)
            except Exception as e:
                log.error(f"Error handling {msg_type}: {e}", exc_info=True)
                await self._send_error(
                    f"Error processing message type: {msg_type}", "HANDLER_ERROR", recoverable=True
                )
        else:
            log.warning(f"Unknown message type: {msg_type}")
            await self._publish_client_message(msg_type, message_data)

    def _normalize_message_format(self, message_data: dict[str, Any]) -> dict[str, Any]:
        """Normalize message format to use 't' and 'p' consistently"""
        if "type" in message_data and "t" not in message_data:
            message_data["t"] = message_data.pop("type")
        if "payload" in message_data and "p" not in message_data:
            message_data["p"] = message_data.pop("payload")
        if "p" not in message_data:
            message_data["p"] = {}
        return message_data

    # -----------------------------------------------------------------
    # Ping / Pong
    # -----------------------------------------------------------------

    async def handle_ping(self, message_data: Any) -> None:
        """Handle ping message with enhanced latency tracking"""
        timestamp = None

        if isinstance(message_data, str):
            if ":" in message_data:
                try:
                    timestamp = int(message_data.split(":", 1)[1])
                except Exception:
                    timestamp = int(time.time() * 1000)
        elif isinstance(message_data, dict):
            payload = message_data.get("p", {})
            timestamp = payload.get("timestamp") or payload.get("ts")

        if timestamp is None:
            timestamp = int(time.time() * 1000)

        server_ts = int(time.time() * 1000)
        await self.connection.send_message(
            {
                "t": "PONG",
                "p": {
                    "client_timestamp": timestamp,
                    "server_timestamp": server_ts,
                    "connection_id": self.connection.conn_id,
                },
            },
            priority=PRIORITY_CRITICAL,
        )

        latency = max(0, time.time() * 1000 - timestamp)
        self.connection.metrics.record_latency(latency)
        self.connection.network_analyzer.record_latency(latency)

    async def handle_pong(self, message_data: dict[str, Any]) -> None:
        """Handle pong response from client"""
        payload = message_data.get("p", {})
        if "server_timestamp" in payload:
            latency = datetime.now().timestamp() * 1000 - payload["server_timestamp"]
            self.connection.metrics.record_latency(latency)

    # -----------------------------------------------------------------
    # Connection management
    # -----------------------------------------------------------------

    async def handle_client_hello(self, message_data: dict[str, Any]) -> None:
        """Handle client hello message for protocol negotiation"""
        payload = message_data.get("p", {})

        client_version = payload.get("client_version", "unknown")
        protocol_version = payload.get("protocol_version", 2)
        features = payload.get("features", {})
        capabilities = payload.get("capabilities", [])

        self.connection.client_version = client_version
        self.connection.protocol_version = protocol_version
        self.connection.client_features = features
        self.connection.client_capabilities = capabilities

        log.info(f"Client hello received - Version: {client_version}, Protocol: {protocol_version}")
        log.debug(f"Client features: {features}")
        log.debug(f"Client capabilities: {capabilities}")

        await self.connection.send_message(
            {
                "t": "server_hello",
                "p": {
                    "server_version": self.connection.server_version,
                    "protocol_version": self.connection.protocol_version,
                    "supported_features": {
                        "compression": self.connection.compression_enabled,
                        "encryption": self.connection.encryption_enabled,
                        "batching": True,
                        "priority_queue": True,
                        "circuit_breaker": True,
                        "message_signing": self.connection.security_manager.message_signing_enabled,
                        "health_check": True,
                        "metrics": True,
                    },
                    "server_time": datetime.now(UTC).isoformat(),
                    "connection_id": self.connection.conn_id,
                    "message": self.connection.server_welcome_message,
                },
            },
            priority=PRIORITY_HIGH,
        )

    async def handle_connection_state_request(self, message_data: dict[str, Any]) -> None:
        """Handle connection state request"""
        state = getattr(self.connection, "connection_state", ConnectionState.CONNECTED)

        await self.connection.send_message(
            {
                "t": "connection_state_response",
                "p": {
                    "state": state.value if isinstance(state, Enum) else str(state),
                    "connection_id": self.connection.conn_id,
                    "connected_since": self.connection.metrics.connected_since.isoformat()
                    if self.connection.metrics.connected_since
                    else None,
                    "metrics": {
                        "messages_sent": self.connection.metrics.messages_sent,
                        "messages_received": self.connection.metrics.messages_received,
                        "compression_ratio": self.connection.metrics.compression_ratio,
                        "avg_latency": self.connection.metrics.avg_latency,
                    },
                },
            },
            priority=PRIORITY_HIGH,
        )

    # -----------------------------------------------------------------
    # Subscription management (generic)
    # -----------------------------------------------------------------

    async def handle_subscription(self, message_data: dict[str, Any]) -> None:
        """Handle subscription management with enhanced feedback"""
        payload = message_data.get("p", {})
        action = payload.get("action")
        topics = payload.get("topics", [])

        if not topics:
            await self._send_error("No topics specified", "INVALID_SUBSCRIPTION")
            return

        success_topics = []
        failed_topics = []
        already_subscribed = []

        if action == "subscribe":
            for topic in topics:
                if topic in self.connection.subscriptions:
                    already_subscribed.append(topic)
                    success_topics.append(topic)
                    continue

                self.connection.pending_subscriptions.add(topic)

                try:
                    normalized_topic = self._normalize_topic(topic)
                    log.debug(f"Subscribing to topic: {topic} (normalized: {normalized_topic})")

                    subscription_id = await self.connection.event_bus.subscribe(
                        pattern=normalized_topic,
                        handler=self._create_event_handler(),
                        connection_id=self.connection.conn_id,
                    )

                    self.connection.subscription_ids[topic] = subscription_id
                    self.connection.subscriptions.add(topic)
                    self.connection.pending_subscriptions.discard(topic)
                    self.connection.failed_subscriptions.discard(topic)
                    success_topics.append(topic)

                    log.info(
                        f"Subscribed {self.connection.conn_id} to {topic} (normalized: {normalized_topic})"
                    )

                except Exception as e:
                    log.error(f"Failed to subscribe to {topic}: {e}", exc_info=True)
                    self.connection.failed_subscriptions.add(topic)
                    self.connection.pending_subscriptions.discard(topic)
                    failed_topics.append({"topic": topic, "error": str(e)})

            # Notify subscription hooks
            for hook in self._subscription_hooks:
                try:
                    await hook.on_subscribe(self.connection, topics, success_topics, failed_topics)
                except Exception as e:
                    log.error(f"Subscription hook error: {e}", exc_info=True)

        elif action == "unsubscribe":
            for topic in topics:
                if topic not in self.connection.subscriptions:
                    success_topics.append(topic)
                    continue

                if topic in self.connection.subscription_ids:
                    try:
                        await self.connection.event_bus.unsubscribe(
                            self.connection.subscription_ids[topic]
                        )
                        del self.connection.subscription_ids[topic]
                        success_topics.append(topic)
                    except Exception as e:
                        log.error(f"Failed to unsubscribe from {topic}: {e}")
                        failed_topics.append({"topic": topic, "error": str(e)})

                self.connection.subscriptions.discard(topic)
                self.connection.failed_subscriptions.discard(topic)

            # Notify subscription hooks
            for hook in self._subscription_hooks:
                try:
                    await hook.on_unsubscribe(
                        self.connection, topics, success_topics, failed_topics
                    )
                except Exception as e:
                    log.error(f"Unsubscription hook error: {e}", exc_info=True)

        else:
            await self._send_error(f"Invalid subscription action: {action}", "INVALID_ACTION")
            return

        # Send subscription update
        await self.connection.send_message(
            {
                "t": "subscription_update",
                "p": {
                    "action": action,
                    "success": len(failed_topics) == 0,
                    "message": f"Successfully {action}d {len(success_topics)} topics",
                    "topics": list(self.connection.subscriptions),
                    "success_topics": success_topics,
                    "failed_topics": failed_topics,
                    "already_subscribed": already_subscribed,
                    "pending_subscriptions": list(self.connection.pending_subscriptions),
                    "failed_subscriptions": list(self.connection.failed_subscriptions),
                    "active_subscriptions": list(self.connection.subscriptions),
                    "subscription_limits": {
                        "max_topics": 100,
                        "current_topics": len(self.connection.subscriptions),
                    },
                    "timestamp": datetime.now(UTC).isoformat(),
                },
            },
            priority=PRIORITY_NORMAL,
        )

    # -----------------------------------------------------------------
    # Sync request (generic)
    # -----------------------------------------------------------------

    async def handle_sync_request(self, message_data: dict[str, Any]) -> None:
        """Handle sync request for initial data.

        The generic handler sends a snapshot_complete message.  Domain-specific
        sync logic should be added via subscription hooks or by overriding
        this method.
        """
        payload = message_data.get("p", {})
        topics = payload.get("topics", [])
        last_sequence = payload.get("last_sequence", 0)

        log.info(f"Sync request received - User: {self.connection.user_id}, Topics: {topics}")

        # Send snapshot complete
        await self.connection.send_message(
            {
                "t": "snapshot_complete",
                "p": {
                    "message": "Initial sync complete",
                    "success": True,
                    "snapshots_sent": [],
                    "details": {
                        "sequence": self.connection.sequence_number,
                        "topics": topics,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "last_sequence_processed": last_sequence,
                        "connection_id": self.connection.conn_id,
                    },
                },
            }
        )

        if hasattr(self.connection, "mark_initial_sync_complete"):
            await self.connection.mark_initial_sync_complete()

        log.info("Sync request completed")

    # -----------------------------------------------------------------
    # Batch handling
    # -----------------------------------------------------------------

    async def handle_batch_message(self, message_data: dict[str, Any]) -> None:
        """Handle batch of messages from client"""
        payload = message_data.get("p", {})
        messages = payload.get("messages", [])

        results = []
        for idx, msg in enumerate(messages):
            try:
                msg_type = msg.get("t") or msg.get("type")
                if msg_type == "batch_message":
                    results.append(
                        {"index": idx, "success": False, "error": "Nested batches not allowed"}
                    )
                    continue

                msg = self._normalize_message_format(msg)
                await self.handle_message(msg)
                results.append({"index": idx, "success": True})
            except Exception as e:
                log.error(f"Error processing batch message {idx}: {e}")
                results.append({"index": idx, "success": False, "error": str(e)})

        await self.connection.send_message(
            {
                "t": "batch_message_result",
                "p": {
                    "total": len(messages),
                    "successful": sum(1 for r in results if r["success"]),
                    "failed": sum(1 for r in results if not r["success"]),
                    "results": results,
                },
            },
            priority=PRIORITY_NORMAL,
        )

    # -----------------------------------------------------------------
    # Health and metrics
    # -----------------------------------------------------------------

    async def handle_health_check(self, message_data: dict[str, Any]) -> None:
        """Handle health check request with comprehensive diagnostics"""
        diagnostics = self.connection.network_analyzer.analyze()
        cb_metrics = self.connection.circuit_breaker.get_metrics()
        sequence_stats = await self.connection.event_sequencer.get_buffer_stats()

        await self.connection.send_message(
            {
                "t": "health_check_response",
                "p": {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "server_time": int(datetime.now().timestamp() * 1000),
                    "connection_id": self.connection.conn_id,
                    "status": "healthy",
                    "diagnostics": {
                        "connection_quality": diagnostics["quality"],
                        "network_jitter": diagnostics["jitter"],
                        "packet_loss": diagnostics["packet_loss"],
                        "avg_latency": self.connection.metrics.avg_latency,
                        "suggestions": diagnostics["suggestions"],
                    },
                    "circuit_breaker": {
                        "state": cb_metrics["state"],
                        "failures": cb_metrics["failure_count"],
                        "success_count": cb_metrics.get("success_count", 0),
                        "last_failure_time": cb_metrics.get("last_failure_time"),
                    },
                    "queue_stats": self.connection.message_queue.get_stats(),
                    "sequence_stats": sequence_stats,
                    "subscriptions": {
                        "active": len(self.connection.subscriptions),
                        "pending": len(self.connection.pending_subscriptions),
                        "failed": len(self.connection.failed_subscriptions),
                    },
                    "client_info": {
                        "version": self.connection.client_version,
                        "protocol": self.connection.protocol_version,
                        "features": self.connection.client_features,
                    },
                },
            },
            priority=PRIORITY_HIGH,
        )

        self.connection.metrics.last_health_check = datetime.now(UTC)

    async def handle_health_check_response(self, message_data: dict[str, Any]) -> None:
        """Process health check response from client with quality assessment"""
        payload = message_data.get("p", {})

        if "stats" in payload:
            log.info(f"Client stats for {self.connection.conn_id}: {payload['stats']}")

        if "diagnostics" in payload:
            diagnostics = payload["diagnostics"]
            quality = diagnostics.get("connectionQuality", diagnostics.get("quality"))

            if quality in ["poor", "fair"]:
                recommendations = self._get_quality_recommendations(quality, diagnostics)

                await self.connection.send_message(
                    {
                        "t": "connection_quality",
                        "p": {
                            "quality": quality,
                            "message": "Connection quality could be improved",
                            "suggestions": recommendations["suggestions"],
                            "recommended_settings": recommendations["settings"],
                            "diagnostics": diagnostics,
                        },
                    },
                    priority=PRIORITY_HIGH,
                )

    def _get_quality_recommendations(
        self, quality: str, diagnostics: dict[str, Any]
    ) -> dict[str, Any]:
        """Generate quality improvement recommendations"""
        suggestions = []
        settings = {"compression": True, "batch_size": 20, "batch_timeout": 200}

        jitter = diagnostics.get("jitter", 0)
        packet_loss = diagnostics.get("packetLoss", 0)
        latency = diagnostics.get("avgLatency", 0)

        if jitter > 50:
            suggestions.append("High jitter detected. Consider using a wired connection.")
            settings["batch_size"] = 30
            settings["batch_timeout"] = 300

        if packet_loss > 1:
            suggestions.append("Packet loss detected. Check network congestion.")
            settings["compression"] = True

        if latency > 200:
            suggestions.append("High latency detected. Consider connecting to a closer server.")
            settings["batch_size"] = 50

        if not suggestions:
            suggestions.append("Consider closing bandwidth-intensive applications.")

        return {"suggestions": suggestions, "settings": settings}

    async def handle_metrics_request(self, message_data: dict[str, Any]) -> None:
        """Handle metrics request with comprehensive stats"""
        event_bus_stats = self.connection.event_bus.get_metrics()
        sequence_stats = await self.connection.event_sequencer.get_buffer_stats()

        await self.connection.send_message(
            {
                "t": "metrics_response",
                "p": {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "connection_id": self.connection.conn_id,
                    "connection_stats": self.connection.metrics.to_dict(),
                    "subscriptions": {
                        "active": list(self.connection.subscriptions),
                        "pending": list(self.connection.pending_subscriptions),
                        "failed": list(self.connection.failed_subscriptions),
                        "count": len(self.connection.subscriptions),
                    },
                    "event_bus_stats": event_bus_stats,
                    "queue_stats": self.connection.message_queue.get_stats(),
                    "circuit_breaker": self.connection.circuit_breaker.get_metrics(),
                    "diagnostics": self.connection.network_analyzer.analyze(),
                    "sequence_stats": sequence_stats,
                    "compression_stats": self.connection.compression_manager.get_stats(),
                    "security": self.connection.security_manager.get_security_info(),
                    "uptime": (
                        datetime.now(UTC) - self.connection.metrics.connected_since
                    ).total_seconds()
                    if self.connection.metrics.connected_since
                    else 0,
                },
            }
        )

    async def handle_metrics_response(self, message_data: dict[str, Any]) -> None:
        """Handle metrics response from client"""
        payload = message_data.get("p", {})

        if "queue_stats" in payload:
            queue_stats = payload["queue_stats"]
            if queue_stats.get("backpressure"):
                log.warning(f"Client {self.connection.conn_id} experiencing backpressure")

        if "offline_queue" in payload:
            offline = payload["offline_queue"]
            if offline.get("size", 0) > 0:
                log.info(f"Client {self.connection.conn_id} has {offline['size']} offline messages")

    # -----------------------------------------------------------------
    # Configuration
    # -----------------------------------------------------------------

    async def handle_config_request(self, message_data: dict[str, Any]) -> None:
        """Handle configuration request"""
        await self.connection.send_message(
            {
                "t": "config_response",
                "p": {
                    "compression_enabled": self.connection.compression_enabled,
                    "compression_threshold": self.connection.compression_threshold,
                    "encryption_enabled": self.connection.encryption_enabled,
                    "batch_size": self.connection.batch_size,
                    "batch_timeout": self.connection.batch_timeout,
                    "max_queue_size": self.connection.message_queue.max_size,
                    "protocol_version": self.connection.protocol_version,
                    "heartbeat_interval": 15000,
                    "idle_timeout": 40000,
                    "rate_limits": {
                        "capacity": self.connection.rate_limiter.config.capacity,
                        "refill_rate": self.connection.rate_limiter.config.refill_rate,
                    },
                },
            },
            priority=PRIORITY_HIGH,
        )

    async def handle_config_update(self, message_data: dict[str, Any]) -> None:
        """Handle configuration update request with validation"""
        payload = message_data.get("p", {})
        updated_settings = {}

        if "compression_enabled" in payload:
            self.connection.compression_enabled = payload["compression_enabled"]
            updated_settings["compression_enabled"] = self.connection.compression_enabled

        if "compression_threshold" in payload:
            threshold = payload["compression_threshold"]
            if 512 <= threshold <= 10240:
                self.connection.compression_threshold = threshold
                updated_settings["compression_threshold"] = threshold

        if "batching_enabled" in payload:
            if payload["batching_enabled"]:
                batch_size = payload.get("batch_size", 10)
                batch_timeout = payload.get("batch_timeout", 0.1)
                self.connection.batch_size = max(1, min(100, int(batch_size)))
                self.connection.batch_timeout = max(0.01, min(5.0, float(batch_timeout)))
            else:
                self.connection.batch_size = 1
            updated_settings["batch_size"] = self.connection.batch_size
            updated_settings["batch_timeout"] = self.connection.batch_timeout

        if "max_queue_size" in payload:
            max_size = payload["max_queue_size"]
            if 100 <= max_size <= 5000:
                self.connection.message_queue.max_size = max_size
                updated_settings["max_queue_size"] = max_size

        log.info(f"Configuration updated for {self.connection.conn_id}: {updated_settings}")

        await self.connection.send_message(
            {
                "t": "config_update_response",
                "p": {
                    "success": True,
                    "updated_settings": updated_settings,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
            },
            priority=PRIORITY_HIGH,
        )

    # -----------------------------------------------------------------
    # Priority message
    # -----------------------------------------------------------------

    async def handle_priority_message(self, message_data: dict[str, Any]) -> None:
        """Handle priority message from client"""
        payload = message_data.get("p", {})
        priority = message_data.get("pri", message_data.get("priority", PRIORITY_NORMAL))

        valid_priorities = [1, 3, 5, 8, 10]
        if priority not in valid_priorities:
            priority = min(valid_priorities, key=lambda x: abs(x - priority))

        await self.connection.event_bus.publish(
            f"priority:{self.connection.user_id}:messages",
            {
                "content": payload.get("content", payload),
                "priority": priority,
                "ttl": payload.get("ttl"),
                "user_id": self.connection.user_id,
                "conn_id": self.connection.conn_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "correlation_id": message_data.get("cid"),
            },
        )

    # -----------------------------------------------------------------
    # Debug / diagnostics
    # -----------------------------------------------------------------

    async def handle_debug_request(self, message_data: dict[str, Any]) -> None:
        """Handle debug information request"""
        debug_info = {
            "connection_id": self.connection.conn_id,
            "user_id": self.connection.user_id,
            "registered_handlers": list(self.handlers.keys()),
            "subscriptions": {
                "active": list(self.connection.subscriptions),
                "pending": list(self.connection.pending_subscriptions),
                "failed": list(self.connection.failed_subscriptions),
                "subscription_ids": list(self.connection.subscription_ids.keys()),
            },
            "connection_state": getattr(
                self.connection, "connection_state", ConnectionState.CONNECTED
            ).value
            if hasattr(self.connection, "connection_state")
            else "connected",
            "metrics": self.connection.metrics.to_dict(),
            "compression": {
                "enabled": self.connection.compression_enabled,
                "threshold": self.connection.compression_threshold,
                "stats": self.connection.compression_manager.get_stats(),
            },
            "encryption": {
                "enabled": self.connection.encryption_enabled,
            },
            "sequence": {
                "current": self.connection.sequence_number,
                "seen_messages": len(self.connection.seen_message_ids),
            },
            "protocol_version": self.connection.protocol_version,
            "client_version": self.connection.client_version,
            "client_features": self.connection.client_features,
            "queue_stats": self.connection.message_queue.get_stats(),
        }

        await self.connection.send_message(
            {"t": "debug_response", "p": debug_info}, priority=PRIORITY_HIGH
        )

    async def handle_sequence_stats_request(self, message_data: dict[str, Any]) -> None:
        """Handle sequence statistics request"""
        stats = await self.connection.event_sequencer.get_buffer_stats()

        await self.connection.send_message(
            {"t": "sequence_stats_response", "p": stats}, priority=PRIORITY_HIGH
        )

    # -----------------------------------------------------------------
    # Security / encryption
    # -----------------------------------------------------------------

    async def handle_encryption_request(self, message_data: dict[str, Any]) -> None:
        """Handle encryption setup request"""
        payload = message_data.get("p", {})
        action = payload.get("action", "status")

        if action == "enable":
            if not self.connection.encryption_enabled:
                await self.connection.security_manager.initialize(
                    {
                        "encryption_enabled": True,
                        "message_signing_enabled": payload.get("signing", False),
                    }
                )
                self.connection.encryption_enabled = True

            public_key = payload.get("public_key")
            if public_key:
                await self.connection.security_manager.set_server_public_key(public_key)

            server_key = await self.connection.security_manager.get_public_key()

            await self.connection.send_message(
                {
                    "t": "encryption_response",
                    "p": {
                        "enabled": True,
                        "public_key": server_key,
                        "algorithms": {
                            "encryption": "AES-GCM-256",
                            "signing": "HMAC-SHA256",
                            "key_exchange": "ECDH-P256",
                        },
                    },
                },
                priority=PRIORITY_HIGH,
            )

        elif action == "disable":
            self.connection.encryption_enabled = False
            await self.connection.send_message(
                {"t": "encryption_response", "p": {"enabled": False}}, priority=PRIORITY_HIGH
            )

        else:  # status
            info = self.connection.security_manager.get_security_info()
            await self.connection.send_message(
                {"t": "encryption_response", "p": info}, priority=PRIORITY_HIGH
            )

    async def handle_key_rotation_request(self, message_data: dict[str, Any]) -> None:
        """Handle key rotation request"""
        if self.connection.encryption_enabled:
            await self.connection.security_manager.rotate_keys()
            new_key = await self.connection.security_manager.get_public_key()

            await self.connection.send_message(
                {
                    "t": "key_rotation_response",
                    "p": {
                        "success": True,
                        "new_public_key": new_key,
                        "timestamp": datetime.now(UTC).isoformat(),
                    },
                },
                priority=PRIORITY_HIGH,
            )
        else:
            await self._send_error("Encryption not enabled", "ENCRYPTION_REQUIRED")

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _normalize_topic(self, topic: str) -> str:
        """Normalize topic name for event bus.

        Uses the user-provided topic_normalizer if set, otherwise returns
        the topic as-is.
        """
        if self._topic_normalizer:
            return self._topic_normalizer(topic, self.connection.user_id)
        return topic

    def _create_event_handler(self):
        """Create an event handler that sends events to WebSocket"""
        # Track events by message ID to prevent duplicates
        processed_event_ids: OrderedDict = OrderedDict()

        async def handler(event: dict[str, Any]) -> None:
            """Handle events from the event bus and forward to WebSocket"""
            try:
                if not self.connection._running:
                    return

                event_type = event.get("event_type", event.get("type", event.get("t", "unknown")))
                event_id = event.get("event_id", event.get("_metadata", {}).get("event_id"))

                log.debug(f"[HANDLER_START] Processing event: {event_type}, id: {event_id}")

                # Deduplicate
                if event_id and event_id in processed_event_ids:
                    log.debug(f"Skipping already processed event {event_id}")
                    return

                if event_id:
                    processed_event_ids[event_id] = True
                    while len(processed_event_ids) > 5000:
                        processed_event_ids.popitem(last=False)

                # If already in WebSocket format, send as-is
                if all(key in event for key in ["v", "t", "p", "id", "ts"]):
                    log.debug(f"[HANDLER_SENDING] {event.get('t')}")
                    await self.connection.send_message(event)
                else:
                    # Raw event -- try to use the connection's transformer if available
                    # Otherwise send with minimal wrapping
                    seq = self.connection.get_next_sequence()
                    transformed = {
                        "v": 2,
                        "id": event.get(
                            "event_id", event.get("id", str(asyncio.get_event_loop().time()))
                        ),
                        "t": event_type.lower(),
                        "ts": event.get("timestamp", datetime.now(UTC).isoformat()),
                        "seq": seq,
                        "p": event.get("payload", event),
                    }
                    log.debug(f"Wrapped {event_type} -> {transformed.get('t')}")
                    await self.connection.send_message(transformed)

                log.debug(f"Sent {event_type} to {self.connection.conn_id}")

            except Exception as e:
                log.error(f"Error handling event for {self.connection.conn_id}: {e}", exc_info=True)

        return handler

    async def _send_error(
        self, message: str, code: str, details: str | None = None, recoverable: bool = True
    ) -> None:
        """Send an error message to client with standard format"""
        error_message = {
            "t": "error",
            "p": {
                "message": message,
                "code": code,
                "details": details,
                "recoverable": recoverable,
                "timestamp": datetime.now(UTC).isoformat(),
                "connection_id": self.connection.conn_id,
                "severity": "error" if not recoverable else "warning",
            },
        }

        log.error(f"Sending error to client: {code} - {message}")
        await self.connection.send_message(error_message, priority=PRIORITY_HIGH)

    async def _publish_client_message(self, msg_type: str, message_data: dict[str, Any]) -> None:
        """Publish unknown message types to event bus for custom handling"""
        await self.connection.event_bus.publish(
            f"client:{self.connection.user_id}:messages",
            {
                "type": msg_type,
                "payload": message_data.get("p", {}),
                "user_id": self.connection.user_id,
                "conn_id": self.connection.conn_id,
                "timestamp": datetime.now(UTC).isoformat(),
                "raw_message": message_data,
                "client_version": self.connection.client_version,
                "protocol_version": self.connection.protocol_version,
            },
        )
