# =============================================================================
# WSE — WebSocket Engine
# =============================================================================

import asyncio
import json
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol, runtime_checkable

try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False

import contextlib

from ..metrics.connection_metrics import (
    ConnectionMetrics,
    NetworkQualityAnalyzer,
    ws_bytes_received_total,
    ws_bytes_sent_total,
    ws_connections_active,
    ws_connections_total,
    ws_disconnections_total,
    ws_messages_received_total,
    ws_queue_backpressure,
    ws_queue_dropped_total,
    ws_queue_size,
)
from ..reliability.circuit_breaker import CircuitBreaker
from ..reliability.config import CircuitBreakerConfig, RateLimiterConfig
from ..reliability.rate_limiter import RateLimiter
from .compression import CompressionManager
from .queue import PriorityMessageQueue
from .security import SecurityManager
from .sequencer import EventSequencer

log = logging.getLogger("wse.connection")

# Check if Prometheus metrics are available
try:
    from prometheus_client import Counter  # noqa: F401

    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

# Constants matching frontend
HEARTBEAT_INTERVAL = 15  # seconds
IDLE_TIMEOUT = 90  # seconds (6x heartbeat interval, tolerates 5 missed pings)
HEALTH_CHECK_INTERVAL = 30  # seconds
METRICS_INTERVAL = 60  # seconds
COMPRESSION_THRESHOLD = 1024  # bytes
MAX_MESSAGE_SIZE = 1024 * 1024  # 1MB - reject messages larger than this

# Event types that should never be filtered during startup grace period (O(1) lookup)
_NEVER_FILTER_TYPES = frozenset(
    {
        "server_ready",
        "subscription_update",
        "error",
        "connection_state_change",
        "server_hello",
        "health_check_response",
        "heartbeat",
        "PONG",
        "snapshot_complete",
    }
)


# =============================================================================
# WebSocket Protocol (framework-agnostic)
# =============================================================================


@runtime_checkable
class WebSocketProtocol(Protocol):
    """Protocol for WebSocket implementations (FastAPI, aiohttp, etc.)"""

    async def send_text(self, data: str) -> None: ...
    async def send_bytes(self, data: bytes) -> None: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...

    @property
    def client_state(self) -> Any:
        """Return current connection state. Should be comparable to 'CONNECTED'."""
        ...


class WebSocketState(Enum):
    """WebSocket connection states (mirrors Starlette for compatibility)"""

    CONNECTING = 0
    CONNECTED = 1
    DISCONNECTED = 2


class ConnectionState(Enum):
    """WebSocket connection states matching frontend"""

    PENDING = "pending"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    DEGRADED = "degraded"


# =============================================================================
# Event Bus Protocol (pluggable pub/sub)
# =============================================================================


@runtime_checkable
class EventBusProtocol(Protocol):
    """Protocol for the event bus (PubSubBus or any compatible implementation)."""

    async def subscribe(
        self, pattern: str, handler: Any, connection_id: str = "", **kwargs
    ) -> str: ...

    async def unsubscribe(self, subscription_id: str) -> None: ...

    async def unsubscribe_connection(self, connection_id: str) -> int: ...

    async def publish(self, topic: str, event: dict[str, Any], **kwargs) -> None: ...

    def get_metrics(self) -> dict[str, Any]: ...


# =============================================================================
# JSON Serialization
# =============================================================================


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects and other types."""

    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, (Decimal, uuid.UUID)):
            return str(obj)
        elif isinstance(obj, Enum):
            return obj.value
        elif hasattr(obj, "__dict__"):
            try:
                obj_dict = {}
                for key, value in obj.__dict__.items():
                    if key.startswith("_"):
                        continue
                    if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                        obj_dict[key] = value
                    elif isinstance(value, (datetime, date, Decimal, uuid.UUID, Enum)):
                        obj_dict[key] = self.default(value)
                return obj_dict
            except (TypeError, ValueError):
                return str(obj)
        return super().default(obj)


def _orjson_default(obj):
    """orjson default handler for types not natively supported."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


def _json_dumps(data: dict[str, Any]) -> str:
    """Serialize dict to JSON string using orjson if available, stdlib fallback."""
    if ORJSON_AVAILABLE:
        return orjson.dumps(data, default=_orjson_default).decode()
    return json.dumps(data, cls=DateTimeEncoder)


# =============================================================================
# Signed message types (configurable)
# =============================================================================

# Default set of message types that should be signed for integrity.
# Empty by default -- configure via WSEConnection constructor for your domain.
DEFAULT_SIGNED_MESSAGE_TYPES: frozenset[str] = frozenset()


# =============================================================================
# WSEConnection
# =============================================================================


@dataclass
class WSEConnection:
    """Manages a single WebSocket connection with all features and frontend compatibility"""

    conn_id: str
    user_id: str
    ws: Any  # WebSocketProtocol -- typed as Any to avoid dataclass Protocol issues
    event_bus: Any  # EventBusProtocol

    # Configuration
    protocol_version: int = 1
    compression_enabled: bool = True
    encryption_enabled: bool = False
    message_signing_enabled: bool = False
    batch_size: int = 10
    batch_timeout: float = 0.1
    compression_threshold: int = COMPRESSION_THRESHOLD
    signed_message_types: frozenset = field(default_factory=lambda: DEFAULT_SIGNED_MESSAGE_TYPES)

    # Server info (for server_hello)
    server_version: str = "1.1.1"
    server_welcome_message: str = "Welcome to WSE"

    # Connection state
    connection_state: ConnectionState = field(default=ConnectionState.PENDING)

    # Components (initialized in __post_init__)
    metrics: ConnectionMetrics = field(default_factory=ConnectionMetrics)
    circuit_breaker: CircuitBreaker = field(init=False)
    rate_limiter: RateLimiter = field(init=False)
    message_queue: PriorityMessageQueue = field(
        default_factory=lambda: PriorityMessageQueue(batch_size=10)
    )
    compression_manager: CompressionManager = field(default_factory=CompressionManager)
    security_manager: SecurityManager = field(default_factory=SecurityManager)
    network_analyzer: NetworkQualityAnalyzer = field(default_factory=NetworkQualityAnalyzer)
    event_sequencer: EventSequencer = field(default_factory=EventSequencer)

    # State
    subscriptions: set[str] = field(default_factory=set)
    subscription_ids: dict[str, str] = field(default_factory=dict)
    pending_subscriptions: set[str] = field(default_factory=set)
    failed_subscriptions: set[str] = field(default_factory=set)

    # Client information
    client_features: dict[str, bool] = field(default_factory=dict)
    client_version: str = "unknown"
    client_capabilities: list[str] = field(default_factory=list)

    # Sequencing
    sequence_number: int = 0
    seen_message_ids: deque = field(default_factory=lambda: deque(maxlen=1000))
    _seen_ids_set: set = field(default_factory=set)  # O(1) dedup lookup

    # Control
    _running: bool = True
    _tasks: set[asyncio.Task] = field(default_factory=set)
    _last_activity: float = field(default_factory=time.time)

    # Debug mode
    debug_mode: bool = field(default=False)
    event_count: dict[str, int] = field(default_factory=dict)

    # Startup filtering
    initial_sync_complete: bool = field(default=False)
    startup_grace_period: float = field(default=10.0)
    connection_start_time: float = field(default_factory=time.time)

    # Cleanup hook -- users can register an async callable that runs on cleanup
    _cleanup_hooks: list[Any] = field(default_factory=list)

    # Sender loop wakeup event
    _queue_event: asyncio.Event | None = field(default=None)

    def __post_init__(self):
        """Initialize components that need configuration"""
        cb_config = CircuitBreakerConfig(
            name=f"wse_{self.conn_id}",
            failure_threshold=10,
            success_threshold=3,
            reset_timeout_seconds=30,
            half_open_max_calls=5,
            window_size=50,
            failure_rate_threshold=0.3,
        )
        self.circuit_breaker = CircuitBreaker(cb_config)

        rl_config = RateLimiterConfig(
            algorithm="token_bucket",
            capacity=1000,
            refill_rate=100.0,
        )
        self.rate_limiter = RateLimiter(name=f"wse_{self.conn_id}", config=rl_config)

    def add_cleanup_hook(self, hook) -> None:
        """Register an async callable to run during cleanup."""
        self._cleanup_hooks.append(hook)

    async def initialize(self) -> None:
        """Initialize the connection with state management"""
        await self.set_state(ConnectionState.CONNECTING)

        self.metrics.connected_since = datetime.now(UTC)
        self.metrics.connection_attempts += 1
        self.metrics.successful_connections += 1

        if METRICS_AVAILABLE:
            ws_connections_total.labels(user_id=self.user_id).inc()
            ws_connections_active.inc()

        await self.security_manager.initialize(
            {
                "encryption_enabled": self.encryption_enabled,
                "message_signing_enabled": True,  # Always enable for selective signing capability
            }
        )

        self._queue_event = asyncio.Event()
        self._start_background_tasks()
        self._reset_idle_timer()

        await self.set_state(ConnectionState.CONNECTED)

        if log.isEnabledFor(logging.DEBUG):
            self.debug_mode = True

        log.info(f"WebSocket connection {self.conn_id} initialized for user {self.user_id}")

    async def set_state(self, state: ConnectionState) -> None:
        """Update connection state and notify client"""
        old_state = self.connection_state
        self.connection_state = state

        if old_state != state:
            log.info(f"Connection {self.conn_id} state changed: {old_state.value} -> {state.value}")

            initial_states = [ConnectionState.PENDING, ConnectionState.CONNECTING]
            if old_state in initial_states and state == ConnectionState.CONNECTED:
                log.debug("Skipping state change notification during initial connection")
                return

            await self.send_message(
                {
                    "t": "connection_state_change",
                    "p": {
                        "old_state": old_state.value,
                        "new_state": state.value,
                        "timestamp": datetime.now(UTC).isoformat(),
                        "connection_id": self.conn_id,
                    },
                },
                priority=10,
            )

    async def mark_initial_sync_complete(self) -> None:
        """Mark that initial sync is complete to stop filtering old events"""
        self.initial_sync_complete = True
        log.info(f"Initial sync marked complete for connection {self.conn_id}")

    def _start_background_tasks(self) -> None:
        """Start all background tasks"""
        tasks = [
            self._sender_loop(),
            self._heartbeat_loop(),
            self._health_check_loop(),
            self._metrics_collection_loop(),
            self._sequence_cleanup_loop(),
        ]

        if self.debug_mode:
            tasks.append(self._debug_stats_loop())

        for task_coro in tasks:
            task = asyncio.create_task(task_coro)
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def get_next_sequence(self) -> int:
        """Get the next sequence number"""
        self.sequence_number += 1
        return self.sequence_number

    def is_duplicate_message(self, message_id: str) -> bool:
        """Check if a message is duplicate (O(1) set lookup)"""
        if message_id in self._seen_ids_set:
            return True
        self._seen_ids_set.add(message_id)
        self.seen_message_ids.append(message_id)
        # Keep set in sync when deque evicts oldest
        if len(self._seen_ids_set) > len(self.seen_message_ids):
            self._seen_ids_set = set(self.seen_message_ids)
        return False

    def _is_ws_connected(self) -> bool:
        """Check if the underlying WebSocket is still connected."""
        state = getattr(self.ws, "client_state", None)
        if state is None:
            return True  # Assume connected if state not available
        # Support Starlette WebSocketState enum or our own
        if isinstance(state, Enum):
            return state.value == WebSocketState.CONNECTED.value or state.name == "CONNECTED"
        return state == WebSocketState.CONNECTED

    async def send_message(
        self, message: dict[str, Any], priority: int = 5, _bypass_rate_limit: bool = False
    ) -> bool:
        """Queue a message for sending with format normalization"""
        if not self._running:
            return False

        # Startup filtering: skip stale events during grace period
        if not self.initial_sync_complete:
            if time.time() - self.connection_start_time < self.startup_grace_period:
                event_type = message.get("t", "")
                if event_type and event_type not in _NEVER_FILTER_TYPES:
                    msg_ts = message.get("_wse_received_at")
                    if msg_ts and (time.time() - msg_ts) > 10:
                        return False

        message["_wse_received_at"] = time.time()

        message_type = message.get("t") or message.get("type", "unknown")
        message_id = message.get("id", "unknown")
        log.debug(f"[WSE_QUEUE] Event: {message_type}, ID: {message_id}, Priority: {priority}")

        # Normalize message format for frontend compatibility
        if "type" in message and "t" not in message:
            message["t"] = message.pop("type")

        if "payload" in message and "p" not in message:
            message["p"] = message.pop("payload")

        # Check rate limit
        if not _bypass_rate_limit and not await self.rate_limiter.acquire():
            self.metrics.messages_dropped += 1
            await self._send_rate_limit_warning()
            return False

        # Add message ID if not present
        if "id" not in message:
            message["id"] = str(uuid.uuid4())

        if "seq" not in message:
            message["seq"] = self.get_next_sequence()

        if "v" not in message:
            message["v"] = self.protocol_version
        if "ts" not in message:
            message["ts"] = datetime.now(UTC).isoformat()

        if self.debug_mode:
            event_type = message.get("t", "unknown")
            self.event_count[event_type] = self.event_count.get(event_type, 0) + 1

        result = await self.message_queue.enqueue(message, priority)
        # Wake sender loop immediately
        if result and self._queue_event:
            self._queue_event.set()
        return result

    async def handle_incoming(self, data: str | bytes) -> dict[str, Any] | None:
        """Handle incoming WebSocket message and return parsed data"""
        self._last_activity = time.time()

        message_size = len(data)  # Works for both bytes and str (ASCII JSON)
        if message_size > MAX_MESSAGE_SIZE:
            log.warning(
                f"Message size {message_size} exceeds limit {MAX_MESSAGE_SIZE} bytes, rejecting"
            )
            self.metrics.protocol_errors += 1
            await self.send_message(
                {
                    "t": "error",
                    "p": {
                        "code": "MESSAGE_TOO_LARGE",
                        "message": f"Message size {message_size} exceeds maximum allowed size of {MAX_MESSAGE_SIZE} bytes",
                        "max_size": MAX_MESSAGE_SIZE,
                    },
                },
                priority=10,
            )
            return None

        self.metrics.messages_received += 1
        self.metrics.last_message_received = datetime.now(UTC)

        if isinstance(data, bytes):
            byte_count = len(data)
            self.metrics.bytes_received += byte_count
            self.network_analyzer.record_bytes(byte_count)

            if METRICS_AVAILABLE:
                ws_messages_received_total.labels(message_type="binary").inc()
                ws_bytes_received_total.inc(byte_count)

            return await self._parse_binary_message(data)
        else:
            byte_count = len(data)  # ASCII JSON: len(str) == byte count
            self.metrics.bytes_received += byte_count
            self.network_analyzer.record_bytes(byte_count)

            if METRICS_AVAILABLE:
                ws_messages_received_total.labels(message_type="text").inc()
                ws_bytes_received_total.inc(byte_count)

            return self._parse_text_message(data)

    def _parse_text_message(self, data: str) -> dict[str, Any] | None:
        """Parse text message with special handling"""
        _prefix = data[:9].upper()
        if _prefix.startswith("WSE:PING") or _prefix.startswith("PING"):
            self.network_analyzer.record_packet_received()
            return {"type": "ping", "raw": data}

        if data.startswith("WSE:PONG:") or data.startswith("PONG:"):
            try:
                self.network_analyzer.record_packet_received()
                timestamp = int(data.split(":")[-1])
                latency = int(datetime.now().timestamp() * 1000) - timestamp
                self.metrics.record_latency(latency)
                self.network_analyzer.record_latency(latency)
                log.debug(f"PONG received (text), latency: {latency}ms")
            except Exception:
                pass
            return None

        try:
            json_data = data
            if data.startswith("WSE{"):
                json_data = data[3:]
            elif data.startswith("S{") or data.startswith("U{"):
                json_data = data[1:]

            parsed = orjson.loads(json_data) if ORJSON_AVAILABLE else json.loads(json_data)

            if "type" in parsed and "t" not in parsed:
                parsed["t"] = parsed.pop("type")
            if "payload" in parsed and "p" not in parsed:
                parsed["p"] = parsed.pop("payload")
            return parsed
        except (json.JSONDecodeError, Exception):
            self.metrics.protocol_errors += 1
            log.warning(f"Invalid JSON received: {data[:100]}")
            return None

    async def _parse_binary_message(self, data: bytes) -> dict[str, Any] | None:
        """Parse binary message with compression and encryption support"""
        try:
            log.debug(f"Parsing binary message: {len(data)} bytes, first 10: {data[:10].hex()}")

            if data.startswith(b"C:"):
                decompressed = self.compression_manager.decompress(data[2:])
                self.metrics.compression_hits += 1
                if ORJSON_AVAILABLE:
                    parsed = orjson.loads(decompressed)
                else:
                    parsed = json.loads(decompressed)
                log.debug(
                    f"Decompressed C: prefixed: {len(data)} -> {len(decompressed)} bytes, type: {parsed.get('t')}"
                )
                return self._normalize_message_format(parsed)

            elif data.startswith(b"M:"):
                parsed = self.compression_manager.unpack_msgpack(data[2:])
                return self._normalize_message_format(parsed)

            elif data.startswith(b"E:") and self.encryption_enabled:
                decrypted = await self.security_manager.decrypt_message(data[2:])
                if decrypted:
                    parsed = orjson.loads(decrypted) if ORJSON_AVAILABLE else json.loads(decrypted)
                    return self._normalize_message_format(parsed)
                else:
                    self.metrics.protocol_errors += 1
                    return None

            else:
                parsed = orjson.loads(data) if ORJSON_AVAILABLE else json.loads(data)
                return self._normalize_message_format(parsed)

        except Exception as e:
            self.metrics.protocol_errors += 1
            log.error(f"Failed to parse binary message: {e}")
            return None

    def _normalize_message_format(self, message: dict[str, Any]) -> dict[str, Any]:
        """Normalize message format for consistency"""
        if "type" in message and "t" not in message:
            message["t"] = message.pop("type")
        if "payload" in message and "p" not in message:
            message["p"] = message.pop("payload")
        return message

    async def _send_raw_message(self, message: dict[str, Any]) -> None:
        """Send a message through the WebSocket with proper formatting"""
        send_start_time = time.time()
        now_utc = datetime.now(UTC)
        message_type = message.get("t", "unknown")
        message.get("priority", 5)

        try:
            if not self._running or not self._is_ws_connected():
                return

            # Calculate WSE processing time for observability
            wse_received_at = message.pop("_wse_received_at", None)
            if wse_received_at:
                wse_processing_ms = int((time.time() - wse_received_at) * 1000)
                message["wse_processing_ms"] = wse_processing_ms

                event_type = message.get("t")
                high_freq_types = (
                    "quote_update",
                    "bar_update",
                    "trade_update",
                    "orderbook_update",
                    "PONG",
                )
                if event_type not in high_freq_types:
                    if wse_processing_ms > 100:
                        log.warning(
                            f"High WSE processing time: {wse_processing_ms}ms for {event_type}"
                        )
                    elif wse_processing_ms > 50:
                        log.debug(
                            f"Elevated WSE processing time: {wse_processing_ms}ms for {event_type}"
                        )

            if "v" not in message:
                message["v"] = self.protocol_version
            if "ts" not in message:
                message["ts"] = now_utc.isoformat()

            # Selective message signing
            message_type = message.get("t", "")
            should_sign = self.message_signing_enabled or message_type in self.signed_message_types

            if should_sign:
                signature = await self.security_manager.sign_message(message)
                if signature:
                    message["sig"] = signature
                    log.debug(f"Signed message type: {message_type} (sig length: {len(signature)})")
                else:
                    log.warning(f"Failed to sign message type: {message_type}")

            # Message category prefix
            msg_category = message.pop("_msg_cat", None)
            if msg_category is None:
                msg_type = message.get("t", "")
                if "_snapshot" in msg_type or "snapshot_" in msg_type:
                    msg_category = "S"
                elif msg_type in (
                    "server_ready",
                    "server_hello",
                    "client_hello_ack",
                    "connection_state_change",
                    "subscription_update",
                    "snapshot_complete",
                    "error",
                    "pong",
                    "PONG",
                ):
                    msg_category = "WSE"
                else:
                    msg_category = "U"

            # Reorder keys: t first, v last (easier to read in logs)
            wire = {}
            if "t" in message:
                wire["t"] = message["t"]
            for k, val in message.items():
                if k not in ("t", "v"):
                    wire[k] = val
            if "v" in message:
                wire["v"] = message["v"]

            # Serialize to bytes directly (avoid bytes->str->bytes round-trip)
            if ORJSON_AVAILABLE:
                json_bytes = orjson.dumps(wire, default=_orjson_default)
            else:
                json_bytes = json.dumps(wire, cls=DateTimeEncoder).encode("utf-8")
            data_bytes = msg_category.encode("ascii") + json_bytes

            log.debug(
                f"[WSE_SENDING] Event: {message_type}, "
                f"Size: {len(data_bytes)}B, "
                f"Compress: {self.compression_enabled and len(data_bytes) > self.compression_threshold}"
            )

            should_compress = (
                self.compression_enabled
                and len(data_bytes) > self.compression_threshold
                and not message.get("encrypted", False)
            )

            # Encrypt if enabled
            if self.encryption_enabled and message.get("encrypted", False):
                encrypted = await self.security_manager.encrypt_message(data_bytes)
                if encrypted:
                    byte_count = len(encrypted) + 2
                    await self.ws.send_bytes(
                        b"E:" + encrypted.encode("utf-8")
                        if isinstance(encrypted, str)
                        else b"E:" + encrypted
                    )
                    self.metrics.bytes_sent += byte_count
                    if METRICS_AVAILABLE:
                        ws_bytes_sent_total.inc(byte_count)
                    log.debug(f"Sent encrypted message: {len(data_bytes)} -> {byte_count} bytes")
                else:
                    await self.ws.send_bytes(data_bytes)
                    self.metrics.bytes_sent += len(data_bytes)
                    if METRICS_AVAILABLE:
                        ws_bytes_sent_total.inc(len(data_bytes))
                    log.warning("Encryption failed, sent as plain bytes")

            elif should_compress:
                if len(data_bytes) > 10240:
                    compressed = await self.compression_manager.compress_async(data_bytes)
                else:
                    compressed = self.compression_manager.compress(data_bytes)

                byte_count = len(compressed) + 2
                await self.ws.send_bytes(b"C:" + compressed)
                self.metrics.compression_hits += 1
                self.metrics.bytes_sent += byte_count
                if METRICS_AVAILABLE:
                    ws_bytes_sent_total.inc(byte_count)

                compression_ratio = len(compressed) / len(data_bytes)
                self.metrics.compression_ratio = compression_ratio

                log.debug(
                    f"Sent compressed message with C: prefix - "
                    f"Type: {message.get('t')}, "
                    f"Original: {len(data_bytes)} bytes, "
                    f"Compressed: {len(compressed)} bytes, "
                    f"Ratio: {compression_ratio:.2f}"
                )
            else:
                await self.ws.send_text(data_bytes.decode("utf-8"))
                self.metrics.bytes_sent += len(data_bytes)
                if METRICS_AVAILABLE:
                    ws_bytes_sent_total.inc(len(data_bytes))
                log.debug(f"Sent plain text message: {len(data_bytes)} bytes")

            self.metrics.messages_sent += 1
            self.metrics.last_message_sent = now_utc

            total_send_latency_ms = (time.time() - send_start_time) * 1000

            log.debug(
                f"[WSE_SENT] Event: {message_type}, "
                f"Bytes: {self.metrics.bytes_sent}, "
                f"Latency: {total_send_latency_ms:.2f}ms, "
                f"Total: {self.metrics.messages_sent}"
            )

            # Batch circuit_breaker calls (every 100 msgs) to reduce Task overhead
            if self.metrics.messages_sent % 100 == 0:
                self.circuit_breaker.record_success()
            self._last_activity = time.time()

        except Exception as e:
            log.error(f"Send error: {e}")
            self.circuit_breaker.record_failure()
            self.metrics.last_error_message = str(e)
            await self.set_state(ConnectionState.ERROR)
            raise

    async def _sender_loop(self) -> None:
        """Send queued messages with batch support"""
        while self._running:
            try:
                batch = await self.message_queue.dequeue_batch()

                if batch:
                    if self.client_features.get("batch_messages", False) and len(batch) > 1:
                        batch_message = {
                            "t": "batch",
                            "p": {"messages": [msg for _, msg in batch], "count": len(batch)},
                        }
                        await self._send_raw_message(batch_message)
                    else:
                        for _priority, message in batch:
                            await self._send_raw_message(message)
                else:
                    # Event-based wakeup instead of fixed sleep
                    if self._queue_event:
                        self._queue_event.clear()
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(
                                self._queue_event.wait(), timeout=self.batch_timeout
                            )
                    else:
                        await asyncio.sleep(self.batch_timeout)

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Sender loop error: {e}")
                self.circuit_breaker.record_failure()
                await asyncio.sleep(1)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats"""
        while self._running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)

                await self.send_message(
                    {
                        "t": "heartbeat",
                        "p": {
                            "timestamp": int(datetime.now().timestamp() * 1000),
                            "sequence": self.sequence_number,
                        },
                    },
                    priority=10,
                )

                if self._running and self.ws and self._is_ws_connected():
                    try:
                        self.network_analyzer.record_packet_sent()
                        ping_msg = {
                            "t": "PING",
                            "p": {"timestamp": int(datetime.now().timestamp() * 1000)},
                            "v": self.protocol_version,
                        }
                        ping_data = "WSE" + _json_dumps(ping_msg)
                        await self.ws.send_text(ping_data)
                        self.metrics.messages_sent += 1
                    except Exception as e:
                        if self._running:
                            log.error(f"Failed to send PING: {e}")
                        else:
                            log.debug(f"PING skipped during shutdown: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Heartbeat error: {e}")

    async def _health_check_loop(self) -> None:
        """Perform periodic health checks and idle timeout detection"""
        while self._running:
            try:
                await asyncio.sleep(HEALTH_CHECK_INTERVAL)

                idle_duration = time.time() - self._last_activity
                if idle_duration > IDLE_TIMEOUT:
                    log.warning(f"Connection {self.conn_id} idle timeout ({idle_duration:.0f}s)")
                    self._running = False
                    self.message_queue.clear()
                    await self.set_state(ConnectionState.DISCONNECTED)
                    await self.ws.close(code=1000, reason="Idle timeout")
                    break

                diagnostics = self.network_analyzer.analyze()

                cb_state = await self.circuit_breaker.get_state()
                if cb_state["state"] == "OPEN":
                    await self.set_state(ConnectionState.DEGRADED)
                    if self.circuit_breaker.can_execute():
                        log.info(f"Circuit breaker for {self.conn_id} transitioning to HALF_OPEN")
                elif (
                    cb_state["state"] == "CLOSED"
                    and self.connection_state == ConnectionState.DEGRADED
                ):
                    await self.set_state(ConnectionState.CONNECTED)

                if diagnostics["quality"] in ["poor", "fair"]:
                    await self.send_message(
                        {
                            "t": "connection_quality",
                            "p": {
                                "quality": diagnostics["quality"],
                                "suggestions": diagnostics["suggestions"],
                                "metrics": self.metrics.to_dict(),
                                "jitter": diagnostics["jitter"],
                                "packet_loss": diagnostics["packet_loss"],
                            },
                        },
                        priority=8,
                    )

                if self.debug_mode:
                    log.debug(
                        f"Health check - Quality: {diagnostics['quality']}, "
                        f"Jitter: {diagnostics['jitter']}ms, "
                        f"Packet loss: {diagnostics['packet_loss']}%"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Health check error: {e}")

    async def _metrics_collection_loop(self) -> None:
        """Collect and calculate metrics"""
        last_message_count = 0
        last_byte_count = 0

        while self._running:
            try:
                await asyncio.sleep(METRICS_INTERVAL)

                current_messages = self.metrics.messages_received + self.metrics.messages_sent
                self.metrics.message_rate = (
                    current_messages - last_message_count
                ) / METRICS_INTERVAL
                last_message_count = current_messages

                current_bytes = self.metrics.bytes_received + self.metrics.bytes_sent
                self.metrics.bandwidth = (current_bytes - last_byte_count) / METRICS_INTERVAL
                last_byte_count = current_bytes

                if self.metrics.compression_hits > 0:
                    total_original = self.metrics.bytes_sent / (self.metrics.compression_ratio or 1)
                    self.metrics.compression_ratio = (
                        self.metrics.bytes_sent / total_original if total_original > 0 else 1
                    )

                if METRICS_AVAILABLE:
                    queue_stats = self.message_queue.get_stats()
                    ws_queue_size.labels(connection_id=self.conn_id, priority="total").set(
                        queue_stats["size"]
                    )
                    ws_queue_backpressure.labels(connection_id=self.conn_id).set(
                        1 if queue_stats["backpressure"] else 0
                    )
                    for priority, dropped in queue_stats.get("dropped_by_priority", {}).items():
                        ws_queue_dropped_total.labels(
                            connection_id=self.conn_id, priority=str(priority)
                        ).inc(dropped)

                log.info(
                    f"Connection {self.conn_id} metrics - "
                    f"Rate: {self.metrics.message_rate:.2f} msg/s, "
                    f"Bandwidth: {self.metrics.bandwidth:.2f} B/s, "
                    f"Compression: {self.metrics.compression_ratio:.2f}"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Metrics collection error: {e}")

    async def _sequence_cleanup_loop(self) -> None:
        """Periodically clean up sequence tracking"""
        while self._running:
            try:
                await asyncio.sleep(300)
                await self.event_sequencer.cleanup()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Sequence cleanup error: {e}")

    async def _debug_stats_loop(self) -> None:
        """Periodically log debug statistics"""
        while self._running and self.debug_mode:
            try:
                await asyncio.sleep(60)

                log.info(f"=== Connection {self.conn_id} Debug Stats ===")
                log.info(f"Messages sent: {self.metrics.messages_sent}")
                log.info(f"Messages received: {self.metrics.messages_received}")
                log.info(f"Active subscriptions: {list(self.subscriptions)}")
                log.info(f"Queue size: {self.message_queue.size}")
                log.info(f"Event types sent: {dict(self.event_count)}")

                self.event_count.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Debug stats error: {e}")

    def _reset_idle_timer(self) -> None:
        """Reset idle timeout by updating last activity timestamp."""
        self._last_activity = time.time()

    async def _send_rate_limit_warning(self) -> None:
        """Send rate limit warning to client."""
        rl_status = self.rate_limiter.get_status()

        await self.send_message(
            {
                "t": "rate_limit_warning",
                "p": {
                    "message": "Rate limit exceeded. Please slow down your requests.",
                    "code": "RATE_LIMIT_EXCEEDED",
                    "limit": rl_status.get("capacity", 1000),
                    "window": 1.0,
                    "retry_after": 1.0,
                    "current_usage": rl_status.get("capacity", 1000)
                    - rl_status.get("available_tokens", 0),
                    "stats": {
                        "tokens_remaining": rl_status.get("available_tokens", 0),
                        "refill_rate": rl_status.get("refill_rate", 100),
                        "capacity": rl_status.get("capacity", 1000),
                    },
                },
            },
            priority=10,
            _bypass_rate_limit=True,
        )

    async def cleanup(self) -> None:
        """Clean up connection resources"""
        self._running = False

        await self.set_state(ConnectionState.DISCONNECTED)

        if METRICS_AVAILABLE:
            ws_connections_active.dec()
            ws_disconnections_total.labels(reason="normal").inc()

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Run registered cleanup hooks
        for hook in self._cleanup_hooks:
            try:
                await hook(self.user_id, self.conn_id)
            except Exception as e:
                log.error(f"Error in cleanup hook for {self.conn_id}: {e}")

        # Clean up encryption keys for this connection
        if hasattr(self.security_manager, "remove_connection"):
            self.security_manager.remove_connection(self.conn_id)

        # Bulk unsubscribe all handlers
        try:
            removed = await self.event_bus.unsubscribe_connection(self.conn_id)
            log.info(f"Bulk unsubscribed {removed} handlers for connection {self.conn_id}")
        except Exception as e:
            log.error(f"Error in bulk unsubscribe for {self.conn_id}: {e}")
            for sub_id in self.subscription_ids.values():
                try:
                    await self.event_bus.unsubscribe(sub_id)
                except Exception as e2:
                    log.error(f"Error unsubscribing {sub_id}: {e2}")

        # Cleanup components
        await self.event_sequencer.shutdown()

        # Log final metrics
        log.info(
            f"Connection {self.conn_id} closed - "
            f"Messages: {self.metrics.messages_sent}/{self.metrics.messages_received}, "
            f"Bytes: {self.metrics.bytes_sent}/{self.metrics.bytes_received}, "
            f"Compression hits: {self.metrics.compression_hits}, "
            f"Compression ratio: {self.metrics.compression_ratio:.2f}"
        )

        if self.debug_mode:
            log.info(f"Final event count: {dict(self.event_count)}")
