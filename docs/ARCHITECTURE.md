# WSE Architecture

## Overview

WSE follows a layered architecture with clear separation between transport, protocol, and application concerns. The system supports horizontal scaling through Redis Pub/Sub coordination.

## Deployment Modes

WSE supports two deployment modes:

### Router Mode (embedded in FastAPI)

Mount WSE as a FastAPI router on the same port as your existing app. Best for prototyping, small-to-medium workloads, and when you want a single process.

```python
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig

app = FastAPI()
wse = create_wse_router(WSEConfig(redis_url="redis://localhost:6379"))
app.include_router(wse, prefix="/wse")
```

### Standalone Mode (dedicated Rust server)

Run `RustWSEServer` on its own port for maximum throughput. The entire WebSocket server runs in a Rust tokio runtime — no FastAPI overhead, no GIL on the hot path. This is how WSE achieves 14M msg/s on JSON.

```python
from wse_server._wse_accel import RustWSEServer

server = RustWSEServer(
    "0.0.0.0", 5006,
    max_connections=10000,
    jwt_secret=b"your-secret-key",
    jwt_issuer="your-app",
    jwt_audience="your-api",
)
server.start()

# Drain inbound events in a background thread
while True:
    events = server.drain_inbound(256, 50)  # batch size, timeout ms
    for event in events:
        handle(event)
```

Standalone mode gives you a dedicated Rust tokio runtime. JWT validation, ping/pong, rate limiting, compression, and the WebSocket transport itself all run in Rust with zero GIL acquisition.

All benchmarks (including the 14M msg/s JSON on EPYC) use standalone mode.

---

## Backend Architecture (Router Mode)

```
server/
├── wse_router.py              # FastAPI WebSocket endpoint + HTTP endpoints
├── dependencies.py            # Dependency injection (snapshot service)
├── core/
│   ├── pubsub_bus.py          # Redis Pub/Sub coordination (multi-instance)
│   ├── types.py               # EventPriority enum
│   ├── event_mappings.py      # Domain event -> WSE event type mappings
│   ├── event_transformer.py   # Domain event -> WSE message transformation
│   └── event_filters.py       # Event filtering rules
├── websocket/
│   ├── wse_connection.py      # Connection state, send/receive, background tasks
│   ├── wse_handlers.py        # Message routing and handler implementations
│   ├── wse_manager.py         # Connection registry (all active connections)
│   ├── wse_queue.py           # 5-level priority message queue
│   ├── wse_compression.py     # zlib + msgpack compression
│   ├── wse_security.py        # Fernet encryption + JWT signing
│   └── wse_event_sequencer.py # Sequence tracking and gap detection
└── services/
    └── snapshot_service.py    # Full state snapshot provider
```

### Component Responsibilities

#### WSE Router (`wse_router.py`)

Entry point for WebSocket connections. Handles:
- WebSocket accept and JWT authentication (multi-source: query, header, cookie)
- Connection initialization with feature negotiation
- Main message receive loop with timeout-based polling
- Graceful shutdown with cleanup

HTTP endpoints:
- `GET /wse/health` - Service health check
- `GET /wse/debug` - Debug information (subscriptions, PubSub metrics)
- `GET /wse/compression-test` - Compression effectiveness test

#### WSE Connection (`wse_connection.py`)

Manages a single WebSocket connection lifecycle:

**Background tasks** (started on initialization):
1. **Sender loop** - Dequeues from priority queue, sends via WebSocket
2. **Heartbeat loop** - 15s interval heartbeat + JSON PING for latency
3. **Health check loop** - 30s interval idle detection + circuit breaker check
4. **Metrics collection loop** - 60s interval rate/bandwidth calculation
5. **Sequence cleanup loop** - 5min interval sequencer maintenance

**Message flow (outbound)**:
```
send_message() -> rate limit check -> format normalization -> priority queue
    -> sender_loop() -> _send_raw_message() -> sign -> serialize (orjson)
    -> compress/encrypt -> WebSocket.send_text/send_bytes
```

**Message flow (inbound)**:
```
WebSocket.receive() -> handle_incoming() -> size validation
    -> text: _parse_text_message() (strip prefix, orjson parse)
    -> binary: _parse_binary_message() (C:/E:/M: prefix handling)
    -> normalize format -> WSEHandler.handle_message()
```

**Serialization**: Uses `orjson` for 3-5x faster JSON serialization/deserialization compared to stdlib `json`. Custom `_orjson_default` handler for `Decimal` and `Enum` types (orjson natively handles `datetime`, `UUID`, `int`, `float`).

#### WSE Handlers (`wse_handlers.py`)

Routes incoming messages to handler methods. Supports 25+ message types:

| Category | Types |
|----------|-------|
| Ping/Pong | `ping`, `PING`, `pong`, `PONG` |
| Connection | `client_hello`, `connection_state_request` |
| Subscription | `subscription`, `subscription_update` |
| Sync | `sync_request` |
| Health | `health_check`, `health_check_request`, `health_check_response` |
| Metrics | `metrics_request`, `metrics_response` |
| Config | `config_update`, `config_request` |
| Messages | `priority_message`, `batch_message` |
| Debug | `debug_handlers`, `debug_request`, `sequence_stats_request` |
| Security | `encryption_request`, `key_rotation_request` |
| System | `system_status_request` |

Unknown message types are published to the event bus for custom handling.

**Event handler factory** (`_create_event_handler`):
- Creates a closure per subscription that bridges PubSub events to WebSocket
- Implements deduplication via `OrderedDict` (FIFO, 5000 entry limit)
- Detects pre-formatted WSE messages vs raw domain events
- Transforms raw events via `EventTransformer`

#### Priority Queue (`wse_queue.py`)

5-level async priority queue with backpressure:

```
CRITICAL (10) -> heartbeat, server_ready, errors
HIGH     (8)  -> health checks, config responses
NORMAL   (5)  -> most events (default)
LOW      (3)  -> background updates
BACKGROUND(1) -> telemetry, stats
```

When full (1000 messages):
1. Drop BACKGROUND messages first
2. Drop LOW messages
3. Drop NORMAL messages
4. HIGH/CRITICAL are never dropped unless queue is entirely HIGH/CRITICAL

#### PubSubBus (`pubsub_bus.py`)

Redis Pub/Sub for multi-instance coordination:

```
Publisher (any instance) -> Redis PUBLISH wse:user:123:events -> All instances
    -> Each instance checks local connections -> Sends to matching WebSockets
```

Features:
- **SUBSCRIBE** for exact topics, **PSUBSCRIBE** for wildcard patterns
- **Circuit breaker** with exponential backoff (1.5x factor, 60s max)
- **Dead Letter Queue** for failed handler invocations (Redis List, 7-day TTL)
- **Connection-scoped cleanup** via `unsubscribe_connection(conn_id)`
- All handlers invoked as `asyncio.create_task()` (non-blocking)

#### Compression (`wse_compression.py`)

| Method | When Used | Benefit |
|--------|-----------|---------|
| zlib (sync) | Messages < 10 KB | Low latency |
| zlib (async) | Messages > 10 KB | Non-blocking |
| msgpack | Binary protocol clients | 30-50% smaller than JSON |

Default threshold: 1024 bytes. Compression level: 6 (balanced speed/ratio).

#### Security (`wse_security.py`)

| Feature | Implementation |
|---------|----------------|
| Encryption | Fernet (symmetric) |
| Message signing | JWT with SHA-256 payload hash + nonce |
| Session tokens | 24-hour access tokens per connection |
| Channel encryption | Per-channel tokens with 1-hour expiry |

Selective signing: Only critical operations are signed. Data updates and status messages skip signing for performance.

---

## Frontend Architecture

```
client/
├── index.ts                    # Public API exports
├── types.ts                    # TypeScript interfaces and types
├── constants.ts                # Protocol constants, event maps
├── hooks/
│   └── useWSE.ts               # Main React hook (single entry point)
├── stores/
│   ├── useWSEStore.ts          # Zustand store (connection state, metrics)
│   └── useMessageQueueStore.ts # Message queue state
├── services/
│   ├── ConnectionManager.ts    # WebSocket lifecycle management
│   ├── MessageProcessor.ts     # Inbound message parsing and routing
│   ├── ConnectionPool.ts       # Multi-endpoint connection pooling
│   ├── NetworkMonitor.ts       # Online/offline detection
│   ├── RateLimiter.ts          # Client-side rate limiting
│   ├── OfflineQueue.ts         # IndexedDB persistence for offline messages
│   ├── EventSequencer.ts       # Sequence gap detection
│   └── AdaptiveQualityManager.ts # Dynamic quality adjustment
├── protocols/
│   ├── compression.ts          # zlib decompression (pako)
│   └── transformer.ts          # Domain event -> CustomEvent mapping
├── handlers/
│   ├── index.ts                # Handler registry
│   └── EventHandlers.ts        # Generic event handling
└── utils/
    ├── logger.ts               # Structured logging with levels
    ├── circuitBreaker.ts       # Client-side circuit breaker
    └── security.ts             # AES-GCM encryption, HMAC signing, ECDH key exchange
```

### useWSE Hook

The `useWSE` hook is the single entry point for all WSE functionality:

```typescript
const {
  isConnected,        // boolean - connection status
  connectionState,    // ConnectionState enum
  subscribe,          // (topics: string[]) => void
  unsubscribe,        // (topics: string[]) => void
  sendMessage,        // (message: any) => void
  lastMessage,        // last received message
  metrics,            // connection metrics
  diagnostics,        // network diagnostics
} = useWSE(config);
```

Configuration:
```typescript
interface WSEConfig {
  endpoints: string[];          // WebSocket URLs
  reconnection?: {
    maxRetries?: number;        // default: Infinity
    initialDelay?: number;      // default: 1000ms
    maxDelay?: number;          // default: 30000ms
    backoffMultiplier?: number; // default: 1.5
  };
  security?: {
    encryption?: boolean;       // default: false
    signing?: boolean;          // default: false
  };
  performance?: {
    compression?: boolean;      // default: true
    batchSize?: number;         // default: 10
    batchTimeout?: number;      // default: 100ms
  };
  offline?: {
    enabled?: boolean;          // default: true
    maxSize?: number;           // default: 1000
  };
  diagnostics?: {
    enabled?: boolean;          // default: true
  };
}
```

### State Management Pattern

```
Server data (snapshots, updates)  ->  React Query + WSE cache merge
Local UI state (toggles, forms)   ->  useState
Persisted UI state (preferences)  ->  Zustand
```

React Query integration (TkDodo pattern):
```typescript
// staleTime: Infinity - WSE controls all updates
const { data } = useQuery({
  queryKey: ['resources'],
  queryFn: fetchResources,
  staleTime: Infinity,
});

// WSE merges into React Query cache
useEffect(() => {
  const handler = (e: CustomEvent) => {
    queryClient.setQueryData(['resources'], (old) => ({
      ...old,
      ...e.detail,
    }));
  };
  window.addEventListener('resource_update', handler);
  return () => window.removeEventListener('resource_update', handler);
}, [queryClient]);
```

### Message Processing Pipeline

```
WebSocket.onmessage
    -> Binary: check C:/E:/M: prefix -> decompress/decrypt/unpack
    -> Text: strip WSE/S/U prefix -> JSON.parse
    -> Deduplication check (message ID)
    -> Sequence check (gap detection)
    -> Handler registry lookup by event type
    -> Domain handler (custom handlers per event type)
    -> window.dispatchEvent(new CustomEvent(eventType, { detail: payload }))
    -> React Query cache merge (via event listener)
```

---

## Python Client Architecture

```
python-client/
├── wse_client/
│   ├── __init__.py            # Public API: connect(), AsyncWSEClient, SyncWSEClient
│   ├── client.py              # AsyncWSEClient (async context manager, async iterator)
│   ├── sync_client.py         # SyncWSEClient (threaded wrapper for sync code)
│   ├── connection.py          # ConnectionManager (lifecycle, reconnect, heartbeat)
│   ├── protocol.py            # MessageCodec (prefix strip, JSON parse, envelope)
│   ├── compression.py         # zlib compress/decompress
│   ├── msgpack_handler.py     # msgpack encode/decode (optional dep)
│   ├── security.py            # ECDH P-256, AES-GCM-256, HMAC-SHA256
│   ├── circuit_breaker.py     # CLOSED/OPEN/HALF_OPEN state machine
│   ├── rate_limiter.py        # Token bucket
│   ├── event_sequencer.py     # Dedup + out-of-order buffering
│   ├── connection_pool.py     # Multi-endpoint, health scoring, 3 LB strategies
│   ├── network_monitor.py     # Latency/jitter/quality analysis
│   ├── types.py               # Enums, dataclasses (WSEEvent, ConnectionState)
│   ├── errors.py              # WSEError hierarchy
│   └── constants.py           # Protocol version, timeouts, thresholds
└── tests/
    └── test_*.py              # Unit tests for all modules
```

### AsyncWSEClient

Primary interface. Async context manager + async iterator:

```python
async with connect("ws://localhost:5006/wse", token="jwt") as client:
    await client.subscribe(["notifications"])
    async for event in client:
        print(event.type, event.payload)
```

### Message Processing Pipeline

```
WebSocket.recv()
    -> Binary: check C:/E:/M: prefix -> decompress/decrypt/unpack
    -> Text: strip WSE/S/U prefix -> json.loads (or orjson)
    -> Deduplication check (event ID, 10K window)
    -> Sequence check (gap detection, reorder buffer)
    -> System handler dispatch (server_ready, error, PONG, etc.)
    -> User handler callbacks (@client.on("type"))
    -> Async iterator queue (async for event in client)
```

### Wire Compatibility

The Python client speaks the same wire protocol as the TypeScript client. All prefix formats, binary frame detection, encryption handshake, and message envelope are identical. A Python client can connect to the same server as React clients simultaneously.

---

## Multi-Instance Scaling

See **[REDIS_PUBSUB.md](REDIS_PUBSUB.md)** for full Rust standalone Redis Pub/Sub documentation (reliability, circuit breaker, DLQ, metrics, PyO3 API).

```
                    Load Balancer
                   /      |      \
              Instance A  B       C
                 |        |       |
              Redis Pub/Sub (shared)
                 |        |       |
              User 1    User 2  User 3
```

Each instance:
1. Accepts WebSocket connections from its assigned users
2. Subscribes to Redis channels for connected users
3. Receives ALL published events via Redis broadcast
4. Checks if the target user is connected to THIS instance
5. Sends to local WebSocket if connected, ignores otherwise

### Router Mode (Python PubSubBus)

```
Domain Event (any instance)
    -> WSE Publisher -> PubSubBus.publish()
    -> Redis PUBLISH wse:user:123:events
    -> ALL instances receive via SUBSCRIBE/PSUBSCRIBE
    -> Instance with user 123 connected -> sends to WebSocket
    -> Other instances -> silently ignore
```

### Standalone Mode (Rust redis_pubsub)

```
Domain Event (any instance)
    -> server.publish("user:123:events", payload)
    -> Rust listener_task -> Redis PUBLISH wse:user:123:events
    -> ALL instances receive via PSUBSCRIBE wse:*
    -> dispatch_to_subscribers() -> exact + glob match
    -> ConnectionHandle.tx -> WebSocket
```

Standalone mode includes: auto-reconnect with exponential backoff, circuit breaker (10 failures / 60s reset), publish retry (3 attempts), Dead Letter Queue (1000 entries), AtomicU64 metrics.

## Metrics

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `ws_connections_active` | Gauge | Current active connections |
| `ws_connections_total` | Counter | Total connections (by user) |
| `ws_disconnections_total` | Counter | Disconnections (by reason) |
| `ws_messages_sent_total` | Counter | Messages sent (by type, priority) |
| `ws_messages_received_total` | Counter | Messages received (by type) |
| `ws_message_send_latency_seconds` | Histogram | Send latency (by priority) |
| `ws_bytes_sent_total` | Counter | Total bytes sent |
| `ws_bytes_received_total` | Counter | Total bytes received |
| `ws_queue_size` | Gauge | Queue depth (by connection, priority) |
| `ws_queue_dropped_total` | Counter | Dropped messages (by connection, priority) |
| `ws_queue_backpressure` | Gauge | Backpressure indicator |
| `pubsub_published_total` | Counter | PubSub publishes (by topic, priority) |
| `pubsub_received_total` | Counter | PubSub receives (by pattern) |
| `pubsub_publish_latency_seconds` | Histogram | PubSub publish latency |
| `pubsub_handler_errors_total` | Counter | Handler errors (by pattern, error type) |
| `dlq_messages_total` | Counter | Dead letter queue entries |
| `dlq_size` | Gauge | DLQ size (by channel) |
| `dlq_replayed_total` | Counter | DLQ replayed messages |
