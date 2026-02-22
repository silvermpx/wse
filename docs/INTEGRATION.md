# WSE Integration Guide

This guide covers integrating WSE into a new project. WSE is designed as a self-contained module that can be extracted and reused.

## Deployment Modes

WSE supports two deployment modes. Choose based on your performance requirements.

### Router Mode (embedded in FastAPI)

Mount WSE as a FastAPI router on the same port as your app. Simplest setup — one process, one port.

```python
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig

app = FastAPI()
wse = create_wse_router(WSEConfig(redis_url="redis://localhost:6379"))
app.include_router(wse, prefix="/wse")

# Publish from anywhere
await wse.publish("notifications", {"text": "Hello!"})
```

### Standalone Mode (dedicated Rust server)

Run `RustWSEServer` on its own port for maximum throughput. The WebSocket server runs entirely in a Rust tokio runtime — no FastAPI overhead, no GIL on the hot path. This is how WSE achieves 2M msg/s on EPYC.

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

while True:
    events = server.drain_inbound(256, 50)
    for event in events:
        handle(event)
```

With standalone mode:
- JWT validation happens in Rust during the WebSocket handshake (0.01ms)
- `server_ready` is sent from Rust before Python runs
- Python receives pre-validated events via `drain_inbound()` (batch polling, one GIL acquisition per batch)
- Ping/pong, rate limiting, compression all run in Rust

---

## Router Mode Integration

### Prerequisites

- Python 3.12+
- FastAPI
- Redis (for multi-instance coordination)
- `orjson` package (optional, falls back to `json`)

### Required Files

Copy the following directory structure:

```
server/
├── wse_router.py
├── dependencies.py
├── core/
│   ├── pubsub_bus.py
│   ├── types.py
│   ├── event_mappings.py
│   ├── event_transformer.py
│   └── event_filters.py
├── websocket/
│   ├── wse_connection.py
│   ├── wse_handlers.py
│   ├── wse_manager.py
│   ├── wse_queue.py
│   ├── wse_compression.py
│   ├── wse_security.py
│   └── wse_event_sequencer.py
└── services/
    └── snapshot_service.py
```

### Step 1: Install Dependencies

```bash
pip install fastapi redis orjson msgpack
# Optional: pip install prometheus-client  (for metrics)
```

### Step 2: Set Up Redis

```python
# infrastructure/redis_client.py
import redis.asyncio as redis

_redis_pool = None

async def get_redis_instance():
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=False,
        )
    return _redis_pool

async def publish(channel: str, message: str, r=None):
    """Publish message to Redis channel."""
    client = r or await get_redis_instance()
    await client.publish(channel, message)
```

### Step 3: Initialize PubSubBus

```python
# app/lifespan.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from server.core.pubsub_bus import PubSubBus

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    redis_client = await get_redis_instance()
    bus = PubSubBus(redis_client=redis_client)
    await bus.initialize()
    app.state.pubsub_bus = bus

    yield

    # Shutdown
    await bus.shutdown()
```

### Step 4: Mount the Router

```python
# app/main.py
from fastapi import FastAPI
from server.wse_router import router as wse_router

app = FastAPI(lifespan=lifespan)
app.include_router(wse_router)
```

### Step 5: Implement Snapshot Service

The snapshot service provides initial state data when clients connect. Implement the `SnapshotServiceProtocol`:

```python
# server/services/snapshot_service.py
from typing import Protocol, List, Dict, Any
from uuid import UUID

class SnapshotServiceProtocol(Protocol):
    async def get_resources_snapshot(
        self,
        user_id: UUID,
        include_archived: bool = False,
    ) -> List[Dict[str, Any]]:
        ...

    async def get_data_snapshot(
        self,
        user_id: UUID,
        include_details: bool = False,
    ) -> List[Dict[str, Any]]:
        ...
```

### Step 6: Publish Events

From anywhere in your application:

```python
from server.core.types import EventPriority

# Get the PubSubBus (from app.state or dependency injection)
bus = app.state.pubsub_bus

# Publish an event
await bus.publish(
    topic=f"user:{user_id}:data_events",
    event={
        "event_type": "item_created",
        "item_id": str(item.id),
        "name": "Example Item",
        "status": "active",
    },
    priority=EventPriority.HIGH,
)
```

### Step 7: Customize Event Types

Edit `core/event_mappings.py` to define your domain events:

```python
# Map domain event types to WSE event types
EVENT_TYPE_MAP = {
    "ItemCreated": "item_created",
    "ItemUpdated": "item_updated",
    "StatusChanged": "status_update",
    # Add your domain events here
}
```

Edit `core/event_transformer.py` to transform domain events into WSE messages:

```python
class EventTransformer:
    @staticmethod
    def transform_for_ws(event: dict, sequence: int) -> dict:
        event_type = EVENT_TYPE_MAP.get(
            event.get('event_type', ''),
            event.get('event_type', 'unknown')
        )
        return {
            'v': 2,
            'id': str(generate_uuid()),
            't': event_type,
            'ts': datetime.now(timezone.utc).isoformat(),
            'seq': sequence,
            'p': event,
        }
```

### Step 8: JWT Authentication

WSE expects a JWT token with a `sub` claim containing the user ID:

```python
# Your JWT token should have:
{
    "sub": "019c53c4-abcd-7def-8901-234567890abc",  # User UUID
    "exp": 1708441800,
    "iat": 1708440000,
}
```

Modify `wse_router.py` to use your JWT validation:

```python
# Replace JwtTokenManager with your auth module
from your_app.auth import validate_jwt_token

payload = validate_jwt_token(token_to_validate)
user_id = UUID(payload["sub"])
```

---

## Frontend Integration

### Prerequisites

- React 18+
- TypeScript 5+
- Zustand (state management)
- `pako` package (zlib decompression)

### Required Files

Copy the following directory:

```
client/
├── index.ts
├── types.ts
├── constants.ts
├── hooks/useWSE.ts
├── stores/
│   ├── useWSEStore.ts
│   └── useMessageQueueStore.ts
├── services/
│   ├── ConnectionManager.ts
│   ├── MessageProcessor.ts
│   ├── ConnectionPool.ts
│   ├── NetworkMonitor.ts
│   ├── RateLimiter.ts
│   ├── OfflineQueue.ts
│   ├── EventSequencer.ts
│   └── AdaptiveQualityManager.ts
├── protocols/
│   ├── compression.ts
│   └── transformer.ts
├── handlers/
│   ├── index.ts
│   └── EventHandlers.ts    # Start with this, add domain handlers later
└── utils/
    ├── logger.ts
    ├── circuitBreaker.ts
    └── security.ts
```

### Step 1: Install Dependencies

```bash
npm install zustand pako
npm install -D @types/pako
```

### Step 2: Add the WSE Hook

```typescript
// In your main App component or layout
import { useWSE } from './wse';

function App() {
  const { isConnected } = useWSE({
    endpoints: [`ws://${window.location.host}/wse`],
  });

  return (
    <div>
      {/* Your app */}
    </div>
  );
}
```

### Step 3: Create Domain Handlers

```typescript
// client/handlers/MyHandlers.ts
import { logger } from '../utils/logger';

export function registerMyHandlers() {
  // Handler for custom events
  window.addEventListener('item_created', ((e: CustomEvent) => {
    logger.info('Item created:', e.detail);
    // Update your state/store here
  }) as EventListener);

  window.addEventListener('status_update', ((e: CustomEvent) => {
    logger.info('Status updated:', e.detail);
  }) as EventListener);
}
```

### Step 4: React Query Integration

```typescript
// In your data components
import { useQuery, useQueryClient } from '@tanstack/react-query';

function ItemList() {
  const queryClient = useQueryClient();

  const { data: items } = useQuery({
    queryKey: ['items'],
    queryFn: fetchItems,
    staleTime: Infinity,  // WSE controls updates
  });

  // Merge WSE updates into React Query cache
  useEffect(() => {
    const handler = (e: CustomEvent) => {
      queryClient.setQueryData(['items'], (old: Item[] | undefined) => {
        if (!old) return [e.detail];
        return old.map(o => o.id === e.detail.id ? { ...o, ...e.detail } : o);
      });
    };

    window.addEventListener('item_updated', handler as EventListener);
    return () => window.removeEventListener('item_updated', handler as EventListener);
  }, [queryClient]);

  return <div>{/* render items */}</div>;
}
```

---

## Python Client Integration

The Python client (`wse-client` on PyPI) connects to WSE from backend services, CLI tools, scripts, and integration tests.

### Install

```bash
pip install wse-client            # Core (websockets only)
pip install wse-client[crypto]    # + ECDH/AES-GCM encryption
pip install wse-client[all]       # + crypto + msgpack + orjson
```

### Async Client

```python
from wse_client import connect

async with connect("ws://localhost:5006/wse", token="your-jwt") as client:
    await client.subscribe(["notifications", "live_data"])
    async for event in client:
        print(event.type, event.payload)
```

### Sync Client

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5006/wse", token="your-jwt")
client.connect()
client.subscribe(["notifications"])

event = client.recv(timeout=5.0)
print(event.type, event.payload)

client.close()
```

### Callback Pattern

```python
from wse_client import AsyncWSEClient

client = AsyncWSEClient("ws://localhost:5006/wse", token="your-jwt")

@client.on("notifications")
def handle_notification(event):
    print(f"Notification: {event.payload}")

@client.on("price_update")
def handle_price(event):
    print(f"Price: {event.payload['symbol']} = {event.payload['price']}")
```

### Use Cases

| Use Case | Pattern |
|----------|---------|
| **Microservice-to-microservice** | AsyncWSEClient in FastAPI lifespan |
| **CLI tool** | SyncWSEClient with `recv()` loop |
| **Integration tests** | AsyncWSEClient with `async with` context manager |
| **Data pipeline** | AsyncWSEClient async iterator with batch processing |
| **Monitoring** | Callback pattern with `@client.on()` handlers |

### Python Client Configuration

```python
from wse_client import AsyncWSEClient
from wse_client.types import ReconnectConfig, LoadBalancingStrategy

client = AsyncWSEClient(
    url="ws://localhost:5006/wse",
    token="your-jwt",
    reconnect=ReconnectConfig(
        max_attempts=-1,            # Infinite retries (default)
        base_delay=1.0,             # 1s initial delay
        max_delay=30.0,             # 30s max delay
        factor=1.5,                 # Backoff multiplier
    ),
    endpoints=["ws://primary:5006/wse", "ws://fallback:5006/wse"],
    load_balancing=LoadBalancingStrategy.WEIGHTED_RANDOM,
    queue_size=10000,               # Event queue size
)
```

---

## Configuration Reference

### Backend Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `JWT_SECRET_KEY` | (required) | JWT signing secret |
| `FERNET_KEY` | (optional) | Fernet encryption key |
| `WSE_COMPRESSION_THRESHOLD` | `1024` | Compression threshold in bytes |
| `WSE_MAX_MESSAGE_SIZE` | `1048576` | Max message size (1 MB) |
| `WSE_HEARTBEAT_INTERVAL` | `15` | Heartbeat interval in seconds |
| `WSE_IDLE_TIMEOUT` | `90` | Idle timeout in seconds |

### Frontend Configuration

```typescript
const config: WSEConfig = {
  endpoints: ['wss://api.example.com/wse'],

  reconnection: {
    maxRetries: Infinity,
    initialDelay: 1000,
    maxDelay: 30000,
    backoffMultiplier: 1.5,
  },

  security: {
    encryption: false,     // Enable for E2E encryption
    signing: false,        // Enable for message signing
  },

  performance: {
    compression: true,     // Enable zlib compression
    batchSize: 10,         // Messages per batch
    batchTimeout: 100,     // Batch timeout in ms
  },

  offline: {
    enabled: true,         // IndexedDB offline queue
    maxSize: 1000,         // Max offline messages
  },

  diagnostics: {
    enabled: true,         // Network quality monitoring
  },
};
```

---

## Adapting for Different Use Cases

### Chat / Messaging

WSE is well-suited for chat applications:

1. Enable E2E encryption (`security.encryption: true`)
2. Create a `ChatHandlers.ts` for message events
3. Use `message_sent`, `message_received`, `typing_indicator` event types
4. Leverage offline queue for message delivery on reconnect

### IoT / Sensor Data

For high-frequency sensor data:

1. Enable compression (data is typically repetitive)
2. Use LOW priority for telemetry, HIGH for alerts
3. Increase batch size (50-100) for throughput
4. Consider msgpack for binary efficiency

### Financial / Real-Time Data

For latency-sensitive real-time data:

1. Enable message signing for critical operations
2. Use CRITICAL priority for important confirmations
3. Keep compression disabled for latency-sensitive paths
4. Use circuit breaker to handle upstream outages

### Collaborative Editing

For real-time collaboration:

1. Use sequence numbers for conflict detection
2. Leverage deduplication for idempotent operations
3. Use sync_request for initial state loading
4. Create operation-specific event types
