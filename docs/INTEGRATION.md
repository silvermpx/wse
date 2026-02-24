# WSE Integration Guide

This guide covers integrating WSE into a new or existing project.

## Deployment Modes

WSE supports two deployment modes. Choose based on your performance requirements.

### Router Mode (embedded in FastAPI)

Mount WSE as a FastAPI router on the same port as your app. One process, one port.

```python
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig
import redis.asyncio as redis

app = FastAPI()

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=False)

wse = create_wse_router(WSEConfig(
    redis_client=redis_client,
))

app.include_router(wse, prefix="/wse")
```

Publish events from anywhere in your app:

```python
bus = app.state.pubsub_bus

await bus.publish(
    topic="notifications",
    event={"event_type": "order_shipped", "order_id": 42},
)
```

### Standalone Mode (dedicated Rust server)

Run `RustWSEServer` on its own port for maximum throughput. The WebSocket server runs entirely in a Rust tokio runtime -- no FastAPI overhead, no GIL on the hot path. This is how WSE achieves 14M msg/s JSON on EPYC.

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

### Step 1: Install

```bash
pip install wse-server
```

The `wse-server` package includes all server modules and the prebuilt Rust engine. Dependencies (FastAPI, redis, orjson, etc.) are installed automatically.

### Step 2: Set Up Redis

Redis is required for multi-instance coordination. For single-instance setups you can pass `redis_client=None` (default) and only in-process PubSub is used.

```python
import redis.asyncio as redis

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=False,
)
```

### Step 3: Create and Mount the Router

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)

wse = create_wse_router(WSEConfig(
    redis_client=redis_client,          # None for single-instance
    auth_handler=your_auth_function,    # Optional: async (WebSocket) -> str|None
    snapshot_provider=your_snapshots,   # Optional: SnapshotProvider implementation
    default_topics=["public"],          # Topics for all new connections
    heartbeat_interval=15.0,            # Seconds
    idle_timeout=90.0,                  # Seconds
    enable_compression=True,
    enable_debug=False,                 # Exposes /wse/debug endpoint
))

app.include_router(wse, prefix="/wse")
```

See `WSEConfig` in `wse_server.router` for all available options.

### Step 4: Implement Snapshot Provider (Optional)

If you want to deliver initial state when clients connect, implement the `SnapshotProvider` protocol:

```python
from wse_server import SnapshotProvider
from typing import Any

class MySnapshotProvider:
    async def get_snapshot(self, user_id: str, topics: list[str]) -> dict[str, Any]:
        """Return current state keyed by topic."""
        result = {}
        if "dashboard" in topics:
            result["dashboard"] = await fetch_dashboard_data(user_id)
        if "notifications" in topics:
            result["notifications"] = await fetch_unread(user_id)
        return result
```

Pass it to the config:

```python
wse = create_wse_router(WSEConfig(
    snapshot_provider=MySnapshotProvider(),
))
```

### Step 5: Publish Events

From anywhere in your application -- FastAPI endpoints, background tasks, Celery workers:

```python
# In a FastAPI endpoint
from fastapi import Request

@app.post("/items")
async def create_item(request: Request, body: CreateItemRequest):
    item = save_item(body)

    bus = request.app.state.pubsub_bus
    await bus.publish(
        topic=f"user:{item.owner_id}:events",
        event={
            "event_type": "item_created",
            "item_id": str(item.id),
            "name": item.name,
            "status": "active",
        },
    )
    return item
```

Topic naming conventions:
- `user:{user_id}:events` -- user-specific events
- `chat:{channel_id}` -- channel broadcasts
- `notifications` -- global notifications
- Pattern matching with glob wildcards: `user:*:events`

### Step 6: JWT Authentication

WSE expects a JWT token with a `sub` claim containing the user ID.

For **Router mode**, pass an `auth_handler`:

```python
from fastapi import WebSocket
import jwt

async def auth_handler(websocket: WebSocket) -> str | None:
    """Validate JWT and return user_id, or None to reject."""
    token = websocket.cookies.get("access_token")
    if not token:
        auth_header = websocket.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

    if not token:
        return None

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload["sub"]
    except jwt.InvalidTokenError:
        return None

wse = create_wse_router(WSEConfig(auth_handler=auth_handler))
```

For **Standalone mode**, configure Rust JWT directly:

```python
server = RustWSEServer(
    "0.0.0.0", 5006,
    jwt_secret=b"your-secret-key",
    jwt_issuer="your-app",      # Optional: validate iss claim
    jwt_audience="your-api",    # Optional: validate aud claim
)
```

JWT payload format:

```json
{
    "sub": "019c53c4-abcd-7def-8901-234567890abc",
    "exp": 1708441800,
    "iat": 1708440000
}
```

---

## Frontend Integration (React)

### Step 1: Install

```bash
npm install wse-client
```

All dependencies (zustand, pako, msgpack) are bundled.

### Step 2: Add the WSE Hook

```tsx
import { useWSE } from 'wse-client';

function App() {
  const { isConnected, connectionHealth } = useWSE({
    topics: ['notifications', 'live_data'],
    endpoints: [`ws://${window.location.host}/wse`],
  });

  return (
    <div>
      <p>Status: {connectionHealth}</p>
      {/* Your app */}
    </div>
  );
}
```

### Step 3: Listen for Events

WSE dispatches events as DOM `CustomEvent`s on `window`. The event name matches the `event_type` from the server.

```typescript
// In a component or handler setup
useEffect(() => {
  const handler = (e: CustomEvent) => {
    console.log('Item created:', e.detail);
    // e.detail contains the full event payload
  };

  window.addEventListener('item_created', handler as EventListener);
  return () => window.removeEventListener('item_created', handler as EventListener);
}, []);
```

### Step 4: React Query Integration

WSE works well with React Query. Set `staleTime: Infinity` and let WSE push updates into the cache:

```typescript
import { useQuery, useQueryClient } from '@tanstack/react-query';

function ItemList() {
  const queryClient = useQueryClient();

  const { data: items } = useQuery({
    queryKey: ['items'],
    queryFn: fetchItems,
    staleTime: Infinity,  // WSE controls freshness
  });

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
    queue_size=10000,               # Event queue size
)
```

---

## Configuration Reference

### WSEConfig (Router Mode)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `auth_handler` | `None` | `async (WebSocket) -> str\|None`. Return user_id or None to reject |
| `snapshot_provider` | `None` | `SnapshotProvider` implementation for initial state |
| `default_topics` | `[]` | Topics for connections that don't specify any |
| `allowed_origins` | `[]` | CSWSH protection. Empty = allow all (dev mode) |
| `redis_client` | `None` | Redis client for multi-instance pub/sub |
| `max_message_size` | `1048576` | Max inbound message size (1 MB) |
| `heartbeat_interval` | `15.0` | Server heartbeat in seconds |
| `idle_timeout` | `90.0` | Close idle connections after N seconds |
| `enable_compression` | `True` | Per-message zlib compression |
| `enable_debug` | `False` | Expose /wse/debug and /wse/compression-test endpoints |

### RustWSEServer (Standalone Mode)

| Parameter | Description |
|-----------|-------------|
| `host` | Bind address (e.g. `"0.0.0.0"`) |
| `port` | Listen port (e.g. `5006`) |
| `max_connections` | Maximum concurrent WebSocket connections |
| `jwt_secret` | HS256 signing key (bytes). Enables Rust JWT validation |
| `jwt_issuer` | Optional: validate `iss` claim |
| `jwt_audience` | Optional: validate `aud` claim |

### Frontend Configuration

```typescript
const config: UseWSEConfig = {
  topics: ['notifications'],
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

```python
# Server: broadcast a message to a channel
await bus.publish(
    topic=f"chat:{channel_id}",
    event={
        "event_type": "message_sent",
        "message_id": str(msg.id),
        "text": msg.text,
        "author": msg.author_name,
        "timestamp": msg.created_at.isoformat(),
    },
)
```

```tsx
// React: render messages
window.addEventListener('message_sent', (e: CustomEvent) => {
  appendMessage(e.detail);
});
```

Recommendations:
- Enable E2E encryption (`security.encryption: true` on the client)
- Use `message_sent`, `message_received`, `typing_indicator` event types
- The offline queue stores messages while disconnected and replays on reconnect

### IoT / Sensor Data

```python
# Server: push sensor readings
await bus.publish(
    topic=f"device:{device_id}:telemetry",
    event={
        "event_type": "sensor_reading",
        "temperature": 23.5,
        "humidity": 67.2,
        "battery_pct": 84,
    },
)
```

Recommendations:
- Enable compression -- sensor data is typically repetitive
- Use LOW priority for telemetry, HIGH for alerts
- Increase batch size (50-100) for throughput
- Consider msgpack (`pip install wse-client[all]`) for binary efficiency

### Financial / Real-Time Data

```python
# Server: push price updates with signing
await bus.publish(
    topic="prices",
    event={
        "event_type": "price_update",
        "symbol": "AAPL",
        "price": 187.42,
        "volume": 1_234_567,
    },
)
```

Recommendations:
- Enable message signing for critical operations
- Use CRITICAL priority for order confirmations
- Keep compression disabled for latency-sensitive paths
- Circuit breaker handles upstream outages gracefully

### Collaborative Editing

```python
# Server: broadcast an edit operation
await bus.publish(
    topic=f"doc:{document_id}",
    event={
        "event_type": "text_insert",
        "position": 42,
        "text": "hello",
        "author": user.name,
        "seq": operation_sequence,
    },
)
```

Recommendations:
- Use sequence numbers for conflict detection and ordering
- Deduplication prevents duplicate operations on reconnect
- Use `request_snapshot()` for initial document state
- Define granular event types: `text_insert`, `text_delete`, `cursor_move`
