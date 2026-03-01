# WSE Integration Guide

Complete reference for integrating `wse-server` into your application. Covers server setup, authentication, messaging, subscriptions, clustering, and client SDKs.

---

## 1. Installation

```bash
pip install wse-server
```

The package includes the Python server module and the prebuilt Rust engine (`wse-accel`). No separate compilation step required.

---

## 2. Basic Server Setup

```python
from wse_server import RustWSEServer, rust_jwt_encode
import time

server = RustWSEServer(
    "0.0.0.0", 5007,
    max_connections=10_000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="my-app",
    jwt_audience="my-api",
    jwt_cookie_name="access_token",  # optional, default: "access_token"
)
server.enable_drain_mode()
server.start()
```

The WebSocket server runs entirely in a Rust tokio runtime - no GIL on the hot path. Python receives pre-validated events via `drain_inbound()`, acquiring the GIL once per batch.

### Running alongside FastAPI

WSE runs on its own port with its own TCP listener. To run it alongside FastAPI, start WSE in the FastAPI lifespan and run the drain loop in a background thread:

```python
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from wse_server import RustWSEServer, rust_jwt_encode

server = RustWSEServer(
    "0.0.0.0", 5007,
    max_connections=10_000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="my-app",
    jwt_audience="my-api",
    jwt_cookie_name="access_token",  # optional, default: "access_token"
)

# --- Event processing (runs in a background thread) -----------------------

users: dict[str, str] = {}  # conn_id -> user_id

def drain_loop():
    while server.is_running():
        events = server.drain_inbound(256, 50)
        for ev in events:
            event_type, conn_id, data = ev
            if event_type == "auth_connect":
                users[conn_id] = data
                server.subscribe_connection(conn_id, ["notifications"])
            elif event_type == "msg":
                handle_message(conn_id, data)
            elif event_type == "disconnect":
                users.pop(conn_id, None)

def handle_message(conn_id: str, data: dict):
    msg_type = data.get("t", "")
    if msg_type == "chat":
        server.broadcast("chat", f'{{"t":"chat","p":{{"text":"{data["p"]["text"]}"}}}}'  )

# --- FastAPI lifespan ------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    server.enable_drain_mode()
    server.start()
    worker = threading.Thread(target=drain_loop, daemon=True)
    worker.start()
    yield
    server.stop()

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health():
    return server.health_snapshot()

@app.post("/broadcast/{topic}")
def broadcast(topic: str, message: str):
    server.broadcast(topic, message)

@app.get("/token/{user_id}")
def get_token(user_id: str):
    import time
    token = rust_jwt_encode(
        {"sub": user_id, "iss": "my-app", "aud": "my-api",
         "exp": int(time.time()) + 3600, "iat": int(time.time())},
        b"replace-with-a-strong-secret-key!",
    )
    return {"token": token}
```

Run with:

```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

This gives you two endpoints:
- **FastAPI** on port 8000 for REST API (health checks, token generation, broadcast triggers)
- **WSE** on port 5007 for WebSocket connections (handled entirely by Rust)

The drain loop runs in a daemon thread and processes WebSocket events independently from FastAPI request handling. Both share the same `server` instance, so REST endpoints can call `server.broadcast()`, `server.send()`, `server.health_snapshot()`, etc.

For production, put both behind the same reverse proxy (nginx, Caddy) and route `/wse` to port 5007 and everything else to port 8000.

---

## 3. JWT Authentication

Configure `jwt_secret`, `jwt_issuer`, `jwt_audience`, and optionally `jwt_cookie_name` in the constructor. The server validates JWT tokens during the WebSocket handshake in Rust (zero-GIL, sub-millisecond).

**Token claims:**

| Claim | Required | Description |
|-------|----------|-------------|
| `sub` | Yes | User ID (returned as `user_id` in auth_connect event) |
| `exp` | Yes | Expiration timestamp (Unix epoch) |
| `iat` | Yes | Issued-at timestamp (Unix epoch) |
| `iss` | If configured | Issuer (validated only when `jwt_issuer` is set) |
| `aud` | If configured | Audience (validated only when `jwt_audience` is set) |

**Token delivery:** The client sends the token as an `Authorization: Bearer <token>` header or a cookie during the WebSocket handshake. The cookie name defaults to `access_token` and can be changed via the `jwt_cookie_name` constructor parameter.

**Connection events:**
- With JWT configured: `auth_connect` event fires with the validated `user_id`
- Without JWT configured: `connect` event fires with the raw cookies string

**Generating tokens:**

```python
token = rust_jwt_encode(
    {"sub": "user-1", "iss": "my-app", "aud": "my-api",
     "exp": int(time.time()) + 3600, "iat": int(time.time())},
    b"replace-with-a-strong-secret-key!",
)
```

`rust_jwt_encode` runs in Rust - use it instead of PyJWT for consistency with the server's validation logic.

---

## 4. Event Handling

### Drain Mode (recommended)

Drain mode gives you full control over the event loop. Call `enable_drain_mode()` before `start()`, then poll with `drain_inbound()`.

```python
server.enable_drain_mode()
server.start()

while True:
    events = server.drain_inbound(256, 50)  # batch_size, timeout_ms
    for ev in events:
        event_type = ev[0]
        conn_id = ev[1]

        if event_type == "auth_connect":
            user_id = ev[2]
            server.subscribe_connection(conn_id, ["updates"])
        elif event_type == "msg":
            data = ev[2]  # parsed dict
        elif event_type == "raw":
            text = ev[2]  # unparsed string
        elif event_type == "bin":
            data = ev[2]  # bytes
        elif event_type == "disconnect":
            pass
        elif event_type == "connect":
            cookies = ev[2]  # no-auth mode only
        elif event_type == "presence_join":
            info = ev[2]    # {"topic": ..., "user_id": ..., "data": ...}
        elif event_type == "presence_leave":
            info = ev[2]    # {"topic": ..., "user_id": ..., "data": ...}
```

**`drain_inbound(batch_size, timeout_ms)`** - blocks up to `timeout_ms` waiting for the first event, then drains up to `batch_size` events without blocking. Returns a list of tuples. The GIL is released during the channel wait and acquired once for the entire batch.

### Event Types

| Type | Trigger | ev[2] payload |
|------|---------|---------------|
| `"connect"` | New connection (no JWT configured) | cookies string |
| `"auth_connect"` | JWT-validated connection | user_id string |
| `"msg"` | Client sent WSE-prefixed JSON | parsed dict |
| `"raw"` | Client sent non-JSON text | raw string |
| `"bin"` | Client sent binary frame | bytes |
| `"disconnect"` | Connection closed | None |
| `"presence_join"` | User's first connection joined a topic | dict: topic, user_id, data |
| `"presence_leave"` | User's last connection left a topic | dict: topic, user_id, data |

### Callback Mode (alternative)

If you prefer a callback-driven model instead of polling:

```python
def on_connect(conn_id, user_id):
    print(f"Connected: {user_id}")

def on_message(conn_id, data):
    print(f"Message: {data}")

def on_disconnect(conn_id):
    print(f"Disconnected: {conn_id}")

server.set_callbacks(on_connect, on_message, on_disconnect)
server.start()
```

Drain mode is recommended for production workloads - it batches events efficiently and gives you explicit control over backpressure. Callbacks use `spawn_blocking` per invocation, which acquires the GIL more frequently.

---

## 5. Sending Messages

```python
# Send raw text to one connection
server.send(conn_id, 'WSE{"t":"hello","p":{"msg":"hi"},"v":1}')

# Send binary to one connection
server.send_bytes(conn_id, b"\x00\x01\x02")

# Send event dict (auto-serialized, auto-compressed if > threshold)
bytes_sent = server.send_event(conn_id, {"t": "update", "p": {"value": 42}})

# Broadcast to ALL connections (pre-framed, single frame build)
server.broadcast_all('WSE{"t":"notification","p":{},"v":1}')

# Broadcast to topic subscribers (local instance only)
server.broadcast_local("prices", '{"t":"price","p":{"symbol":"AAPL","price":187.42}}')

# Broadcast to topic subscribers (local + cluster peers)
server.broadcast("prices", '{"t":"price","p":{"symbol":"AAPL","price":187.42}}')
```

**Method summary:**

| Method | Scope | Use case |
|--------|-------|----------|
| `send(conn_id, text)` | Single connection | Direct text messages |
| `send_bytes(conn_id, data)` | Single connection | Binary data |
| `send_event(conn_id, dict)` | Single connection | Structured events (auto-serialized) |
| `broadcast_all(text)` | All connections | Global announcements (text) |
| `broadcast_all_bytes(data)` | All connections | Global announcements (binary) |
| `broadcast_local(topic, text)` | Topic subscribers (local) | Single-instance topic fan-out |
| `broadcast(topic, text)` | Topic subscribers (all instances) | Multi-instance topic fan-out |

`broadcast_all` and `broadcast_local` never touch the network - they fan out directly to local connections. `broadcast` additionally forwards to cluster peers via the native TCP mesh protocol.

---

## 6. Topic Subscriptions

```python
# Subscribe connection to topics
server.subscribe_connection(conn_id, ["prices", "news"])

# Unsubscribe from specific topics
server.unsubscribe_connection(conn_id, ["news"])

# Unsubscribe from all topics
server.unsubscribe_connection(conn_id, None)

# Get subscriber count for a topic
count = server.get_topic_subscriber_count("prices")
```

Topic subscriptions are managed per-connection. When a connection disconnects, its subscriptions are automatically cleaned up. In cluster mode, subscription interest is propagated to peers so that `broadcast()` only forwards messages to nodes that have active subscribers.

---

## 7. Presence Tracking

Presence tracking lets you monitor which users are active in a topic, with automatic join/leave events when users subscribe or disconnect.

### Enabling Presence

Pass `presence_enabled=True` in the constructor:

```python
server = RustWSEServer(
    "0.0.0.0", 5007,
    presence_enabled=True,
    presence_max_data_size=4096,   # max bytes per user's presence data
    presence_max_members=0,        # 0 = unlimited members per topic
)
```

### Subscribing with Presence Data

When subscribing a connection, pass a dict as the third argument to attach presence metadata:

```python
server.subscribe_connection(conn_id, ["chat-1"], {"status": "online", "name": "Alice"})
```

This triggers a `presence_join` event broadcast to all subscribers of `chat-1`. If the same user (same JWT `sub` claim) connects from multiple tabs, only the first connection emits a join event.

### Querying Members

```python
members = server.presence("chat-1")
# Returns dict: {
#     "alice": {"data": {"status": "online", "name": "Alice"}, "connections": 2},
#     "bob":   {"data": {"status": "away"},  "connections": 1},
# }
```

For lightweight counts without iterating all members:

```python
stats = server.presence_stats("chat-1")
# Returns dict: {"num_users": 2, "num_connections": 3}
```

### Updating Presence Data

To change a user's presence data across all their subscribed topics:

```python
server.update_presence(conn_id, {"status": "away"})
```

This broadcasts a `presence_update` event to every topic where the connection has presence.

### Presence Events

The following events appear in `drain_inbound()`:

| Event Type | Trigger | ev[2] payload |
|------------|---------|---------------|
| `"presence_join"` | First connection for a user subscribes to a topic | `{"topic": "...", "user_id": "...", "data": {...}}` |
| `"presence_leave"` | Last connection for a user leaves a topic | `{"topic": "...", "user_id": "...", "data": {...}}` |

These events are also broadcast to all WebSocket subscribers of the topic as structured messages:

```json
{"t": "presence_join", "p": {"user_id": "alice", "data": {"status": "online"}}}
{"t": "presence_leave", "p": {"user_id": "alice", "data": {"status": "online"}}}
{"t": "presence_update", "p": {"user_id": "alice", "data": {"status": "away"}}}
```

### Multi-Connection Behavior

Presence is tracked at the user level, not the connection level. If a user opens multiple browser tabs (multiple WebSocket connections with the same JWT `sub`):

- **Join**: only the first connection triggers a `presence_join` event
- **Leave**: only when all connections disconnect does a `presence_leave` fire
- **Data**: presence data reflects the most recent `subscribe_connection` or `update_presence` call

### Auto-Cleanup

When a connection disconnects, its presence is automatically removed from all topics. If it was the user's last connection in a topic, a `presence_leave` event is broadcast. A background sweep runs every 30 seconds to clean up any stale entries from connections that were not properly closed.

### Cluster Sync

In cluster mode, presence state is synchronized across all nodes. When a user joins or leaves on one node, the event is propagated to all peers via `PresenceUpdate` frames. On peer connect, a full presence state sync ensures all nodes have a consistent view. Conflict resolution uses last-write-wins based on wall-clock timestamps.

---

## 8. Message Recovery

Message recovery allows clients to catch up on missed messages after a reconnect. Enable it in the constructor:

```python
server = RustWSEServer(
    "0.0.0.0", 5007,
    recovery_enabled=True,
    recovery_buffer_size=256,    # slots per topic (power-of-2)
    recovery_ttl=300,            # 5 minutes
    recovery_max_messages=500,
    recovery_memory_budget=268435456,  # 256 MB
)
```

**Subscribing with recovery on reconnect:**

```python
result = server.subscribe_with_recovery(
    conn_id, ["prices"],
    recover=True,
    epoch=client_epoch,    # from previous session
    offset=client_offset,  # from previous session
)
# result: {"topics": {"prices": {"epoch": 123, "offset": 456, "recovered": True, "count": 12}}}
```

| Parameter | Description |
|-----------|-------------|
| `recovery_buffer_size` | Ring buffer slots per topic. Must be a power of 2. |
| `recovery_ttl` | Maximum age (seconds) of recoverable messages. |
| `recovery_max_messages` | Maximum messages returned in a single recovery. |
| `recovery_memory_budget` | Hard cap on total memory used by all recovery buffers. |

The client stores the `epoch` and `offset` from its last received message. On reconnect, it passes these values to `subscribe_with_recovery`, and the server replays any messages the client missed (up to the buffer limits).

---

## 9. Cluster Setup

Clustering connects multiple WSE instances into a broadcast mesh. Messages sent via `broadcast()` are forwarded to all peers.

**Basic cluster (no TLS):**

```python
server.connect_cluster(peers=["10.0.0.2:9999", "10.0.0.3:9999"])
```

**Cluster with mTLS:**

```python
server.connect_cluster(
    peers=["10.0.0.2:9999"],
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)
```

**Cluster with gossip discovery (seed nodes):**

```python
server.connect_cluster(
    peers=[],
    seeds=["10.0.0.2:9999"],
    cluster_addr="10.0.0.1:9999",
    cluster_port=9999,
)
```

With gossip discovery, nodes find each other through seed nodes and maintain membership automatically. New nodes only need to know at least one seed to join the cluster.

**Cluster status:**

```python
server.cluster_connected()      # bool - True if connected to at least one peer
server.cluster_peers_count()    # int - number of active peer connections
```

---

## 10. Health Monitoring

```python
health = server.health_snapshot()
```

Returns a dict with the current server state:

```python
{
    "connections": 150,
    "inbound_queue_depth": 0,
    "inbound_dropped": 0,
    "uptime_secs": 3600.5,
    "recovery_enabled": True,
    "recovery_topic_count": 5,
    "recovery_total_bytes": 1048576,
    "cluster_connected": True,
    "cluster_peer_count": 2,
    "cluster_messages_sent": 50000,
    "cluster_messages_delivered": 49950,
    "cluster_messages_dropped": 0,
    "cluster_bytes_sent": 1048576,
    "cluster_bytes_received": 1024000,
    "cluster_reconnect_count": 0,
    "cluster_unknown_message_types": 0,
    "cluster_dlq_size": 0,
    "presence_enabled": True,
    "presence_topics": 3,
    "presence_total_users": 25,
}
```

| Field | Description |
|-------|-------------|
| `connections` | Active WebSocket connections |
| `inbound_queue_depth` | Events waiting to be drained |
| `inbound_dropped` | Events dropped due to full queue (should be 0) |
| `uptime_secs` | Server uptime in seconds |
| `recovery_enabled` | Whether message recovery is active |
| `recovery_topic_count` | Topics with active recovery buffers |
| `recovery_total_bytes` | Memory used by recovery buffers |
| `cluster_connected` | Whether cluster mesh is active |
| `cluster_peer_count` | Number of connected cluster peers |
| `cluster_messages_sent` | Total messages forwarded to peers |
| `cluster_messages_delivered` | Total messages received from peers |
| `cluster_messages_dropped` | Messages that failed to send to peers |
| `cluster_bytes_sent` | Total bytes sent to peers |
| `cluster_bytes_received` | Total bytes received from peers |
| `cluster_reconnect_count` | Number of peer reconnections |
| `cluster_unknown_message_types` | Unrecognized frame types received |
| `cluster_dlq_size` | Dead letter queue entries |
| `presence_enabled` | Whether presence tracking is active |
| `presence_topics` | Topics with active presence tracking |
| `presence_total_users` | Total tracked users across all topics |

---

## 11. Connection Management

```python
# Get all connection IDs
conn_ids = server.get_connections()

# Get count (lock-free, AtomicUsize)
count = server.get_connection_count()

# Disconnect a specific connection
server.disconnect(conn_id)

# Check queue stats
depth = server.inbound_queue_depth()
dropped = server.inbound_dropped_count()

# Retrieve failed cluster messages from dead letter queue
dlq = server.get_cluster_dlq_entries()
```

`get_connection_count()` is lock-free and safe to call at high frequency (e.g., in health check endpoints). `get_connections()` takes a snapshot of all connection IDs and is slightly more expensive.

---

## 12. Client SDKs

### Python Client

```bash
pip install wse-client
```

```python
from wse_client import AsyncWSEClient

async with AsyncWSEClient("ws://localhost:5007/wse", token="...") as client:
    await client.subscribe(["prices"])
    async for event in client:
        print(event)
```

### TypeScript / React Client

```bash
npm install wse-client
```

**Recommended file structure:**

```
src/wse/
├── index.ts              # re-export: export * from 'wse-client'
├── config.ts             # App-specific: endpoints, auth refresh, topics
└── handlers/
    ├── index.ts           # registerAllHandlers()
    ├── BrokerHandlers.ts  # Domain-specific event handlers
    └── ...
```

Everything else (services, stores, hooks, protocols, utils, types, constants) comes from the package. Use a barrel re-export so existing `@/wse` imports keep working:

```typescript
// src/wse/index.ts
export * from 'wse-client';
```

**App-level config** (endpoints, auth, topics):

```typescript
// src/wse/config.ts
export const APP_DEFAULT_TOPICS = [
  'broker_events', 'account_events', 'market_data',
] as const;

export function getAppEndpoints(): string[] {
  const url = import.meta.env?.VITE_WSE_URL;  // Vite
  if (url) {
    const normalized = url.replace(/\/$/, '');
    return [normalized.endsWith('/wse') ? normalized : `${normalized}/wse`];
  }
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  return [`${protocol}://${window.location.host}/wse`];
}

export async function refreshAuthToken(): Promise<void> {
  const res = await fetch('/api/auth/refresh', {
    method: 'POST', credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
  });
  if (!res.ok) throw new Error(`Token refresh failed: ${res.status}`);
}
```

**Provider setup:**

```tsx
import { useWSE } from 'wse-client';
import type { UseWSEConfig } from 'wse-client';
import { APP_DEFAULT_TOPICS, getAppEndpoints, refreshAuthToken } from '@/wse/config';

export function WSEProvider({ children }: { children: ReactNode }) {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated);

  const wseConfig: UseWSEConfig = {
    endpoints: getAppEndpoints(),
    refreshAuthToken,
  };

  const wse = useWSE(
    isAuthenticated ? 'cookie' : undefined,  // 'cookie' = HttpOnly cookie auth
    [...APP_DEFAULT_TOPICS],
    wseConfig,
  );

  return <WSEContext.Provider value={wse}>{children}</WSEContext.Provider>;
}
```

**Key points:**
- `useWSE(token?, initialTopics?, config?)` — 3 arguments
- `refreshAuthToken` goes inside `UseWSEConfig`, not as a separate argument
- Pass `'cookie'` as token for HttpOnly cookie auth (browser sends cookies on WS upgrade)
- Pass `undefined` when not authenticated (prevents connection)

**Event handlers** — register by the `"t"` value the backend publisher sets:

```typescript
// handlers/index.ts
export function registerAllHandlers(messageProcessor: {
  registerHandler: (type: string, handler: (msg: any) => void) => void;
}): void {
  messageProcessor.registerHandler('broker_health_update', BrokerHandlers.handleHealthUpdate);
  messageProcessor.registerHandler('quote_update', MarketDataHandlers.handleQuoteUpdate);
}
```

Frontend never maps domain event names. It receives ready `"t"` + `"p"` from the backend publisher. See Section 14 for the publishing architecture.

Both clients handle reconnection with exponential backoff, automatic resubscription, and message recovery (when enabled on the server).

---

## 13. Production Deployment

### Server Sizing

- Set `max_connections` based on available memory. Approximately 1 KB per idle connection, more under active message flow.
- Use `drain_inbound(256, 50)` for optimal throughput. Batch size 256 amortizes GIL acquisition. Timeout 50ms balances latency and CPU usage.
- Monitor `health_snapshot()` for queue depth and dropped events. A non-zero `inbound_dropped` indicates the drain loop is too slow.

### TLS Termination

- Run behind a reverse proxy (nginx, HAProxy, Caddy) for TLS on client-facing connections. WSE serves WebSocket over plain TCP.
- Use mTLS for cluster peer connections in production. Configure `tls_ca`, `tls_cert`, `tls_key` in `connect_cluster()`.
- Always use `wss://` in production. The `access_token` cookie has `Secure` flag, which requires HTTPS.

### Recovery Configuration

- Enable recovery for topics where missed messages matter (chat, order updates, notifications).
- Set `recovery_memory_budget` to cap total memory across all topic buffers. Default 256 MB is suitable for most workloads.
- Choose `recovery_buffer_size` as a power of 2 based on message rate and expected reconnect window. For example: 1000 msg/s with 30s reconnect window needs at least 32768 slots.
- Recovery is local to each node. For cluster deployments, use sticky sessions (HAProxy `stick-table`, consistent hashing) to route clients back to the same node.

### Cluster Scaling

- Start with cluster mode when a single instance cannot handle the connection count or you need geographic distribution.
- Gossip discovery reduces configuration overhead. New nodes only need one seed address to join.
- Interest-based routing reduces inter-node bandwidth. Only topics with active subscribers on both nodes generate cross-node traffic.
- Monitor cluster health via `cluster_peers_count()` and the `cluster_messages_sent`/`cluster_messages_delivered` counters in `health_snapshot()`.

### Presence in Production

- Presence data size is bounded by `presence_max_data_size` (default 4 KB per user). Keep presence payloads small - status, display name, avatar URL.
- Use `presence_stats()` instead of `presence()` for health checks and dashboards. It returns O(1) counts without iterating members.
- In cluster mode, presence sync adds one PresenceUpdate frame per join/leave/update event. A full presence sync (PresenceFull frame) is sent when a new peer connects.

### Monitoring

Key metrics to watch:

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| `inbound_dropped` | `health_snapshot()` | > 0 |
| `inbound_queue_depth` | `health_snapshot()` | > 50% of `max_inbound_queue_size` |
| `cluster_peer_count` | `health_snapshot()` | < expected peer count |
| `recovery_total_bytes` | `health_snapshot()` | > 80% of `recovery_memory_budget` |
| Connection count | `get_connection_count()` | > 90% of `max_connections` |

---

## 14. Publishing Patterns

How to publish real-time events from backend to frontend via WSE. Two patterns depending on your architecture.

| Architecture | Pattern | Transformer | Mapping Table |
|-------------|---------|-------------|---------------|
| **Hexagonal / Publisher-based** | A | No | No — publisher method IS the mapping |
| **Event Sourcing / EventBus** | B | Yes (`rust_transform_event`) | Yes (`INTERNAL_TO_WS_EVENT_TYPE_MAP`) |

### Pattern A: Publisher-Based (Hexagonal Architecture)

Each publisher method explicitly sets `"t"` and `"p"`. No mapping table, no transformer. Best when publishers control what events are emitted.

```
Domain Layer                    →  Defines PORT (interface)
                                   WSEPublisherPort.publish_validation_result(user_id, result)
                                   Domain does NOT know about WebSocket, JSON, "t"/"p", topics.

Presentation Layer (adapter)    →  Publisher IMPLEMENTS port:
                                   → builds {"t": "status_update", "p": {...}, "id": uuid, "ts": iso, "v": 1}
                                   → calls server.broadcast_local(topic, json)

Rust WSE Server                 →  broadcast_local(topic, json) — fan-out. Transport only.

Frontend                        →  registerHandler("status_update", Handlers.handleStatusUpdate)
                                   Receives ready "t" + "p". No mapping.
```

**Publisher method = mapping:**

```python
class GatewayPublisher:
    def _build_event(self, event_type: str, payload: dict) -> dict:
        return {
            "t": event_type,
            "id": str(uuid7()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "p": payload,
            "v": 1,
        }

    async def publish_broker_health(self, user_id, connections, market_data):
        event = self._build_event("broker_health_update", {
            "connections": connections,
            "market_data_stream": market_data,
        })
        self._publish_to_rust(f"user:{user_id}:broker_health", event)
```

**Domain port / adapter split:**

```python
# domain/my_domain/ports/outbound/wse_publisher.py (PORT)
class WSEPublisherPort(ABC):
    @abstractmethod
    async def publish_result(self, user_id: str, result: Result) -> None: ...

# presentation/wse/publishers/my_publisher.py (ADAPTER)
class MyPublisher(WSEPublisherPort):
    async def publish_result(self, user_id: str, result: Result) -> None:
        event = self._build_event("result_update", {"score": result.score})
        self._publish_to_rust(f"user:{user_id}:events", event)
```

Domain says WHAT to publish. Publisher decides HOW (format, topic, transport).

**Infrastructure publishers** (no domain port needed):

For events originating in infrastructure (market data streams, broker health), the publisher can be standalone — no port required. Used when the event source is infrastructure rather than domain business logic.

**Adding a new event (3 files):**

1. Domain port — add abstract method
2. Publisher — implement method, set `"t"`
3. Frontend handler — `registerHandler('my_event', handler)`

No mapping tables, no transformer, no config updates.

### Pattern B: Event Sourcing (EventBus + Transformer)

Domain aggregates emit events (`UserProfileUpdated`, `BrokerAccountLinked`). These arrive as a single stream via EventBus. Publisher doesn't have dedicated methods per event — it receives whatever the aggregates emit.

```
EventBus (RedPanda / Kafka)     →  Aggregates emit: UserProfileUpdated, CounterpartyCreated, ...

Domain Publisher                →  1. Reads "event_type" from raw event
                                   2. Looks up WS type via INTERNAL_TO_WS_EVENT_TYPE_MAP
                                   3. rust_transform_event() builds wire format
                                   4. server.broadcast(topic, json)

Rust WSE Server                 →  broadcast(topic, json) — fan-out. Transport only.

Frontend                        →  registerHandler("user_profile_update", handler)
                                   Receives ready "t" + "p". No mapping.
```

**Mapping table** (inlined in publisher, not a separate file):

```python
INTERNAL_TO_WS_EVENT_TYPE_MAP: dict[str, str] = {
    'UserProfileUpdated': 'user_profile_update',
    'CounterpartyCreated': 'counterparty_update',
    'CustomsDeclarationSubmitted': 'customs_declaration_update',
    # ...
}
```

Events not in the table are silently filtered (not forwarded to frontend).

**Publisher:**

```python
from wse_server._wse_accel import rust_transform_event

class WSEDomainPublisher:
    async def _handle_event(self, event: dict):
        ws_type = INTERNAL_TO_WS_EVENT_TYPE_MAP.get(event.get("event_type"))
        if not ws_type:
            return  # filtered
        ws_message = rust_transform_event(event, next(self._seq), INTERNAL_TO_WS_EVENT_TYPE_MAP)
        if ws_message:
            self._server.broadcast(topic, json.dumps(ws_message, default=str))
```

**Adding a new event (2 steps):**

1. Mapping table — add one line: `'MyDomainEvent': 'my_event_update'`
2. Frontend handler — `registerHandler('my_event_update', handler)`

### Wire Format (Both Patterns)

```json
{
  "t": "broker_health_update",
  "id": "0194a5b2-...",
  "ts": "2026-03-01T12:00:00.000000Z",
  "p": { "connections": [], "market_data_stream": {} },
  "v": 1
}
```

| Field | Type | Description |
|-------|------|-------------|
| `t` | string | Event type (snake_case, set by publisher or transformer) |
| `id` | string | UUID7 message ID |
| `ts` | string | ISO 8601 timestamp |
| `p` | object | Payload (domain data, ready to consume) |
| `v` | int | Protocol version |

Optional: `c` (`"S"` for snapshots), `seq` (if sequencing enabled).

### Topic Naming

```
user:{user_id}:{domain}_events    # Per-user domain events
system:{category}                  # Global system events
```

### Pattern A vs Pattern B

| | Pattern A (Publisher-Based) | Pattern B (Event Sourcing) |
|---|---|---|
| Event source | Publisher methods (explicit) | EventBus stream (aggregates) |
| Who sets `"t"` | Publisher method directly | Mapping table lookup |
| Transformer | Not needed | `rust_transform_event()` |
| Mapping table | Anti-pattern | Required |
| Adding event | Add publisher method + handler | Add mapping line + handler |
| Best for | Services, gateways, infrastructure | Event sourcing, CQRS, DDD |

**Common to both:** Frontend never maps domain event names. Server is transport-only.
