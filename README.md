# WSE -- WebSocket Engine

**A complete, out-of-the-box solution for real-time communication between React, Python, and backend services.**

Three packages. Four lines of code. Your frontend and backend talk in real time.

[![CI](https://github.com/silvermpx/wse/actions/workflows/ci.yml/badge.svg)](https://github.com/silvermpx/wse/actions/workflows/ci.yml)
[![PyPI - Server](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![PyPI - Client](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why WSE?

Building real-time features between React and Python is painful. You need WebSocket handling, reconnection logic, message ordering, authentication, encryption, offline support, health monitoring. That's weeks of work before you ship a single feature.

**WSE gives you all of this out of the box.**

Install `wse-server` on your backend, `wse-client` on your frontend (React or Python). Everything works immediately: auto-reconnection, message encryption, sequence ordering, offline queues, health monitoring. No configuration required for the defaults. Override what you need.

The engine is Rust-accelerated via PyO3. **2M msg/s** point-to-point, **2.1M deliveries/s** fan-out broadcast, **500K concurrent connections** with zero message loss -- benchmarked on AMD EPYC 7502P (32 cores). Multi-instance horizontal scaling via Redis pub/sub. Sub-millisecond connection latency with Rust JWT authentication.

---

## Quick Start

### Server (Python) -- Router Mode

Embed WSE into your existing FastAPI app on the same port:

```python
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig

app = FastAPI()

wse = create_wse_router(WSEConfig(
    redis_url="redis://localhost:6379",
))

app.include_router(wse, prefix="/wse")

# Publish from anywhere in your app
await wse.publish("notifications", {"text": "Order shipped!", "order_id": 42})
```

### Server (Python) -- Standalone Mode

Run the Rust WebSocket server on a dedicated port for maximum throughput:

```python
from wse_server._wse_accel import RustWSEServer

server = RustWSEServer(
    "0.0.0.0", 5006,
    max_connections=10000,
    jwt_secret=b"your-secret-key",     # Rust JWT validation in handshake
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

Standalone mode gives you a dedicated Rust tokio runtime on its own port -- no FastAPI overhead, no GIL on the hot path. This is how WSE achieves 14M msg/s on JSON.

### Client (React)

```tsx
import { useWSE } from 'wse-client';

function Dashboard() {
  const { isConnected, connectionHealth } = useWSE({
    topics: ['notifications', 'live_data'],
    endpoints: ['ws://localhost:8000/wse'],
  });

  useEffect(() => {
    const handler = (e: CustomEvent) => {
      console.log('New notification:', e.detail);
    };
    window.addEventListener('notifications', handler);
    return () => window.removeEventListener('notifications', handler);
  }, []);

  return <div>Status: {connectionHealth}</div>;
}
```

That's it. Your React app receives real-time updates from your Python backend.

### Client (Python)

```python
from wse_client import connect

async with connect("ws://localhost:5006/wse", token="your-jwt") as client:
    await client.subscribe(["notifications", "live_data"])
    async for event in client:
        print(event.type, event.payload)
```

Same wire protocol, same features. Use the Python client for backend-to-backend communication, microservices, CLI tools, integration tests, or any non-browser use case.

---

## What You Get Out of the Box

Everything listed below works the moment you install. No extra setup.

### Reactive Interface

Real-time data flow from Python to React. One hook (`useWSE`) on the client, one `publish()` call on the server. Events appear in your components instantly.

### Auto-Reconnection

Exponential backoff with jitter. Connection drops? The client reconnects automatically. No lost messages -- offline queue with IndexedDB persistence stores messages while disconnected and replays them on reconnect.

### End-to-End Encryption

ECDH P-256 key exchange with AES-GCM-256 per connection. Each connection negotiates a unique session key during the handshake (server_ready / client_hello), then all sensitive messages are encrypted end-to-end. HMAC-SHA256 message signing for integrity. Pluggable encryption and token providers via Python protocols.

### Message Ordering

Sequence numbers with gap detection and reordering buffer. Messages arrive in order even under high load or network instability. Out-of-order messages are buffered and delivered once the gap fills.

### Authentication

JWT-based with HS256, validated in Rust during the WebSocket handshake (zero GIL, 0.01ms decode). Per-connection, per-topic access control. Cookie-based token extraction for seamless browser auth. Fallback to Python auth handler when Rust JWT is not configured.

### Health Monitoring

Connection quality scoring (excellent / good / fair / poor), latency tracking, jitter analysis, packet loss detection. Your UI knows when the connection is degraded and can react accordingly.

### Scaling

Redis pub/sub for multi-instance fan-out. Run N server instances behind a load balancer -- publish on any instance, all subscribers receive the message regardless of which instance they're connected to. Pipelined PUBLISH (up to 64 per round-trip), circuit breaker, exponential backoff with jitter, dead letter queue for failed messages. Capacity scales linearly with instances: tested up to 1.04M deliveries/s per instance.

### Rust Performance

Compression, sequencing, filtering, rate limiting, and the WebSocket server itself are implemented in Rust via PyO3. Python API stays the same. Rust accelerates transparently.

---

## Full Feature List

### Server (Python + Rust)

| Feature | Description |
|---------|-------------|
| **Drain Mode** | Batch-polling inbound events from Rust. One GIL acquisition per batch (up to 256 messages) instead of per-message Python callbacks. Condvar-based wakeup for zero busy-wait. |
| **Write Coalescing** | Outbound pipeline: `feed()` + batch `try_recv()` + single `flush()`. Reduces syscalls under load by coalescing multiple messages into one write. |
| **Ping/Pong in Rust** | Heartbeat handled entirely in Rust with zero Python round-trips. Configurable intervals. TCP_NODELAY on accept for minimal latency. |
| **5-Level Priority Queue** | CRITICAL(10), HIGH(8), NORMAL(5), LOW(3), BACKGROUND(1). Smart dropping under backpressure: lower-priority messages are dropped first. Batch dequeue ordered by priority. |
| **Dead Letter Queue** | Redis-backed DLQ for failed messages. 7-day TTL, 1000-message cap per channel. Manual replay via `replay_dlq_message()`. Prometheus metrics for DLQ size and replay count. |
| **MongoDB-like Filters** | 14 operators: `$eq`, `$ne`, `$gt`, `$lt`, `$gte`, `$lte`, `$in`, `$nin`, `$regex`, `$exists`, `$contains`, `$startswith`, `$endswith`. Logical: `$and`, `$or`. Dot-notation for nested fields (`payload.price`). Compiled regex cache. |
| **Event Sequencer** | Monotonic sequence numbers with AHashSet dedup. Size-based and age-based eviction. Gap detection on both server and client. |
| **Compression** | Flate2 zlib with configurable levels (1-9). Adaptive threshold -- only compress when payload exceeds size limit. Binary mode via msgpack (rmp-serde) for 30% smaller payloads. |
| **Rate Limiter** | Atomic token-bucket rate limiter in Rust. Per-connection rate enforcement. 100K tokens capacity, 10K tokens/sec refill. |
| **Message Deduplication** | AHashSet-backed dedup with bounded queue. Prevents duplicate delivery across reconnections and Redis fan-out. |
| **Wire Envelope** | Protocol v1: `{t, id, ts, seq, p, v}`. Generic payload extraction with automatic type conversion (UUID, datetime, Enum, bytes to JSON-safe primitives). Latency tracking (`latency_ms` field). |
| **Snapshot Provider** | Protocol for initial state delivery. Implement `get_snapshot(user_id, topics)` and clients receive current state immediately on subscribe -- no waiting for the next publish cycle. |
| **Circuit Breaker** | Three-state machine (CLOSED / OPEN / HALF_OPEN). Sliding-window failure tracking. Automatic recovery probes. Prevents cascade failures when downstream services are unhealthy. |
| **Message Categories** | `S` (snapshot), `U` (update), `WSE` (system). Category prefixing for client-side routing and filtering. |
| **Multi-Instance Orchestration** | Horizontal scaling via Redis pub/sub. Publish on any instance, all subscribers receive the message. Pipelined PUBLISH (64 commands/batch, 3 retries), circuit breaker (10-fail threshold, 60s reset), exponential backoff with jitter, dead letter queue (1000-entry ring buffer). 1.04M deliveries/s per instance, linear scaling with N instances. |
| **PubSub Bus** | Redis pub/sub with PSUBSCRIBE pattern matching. Glob wildcard topic routing (`user:*:events`). orjson fast-path serialization. Non-blocking handler invocation. |
| **Pluggable Security** | `EncryptionProvider` and `TokenProvider` protocols. Built-in: AES-GCM-256 with ECDH P-256 key exchange, HMAC-SHA256 signing, selective message signing. Rust-accelerated crypto (SHA-256, HMAC, AES-GCM, ECDH). |
| **Rust JWT Authentication** | HS256 JWT validation in Rust during the WebSocket handshake. Zero GIL acquisition on the connection critical path. 0.01ms decode (85x faster than Python). Cookie extraction and `server_ready` sent from Rust before Python runs. |
| **Lock-Free Server Queries** | `get_connection_count()` uses `AtomicUsize` — zero GIL, zero blocking, safe to call from async Python handlers. No channel round-trip to the tokio runtime. |
| **Inbound MsgPack** | Binary frames from msgpack clients are parsed in Rust via rmpv. Python receives pre-parsed dicts regardless of wire format. Zero Python overhead for msgpack connections. |
| **Connection Metrics** | Prometheus-compatible stubs for: messages sent/received, publish latency, DLQ size, handler errors, circuit breaker state. Drop-in Prometheus integration or use the built-in stubs. |

### Client (React + TypeScript)

| Feature | Description |
|---------|-------------|
| **useWSE Hook** | Single React hook for the entire WebSocket lifecycle. Accepts topics, endpoints, auth tokens. Returns `isConnected`, `connectionHealth`, connection controls. |
| **Connection Pool** | Multi-endpoint support with health-scored failover. Three load-balancing strategies: weighted-random, least-connections, round-robin. Automatic health checks with latency tracking. |
| **Adaptive Quality Manager** | Adjusts React Query defaults based on connection quality. Excellent: `staleTime: Infinity` (pure WebSocket). Poor: aggressive polling fallback. Dispatches `wse:quality-change` events. Optional QueryClient integration. |
| **Offline Queue** | IndexedDB-backed persistent queue. Messages are stored when disconnected and replayed on reconnect, ordered by priority. Configurable max size and TTL. |
| **Network Monitor** | Real-time latency, jitter, and packet-loss analysis. Determines connection quality (excellent / good / fair / poor). Generates diagnostic suggestions. |
| **Event Sequencer** | Client-side sequence validation with gap detection. Out-of-order buffer for reordering. Duplicate detection via seen-ID window with age-based eviction. |
| **Circuit Breaker** | Client-side circuit breaker for connection attempts. Prevents reconnection storms when the server is down. Configurable failure threshold and recovery timeout. |
| **Compression + msgpack** | Client-side decompression (pako zlib) and msgpack decoding. Automatic detection of binary vs JSON frames. |
| **Zustand Stores** | `useWSEStore` for connection state, latency history, diagnostics. `useMessageQueueStore` for message buffering with priority. Lightweight, no boilerplate. |
| **Rate Limiter** | Client-side token-bucket rate limiter for outbound messages. Prevents flooding the server. |
| **Security Manager** | Client-side HMAC verification and optional decryption. Validates message integrity before dispatching to handlers. |

### Client (Python)

| Feature | Description |
|---------|-------------|
| **Async + Sync API** | `AsyncWSEClient` with async context manager and async iterator. `SyncWSEClient` wrapper for threaded/synchronous code. |
| **Connection Manager** | Auto-reconnection with 4 strategies (exponential, linear, fibonacci, adaptive). Jitter, configurable max attempts. Heartbeat with PING/PONG. |
| **Connection Pool** | Multi-endpoint support with health scoring. Weighted-random, least-connections, round-robin load balancing. |
| **Circuit Breaker** | Three-state machine (CLOSED / OPEN / HALF_OPEN). Prevents reconnection storms. |
| **Rate Limiter** | Client-side token-bucket rate limiter for outbound messages. |
| **Event Sequencer** | Duplicate detection (10K ID window) and out-of-order reordering buffer. |
| **Network Monitor** | Latency, jitter, packet loss analysis. Connection quality scoring. |
| **Security** | ECDH P-256 key exchange, AES-GCM-256 encryption, HMAC-SHA256 signing. Wire-compatible with server and TypeScript client. |
| **Compression + msgpack** | Zlib decompression and msgpack decoding. Automatic binary frame detection. |

---

## Performance

Rust-accelerated engine via PyO3. All numbers below from AMD EPYC 7502P (32 cores, 128 GB).

### Point-to-Point Throughput

| Metric | 64 Workers | 128 Workers |
|--------|-----------|-------------|
| **Sustained throughput (JSON)** | **2,045,000 msg/s** | 2,013,000 msg/s |
| **Sustained throughput (MsgPack)** | **2,072,000 msg/s** | 2,041,000 msg/s |
| **Burst throughput (JSON)** | 1,557,000 msg/s | **1,836,000 msg/s** |
| **Connection latency** | 2.60 ms median | 2.96 ms median |
| **Ping RTT** | 0.26 ms median | 0.41 ms median |
| **64KB messages** | **256K msg/s (16.0 GB/s)** | 238K msg/s (14.9 GB/s) |

### Fan-out Broadcast

Server broadcasts to N subscribers. Zero message loss at every tier.

| Subscribers | Deliveries/s | Bandwidth | Gaps |
|-------------|-------------|-----------|------|
| 10 | **2.1M** | 295 MB/s | 0 |
| 1,000 | 1.4M | 185 MB/s | 0 |
| 10,000 | 1.2M | 163 MB/s | 0 |
| 100,000 | 1.7M | 234 MB/s | 0 |
| 500,000 | 1.4M | 128 MB/s | 0 |

### Multi-Instance (Redis)

Two server processes coordinated via Redis pub/sub. Publisher on Server A, subscribers on Server B.

| Subscribers (Server B) | Deliveries/s | Gaps |
|-------------------------|-------------|------|
| 10 | 448K | 0 |
| 500 | **1.04M** | 0 |
| 5,000 | 778K | 0 |

Capacity scales linearly: 2 instances = ~2M del/s, 3 instances = ~3M del/s.

See [BENCHMARKS.md](docs/BENCHMARKS.md) and [Fan-out Benchmarks](docs/BENCHMARKS_FANOUT.md) for full results and methodology.

---

## Use Cases

WSE works for any real-time communication between frontend and backend:

- **Live dashboards** -- stock prices, sensor data, analytics, monitoring panels
- **Notifications** -- order updates, alerts, system events pushed to the browser
- **Collaborative apps** -- shared cursors, document editing, whiteboarding
- **Chat and messaging** -- group chats, DMs, typing indicators, read receipts
- **IoT and telemetry** -- device status, real-time metrics, command and control
- **Gaming** -- game state sync, leaderboards, matchmaking updates

---

## Installation

```bash
# Server (Python) -- includes prebuilt Rust engine
pip install wse-server

# Client (React/TypeScript)
npm install wse-client

# Client (Python) -- pure Python, no Rust required
pip install wse-client
```

Server: prebuilt wheels for Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), and Windows. Python 3.12+ (ABI3 stable -- one wheel per platform).

Python client: pure Python, Python 3.11+. Optional extras: `pip install wse-client[crypto]` for encryption, `pip install wse-client[all]` for everything.

---

## Architecture

```
React Client (TypeScript)       Python Client              Server (Python + Rust)
========================        ========================   ========================

useWSE hook                     AsyncWSEClient             FastAPI Router (/wse)
    |                               |                        -- OR --
    v                               v                      RustWSEServer (:5006)
ConnectionPool                  ConnectionPool                  |
    |  (multi-endpoint,             |  (multi-endpoint,         v
    |   health scoring)             |   health scoring)   Rust Engine (PyO3)
    v                               v                          |
ConnectionManager               ConnectionManager              v
    |  (auto-reconnect,             |  (auto-reconnect,   EventTransformer
    |   circuit breaker)            |   circuit breaker)       |
    v                               v                          v
MessageProcessor                MessageCodec              PriorityQueue
    |  (decompress, verify,         |  (decompress,            |
    |   sequence, dispatch)         |   sequence, dedup)       v
    v                               v                     Sequencer + Dedup
AdaptiveQualityManager          NetworkMonitor                 |
    |                               |  (quality scoring,       v
    v                               |   latency, jitter)  Compression + Rate Limiter
Zustand Store                       v                          |
    |                           Event handlers /               v
    v                           async iterator            PubSub Bus (Redis)
React Components                                               |
                                                               v
                                                          Dead Letter Queue
```

**Wire format (v1):**

```json
{
  "v": 1,
  "id": "019503a1-...",
  "t": "price_update",
  "ts": "2026-01-15T10:30:00Z",
  "seq": 42,
  "p": { "symbol": "AAPL", "price": 187.42 }
}
```

---

## Packages

| Package | Registry | Language | Install |
|---------|----------|----------|---------|
| `wse-server` | [PyPI](https://pypi.org/project/wse-server/) | Python + Rust | `pip install wse-server` |
| `wse-client` | [npm](https://www.npmjs.com/package/wse-client) | TypeScript + React | `npm install wse-client` |
| `wse-client` | [PyPI](https://pypi.org/project/wse-client/) | Python | `pip install wse-client` |

All packages are standalone. No shared dependencies between server and clients.

---

## Documentation

| Document | Description |
|----------|-------------|
| [Protocol Spec](docs/PROTOCOL.md) | Wire format, versioning, encryption |
| [Architecture](docs/ARCHITECTURE.md) | System design, data flow |
| [Benchmarks](docs/BENCHMARKS.md) | Methodology, results, comparisons |
| [Security Model](docs/SECURITY.md) | Encryption, auth, threat model |
| [Integration Guide](docs/INTEGRATION.md) | FastAPI setup, Redis, deployment |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Rust engine | PyO3 + maturin | Compression, sequencing, filtering, rate limiting, WebSocket server |
| Server framework | FastAPI + Starlette | ASGI WebSocket handling |
| Serialization | orjson (Rust) | Zero-copy JSON |
| Binary protocol | msgpack (rmp-serde) | 30% smaller payloads |
| Encryption | AES-GCM-256 + ECDH P-256 (Rust) | Per-connection E2E encryption with key exchange |
| Message signing | HMAC-SHA256 (Rust) | Per-message integrity verification |
| Authentication | Rust JWT (HS256) | Zero-GIL token validation in handshake |
| Pub/Sub backbone | Redis Pub/Sub | Multi-process fan-out |
| Dead Letter Queue | Redis Lists | Failed message recovery |
| Client state | Zustand | Lightweight React store |
| Client hooks | React 18+ | useWSE hook with TypeScript |
| Offline storage | IndexedDB | Persistent offline queue |
| Python client | websockets + cryptography | Async/sync WebSocket client |
| Build system | maturin | Rust+Python hybrid wheels |

---

## License

MIT
