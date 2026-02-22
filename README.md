# WSE -- WebSocket Engine

**A complete, out-of-the-box solution for building reactive interfaces with React and Python.**

Two packages. Four lines of code. Your frontend and backend talk in real time.

[![CI](https://github.com/silvermpx/wse/actions/workflows/ci.yml/badge.svg)](https://github.com/silvermpx/wse/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Why WSE?

Building real-time features between React and Python is painful. You need WebSocket handling, reconnection logic, message ordering, authentication, encryption, offline support, health monitoring. That's weeks of work before you ship a single feature.

**WSE gives you all of this out of the box.**

Install `wse-server` on your backend, `wse-client` on your frontend. Everything works immediately: auto-reconnection, message encryption, sequence ordering, offline queues, health monitoring. No configuration required for the defaults. Override what you need.

The engine is Rust-accelerated via PyO3. Up to **0.5M msg/s** JSON burst throughput (10 workers, Apple M2). 350K+ msg/s sustained. Sub-millisecond connection latency (0.53ms median) with Rust JWT authentication.

---

## Quick Start

### Server (Python)

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

Redis pub/sub for multi-process fan-out. Run multiple server workers behind a load balancer. Clients get messages from any worker. Fire-and-forget delivery with sub-millisecond latency.

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
| **Wire Envelope** | Protocol v2: `{t, id, ts, seq, p, v}`. Generic payload extraction with automatic type conversion (UUID, datetime, Enum, bytes to JSON-safe primitives). Latency tracking (`latency_ms` field). |
| **Snapshot Provider** | Protocol for initial state delivery. Implement `get_snapshot(user_id, topics)` and clients receive current state immediately on subscribe -- no waiting for the next publish cycle. |
| **Circuit Breaker** | Three-state machine (CLOSED / OPEN / HALF_OPEN). Sliding-window failure tracking. Automatic recovery probes. Prevents cascade failures when downstream services are unhealthy. |
| **Message Categories** | `S` (snapshot), `U` (update), `WSE` (system). Category prefixing for client-side routing and filtering. |
| **PubSub Bus** | Redis pub/sub with PSUBSCRIBE pattern matching. orjson fast-path serialization. Custom JSON encoder for UUID, datetime, Decimal. Non-blocking handler invocation. |
| **Pluggable Security** | `EncryptionProvider` and `TokenProvider` protocols. Built-in: AES-GCM-256 with ECDH P-256 key exchange, HMAC-SHA256 signing, selective message signing. Rust-accelerated crypto (SHA-256, HMAC, AES-GCM, ECDH). |
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

---

## Performance

Rust-accelerated engine via PyO3. Benchmarked on Apple M2 (8 cores), localhost.

| Metric | Single Client | 10 Workers |
|--------|--------------|------------|
| **Sustained throughput (JSON)** | 113,000 msg/s | **356,000 msg/s** |
| **Sustained throughput (MsgPack)** | 116,000 msg/s | **345,000 msg/s** |
| **Burst throughput (JSON)** | 106,000 msg/s | **488,000 msg/s** |
| **Connection latency** | **0.53 ms** median | 2.20 ms median |
| **Ping RTT** | **0.09 ms** median | 0.18 ms median |
| **64KB messages** | 43K msg/s (2.7 GB/s) | **164K msg/s (10.2 GB/s)** |

See [BENCHMARKS.md](docs/BENCHMARKS.md) for full results and methodology.

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
```

Prebuilt wheels for Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), and Windows.
Python 3.12+ (ABI3 stable -- one wheel per platform).

---

## Architecture

```
Client (React + TypeScript)              Server (Python + Rust)
========================                 ========================

useWSE hook                              FastAPI Router (/wse)
    |                                        |
    v                                        v
ConnectionPool                           Rust Engine (PyO3)
    |  (multi-endpoint,                      |  (drain mode,
    |   health scoring)                      |   write coalescing)
    v                                        v
ConnectionManager                        EventTransformer
    |  (auto-reconnect,                      |  (wire envelope,
    |   circuit breaker)                     |   type conversion)
    v                                        v
MessageProcessor                         PriorityQueue
    |  (decompress, verify,                  |  (5 levels,
    |   sequence, dispatch)                  |   smart dropping)
    v                                        v
AdaptiveQualityManager                   Sequencer + Dedup
    |  (quality scoring,                     |  (AHashSet,
    |   React Query tuning)                  |   gap detection)
    v                                        v
Zustand Store                            Compression + Rate Limiter
    |                                        |  (flate2, token bucket)
    v                                        v
React Components                         PubSub Bus (Redis)
                                             |
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

Both packages are standalone. No shared dependencies between server and client.

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
| Build system | maturin | Rust+Python hybrid wheels |

---

## License

MIT
