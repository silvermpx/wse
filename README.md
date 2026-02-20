# WSE — WebSocket Engine

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

The engine is Rust-accelerated via PyO3. Up to **1.1M msg/s** burst throughput. 285K msg/s sustained with JSON.

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

**Reactive Interface** -- real-time data flow from Python to React, with a single hook (`useWSE`) and a publish call on the server. Events appear in your components instantly.

**Auto-Reconnection** -- exponential backoff with jitter. Connection drops? The client reconnects automatically. No lost messages thanks to offline queue with IndexedDB persistence.

**End-to-End Encryption** -- AES-256-GCM per channel, HMAC-SHA256 message signing. Encrypted before it leaves the server, decrypted in the browser. No plaintext on the wire.

**Message Ordering** -- sequence numbers with gap detection and reordering buffer. Messages arrive in order, even under high load or network instability.

**Authentication** -- JWT-based with configurable claims. Per-connection, per-topic access control. Plug in your own auth handler or use the built-in one.

**Health Monitoring** -- connection quality scoring, latency tracking, circuit breaker. Your UI knows when the connection is degraded and can react accordingly.

**Scaling** -- Redis pub/sub for multi-process fan-out. Run multiple server workers behind a load balancer. Clients get messages from any worker.

**Rust Performance** -- compression, sequencing, filtering, rate limiting, and the WebSocket server itself are implemented in Rust via PyO3. Python API stays the same. Rust accelerates transparently.

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

## Performance

Rust-accelerated engine via PyO3. Benchmarked on Apple M3, single process, 1KB JSON.

| Mode | Throughput | Latency (p50) | Latency (p99) |
|------|-----------|---------------|---------------|
| **Rust (binary)** | **1,100,000 msg/s** | **0.009 ms** | **0.04 ms** |
| **Rust (JSON)** | **285,000 msg/s** | **0.03 ms** | **0.15 ms** |
| Pure Python | 106,000 msg/s | 0.09 ms | 0.4 ms |

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
ConnectionManager                        Rust Engine (PyO3)
    |  (auto-reconnect,                      |  (compress, sequence,
    |   circuit breaker)                     |   filter, rate limit)
    v                                        v
MessageProcessor                         EventTransformer
    |  (decompress, verify,                  |  (wire envelope,
    |   sequence, dispatch)                  |   msgpack/JSON)
    v                                        v
Zustand Store                            PubSub Bus
    |                                        |
    v                                        v
React Components                         Redis (multi-process)
```

**Wire format (v2):**

```json
{
  "v": 2,
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
| Rust engine | PyO3 + maturin | Compression, sequencing, filtering, rate limiting |
| Server framework | FastAPI + Starlette | ASGI WebSocket handling |
| Serialization | orjson (Rust) | Zero-copy JSON |
| Binary protocol | msgpack (rmp-serde) | 30% smaller payloads |
| Encryption | AES-256-GCM (Rust) | Per-channel E2E encryption |
| Authentication | PyJWT | Token verification |
| Pub/Sub backbone | Redis Streams | Multi-process fan-out |
| Client state | Zustand | Lightweight React store |
| Client hooks | React 18+ | useWSE hook with TypeScript |
| Build system | maturin | Rust+Python hybrid wheels |

---

## License

MIT
