# WSE — WebSocket Engine

**Real-time React + Python, out of the box.**

Install two packages. Connect your FastAPI backend to your React frontend. Live data in minutes, not weeks.

[![CI](https://github.com/silvermpx/wse/actions/workflows/ci.yml/badge.svg)](https://github.com/silvermpx/wse/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What is WSE?

WSE is a production-ready WebSocket engine that connects Python backends to React frontends in real time. It handles everything you'd otherwise build yourself: reconnection, authentication, encryption, message ordering, offline queues, health monitoring.

**Server:** `pip install wse-server` -- FastAPI router, 4 lines to set up.
**Client:** `npm install wse-client` -- React hook, works with Zustand.

The engine is Rust-accelerated via PyO3. Up to **1.1M msg/s** burst throughput (single process, binary mode). 285K msg/s sustained with JSON.

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

That's it. Your React app now receives real-time updates from your Python backend.

---

## Use Cases

- **Live dashboards** -- stock prices, sensor data, analytics, monitoring panels
- **Notifications** -- order updates, alerts, system events pushed to the browser
- **Collaborative apps** -- shared state, cursors, document editing, whiteboarding
- **Chat and messaging** -- group chats, DMs, typing indicators, read receipts
- **IoT and telemetry** -- device status, real-time metrics, command and control
- **Gaming** -- game state sync, leaderboards, matchmaking updates

---

## Performance

Rust-accelerated engine via PyO3. Benchmarked on Apple M3 Max, single process, 1KB JSON.

| Mode | Throughput | Latency (p50) | Latency (p99) |
|------|-----------|---------------|---------------|
| **Rust (binary)** | **1,100,000 msg/s** | **0.009 ms** | **0.04 ms** |
| **Rust (JSON)** | **285,000 msg/s** | **0.03 ms** | **0.15 ms** |
| Pure Python | 106,000 msg/s | 0.09 ms | 0.4 ms |

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology and reproducibility.

---

## Features

**Rust-Accelerated Core**
- Compression, sequencing, filtering, rate limiting -- all in Rust via PyO3
- Zero-copy JSON with orjson, binary protocol with msgpack
- Python API stays the same -- Rust accelerates transparently

**Security**
- End-to-end AES-256-GCM encryption per channel
- HMAC-SHA256 message signing and integrity verification
- JWT authentication with configurable claims
- Per-connection topic-level access control

**Reliability**
- Automatic reconnection with exponential backoff + jitter
- Sequence numbers with gap detection and reordering buffer
- Circuit breaker with configurable failure threshold
- Offline queue with IndexedDB persistence (messages survive page reload)
- Connection health monitoring with adaptive quality scoring

**Scaling**
- Redis pub/sub for multi-process fan-out
- Horizontal scaling across workers with shared state
- Per-topic subscriptions -- clients receive only what they subscribe to

**Developer Experience**
- 4-line server setup with FastAPI
- React hook (`useWSE`) with full TypeScript types
- Zustand store integration for state management
- Protocol versioning (v2) for backward compatibility
- Priority queues (5 levels) for message ordering

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
