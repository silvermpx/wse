# WSE — WebSocket Engine

**Up to 1.1M msg/s. End-to-end encrypted. Rust-accelerated.**

<!-- badges: pypi, npm, ci, coverage, license -->

---

## Performance

Rust-accelerated engine via PyO3. Benchmarked on Apple M3 Max, single process, 1KB JSON.

| Mode | Throughput | Latency (p50) | Latency (p99) |
|------|-----------|---------------|---------------|
| **Rust (binary)** | **1,100,000 msg/s** | **0.009 ms** | **0.04 ms** |
| **Rust (JSON)** | **285,000 msg/s** | **0.03 ms** | **0.15 ms** |
| Pure Python | 106,000 msg/s | 0.09 ms | 0.4 ms |

Compared to alternatives:

| Library | Throughput | Encrypted | Priority Queue | Offline Queue |
|---------|-----------|-----------|----------------|---------------|
| **WSE** | **1.1M msg/s** | AES-256-GCM | Yes (5-level) | Yes (IndexedDB) |
| ws (Node) | ~50K msg/s | No | No | No |
| Socket.IO | ~25K msg/s | No | No | No |
| SSE | ~10K msg/s | No | No | No |

See [benchmarks/](benchmarks/) and [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for methodology.

---

## Installation

```bash
# Server (Python) — includes prebuilt Rust engine
pip install wse-server

# Client (React/TypeScript)
npm install wse-client
```

Prebuilt wheels for Linux (x86_64, aarch64), macOS (x86_64, arm64), and Windows (x86_64).
Python 3.12+ (ABI3 stable — one wheel per platform).

---

## Features

**Rust-Accelerated Engine**
- Compression (zlib via flate2), sequencing, filtering, rate limiting — all in Rust
- PyO3 native module — no subprocess, no FFI overhead
- Python API unchanged — drop-in acceleration

**Security**
- End-to-end AES-256-GCM encryption per channel
- HMAC-SHA256 message signing
- JWT authentication with configurable claims
- Per-connection scope enforcement

**Reliability**
- Automatic reconnection with exponential backoff + jitter
- Sequence numbers with gap detection and out-of-order buffering
- Circuit breaker with configurable threshold
- Offline queue with IndexedDB persistence
- Connection health monitoring with adaptive quality

**Scaling**
- Redis pub/sub for multi-process fan-out
- Horizontal scaling across workers with shared state
- Per-topic subscription — clients receive only what they need

**Developer Experience**
- 4-line server setup with FastAPI
- React hook with full TypeScript types
- Zustand store integration
- Protocol versioning (v2) for backward compatibility

---

## Quick Start

### Server (Python)

```python
from fastapi import FastAPI
from wse_server import create_wse_router, WSEConfig

app = FastAPI()

wse = create_wse_router(WSEConfig(
    redis_url="redis://localhost:6379",
    encryption_key="your-32-byte-key-here...",
))

app.include_router(wse, prefix="/wse")
```

Publish from anywhere:

```python
await wse.publish("prices", {"symbol": "AAPL", "price": 187.42})
```

### Client (React)

```typescript
import { useWSE } from 'wse-client';

function PriceDisplay() {
  const wse = useWSE(authToken, ['prices'], {
    endpoints: ['ws://localhost:8000/wse'],
  });

  useEffect(() => {
    const handler = (e: CustomEvent) => {
      console.log('Price update:', e.detail);
    };
    window.addEventListener('price_update', handler);
    return () => window.removeEventListener('price_update', handler);
  }, []);

  return <div>Status: {wse.connectionHealth}</div>;
}
```

---

## Architecture

```
Client (React/TypeScript)              Server (Python + Rust)
---------------------                  ---------------------

useWSE hook                            FastAPI Router
    |                                      |
    v                                      v
ConnectionManager                      Rust WSE Server
    |                                      |
    v                                      v
MessageProcessor                       EventTransformer (Rust)
    |                                      |
    v                                      v
Zustand Store                          PubSub Bus
    |                                      |
    v                                      v
React Components                       Redis Streams (optional)
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

Category prefixes on the wire: `WSE{` (system), `S{` (snapshot), `U{` (update).

---

## Packages

| Package | Registry | Language | Install |
|---------|----------|----------|---------|
| `wse-server` | PyPI | Python + Rust | `pip install wse-server` |
| `wse-client` | npm | TypeScript + React | `npm install wse-client` |

Both packages are standalone — no shared dependencies between server and client.

---

## When NOT to Use WSE

| Use case | Better tool | Why |
|----------|-------------|-----|
| Simple chat rooms | Socket.IO | Built-in rooms, namespaces, broadcast |
| Service-to-service RPC | gRPC | Schema enforcement, bidirectional streaming |
| Message queues | RabbitMQ, Kafka | Persistent queues, consumer groups |
| Static dashboards | SSE | Simpler protocol, no WebSocket overhead |
| File uploads | HTTP multipart | WebSocket not designed for large binaries |

WSE is for: real-time UI updates, live data feeds, monitoring dashboards, collaborative state sync, and high-throughput server-to-client event delivery.

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
| Pub/Sub backbone | Redis Streams | Multi-process fan-out, replay |
| Client state | Zustand | Lightweight React store |
| Client hooks | React 18+ | useWSE hook |
| Build system | maturin | Rust+Python hybrid wheels |

---

## License

MIT
