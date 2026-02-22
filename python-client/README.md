# wse-client

Python client for [WSE (WebSocket Engine)](https://github.com/silvermpx/wse) -- real-time event streaming with auto-reconnection, compression, encryption, and connection resilience.

[![PyPI](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![Python](https://img.shields.io/pypi/pyversions/wse-client)](https://pypi.org/project/wse-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

Feature parity with the TypeScript client. Pure Python, no Rust dependency.

## Installation

```bash
pip install wse-client
```

Optional extras:

```bash
pip install wse-client[crypto]   # E2E encryption (ECDH + AES-GCM)
pip install wse-client[msgpack]  # MessagePack binary encoding
pip install wse-client[orjson]   # Faster JSON (de)serialization
pip install wse-client[all]      # Everything
```

## Quick Start

### Async (recommended)

```python
from wse_client import AsyncWSEClient

async with AsyncWSEClient("ws://localhost:5006/wse", token="jwt...") as client:
    await client.subscribe(["notifications", "trades"])

    async for event in client:
        print(event.type, event.payload)
```

### Sync

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5006/wse", token="jwt...")
client.connect()
client.subscribe(["notifications"])

event = client.recv(timeout=5.0)
print(event.type, event.payload)

client.close()
```

### Callbacks

```python
client = SyncWSEClient("ws://localhost:5006/wse", token="jwt...")

@client.on("notifications")
def handle(event):
    print(event.payload)

@client.on("*")
def catch_all(event):
    print(f"[{event.type}] {event.payload}")

client.connect()
client.run_forever()
```

## API Reference

### AsyncWSEClient

| Method | Description |
|--------|-------------|
| `connect()` | Open WebSocket connection |
| `disconnect()` | Close connection gracefully |
| `send(type, payload, priority=5)` | Send a message |
| `send_with_retry(type, payload, max_retries=5)` | Send with exponential backoff |
| `send_batch(messages)` | Send multiple messages in one frame |
| `subscribe(topics)` | Subscribe to event topics |
| `unsubscribe(topics)` | Unsubscribe from topics |
| `request_snapshot(topics)` | Request current state for topics |
| `on(event_type)` | Register event handler (decorator) |
| `off(event_type, handler)` | Remove event handler |
| `recv(timeout=None)` | Receive next event explicitly |
| `force_reconnect()` | Force a reconnection |
| `change_endpoint(url)` | Switch to a different server |
| `get_stats()` | Connection and message statistics |

**Properties:** `is_connected`, `is_ready`, `is_fully_ready`, `state`, `connection_quality`, `subscribed_topics`, `queue_size`

**Context manager and async iterator:**

```python
async with AsyncWSEClient(url) as client:  # auto connect/disconnect
    async for event in client:              # async iterator
        ...
```

### SyncWSEClient

Same API as `AsyncWSEClient` but blocking. Runs the async client in a background daemon thread.

Additional methods:

| Method | Description |
|--------|-------------|
| `run_forever()` | Block and dispatch events to callbacks |
| `recv(timeout=None)` | Block until next event (raises `WSETimeoutError`) |

### WSEEvent

```python
@dataclass(frozen=True, slots=True)
class WSEEvent:
    type: str                    # Event type ("t" field)
    payload: dict[str, Any]      # Event data ("p" field)
    id: str | None = None        # Message ID
    sequence: int | None = None  # Sequence number
    timestamp: str | None = None # ISO 8601 timestamp
    version: int = 1             # Protocol version
    category: str | None = None  # Message category (system/snapshot/update)
    priority: int | None = None  # Message priority (1-10)
```

## Features

### Auto-Reconnection

Automatic reconnection with configurable strategy:

```python
from wse_client import AsyncWSEClient, ReconnectConfig, ReconnectMode

client = AsyncWSEClient(
    "ws://localhost:5006/wse",
    reconnect=ReconnectConfig(
        mode=ReconnectMode.EXPONENTIAL,  # or LINEAR, FIBONACCI, ADAPTIVE
        base_delay=1.0,
        max_delay=30.0,
        factor=1.5,
        jitter=True,
    ),
)
```

### Message Priority

```python
from wse_client import MessagePriority

await client.send("alert", {"msg": "critical"}, priority=MessagePriority.CRITICAL)
await client.send("log", {"msg": "debug info"}, priority=MessagePriority.BACKGROUND)
```

### Compression

Built-in zlib compression for messages over 1 KB. Transparent -- no configuration needed.

### E2E Encryption

ECDH P-256 key exchange with AES-GCM-256 encryption (requires `cryptography`):

```bash
pip install wse-client[crypto]
```

Wire-compatible with the TypeScript client and Rust server.

### Circuit Breaker

Prevents connection storms. Opens after 5 consecutive failures, retries after 60s cooldown.

### Rate Limiting

Token bucket rate limiter (1000 tokens, 100/sec refill). Prevents message flooding.

### Event Sequencing

Automatic duplicate detection (sliding window) and out-of-order event buffering (up to 100 gap).

### Network Quality Monitoring

Real-time latency, jitter, and packet loss tracking:

```python
stats = client.get_stats()
print(stats["network"]["quality"])    # EXCELLENT / GOOD / FAIR / POOR
print(stats["network"]["latency_ms"]) # Round-trip time
print(stats["network"]["jitter_ms"])  # Latency variance
```

### Connection Pool

Multi-endpoint support with health scoring and load balancing:

```python
from wse_client import ConnectionPool, LoadBalancingStrategy

pool = ConnectionPool(
    ["ws://server1:5006/wse", "ws://server2:5006/wse"],
    strategy=LoadBalancingStrategy.WEIGHTED_RANDOM,
)
url = pool.select_endpoint()
```

## Wire Protocol

The client speaks WSE wire protocol v1:

- **Text frames:** Category prefix (`WSE{`, `S{`, `U{`) + JSON envelope
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload
- **Heartbeat:** JSON PING/PONG every 15s with latency tracking

Full protocol spec: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- Python 3.11+
- `websockets >= 13.0`

Optional:
- `cryptography >= 43.0` (E2E encryption)
- `msgpack >= 1.0` (binary encoding)
- `orjson >= 3.10` (fast JSON)

## License

MIT
