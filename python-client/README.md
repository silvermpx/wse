# wse-client

Python client for [WSE (WebSocket Engine)](https://github.com/niceguy135/wse) - real-time event streaming with auto-reconnect, compression, encryption, and connection resilience.

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
from wse_client import connect

async with connect("ws://localhost:5007/wse", token="<jwt>") as client:
    await client.subscribe(["notifications", "trades"])

    async for event in client:
        print(event.type, event.payload)
```

### Sync

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5007/wse", token="<jwt>")
client.connect()
client.subscribe(["notifications"])

event = client.recv(timeout=5.0)
print(event.type, event.payload)

client.close()
```

### Callbacks

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5007/wse", token="<jwt>")

@client.on("notifications")
def handle(event):
    print(event.payload)

@client.on_any
def catch_all(event):
    print(f"[{event.type}] {event.payload}")

client.connect()
client.run_forever()
```

## Constructor Parameters

`AsyncWSEClient(url, **kwargs)`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | WebSocket server URL |
| `token` | str | None | JWT token for authentication |
| `topics` | list[str] | None | Topics to auto-subscribe after connecting |
| `reconnect` | ReconnectConfig | default config | Reconnection strategy |
| `extra_headers` | dict[str, str] | None | Additional HTTP headers for the handshake |
| `queue_size` | int | 1000 | Max events buffered for the async iterator |

The `connect(url, **kwargs)` factory returns an `AsyncWSEClient` configured as an async context manager.

## API Reference

### AsyncWSEClient

| Method | Description |
|--------|-------------|
| `connect()` | Open WebSocket connection |
| `disconnect()` | Close connection gracefully |
| `send(type, payload, priority=NORMAL, correlation_id=None)` | Send a structured message |
| `send_with_retry(type, payload, priority=NORMAL, correlation_id=None, max_retries=5)` | Send with exponential backoff retries |
| `send_batch(messages)` | Send multiple messages in a single frame |
| `subscribe(topics, recover=False)` | Subscribe to event topics, optionally recovering missed messages |
| `unsubscribe(topics)` | Unsubscribe from topics |
| `request_snapshot(topics)` | Request current state snapshot for topics |
| `on(event_type)` | Register event handler (decorator) |
| `on_any(handler)` | Register a wildcard handler that receives all events |
| `off(event_type, handler)` | Remove a specific event handler |
| `recv(timeout=None)` | Receive next event (blocks until available) |
| `force_reconnect()` | Force an immediate reconnection |
| `change_endpoint(url)` | Switch to a different server URL |
| `get_stats()` | Connection statistics, message counts, network quality |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `is_connected` | bool | WebSocket connection is open |
| `is_ready` | bool | `server_ready` handshake completed |
| `is_fully_ready` | bool | Connected, server_ready received, and client_hello sent |
| `state` | ConnectionState | Current connection state enum |
| `connection_quality` | ConnectionQuality | Network quality assessment |
| `subscribed_topics` | set[str] | Currently subscribed topics |
| `queue_size` | int | Events waiting in the receive queue |
| `recovery_enabled` | bool | Whether server supports message recovery |
| `recovery_state` | dict | Per-topic recovery state: `{topic: (epoch, offset)}` |

**Context manager and async iterator:**

```python
async with AsyncWSEClient(url) as client:  # auto connect/disconnect
    async for event in client:              # yields WSEEvent objects
        ...
```

### SyncWSEClient

Same API as `AsyncWSEClient` but blocking. Runs the async client in a background daemon thread.

| Method | Description |
|--------|-------------|
| `run_forever()` | Block and dispatch events to registered callbacks |
| `recv(timeout=None)` | Block until next event (raises `WSETimeoutError` on timeout) |
| `close()` | Disconnect and shut down the background thread |

### WSEEvent

```python
@dataclass(frozen=True, slots=True)
class WSEEvent:
    type: str                        # Event type ("t" field)
    payload: dict[str, Any]          # Event data ("p" field)
    id: str | None = None            # Message ID
    sequence: int | None = None      # Sequence number
    timestamp: str | None = None     # ISO 8601 timestamp
    version: int = 1                 # Protocol version
    category: str | None = None      # Message category (system/snapshot/update)
    priority: int | None = None      # Message priority (see MessagePriority)
    correlation_id: str | None = None  # Request correlation ID
    signature: str | None = None     # Message signature (if signed)
```

## Features

### Auto-Reconnection

Four reconnection strategies with configurable parameters:

```python
from wse_client import AsyncWSEClient, ReconnectConfig, ReconnectMode

client = AsyncWSEClient(
    "ws://localhost:5007/wse",
    reconnect=ReconnectConfig(
        mode=ReconnectMode.EXPONENTIAL,  # EXPONENTIAL, LINEAR, FIBONACCI, ADAPTIVE
        base_delay=1.0,                  # Initial delay in seconds
        max_delay=30.0,                  # Maximum delay cap
        factor=1.5,                      # Backoff multiplier
        jitter=True,                     # Add randomness to prevent thundering herd
    ),
)
```

On reconnect, the client automatically re-subscribes to all previously subscribed topics.

### Message Priority

Five priority levels for message ordering:

```python
from wse_client import MessagePriority

await client.send("alert", {"msg": "critical"}, priority=MessagePriority.CRITICAL)     # 10
await client.send("update", {"data": "..."}, priority=MessagePriority.HIGH)             # 8
await client.send("status", {"ok": True}, priority=MessagePriority.NORMAL)              # 5
await client.send("metric", {"cpu": 42}, priority=MessagePriority.LOW)                  # 3
await client.send("log", {"msg": "debug"}, priority=MessagePriority.BACKGROUND)         # 1
```

### Compression

Built-in zlib compression for messages over 1 KB. Applied automatically on send - no configuration needed. The server decompresses transparently.

### E2E Encryption

ECDH P-256 key exchange with AES-GCM-256 encryption. Requires the `cryptography` package:

```bash
pip install wse-client[crypto]
```

Key exchange happens automatically during the WebSocket handshake. All messages are encrypted with per-connection session keys derived via HKDF-SHA256. Wire format is the `E:` prefix + 12-byte IV + ciphertext + 16-byte auth tag, compatible with the TypeScript client and Rust server.

### Circuit Breaker

Prevents connection storms during outages. Opens after 5 consecutive failures, rejects further attempts for 60s, then enters half-open state for recovery probing.

States: CLOSED (normal) -> OPEN (blocking) -> HALF_OPEN (probing) -> CLOSED (recovered)

### Rate Limiting

Client-side token bucket rate limiter (1000 tokens, 100/sec refill). Prevents message flooding and coordinates with the server's rate limit feedback (`rate_limit_warning` at 20% capacity).

### Event Sequencing

Automatic duplicate detection via sliding window (10,000-entry dedup cache). Out-of-order event buffering with configurable gap tolerance (up to 100 sequence gap). Missed sequences trigger automatic gap recovery.

### Network Quality Monitoring

Real-time network quality assessment based on PING/PONG round-trip measurements:

```python
stats = client.get_stats()
print(stats["network"]["quality"])       # EXCELLENT / GOOD / FAIR / POOR
print(stats["network"]["latency_ms"])    # Average round-trip time
print(stats["network"]["jitter_ms"])     # Latency variance
print(stats["network"]["packet_loss"])   # Estimated packet loss ratio
```

### Connection Pool

Multi-endpoint support with health scoring and automatic failover:

```python
from wse_client import ConnectionPool, LoadBalancingStrategy

pool = ConnectionPool(
    ["ws://server1:5007/wse", "ws://server2:5007/wse", "ws://server3:5007/wse"],
    strategy=LoadBalancingStrategy.WEIGHTED_RANDOM,  # ROUND_ROBIN, WEIGHTED_RANDOM, LEAST_CONNECTIONS
)
url = pool.select_endpoint()
```

Endpoints are scored based on connection success rate, latency, and recent failures. Unhealthy endpoints are deprioritized automatically.

## Error Handling

```python
from wse_client.errors import (
    WSEError,                # Base exception
    WSEConnectionError,      # Connection failures
    WSETimeoutError,         # Operation timeouts
    WSEAuthError,            # Authentication failures
    WSERateLimitError,       # Rate limit exceeded
    WSEProtocolError,        # Wire protocol violations
    WSECircuitBreakerError,  # Circuit breaker open
    WSEEncryptionError,      # Encryption/decryption failures
)
```

## Wire Protocol

The client speaks WSE wire protocol v1:

- **Text frames:** JSON with `c` field for category (`WSE`, `S`, `U`)
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload
- **Heartbeat:** JSON PING/PONG with latency tracking

Full protocol specification: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- Python 3.11+
- `websockets >= 13.0`

Optional:
- `cryptography >= 43.0` (E2E encryption)
- `msgpack >= 1.0` (MessagePack binary encoding)
- `orjson >= 3.10` (faster JSON serialization)

## License

MIT
