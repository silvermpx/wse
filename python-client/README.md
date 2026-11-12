# wse-client

Python client for [WSE (WebSocket Engine)](https://github.com/silvermpx/wse) - real-time event streaming with auto-reconnect, message recovery, E2E encryption, and connection resilience.

[![PyPI](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![Python](https://img.shields.io/pypi/pyversions/wse-client)](https://pypi.org/project/wse-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE)

Pure Python, no Rust dependency. Speaks the same wire protocol as the TS client.

## Installation

```bash
pip install wse-client
```

Optional extras:

```bash
pip install wse-client[crypto]   # E2E encryption (ECDH + AES-GCM)
pip install wse-client[msgpack]  # MessagePack wire format (wire_format="msgpack")
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

`AsyncWSEClient(url, **kwargs)` (and `SyncWSEClient`, which takes the same keywords):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | WebSocket server URL |
| `token` | str | None | JWT token, sent as `access_token` cookie and `Authorization: Bearer` header |
| `token_provider` | callable | None | Sync or async callable returning the token; called before every connect and reconnect attempt (overrides `token`) |
| `topics` | list[str] | None | Topics to subscribe to after connecting |
| `reconnect` | ReconnectConfig | default config | Reconnection strategy |
| `extra_headers` | dict[str, str] | None | Additional HTTP headers for the handshake |
| `queue_size` | int | 1000 | Max events buffered for the iterator / `recv()`; the oldest is dropped when full |
| `encryption` | bool | False | Require E2E encryption (ECDH P-256 + AES-GCM-256; needs the `[crypto]` extra) |
| `endpoints` | list[str] | None | Additional endpoints for the built-in connection pool / failover |
| `load_balancing` | LoadBalancingStrategy | WEIGHTED_RANDOM | Endpoint selection strategy when `endpoints` is set |
| `rate_limit` | float \| None | 100 | Outbound limit for `send()` / `send_with_retry()` / `send_batch()` in messages per second; `None` disables it |
| `rate_limit_burst` | int | 10 x `rate_limit` | Messages that may go out at once before the rate applies |
| `offline_queue` | bool | False | Hold messages sent while disconnected and send them once after the next reconnect |
| `max_message_size` | int \| None | 16 MiB | Largest inbound message, checked on the frame and while inflating compressed frames; `None` disables it |
| `recovery_timeout` | float | 3.0 | Seconds a topic waits for a recovery replay after an offset gap (`0` never waits) |
| `wire_format` | str | `"json"` | `"msgpack"` asks the server to send this connection's messages as MessagePack |

The `connect(url, **kwargs)` factory returns an `AsyncWSEClient` (not connected yet) to use as an async context manager.

## API Reference

### AsyncWSEClient

| Method | Description |
|--------|-------------|
| `connect()` | Open the connection and complete the handshake (raises on failure, see [Error Handling](#error-handling)) |
| `disconnect()` / `close()` | Close the connection; iteration ends after the queued events |
| `send(type, payload, priority=NORMAL, correlation_id=None)` | Send a message; returns a `SendResult` |
| `send_with_retry(type, payload, priority=NORMAL, correlation_id=None, max_retries=5)` | Send with exponential backoff; one message id across all attempts |
| `send_batch(messages)` | Send multiple messages in a single frame |
| `subscribe(topics, recover=False)` | Subscribe to topics, optionally recovering missed messages |
| `unsubscribe(topics)` | Unsubscribe from topics (and forget their recovery positions) |
| `request_snapshot(topics)` | Request a state snapshot; an identical request within 5 s is not resent |
| `on(event_type)` | Register event handler (decorator) |
| `on_any(handler)` | Register a wildcard handler that receives all events |
| `off(event_type, handler)` | Remove a specific event handler |
| `recv(timeout=None)` | Receive the next event |
| `force_reconnect()` | Reconnect now (also revives a client that ended, e.g. after `set_token()`) |
| `change_endpoint(url)` | Switch to a different server URL; subscribed topics are subscribed again there |
| `set_token(token)` | Use a new token for the next connect / reconnect |
| `get_stats()` | Connection statistics, message counts, network quality, recovery counters |

`send()` returns a `SendResult` (an `IntEnum`, truthy when the message was accepted):

| Value | Meaning |
|-------|---------|
| `SendResult.SENT` | Written to the socket |
| `SendResult.QUEUED` | Held by the offline queue (`offline_queue=True`); sent once after the next reconnect - do not retry it |
| `SendResult.FAILED` | Neither sent nor queued (not connected, rate limited); the caller decides whether to retry |

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `is_connected` | bool | WebSocket connection is open |
| `is_ready` | bool | `server_ready` received on the current connection |
| `is_fully_ready` | bool | Connected, server_ready received, and client_hello sent |
| `state` | ConnectionState | Current connection state enum |
| `last_error` | WSEError \| None | Why the client ended for good, `None` while it is healthy |
| `connection_quality` | ConnectionQuality | Network quality assessment |
| `subscribed_topics` | set[str] | Currently subscribed topics |
| `queue_size` | int | Events waiting in the receive queue |
| `recovery_enabled` | bool | Whether the server announced message recovery |
| `recovery_state` | dict | Per-topic recovery anchor `{topic: (epoch, offset)}`: the last offset delivered in order |

**Context manager and async iterator:**

```python
async with AsyncWSEClient(url) as client:  # auto connect/disconnect
    async for event in client:              # yields WSEEvent objects
        ...
```

Reconnects are invisible to the iterator: it keeps yielding events across them. It stops after `disconnect()`, and raises the reason when the client ends for good (`WSEAuthError`, `WSEEncryptionError`, or `WSEConnectionError` once `max_attempts` is used up).

### SyncWSEClient

Same API as `AsyncWSEClient` but blocking. Runs the async client in a background daemon thread; stats and state reads run on that thread's event loop.

| Method | Description |
|--------|-------------|
| `run_forever()` | Block and dispatch events to registered callbacks; returns after `close()`, raises the reason when the client ends for good |
| `recv(timeout=None)` | Block until next event (raises `WSETimeoutError` on timeout) |
| `set_error_handler(handler)` | Callback (called from the background thread) for the error that ends the client |
| `background_error` | The error that ended the client, or `None` |
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
    topic: str | None = None         # Recovery stamp: topic ("tp" field)
    epoch: str | None = None         # Recovery stamp: buffer epoch, 8-hex ("e")
    offset: int | None = None        # Recovery stamp: per-topic offset ("o")
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
        max_attempts=-1,                 # -1 = retry forever
        jitter=True,                     # Add randomness to prevent thundering herd
    ),
)
```

`ADAPTIVE` currently uses the same curve as `EXPONENTIAL` (kept for API parity with the TS client). Delays never exceed `max_delay` (and 5 minutes), however long the outage.

Which closes are reconnected:

| Close | What the client does |
|-------|----------------------|
| `disconnect()` / `close()` (client-initiated) | Final |
| Server: 4401 / 4403 / 1008, or HTTP 401 / 403 on the upgrade | Final, raises `WSEAuthError` - with a `token_provider`, first fetches a fresh token and retries (3 rejections in a row end it) |
| Server: 1000, 1001 (stop), 1005 (kick), 1011, 1012, 1013, 4300 (drain), 4429, any other code; network loss | Reconnect with backoff |
| A failed reconnect attempt | Next attempt with backoff (never terminal); only `max_attempts` ends the client |
| Any close before the first handshake of `connect()` completed | `connect()` raises; nothing reconnects in the background |

On every new connection the client subscribes again to all subscribed topics (with recovery positions when the server supports recovery) and, with `offline_queue=True`, sends the queued messages.

### Authentication

```python
client = AsyncWSEClient(url, token="<jwt>")              # fixed token
client = AsyncWSEClient(url, token_provider=get_token)   # fresh token per attempt (sync or async)
client.set_token("<new jwt>")                            # used from the next (re)connect
await client.force_reconnect()                           # ... or right away
```

A rejected token is kept (not wiped), so `force_reconnect()` after `set_token()` revives a client that ended with `WSEAuthError`.

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

### Offline Queue

Off by default. With `offline_queue=True`, `send()` called while disconnected returns `SendResult.QUEUED` and the message goes out once after the next reconnect (highest priority first, up to 1000 messages, dropped after 1 hour). Rate-limited sends are never queued, and `send_with_retry()` never queues its failed attempts.

### Compression

The client inflates compressed frames from the server (`C:` prefix or raw zlib), bounded by `max_message_size`: a frame that would inflate past the limit is dropped without being inflated in full. Outbound messages are sent uncompressed.

### E2E Encryption

ECDH P-256 key exchange with AES-GCM-256 encryption. Requires the `cryptography` package:

```bash
pip install wse-client[crypto]
```

Enable it with `AsyncWSEClient(url, encryption=True)` (it is off by default). The client sends its public key in `client_hello` as the first frame of every connection and derives the session key from the server's key in `server_hello` (HKDF-SHA256). Wire format is the `E:` prefix + 12-byte IV + ciphertext + 16-byte auth tag, compatible with the TS client and Rust server.

Encryption fails closed:

- Every application message (sends, subscriptions, recovery requests, replies) is encrypted. Messages sent before the key exchange completed wait for it; nothing goes out in plaintext.
- Each connection gets a fresh key; no message is sent with the key of an earlier connection.
- If the server does not complete the exchange (no encryption support, `KEY_EXCHANGE_FAILED`, no `server_hello` within 10 s), the client closes the connection and ends with `WSEEncryptionError`; it never falls back to plaintext.
- `client_hello` and the heartbeat `ping` / `PONG` stay plaintext control frames: they carry no application data, and the server answers pings only on its plaintext path.

### Circuit Breaker

Prevents connection storms during outages. Opens after 5 consecutive failures, rejects further attempts for 60s, then enters half-open state for recovery probing. While it is open, reconnect attempts wait on the backoff schedule.

States: CLOSED (normal) -> OPEN (blocking) -> HALF_OPEN (probing) -> CLOSED (recovered)

### Rate Limiting

Client-side token bucket for outbound application messages: 100 messages/s with a burst of 1000 by default, set with `rate_limit=` / `rate_limit_burst=`, disabled with `rate_limit=None`. A rate-limited `send()` returns `SendResult.FAILED` (one warning every 5 s, counted in `get_stats()["rate_limiter"]["rate_limited"]`). Protocol frames - heartbeat, PONG, client_hello, subscriptions, recovery requests, health / metrics replies - are never limited. The limiter does not read the server's limits: the server's `rate_limit_warning` events concern the messages it sends to this connection, and are passed to your handlers.

### Idempotent delivery & recovery

Stamped topic messages carry a per-message `(topic, epoch, offset)` (`tp`/`e`/`o`).
Dedup and ordering are driven by the per-topic `(epoch, offset)`, which is
authoritative and survives reconnects: a duplicate (offset already seen on the
same epoch) is dropped and an in-order message is delivered. An epoch change
(server restart) re-baselines the topic. Unstamped messages fall back to a
sliding-window id dedup (10,000-entry cache). The server's global `seq` is a
diagnostic only - it is not used for ordering.

A topic never goes dark after an offset gap:

- When the server supports recovery, the client asks it to replay the topic from
  the last offset delivered in order, and holds the messages that arrive past the
  hole for up to `recovery_timeout` seconds (one request in flight per topic).
  Replayed messages are delivered in order, then the held ones.
- If the hole is not filled in time - or the server cannot replay it, does not
  answer, or recovery is not available - the client delivers the held messages
  and reports the loss as a `recovery_gap` event:

```python
@client.on("recovery_gap")
def on_gap(event):
    p = event.payload  # topic, epoch, first_missing, last_missing, missed, reason
    print(f"{p['topic']}: lost {p['missed']} messages ({p['reason']})")
```

The `recovery_gap` event also reaches `on_any` handlers and the iterator. `reason` is one of
`unavailable`, `timeout`, `not_recovered`, `incomplete`, `overflow`,
`epoch_changed`, `closed`. `recovery_state` reports the last offset delivered per
topic (never a server head that was not delivered). Recovery requests are sent
as `subscription_update` messages with `recover: true`; the server application
answers them (see `subscribe_with_recovery` in the server docs).

### Network Quality Monitoring

Real-time network quality assessment based on PING/PONG round-trip measurements:

```python
stats = client.get_stats()
print(stats["network"]["quality"])       # EXCELLENT / GOOD / FAIR / POOR (UNKNOWN until enough samples)
print(stats["network"]["latency_ms"])    # Average round-trip time
print(stats["network"]["jitter_ms"])     # Latency variance
print(stats["network"]["packet_loss"])   # Estimated packet loss (%)
print(stats["recovery"])                 # enabled, pending_gaps, gaps, missed, recovered
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

| Error | Raised by | When |
|-------|-----------|------|
| `WSEAuthError` | `connect()`, iterator, `recv()`, `run_forever()` | The server rejected the credentials (close 4401 / 4403 / 1008, HTTP 401 / 403), or the `token_provider` failed during `connect()` |
| `WSEEncryptionError` | `connect()`, `send()`, iterator, `recv()` | `encryption=True` and the key exchange failed |
| `WSEConnectionError` | `connect()`, iterator, `recv()` | The connection could not be opened or closed during the handshake; reconnect attempts ran out; `recv()` after `disconnect()` |
| `WSETimeoutError` | `connect()`, `SyncWSEClient.connect()` / `recv()` | Opening the connection (or the sync `recv`) timed out |
| `WSECircuitBreakerError` | `connect()` | The circuit breaker is open |

`WSERateLimitError` and `WSEProtocolError` are exported for compatibility; the client does not raise them (a rate-limited send returns `SendResult.FAILED`, an undecodable frame is logged and dropped).

## Wire Protocol

The client speaks WSE wire protocol v1:

- **Text frames:** JSON with `c` field for category (`WSE`, `S`, `U`)
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` AES-GCM) + payload; with `wire_format="msgpack"` the client asks for `?format=msgpack` and decodes `M:` frames, encrypted ones included
- **Handshake:** `client_hello` is the first frame of every connection; the connection is ready at the first `server_ready` or `server_hello` (with encryption: once the key exchange completed)
- **Heartbeat:** JSON PING/PONG with latency tracking (plaintext control frames)

Full protocol specification: [PROTOCOL.md](../docs/PROTOCOL.md)

## Requirements

- Python 3.11+
- `websockets >= 13.0`

Optional:
- `cryptography >= 43.0` (E2E encryption)
- `msgpack >= 1.0` (MessagePack wire format)
- `orjson >= 3.10` (faster JSON serialization)

## License

MIT
