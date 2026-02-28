# WSE - WebSocket Engine

[![PyPI - Server](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![PyPI - Client](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

High-performance WebSocket server built in Rust with native clustering, message recovery, presence tracking, and real-time fan-out. Exposed to Python via PyO3 with zero GIL overhead on the data path.

## Features

### Server

| Feature | Details |
|---------|---------|
| **Rust core** | tokio async runtime, tungstenite WebSocket transport, dedicated thread pool, zero GIL on the data path |
| **JWT authentication** | Rust-native HS256 validation during handshake (0.01ms), cookie + Authorization header extraction |
| **Protocol negotiation** | `client_hello`/`server_hello` handshake with feature discovery, capability advertisement, version agreement |
| **Topic subscriptions** | Per-connection topic subscriptions with automatic cleanup on disconnect |
| **Pre-framed broadcast** | WebSocket frame built once, shared via Arc across all connections, single allocation per broadcast |
| **Vectored writes** | `write_vectored` (writev syscall) batches multiple frames per connection in a single kernel call |
| **Write coalescing** | Write task drains up to 256 pending frames per iteration via `recv_many` |
| **DashMap state** | Lock-free sharded concurrent hash maps for topics, rates, formats, activity tracking |
| **mimalloc allocator** | Global allocator optimized for multi-threaded workloads with frequent small allocations |
| **Deduplication** | 50,000-entry AHashSet with FIFO eviction per `send_event()` call |
| **Rate limiting** | Per-connection token bucket: 100K capacity, 10K/s refill, client warning at 20% remaining |
| **Zombie detection** | Server pings every 25s, force-closes connections with no activity for 60s |
| **Drain mode** | Lock-free crossbeam bounded channel, Python acquires GIL once per batch (not per event) |
| **Compression** | zlib for client-facing messages above threshold (default 1024 bytes) |
| **MessagePack** | Opt-in binary transport via `?format=msgpack`, roughly 2x faster serialization, 30% smaller |
| **Message signing** | Selective HMAC-SHA256 signing for critical operations, nonce-based replay prevention |

### End-to-End Encryption

| Feature | Details |
|---------|---------|
| **Key exchange** | ECDH P-256 (per-connection keypair, automatic during handshake) |
| **Encryption** | AES-GCM-256 with unique 12-byte IV per message |
| **Key derivation** | HKDF-SHA256 (salt: `wse-encryption`, info: `aes-gcm-key`) |
| **Wire format** | `E:` prefix + 12-byte IV + AES-GCM ciphertext + 16-byte auth tag |
| **Key rotation** | Configurable rotation interval (default 1 hour), automatic renegotiation |
| **Replay prevention** | Nonce cache (10K entries, 5-minute TTL) on the client side |

### Cluster Protocol

| Feature | Details |
|---------|---------|
| **Topology** | Full TCP mesh, direct peer-to-peer connections |
| **Wire format** | Custom binary frames: 8-byte header + topic + payload, 12 message types |
| **Interest routing** | SUB/UNSUB/RESYNC frames, messages forwarded only to peers with matching subscribers |
| **Gossip discovery** | PeerAnnounce/PeerList frames, new nodes need one seed address to join |
| **mTLS** | rustls + tokio-rustls, P-256 certificates, WebPkiClientVerifier for both sides |
| **Compression** | zstd level 1 for payloads above 256 bytes, capability-negotiated, output capped at 1 MB |
| **Heartbeat** | 5s ping interval, 15s timeout, dead peer detection |
| **Circuit breaker** | 10 failures to open, 60s reset, 3 half-open probe calls |
| **Dead letter queue** | 1000-entry ring buffer for failed cluster sends |
| **Presence sync** | PresenceUpdate/PresenceFull frames, CRDT last-write-wins conflict resolution |

### Presence Tracking

| Feature | Details |
|---------|---------|
| **Per-topic tracking** | Which users are active in each topic, with custom metadata (status, avatar, etc.) |
| **User-level grouping** | Multiple connections from same JWT `sub` share one presence entry |
| **Join/leave lifecycle** | `presence_join` on first connection, `presence_leave` on last disconnect |
| **O(1) stats** | `presence_stats()` returns member/connection counts without iteration |
| **Data updates** | `update_presence()` broadcasts to all topics where the user is present |
| **Cluster sync** | Synchronized across all nodes, CRDT last-write-wins resolution |
| **TTL sweep** | Background task every 30s removes entries from dead connections |

### Message Recovery

| Feature | Details |
|---------|---------|
| **Ring buffers** | Per-topic, power-of-2 capacity, bitmask indexing (single AND instruction) |
| **Epoch+offset tracking** | Precise recovery positioning, epoch changes on buffer recreation |
| **Memory management** | Global budget (default 256 MB), TTL eviction, LRU eviction when over budget |
| **Zero-copy storage** | Recovery entries share `Bytes` (Arc) with the broadcast path |
| **Recovery on reconnect** | `subscribe_with_recovery()` replays missed messages automatically |

### Client SDKs (Python + TypeScript/React)

| Feature | Details |
|---------|---------|
| **Auto-reconnection** | 4 strategies: exponential, linear, fibonacci, adaptive backoff with jitter |
| **Connection pool** | Multi-endpoint with health scoring, 3 load balancing strategies, automatic failover |
| **Circuit breaker** | CLOSED/OPEN/HALF_OPEN state machine, prevents connection storms |
| **Rate limiting** | Client-side token bucket, coordinates with server feedback |
| **E2E encryption** | Wire-compatible AES-GCM-256 + ECDH P-256 (both clients speak the same protocol) |
| **Event sequencing** | Duplicate detection (sliding window) + out-of-order buffering |
| **Network monitor** | Real-time latency, jitter, packet loss measurement, quality scoring |
| **Priority queues** | 5 levels from CRITICAL to BACKGROUND |
| **Offline queue** | IndexedDB persistence (TypeScript), replayed on reconnect |
| **Compression** | Automatic zlib for messages above threshold |
| **MessagePack** | Binary encoding for smaller payloads and faster serialization |
| **Message signing** | HMAC-SHA256 integrity verification |

### Transport Security

| Feature | Details |
|---------|---------|
| **Origin validation** | `ALLOWED_ORIGINS` env var, rejects unlisted origins with close code 4403 |
| **Cookie auth** | `access_token` HTTP-only cookie with `Secure + SameSite=Lax` (OWASP recommended for browsers) |
| **Frame protection** | 1 MB max frame size, serde_json parsing (no eval), escaped user IDs in server_ready |
| **Cluster frame protection** | zstd decompression output capped at 1 MB (MAX_FRAME_SIZE), protocol version validation |

---

## Quick Start

```bash
pip install wse-server
```

```python
from wse_server import RustWSEServer, rust_jwt_encode
import time, threading

server = RustWSEServer(
    "0.0.0.0", 5007,
    max_connections=10_000,
    jwt_secret=b"replace-with-a-strong-secret-key!",
    jwt_issuer="my-app",
    jwt_audience="my-api",
)
server.enable_drain_mode()
server.start()

def handle_events(srv):
    while True:
        for ev in srv.drain_inbound(256, 50):
            if ev[0] == "auth_connect":
                srv.subscribe_connection(ev[1], ["updates"])
            elif ev[0] == "msg":
                print(f"Message from {ev[1]}: {ev[2]}")
            elif ev[0] == "disconnect":
                print(f"Disconnected: {ev[1]}")

threading.Thread(target=handle_events, args=(server,), daemon=True).start()

while server.is_running():
    time.sleep(1)
```

Generate a test token:

```python
token = rust_jwt_encode(
    {"sub": "user-1", "iss": "my-app", "aud": "my-api",
     "exp": int(time.time()) + 3600, "iat": int(time.time())},
    b"replace-with-a-strong-secret-key!",
)
```

---

## Server Configuration

`RustWSEServer` constructor parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | required | Bind address |
| `port` | required | Bind port |
| `max_connections` | 1000 | Maximum concurrent WebSocket connections |
| `jwt_secret` | None | HS256 secret for JWT validation (bytes, min 32 bytes). `None` disables authentication |
| `jwt_issuer` | None | Expected `iss` claim. Skipped if `None` |
| `jwt_audience` | None | Expected `aud` claim. Skipped if `None` |
| `max_inbound_queue_size` | 131072 | Drain mode bounded queue capacity |
| `recovery_enabled` | False | Enable per-topic message recovery buffers |
| `recovery_buffer_size` | 128 | Ring buffer slots per topic (rounded to power-of-2) |
| `recovery_ttl` | 300 | Buffer TTL in seconds before eviction |
| `recovery_max_messages` | 500 | Max messages returned per recovery response |
| `recovery_memory_budget` | 268435456 | Global memory limit for all recovery buffers (bytes, default 256 MB) |
| `presence_enabled` | False | Enable per-topic presence tracking |
| `presence_max_data_size` | 4096 | Max bytes for a user's presence metadata |
| `presence_max_members` | 0 | Max tracked members per topic (0 = unlimited) |

---

## API Reference

### Lifecycle

```python
server.start()                          # Start the server
server.stop()                           # Graceful shutdown
server.is_running()                     # Check server status (bool)
```

### Event Handling

**Drain mode** (recommended) - events are queued in a lock-free crossbeam channel. Python polls in batches, acquiring the GIL once per batch.

```python
server.enable_drain_mode()              # Switch to batch-polling mode (call before start)
events = server.drain_inbound(256, 50)  # Poll up to 256 events, wait up to 50ms
```

Each event is a tuple: `(event_type, conn_id, payload)`

| Event Type | Trigger | Payload |
|------------|---------|---------|
| `"auth_connect"` | JWT-validated connection | user_id (string) |
| `"connect"` | Connection without JWT | cookies (string) |
| `"msg"` | Client sent WSE-prefixed JSON | parsed dict |
| `"raw"` | Client sent plain text | raw string |
| `"bin"` | Client sent binary frame | bytes |
| `"disconnect"` | Connection closed | None |
| `"presence_join"` | User's first connection joined a topic | dict with user_id, topic, data |
| `"presence_leave"` | User's last connection left a topic | dict with user_id, topic, data |

**Callback mode** - alternative to drain mode. Callbacks are invoked via `spawn_blocking` per event.

```python
server.set_callbacks(on_connect, on_message, on_disconnect)
```

### Sending Messages

```python
server.send(conn_id, text)                      # Send text to one connection
server.send_bytes(conn_id, data)                 # Send binary to one connection
server.send_event(conn_id, event_dict)           # Send structured event (auto-serialized, deduped, rate-checked)

server.broadcast_all(text)                       # Send to every connected client (text)
server.broadcast_all_bytes(data)                 # Send to every connected client (binary)
server.broadcast_local(topic, text)              # Fan-out to topic subscribers on this instance
server.broadcast(topic, text)                    # Fan-out to topic subscribers across all cluster nodes
```

| Method | Scope | Notes |
|--------|-------|-------|
| `send` | Single connection | Raw text frame |
| `send_bytes` | Single connection | Raw binary frame |
| `send_event` | Single connection | JSON-serialized, compressed if above threshold, deduplication via 50K-entry FIFO window |
| `broadcast_all` | All connections | Pre-framed, single frame build shared via Arc |
| `broadcast_local` | Topic (local) | Pre-framed, DashMap subscriber lookup, stored in recovery buffer if enabled |
| `broadcast` | Topic (all nodes) | Local fan-out + forwarded to cluster peers with matching interest |

### Topic Subscriptions

```python
server.subscribe_connection(conn_id, ["prices", "news"])              # Subscribe to topics
server.subscribe_connection(conn_id, ["chat"], {"status": "online"})  # Subscribe with presence data
server.unsubscribe_connection(conn_id, ["news"])                      # Unsubscribe from specific topics
server.unsubscribe_connection(conn_id, None)                          # Unsubscribe from all topics
server.get_topic_subscriber_count("prices")                           # Subscriber count for a topic
```

Subscriptions are cleaned up automatically on disconnect. In cluster mode, interest changes are propagated to peers via SUB/UNSUB frames.

### Presence Tracking

Requires `presence_enabled=True` in the constructor.

```python
# Query members in a topic
members = server.presence("chat-room")
# {"alice": {"data": {"status": "online"}, "connections": 2},
#  "bob":   {"data": {"status": "away"},   "connections": 1}}

# Lightweight counts (O(1), no iteration)
stats = server.presence_stats("chat-room")
# {"num_users": 2, "num_connections": 3}

# Update a user's presence data across all their subscribed topics
server.update_presence(conn_id, {"status": "away"})
```

Presence is tracked at the user level (JWT `sub` claim). Multiple connections from the same user share a single presence entry. `presence_join` fires on first connection, `presence_leave` on last disconnect. In cluster mode, presence state is synchronized across all nodes using CRDT last-write-wins resolution.

### Message Recovery

Requires `recovery_enabled=True` in the constructor.

```python
result = server.subscribe_with_recovery(
    conn_id, ["prices"],
    recover=True,
    epoch=client_epoch,       # From previous session
    offset=client_offset,     # From previous session
)
# {"topics": {"prices": {"epoch": 123, "offset": 456, "recovered": True, "count": 12}}}
```

The server maintains per-topic ring buffers (power-of-2 capacity, bitmask indexing). Clients store the `epoch` and `offset` from their last received message. On reconnect, the server replays missed messages from the ring buffer. If the gap is too large or the epoch has changed, the client receives a `NotRecovered` status and should re-subscribe from scratch.

Memory is managed with a global budget (default 256 MB), TTL eviction for idle buffers, and LRU eviction when over budget.

### Cluster

```python
# Join a cluster mesh with mTLS
server.connect_cluster(
    peers=["10.0.0.2:9999", "10.0.0.3:9999"],
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)

# With gossip discovery (only seed addresses needed)
server.connect_cluster(
    peers=[],
    seeds=["10.0.0.2:9999"],
    cluster_addr="10.0.0.1:9999",
    cluster_port=9999,
)

server.cluster_connected()       # True if connected to at least one peer
server.cluster_peers_count()     # Number of active peer connections
```

Nodes form a full TCP mesh automatically. The cluster protocol uses a custom binary frame format with an 8-byte header, 12 message types, and capability negotiation during handshake. Features:

- **Interest-based routing** - SUB/UNSUB/RESYNC frames. Messages are only forwarded to peers with matching subscribers.
- **Gossip discovery** - PeerAnnounce/PeerList frames. New nodes need one seed address to join.
- **mTLS** - mutual TLS via rustls with P-256 certificates and WebPkiClientVerifier.
- **zstd compression** - payloads above 256 bytes compressed at level 1, capability-negotiated.
- **Circuit breaker** - 10 failures to open, 60s reset, 3 half-open probe calls.
- **Heartbeat** - 5s interval, 15s timeout, dead peer detection.
- **Dead letter queue** - 1000-entry ring buffer for failed cluster sends.
- **Presence sync** - PresenceUpdate/PresenceFull frames with CRDT conflict resolution.

### Health Monitoring

```python
health = server.health_snapshot()
# {
#     "connections": 150,
#     "inbound_queue_depth": 0,
#     "inbound_dropped": 0,
#     "uptime_secs": 3600.5,
#     "recovery_enabled": True,
#     "recovery_topic_count": 5,
#     "recovery_total_bytes": 1048576,
#     "cluster_connected": True,
#     "cluster_peer_count": 2,
#     "cluster_messages_sent": 50000,
#     "cluster_messages_delivered": 49950,
#     "cluster_messages_dropped": 0,
#     "cluster_bytes_sent": 1048576,
#     "cluster_bytes_received": 1024000,
#     "cluster_reconnect_count": 0,
#     "cluster_unknown_message_types": 0,
#     "cluster_dlq_size": 0,
#     "presence_enabled": True,
#     "presence_topics": 3,
#     "presence_total_users": 25,
# }
```

### Connection Management

```python
server.get_connection_count()        # Lock-free AtomicUsize read
server.get_connections()             # List all connection IDs (snapshot)
server.disconnect(conn_id)           # Force-disconnect a connection
server.inbound_queue_depth()         # Events waiting to be drained
server.inbound_dropped_count()       # Events dropped due to full queue
server.get_cluster_dlq_entries()     # Retrieve failed cluster messages from dead letter queue
```

---

## Security

### JWT Authentication

Rust-native HS256 validation during the WebSocket handshake. Zero GIL acquisition, 0.01ms per decode.

Token delivery:
- **Browser clients**: `access_token` HTTP-only cookie (set by your login endpoint, attached automatically by the browser)
- **Backend clients**: `Authorization: Bearer <token>` header and/or `access_token` cookie
- **API clients**: `Authorization: Bearer <token>` header

Required claims: `sub` (user ID), `exp` (expiration), `iat` (issued at). Optional: `iss`, `aud` (validated if configured).

### End-to-End Encryption

Per-connection session keys via ECDH P-256 key exchange, AES-GCM-256 encryption, HKDF-SHA256 key derivation.

Wire format: `E:` prefix + 12-byte IV + AES-GCM ciphertext + 16-byte auth tag.

Enable on the client side - the server handles key exchange automatically during the handshake.

### Rate Limiting

Per-connection token bucket: 100,000 token capacity, 10,000 tokens/second refill. Clients receive a `rate_limit_warning` at 20% remaining capacity, and `RATE_LIMITED` error when exceeded.

### Deduplication

`send_event()` maintains a 50,000-entry AHashSet with FIFO eviction. Duplicate message IDs are dropped before serialization.

### Zombie Detection

Server pings every connected client every 25 seconds. Connections with no activity for 60 seconds are force-closed.

Full security documentation: [docs/SECURITY.md](docs/SECURITY.md)

---

## Wire Protocol

WSE uses a custom wire protocol with category-prefixed messages:

**Text frames:** `WSE{...}` (system), `S{...}` (snapshot), `U{...}` (update) + JSON envelope

**Binary frames:** `C:` (zlib compressed), `M:` (MessagePack), `E:` (AES-GCM encrypted), raw zlib (0x78 magic byte)

**MessagePack transport:** opt-in per connection via `?format=msgpack` query parameter. Roughly 2x faster serialization and 30% smaller payloads.

**Protocol negotiation:** `client_hello`/`server_hello` handshake with feature discovery, capability advertisement, and version agreement.

Full protocol specification: [docs/PROTOCOL.md](docs/PROTOCOL.md)

---

## Compression

Two compression layers:

- **Client-facing:** zlib for messages above the configurable threshold (default 1024 bytes). Applied automatically by `send_event()`.
- **Inter-peer (cluster):** zstd level 1 for payloads above 256 bytes. Capability-negotiated during handshake. Decompression output capped at 1 MB (MAX_FRAME_SIZE).

---

## Client SDKs

### Python

```bash
pip install wse-client
```

Full-featured async and sync client with connection pool, circuit breaker, auto-reconnect, E2E encryption, and msgpack binary transport.

```python
from wse_client import connect

async with connect("ws://localhost:5007/wse", token="<jwt>") as client:
    await client.subscribe(["updates"])
    async for event in client:
        print(event.type, event.payload)
```

**Sync interface:**

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5007/wse", token="<jwt>")
client.connect()
client.subscribe(["updates"])

@client.on("updates")
def handle(event):
    print(event.payload)

client.run_forever()
```

Key features: 4 reconnect strategies (exponential, linear, fibonacci, adaptive), connection pool with health scoring and 3 load balancing strategies, circuit breaker, token bucket rate limiter, event sequencer with dedup and reorder buffering, network quality monitoring (latency/jitter/packet loss).

See [python-client/README.md](python-client/README.md) for full API reference.

### TypeScript / React

```bash
npm install wse-client
```

Single React hook (`useWSE`) for connection lifecycle, subscriptions, and message dispatch.

```tsx
import { useWSE } from 'wse-client';

function App() {
  const { sendMessage, connectionHealth } = useWSE(
    '<jwt-token>',
    ['updates'],
    { endpoints: ['ws://localhost:5007/wse'] },
  );

  return <div>Status: {connectionHealth}</div>;
}
```

Key features: offline queue with IndexedDB persistence, adaptive quality management, connection pool with health scoring, E2E encryption (Web Crypto API), message batching, 5 priority levels, Zustand store for external state access.

See [client/README.md](client/README.md) for full API reference.

---

## Performance

Benchmarked on AMD EPYC 7502P (64 cores, 128 GB RAM), Ubuntu 24.04.

| Mode | Peak Throughput | Connections | Message Loss |
|------|----------------|-------------|--------------|
| Standalone (fan-out) | 4.3M deliveries/s | 500K | 0% |
| Standalone (inbound JSON) | 14.4M msg/s | 500K | 0% |
| Standalone (inbound msgpack) | 30M msg/s | 500K | 0% |
| Cluster (2 nodes) | 9.5M deliveries/s | 20K per node | 0% |

Sub-millisecond latency. Median 0.38ms with JWT authentication. Connection handshake: 0.53ms median (Rust JWT path).

Detailed results: [Benchmarks](docs/BENCHMARKS.md) | [Fan-out](docs/BENCHMARKS_FANOUT.md) | [Rust Client](docs/BENCHMARKS_RUST_CLIENT.md) | [Python Client](docs/BENCHMARKS_PYTHON_CLIENT.md) | [TypeScript Client](docs/BENCHMARKS_TS_CLIENT.md)

---

## Examples

Working examples in the [`examples/`](examples/) directory:

| Example | Description |
|---------|-------------|
| [`standalone_basic.py`](examples/standalone_basic.py) | Basic server with JWT auth and echo |
| [`standalone_broadcast.py`](examples/standalone_broadcast.py) | Topic-based pub/sub with broadcasting |
| [`standalone_presence.py`](examples/standalone_presence.py) | Per-topic presence tracking with join/leave events |
| [`standalone_recovery.py`](examples/standalone_recovery.py) | Message recovery on reconnect with epoch+offset |

---

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - server internals, PyO3 bridge, concurrency model
- [Integration Guide](docs/INTEGRATION.md) - complete setup and API reference
- [Wire Protocol](docs/PROTOCOL.md) - message format specification
- [Cluster Protocol](docs/CLUSTER_PROTOCOL.md) - TCP mesh, frame format, gossip, interest routing
- [Security](docs/SECURITY.md) - JWT, encryption, mTLS, rate limiting, circuit breaker
- [Deployment](docs/DEPLOYMENT.md) - production setup, Docker, Kubernetes, cluster configuration
- [Migration Guide](docs/MIGRATION.md) - upgrading from v1.x to v2.0
- [Contributing](CONTRIBUTING.md) - development setup and coding standards
- [Changelog](CHANGELOG.md) - version history

---

## License

MIT
