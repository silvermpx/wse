# WSE - WebSocket Engine

[![PyPI - Server](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![PyPI - Client](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

High-performance WebSocket server with native clustering, message recovery, and real-time fan-out. Rust core, Python API.

---

## Key Features

- **Rust WebSocket server** - tokio async runtime with tungstenite, dedicated thread pool, zero GIL on the hot path
- **Cluster protocol** - TCP mesh with interest-based routing, mTLS peer authentication, gossip discovery, zstd compression
- **Message recovery** - per-topic ring buffers with epoch+offset tracking, automatic replay on reconnect
- **JWT authentication** - Rust-native HS256 validation during handshake, zero GIL acquisition, 0.01ms decode
- **Protocol negotiation** - client_hello/server_hello handshake with feature discovery and version agreement
- **Presence tracking** - per-topic user presence with join/leave/update events, multi-connection deduplication, cluster sync
- **Real-time fan-out** - topic subscriptions, broadcast to millions, pre-framed writes, vectored I/O
- **Rate limiting** - per-connection token bucket, 100K capacity, client feedback on threshold
- **E2E encryption** - AES-GCM-256 with ECDH P-256 key exchange, per-connection session keys
- **Compression** - zlib + zstd with adaptive thresholds, msgpack binary transport
- **Client SDKs** - Python (async + sync) and TypeScript/React with full feature parity

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
    jwt_secret=b"your-secret-key",
    jwt_issuer="my-app",
    jwt_audience="my-api",
)
server.enable_drain_mode()
server.start()

def handle_events(srv):
    while True:
        for ev in srv.drain_inbound(256, 50):
            if ev[0] == "auth_connect":
                print(f"Connected: {ev[2]}")
                srv.subscribe_connection(ev[1], ["updates"])
            elif ev[0] == "msg":
                print(f"Message: {ev[2]}")
            elif ev[0] == "disconnect":
                print(f"Disconnected: {ev[1]}")

threading.Thread(target=handle_events, args=(server,), daemon=True).start()

while server.is_running():
    time.sleep(1)
```

---

## Server Configuration

`RustWSEServer` constructor parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | required | Bind address |
| `port` | required | Bind port |
| `max_connections` | 1000 | Maximum concurrent connections |
| `jwt_secret` | None | JWT HS256 secret (bytes). None disables auth |
| `jwt_issuer` | None | Expected JWT issuer claim |
| `jwt_audience` | None | Expected JWT audience claim |
| `max_inbound_queue_size` | 131072 | Drain mode queue capacity |
| `recovery_enabled` | False | Enable message recovery |
| `recovery_buffer_size` | 128 | Ring buffer slots per topic (rounded to power-of-2) |
| `recovery_ttl` | 300 | Buffer TTL in seconds |
| `recovery_max_messages` | 500 | Max messages per recovery response |
| `recovery_memory_budget` | 268435456 | Global recovery memory limit (bytes) |
| `presence_enabled` | False | Enable per-topic presence tracking |
| `presence_max_data_size` | 4096 | Max bytes for user presence data |
| `presence_max_members` | 0 | Max tracked members per topic (0 = unlimited) |

---

## API Overview

**Lifecycle**

- `start()` - start the server
- `stop()` - graceful shutdown
- `is_running()` - check server status

**Events**

- `enable_drain_mode()` - switch to batch-polling inbound events
- `drain_inbound(batch_size, timeout_ms)` - poll inbound events (auth_connect, msg, disconnect)
- `set_callbacks(on_connect, on_message, on_disconnect)` - callback-based event handling

**Send**

- `send(conn_id, data)` - send text to a connection
- `send_bytes(conn_id, data)` - send binary to a connection
- `send_event(conn_id, event)` - send a structured event (auto-serialized, auto-compressed)

**Broadcast**

- `broadcast_all(data)` - send to every connected client
- `broadcast_local(topic, data)` - fan-out to topic subscribers on this instance
- `broadcast(topic, data)` - fan-out to topic subscribers across all cluster nodes

**Topics**

- `subscribe_connection(conn_id, topics)` - subscribe a connection to topics
- `unsubscribe_connection(conn_id, topics)` - unsubscribe (None = all topics)
- `subscribe_with_recovery(conn_id, topics, recover, epoch, offset)` - subscribe and replay missed messages

**Presence**

- `presence(topic)` - get all members in a topic
- `presence_stats(topic)` - get lightweight member/connection counts
- `update_presence(conn_id, data)` - update a user's presence data across all topics

**Cluster**

- `connect_cluster(peers, cluster_port, ...)` - join a cluster mesh
- `cluster_connected()` - check if cluster is active
- `cluster_peers_count()` - number of connected peers

**Query**

- `get_connection_count()` - current connection count (lock-free atomic)
- `get_connections()` - list all connection IDs
- `health_snapshot()` - server health metrics dict

---

## Cluster Setup

```python
server.connect_cluster(
    peers=["10.0.0.2:9999", "10.0.0.3:9999"],
    tls_ca="/etc/wse/ca.pem",
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)
```

Nodes form a full TCP mesh automatically. Interest-based routing ensures messages only reach peers with matching subscribers. Peer connections are secured with mTLS. New nodes are discovered via gossip protocol: add one seed peer and the rest are found automatically. Inter-peer traffic is compressed with zstd.

---

## Client SDKs

**Python**

```bash
pip install wse-client
```

Async and sync clients, connection pool with health-scored failover, circuit breaker, auto-reconnection, E2E encryption, msgpack binary transport.

```python
from wse_client import connect

async with connect("ws://localhost:5007/wse", token="your-jwt") as client:
    await client.subscribe(["updates"])
    async for event in client:
        print(event.type, event.payload)
```

**TypeScript / React**

```bash
npm install wse-client
```

React hooks (`useWSE`), connection management, adaptive quality scoring, offline queue with IndexedDB persistence, binary transport.

```tsx
import { useWSE } from 'wse-client';

function App() {
  const { isConnected, connectionHealth } = useWSE({
    topics: ['updates'],
    endpoints: ['ws://localhost:5007/wse'],
  });

  return <div>Status: {connectionHealth}</div>;
}
```

---

## Performance

Benchmarked on AMD EPYC 7502P (64 cores, 128GB RAM), Ubuntu 24.04.

| Mode | Peak Deliveries/s | Connections |
|------|-------------------|-------------|
| Standalone | 14.4M del/s | 500K |
| Cluster | 2.6M del/s | 20K |
| Redis | 2.8M del/s | 500 |

Zero message loss at every tier. Sub-millisecond latency (0.38ms median with JWT auth).

See full results: [Benchmarks](docs/BENCHMARKS.md) | [Fan-out](docs/BENCHMARKS_FANOUT.md) | [Rust Client](docs/BENCHMARKS_RUST_CLIENT.md) | [Python Client](docs/BENCHMARKS_PYTHON_CLIENT.md) | [TypeScript Client](docs/BENCHMARKS_TS_CLIENT.md)

---

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Integration Guide](docs/INTEGRATION.md)
- [Protocol Reference](docs/PROTOCOL.md)
- [Cluster Protocol](docs/CLUSTER_PROTOCOL.md)
- [Security](docs/SECURITY.md)
- [Changelog](CHANGELOG.md)

---

## License

MIT
