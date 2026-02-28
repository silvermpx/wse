# Migration Guide

## Migrating from v1.x to v2.0

WSE v2.0 removes the FastAPI Router mode and Redis pub/sub in favor of a standalone Rust server with a native cluster protocol. This guide covers the breaking changes and migration steps.

---

## Breaking Changes

### Router Mode Removed

The FastAPI Router mode (`WSERouter`) has been removed. All WebSocket handling, message routing, and session management now runs entirely in Rust via `RustWSEServer`.

**Before (v1.x):**

```python
from wse_server import WSERouter
from fastapi import FastAPI

app = FastAPI()
router = WSERouter()
app.include_router(router.router)
```

**After (v2.0):**

```python
from wse_server import RustWSEServer

server = RustWSEServer("0.0.0.0", 5007, max_connections=10_000)
server.enable_drain_mode()
server.start()
```

The server runs independently on its own port. No FastAPI, no ASGI.

### Redis Pub/Sub Replaced by Cluster Protocol

Redis is no longer required for multi-instance coordination. The native cluster protocol handles cross-node message delivery via direct TCP connections.

**Before (v1.x):**

```python
server.connect_redis("redis://localhost:6379")
server.publish("topic", data)  # Via Redis
```

**After (v2.0):**

```python
server.connect_cluster(
    peers=["10.0.0.2:9999"],
    cluster_port=9999,
)
server.broadcast("topic", data)  # Via TCP mesh
```

### Method Renames

| v1.x | v2.0 | Notes |
|------|------|-------|
| `publish(topic, data)` | `broadcast(topic, data)` | Cross-instance fan-out |
| `publish_local(topic, data)` | `broadcast_local(topic, data)` | Local-only fan-out |
| N/A | `broadcast_all(data)` | Send to all connections |

### Removed Dependencies

The following packages are no longer required:

- `fastapi`
- `starlette`
- `redis`
- `uvicorn`

### Removed Modules

- `wse_server.router`
- `wse_server.dependencies`
- `wse_server.connection`
- `wse_server.core.pubsub`
- `wse_server.reliability`
- `wse_server.metrics`

---

## New Features in v2.0

### Cluster Protocol

Direct TCP mesh between server instances. Replaces Redis pub/sub with lower latency and no external dependencies.

```python
server.connect_cluster(
    peers=["10.0.0.2:9999"],
    tls_ca="/etc/wse/ca.pem",       # Optional: mTLS
    tls_cert="/etc/wse/node.pem",
    tls_key="/etc/wse/node.key",
    cluster_port=9999,
)
```

Features: interest-based routing, gossip discovery, mTLS, zstd compression, circuit breaker, heartbeat, dead letter queue.

### Presence Tracking

Per-topic user presence with automatic join/leave events.

```python
server = RustWSEServer(..., presence_enabled=True)
server.subscribe_connection(conn_id, ["chat"], {"status": "online"})
members = server.presence("chat")
```

### Message Recovery

Per-topic ring buffers for recovering missed messages on reconnect.

```python
server = RustWSEServer(..., recovery_enabled=True, recovery_buffer_size=256)
result = server.subscribe_with_recovery(conn_id, ["prices"], recover=True, epoch=e, offset=o)
```

### Protocol Negotiation

`client_hello`/`server_hello` handshake with feature discovery. The server advertises capabilities (compression, recovery, cluster, batching, msgpack), connection limits, and protocol version.

### Server-Initiated Ping

Server pings every client every 25 seconds. Connections with no activity for 60 seconds are force-closed (zombie detection).

### Rate Limit Feedback

Clients receive `rate_limit_warning` at 20% remaining capacity and `RATE_LIMITED` error when exceeded.

---

## Migration Steps

1. **Remove FastAPI/Router code.** Replace `WSERouter` with `RustWSEServer`. The server now runs on its own port.

2. **Update event handling.** Switch from FastAPI WebSocket handlers to `drain_inbound()` polling or `set_callbacks()`.

3. **Replace Redis with cluster.** If using `connect_redis()` for multi-instance, switch to `connect_cluster()`. If only using single-instance, no changes needed.

4. **Rename publish methods.** `publish()` -> `broadcast()`, `publish_local()` -> `broadcast_local()`.

5. **Remove unused dependencies.** Uninstall `fastapi`, `starlette`, `redis`, `uvicorn` if only used by WSE.

6. **Update client SDKs.** Both Python and TypeScript clients are wire-compatible with v2.0. Update to the latest version for recovery and presence support.

7. **Test.** Run your application and verify event handling, subscriptions, and message delivery work as expected.
