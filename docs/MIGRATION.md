# Migration Guide

## Migrating from v2.x to v3.0

3.0 changes behaviour that was unsafe or did not work as documented. Most applications change a few lines; the list below is everything that can break.

### Server

- **`connect_cluster()` needs `cluster_port`**, or `single_port=True` to take cluster links on the WebSocket port. Before, any connection to the WebSocket port that started like a cluster frame joined as an unauthenticated peer.
- **`start()` raises `OSError`** when the address is taken, instead of returning a server that is silently gone. `connect_cluster()` raises `OSError` when it cannot bind the cluster port.
- **`drain()` blocks** until the clients are gone or the timeout, and returns how many it had to drop.
- **JWT options are checked at startup.** HMAC secrets must be at least the hash size (32, 48 or 64 bytes); a PEM key is refused as an HMAC secret; `jwt_issuer` / `jwt_audience` now also reject tokens that omit the claim; any `jwt_*` option without `jwt_secret` is an error (before, such a server accepted everyone).
- **A malformed or empty `wse_topics` claim denies every topic** instead of allowing all of them.
- **The rate limit applies to what clients send**, as documented: messages over it are dropped with a `RATE_LIMITED` error and a client that keeps on is closed with 4429. `send_event()` is no longer throttled.
- **Connections close when their token expires** (code 4403); the clients reconnect with a fresh token.
- **New close codes**: 1013 (slow consumer), 4403, 4408 (idle), 4429, 4800, 4802, 4803. See [PROTOCOL.md](PROTOCOL.md#websocket-close-codes).
- **`on_connect(conn_id, user_id)`** in callback mode gets the validated user id (the Cookie header only without `jwt_secret`), and a connection's callbacks run in order on worker threads.
- **`subscribe_connection()` returns the topics it took.** Topic names are checked (not empty, at most 256 bytes, no control characters) and a connection holds at most 16 patterns.
- **A pattern subscription gets only the topics its connection's ACL allows.**
- **Out-of-range options raise `ValueError`** (ping_interval 0, rate limits below 1 or NaN, zero sizes).
- **Values that cannot be JSON raise `ValueError`**: a dict containing itself, or nesting deeper than 128 levels, in `send_event()`, presence data and `rust_jwt_encode()`.
- **Applications that authenticate connections themselves** (no `jwt_secret`) call `accept_connection(conn_id, user_id)` to send `server_ready`.
- **msgpack clients get msgpack everywhere** (topic messages, broadcasts, replays, replies), not only from `send_event()`.

### End-to-end encryption

The new E2E sessions authenticate the server, so a client can tell the server from whatever terminates TLS in between. The first design keeps working on both sides during the migration:

1. Upgrade the server; `e2e_legacy=True` (the default) keeps serving 2.x clients.
2. Give the server an identity key (`e2e_identity_key`, see [SECURITY.md](SECURITY.md#end-to-end-encryption)) and publish `e2e_identity_public_key()` to your clients.
3. Upgrade the clients with that key pinned:
   - Python: `AsyncWSEClient(url, encryption=True, e2e_server_keys=[SERVER_KEY])`
   - TypeScript: `security: { encryptionEnabled: true, serverKeys: [SERVER_KEY] }`
4. Once no 2.x client is left, set `e2e_legacy=False`.

With encryption on, a 3.x client refuses to start without a pinned key, `e2e_unauthenticated=True` / `security.unauthenticated: true`, or `e2e_legacy=True` / `security.legacyE2e: true` (for 2.x servers).

### Message signing is gone

It signed nothing: the Python client signed with a random key the server never knew, the TypeScript functions had no callers and the server never read a signature. Integrity in transit comes from TLS and the E2E sessions.

- TypeScript: drop `security.messageSignature`; the store's `security.messageSigningEnabled` is gone.
- Python: `WSEEvent.signature` and the `SecurityManager`'s signing and key rotation are gone.

### Python client

- `encryption=True` needs a pinned key, `e2e_unauthenticated` or `e2e_legacy` (above).
- `send()` returns `SendResult` (`SENT`, `QUEUED`, `FAILED`); the offline queue is opt-in (`offline_queue=True`).
- Every close except an auth rejection reconnects; `connect()` raises `WSEAuthError` at once on a rejected token.
- `wire_format="msgpack"` now sends msgpack too.

### TypeScript client

- `security.encryptionEnabled` needs `serverKeys`, `unauthenticated` or `legacyE2e` (above).
- One connection per page, shared by every `useWSE()` call; `maxAttempts: -1` (the default) never gives up.
- `wireFormat: 'msgpack'` is new.

---

## Migrating from v2.x to v2.4

v2.4.0 has **no breaking API changes**. The wire and constructor surface are
backward compatible; the changes below are opt-in or transparent.

- **Per-message recovery stamp.** When `recovery_enabled=True`, every topic
  broadcast now carries injected `tp`/`e`/`o` (topic, epoch, offset) fields. This
  is wire-compatible — older clients simply ignore the extra fields. The bundled
  Python and TypeScript clients use them automatically for idempotent dedup and
  gap recovery.
- **Client SDK behavior change (no API change).** Both clients no longer buffer or
  reorder out-of-order messages by the global `seq` (which was never a per-topic
  order). They now dedup idempotently by per-topic `(epoch, offset)` and trigger
  automatic gap recovery — messages are never held back waiting for a sequence.
  If you depended on the old reorder-buffer behavior, note that ordering is now
  per-topic via the recovery stamp.
- **Server hardening (no API change).** A pre-handshake connection cap (512)
  guards against slowloris; callback-mode `on_message` dispatch is bounded (256
  concurrent per connection, excess shed); recovery replay is fail-closed against
  the connection's topic ACL. No configuration is required.
- **TS client:** `WSE_VERSION` is now `'2.4.0'`; the NetworkMonitor reports real
  latency/jitter (no longer a constant reading) and no longer exposes an app-layer
  `packetLoss` field.

---

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

6. **Update client SDKs.** Both Python and TS clients are wire-compatible with v2.0. Update to the latest version for recovery and presence support.

7. **Test.** Run your application and verify event handling, subscriptions, and message delivery work as expected.
