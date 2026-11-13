# WSE - WebSocket Engine

[![PyPI - Server](https://img.shields.io/pypi/v/wse-server)](https://pypi.org/project/wse-server/)
[![PyPI - Client](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![npm](https://img.shields.io/npm/v/wse-client)](https://www.npmjs.com/package/wse-client)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

High-performance WebSocket server built in Rust with native clustering, E2E encryption, message recovery, presence tracking, and real-time fan-out. Exposed to Python via PyO3 with zero GIL overhead on the data path. Ships with Rust-accelerated utilities for application code: priority queues, rate limiters, event sequencing, compression, cryptography, and JWT.

## Features

### Server

| Feature | Details |
|---------|---------|
| **Rust core** | tokio async runtime, tungstenite WebSocket transport, dedicated thread pool, zero GIL on the data path |
| **JWT authentication** | HS256, RS256, ES256 algorithms via jsonwebtoken crate. Validated during handshake (0.01ms), cookie + Authorization header extraction, key rotation, kid validation |
| **Protocol negotiation** | `client_hello`/`server_hello` handshake with feature discovery, capability advertisement, version agreement |
| **Topic subscriptions** | Per-connection topic subscriptions with automatic cleanup on disconnect |
| **Pre-framed broadcast** | WebSocket frame built once, shared via Arc across all connections, single allocation per broadcast |
| **Vectored writes** | `write_vectored` (writev syscall) batches multiple frames per connection in a single kernel call |
| **Write coalescing** | One write task per connection, the only writer to its socket: up to 256 control frames and every queued broadcast frame per turn |
| **DashMap state** | Lock-free sharded concurrent hash maps for topics, formats, activity tracking |
| **mimalloc allocator** | Global allocator optimized for multi-threaded workloads with frequent small allocations |
| **Deduplication** | 50,000-entry AHashSet with FIFO eviction per `send_event()` call |
| **Rate limiting** | Per-connection token bucket on what clients send: 100K capacity, 10K/s refill, client warning at 20% remaining |
| **Idle detection** | Server pings every 25s, closes connections that sent nothing for 60s; every server close is enforced 5s later |
| **Drain mode** | Lock-free crossbeam bounded channel, Python acquires GIL once per batch (not per event) |
| **Compression** | zlib for client-facing messages above threshold (default 1024 bytes) |
| **MessagePack** | Opt-in binary transport via `?format=msgpack`: everything the server sends such a client goes in msgpack (topics, broadcasts, replays, direct sends), and it may send in msgpack too; 24-41% fewer bytes than JSON on typical messages |
| **Queue groups** | Round-robin dispatch within named groups for load-balanced worker pools |
| **Topic ACL** | Per-connection allow/deny glob patterns for topic access control |
| **Graceful drain** | `drain()` sends Close frame to all clients, rejects new connections, notifies cluster peers |

### End-to-End Encryption

| Feature | Details |
|---------|---------|
| **Server identity** | The server signs every handshake with its identity key (ECDSA P-256, `e2e_identity_key`); clients pin the public key (`e2e_identity_public_key()`), so nothing in between, such as a TLS-terminating proxy, can answer for the server |
| **Key exchange** | Ephemeral ECDH P-256 on both sides, per connection |
| **Keys** | HKDF-SHA256 bound to the handshake transcript, one key schedule per direction |
| **Frames** | AES-256-GCM; the nonce is the frame's sequence number, and only the next frame opens: a replayed, reordered, dropped or foreign frame ends the connection |
| **Rekeying** | Each direction moves to a new key after 2^20 frames or 10 minutes |
| **Everything sealed** | After the handshake every frame is sealed, pings and protocol messages included |
| **Older clients** | `e2e_legacy=True` (the default) still serves 2.x clients with the first design: one key, random nonces, no server identity |

See [SECURITY.md](docs/SECURITY.md#end-to-end-encryption) for setup and the threat model.

### Cluster Protocol

| Feature | Details |
|---------|---------|
| **Topology** | Full TCP mesh, direct peer-to-peer connections |
| **Wire format** | Length-prefixed binary frames: 8-byte header + topic + payload, 13 message types |
| **Interest routing** | SUB/UNSUB/RESYNC frames, messages forwarded only to peers with matching subscribers |
| **Gossip discovery** | PeerAnnounce/PeerList frames, new nodes need one seed address to join |
| **mTLS** | rustls + tokio-rustls, P-256 certificates, WebPkiClientVerifier for both sides |
| **Compression** | zstd level 1 for payloads above 256 bytes, capability-negotiated, output capped at 1 MB |
| **Heartbeat** | 5s ping interval, 15s timeout, dead peer detection |
| **Circuit breaker** | 10 failures to open, 60s reset, 3 half-open probe calls |
| **Dead letter queue** | 1000-entry ring buffer for failed cluster sends |
| **Presence sync** | PresenceUpdate frames, last-write-wins; a full sync round on connect, and again for a peer that missed an update, drops what the round does not hold |
| **Topology API** | `cluster_info()` returns the linked peers: address, instance_id, capabilities, connected_at |

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
| **Per-message stamp** | When recovery is enabled, every topic broadcast carries `tp`/`e`/`o` (topic, epoch, offset) so clients dedup idempotently and detect gaps from the messages themselves |

### Client SDKs (Python + TypeScript/React)

| Feature | Details |
|---------|---------|
| **Auto-reconnection** | 4 strategies: exponential, linear, fibonacci, adaptive backoff with jitter |
| **Connection pool** | Multi-endpoint with health scoring, 3 load balancing strategies, automatic failover |
| **Circuit breaker** | CLOSED/OPEN/HALF_OPEN state machine, prevents connection storms |
| **Rate limiting** | Client-side token bucket, coordinates with server feedback |
| **E2E encryption** | Sessions with a pinned server identity (both clients speak the same protocol), the first design for 2.x servers |
| **Event sequencing** | Duplicate detection (sliding window) + per-topic `(epoch, offset)` idempotent dedup with automatic gap recovery |
| **Network monitor** | Real-time latency, jitter, and connection-quality scoring (Python client also estimates packet loss) |
| **Priority queues** | 5 levels from CRITICAL to BACKGROUND |
| **Offline queue** | IndexedDB persistence (TypeScript), replayed on reconnect |
| **Compression** | Automatic zlib for messages above threshold |
| **MessagePack** | `wire_format="msgpack"` / `wireFormat: 'msgpack'`: msgpack both ways |

### Transport Security

| Feature | Details |
|---------|---------|
| **Origin validation** | `allowed_origins` refuses browser upgrades from other origins (403), against CSWSH |
| **Cookie auth** | `access_token` HTTP-only cookie with `Secure + SameSite=Lax` (OWASP recommended for browsers) |
| **Frame protection** | 1 MB max frame size, serde_json parsing (no eval), escaped user IDs in server_ready; inbound msgpack rejected beyond 64 nesting levels |
| **Cluster frame protection** | zstd decompression output capped at 1 MB (MAX_FRAME_SIZE), protocol version validation |
| **Slowloris protection** | At most 512 concurrent pre-handshake connections; sockets that never complete the WS upgrade are bounded and dropped |
| **Bounded callback dispatch** | Callback mode caps in-flight `on_message` callbacks at 256 per connection; excess inbound work is shed and counted as rate-limited |
| **Fail-closed recovery ACL** | Recovery replay applies the connection's topic ACL — unauthorized topics are never replayed or position-probed |

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
| `jwt_secret` | None | JWT key for validation. HS256: shared secret (bytes, min 32). RS256/ES256: PEM public key. `None` disables auth |
| `jwt_issuer` | None | Expected `iss` claim. Skipped if `None` |
| `jwt_audience` | None | Expected `aud` claim. Skipped if `None` |
| `jwt_cookie_name` | "access_token" | Cookie name for JWT token extraction |
| `jwt_previous_secret` | None | Previous key for zero-downtime rotation. HS256: previous secret. RS256/ES256: previous public key PEM |
| `jwt_key_id` | None | Expected `kid` header claim. Rejects tokens with mismatched key ID |
| `jwt_algorithm` | None | JWT algorithm the server expects: `"HS256"` (default), `"HS384"`, `"HS512"`, `"RS256"`, `"RS384"`, `"RS512"`, `"PS256"`, `"PS384"`, `"PS512"`, `"ES256"`, `"ES384"` or `"EdDSA"`. The token's own `alg` header never overrides it. |
| `jwt_private_key` | None | PEM private key `issue_token()` signs with for the asymmetric algorithms (RS*, PS*, ES*, EdDSA). Not needed for the HMAC ones (HS*), which sign with `jwt_secret` |
| `max_inbound_queue_size` | 131072 | Client messages waiting for the application (drain queue or running callbacks) before new ones are dropped; connect, disconnect and presence events are never dropped |
| `recovery_enabled` | False | Enable per-topic message recovery buffers |
| `recovery_buffer_size` | 128 | Ring buffer slots per topic (rounded to power-of-2) |
| `recovery_ttl` | 300 | Buffer TTL in seconds before eviction |
| `recovery_max_messages` | 500 | Max messages returned per recovery response |
| `recovery_memory_budget` | 268435456 | Global memory limit for all recovery buffers (bytes, default 256 MB) |
| `presence_enabled` | False | Enable per-topic presence tracking |
| `presence_max_data_size` | 4096 | Max bytes for a user's presence metadata |
| `presence_max_members` | 0 | Max tracked members per topic (0 = unlimited) |
| `max_outbound_queue_bytes` | 16777216 | Per-connection outbound buffer limit (bytes, default 16 MB). Messages dropped when exceeded |
| `max_subscriptions_per_connection` | 0 | Max topics per connection (0 = unlimited). Prevents topic explosion DoS |
| `rate_limit_capacity` | 100000.0 | Messages a client may send in a burst (token bucket capacity per connection, at least 1; `float("inf")` for no limit) |
| `rate_limit_refill` | 10000.0 | Messages per second a client may send after the burst (refill rate, above 0) |
| `max_message_size` | 1048576 | Maximum WebSocket frame size in bytes (default 1 MB) |
| `ping_interval` | 25 | Server-initiated ping interval in seconds (at least 1, below `idle_timeout`) |
| `idle_timeout` | 60 | Close connections that sent nothing for this many seconds (code 4408) |
| `max_inbound_queue_bytes` | 67108864 | The same budget in bytes (default 64 MB); a single larger message still gets into an empty queue |
| `allowed_origins` | None | Origins (`scheme://host[:port]`) browsers may connect from; others get HTTP 403. `None` checks nothing |
| `client_subscriptions` | False | Answer the clients' own subscribe, unsubscribe and recovery requests in the server, within each connection's topic ACL; otherwise they reach the application as messages |
| `e2e_identity_key` | None | PEM or DER (PKCS#8) ECDSA P-256 private key the server signs E2E handshakes with; clients pin its public key, `e2e_identity_public_key()`. Without it, E2E clients must accept an unauthenticated server |
| `e2e_legacy` | True | Also serve the first E2E design (2.x clients); `False` refuses it with close code 4800 |

---

## API Reference

### Lifecycle

```python
server.start()                          # Bind and start (OSError if the port is taken)
server.stop()                           # Close every connection (1001) and stop
server.drain(4300, "", timeout=30)      # Close clients, wait, return how many were forced
server.is_running()                     # Check server status (bool)
server.is_draining()                    # True between drain() and the next start()

with RustWSEServer("0.0.0.0", 5007) as server:   # start() ... stop()
    ...
```

Rust-side logs go to standard output at level `info`; set `RUST_LOG` (for example `RUST_LOG=_wse_accel=debug`) to change it.

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

**Callback mode** - alternative to drain mode. The callbacks of one connection run in the order of its events (`on_connect` first, `on_disconnect` last) on the server's worker threads; `on_connect(conn_id, user_id)` gets the validated user id, or the Cookie header without `jwt_secret`.

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
| `send_event` | Single connection | JSON-serialized (msgpack for msgpack clients), compressed if above threshold, deduplication via 50K-entry FIFO window |
| `broadcast_all` | All connections | Pre-framed, single frame build shared via Arc |
| `broadcast_local` | Topic (local) | Pre-framed, DashMap subscriber lookup, stored in recovery buffer if enabled |
| `broadcast` | Topic (all nodes) | Local fan-out + forwarded to cluster peers with matching interest |

A client that asked for msgpack (`?format=msgpack`) gets in msgpack whatever would go to it as JSON text: topic messages, broadcasts, recovery replays, `send` and `send_event`. Bytes sent with `send_bytes` or `broadcast_all_bytes` go as they are.

### Topic Subscriptions

```python
server.subscribe_connection(conn_id, ["prices", "news"])              # Subscribe; returns the topics it took (ACL, limits)
server.subscribe_connection(conn_id, ["chat"], {"status": "online"})  # Subscribe with presence data
server.subscribe_connection(conn_id, ["tasks"], queue_group="workers") # Subscribe with queue group (round-robin)
server.unsubscribe_connection(conn_id, ["news"])                      # Unsubscribe from specific topics
server.unsubscribe_connection(conn_id, None)                          # Unsubscribe from all topics
server.get_topic_subscriber_count("prices")                           # Subscriber count for a topic
```

Subscriptions are cleaned up automatically on disconnect. In cluster mode, interest changes are propagated to peers via SUB/UNSUB frames.

**Queue groups**: connections in the same `queue_group` receive messages round-robin instead of fanout. Normal subscribers (no queue group) still receive all messages. Useful for distributing work across a pool of consumers.

### Topic ACL

Per-connection topic access control with glob pattern matching.

```python
# Allow only "user:*" topics, deny everything else
server.set_topic_acl(conn_id, allow=["user:*"])

# Allow "data:*" but deny "data:internal:*"
server.set_topic_acl(conn_id, allow=["data:*"], deny=["data:internal:*"])

# Must be called before subscribe_connection
server.subscribe_connection(conn_id, ["data:prices"])   # allowed
server.subscribe_connection(conn_id, ["data:internal:audit"])  # denied
```

Deny patterns take precedence over allow patterns. Supports `*` (any characters) and `?` (single character) wildcards. Applied at subscribe time.

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
# {"topics": {"prices": {"epoch": "0000007b", "offset": 456, "recovered": True, "count": 12}}}
```

The server maintains per-topic ring buffers (power-of-2 capacity, bitmask indexing). When recovery is enabled, every topic broadcast is stamped with `tp`/`e`/`o` (topic, epoch, offset) fields, so clients track their per-topic position from the messages themselves, drop duplicates idempotently by `(epoch, offset)`, and automatically request recovery when they detect an offset gap. On reconnect, the server replays missed messages from the ring buffer. If the gap is too large or the epoch has changed, the client receives a `NotRecovered` status and should re-subscribe from scratch.

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

# One host or a closed network: links share the WebSocket port (plaintext)
server.connect_cluster(peers=["10.0.0.2:5007"], single_port=True)

server.cluster_connected()       # True if connected to at least one peer
server.cluster_peers_count()     # Number of active peer connections
server.cluster_info()            # Linked peers (address, instance_id, capabilities, connected_at)
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
server.drain(close_code=4300, close_reason="shutting down", timeout=10)  # Graceful drain
server.inbound_queue_depth()         # Events waiting to be drained
server.inbound_dropped_count()       # Client messages dropped over the inbound budget
server.get_cluster_dlq_entries()     # Retrieve failed cluster messages from dead letter queue
```

`drain()` sends a WebSocket Close frame to all connected clients and rejects new connections. The drain wait runs as a separate task, so the command processor stays responsive. Use for zero-downtime deployments and rolling restarts.

---

## Security

### JWT Authentication

Rust-native HS256 validation during the WebSocket handshake. Zero GIL acquisition, 0.01ms per decode.

Token delivery:
- **Browser clients**: `access_token` HTTP-only cookie (set by your login endpoint, attached automatically by the browser)
- **Backend clients**: `Authorization: Bearer <token>` header and/or `access_token` cookie
- **API clients**: `Authorization: Bearer <token>` header

Required claims: `sub` (user ID), `exp` (expiration). Recommended: `iat` (issued at). Optional: `iss`, `aud` (validated if configured).

`server.issue_token(sub, ttl=900, claims=None)` signs a token with the server's own configuration, for a login endpoint in the same process; `server.reload_jwt_keys(...)` swaps keys on a running server.

### End-to-End Encryption

On top of TLS, a client can seal every frame for the server alone. The server proves who it is with an identity key:

```bash
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out e2e-identity.pem
```

```python
server = RustWSEServer("0.0.0.0", 5007, jwt_secret=..., e2e_identity_key=open("e2e-identity.pem", "rb").read())
print(server.e2e_identity_public_key())   # the key clients pin (base64)
```

Clients pin that key: `AsyncWSEClient(url, encryption=True, e2e_server_keys=[key])` in Python, `security: { encryptionEnabled: true, serverKeys: [key] }` in TypeScript. After the handshake every frame is sealed with a key per direction (AES-256-GCM, sequence numbers, rekeying); a replayed or forged frame ends the connection. Setup, key rotation and the threat model: [docs/SECURITY.md](docs/SECURITY.md#end-to-end-encryption).

### Rate Limiting

Per-connection token bucket on the messages clients send: 100,000 messages capacity, 10,000 messages/second refill. Clients receive a `rate_limit_warning` at 20% remaining capacity, and a `RATE_LIMITED` error when their messages are dropped; a client that keeps sending for 10 seconds after that is closed with code 4429. What the server sends is bounded by `max_outbound_queue_bytes` instead.

### Deduplication

`send_event()` maintains a 50,000-entry AHashSet with FIFO eviction. Duplicate message IDs are dropped before serialization.

### Idle Detection

Server pings every connected client every 25 seconds. Connections that sent nothing for 60 seconds are closed with code 4408; WebSocket protocol pings count as activity. A client that does not answer a close frame within 5 seconds is disconnected.

Full security documentation: [docs/SECURITY.md](docs/SECURITY.md)

---

## Wire Protocol

WSE uses a custom wire protocol with categorized JSON messages:

**Text frames:** JSON with `c` field for category: `"WSE"` (system), `"S"` (snapshot), `"U"` (update)

**Binary frames:** `C:` (zlib compressed), `M:` (MessagePack), `E:` (E2E sealed), raw zlib (0x78 magic byte)

**MessagePack transport:** opt-in per connection via the `?format=msgpack` query parameter (`wire_format="msgpack"` in the Python client, `wireFormat: 'msgpack'` in the TypeScript one). The server then sends that connection msgpack instead of JSON text, and reads msgpack from it; on typical messages that is 24-41% fewer bytes. E2E sealed frames carry it too.

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

Key features: 4 reconnect strategies (exponential, linear, fibonacci, adaptive), connection pool with health scoring and 3 load balancing strategies, circuit breaker, token bucket rate limiter, event sequencer with id dedup plus per-topic `(epoch, offset)` idempotent dedup and gap recovery, network quality monitoring (latency/jitter/packet loss).

See [python-client/](python-client/) for full source and examples.

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

See [ts-client/](ts-client/) for full source and examples.

---

## Performance

3.0 against 2.5.1 on the same machine (Intel Xeon Gold 5412U, 48 threads), server and benchmark client on separate cores:

| Test | 2.5.1 | 3.0 |
|------|-------|-----|
| Broadcast, 1,000 / 5,000 / 10,000 subscribers | 5.4 / 6.0 / 7.4M deliveries/s | **9.4 / 8.2M** / same |
| Topic through a cluster peer, 1,000 / 10,000 subscribers | 5.9 / 5.7M deliveries/s | **6.8 / 8.4M** |
| Inbound, 100 / 1,000 connections | 8.3 / 7.7M msg/s | same |
| Peak memory under a publisher that never pauses | 35-39 GB | **19-21 GB** |

"Same": within the run-to-run noise of the machine (about 10%).

Peak numbers measured with 2.x on AMD EPYC 7502P (32 cores / 64 threads, 128 GB RAM), Ubuntu 24.04:

| Mode | Peak Throughput | Connections | Message Loss |
|------|----------------|-------------|--------------|
| Standalone (fan-out) | 4.7M deliveries/s | 100K | 0% |
| Standalone (inbound JSON) | 14.7M msg/s | 500K | 0% |
| Standalone (inbound msgpack) | 30M msg/s | 500K | 0% |
| Cluster (2 nodes, 50/50) | 6.6M deliveries/s | 500 per node | 0% |

Sub-millisecond latency. Median 0.38ms with JWT authentication. Connection handshake: 0.53ms median (Rust JWT path).

Detailed results: [Benchmarks](docs/BENCHMARKS.md) | [Fan-out](docs/BENCHMARKS_FANOUT.md) | [Rust Client](docs/BENCHMARKS_RUST_CLIENT.md) | [Python Client](docs/BENCHMARKS_PYTHON_CLIENT.md) | [TS Client](docs/BENCHMARKS_TS_CLIENT.md)

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
- [Rust Utilities](docs/RUST_UTILITIES.md) - priority queues, rate limiters, sequencing, compression, crypto, JWT
- [Security](docs/SECURITY.md) - JWT, encryption, mTLS, rate limiting, circuit breaker
- [Deployment](docs/DEPLOYMENT.md) - production setup, Docker, Kubernetes, cluster configuration
- [Migration Guide](docs/MIGRATION.md) - upgrading from v1.x to v2.x
- [Contributing](CONTRIBUTING.md) - development setup and coding standards
- [Changelog](CHANGELOG.md) - version history

---

## License

MIT
