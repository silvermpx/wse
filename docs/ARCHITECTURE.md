# WSE Architecture

Technical reference for WSE internals. Covers the Rust server, PyO3 bridge, cluster protocol, message recovery, performance architecture, and client implementations.

---

## System Overview

WSE is a WebSocket server written in Rust, exposed to Python via PyO3. The entire transport layer - WebSocket accept, framing, ping/pong, JWT validation, rate limiting, topic fan-out, and cluster coordination - runs inside a tokio multi-threaded runtime. Python interacts with the server through a thin PyO3 interface, either polling for events (drain mode) or receiving callbacks.

```
Python Application
    |
    v
RustWSEServer (PyO3)
    |
    +-- tokio runtime (multi-threaded)
    |     +-- TCP listener (tungstenite WebSocket)
    |     +-- Connection tasks (one per client)
    |     +-- Cluster peer tasks (TCP mesh)
    |     +-- Ping/zombie detection task
    |     +-- Recovery cleanup task
    |
    +-- SharedState
          +-- connections: Arc<RwLock<HashMap<String, ConnectionHandle>>>
          +-- topic_subscribers: Arc<DashMap<String, DashSet<String>>>
          +-- conn_topics: DashMap<String, DashSet<String>>
          +-- conn_formats: DashMap<String, bool>
          +-- conn_rates: DashMap<String, PerConnRate>
          +-- conn_last_activity: DashMap<String, Instant>
          +-- connection_count: AtomicUsize
          +-- inbound_tx/rx: crossbeam bounded channel
          +-- recovery: Option<Arc<RecoveryManager>>
          +-- presence: Option<Arc<PresenceManager>>
          +-- cluster_cmd_tx: RwLock<Option<Sender<ClusterCommand>>>
          +-- cluster_metrics: Arc<ClusterMetrics>
          +-- cluster_dlq: Arc<Mutex<ClusterDlq>>
```

---

## Rust Server Internals

### Runtime and Threading

The server spawns a dedicated Rust thread that creates a tokio multi-threaded runtime. All async I/O - TCP accept, WebSocket framing, cluster peer communication, heartbeat timers - runs on tokio worker threads. Python threads never touch the hot path.

Key runtime properties:

- **One task per connection**: each WebSocket client gets a dedicated read task and write task.
- **TCP_NODELAY** set on accept for minimal latency.
- **mimalloc** global allocator for reduced fragmentation and better multi-threaded allocation performance.

### Connection Lifecycle

```
TCP accept
  -> TLS handshake (if configured)
  -> WebSocket upgrade (tungstenite)
  -> OnceLock handshake: extract cookies, format preference, Authorization header
  -> JWT validation in Rust (if jwt_secret configured, zero GIL)
  -> Spawn read task + write task
  -> Register in SharedState (connections, conn_topics, conn_last_activity)
  -> Push Connect/AuthConnect event to inbound channel
  ...
  -> Client disconnects or zombie detected
  -> Clean up all maps (connections, topics, rates, activity)
  -> Push Disconnect event to inbound channel
```

### Dual Write Path

Each connection has a write task that receives `WsFrame` variants through an unbounded mpsc channel:

- **`WsFrame::Msg(Message)`** - Per-connection messages (control frames, individual sends). Goes through tungstenite's framing pipeline: `feed()` then `flush()`.
- **`WsFrame::PreFramed(Bytes)`** - Broadcast messages with pre-built WebSocket frames. Bypasses tungstenite entirely and writes raw bytes to the TCP stream.

This separation means broadcasts avoid per-connection frame encoding. The WebSocket frame is built once and the resulting `Bytes` (backed by `Arc`) is shared across all connections.

### Write Coalescing

The write task batches up to 256 frames per iteration using `recv_many`:

```
Write Task Loop:
  1. rx.recv_many(&mut batch, 256)    - bulk atomic drain
  2. Separate batch into PreFramed and Msg vectors
  3. PreFramed: collect into IoSlice[], write_vectored (single writev syscall)
     - If partial write, write_all for remainder
  4. Msg: feed() each to tungstenite, single flush()
  5. Clear batch, loop
```

The `write_vectored` (writev) path sends all pending broadcast frames in a single syscall with zero memcpy - each `IoSlice` points directly into the `Bytes` buffer.

### Pre-framed WebSocket Broadcast

Server-to-client WebSocket frames do not require masking (per RFC 6455). This means every client receives identical bytes for the same broadcast message. WSE exploits this:

```
broadcast_all("hello")
  -> encode_ws_frame(0x01, "hello")   // opcode=text, build header+payload
  -> Bytes (Arc-shared, single allocation)
  -> WsFrame::PreFramed(bytes.clone()) sent to every connection's channel
  -> Write task: raw TCP write (bypasses tungstenite)
```

The frame encoding handles all three WebSocket payload length formats (7-bit, 16-bit, 64-bit) and sets FIN=1 on every frame.

---

## Rust Utility Modules

In addition to the transport server, WSE exports Rust-accelerated utility classes and functions for use in Python application code: priority queues, token bucket rate limiter, event sequencer, compression manager, AES-GCM encryption, ECDH key exchange, HMAC/SHA-256, JWT, event transformer, and MongoDB-style event filters.

These are the same modules that previously existed as pure Python in the old router-based server. They now run in Rust for 10-20x speedup while keeping the same Python API.

Full reference with usage examples: [RUST_UTILITIES.md](RUST_UTILITIES.md)

---

## PyO3 Bridge

### Two Inbound Modes

**Drain mode** (recommended for production):

Events are queued in a lock-free crossbeam bounded channel. Python polls with `drain_inbound(batch_size, timeout_ms)`, which releases the GIL while waiting on the channel and acquires it once for the entire batch conversion.

```python
server.enable_drain_mode()
while True:
    events = server.drain_inbound(256, 50)  # batch_size, timeout_ms
    for event in events:
        event_type, conn_id, data = event
        handle(event_type, conn_id, data)
```

**Callback mode**:

Python callbacks are invoked via `spawn_blocking` to run off tokio worker threads. The GIL is acquired per callback invocation. Simpler to use but higher overhead under load.

### Event Types

`drain_inbound()` returns a list of tuples: `(event_type, conn_id, data)`

| Event Type     | Data Field                              |
|----------------|-----------------------------------------|
| `"connect"`    | Cookie string from HTTP upgrade request |
| `"auth_connect"` | User ID extracted from JWT (validated in Rust) |
| `"msg"`        | Parsed dict (JSON auto-parsed by Rust)  |
| `"raw"`        | Unparsed text string                    |
| `"bin"`        | Raw bytes                               |
| `"disconnect"` | None                                    |
| `"presence_join"` | dict: user_id, topic, data           |
| `"presence_leave"` | dict: user_id, topic, data          |

### GIL Management

- **drain_inbound**: GIL released during channel wait (`py.detach()`). Acquired once for batch conversion to Python objects.
- **Callbacks**: GIL acquired via `spawn_blocking` per invocation.
- **send/broadcast methods**: No GIL needed - these send commands through a standard library `mpsc` channel to the tokio runtime.
- **PyObject conversion**: `pyobj_to_json` and `json_to_pyobj` handle all Python types including `datetime`, `UUID`, `Decimal`, `Enum`, nested `dict`/`list`, and `bytes` (hex-encoded).

### Outbound API

```
Python                          Rust Behavior
------                          -------------
server.send(conn_id, data)   -> Send raw text frame via connection channel
server.broadcast_all(data)   -> Pre-frame once, fan-out to all connections
server.broadcast_local(t, d) -> Topic lookup, fan-out to matching subscribers
server.broadcast(t, d)       -> Local fan-out + ClusterCommand::Publish to peers
```

---

## Message Flow

### Inbound (Client to Server)

```
Client WebSocket frame
  -> tokio read task
  -> Parse: JSON text / msgpack binary / raw text / binary
  -> Push InboundEvent to crossbeam channel
  -> Python: drain_inbound(256, 50)
  -> Returns list of (event_type, conn_id, payload) tuples
```

### Outbound (Server to Client)

```
server.send(conn_id, event_dict)
  -> Deduplication check (AHashSet, 50K FIFO window)
  -> Rate limit check (token bucket)
  -> Serialize: JSON with serde_json, or msgpack via rmpv
  -> Compress if above threshold (zlib)
  -> Build WSE envelope: {"t": type, "p": payload, "id": uuid, "seq": N, "ts": timestamp, "v": 1}
  -> Send through connection's mpsc channel
  -> Write task: tungstenite feed() + flush()
```

### Broadcast (Server to All/Topic)

```
server.broadcast_all(data)
  -> pre_frame_text(data) - build WebSocket frame once
  -> Clone Bytes (Arc refcount bump) to each connection channel
  -> Write tasks: raw TCP write via write_vectored

server.broadcast_local(topic, data)
  -> pre_frame_text(data)
  -> Store in RecoveryManager (if enabled, zero-copy Bytes sharing)
  -> Lookup topic_subscribers DashMap
  -> Collect matching ConnectionHandle senders (with dedup via AHashSet)
  -> Fan-out PreFramed to matched connections

server.broadcast(topic, data)
  -> broadcast_local(topic, data)          - local fan-out
  -> ClusterCommand::Publish{topic, data}  - forward to cluster peers
```

---

## Cluster Architecture

WSE supports horizontal scaling through a direct TCP mesh between server instances. Every peer connects to every other peer, forming a full mesh topology.

```
                     Load Balancer
                    /      |      \
               Node A    Node B    Node C
                  \        |       /
                TCP Mesh (full connectivity)
                 A <-> B, A <-> C, B <-> C
```

### Wire Protocol

Binary frame format - 8-byte header followed by topic and payload:

```
+--------+-------+-----------+-------------+-------+---------+
| type   | flags | topic_len | payload_len | topic | payload |
| u8     | u8    | u16 LE    | u32 LE      | bytes | bytes   |
+--------+-------+-----------+-------------+-------+---------+
  1 byte   1 byte  2 bytes     4 bytes       var     var
```

Twelve message types:

| Type (u8) | Name           | Purpose                                  |
|-----------|----------------|------------------------------------------|
| 0x01      | MSG            | Topic message payload                    |
| 0x02      | PING           | Heartbeat request                        |
| 0x03      | PONG           | Heartbeat response                       |
| 0x04      | HELLO          | Handshake with capabilities              |
| 0x05      | SHUTDOWN       | Graceful disconnect notification         |
| 0x06      | SUB            | Subscribe to topic (interest routing)    |
| 0x07      | UNSUB          | Unsubscribe from topic                   |
| 0x08      | RESYNC         | Full topic set synchronization           |
| 0x09      | PeerAnnounce   | Gossip: announce a new peer address      |
| 0x0A      | PeerList       | Gossip: share known peer list            |
| 0x0B      | PresenceUpdate | Presence join/leave/update for one user  |
| 0x0C      | PresenceFull   | Full presence state sync on peer connect |

### Handshake

When two peers connect, they exchange HELLO frames:

```
HELLO payload:
  magic:         "WSE\x00" (4 bytes)
  version:       u16 LE (protocol version, currently 1)
  instance_id:   UUID string (unique per server instance)
  capabilities:  u32 LE (bitfield of supported features)
```

Capability negotiation uses bitwise AND of both peers' capability fields. Currently defined capabilities:

| Bit | Flag                  | Feature                    |
|-----|-----------------------|----------------------------|
| 0   | CAP_INTEREST_ROUTING  | SUB/UNSUB/RESYNC support   |
| 1   | CAP_COMPRESSION       | zstd inter-peer compression|
| 2   | CAP_PRESENCE          | Presence sync frames       |

### Interest-Based Routing

Rather than flooding every MSG to every peer, WSE tracks which topics each peer has subscribers for:

- When a local client subscribes to a topic, the server sends a **SUB** frame to all peers.
- When the last local subscriber unsubscribes, the server sends an **UNSUB** frame.
- On peer reconnect, a **RESYNC** frame sends the complete local topic set.
- MSG frames are only forwarded to peers whose interest tables contain the topic.

This reduces inter-peer bandwidth proportionally to topic overlap between instances.

### Gossip Discovery

For dynamic environments (auto-scaling, container orchestration), peers discover each other through gossip:

- **PeerAnnounce**: when a node learns about a new peer, it broadcasts the address to all other connected peers (excluding the source).
- **PeerList**: periodically shared list of all known peer addresses.

Nodes that receive a previously unknown peer address initiate an outbound connection automatically.

### Inter-Peer Compression

MSG frames with payloads above 256 bytes are compressed with zstd (level 1) before transmission. The `FLAG_COMPRESSED` bit (0x01) in the frame flags byte signals compressed payloads. If the compressed output is not smaller than the original, the frame is sent uncompressed.

### mTLS

Cluster connections support mutual TLS using rustls + tokio-rustls:

- **P-256 certificates** with WebPKI validation.
- A single node certificate and key is used for both server (accept) and client (connect) roles.
- **WebPkiClientVerifier** requires all connecting peers to present a valid certificate signed by the shared CA.
- Configuration: `tls_cert`, `tls_key`, `tls_ca` (PEM format).

### Reliability

| Mechanism              | Configuration                                    |
|------------------------|--------------------------------------------------|
| Circuit breaker        | 10 failures to open, 60s reset, 3 half-open calls |
| Exponential backoff    | 1s initial, 1.5x multiplier, 60s max, 20% jitter |
| Heartbeat              | 5s interval, 15s timeout                         |
| Graceful shutdown      | SHUTDOWN frame sent to all peers before exit     |
| Dead letter queue      | 1000-entry ring for failed cluster sends         |

### Cluster Metrics

All counters are `AtomicU64` for lock-free concurrent updates:

| Counter              | Description                         |
|----------------------|-------------------------------------|
| messages_sent        | Total MSG frames sent to peers      |
| messages_delivered   | Successfully enqueued to local subs |
| messages_dropped     | Failed sends (channel full/closed)  |
| bytes_sent           | Total bytes sent to peers           |
| bytes_received       | Total bytes received from peers     |
| reconnect_count      | Number of peer reconnections        |
| connected_peers      | Current connected peer count        |
| unknown_message_types| Unrecognized frame types received   |

---

## Message Recovery

WSE provides automatic message recovery for clients that temporarily disconnect. When recovery is enabled, the server maintains per-topic ring buffers that store recent broadcast messages.

### Ring Buffer Design

```
TopicRecoveryBuffer
  +-- entries: Box<[Option<RecoveryEntry>]>   (power-of-2 sized)
  +-- mask: u64                                (capacity - 1, for bitmask indexing)
  +-- head_offset: u64                         (next write position)
  +-- tail_offset: u64                         (oldest valid position)
  +-- epoch: u32                               (unique per buffer creation)
  +-- total_bytes: usize                       (current memory usage)
  +-- last_write: Instant                      (for TTL eviction)
```

- Ring buffer capacity is a power of two (default: 128 slots, configurable via `buffer_size_bits`).
- Indexing uses bitmask: `offset & mask` instead of modulo, which compiles to a single AND instruction.
- When the buffer is full, the oldest entry is evicted by advancing `tail_offset`.

### Epoch

Each buffer gets a unique epoch generated from: `timestamp_nanos + PID + atomic_counter`. The epoch changes when a buffer is recreated (server restart, TTL eviction). Clients use the epoch to detect whether their cached position is still valid.

### Recovery Flow

```
Client reconnects with (topic, epoch, offset)
  -> RecoveryManager.recover(topic, epoch, offset)
     |
     +-- No buffer exists      -> NoHistory
     +-- Epoch mismatch        -> NotRecovered { current_epoch, current_offset }
     +-- Offset before tail    -> NotRecovered (gap too large, messages evicted)
     +-- Offset within range   -> Recovered { messages[], epoch, offset }
     +-- Truncated by cap      -> NotRecovered (too many messages to replay)
```

Three outcomes:

| Result        | Meaning                                           | Client Action         |
|---------------|---------------------------------------------------|-----------------------|
| Recovered     | Messages replayed from ring buffer                | Process messages      |
| NotRecovered  | Epoch mismatch or offset gap too large             | Re-subscribe (full)   |
| NoHistory     | No recovery buffer exists for this topic           | Re-subscribe (full)   |

### Memory Management

- **Global budget**: default 256 MB across all topic buffers.
- **TTL eviction**: buffers with no writes for 300 seconds (default) are removed.
- **LRU eviction**: if still over budget after TTL cleanup, oldest-written buffers are evicted first.
- A background cleanup task runs periodically to enforce these limits.
- Recovery entries store `Bytes` (Arc-shared with the broadcast path) - zero-copy between broadcast and recovery storage.

### Configuration

| Parameter              | Default   | Description                           |
|------------------------|-----------|---------------------------------------|
| buffer_size_bits       | 7         | Ring capacity = 2^7 = 128 messages    |
| history_ttl_secs       | 300       | Idle buffer eviction (seconds)        |
| max_recovery_messages  | 500       | Max messages returned per recovery    |
| global_memory_budget   | 256 MB    | Total memory across all topic buffers |

### Cluster Limitation

Message recovery is **local to each node**. Recovery ring buffers are not replicated across cluster peers. If a client reconnects to a different node than the one it was originally connected to, recovery will return `NoHistory` because that node has no record of the client's previous position.

For cluster deployments, use **sticky sessions** (e.g., HAProxy with `stick-table`, or consistent hashing by user ID) to ensure clients reconnect to the same node. Alternatively, accept that recovery works only for same-node reconnections and handle `NoHistory` by re-subscribing from scratch.

---

## Presence Tracking

WSE provides per-topic presence tracking that monitors which users are active in each topic. Presence state is managed entirely in Rust for lock-free concurrent access.

### State Model

```
PresenceManager
  +-- topic_presence: DashMap<String, DashMap<String, PresenceEntry>>
  |     Topic -> User ID -> { user_id, data (JSON), connections: Vec<ConnMeta>, updated_at }
  +-- topic_presence_stats: DashMap<String, PresenceStats>
  |     Topic -> { num_users: AtomicUsize, num_connections: AtomicUsize }
  +-- conn_user_id: DashMap<String, String>
  |     Connection ID -> User ID (populated from JWT `sub` claim)
  +-- conn_presence_topics: DashMap<String, DashSet<String>>
        Connection ID -> Set of topics where this connection has presence
```

### User-Level Grouping

Presence is tracked per user, not per connection. Multiple WebSocket connections from the same user (identified by their JWT `sub` claim) share a single presence entry. This means:

- **Join event**: emitted only when the first connection for a user subscribes to a topic
- **Leave event**: emitted only when the last connection for a user is removed from a topic
- **Connection list**: each `PresenceEntry` tracks all active connection IDs for that user, enabling accurate multi-tab/multi-device presence

### Lifecycle

Presence tracking is tied to the subscription lifecycle:

1. **Subscribe with presence data**: `subscribe_connection(conn_id, topics, presence_data)` calls `PresenceManager::track()` for each topic. If this is the user's first connection in the topic, a `presence_join` event is broadcast.
2. **Unsubscribe**: `unsubscribe_connection(conn_id, topics)` calls `PresenceManager::untrack_topics()`. If this was the user's last connection, a `presence_leave` event is broadcast.
3. **Disconnect**: `PresenceManager::remove_connection()` removes the connection from all topics. Any topics where the user has no remaining connections emit a `presence_leave`.

### TTL Sweep

A background task runs every 30 seconds calling `sweep_dead_connections()`. This checks every tracked connection against the active connections map. Entries where all connections are dead are removed, and `presence_leave` events are broadcast for any affected users. This handles edge cases where a connection was not properly cleaned up (e.g., network partition).

### Cluster Sync

In cluster mode, presence state is synchronized between nodes using two frame types:

- **PresenceUpdate (0x0B)**: sent on every join, leave, or data update. Contains the topic, user ID, action byte (0=join, 1=leave, 2=update), timestamp, and presence data JSON. Peers apply the update using CRDT-style last-write-wins conflict resolution based on the `updated_at` timestamp.
- **PresenceFull (0x0C)**: sent when a new peer connects. Contains the full presence state as JSON, keyed by topic and user ID. The receiver merges this state, creating entries for remote users with sentinel connection IDs (`__remote__<user_id>`).

Remote-only entries (users connected to other nodes) are included in `presence()` query results but are distinguished internally. When a remote leave is received, only entries with no local connections are removed, preventing a remote leave from clearing a user who is also connected locally.

The `CAP_PRESENCE` capability flag (bit 2) in the HELLO handshake controls whether presence sync is active between two peers. Both nodes must advertise this capability for presence frames to be exchanged.

---

## Performance Architecture

### Pre-framed Broadcast

The single largest optimization. For a broadcast to N connections, the WebSocket frame (header + payload) is encoded once into a `Bytes` object. Each connection receives an `Arc` clone (reference count bump, no copy). Write tasks send the raw bytes directly to the TCP stream, bypassing tungstenite's per-message encoding.

### DashMap

Most shared state maps (`topic_subscribers`, `conn_topics`, `conn_formats`, `conn_rates`, `conn_last_activity`) use `DashMap` - a sharded concurrent hash map. The `connections` map uses `Arc<RwLock<HashMap>>` since connection registration and removal are relatively infrequent compared to reads during fan-out.

### Vectored Writes (writev)

The write task collects all pending `PreFramed` bytes into `IoSlice` arrays and issues a single `write_vectored` syscall. This sends multiple broadcast frames to one client in a single kernel call without copying them into a contiguous buffer.

### mimalloc

The Rust binary uses mimalloc as the global allocator. This provides better performance than the system allocator for multi-threaded workloads with frequent small allocations (connection state, frame buffers, hash map entries).

### Deduplication

`send_event()` maintains an `AHashSet` with a 50,000-entry FIFO eviction window. Events with duplicate IDs are dropped before serialization, preventing redundant work and bandwidth waste in multi-path delivery scenarios.

### Rate Limiting

Token bucket per connection:

| Parameter    | Value   |
|--------------|---------|
| Capacity     | 100,000 |
| Refill rate  | 10,000/s|

When a connection exceeds its rate limit:
1. An error event with `RATE_LIMITED` code and `retry_after` hint is sent to the client.
2. A warning is emitted at 20% remaining capacity (throttled via `last_warning` timestamp to avoid log spam).
3. The offending message is dropped.

### Serialization

- **JSON**: `serde_json` for serialization, with a custom `pyobj_to_json` converter that handles Python `datetime`, `UUID`, `Decimal`, `Enum`, `bytes` (hex), nested dicts/lists.
- **msgpack**: `rmpv` for binary serialization. Same type coverage as JSON path. Clients negotiate format via the WebSocket handshake (`wants_msgpack` flag).
- **Compression**: zlib (flate2) for client-facing messages above the configurable threshold (default 1024 bytes). zstd for inter-peer cluster messages above 256 bytes.

---

## Frontend Architecture (TypeScript/React)

```
client/
+-- index.ts                       Public API exports
+-- types.ts                       TypeScript interfaces and types
+-- constants.ts                   Protocol constants, event maps
+-- hooks/
|   +-- useWSE.ts                  Main React hook (single entry point)
+-- stores/
|   +-- useWSEStore.ts             Zustand store (connection state, metrics)
|   +-- useMessageQueueStore.ts    Message queue state
+-- services/
|   +-- ConnectionManager.ts       WebSocket lifecycle management
|   +-- MessageProcessor.ts        Inbound message parsing and routing
|   +-- ConnectionPool.ts          Multi-endpoint connection pooling
|   +-- NetworkMonitor.ts          Online/offline detection
|   +-- RateLimiter.ts             Client-side rate limiting
|   +-- OfflineQueue.ts            IndexedDB persistence for offline messages
|   +-- EventSequencer.ts          Sequence gap detection
|   +-- AdaptiveQualityManager.ts  Dynamic quality adjustment
+-- protocols/
|   +-- compression.ts             zlib decompression (pako)
|   +-- transformer.ts             Domain event -> CustomEvent mapping
+-- handlers/
|   +-- index.ts                   Handler registry
|   +-- EventHandlers.ts           Generic event handling
+-- utils/
    +-- logger.ts                  Structured logging with levels
    +-- circuitBreaker.ts          Client-side circuit breaker
    +-- security.ts                AES-GCM encryption, HMAC signing, ECDH key exchange
```

### useWSE Hook

Single entry point for all WSE functionality in React:

```typescript
const {
  connectionHealth,   // ConnectionState enum
  subscribe,          // (topics: string[], options?) => void
  unsubscribe,        // (topics: string[]) => void
  sendMessage,        // (type: string, payload: any, options?) => void
  sendBatch,          // (messages: Array<{type, payload}>) => void
  stats,              // ConnectionMetrics
  activeTopics,       // string[]
  diagnostics,        // NetworkDiagnostics | null
  forceReconnect,     // () => void
  requestSnapshot,    // (topics?) => void
} = useWSE(token, initialTopics, config);
```

Configuration:

```typescript
interface WSEConfig {
  endpoints: string[];              // WebSocket URLs
  reconnection?: {
    mode?: string;                  // default: 'adaptive'
    baseDelay?: number;             // default: 1000
    maxDelay?: number;              // default: 30000
    maxAttempts?: number;           // default: 10
    factor?: number;                // default: 1.5
    jitter?: boolean;               // default: true
  };
  security?: {
    encryptionEnabled?: boolean;    // default: false
    messageSignature?: boolean;     // default: false
  };
  performance?: {
    compressionThreshold?: number;  // default: 1024
    batchSize?: number;             // default: 10
    batchTimeout?: number;          // default: 500
    maxQueueSize?: number;          // default: 10000
  };
  offline?: {
    enabled?: boolean;              // default: true
    maxSize?: number;               // default: 1000
    persistToStorage?: boolean;     // default: true
  };
  diagnostics?: {
    enabled?: boolean;              // default: false
    sampleRate?: number;            // default: 0.1
  };
}
```

### Client Message Processing

```
WebSocket.onmessage
  -> Binary: check C:/E:/M: prefix -> decompress/decrypt/unpack
  -> Text: strip WSE/S/U prefix -> JSON.parse
  -> Deduplication check (message ID)
  -> Sequence check (gap detection)
  -> Handler registry lookup by event type
  -> Domain handler (custom handlers per event type)
  -> window.dispatchEvent(new CustomEvent(eventType, { detail: payload }))
  -> React Query cache merge (via event listener)
```

---

## Python Client Architecture

```
python-client/wse_client/
+-- __init__.py            Public API: connect(), AsyncWSEClient, SyncWSEClient
+-- client.py              AsyncWSEClient (async context manager, async iterator)
+-- sync_client.py         SyncWSEClient (threaded wrapper for sync code)
+-- connection.py          ConnectionManager (lifecycle, reconnect, heartbeat)
+-- protocol.py            MessageCodec (prefix strip, JSON parse, envelope)
+-- compression.py         zlib compress/decompress
+-- msgpack_handler.py     msgpack encode/decode (optional dependency)
+-- security.py            ECDH P-256, AES-GCM-256, HMAC-SHA256
+-- circuit_breaker.py     CLOSED/OPEN/HALF_OPEN state machine
+-- rate_limiter.py        Token bucket
+-- event_sequencer.py     Dedup + out-of-order buffering
+-- connection_pool.py     Multi-endpoint, health scoring, 3 LB strategies
+-- network_monitor.py     Latency/jitter/quality analysis
+-- types.py               Enums, dataclasses (WSEEvent, ConnectionState)
+-- errors.py              WSEError hierarchy
+-- constants.py           Protocol version, timeouts, thresholds
```

### AsyncWSEClient

Primary interface. Async context manager and async iterator:

```python
async with connect("ws://localhost:5006/wse", token="jwt") as client:
    await client.subscribe(["notifications"])
    async for event in client:
        print(event.type, event.payload)
```

### Python Client Message Processing

```
WebSocket.recv()
  -> Binary: check C:/E:/M: prefix -> decompress/decrypt/unpack
  -> Text: strip WSE/S/U prefix -> json.loads
  -> Deduplication check (event ID, 10K window)
  -> Sequence check (gap detection, reorder buffer)
  -> System handler dispatch (server_ready, error, PONG, etc.)
  -> User handler callbacks (@client.on("type"))
  -> Async iterator queue (async for event in client)
```

### Wire Compatibility

The Python client and TypeScript client speak the same wire protocol. All prefix formats, binary frame detection, encryption handshake, and message envelope structure are identical. Both clients can connect to the same server simultaneously.

---

## Horizontal Scaling

### Single Instance

A single WSE server handles all connections locally. Topic fan-out goes through `broadcast_local()`, which looks up subscribers in the `topic_subscribers` DashMap and enqueues pre-framed messages to matching connections.

### Multi-Instance (Cluster)

```
                     Load Balancer
                    /      |      \
               Node A    Node B    Node C
                  \        |       /
                TCP Mesh (direct peer connections)
```

When `broadcast(topic, data)` is called:

1. Local fan-out to subscribers on this instance (same as `broadcast_local`).
2. `ClusterCommand::Publish` sent to the cluster manager task.
3. Cluster manager checks each peer's interest table.
4. MSG frame sent only to peers with matching topic subscriptions.
5. Receiving peers dispatch to their local subscribers via `collect_topic_senders()`.

---

## Concurrency Model Summary

```
+-------------------+----------------------------+---------------------------+
| Component         | Concurrency Primitive      | Access Pattern            |
+-------------------+----------------------------+---------------------------+
| connections       | Arc<RwLock<HashMap>>        | Read-heavy, write on connect/disconnect |
| topic_subscribers | Arc<DashMap<String, DashSet>>| Concurrent read/write     |
| conn_topics       | DashMap<String, DashSet>    | Concurrent read/write     |
| conn_formats      | DashMap<String, bool>       | Per-connection read/write  |
| conn_rates        | DashMap                     | Per-connection write       |
| conn_last_activity| DashMap                     | Per-connection write       |
| inbound channel   | crossbeam bounded channel   | MPSC, lock-free           |
| connection channel| tokio mpsc::unbounded       | SPSC per connection       |
| cluster peer data | tokio mpsc::bounded(10K)    | Per-peer backpressure     |
| cluster command   | tokio mpsc::unbounded       | MPSC                      |
| recovery buffers  | DashMap<TopicRecoveryBuffer>| Per-topic write lock      |
| presence state    | DashMap<DashMap<PresenceEntry>>| Per-topic per-user write |
| presence stats    | DashMap<AtomicUsize counters>| Lock-free                |
| dedup state       | Mutex<AHashSet + VecDeque>  | Exclusive on send_event   |
| cluster metrics   | AtomicU64 counters          | Lock-free                 |
| connection count  | AtomicUsize                 | Lock-free                 |
+-------------------+----------------------------+---------------------------+
```
