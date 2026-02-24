# Redis Pub/Sub (Standalone Rust Server)

## Overview

The Rust standalone server includes a built-in Redis Pub/Sub module for multi-instance horizontal scaling. When multiple WSE server instances run behind a load balancer, Redis coordinates message delivery across all instances.

**Without Redis:** Single-instance mode. `broadcast()` and `send_event()` deliver to local connections only.

**With Redis:** Multi-instance mode. `publish()` sends via Redis, all instances receive and fan-out to their local connections.

```
Instance A                Instance B                Instance C
    |                         |                         |
    +------------- Redis Pub/Sub (shared) --------------+
    |                         |                         |
 User 1,2                  User 3,4                  User 5,6
```

## Architecture

### Rust Modules

| File | Purpose |
|------|---------|
| `server.rs` | PyO3 API, shared state, WebSocket transport |
| `redis_pubsub.rs` | Redis listener, fan-out dispatch, metrics, DLQ |
| `reliability.rs` | Circuit breaker, exponential backoff (reusable) |

### Data Flow

```
Python domain code
    |
    v
server.publish("user:123:events", payload)     # PyO3 call
    |
    v
RedisCommand::Publish → cmd_tx (unbounded channel)
    |
    v
listener_task → publish_with_retry → Redis PUBLISH wse:user:123:events
    |
    v
Redis broadcasts to ALL subscriber instances
    |
    v
listener_task → msg_stream.next() → strip "wse:" prefix
    |
    v
dispatch_to_subscribers(topic, payload)
    |
    ├── exact match: topic_subscribers["user:123:events"]
    └── glob match:  topic_subscribers["user:*:events"]
    |
    v
ConnectionHandle.tx.send(Message::Text(payload))  → WebSocket
```

### Topic Routing

Subscriptions are stored in two DashMaps:

- `topic_subscribers: DashMap<String, DashSet<String>>` — topic -> set of conn_ids
- `conn_topics: DashMap<String, DashSet<String>>` — conn_id -> set of topics

Fan-out supports exact match and glob patterns (`*`, `?`):

```python
server.subscribe_connection("conn-abc", ["user:123:events"])           # exact
server.subscribe_connection("conn-abc", ["user:*:events"])             # wildcard
server.subscribe_connection("conn-abc", ["user:123:events", "trades"]) # multiple
```

Deduplication prevents double delivery when a connection matches both exact and wildcard.

## PyO3 API Reference

### Setup

```python
from wse_server._wse_accel import RustWSEServer

server = RustWSEServer(
    "0.0.0.0", 5006,
    max_connections=10000,
    jwt_secret=b"secret",
    jwt_issuer="my-app",
    jwt_audience="my-api",
)
server.start()

# Connect to Redis (optional — server works without it)
server.connect_redis("redis://localhost:6379")
```

### Subscription Management

```python
# Subscribe a connection to topics (exact or wildcard)
server.subscribe_connection("conn-abc", ["user:123:events", "trades"])

# Unsubscribe from specific topics
server.unsubscribe_connection("conn-abc", ["trades"])

# Unsubscribe from ALL topics (on disconnect)
server.unsubscribe_connection("conn-abc")

# Get subscriber count for a topic
count = server.get_topic_subscriber_count("user:123:events")
```

### Publishing

Two publish methods — local and Redis:

```python
# Local fan-out to topic subscribers (NO Redis, single instance)
# Use this for domain events: market data, balance updates, trade fills
server.publish_local("bars:AAPL", '{"t":"bar","p":{"c":185.50}}')

# Redis fan-out (multi-instance horizontal scaling ONLY)
# Use this ONLY when running multiple WSE instances behind a load balancer
server.publish("user:123:events", '{"t":"balance_update","p":{"equity":50000}}')
```

**`publish_local(topic, data)`** dispatches to local topic subscribers directly.
No Redis connection required. Same topic matching (exact + glob).
Speed: same as broadcast (~2.1M deliveries/s).

**`publish(topic, data)`** sends via Redis for cross-instance delivery.
The `publish()` method:
1. Sends `RedisCommand::Publish` to the listener task via unbounded channel
2. Listener retries up to 3 times (100ms, 200ms delays)
3. On final failure, pushes to Dead Letter Queue
Speed: ~13K published messages/s (Redis round-trip bottleneck).

### Health Monitoring

```python
# Check Redis connection status
is_connected = server.redis_connected()  # bool

# Full health snapshot (all metrics)
health = server.health_snapshot()
# Returns dict:
# {
#     "connections": 42,
#     "inbound_queue_depth": 12,
#     "inbound_dropped": 0,
#     "redis_connected": True,
#     "redis_messages_received": 15000,
#     "redis_messages_published": 8000,
#     "redis_messages_delivered": 45000,
#     "redis_messages_dropped": 3,
#     "redis_publish_errors": 0,
#     "redis_reconnect_count": 1,
#     "redis_dlq_size": 0,
#     "uptime_secs": 3600.5,
# }

# Drain failed messages from Dead Letter Queue
entries = server.get_dlq_entries()
# Returns list of dicts:
# [{"channel": "wse:user:123:events", "payload": "...", "error": "Connection refused"}]
# NOTE: drain_all — entries are removed after reading
```

## Reliability Features

### Reconnection with Exponential Backoff

If Redis disconnects, the listener task automatically reconnects:

```
connect_and_run()  ← inner loop (handles one connection session)
    |
    ↓ Err (stream ended / connection lost)
    |
listener_task()    ← outer loop (reconnects with backoff)
    |
    ├── backoff.next_delay()  → 1s, 1.5s, 2.25s, ... up to 60s
    ├── +-20% jitter to avoid thundering herd
    └── Reset backoff on successful connection
```

**Config (matches Python PubSubBus):**
- Initial delay: 1s
- Multiplier: 1.5x
- Max delay: 60s
- Jitter: +/-20%

During backoff, incoming publish commands are sent to DLQ (not lost silently).

### Circuit Breaker

Two circuit breakers protect the system:

| Breaker | Protects | Failure Threshold | Reset Timeout |
|---------|----------|-------------------|---------------|
| `conn_breaker` | Redis connection attempts | 10 failures | 60s |
| `pub_breaker` | PUBLISH commands | 10 failures | 60s |

State machine: `CLOSED -> OPEN -> HALF_OPEN -> CLOSED`

- **CLOSED:** Normal operation. Count failures.
- **OPEN:** After 10 failures, reject all operations for 60s.
- **HALF_OPEN:** After timeout, allow 3 probe calls. 3 successes -> CLOSED, 1 failure -> OPEN.

### Dead Letter Queue (DLQ)

Failed publishes are stored in an in-memory ring buffer:

- Max entries: 1000 (oldest evicted on overflow)
- Populated on: publish retry exhaustion, circuit breaker open, Redis disconnected
- Drained via: `server.get_dlq_entries()` (returns and removes all entries)

### Publish Retry

Each PUBLISH command is retried up to 3 times:

```
Attempt 1 → fail → wait 100ms
Attempt 2 → fail → wait 200ms
Attempt 3 → fail → DLQ + increment publish_errors metric
```

### Metrics

All metrics are lock-free `AtomicU64` counters:

| Metric | Description |
|--------|-------------|
| `messages_received` | Messages received from Redis subscription |
| `messages_published` | Successful PUBLISH commands |
| `messages_delivered` | Fan-out deliveries to WebSocket connections |
| `messages_dropped` | Failed WebSocket sends (channel full/closed) |
| `publish_errors` | Failed PUBLISH after all retries |
| `reconnect_count` | Redis reconnection attempts |
| `connected` | Current connection status (bool) |

## Integration Pattern (SQV)

Current SQV uses Python publishers. Migration path to Rust pub/sub:

```python
# Before (Python PubSubBus):
await pubsub_bus.publish("wse:user:123:balance", json.dumps(payload))

# After (Rust server):
server.publish("user:123:balance", json.dumps(payload))
```

Domain publishers call `server.publish()` instead of `pubsub_bus.publish()`. The `wse:` prefix is added automatically by the Rust server.

## API Methods

| Method | Redis? | Speed | Use Case |
|--------|--------|-------|----------|
| `broadcast(data)` | No | ~2.1M del/s | All connections (system announcements) |
| `send_event(conn_id, event)` | No | Direct | One specific connection |
| `publish_local(topic, data)` | No | ~2.1M del/s | Topic fan-out, single instance (market data, domain events) |
| `publish(topic, data)` | Yes | ~13K msg/s | Horizontal scaling ONLY (multi-instance coordination) |
| `subscribe_connection()` | No | - | Register topic interest |
| `unsubscribe_connection()` | No | - | Remove topic interest |

**Decision guide:**
- Single server? Use `broadcast()` or `publish_local()`. No Redis needed.
- Multiple servers behind LB? Add `connect_redis()` + use `publish()`.
- Domain events (market data, trades, balances)? Always `publish_local()`.
- Redis is for orchestration only, never for single-instance message routing.

## Wire Format

Redis channels use `wse:` prefix: `wse:user:123:events`, `wse:trades`, `wse:system:*`.

The listener subscribes via `PSUBSCRIBE wse:*` to receive all WSE messages. On receive, the `wse:` prefix is stripped to get the topic name for local routing.

Payload is passed through as-is (typically JSON string). No additional encoding by the pub/sub layer.
