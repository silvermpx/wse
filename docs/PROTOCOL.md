# WSE Wire Protocol Specification

Version: 1.0

## Message Format

All WSE messages are JSON objects with the following structure:

```json
{
  "t": "event_type",
  "id": "019c53c4-abcd-7def-8901-234567890abc",
  "ts": "2026-02-20T15:30:00.000Z",
  "seq": 42,
  "p": {
    "key": "value"
  },
  "v": 1
}
```

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `v` | `int` | Protocol version (currently `1`) |
| `id` | `string` | Unique message ID (UUID v7) |
| `t` | `string` | Event type (e.g., `status_update`) |
| `ts` | `string` | ISO 8601 timestamp with timezone |
| `seq` | `int` | Monotonically increasing sequence number |
| `p` | `object` | Event payload (type-specific data) |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `sig` | `string` | JWT signature for signed messages |
| `pri` | `int` | Priority (1=BACKGROUND, 3=LOW, 5=NORMAL, 8=HIGH, 10=CRITICAL) |
| `cid` | `string` | Correlation ID for request/response pairing |
| `wse_processing_ms` | `int` | Server-side processing time in milliseconds |

## Message Categories

Messages are prefixed with a category identifier before the JSON payload:

| Prefix | Category | Description | Examples |
|--------|----------|-------------|----------|
| `WSE` | System | Protocol-level messages | `server_ready`, `PING`, `PONG`, `error` |
| `S` | Snapshot | Full state snapshots | `resource_snapshot`, `user_data_snapshot` |
| `U` | Update | Incremental updates | `status_update`, `data_update` |

### Wire Format

```
WSE{"t":"server_ready","p":{...},"v":1}
S{"t":"resource_snapshot","p":{...},"v":1}
U{"t":"status_update","p":{...},"v":1}
```

The client must strip the prefix before JSON parsing:

```typescript
if (data.startsWith('WSE{')) json = data.slice(3);
else if (data.startsWith('S{') || data.startsWith('U{')) json = data.slice(1);
```

## Transport Modes

### Text Mode (default)

Standard WebSocket text frames with category prefix + JSON.

### Binary Mode: Compression

When a message exceeds the compression threshold (default: 1024 bytes), the server sends a binary frame with `C:` prefix:

```
C:<zlib-compressed-data>
```

The client detects the `C:` prefix, strips it, decompresses with zlib, and parses the resulting JSON string (which still includes the category prefix).

### Binary Mode: Encryption

When encryption is enabled, the server sends a binary frame with `E:` prefix:

```
E:<IV (12 bytes)><AES-GCM encrypted data>
```

Encryption uses:
- **Algorithm**: AES-GCM-256
- **IV**: 12 bytes (96 bits), unique per message
- **Key exchange**: ECDH P-256

### Binary Mode: MessagePack

For ultra-compact serialization, `M:` prefix indicates msgpack:

```
M:<msgpack-encoded-data>
```

## Connection Lifecycle

### 1. Connect

```
Client -> Server: WebSocket UPGRADE /wse?token=<JWT>&compression=true&protocol_version=1
Server -> Client: 101 Switching Protocols
```

### 2. Server Ready

Immediately after connection:

```json
WSE{"t":"server_ready","p":{
  "message": "Connection established",
  "details": {
    "version": 1,
    "features": {
      "compression": true,
      "encryption": false,
      "batching": true,
      "priority_queue": true,
      "circuit_breaker": true,
      "message_signing": false,
      "health_check": true,
      "metrics": true
    },
    "connection_id": "ws_f186cd9d_019c53c4",
    "server_time": "2026-02-20T15:30:00.000Z",
    "user_id": "019c53c4-abcd-7def-8901-234567890abc"
  }
}}
```

### 3. Auto-Subscribe

Server automatically subscribes the connection to default topics and sends subscription confirmation:

```json
U{"t":"subscription_update","p":{
  "action": "subscribe",
  "success": true,
  "topics": ["notifications", "data_events", ...],
  "success_topics": [...],
  "active_subscriptions": [...]
}}
```

### 4. Snapshots

After subscription, server sends full state snapshots:

```json
S{"t":"resource_snapshot","p":{
  "resources": [...],
  "count": 2,
  "timestamp": "2026-02-20T15:30:00.123Z"
}}
```

### 5. Incremental Updates

Real-time updates follow snapshots:

```json
U{"t":"status_update","p":{
  "resource_id": "...",
  "connected": true,
  "response_time_ms": 45
}}
```

### 6. Heartbeat

Server sends heartbeat every 15 seconds:

```json
WSE{"t":"heartbeat","p":{"timestamp":1708441800000,"sequence":42},"v":1}
```

Server also sends JSON PING for latency measurement:

```json
WSE{"t":"PING","p":{"timestamp":1708441800000},"v":1}
```

Client responds with:

```json
{"t":"PONG","p":{"client_timestamp":1708441800000,"server_timestamp":1708441800050}}
```

### 7. Disconnect

- **Idle timeout**: Server closes after 90s of inactivity (code 1000)
- **Circuit breaker**: Server closes after too many errors (code 1011)
- **Auth failure**: Server closes with code 4401
- **Normal close**: Code 1000

## Client-to-Server Messages

### Subscribe / Unsubscribe

```json
{
  "t": "subscription",
  "p": {
    "action": "subscribe",
    "topics": ["notifications", "data_updates"]
  }
}
```

### Sync Request

Request initial data snapshots:

```json
{
  "t": "sync_request",
  "p": {
    "topics": ["notifications", "data_events"],
    "include_snapshots": true
  }
}
```

### Client Hello

Protocol negotiation:

```json
{
  "t": "client_hello",
  "p": {
    "client_version": "1.0.0",
    "protocol_version": 1,
    "features": {
      "compression": true,
      "batch_messages": true
    },
    "capabilities": ["compression", "encryption", "batching"]
  }
}
```

### Config Update

Dynamic configuration:

```json
{
  "t": "config_update",
  "p": {
    "compression_enabled": true,
    "compression_threshold": 2048,
    "batching_enabled": true,
    "batch_size": 20,
    "batch_timeout": 0.2
  }
}
```

### Health Check

```json
{
  "t": "health_check",
  "p": {}
}
```

### Metrics Request

```json
{
  "t": "metrics_request",
  "p": {}
}
```

## Batching

When the client supports batching (`batch_messages: true` in features), the server may send multiple messages in a single frame:

```json
U{"t":"batch","p":{
  "messages": [
    {"t":"data_update","p":{...}},
    {"t":"status_update","p":{...}}
  ],
  "count": 2
}}
```

## Message Signing

Critical operations are signed with JWT:

```json
U{"t":"action_completed","p":{...},"sig":"eyJhbGciOiJIUzI1NiJ9...","v":1}
```

The `sig` field contains a JWT with:
- `hash`: SHA-256 hash of the JSON-serialized payload
- `signed_at`: Unix timestamp
- `nonce`: Random 16-byte hex string

### Signed Message Types

Configure which event types require signing based on your application's security requirements. Typical candidates include state-changing operations, financial transactions, and configuration changes.

## Error Codes

| Code | Meaning |
|------|---------|
| `AUTH_FAILED` | JWT authentication failed |
| `RATE_LIMIT_EXCEEDED` | Client sending too fast |
| `MESSAGE_TOO_LARGE` | Message exceeds 1 MB limit |
| `INVALID_SUBSCRIPTION` | No topics specified |
| `INVALID_ACTION` | Unknown subscription action |
| `HANDLER_ERROR` | Server-side handler error (recoverable) |
| `CIRCUIT_BREAKER_OPEN` | Too many errors, connection closing |
| `ENCRYPTION_REQUIRED` | Operation requires encryption |
| `SYNC_ERROR` | Snapshot sync failed |
| `SNAPSHOT_ERROR` | Individual snapshot failed |
| `SERVER_ERROR` | Internal server error |
| `INIT_ERROR` | Connection initialization failed |

## WebSocket Close Codes

| Code | Meaning |
|------|---------|
| 1000 | Normal closure / idle timeout |
| 1011 | Server error / circuit breaker |
| 4401 | Authentication required |

## Default Topics

Configure default topics for your application. Example topics:

| Topic | Description |
|-------|-------------|
| `notifications` | User notification events |
| `data_events` | Data lifecycle events |
| `user_events` | User account events |
| `system_events` | System-wide announcements |
| `monitoring_events` | Monitoring and diagnostics |
