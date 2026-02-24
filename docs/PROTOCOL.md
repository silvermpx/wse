# WSE Wire Protocol Specification

Version: 1 (Rust transport)

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
| `t` | `string` | Event type (e.g., `user_update`, `chat_message`) |
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
| `S` | Snapshot | Full state snapshots | `user_snapshot`, `channel_snapshot` |
| `U` | Update | Incremental updates | `user_update`, `chat_message` |

### Wire Format

```
WSE{"t":"server_ready","p":{...},"v":1}
S{"t":"user_snapshot","p":{...},"v":1}
U{"t":"user_update","p":{...},"v":1}
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

When encryption is enabled, messages are sent as binary frames with `E:` prefix:

```
E:<IV (12 bytes)><AES-GCM ciphertext + 16-byte auth tag>
```

Encryption spec:

| Parameter | Value |
|-----------|-------|
| Algorithm | AES-GCM-256 |
| IV | 12 bytes (96 bits), random per message |
| Auth tag | 16 bytes (128 bits), appended to ciphertext |
| Key exchange | ECDH P-256 with HKDF-SHA256 |
| HKDF salt | `wse-encryption` (UTF-8 bytes) |
| HKDF info | `aes-gcm-key` (UTF-8 bytes) |
| HKDF hash | SHA-256 |
| Derived key | 32 bytes (256 bits) |

**Key exchange flow:**

1. Server generates ECDH P-256 keypair during connection setup
2. Server sends its public key (65 bytes, uncompressed SEC1) in `server_ready`
3. Client generates its own ECDH keypair
4. Client derives shared secret from server's public key
5. Client sends its public key in `client_hello`
6. Server derives the same shared secret from client's public key
7. Both sides run HKDF to get the AES-256 key
8. All subsequent `E:`-prefixed messages use this key

The plaintext inside the encrypted envelope is the full message including category prefix (e.g. `U{...}`).

### Binary Mode: JSON (default since v1.1)

All JSON messages are sent as WebSocket binary frames instead of text frames. This eliminates UTF-8 validation overhead in the WebSocket layer, providing 5-15% higher throughput. The payload format is identical — category prefix followed by JSON — but delivered in a binary frame.

Clients should handle both text and binary frames containing JSON for backward compatibility.

### Binary Mode: MessagePack

For compact serialization, `M:` prefix indicates msgpack-encoded data:

```
M:<msgpack-encoded-data>
```

To opt in, connect with `?format=msgpack` query parameter:

```
ws://host:port/wse?format=msgpack
```

When enabled, all outbound messages from the server use msgpack instead of JSON. The msgpack payload contains the same fields (`t`, `p`, `id`, `ts`, `seq`, `v`) but encoded with MessagePack. No category prefix is used — the `M:` header replaces it.

Requires `@msgpack/msgpack` (JS) or `msgpack` (Python) on the client side.

## Connection Lifecycle

### 1. Connect

```
Client → Rust Server (port 5006): WebSocket UPGRADE /wse
  Cookie: access_token=<JWT>
Server → Client: 101 Switching Protocols
```

Authentication is via HTTP-only `access_token` cookie (same as REST API). No query parameter needed.

### 2. Authentication

Two paths depending on server configuration:

**Rust JWT (when `jwt_secret` is configured):**
1. Rust extracts `access_token` from cookie header during handshake
2. Rust validates JWT signature, expiry, issuer, audience
3. On success: Rust sends `server_ready` immediately (zero GIL)
4. Rust pushes `AuthConnect{conn_id, user_id}` to drain queue
5. Python receives pre-validated user_id, sets up subscriptions and snapshots

**Python fallback (when `jwt_secret` is not configured):**
1. Rust pushes raw `Connect{conn_id, cookies}` to drain queue
2. Python extracts and validates JWT from cookies
3. Python sends `server_ready` after validation

The Rust JWT path eliminates GIL acquisition from the connection critical path, reducing median connection latency from ~23ms to 0.47ms.

### 3. Server Ready

Sent after authentication succeeds (from Rust when JWT is configured, from Python otherwise):

```json
WSE{"v":1,"t":"server_ready","p":{
  "message": "Connection established (Rust transport)",
  "details": {
    "version": 1,
    "features": {
      "compression": true,
      "encryption": true,
      "batching": true,
      "priority_queue": true,
      "circuit_breaker": true,
      "rust_transport": true,
      "rust_jwt": true
    },
    "connection_id": "rs_f186cd9d_019c53c4",
    "server_time": "2026-02-20T15:30:00.000Z",
    "user_id": "019c53c4-abcd-7def-8901-234567890abc",
    "encryption_public_key": "<base64-encoded 65-byte ECDH P-256 public key>"
  }
}}
```

When `encryption` is `true`, the `encryption_public_key` field contains the server's ECDH P-256 public key (65 bytes, uncompressed SEC1, base64-encoded). The client uses this to derive the shared AES-256 key.

When `rust_jwt` is `true`, JWT validation was performed by the Rust server during the handshake (before this message was sent). The `user_id` field is the validated subject from the JWT.

### 4. Auto-Subscribe

Server automatically subscribes the connection to default topics and sends subscription confirmation:

```json
U{"t":"subscription_update","p":{
  "action": "subscribe",
  "success": true,
  "topics": ["notifications", "chat_messages", ...],
  "success_topics": [...],
  "active_subscriptions": [...]
}}
```

### 5. Snapshots

After subscription, server sends full state snapshots:

```json
S{"t":"user_snapshot","p":{
  "users": [...],
  "count": 2,
  "timestamp": "2026-02-20T15:30:00.123Z"
}}
```

### 6. Incremental Updates

Real-time updates follow snapshots:

```json
U{"t":"user_update","p":{
  "user_id": "...",
  "status": "online",
  "last_seen": "2026-02-20T15:30:00.123Z"
}}
```

### 7. Heartbeat

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

### 8. Disconnect

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
    "topics": ["notifications", "chat_messages"]
  }
}
```

### Sync Request

Request initial data snapshots:

```json
{
  "t": "sync_request",
  "p": {
    "topics": ["notifications", "chat_messages"],
    "include_snapshots": true
  }
}
```

### Client Hello

Protocol negotiation. When encryption is enabled, includes the client's ECDH public key:

```json
{
  "t": "client_hello",
  "p": {
    "client_version": "2.0.0",
    "protocol_version": 2,
    "features": {
      "compression": true,
      "batch_messages": true,
      "encryption": true
    },
    "capabilities": ["compression", "encryption", "batching"],
    "encryption_public_key": "<base64-encoded 65-byte ECDH P-256 public key>"
  }
}
```

Once the server receives `client_hello` with `encryption_public_key`, it derives the shared AES-256 key. All subsequent messages from both sides may be sent as `E:`-prefixed encrypted binary frames.

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
    {"t":"notification","p":{...}},
    {"t":"chat_message","p":{...}}
  ],
  "count": 2
}}
```

## Message Signing

Messages matching a configurable set of types are signed for integrity verification. The set is empty by default — configure via `signed_message_types` in the connection constructor. When `message_signing_enabled` is `True`, all messages are signed.

```json
U{"t":"my_critical_event","p":{...},"sig":"<hash>:<timestamp>:<nonce>:<hmac>","v":1}
```

The `sig` field contains an HMAC-SHA256 signature (or JWT when a TokenProvider is configured):
- `hash`: SHA-256 hash of the JSON-serialized payload
- `signed_at`: Unix timestamp
- `nonce`: Random 16-byte hex string
- `signature`: HMAC-SHA256 of `hash:signed_at:nonce`

Example signed types (domain-specific, not built into WSE):

```
payment_completed, account_transfer, config_change, ...
```

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
| `SERVER_ERROR` | Internal server error |
| `INIT_ERROR` | Connection initialization failed |

## WebSocket Close Codes

| Code | Meaning |
|------|---------|
| 1000 | Normal closure / idle timeout |
| 1011 | Server error / circuit breaker |
| 4401 | Authentication required |

## Topics

Topics are application-defined. WSE does not enforce any topic names — define topics that match your domain. Examples:

| Topic | Description |
|-------|-------------|
| `notifications` | User notifications |
| `chat_messages` | Chat room messages |
| `system_events` | System-wide announcements |
| `user_presence` | Online/offline status updates |

Configure default topics via `auto_subscribe_topics` in your WSE router setup.
