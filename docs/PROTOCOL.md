# WSE Wire Protocol Specification

Version: 1 (Rust transport)

> For the cluster inter-node binary protocol, see [Cluster Protocol](CLUSTER_PROTOCOL.md).

## Message Format

All WSE messages are JSON objects with the following structure:

```json
{
  "c": "U",
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

### Field Order

Fields are ordered for readability in DevTools: `c` first, `t` second, `v` last. On recovery-stamped topic publications the `tp`/`e`/`o` fields precede `c`.

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `c` | `string` | Message category (`"U"`, `"S"`, or `"WSE"`) |
| `t` | `string` | Event type (e.g., `user_update`, `chat_message`) |
| `id` | `string` | Unique message ID (UUID v7) |
| `ts` | `string` | ISO 8601 timestamp with timezone |
| `seq` | `int` | Process-global monotonic counter (diagnostic only; NOT a per-topic order — use `tp`/`e`/`o` for ordering and dedup) |
| `p` | `object` | Event payload (type-specific data) |
| `v` | `int` | Protocol version (currently `1`) |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `pri` | `int` | Priority (1=BACKGROUND, 3=LOW, 5=NORMAL, 8=HIGH, 10=CRITICAL) |
| `cid` | `string` | Correlation ID for request/response pairing |
| `wse_processing_ms` | `int` | Server-side processing time in milliseconds |
| `tp` | `string` | Recovery stamp: topic this message was published to (see below) |
| `e` | `string` | Recovery stamp: topic buffer epoch, 8 hex chars (e.g. `"0000abcd"`) |
| `o` | `int` | Recovery stamp: per-topic monotonic offset (u64) |

### Recovery stamp (`tp` / `e` / `o`)

When recovery is enabled, every **topic publication** carries three extra
top-level fields so clients can dedupe idempotently, detect gaps, and resume
from their last-seen position:

```json
{"tp":"chat_messages","e":"0000abcd","o":42,"c":"U","t":"chat_message","p":{…},"v":1}
```

- `tp` — the topic. A single connection receives many topics, so the topic is
  needed to attribute each `(epoch, offset)` to the right per-topic sequence.
- `e` — the topic buffer's epoch as an 8-hex string. It changes when the server
  restarts (the buffer is recreated), invalidating old offsets.
- `o` — the message's offset within that topic's buffer, monotonically
  increasing. The same `(tp, e, o)` is stamped on the live message and on any
  recovery replay of it, giving each message a stable identity across both.

Client dedup / gap logic (per topic): a different `e` than last seen → epoch
changed, accept and re-baseline; `o <= last` on the same epoch → duplicate,
drop; `o == last + 1` → in order; `o > last + 1` → gap, recover from the last
in-order position. Control/system messages (`server_ready`, `ping`, …) are **not**
stamped. The cluster inter-node MSG frame carries the same `(epoch, offset)` in a
binary trailer (see [Cluster Protocol](CLUSTER_PROTOCOL.md)).

## Message Categories

The `c` field identifies the message category:

| Value | Category | Description | Examples |
|-------|----------|-------------|----------|
| `"WSE"` | System | Protocol-level messages | `server_ready`, `ping`, `PONG`, `error` |
| `"S"` | Snapshot | Full state snapshots | `user_snapshot`, `channel_snapshot` |
| `"U"` | Update | Incremental updates | `user_update`, `chat_message` |

### Wire Format

```json
{"c":"WSE","t":"server_ready","p":{...},"v":1}
{"c":"S","t":"user_snapshot","p":{...},"v":1}
{"c":"U","t":"user_update","p":{...},"v":1}
```

No wire prefix. Category is inside the JSON object as the `c` field.

## Transport Modes

### Text Mode (default)

Standard WebSocket text frames with JSON.

### Binary Mode: Compression

When a message exceeds the compression threshold (default: 1024 bytes), the server sends a binary frame with `C:` prefix:

```
C:<zlib-compressed-data>
```

The client detects the `C:` prefix, strips it, decompresses with zlib, and parses the resulting JSON string.

### Binary Mode: Encryption

On an E2E connection every frame after the handshake is a binary frame that starts with `E:`. See [End-to-End Encryption](#end-to-end-encryption).

### Binary Mode: JSON (default since v1.1)

All JSON messages are sent as WebSocket binary frames instead of text frames. This eliminates UTF-8 validation overhead in the WebSocket layer, providing 5-15% higher throughput. The payload format is identical JSON with `c` field, but delivered in a binary frame.

Clients should handle both text and binary frames containing JSON for backward compatibility.

### Binary Mode: MessagePack

A client opts in with the `?format=msgpack` query parameter:

```
ws://host:port/wse?format=msgpack
```

The server then sends that connection msgpack wherever it would send JSON text: topic messages, broadcasts, recovery replays, replies, `send()` text and `send_event()`. Each is a binary frame with the `M:` prefix:

```
M:<msgpack-encoded message>
```

The message is the same map as in JSON (`c`, `t`, `p`, `id`, `ts`, `seq`, `v`, `tp`/`e`/`o`), encoded once per message and shared by every msgpack connection that gets it. A float takes 32 bits when that holds it exactly, else 64. Bytes the application sends as bytes (`send_bytes()`, `broadcast_all_bytes()`) go as they are, and the server's own pings and errors stay JSON.

Such a client may send msgpack too: binary frames holding one msgpack value, with or without the `M:` prefix. The server reads its protocol messages (`ping`, `client_config`, `subscription_update`) in msgpack like their JSON form, and hands the rest to the application as the same dict a JSON message gives. Values nested deeper than 64 levels are refused before decoding, so a crafted payload can't overflow the decoder's stack. Inside E2E frames the clients send JSON.

On typical messages msgpack takes 24-41% fewer bytes than JSON. Clients need `@msgpack/msgpack` (TypeScript, a dependency) or `msgpack` (Python, the `wse-client[msgpack]` extra).

## Connection Lifecycle

### 1. Connect

```
Client → Rust Server (port 5006): WebSocket UPGRADE /wse
  Cookie: access_token=<JWT>
Server → Client: 101 Switching Protocols
```

Authentication is via HTTP-only cookie (default name: `access_token`, configurable via `jwt_cookie_name`). No query parameter needed.

### 2. Authentication

Two paths depending on server configuration:

**Rust JWT (when `jwt_secret` is configured):**
1. Rust extracts JWT from cookie header during handshake (cookie name configured via `jwt_cookie_name`, default `access_token`)
2. Rust validates JWT signature, expiry, issuer, audience
3. On success: Rust sends `server_ready` immediately (zero GIL)
4. Rust pushes `AuthConnect{conn_id, user_id}` to drain queue
5. Python receives pre-validated user_id, sets up subscriptions and snapshots

**Python fallback (when `jwt_secret` is not configured):**
1. Rust pushes raw `Connect{conn_id, cookies}` to drain queue
2. Python extracts and validates JWT from cookies
3. Python calls `accept_connection(conn_id, user_id)`, which sends `server_ready`

The Rust JWT path eliminates GIL acquisition from the connection critical path, reducing median connection latency from ~23ms to 0.53ms.

### 3. Server Ready

Sent after authentication succeeds (from Rust when JWT is configured, from Python otherwise):

```json
{"c":"WSE","t":"server_ready","p":{
  "message": "Connection established (Rust transport)",
  "details": {
    "version": 1,
    "features": {
      "compression": true,
      "encryption": false,
      "batching": true,
      "priority_queue": true,
      "circuit_breaker": true,
      "rust_transport": true,
      "rust_jwt": true,
      "recovery": true
    },
    "connection_id": "rs_f186cd9d_019c53c4",
    "server_time": "2026-02-28T12:00:00.000Z",
    "user_id": "019c53c4-abcd-7def-8901-234567890abc"
  }
},"v":1}
```

When `rust_jwt` is `true`, JWT validation was performed by the Rust server during the handshake (before this message was sent). The `user_id` field is the validated subject from the JWT.

Encryption key exchange happens separately, in `client_hello` / `server_hello` (see [Protocol Negotiation](#protocol-negotiation)), not in `server_ready`.

### 4. Subscriptions

Two ways, often both:

- The application subscribes a connection with `subscribe_connection(conn_id, topics)` (for example from its `auth_connect` handler).
- The client asks itself: both SDKs send `subscription_update` requests for the topics their application subscribes to, and again for all of them after every reconnect, with their recovery positions (see [Subscribe / Unsubscribe](#subscribe--unsubscribe)). A server created with `client_subscriptions=True` answers those requests itself, checking each topic against the connection's topic ACL; otherwise they reach the application as ordinary messages.

### 5. Updates

Real-time updates are sent via topic broadcast:

```json
{"c":"U","t":"user_update","p":{
  "user_id": "...",
  "status": "online",
  "last_seen": "2026-02-20T15:30:00.123Z"
},"v":1}
```

### 6. Heartbeat

Server sends a ping every 25 seconds:

```json
{"c":"WSE","t":"ping","p":{"server_time":"2026-02-28T12:00:00.000Z"}}
```

Client responds with a PONG echoing the server timestamp plus its own:

```json
{"c":"WSE","t":"PONG","p":{"client_timestamp":1708441800050,"server_timestamp":1708441800000}}
```

### 7. Disconnect

The server closes a connection with a close frame and one of the codes under
[WebSocket Close Codes](#websocket-close-codes). From then on it delivers
nothing more the client sends, and it waits 5 seconds for the client's close
frame; a client that has not answered by then is disconnected.

A client that closes first gets its own close code back.

A server that is full (`max_connections`) or draining answers the upgrade
request with HTTP `503 Service Unavailable` and `Retry-After: 5`.

## Presence Events

When presence tracking is enabled, the server broadcasts presence events to all subscribers of a topic.

### Presence Join (server to client)

Sent when the first connection for a user subscribes to a topic with presence data:

```json
{"c":"WSE","t":"presence_join","p":{"user_id":"alice","data":{"status":"online","name":"Alice"}},"v":1}
```

### Presence Leave (server to client)

Sent when the last connection for a user is removed from a topic:

```json
{"c":"WSE","t":"presence_leave","p":{"user_id":"alice","data":{"status":"online","name":"Alice"}},"v":1}
```

### Presence Update (server to client)

Sent when a user's presence data is updated via `update_presence()`:

```json
{"c":"WSE","t":"presence_update","p":{"user_id":"alice","data":{"status":"away"}},"v":1}
```

Presence is queried through the Python API (`server.presence(topic)`, `server.presence_stats(topic)`), not through WebSocket messages.

---

## Client-to-Server Messages

### Subscribe / Unsubscribe

One request covers many topics:

```json
{"c":"WSE","t":"subscription_update","p":{
  "action": "subscribe",
  "topics": ["notifications", "chat_messages"]
},"id":"...","v":1}
```

```json
{"c":"WSE","t":"subscription_update","p":{
  "action": "unsubscribe",
  "topics": ["chat_messages"]
},"id":"...","v":1}
```

To resume after a gap or a reconnect, a subscribe carries `recover: true` and, under `recovery`, the last position the client has for each topic that has one: the `e` and `o` of the last message it delivered in order. Topics without a position are only listed in `topics`. Subscribing to a topic already subscribed is harmless and still recovers.

```json
{"c":"WSE","t":"subscription_update","p":{
  "action": "subscribe",
  "topics": ["chat_messages", "prices"],
  "recover": true,
  "recovery": {"chat_messages": {"epoch": "0000abcd", "offset": 456}}
},"v":1}
```

### Recovery Reply

A subscribe gets one reply; an unsubscribe gets none. The missed messages of each topic, offsets after the given one, come first, byte-identical to live ones (with their `tp`/`e`/`o`), then the reply:

```json
{"c":"WSE","t":"subscription_update","id":"...","p":{
  "success": true,
  "success_topics": ["chat_messages", "prices"],
  "failed_topics": [],
  "recovery": {
    "chat_messages": {"epoch": "0000abcd", "offset": 512, "recovered": true, "count": 56},
    "prices": {"epoch": "00001f2e", "offset": 90, "recovered": false, "count": 0}
  }
},"v":1}
```

| Field | Meaning |
|-------|---------|
| `success` | `true` unless every topic failed |
| `success_topics` | Topics subscribed |
| `failed_topics` | Topics refused: denied by the connection's topic ACL, invalid names, or over its subscription limits |
| `recovery[topic].epoch` | The topic's epoch, 8 hex digits; `null` when the topic has no history yet |
| `recovery[topic].offset` | Inclusive: the last offset the client now has. When `recovered` is `false`, the last offset published before the subscription began; everything after it arrives live, so a client may take it as its position without skipping anything |
| `recovery[topic].recovered` | `true` when every missed message was replayed. `false` when there was no position, the epoch changed (a server restart), the history was evicted or too long (`recovery_max_messages`), or the connection's outbound budget cut the replay short (then `offset` is the last one replayed) |
| `recovery[topic].count` | Messages replayed |

An omitted `recovered` means `true`. With `client_subscriptions=True` the server answers these requests itself; otherwise the application answers them, with `subscribe_with_recovery(conn_id, topics, recover=True, positions=p["recovery"])` and a reply built from its result.

### Sync Request

Request initial data snapshots:

```json
{"c":"S","t":"sync_request","p":{
  "topics": ["notifications", "chat_messages"],
  "include_snapshots": true
},"v":1}
```

### Client Hello

Protocol negotiation, and with encryption the client's half of the key exchange: see [Protocol Negotiation](#protocol-negotiation).

### Client Config

What the client asks of compression from now on (the TypeScript client sends it when its compression setting changes). The same wish can come with `client_hello` (`features.compression`, `compression_threshold`) or in the URL (`?compression=false`):

```json
{"c":"WSE","t":"client_config","p":{
  "compression": true,
  "compression_threshold": 2048
},"v":1}
```

The server then compresses `send_event()` messages to this connection above the client's threshold, or never with `"compression": false`; an application call with `compression_threshold=0` still turns compression off. The message does not reach the application.

## Batches

An application may send several messages in one frame as a `batch` message; both SDKs unpack it and handle each message in order:

```json
{"c":"WSE","t":"batch","p":{
  "messages": [
    {"t":"notification","p":{"title":"New message"}},
    {"t":"chat_message","p":{"text":"Hello"}}
  ],
  "count": 2
},"v":1}
```

## Error Codes

The server's own `error` messages (`{"c":"WSE","t":"error","p":{"code":...,"message":...}}`):

| Code | Sent when |
|------|-----------|
| `AUTH_FAILED` | The token was refused at the upgrade; the close (4401) follows |
| `AUTH_REQUIRED` | No token, on a server with `jwt_secret`; the close (4401) follows |
| `RATE_LIMITED` | A message was dropped over the client's rate limit (at most once a second) |
| `KEY_EXCHANGE_FAILED` | The first design's key exchange failed |
| `E2E_UNSUPPORTED` | No E2E suite in common, or the first design refused; the close (4800) follows |

Applications send their own codes in the same shape.

## WebSocket Close Codes

| Code | Sent when | The clients |
|------|-----------|-------------|
| 1000 | The application called `disconnect()` | reconnect |
| 1001 | The server stops | reconnect |
| 1013 | The client reads too slowly: its outbound queue stayed over `max_outbound_queue_bytes` for two ping intervals. Also: the server filled up during the upgrade | reconnect |
| 4300 | Graceful drain (the code passed to `drain()`, 4300 by default) | reconnect |
| 4401 | Authentication failed at the upgrade | stop (a client with a token provider retries with a new token) |
| 4403 | The token the connection opened with expired | stop (a client with a token provider retries with a new token) |
| 4408 | Nothing received from the client for `idle_timeout` seconds | reconnect |
| 4429 | The client kept sending past the rate limit for 10 seconds | reconnect |
| 4800 | No E2E suite in common, or a first-design client on a server with `e2e_legacy=False` (after an `E2E_UNSUPPORTED` error) | stop, with an encryption error |
| 4802 | An E2E frame from the client out of order, forged, or plaintext inside a session | reconnect (a new session) |
| 4803 | The E2E frame counter is used up | reconnect (a new session) |

## Topics

Topics are application-defined. WSE does not enforce any topic names - define topics that match your domain. Examples:

| Topic | Description |
|-------|-------------|
| `notifications` | User notifications |
| `chat_messages` | Chat room messages |
| `system_events` | System-wide announcements |
| `user_presence` | Online/offline status updates |

Subscribe connections to topics in your `drain_inbound()` event handler using `subscribe_connection(conn_id, topics)`.

## Protocol Negotiation

After the WebSocket connection is established and the server sends `server_ready`, the client sends a `client_hello` message to negotiate protocol features, version, and capabilities. The server responds with `server_hello` confirming the negotiated settings.

### Client Hello

Sent by the client right after the connection opens (the TypeScript client waits for `server_ready`):

```json
{"c":"WSE","t":"client_hello","p":{
  "client_version": "3.0.0",
  "protocol_version": 1,
  "features": {
    "compression": true,
    "encryption": true,
    "batching": true,
    "priority_queue": true,
    "circuit_breaker": true,
    "offline_queue": true,
    "rate_limiting": true,
    "health_check": true,
    "metrics": true
  },
  "connection_id": "rs_f186cd9d_019c53c4",
  "compression_threshold": 1024,
  "e2e": {
    "suites": ["p256-aes256gcm"],
    "client_public_key": "<base64>",
    "client_random": "<base64>"
  }
},"v":1}
```

| Field | Type | Description |
|-------|------|-------------|
| `client_version` | `string` | Version of the client library |
| `protocol_version` | `int` | Highest protocol version the client supports |
| `features` | `object` | What the client does; the server reads `compression` (and `encryption` to know an E2E client) |
| `compression_threshold` | `int` | From which size the client wants messages compressed |
| `e2e` | `object` | The E2E offer, see [End-to-End Encryption](#end-to-end-encryption) |
| `encryption_public_key` | `string` | Instead of `e2e`: the first design's ECDH public key |

### Server Hello

Sent by the server in response to `client_hello`:

```json
{"c":"WSE","t":"server_hello","p":{
  "server_version": "3.0.0",
  "protocol_version": 1,
  "features": {
    "compression": true,
    "recovery": true,
    "cluster": false,
    "batching": true,
    "priority_queue": true,
    "msgpack": true,
    "encryption": true
  },
  "limits": {
    "max_message_size": 1048576,
    "rate_limit_capacity": 100000,
    "rate_limit_refill": 10000
  },
  "ping_interval": 25000,
  "connection_id": "rs_f186cd9d_019c53c4",
  "server_time": "2026-02-28T12:00:00.000Z",
  "e2e": {
    "suite": "p256-aes256gcm",
    "server_public_key": "<base64>",
    "key_id": "3f6c0a1b2c3d4e5f",
    "alg": "ES256",
    "signature": "<base64>"
  }
},"v":1}
```

| Field | Type | Description |
|-------|------|-------------|
| `server_version` | `string` | WSE server version |
| `protocol_version` | `int` | Negotiated protocol version (minimum of client and server) |
| `features` | `object` | `compression`, `recovery`, `cluster`, `msgpack` and `encryption` describe this server; `batching` and `priority_queue` are always `true`, kept for older clients |
| `limits` | `object` | Server-enforced limits for this connection |
| `ping_interval` | `int` | Server ping interval in milliseconds |
| `connection_id` | `string` | Unique connection identifier |
| `server_time` | `string` | Server wall clock (ISO 8601) |
| `e2e` | `object` | The answer to the client's E2E offer |
| `encryption_public_key` | `string` | Instead of `e2e`: the first design's ECDH public key |

The negotiated protocol version is the minimum of both sides.

## End-to-End Encryption

One suite, `p256-aes256gcm`: ECDH P-256, HKDF-SHA256 and AES-256-GCM, with the server's identity signing in ECDSA P-256/SHA-256 (`ES256`). [SECURITY.md](SECURITY.md#end-to-end-encryption) explains setup and guarantees; `tests/vectors/e2e.json` holds test vectors for everything below.

### Handshake

The client offers, in `client_hello.p.e2e`, the suites it speaks, a fresh ECDH public key (65-byte uncompressed point, base64) and 32 random bytes (base64). The server answers in `server_hello.p.e2e` with the suite it chose, its own fresh ECDH public key, `key_id` (the first 8 bytes of SHA-256 over its identity public key, hex) and the signature over the transcript `T` (64 bytes `r || s`, base64). A server without an identity key sends `alg` and `signature` as `null` and `key_id` as `0000000000000000`; only a client that allows unauthenticated servers goes on.

The transcript, each length one byte:

```
"WSE-E2E server_hello\0" | n | n × (len | suite) | len | chosen suite
  | client_random (32) | client_public_key (65) | server_public_key (65)
  | len | connection_id | key_id (8)
```

`connection_id` is the one in `server_hello`. A client pinning keys takes the one whose id is `key_id` and checks the signature; no match, or a wrong signature, ends the connection on the client side.

### Keys

`TH` = SHA-256(T); `H(ikm, label, n)` = HKDF-SHA256 with salt `TH` and info `label`, n bytes; `Z` = the ECDH shared secret.

| | |
|-|-|
| Client-to-server secret, epoch 0 | `H(Z, "wse/e2e c2s", 32)` |
| Server-to-client secret, epoch 0 | `H(Z, "wse/e2e s2c", 32)` |
| An epoch's key, from its secret `S` | `H(S, "wse/e2e key", 32)` |
| An epoch's IV | `H(S, "wse/e2e iv", 4)` |
| The next epoch's secret | `H(S, "wse/e2e next", 32)` |

### Frames

Binary frames:

```
"E:" | format 0x01 | type (0 data, 1 close) | epoch (u32 BE) | seq (u64 BE) | ciphertext | tag (16)
```

The nonce is the epoch's IV followed by `seq` (u64 BE); the additional data is the direction (`0x01` client to server, `0x02` server to client), `TH` and the 14-byte header. Each direction counts `seq` from 0, one per frame, across epochs. A receiver opens only the frame with the next `seq`, in the current epoch or the next one (whose key it derives on trial and keeps only when the tag holds), and at most 2^24 frames per epoch; anything else closes the connection with 4802. A sender moves to the next epoch after 2^20 frames or 600 seconds, and past `seq` 2^53 - 1 closes with 4803. The plaintext is the message as it would go unsealed: JSON, or `M:` msgpack on a msgpack connection.

`server_hello` is the last plaintext frame the server sends: its sending keys are installed right behind it. Once it has answered `client_hello` the server takes only sealed frames (WebSocket control frames apart), and a client sends nothing but `client_hello` until it holds the keys.

### The first design

For 2.x clients and servers. The client sends `encryption_public_key` (a 65-byte ECDH point, base64) in `client_hello`, the server answers with its own in `server_hello`. Both derive one key with HKDF-SHA256 (no salt, info `wse-encryption/aes-gcm-key`, 32 bytes) and seal every message with AES-256-GCM under a random 12-byte IV: `E:` | IV | ciphertext | tag. Refused with 4800 by a server with `e2e_legacy=False`.

## Server-Initiated Ping

The server sends periodic ping messages to detect stale connections and measure round-trip latency.

**Ping interval**: every 25 seconds.

**Idle timeout**: a connection that has sent no frame for `idle_timeout` seconds (60 by default) is closed with code 4408. Every frame counts: messages, PONG replies, and WebSocket protocol pings and pongs, so a plain WebSocket client can stay connected with protocol pings alone. The server answers protocol pings itself.

### Ping Message

```json
{"c":"WSE","t":"ping","p":{"server_time":"2026-02-28T12:00:00.000Z"}}
```

### Expected Pong Response

The client must respond with a PONG containing both the original server timestamp and the client's own timestamp:

```json
{"c":"WSE","t":"PONG","p":{"client_timestamp":1708441800050,"server_timestamp":1708441800000}}
```

A client that answers nothing within the idle timeout is closed with code 4408.

## Rate Limit Feedback

WSE enforces per-connection rate limits using a token bucket algorithm. The server provides feedback before and after limits are exceeded, giving clients the opportunity to back off gracefully.

**Default configuration**:

| Parameter | Value |
|-----------|-------|
| Bucket capacity | 100,000 tokens |
| Refill rate | 10,000 tokens/second |

### Rate Limit Warning

When 80% of the bucket is consumed (20% remaining), the server sends a warning:

```json
{"c":"WSE","t":"rate_limit_warning","p":{
  "current_rate": 80000,
  "limit": 100000,
  "remaining": 20000,
  "retry_after": 1.0
},"v":1}
```

This gives the client a window to reduce its send rate before messages are rejected.

### Rate Limit Exceeded

When the bucket is empty, the server rejects messages with an error:

```json
{"c":"WSE","t":"error","p":{
  "code": "RATE_LIMITED",
  "message": "Rate limit exceeded",
  "retry_after": 1.0
},"v":1}
```

| Field | Type | Description |
|-------|------|-------------|
| `code` | `string` | Error code (`RATE_LIMITED`) |
| `message` | `string` | Human-readable description |
| `retry_after` | `float` | Suggested backoff before retrying (seconds) |

The limit applies to the messages a client sends (text and binary frames), per connection. A dropped message is not delivered to the application; the error is sent at most once a second. The client should stop sending, wait for `retry_after` seconds, and resume at a lower rate. A client that keeps sending into the empty bucket for 10 seconds is closed with code 4429.
