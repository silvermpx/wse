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
- `e` — the epoch of the message's stream, as an 8-hex string. Each server
  that publishes to a topic stamps its own stream with its own epoch, so in a
  cluster one topic interleaves several streams. What a server publishes to
  its own subscribers only (`publish_local`, Python `broadcast_local`) is a
  second stream of the topic, with an epoch of its own. A server begins a new
  epoch when it restarts, or when the topic's history was dropped and begins
  again.
- `o` — the message's offset within its stream, monotonically increasing. The
  same `(tp, e, o)` is stamped on the live message and on any recovery replay
  of it, giving each message a stable identity across both.

Client dedup / gap logic, per stream `(tp, e)`: a stream not seen before →
accept and start it; `o <= last` → duplicate, drop; `o == last + 1` → in
order; `o > last + 1` → gap, recover from the last in-order position. Each
stream is in order on its own; a message of one stream is no gap in another.
Control/system messages (`server_ready`, `ping`, …) are **not** stamped. The
cluster inter-node MSG frame carries the same `(epoch, offset)` in a binary
trailer (see [Cluster Protocol](CLUSTER_PROTOCOL.md)).

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
3. Python calls `accept_connection(conn_id, user_id)`, which sends `server_ready`.
   Until then the connection gets no broadcasts, and with `client_subscriptions`
   the server refuses every topic it asks for.

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
      "recovery": true,
      "acks": true
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
- The client asks itself: every client (TypeScript, Python, and Rust with its packages) sends `subscription_update` requests for the topics its application subscribes to, and again for all of them after every reconnect, with their recovery positions (see [Subscribe / Unsubscribe](#subscribe--unsubscribe)). A server created with `client_subscriptions=True` answers those requests itself, checking each topic against the connection's topic ACL; otherwise they reach the application as ordinary messages.

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

To resume after a gap or a reconnect, a subscribe carries `recover: true` and, under `recovery`, the last position the client has for each topic that has one: the `e` and `o` of the last message it delivered in order, and under `streams` the last offset it holds of each stream of the topic. Topics without a position are only listed in `topics`. Subscribing to a topic already subscribed is harmless and still recovers.

```json
{"c":"WSE","t":"subscription_update","p":{
  "action": "subscribe",
  "topics": ["chat_messages", "prices"],
  "recover": true,
  "recovery": {"chat_messages": {
    "epoch": "0000abcd", "offset": 456,
    "streams": {"0000abcd": 456, "00007e57": 88}
  }}
},"v":1}
```

With `streams`, each stream is replayed from the client's own offset in it, whatever server it was on before: the order in which servers had the streams does not matter. The reply says `recovered: false`, and nothing is replayed, when the server cannot show the client missed nothing: a named stream it does not hold, or holds with a hole after the client's offset, or holds only since it took the topic up again; a stream the client does not name that has a message from before the client's last one, or that the server does not hold from its first message; one of the topic's histories dropped, or a message of it not kept for the memory budget, from after the client's last one; more than `recovery_max_messages` in all. A server before 3.1 ignores `streams`. Without them (clients before 3.1), every stream but the last message's is replayed from that message's place in the server's own order, which another server may have had differently; a history dropped or a message not kept after the client's last one fails it the same way.

### Recovery Reply

A subscribe gets one reply; an unsubscribe gets none. The missed messages of each topic, offsets after the given one, come first, byte-identical to live ones (with their `tp`/`e`/`o`), then the reply:

```json
{"c":"WSE","t":"subscription_update","id":"...","p":{
  "success": true,
  "success_topics": ["chat_messages", "prices"],
  "failed_topics": [],
  "recovery": {
    "chat_messages": {"epoch": "0000abcd", "offset": 512, "recovered": true, "count": 56,
                      "streams": {"0000abcd": 512, "00007e57": 93}},
    "prices": {"epoch": "00001f2e", "offset": 90, "recovered": false, "count": 0,
               "streams": {"00001f2e": 90}}
  }
},"v":1}
```

| Field | Meaning |
|-------|---------|
| `success` | `true` unless every topic failed |
| `success_topics` | Topics subscribed |
| `failed_topics` | Topics refused: denied by the connection's topic ACL, invalid names, or over its subscription limits |
| `recovery[topic].epoch` | The topic's epoch, 8 hex digits; `null` when the topic has no history yet |
| `recovery[topic].offset` | Inclusive: the last offset the client now has. When `recovered` is `false`, where the client starts: everything after it arrives live, and a message at or before it published once the subscription began was sent before this reply, so a client may take it as its position without skipping anything |
| `recovery[topic].recovered` | `true` when every missed message was replayed. `false` when there was no position, the epoch changed (a server restart), the history was evicted or too long (`recovery_max_messages`), a message came after the position that no node kept (its node had no room left for history, a link too full to take it, or a node told of its loss after a lost link), every topic of a node that went without SHUTDOWN once another comes up at its address, or the connection's outbound budget cut the replay short (then `offset` is the last one replayed) |
| `recovery[topic].count` | Messages replayed |
| `recovery[topic].streams` | Each stream of the topic the server holds, epoch to the offset the client starts from in it: everything after it arrives live, and what was published at or before it once the subscription began was sent before this reply. A client takes, per stream, the larger of its own offset and this one, and forgets a stream it named that is missing here (this server cannot replay it, so naming it again fails every later recovery). From 3.1 |

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
| `AUTH_FAILED` | The token was refused at the upgrade, or the session's proof of the token's key (`p.reason`: `proof_missing`, `proof_invalid`, see [Binding the session](#binding-the-session-to-the-clients-key)); the close (4401) follows |
| `AUTH_REQUIRED` | No token, on a server with `jwt_secret`; the close (4401) follows |
| `RATE_LIMITED` | A message was dropped over the client's rate limit (at most once a second) |
| `KEY_EXCHANGE_FAILED` | The first design's key exchange failed |
| `E2E_UNSUPPORTED` | No E2E suite in common, or the first design refused; the close (4800) follows |
| `SIGNATURE_REFUSED` | A signed message failed a check; `p.reason` says which and `p.n` names the message (see [Signed Messages](#signed-messages)) |

Applications send their own codes in the same shape.

## WebSocket Close Codes

| Code | Sent when | The clients |
|------|-----------|-------------|
| 1000 | The application called `disconnect()` | reconnect |
| 1001 | The server stops | reconnect |
| 1011 | The server application's handler failed (a Rust `Handler` call panicked) | reconnect |
| 1013 | The client reads too slowly: its outbound queue stayed over `max_outbound_queue_bytes` for two ping intervals. Also: the server filled up during the upgrade | reconnect |
| 4300 | Graceful drain (the code passed to `drain()`, 4300 by default) | reconnect |
| 4401 | Authentication failed: at the upgrade, or the session's proof of the token's key | stop (a client with a token provider retries with a new token) |
| 4403 | The token the connection opened with expired | stop (a client with a token provider retries with a new token) |
| 4408 | Nothing received from the client for `idle_timeout` seconds; or a connection that announced an E2E session did not open it, and prove its key, within 10 seconds | reconnect |
| 4409 | The client's outbox went on on another node, or on another connection of this one (a stale connection) | reconnect |
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

Sent by the client right after the connection opens (without E2E the TypeScript client waits for `server_ready`; with an E2E session both send it at once, see [End-to-End Encryption](#end-to-end-encryption)):

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
| `outbox` | `object` | The client's outbox (3.2): `producer`, `node`, `written`, `from`, see [The client's outbox](#the-clients-outbox) |

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
    "encryption": true,
    "signing": false,
    "acks": true
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
| `features` | `object` | `compression`, `recovery`, `cluster`, `msgpack`, `encryption` and `signing` (it has a signing key) describe this server; `acks` (3.2, also in `server_ready`) says it answers the client's outbox; `batching` and `priority_queue` are always `true`, kept for older clients. With recovery, `history_ttl` is how many seconds a stream that receives nothing stays recoverable here (from 3.1; also in `server_ready`): a client forgets a stream it heard nothing of for longer while the connection's frames showed it alive, instead of naming it |
| `limits` | `object` | Server-enforced limits for this connection |
| `ping_interval` | `int` | Server ping interval in milliseconds |
| `connection_id` | `string` | Unique connection identifier |
| `server_time` | `string` | Server wall clock (ISO 8601) |
| `e2e` | `object` | The answer to the client's E2E offer |
| `encryption_public_key` | `string` | Instead of `e2e`: the first design's ECDH public key |

The negotiated protocol version is the minimum of both sides.

## The client's outbox

From 3.2 a client keeps each message it sends, except one sent live or signed (a signed message has a receipt of its own), until the server answers for it, and sends what was not answered again after a reconnect. The server confirms what its application took, drops a repeat of a message it took already and says what it refused; what it cannot tell reaches the application marked as a possible repeat (see below).

**Numbers.** Each outbox is a producer with a random id, and each of its messages carries its number, `s`, from 1 up, first in the object: `{"s":42,"t":"order","p":{...},"id":"..."}`. A client sends its messages in the order of their numbers; a number may be skipped (a message that expired before it was sent). The number comes first so that the server can read it from the first bytes of a frame it does not parse (one its rate limit drops). `s` is an outbox number only on a connection whose `client_hello` named an outbox (below), once the server answered with `outbox`; before that, or without such a hello, it is the application's own field, handed on as it is. So a client numbers its messages only after the `outbox` answer, and to a server without `features.acks` it sends them unnumbered and takes a message as delivered once it is written.

**Hello.** The client names its outbox in `client_hello`:

```json
{"c":"WSE","t":"client_hello","p":{ "...": "...",
  "outbox": {"producer": "q3Jf...", "node": "019c53c4-...", "written": 41, "from": 38}
},"v":1}
```

| Field | Description |
|-------|-------------|
| `producer` | The outbox's id, up to 64 characters |
| `node` | The node the client was connected to last, as that node named itself (`null` the first time) |
| `written` | The highest number the client may have written on an earlier connection without an answer: a client writes a message only once its store holds that it may have gone |
| `from` | The first number the client still waits to hear of: it heard of every refusal before it, and the server forgets those |

Once the connection is admitted (`server_ready`), the server answers what it decided of that producer and names itself. Every message up to `acked` was taken or refused; `refused` lists the refusals among them the client may not have heard of (its connection went before their `nack`), in the form of a `nack`'s payload; `unsure`, when the server cannot tell past `acked` whether it took a message (see below), is the highest such number. Past `acked` and `unsure` the server took none of the client's messages: one the client may have written is as one not written yet, and goes as `expired` once its time ran out. The client tells its application of each, and sends the rest, in order, before anything new:

```json
{"c":"WSE","t":"outbox","p":{"acked": 39, "node": "019c53d0-...",
  "refused": [{"s": 38, "code": "busy"}]},"v":1}
```

`refused` and `unsure` are left out when there is none. A producer is kept per user: another user's connection cannot reach it. The server decides one producer's messages one at a time, its application's handler included, so a connection that comes back while a message is being decided is answered once the decision is in. The connection whose hello came last holds the producer: what an older one still brings (bytes held up on the way) is not taken, and that connection is closed with 4409, so what the newer one was told holds.

**Answers.** The server answers after its application took a message (a Rust `Handler::message` returned, or the Python server queued it for `drain_inbound` or its callback; a Python server with neither refuses it, `busy`), one `ack` for a batch of frames read, refusals first:

```json
{"c":"WSE","t":"ack","p":{"s":42},"v":1}
{"c":"WSE","t":"nack","p":{"from":43,"s":44,"code":"rate_limited"},"v":1}
```

`ack` confirms every message up to and with `s`; `nack` refuses `s` (or `from` to `s`) for good, and the client tells its application. The codes: `busy` (the application had no room for it), `rate_limited` (dropped by the client's rate limit: refused at once, its number read from the frame's first bytes), `missing` (a number skipped on the connection), `not_ready` (before the application took the connection). A repeat of a message taken already is confirmed again and not handed on; a repeat of one refused is refused again. The answers are never dropped for the connection's outbound budget: a client waits for them, and is owed no more of them than its outbox holds.

**What the server cannot tell.** A client that comes to another node names its last node: when the hello's `written` is above 0 and the new node has a link to that node that takes the question (`CAP_PRODUCERS`), it asks it once what it decided, what it refused and what it could not tell (the cluster's `PRODUCER_QUERY`), and that node stops taking messages of the producer. The server cannot tell whether it took messages up to `written` if the last node is gone, does not answer within 2 seconds, cannot be asked over such a link, or has forgotten the producer. Each node keeps decisions for up to a day after the producer's last connection closes. It holds at most 262,144 idle producers across all users, or twice its connection limit if that is more, and forgets the longest-disconnected first when it needs room.

After the node forgets a producer, a message above `written` goes without a repeat mark. The node decides a retried message anew if it forgot an earlier refusal. A particular message's outcome is also unknown when its handler panicked or its refusal was let go because the producer held 1024 runs of refusals its client had not heard of. `acked` stops short of the first such message the client still waits for; sent again, it reaches the application with `"possible_repeat": true`, and the application checks the message's `id` against what it applied. These are the only cases in which an application can see a message twice.

**A planned stop.** On `drain`, stop and `disconnect`, each connection stops taking messages, answers what it took and only then closes.

## End-to-End Encryption

One suite, `p256-aes256gcm`: ECDH P-256, HKDF-SHA256 and AES-256-GCM, with the server's identity signing in ECDSA P-256/SHA-256 (`ES256`). [SECURITY.md](SECURITY.md#end-to-end-encryption) explains setup and guarantees; [`e2e.json`](https://github.com/silvermpx/wse/blob/main/crates/wse/tests/vectors/e2e.json) holds test vectors for everything below.

### Handshake

The session comes first. The client announces it in the upgrade request (`e2e=1` in the query) and sends `client_hello` as soon as the socket opens, without waiting for `server_ready`. Until the session exists the server holds everything back: the connection joins no broadcast, the application does not learn of it (no `auth_connect` or `connect` event) and it gets no `server_ready`; it answers heartbeats only and ignores whatever else the client sends. Once it has answered with `server_hello`, it sends `server_ready` sealed, and only then does the application learn of the connection, so nothing the application sends ever goes in plaintext. A session offer without the announcement, or an announced connection whose `client_hello` carries no offer, gets `E2E_UNSUPPORTED` and close 4800.

Before the session the client takes unsealed only `server_hello`, an `error` and heartbeats; any other unsealed frame, then or inside the session, ends the connection on the client side.

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

`server_hello` is the last plaintext frame the server sends: its sending keys are installed right behind it, and every data frame queued after them is sealed. Once it has answered `client_hello` the server takes only sealed frames (WebSocket control frames apart), and a client sends nothing but `client_hello` until it holds the keys. The server seals with the counter nonce above through the library's TLS 1.2 record construction, which refuses a nonce that does not grow: in a `fips` build that is the approved way to use AES-GCM.

### Binding the session to the client's key

A token is a bearer token: whatever terminates TLS sees it in the upgrade, and could open a session of its own with it. A token that names the client's key in its `cnf` claim (RFC 7800: `{"jwk": {...}}`, or `{"jkt": "<RFC 7638 thumbprint>"}`) binds the connection to that key, as TLS 1.3's client CertificateVerify binds a certificate to its handshake:

1. The connection must open a session. A bound token without `e2e=1`, or a `cnf` that names no P-256 key, is refused at the upgrade (`AUTH_FAILED`, close 4401).
2. The client's first sealed message is its proof:

   ```json
   {"c":"WSE","t":"client_auth","p":{"key":"<base64 uncompressed point>","sig":"<base64 r || s>"}}
   ```

   `sig` is ECDSA P-256/SHA-256 over `"WSE-E2E client_auth\0" | TH | key`: the label, the session's transcript hash (32 bytes) and the key (65 bytes).
3. Until the proof holds the server admits nothing: no `server_ready`, no broadcasts, no pings, and the application does not learn of the connection. A first sealed message that is no proof, or a proof for another key or that does not verify, gets a sealed `error` `AUTH_FAILED` with `p.reason` (`proof_missing`, `proof_invalid`) and close 4401. A connection that has not opened its session and proved its key 10 seconds after the upgrade is closed with 4408; its heartbeats do not extend that.
4. Then the sealed `server_ready` names the key: `p.details.cnf = {"jkt": "<thumbprint>"}`. A client bound to its key goes on only when that thumbprint is its own; otherwise its token was replaced on the way, by one naming no key or another, and it stops.

`TH` covers both ephemeral keys, the connection id and the server's key id: no one in between can have a proof signed for a session of its own, whether or not the client pins the server's identity.

A server application that authenticates its connections itself binds them the same way with `accept_connection(conn_id, user_id, client_key=...)` (`Publisher::accept_bound`): the proof and the accept meet in either order. A `client_auth` on a connection that nothing binds is ignored. On a server with `bind_sessions=False` a token's key only checks signed messages.

### The first design

For 2.x clients and servers. The client sends `encryption_public_key` (a 65-byte ECDH point, base64) in `client_hello`, the server answers with its own in `server_hello`. Both derive one key with HKDF-SHA256 (no salt, info `wse-encryption/aes-gcm-key`, 32 bytes) and seal every message with AES-256-GCM under a random 12-byte IV: `E:` | IV | ciphertext | tag. Refused with 4800 by a server with `e2e_legacy=False`.

## Signed Messages

A message whose sender must be provable later (an order, a fill) travels as the exact text its sender signed, next to the signature and the signing key's id. [SECURITY.md](SECURITY.md#signed-messages) explains what it proves and how keys are bound; [`sign.json`](https://github.com/silvermpx/wse/blob/main/crates/wse/tests/vectors/sign.json) holds test vectors for everything below.

### Wire format

```json
{"t": "signed", "sb": "<body>", "sig": "<signature>", "kid": "<key id>"}
```

| Field | Meaning |
|-------|---------|
| `sb` | The body: a JSON object as text, signed byte for byte. It is never re-encoded: the server's own fields (`c`, the recovery stamp `tp` / `e` / `o`) and msgpack conversion touch only the outer object, so the signature survives delivery, recovery replays and cluster relays |
| `sig` | ECDSA P-256 / SHA-256 over the label and the body, 64 bytes `r \|\| s`, base64 |
| `kid` | The signing key's id: the first 8 bytes of SHA-256 over its public key (65-byte uncompressed point), hex |

The signed bytes are a label naming the signer's side, then the body as UTF-8:

| Signer | Label |
|--------|-------|
| Client | `WSE-SIG client\0` |
| Server | `WSE-SIG server\0` |

### Client to server

The body carries `t` (the message type), `p` (its payload, optional), `aud` (the server's signing key id, from `server_ready` or `server_hello`; in an E2E session from the sealed `server_ready` only, and one of the pinned server signing keys when the client pins any), `ts` (milliseconds since the epoch) and `n` (unique per message, at most 128 characters: an order id, say).

The server takes the message only when the connection holds a key with id `kid` (from its token's `cnf` claim, or bound by the application), the signature verifies, `aud` is the id of the server's own signing key, `ts` is within `signature_window` seconds (30) of the server's clock, and no message with the same `kid` and `n` came within that window. It hands the application the body's fields with `signature` (`body`, `sig`, `kid`) added, then sends the client a signed `receipt`: a receipt means the application has the message. Otherwise the client gets an `error` with code `SIGNATURE_REFUSED`, `n` when the body reads, and a `reason`:

| `reason` | Why |
|----------|-----|
| `malformed` | Not a signed message the server can read: a field missing, the body not a JSON object, no `t`, `ts` or `n`, `n` longer than 128 characters, a key twice in one object, or an integer beyond 64 bits (readers would not agree on what such a body says) |
| `unknown_key` | The connection holds no key with this id |
| `bad_signature` | The signature does not verify |
| `wrong_audience` | Signed for another server's key |
| `stale` | `ts` outside the window |
| `replayed` | This `kid` and `n` came already |
| `not_enabled` | The server has no signing key |
| `busy` | Verified, but the application cannot take it now (its queue is full). Not received: send it again, with the same `n` |

### Server to client

The server signs with its own key (`signing_key`, apart from the E2E identity key). The body carries `t`, `p`, `ts`, `n`; for a topic message `tp`, and a client drops a message whose signed `tp` is not the topic it came on; for a message to one user `to`, that user's id (the token's `sub`, or the `user_id` the application accepted the connection for), and a client drops a message whose `to` is not the `user_id` of its `server_ready`. `server_ready` (`p.details.signing`) and `server_hello` (`p.signing`) name the key: `{"kid": ..., "public_key": ...}`. Clients check signatures against the keys they pin, not against these.

The receipt for a client's signed message is a server-signed message of type `receipt`: `p` holds the client's `kid` and `n`, `sha256` (SHA-256 of the body it got, base64) and `received` (the server's time, milliseconds).

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
