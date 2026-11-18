# wse-client

Python client for [WSE](https://github.com/silvermpx/wse), the WebSocket engine for reactive user interfaces: topics with message recovery, reconnects, MessagePack, an offline queue, end-to-end encryption and signed messages.

[![PyPI](https://img.shields.io/pypi/v/wse-client)](https://pypi.org/project/wse-client/)
[![Python](https://img.shields.io/pypi/pyversions/wse-client)](https://pypi.org/project/wse-client/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/silvermpx/wse/blob/main/LICENSE)

Pure Python, no Rust dependency. It speaks the same protocol as the TypeScript and Rust clients.

## Installation

```bash
pip install wse-client
```

Optional extras:

```bash
pip install wse-client[crypto]   # E2E encryption
pip install wse-client[msgpack]  # MessagePack wire format (wire_format="msgpack")
pip install wse-client[orjson]   # Faster JSON (de)serialization
pip install wse-client[all]      # Everything
```

## Quick Start

### Async (recommended)

```python
from wse_client import connect

async with connect("ws://localhost:5007/wse", token="<jwt>") as client:
    await client.subscribe(["notifications", "trades"])

    async for event in client:
        print(event.type, event.payload)
```

### Sync

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5007/wse", token="<jwt>")
client.connect()
client.subscribe(["notifications"])

event = client.recv(timeout=5.0)
print(event.type, event.payload)

client.close()
```

### Callbacks

```python
from wse_client import SyncWSEClient

client = SyncWSEClient("ws://localhost:5007/wse", token="<jwt>", topics=["notifications"])

# By event type (the "t" of a message), whatever topic it comes on.
@client.on("notification")
def handle(event):
    print(event.payload)

@client.on_any
def catch_all(event):
    print(f"[{event.type}] {event.payload}")

client.connect()
client.run_forever()
```

Handlers also hear the connection itself: `connected` (`connection_id`, `user_id`, `reconnected`, `recovery`) each time the server takes a connection, `disconnected` (`code`, `reason`, `reconnecting`) when one is lost, and the server's `subscription_update` replies (`success_topics`, `failed_topics`, `recovery`). The event stream (`async for`, `recv()`) carries the server's messages only.

## Constructor Parameters

`AsyncWSEClient(url, **kwargs)` (and `SyncWSEClient`, which takes the same keywords):

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | WebSocket server URL |
| `token` | str | None | JWT token, sent as `access_token` cookie and `Authorization: Bearer` header |
| `token_provider` | callable | None | Sync or async callable returning the token; called before every connect and reconnect attempt (overrides `token`) |
| `topics` | list[str] | None | Topics to subscribe to after connecting |
| `reconnect` | ReconnectConfig | default config | Reconnection strategy |
| `extra_headers` | dict[str, str] | None | Additional HTTP headers for the handshake |
| `queue_size` | int | 1000 | Events buffered for the iterator / `recv()`. Once the application reads them, a full buffer stops reading the socket until it has room, so nothing is dropped (the server holds back, or closes a client that stays behind, which then recovers); before that, with handlers only, the oldest is dropped |
| `cookie_name` | str | `"access_token"` | The cookie the token goes in (the server's `jwt_cookie_name`). Cookies already in `extra_headers` are kept, and an `Authorization` header there wins over the token's |
| `encryption` | bool | False | Require E2E encryption (needs the `[crypto]` extra and one of the three below) |
| `e2e_server_keys` | list[str] | None | The server's identity public keys (`e2e_identity_public_key()`, base64) one of which must sign the handshake |
| `e2e_unauthenticated` | bool | False | Accept a server without an identity key: safe only from passive listeners |
| `e2e_legacy` | bool | False | Speak the first E2E design, for servers before 3.0 |
| `e2e_bind_session` | bool | True | With `signing_key`, prove the key inside the session and require the server to confirm it: the token names the key in its `cnf` claim. `False` for a key bound with `set_client_key()` |
| `endpoints` | list[str] | None | Additional endpoints for the built-in connection pool / failover |
| `load_balancing` | LoadBalancingStrategy | WEIGHTED_RANDOM | Endpoint selection strategy when `endpoints` is set |
| `rate_limit` | float \| None | 100 | Outbound limit for `send()` / `send_with_retry()` / `send_batch()` in messages per second; `None` disables it |
| `rate_limit_burst` | int | 10 x `rate_limit` | Messages that may go out at once before the rate applies |
| `offline` | Offline \| None | `Offline()` | The outbox: every message but a live or signed one waits until the server answers for it (see [The outbox](#the-outbox)); `None` has none, as before 3.2 |
| `offline_queue` | bool | False | Without an outbox (`offline=None`): hold messages sent while disconnected and send them once after the next reconnect |
| `max_message_size` | int \| None | 16 MiB | Largest inbound message, checked on the frame and while inflating compressed frames; `None` disables it |
| `recovery_timeout` | float | 3.0 | Seconds a topic waits for a recovery replay after an offset gap (`0` never waits) |
| `wire_format` | str | `"json"` | `"msgpack"`: the server sends this connection MessagePack and the client sends in it too, except inside E2E frames (needs the `[msgpack]` extra) |
| `signing_key` | EC key or PEM/DER bytes | None | This client's ECDSA P-256 key for `send(..., signed=True)` (needs the `[crypto]` extra) |
| `server_signing_keys` | list[str] | None | The server's signing public keys (`signing_public_key()`, base64) its signed messages are checked against |
| `cookie_name` | str | `"access_token"` | The cookie the token goes in at the upgrade, besides the `Authorization` header |
| `delivery` | DeliveryConfig \| None | None | recover-or-resync (see below); None: best effort |

The `connect(url, **kwargs)` factory returns an `AsyncWSEClient` (not connected yet) to use as an async context manager.

## API Reference

### AsyncWSEClient

| Method | Description |
|--------|-------------|
| `connect()` | Open the connection and complete the handshake (raises on failure, see [Error Handling](#error-handling)) |
| `start()` | Return at once and connect in the background: a failed first attempt is made again as any later one, the initial topics are subscribed once the server took the connection, and what is sent meanwhile waits in the outbox (for an application that may start without a network) |
| `pause(reason)` / `resume(reason)` | End the connection while any reason holds (`background`, `offline`, ...) and connect again at once when the last is cleared; topics, positions and the outbox stay. Handlers hear `paused` with the reasons; `paused` (the property) lists them |
| `disconnect()` / `close()` | Close the connection; iteration ends after the queued events |
| `send(type, payload, priority=NORMAL, correlation_id=None)` | Send a message; returns a `SendResult` |
| `send_with_retry(type, payload, priority=NORMAL, correlation_id=None, max_retries=5)` | Send with exponential backoff; one message id across all attempts |
| `send_batch(messages)` | Send multiple messages in a single frame |
| `subscribe(topics, recover=False)` | Subscribe to topics, optionally recovering missed messages; while disconnected the topics are subscribed on the next connection |
| `unsubscribe(topics)` | Unsubscribe from topics (and forget their recovery positions), connected or not |
| `request_snapshot(topics)` | Request a state snapshot; an identical request within 5 s is not resent |
| `on(event_type)` | Register event handler (decorator) |
| `on_any(handler)` | Register a wildcard handler that receives all events |
| `off(event_type, handler)` | Remove a specific event handler |
| `recv(timeout=None)` | Receive the next event |
| `force_reconnect()` | Reconnect now (also revives a client that ended, e.g. after `set_token()`) |
| `change_endpoint(url)` | Switch to a different server URL; subscribed topics are subscribed again there |
| `set_token(token)` | Use a new token for the next connect / reconnect |
| `get_stats()` | Connection statistics, message counts, network quality, recovery counters |

`send()` returns a `SendResult` (an `IntEnum`, truthy when the message was accepted):

| Value | Meaning |
|-------|---------|
| `SendResult.SENT` | Written to the socket; with the outbox, the `delivered` event says when the server took it |
| `SendResult.QUEUED` | Waits in the outbox (or the offline queue), and goes on its own - do not retry it |
| `SendResult.FAILED` | Neither sent nor kept (the outbox full, rate limited; without an outbox, not connected); the caller decides whether to retry |

`send(..., live=True)` sends only on the connection that is open now: the offline queue never holds
it for a later one. Use it for commands that must not run late (a valve, a door, an order); signed
messages (`signed=True`) are always sent this way.

**Properties:**

| Property | Type | Description |
|----------|------|-------------|
| `is_connected` | bool | WebSocket connection is open |
| `is_ready` | bool | `server_ready` received on the current connection |
| `is_fully_ready` | bool | Connected, server_ready received, and client_hello sent |
| `state` | ConnectionState | Current connection state enum |
| `last_error` | WSEError \| None | Why the client ended for good, `None` while it is healthy |
| `connection_quality` | ConnectionQuality | Network quality assessment |
| `subscribed_topics` | set[str] | Currently subscribed topics |
| `queue_size` | int | Events waiting in the receive queue |
| `recovery_enabled` | bool | Whether the server announced message recovery |
| `recovery_state` | dict | Per-topic recovery anchor `{topic: (epoch, offset)}`: the last offset delivered in order |

**Context manager and async iterator:**

```python
async with AsyncWSEClient(url) as client:  # auto connect/disconnect
    async for event in client:              # yields WSEEvent objects
        ...
```

Reconnects are invisible to the iterator: it keeps yielding events across them. It stops after `disconnect()`, and raises the reason when the client ends for good (`WSEAuthError`, `WSEEncryptionError`, or `WSEConnectionError` once `max_attempts` is used up).

### SyncWSEClient

Same API as `AsyncWSEClient` but blocking. Runs the async client in a background daemon thread; stats and state reads run on that thread's event loop.

| Method | Description |
|--------|-------------|
| `run_forever()` | Block and dispatch events to registered callbacks; returns after `close()`, raises the reason when the client ends for good |
| `recv(timeout=None)` | Block until next event (raises `WSETimeoutError` on timeout) |
| `set_error_handler(handler)` | Callback (called from the background thread) for the error that ends the client |
| `background_error` | The error that ended the client, or `None` |
| `close()` | Disconnect and shut down the background thread |

### WSEEvent

```python
@dataclass(frozen=True, slots=True)
class WSEEvent:
    type: str                        # Event type ("t" field)
    payload: dict[str, Any]          # Event data ("p" field)
    id: str | None = None            # Message ID
    sequence: int | None = None      # Sequence number
    timestamp: str | None = None     # ISO 8601 timestamp
    version: int = 1                 # Protocol version
    category: str | None = None      # Message category (system/snapshot/update)
    priority: int | None = None      # Message priority (see MessagePriority)
    correlation_id: str | None = None  # Request correlation ID
    topic: str | None = None         # Recovery stamp: topic ("tp" field)
    epoch: str | None = None         # Recovery stamp: buffer epoch, 8-hex ("e")
    offset: int | None = None        # Recovery stamp: per-topic offset ("o")
    signature: SignatureProof | None = None  # A server-signed message: body, sig, kid, verified
    generation: int = 0              # The user generation it was read in (commit counts in it)
```

## Features

### Auto-Reconnection

Four reconnection strategies with configurable parameters:

```python
from wse_client import AsyncWSEClient, ReconnectConfig, ReconnectMode

client = AsyncWSEClient(
    "ws://localhost:5007/wse",
    reconnect=ReconnectConfig(
        mode=ReconnectMode.EXPONENTIAL,  # EXPONENTIAL, LINEAR, FIBONACCI, ADAPTIVE
        base_delay=1.0,                  # Initial delay in seconds
        max_delay=30.0,                  # Maximum delay cap
        factor=1.5,                      # Backoff multiplier
        max_attempts=-1,                 # -1 = retry forever
        jitter=True,                     # Add randomness to prevent thundering herd
    ),
)
```

`ADAPTIVE` currently uses the same curve as `EXPONENTIAL` (kept for API parity with the TS client). Delays never exceed `max_delay` (and 5 minutes), however long the outage.

Which closes are reconnected:

| Close | What the client does |
|-------|----------------------|
| `disconnect()` / `close()` (client-initiated) | Final |
| Server: 4401 / 4403 / 1008, or HTTP 401 / 403 on the upgrade | Final, raises `WSEAuthError` - with a `token_provider`, first fetches a fresh token and retries (3 rejections in a row end it) |
| Server: 1000, 1001 (stop), 1005 (kick), 1011, 1012, 1013, 4300 (drain), 4429, any other code; network loss | Reconnect with backoff |
| A failed reconnect attempt | Next attempt with backoff (never terminal); only `max_attempts` ends the client |
| Any close before the first handshake of `connect()` completed | `connect()` raises; nothing reconnects in the background |

On every new connection the client subscribes again to all subscribed topics (with recovery positions when the server supports recovery) and, with `offline_queue=True`, sends the queued messages.

### Authentication

```python
client = AsyncWSEClient(url, token="<jwt>")              # fixed token
client = AsyncWSEClient(url, token_provider=get_token)   # fresh token per attempt (sync or async)
client.set_token("<new jwt>")                            # used from the next (re)connect
await client.force_reconnect()                           # ... or right away
```

A rejected token is kept (not wiped), so `force_reconnect()` after `set_token()` revives a client that ended with `WSEAuthError`.

### Message Priority

Five priority levels for message ordering:

```python
from wse_client import MessagePriority

await client.send("alert", {"msg": "critical"}, priority=MessagePriority.CRITICAL)     # 10
await client.send("update", {"data": "..."}, priority=MessagePriority.HIGH)             # 8
await client.send("status", {"ok": True}, priority=MessagePriority.NORMAL)              # 5
await client.send("metric", {"cpu": 42}, priority=MessagePriority.LOW)                  # 3
await client.send("log", {"msg": "debug"}, priority=MessagePriority.BACKGROUND)         # 1
```

### The outbox

Every message sent, except a live or signed one, waits in the outbox until the server answers for it, goes again after a reconnect, and is numbered: a 3.2 server drops a repeat of one it took. What the server cannot tell (the client comes from a cluster node that is gone, or its handler panicked on the message) reaches the server's application marked `possible_repeat`, and an application that must apply an order once checks the message's `id` against what it applied. The handlers of `delivered` (`{"ids": [...]}`) and `not_delivered` (`{"reason": ..., "ids": [...]}`) hear what became of each, by its id - the application's own with `send(..., id=...)`, a random UUID without one. They are handlers' events, as `connected` is, never on the stream `recv()` reads.

```python
@client.on("delivered")
def delivered(event):
    print("the server took", event.payload["ids"])

@client.on("not_delivered")
def not_delivered(event):
    print(event.payload["reason"], event.payload["ids"])

await client.send("order", {"item": "tea"}, id="order-17", ttl=60)
```

A message is not delivered when the server refused it (`busy`, `rate_limited`, `missing`, `not_ready`), when it was not written within its `ttl` or `Offline.max_age` (`expired`, an hour by default; once written it waits for the answer whatever its age), when it was the last user's (`user_changed`), or when the application cleared the outbox (`cleared`: `client.clear()` on sign-out). At most `Offline.max_messages` (100,000) and `Offline.max_bytes` (16 MiB) wait, the ones not answered yet included; past either `send()` returns `SendResult.FAILED`. `client.kept` says how many wait. `disconnect()` waits up to a second for the server's answers; what waits stays for the next `connect()`. It sends in the order of sending, whatever the priority. With a server older than 3.2 a message is gone once written, as before. `send_with_retry()` is `send()` with an outbox: the outbox retries.

`Offline(path="/var/lib/app/queue")` keeps the outbox in a file, so that it outlives the process: the file the Rust client writes, line by line (each checked with a CRC-32 and a salt, read up to the first that does not check), the client's alone while the process runs (a lock file beside it, `queue.lock`). A thread of its own writes the lines of a batch at once and syncs them to the disk (on macOS past the drive's cache); a message goes on the connection once the file holds it, so that after a crash its number is never another message's; the file is written anew once what went outweighs what is kept, and when the process ends it writes what it was handed. A file that cannot be opened or written is a `storage_failed` event (`{"operation", "message"}`) to the handlers, and the outbox goes on in memory.

### Compression

The client inflates compressed frames from the server (`C:` prefix or raw zlib), bounded by `max_message_size`: a frame that would inflate past the limit is dropped without being inflated in full. Outbound messages are sent uncompressed.

### E2E Encryption

Each frame sealed for the server alone, with the server's identity checked. Requires the `cryptography` package:

```bash
pip install wse-client[crypto]
```

```python
client = AsyncWSEClient(url, token=token, encryption=True, e2e_server_keys=[SERVER_KEY])
```

`SERVER_KEY` is what the server's `e2e_identity_public_key()` returns; list two during a key rotation. Instead of pinning, `e2e_unauthenticated=True` accepts any server (safe only from passive listeners), and `e2e_legacy=True` speaks the first design to servers before 3.0. With `encryption=True` and none of the three, the client refuses to start. Setup, guarantees and the wire format: [SECURITY.md](https://github.com/silvermpx/wse/blob/main/docs/SECURITY.md#end-to-end-encryption).

Encryption fails closed:

- After the handshake every frame is sealed, both ways: application messages, subscriptions, recovery requests, pings and PONGs. Messages sent before the handshake completed wait for it; nothing goes out in plaintext, and a heartbeat due meanwhile is skipped.
- Each connection runs a fresh session; no frame is sealed with the keys of an earlier one.
- If the server's identity does not check, a frame does not open (replayed, reordered, forged), or the server refuses (`KEY_EXCHANGE_FAILED`, `E2E_UNSUPPORTED`, no `server_hello` within 10 s), the client closes the connection and ends with `WSEEncryptionError`; it never falls back to plaintext.

### Signed Messages

A message that may be disputed (an order) can go signed with this client's own key, and the server's signed messages arrive with their proof. Requires the `cryptography` package (`[crypto]` extra).

```python
from wse_client import AsyncWSEClient, generate_signing_key, public_key_jwk

key = generate_signing_key()          # keep it on this device
# Your backend puts public_key_jwk(key) in the token's "cnf" claim. The client
# proves the key inside its end-to-end session, so the token is of no use to
# anyone without the key.

async with AsyncWSEClient(
    url, token=token, signing_key=key, server_signing_keys=[SERVER_SIGNING_KEY],
    encryption=True, e2e_server_keys=[SERVER_KEY],
) as client:
    await client.send("order", {"side": "buy", "qty": "1.5"}, signed=True, nonce="o-17")
    async for event in client:
        if event.type == "receipt":   # the server's signed receipt: event.payload["n"] == "o-17"
            keep(event.signature)     # body, sig, kid: the proof
```

A backend can bind the key with `server.set_client_key()` instead and leave it out of the token; the client then sends signed messages with or without encryption, and with encryption needs `e2e_bind_session=False`.

The server takes a signed message once (`nonce`, 1 to 128 characters, random by default), within its time window, and refuses anything else with a `SIGNATURE_REFUSED` error carrying `n`. A server-signed message no pinned key verifies, or one signed for another topic than the one it came on or for another user, is dropped; without `server_signing_keys` they arrive unchecked (`event.signature.verified` is False). Setup and guarantees: [SECURITY.md](https://github.com/silvermpx/wse/blob/main/docs/SECURITY.md#signed-messages).

### Circuit Breaker

Off by default: the backoff alone spaces reconnect attempts, at most `max_delay` (30 s) apart, so the client is back soon after the server. To pause after failures in a row instead, set `ReconnectConfig(circuit_breaker_threshold=5, circuit_breaker_timeout=60.0)`: after five failed attempts nothing is tried for 60 s, then one attempt, and a failure pauses again. `get_stats()["circuit_breaker"]` shows whether it is on and its state.

States: CLOSED (normal) -> OPEN (blocking) -> HALF_OPEN (probing) -> CLOSED (recovered)

### Rate Limiting

Client-side token bucket for outbound application messages: 100 messages/s with a burst of 1000 by default, set with `rate_limit=` / `rate_limit_burst=`, disabled with `rate_limit=None`. A rate-limited `send()` returns `SendResult.FAILED` (one warning every 5 s, counted in `get_stats()["rate_limiter"]["rate_limited"]`). Protocol frames - heartbeat, PONG, client_hello, subscriptions, recovery requests, health / metrics replies - are never limited. The limiter does not read the server's limits: the server's `rate_limit_warning` events concern the messages it sends to this connection, and are passed to your handlers.

### Idempotent delivery & recovery

Stamped topic messages carry a per-message `(topic, epoch, offset)` (`tp`/`e`/`o`).
Dedup and ordering are driven by the per-topic `(epoch, offset)`, which is
authoritative and survives reconnects: a duplicate (offset already seen on the
same epoch) is dropped and an in-order message is delivered. An epoch change
(server restart) re-baselines the topic. A topic published on several servers of
a cluster carries a stream (an epoch) per server; the client keeps the offset of
each, and a recovery names every one of them. Unstamped messages fall back to a
sliding-window id dedup (10,000-entry cache). The server's global `seq` is a
diagnostic only - it is not used for ordering.

A topic never goes dark after an offset gap:

- When the server supports recovery, the client asks it to replay the topic from
  the last offset delivered in order, and holds the messages that arrive past the
  hole for up to `recovery_timeout` seconds (one request in flight per topic).
  Replayed messages are delivered in order, then the held ones.
- If the hole is not filled in time - or the server cannot replay it, does not
  answer, or recovery is not available - the client delivers the held messages
  and reports the loss as a `recovery_gap` event:

```python
@client.on("recovery_gap")
def on_gap(event):
    p = event.payload  # topic, epoch, first_missing, last_missing, missed, reason
    print(f"{p['topic']}: lost {p['missed']} messages ({p['reason']})")
```

The `recovery_gap` event also reaches `on_any` handlers and the iterator. `reason` is one of
`unavailable`, `timeout`, `not_recovered`, `incomplete`, `overflow`,
`epoch_changed`, `closed`. `recovery_state` reports the last offset delivered per
topic (never a server head that was not delivered). Recovery requests are sent
as `subscription_update` messages with `recover: true`; the server application
answers them (see `subscribe_with_recovery` in the server docs).

### Where every fact counts: recover-or-resync

`delivery=DeliveryConfig(consumer=...)` holds a topic that is not continuous --
from a cold start, after every reconnect, after a break -- until the application
brought its state of it up to date: a `resync` event asks for it, and
`client.resynced(round)` hands on what was held, in order. After applying a
message the application calls `client.commit(event)`; `client.checkpoint()` is
what to save with its own state and give back as `DeliveryConfig(checkpoint=...)`,
and recovery starts from it, so a message can come twice: apply a repeat
safely. A checkpoint of another realm, user, consumer or schema is refused
aloud (a `checkpoint_rejected` event), and a message read before another user's
connection counts for nobody when it is committed.

```python
from wse_client import AsyncWSEClient, DeliveryConfig

delivery = DeliveryConfig(consumer="board", checkpoint=load_saved_checkpoint())
async with AsyncWSEClient(url, token=token, topics=["orders"], delivery=delivery) as client:
    async for event in client:
        if event.type == "resync":
            await reload(event.payload["topics"])  # reasons, committed: per topic
            client.resynced(event.payload["round"])
        elif event.topic == "orders":
            apply(event)
            client.commit(event)
            save_checkpoint(client.checkpoint())
```

A handler that calls `resynced` hears what was held once it returned.
`continuity` events say how a topic's continuity changes and `client.topic_states`
where each topic stands; without a delivery (best effort) nothing is held, and
the states and the checkpoint are kept all the same. It is the delivery of the
TypeScript, Rust, Swift, Kotlin and Dart clients; `SyncWSEClient` takes the same
`delivery`, and each `connect()` of it starts from what the last one committed.
When the server takes a connection for another user than the last, the client
forgets where the last one's topics stood and drops what it queued, in either
mode.

`await client.send("cancel", {"id": 9}, requires=["orders"])` waits in memory
until each topic it requires is continuous on the connection (it returns
`SendResult.QUEUED`), and then joins the outbox with them: each time it goes,
after a reconnect too, they are checked right before it goes, so it waits for
the resync, and what was sent after it waits in its place with it. Sends that
require nothing go past one still waiting in memory. Its `ttl` counts from the
send: one that runs out while it waits is `not_delivered` (`expired`). It is
the user's who sent it: once the server takes a connection for another, it
goes nowhere. At most 1,000 wait in memory.

### Network Quality Monitoring

Real-time network quality assessment based on PING/PONG round-trip measurements:

```python
stats = client.get_stats()
print(stats["network"]["quality"])       # EXCELLENT / GOOD / FAIR / POOR (UNKNOWN until enough samples)
print(stats["network"]["latency_ms"])    # Average round-trip time
print(stats["network"]["jitter_ms"])     # Latency variance
print(stats["network"]["packet_loss"])   # Estimated packet loss (%)
print(stats["recovery"])                 # enabled, pending_gaps, gaps, missed, recovered
```

### Connection Pool

Multi-endpoint support with health scoring and automatic failover:

```python
from wse_client import ConnectionPool, LoadBalancingStrategy

pool = ConnectionPool(
    ["ws://server1:5007/wse", "ws://server2:5007/wse", "ws://server3:5007/wse"],
    strategy=LoadBalancingStrategy.WEIGHTED_RANDOM,  # ROUND_ROBIN, WEIGHTED_RANDOM, LEAST_CONNECTIONS
)
url = pool.select_endpoint()
```

Endpoints are scored based on connection success rate, latency, and recent failures. Unhealthy endpoints are deprioritized automatically.

## Error Handling

```python
from wse_client.errors import (
    WSEError,                # Base exception
    WSEConnectionError,      # Connection failures
    WSETimeoutError,         # Operation timeouts
    WSEAuthError,            # Authentication failures
    WSERateLimitError,       # Rate limit exceeded
    WSEProtocolError,        # Wire protocol violations
    WSECircuitBreakerError,  # Circuit breaker open
    WSEEncryptionError,      # Encryption/decryption failures
)
```

| Error | Raised by | When |
|-------|-----------|------|
| `WSEAuthError` | `connect()`, iterator, `recv()`, `run_forever()` | The server rejected the credentials (close 4401 / 4403 / 1008, HTTP 401 / 403), or the `token_provider` failed during `connect()` |
| `WSEEncryptionError` | `connect()`, `send()`, iterator, `recv()` | `encryption=True` and the key exchange failed |
| `WSEConnectionError` | `connect()`, iterator, `recv()` | The connection could not be opened or closed during the handshake; reconnect attempts ran out; `recv()` after `disconnect()` |
| `WSETimeoutError` | `connect()`, `SyncWSEClient.connect()` / `recv()` | Opening the connection (or the sync `recv`) timed out |
| `WSECircuitBreakerError` | `connect()` | The circuit breaker is open |

`WSERateLimitError` and `WSEProtocolError` are exported for compatibility; the client does not raise them (a rate-limited send returns `SendResult.FAILED`, an undecodable frame is logged and dropped).

## Wire Protocol

The client speaks WSE wire protocol v1:

- **Text frames:** JSON with `c` field for category (`WSE`, `S`, `U`)
- **Binary frames:** Codec prefix (`C:` zlib, `M:` msgpack, `E:` E2E sealed) + payload; with `wire_format="msgpack"` the client asks for `?format=msgpack`, decodes `M:` frames (sealed ones included) and sends its messages as `M:` frames
- **Handshake:** `client_hello` is the first frame of every connection; the connection is ready at the first `server_ready` or `server_hello` (with encryption: once the key exchange completed)
- **Heartbeat:** JSON PING/PONG with latency tracking (sealed like every other frame inside an E2E session)

Full protocol specification: [PROTOCOL.md](https://github.com/silvermpx/wse/blob/main/docs/PROTOCOL.md)

## Requirements

- Python 3.11+
- `websockets >= 13.0`

Optional:
- `cryptography >= 43.0` (E2E encryption)
- `msgpack >= 1.0` (MessagePack wire format)
- `orjson >= 3.10` (faster JSON serialization)

## License

MIT
