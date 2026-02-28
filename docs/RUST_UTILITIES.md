# Rust Utility Reference

Rust-accelerated utilities exposed to Python via PyO3. These are application-layer building blocks for use in your Python server code. The transport layer (WebSocket framing, topic fan-out, JWT validation, rate limiting) uses its own internal implementations and does not depend on these utilities.

All utilities are importable from the top-level package:

```python
from wse_server import (
    RustPriorityQueue,
    RustPriorityMessageQueue,
    RustTokenBucket,
    RustSequencer,
    RustEventSequencer,
    RustCompressionManager,
    rust_compress,
    rust_decompress,
    rust_should_compress,
    rust_aes_gcm_encrypt,
    rust_aes_gcm_decrypt,
    rust_ecdh_generate_keypair,
    rust_ecdh_derive_shared_secret,
    rust_hmac_sha256,
    rust_sha256,
    rust_sign_message,
    rust_jwt_encode,
    rust_jwt_decode,
    rust_transform_event,
    rust_match_event,
)
```

---

## Priority Queues

### RustPriorityQueue

Bounded binary heap. Higher priority numbers are dequeued first. Within the same priority level, messages follow FIFO order.

```python
q = RustPriorityQueue(max_size=500)

q.push(priority=10, message={"action": "alert"})   # returns True
q.push(priority=5, message={"action": "update"})

msg = q.pop()        # returns {"action": "alert"} (priority 10 first)
batch = q.drain(50)  # up to 50 messages in priority order

q.len()              # current size
q.is_full()          # True if at capacity
q.clear()            # empty the queue
```

### RustPriorityMessageQueue

Production queue with five priority levels, smart dropping, batch dequeue, and detailed statistics.

Priority levels:

| Name       | Value |
|------------|-------|
| CRITICAL   | 10    |
| HIGH       | 8     |
| NORMAL     | 5     |
| LOW        | 3     |
| BACKGROUND | 1     |

When the queue is full and a new message arrives, the queue drops the lowest-priority message to make room. BACKGROUND messages are dropped first, then LOW, then NORMAL. HIGH and CRITICAL messages are never evicted by lower-priority arrivals.

```python
q = RustPriorityMessageQueue(max_size=1000, batch_size=10)

q.enqueue({"trade": "AAPL"}, priority=8)     # returns True if accepted
q.enqueue({"heartbeat": True}, priority=1)    # BACKGROUND priority

batch = q.dequeue_batch()  # list of (priority, message) tuples
# [(8, {"trade": "AAPL"}), (1, {"heartbeat": True})]

q.size           # current message count (property)
q.get_stats()    # detailed statistics dict
q.clear()        # empty all queues and reset stats
```

`get_stats()` returns:

| Key                    | Type           | Description                          |
|------------------------|----------------|--------------------------------------|
| size                   | int            | Current total messages               |
| capacity               | int            | Max queue size                       |
| utilization_percent    | float          | Current fill percentage              |
| priority_distribution  | dict[int, int] | Message count per priority level     |
| priority_queue_depths  | dict[str, int] | Per-level depth (e.g. priority_10_depth) |
| dropped_by_priority    | dict[int, int] | Dropped count per priority level     |
| total_dropped          | int            | Total messages dropped               |
| backpressure           | bool           | True if utilization > 80%            |
| oldest_message_age     | float or None  | Seconds since oldest enqueued message|
| processing_rate        | float          | Reserved for future use              |

---

## Rate Limiting

### RustTokenBucket

Token bucket rate limiter with automatic time-based refill.

```python
# 100 requests per second, start full
bucket = RustTokenBucket(capacity=100.0, refill_rate=100.0)

# Try to consume 1 token
if bucket.acquire(1.0):
    process_request()
else:
    reject_request()

bucket.tokens   # current token count after refill (property)
bucket.reset()  # refill to capacity
```

Constructor parameters:

| Parameter       | Type           | Description                        |
|-----------------|----------------|------------------------------------|
| capacity        | float          | Maximum tokens the bucket can hold |
| refill_rate     | float          | Tokens added per second            |
| initial_tokens  | float or None  | Starting tokens (defaults to capacity) |

---

## Sequencing

### RustSequencer

Monotonic sequence counter with sliding-window duplicate detection.

```python
seq = RustSequencer(window_size=10000)

n = seq.next_seq()           # increment and return
n = seq.get_current_seq()    # read without incrementing

if seq.is_duplicate("evt-abc-123"):
    drop(event)

seq.cleanup(max_age_secs=300.0)  # evict entries older than 5 minutes
seq.seen_count()                  # number of tracked event IDs
```

### RustEventSequencer

Full-featured sequencer with per-topic ordering, out-of-order buffering, and gap detection.

```python
es = RustEventSequencer(window_size=10000, max_out_of_order=100)

# Generate sequence numbers
seq_num = es.next_seq()

# Deduplication
if es.is_duplicate("evt-abc-123"):
    drop(event)

# Per-topic ordered delivery
result = es.process_sequenced_event("prices", sequence=42, event=payload)
if result is not None:
    for event in result:      # may include buffered events now in order
        deliver(event)

# Monitoring
es.get_sequence_stats()  # per-topic gaps, buffer sizes, duplicate counts
es.get_buffer_stats()    # topics with buffered (out-of-order) events

# Maintenance
es.cleanup()                       # evict buffered events older than 5 min
es.reset_sequence("prices")        # reset one topic
es.reset_sequence()                # reset all topics
```

`process_sequenced_event` behavior:

| Condition                         | Result                                    |
|-----------------------------------|-------------------------------------------|
| First event on topic              | Delivered immediately, tracking begins     |
| Sequence matches expected         | Delivered with any consecutive buffered events |
| Small gap (within max_out_of_order) | Buffered until gap fills                 |
| Large gap (exceeds max_out_of_order) | Buffer flushed, topic reset, event delivered |
| Sequence below expected           | Dropped (old or duplicate)                |

---

## Compression

### RustCompressionManager

Zlib compression with optional msgpack serialization and statistics tracking.

```python
cm = RustCompressionManager(threshold=1024, compression_level=6)

if cm.should_compress(data):
    compressed = cm.compress(data)
    original = cm.decompress(compressed)

# Msgpack serialization
packed = cm.pack_msgpack({"key": "value"})
unpacked = cm.unpack_msgpack(packed)

# Event packing (msgpack with JSON fallback)
packed = cm.pack_event(event_dict, use_msgpack=True)
event = cm.unpack_event(packed, is_msgpack=True)

cm.get_stats()     # compression statistics
cm.reset_stats()   # clear counters
```

### Standalone Functions

```python
compressed = rust_compress(data, level=6)
original = rust_decompress(compressed)
should = rust_should_compress(data, threshold=1024)  # len(data) > threshold
```

---

## Cryptography

### AES-GCM-256 Encryption

Symmetric encryption using AES-GCM with 256-bit keys. Output format: 12-byte IV + ciphertext + 16-byte authentication tag.

```python
key = os.urandom(32)  # 256-bit key

ciphertext = rust_aes_gcm_encrypt(key, b"secret message")
# ciphertext = IV (12) + encrypted data + tag (16)

plaintext = rust_aes_gcm_decrypt(key, ciphertext)
# b"secret message"
```

### ECDH P-256 Key Exchange

Generate ephemeral keypairs and derive shared AES-256 keys via ECDH + HKDF-SHA256.

```python
# Each side generates a keypair
priv_a, pub_a = rust_ecdh_generate_keypair()  # (32 bytes, 65 bytes SEC1)
priv_b, pub_b = rust_ecdh_generate_keypair()

# Each side derives the same shared key
key_a = rust_ecdh_derive_shared_secret(priv_a, pub_b)  # 32 bytes
key_b = rust_ecdh_derive_shared_secret(priv_b, pub_a)  # 32 bytes
assert key_a == key_b

# Use the shared key for AES-GCM
ciphertext = rust_aes_gcm_encrypt(key_a, b"encrypted with shared secret")
plaintext = rust_aes_gcm_decrypt(key_b, ciphertext)
```

HKDF parameters: salt=`wse-encryption`, info=`aes-gcm-key`, hash=SHA-256.

### HMAC and Hashing

```python
mac = rust_hmac_sha256(key=b"secret", data=b"message")  # 32 bytes
hex_digest = rust_sha256(b"data")                        # hex string
signature = rust_sign_message('{"event":"trade"}', secret=b"key")  # hex HMAC
```

`rust_sign_message` computes SHA-256 of the payload string, then HMAC-SHA256 of that hash.

---

## JWT

HS256 JSON Web Token encoding and decoding.

```python
import time

claims = {
    "sub": "user-123",
    "iss": "my-app",
    "aud": "wse",
    "exp": int(time.time()) + 3600,
    "iat": int(time.time()),
}

token = rust_jwt_encode(claims, secret=b"my-secret")

decoded = rust_jwt_decode(
    token,
    secret=b"my-secret",
    issuer="my-app",      # optional, validates iss claim
    audience="wse",       # optional, validates aud claim
)
# Returns dict of claims, or None on any validation failure
```

Validation rules:
- `exp` claim is required and checked against current time
- `nbf` claim is optional, allows 5 minutes of clock skew
- `iss` validated only when `issuer` parameter is provided
- `aud` validated only when `audience` parameter is provided (supports string or array format)

---

## Event Processing

### rust_transform_event

Transforms a raw event dict into the WSE wire envelope format.

```python
event = {
    "event_type": "order_placed",
    "payload": {"symbol": "AAPL", "qty": 100},
}

type_map = {"order_placed": "order"}

envelope = rust_transform_event(event, sequence=42, event_type_map=type_map)
# {
#     "t": "order",
#     "p": {"symbol": "AAPL", "qty": 100},
#     "id": "...",     # UUID v7
#     "ts": "...",     # ISO 8601 UTC
#     "seq": 42,
#     "v": 1,
#     "original_event_type": "order_placed"
# }
```

The transformer handles UUID, datetime, Decimal, and Enum conversion to JSON-safe types. Events that already have the wire format (`t`, `p`, `id`, `ts`, `v`) are returned unchanged.

### rust_match_event

MongoDB-style event filtering with support for nested field access.

```python
event = {
    "t": "trade",
    "p": {"symbol": "AAPL", "price": 185.50, "exchange": "NASDAQ"},
}

# Simple equality
rust_match_event(event, {"t": "trade"})  # True

# Comparison operators
rust_match_event(event, {"p.price": {"$gt": 100}})  # True

# Multiple conditions (implicit AND)
rust_match_event(event, {
    "p.symbol": {"$in": ["AAPL", "GOOG"]},
    "p.price": {"$gte": 150},
})  # True

# String operations
rust_match_event(event, {"p.exchange": {"$startswith": "NAS"}})  # True

# Regex
rust_match_event(event, {"p.symbol": {"$regex": "^AA"}})  # True

# Logical operators
rust_match_event(event, {
    "$or": [
        {"p.symbol": "AAPL"},
        {"p.symbol": "GOOG"},
    ]
})  # True
```

Supported operators: `$eq`, `$ne`, `$gt`, `$lt`, `$gte`, `$lte`, `$in`, `$nin`, `$regex`, `$exists`, `$contains`, `$startswith`, `$endswith`, `$and`, `$or`.

Dot notation (`p.price`) traverses nested dicts.
