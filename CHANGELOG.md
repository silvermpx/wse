# Changelog

## v2.0.3 (2026-02-28)

### Bug Fixes (Python Client)

- Fix HMAC signing auto-activation when encryption enabled
- Fix `send_bytes()` bypassing E2E encryption
- Fix key rotation not restarting after reconnect
- Fix offline queue message loss on partial flush failure
- Fix `SyncWSEClient` stale error state on retry
- Fix connection pool active count growing on force-reconnect

## v2.0.2 (2026-02-28)

Bug fixes and documentation updates.

## v2.0.1 (2026-02-28)

Minor fixes.

## v2.0.0 (2026-02-28)

### Breaking Changes

- **Standalone-only architecture**: the FastAPI Router mode has been removed. `RustWSEServer` is now the only deployment mode. All Python-side connection handling, message routing, and session management code has been removed in favor of the Rust implementation.
- **Removed dependencies**: `fastapi`, `starlette`, and `redis` are no longer required.
- **Removed modules**: `wse_server.router`, `wse_server.dependencies`, `wse_server.connection`, `wse_server.core.pubsub`, `wse_server.reliability`, `wse_server.metrics`

### Cluster Protocol (NEW)

Custom binary TCP mesh protocol replacing Redis pub/sub for multi-instance coordination:

- **Broadcast mesh**: direct peer-to-peer TCP connections with full mesh topology, custom 8-byte binary frame header, HELLO handshake with magic bytes and version negotiation
- **Interest-based routing**: SUB/UNSUB/RESYNC frames allow peers to announce topic subscriptions. Messages are only forwarded to peers with matching subscribers, reducing inter-node bandwidth
- **Wire protocol versioning**: version negotiated in HELLO handshake, minimum version enforcement, unknown message types silently ignored for forward compatibility
- **mTLS**: mutual TLS via rustls + tokio-rustls with P-256 certificates and WebPkiClientVerifier for peer authentication on untrusted networks
- **Dynamic peer discovery**: gossip-based discovery via PeerAnnounce/PeerList frames. New nodes only need one seed address to join the cluster
- **Inter-peer compression**: zstd compression for messages above 256 bytes, capability-negotiated, decompression output capped at 1 MB
- **Reliability**: per-peer circuit breaker (10 failures, 60s reset), exponential backoff reconnect, heartbeat (5s ping, 15s timeout), dead letter queue (1000 entries)

### Presence Tracking (NEW)

Per-topic presence tracking with automatic user-level join/leave events:

- **Per-topic presence**: track which users are active in each topic with custom metadata (status, display name, etc.)
- **User-level grouping**: multiple connections from the same user (JWT `sub`) are deduplicated into a single presence entry. Join fires on first connection, leave fires on last disconnect
- **Presence query API**: `presence(topic)` returns full member list, `presence_stats(topic)` returns O(1) member/connection counts
- **Data updates**: `update_presence(conn_id, data)` broadcasts updated presence data across all topics where the user is present
- **Cluster presence sync**: PresenceUpdate (0x0B) and PresenceFull (0x0C) frames synchronize presence state across all cluster nodes with CRDT last-write-wins conflict resolution
- **TTL sweep**: background task every 30s removes entries from dead connections
- **Configurable limits**: `presence_max_data_size` (default 4096 bytes), `presence_max_members` per topic (default unlimited)

### Message Recovery (NEW)

Per-topic ring buffer system for recovering missed messages on reconnect:

- Power-of-2 ring buffers per topic with configurable size (default 128 slots)
- Epoch+offset tracking for precise recovery positioning
- Global memory budget (default 256MB) with TTL and LRU eviction
- `subscribe_with_recovery()` returns per-topic recovery status and replays missed messages

### Protocol Negotiation (NEW)

- `client_hello`/`server_hello` handshake with feature discovery and version negotiation
- Server advertises capabilities (compression, recovery, cluster, batching, msgpack), limits (message size, rate limits), and connection metadata

### Server-Initiated Ping + Zombie Detection (NEW)

- Server pings every connected client every 25 seconds
- Connections with no activity for 60 seconds are force-closed
- Prevents resource leaks from abandoned connections

### Rate Limit Feedback (NEW)

- Client receives `rate_limit_warning` when approaching the limit (20% remaining)
- Client receives `error` with code `RATE_LIMITED` when exceeded
- Token bucket: 100K capacity, 10K/s refill per connection

### Performance

- **Pre-framed WebSocket broadcast**: frame built once per broadcast, raw bytes shared via Arc across all connections
- **DashMap**: lock-free concurrent hash maps for topics, formats, rates, and activity tracking
- **Vectored writes (writev)**: batch frame delivery via write_vectored syscall
- **CPU-aware fan-out**: hybrid task chunking based on available CPU cores
- **mimalloc**: global allocator for improved multi-threaded allocation

### Bug Fixes

- Fixed TOCTOU race in topic subscriber cleanup
- Fixed JSON injection in error response builder
- Fixed mutex poisoning panics in DLQ and config
- Fixed event type naming in examples ("message" -> "msg")
- Updated Python client CLIENT_VERSION to 2.0.0

### Integration Tests

25 tests covering connection lifecycle, messaging, broadcast, subscriptions, protocol negotiation, health monitoring, recovery, and server lifecycle.

## v1.4.0 (2026-02-24)

### Redis Pub/Sub (NEW)

Built-in Redis Pub/Sub module for multi-instance horizontal scaling:

- **`publish(topic, data)`** - cross-instance message delivery via Redis with pipelined PUBLISH (up to 64 commands per round-trip, ~45K msg/s with Redis 8.6)
- **`publish_local(topic, data)`** - single-instance topic fan-out without Redis (~2.1M del/s)
- **`subscribe_connection(conn_id, topics)`** - topic subscriptions with exact match and glob patterns (`*`, `?`)
- **PSUBSCRIBE wse:\*** - automatic Redis subscription, `wse:` prefix stripped for local topic routing
- **Deduplication** - connections matching both exact and wildcard patterns receive messages once

### Reliability

- **Auto-reconnection** with exponential backoff (1s initial, 1.5x multiplier, 60s max, +/-20% jitter)
- **Circuit breaker** - two breakers (connection + publish), 10-failure threshold, 60s reset, HALF_OPEN probe with 3 test calls
- **Dead Letter Queue** - in-memory ring buffer (1000 entries), populated on publish failure or circuit breaker open, drained via `get_dlq_entries()`
- **Pipelined publish with retry** - batches up to 64 PUBLISH commands, retries 3x (100ms, 200ms delays), then DLQ

### Health Monitoring

- **`health_snapshot()`** - full server health dict: connections, queue depth, Redis status, all metrics, uptime
- **`redis_connected()`** - Redis connection status (bool)
- **Metrics** - AtomicU64 counters: messages received/published/delivered/dropped, publish errors, reconnect count

### Server Improvements

- **Idle connection timeout** - configurable per-connection timeout (default 300s), reset on any message or ping
- **Server uptime tracking** - `started_at` tracked, exposed in `health_snapshot()`

### Fan-out Benchmarks

- **Single-instance broadcast**: 2.1M deliveries/s, 500K connections, zero message loss
- **Multi-instance (Redis 8.6)**: 1.04M deliveries/s peak at 500 subscribers, zero gaps
- **Fan-out benchmark suite** - `bench_fanout_server.py` (broadcast/pubsub/subscribe modes) + `wse-bench --test fanout-broadcast/fanout-multi`

## v1.3.9 (2026-02-24)

### Benchmarks

- **TypeScript benchmark client** (`benchmarks/ts-bench/`) - 7-test suite matching the Rust bench, measuring Node.js consumer performance: 116K msg/s single-process, 7.0M msg/s at 64 processes (97% linear scaling), 64K+ stable connections, 100% hold survival
- **Rust benchmark client** (`benchmarks/rust-bench/`) - confirmed 14.2M msg/s JSON, 30M msg/s binary, 500K connections with zero failures on EPYC 7502P
- **Full cross-client benchmark documentation** - Rust, Python, and TypeScript results with per-tier breakdowns, latency percentiles, payload size matrices, and format comparisons

### Bug Fixes (TypeScript Benchmark)

- Fixed `connectAndHandshake` resolving before `client_hello` send callback fires (send errors were silently swallowed)
- Fixed `server_version` stuck at 1.2.2 in `WSEConnection` config (now synced with release version)

## v1.3.8 (2026-02-24)

### Bug Fixes (Rust Server)

- Fixed `push_inbound` only counting `Full` errors - now counts all send failures including channel disconnect during shutdown (was silently dropping events without incrementing `inbound_dropped`)
- Fixed inconsistent queue size cap between standalone (uncapped) and embedded (65536) - both now cap at 131072

## v1.3.7 (2026-02-24)

### Performance Fix (Rust Server)

- **Replaced `Mutex<VecDeque>` with `crossbeam-channel` in drain mode** - eliminates lock contention when N Tokio tasks push to the inbound queue simultaneously. `try_send()` is lock-free (atomic CAS), replacing `Mutex::lock()` which blocked Tokio worker threads under high concurrency.
- **Bounded inbound queue** - added `max_inbound_queue_size` (default 65536) to prevent OOM under sustained load. When full, new events are dropped (back-pressure) instead of unbounded growth.
- **Drain mode throughput now matches non-drain mode** - benchmarked at 6.9M msg/s at 1000 connections (was 178K msg/s with Mutex, 38x improvement). Zero overhead from the channel.

### Dependencies

- Added `crossbeam-channel 0.5` (lock-free MPMC bounded channel)

## v1.3.6 (2026-02-23)

### Bug Fixes (Python Client)

- Fixed auth: send JWT as both Cookie and Authorization header (Rust server reads Cookie)
- Fixed PONG handling: parse WSE JSON PONG format from Rust server (was only handling colon-prefix)
- Fixed WebSocket resource leak: proper `__aexit__` cleanup in `_force_reconnect` and `_handle_close_code`
- Fixed `server_ready` parsing: read `connection_id` from nested `details` object
- Fixed asyncio task GC: all `ensure_future` calls now tracked in `_background_tasks` set
- Fixed uuid7 → uuid4 for Python 3.11-3.14 compatibility (uuid7 is stdlib only in 3.14)
- Fixed `send_batch` mutating codec sequence directly (now uses sequencer)
- Fixed background tasks not cancelled on disconnect (heartbeat, listener loops)
- Fixed `SyncWSEClient._connect_error` not initialized in `__init__`
- Fixed `SyncWSEClient.close()` not waiting for disconnect completion
- Fixed reconnect not cancelling old heartbeat task (duplicate loops)
- Fixed IV cache using unordered `set` (now `dict` for FIFO eviction)
- Fixed event sequencer window: 1,000 → 10,000 (matching TS client)
- Fixed CLIENT_VERSION constant out of date
- Fixed `max_attempts=0` treated as infinite retries
- Fixed PING case mismatch: send lowercase `"ping"` (Rust server fast-path checks lowercase only)
- Fixed `connect()` factory: sync return (no await), prevents double-connect with `async with`
- Fixed `_handle_pong_event`: read `client_timestamp` key (was reading `timestamp`, Rust server uses `client_timestamp`)
- Fixed `ConnectionClosedOK` handler: cancel heartbeat task and clear `_ws` (was leaving zombie heartbeat loop)
- Fixed resource leak on timeout: call `__aexit__` on ws context manager when `asyncio.wait_for` times out
- Fixed `disconnect()` race condition: await cancelled tasks before closing WebSocket (tasks could trigger spurious reconnect)
- Removed dead code block in `_force_reconnect` (duplicate unreachable `_ws_cm` check)
- Fixed double PONG latency recording: removed transport-layer `_record_pong_latency` for JSON PONGs (single recording via `_handle_pong_event`)
- Fixed `ConnectionClosedOK` resource leak: `_ws_cm.__aexit__()` now called on server-initiated clean close (TCP socket was orphaned)
- Fixed stale `_recv_task` reference: cleared to `None` in `_handle_close_code` after task exits
- Fixed `_force_reconnect` task cancellation order: cancel and await tasks BEFORE closing WebSocket (was closing socket first, causing spurious exceptions in recv loop)
- Fixed `send_with_retry` sleeping after last failed attempt (unnecessary 2s delay before returning False)
- Fixed `request_snapshot` sleeping after last failed attempt (same pattern)
- Fixed Fibonacci reconnect zero-delay on first attempt: `_fib(0)=0` → now uses `_fib(attempt+1)` so first delay is `base_delay * 1`
- Fixed `ConnectionClosedOK` handler: `__aexit__` now awaited directly instead of fire-and-forget (was cancelled by `disconnect()`)
- Fixed `_on_raw_message` text frame byte counting: `len(str)` → `len(str.encode("utf-8"))` for accurate stats

### Bug Fixes (TypeScript Client)

- Fixed binary frame prefix stripping in `handleIncomingMessage`: server sends uncompressed JSON as binary with `WSE{`/`S{`/`U{` prefix - now stripped before parse (was bypassing age filter and dedup)
- Fixed binary frame prefix stripping in `processBinaryMessage`: category prefix (`WSE{`/`S{`/`U{`) stripped before JSON.parse in step 4 (was falling through to "unknown format" error)
- Fixed key rotation breaking ECDH encryption: `rotateKeys()` now skips when shared secret exists
- Fixed `beforeunload` listener accumulating on HMR: handler tracked on `globalThis`, removed before re-add
- Fixed `processPendingServerReady` calling full `handleServerReady` (caused duplicate `client_hello`): now only fires `onServerReady` callback in both `ConnectionManager` and `MessageProcessor`
- Fixed `clientHelloRetryTimer` leak: tracked and cleared in `clearAllTimers()`
- Fixed `server_ready`/`server_hello` throttled at 500ms: now zero-throttle (reconnect handshake was silently blocked)
- Fixed `messagesSent` metric double-counted: removed duplicate increment from `processBatch` (already counted in `ConnectionManager.send`)
- Fixed `sendMessage` retry timer leak: cancel pending retry timers on cleanup (was leaking closures after unmount)
- Fixed `tokenRefreshAttempts` not reset between calls: second invocation permanently blocked token refresh
- Fixed `handleStateChange` stale closure: routed through `stableCallbacks.current` (consistent with `onMessage`/`onServerReady`)
- Fixed `processTextMessage` missing array prefix handling: added `WSE[`/`S[`/`U[` stripping (was silently dropping array-prefixed text frames)
- Fixed `ConnectionPool` initial health-check timer leak: stored and cleared in `destroy()`
- Fixed `getBestEndpoint` unnecessary optional chaining: removed `?.` on guaranteed-non-empty array
- Fixed stale `validToken` closure in circuit breaker interval: clear old interval before creating new one (was using stale token after rotation)
- Fixed `error` and `subscription_update` messages silently dropped by 500ms throttle: added to zero-throttle list

### Bug Fixes (Rust Server)

- Fixed JWT validation: empty issuer/audience config now skips validation (was passing `Some("")` which rejected all tokens)
- Fixed JWT exp required: tokens without `exp` claim are now rejected (were accepted indefinitely)
- Fixed auth fallback: server now reads `Authorization: Bearer <token>` header when no `access_token` cookie present (enables Python/API client auth)
- Fixed `Bearer` prefix matching: now case-insensitive per RFC 7235 (accepts `bearer`, `BEARER`, etc.)
- Fixed JWT expiration boundary: `now >= exp` per RFC 7519 (was `now > exp`, accepting tokens at exact expiration second)
- Fixed `conn_rates` memory leak: per-connection rate limiter state now cleaned up on disconnect (was growing unbounded)
- Fixed JSON injection in `build_server_ready`: user_id from JWT `sub` claim now properly escaped via `serde_json` (was raw format string interpolation)
- Replaced `get_connection_count()` channel round-trip with `AtomicUsize` - zero GIL, zero blocking, nanosecond reads from Python async handlers

### Documentation

- Updated SECURITY.md: documented cookie-based auth as primary method for browsers (OWASP recommended), added client-type auth matrix, added examples for browser/Python/API clients

## v1.3.2 (2026-02-22)

### Bug Fixes

- Fixed Python client packaging: `__init__.py` now exports public API (`connect`, `AsyncWSEClient`, `SyncWSEClient`, `WSEEvent`, all error types)

## v1.3.1 (2026-02-22)

### Documentation

- Added Python client to main README (quick start, feature table, architecture diagram, installation)
- Added Python client architecture section to ARCHITECTURE.md
- Added Python client integration guide to INTEGRATION.md (async, sync, callback patterns, configuration)
- Fixed ruff lint errors across Python client (unused imports, formatting)

## v1.3.0 (2026-02-22)

### Python Client (NEW)

Full-featured async Python client (`wse-client`) with feature parity to the TypeScript client:

- **AsyncWSEClient** - async context manager, async iterator, callback pattern
- **SyncWSEClient** - thread-safe synchronous wrapper with `run_forever()`
- **Wire protocol** - full binary frame support (zlib, msgpack, AES-GCM encrypted)
- **Security** - ECDH P-256 key exchange, AES-GCM-256 encryption, HMAC-SHA256 signing
- **Resilience** - circuit breaker, token bucket rate limiter, reconnect with 4 strategies
- **Connection pool** - multi-endpoint with health scoring, 3 load balancing strategies
- **Network monitor** - latency/jitter/packet-loss quality analysis
- **Event sequencer** - deduplication + out-of-order buffering

### Bug Fixes

- Fixed PING/PONG timestamp extraction for WSE-prefixed frames
- Fixed IV cache overflow (evict oldest half instead of clear-all)
- Fixed double-dispatch of user event handlers in system handler chain
- Fixed SyncWSEClient.connect() silently succeeding on connection failure
- Fixed batch messages missing timestamp field
- Fixed plain-text PONG not updating NetworkMonitor latency
- Fixed connection pool `_preferred_endpoint` initialization

## v1.2.2 (2026-02-22)

### Documentation

- Added standalone mode (`RustWSEServer`) documentation to README, Architecture, Integration Guide, and Benchmarks
- Documented both deployment modes: Router (embedded FastAPI) and Standalone (dedicated Rust server)
- Clarified that all benchmarks use standalone mode
- Updated benchmark methodology section with standalone server instructions
- Cleaned up PROTOCOL.md - replaced domain-specific examples with generic ones

## v1.2.1 (2026-02-22)

### Benchmark Suite

- Added multi-process benchmark (`benchmarks/bench_wse_multiprocess.py`) - stress-tests with N parallel workers, 9 test categories including sustained load and msgpack
- Added standalone benchmark server (`benchmarks/bench_server.py`) - minimal Rust WSE with JWT auth, no database needed
- Fixed code formatting (ruff)
- Fixed README protocol version (v1, not v2)

## v1.2.0 (2026-02-22)

### Rust JWT Authentication

Pure Rust HS256 JWT encode/decode module, eliminating Python JWT libraries from the connection critical path:

- **`rust_jwt_encode(claims, secret)`** - HS256 JWT creation with base64url encoding
- **`rust_jwt_decode(token, secret, issuer, audience)`** - JWT validation with signature verification, expiry check, issuer/audience claims
- **Zero-GIL handshake authentication** - when `jwt_secret` is configured, the Rust server validates JWT cookies during the WebSocket handshake and sends `server_ready` directly from Rust, before any Python code runs
- **`InboundEvent::AuthConnect`** - new drain queue event type for Rust-authenticated connections. Python receives `user_id` directly and skips JWT decode entirely
- **Cookie extraction in Rust** - `access_token` parsed from HTTP cookie header using zero-copy string operations during handshake

**Connection flow with Rust JWT:**
```
Client connects -> Rust WebSocket handshake -> Rust extracts cookie
-> Rust JWT decode (0.01ms) -> Rust sends server_ready -> push AuthConnect to drain
-> Python does async setup (subscriptions, snapshots)
```

**Connection latency improvement:** Median 0.53ms (was ~23ms with Python JWT). 27x faster.

### E2E Encryption: AES-GCM-256 + ECDH P-256

Full end-to-end encryption with ECDH key exchange, matching the frontend `security.ts` implementation exactly:

- **Rust-accelerated AES-GCM-256** encrypt/decrypt via `aes-gcm` crate (`rust_aes_gcm_encrypt`, `rust_aes_gcm_decrypt`)
- **ECDH P-256 key exchange** via `p256` crate (`rust_ecdh_generate_keypair`, `rust_ecdh_derive_shared_secret`)
- **HKDF-SHA256** key derivation with salt=`wse-encryption`, info=`aes-gcm-key` (matching frontend)
- **Per-connection session keys** - each connection gets its own ECDH keypair and derived AES-256 key
- Wire format: `E:` prefix + 12-byte IV + AES-GCM ciphertext + 16-byte auth tag

**Key exchange flow:**

1. Server generates ECDH keypair, sends public key (65 bytes SEC1) in `server_ready`
2. Client sends its public key in `client_hello`
3. Both derive shared AES-256 key via ECDH + HKDF
4. All subsequent `E:`-prefixed messages use this key

New Python classes:

- `AesGcmProvider` - built-in `EncryptionProvider` implementation with per-connection keys
- `SecurityManager.generate_keypair(conn_id)` - ECDH keypair generation
- `SecurityManager.derive_session_key(conn_id, peer_pk)` - session key derivation
- `SecurityManager.remove_connection(conn_id)` - cleanup on disconnect

### Connection Latency Optimization

Backend handshake latency reduced by 27x (median 23ms -> 0.53ms):

- **Rust JWT validation** - JWT decoded entirely in Rust during WebSocket handshake, zero GIL acquisition on the connection critical path
- **OnceLock handshake** - replaced 2x `Arc<Mutex>` with a single `OnceLock<HandshakeResult>` in the WebSocket accept callback. Eliminates 4 lock operations per connection.
- **DashMap for conn_formats** - replaced `std::sync::Mutex<HashMap>` with lock-free `DashMap` for msgpack format tracking. One fewer lock acquisition during connection registration.
- **spawn_blocking for on_connect** - Python `on_connect` callback now runs in a background thread via `tokio::task::spawn_blocking` instead of blocking the async connection setup.
- **SIMD via target-cpu=native** - `.cargo/config.toml` enables native CPU features (AVX2/NEON) for faster zlib compression, HTTP header parsing, and HashMap operations.

### Large Message Throughput

64KB message throughput improved from 8K msg/s to 40K+ msg/s (5x improvement, 2.5 GB/s bandwidth):

- Binary frame path optimization in Rust transport
- Reduced allocation overhead for large payloads
- Direct buffer flush without intermediate copies

### Inbound MsgPack Parsing in Rust

Binary frames from msgpack-connected clients are now parsed entirely in Rust:

- Connections with `?format=msgpack` are tracked in `conn_formats` DashMap
- Binary frames from msgpack connections: `rmpv::decode::read_value` -> `rmpv_to_serde_json` -> `normalize_json_keys` -> `InboundEvent::Message` (pre-parsed dict)
- Python drain queue receives dicts regardless of wire format (JSON or MsgPack)
- Non-msgpack binary frames continue as `InboundEvent::Binary` (raw bytes)
- Callback mode also parses msgpack to Python dicts via `json_to_pyobj`

### Selective Message Signing

Configurable set of message types that are always signed for integrity, regardless of the global `message_signing_enabled` flag:

- `DEFAULT_SIGNED_MESSAGE_TYPES` - empty `frozenset` by default (configure for your domain)
- `WSEConnection(signed_message_types=frozenset({...}))` - pass your critical types
- Logic: sign if `message_signing_enabled` OR `message_type in signed_message_types`
- Signature format: `hash:timestamp:nonce:hmac` (or JWT via TokenProvider)

### Test Suite

- **Rust tests**: 15 inline tests - JWT encode/decode (10 tests), AES-GCM roundtrip, wrong-key rejection, ECDH symmetry, HKDF derivation, HMAC determinism
- **Python tests**: 100 security tests (was 48) - added JWT encode/decode, Rust crypto, ECDH, AesGcmProvider, SecurityManager ECDH, and full E2E encryption tests
- **Full suite**: 376+ tests passing

### Dependencies

New Rust crates: `aes-gcm 0.10`, `p256 0.13` (with `ecdh` feature), `hkdf 0.12`, `dashmap 6`, `base64 0.22`

## v1.1.1 (2026-02-21)

### CI/CD

- Added Python 3.14 to test matrix (CI + Release)
- Fixed cargo fmt and clippy warnings

## v1.1.0 (2026-02-21)

### Binary Frames for JSON

All outbound JSON messages now use WebSocket binary frames instead of text frames. Binary frames skip UTF-8 validation in the WebSocket layer, providing 5-15% higher throughput with zero behavioral change. Clients that already handle binary frames (compression, encryption) work without modification.

### MessagePack Transport (opt-in)

Per-connection msgpack support via `?format=msgpack` query parameter on the WebSocket URL. When enabled, outbound messages are serialized with MessagePack (`M:` prefix) instead of JSON - roughly 2x faster serialization and ~30% smaller on the wire.

The server extracts the format preference during the WebSocket handshake and applies it automatically to all `send_event()` calls for that connection. No changes needed on the Python publisher side.

Requires `@msgpack/msgpack` (JS client) or `msgpack` (Python client).

### Internal

- Added `serde_json_to_rmpv()` conversion helper in `compression.rs`
- Per-connection format tracking in `conn_formats` map on `SharedState`
- Automatic cleanup of format preference on disconnect

## v1.0.3 (2026-02-20)

Initial public release. Rust-accelerated WebSocket engine with:
- tokio + tungstenite transport (130K+ msg/s sustained)
- Drain mode for batched GIL management
- Write coalescing (up to 64 messages per flush)
- zlib compression with `C:` prefix
- AES-GCM-256 encryption with `E:` prefix
- Per-connection deduplication and rate limiting
- Ping/pong handled entirely in Rust
- TCP_NODELAY on accept
