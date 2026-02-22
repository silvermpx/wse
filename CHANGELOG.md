# Changelog

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

- **AsyncWSEClient** — async context manager, async iterator, callback pattern
- **SyncWSEClient** — thread-safe synchronous wrapper with `run_forever()`
- **Wire protocol** — full binary frame support (zlib, msgpack, AES-GCM encrypted)
- **Security** — ECDH P-256 key exchange, AES-GCM-256 encryption, HMAC-SHA256 signing
- **Resilience** — circuit breaker, token bucket rate limiter, reconnect with 4 strategies
- **Connection pool** — multi-endpoint with health scoring, 3 load balancing strategies
- **Network monitor** — latency/jitter/packet-loss quality analysis
- **Event sequencer** — deduplication + out-of-order buffering

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
- Cleaned up PROTOCOL.md — replaced domain-specific examples with generic ones

## v1.2.1 (2026-02-22)

### Benchmark Suite

- Added multi-process benchmark (`benchmarks/bench_wse_multiprocess.py`) — stress-tests with N parallel workers, 9 test categories including sustained load and msgpack
- Added standalone benchmark server (`benchmarks/bench_server.py`) — minimal Rust WSE with JWT auth, no database needed
- Fixed code formatting (ruff)
- Fixed README protocol version (v1, not v2)

## v1.2.0 (2026-02-22)

### Rust JWT Authentication

Pure Rust HS256 JWT encode/decode module, eliminating Python JWT libraries from the connection critical path:

- **`rust_jwt_encode(claims, secret)`** — HS256 JWT creation with base64url encoding
- **`rust_jwt_decode(token, secret, issuer, audience)`** — JWT validation with signature verification, expiry check, issuer/audience claims
- **Zero-GIL handshake authentication** — when `jwt_secret` is configured, the Rust server validates JWT cookies during the WebSocket handshake and sends `server_ready` directly from Rust, before any Python code runs
- **`InboundEvent::AuthConnect`** — new drain queue event type for Rust-authenticated connections. Python receives `user_id` directly and skips JWT decode entirely
- **Cookie extraction in Rust** — `access_token` parsed from HTTP cookie header using zero-copy string operations during handshake

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
- **Per-connection session keys** — each connection gets its own ECDH keypair and derived AES-256 key
- Wire format: `E:` prefix + 12-byte IV + AES-GCM ciphertext + 16-byte auth tag

**Key exchange flow:**

1. Server generates ECDH keypair, sends public key (65 bytes SEC1) in `server_ready`
2. Client sends its public key in `client_hello`
3. Both derive shared AES-256 key via ECDH + HKDF
4. All subsequent `E:`-prefixed messages use this key

New Python classes:

- `AesGcmProvider` — built-in `EncryptionProvider` implementation with per-connection keys
- `SecurityManager.generate_keypair(conn_id)` — ECDH keypair generation
- `SecurityManager.derive_session_key(conn_id, peer_pk)` — session key derivation
- `SecurityManager.remove_connection(conn_id)` — cleanup on disconnect

### Connection Latency Optimization

Backend handshake latency reduced by 27x (median 23ms -> 0.53ms):

- **Rust JWT validation** — JWT decoded entirely in Rust during WebSocket handshake, zero GIL acquisition on the connection critical path
- **OnceLock handshake** — replaced 2x `Arc<Mutex>` with a single `OnceLock<HandshakeResult>` in the WebSocket accept callback. Eliminates 4 lock operations per connection.
- **DashMap for conn_formats** — replaced `std::sync::Mutex<HashMap>` with lock-free `DashMap` for msgpack format tracking. One fewer lock acquisition during connection registration.
- **spawn_blocking for on_connect** — Python `on_connect` callback now runs in a background thread via `tokio::task::spawn_blocking` instead of blocking the async connection setup.
- **SIMD via target-cpu=native** — `.cargo/config.toml` enables native CPU features (AVX2/NEON) for faster zlib compression, HTTP header parsing, and HashMap operations.

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

- `DEFAULT_SIGNED_MESSAGE_TYPES` — empty `frozenset` by default (configure for your domain)
- `WSEConnection(signed_message_types=frozenset({...}))` — pass your critical types
- Logic: sign if `message_signing_enabled` OR `message_type in signed_message_types`
- Signature format: `hash:timestamp:nonce:hmac` (or JWT via TokenProvider)

### Test Suite

- **Rust tests**: 15 inline tests — JWT encode/decode (10 tests), AES-GCM roundtrip, wrong-key rejection, ECDH symmetry, HKDF derivation, HMAC determinism
- **Python tests**: 100 security tests (was 48) — added JWT encode/decode, Rust crypto, ECDH, AesGcmProvider, SecurityManager ECDH, and full E2E encryption tests
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

Per-connection msgpack support via `?format=msgpack` query parameter on the WebSocket URL. When enabled, outbound messages are serialized with MessagePack (`M:` prefix) instead of JSON — roughly 2x faster serialization and ~30% smaller on the wire.

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
