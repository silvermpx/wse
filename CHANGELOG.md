# Changelog

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
