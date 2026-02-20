"""Rust acceleration for WSE. Rust is mandatory -- crashes if not built."""

from wse_server._wse_accel import (
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityMessageQueue,
    RustPriorityQueue,
    RustSequencer,
    RustTokenBucket,
    RustWSEServer,
    rust_compress,
    rust_decompress,
    rust_hmac_sha256,
    rust_match_event,
    rust_sha256,
    rust_should_compress,
    rust_sign_message,
    rust_transform_event,
)

__all__ = [
    "RustCompressionManager",
    "RustEventSequencer",
    "RustPriorityMessageQueue",
    "RustPriorityQueue",
    "RustSequencer",
    "RustTokenBucket",
    "RustWSEServer",
    "rust_compress",
    "rust_decompress",
    "rust_hmac_sha256",
    "rust_match_event",
    "rust_sha256",
    "rust_should_compress",
    "rust_sign_message",
    "rust_transform_event",
]
