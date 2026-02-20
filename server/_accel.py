"""Rust acceleration for WSE. Rust is mandatory -- crashes if not built."""

from server._wse_accel import (
    rust_transform_event,
    rust_compress,
    rust_decompress,
    rust_should_compress,
    rust_hmac_sha256,
    rust_sha256,
    rust_sign_message,
    rust_match_event,
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityQueue,
    RustPriorityMessageQueue,
    RustTokenBucket,
    RustSequencer,
    RustWSEServer,
)
