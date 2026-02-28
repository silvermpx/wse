"""WSE Server -- High-performance WebSocket engine powered by Rust."""

from wse_server._wse_accel import (
    RustCompressionManager,
    RustEventSequencer,
    RustPriorityMessageQueue,
    RustWSEServer,
    rust_aes_gcm_decrypt,
    rust_aes_gcm_encrypt,
    rust_compress,
    rust_decompress,
    rust_ecdh_derive_shared_secret,
    rust_ecdh_generate_keypair,
    rust_hmac_sha256,
    rust_jwt_decode,
    rust_jwt_encode,
    rust_match_event,
    rust_sha256,
    rust_sign_message,
    rust_transform_event,
)
from wse_server.core.filters import EventFilter
from wse_server.core.types import DeliveryGuarantee, EventPriority

__version__ = "2.0.0"

__all__ = [
    "RustWSEServer",
    "RustCompressionManager",
    "RustEventSequencer",
    "RustPriorityMessageQueue",
    "rust_jwt_encode",
    "rust_jwt_decode",
    "rust_aes_gcm_encrypt",
    "rust_aes_gcm_decrypt",
    "rust_ecdh_generate_keypair",
    "rust_ecdh_derive_shared_secret",
    "rust_hmac_sha256",
    "rust_sha256",
    "rust_compress",
    "rust_decompress",
    "rust_sign_message",
    "rust_match_event",
    "rust_transform_event",
    "EventFilter",
    "EventPriority",
    "DeliveryGuarantee",
    "__version__",
]
