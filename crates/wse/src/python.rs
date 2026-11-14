//! The Python module (`wse_server._wse_accel`): the server class and the
//! helpers, registered by the `wse-py` crate that builds the extension.

use pyo3::prelude::*;

/// Add the module's classes and functions to `m`.
pub fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Event transformer
    m.add_function(wrap_pyfunction!(
        crate::transformer::rust_transform_event,
        m
    )?)?;

    // Compression
    m.add_function(wrap_pyfunction!(crate::compression::rust_compress, m)?)?;
    m.add_function(wrap_pyfunction!(crate::compression::rust_decompress, m)?)?;
    m.add_function(wrap_pyfunction!(
        crate::compression::rust_should_compress,
        m
    )?)?;
    m.add_class::<crate::compression::RustCompressionManager>()?;

    // Rate limiter
    m.add_class::<crate::rate_limiter::RustTokenBucket>()?;

    // Event sequencer (simple, backward compat)
    m.add_class::<crate::sequencer::RustSequencer>()?;
    // Event sequencer (full parity with Python EventSequencer)
    m.add_class::<crate::sequencer::RustEventSequencer>()?;

    // Priority queue
    m.add_class::<crate::queue::RustPriorityQueue>()?;
    m.add_class::<crate::queue::RustPriorityMessageQueue>()?;

    // Event filters
    m.add_function(wrap_pyfunction!(crate::filters::rust_match_event, m)?)?;

    // HMAC signing
    m.add_function(wrap_pyfunction!(crate::security::rust_hmac_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(crate::security::rust_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(crate::security::rust_sign_message, m)?)?;

    // AES-GCM-256 encryption
    m.add_function(wrap_pyfunction!(crate::security::rust_aes_gcm_encrypt, m)?)?;
    m.add_function(wrap_pyfunction!(crate::security::rust_aes_gcm_decrypt, m)?)?;

    // ECDH P-256 key exchange
    m.add_function(wrap_pyfunction!(
        crate::security::rust_ecdh_generate_keypair,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::security::rust_ecdh_derive_shared_secret,
        m
    )?)?;

    // JWT encode/decode
    m.add_function(wrap_pyfunction!(crate::jwt_py::rust_jwt_decode, m)?)?;
    m.add_function(wrap_pyfunction!(crate::jwt_py::rust_jwt_encode, m)?)?;

    // WebSocket server
    m.add_class::<crate::server::RustWSEServer>()?;

    Ok(())
}
