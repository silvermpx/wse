use pyo3::prelude::*;

mod jwt;
mod wse;

/// WSE Rust acceleration module.
/// Hot-path operations reimplemented in Rust for 10-20x speedup.
#[pymodule]
fn _wse_accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Event transformer
    m.add_function(wrap_pyfunction!(wse::transformer::rust_transform_event, m)?)?;

    // Compression
    m.add_function(wrap_pyfunction!(wse::compression::rust_compress, m)?)?;
    m.add_function(wrap_pyfunction!(wse::compression::rust_decompress, m)?)?;
    m.add_function(wrap_pyfunction!(wse::compression::rust_should_compress, m)?)?;
    m.add_class::<wse::compression::RustCompressionManager>()?;

    // Rate limiter
    m.add_class::<wse::rate_limiter::RustTokenBucket>()?;

    // Event sequencer (simple, backward compat)
    m.add_class::<wse::sequencer::RustSequencer>()?;
    // Event sequencer (full parity with Python EventSequencer)
    m.add_class::<wse::sequencer::RustEventSequencer>()?;

    // Priority queue
    m.add_class::<wse::queue::RustPriorityQueue>()?;
    m.add_class::<wse::queue::RustPriorityMessageQueue>()?;

    // Event filters
    m.add_function(wrap_pyfunction!(wse::filters::rust_match_event, m)?)?;

    // HMAC signing
    m.add_function(wrap_pyfunction!(wse::security::rust_hmac_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(wse::security::rust_sha256, m)?)?;
    m.add_function(wrap_pyfunction!(wse::security::rust_sign_message, m)?)?;

    // AES-GCM-256 encryption
    m.add_function(wrap_pyfunction!(wse::security::rust_aes_gcm_encrypt, m)?)?;
    m.add_function(wrap_pyfunction!(wse::security::rust_aes_gcm_decrypt, m)?)?;

    // ECDH P-256 key exchange
    m.add_function(wrap_pyfunction!(
        wse::security::rust_ecdh_generate_keypair,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        wse::security::rust_ecdh_derive_shared_secret,
        m
    )?)?;

    // JWT encode/decode
    m.add_function(wrap_pyfunction!(jwt::rust_jwt_decode, m)?)?;
    m.add_function(wrap_pyfunction!(jwt::rust_jwt_encode, m)?)?;

    // WebSocket server
    m.add_class::<wse::server::RustWSEServer>()?;

    Ok(())
}
