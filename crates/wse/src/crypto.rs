//! The one crypto library behind cluster TLS, JWT validation, end-to-end
//! encryption and the Python crypto helpers, chosen when the extension is
//! built:
//!
//! - `aws-lc` (default, the published wheels): aws-lc-rs.
//! - `fips`: aws-lc-rs built from its FIPS 140-3 validated module. Building it
//!   needs CMake and Go:
//!   `MATURIN_PEP517_ARGS="--features fips" pip install wse-server --no-binary wse-server`.
//!
//! The functions live in `crate::protocol::crypto`, shared with the Rust
//! client. TLS gets its provider from here explicitly; nothing is installed
//! as the process-wide rustls default, so an application's own TLS setup is
//! left alone.

#[cfg(not(feature = "aws-lc"))]
compile_error!("choose a crypto library: the `aws-lc` feature (default) or `fips`");

use std::sync::Arc;

pub(crate) use crate::protocol::crypto::*;

/// The rustls provider for cluster TLS.
pub(crate) fn tls_provider() -> Arc<rustls::crypto::CryptoProvider> {
    #[cfg(feature = "fips")]
    let provider = rustls::crypto::default_fips_provider();
    #[cfg(not(feature = "fips"))]
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    Arc::new(provider)
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_library_is_reported() {
        assert!(super::LIBRARY.starts_with("aws-lc-rs") || super::LIBRARY == "RustCrypto");
        assert!(!super::tls_provider().cipher_suites.is_empty());
    }
}
