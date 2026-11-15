//! The pieces of the protocol that the server and the Rust client
//! ([`wse-client`](https://crates.io/crates/wse-client)) share: the crypto,
//! end-to-end sessions and signed messages, one implementation for both
//! sides. On aws-lc-rs; in the browser, and with
//! `RUSTFLAGS="--cfg wse_rustcrypto"`, on RustCrypto.
//!
//! Built without the `server` feature, the crate is this module alone: that
//! is how the client depends on it, in the browser too. Applications rarely
//! need it directly.

// The FIPS module is aws-lc-rs's: the RustCrypto backend has none.
#[cfg(all(feature = "fips", any(target_arch = "wasm32", wse_rustcrypto)))]
compile_error!("the fips feature needs aws-lc-rs, which the RustCrypto backend replaces");

pub mod crypto;
pub mod e2e;
pub mod signing;
