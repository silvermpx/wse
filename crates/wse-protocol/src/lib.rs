#![doc = include_str!("../README.md")]

// The FIPS module is aws-lc-rs's: the RustCrypto backend has none.
#[cfg(all(feature = "fips", any(target_arch = "wasm32", wse_rustcrypto)))]
compile_error!("the fips feature needs aws-lc-rs, which the RustCrypto backend replaces");

pub mod crypto;
pub mod e2e;
pub mod signing;
