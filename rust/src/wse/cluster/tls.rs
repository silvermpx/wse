//! The cluster mTLS configuration.

use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio_rustls::{TlsAcceptor, TlsConnector};

// ---------------------------------------------------------------------------
// TLS configuration
// ---------------------------------------------------------------------------

/// Pre-built TLS configs for both server (accept) and client (connect) roles.
/// A single node cert+key is used for both roles (like CockroachDB).
#[derive(Clone)]
pub(crate) struct ClusterTlsConfig {
    pub(crate) acceptor: TlsAcceptor,
    pub(crate) connector: TlsConnector,
}

/// Build cluster TLS config from PEM file paths.
/// Requires: node cert, node private key (unencrypted), CA cert.
pub(crate) fn build_cluster_tls(
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
) -> Result<ClusterTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| format!("Failed to open cert file '{}': {}", cert_path, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse certs from '{}': {}", cert_path, e))?;
    if certs.is_empty() {
        return Err(format!("No certificates found in '{}'", cert_path).into());
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("Failed to load private key '{}': {}", key_path, e))?;

    let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(ca_path)
        .map_err(|e| format!("Failed to open CA file '{}': {}", ca_path, e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to parse CA certs from '{}': {}", ca_path, e))?;
    if ca_certs.is_empty() {
        return Err(format!("No CA certificates found in '{}'", ca_path).into());
    }

    let mut root_store = RootCertStore::empty();
    for cert in &ca_certs {
        root_store.add(cert.clone())?;
    }

    // Server config: require client certs (mTLS)
    let client_verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(root_store.clone()),
        crate::crypto::tls_provider(),
    )
    .build()
    .map_err(|e| format!("Failed to build client verifier: {}", e))?;
    // The provider comes from the crate's crypto library, explicitly: nothing
    // is installed as the process-wide rustls default.
    let provider = crate::crypto::tls_provider();
    let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs.clone(), key.clone_key())
        .map_err(|e| format!("Failed to build server TLS config: {}", e))?;

    // Client config: present our cert, verify server against CA
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .with_root_certificates(root_store)
        .with_client_auth_cert(certs, key)
        .map_err(|e| format!("Failed to build client TLS config: {}", e))?;

    Ok(ClusterTlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(server_config)),
        connector: TlsConnector::from(Arc::new(client_config)),
    })
}
