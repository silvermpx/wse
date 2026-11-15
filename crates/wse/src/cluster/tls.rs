//! The cluster's TLS: mutual TLS from certificate files, or TLS from a
//! shared secret.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{WebPkiSupportedAlgorithms, verify_tls13_signature_with_raw_key};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, SubjectPublicKeyInfoDer,
    UnixTime,
};
use rustls::server::WebPkiClientVerifier;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use zeroize::Zeroizing;

// ---------------------------------------------------------------------------
// TLS configuration
// ---------------------------------------------------------------------------

/// Pre-built TLS configs for both server (accept) and client (connect) roles.
/// A single node cert+key is used for both roles (like CockroachDB).
#[derive(Clone)]
pub(crate) struct ClusterTlsConfig {
    pub(crate) acceptor: TlsAcceptor,
    pub(crate) connector: TlsConnector,
    /// Made from the cluster's secret, not from certificate files.
    pub(crate) from_secret: bool,
}

/// The least a cluster secret holds: from one recorded handshake, a key
/// made of fewer random bytes could be guessed offline.
pub(crate) const MIN_SECRET_LEN: usize = 32;

/// A P-256 SubjectPublicKeyInfo up to its 65-byte point.
const P256_SPKI_PREFIX: [u8; 26] = [
    0x30, 0x59, 0x30, 0x13, 0x06, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01, 0x06, 0x08, 0x2a,
    0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07, 0x03, 0x42, 0x00,
];

/// The cluster's key, the same on every node that holds the secret: a P-256
/// key from HKDF-SHA256 over it. Its PKCS#8 and its SubjectPublicKeyInfo.
fn cluster_key(secret: &[u8]) -> Result<(Zeroizing<Vec<u8>>, Vec<u8>), String> {
    use aws_lc_rs::{agreement, signature};
    for counter in 0..=u8::MAX {
        let mut scalar = Zeroizing::new([0u8; 32]);
        crate::protocol::crypto::hkdf_sha256(
            b"WSE cluster",
            secret,
            &[b"WSE cluster key ".as_slice(), &[counter]].concat(),
            scalar.as_mut(),
        )
        .map_err(|_| "HKDF failed".to_owned())?;
        // An output past the curve's order, about once in 2^32, takes the
        // next counter.
        let Ok(private) = agreement::PrivateKey::from_private_key(&agreement::ECDH_P256, &*scalar)
        else {
            continue;
        };
        let public = private
            .compute_public_key()
            .map_err(|_| "the cluster key's public half".to_owned())?;
        let pair = signature::EcdsaKeyPair::from_private_key_and_public_key(
            &signature::ECDSA_P256_SHA256_ASN1_SIGNING,
            &*scalar,
            public.as_ref(),
        )
        .map_err(|e| format!("the cluster key: {e}"))?;
        let pkcs8 = pair
            .to_pkcs8v1()
            .map_err(|_| "the cluster key's PKCS#8".to_owned())?;
        let mut spki = P256_SPKI_PREFIX.to_vec();
        spki.extend_from_slice(public.as_ref());
        return Ok((Zeroizing::new(pkcs8.as_ref().to_vec()), spki));
    }
    Err("no P-256 key from the secret".to_owned())
}

/// Accepts a peer that shows the cluster's key, and nothing else: a raw
/// public key (RFC 7250) equal to it, and a handshake signature it makes.
#[derive(Debug)]
struct ClusterKey {
    spki: Vec<u8>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ClusterKey {
    fn check(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> Result<(), rustls::Error> {
        if intermediates.is_empty() && end_entity.as_ref() == self.spki.as_slice() {
            Ok(())
        } else {
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn signature(
        &self,
        message: &[u8],
        key: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature_with_raw_key(
            message,
            &SubjectPublicKeyInfoDer::from(key.as_ref()),
            dss,
            &self.algorithms,
        )
    }
}

/// TLS 1.3 only: the raw public keys need it.
fn no_tls12() -> rustls::Error {
    rustls::Error::General("cluster links speak TLS 1.3 only".to_owned())
}

impl ServerCertVerifier for ClusterKey {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.check(end_entity, intermediates)
            .map(|()| ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(no_tls12())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

impl ClientCertVerifier for ClusterKey {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.check(end_entity, intermediates)
            .map(|()| ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(no_tls12())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// The cluster's TLS from a shared secret, without certificate files: every
/// node derives the same key from the secret and proves it, both ways, in a
/// TLS 1.3 handshake with raw public keys. A node without the secret cannot
/// link, and every link is encrypted.
pub(crate) fn build_cluster_tls_from_secret(
    secret: &[u8],
) -> Result<ClusterTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    if secret.len() < MIN_SECRET_LEN {
        return Err(format!(
            "the cluster secret holds {} bytes; it needs at least {MIN_SECRET_LEN} random ones \
             (openssl rand -base64 32)",
            secret.len()
        )
        .into());
    }
    let (pkcs8, spki) = cluster_key(secret)?;
    let provider = crate::crypto::tls_provider();
    let signing_key = provider
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            pkcs8.as_slice().to_vec(),
        )))
        .map_err(|e| format!("the cluster key: {e}"))?;
    let key = Arc::new(CertifiedKey::new(
        vec![CertificateDer::from(spki.clone())],
        signing_key,
    ));
    let verifier = Arc::new(ClusterKey {
        spki,
        algorithms: provider.signature_verification_algorithms,
    });

    let mut server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .with_client_cert_verifier(verifier.clone())
        .with_cert_resolver(Arc::new(
            rustls::server::AlwaysResolvesServerRawPublicKeys::new(Arc::clone(&key)),
        ));
    // Every link proves the key: no session is resumed.
    server_config.send_tls13_tickets = 0;

    let mut client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("TLS protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(
            rustls::client::AlwaysResolvesClientRawPublicKeys::new(key),
        ));
    client_config.resumption = rustls::client::Resumption::disabled();

    Ok(ClusterTlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(server_config)),
        connector: TlsConnector::from(Arc::new(client_config)),
        from_secret: true,
    })
}

/// Build cluster TLS config from PEM file paths.
/// Requires: node cert, node private key (unencrypted), CA cert.
pub(crate) fn build_cluster_tls(
    cert_path: impl AsRef<std::path::Path>,
    key_path: impl AsRef<std::path::Path>,
    ca_path: impl AsRef<std::path::Path>,
) -> Result<ClusterTlsConfig, Box<dyn std::error::Error + Send + Sync>> {
    let (cert_path, key_path, ca_path) = (cert_path.as_ref(), key_path.as_ref(), ca_path.as_ref());
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
        .map_err(|e| format!("Failed to open cert file '{}': {}", cert_path.display(), e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            format!(
                "Failed to parse certs from '{}': {}",
                cert_path.display(),
                e
            )
        })?;
    if certs.is_empty() {
        return Err(format!("No certificates found in '{}'", cert_path.display()).into());
    }

    let key = PrivateKeyDer::from_pem_file(key_path)
        .map_err(|e| format!("Failed to load private key '{}': {}", key_path.display(), e))?;

    let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(ca_path)
        .map_err(|e| format!("Failed to open CA file '{}': {}", ca_path.display(), e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            format!(
                "Failed to parse CA certs from '{}': {}",
                ca_path.display(),
                e
            )
        })?;
    if ca_certs.is_empty() {
        return Err(format!("No CA certificates found in '{}'", ca_path.display()).into());
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
        from_secret: false,
    })
}
