//! A wss:// door in front of a plain WSE server, with a certificate
//! authority of its own. The server's certificate names localhost,
//! 127.0.0.1 and 10.0.2.2 (this machine as Android's emulator sees it); each
//! connection is passed on to the plain server as it is.

use std::net::SocketAddr;
use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

/// Opens the door on `listener` in front of `plain`; returns the CA's
/// certificate (PEM) a client trusts.
pub fn open(listener: TcpListener, plain: SocketAddr) -> String {
    let mut ca = CertificateParams::default();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca.distinguished_name
        .push(DnType::CommonName, "WSE test CA");
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().expect("a CA key"))
        .expect("the CA's certificate");
    let key = KeyPair::generate().expect("the server's key");
    let mut server = CertificateParams::new(vec!["localhost".to_owned()]).expect("a name");
    server
        .subject_alt_names
        .push(SanType::IpAddress([127, 0, 0, 1].into()));
    server
        .subject_alt_names
        .push(SanType::IpAddress([10, 0, 2, 2].into()));
    server
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let certificate = server
        .signed_by(&key, &*ca)
        .expect("the server's certificate");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the default versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone(), ca.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key.serialize_der())),
        )
        .expect("the server's certificate and key");
    let acceptor = TlsAcceptor::from(Arc::new(config));
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(socket).await else {
                    return;
                };
                let Ok(mut inner) = TcpStream::connect(plain).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut tls, &mut inner).await;
            });
        }
    });
    ca.pem()
}
