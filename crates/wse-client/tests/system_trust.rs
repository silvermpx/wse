//! The system's verifier connects where the system trusts the server's CA.
//! On Linux the system's trust is the CA bundle OpenSSL and the system tools
//! read, which SSL_CERT_FILE names; the test names its own CA there before
//! the verifier (made once for the process) reads it. That the verifier turns
//! down a CA the system does not know, the `native` tests show. One test in
//! this binary: nothing else reads the environment while it is set.

#![cfg(all(feature = "native", target_os = "linux"))]

use std::time::Duration;

use tokio::net::TcpListener;
use wse_client::native::{CancelToken, NativeClient, NativeConfig, NativeTrust};

#[path = "common/tls.rs"]
mod tls;

const SECRET: &str = "a-test-secret-of-at-least-thirty-two-bytes";

struct App;

impl wse::Handler for App {
    type Connection = ();

    fn connect(&self, _connect: &wse::Connect<'_>) -> Option<()> {
        Some(())
    }

    fn message(&self, _conn: &mut (), _message: wse::Inbound, _size: usize) -> bool {
        true
    }

    fn signed(&self, _conn: &mut (), _message: serde_json::Value, _size: usize) -> bool {
        true
    }

    fn disconnect(&self, _conn: ()) {}
}

#[tokio::test]
async fn the_systems_verifier_connects_where_the_system_trusts_the_ca() {
    let mut settings = wse::Config::default();
    settings.jwt = Some(wse::JwtSettings::new(SECRET));
    let server = wse::Server::new(settings).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plain = listener.local_addr().unwrap();
    server.start(listener, App).unwrap();
    let door = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = door.local_addr().unwrap().port();
    let ca = tls::open(door, plain);
    let token = server
        .publisher()
        .issue_token("ann", Duration::from_secs(600), serde_json::Map::new())
        .unwrap();
    let config = NativeConfig {
        url: format!("wss://localhost:{port}/wse"),
        token: Some(token),
        trust: NativeTrust::Platform,
        ..NativeConfig::default()
    };
    let bundle = std::env::temp_dir().join(format!("wse-system-trust-{}.pem", std::process::id()));
    std::fs::write(&bundle, &ca).unwrap();
    // SAFETY: the one test of this binary; no other thread reads the
    // environment meanwhile.
    unsafe {
        std::env::set_var("SSL_CERT_FILE", &bundle);
        std::env::remove_var("SSL_CERT_DIR");
    }
    let connected = NativeClient::connect(config, None, &CancelToken::new()).await;
    let _ = std::fs::remove_file(&bundle);
    let client = connected.expect("the CA the system trusts");
    assert_eq!(client.user_id().as_deref(), Some("ann"));
    client.disconnect(1_000).await;
    server.stop().await;
}
