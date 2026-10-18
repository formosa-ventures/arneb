//! End-to-end tests for pgwire TLS (`[tls]`).
//!
//! Each test starts a real `ProtocolServer` on an ephemeral port with a
//! throwaway CA and connects with `tokio-postgres` over rustls.

mod support;

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::config::SslMode;
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{
    AuthMethod, PgwireTls, PgwireTlsMode, ProtocolConfig, ProtocolServer, ScramVerifier, ServerTls,
    UserCredentials,
};

use support::TestPki;

async fn start_server(tls: Option<PgwireTls>, auth: AuthMethod) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_auth(auth)
    .with_tls(tls);
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

fn pgwire_tls(pki: &TestPki, mode: PgwireTlsMode) -> PgwireTls {
    let tls = ServerTls::load(&pki.cert_path, &pki.key_path).unwrap();
    PgwireTls {
        acceptor: tls.pgwire_acceptor(),
        mode,
    }
}

fn config(port: u16, ssl: SslMode) -> tokio_postgres::Config {
    let mut c = tokio_postgres::Config::new();
    c.host("localhost").port(port).user("tester").ssl_mode(ssl);
    c
}

async fn select_one<T>(
    config: tokio_postgres::Config,
    tls: T,
) -> Result<String, tokio_postgres::Error>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>,
    T::Stream: Send + 'static,
{
    let (client, conn) = config.connect(tls).await?;
    tokio::spawn(conn);
    let rows = client.simple_query("SELECT 1").await?;
    Ok(rows
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap())
}

/// `sslmode=require` with a client that verifies the chain and the host name
/// against the test CA — the verify-full case.
#[tokio::test]
async fn verify_full_client_connects_in_required_mode() {
    let pki = TestPki::new();
    let port = start_server(
        Some(pgwire_tls(&pki, PgwireTlsMode::Required)),
        AuthMethod::None,
    )
    .await;
    let tls = MakeRustlsConnect::new(pki.client_config());
    assert_eq!(
        select_one(config(port, SslMode::Require), tls)
            .await
            .unwrap(),
        "1"
    );
}

#[tokio::test]
async fn client_trusting_another_ca_is_refused() {
    let pki = TestPki::new();
    let other = TestPki::new();
    let port = start_server(
        Some(pgwire_tls(&pki, PgwireTlsMode::Required)),
        AuthMethod::None,
    )
    .await;
    let tls = MakeRustlsConnect::new(other.client_config());
    assert!(select_one(config(port, SslMode::Require), tls)
        .await
        .is_err());
}

#[tokio::test]
async fn plaintext_rejected_with_28000_in_required_mode() {
    let pki = TestPki::new();
    let port = start_server(
        Some(pgwire_tls(&pki, PgwireTlsMode::Required)),
        AuthMethod::None,
    )
    .await;
    let err = select_one(config(port, SslMode::Disable), NoTls)
        .await
        .unwrap_err();
    let db = err.as_db_error().expect("server error");
    assert_eq!(db.code(), &SqlState::INVALID_AUTHORIZATION_SPECIFICATION);
    assert_eq!(db.severity(), "FATAL");
    assert!(db.message().contains("SSL"), "{}", db.message());
}

#[tokio::test]
async fn optional_mode_accepts_tls_and_plaintext() {
    let pki = TestPki::new();
    let port = start_server(
        Some(pgwire_tls(&pki, PgwireTlsMode::Optional)),
        AuthMethod::None,
    )
    .await;
    assert_eq!(
        select_one(config(port, SslMode::Disable), NoTls)
            .await
            .unwrap(),
        "1"
    );
    let tls = MakeRustlsConnect::new(pki.client_config());
    assert_eq!(
        select_one(config(port, SslMode::Require), tls)
            .await
            .unwrap(),
        "1"
    );
}

#[tokio::test]
async fn tls_with_scram_password() {
    let pki = TestPki::new();
    let auth = AuthMethod::ScramSha256(Arc::new(
        UserCredentials::new(vec![(
            "tester".to_owned(),
            ScramVerifier::generate("s3cret").unwrap(),
        )])
        .unwrap(),
    ));
    let port = start_server(Some(pgwire_tls(&pki, PgwireTlsMode::Required)), auth).await;

    let mut ok = config(port, SslMode::Require);
    ok.password("s3cret");
    let tls = MakeRustlsConnect::new(pki.client_config());
    assert_eq!(select_one(ok, tls).await.unwrap(), "1");

    let mut bad = config(port, SslMode::Require);
    bad.password("wrong");
    let tls = MakeRustlsConnect::new(pki.client_config());
    let err = select_one(bad, tls).await.unwrap_err();
    assert_eq!(
        err.as_db_error().unwrap().code(),
        &SqlState::INVALID_PASSWORD
    );
}

/// Raw `SSLRequest` (length 8, code 80877103); returns the one-byte answer.
async fn ssl_request(port: u16) -> (TcpStream, u8) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        .await
        .unwrap();
    let mut b = [0u8; 1];
    s.read_exact(&mut b).await.unwrap();
    (s, b[0])
}

#[tokio::test]
async fn without_tls_ssl_request_is_answered_n() {
    let port = start_server(None, AuthMethod::None).await;
    assert_eq!(ssl_request(port).await.1, b'N');
    assert_eq!(
        select_one(config(port, SslMode::Prefer), NoTls)
            .await
            .unwrap(),
        "1"
    );
}

#[tokio::test]
async fn failed_handshake_closes_only_that_connection() {
    let pki = TestPki::new();
    let port = start_server(
        Some(pgwire_tls(&pki, PgwireTlsMode::Required)),
        AuthMethod::None,
    )
    .await;
    let (mut s, answer) = ssl_request(port).await;
    assert_eq!(answer, b'S');
    // Not a TLS ClientHello: the handshake fails and the server drops us.
    s.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let mut buf = [0u8; 64];
    let _ = s.read(&mut buf).await;
    drop(s);

    let tls = MakeRustlsConnect::new(pki.client_config());
    assert_eq!(
        select_one(config(port, SslMode::Require), tls)
            .await
            .unwrap(),
        "1"
    );
}
