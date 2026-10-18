//! End-to-end tests for HTTPS on the Trino listener (`[tls]`).

mod support;

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ServerTls, TrinoConfig, TrinoServer};

use support::TestPki;

async fn start_https(pki: &TestPki) -> u16 {
    start_https_with(pki, TrinoConfig::default()).await
}

async fn start_https_with(pki: &TestPki, config: TrinoConfig) -> u16 {
    let tls = ServerTls::load(&pki.cert_path, &pki.key_path).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            ..config
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_tls(Some(tls.trino_acceptor()));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

fn client(pki: &TestPki) -> reqwest::Client {
    reqwest::Client::builder()
        .tls_backend_preconfigured(pki.client_config())
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// POST `SELECT 1` and follow `nextUri` to completion; returns the rows.
async fn select_one(client: &reqwest::Client, port: u16) -> Vec<Value> {
    let mut doc: Value = client
        .post(format!("https://localhost:{port}/v1/statement"))
        .header("X-Trino-User", "tester")
        .body("SELECT 1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut rows = Vec::new();
    loop {
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().cloned());
        }
        let Some(next) = doc.get("nextUri").and_then(Value::as_str) else {
            break;
        };
        assert!(next.starts_with("https://"), "nextUri {next}");
        doc = client.get(next).send().await.unwrap().json().await.unwrap();
    }
    assert_eq!(doc["stats"]["state"], "FINISHED", "{doc}");
    rows
}

#[tokio::test]
async fn https_select_pages_to_completion() {
    let pki = TestPki::new();
    let port = start_https(&pki).await;
    let rows = select_one(&client(&pki), port).await;
    assert_eq!(rows, vec![serde_json::json!([1])]);
}

/// HTTP/2 has no `Host` header; `nextUri` must still point at the address
/// the client used (here `127.0.0.1`, not the `localhost` fallback).
#[tokio::test]
async fn next_uri_uses_the_h2_authority() {
    let pki = TestPki::new();
    let port = start_https(&pki).await;
    let mut config = pki.client_config();
    config.alpn_protocols = vec![b"h2".to_vec()];
    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(config)
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let resp = client
        .post(format!("https://127.0.0.1:{port}/v1/statement"))
        .header("X-Trino-User", "tester")
        .body("SELECT 1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    let doc: Value = resp.json().await.unwrap();
    let next = doc["nextUri"].as_str().unwrap();
    assert!(
        next.starts_with(&format!("https://127.0.0.1:{port}/")),
        "nextUri {next}"
    );
}

#[tokio::test]
async fn plain_http_fails_without_killing_the_listener() {
    let pki = TestPki::new();
    let port = start_https(&pki).await;

    // reqwest over plain HTTP to the TLS port fails.
    let plain = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    assert!(plain
        .get(format!("http://127.0.0.1:{port}/v1/info"))
        .send()
        .await
        .is_err());

    // A raw socket sending HTTP gets the connection closed.
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(b"GET /v1/info HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut buf)).await;
    assert!(
        !buf.starts_with(b"HTTP/"),
        "got an HTTP response over plaintext"
    );

    // The listener still serves TLS clients.
    let rows = select_one(&client(&pki), port).await;
    assert_eq!(rows.len(), 1);
}

/// A client that opens a TLS connection and then trickles (or never
/// finishes) its request headers is disconnected after
/// `header_read_timeout`.
#[tokio::test]
async fn slow_request_headers_time_out() {
    let pki = TestPki::new();
    let port = start_https_with(
        &pki,
        TrinoConfig {
            header_read_timeout: Duration::from_millis(300),
            ..TrinoConfig::default()
        },
    )
    .await;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(pki.client_config()));
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(name, tcp).await.unwrap();
    tls.write_all(b"GET /v1/info HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let mut buf = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(10), tls.read_to_end(&mut buf)).await;
    assert!(closed.is_ok(), "connection still open after 10s");
    assert!(started.elapsed() < Duration::from_secs(5));

    // The listener still serves well-behaved clients.
    let rows = select_one(&client(&pki), port).await;
    assert_eq!(rows.len(), 1);
}
