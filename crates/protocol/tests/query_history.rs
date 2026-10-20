//! Query history end to end: pgwire and Trino statements are recorded in a
//! shared `QueryTracker` and readable as `system.runtime.queries` through
//! both protocols.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer};
use arneb_scheduler::QueryTracker;

fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    (
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
}

async fn pg(tracker: Arc<QueryTracker>, user: &str) -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_query_tracker(tracker);
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user={user}"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
}

async fn trino(tracker: Arc<QueryTracker>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            max_wait: Duration::from_millis(50),
            ..TrinoConfig::default()
        },
        cm,
        cr,
    )
    .with_query_tracker(tracker);
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Submit `sql` as `user` and follow nextUri; returns every data row.
async fn trino_rows(base: &str, user: &str, sql: &str) -> Vec<Vec<Value>> {
    let http = reqwest::Client::new();
    let mut doc: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", user)
        .body(sql.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut rows = Vec::new();
    loop {
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().map(|r| r.as_array().unwrap().clone()));
        }
        let Some(next) = doc.get("nextUri").and_then(Value::as_str) else {
            break;
        };
        doc = http.get(next).send().await.unwrap().json().await.unwrap();
    }
    rows
}

fn pg_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn statements_from_both_protocols_appear_in_system_runtime_queries() {
    let tracker = Arc::new(QueryTracker::with_history_limit(50));
    let client = pg(Arc::clone(&tracker), "ann").await;
    let base = trino(Arc::clone(&tracker)).await;

    client.simple_query("SELECT 42 AS answer").await.unwrap();
    assert!(client.simple_query("SELECT nope").await.is_err());
    trino_rows(&base, "bob", "SELECT 7").await;

    // Read through pgwire, with a filter and an ordering the engine applies.
    let rows = pg_rows(
        &client
            .simple_query(
                "SELECT source, \"user\", state, rows, error_message \
                 FROM system.runtime.queries \
                 WHERE query IN ('SELECT 42 AS answer', 'SELECT nope', 'SELECT 7') \
                 ORDER BY query",
            )
            .await
            .unwrap(),
    );
    let s = |v: &str| Some(v.to_string());
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows[0][..4], [s("pgwire"), s("ann"), s("FINISHED"), s("1")]);
    assert_eq!(rows[1][..4], [s("trino"), s("bob"), s("FINISHED"), s("1")]);
    assert_eq!(rows[2][..3], [s("pgwire"), s("ann"), s("FAILED")]);
    assert!(
        rows[2][4].as_deref().unwrap_or("").contains("nope"),
        "{rows:?}"
    );

    // A reader sees itself running, through either protocol.
    let me = pg_rows(
        &client
            .simple_query(
                "SELECT state FROM system.runtime.queries \
                 WHERE query LIKE 'SELECT state FROM system.runtime.queries%'",
            )
            .await
            .unwrap(),
    );
    assert_eq!(me, vec![vec![s("RUNNING")]]);

    let running = trino_rows(
        &base,
        "bob",
        "SELECT count(*) FROM system.runtime.queries WHERE state = 'RUNNING'",
    )
    .await;
    assert_eq!(running, vec![vec![Value::from(1)]]);
}
