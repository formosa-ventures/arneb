//! admission-control D5 end to end, on both listeners. The test holds the
//! only slot itself, so the server's statement must queue (deterministic, no
//! timing), then releases it and the statement completes.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer};
use arneb_scheduler::AdmissionController;

async fn wait_for(cond: impl Fn() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

async fn pg_server(admission: &AdmissionController) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_admission(Some(admission.clone()));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn pg_client(port: u16) -> tokio_postgres::Client {
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=t"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn pgwire_queues_rejects_and_bypasses() {
    let admission = AdmissionController::new(1, 1);
    let port = pg_server(&admission).await;
    let held = admission.admit("test").unwrap().wait().await;

    // Queued behind the held slot.
    let waiting = pg_client(port).await;
    let queued = tokio::spawn(async move { waiting.simple_query("SELECT 1").await });
    wait_for(|| admission.snapshot().queued == 1).await;
    assert!(!queued.is_finished());

    // Queue full: rejected at once with 53000.
    let other = pg_client(port).await;
    let err = other.simple_query("SELECT 2").await.unwrap_err();
    let db = err.as_db_error().expect("server error");
    assert_eq!(db.code().code(), "53000", "{}", db.message());
    assert!(db.message().contains("queue is full"), "{}", db.message());

    // Session and metadata statements bypass admission.
    other
        .simple_query("SET search_path TO default")
        .await
        .unwrap();
    other
        .simple_query("SELECT typname FROM pg_catalog.pg_type LIMIT 1")
        .await
        .unwrap();

    drop(held);
    let rows = tokio::time::timeout(Duration::from_secs(5), queued)
        .await
        .expect("queued statement finished")
        .unwrap()
        .unwrap();
    assert!(!rows.is_empty());
    assert_eq!(admission.snapshot().running, 0);
}

async fn trino_server(admission: &AdmissionController) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            max_wait: Duration::from_millis(50),
            ..TrinoConfig::default()
        },
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
    .with_admission(Some(admission.clone()));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    format!("http://127.0.0.1:{port}")
}

async fn submit(http: &reqwest::Client, base: &str, sql: &str) -> Value {
    http.post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "t")
        .body(sql.to_owned())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn next(http: &reqwest::Client, doc: &Value) -> Value {
    let uri = doc["nextUri"].as_str().expect("nextUri");
    http.get(uri).send().await.unwrap().json().await.unwrap()
}

#[tokio::test]
async fn trino_reports_queued_then_runs_and_rejects_when_full() {
    let admission = AdmissionController::new(1, 1);
    let base = trino_server(&admission).await;
    let http = reqwest::Client::new();
    let held = admission.admit("test").unwrap().wait().await;

    let mut doc = submit(&http, &base, "SELECT 1").await;
    wait_for(|| admission.snapshot().queued == 1).await;
    doc = next(&http, &doc).await;
    assert_eq!(doc["stats"]["state"], "QUEUED", "{doc}");
    assert_eq!(doc["stats"]["queued"], true);

    // A second query over the full queue fails with QUERY_QUEUE_FULL.
    let mut full = submit(&http, &base, "SELECT 2").await;
    while full.get("nextUri").is_some() {
        full = next(&http, &full).await;
    }
    assert_eq!(full["stats"]["state"], "FAILED", "{full}");
    assert_eq!(full["error"]["errorName"], "QUERY_QUEUE_FULL", "{full}");
    assert_eq!(
        full["error"]["errorType"], "INSUFFICIENT_RESOURCES",
        "{full}"
    );

    drop(held);
    while doc.get("nextUri").is_some() {
        doc = next(&http, &doc).await;
    }
    assert_eq!(doc["stats"]["state"], "FINISHED", "{doc}");
    wait_for(|| admission.snapshot().running == 0).await;
}

#[tokio::test]
async fn unlimited_admission_never_queues() {
    let admission = AdmissionController::new(0, 0);
    let port = pg_server(&admission).await;
    let client = pg_client(port).await;
    for _ in 0..5 {
        client.simple_query("SELECT 1").await.unwrap();
    }
    assert_eq!(admission.snapshot().queued, 0);
}

async fn finish(http: &reqwest::Client, mut doc: Value) -> Value {
    while doc.get("nextUri").is_some() {
        doc = next(http, &doc).await;
    }
    doc
}

#[tokio::test]
async fn trino_execute_is_admitted_like_the_statement_it_runs() {
    let admission = AdmissionController::new(1, 2);
    let base = trino_server(&admission).await;
    let http = reqwest::Client::new();
    let held = admission.admit("test").unwrap().wait().await;

    // `EXECUTE IMMEDIATE` and `EXECUTE` of a prepared SELECT both queue.
    let immediate = submit(&http, &base, "EXECUTE IMMEDIATE 'SELECT 1'").await;
    wait_for(|| admission.snapshot().queued == 1).await;
    let prepared: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "t")
        .header("X-Trino-Prepared-Statement", "q1=SELECT%20%3F%20%2B%201")
        .body("EXECUTE q1 USING 41")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    wait_for(|| admission.snapshot().queued == 2).await;

    // Control statements, including an unknown prepared name, never queue.
    for sql in [
        "SHOW CATALOGS",
        "PREPARE q2 FROM SELECT 1",
        "EXECUTE missing",
    ] {
        let doc = finish(&http, submit(&http, &base, sql).await).await;
        assert!(
            doc["stats"]["state"] == "FINISHED" || doc["stats"]["state"] == "FAILED",
            "{sql}: {doc}"
        );
        assert_ne!(
            doc["error"]["errorName"], "QUERY_QUEUE_FULL",
            "{sql}: {doc}"
        );
    }
    assert_eq!(admission.snapshot().queued, 2);

    drop(held);
    let immediate = finish(&http, immediate).await;
    assert_eq!(immediate["stats"]["state"], "FINISHED", "{immediate}");
    let prepared = finish(&http, prepared).await;
    assert_eq!(prepared["stats"]["state"], "FINISHED", "{prepared}");
    assert_eq!(prepared["data"][0][0], 42, "{prepared}");
    wait_for(|| admission.snapshot().running == 0).await;
}
