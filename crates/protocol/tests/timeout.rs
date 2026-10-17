//! admission-control D7 end to end: pgwire `statement_timeout` and Trino
//! `query_max_execution_time` / `query_max_run_time`.
//!
//! A stub distributed executor sleeps "forever" and records when its future
//! is dropped, standing in for a long distributed query: a timeout must drop
//! it (which is what cancels the query on the workers).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::error::ArnebError;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::ExecutionContext;
use arneb_planner::LogicalPlan;
use arneb_protocol::{
    DistributedExecutor, ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer,
};
use arneb_scheduler::AdmissionController;

/// Sleeps for 60 s; sets `dropped` when its future is dropped early.
#[derive(Default)]
struct Slow {
    dropped: Arc<AtomicBool>,
}

struct SetOnDrop(Arc<AtomicBool>);
impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl DistributedExecutor for Slow {
    async fn execute(
        &self,
        _plan: LogicalPlan,
        _ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ArnebError> {
        let _guard = SetOnDrop(Arc::clone(&self.dropped));
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(Vec::new())
    }

    fn has_workers(&self) -> bool {
        true
    }
}

fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    (
        Arc::new(CatalogManager::new("memory", "default")),
        Arc::new(ConnectorRegistry::new()),
    )
}

async fn pg(
    executor: Option<Arc<Slow>>,
    default: Option<Duration>,
    admission: Option<AdmissionController>,
) -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let mut server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_statement_timeout(default)
    .with_admission(admission);
    if let Some(e) = executor {
        server = server.with_distributed_executor(e);
    }
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=t"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
}

async fn show(client: &tokio_postgres::Client) -> String {
    client
        .simple_query("SHOW statement_timeout")
        .await
        .unwrap()
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn pgwire_set_show_reset() {
    let client = pg(None, None, None).await;
    assert_eq!(show(&client).await, "0");
    client
        .simple_query("SET statement_timeout = '5s'")
        .await
        .unwrap();
    assert_eq!(show(&client).await, "5s");
    client
        .simple_query("SET statement_timeout TO 1500")
        .await
        .unwrap();
    assert_eq!(show(&client).await, "1500ms");
    client
        .simple_query("RESET statement_timeout")
        .await
        .unwrap();
    assert_eq!(show(&client).await, "0");
    let err = client
        .simple_query("SET statement_timeout = 'soon'")
        .await
        .unwrap_err();
    assert_eq!(err.as_db_error().unwrap().code().code(), "22023");

    // The server default shows until the session overrides it.
    let client = pg(None, Some(Duration::from_secs(30)), None).await;
    assert_eq!(show(&client).await, "30s");
    client
        .simple_query("SET statement_timeout = 0")
        .await
        .unwrap();
    assert_eq!(show(&client).await, "0");
    client.simple_query("RESET ALL").await.unwrap();
    assert_eq!(show(&client).await, "30s");
}

#[tokio::test]
async fn pgwire_timeout_cancels_and_the_connection_survives() {
    let slow = Arc::new(Slow::default());
    let dropped = Arc::clone(&slow.dropped);
    let client = pg(Some(slow), None, None).await;
    client
        .simple_query("SET statement_timeout = '200ms'")
        .await
        .unwrap();
    let err = client.simple_query("SELECT 1").await.unwrap_err();
    let db = err.as_db_error().expect("server error");
    assert_eq!(db.code().code(), "57014", "{}", db.message());
    assert!(
        db.message().contains("statement timeout"),
        "{}",
        db.message()
    );
    assert!(
        dropped.load(Ordering::SeqCst),
        "the query future was dropped"
    );
    // Same connection, still usable.
    assert_eq!(show(&client).await, "200ms");
}

#[tokio::test]
async fn pgwire_server_default_and_queue_time_count() {
    // No session SET: the server default applies, and it covers the queue
    // wait (PostgreSQL semantics).
    let admission = AdmissionController::new(1, 10);
    let held = admission.admit("test").unwrap().wait().await;
    let client = pg(
        None,
        Some(Duration::from_millis(200)),
        Some(admission.clone()),
    )
    .await;
    let err = client.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(err.as_db_error().unwrap().code().code(), "57014");
    // The timed-out waiter left the queue.
    assert_eq!(admission.snapshot().queued, 0);
    drop(held);
}

async fn trino(executor: Option<Arc<Slow>>, admission: Option<AdmissionController>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let mut server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            max_wait: Duration::from_millis(50),
            ..TrinoConfig::default()
        },
        cm,
        cr,
    )
    .with_admission(admission);
    if let Some(e) = executor {
        server = server.with_distributed_executor(e);
    }
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Submit with session properties and follow nextUri to the end.
async fn run(base: &str, session: &str) -> Value {
    let http = reqwest::Client::new();
    let mut req = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "t")
        .body("SELECT 1");
    if !session.is_empty() {
        req = req.header("X-Trino-Session", session);
    }
    let mut doc: Value = req.send().await.unwrap().json().await.unwrap();
    while let Some(next) = doc.get("nextUri").and_then(Value::as_str) {
        doc = http.get(next).send().await.unwrap().json().await.unwrap();
    }
    doc
}

#[tokio::test]
async fn trino_execution_time_limit_cancels() {
    let slow = Arc::new(Slow::default());
    let dropped = Arc::clone(&slow.dropped);
    let base = trino(Some(slow), None).await;
    let doc = run(&base, "query_max_execution_time=200ms").await;
    assert_eq!(doc["stats"]["state"], "FAILED", "{doc}");
    assert_eq!(doc["error"]["errorName"], "EXCEEDED_TIME_LIMIT", "{doc}");
    assert_eq!(doc["error"]["errorType"], "INSUFFICIENT_RESOURCES", "{doc}");
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn trino_execution_time_excludes_the_queue_but_run_time_does_not() {
    let admission = AdmissionController::new(1, 10);
    let base = trino(None, Some(admission.clone())).await;

    // Queued for 400 ms under a 200 ms execution limit: still finishes.
    let held = admission.admit("test").unwrap().wait().await;
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        drop(held);
    });
    let doc = run(&base, "query_max_execution_time=200ms").await;
    assert_eq!(doc["stats"]["state"], "FINISHED", "{doc}");
    release.await.unwrap();

    // A 200 ms run-time limit counts the queue wait.
    let held = admission.admit("test").unwrap().wait().await;
    let doc = run(&base, "query_max_run_time=200ms").await;
    assert_eq!(doc["error"]["errorName"], "EXCEEDED_TIME_LIMIT", "{doc}");
    assert_eq!(admission.snapshot().queued, 0);
    drop(held);
}

#[tokio::test]
async fn trino_bad_duration_is_invalid_session_property() {
    let base = trino(None, None).await;
    let doc = run(&base, "query_max_execution_time=soon").await;
    assert_eq!(
        doc["error"]["errorName"], "INVALID_SESSION_PROPERTY",
        "{doc}"
    );
}
