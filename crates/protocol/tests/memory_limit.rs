//! admission-control D1/D3 end to end: a statement over its per-query memory
//! limit fails with pgwire `53200` / Trino `EXCEEDED_LOCAL_MEMORY_LIMIT`,
//! and the next statement on the same server runs normally.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use arneb_protocol::{ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer};

/// `memory.default.t(id)` with 10 000 distinct ids.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from((0..10_000).collect::<Vec<_>>()))],
    )
    .unwrap();
    let table = Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
        }],
        vec![batch],
    ));
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let cm = Arc::new(CatalogManager::new("memory", "default"));
    cm.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (cm, Arc::new(registry))
}

const GROUP_BY: &str = "SELECT id, count(*) FROM t GROUP BY id";
/// Far below what a 10 000-group hash aggregate reserves.
const TINY_LIMIT: usize = 1024;

fn node_pool() -> Arc<dyn MemoryPool> {
    Arc::new(GreedyMemoryPool::new(1 << 30))
}

#[tokio::test]
async fn pgwire_over_limit_is_53200_and_the_server_recovers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let pool = node_pool();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_memory_pool(Arc::clone(&pool))
    .with_query_memory_limit(Some(TINY_LIMIT));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=tester"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);

    let err = client.simple_query(GROUP_BY).await.unwrap_err();
    let db = err.as_db_error().expect("server error");
    assert_eq!(db.code(), &SqlState::OUT_OF_MEMORY, "{}", db.message());
    assert!(db.message().contains("memory limit"), "{}", db.message());

    // The failed statement released its reservations; the connection and
    // a statement that needs no reservation still work.
    assert_eq!(pool.reserved(), 0);
    let rows = client.simple_query("SELECT 1").await.unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn trino_over_limit_is_exceeded_local_memory_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            ..TrinoConfig::default()
        },
        cm,
        cr,
    )
    .with_memory_pool(node_pool())
    .with_query_memory_limit(Some(TINY_LIMIT));
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut doc: Value = http
        .post(format!("http://127.0.0.1:{port}/v1/statement"))
        .header("X-Trino-User", "tester")
        .body(GROUP_BY)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    while let Some(next) = doc.get("nextUri").and_then(Value::as_str) {
        doc = http.get(next).send().await.unwrap().json().await.unwrap();
    }
    assert_eq!(doc["stats"]["state"], "FAILED", "{doc}");
    assert_eq!(
        doc["error"]["errorName"], "EXCEEDED_LOCAL_MEMORY_LIMIT",
        "{doc}"
    );
}
