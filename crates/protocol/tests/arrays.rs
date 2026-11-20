//! Array support and aggregate call modifiers over pgwire (G6 stage 2b-1).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// `file.default.t(x BIGINT)` holding the rows 1, 2, 3.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let t = Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: "x".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        vec![batch],
    ));
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("t", t);
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", default_schema);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn pg_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=smoke dbname=file"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
}

#[tokio::test]
async fn order_by_in_other_aggregate_is_refused() {
    let client = connect(pg_server().await).await;
    let err = client
        .simple_query("SELECT sum(x ORDER BY x) FROM t")
        .await
        .expect_err("ORDER BY inside sum() must be refused");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(message.contains("ORDER BY inside"), "{message}");
}
