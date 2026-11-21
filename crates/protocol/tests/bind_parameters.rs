//! End-to-end test that Extended Query parameters bind as values, never as
//! SQL text, even when one parameter's value looks like a placeholder.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// A server with one table `t(v varchar)` holding the single row `secret`.
async fn start_server() -> u16 {
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(StringArray::from(vec!["secret"]))],
    )
    .unwrap();
    let table = Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: "v".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
        }],
        vec![batch],
    ));
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let cm = Arc::new(CatalogManager::new("memory", "default"));
    cm.register_catalog("memory", catalog.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(catalog, "default")),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        Arc::new(registry),
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn connect(port: u16) -> Client {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=test dbname=memory"),
        NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(conn);
    client
}

/// Binding `$2 = '$1'` must not pull `$1`'s value into the statement a
/// second time. If it did, `$1`'s text would land outside its quotes and
/// run as SQL: here `OR 1=1` would return the row.
#[tokio::test]
async fn a_parameter_that_looks_like_a_placeholder_cannot_inject_sql() {
    let port = start_server().await;
    let c = connect(port).await;
    let rows = c
        .query(
            "SELECT v FROM t WHERE v = $1 AND v = $2",
            &[&" OR 1=1 --", &"$1"],
        )
        .await
        .unwrap();
    assert!(rows.is_empty(), "no row matches both literal values");

    // The values bind exactly as sent.
    let rows = c
        .query("SELECT $1 AS a, $2 AS b", &[&"it's $2", &"$1"])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "it's $2");
    assert_eq!(rows[0].get::<_, String>(1), "$1");
}
