//! End-to-end tests for per-connection `SET search_path` over pgwire.
//!
//! Two schemas (`default`, `alt`) each hold a table `t` whose single row
//! names the schema it lives in, so `SELECT v FROM t` reveals which schema
//! an unqualified name resolved to.

use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

fn schema_with_marker(marker: &str) -> Arc<MemorySchema> {
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "v",
        ArrowDataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![Arc::new(StringArray::from(vec![marker]))],
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
    schema
}

async fn start_server() -> u16 {
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema_with_marker("default"));
    catalog.register_schema("alt", schema_with_marker("alt"));
    let cm = Arc::new(CatalogManager::new("memory", "default"));
    cm.register_catalog("memory", catalog.clone());
    let mut registry = ConnectorRegistry::new();
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

/// First column of the first row, via the Simple Query protocol.
async fn simple_value(client: &Client, sql: &str) -> String {
    let msgs = client.simple_query(sql).await.expect(sql);
    msgs.iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or_default().to_string()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no row for {sql}"))
}

/// Which schema `t` resolves to, via the Extended Query protocol.
async fn extended_marker(client: &Client) -> String {
    let rows = client.query("SELECT v FROM t", &[]).await.unwrap();
    rows[0].get(0)
}

#[tokio::test]
async fn set_search_path_switches_unqualified_resolution() {
    let port = start_server().await;
    let c = connect(port).await;
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "default");

    c.simple_query("SET search_path = alt").await.unwrap();
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "alt");
    assert_eq!(simple_value(&c, "SHOW search_path").await, "alt");
    assert_eq!(simple_value(&c, "SELECT current_schema()").await, "alt");

    c.simple_query("SET search_path TO \"default\"")
        .await
        .unwrap();
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "default");

    // Only one current schema: the first entry that exists wins.
    c.simple_query("SET search_path = 'nope', 'alt', 'default'")
        .await
        .unwrap();
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "alt");
    assert_eq!(
        simple_value(&c, "SHOW search_path").await,
        "nope, alt, default"
    );

    // catalog.schema form.
    c.simple_query("SET search_path = memory.alt")
        .await
        .unwrap();
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "alt");

    c.simple_query("RESET search_path").await.unwrap();
    assert_eq!(simple_value(&c, "SELECT v FROM t").await, "default");
    assert_eq!(simple_value(&c, "SHOW search_path").await, "default");
}

#[tokio::test]
async fn search_path_does_not_leak_between_connections() {
    let port = start_server().await;
    let a = connect(port).await;
    let b = connect(port).await;

    a.simple_query("SET search_path = alt").await.unwrap();
    assert_eq!(simple_value(&a, "SELECT v FROM t").await, "alt");
    assert_eq!(simple_value(&b, "SELECT v FROM t").await, "default");
    assert_eq!(simple_value(&b, "SHOW search_path").await, "default");
}

#[tokio::test]
async fn extended_query_protocol_honors_search_path() {
    let port = start_server().await;
    let c = connect(port).await;

    // SET sent through the Extended Query protocol too.
    c.execute("SET search_path TO alt", &[]).await.unwrap();
    assert_eq!(extended_marker(&c).await, "alt");

    let stmt = c.prepare("SELECT v FROM t WHERE v = $1").await.unwrap();
    assert_eq!(stmt.columns()[0].name(), "v");
    let rows = c.query(&stmt, &[&"alt"]).await.unwrap();
    assert_eq!(rows.len(), 1);

    c.simple_query("RESET search_path").await.unwrap();
    assert_eq!(extended_marker(&c).await, "default");
}

#[tokio::test]
async fn unknown_schema_is_accepted_but_resolution_fails() {
    let port = start_server().await;
    let c = connect(port).await;

    // PostgreSQL accepts nonexistent schemas in search_path.
    c.simple_query("SET search_path = nope").await.unwrap();
    assert!(c.simple_query("SELECT v FROM t").await.is_err());
    // Qualified names still work.
    assert_eq!(simple_value(&c, "SELECT v FROM alt.t").await, "alt");
}
