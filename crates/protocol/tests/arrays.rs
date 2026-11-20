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

/// Text of the first column of the first data row of a simple query.
async fn first_text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    for m in client.simple_query(sql).await.unwrap() {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            return r.get(0).map(str::to_string);
        }
    }
    panic!("no rows for {sql}");
}

#[tokio::test]
async fn array_literal_text() {
    let client = connect(pg_server().await).await;
    assert_eq!(
        first_text(&client, "SELECT ARRAY[1,2,3]").await.unwrap(),
        "{1,2,3}"
    );
    let stmt = client.prepare("SELECT ARRAY[1,2,3]").await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1016);
}

#[tokio::test]
async fn array_literal_common_supertype() {
    let client = connect(pg_server().await).await;
    // Arneb types `2.5` as DECIMAL(2,1), so the common supertype of (1, 2.5)
    // is DECIMAL(..,1) and the integer element renders with the decimal
    // scale ("1.0"), where PostgreSQL's numeric prints `{1,2.5}`.
    assert_eq!(
        first_text(&client, "SELECT ARRAY[1, 2.5]").await.unwrap(),
        "{1.0,2.5}"
    );
}

#[tokio::test]
async fn empty_array_without_cast_is_refused() {
    let client = connect(pg_server().await).await;
    client
        .simple_query("SELECT ARRAY[]")
        .await
        .expect_err("ARRAY[] without a cast has no element type");
}

#[tokio::test]
async fn array_text_encoding_round_trips() {
    let client = connect(pg_server().await).await;
    // PostgreSQL's array_out double-quotes an element that is empty, equals
    // NULL (any case), or contains `,` `"` `\` `{` `}` or whitespace, and
    // backslash-escapes only `"` and `\` inside the quotes. Braces are
    // quoted, not escaped. (Derived from the documented rules: no PostgreSQL
    // was reachable on 5433 when this was written.)
    let got = first_text(
        &client,
        r#"SELECT ARRAY['', 'a,b', 'q"x', 'b\s', '{c}', 'sp ace', 'NULL', 'plain', NULL]"#,
    )
    .await
    .unwrap();
    assert_eq!(
        got,
        r#"{"","a,b","q\"x","b\\s","{c}","sp ace","NULL",plain,NULL}"#
    );
}

#[tokio::test]
async fn array_of_columns_is_evaluated_per_row() {
    let client = connect(pg_server().await).await;
    let mut got = Vec::new();
    for m in client
        .simple_query("SELECT ARRAY[x, x + 1] FROM t ORDER BY x")
        .await
        .unwrap()
    {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            got.push(r.get(0).unwrap().to_string());
        }
    }
    assert_eq!(got, ["{1,2}", "{2,3}", "{3,4}"]);
}

#[tokio::test]
async fn array_null_and_text_types() {
    let client = connect(pg_server().await).await;
    // `ARRAY[NULL]` is text[] (1009 would be PostgreSQL's; Arneb's text is
    // varchar, so varchar[] 1015).
    assert_eq!(
        first_text(&client, "SELECT ARRAY[NULL, 'a']")
            .await
            .unwrap(),
        "{NULL,a}"
    );
    let stmt = client.prepare("SELECT ARRAY['a', 'b']").await.unwrap();
    assert_eq!(stmt.columns()[0].type_().oid(), 1015);
}
