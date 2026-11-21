//! SQL features Metabase needs over pgwire (G6 stage 2b-2).

use std::sync::Arc;

use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::ColumnInfo;
use arneb_common::types::DataType;
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// A single-column memory table.
fn one_column(name: &str, arrow_type: ArrowDataType, values: ArrayRef) -> Arc<MemoryTable> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        arrow_type.clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
    Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: name.to_string(),
            data_type: DataType::try_from(arrow_type).unwrap(),
            nullable: true,
        }],
        vec![batch],
    ))
}

/// Registers the fixture tables under `file.default`:
/// `words(w VARCHAR)`, `t(x BIGINT)` = 1,2,3, `e(x BIGINT)` empty and
/// `dt(d DATE)` = 2000-03-31, 1996-01-02.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "words",
        one_column(
            "w",
            ArrowDataType::Utf8,
            Arc::new(StringArray::from(vec!["pg_toast", "Public", "default"])),
        ),
    );
    schema.register_table(
        "t",
        one_column(
            "x",
            ArrowDataType::Int64,
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ),
    );
    schema.register_table(
        "e",
        one_column(
            "x",
            ArrowDataType::Int64,
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ),
    );
    // 2000-03-31 = day 11047, 1996-01-02 = day 9497.
    schema.register_table(
        "dt",
        one_column(
            "d",
            ArrowDataType::Date32,
            Arc::new(Date32Array::from(vec![11047, 9497])),
        ),
    );
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", schema);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn connect() -> tokio_postgres::Client {
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
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=smoke dbname=file"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    client
}

/// All data rows of a simple query as optional text cells.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_string)).collect())
            }
            _ => None,
        })
        .collect()
}

fn text(v: &str) -> Vec<Option<String>> {
    vec![Some(v.to_string())]
}

#[tokio::test]
async fn regex_operators() {
    let client = connect().await;
    assert_eq!(
        rows(&client, "SELECT w FROM words WHERE w !~ '^pg_' ORDER BY w").await,
        [text("Public"), text("default")]
    );
    assert_eq!(
        rows(&client, "SELECT count(*) FROM words WHERE w ~* '^public$'").await,
        [text("1")]
    );
    assert_eq!(
        rows(&client, "SELECT NULL ~ 'a', 'a' ~ NULL").await,
        [vec![None, None]]
    );
    let err = client
        .simple_query("SELECT 'ab' ~ '(?<=a)b'")
        .await
        .expect_err("look-behind is unsupported");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(message.contains('~'), "{message}");
}

#[tokio::test]
async fn untyped_literal_numbers() {
    let client = connect().await;
    assert_eq!(
        rows(&client, "SELECT count(*) FROM t WHERE x = '2'").await,
        [text("1")]
    );
    assert_eq!(
        rows(&client, "SELECT NULLIF(x, '1') FROM t ORDER BY x").await,
        [vec![None], text("2"), text("3")]
    );
    assert_eq!(
        rows(&client, "SELECT COALESCE(x, '0') + 1 FROM t ORDER BY x").await,
        [text("2"), text("3"), text("4")]
    );
    let err = client
        .simple_query("SELECT count(*) FROM t WHERE x = 'abc'")
        .await
        .expect_err("non-numeric literal");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(
        message.contains("invalid input syntax for type bigint: \"abc\""),
        "{message}"
    );
}
