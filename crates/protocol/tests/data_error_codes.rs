//! A bad value (a cast the target cannot hold, an overflow, a division by
//! zero) reaches the client as a data error: pgwire SQLSTATE class 22, as
//! PostgreSQL reports the same failure, and Trino's `StandardErrorCode`, as
//! Trino raises it. Folded at plan time or evaluated at run time, the same
//! failure gets the same code.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer};

/// `memory.default.d`: the second row of every column is a bad value for
/// the casts and arithmetic below.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let columns: Vec<(&str, DataType, ArrayRef)> = vec![
        (
            "big",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![7, 3_000_000_000])),
        ),
        (
            "s",
            DataType::Utf8,
            Arc::new(StringArray::from(vec!["12", "abc"])),
        ),
        (
            "ds",
            DataType::Utf8,
            Arc::new(StringArray::from(vec!["2024-01-01", "2024-13-45"])),
        ),
        (
            "i",
            DataType::Int32,
            Arc::new(Int32Array::from(vec![1, i32::MAX])),
        ),
        ("z", DataType::Int32, Arc::new(Int32Array::from(vec![1, 0]))),
        (
            "f",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![1.5, f64::NAN])),
        ),
    ];
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _, array)| Field::new(*name, array.data_type().clone(), false))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.iter().map(|(_, _, a)| a.clone()).collect(),
    )
    .unwrap();
    let table = MemoryTable::new(
        columns
            .iter()
            .map(|(name, data_type, _)| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.clone(),
                nullable: false,
            })
            .collect(),
        vec![batch],
    );
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("d", Arc::new(table));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let cm = Arc::new(CatalogManager::new("memory", "default"));
    cm.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (cm, Arc::new(registry))
}

/// `(sql, pgwire SQLSTATE, Trino errorCode, Trino errorName)`.
const CASES: &[(&str, &str, i64, &str)] = &[
    // Folded (or checked) at plan time.
    (
        "SELECT CAST('abc' AS INTEGER)",
        "22P02",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT CAST(300 AS TINYINT)",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
    (
        "SELECT CAST(2147483647.5e0 AS INTEGER)",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
    (
        "SELECT CAST('2024-13-45' AS DATE)",
        "22007",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    // Evaluated at run time.
    (
        "SELECT CAST(s AS INTEGER) FROM d",
        "22P02",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT CAST(big AS INTEGER) FROM d",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
    (
        "SELECT CAST(ds AS DATE) FROM d",
        "22007",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT i + i FROM d",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
    ("SELECT i / z FROM d", "22012", 8, "DIVISION_BY_ZERO"),
    // NaN to an integer: PostgreSQL says out of range, Trino invalid cast
    // argument. ±Infinity is out of range in both.
    (
        "SELECT CAST(f AS INTEGER) FROM d",
        "22003",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT CAST(nan() AS BIGINT)",
        "22003",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT CAST(infinity() AS INTEGER)",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
    // A string whose text reads like the NaN error is still a bad string.
    (
        "SELECT CAST('a NaN to b' AS INTEGER)",
        "22P02",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    (
        "SELECT CAST(s || ' NaN to b' AS INTEGER) FROM d",
        "22P02",
        9,
        "INVALID_CAST_ARGUMENT",
    ),
    ("SELECT i % z FROM d", "22012", 8, "DIVISION_BY_ZERO"),
    (
        "SELECT CAST(big AS DECIMAL(5, 0)) FROM d",
        "22003",
        19,
        "NUMERIC_VALUE_OUT_OF_RANGE",
    ),
];

#[tokio::test]
async fn pgwire_reports_data_errors_with_their_sqlstate() {
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
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user=tester"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);

    for (sql, sqlstate, _, _) in CASES {
        let err = client.simple_query(sql).await.unwrap_err();
        let db = err
            .as_db_error()
            .unwrap_or_else(|| panic!("{sql}: no server error: {err}"));
        assert_eq!(db.code().code(), *sqlstate, "{sql}: {}", db.message());
    }

    // TRY still turns the failing rows into NULL, and the connection is fine.
    let rows = client
        .simple_query("SELECT TRY(CAST(s AS INTEGER)), TRY(i / z) FROM d")
        .await
        .unwrap();
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn trino_reports_data_errors_with_their_error_code() {
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
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    for (sql, _, code, name) in CASES {
        let mut doc: Value = http
            .post(format!("http://127.0.0.1:{port}/v1/statement"))
            .header("X-Trino-User", "tester")
            .body(*sql)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        while let Some(next) = doc.get("nextUri").and_then(Value::as_str) {
            doc = http.get(next).send().await.unwrap().json().await.unwrap();
        }
        assert_eq!(doc["stats"]["state"], "FAILED", "{sql}: {doc}");
        assert_eq!(doc["error"]["errorName"], *name, "{sql}: {doc}");
        assert_eq!(doc["error"]["errorCode"], *code, "{sql}: {doc}");
        assert_eq!(doc["error"]["errorType"], "USER_ERROR", "{sql}: {doc}");
    }
}
