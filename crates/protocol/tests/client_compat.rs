//! What BI and ELT clients send when they connect (client-smoke): psycopg2
//! and SQLAlchemy wrap statements in transactions and parse `version()`,
//! dbt-trino writes `INFORMATION_SCHEMA` in upper case, SQLAlchemy's Trino
//! dialect reads `"system"."metadata"."table_comments"`.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer};

/// `memory.default.nation` with three rows.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n_nationkey",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef],
    )
    .unwrap();
    let nation = Arc::new(MemoryTable::new(
        vec![ColumnInfo {
            name: "n_nationkey".into(),
            data_type: DataType::Int64,
            nullable: false,
        }],
        vec![batch],
    ));
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("nation", nation);
    let memory = Arc::new(MemoryCatalog::new());
    memory.register_schema("default", default_schema);
    let cm = CatalogManager::new("memory", "default");
    cm.register_catalog("memory", memory.clone());
    let mut registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(memory, "default")),
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

fn first_value(messages: &[SimpleQueryMessage]) -> String {
    messages
        .iter()
        .find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or_default().to_owned()),
            _ => None,
        })
        .expect("a row")
}

#[tokio::test]
async fn pgwire_answers_driver_session_queries() {
    let port = pg_server().await;
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=alice dbname=memory"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    for (sql, tag) in [
        ("BEGIN", "BEGIN"),
        ("START TRANSACTION READ ONLY", "START TRANSACTION"),
        ("START TRANSACTION;", "START TRANSACTION"),
        ("COMMIT", "COMMIT"),
        ("ROLLBACK;", "ROLLBACK"),
        ("END", "COMMIT"),
        ("SAVEPOINT \"_pg3_1\"", "SAVEPOINT"),
        ("RELEASE \"_pg3_1\"", "RELEASE"),
    ] {
        let messages = client.simple_query(sql).await.unwrap();
        assert!(
            messages
                .iter()
                .any(|m| matches!(m, SimpleQueryMessage::CommandComplete(_))),
            "{sql} -> {tag}"
        );
    }

    let version = first_value(&client.simple_query("SELECT version()").await.unwrap());
    assert!(version.starts_with("PostgreSQL 14.0 "), "{version}");
    assert!(version.contains("Arneb"), "{version}");
    for (sql, want) in [
        ("SHOW standard_conforming_strings", "on"),
        ("SHOW transaction isolation level", "read committed"),
        ("SHOW server_version_num", "140000"),
        ("SHOW server_version", "14.0"),
        ("SELECT current_user", "alice"),
        ("SELECT count(*) FROM NATION", "3"),
        ("SELECT count(*) FROM Default.Nation", "3"),
        // dbt-trino's catalog query joins with USING.
        (
            "SELECT count(n_nationkey) FROM nation a JOIN nation b USING (\"n_nationkey\")",
            "3",
        ),
        (
            "SELECT count(*) FROM nation a LEFT JOIN nation b USING (n_nationkey) \
             WHERE b.n_nationkey IS NOT NULL",
            "3",
        ),
    ] {
        assert_eq!(
            first_value(&client.simple_query(sql).await.unwrap()),
            want,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn join_using_merges_the_joined_columns() {
    let port = pg_server().await;
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=alice dbname=memory"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);
    let rows = client
        .simple_query("SELECT * FROM nation a JOIN nation b USING (n_nationkey)")
        .await
        .unwrap();
    let widths: Vec<usize> = rows
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.len()),
            _ => None,
        })
        .collect();
    assert_eq!(widths, vec![1, 1, 1]);
    let err = client
        .simple_query("SELECT * FROM nation a RIGHT JOIN nation b USING (n_nationkey)")
        .await
        .unwrap_err();
    assert!(
        err.as_db_error()
            .unwrap()
            .message()
            .contains("only supported with INNER and LEFT"),
        "{err}"
    );
}

async fn trino_server() -> String {
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
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Rows of `sql`, or the error document.
async fn trino(base: &str, sql: &str) -> Result<Vec<Vec<Value>>, Value> {
    let http = reqwest::Client::new();
    let mut doc: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "alice")
        .header("X-Trino-Catalog", "memory")
        .header("X-Trino-Schema", "default")
        .body(sql.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut rows = Vec::new();
    loop {
        if let Some(err) = doc.get("error") {
            return Err(err.clone());
        }
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().map(|r| r.as_array().unwrap().clone()));
        }
        let Some(next) = doc.get("nextUri").and_then(Value::as_str) else {
            return Ok(rows);
        };
        doc = http.get(next).send().await.unwrap().json().await.unwrap();
    }
}

#[tokio::test]
async fn trino_resolves_client_metadata_queries() {
    let base = trino_server().await;
    let one = |rows: Vec<Vec<Value>>| rows[0][0].clone();

    assert_eq!(
        one(trino(&base, "SELECT current_catalog, current_schema")
            .await
            .unwrap()),
        "memory"
    );
    let rows = trino(&base, "SELECT current_schema").await.unwrap();
    assert_eq!(rows[0][0], "default");
    // dbt-trino's catalog query.
    let rows = trino(
        &base,
        "SELECT table_name FROM memory.INFORMATION_SCHEMA.tables \
         WHERE table_schema = 'default'",
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![Value::from("nation")]]);
    // SQLAlchemy's Trino dialect, reflecting a table.
    let rows = trino(
        &base,
        "SELECT \"comment\" FROM \"system\".\"metadata\".\"table_comments\" \
         WHERE \"catalog_name\" = 'memory' AND \"schema_name\" = 'default' \
         AND \"table_name\" = 'nation'",
    )
    .await
    .unwrap();
    assert_eq!(rows, vec![vec![Value::Null]]);
    assert_eq!(
        one(trino(&base, "SELECT count(*) FROM NATION").await.unwrap()),
        3
    );
}
