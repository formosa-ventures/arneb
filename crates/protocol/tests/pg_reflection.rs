//! SQLAlchemy 2.1 postgresql-dialect reflection SQL, run verbatim (G6 stage
//! 2b-1). Each family has the statement psycopg2 sends and the one psycopg 3
//! sends (it inlines `'x'::VARCHAR`, `0::SMALLINT`, `1::INTEGER` casts); both
//! must plan and execute without the pgwire `pg_catalog` fallback. No test in
//! this binary triggers the fallback, so each asserts the process-global
//! counter did not move.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// The oid SQLAlchemy inlined for `nation` when the SQL was captured.
const CAPTURED_OID: &str = "1778988401";

const NATION: &[(&str, DataType, bool)] = &[
    ("n_nationkey", DataType::Int64, false),
    ("n_name", DataType::Utf8, false),
    ("n_regionkey", DataType::Int64, false),
    ("n_comment", DataType::Utf8, true),
];

fn table(columns: &[(&str, DataType, bool)]) -> Arc<MemoryTable> {
    let infos: Vec<ColumnInfo> = columns
        .iter()
        .map(|(name, data_type, nullable)| ColumnInfo {
            name: name.to_string(),
            data_type: data_type.clone(),
            nullable: *nullable,
        })
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    // Only the catalog is read here; the batch is never scanned.
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

/// `file.default.{nation, region}`, with `file` the default catalog.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("nation", table(NATION));
    default_schema.register_table("region", table(&[("r_regionkey", DataType::Int64, false)]));
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

fn fallbacks() -> u64 {
    arneb_common::metrics::pg_catalog_fallbacks()
}

/// A statement's result: the column names from RowDescription and the rows.
struct Answer {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

async fn run(client: &tokio_postgres::Client, sql: &str) -> Answer {
    let messages = client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n{e:?}"));
    let mut columns = Vec::new();
    let mut rows = Vec::new();
    for m in &messages {
        match m {
            SimpleQueryMessage::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name().to_owned()).collect();
            }
            SimpleQueryMessage::Row(r) => {
                rows.push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => {}
        }
    }
    Answer { columns, rows }
}

async fn nation_oid(client: &tokio_postgres::Client) -> String {
    let a = run(client, "SELECT oid FROM pg_class WHERE relname = 'nation'").await;
    assert_eq!(a.rows.len(), 1, "{:?}", a.rows);
    a.rows[0][0].clone().unwrap()
}

fn captured(file: &str) -> String {
    let path = format!(
        "{}/tests/data/sqlalchemy_reflection/{file}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// Runs both drivers' variants of `family`, with the captured oid replaced
/// by the harness's `nation` oid, and returns their answers (psycopg2 first).
async fn reflect(family: &str) -> Vec<Answer> {
    let before = fallbacks();
    let client = connect(pg_server().await).await;
    let oid = nation_oid(&client).await;
    let mut answers = Vec::new();
    for driver in ["psycopg2", "psycopg"] {
        let sql = captured(&format!("{family}_{driver}.sql")).replace(CAPTURED_OID, &oid);
        answers.push(run(&client, &sql).await);
    }
    assert_eq!(
        fallbacks(),
        before,
        "{family}: a statement hit the fallback"
    );
    answers
}

/// Every family but `columns` reflects nothing: Arneb has no constraints or
/// indexes. The answer still has one column per `SELECT` item.
async fn reflect_empty(family: &str, columns: &[&str]) {
    for a in reflect(family).await {
        assert_eq!(a.columns, columns, "{family}");
        assert!(a.rows.is_empty(), "{family}: {:?}", a.rows);
    }
}

#[tokio::test]
async fn reflect_columns() {
    for a in reflect("columns").await {
        assert_eq!(
            a.columns,
            [
                "name",
                "format_type",
                "default",
                "not_null",
                "table_name",
                "comment",
                "generated",
                "identity_options",
                "collation"
            ]
        );
        let names: Vec<_> = a.rows.iter().map(|r| r[0].clone().unwrap()).collect();
        assert_eq!(names, ["n_nationkey", "n_name", "n_regionkey", "n_comment"]);
        for (row, (_, _, nullable)) in a.rows.iter().zip(NATION) {
            let expected_type = match row[0].as_deref().unwrap() {
                "n_nationkey" | "n_regionkey" => "bigint",
                _ => "character varying",
            };
            assert_eq!(row[1].as_deref(), Some(expected_type), "{row:?}");
            assert_eq!(row[2], None, "default {row:?}");
            let not_null = if *nullable { "f" } else { "t" };
            assert_eq!(row[3].as_deref(), Some(not_null), "not_null {row:?}");
            assert_eq!(row[4].as_deref(), Some("nation"), "{row:?}");
            assert_eq!(row[7], None, "identity_options {row:?}");
            assert_eq!(row[8], None, "collation {row:?}");
        }
    }
}

#[tokio::test]
async fn reflect_pk() {
    reflect_empty(
        "pk",
        &[
            "conrelid",
            "cols",
            "conname",
            "description",
            "indnkeyatts",
            "indnullsnotdistinct",
        ],
    )
    .await;
}

#[tokio::test]
async fn reflect_unique() {
    reflect_empty(
        "unique",
        &[
            "conrelid",
            "cols",
            "conname",
            "description",
            "indnkeyatts",
            "indnullsnotdistinct",
        ],
    )
    .await;
}

/// `nation` has no foreign keys, but the LEFT JOIN keeps one row for the
/// table with NULL constraint columns, as PostgreSQL returns.
#[tokio::test]
async fn reflect_fk() {
    for a in reflect("fk").await {
        assert_eq!(
            a.columns,
            ["relname", "conname", "anon_1", "nspname", "description"]
        );
        assert_eq!(
            a.rows,
            [vec![Some("nation".to_owned()), None, None, None, None]]
        );
    }
}

/// Same shape as `reflect_fk`: one row per table, NULL constraint columns.
#[tokio::test]
async fn reflect_check() {
    for a in reflect("check").await {
        assert_eq!(a.columns, ["relname", "conname", "anon_1", "description"]);
        assert_eq!(a.rows, [vec![Some("nation".to_owned()), None, None, None]]);
    }
}

#[tokio::test]
async fn reflect_indexes() {
    reflect_empty(
        "indexes",
        &[
            "indrelid",
            "relname",
            "indisunique",
            "indisvalid",
            "has_constraint",
            "indoption",
            "reloptions",
            "relam",
            "filter_definition",
            "indnkeyatts",
            "indnullsnotdistinct",
            "elements",
            "elements_is_expr",
            "elements_opclass",
        ],
    )
    .await;
}

/// `Table(..., autoload_with=engine)` also reads the table options:
/// `nation` is a plain heap table in the default tablespace with no
/// parents, so everything but its oid and name is NULL.
#[tokio::test]
async fn reflect_table_options() {
    for a in reflect("table_options").await {
        assert_eq!(
            a.columns,
            [
                "oid",
                "relname",
                "reloptions",
                "relhasoids",
                "access_method_name",
                "tablespace_name",
                "parent_table_names"
            ]
        );
        assert_eq!(a.rows.len(), 1, "{:?}", a.rows);
        let row = &a.rows[0];
        assert!(row[0].is_some(), "{row:?}");
        assert_eq!(row[1].as_deref(), Some("nation"));
        assert_eq!(row[2..], [None, None, None, None, None], "{row:?}");
    }
}
