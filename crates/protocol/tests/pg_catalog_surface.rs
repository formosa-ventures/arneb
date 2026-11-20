//! The `pg_catalog` surface SQLAlchemy reflection reads (G6 stage 2b): new
//! columns and tables, catalog functions, `json_build_object` and runtime
//! `regclass`. No test here may reach the pgwire fallback, so each asserts
//! the process-global fallback counter did not move (this binary has no
//! test that deliberately triggers it).

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::{AccessRules, CatalogManager};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

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

/// `file.default.{nation, region}` and `file.staging.raw`, with `file` the
/// default catalog.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table(
        "nation",
        table(&[
            ("n_nationkey", DataType::Int64, false),
            ("n_name", DataType::Utf8, true),
            (
                "n_price",
                DataType::Decimal128 {
                    precision: 12,
                    scale: 2,
                },
                true,
            ),
        ]),
    );
    default_schema.register_table("region", table(&[("r_regionkey", DataType::Int32, false)]));
    let staging = Arc::new(MemorySchema::new());
    staging.register_table("raw", table(&[("payload", DataType::Utf8, true)]));
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", default_schema);
    file.register_schema("staging", staging);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );
    (Arc::new(cm), Arc::new(registry))
}

async fn pg_server(rules: Option<&str>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_access_rules(rules.map(|r| Arc::new(AccessRules::from_json(r).unwrap())));
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

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n{e:?}"))
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect())
            }
            _ => None,
        })
        .collect()
}

fn column(rows: &[Vec<Option<String>>], i: usize) -> Vec<String> {
    rows.iter()
        .map(|r| r[i].clone().unwrap_or_default())
        .collect()
}

fn fallbacks() -> u64 {
    arneb_common::metrics::pg_catalog_fallbacks()
}

async fn one(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    let mut r = rows(client, sql).await;
    assert_eq!(r.len(), 1, "{sql}: {r:?}");
    r.remove(0)
}

#[tokio::test]
async fn new_attribute_columns_have_defaults() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT atthasdef, attidentity, attgenerated, attcollation FROM pg_attribute \
         WHERE attnum > 0 LIMIT 1",
    )
    .await;
    assert_eq!(
        r,
        [
            Some("f".into()),
            Some(String::new()),
            Some(String::new()),
            Some("0".into())
        ]
    );
    let r = one(
        &client,
        "SELECT relam, reloptions, (SELECT typcollation FROM pg_type WHERE oid = 23) \
         FROM pg_class WHERE relname = 'nation'",
    )
    .await;
    assert_eq!(r, [Some("2".into()), None, Some("0".into())]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn new_catalog_tables_are_empty_and_typed() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    for table in ["pg_index", "pg_sequence", "pg_collation", "pg_opclass"] {
        let r = one(&client, &format!("SELECT count(*) FROM {table}")).await;
        assert_eq!(r, [Some("0".into())], "{table}");
    }
    assert!(rows(&client, "SELECT unnest(indkey) FROM pg_index")
        .await
        .is_empty());
    assert!(rows(
        &client,
        "SELECT indexrelid, indrelid, indisprimary, indisunique, indisvalid, indnkeyatts, \
         indnullsnotdistinct, indpred, indkey, indclass, indoption FROM pg_index"
    )
    .await
    .is_empty());
    assert!(rows(
        &client,
        "SELECT seqrelid, seqstart, seqincrement, seqmin, seqmax, seqcache, seqcycle \
         FROM pg_sequence"
    )
    .await
    .is_empty());
    assert!(rows(
        &client,
        "SELECT oid, collname, collnamespace FROM pg_collation"
    )
    .await
    .is_empty());
    assert!(
        rows(&client, "SELECT oid, opcname, opcdefault FROM pg_opclass")
            .await
            .is_empty()
    );
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn catalog_functions_return_null() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT pg_get_constraintdef(1, true), pg_get_expr('x', 1), pg_get_indexdef(1, 1, true), \
         pg_get_serial_sequence('a', 'b'), pg_collation_is_visible(1), pg_get_constraintdef(1), \
         pg_catalog.pg_get_expr('x', 1, true)",
    )
    .await;
    assert_eq!(r, [None, None, None, None, Some("t".into()), None, None]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn json_build_object_encodes_types() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    // PostgreSQL's exact spacing (documented json_build_object output).
    let r = one(
        &client,
        "SELECT json_build_object('a', 1, 'b', true, 'c', NULL, 'd', 'x\"y')",
    )
    .await;
    assert_eq!(
        r[0].as_deref(),
        Some(r#"{"a" : 1, "b" : true, "c" : null, "d" : "x\"y"}"#)
    );
    let r = one(
        &client,
        "SELECT json_build_object('k'::VARCHAR, 1.5), \
         json_build_object()",
    )
    .await;
    assert_eq!(r, [Some(r#"{"k" : 1.5}"#.into()), Some("{}".into())]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn json_build_object_odd_args_refused() {
    let client = connect(pg_server(None).await).await;
    let err = client
        .simple_query("SELECT json_build_object('a', 1, 'b')")
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("even number of arguments"),
        "{err:?}"
    );
}

#[tokio::test]
async fn regclass_runtime_casts() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT CAST(CAST(oid AS REGCLASS) AS TEXT) FROM pg_class WHERE relname = 'nation'",
    )
    .await;
    assert_eq!(r, [Some("nation".into())]);
    let r = one(
        &client,
        "SELECT CAST(CAST(oid AS REGCLASS) AS TEXT) FROM pg_class WHERE relname = 'raw'",
    )
    .await;
    assert_eq!(r, [Some("staging.raw".into())]);
    let r = one(
        &client,
        "SELECT CAST(CAST(oid AS REGCLASS) AS TEXT) FROM pg_class WHERE relname = 'pg_class'",
    )
    .await;
    assert_eq!(r, [Some("pg_class".into())]);
    let r = one(&client, "SELECT CAST(CAST(NULL AS TEXT) AS REGCLASS)").await;
    assert_eq!(r, [None]);
    let r = one(
        &client,
        "SELECT CAST(CAST(c.relname AS TEXT) AS REGCLASS) = c.oid, CAST(c.oid AS REGCLASS) = c.oid \
         FROM pg_class c WHERE c.relname = 'nation'",
    )
    .await;
    assert_eq!(r, [Some("t".into()), Some("t".into())]);
    // An unknown oid shows as its number.
    let r = one(&client, "SELECT CAST(CAST(99 AS REGCLASS) AS TEXT)").await;
    assert_eq!(r, [Some("99".into())]);
    // A string literal still folds at plan time, without a check.
    let r = one(
        &client,
        "SELECT CAST('pg_catalog.pg_constraint' AS REGCLASS)",
    )
    .await;
    assert_eq!(r, [Some("2606".into())]);
    let err = client
        .simple_query("SELECT CAST(CAST('nope' AS TEXT) AS REGCLASS)")
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("does not exist"), "{err:?}");
    assert_eq!(fallbacks(), before);
}

/// The identity-column subquery SQLAlchemy sends, with its regclass chain.
#[tokio::test]
async fn serial_sequence_chain_plans() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = rows(
        &client,
        "SELECT a.attname, CASE WHEN a.attidentity != '' THEN (SELECT s.seqstart FROM pg_sequence s \
         WHERE a.attidentity != '' AND s.seqrelid = CAST(CAST(pg_get_serial_sequence(\
         CAST(CAST(a.attrelid AS REGCLASS) AS TEXT), a.attname) AS REGCLASS) AS OID)) END \
         FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
         WHERE c.relname = 'nation' ORDER BY a.attnum",
    )
    .await;
    assert_eq!(column(&r, 0), ["n_nationkey", "n_name", "n_price"]);
    assert!(r.iter().all(|row| row[1].is_none()));
    assert_eq!(fallbacks(), before);
}
