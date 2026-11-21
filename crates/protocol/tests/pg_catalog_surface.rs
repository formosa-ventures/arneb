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
    for table in [
        "pg_index",
        "pg_sequence",
        "pg_collation",
        "pg_opclass",
        "pg_inherits",
    ] {
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
    assert!(rows(
        &client,
        "SELECT inhrelid, inhparent, inhseqno, inhdetachpending FROM pg_inherits"
    )
    .await
    .is_empty());
    assert_eq!(fallbacks(), before);
}

/// `current_setting` reads the same server settings `SHOW` does
/// (SQLAlchemy's table-options query reads `default_table_access_method`).
#[tokio::test]
async fn current_setting_reads_server_settings() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT current_setting('default_table_access_method'), \
         pg_catalog.current_setting('server_version'), current_setting('nope', true)",
    )
    .await;
    assert_eq!(r, [Some("heap".into()), Some("14.0".into()), None]);
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

/// A binary `i64` parameter over a pg_catalog table: the binary decoding
/// infers its type over the same catalog Describe plans against, so the
/// oid is read as an int and not as text.
#[tokio::test]
async fn binary_oid_parameter_over_pg_class() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let oid = one(&client, "SELECT oid FROM pg_class WHERE relname = 'nation'").await[0]
        .clone()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    let rows = client
        .query("SELECT relname FROM pg_class WHERE oid = $1", &[&oid])
        .await
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(names, ["nation"]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn stat_user_tables_lists_visible_tables() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = rows(
        &client,
        "SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY relname",
    )
    .await;
    let expected: Vec<Vec<Option<String>>> = [
        ("default", "nation"),
        ("staging", "raw"),
        ("default", "region"),
    ]
    .iter()
    .map(|(s, n)| vec![Some(s.to_string()), Some(n.to_string()), Some("0".into())])
    .collect();
    assert_eq!(r, expected);
    // The same relations pg_class lists as user tables, with the same oids.
    let joined = one(
        &client,
        "SELECT count(*) FROM pg_stat_user_tables st JOIN pg_class c ON c.oid = st.relid \
         WHERE c.relkind = 'r' AND c.relname = st.relname",
    )
    .await;
    assert_eq!(joined, [Some("3".into())]);
    // NULLIF(n_live_tup, '0') is NULL for a zero count.
    let r = one(
        &client,
        "SELECT NULLIF(n_live_tup, '0') FROM pg_stat_user_tables LIMIT 1",
    )
    .await;
    assert_eq!(r, [None]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn enum_is_empty() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(&client, "SELECT count(*) FROM pg_enum").await;
    assert_eq!(r, [Some("0".into())]);
    assert!(rows(
        &client,
        "SELECT oid, enumtypid, enumsortorder, enumlabel FROM pg_enum"
    )
    .await
    .is_empty());
    assert_eq!(fallbacks(), before);
}

/// `pg_tables` lists what `pg_class` lists (every relation is a table);
/// Arneb has no views or materialized views.
#[tokio::test]
async fn pg_tables_lists_relations_and_views_are_empty() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = rows(
        &client,
        "SELECT schemaname, tablename, tableowner, hasindexes FROM pg_tables \
         WHERE schemaname !~ '^pg_' ORDER BY 1, 2",
    )
    .await;
    let names: Vec<_> = r
        .iter()
        .map(|row| {
            format!(
                "{}.{}",
                row[0].as_deref().unwrap(),
                row[1].as_deref().unwrap()
            )
        })
        .collect();
    assert_eq!(names, ["default.nation", "default.region", "staging.raw"]);
    assert!(r.iter().all(|row| row[3].as_deref() == Some("f")), "{r:?}");
    let class = one(&client, "SELECT count(*) FROM pg_class").await;
    let tables = one(&client, "SELECT count(*) FROM pg_tables").await;
    assert_eq!(class, tables);
    for sql in [
        "SELECT schemaname, viewname, viewowner, definition FROM pg_views",
        "SELECT schemaname, matviewname, matviewowner, tablespace, hasindexes, \
         ispopulated, definition FROM pg_matviews",
    ] {
        assert!(rows(&client, sql).await.is_empty(), "{sql}");
    }
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn constraint_key_arrays() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    assert!(rows(&client, "SELECT conkey, confkey FROM pg_constraint")
        .await
        .is_empty());
    assert!(rows(
        &client,
        "SELECT unnest(conkey), unnest(confkey) FROM pg_constraint"
    )
    .await
    .is_empty());
    // The columns are BIGINT arrays: indexing one plans and types.
    let r = one(
        &client,
        "SELECT count(*) FROM (SELECT conkey[1] AS k FROM pg_constraint) t",
    )
    .await;
    assert_eq!(r, [Some("0".into())]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn col_description_is_null() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT col_description(CAST('nation'::regclass AS OID), 1), obj_description(1), \
         obj_description(1, 'pg_class')",
    )
    .await;
    assert_eq!(r, [None, None, None]);
    let r = one(
        &client,
        "SELECT col_description(CAST(CAST(format('%I.%I', CAST('default' AS TEXT), \
         CAST('nation' AS TEXT)) AS REGCLASS) AS OID), 2)",
    )
    .await;
    assert_eq!(r, [None]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn format_over_pgwire() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = one(
        &client,
        "SELECT format('%s', udt_name), format('\"%s\".\"%s\"', 'a', 'b'), \
         format('%I.%I', 'Mixed', 'select'), format('%L', 'it''s'), format('%s|%s', 1, NULL) \
         FROM (SELECT 'int4' AS udt_name) t",
    )
    .await;
    assert_eq!(
        r,
        [
            Some("int4".into()),
            Some("\"a\".\"b\"".into()),
            Some("\"Mixed\".\"select\"".into()),
            Some("'it''s'".into()),
            Some("1|".into())
        ]
    );
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn information_schema_runs_in_engine() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    // Quoted, as Metabase writes it; the WHERE filter applies (the fixed
    // handler returned every table's columns whatever the query asked).
    let r = rows(
        &client,
        "SELECT column_name, udt_name, data_type, is_nullable \
         FROM \"information_schema\".\"columns\" WHERE table_name = 'nation' \
         ORDER BY ordinal_position",
    )
    .await;
    let expected: Vec<Vec<Option<String>>> = [
        ["n_nationkey", "int8", "bigint", "NO"],
        ["n_name", "varchar", "character varying", "YES"],
        ["n_price", "numeric", "numeric", "YES"],
    ]
    .iter()
    .map(|row| row.iter().map(|v| Some(v.to_string())).collect())
    .collect();
    assert_eq!(r, expected);

    let r = rows(
        &client,
        "SELECT table_catalog, table_schema, udt_catalog, udt_schema, column_default, \
         is_identity, is_generated, character_maximum_length, numeric_precision, \
         numeric_scale, datetime_precision \
         FROM information_schema.columns WHERE table_name = 'nation' ORDER BY ordinal_position",
    )
    .await;
    let n = |v: &str| Some(v.to_string());
    let common = [
        n("file"),
        n("default"),
        n("file"),
        n("pg_catalog"),
        None,
        n("NO"),
        n("NEVER"),
    ];
    let tail = [
        [None, n("64"), n("0"), None],
        [None, None, None, None],
        [None, n("12"), n("2"), None],
    ];
    let expected: Vec<Vec<Option<String>>> = tail
        .iter()
        .map(|t| common.iter().chain(t.iter()).cloned().collect())
        .collect();
    assert_eq!(r, expected);

    let tables = rows(
        &client,
        "SELECT table_catalog, table_schema, table_name, table_type \
         FROM information_schema.tables WHERE table_schema = 'default' ORDER BY table_name",
    )
    .await;
    assert_eq!(
        tables,
        [
            [n("file"), n("default"), n("nation"), n("BASE TABLE")],
            [n("file"), n("default"), n("region"), n("BASE TABLE")],
        ]
    );
    let schemata = rows(
        &client,
        "SELECT catalog_name, schema_name FROM information_schema.schemata \
         WHERE schema_name NOT IN ('pg_catalog', 'information_schema') ORDER BY 2",
    )
    .await;
    assert_eq!(
        schemata,
        [[n("file"), n("default")], [n("file"), n("staging")]]
    );
    for empty in ["views", "table_constraints", "key_column_usage"] {
        let r = rows(
            &client,
            &format!("SELECT count(*) FROM information_schema.{empty}"),
        )
        .await;
        assert_eq!(r, [[n("0")]], "{empty}");
    }
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn information_schema_joins_pg_catalog() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    let r = rows(
        &client,
        "SELECT c.column_name FROM information_schema.columns c \
         JOIN pg_catalog.pg_namespace n ON n.nspname = c.table_schema \
         WHERE c.table_name = 'nation' ORDER BY 1",
    )
    .await;
    assert_eq!(column(&r, 0), ["n_name", "n_nationkey", "n_price"]);
    // Metabase's primary-key subquery over the (empty) constraint tables.
    let r = rows(
        &client,
        "SELECT \"c\".\"column_name\", \"pk\".\"column_name\" IS NOT NULL AS \"pk?\" \
         FROM \"information_schema\".\"columns\" AS \"c\" LEFT JOIN \
         (SELECT \"tc\".\"table_schema\", \"tc\".\"table_name\", \"kc\".\"column_name\" \
          FROM \"information_schema\".\"table_constraints\" AS \"tc\" \
          INNER JOIN \"information_schema\".\"key_column_usage\" AS \"kc\" \
          ON (\"tc\".\"constraint_name\" = \"kc\".\"constraint_name\") \
          AND (\"tc\".\"table_schema\" = \"kc\".\"table_schema\") \
          AND (\"tc\".\"table_name\" = \"kc\".\"table_name\") \
          WHERE \"tc\".\"constraint_type\" = 'PRIMARY KEY') AS \"pk\" \
         ON (\"c\".\"table_schema\" = \"pk\".\"table_schema\") \
         AND (\"c\".\"table_name\" = \"pk\".\"table_name\") \
         AND (\"c\".\"column_name\" = \"pk\".\"column_name\") \
         WHERE \"c\".\"table_schema\" IN ('default') AND \"c\".\"table_name\" IN ('region')",
    )
    .await;
    assert_eq!(r, [[Some("r_regionkey".into()), Some("f".into())]]);
    assert_eq!(fallbacks(), before);
}

#[tokio::test]
async fn format_over_information_schema_rows() {
    let before = fallbacks();
    let client = connect(pg_server(None).await).await;
    // A literal template beside a column argument, over several rows.
    let r = rows(
        &client,
        "SELECT FORMAT('%s', \"c\".\"udt_name\"), FORMAT('%I.%I', table_schema, table_name) \
         FROM \"information_schema\".\"columns\" AS \"c\" \
         ORDER BY table_schema, table_name, ordinal_position",
    )
    .await;
    assert_eq!(
        column(&r, 0),
        ["int8", "varchar", "numeric", "int4", "varchar"]
    );
    assert_eq!(
        column(&r, 1),
        [
            "\"default\".nation",
            "\"default\".nation",
            "\"default\".nation",
            "\"default\".region",
            "staging.raw"
        ]
    );
    assert_eq!(fallbacks(), before);
}
