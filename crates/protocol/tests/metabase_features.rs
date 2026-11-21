//! SQL features Metabase needs over pgwire (G6 stage 2b-2).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int16Array, Int64Array, Int8Array, StringArray,
};
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

/// A memory table of `(k BIGINT, v <v's type>)`.
fn keyed(keys: Vec<i64>, v: ArrayRef) -> Arc<MemoryTable> {
    let v_type = v.data_type().clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", ArrowDataType::Int64, true),
        Field::new("v", v_type.clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys)), v]).unwrap();
    Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "k".to_string(),
                data_type: DataType::Int64,
                nullable: true,
            },
            ColumnInfo {
                name: "v".to_string(),
                data_type: DataType::try_from(v_type).unwrap(),
                nullable: true,
            },
        ],
        vec![batch],
    ))
}

/// Registers the fixture tables under `file.default`:
/// `words(w VARCHAR)`, `t(x BIGINT)` = 1,2,3, `e(x BIGINT)` empty and
/// `dt(d DATE)` = 2000-03-31, 1996-01-02; `(k, v)` tables `sm` (SMALLINT),
/// `ti` (TINYINT) = (1,1), (1,2), (1,4), (2,10) and `num` (DECIMAL(10,2)) =
/// (1,1.00), (1,2.50), (2,3.25), (2,4.00), (2,10.10).
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
    let keys = vec![1, 1, 1, 2];
    schema.register_table(
        "sm",
        keyed(keys.clone(), Arc::new(Int16Array::from(vec![1, 2, 4, 10]))),
    );
    schema.register_table(
        "ti",
        keyed(keys, Arc::new(Int8Array::from(vec![1, 2, 4, 10]))),
    );
    schema.register_table(
        "num",
        keyed(
            vec![1, 1, 2, 2, 2],
            Arc::new(
                Decimal128Array::from(vec![100, 250, 325, 400, 1010])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
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
        .unwrap_or_else(|e| panic!("{sql}: {:?}", e.as_db_error().map(|d| d.message())))
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
    assert_eq!(
        rows(
            &client,
            "SELECT COALESCE(NULLIF(x, 1), '0') FROM t ORDER BY x"
        )
        .await,
        [text("0"), text("2"), text("3")]
    );
    assert_eq!(
        rows(&client, "SELECT sum(COALESCE(x, '0')) FROM t").await,
        [text("6")]
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

/// The RowDescription type OID of a statement's first column.
async fn first_oid(client: &tokio_postgres::Client, sql: &str) -> u32 {
    let stmt = client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    stmt.columns()[0].type_().oid()
}

#[tokio::test]
async fn interval_arithmetic() {
    let client = connect().await;
    // Month ends clamp: 2000-03-31 minus a month is 2000-02-29.
    assert_eq!(
        rows(&client, "SELECT d - INTERVAL '1 month' FROM dt ORDER BY d").await,
        [text("1995-12-02"), text("2000-02-29")]
    );
    let day = "SELECT d + INTERVAL '1 day' FROM dt ORDER BY d";
    assert_eq!(
        rows(&client, day).await,
        [text("1996-01-03"), text("2000-04-01")]
    );
    assert_eq!(first_oid(&client, day).await, 1082, "date");
    assert_eq!(
        rows(&client, "SELECT d + (INTERVAL '1 day') FROM dt ORDER BY d").await,
        [text("1996-01-03"), text("2000-04-01")]
    );
    assert_eq!(
        rows(&client, "SELECT d + INTERVAL '1 week' FROM dt ORDER BY d").await,
        [text("1996-01-09"), text("2000-04-07")]
    );
    // 2000-03-31 + 3 months clamps to 2000-06-30.
    assert_eq!(
        rows(
            &client,
            "SELECT d + INTERVAL '1 quarter' FROM dt ORDER BY d"
        )
        .await,
        [text("1996-04-02"), text("2000-06-30")]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT CAST(CAST(d AS TIMESTAMP) + INTERVAL '1 day' AS VARCHAR) FROM dt ORDER BY d LIMIT 1"
        )
        .await,
        [text("1996-01-03T00:00:00")]
    );
    // 02:00 on 1996-01-02, in Arneb's existing timestamp-to-varchar text.
    assert_eq!(
        rows(
            &client,
            "SELECT CAST(d + INTERVAL '2 hours' AS VARCHAR) FROM dt ORDER BY d LIMIT 1"
        )
        .await,
        [text("1996-01-02T02:00:00")]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT count(*) FROM dt WHERE d >= DATE_TRUNC('year', NOW() + INTERVAL '-40 year')"
        )
        .await,
        [text("2")]
    );
    // Metabase's Sunday-based week: PostgreSQL gives 1995-12-31.
    assert_eq!(
        rows(
            &client,
            "SELECT CAST(DATE_TRUNC('week', d + INTERVAL '1 day') AS DATE) + INTERVAL '-1 day' \
             FROM dt ORDER BY d LIMIT 1"
        )
        .await,
        [text("1995-12-31")]
    );
    let diff = "SELECT CAST('2000-01-01' AS DATE) - d FROM dt ORDER BY d";
    assert_eq!(rows(&client, diff).await, [text("1460"), text("-90")]);
    assert_eq!(first_oid(&client, diff).await, 23, "int4");
    // A DATE against TIMESTAMP bounds compares as TIMESTAMP (Metabase's
    // x-ray "last 30 days" card); 2000-03-31 00:00 is inside the bounds.
    assert_eq!(
        rows(
            &client,
            "SELECT count(*) FROM dt WHERE d BETWEEN \
             CAST(CAST(DATE '2000-03-31' AS DATE) AS TIMESTAMPTZ) AND \
             CAST(CAST(DATE '2000-04-01' + INTERVAL '-1 day' AS DATE) AS TIMESTAMPTZ)"
        )
        .await,
        [text("1")]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT count(*) FROM dt WHERE d IN (CAST(DATE '1996-01-02' AS TIMESTAMP), DATE '2000-03-31')"
        )
        .await,
        [text("2")]
    );
    let err = client
        .simple_query("SELECT INTERVAL '1 day'")
        .await
        .expect_err("a bare interval is unsupported");
    let message = err
        .as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string());
    assert!(message.contains("interval"), "{message}");
}

/// The error message of a statement that must fail.
async fn error_message(client: &tokio_postgres::Client, sql: &str) -> String {
    let err = client.simple_query(sql).await.expect_err(sql);
    err.as_db_error()
        .map_or_else(|| err.to_string(), |e| e.message().to_string())
}

#[tokio::test]
async fn ordered_set_percentiles() {
    let client = connect().await;
    let cases = [
        (
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t",
            Some("2"),
        ),
        (
            "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x) FROM t",
            Some("1.5"),
        ),
        (
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM t",
            Some("2"),
        ),
        (
            "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC) FROM t",
            Some("2.5"),
        ),
        (
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM e",
            None,
        ),
        (
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM t WHERE x = 3",
            Some("3"),
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(
            rows(&client, sql).await,
            [vec![want.map(str::to_string)]],
            "{sql}"
        );
    }
    // Grouped: odd {1, 3} and even {2} both have median 2.
    assert_eq!(
        rows(
            &client,
            "SELECT x % 2, percentile_cont(0.5) WITHIN GROUP (ORDER BY x) \
             FROM t GROUP BY x % 2 ORDER BY 1"
        )
        .await,
        [
            vec![Some("0".to_string()), Some("2".to_string())],
            vec![Some("1".to_string()), Some("2".to_string())],
        ]
    );
    let message = error_message(
        &client,
        "SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY x) FROM t",
    )
    .await;
    assert!(message.contains("between 0 and 1"), "{message}");
    let message = error_message(&client, "SELECT sum(x) WITHIN GROUP (ORDER BY x) FROM t").await;
    assert!(message.contains("WITHIN GROUP"), "{message}");
}

/// `percentile_cont` interpolates over small-integer and decimal keys too;
/// `percentile_disc` returns a value of the key's own type.
#[tokio::test]
async fn percentiles_over_small_integers_and_decimals() {
    let client = connect().await;
    for table in ["sm", "ti"] {
        assert_eq!(
            rows(
                &client,
                &format!(
                    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v), \
                     percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FROM {table}"
                )
            )
            .await,
            [cells(&[Some("3"), Some("2")])],
            "{table}"
        );
        assert_eq!(
            rows(
                &client,
                &format!(
                    "SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) \
                     FROM {table} GROUP BY k ORDER BY k"
                )
            )
            .await,
            [
                cells(&[Some("1"), Some("2")]),
                cells(&[Some("2"), Some("10")])
            ],
            "{table}"
        );
    }
    let grouped = "SELECT k, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS c, \
                   percentile_disc(0.5) WITHIN GROUP (ORDER BY v) AS d \
                   FROM num GROUP BY k ORDER BY k";
    assert_eq!(
        rows(&client, grouped).await,
        [
            cells(&[Some("1"), Some("1.75"), Some("1.00")]),
            cells(&[Some("2"), Some("4"), Some("4.00")])
        ]
    );
    let stmt = client.prepare(grouped).await.unwrap();
    let types: Vec<_> = stmt.columns().iter().map(|c| c.type_().clone()).collect();
    assert_eq!(
        types,
        [
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::FLOAT8,
            tokio_postgres::types::Type::NUMERIC
        ]
    );
}

fn cells(v: &[Option<&str>]) -> Vec<Option<String>> {
    v.iter().map(|c| c.map(str::to_string)).collect()
}

#[tokio::test]
async fn array_subscripts_and_composites() {
    let client = connect().await;
    assert_eq!(
        rows(
            &client,
            "SELECT (ARRAY[10,20,30])[1], (ARRAY[10,20,30])[3], (ARRAY[10,20,30])[0], \
             (ARRAY[10,20,30])[-1], (ARRAY[10,20,30])[4]"
        )
        .await,
        [cells(&[Some("10"), Some("30"), None, None, None])]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT (CAST(NULL AS BIGINT[]))[1], (ARRAY[1])[CAST(NULL AS BIGINT)]"
        )
        .await,
        [cells(&[None, None])]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT (pg_catalog.current_schemas(true))[1], (current_schemas(false))[1]"
        )
        .await,
        [cells(&[Some("pg_catalog"), Some("default")])]
    );
    // An SRF in a subquery, its struct read through an outer column (the
    // JDBC getPrimaryKeys shape).
    assert_eq!(
        rows(
            &client,
            "SELECT k.n, (k.e).x, (k.e).n FROM \
             (SELECT information_schema._pg_expandarray(ARRAY[7,8]) AS e, 0 AS n) k ORDER BY 2"
        )
        .await,
        [
            cells(&[Some("0"), Some("7"), Some("1")]),
            cells(&[Some("0"), Some("8"), Some("2")]),
        ]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT (information_schema._pg_expandarray(ARRAY[5])).n"
        )
        .await,
        [text("1")]
    );
    let message = error_message(
        &client,
        "SELECT (information_schema._pg_expandarray(ARRAY[5])).zz",
    )
    .await;
    assert!(message.contains("zz"), "{message}");
    let message = error_message(&client, "SELECT (ARRAY[1,2])[1:2]").await;
    assert!(message.contains("slice"), "{message}");
    // A column index, and the getPrimaryKeys filter: the struct column of a
    // subquery compared field-wise in the outer WHERE, beside a lockstep
    // `(srf).n` of the same call.
    assert_eq!(
        rows(&client, "SELECT x, (ARRAY[10,20,30])[x] FROM t ORDER BY x").await,
        [
            cells(&[Some("1"), Some("10")]),
            cells(&[Some("2"), Some("20")]),
            cells(&[Some("3"), Some("30")]),
        ]
    );
    assert_eq!(
        rows(
            &client,
            "SELECT result.v, result.key_seq FROM (SELECT t.x AS v, \
             (information_schema._pg_expandarray(ARRAY[1,3])).n AS key_seq, \
             information_schema._pg_expandarray(ARRAY[1,3]) AS keys FROM t) result \
             WHERE result.v = (result.keys).x ORDER BY result.key_seq"
        )
        .await,
        [
            cells(&[Some("1"), Some("1")]),
            cells(&[Some("3"), Some("2")]),
        ]
    );
    // A composite reaching the client goes in text, as Arrow displays it.
    assert_eq!(
        rows(
            &client,
            "SELECT information_schema._pg_expandarray(ARRAY[7])"
        )
        .await,
        [cells(&[Some("{x: 7, n: 1}")])]
    );
}
