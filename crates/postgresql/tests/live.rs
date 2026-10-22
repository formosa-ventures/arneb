//! Tests against a real PostgreSQL loaded with `docker/postgres/init`.
//!
//! Skipped unless `ARNEB_TEST_POSTGRES_URL` is set, e.g.
//!
//! ```text
//! docker compose up -d postgres
//! ARNEB_TEST_POSTGRES_URL=postgresql://arneb:arneb@127.0.0.1:5433/arneb \
//!   cargo test -p arneb-postgresql --test live
//! ```

use std::collections::BTreeSet;
use std::sync::Arc;

use arneb_catalog::{CatalogProvider, TableProvider};
use arneb_common::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_common::stream::collect_stream;
use arneb_common::types::{ScalarValue, TableReference, TimeUnit};
use arneb_connectors::ConnectorFactory;
use arneb_execution::{DataSource, ScanContext};
use arneb_planner::PlanExpr;
use arneb_postgresql::{PostgresCatalog, PostgresConfig};
use arneb_sql_parser::ast::BinaryOp;
use arrow::array::{
    Array, AsArray, BinaryArray, Decimal128Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{Date32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use futures::StreamExt;

fn url() -> Option<String> {
    std::env::var("ARNEB_TEST_POSTGRES_URL").ok()
}

macro_rules! require_postgres {
    () => {
        match url() {
            Some(u) => u,
            None => {
                eprintln!("ARNEB_TEST_POSTGRES_URL not set; skipping");
                return;
            }
        }
    };
}

async fn catalog(url: &str, tweak: impl FnOnce(&mut PostgresConfig)) -> PostgresCatalog {
    let mut config = PostgresConfig::new(url);
    tweak(&mut config);
    PostgresCatalog::connect("pg", config).await.unwrap()
}

async fn table(cat: &PostgresCatalog, schema: &str, name: &str) -> Arc<dyn TableProvider> {
    cat.schema(schema)
        .await
        .unwrap_or_else(|| panic!("schema {schema}"))
        .table(name)
        .await
        .unwrap_or_else(|| panic!("table {name}"))
}

async fn source(cat: &PostgresCatalog, schema: &str, name: &str) -> Arc<dyn DataSource> {
    let t = table(cat, schema, name).await;
    cat.connector_factory()
        .create_data_source(
            &TableReference {
                catalog: Some("pg".into()),
                schema: Some(schema.into()),
                table: name.into(),
            },
            &t.schema(),
            &t.properties(),
        )
        .await
        .unwrap()
}

async fn scan_all(ds: &dyn DataSource, ctx: &ScanContext) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for p in 0..ds.partition_count() {
        out.extend(
            collect_stream(ds.scan(ctx, p).await.unwrap())
                .await
                .unwrap(),
        );
    }
    out
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn col(index: usize) -> PlanExpr {
    PlanExpr::Column {
        index,
        name: format!("c{index}"),
        span: None,
    }
}

fn lit(value: ScalarValue) -> PlanExpr {
    PlanExpr::Literal { value, span: None }
}

fn bin(l: PlanExpr, op: BinaryOp, r: PlanExpr) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(l),
        op,
        right: Box::new(r),
        span: None,
    }
}

async fn remote_count(url: &str, sql: &str) -> i64 {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    client.query_one(sql, &[]).await.unwrap().get(0)
}

#[tokio::test]
async fn discovery() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let schemas = cat.schema_names().await;
    assert!(schemas.contains(&"tpch".to_string()), "{schemas:?}");
    assert!(schemas.contains(&"arneb_test".to_string()));
    assert!(!schemas
        .iter()
        .any(|s| s.starts_with("pg_") || s == "information_schema"));

    let tables = cat.schema("arneb_test").await.unwrap().table_names().await;
    for t in ["all_types", "keyed", "MixedCase", "keyed_view", "keyed_mv"] {
        assert!(tables.contains(&t.to_string()), "{t} in {tables:?}");
    }
    // Case-insensitive fallback, and an unknown table.
    let mixed = table(&cat, "arneb_test", "mixedcase").await;
    assert_eq!(mixed.schema()[0].name, "Id");
    assert!(cat
        .schema("arneb_test")
        .await
        .unwrap()
        .table("missing")
        .await
        .is_none());

    let nation = table(&cat, "tpch", "nation").await;
    assert_eq!(nation.statistics().unwrap().row_count, Some(25));
    assert!(!nation.schema()[1].nullable, "n_name is NOT NULL");
}

#[tokio::test]
async fn every_mapped_type_round_trips() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "arneb_test", "all_types").await;
    // Every column but the jsonb one (17).
    let ctx = ScanContext::default().with_projection((0..17).collect());
    let batches = scan_all(ds.as_ref(), &ctx).await;
    assert_eq!(rows(&batches), 3);
    let b = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let by_id = |id: i32| {
        b.column(0)
            .as_primitive::<Int32Type>()
            .iter()
            .position(|v| v == Some(id))
            .unwrap()
    };
    let (r1, r2, r3) = (by_id(1), by_id(2), by_id(3));

    assert!(b.column(1).as_boolean().value(r1));
    assert_eq!(b.column(2).as_primitive::<Int16Type>().value(r1), -32768);
    assert_eq!(b.column(3).as_primitive::<Int32Type>().value(r2), i32::MIN);
    assert_eq!(b.column(4).as_primitive::<Int64Type>().value(r1), i64::MIN);
    assert_eq!(b.column(6).as_primitive::<Float64Type>().value(r2), 1e300);
    let dec = b
        .column(7)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(dec.value(r1), 123_456_789_012_345);
    assert_eq!(dec.value(r2), -5);
    let num = b
        .column(8)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(num.scale(), 10);
    assert_eq!(num.value(r1), 31_415_926_535);
    assert_eq!(num.value(r2), -123_456_789_012_345_678_905_000_000_000);
    let text = |c: usize| b.column(c).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text(9).value(r1), "ab   ", "char(5) keeps its padding");
    assert_eq!(text(10).value(r1), "O'Brien");
    assert_eq!(text(10).value(r2), "");
    assert_eq!(text(11).value(r2), "naïve ✓");
    let dates = b.column(12).as_primitive::<Date32Type>();
    assert_eq!(dates.value(r1), 10_471); // 1998-09-02
    assert_eq!(dates.value(r2), -719_162); // 0001-01-01
    let ts = b
        .column(13)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.value(r1), 981_173_106_789_012);
    assert_eq!(ts.value(r2), -1);
    let tstz = b
        .column(14)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(tstz.timezone(), Some("UTC"));
    assert_eq!(tstz.value(r1), 981_165_906_000_000); // 02:05:06 UTC
    assert_eq!(text(15).value(r1), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    let bytes = b.column(16).as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(bytes.value(r1), [0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(bytes.value(r2), b"");
    for c in 1..17 {
        assert!(b.column(c).is_null(r3), "column {c} NULL in row 3");
    }
    let _ = TimeUnit::Microsecond;
}

#[tokio::test]
async fn unsupported_type_fails_only_when_read() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "arneb_test", "all_types").await;
    for ctx in [
        ScanContext::default(),
        ScanContext::default().with_projection(vec![0, 17]),
    ] {
        let err = match ds.scan(&ctx, 0).await {
            Ok(_) => panic!("scan with c_json should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("c_json") && err.contains("jsonb"), "{err}");
    }
    let ok = scan_all(
        ds.as_ref(),
        &ScanContext::default().with_projection(vec![0]),
    )
    .await;
    assert_eq!(rows(&ok), 3);
}

#[tokio::test]
async fn user_type_named_like_a_builtin_is_unsupported() {
    let url = require_postgres!();
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(conn);
    client
        .batch_execute(
            "DROP SCHEMA IF EXISTS arneb_shadow CASCADE; \
             CREATE SCHEMA arneb_shadow; \
             CREATE TYPE arneb_shadow.date AS ENUM ('x'); \
             CREATE TABLE arneb_shadow.t (id int, d arneb_shadow.date); \
             INSERT INTO arneb_shadow.t VALUES (1, 'x');",
        )
        .await
        .unwrap();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "arneb_shadow", "t").await;
    let err = match ds.scan(&ScanContext::default(), 0).await {
        Ok(_) => panic!("an enum named date must not be read as a date"),
        Err(e) => e.to_string(),
    };
    client
        .batch_execute("DROP SCHEMA arneb_shadow CASCADE")
        .await
        .unwrap();
    assert!(err.contains("arneb_shadow.date"), "{err}");
}

#[tokio::test]
async fn inexact_numeric_fails() {
    let url = require_postgres!();
    // At scale 0 the unconstrained numeric 3.1415926535 cannot be read exactly.
    let cat = catalog(&url, |c| c.unconstrained_numeric_scale = 0).await;
    let ds = source(&cat, "arneb_test", "all_types").await;
    let stream = ds
        .scan(&ScanContext::default().with_projection(vec![8]), 0)
        .await
        .unwrap();
    let err = collect_stream(stream).await.unwrap_err().to_string();
    assert!(err.contains("c_num") && err.contains("rounding"), "{err}");
}

#[tokio::test]
async fn pushed_filters_match_remote_results() {
    let url = require_postgres!();
    let cat = catalog(&url, |c| {
        c.split_min_rows = 50_000;
        c.max_splits = 4;
    })
    .await;
    let ds = source(&cat, "arneb_test", "keyed").await;
    // grp = 3 AND label LIKE 'label-1%' AND id BETWEEN 1000 AND 150000
    // AND upper(label) = 'X' OR TRUE   (the last stays local)
    let filter = bin(
        bin(col(1), BinaryOp::Eq, lit(ScalarValue::Int32(3))),
        BinaryOp::And,
        PlanExpr::Between {
            expr: Box::new(col(0)),
            negated: false,
            low: Box::new(lit(ScalarValue::Int64(1_000))),
            high: Box::new(lit(ScalarValue::Int64(150_000))),
            span: None,
        },
    );
    let like = bin(
        col(2),
        BinaryOp::Like,
        lit(ScalarValue::Utf8("label-1%".into())),
    );
    let ctx = ScanContext::default().with_filters(vec![filter, like]);
    let got = rows(&scan_all(ds.as_ref(), &ctx).await);
    let expected = remote_count(
        &url,
        "SELECT count(*) FROM arneb_test.keyed \
         WHERE grp = 3 AND id BETWEEN 1000 AND 150000 AND label LIKE 'label-1%'",
    )
    .await;
    assert_eq!(got as i64, expected);
    assert!(got > 0);
}

#[tokio::test]
async fn uuid_conditions_stay_local() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "arneb_test", "all_types").await;
    // Pushed, PostgreSQL would compare as uuid: `<>` against the upper-case
    // spelling drops row 1, and `= 'xyz'` / LIKE would be errors. Kept local,
    // the source returns every row and FilterExec decides.
    let upper = lit(ScalarValue::Utf8(
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11".into(),
    ));
    for filter in [
        bin(col(15), BinaryOp::NotEq, upper),
        bin(col(15), BinaryOp::Eq, lit(ScalarValue::Utf8("xyz".into()))),
        bin(
            col(15),
            BinaryOp::Like,
            lit(ScalarValue::Utf8("a0%".into())),
        ),
    ] {
        let ctx = ScanContext::default()
            .with_projection(vec![0, 15])
            .with_filters(vec![filter]);
        assert_eq!(rows(&scan_all(ds.as_ref(), &ctx).await), 3);
    }
}

#[tokio::test]
async fn split_scan_returns_every_row_once() {
    let url = require_postgres!();
    let cat = catalog(&url, |c| {
        c.split_min_rows = 50_000;
        c.max_splits = 4;
    })
    .await;
    let ds = source(&cat, "arneb_test", "keyed").await;
    assert_eq!(ds.partition_count(), 4);
    let ctx = ScanContext::default().with_projection(vec![0]);
    let mut ids = BTreeSet::new();
    let mut total = 0;
    for p in 0..4 {
        let batches = collect_stream(ds.scan(&ctx, p).await.unwrap())
            .await
            .unwrap();
        let n = rows(&batches);
        assert!(n > 0, "partition {p} is empty");
        for b in &batches {
            assert!(b.num_rows() <= 8192);
            ids.extend(
                b.column(0)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        total += n;
    }
    assert_eq!(total, 200_000);
    assert_eq!(ids.len(), 200_000);
    assert_eq!(ids.first(), Some(&1));
    assert_eq!(ids.last(), Some(&200_000));

    // Small tables and views are not split.
    assert_eq!(source(&cat, "tpch", "nation").await.partition_count(), 1);
    assert_eq!(
        source(&cat, "arneb_test", "keyed_view")
            .await
            .partition_count(),
        1
    );
}

#[tokio::test]
async fn limit_and_empty_projection() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "tpch", "nation").await;
    let limited = scan_all(ds.as_ref(), &ScanContext::default().with_limit(5)).await;
    assert_eq!(rows(&limited), 5);
    // An untranslatable filter disables the remote LIMIT.
    let local = PlanExpr::Function {
        name: "upper".into(),
        args: vec![col(1)],
        distinct: false,
        span: None,
    };
    let ctx = ScanContext::default().with_limit(5).with_filters(vec![bin(
        local,
        BinaryOp::Eq,
        lit(ScalarValue::Utf8("X".into())),
    )]);
    assert_eq!(rows(&scan_all(ds.as_ref(), &ctx).await), 25);
    // count(*)-style scan: no columns, all rows.
    let counted = scan_all(ds.as_ref(), &ScanContext::default().with_projection(vec![])).await;
    assert_eq!(rows(&counted), 25);
    assert_eq!(counted[0].num_columns(), 0);
}

#[tokio::test]
async fn small_pool_serves_many_partitions_and_survives_cancellation() {
    let url = require_postgres!();
    let cat = catalog(&url, |c| {
        c.max_connections = 2;
        c.split_min_rows = 20_000;
        c.max_splits = 8;
    })
    .await;
    let ds = source(&cat, "arneb_test", "keyed").await;
    assert_eq!(ds.partition_count(), 8);
    let ctx = ScanContext::default().with_projection(vec![0]);

    // Drop a stream mid-COPY: its connection must not poison the pool.
    let mut s = ds.scan(&ctx, 0).await.unwrap();
    assert!(s.next().await.unwrap().is_ok());
    drop(s);

    // All 8 partitions at once over 2 connections.
    let mut streams = Vec::new();
    for p in 0..8 {
        streams.push(collect_stream(ds.scan(&ctx, p).await.unwrap()));
    }
    let results = futures::future::join_all(streams).await;
    let total: usize = results.into_iter().map(|r| rows(&r.unwrap())).sum();
    assert_eq!(total, 200_000);
}

#[tokio::test]
async fn a_full_pool_fails_after_the_wait_timeout() {
    let url = require_postgres!();
    let cat = catalog(&url, |c| {
        c.max_connections = 1;
        c.pool_wait_timeout_secs = 1;
        c.split_min_rows = 20_000;
        c.max_splits = 2;
    })
    .await;
    let ds = source(&cat, "arneb_test", "keyed").await;
    let ctx = ScanContext::default().with_projection(vec![0]);

    // Partition 0 holds the only connection mid-COPY.
    let mut first = ds.scan(&ctx, 0).await.unwrap();
    assert!(first.next().await.unwrap().is_ok());

    let started = std::time::Instant::now();
    let err = match collect_stream(ds.scan(&ctx, 1).await.unwrap()).await {
        Ok(_) => panic!("a scan with no free connection should fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("pool_wait_timeout_secs"), "{err}");
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    drop(first);
}

#[tokio::test]
async fn decode_buffer_is_accounted_and_released() {
    let url = require_postgres!();
    let cat = catalog(&url, |_| {}).await;
    let ds = source(&cat, "arneb_test", "keyed").await;
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let ctx = ScanContext {
        memory_pool: pool.clone(),
        ..Default::default()
    };
    let mut s = ds.scan(&ctx, 0).await.unwrap();
    assert!(s.next().await.unwrap().is_ok());
    assert!(pool.reserved() > 0, "decode buffer tracked while streaming");
    while let Some(b) = s.next().await {
        b.unwrap();
    }
    assert_eq!(pool.reserved(), 0, "released at end of stream");
}

#[tokio::test]
async fn unreachable_database_is_a_connection_error() {
    if url().is_none() {
        return;
    }
    let mut config = PostgresConfig::new("postgresql://127.0.0.1:1/arneb");
    config.password = Some("do-not-log-me".into());
    let err = PostgresCatalog::connect("pg", config)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("connection failed"), "{err}");
    assert!(!err.contains("do-not-log-me"), "{err}");
}

fn ids(batches: &[RecordBatch]) -> BTreeSet<i64> {
    batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec())
        .collect()
}

#[tokio::test]
async fn dynamic_in_list_is_exact_under_the_threshold_and_a_range_over_it() {
    let url = require_postgres!();
    let keys = [5_i64, 17, 4_000, 4_001, 90_000];
    let dynamic = PlanExpr::InList {
        expr: Box::new(col(0)),
        list: keys.iter().map(|&k| lit(ScalarValue::Int64(k))).collect(),
        negated: false,
        span: None,
    };
    let ctx = ScanContext {
        filters: vec![dynamic],
        dynamic_filters_start: Some(0),
        ..Default::default()
    }
    .with_projection(vec![0]);

    let exact_cat = catalog(&url, |c| c.domain_compaction_threshold = 10).await;
    let exact = ids(&scan_all(&*source(&exact_cat, "arneb_test", "keyed").await, &ctx).await);
    assert_eq!(exact, keys.into_iter().collect());

    // Over the threshold the scan returns the whole min..max range: a
    // superset the join above it narrows back down.
    let compact_cat = catalog(&url, |c| c.domain_compaction_threshold = 2).await;
    let ranged = ids(&scan_all(&*source(&compact_cat, "arneb_test", "keyed").await, &ctx).await);
    assert_eq!(ranged, (5..=90_000).collect());
    assert!(exact.is_subset(&ranged));
}
