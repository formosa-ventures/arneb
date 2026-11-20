//! CAST of a DECIMAL, DOUBLE or REAL to an integer type follows Trino: the
//! value is rounded half away from zero, not truncated. Expected values were
//! taken from Trino 484. TPC-DS q54 buckets customers with
//! `CAST((revenue / 50) AS INTEGER)`; at SF30 truncation put customers one
//! segment too low.
//!
//! Both paths are covered: a cast of a literal (folded at plan time) and a
//! cast of a column (evaluated per batch), with DECIMAL in both its Arrow
//! storages (Decimal128 and Decimal64).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal64Array, Float32Array, Float64Array, Int32Array,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

fn table(columns: Vec<(&str, DataType, ArrayRef)>) -> MemoryTable {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns.iter().map(|(_, _, a)| a.clone()).collect(),
    )
    .unwrap();
    MemoryTable::new(
        columns
            .iter()
            .map(|(name, data_type, _)| ColumnInfo {
                name: name.to_string(),
                data_type: data_type.clone(),
                nullable: true,
            })
            .collect(),
        vec![batch],
    )
}

fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    // v: one row per case, k orders them. x128/x64 hold the same decimals.
    schema.register_table(
        "v",
        Arc::new(table(vec![
            (
                "k",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ),
            (
                "x128",
                DataType::Decimal128 {
                    precision: 20,
                    scale: 2,
                },
                Arc::new(
                    Decimal128Array::from(vec![Some(861762), Some(-861750), Some(861749), None])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ),
            ),
            (
                "x64",
                DataType::Decimal64 {
                    precision: 8,
                    scale: 2,
                },
                Arc::new(
                    Decimal64Array::from(vec![Some(861762), Some(-861750), Some(861749), None])
                        .with_precision_and_scale(8, 2)
                        .unwrap(),
                ),
            ),
            (
                "d",
                DataType::Float64,
                Arc::new(Float64Array::from(vec![
                    Some(8617.62),
                    Some(-8617.5),
                    Some(8617.49),
                    None,
                ])),
            ),
            (
                "r",
                DataType::Float32,
                Arc::new(Float32Array::from(vec![
                    Some(2.5),
                    Some(-2.5),
                    Some(2.49),
                    None,
                ])),
            ),
            (
                "s64",
                DataType::Decimal64 {
                    precision: 8,
                    scale: 2,
                },
                Arc::new(
                    Decimal64Array::from(vec![Some(250), Some(-250), Some(249), None])
                        .with_precision_and_scale(8, 2)
                        .unwrap(),
                ),
            ),
        ])),
    );
    // b: values within half a unit of the integer bounds. Row 1 rounds onto
    // the bound, row 2 rounds just past it.
    schema.register_table(
        "b",
        Arc::new(table(vec![
            ("k", DataType::Int32, Arc::new(Int32Array::from(vec![1, 2]))),
            (
                "hi_d",
                DataType::Float64,
                Arc::new(Float64Array::from(vec![2147483647.49, 2147483647.5])),
            ),
            (
                "lo_d",
                DataType::Float64,
                Arc::new(Float64Array::from(vec![-2147483648.49, -2147483648.5])),
            ),
            (
                "hi_r",
                DataType::Float32,
                Arc::new(Float32Array::from(vec![127.25_f32, 127.5])),
            ),
            (
                "hi_m128",
                DataType::Decimal128 {
                    precision: 20,
                    scale: 1,
                },
                Arc::new(
                    Decimal128Array::from(vec![
                        i64::MAX as i128 * 10 + 4,
                        i64::MAX as i128 * 10 + 5,
                    ])
                    .with_precision_and_scale(20, 1)
                    .unwrap(),
                ),
            ),
            (
                "lo_m64",
                DataType::Decimal64 {
                    precision: 5,
                    scale: 1,
                },
                Arc::new(
                    Decimal64Array::from(vec![-1284, -1285])
                        .with_precision_and_scale(5, 1)
                        .unwrap(),
                ),
            ),
            (
                "hi_m64",
                DataType::Decimal64 {
                    precision: 6,
                    scale: 1,
                },
                Arc::new(
                    Decimal64Array::from(vec![327674, 327675])
                        .with_precision_and_scale(6, 1)
                        .unwrap(),
                ),
            ),
        ])),
    );
    // sales: TPC-DS q54's shape, a per-customer sum of a DECIMAL(7, 2)
    // price, stored as Decimal128 like a Hive scan's. Customer 1 sums to
    // 430881.00, customer 2 to 430849.00.
    schema.register_table(
        "sales",
        Arc::new(table(vec![
            (
                "customer",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![1, 1, 1, 1, 1, 2, 2, 2, 2, 2])),
            ),
            (
                "price",
                DataType::Decimal128 {
                    precision: 7,
                    scale: 2,
                },
                Arc::new(
                    Decimal128Array::from(vec![
                        8617620, 8617620, 8617620, 8617620, 8617620, 8616980, 8616980, 8616980,
                        8616980, 8616980,
                    ])
                    .with_precision_and_scale(7, 2)
                    .unwrap(),
                ),
            ),
        ])),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql`; return every row as its cells' text (`NULL` for null) and the
/// Arrow type of each column.
async fn rows(sql: &str) -> (Vec<Vec<String>>, Vec<ArrowDataType>) {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    let mut types = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        types = b.columns().iter().map(|c| c.data_type().clone()).collect();
        for r in 0..b.num_rows() {
            out.push(
                b.columns()
                    .iter()
                    .map(|c| {
                        if c.is_null(r) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(c, r).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    (out, types)
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

#[tokio::test]
async fn decimal_literal_rounds_half_away_from_zero() {
    let (got, types) = rows(
        "SELECT CAST(DECIMAL '2.5' AS INTEGER), CAST(DECIMAL '-2.5' AS INTEGER), \
         CAST(DECIMAL '8617.62' AS INTEGER), CAST(DECIMAL '-8617.62' AS BIGINT)",
    )
    .await;
    assert_eq!(got, vec![row(&["3", "-3", "8618", "-8618"])]);
    assert_eq!(
        types,
        vec![
            ArrowDataType::Int32,
            ArrowDataType::Int32,
            ArrowDataType::Int32,
            ArrowDataType::Int64
        ]
    );
}

#[tokio::test]
async fn decimal_literal_below_half_rounds_toward_zero() {
    let (got, _) = rows(
        "SELECT CAST(DECIMAL '2.49' AS INTEGER), CAST(DECIMAL '-2.49' AS INTEGER), \
         CAST(DECIMAL '8617.42' AS INTEGER)",
    )
    .await;
    assert_eq!(got, vec![row(&["2", "-2", "8617"])]);
}

#[tokio::test]
async fn double_and_real_literals_round_half_away_from_zero() {
    let (got, _) = rows(
        "SELECT CAST(2.5e0 AS INTEGER), CAST(-2.5e0 AS INTEGER), CAST(2.49e0 AS BIGINT), \
         CAST(REAL '2.5' AS INTEGER), CAST(-2.5e0 AS BIGINT)",
    )
    .await;
    assert_eq!(got, vec![row(&["3", "-3", "2", "3", "-3"])]);
}

/// The column path: the cast runs on each batch, not at plan time. The NULL
/// row stays NULL.
#[tokio::test]
async fn decimal_and_double_columns_round_half_away_from_zero() {
    let (got, types) = rows(
        "SELECT CAST(x128 AS INTEGER), CAST(x128 AS BIGINT), CAST(x64 AS INTEGER), \
         CAST(x64 AS BIGINT), CAST(d AS INTEGER), CAST(d AS BIGINT) FROM v ORDER BY k",
    )
    .await;
    assert_eq!(
        got,
        vec![
            row(&["8618", "8618", "8618", "8618", "8618", "8618"]),
            row(&["-8618", "-8618", "-8618", "-8618", "-8618", "-8618"]),
            row(&["8617", "8617", "8617", "8617", "8617", "8617"]),
            row(&["NULL", "NULL", "NULL", "NULL", "NULL", "NULL"]),
        ]
    );
    assert_eq!(
        types,
        vec![
            ArrowDataType::Int32,
            ArrowDataType::Int64,
            ArrowDataType::Int32,
            ArrowDataType::Int64,
            ArrowDataType::Int32,
            ArrowDataType::Int64
        ]
    );
}

#[tokio::test]
async fn real_and_decimal_columns_to_small_integers_round_half_away_from_zero() {
    let (got, types) = rows(
        "SELECT CAST(r AS TINYINT), CAST(r AS SMALLINT), CAST(r AS INTEGER), \
         CAST(s64 AS SMALLINT), CAST(s64 AS TINYINT) FROM v ORDER BY k",
    )
    .await;
    assert_eq!(
        got,
        vec![
            row(&["3", "3", "3", "3", "3"]),
            row(&["-3", "-3", "-3", "-3", "-3"]),
            row(&["2", "2", "2", "2", "2"]),
            row(&["NULL", "NULL", "NULL", "NULL", "NULL"]),
        ]
    );
    assert_eq!(
        types,
        vec![
            ArrowDataType::Int8,
            ArrowDataType::Int16,
            ArrowDataType::Int32,
            ArrowDataType::Int16,
            ArrowDataType::Int8
        ]
    );
}

/// TPC-DS q54's expression shape: a decimal SUM divided by 50, cast to
/// INTEGER, then used as a grouping key.
#[tokio::test]
async fn q54_segment_of_a_decimal_sum_rounds() {
    let (got, _) = rows(
        "SELECT CAST(revenue / 50 AS INTEGER) AS segment, count(*) FROM \
         (SELECT customer, sum(price) AS revenue FROM sales GROUP BY customer) g \
         GROUP BY CAST(revenue / 50 AS INTEGER) ORDER BY segment",
    )
    .await;
    // 430881.00 / 50 = 8617.62 -> 8618; 430849.00 / 50 = 8616.98 -> 8617.
    assert_eq!(got, vec![row(&["8617", "1"]), row(&["8618", "1"])]);
}

/// Run `sql` and return its error text; panics if it succeeds.
async fn error(sql: &str) -> String {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    match arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool).await {
        Ok(_) => panic!("query succeeded, expected an error: {sql}"),
        Err(e) => e.to_string(),
    }
}

/// A value that rounds onto the bound is fine; one that rounds half a unit
/// past it is an out-of-range error, as in Trino. Before, Arrow's truncated
/// result (the bound itself) leaked through.
#[tokio::test]
async fn literals_rounding_past_the_integer_range_are_errors() {
    let (got, _) = rows(
        "SELECT CAST(2147483647.49e0 AS INTEGER), CAST(-2147483648.49e0 AS INTEGER), \
         CAST(DECIMAL '9223372036854775807.4' AS BIGINT), \
         CAST(DECIMAL '-9223372036854775808.4' AS BIGINT)",
    )
    .await;
    assert_eq!(
        got,
        vec![row(&[
            "2147483647",
            "-2147483648",
            "9223372036854775807",
            "-9223372036854775808"
        ])]
    );
    for sql in [
        "SELECT CAST(2147483647.5e0 AS INTEGER)",
        "SELECT CAST(-2147483648.5e0 AS INTEGER)",
        "SELECT CAST(DECIMAL '9223372036854775807.5' AS BIGINT)",
        "SELECT CAST(DECIMAL '-9223372036854775808.5' AS BIGINT)",
        "SELECT CAST(DECIMAL '2147483647.5' AS INTEGER)",
    ] {
        let err = error(sql).await;
        assert!(err.contains("out of range"), "{sql}: {err}");
    }
}

#[tokio::test]
async fn columns_rounding_onto_the_integer_bound_keep_it() {
    let (got, _) = rows(
        "SELECT CAST(hi_d AS INTEGER), CAST(lo_d AS INTEGER), CAST(hi_r AS TINYINT), \
         CAST(hi_m128 AS BIGINT), CAST(lo_m64 AS TINYINT), CAST(hi_m64 AS SMALLINT) \
         FROM b WHERE k = 1",
    )
    .await;
    assert_eq!(
        got,
        vec![row(&[
            "2147483647",
            "-2147483648",
            "127",
            "9223372036854775807",
            "-128",
            "32767"
        ])]
    );
}

#[tokio::test]
async fn columns_rounding_past_the_integer_range_are_errors() {
    for (column, target) in [
        ("hi_d", "INTEGER"),
        ("lo_d", "INTEGER"),
        ("hi_r", "TINYINT"),
        ("hi_m128", "BIGINT"),
        ("lo_m64", "TINYINT"),
        ("hi_m64", "SMALLINT"),
    ] {
        let sql = format!("SELECT CAST({column} AS {target}) FROM b");
        let err = error(&sql).await;
        assert!(err.contains("out of range"), "{sql}: {err}");
    }
    // The review's repro: a decimal column built from literals.
    let sql = "SELECT CAST(m AS TINYINT) FROM \
               (SELECT DECIMAL '127.5' AS m UNION ALL SELECT DECIMAL '-2.5') s";
    let err = error(sql).await;
    assert!(err.contains("out of range"), "{sql}: {err}");
}

/// TRY turns the out-of-range rows into NULL and keeps the others.
#[tokio::test]
async fn try_of_a_cast_rounding_past_the_range_is_null() {
    let (got, _) = rows(
        "SELECT TRY(CAST(hi_d AS INTEGER)), TRY(CAST(lo_d AS INTEGER)), \
         TRY(CAST(hi_r AS TINYINT)), TRY(CAST(hi_m128 AS BIGINT)), \
         TRY(CAST(lo_m64 AS TINYINT)), TRY(CAST(hi_m64 AS SMALLINT)) FROM b ORDER BY k",
    )
    .await;
    assert_eq!(
        got,
        vec![
            row(&[
                "2147483647",
                "-2147483648",
                "127",
                "9223372036854775807",
                "-128",
                "32767"
            ]),
            row(&["NULL", "NULL", "NULL", "NULL", "NULL", "NULL"]),
        ]
    );
}

/// Over a literal the cast would fail at plan-time folding; TRY still
/// yields NULL rather than a planning error.
#[tokio::test]
async fn try_of_a_literal_cast_rounding_past_the_range_is_null() {
    let (got, _) = rows(
        "SELECT TRY(CAST(2147483647.5e0 AS INTEGER)), \
         TRY(CAST(DECIMAL '9223372036854775807.5' AS BIGINT)), \
         TRY(CAST(2147483647.49e0 AS INTEGER))",
    )
    .await;
    assert_eq!(got, vec![row(&["NULL", "NULL", "2147483647"])]);
}
