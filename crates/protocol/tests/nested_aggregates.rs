//! Aggregates nested inside CAST, scalar functions and CASE, e.g.
//! `CAST(sum(x) AS DECIMAL(15,4))` (TPC-DS q49) or `abs(sum(x))`. The query
//! must still be planned as an aggregation. Expected rows were taken from
//! Trino 484.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// | g | x    |
/// |---|------|
/// | 1 | 1    |
/// | 1 | 2    |
/// | 2 | -5   |
/// | 3 | NULL |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let fields = vec![
        Field::new("g", ArrowDataType::Int32, false),
        Field::new("x", ArrowDataType::Int32, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 1, 2, 3])),
        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(-5), None])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "g".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "x".into(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ],
        vec![batch],
    ));
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", table);
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run `sql`; return every cell as text (`NULL` for nulls).
async fn rows(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
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
    out
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[tokio::test]
async fn cast_of_an_aggregate_without_group_by() {
    assert_eq!(
        rows("SELECT CAST(sum(x) AS BIGINT) FROM t").await,
        table(&[&["-2"]])
    );
}

#[tokio::test]
async fn cast_of_an_aggregate_per_group() {
    assert_eq!(
        rows("SELECT g, CAST(sum(x) AS DOUBLE) FROM t GROUP BY g ORDER BY g").await,
        table(&[&["1", "3.0"], &["2", "-5.0"], &["3", "NULL"]])
    );
}

#[tokio::test]
async fn scalar_function_of_an_aggregate() {
    assert_eq!(
        rows("SELECT g, abs(sum(x)) FROM t GROUP BY g ORDER BY g").await,
        table(&[&["1", "3"], &["2", "5"], &["3", "NULL"]])
    );
    assert_eq!(
        rows("SELECT g, coalesce(max(x), 0) FROM t GROUP BY g ORDER BY g").await,
        table(&[&["1", "2"], &["2", "-5"], &["3", "0"]])
    );
}

#[tokio::test]
async fn case_over_an_aggregate() {
    assert_eq!(
        rows(
            "SELECT g, CASE WHEN sum(x) > 2 THEN 'big' ELSE 'small' END \
             FROM t GROUP BY g ORDER BY g"
        )
        .await,
        table(&[&["1", "big"], &["2", "small"], &["3", "small"]])
    );
}

#[tokio::test]
async fn ratio_of_cast_aggregates_as_in_tpcds_q49() {
    assert_eq!(
        rows(
            "SELECT t.g, CAST(CAST(sum(t.x) AS DECIMAL(15,4)) / \
             CAST(count(t.x) AS DECIMAL(15,4)) AS DOUBLE) \
             FROM t GROUP BY t.g ORDER BY t.g"
        )
        .await,
        table(&[&["1", "1.5"], &["2", "-5.0"], &["3", "NULL"]])
    );
}

#[tokio::test]
async fn group_key_combined_with_a_cast_aggregate() {
    assert_eq!(
        rows("SELECT g + CAST(sum(x) AS INTEGER) FROM t GROUP BY g ORDER BY 1 NULLS LAST").await,
        table(&[&["-3"], &["4"], &["NULL"]])
    );
}

#[tokio::test]
async fn cast_aggregate_in_having() {
    assert_eq!(
        rows("SELECT g FROM t GROUP BY g HAVING CAST(sum(x) AS BIGINT) > 0 ORDER BY g").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn function_of_an_aggregate_in_order_by() {
    assert_eq!(
        rows("SELECT g FROM t GROUP BY g ORDER BY abs(sum(x)) DESC NULLS LAST").await,
        table(&[&["2"], &["1"], &["3"]])
    );
}

#[tokio::test]
async fn non_aggregated_column_beside_a_cast_aggregate_is_an_error() {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let res = arneb_protocol::__private::execute_query(
        "SELECT CAST(x AS BIGINT), CAST(sum(x) AS BIGINT) FROM t",
        &cm,
        &cr,
        None,
        &pool,
    )
    .await;
    assert!(
        res.is_err(),
        "x is neither grouped nor aggregated, so the query must be rejected"
    );
}
