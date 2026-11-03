//! A simple CASE (`CASE operand WHEN value ...`) compares the operand with
//! each WHEN value, so both must be coerced to a common type as for `=`.
//! `CASE mean WHEN 0 ...` with a DOUBLE `mean` (TPC-DS q39a/q39b) failed at
//! run time with "compare_op received mismatched types Float64 vs Int64".
//! Expected rows were taken from Trino 484.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

/// | k | f    | i    |
/// |---|------|------|
/// | 1 | 1.5  | 3    |
/// | 2 | 0.0  | 2    |
/// | 3 | NULL | NULL |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let fields = vec![
        Field::new("k", ArrowDataType::Int64, false),
        Field::new("f", ArrowDataType::Float64, true),
        Field::new("i", ArrowDataType::Int64, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Float64Array::from(vec![Some(1.5), Some(0.0), None])),
        Arc::new(Int64Array::from(vec![Some(3), Some(2), None])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let column = |name: &str, data_type: DataType, nullable: bool| ColumnInfo {
        name: name.into(),
        data_type,
        nullable,
    };
    let table = Arc::new(MemoryTable::new(
        vec![
            column("k", DataType::Int64, false),
            column("f", DataType::Float64, true),
            column("i", DataType::Int64, true),
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

async fn run(sql: &str) -> Result<Vec<RecordBatch>, String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map(|(_plan, batches)| batches)
        .map_err(|e| e.to_string())
}

/// Run `sql`; return the first column of every row as text (`NULL` for nulls).
async fn column(sql: &str) -> Vec<String> {
    let batches = run(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in &batches {
        let c = b.column(0);
        for r in 0..b.num_rows() {
            out.push(if c.is_null(r) {
                "NULL".to_string()
            } else {
                array_value_to_string(c, r).unwrap()
            });
        }
    }
    out
}

#[tokio::test]
async fn double_operand_with_integer_when_value() {
    assert_eq!(
        column("SELECT CASE f WHEN 0 THEN 'z' ELSE 'nz' END FROM t ORDER BY k").await,
        vec!["nz", "z", "nz"]
    );
}

#[tokio::test]
async fn integer_operand_with_double_when_value() {
    assert_eq!(
        column(
            "SELECT CASE i WHEN CAST(2 AS DOUBLE) THEN 'two' ELSE 'other' END FROM t ORDER BY k"
        )
        .await,
        vec!["other", "two", "other"]
    );
}

#[tokio::test]
async fn decimal_and_integer_both_ways() {
    assert_eq!(
        column("SELECT CASE CAST(1.0 AS DECIMAL(2, 1)) WHEN 1 THEN 'one' ELSE 'other' END").await,
        vec!["one"]
    );
    assert_eq!(
        column("SELECT CASE 1 WHEN CAST(1.0 AS DECIMAL(2, 1)) THEN 'one' ELSE 'other' END").await,
        vec!["one"]
    );
}

#[tokio::test]
async fn several_when_values_of_different_numeric_types() {
    assert_eq!(
        column(
            "SELECT CASE f WHEN 0 THEN 'zero' WHEN CAST(1.5 AS DECIMAL(2, 1)) THEN 'one and a half' \
             ELSE 'other' END FROM t ORDER BY k"
        )
        .await,
        vec!["one and a half", "zero", "other"]
    );
}

#[tokio::test]
async fn as_in_tpcds_q39_over_an_average() {
    // avg() is DOUBLE; `CASE mean WHEN 0 THEN 0 ELSE stdev / mean END`.
    assert_eq!(
        column(
            "SELECT CASE m WHEN 0 THEN 0 ELSE 3 / m END \
             FROM (SELECT k, avg(f) m FROM t GROUP BY k) s ORDER BY k"
        )
        .await,
        vec!["2.0", "0.0", "NULL"]
    );
}

#[tokio::test]
async fn incompatible_when_value_is_an_error() {
    assert!(
        run("SELECT CASE i WHEN 'a' THEN 1 END FROM t")
            .await
            .is_err(),
        "an integer operand cannot be compared with a varchar WHEN value"
    );
}
