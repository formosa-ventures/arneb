//! A CASE branch is evaluated only for the rows that reach it, as in Trino:
//! `CASE WHEN veh > 0 THEN dep / veh END` must not divide by zero on the
//! rows where `veh = 0` (TPC-DS q34 and q73). Expected rows were taken from
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

/// One batch of non-nullable Int32 columns `dep`, `veh`.
fn batch(dep: Vec<i32>, veh: Vec<i32>) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("dep", ArrowDataType::Int32, false),
        Field::new("veh", ArrowDataType::Int32, false),
    ]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(dep)),
        Arc::new(Int32Array::from(veh)),
    ];
    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

/// | dep | veh |
/// |-----|-----|
/// | 3   | 2   |
/// | 5   | 0   |
/// | 2   | 1   |
/// | 4   | 0   |
///
/// Split over two batches.
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let columns = ["dep", "veh"]
        .iter()
        .map(|n| ColumnInfo {
            name: n.to_string(),
            data_type: DataType::Int32,
            nullable: false,
        })
        .collect();
    let table = MemoryTable::new(
        columns,
        vec![batch(vec![3, 5], vec![2, 0]), batch(vec![2, 4], vec![1, 0])],
    );
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("h", Arc::new(table));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

async fn run(sql: &str) -> Result<Vec<Vec<String>>, String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map_err(|e| e.to_string())?;
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
    Ok(out)
}

async fn rows(sql: &str) -> Vec<Vec<String>> {
    run(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[tokio::test]
async fn guarded_decimal_division_in_a_filter_as_in_tpcds_q34() {
    assert_eq!(
        rows(
            "SELECT dep FROM h WHERE (CASE WHEN veh > 0 \
             THEN CAST(dep AS DECIMAL(7,2)) / veh ELSE NULL END) > DECIMAL '1.2' \
             ORDER BY dep"
        )
        .await,
        table(&[&["2"], &["3"]])
    );
}

#[tokio::test]
async fn guarded_integer_division_in_a_projection() {
    assert_eq!(
        rows("SELECT dep, CASE WHEN veh <> 0 THEN dep / veh ELSE -1 END FROM h ORDER BY dep").await,
        table(&[&["2", "2"], &["3", "1"], &["4", "-1"], &["5", "-1"]])
    );
}

#[tokio::test]
async fn else_branch_skips_rows_a_simple_case_matched() {
    assert_eq!(
        rows("SELECT dep, CASE veh WHEN 0 THEN NULL ELSE dep / veh END FROM h ORDER BY dep").await,
        table(&[&["2", "2"], &["3", "1"], &["4", "NULL"], &["5", "NULL"]])
    );
}

#[tokio::test]
async fn later_when_condition_skips_rows_an_earlier_one_matched() {
    assert_eq!(
        rows(
            "SELECT dep, CASE WHEN veh = 0 THEN 0 WHEN dep / veh > 1 THEN 1 ELSE 2 END \
             FROM h ORDER BY dep"
        )
        .await,
        table(&[&["2", "1"], &["3", "2"], &["4", "0"], &["5", "0"]])
    );
}

#[tokio::test]
async fn guarded_case_nested_in_arithmetic() {
    assert_eq!(
        rows("SELECT dep, CASE WHEN veh = 0 THEN 0 ELSE dep / veh END + 1 FROM h ORDER BY dep")
            .await,
        table(&[&["2", "3"], &["3", "2"], &["4", "1"], &["5", "1"]])
    );
}

#[tokio::test]
async fn division_by_zero_in_a_taken_branch_still_fails() {
    let res = run("SELECT dep, CASE WHEN veh = 0 THEN dep / veh END FROM h ORDER BY dep").await;
    let err = res.expect_err("dividing by zero on the rows that take the branch must fail");
    assert!(
        err.to_lowercase().contains("divi"),
        "unexpected error: {err}"
    );
}
