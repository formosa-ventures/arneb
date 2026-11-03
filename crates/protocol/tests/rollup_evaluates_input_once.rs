//! GROUP BY ROLLUP evaluates its input once, as Trino does, instead of once
//! per grouping level. Measured at SF1: TPC-DS q14a takes 9.9 s with
//! ROLLUP(4 keys) against 2.1 s for a plain GROUP BY of the same keys (Trino:
//! 2.3 s and 2.6 s), and q77 2.2 s against 1.1 s; arneb's plans scan every
//! input table once per level (q77 3x, q14a 20x per fact table). The rows
//! themselves are covered by group_by_rollup.rs.
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

/// | a    | b    | v |
/// |------|------|---|
/// | 1    | 10   | 5 |
/// | 1    | 20   | 7 |
/// | 2    | 10   | 3 |
/// | 2    | NULL | 4 |
/// | NULL | 30   | 1 |
///
/// `a` and `b` hold NULLs of their own, so only GROUPING() tells a rolled-up
/// row from a data NULL.
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let cols: [(&str, Vec<Option<i32>>, bool); 3] = [
        ("a", vec![Some(1), Some(1), Some(2), Some(2), None], true),
        (
            "b",
            vec![Some(10), Some(20), Some(10), None, Some(30)],
            true,
        ),
        (
            "v",
            vec![Some(5), Some(7), Some(3), Some(4), Some(1)],
            false,
        ),
    ];
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _, nullable)| Field::new(*n, ArrowDataType::Int32, *nullable))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v, _)| Arc::new(Int32Array::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let table = MemoryTable::new(
        cols.iter()
            .map(|(n, _, nullable)| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: *nullable,
            })
            .collect(),
        vec![batch],
    );
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("s", Arc::new(table));
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

/// How many times the plan of `sql` scans `table`.
async fn scans_of(sql: &str, table: &str) -> usize {
    let needle = format!("TableScan: {table} ");
    rows(&format!("EXPLAIN {sql}"))
        .await
        .iter()
        .flatten()
        .map(|line| line.matches(&needle).count())
        .sum()
}

#[tokio::test]
async fn plain_group_by_scans_its_table_once() {
    assert_eq!(
        scans_of("SELECT a, b, sum(v) FROM s GROUP BY a, b", "s").await,
        1
    );
}

#[tokio::test]
async fn rollup_scans_its_table_once() {
    assert_eq!(
        scans_of("SELECT a, b, sum(v) FROM s GROUP BY ROLLUP(a, b)", "s").await,
        1
    );
}

#[tokio::test]
async fn rollup_over_a_union_scans_each_branch_once() {
    let sql = "SELECT k, sum(v) FROM (SELECT a k, v FROM s UNION ALL SELECT b k, v FROM s) u \
               GROUP BY ROLLUP(k)";
    assert_eq!(scans_of(sql, "s").await, 2);
}
