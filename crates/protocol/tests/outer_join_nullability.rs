//! Outer joins over columns a connector declares NOT NULL. The side of an
//! outer join that can go unmatched is null-extended, so its columns must be
//! nullable in the join's output even when the table declares them non-null.

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

/// A table whose Int32 columns are all declared non-nullable.
fn not_null_table(cols: &[(&str, Vec<i32>)]) -> Arc<MemoryTable> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, ArrowDataType::Int32, false))
        .collect();
    let arrays: Vec<ArrayRef> = cols
        .iter()
        .map(|(_, v)| Arc::new(Int32Array::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    Arc::new(MemoryTable::new(
        cols.iter()
            .map(|(n, _)| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect(),
        vec![batch],
    ))
}

/// | a.x | a.v | | b.x | b.y |
/// |-----|-----| |-----|-----|
/// | 1   | 100 | | 1   | 10  |
/// | 2   | 200 | | 4   | 40  |
/// | 3   | 300 |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "a",
        not_null_table(&[("x", vec![1, 2, 3]), ("v", vec![100, 200, 300])]),
    );
    schema.register_table(
        "b",
        not_null_table(&[("x", vec![1, 4]), ("y", vec![10, 40])]),
    );
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
async fn left_join_null_extends_not_null_right_columns() {
    assert_eq!(
        rows("SELECT a.x, b.y FROM a LEFT JOIN b ON a.x = b.x ORDER BY a.x").await,
        table(&[&["1", "10"], &["2", "NULL"], &["3", "NULL"]])
    );
}

#[tokio::test]
async fn left_join_without_sort_or_projection_on_top() {
    assert_eq!(
        rows("SELECT count(b.y), count(*) FROM a LEFT JOIN b ON a.x = b.x").await,
        table(&[&["1", "3"]])
    );
}

#[tokio::test]
async fn right_join_null_extends_not_null_left_columns() {
    assert_eq!(
        rows("SELECT a.v, b.x FROM a RIGHT JOIN b ON a.x = b.x ORDER BY b.x").await,
        table(&[&["100", "1"], &["NULL", "4"]])
    );
}

#[tokio::test]
async fn full_join_null_extends_both_sides() {
    assert_eq!(
        rows(
            "SELECT a.x, b.x FROM a FULL JOIN b ON a.x = b.x \
             ORDER BY coalesce(a.x, b.x)"
        )
        .await,
        table(&[&["1", "1"], &["2", "NULL"], &["3", "NULL"], &["NULL", "4"]])
    );
}

#[tokio::test]
async fn left_join_with_non_equi_condition() {
    // No equi key: runs as a nested-loop join rather than a hash join.
    assert_eq!(
        rows("SELECT a.x, b.y FROM a LEFT JOIN b ON a.x > b.x + 1 ORDER BY a.x").await,
        table(&[&["1", "NULL"], &["2", "NULL"], &["3", "10"]])
    );
}

#[tokio::test]
async fn null_extended_column_through_group_by() {
    assert_eq!(
        rows(
            "SELECT b.y, count(*) FROM a LEFT JOIN b ON a.x = b.x \
             GROUP BY b.y ORDER BY b.y NULLS LAST"
        )
        .await,
        table(&[&["10", "1"], &["NULL", "2"]])
    );
}

#[tokio::test]
async fn inner_join_over_not_null_columns_still_works() {
    assert_eq!(
        rows("SELECT a.x, b.y FROM a JOIN b ON a.x = b.x ORDER BY a.x").await,
        table(&[&["1", "10"]])
    );
}

#[tokio::test]
async fn order_by_after_a_wildcard_does_not_take_another_tables_qualifier() {
    // `t.*` expands to two expressions, so `b.x` is the third expression but
    // the second select item. Pairing items with expressions by position
    // would label `t.x` as `b.x` and sort by the wrong column.
    let sql = "WITH t AS (SELECT v AS w, x FROM a) \
               SELECT t.*, b.x FROM t, b ORDER BY b.x + 0, 1";
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    if let Ok((_plan, batches)) =
        arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool).await
    {
        let b_x: Vec<String> = batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(move |r| (batch, r)))
            .map(|(batch, r)| array_value_to_string(batch.column(2), r).unwrap())
            .collect();
        assert_eq!(
            b_x,
            ["1", "1", "1", "4", "4", "4"],
            "sorted by the wrong column"
        );
    }
}
