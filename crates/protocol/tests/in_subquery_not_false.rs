//! `x IN (subquery)` where the planner needs the rows on which it is not
//! FALSE (#230): `x NOT IN (...) OR ...` and `NOT (x IN (...) AND ...)`.
//! Expected rows follow SQL three-valued logic, as in Trino: a NULL `x` is
//! unknown unless the subquery is empty, and a NULL in the subquery makes
//! every non-matching `x` unknown.

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

fn int_table(column: &str, values: Vec<Option<i32>>, nullable: bool) -> MemoryTable {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        ArrowDataType::Int32,
        nullable,
    )]));
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    MemoryTable::new(
        vec![ColumnInfo {
            name: column.to_string(),
            data_type: DataType::Int32,
            nullable,
        }],
        vec![batch],
    )
}

/// - `s(x)`: 1, 2, 2, NULL
/// - `u(y)`: 1, 1 (not nullable)
/// - `t(y)`: 1, NULL
/// - `n(y)`: NULL, NULL
/// - `e(y)`: no rows
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "s",
        Arc::new(int_table("x", vec![Some(1), Some(2), Some(2), None], true)),
    );
    schema.register_table("u", Arc::new(int_table("y", vec![Some(1), Some(1)], false)));
    schema.register_table("t", Arc::new(int_table("y", vec![Some(1), None], true)));
    schema.register_table("n", Arc::new(int_table("y", vec![None, None], true)));
    schema.register_table("e", Arc::new(int_table("y", vec![], true)));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let registry = ConnectorRegistry::new();
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

/// The `x` values `sql` returns, sorted, `NULL` last.
async fn xs(sql: &str) -> Vec<String> {
    let mut got: Vec<String> = rows(sql).await.into_iter().map(|r| r[0].clone()).collect();
    got.sort_by_key(|v| (v == "NULL", v.clone()));
    got
}

fn expect(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

/// `x NOT IN (S) OR x = 2`: a 2 is kept even when `NOT IN` is NULL.
#[tokio::test]
async fn not_in_or_a_predicate() {
    let sql = |sub: &str| format!("SELECT x FROM s WHERE x NOT IN ({sub}) OR x = 2");
    assert_eq!(xs(&sql("SELECT y FROM u")).await, expect(&["2", "2"]));
    assert_eq!(xs(&sql("SELECT y FROM t")).await, expect(&["2", "2"]));
    assert_eq!(xs(&sql("SELECT y FROM n")).await, expect(&["2", "2"]));
    assert_eq!(
        xs(&sql("SELECT y FROM e")).await,
        expect(&["1", "2", "2", "NULL"])
    );
}

/// `x NOT IN (S) OR x IS NULL`: the NULL `x` comes through the OR's second
/// branch when S has rows, and only once (through the first) when S is
/// empty.
#[tokio::test]
async fn not_in_or_is_null_keeps_the_null_row_once() {
    let sql = |sub: &str| format!("SELECT x FROM s WHERE x NOT IN ({sub}) OR x IS NULL");
    assert_eq!(
        xs(&sql("SELECT y FROM u")).await,
        expect(&["2", "2", "NULL"])
    );
    assert_eq!(xs(&sql("SELECT y FROM t")).await, expect(&["NULL"]));
    assert_eq!(xs(&sql("SELECT y FROM n")).await, expect(&["NULL"]));
    assert_eq!(
        xs(&sql("SELECT y FROM e")).await,
        expect(&["1", "2", "2", "NULL"])
    );
}

/// `NOT (x IN (S) AND x > 1)`: kept where the conjunction is FALSE.
#[tokio::test]
async fn not_of_in_and_a_predicate() {
    let sql = |sub: &str| format!("SELECT x FROM s WHERE NOT (x IN ({sub}) AND x > 1)");
    assert_eq!(xs(&sql("SELECT y FROM u")).await, expect(&["1", "2", "2"]));
    assert_eq!(xs(&sql("SELECT y FROM t")).await, expect(&["1"]));
    assert_eq!(xs(&sql("SELECT y FROM n")).await, expect(&["1"]));
    assert_eq!(
        xs(&sql("SELECT y FROM e")).await,
        expect(&["1", "2", "2", "NULL"])
    );
}

/// The same shapes over a subquery that a filter empties at run time.
#[tokio::test]
async fn a_subquery_emptied_by_a_filter() {
    assert_eq!(
        xs("SELECT x FROM s WHERE x NOT IN (SELECT y FROM u WHERE y > 5) OR x IS NULL").await,
        expect(&["1", "2", "2", "NULL"])
    );
}

/// A non-nullable outer column over a nullable subquery.
#[tokio::test]
async fn a_non_nullable_outer_column() {
    assert_eq!(
        xs("SELECT y FROM u WHERE y NOT IN (SELECT y FROM t) OR y = 1").await,
        expect(&["1", "1"])
    );
    assert_eq!(
        xs("SELECT y FROM u WHERE NOT (y IN (SELECT y FROM n) AND y > 0)").await,
        expect(&[])
    );
}
