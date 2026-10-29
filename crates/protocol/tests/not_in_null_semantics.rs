//! `x NOT IN (subquery)` under SQL three-valued logic. A NULL `x` is unknown
//! unless the subquery is empty, and a NULL in the subquery makes every
//! non-matching `x` unknown. Expected counts were taken from Trino 484.

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

/// `SELECT count(*) FROM s WHERE x NOT IN (<subquery>)`.
async fn not_in_count(subquery: &str) -> Vec<Vec<String>> {
    rows(&format!(
        "SELECT count(*) FROM s WHERE x NOT IN ({subquery})"
    ))
    .await
}

#[tokio::test]
async fn null_outer_key_is_dropped_when_the_subquery_has_rows() {
    assert_eq!(not_in_count("SELECT y FROM u").await, vec![vec!["2"]]);
}

#[tokio::test]
async fn null_outer_key_is_kept_when_the_subquery_is_empty() {
    assert_eq!(not_in_count("SELECT y FROM e").await, vec![vec!["4"]]);
}

#[tokio::test]
async fn null_outer_key_is_kept_when_a_filter_empties_the_subquery() {
    assert_eq!(
        not_in_count("SELECT y FROM u WHERE y > 5").await,
        vec![vec!["4"]]
    );
}

#[tokio::test]
async fn a_null_in_the_subquery_drops_every_row() {
    assert_eq!(not_in_count("SELECT y FROM t").await, vec![vec!["0"]]);
}

#[tokio::test]
async fn a_subquery_of_only_nulls_drops_every_row() {
    assert_eq!(not_in_count("SELECT y FROM n").await, vec![vec!["0"]]);
}

#[tokio::test]
async fn filtering_the_nulls_out_of_the_subquery_restores_the_rows() {
    assert_eq!(
        not_in_count("SELECT y FROM t WHERE y IS NOT NULL").await,
        vec![vec!["2"]]
    );
}

#[tokio::test]
async fn in_is_unchanged_by_a_null_in_the_subquery() {
    assert_eq!(
        rows("SELECT count(*) FROM s WHERE x IN (SELECT y FROM t)").await,
        vec![vec!["1"]]
    );
}
