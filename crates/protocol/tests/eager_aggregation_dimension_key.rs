//! GROUP BY a dimension's join key with aggregates over the fact side, the
//! shape eager aggregation pre-aggregates below the join (TPC-DS q77's
//! `ss` CTE). Expected rows were taken from Trino 484.

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

fn int_table(columns: &[(&str, Vec<Option<i32>>, bool)]) -> MemoryTable {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(n, _, nullable)| Field::new(*n, ArrowDataType::Int32, *nullable))
        .collect();
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .map(|(_, v, _)| Arc::new(Int32Array::from(v.clone())) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    MemoryTable::new(
        columns
            .iter()
            .map(|(n, _, nullable)| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: *nullable,
            })
            .collect(),
        vec![batch],
    )
}

/// - `f(fk, amt)`: (1,10) (1,20) (2,5) (NULL,7) (9,100); key 9 has no
///   dimension row and keys 3, 4 have no facts.
/// - `d(dk)`: 1, 2, 3, 4
/// - `d2(dk)`: 1, 1, 2 (a dimension with a duplicated key)
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "f",
        Arc::new(int_table(&[
            ("fk", vec![Some(1), Some(1), Some(2), None, Some(9)], true),
            (
                "amt",
                vec![Some(10), Some(20), Some(5), Some(7), Some(100)],
                false,
            ),
        ])),
    );
    schema.register_table(
        "d",
        Arc::new(int_table(&[(
            "dk",
            vec![Some(1), Some(2), Some(3), Some(4)],
            false,
        )])),
    );
    schema.register_table(
        "d2",
        Arc::new(int_table(&[("dk", vec![Some(1), Some(1), Some(2)], false)])),
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
async fn fact_joined_to_dimension_grouped_by_dimension_key() {
    assert_eq!(
        rows("SELECT dk, sum(amt) FROM f JOIN d ON fk = dk GROUP BY dk ORDER BY dk").await,
        table(&[&["1", "30"], &["2", "5"]])
    );
}

#[tokio::test]
async fn dimension_joined_to_fact_grouped_by_dimension_key() {
    assert_eq!(
        rows("SELECT dk, sum(amt) FROM d JOIN f ON dk = fk GROUP BY dk ORDER BY dk").await,
        table(&[&["1", "30"], &["2", "5"]])
    );
}

#[tokio::test]
async fn comma_join_grouped_by_dimension_key_as_in_tpcds_q77() {
    assert_eq!(
        rows("SELECT dk, sum(amt) FROM f, d WHERE fk = dk GROUP BY dk ORDER BY dk").await,
        table(&[&["1", "30"], &["2", "5"]])
    );
}

#[tokio::test]
async fn dimension_left_join_keeps_keys_without_facts() {
    assert_eq!(
        rows(
            "SELECT dk, sum(amt), count(amt), min(amt), max(amt) \
             FROM d LEFT JOIN f ON dk = fk GROUP BY dk ORDER BY dk"
        )
        .await,
        table(&[
            &["1", "30", "2", "10", "20"],
            &["2", "5", "1", "5", "5"],
            &["3", "NULL", "0", "NULL", "NULL"],
            &["4", "NULL", "0", "NULL", "NULL"],
        ])
    );
}

#[tokio::test]
async fn duplicated_dimension_key_counts_each_match() {
    assert_eq!(
        rows("SELECT dk, sum(amt) FROM f JOIN d2 ON fk = dk GROUP BY dk ORDER BY dk").await,
        table(&[&["1", "60"], &["2", "5"]])
    );
}

#[tokio::test]
async fn fact_filter_applies_before_grouping() {
    assert_eq!(
        rows(
            "SELECT dk, sum(amt) FROM f JOIN d ON fk = dk WHERE amt > 5 \
             GROUP BY dk ORDER BY dk"
        )
        .await,
        table(&[&["1", "30"]])
    );
}
