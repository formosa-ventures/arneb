//! `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` window frames, as in
//! TPC-DS q51. Unlike the default RANGE frame, a ROWS frame stops at the
//! current row even when later rows tie on the ORDER BY key. Expected rows
//! were taken from Trino 484.

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

/// `t(p, d, id, v)`; `d` ties within each partition and `id` breaks them.
///
/// | p | d | id | v    |
/// |---|---|----|------|
/// | 1 | 1 | 1  | 10   |
/// | 1 | 2 | 2  | 20   |
/// | 1 | 2 | 3  | 5    |
/// | 1 | 3 | 4  | NULL |
/// | 1 | 4 | 5  | 7    |
/// | 2 | 1 | 6  | 3    |
/// | 2 | 1 | 7  | 4    |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "t",
        Arc::new(int_table(&[
            ("p", [1, 1, 1, 1, 1, 2, 2].map(Some).to_vec(), false),
            ("d", [1, 2, 2, 3, 4, 1, 1].map(Some).to_vec(), false),
            ("id", [1, 2, 3, 4, 5, 6, 7].map(Some).to_vec(), false),
            (
                "v",
                vec![Some(10), Some(20), Some(5), None, Some(7), Some(3), Some(4)],
                true,
            ),
        ])),
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
async fn running_sum_over_a_rows_frame() {
    assert_eq!(
        rows(
            "SELECT id, sum(v) OVER (PARTITION BY p ORDER BY d, id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id"
        )
        .await,
        table(&[
            &["1", "10"],
            &["2", "30"],
            &["3", "35"],
            &["4", "35"],
            &["5", "42"],
            &["6", "3"],
            &["7", "7"],
        ])
    );
}

#[tokio::test]
async fn explicit_range_frame_still_includes_tied_rows() {
    assert_eq!(
        rows(
            "SELECT id, sum(v) OVER (PARTITION BY p ORDER BY d \
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id"
        )
        .await,
        table(&[
            &["1", "10"],
            &["2", "35"],
            &["3", "35"],
            &["4", "35"],
            &["5", "42"],
            &["6", "7"],
            &["7", "7"],
        ])
    );
}

#[tokio::test]
async fn default_frame_is_unchanged() {
    assert_eq!(
        rows("SELECT id, sum(v) OVER (PARTITION BY p ORDER BY d) FROM t ORDER BY id").await,
        table(&[
            &["1", "10"],
            &["2", "35"],
            &["3", "35"],
            &["4", "35"],
            &["5", "42"],
            &["6", "7"],
            &["7", "7"],
        ])
    );
}

#[tokio::test]
async fn running_max_over_the_short_rows_form() {
    assert_eq!(
        rows(
            "SELECT id, max(v) OVER (PARTITION BY p ORDER BY d, id ROWS UNBOUNDED PRECEDING) \
             FROM t ORDER BY id"
        )
        .await,
        table(&[
            &["1", "10"],
            &["2", "20"],
            &["3", "20"],
            &["4", "20"],
            &["5", "20"],
            &["6", "3"],
            &["7", "4"],
        ])
    );
}

#[tokio::test]
async fn running_count_skips_nulls_over_a_rows_frame() {
    assert_eq!(
        rows(
            "SELECT id, count(v) OVER (PARTITION BY p ORDER BY d, id \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t ORDER BY id"
        )
        .await,
        table(&[
            &["1", "1"],
            &["2", "2"],
            &["3", "3"],
            &["4", "3"],
            &["5", "4"],
            &["6", "1"],
            &["7", "2"],
        ])
    );
}

#[tokio::test]
async fn rows_frame_over_an_aggregate_as_in_tpcds_q51() {
    assert_eq!(
        rows(
            "SELECT p, d, sum(sum(v)) OVER (PARTITION BY p ORDER BY d \
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
             FROM t GROUP BY p, d ORDER BY p, d"
        )
        .await,
        table(&[
            &["1", "1", "10"],
            &["1", "2", "35"],
            &["1", "3", "35"],
            &["1", "4", "42"],
            &["2", "1", "7"],
        ])
    );
}
