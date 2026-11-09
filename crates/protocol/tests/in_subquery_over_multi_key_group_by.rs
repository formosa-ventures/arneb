//! `x IN (SELECT k FROM s GROUP BY k, v)` keeps each outer row at most once.
//! The subquery is unique on (k, v), not on k alone, so an outer row whose
//! key appears under several `v` must still be returned once. TPC-DS q23a/q23b
//! filter `cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)` where
//! frequent_ss_items groups by (itemdesc, item_sk, solddate); at SF30 each
//! item_sk appears ~79 times there and q23a's sum came out ~95x too large.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
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
    // t: outer rows; x = 1 has three (k, v) groups in s, x = 2 one, x = 3 none,
    // x = NULL never matches.
    schema.register_table(
        "t",
        Arc::new(table(vec![
            (
                "x",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None])),
            ),
            (
                "amount",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ),
        ])),
    );
    schema.register_table(
        "s",
        Arc::new(table(vec![
            (
                "k",
                DataType::Int32,
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                ])),
            ),
            (
                "v",
                DataType::Utf8,
                Arc::new(StringArray::from(vec!["a", "b", "c", "a", "a", "a"])),
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

/// Run `sql`; return every row as its cells' text (`NULL` for null).
async fn rows(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut out = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
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
    out
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

#[tokio::test]
async fn in_over_a_two_key_group_by_counts_each_outer_row_once() {
    assert_eq!(
        rows("SELECT count(*), sum(amount) FROM t WHERE x IN (SELECT k FROM s GROUP BY k, v)")
            .await,
        vec![row(&["2", "30"])]
    );
}

#[tokio::test]
async fn in_over_a_two_key_group_by_with_having_counts_each_outer_row_once() {
    // (1, 'a') has 2 rows; HAVING count(*) >= 1 keeps every group, so k = 1
    // still appears under three v values.
    assert_eq!(
        rows(
            "SELECT x, amount FROM t WHERE x IN \
             (SELECT k FROM s GROUP BY k, v HAVING count(*) >= 1) ORDER BY x"
        )
        .await,
        vec![row(&["1", "10"]), row(&["2", "20"])]
    );
}

#[tokio::test]
async fn in_over_a_group_by_on_the_key_alone_still_matches_once() {
    assert_eq!(
        rows("SELECT count(*), sum(amount) FROM t WHERE x IN (SELECT k FROM s GROUP BY k)").await,
        vec![row(&["2", "30"])]
    );
}

#[tokio::test]
async fn in_over_a_two_key_group_by_in_a_join_shape_like_q23() {
    // q23's shape: the IN filter sits on a fact row that is also joined.
    assert_eq!(
        rows(
            "SELECT sum(t.amount) FROM t, (SELECT 1 AS one) o \
             WHERE t.x IN (SELECT k FROM s GROUP BY v, k) AND o.one = 1"
        )
        .await,
        vec![row(&["30"])]
    );
}
