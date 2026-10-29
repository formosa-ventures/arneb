//! GROUP BY ROLLUP(...) and GROUPING(...), as TPC-DS q05 q14a q18 q22 q27
//! q36 q67 q70 q77 q80 q86 use them. Expected rows were taken from Trino 484.

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

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

const LVL: &str = "grouping(a) * 2 + grouping(b) AS lvl";
const ORDER: &str = "ORDER BY lvl, coalesce(a, -1), coalesce(b, -1)";

#[tokio::test]
async fn rollup_adds_subtotal_and_grand_total_rows() {
    assert_eq!(
        rows(&format!(
            "SELECT a, b, sum(v), count(*), {LVL} FROM s GROUP BY ROLLUP(a, b) {ORDER}"
        ))
        .await,
        table(&[
            &["NULL", "30", "1", "1", "0"],
            &["1", "10", "5", "1", "0"],
            &["1", "20", "7", "1", "0"],
            &["2", "NULL", "4", "1", "0"],
            &["2", "10", "3", "1", "0"],
            &["NULL", "NULL", "1", "1", "1"],
            &["1", "NULL", "12", "2", "1"],
            &["2", "NULL", "7", "2", "1"],
            &["NULL", "NULL", "20", "5", "3"],
        ])
    );
}

#[tokio::test]
async fn grouping_tells_rolled_up_rows_from_data_nulls() {
    assert_eq!(
        rows(&format!(
            "SELECT a, b, grouping(a), grouping(b), grouping(a) + grouping(b), sum(v), {LVL} \
             FROM s GROUP BY ROLLUP(a, b) {ORDER}"
        ))
        .await,
        table(&[
            &["NULL", "30", "0", "0", "0", "1", "0"],
            &["1", "10", "0", "0", "0", "5", "0"],
            &["1", "20", "0", "0", "0", "7", "0"],
            &["2", "NULL", "0", "0", "0", "4", "0"],
            &["2", "10", "0", "0", "0", "3", "0"],
            &["NULL", "NULL", "0", "1", "1", "1", "1"],
            &["1", "NULL", "0", "1", "1", "12", "1"],
            &["2", "NULL", "0", "1", "1", "7", "1"],
            &["NULL", "NULL", "1", "1", "2", "20", "3"],
        ])
    );
}

#[tokio::test]
async fn rank_partitioned_by_grouping_as_in_tpcds_q36() {
    assert_eq!(
        rows(
            "SELECT sum(v) AS sv, a, b, grouping(a) + grouping(b) AS lochierarchy, \
             rank() OVER (PARTITION BY grouping(a) + grouping(b), \
             CASE WHEN grouping(b) = 0 THEN a END ORDER BY sum(v) DESC) AS rk \
             FROM s GROUP BY ROLLUP(a, b) \
             ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN coalesce(a, 9) END, \
             rk, coalesce(b, -1)"
        )
        .await,
        table(&[
            &["20", "NULL", "NULL", "2", "1"],
            &["12", "1", "NULL", "1", "1"],
            &["7", "2", "NULL", "1", "2"],
            &["1", "NULL", "NULL", "1", "3"],
            &["7", "1", "20", "0", "1"],
            &["5", "1", "10", "0", "2"],
            &["4", "2", "NULL", "0", "1"],
            &["3", "2", "10", "0", "2"],
            &["1", "NULL", "30", "0", "1"],
        ])
    );
}

#[tokio::test]
async fn rollup_over_a_union_as_in_tpcds_q05() {
    assert_eq!(
        rows(
            "SELECT channel, id, sum(x), grouping(channel) * 2 + grouping(id) AS lvl FROM \
             (SELECT 'store' AS channel, a AS id, v AS x FROM s \
              UNION ALL SELECT 'web' AS channel, b AS id, v AS x FROM s) t \
             GROUP BY ROLLUP(channel, id) ORDER BY lvl, channel, coalesce(id, -1)"
        )
        .await,
        table(&[
            &["store", "NULL", "1", "0"],
            &["store", "1", "12", "0"],
            &["store", "2", "7", "0"],
            &["web", "NULL", "4", "0"],
            &["web", "10", "8", "0"],
            &["web", "20", "7", "0"],
            &["web", "30", "1", "0"],
            &["store", "NULL", "20", "1"],
            &["web", "NULL", "20", "1"],
            &["NULL", "NULL", "40", "3"],
        ])
    );
}

#[tokio::test]
async fn rollup_over_no_rows_still_has_the_grand_total() {
    assert_eq!(
        rows("SELECT a, sum(v), count(*) FROM s WHERE v > 100 GROUP BY ROLLUP(a)").await,
        table(&[&["NULL", "NULL", "0"]])
    );
}

#[tokio::test]
async fn having_filters_every_rollup_level() {
    assert_eq!(
        rows(&format!(
            "SELECT a, b, sum(v), {LVL} FROM s GROUP BY ROLLUP(a, b) HAVING sum(v) > 6 {ORDER}"
        ))
        .await,
        table(&[
            &["1", "20", "7", "0"],
            &["1", "NULL", "12", "1"],
            &["2", "NULL", "7", "1"],
            &["NULL", "NULL", "20", "3"],
        ])
    );
}

#[tokio::test]
async fn min_max_count_per_rollup_level() {
    assert_eq!(
        rows(&format!(
            "SELECT a, b, min(v), max(v), count(v), {LVL} FROM s WHERE a IS NOT NULL \
             GROUP BY ROLLUP(a, b) {ORDER}"
        ))
        .await,
        table(&[
            &["1", "10", "5", "5", "1", "0"],
            &["1", "20", "7", "7", "1", "0"],
            &["2", "NULL", "4", "4", "1", "0"],
            &["2", "10", "3", "3", "1", "0"],
            &["1", "NULL", "5", "7", "2", "1"],
            &["2", "NULL", "3", "4", "2", "1"],
            &["NULL", "NULL", "3", "7", "4", "3"],
        ])
    );
}
