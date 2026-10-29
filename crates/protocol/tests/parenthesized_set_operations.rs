//! Set operations over parenthesized query bodies, `(SELECT ...) UNION ALL
//! (SELECT ...)`, as in TPC-DS q02 (UNION ALL), q08 (INTERSECT) and q87
//! (EXCEPT). Expected rows were taken from Trino 484.

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

/// - `x(a)`: 1, 1, 2, 3, NULL
/// - `y(a)`: 2, 2, 4, NULL
/// - `z(a)`: 3
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "x",
        Arc::new(int_table(&[(
            "a",
            vec![Some(1), Some(1), Some(2), Some(3), None],
            true,
        )])),
    );
    schema.register_table(
        "y",
        Arc::new(int_table(&[(
            "a",
            vec![Some(2), Some(2), Some(4), None],
            true,
        )])),
    );
    schema.register_table("z", Arc::new(int_table(&[("a", vec![Some(3)], true)])));
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

/// The first column of every row.
async fn column(sql: &str) -> Vec<String> {
    rows(sql).await.into_iter().map(|r| r[0].clone()).collect()
}

#[tokio::test]
async fn union_all_of_parenthesized_bodies_as_in_tpcds_q02() {
    assert_eq!(
        column(
            "SELECT a FROM ((SELECT a FROM x) UNION ALL (SELECT a FROM y)) u \
             ORDER BY a NULLS LAST"
        )
        .await,
        ["1", "1", "2", "2", "2", "3", "4", "NULL", "NULL"]
    );
}

#[tokio::test]
async fn chained_except_of_parenthesized_bodies_as_in_tpcds_q87() {
    assert_eq!(
        column(
            "SELECT count(*) FROM ((SELECT a FROM x) EXCEPT (SELECT a FROM y) \
             EXCEPT (SELECT a FROM z)) e"
        )
        .await,
        ["1"]
    );
}

#[tokio::test]
async fn except_drops_duplicates_and_matches_nulls() {
    assert_eq!(
        column(
            "SELECT a FROM ((SELECT a FROM x) EXCEPT (SELECT a FROM y)) e \
             ORDER BY a NULLS LAST"
        )
        .await,
        ["1", "3"]
    );
}

#[tokio::test]
async fn intersect_of_parenthesized_bodies_as_in_tpcds_q08() {
    assert_eq!(
        column(
            "SELECT a FROM ((SELECT a FROM x) INTERSECT (SELECT a FROM y)) i \
             ORDER BY a NULLS LAST"
        )
        .await,
        ["2", "NULL"]
    );
}

#[tokio::test]
async fn order_by_and_limit_inside_each_parenthesized_body() {
    assert_eq!(
        column(
            "SELECT a FROM ((SELECT a FROM x ORDER BY a NULLS LAST LIMIT 2) \
             UNION ALL (SELECT a FROM y ORDER BY a DESC NULLS LAST LIMIT 1)) l \
             ORDER BY a"
        )
        .await,
        ["1", "1", "4"]
    );
}

#[tokio::test]
async fn distinct_inside_parenthesized_bodies_keeps_union_all_duplicates() {
    assert_eq!(
        column(
            "SELECT a FROM ((SELECT DISTINCT a FROM x) UNION ALL \
             (SELECT DISTINCT a FROM y)) d ORDER BY a NULLS LAST"
        )
        .await,
        ["1", "2", "2", "3", "4", "NULL", "NULL"]
    );
}

#[tokio::test]
async fn top_level_union_of_parenthesized_bodies() {
    assert_eq!(
        column("(SELECT a FROM x) UNION (SELECT a FROM y) ORDER BY a NULLS LAST").await,
        ["1", "2", "3", "4", "NULL"]
    );
}

#[tokio::test]
async fn aggregate_over_union_all_of_parenthesized_bodies() {
    assert_eq!(
        rows(
            "SELECT s, sum(a) FROM ((SELECT 'x' s, a FROM x) UNION ALL \
             (SELECT 'y' s, a FROM y)) g GROUP BY s ORDER BY s"
        )
        .await,
        vec![
            vec!["x".to_string(), "7".to_string()],
            vec!["y".to_string(), "8".to_string()],
        ]
    );
}
