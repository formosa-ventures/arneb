//! Filters over a UNION ALL view whose branches tag rows with a constant
//! (TPC-DS q04/q11/q74 `year_total`), executed end to end: pruning branches
//! must never change the rows, including when a nested UNION ALL loses every
//! branch.

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

/// s_tab = (1,10) (2,20); c_tab = (1,30) (3,40); w_tab = (2,50) (3,60).
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    for (name, ids, vs) in [
        ("s_tab", [1, 2], [10, 20]),
        ("c_tab", [1, 3], [30, 40]),
        ("w_tab", [2, 3], [50, 60]),
    ] {
        let fields = vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("v", ArrowDataType::Int32, false),
        ];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(vs.to_vec())),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let columns = ["id", "v"]
            .iter()
            .map(|n| ColumnInfo {
                name: n.to_string(),
                data_type: DataType::Int32,
                nullable: false,
            })
            .collect();
        schema.register_table(name, Arc::new(MemoryTable::new(columns, vec![batch])));
    }
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

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

/// Nested exactly like q04's `year_total`: `(s UNION ALL c) UNION ALL w`.
const YT: &str = "WITH yt AS ( \
    SELECT id, v, 's' AS t FROM s_tab \
    UNION ALL SELECT id, v, 'c' AS t FROM c_tab \
    UNION ALL SELECT id, v, 'w' AS t FROM w_tab)";

#[tokio::test]
async fn selecting_the_last_branch_prunes_the_whole_inner_union() {
    assert_eq!(
        rows(&format!(
            "{YT} SELECT id, v, t FROM yt WHERE t = 'w' ORDER BY id"
        ))
        .await,
        table(&[&["2", "50", "w"], &["3", "60", "w"]])
    );
}

#[tokio::test]
async fn a_filter_no_branch_matches_returns_no_rows() {
    assert_eq!(
        rows(&format!("{YT} SELECT id, t FROM yt WHERE t = 'x'")).await,
        Vec::<Vec<String>>::new()
    );
}

#[tokio::test]
async fn two_references_each_read_their_own_branch() {
    assert_eq!(
        rows(&format!(
            "{YT} SELECT a.id, a.v, b.v FROM yt a, yt b \
             WHERE a.id = b.id AND a.t = 's' AND b.t = 'c' ORDER BY a.id"
        ))
        .await,
        table(&[&["1", "10", "30"]])
    );
}

#[tokio::test]
async fn an_aggregate_over_the_kept_branches_sees_every_kept_row() {
    assert_eq!(
        rows(&format!(
            "{YT} SELECT count(*), sum(v) FROM yt WHERE t <> 's'"
        ))
        .await,
        table(&[&["4", "180"]])
    );
}

/// The NULL-tagged branch comes first: a UNION ALL takes its nullability from
/// its first input today, which is a separate gap from branch pruning.
#[tokio::test]
async fn a_null_constant_branch_is_kept_for_is_null_and_dropped_for_equality() {
    let with_null = "WITH yt AS ( \
        SELECT id, v, CAST(NULL AS VARCHAR) AS t FROM c_tab \
        UNION ALL SELECT id, v, 's' AS t FROM s_tab)";
    assert_eq!(
        rows(&format!(
            "{with_null} SELECT id, t FROM yt WHERE t IS NULL ORDER BY id"
        ))
        .await,
        table(&[&["1", "NULL"], &["3", "NULL"]])
    );
    assert_eq!(
        rows(&format!(
            "{with_null} SELECT id, t FROM yt WHERE t = 's' ORDER BY id"
        ))
        .await,
        table(&[&["1", "s"], &["2", "s"]])
    );
}
