//! Uncorrelated scalar subqueries are evaluated before execution wherever
//! they sit: inside CASE (TPC-DS q09), BETWEEN under a join (q54), a HAVING
//! below a window function (q44), a UNION ALL branch (q23a/q23b) and function
//! arguments. Expected rows were taken from Trino 484.

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

/// A table of non-nullable Int32 columns.
fn int_table(cols: &[(&str, Vec<i32>)]) -> Arc<MemoryTable> {
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

/// t(k, v) = (1, 10), (2, 20), (3, 30), (4, 40)
/// u(uk, w) = (1, 100), (2, 200), (3, 300)
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        "t",
        int_table(&[("k", vec![1, 2, 3, 4]), ("v", vec![10, 20, 30, 40])]),
    );
    schema.register_table(
        "u",
        int_table(&[("uk", vec![1, 2, 3]), ("w", vec![100, 200, 300])]),
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

async fn run(sql: &str) -> Result<Vec<Vec<String>>, String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map_err(|e| e.to_string())?;
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
    Ok(out)
}

async fn rows(sql: &str) -> Vec<Vec<String>> {
    run(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
}

fn table(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[tokio::test]
async fn subqueries_in_case_condition_and_branches_as_in_tpcds_q09() {
    let sql = |threshold: i32| {
        format!(
            "SELECT CASE WHEN (SELECT count(*) FROM t WHERE v > 15) > {threshold} \
             THEN (SELECT sum(v) FROM t) ELSE (SELECT max(v) FROM t) END \
             FROM u WHERE uk = 1"
        )
    };
    assert_eq!(rows(&sql(2)).await, table(&[&["100"]]));
    assert_eq!(rows(&sql(3)).await, table(&[&["40"]]));
}

#[tokio::test]
async fn subqueries_as_between_bounds_under_a_join_as_in_tpcds_q54() {
    assert_eq!(
        rows(
            "SELECT t.k, u.w FROM t JOIN u ON t.k = u.uk \
             WHERE t.v BETWEEN (SELECT min(w) FROM u) / 10 \
             AND (SELECT max(w) FROM u) / 10 - 10 ORDER BY 1"
        )
        .await,
        table(&[&["1", "100"], &["2", "200"]])
    );
}

#[tokio::test]
async fn subquery_in_having_below_a_window_as_in_tpcds_q44() {
    assert_eq!(
        rows(
            "SELECT k, rank() OVER (ORDER BY s DESC) FROM \
             (SELECT k, sum(v) s FROM t GROUP BY k \
              HAVING sum(v) > 0.5 * (SELECT avg(v) FROM t)) x ORDER BY k"
        )
        .await,
        table(&[&["2", "3"], &["3", "2"], &["4", "1"]])
    );
}

#[tokio::test]
async fn subqueries_in_both_union_all_branches_as_in_tpcds_q23() {
    assert_eq!(
        rows(
            "SELECT k FROM t WHERE v > (SELECT avg(v) FROM t) \
             UNION ALL SELECT uk FROM u WHERE w < (SELECT max(w) FROM u) ORDER BY 1"
        )
        .await,
        table(&[&["1"], &["2"], &["3"], &["4"]])
    );
}

#[tokio::test]
async fn subquery_in_having_of_an_in_subquery() {
    assert_eq!(
        rows(
            "SELECT uk FROM u WHERE uk IN \
             (SELECT k FROM t GROUP BY k HAVING sum(v) > (SELECT max(v) FROM t) / 2) \
             ORDER BY uk"
        )
        .await,
        table(&[&["3"]])
    );
}

#[tokio::test]
async fn subquery_as_a_function_argument() {
    assert_eq!(
        rows("SELECT k FROM t WHERE abs(v - (SELECT max(v) FROM t) / 2) < 10 ORDER BY k").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn empty_subquery_inside_case_is_null() {
    assert_eq!(
        rows(
            "SELECT CASE WHEN (SELECT v FROM t WHERE k > 9) IS NULL THEN 1 ELSE 2 END \
             FROM u WHERE uk = 1"
        )
        .await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn multi_row_subquery_inside_case_still_fails() {
    let err = run("SELECT CASE WHEN (SELECT v FROM t) > 0 THEN 1 END FROM u")
        .await
        .expect_err("a scalar subquery returning several rows must fail");
    assert!(err.contains("at most one row"), "unexpected error: {err}");
}
