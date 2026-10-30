//! Column aliases on a table or a subquery in FROM, `FROM a AS t(u, w)` and
//! `FROM (SELECT ...) s(u)`. The aliases rename the columns: the old names go
//! out of scope, the list must name every column, and a repeated alias is
//! accepted but ambiguous when referenced. Expected rows and errors were taken
//! from Trino 484.
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

fn int_table(cols: &[(&str, Vec<i32>)]) -> Arc<MemoryTable> {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, ArrowDataType::Int32, true))
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
                nullable: true,
            })
            .collect(),
        vec![batch],
    ))
}

/// `a(x, v)`: (1, 10), (2, 20).
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("a", int_table(&[("x", vec![1, 2]), ("v", vec![10, 20])]));
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

/// Plan and run `sql`, expecting an error; return its message.
async fn error(sql: &str) -> String {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    match arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool).await {
        Ok(_) => panic!("query should fail: {sql}"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn table_column_aliases_rename_the_columns() {
    assert_eq!(
        rows("SELECT u FROM a AS t(u, w) ORDER BY u").await,
        table(&[&["1"], &["2"]])
    );
}

#[tokio::test]
async fn table_column_aliases_are_reached_through_the_table_alias() {
    assert_eq!(
        rows("SELECT t.w FROM a t(u, w) ORDER BY t.w").await,
        table(&[&["10"], &["20"]])
    );
}

#[tokio::test]
async fn wildcard_over_table_column_aliases() {
    assert_eq!(
        rows("SELECT * FROM a AS t(u, w) ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
}

#[tokio::test]
async fn original_column_name_is_out_of_scope() {
    let message = error("SELECT x FROM a AS t(u, w)").await;
    assert!(message.contains('x'), "{message}");
    let message = error("SELECT t.x FROM a AS t(u, w)").await;
    assert!(message.contains("t.x"), "{message}");
}

#[tokio::test]
async fn alias_list_must_name_every_column() {
    let message = error("SELECT u FROM a AS t(u)").await;
    assert!(message.to_lowercase().contains("alias"), "{message}");
}

#[tokio::test]
async fn subquery_column_alias() {
    assert_eq!(
        rows("SELECT u FROM (SELECT 1 AS x) s(u)").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn subquery_column_alias_in_an_expression() {
    assert_eq!(
        rows("SELECT s.u + 1 FROM (SELECT x FROM a) s(u) ORDER BY 1").await,
        table(&[&["2"], &["3"]])
    );
}

#[tokio::test]
async fn subquery_column_alias_as_a_join_key() {
    assert_eq!(
        rows("SELECT q.u, a.v FROM a JOIN (SELECT x FROM a) q(u) ON a.x = q.u ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
}

#[tokio::test]
async fn repeated_alias_is_accepted_until_referenced() {
    assert_eq!(
        rows("SELECT * FROM (SELECT x, v FROM a) s(u, u) ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
    let message = error("SELECT u FROM (SELECT x, v FROM a) s(u, u)").await;
    assert!(message.contains("ambiguous"), "{message}");
}

#[tokio::test]
async fn quoted_column_alias_is_reached_without_quotes() {
    assert_eq!(
        rows("SELECT t.u FROM a AS t(\"u\", w) ORDER BY 1").await,
        table(&[&["1"], &["2"]])
    );
}

#[tokio::test]
async fn typed_column_alias_is_rejected() {
    // Trino rejects a type in a FROM alias list as a syntax error. It must be
    // refused at the alias list, not surface later as a missing column.
    let message = error("SELECT u FROM a AS t(u INT, w INT)").await;
    assert!(message.to_lowercase().contains("alias"), "{message}");
}
