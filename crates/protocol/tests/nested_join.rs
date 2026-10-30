//! Parenthesized joins in FROM, e.g. `FROM (a JOIN b ON ...) JOIN c ON ...`
//! (TPC-DS q40, q49, q72, q75, q78, q93, q97 in Trino's form). Every join
//! inside the parentheses must take part in the query, whether the nested
//! join leads the FROM clause or sits on the right side of another join.

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

/// Every column is nullable, as Hive columns are.
///
/// | a.x | | b.x | b.y | | c.y |
/// |-----| |-----|-----| |-----|
/// | 1   | | 1   | 10  | | 10  |
/// | 2   | | 2   | 20  | | 99  |
/// | 3   |
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("a", int_table(&[("x", vec![1, 2, 3])]));
    schema.register_table("b", int_table(&[("x", vec![1, 2]), ("y", vec![10, 20])]));
    schema.register_table("c", int_table(&[("y", vec![10, 99])]));
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
async fn leading_nested_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM (a JOIN b ON a.x = b.x)").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn leading_nested_join_exposes_right_columns() {
    assert_eq!(
        rows("SELECT a.x, b.y FROM (a JOIN b ON a.x = b.x) ORDER BY a.x").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
}

#[tokio::test]
async fn doubly_parenthesized_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM ((a JOIN b ON a.x = b.x))").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn leading_nested_left_join_then_comma_table() {
    // TPC-DS q93's shape: the WHERE references the nested join's right side.
    assert_eq!(
        rows("SELECT a.x, c.y FROM (a LEFT JOIN b ON a.x = b.x), c WHERE b.y = c.y").await,
        table(&[&["1", "10"]])
    );
}

#[tokio::test]
async fn leading_nested_left_join_keeps_unmatched_rows() {
    assert_eq!(
        rows("SELECT a.x FROM (a LEFT JOIN b ON a.x = b.x) WHERE b.y IS NULL").await,
        table(&[&["3"]])
    );
}

#[tokio::test]
async fn leading_nested_join_then_join() {
    assert_eq!(
        rows("SELECT a.x FROM (a JOIN b ON a.x = b.x) JOIN c ON b.y = c.y").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn nested_inner_join_on_right_side_of_inner_join() {
    assert_eq!(
        rows("SELECT count(*) FROM a JOIN (b JOIN c ON b.y = c.y) ON a.x = b.x").await,
        table(&[&["1"]])
    );
}

#[tokio::test]
async fn nested_inner_join_on_right_side_of_left_join_is_not_flattened() {
    // a LEFT JOIN (b JOIN c): rows 2 and 3 of `a` survive with NULLs. Flattening
    // to (a LEFT JOIN b) JOIN c would drop them.
    assert_eq!(
        rows(
            "SELECT a.x, b.y FROM a LEFT JOIN (b JOIN c ON b.y = c.y) ON a.x = b.x \
             ORDER BY a.x"
        )
        .await,
        table(&[&["1", "10"], &["2", "NULL"], &["3", "NULL"]])
    );
}

#[tokio::test]
async fn nested_left_join_on_right_side_of_inner_join() {
    // a JOIN (b LEFT JOIN c): b's row 2 has no match in c, so it keeps a NULL
    // c.y and still joins a's row 2.
    assert_eq!(
        rows(
            "SELECT a.x, b.y, c.y FROM a JOIN (b LEFT JOIN c ON b.y = c.y) ON a.x = b.x \
             ORDER BY a.x"
        )
        .await,
        table(&[&["1", "10", "10"], &["2", "20", "NULL"]])
    );
}

// A parenthesized join with an alias, `FROM (a JOIN b ON ...) t`, as Trino
// accepts it: its columns are reached through `t`, the inner table names are
// out of scope, and a name both sides share is ambiguous through `t`.

#[tokio::test]
async fn aliased_join_keeps_its_join() {
    assert_eq!(
        rows("SELECT count(*) FROM (a JOIN b ON a.x = b.x) t").await,
        table(&[&["2"]])
    );
}

#[tokio::test]
async fn aliased_join_columns_are_reached_through_the_alias() {
    assert_eq!(
        rows("SELECT t.y FROM (a JOIN b ON a.x = b.x) t ORDER BY t.y").await,
        table(&[&["10"], &["20"]])
    );
}

#[tokio::test]
async fn aliased_join_wildcard_returns_both_sides() {
    assert_eq!(
        rows("SELECT * FROM (a JOIN b ON a.x = b.x) t ORDER BY 1").await,
        table(&[&["1", "1", "10"], &["2", "2", "20"]])
    );
}

#[tokio::test]
async fn aliased_left_join_keeps_unmatched_rows() {
    assert_eq!(
        rows("SELECT y FROM (a LEFT JOIN b ON a.x = b.x) t ORDER BY 1 NULLS LAST").await,
        table(&[&["10"], &["20"], &["NULL"]])
    );
}

#[tokio::test]
async fn aliased_join_joined_to_another_table() {
    assert_eq!(
        rows(
            "SELECT t.y, c.y FROM (a JOIN b ON a.x = b.x) t \
             LEFT JOIN c ON t.y = c.y ORDER BY 1"
        )
        .await,
        table(&[&["10", "10"], &["20", "NULL"]])
    );
}

#[tokio::test]
async fn aliased_join_with_column_aliases() {
    assert_eq!(
        rows("SELECT u, v FROM (b JOIN c ON b.y = c.y) t(u, v, w)").await,
        table(&[&["1", "10"]])
    );
}

#[tokio::test]
async fn inner_table_names_are_out_of_scope_after_the_alias() {
    let message = error("SELECT b.y FROM (a JOIN b ON a.x = b.x) t").await;
    assert!(message.contains("b.y"), "{message}");
}

#[tokio::test]
async fn a_name_both_sides_share_is_ambiguous_through_the_alias() {
    let message = error("SELECT t.x FROM (a JOIN b ON a.x = b.x) t").await;
    assert!(message.contains("ambiguous"), "{message}");
}

// With USING, the right-hand key is a hidden copy; through the alias only the
// visible columns count, as in Trino 484.

#[tokio::test]
async fn aliased_using_join_wildcard_returns_the_visible_columns() {
    assert_eq!(
        rows("SELECT * FROM (a JOIN b USING (x)) t ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
    assert_eq!(
        rows("SELECT t.* FROM (a JOIN b USING (x)) t ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
}

#[tokio::test]
async fn aliased_using_key_is_not_ambiguous_through_the_alias() {
    assert_eq!(
        rows("SELECT t.x FROM (a JOIN b USING (x)) t ORDER BY 1").await,
        table(&[&["1"], &["2"]])
    );
}

#[tokio::test]
async fn aliased_using_join_column_aliases_name_the_visible_columns() {
    assert_eq!(
        rows("SELECT k, v FROM (a JOIN b USING (x)) t(k, v) ORDER BY 1").await,
        table(&[&["1", "10"], &["2", "20"]])
    );
    let message = error("SELECT * FROM (a JOIN b USING (x)) t(k, v, w)").await;
    assert!(message.to_lowercase().contains("alias"), "{message}");
}

#[tokio::test]
async fn typed_column_alias_on_a_nested_join_is_rejected() {
    // Trino rejects a type in a FROM alias list as a syntax error.
    let message = error("SELECT u FROM (b JOIN c ON b.y = c.y) t(u INT, v INT, w INT)").await;
    assert!(message.to_lowercase().contains("alias"), "{message}");
}
