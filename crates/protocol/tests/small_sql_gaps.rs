//! Small SQL gaps from #249: strict casts at run time, casts of a literal to
//! SMALLINT/TINYINT, and `FROM (VALUES ...)`. Expected values follow Trino.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
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
    // n: one BIGINT out of INTEGER range, one in range, one NULL; s: strings.
    schema.register_table(
        "n",
        Arc::new(table(vec![
            (
                "big",
                DataType::Int64,
                Arc::new(Int64Array::from(vec![Some(7), Some(3_000_000_000), None])),
            ),
            (
                "s",
                DataType::Utf8,
                Arc::new(StringArray::from(vec![Some("12"), Some("abc"), None])),
            ),
        ])),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(catalog, "default")),
    );
    (catalog_manager, Arc::new(registry))
}

async fn run(sql: &str) -> Result<(Vec<Vec<String>>, Vec<ArrowDataType>), String> {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    let mut types = Vec::new();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        types = b.columns().iter().map(|c| c.data_type().clone()).collect();
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
    Ok((out, types))
}

async fn rows(sql: &str) -> (Vec<Vec<String>>, Vec<ArrowDataType>) {
    run(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"))
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

// -- Strict casts at run time --

#[tokio::test]
async fn a_column_cast_out_of_integer_range_is_an_error() {
    let err = run("SELECT CAST(big AS INTEGER) FROM n").await.unwrap_err();
    assert!(err.contains("3000000000"), "{err}");
    let err = run("SELECT CAST(big AS SMALLINT) FROM n WHERE big > 100")
        .await
        .unwrap_err();
    assert!(err.to_lowercase().contains("cast"), "{err}");
}

#[tokio::test]
async fn a_column_cast_of_an_invalid_string_is_an_error() {
    let err = run("SELECT CAST(s AS INTEGER) FROM n").await.unwrap_err();
    assert!(err.contains("abc"), "{err}");
}

#[tokio::test]
async fn try_cast_still_turns_the_failing_rows_into_null() {
    let (got, _) = rows("SELECT TRY(CAST(big AS INTEGER)), TRY(CAST(s AS INTEGER)) FROM n").await;
    assert_eq!(
        got,
        vec![
            row(&["7", "12"]),
            row(&["NULL", "NULL"]),
            row(&["NULL", "NULL"])
        ]
    );
}

#[tokio::test]
async fn a_column_cast_in_range_keeps_its_values() {
    let (got, types) = rows("SELECT CAST(big AS INTEGER) FROM n WHERE big < 100").await;
    assert_eq!(got, vec![row(&["7"])]);
    assert_eq!(types, vec![ArrowDataType::Int32]);
}

// -- Casts of a literal to SMALLINT / TINYINT --

#[tokio::test]
async fn a_literal_casts_to_smallint_and_tinyint() {
    let (got, types) =
        rows("SELECT CAST(5 AS SMALLINT), CAST(-7 AS TINYINT), CAST('300' AS SMALLINT)").await;
    assert_eq!(got, vec![row(&["5", "-7", "300"])]);
    assert_eq!(
        types,
        vec![
            ArrowDataType::Int16,
            ArrowDataType::Int8,
            ArrowDataType::Int16
        ]
    );
}

#[tokio::test]
async fn a_literal_out_of_smallint_or_tinyint_range_fails_at_plan_time() {
    for sql in [
        "SELECT CAST(70000 AS SMALLINT)",
        "SELECT CAST(300 AS TINYINT)",
        "SELECT CAST('x' AS SMALLINT)",
    ] {
        let err = run(sql).await.unwrap_err();
        assert!(err.contains("invalid literal"), "{sql}: {err}");
    }
}

// -- FROM (VALUES ...) --

#[tokio::test]
async fn values_in_from_with_column_aliases() {
    let (got, _) = rows(
        "SELECT id, name FROM (VALUES (1, 'a'), (2, 'b'), (3, NULL)) AS v(id, name) \
         ORDER BY id DESC",
    )
    .await;
    assert_eq!(
        got,
        vec![row(&["3", "NULL"]), row(&["2", "b"]), row(&["1", "a"])]
    );
}

#[tokio::test]
async fn values_columns_are_named_like_trino_and_share_a_type() {
    let (got, types) = rows("SELECT _col0, _col1 FROM (VALUES (1, 'x'), (2.5, 'y')) t").await;
    assert_eq!(got, vec![row(&["1.0", "x"]), row(&["2.5", "y"])]);
    assert_eq!(types[0], ArrowDataType::Decimal128(20, 1));
}

#[tokio::test]
async fn values_joins_a_table_and_runs_as_a_whole_query() {
    let (got, _) = rows(
        "SELECT n.big, v.label FROM n JOIN (VALUES (7, 'seven')) AS v(k, label) ON n.big = v.k",
    )
    .await;
    assert_eq!(got, vec![row(&["7", "seven"])]);
    let (got, _) = rows("VALUES (1), (2)").await;
    assert_eq!(got, vec![row(&["1"]), row(&["2"])]);
}

#[tokio::test]
async fn a_long_values_list_plans() {
    let list = (0..2000)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(", ");
    let (got, _) = rows(&format!(
        "SELECT count(*), sum(_col0) FROM (VALUES {list}) t"
    ))
    .await;
    assert_eq!(got, vec![row(&["2000", "1999000"])]);
}

#[tokio::test]
async fn values_rows_of_different_widths_are_an_error() {
    let err = run("SELECT * FROM (VALUES (1, 2), (3)) t")
        .await
        .unwrap_err();
    assert!(err.contains("VALUES row 2 has 1 columns"), "{err}");
}

// -- Set operations in a derived table --

/// The outer query sees the set operation's unified column type, not the
/// left input's: `2.5` must not come back as `2`.
#[tokio::test]
async fn a_union_in_a_derived_table_keeps_the_unified_type() {
    let (got, types) =
        rows("SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2.5 AS a) t ORDER BY a").await;
    assert_eq!(got, vec![row(&["1.0"]), row(&["2.5"])]);
    assert_eq!(types, vec![ArrowDataType::Decimal128(20, 1)]);
    let (got, _) = rows("SELECT sum(a) FROM (SELECT 1 AS a UNION ALL SELECT 2.5e0 AS a) t").await;
    assert_eq!(got, vec![row(&["3.5"])]);
    let (got, _) =
        rows("SELECT a FROM (SELECT 2.5 AS a INTERSECT SELECT 2.5 AS a EXCEPT SELECT 1 AS a) t")
            .await;
    assert_eq!(got, vec![row(&["2.5"])]);
    // An untyped NULL branch still takes the other branch's type.
    let (got, _) = rows("SELECT a FROM (SELECT NULL AS a UNION ALL SELECT 2.5 AS a) t").await;
    assert_eq!(got.len(), 2);
}
