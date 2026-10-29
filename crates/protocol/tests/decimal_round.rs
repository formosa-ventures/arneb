//! ROUND over DECIMAL follows Trino: the value is rounded exactly, half away
//! from zero, and the result stays DECIMAL — decimal(p + 1, s) with a scale
//! argument, decimal(p - s + 1, 0) without one (precision capped at 38).
//! Expected values and types were taken from Trino 484. TPC-DS q78 rounds a
//! DECIMAL ratio this way.

use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};

fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", Arc::new(MemorySchema::new()));
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run a one-column, one-row query; return the value as text (`NULL` for
/// null) and the column's Arrow type.
async fn single(sql: &str) -> (String, ArrowDataType) {
    let (cm, cr) = setup();
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "row count of `{sql}`");
    let b = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    assert_eq!(b.num_columns(), 1, "column count of `{sql}`");
    let c = b.column(0);
    let text = if c.is_null(0) {
        "NULL".to_string()
    } else {
        array_value_to_string(c, 0).unwrap()
    };
    (text, c.data_type().clone())
}

async fn assert_round(sql: &str, text: &str, precision: u8, scale: i8) {
    assert_eq!(
        single(sql).await,
        (
            text.to_string(),
            ArrowDataType::Decimal128(precision, scale)
        ),
        "`{sql}`"
    );
}

#[tokio::test]
async fn half_rounds_away_from_zero() {
    assert_round("SELECT round(DECIMAL '1.025', 2)", "1.030", 5, 3).await;
    assert_round("SELECT round(DECIMAL '-1.025', 2)", "-1.030", 5, 3).await;
}

#[tokio::test]
async fn below_half_rounds_down() {
    assert_round("SELECT round(DECIMAL '0.044', 1)", "0.000", 4, 3).await;
}

#[tokio::test]
async fn carry_into_a_new_integer_digit() {
    assert_round("SELECT round(DECIMAL '99.995', 2)", "100.000", 6, 3).await;
}

#[tokio::test]
async fn without_a_scale_argument_rounds_to_an_integer_decimal() {
    assert_round("SELECT round(DECIMAL '1.025')", "1", 2, 0).await;
    assert_round("SELECT round(DECIMAL '-0.5')", "-1", 1, 0).await;
}

#[tokio::test]
async fn negative_scale_rounds_left_of_the_point() {
    assert_round("SELECT round(DECIMAL '12.5', -1)", "10.0", 4, 1).await;
}

#[tokio::test]
async fn scale_argument_wider_than_the_input_keeps_the_value() {
    assert_round("SELECT round(DECIMAL '1.5', 3)", "1.5", 3, 1).await;
}

#[tokio::test]
async fn precision_is_capped_at_38() {
    assert_round(
        "SELECT round(CAST(DECIMAL '1.25' AS DECIMAL(38, 2)), 1)",
        "1.30",
        38,
        2,
    )
    .await;
}

#[tokio::test]
async fn decimal_ratio_as_in_tpcds_q78() {
    assert_round(
        "SELECT round(CAST(41 AS DECIMAL(10, 2)) / (COALESCE(CAST(40 AS BIGINT), 0) + 0), 2)",
        "1.0300000000000000000000",
        31,
        22,
    )
    .await;
}

#[tokio::test]
async fn null_inputs_give_null() {
    assert_round("SELECT round(CAST(NULL AS DECIMAL(4, 3)), 2)", "NULL", 5, 3).await;
    assert_round(
        "SELECT round(DECIMAL '1.025', CAST(NULL AS INTEGER))",
        "NULL",
        5,
        3,
    )
    .await;
}

#[tokio::test]
async fn double_round_is_unchanged() {
    assert_eq!(
        single("SELECT round(DOUBLE '1.025', 2)").await,
        ("1.02".to_string(), ArrowDataType::Float64)
    );
}
