//! `x NOT IN (subquery)` over nullable keys at a size where comparing every
//! outer row with every subquery row would not finish in time. Expected
//! counts follow the same three-valued logic as `not_in_null_semantics.rs`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const N: i32 = 100_000;

fn int_table(column: &str, values: Vec<Option<i32>>) -> MemoryTable {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        ArrowDataType::Int32,
        true,
    )]));
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    MemoryTable::new(
        vec![ColumnInfo {
            name: column.to_string(),
            data_type: DataType::Int32,
            nullable: true,
        }],
        vec![batch],
    )
}

/// All columns are declared nullable, as Hive tables are.
/// - `s(x)`: 0..N plus one NULL
/// - `t(y)`: the even numbers in 0..N, no NULLs
/// - `tn(y)`: the even numbers in 0..N plus one NULL
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let evens: Vec<Option<i32>> = (0..N).step_by(2).map(Some).collect();
    let mut s: Vec<Option<i32>> = (0..N).map(Some).collect();
    s.push(None);
    let mut tn = evens.clone();
    tn.push(None);
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("s", Arc::new(int_table("x", s)));
    schema.register_table("t", Arc::new(int_table("y", evens)));
    schema.register_table("tn", Arc::new(int_table("y", tn)));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let factory = MemoryConnectorFactory::new(catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Run a single-cell query on its own thread and give up after 10 seconds.
fn cell_within_time_limit(sql: &str) -> String {
    let (tx, rx) = mpsc::channel();
    let sql_owned = sql.to_string();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let (cm, cr) = setup();
            let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
            let (_plan, batches) =
                arneb_protocol::__private::execute_query(&sql_owned, &cm, &cr, None, &pool)
                    .await
                    .unwrap_or_else(|e| panic!("query failed: {sql_owned}\n{e}"));
            let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
            array_value_to_string(batch.column(0), 0).unwrap()
        });
        let _ = tx.send(result);
    });
    rx.recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("query did not finish within 10 seconds: {sql}"))
}

#[test]
fn nullable_not_in_over_many_rows() {
    assert_eq!(
        cell_within_time_limit("SELECT count(*) FROM s WHERE x NOT IN (SELECT y FROM t)"),
        "50000"
    );
}

#[test]
fn nullable_not_in_with_a_null_in_the_subquery_over_many_rows() {
    assert_eq!(
        cell_within_time_limit("SELECT count(*) FROM s WHERE x NOT IN (SELECT y FROM tn)"),
        "0"
    );
}
