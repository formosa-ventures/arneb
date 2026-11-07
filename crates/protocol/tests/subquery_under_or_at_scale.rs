//! EXISTS / IN under OR at a size where comparing every outer row with every
//! subquery row would not finish in time.

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

/// - `c(ck)`: 0..N
/// - `w(wk)`: the multiples of 3 in 0..N
/// - `k(kk)`: the multiples of 5 in 0..N
fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(MemorySchema::new());
    schema.register_table("c", Arc::new(int_table("ck", (0..N).map(Some).collect())));
    schema.register_table(
        "w",
        Arc::new(int_table("wk", (0..N).step_by(3).map(Some).collect())),
    );
    schema.register_table(
        "k",
        Arc::new(int_table("kk", (0..N).step_by(5).map(Some).collect())),
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

// Multiples of 3 or 5 in 0..100000: 33334 + 20000 - 6667.
#[test]
fn exists_or_exists_over_many_rows() {
    assert_eq!(
        cell_within_time_limit(
            "SELECT count(*) FROM c WHERE EXISTS (SELECT * FROM w WHERE wk = ck) \
             OR EXISTS (SELECT * FROM k WHERE kk = ck)"
        ),
        "46667"
    );
}

#[test]
fn predicate_or_in_subquery_over_many_rows() {
    assert_eq!(
        cell_within_time_limit(
            "SELECT count(*) FROM c WHERE ck % 3 = 0 OR ck IN (SELECT kk FROM k)"
        ),
        "46667"
    );
}
