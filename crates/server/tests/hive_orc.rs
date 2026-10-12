//! End-to-end SQL over Hive ORC tables (no HMS needed): a
//! `HiveTableProvider` with ORC storage and partition keys, served by the
//! real `HiveConnectorFactory` over an in-memory object store.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::util::display::array_value_to_string;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use arneb_catalog::{CatalogManager, MemoryCatalog, MemorySchema};
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::{ConnectorRegistry, StorageRegistry};
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_hive::catalog::HiveTableProvider;
use arneb_hive::datasource::HiveConnectorFactory;

fn orc_file(ids: &[i32], names: &[&str]) -> Vec<u8> {
    // Mixed-case file column names; HMS columns are lower case.
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("Id", ArrowDataType::Int32, true),
            Field::new("NAME", ArrowDataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut w = orc_rust::ArrowWriterBuilder::new(&mut buf, batch.schema())
        .try_build()
        .unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    buf
}

async fn setup() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (path, ids, names) in [
        ("w/t/region=us/a.orc", &[1, 2][..], &["ann", "bob"][..]),
        ("w/t/region=eu/b.orc", &[3][..], &["cy"][..]),
    ] {
        store
            .put(
                &ObjectPath::parse(path).unwrap(),
                PutPayload::from(orc_file(ids, names)),
            )
            .await
            .unwrap();
    }
    let storage = Arc::new(StorageRegistry::new());
    storage.register_store("s3://lake", store);

    let col = |name: &str, data_type| ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
    };
    let table = HiveTableProvider::new(
        vec![col("id", DataType::Int64), col("name", DataType::Utf8)],
        "s3://lake/w/t".to_string(),
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".to_string(),
    )
    .with_serde_lib("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
    .with_partition_columns(vec![col("region", DataType::Utf8)]);

    let schema = Arc::new(MemorySchema::new());
    schema.register_table("t", Arc::new(table));
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema("default", schema);
    let catalog_manager = Arc::new(CatalogManager::new("lake", "default"));
    catalog_manager.register_catalog("lake", catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("lake", Arc::new(HiveConnectorFactory::new(storage)));
    (catalog_manager, Arc::new(registry))
}

async fn query(sql: &str) -> Vec<Vec<String>> {
    let (cm, cr) = setup().await;
    let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::new());
    let (_plan, batches) = arneb_protocol::__private::execute_query(sql, &cm, &cr, None, &pool)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let mut rows = Vec::new();
    for b in &batches {
        for r in 0..b.num_rows() {
            rows.push(
                b.columns()
                    .iter()
                    .map(|c: &ArrayRef| {
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
    rows
}

fn rows(cells: &[&[&str]]) -> Vec<Vec<String>> {
    cells
        .iter()
        .map(|r| r.iter().map(|s| s.to_string()).collect())
        .collect()
}

#[tokio::test]
async fn orc_select_star_includes_partition_column() {
    assert_eq!(
        query("SELECT * FROM t ORDER BY id").await,
        rows(&[&["1", "ann", "us"], &["2", "bob", "us"], &["3", "cy", "eu"]])
    );
}

#[tokio::test]
async fn orc_filter_and_projection() {
    assert_eq!(
        query("SELECT name FROM t WHERE id >= 2 ORDER BY name").await,
        rows(&[&["bob"], &["cy"]])
    );
}

#[tokio::test]
async fn orc_partition_filter_and_aggregate() {
    assert_eq!(
        query("SELECT region, count(*), sum(id) FROM t WHERE region <> 'xx' GROUP BY region ORDER BY region")
            .await,
        rows(&[&["eu", "1", "3"], &["us", "2", "3"]])
    );
    assert_eq!(
        query("SELECT count(*) FROM t WHERE region = 'us'").await,
        rows(&[&["2"]])
    );
}
