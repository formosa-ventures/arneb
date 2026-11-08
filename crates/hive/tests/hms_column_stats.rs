//! Column statistics read from a live Hive Metastore.
//!
//! Requires `docker compose up -d` (HMS on localhost:9083) and the TPC-DS SF1
//! seed (`docker compose run --rm tpcds-seed`).
//! Run with: `cargo test -p arneb-hive --test hms_column_stats -- --ignored`

use std::sync::Arc;

use arneb_catalog::{CatalogProvider, SchemaProvider, TableLookup};
use arneb_connectors::storage::StorageRegistry;
use arneb_hive::catalog::{HiveCatalogProvider, HmsClient};
use hive_metastore::ThriftHiveMetastoreClientBuilder;
use pilota::FastStr;
use volo_thrift::MaybeException;

const HMS_ADDR: &str = "127.0.0.1:9083";

/// The seed writes tables through Trino, which records column statistics in
/// HMS; the planner's join estimates need those NDVs. They come with the
/// table the schema provider serves, fetched once access is granted.
#[tokio::test]
#[ignore]
async fn table_lookup_returns_hms_column_ndv() {
    assert_eq!(
        catalog_sales_item_ndv("tpcds", "catalog_sales").await,
        Some(17_764)
    );
}

/// The stats call uses the name HMS stored, not the spelling the query used.
#[tokio::test]
#[ignore]
async fn table_lookup_under_another_spelling_returns_hms_column_ndv() {
    assert_eq!(
        catalog_sales_item_ndv("TPCDS", "CATALOG_SALES").await,
        Some(17_764)
    );
}

/// A denied table is refused before anything else is read from HMS.
#[tokio::test]
#[ignore]
async fn a_denied_table_lookup_is_refused() {
    let schema = tpcds_schema("tpcds").await;
    assert!(matches!(
        schema
            .table_if_allowed("catalog_sales", &|_, _| false)
            .await
            .unwrap(),
        TableLookup::Denied
    ));
}

/// Fetching a table's metadata alone reads no column statistics: that call
/// waits until the access check passed (and is skipped for an Iceberg
/// redirect), so a denied table costs no extra HMS round trip.
#[tokio::test]
#[ignore]
async fn get_table_alone_reads_no_column_statistics() {
    let client = HmsClient::new(HMS_ADDR).await.expect("connect to HMS");
    let meta = client
        .get_table("tpcds", "catalog_sales")
        .await
        .expect("tpcds.catalog_sales (seed with `docker compose run --rm tpcds-seed`)");
    assert!(
        meta.column_stats.is_empty(),
        "{:?}",
        meta.column_stats.keys()
    );
}

async fn tpcds_schema(written: &str) -> Arc<dyn SchemaProvider> {
    let client = Arc::new(HmsClient::new(HMS_ADDR).await.expect("connect to HMS"));
    let catalog = HiveCatalogProvider::new(client, Arc::new(StorageRegistry::new()));
    catalog
        .try_schema(written)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("schema {written:?}"))
}

/// NDV of `cs_item_sk` on the table served for `schema`.`table`.
async fn catalog_sales_item_ndv(schema: &str, table: &str) -> Option<u64> {
    let schema = tpcds_schema(schema).await;
    let TableLookup::Found(provider) = schema.table_if_allowed(table, &|_, _| true).await.unwrap()
    else {
        panic!("{table} not found (seed with `docker compose run --rm tpcds-seed`)");
    };
    let stats = provider.statistics().expect("table statistics");
    stats.columns.get("cs_item_sk").and_then(|c| c.ndv)
}

/// A table nobody computed statistics for still resolves; it just has no NDVs.
#[tokio::test]
#[ignore]
async fn get_table_without_column_statistics_still_resolves() {
    let db = "arneb_ndv_test";
    let table = "no_stats";
    let setup = ThriftHiveMetastoreClientBuilder::new("hive_metastore")
        .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
        .address(HMS_ADDR.parse::<std::net::SocketAddr>().unwrap())
        .build();
    let drop_db = || hive_metastore::DropDatabaseRequest {
        name: FastStr::from(db),
        // HMS 4.x rejects a drop without a catalog name (NPE on the null).
        catalog_name: Some(FastStr::from("hive")),
        ignore_unknown_db: true,
        delete_data: false,
        cascade: true,
        soft_delete: None,
        txn_id: None,
        delete_managed_dir: None,
    };
    let _ = setup.drop_database_req(drop_db()).await;
    match setup
        .create_database_req(hive_metastore::CreateDatabaseRequest {
            database_name: FastStr::from(db),
            ..Default::default()
        })
        .await
    {
        Ok(MaybeException::Ok(_)) => {}
        Ok(MaybeException::Exception(ex)) => panic!("HMS create_database_req exception: {ex:?}"),
        Err(e) => panic!("HMS create_database_req failed: {e}"),
    }
    let sd = hive_metastore::StorageDescriptor {
        cols: Some(vec![hive_metastore::FieldSchema {
            name: Some(FastStr::from("id")),
            r#type: Some(FastStr::from("int")),
            comment: None,
        }]),
        location: Some(FastStr::from(format!("s3a://warehouse/{db}/{table}"))),
        input_format: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        )),
        output_format: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        )),
        serde_info: Some(hive_metastore::SerDeInfo {
            name: Some(FastStr::from(table)),
            serialization_lib: Some(FastStr::from(
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            )),
            ..Default::default()
        }),
        num_buckets: Some(-1),
        ..Default::default()
    };
    match setup
        .create_table_req(hive_metastore::CreateTableRequest {
            table: hive_metastore::Table {
                table_name: Some(FastStr::from(table)),
                db_name: Some(FastStr::from(db)),
                owner: Some(FastStr::from("arneb")),
                sd: Some(sd),
                partition_keys: Some(vec![]),
                table_type: Some(FastStr::from("EXTERNAL_TABLE")),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
    {
        Ok(MaybeException::Ok(_)) => {}
        Ok(MaybeException::Exception(ex)) => panic!("HMS create_table_req exception: {ex:?}"),
        Err(e) => panic!("HMS create_table_req failed: {e}"),
    }

    let client = HmsClient::new(HMS_ADDR).await.expect("connect to HMS");
    let result = client.get_table(db, table).await;
    let _ = setup.drop_database_req(drop_db()).await;

    let meta = result.expect("a table without statistics must still resolve");
    assert_eq!(meta.columns.len(), 1);
    assert!(meta.column_stats.is_empty(), "{:?}", meta.column_stats);
}
