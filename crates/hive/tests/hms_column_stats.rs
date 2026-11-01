//! Column statistics read from a live Hive Metastore.
//!
//! Requires `docker compose up -d` (HMS on localhost:9083) and the TPC-DS SF1
//! seed (`docker compose run --rm tpcds-seed`).
//! Run with: `cargo test -p arneb-hive --test hms_column_stats -- --ignored`

use arneb_hive::catalog::HmsClient;
use hive_metastore::ThriftHiveMetastoreClientBuilder;
use pilota::FastStr;
use volo_thrift::MaybeException;

const HMS_ADDR: &str = "127.0.0.1:9083";

/// The seed writes tables through Trino, which records column statistics in
/// HMS; the planner's join estimates need those NDVs.
#[tokio::test]
#[ignore]
async fn get_table_returns_hms_column_ndv() {
    let client = HmsClient::new(HMS_ADDR).await.expect("connect to HMS");
    let meta = client
        .get_table("tpcds", "catalog_sales")
        .await
        .expect("tpcds.catalog_sales (seed with `docker compose run --rm tpcds-seed`)");
    let item = meta
        .column_stats
        .get("cs_item_sk")
        .expect("cs_item_sk column statistics");
    // Trino `SHOW STATS FOR hive.tpcds.catalog_sales` reports 17764 at SF1.
    assert_eq!(item.ndv, Some(17_764));
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
