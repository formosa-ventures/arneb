//! Seeds HMS + MinIO with sample tables for the Arneb Hive demo.
//!
//! Prerequisites: `docker compose up -d` (HMS on :9083, MinIO on :9000)
//!
//! Usage:
//!   cargo run --bin hive-demo-setup
//!
//! Then start Arneb:
//!   cargo run --bin arneb -- --config scripts/arneb-hive-demo.toml
//!
//! Connect:
//!   psql -h 127.0.0.1 -p 5432
//!
//! Query:
//!   SELECT * FROM datalake.demo.cities;
//!   SELECT * FROM datalake.demo.orders;

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hive_metastore::ThriftHiveMetastoreClientBuilder;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use pilota::FastStr;
use volo_thrift::MaybeException;

const MINIO_ENDPOINT: &str = "http://localhost:9000";
const HMS_ADDR: &str = "127.0.0.1:9083";
const BUCKET: &str = "warehouse";
const DB_NAME: &str = "demo";

#[tokio::main]
async fn main() {
    println!("=== Arneb Hive Demo Setup ===\n");

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(BUCKET)
        .with_region("us-east-1")
        .with_endpoint(MINIO_ENDPOINT)
        .with_allow_http(true)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .build()
        .expect("failed to create S3 client for MinIO");

    // --- cities table ---
    println!("[1/4] Creating cities table...");
    let cities_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));
    let cities_batch = RecordBatch::try_new(
        cities_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "Taipei",
                "Tokyo",
                "Seoul",
                "Singapore",
                "Bangkok",
            ])),
            Arc::new(StringArray::from(vec![
                "Taiwan",
                "Japan",
                "South Korea",
                "Singapore",
                "Thailand",
            ])),
            Arc::new(Int64Array::from(vec![
                2_646_000, 13_960_000, 9_776_000, 5_454_000, 10_539_000,
            ])),
        ],
    )
    .unwrap();
    upload_parquet(
        &s3,
        "demo/cities/data.parquet",
        &cities_schema,
        &cities_batch,
    )
    .await;
    println!("      Uploaded 5 rows to s3://{BUCKET}/demo/cities/");

    // --- orders table ---
    println!("[2/4] Creating orders table...");
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("city_id", DataType::Int32, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![101, 102, 103, 104, 105, 106])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 3, 4, 2])),
            Arc::new(StringArray::from(vec![
                "Laptop", "Phone", "Tablet", "Laptop", "Phone", "Tablet",
            ])),
            Arc::new(Float64Array::from(vec![
                1200.0, 800.0, 500.0, 1200.0, 900.0, 450.0,
            ])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 1, 1, 2])),
        ],
    )
    .unwrap();
    upload_parquet(
        &s3,
        "demo/orders/data.parquet",
        &orders_schema,
        &orders_batch,
    )
    .await;
    println!("      Uploaded 6 rows to s3://{BUCKET}/demo/orders/");

    // --- Register in HMS ---
    println!("[3/4] Connecting to HMS at {HMS_ADDR}...");
    let hms = ThriftHiveMetastoreClientBuilder::new("hive_metastore")
        .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
        .address(HMS_ADDR.parse::<std::net::SocketAddr>().unwrap())
        .build();

    // Drop + create database using _req variants (HMS 5.x forward-compatible)
    let drop_req = hive_metastore::DropDatabaseRequest {
        name: FastStr::from(DB_NAME),
        catalog_name: None,
        ignore_unknown_db: true,
        delete_data: true,
        cascade: true,
        soft_delete: None,
        txn_id: None,
        delete_managed_dir: None,
    };
    let _ = hms.drop_database_req(drop_req).await;

    let create_req = hive_metastore::CreateDatabaseRequest {
        database_name: FastStr::from(DB_NAME),
        description: Some(FastStr::from("Arneb demo database")),
        ..Default::default()
    };
    match hms.create_database_req(create_req).await {
        Ok(MaybeException::Ok(_)) => {}
        Ok(MaybeException::Exception(ex)) => {
            eprintln!("WARNING: create_database_req exception: {ex:?}");
        }
        Err(e) => panic!("HMS create_database_req failed: {e}"),
    }

    println!("[4/4] Registering tables in HMS...");
    create_hms_table(
        &hms,
        "cities",
        vec![
            ("id", "int"),
            ("name", "string"),
            ("country", "string"),
            ("population", "bigint"),
        ],
    )
    .await;
    create_hms_table(
        &hms,
        "orders",
        vec![
            ("order_id", "int"),
            ("city_id", "int"),
            ("product", "string"),
            ("amount", "double"),
            ("quantity", "int"),
        ],
    )
    .await;

    println!("\n=== Setup Complete ===\n");
    println!("Start Arneb:");
    println!("  cargo run --bin arneb -- --config scripts/arneb-hive-demo.toml\n");
    println!("Connect:");
    println!("  psql -h 127.0.0.1 -p 5432\n");
    println!("Query examples:");
    println!("  SELECT * FROM datalake.demo.cities;");
    println!("  SELECT * FROM datalake.demo.orders;");
    println!("  SELECT c.name, SUM(o.amount) total");
    println!("    FROM datalake.demo.cities c");
    println!("    JOIN datalake.demo.orders o ON c.id = o.city_id");
    println!("    GROUP BY c.name ORDER BY total DESC;");
}

async fn upload_parquet(
    store: &impl ObjectStore,
    path: &str,
    schema: &Arc<Schema>,
    batch: &RecordBatch,
) {
    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
    store
        .put(&ObjectPath::from(path), PutPayload::from(buf))
        .await
        .expect("failed to upload to MinIO");
}

async fn create_hms_table(
    hms: &hive_metastore::ThriftHiveMetastoreClient,
    name: &str,
    columns: Vec<(&str, &str)>,
) {
    // Real S3 location — the custom HMS image (docker/hive-metastore/) bundles
    // hadoop-aws so HMS can validate s3a:// paths against MinIO directly.
    let hms_location = format!("s3a://{BUCKET}/demo/{name}");

    let serde = hive_metastore::SerDeInfo {
        name: Some(FastStr::from(name.to_string())),
        serialization_lib: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        )),
        ..Default::default()
    };

    let sd = hive_metastore::StorageDescriptor {
        cols: Some(
            columns
                .iter()
                .map(|(col_name, col_type)| hive_metastore::FieldSchema {
                    name: Some(FastStr::from(col_name.to_string())),
                    r#type: Some(FastStr::from(col_type.to_string())),
                    comment: None,
                })
                .collect(),
        ),
        location: Some(FastStr::from(hms_location)),
        input_format: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        )),
        output_format: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        )),
        serde_info: Some(serde),
        num_buckets: Some(-1),
        ..Default::default()
    };

    let tbl = hive_metastore::Table {
        table_name: Some(FastStr::from(name.to_string())),
        db_name: Some(FastStr::from(DB_NAME)),
        owner: Some(FastStr::from("arneb")),
        sd: Some(sd),
        partition_keys: Some(vec![]),
        table_type: Some(FastStr::from("EXTERNAL_TABLE")),
        ..Default::default()
    };

    let create_req = hive_metastore::CreateTableRequest {
        table: tbl,
        ..Default::default()
    };

    match hms.create_table_req(create_req).await {
        Ok(MaybeException::Ok(_)) => println!("      Registered {DB_NAME}.{name}"),
        Ok(MaybeException::Exception(ex)) => {
            eprintln!("      WARNING: create_table_req({name}) exception: {ex:?}")
        }
        Err(e) => panic!("HMS create_table_req({name}) failed: {e}"),
    }
}
