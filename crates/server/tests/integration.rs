#![allow(clippy::while_let_loop, clippy::approx_constant)]

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use arneb_catalog::CatalogManager;
use arneb_connectors::file::{FileCatalog, FileConnectorFactory, FileFormat, FileSchema};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::ProtocolConfig;

/// Helper to start a server on a random port and return the address.
async fn start_test_server(
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let handler_factory = Arc::new(arneb_protocol::__private::HandlerFactory {
        catalog_manager,
        connector_registry,
        distributed_executor: None,
    });

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let handler = handler_factory.clone();
                    tokio::spawn(async move {
                        let _ = pgwire::tokio::process_socket(socket, None, handler).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    addr
}

fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut params = Vec::new();
    params.extend_from_slice(b"user\0");
    params.extend_from_slice(user.as_bytes());
    params.push(0);
    params.extend_from_slice(b"database\0");
    params.extend_from_slice(database.as_bytes());
    params.push(0);
    params.push(0);

    let len = 4 + 4 + params.len();
    let mut msg = Vec::new();
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(&196608i32.to_be_bytes());
    msg.extend_from_slice(&params);
    msg
}

fn build_query_message(sql: &str) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'Q');
    let len = 4 + sql.len() + 1;
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    msg
}

async fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 8192];
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    match tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf)).await {
        Ok(Ok(n)) => buf[..n].to_vec(),
        _ => Vec::new(),
    }
}

fn response_contains_message_type(data: &[u8], msg_type: u8) -> bool {
    let mut pos = 0;
    while pos < data.len() {
        if data[pos] == msg_type {
            return true;
        }
        if pos + 5 <= data.len() {
            let len =
                i32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;
            pos += 1 + len;
        } else {
            break;
        }
    }
    false
}

fn create_server_with_memory_connector() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let mem_schema = Arc::new(MemorySchema::new());
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);

    let factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");

    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", mem_catalog);

    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));

    (catalog_manager, Arc::new(registry))
}

#[tokio::test]
async fn test_server_startup_handshake() {
    let (cm, cr) = create_server_with_memory_connector();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();

    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'R'),
        "expected AuthenticationOk (R)"
    );
    assert!(
        response_contains_message_type(&response, b'Z'),
        "expected ReadyForQuery (Z)"
    );
}

#[tokio::test]
async fn test_query_parquet_table() {
    // Create a temporary parquet file
    let dir = tempfile::tempdir().unwrap();
    let parquet_path = dir.path().join("test.parquet");

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("value", ArrowDataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![10.5, 20.5])),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(&parquet_path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Set up file connector
    let storage_registry = Arc::new(arneb_connectors::StorageRegistry::new());
    let file_factory = Arc::new(FileConnectorFactory::new(storage_registry));
    file_factory
        .register_table(
            "test_data",
            parquet_path.to_str().unwrap(),
            FileFormat::Parquet,
            None,
        )
        .await
        .unwrap();

    let file_schema = Arc::new(FileSchema::new(file_factory.clone()));
    let file_catalog = Arc::new(FileCatalog::new("default", file_schema));

    let catalog_manager = Arc::new(CatalogManager::new("file", "default"));
    catalog_manager.register_catalog("file", file_catalog);

    let mut registry = ConnectorRegistry::new();
    registry.register("file", file_factory);
    let registry = Arc::new(registry);

    let addr = start_test_server(catalog_manager, registry).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    stream
        .write_all(&build_query_message("SELECT id, value FROM test_data"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription (T)"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow (D)"
    );
    assert!(
        response_contains_message_type(&response, b'C'),
        "expected CommandComplete (C)"
    );
    assert!(
        response_contains_message_type(&response, b'Z'),
        "expected ReadyForQuery (Z)"
    );
}

#[tokio::test]
async fn test_query_parquet_via_object_store() {
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};

    // Write Parquet bytes into InMemory ObjectStore
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("value", ArrowDataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![42, 99])),
            Arc::new(Float64Array::from(vec![3.14, 2.72])),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let store = Arc::new(InMemory::new());
    store
        .put(
            &ObjectPath::from("data/test.parquet"),
            PutPayload::from(buf),
        )
        .await
        .unwrap();

    // Register InMemory store as s3://test-bucket
    let storage_registry = Arc::new(arneb_connectors::StorageRegistry::new());
    storage_registry.register_store("s3://test-bucket", store);

    let file_factory = Arc::new(FileConnectorFactory::new(storage_registry));
    file_factory
        .register_table(
            "test_data",
            "s3://test-bucket/data/test.parquet",
            FileFormat::Parquet,
            None,
        )
        .await
        .unwrap();

    let file_schema = Arc::new(FileSchema::new(file_factory.clone()));
    let file_catalog = Arc::new(FileCatalog::new("default", file_schema));

    let catalog_manager = Arc::new(CatalogManager::new("file", "default"));
    catalog_manager.register_catalog("file", file_catalog);

    let mut registry = ConnectorRegistry::new();
    registry.register("file", file_factory);
    let registry = Arc::new(registry);

    let addr = start_test_server(catalog_manager, registry).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    stream
        .write_all(&build_query_message("SELECT id, value FROM test_data"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription (T)"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow (D)"
    );
    assert!(
        response_contains_message_type(&response, b'C'),
        "expected CommandComplete (C)"
    );
    assert!(
        response_contains_message_type(&response, b'Z'),
        "expected ReadyForQuery (Z)"
    );
}

#[tokio::test]
async fn test_query_nonexistent_table_returns_error() {
    let (cm, cr) = create_server_with_memory_connector();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    stream
        .write_all(&build_query_message("SELECT * FROM nonexistent"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'E'),
        "expected ErrorResponse (E) for nonexistent table"
    );
    assert!(
        response_contains_message_type(&response, b'Z'),
        "expected ReadyForQuery (Z) after error"
    );
}

// ===========================================================================
// Phase 2 Integration Tests
// ===========================================================================

fn create_server_with_two_tables() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::MemoryTable;
    use arrow::array::StringArray;

    // Users table: id, name
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
    ]));
    let users_batch = RecordBatch::try_new(
        users_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )
    .unwrap();
    let users_table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".into(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ],
        vec![users_batch],
    ));

    // Orders table: id, user_id, amount
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("user_id", ArrowDataType::Int32, false),
        Field::new("amount", ArrowDataType::Float64, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int32Array::from(vec![1, 2, 1, 3])),
            Arc::new(Float64Array::from(vec![100.0, 200.0, 150.0, 300.0])),
        ],
    )
    .unwrap();
    let orders_table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "user_id".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "amount".into(),
                data_type: DataType::Float64,
                nullable: false,
            },
        ],
        vec![orders_batch],
    ));

    let mem_schema = Arc::new(MemorySchema::new());
    mem_schema.register_table("users", users_table);
    mem_schema.register_table("orders", orders_table);
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);

    let factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");

    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", mem_catalog);

    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));

    (catalog_manager, Arc::new(registry))
}

/// Count DataRow ('D') messages in a PG wire response.
fn count_data_rows(data: &[u8]) -> usize {
    let mut count = 0;
    let mut pos = 0;
    while pos < data.len() {
        if data[pos] == b'D' {
            count += 1;
        }
        if pos + 5 <= data.len() {
            let len =
                i32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]])
                    as usize;
            pos += 1 + len;
        } else {
            break;
        }
    }
    count
}

#[tokio::test]
async fn test_phase2_join_query() {
    let (cm, cr) = create_server_with_two_tables();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Hash join: users JOIN orders ON users.id = orders.user_id
    stream
        .write_all(&build_query_message(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
        ))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription for JOIN query"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow for JOIN query — hash join should produce results"
    );
    // Should have 4 rows (Alice×100, Bob×200, Alice×150, Carol×300)
    let row_count = count_data_rows(&response);
    assert_eq!(row_count, 4, "JOIN should produce 4 rows");
}

#[tokio::test]
async fn test_phase2_where_filter() {
    let (cm, cr) = create_server_with_two_tables();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // WHERE filter: only id > 1
    stream
        .write_all(&build_query_message(
            "SELECT id, name FROM users WHERE id > 1",
        ))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    let row_count = count_data_rows(&response);
    assert_eq!(
        row_count, 2,
        "WHERE id > 1 should return 2 rows (Bob, Carol)"
    );
}

#[tokio::test]
async fn test_phase2_projection_subset() {
    let (cm, cr) = create_server_with_two_tables();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Projection: select only 'name' column
    stream
        .write_all(&build_query_message("SELECT name FROM users"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription for projection query"
    );
    let row_count = count_data_rows(&response);
    assert_eq!(row_count, 3, "SELECT name should return 3 rows");
}

#[tokio::test]
async fn test_phase2_order_by_limit() {
    let (cm, cr) = create_server_with_two_tables();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // ORDER BY + LIMIT
    stream
        .write_all(&build_query_message(
            "SELECT id, name FROM users ORDER BY id DESC LIMIT 2",
        ))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    let row_count = count_data_rows(&response);
    assert_eq!(row_count, 2, "LIMIT 2 should return 2 rows");
}

#[tokio::test]
async fn test_phase2_explain_plan() {
    let (cm, cr) = create_server_with_two_tables();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    stream
        .write_all(&build_query_message(
            "EXPLAIN SELECT name FROM users WHERE id > 1",
        ))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "EXPLAIN should return RowDescription"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "EXPLAIN should return plan text as DataRow"
    );
}

// ===========================================================================
// Hive Connector Integration Tests
// ===========================================================================

/// Integration test for the Hive connector path: manually inject Hive
/// metadata (bypassing HMS Thrift), write Parquet data to an InMemory
/// ObjectStore, and verify the full query path through pgwire.
#[tokio::test]
async fn test_hive_connector_query_via_object_store() {
    use arneb_common::types::{ColumnInfo, DataType};
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    // 1. Write Parquet data to InMemory ObjectStore
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("score", ArrowDataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Carol",
            ])),
            Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1])),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    store
        .put(
            &ObjectPath::from("warehouse/default/students/data.parquet"),
            PutPayload::from(buf),
        )
        .await
        .unwrap();

    // 2. Create StorageRegistry with the InMemory store registered for s3://test-lake
    let storage_registry = Arc::new(arneb_connectors::StorageRegistry::new());
    storage_registry.register_store("s3://test-lake", store);

    // 3. Create HiveConnectorFactory and pre-register the table location
    //    (simulates what the production path should do once task 9.1 is fixed)
    let hive_columns = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
        },
        ColumnInfo {
            name: "name".into(),
            data_type: DataType::Utf8,
            nullable: false,
        },
        ColumnInfo {
            name: "score".into(),
            data_type: DataType::Float64,
            nullable: false,
        },
    ];
    let hive_factory = arneb_hive::datasource::HiveConnectorFactory::new(storage_registry);
    hive_factory.register_table_location(
        "students",
        "s3://test-lake/warehouse/default/students",
        hive_columns.clone(),
    );

    // 4. Create a catalog using HiveTableProvider (no HMS connection needed)
    let hive_table = Arc::new(arneb_hive::catalog::HiveTableProvider::new(
        hive_columns,
        "s3://test-lake/warehouse/default/students".to_string(),
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_string(),
    ));

    let hive_schema = Arc::new(arneb_catalog::MemorySchema::new());
    hive_schema.register_table("students", hive_table);
    let hive_catalog = Arc::new(arneb_catalog::MemoryCatalog::new());
    hive_catalog.register_schema("default", hive_schema);

    // 5. Wire up CatalogManager and ConnectorRegistry
    let catalog_manager = Arc::new(CatalogManager::new("hive_lake", "default"));
    catalog_manager.register_catalog("hive_lake", hive_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("hive_lake", Arc::new(hive_factory));
    let connector_registry = Arc::new(connector_registry);

    // 6. Start test server and query
    let addr = start_test_server(catalog_manager, connector_registry).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Query the Hive table
    stream
        .write_all(&build_query_message("SELECT id, name, score FROM students"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription (T) for Hive table query"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow (D) for Hive table query"
    );
    let row_count = count_data_rows(&response);
    assert_eq!(row_count, 3, "Hive table should return 3 rows");
    assert!(
        response_contains_message_type(&response, b'C'),
        "expected CommandComplete (C)"
    );
}

// ===========================================================================
// MinIO S3 Integration Test (requires Docker: docker compose up -d)
// ===========================================================================

/// Integration test that verifies the full S3 lazy-creation path:
/// `StorageRegistry::with_config(s3_config)` → MinIO → read Parquet.
///
/// Requires: `docker compose up -d` (starts MinIO on localhost:9000).
/// Run with: `cargo test -p arneb-server -- --ignored test_s3_read_via_minio`
#[tokio::test]
#[ignore]
async fn test_s3_read_via_minio() {
    use arneb_connectors::{CloudStorageConfig, S3StorageConfig, StorageRegistry};
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};

    let minio_endpoint = "http://localhost:9000";
    let bucket = "warehouse";

    // 1. Write test Parquet data to MinIO using a direct S3 client
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("city", ArrowDataType::Utf8, false),
        Field::new("population", ArrowDataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(arrow::array::StringArray::from(vec![
                "Taipei", "Tokyo", "Seoul",
            ])),
            Arc::new(Int32Array::from(vec![2_646_000, 13_960_000, 9_776_000])),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Upload directly via AmazonS3Builder (bypasses StorageRegistry)
    let upload_store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint(minio_endpoint)
        .with_allow_http(true)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .build()
        .expect("failed to create S3 client for MinIO");

    upload_store
        .put(
            &ObjectPath::from("test/cities/data.parquet"),
            PutPayload::from(buf),
        )
        .await
        .expect("failed to upload Parquet to MinIO");

    // 2. Read back via StorageRegistry lazy creation (the code path under test)
    let config = CloudStorageConfig {
        s3: Some(S3StorageConfig {
            region: Some("us-east-1".to_string()),
            endpoint: Some(minio_endpoint.to_string()),
            allow_http: true,
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("minioadmin".to_string()),
        }),
    };
    let registry = Arc::new(StorageRegistry::with_config(config));

    // Register the table via FileConnectorFactory
    let file_factory = Arc::new(FileConnectorFactory::new(registry));
    file_factory
        .register_table(
            "cities",
            &format!("s3://{bucket}/test/cities/data.parquet"),
            FileFormat::Parquet,
            None,
        )
        .await
        .expect("failed to register S3 table");

    let file_schema = Arc::new(arneb_connectors::file::FileSchema::new(
        file_factory.clone(),
    ));
    let file_catalog = Arc::new(arneb_connectors::file::FileCatalog::new(
        "default",
        file_schema,
    ));

    let catalog_manager = Arc::new(CatalogManager::new("file", "default"));
    catalog_manager.register_catalog("file", file_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("file", file_factory);
    let connector_registry = Arc::new(connector_registry);

    // 3. Start test server and query via pgwire
    let addr = start_test_server(catalog_manager, connector_registry).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    stream
        .write_all(&build_query_message(
            "SELECT city, population FROM cities ORDER BY population",
        ))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription (T) — is MinIO running? (docker compose up -d)"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow (D) from MinIO Parquet"
    );
    let row_count = count_data_rows(&response);
    assert_eq!(row_count, 3, "should return 3 city rows from MinIO");

    // Cleanup: remove test data from MinIO
    let _ = upload_store
        .delete(&ObjectPath::from("test/cities/data.parquet"))
        .await;
}

/// Full E2E test: HMS Thrift → table metadata → MinIO S3 → Parquet read → pgwire query.
///
/// Requires: `docker compose up -d` (HMS on localhost:9083, MinIO on localhost:9000).
/// Run with: `cargo test -p arneb-server -- --ignored test_hive_e2e_hms_s3_parquet --nocapture`
#[tokio::test]
#[ignore]
async fn test_hive_e2e_hms_s3_parquet() {
    use arneb_connectors::{CloudStorageConfig, S3StorageConfig, StorageRegistry};
    use hive_metastore::ThriftHiveMetastoreClientBuilder;
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};
    use pilota::FastStr;
    use volo_thrift::MaybeException;

    let minio_endpoint = "http://localhost:9000";
    let hms_addr = "127.0.0.1:9083";
    let bucket = "warehouse";
    let db_name = "arneb_e2e_test";
    let table_name = "students";

    // === Phase 1: Setup — create test data in HMS + MinIO ===

    // 1a. Create Parquet data
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
        Field::new("score", ArrowDataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(arrow::array::StringArray::from(vec![
                "Alice", "Bob", "Carol",
            ])),
            Arc::new(Float64Array::from(vec![95.5, 87.3, 92.1])),
        ],
    )
    .unwrap();

    let mut buf = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // 1b. Upload Parquet to MinIO
    let s3_store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint(minio_endpoint)
        .with_allow_http(true)
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .build()
        .expect("failed to create S3 client for MinIO");

    let parquet_path = format!("{db_name}/{table_name}/data.parquet");
    s3_store
        .put(
            &ObjectPath::from(parquet_path.as_str()),
            PutPayload::from(buf),
        )
        .await
        .expect("failed to upload Parquet to MinIO");

    // 1c. Create database + table in HMS via Thrift
    let hms_setup_client = ThriftHiveMetastoreClientBuilder::new("hive_metastore")
        .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
        .address(hms_addr.parse::<std::net::SocketAddr>().unwrap())
        .build();

    // Drop if exists, then create — using _req variants (HMS 5.x forward-compatible)
    let drop_req = hive_metastore::DropDatabaseRequest {
        name: FastStr::from(db_name),
        catalog_name: None,
        ignore_unknown_db: true,
        delete_data: true,
        cascade: true,
        soft_delete: None,
        txn_id: None,
        delete_managed_dir: None,
    };
    let _ = hms_setup_client.drop_database_req(drop_req).await;

    let create_db_req = hive_metastore::CreateDatabaseRequest {
        database_name: FastStr::from(db_name),
        description: Some(FastStr::from("Arneb E2E test database")),
        ..Default::default()
    };
    match hms_setup_client.create_database_req(create_db_req).await {
        Ok(MaybeException::Ok(_)) => {}
        Ok(MaybeException::Exception(ex)) => panic!("HMS create_database_req exception: {ex:?}"),
        Err(e) => panic!("HMS create_database_req failed: {e}"),
    }

    // Create external table — store the real S3 location in HMS.
    // The custom HMS image (docker/hive-metastore/) bundles hadoop-aws,
    // so HMS can validate s3a:// paths against MinIO directly.
    let hms_location = format!("s3a://{bucket}/{db_name}/{table_name}");

    let serde = hive_metastore::SerDeInfo {
        name: Some(FastStr::from(table_name)),
        serialization_lib: Some(FastStr::from(
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        )),
        ..Default::default()
    };

    let sd = hive_metastore::StorageDescriptor {
        cols: Some(vec![
            hive_metastore::FieldSchema {
                name: Some(FastStr::from("id")),
                r#type: Some(FastStr::from("int")),
                comment: None,
            },
            hive_metastore::FieldSchema {
                name: Some(FastStr::from("name")),
                r#type: Some(FastStr::from("string")),
                comment: None,
            },
            hive_metastore::FieldSchema {
                name: Some(FastStr::from("score")),
                r#type: Some(FastStr::from("double")),
                comment: None,
            },
        ]),
        location: Some(FastStr::from(hms_location.clone())),
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

    let hms_table = hive_metastore::Table {
        table_name: Some(FastStr::from(table_name)),
        db_name: Some(FastStr::from(db_name)),
        owner: Some(FastStr::from("arneb")),
        sd: Some(sd),
        partition_keys: Some(vec![]),
        table_type: Some(FastStr::from("EXTERNAL_TABLE")),
        ..Default::default()
    };
    let create_tbl_req = hive_metastore::CreateTableRequest {
        table: hms_table,
        ..Default::default()
    };
    match hms_setup_client.create_table_req(create_tbl_req).await {
        Ok(MaybeException::Ok(_)) => {}
        Ok(MaybeException::Exception(ex)) => panic!("HMS create_table_req exception: {ex:?}"),
        Err(e) => panic!("HMS create_table_req failed: {e}"),
    }

    // === Phase 2: Wire Arneb with real HMS + MinIO ===

    let s3_config = CloudStorageConfig {
        s3: Some(S3StorageConfig {
            region: Some("us-east-1".to_string()),
            endpoint: Some(minio_endpoint.to_string()),
            allow_http: true,
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("minioadmin".to_string()),
        }),
    };
    let storage_registry = Arc::new(StorageRegistry::with_config(s3_config));

    // Connect to real HMS
    let hms_client = arneb_hive::catalog::HmsClient::new(hms_addr)
        .await
        .expect("failed to connect to HMS");
    let hms_client = Arc::new(hms_client);

    let hive_catalog = Arc::new(arneb_hive::catalog::HiveCatalogProvider::new(
        hms_client.clone(),
    ));
    let hive_factory = arneb_hive::datasource::HiveConnectorFactory::new(storage_registry);
    // No manual register_table_location() needed: HMS now stores the real
    // s3a:// location, and HiveTableProvider::properties() carries it through
    // the planner → connector factory pipeline automatically.

    let catalog_manager = Arc::new(CatalogManager::new("hive_test", db_name));
    catalog_manager.register_catalog("hive_test", hive_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("hive_test", Arc::new(hive_factory));
    let connector_registry = Arc::new(connector_registry);

    // === Phase 3: Query via pgwire ===

    let addr = start_test_server(catalog_manager, connector_registry).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("test", "test"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    let sql = format!("SELECT id, name, score FROM hive_test.{db_name}.{table_name}");
    stream.write_all(&build_query_message(&sql)).await.unwrap();
    let response = read_response(&mut stream).await;

    assert!(
        response_contains_message_type(&response, b'T'),
        "expected RowDescription (T) — full HMS+S3 path"
    );
    assert!(
        response_contains_message_type(&response, b'D'),
        "expected DataRow (D) — Hive table via HMS metadata + MinIO Parquet"
    );
    let row_count = count_data_rows(&response);
    assert_eq!(
        row_count, 3,
        "Hive E2E: expected 3 rows from HMS table backed by MinIO Parquet"
    );

    // === Phase 4: Cleanup ===

    let cleanup_drop_req = hive_metastore::DropDatabaseRequest {
        name: FastStr::from(db_name),
        catalog_name: None,
        ignore_unknown_db: true,
        delete_data: true,
        cascade: true,
        soft_delete: None,
        txn_id: None,
        delete_managed_dir: None,
    };
    let _ = hms_setup_client.drop_database_req(cleanup_drop_req).await;
    let _ = s3_store
        .delete(&ObjectPath::from(parquet_path.as_str()))
        .await;
}

#[test]
fn test_protocol_config_derivation() {
    let config = ProtocolConfig {
        bind_address: format!("{}:{}", "0.0.0.0", 5433),
    };
    assert_eq!(config.bind_address, "0.0.0.0:5433");

    let config2 = ProtocolConfig {
        bind_address: format!("{}:{}", "127.0.0.1", 5432),
    };
    assert_eq!(config2.bind_address, "127.0.0.1:5432");
}
