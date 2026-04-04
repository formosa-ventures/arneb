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
    let file_factory = Arc::new(FileConnectorFactory::new());
    file_factory
        .register_table("test_data", &parquet_path, FileFormat::Parquet, None)
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
