use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use trino_catalog::CatalogManager;
use trino_connectors::file::{FileCatalog, FileConnectorFactory, FileFormat, FileSchema};
use trino_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema};
use trino_connectors::ConnectorRegistry;
use trino_protocol::ProtocolConfig;

/// Helper to start a server on a random port and return the address.
async fn start_test_server(
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let handler_factory = Arc::new(trino_protocol::__private::HandlerFactory {
        catalog_manager,
        connector_registry,
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
