use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

/// Helper to start a server on a random port and return the address.
async fn start_test_server(
    catalog_manager: Arc<CatalogManager>,
    connector_registry: Arc<ConnectorRegistry>,
) -> String {
    // Bind to port 0 to get a random available port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let handler_factory = Arc::new(arneb_protocol::__private::HandlerFactory {
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

fn create_empty_server_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    let connector_registry = Arc::new(ConnectorRegistry::new());
    (catalog_manager, connector_registry)
}

fn create_server_with_users_table() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )
    .unwrap();

    let mem_table = Arc::new(MemoryTable::new(
        vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
            },
        ],
        vec![batch],
    ));

    let mem_schema = Arc::new(MemorySchema::new());
    mem_schema.register_table("users", mem_table);
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);

    // Keep a reference to the MemoryCatalog for the ConnectorFactory
    let factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");

    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", mem_catalog);

    let mut connector_registry = ConnectorRegistry::new();
    connector_registry.register("memory", Arc::new(factory));

    (catalog_manager, Arc::new(connector_registry))
}

/// Build a PostgreSQL startup message for protocol v3.
fn build_startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut params = Vec::new();
    params.extend_from_slice(b"user\0");
    params.extend_from_slice(user.as_bytes());
    params.push(0);
    params.extend_from_slice(b"database\0");
    params.extend_from_slice(database.as_bytes());
    params.push(0);
    params.push(0); // terminator

    let len = 4 + 4 + params.len(); // length + version + params
    let mut msg = Vec::new();
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(&196608i32.to_be_bytes()); // version 3.0
    msg.extend_from_slice(&params);
    msg
}

/// Build a PostgreSQL Query message.
fn build_query_message(sql: &str) -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'Q'); // Query message type
    let len = 4 + sql.len() + 1; // length + sql + null terminator
    msg.extend_from_slice(&(len as i32).to_be_bytes());
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    msg
}

/// Build a PostgreSQL Terminate message.
fn build_terminate_message() -> Vec<u8> {
    let mut msg = Vec::new();
    msg.push(b'X'); // Terminate
    msg.extend_from_slice(&4i32.to_be_bytes());
    msg
}

/// Read all available data from the stream (with a timeout).
async fn read_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 4096];
    // Give server time to respond
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    match tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf)).await {
        Ok(Ok(n)) => buf[..n].to_vec(),
        _ => Vec::new(),
    }
}

/// Check if response contains a specific message type byte.
fn response_contains_message_type(data: &[u8], msg_type: u8) -> bool {
    let mut pos = 0;
    while pos < data.len() {
        if data[pos] == msg_type {
            return true;
        }
        // Skip to next message: type(1) + length(4) + payload
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

#[tokio::test]
async fn test_startup_handshake() {
    let (cm, cr) = create_empty_server_state();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    let startup = build_startup_message("testuser", "testdb");
    stream.write_all(&startup).await.unwrap();

    let response = read_response(&mut stream).await;

    // Response should contain 'R' (Authentication) and 'Z' (ReadyForQuery)
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
async fn test_terminate_closes_gracefully() {
    let (cm, cr) = create_empty_server_state();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Send terminate
    stream.write_all(&build_terminate_message()).await.unwrap();

    // Connection should close — further reads should return 0 bytes
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(std::time::Duration::from_millis(500), stream.read(&mut buf))
        .await
        .unwrap_or(Ok(0))
        .unwrap_or(0);
    assert_eq!(n, 0, "expected connection to be closed after Terminate");
}

#[tokio::test]
async fn test_invalid_sql_returns_error() {
    let (cm, cr) = create_empty_server_state();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Send invalid SQL
    stream
        .write_all(&build_query_message("SELEC * FROM users"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    // Should contain ErrorResponse ('E') and ReadyForQuery ('Z')
    assert!(
        response_contains_message_type(&response, b'E'),
        "expected ErrorResponse (E) for invalid SQL"
    );
    assert!(
        response_contains_message_type(&response, b'Z'),
        "expected ReadyForQuery (Z) after error"
    );
}

#[tokio::test]
async fn test_select_from_memory_table() {
    let (cm, cr) = create_server_with_users_table();
    let addr = start_test_server(cm, cr).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(&build_startup_message("testuser", "testdb"))
        .await
        .unwrap();
    let _ = read_response(&mut stream).await;

    // Query the users table
    stream
        .write_all(&build_query_message("SELECT id, name FROM users"))
        .await
        .unwrap();
    let response = read_response(&mut stream).await;

    // Should contain RowDescription ('T'), DataRow ('D'), CommandComplete ('C'), ReadyForQuery ('Z')
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
