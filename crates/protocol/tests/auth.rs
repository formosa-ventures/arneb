//! End-to-end tests for pgwire password authentication (SCRAM-SHA-256).
//!
//! Each test starts a real `ProtocolServer` on an ephemeral port and connects
//! with `tokio-postgres`, a production PostgreSQL client.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_postgres::error::SqlState;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{AuthMethod, ProtocolConfig, ProtocolServer, ScramVerifier, UserCredentials};

fn users_table_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("name", ArrowDataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
        ],
    )
    .unwrap();
    let table = Arc::new(MemoryTable::new(
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
    mem_schema.register_table("users", table);
    let mem_catalog = Arc::new(MemoryCatalog::new());
    mem_catalog.register_schema("default", mem_schema);
    let factory = MemoryConnectorFactory::new(mem_catalog.clone(), "default");
    let catalog_manager = Arc::new(CatalogManager::new("memory", "default"));
    catalog_manager.register_catalog("memory", mem_catalog);
    let mut registry = ConnectorRegistry::new();
    registry.register("memory", Arc::new(factory));
    (catalog_manager, Arc::new(registry))
}

/// Start a server with the given auth mode on an ephemeral port; returns the port.
async fn start_server(auth: AuthMethod) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = users_table_state();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_auth(auth);
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

/// Password mode with two users: `alice` (verifier produced by arneb) and
/// `bob` (verifier produced by `postgres-protocol`, the implementation
/// rust-postgres uses for `ALTER ROLE ... PASSWORD`), proving interop with
/// PostgreSQL's stored-verifier format.
fn password_auth() -> AuthMethod {
    let alice = ScramVerifier::generate("alice-pw").unwrap();
    let bob: ScramVerifier = postgres_protocol::password::scram_sha_256(b"bob-pw")
        .parse()
        .unwrap();
    AuthMethod::ScramSha256(Arc::new(
        UserCredentials::new(vec![("alice".to_string(), alice), ("bob".to_string(), bob)]).unwrap(),
    ))
}

async fn connect(
    port: u16,
    user: &str,
    password: Option<&str>,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut config = tokio_postgres::Config::new();
    config
        .host("127.0.0.1")
        .port(port)
        .user(user)
        .dbname("default");
    if let Some(pw) = password {
        config.password(pw);
    }
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn assert_invalid_password(result: Result<tokio_postgres::Client, tokio_postgres::Error>) {
    let Err(err) = result else {
        panic!("authentication must fail");
    };
    let db = err
        .as_db_error()
        .unwrap_or_else(|| panic!("expected a server error, got: {err}"));
    assert_eq!(db.code(), &SqlState::INVALID_PASSWORD, "{db:?}");
    assert_eq!(db.severity(), "FATAL");
}

#[tokio::test]
async fn correct_password_allows_simple_and_extended_queries() {
    let port = start_server(password_auth()).await;

    for (user, pw) in [("alice", "alice-pw"), ("bob", "bob-pw")] {
        let client = connect(port, user, Some(pw))
            .await
            .unwrap_or_else(|e| panic!("{user} should authenticate: {e}"));

        // Simple Query protocol.
        let msgs = client
            .simple_query("SELECT name FROM users WHERE id = 2")
            .await
            .unwrap();
        let rows: Vec<_> = msgs
            .iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(rows, vec!["Bob".to_string()]);

        // Extended Query protocol (Parse/Bind/Execute).
        let rows = client.query("SELECT id FROM users", &[]).await.unwrap();
        assert_eq!(rows.len(), 3);
    }
}

#[tokio::test]
async fn wrong_password_is_rejected_with_28p01() {
    let port = start_server(password_auth()).await;
    assert_invalid_password(connect(port, "alice", Some("not-the-password")).await);
    // bob's password does not work for alice either.
    assert_invalid_password(connect(port, "alice", Some("bob-pw")).await);
}

#[tokio::test]
async fn unknown_user_is_rejected_with_28p01() {
    let port = start_server(password_auth()).await;
    assert_invalid_password(connect(port, "mallory", Some("alice-pw")).await);
}

#[tokio::test]
async fn missing_password_is_rejected() {
    let port = start_server(password_auth()).await;
    assert!(connect(port, "alice", None).await.is_err());
}

#[tokio::test]
async fn auth_none_accepts_any_client() {
    let port = start_server(AuthMethod::None).await;
    let client = connect(port, "anyone", None).await.unwrap();
    let rows = client.query("SELECT id FROM users", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
}

/// A client that skips the SASL exchange and sends a query right away must
/// not get any rows back, and the connection must be closed.
#[tokio::test]
async fn query_before_authentication_is_refused() {
    let port = start_server(password_auth()).await;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // StartupMessage (protocol 3.0, user=alice).
    let params = b"user\0alice\0database\0default\0\0";
    let mut startup = Vec::new();
    startup.extend_from_slice(&((8 + params.len()) as i32).to_be_bytes());
    startup.extend_from_slice(&196608i32.to_be_bytes());
    startup.extend_from_slice(params);
    stream.write_all(&startup).await.unwrap();

    // Server must answer with AuthenticationSASL ('R', code 10).
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    assert!(n >= 9 && buf[0] == b'R', "expected Authentication message");
    assert_eq!(i32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]), 10);

    // Skip authentication and send a Simple Query.
    let sql = b"SELECT name FROM users\0";
    let mut query = vec![b'Q'];
    query.extend_from_slice(&((4 + sql.len()) as i32).to_be_bytes());
    query.extend_from_slice(sql);
    stream.write_all(&query).await.unwrap();

    // Collect everything until the server closes the socket.
    let mut received = Vec::new();
    let read_all = async {
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => received.extend_from_slice(&buf[..n]),
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), read_all)
        .await
        .expect("server must close the connection");

    let mut pos = 0;
    let mut saw_error = false;
    while pos + 5 <= received.len() {
        let tag = received[pos];
        assert!(
            tag != b'T' && tag != b'D',
            "no row data may be returned before authentication"
        );
        saw_error |= tag == b'E';
        let len = i32::from_be_bytes([
            received[pos + 1],
            received[pos + 2],
            received[pos + 3],
            received[pos + 4],
        ]) as usize;
        pos += 1 + len;
    }
    assert!(saw_error, "expected an ErrorResponse");
}
