//! An `information_schema` statement the engine cannot run still gets the
//! fixed result, counted as a pgwire catalog fallback. Its own binary: the
//! fallback counter is per process, and the other catalog binaries assert
//! it does not move.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_protocol::{ProtocolConfig, ProtocolServer};

fn table() -> Arc<MemoryTable> {
    let infos = vec![ColumnInfo {
        name: "k".into(),
        data_type: DataType::Int64,
        nullable: false,
    }];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "k",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

/// `file.default.{nation, region}`, with `file` the default catalog.
async fn pg_server() -> u16 {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("nation", table());
    default_schema.register_table("region", table());
    let file = Arc::new(MemoryCatalog::new());
    file.register_schema("default", default_schema);
    let cm = CatalogManager::new("file", "default");
    cm.register_catalog("file", file.clone());
    let registry = ConnectorRegistry::new();
    registry.register(
        "file",
        Arc::new(MemoryConnectorFactory::new(file, "default")),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        Arc::new(cm),
        Arc::new(registry),
    );
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

#[tokio::test]
async fn information_schema_fallback_still_answers() {
    let port = pg_server().await;
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=smoke dbname=file"),
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    let before = arneb_common::metrics::pg_catalog_fallbacks();
    // The engine refuses this statement (see FALLBACK_SQL); the fixed
    // handler answers with all of information_schema.tables.
    let mut rows: Vec<Vec<String>> = client
        .simple_query(FALLBACK_SQL)
        .await
        .unwrap_or_else(|e| panic!("{FALLBACK_SQL}\n{e:?}"))
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or_default().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    rows.sort();
    // The fixed handler's full row (catalog, schema, name, type) for both
    // tables, whatever the statement asked.
    assert_eq!(
        rows,
        [
            ["file", "default", "nation", "BASE TABLE"],
            ["file", "default", "region", "BASE TABLE"],
        ]
    );
    assert_eq!(arneb_common::metrics::pg_catalog_fallbacks(), before + 1);
}

const FALLBACK_SQL: &str =
    "SELECT * FROM information_schema.tables WHERE table_name ~~ 'n%' ESCAPE '\\'";
