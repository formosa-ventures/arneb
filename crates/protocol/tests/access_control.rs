//! File-based access control end to end (file-based-access-control): the
//! same rules checked through pgwire and the Trino protocol, for table
//! reads, metadata listings, `execute` and `system.runtime.queries`; and
//! a tracker kill stopping a running statement on either protocol.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

use arneb_catalog::{AccessRules, CatalogManager};
use arneb_common::error::ArnebError;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::ExecutionContext;
use arneb_planner::LogicalPlan;
use arneb_protocol::{
    DistributedExecutor, ProtocolConfig, ProtocolServer, TrinoConfig, TrinoServer,
};
use arneb_scheduler::{KillOutcome, QueryState, QueryTracker};

const RULES: &str = r#"{
    "catalogs": [
        {"catalog": "memory", "allow": "all"},
        {"user": "alice", "catalog": "lake", "allow": "none"},
        {"allow": "read-only"}
    ],
    "schemas": [{"owner": false}],
    "tables": [
        {"user": "alice", "catalog": "memory", "table": "open_t", "privileges": ["SELECT"]},
        {"user": "writer", "catalog": "memory", "privileges": ["SELECT", "INSERT", "DELETE"]},
        {"user": "owner", "catalog": "memory", "privileges": ["OWNERSHIP"]},
        {"user": "admin", "privileges": ["SELECT"]}
    ],
    "queries": [
        {"user": "admin", "allow": ["execute", "view"]},
        {"user": "alice|writer|owner", "allow": ["execute"]}
    ]
}"#;

fn one_column_table() -> Arc<MemoryTable> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        ArrowDataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let infos = vec![ColumnInfo {
        name: "n".into(),
        data_type: DataType::Int64,
        nullable: false,
    }];
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

/// `memory.default.{open_t,secret}` and `lake.sales.orders`.
fn state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("open_t", one_column_table());
    default_schema.register_table("secret", one_column_table());
    let memory = Arc::new(MemoryCatalog::new());
    memory.register_schema("default", default_schema);
    let sales = Arc::new(MemorySchema::new());
    sales.register_table("orders", one_column_table());
    let lake = Arc::new(MemoryCatalog::new());
    lake.register_schema("sales", sales);

    let cm = CatalogManager::new("memory", "default");
    cm.register_catalog("memory", memory.clone());
    cm.register_catalog("lake", lake.clone());
    let mut registry = ConnectorRegistry::new();
    registry.register(
        "memory",
        Arc::new(MemoryConnectorFactory::new(memory, "default")),
    );
    registry.register("lake", Arc::new(MemoryConnectorFactory::new(lake, "sales")));
    (Arc::new(cm), Arc::new(registry))
}

fn rules() -> Option<Arc<AccessRules>> {
    Some(Arc::new(AccessRules::from_json(RULES).unwrap()))
}

/// Executes every plan by never returning on its own.
struct HangingExecutor;

#[async_trait::async_trait]
impl DistributedExecutor for HangingExecutor {
    async fn execute(
        &self,
        _plan: LogicalPlan,
        _exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ArnebError> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(Vec::new())
    }

    fn has_workers(&self) -> bool {
        true
    }
}

async fn pg_server(tracker: Arc<QueryTracker>) -> u16 {
    pg_server_with(tracker, None).await
}

async fn pg_server_with(
    tracker: Arc<QueryTracker>,
    executor: Option<Arc<dyn DistributedExecutor>>,
) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let mut server = ProtocolServer::new(
        ProtocolConfig {
            bind_address: format!("127.0.0.1:{port}"),
        },
        cm,
        cr,
    )
    .with_query_tracker(tracker)
    .with_access_rules(rules());
    if let Some(executor) = executor {
        server = server.with_distributed_executor(executor);
    }
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    port
}

async fn pg(port: u16, user: &str) -> tokio_postgres::Client {
    let (client, conn) =
        tokio_postgres::connect(&format!("host=127.0.0.1 port={port} user={user}"), NoTls)
            .await
            .unwrap();
    tokio::spawn(conn);
    client
}

fn pg_rows(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or_default().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn pg_error(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let err = client.simple_query(sql).await.unwrap_err();
    let db = err.as_db_error().expect("a server error");
    (db.code().code().to_owned(), db.message().to_owned())
}

async fn trino_server(tracker: Arc<QueryTracker>) -> String {
    trino_server_with(tracker, None).await
}

async fn trino_server_with(
    tracker: Arc<QueryTracker>,
    executor: Option<Arc<dyn DistributedExecutor>>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (cm, cr) = state();
    let mut server = TrinoServer::new(
        TrinoConfig {
            bind_address: format!("127.0.0.1:{port}"),
            max_wait: Duration::from_millis(50),
            ..TrinoConfig::default()
        },
        cm,
        cr,
    )
    .with_query_tracker(tracker)
    .with_access_rules(rules());
    if let Some(executor) = executor {
        server = server.with_distributed_executor(executor);
    }
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    format!("http://127.0.0.1:{port}")
}

/// Rows of `sql` run as `user`, or the error document.
async fn trino(base: &str, user: &str, sql: &str) -> Result<Vec<Vec<Value>>, Value> {
    let http = reqwest::Client::new();
    let mut doc: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", user)
        .body(sql.to_string())
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut rows = Vec::new();
    loop {
        if let Some(err) = doc.get("error") {
            return Err(err.clone());
        }
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            rows.extend(data.iter().map(|r| r.as_array().unwrap().clone()));
        }
        let Some(next) = doc.get("nextUri").and_then(Value::as_str) else {
            return Ok(rows);
        };
        doc = http.get(next).send().await.unwrap().json().await.unwrap();
    }
}

fn strings(rows: Vec<Vec<Value>>) -> Vec<String> {
    rows.into_iter()
        .map(|r| r[0].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn pgwire_checks_reads_listings_and_execute() {
    let port = pg_server(Arc::new(QueryTracker::new())).await;
    let alice = pg(port, "alice").await;

    let rows = pg_rows(&alice.simple_query("SELECT n FROM open_t").await.unwrap());
    assert_eq!(rows.len(), 3);
    // Plan errors carry the diagnostic rendering (`error: ...`).
    let (code, message) = pg_error(&alice, "SELECT n FROM secret").await;
    assert_eq!(code, "42501");
    assert!(
        message.contains("Access Denied: Cannot select from table memory.default.secret"),
        "{message}"
    );
    let (code, message) = pg_error(&alice, "SELECT * FROM lake.sales.orders").await;
    assert_eq!(code, "42501");
    assert!(message.contains("lake.sales.orders"), "{message}");
    let (_, missing) = pg_error(&alice, "SELECT * FROM memory.default.nope").await;
    assert!(missing.contains("Access Denied"), "{missing}");

    let tables = pg_rows(
        &alice
            .simple_query(
                "SELECT table_catalog, table_schema, table_name FROM information_schema.tables",
            )
            .await
            .unwrap(),
    );
    // The metadata intercept returns its full row whatever the projection.
    let names: Vec<&[String]> = tables.iter().map(|r| &r[..3]).collect();
    assert_eq!(names, vec![["memory", "default", "open_t"]]);

    // Uppercase names fold to a visible table only, so the denial for a
    // hidden one names it as written, exactly as for a missing table.
    let rows = pg_rows(&alice.simple_query("SELECT n FROM OPEN_T").await.unwrap());
    assert_eq!(rows.len(), 3);
    for table in ["SECRET", "NOPE"] {
        let (code, message) = pg_error(&alice, &format!("SELECT n FROM {table}")).await;
        assert_eq!(code, "42501");
        assert!(
            message.contains(&format!("memory.default.{table}")),
            "{table}: {message}"
        );
    }

    // A hidden schema in search_path is skipped as if it did not exist, so
    // whether the path takes effect does not reveal it.
    for path in ["lake.sales", "lake.nope"] {
        alice
            .simple_query(&format!("SET search_path TO {path}"))
            .await
            .unwrap();
        let (code, message) = pg_error(&alice, "SELECT * FROM orders").await;
        assert_eq!(code, "42501");
        assert!(
            message.contains("memory.default.orders"),
            "{path}: {message}"
        );
    }
    alice.simple_query("RESET search_path").await.unwrap();

    let admin = pg(port, "admin").await;
    let tables = pg_rows(
        &admin
            .simple_query("SELECT table_name FROM information_schema.tables")
            .await
            .unwrap(),
    );
    assert_eq!(tables.len(), 3, "{tables:?}");

    let carol = pg(port, "carol").await;
    assert_eq!(
        pg_error(&carol, "SELECT 1").await,
        ("42501".into(), "Access Denied: Cannot execute query".into())
    );
}

#[tokio::test]
async fn trino_checks_reads_and_listings() {
    let base = trino_server(Arc::new(QueryTracker::new())).await;

    let catalogs = strings(trino(&base, "alice", "SHOW CATALOGS").await.unwrap());
    assert_eq!(catalogs, vec!["memory", "system"]);
    assert!(trino(&base, "alice", "SHOW SCHEMAS FROM lake")
        .await
        .is_err());
    let tables = strings(trino(&base, "alice", "SHOW TABLES").await.unwrap());
    assert_eq!(tables, vec!["open_t"]);

    let err = trino(&base, "alice", "SELECT n FROM secret")
        .await
        .unwrap_err();
    assert_eq!(err["errorName"], "PERMISSION_DENIED");
    assert_eq!(err["errorType"], "USER_ERROR");
    assert!(
        err["message"]
            .as_str()
            .unwrap()
            .contains("Access Denied: Cannot select from table memory.default.secret"),
        "{err}"
    );
    let jdbc = trino(
        &base,
        "alice",
        "SELECT table_name FROM system.jdbc.tables WHERE table_cat = 'memory'",
    )
    .await
    .unwrap();
    assert_eq!(strings(jdbc), vec!["open_t"]);

    let admin = strings(
        trino(&base, "admin", "SHOW TABLES FROM lake.sales")
            .await
            .unwrap(),
    );
    assert_eq!(admin, vec!["orders"]);
    let err = trino(&base, "carol", "SELECT 1").await.unwrap_err();
    assert_eq!(err["errorName"], "PERMISSION_DENIED");
}

#[tokio::test]
async fn runtime_queries_show_other_users_only_where_view_is_allowed() {
    let tracker = Arc::new(QueryTracker::with_history_limit(50));
    let port = pg_server(Arc::clone(&tracker)).await;
    let base = trino_server(Arc::clone(&tracker)).await;
    let alice = pg(port, "alice").await;
    alice.simple_query("SELECT 41").await.unwrap();
    trino(&base, "admin", "SELECT 42").await.unwrap();

    let sql = "SELECT \"user\", query FROM system.runtime.queries \
               WHERE query IN ('SELECT 41', 'SELECT 42') ORDER BY query";
    let seen = pg_rows(&alice.simple_query(sql).await.unwrap());
    assert_eq!(seen, vec![vec!["alice", "SELECT 41"]]);
    let seen = trino(&base, "admin", sql).await.unwrap();
    assert_eq!(seen.len(), 2, "{seen:?}");
}

const INSERT_SQL: &str = "INSERT INTO memory.default.open_t SELECT n FROM memory.default.open_t";

#[tokio::test]
async fn pgwire_refuses_writes_without_the_privilege() {
    let port = pg_server(Arc::new(QueryTracker::new())).await;
    let alice = pg(port, "alice").await;

    let cases = [
        (
            "CREATE TABLE memory.default.t2 (n BIGINT)",
            "Cannot create table memory.default.t2 (requires OWNERSHIP)",
        ),
        (INSERT_SQL, "requires INSERT"),
        ("DELETE FROM memory.default.open_t", "requires DELETE"),
        (
            "DROP VIEW memory.default.v",
            "Access Denied: Cannot drop view memory.default.v (requires OWNERSHIP)",
        ),
        (
            "CREATE VIEW memory.default.v2 AS SELECT n FROM memory.default.open_t",
            "Access Denied: Cannot create view memory.default.v2 (requires OWNERSHIP)",
        ),
        (
            "CREATE TABLE t3 (n BIGINT)",
            "Cannot create table memory.default.t3 (requires OWNERSHIP)",
        ),
    ];
    for (sql, expected) in cases {
        let (code, message) = pg_error(&alice, sql).await;
        assert_eq!(code, "42501", "{sql}: {message}");
        assert!(message.contains(expected), "{sql}: {message}");
    }
    let (code, message) = pg_error(&alice, &format!("EXPLAIN {INSERT_SQL}")).await;
    assert_eq!(code, "42501", "{message}");
    assert!(message.contains("requires INSERT"), "{message}");

    let writer = pg(port, "writer").await;
    let (code, message) = pg_error(&writer, INSERT_SQL).await;
    assert_ne!(code, "42501", "{message}");
    let (code, message) = pg_error(&writer, "CREATE TABLE memory.default.t4 (n BIGINT)").await;
    assert_eq!(code, "42501", "{message}");

    let owner = pg(port, "owner").await;
    let (code, message) = pg_error(&owner, "CREATE TABLE memory.default.t5 (n BIGINT)").await;
    assert_ne!(code, "42501", "{message}");
    let (code, message) = pg_error(&owner, "CREATE TABLE lake.sales.x (n BIGINT)").await;
    assert_eq!(code, "42501", "{message}");
    assert!(message.contains("catalog lake is read-only"), "{message}");
}

#[tokio::test]
async fn trino_refuses_writes_without_the_privilege() {
    let base = trino_server(Arc::new(QueryTracker::new())).await;
    let err = trino(
        &base,
        "alice",
        "CREATE TABLE memory.default.t2 AS SELECT n FROM memory.default.open_t",
    )
    .await
    .unwrap_err();
    assert_eq!(err["errorName"], "PERMISSION_DENIED", "{err}");
    let message = err["message"].as_str().unwrap();
    assert!(message.contains("memory.default.t2"), "{message}");
    assert!(message.contains("OWNERSHIP"), "{message}");

    let err = trino(&base, "owner", "CREATE TABLE memory.default.t6 (n BIGINT)")
        .await
        .unwrap_err();
    assert_ne!(err["errorName"], "PERMISSION_DENIED", "{err}");
}

#[tokio::test]
async fn trino_refuses_explain_of_a_write() {
    let base = trino_server(Arc::new(QueryTracker::new())).await;
    let err = trino(&base, "alice", &format!("EXPLAIN {INSERT_SQL}"))
        .await
        .unwrap_err();
    assert_eq!(err["errorName"], "PERMISSION_DENIED", "{err}");
}

/// The only tracked query, once it is running.
async fn running_query(tracker: &QueryTracker) -> arneb_common::identifiers::QueryId {
    for _ in 0..200 {
        if let Some(q) = tracker
            .list_queries(Some(QueryState::Running))
            .into_iter()
            .next()
        {
            return q.query_id;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no query reached RUNNING");
}

#[tokio::test]
async fn tracker_kill_stops_a_pgwire_statement() {
    let tracker = Arc::new(QueryTracker::new());
    let port = pg_server_with(Arc::clone(&tracker), Some(Arc::new(HangingExecutor))).await;
    let alice = pg(port, "alice").await;
    let statement = tokio::spawn(async move {
        let err = alice
            .simple_query("SELECT n FROM open_t")
            .await
            .unwrap_err();
        let db = err.as_db_error().expect("a server error");
        (db.code().code().to_owned(), db.message().to_owned())
    });
    let id = running_query(&tracker).await;
    assert_eq!(
        tracker.kill_query(&id, "root").unwrap(),
        KillOutcome::Killed {
            was: QueryState::Running
        }
    );
    let (code, message) = tokio::time::timeout(Duration::from_secs(2), statement)
        .await
        .expect("the statement stopped within 2 s")
        .unwrap();
    assert_eq!(code, "57014");
    assert_eq!(message, "canceling statement due to user request");
    assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
}

#[tokio::test]
async fn tracker_kill_stops_a_trino_query() {
    let tracker = Arc::new(QueryTracker::new());
    let base = trino_server_with(Arc::clone(&tracker), Some(Arc::new(HangingExecutor))).await;
    let http = reqwest::Client::new();
    let doc: Value = http
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "alice")
        .body("SELECT n FROM open_t")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let next = doc["nextUri"].as_str().unwrap().to_owned();
    let id = running_query(&tracker).await;
    assert!(matches!(
        tracker.kill_query(&id, "root").unwrap(),
        KillOutcome::Killed { .. }
    ));
    let polled: Value = http.get(&next).send().await.unwrap().json().await.unwrap();
    assert_eq!(polled["stats"]["state"], "FAILED", "{polled}");
    assert_eq!(polled["error"]["errorName"], "USER_CANCELED");
    let message = polled["error"]["message"].as_str().unwrap();
    assert!(message.contains("killed by root"), "{message}");
    assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
}
