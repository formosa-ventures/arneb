//! End-to-end tests for the Trino client REST protocol: a real HTTP server
//! on an ephemeral port, driven with `reqwest` exactly like a Trino client
//! (POST /v1/statement, follow `nextUri`, DELETE to cancel).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use reqwest::header::HeaderMap;
use serde_json::{json, Value};

use arneb_catalog::CatalogManager;
use arneb_common::error::ArnebError;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_execution::ExecutionContext;
use arneb_planner::LogicalPlan;
use arneb_protocol::{DistributedExecutor, TrinoConfig, TrinoServer};
use arneb_scheduler::{QueryState, QueryTracker};

fn column_infos(schema: &Schema) -> Vec<ColumnInfo> {
    schema
        .fields()
        .iter()
        .map(|f| ColumnInfo {
            name: f.name().clone(),
            data_type: DataType::try_from(f.data_type().clone()).unwrap(),
            nullable: f.is_nullable(),
        })
        .collect()
}

fn table(schema: Arc<Schema>, columns: Vec<ArrayRef>) -> Arc<MemoryTable> {
    let infos = column_infos(&schema);
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    Arc::new(MemoryTable::new(infos, vec![batch]))
}

/// `memory.default.{typed,numbers}` plus a second catalog `lake.sales.orders`.
fn engine_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
    let typed_schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowDataType::Int64, false),
        Field::new("small", ArrowDataType::Int32, true),
        Field::new("name", ArrowDataType::Utf8, true),
        Field::new("price", ArrowDataType::Decimal128(12, 2), true),
        Field::new("day", ArrowDataType::Date32, true),
        Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("flag", ArrowDataType::Boolean, true),
        Field::new("ratio", ArrowDataType::Float64, true),
    ]));
    let typed = table(
        typed_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![Some(7), None])),
            Arc::new(StringArray::from(vec![Some("alpha"), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(12345), None])
                    .with_precision_and_scale(12, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Some(9131), None])),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(1_577_934_245_123),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Float64Array::from(vec![Some(0.25), None])),
        ],
    );

    let numbers_schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        ArrowDataType::Int64,
        false,
    )]));
    let numbers = table(
        numbers_schema,
        vec![Arc::new(Int64Array::from((1..=25).collect::<Vec<i64>>()))],
    );

    let default_schema = Arc::new(MemorySchema::new());
    default_schema.register_table("typed", typed);
    default_schema.register_table("numbers", numbers);
    let memory = Arc::new(MemoryCatalog::new());
    memory.register_schema("default", default_schema);

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", ArrowDataType::Int64, false),
        Field::new("status", ArrowDataType::Utf8, false),
    ]));
    let orders = table(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(StringArray::from(vec!["open", "shipped", "open"])),
        ],
    );
    let sales = Arc::new(MemorySchema::new());
    sales.register_table("orders", orders);
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

fn test_config() -> TrinoConfig {
    TrinoConfig {
        bind_address: "127.0.0.1:0".into(),
        max_wait: Duration::from_millis(200),
        max_page_rows: 10,
        ..TrinoConfig::default()
    }
}

async fn start(server: TrinoServer) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { server.serve(listener).await });
    format!("http://{addr}")
}

async fn start_default() -> String {
    let (cm, registry) = engine_state();
    start(TrinoServer::new(test_config(), cm, registry)).await
}

/// Everything a Trino client accumulates while draining a query.
#[derive(Debug, Default)]
struct Drained {
    documents: Vec<Value>,
    columns: Option<Value>,
    rows: Vec<Value>,
    error: Option<Value>,
    headers: Vec<(String, String)>,
    update_type: Option<String>,
}

async fn post(base: &str, sql: &str, headers: &[(&str, &str)]) -> Value {
    let client = reqwest::Client::new();
    let mut req = client
        .post(format!("{base}/v1/statement"))
        .header("X-Trino-User", "test")
        .body(sql.to_string());
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = req.send().await.unwrap();
    assert_eq!(resp.status(), 200);
    resp.json().await.unwrap()
}

fn collect_headers(headers: &HeaderMap, out: &mut Vec<(String, String)>) {
    for (k, v) in headers {
        let name = k.as_str().to_ascii_lowercase();
        if name.starts_with("x-trino-") || name.starts_with("x-presto-") {
            let pair = (name, v.to_str().unwrap().to_string());
            if !out.contains(&pair) {
                out.push(pair);
            }
        }
    }
}

async fn run(base: &str, sql: &str, headers: &[(&str, &str)]) -> Drained {
    let client = reqwest::Client::new();
    let mut drained = Drained::default();
    let mut doc = post(base, sql, headers).await;
    assert_eq!(doc["stats"]["state"], "QUEUED");
    loop {
        if let Some(cols) = doc.get("columns") {
            drained.columns = Some(cols.clone());
        }
        if let Some(data) = doc.get("data").and_then(Value::as_array) {
            drained.rows.extend(data.iter().cloned());
        }
        if let Some(err) = doc.get("error") {
            drained.error = Some(err.clone());
        }
        if let Some(kind) = doc.get("updateType").and_then(Value::as_str) {
            drained.update_type = Some(kind.to_string());
        }
        let next = doc
            .get("nextUri")
            .and_then(Value::as_str)
            .map(str::to_string);
        drained.documents.push(doc);
        let Some(next) = next else { break };
        let resp = client.get(&next).send().await.unwrap();
        assert_eq!(resp.status(), 200, "GET {next}");
        collect_headers(resp.headers(), &mut drained.headers);
        doc = resp.json().await.unwrap();
    }
    drained
}

fn column_names(d: &Drained) -> Vec<String> {
    d.columns
        .as_ref()
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

fn first_column(d: &Drained) -> Vec<Value> {
    d.rows.iter().map(|r| r[0].clone()).collect()
}

#[tokio::test]
async fn info_endpoints() {
    let base = start_default().await;
    let info: Value = reqwest::get(format!("{base}/v1/info"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["starting"], false);
    assert_eq!(info["coordinator"], true);
    assert!(info["nodeVersion"]["version"]
        .as_str()
        .unwrap()
        .contains("arneb"));
    let state: Value = reqwest::get(format!("{base}/v1/info/state"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(state, json!("ACTIVE"));
}

#[tokio::test]
async fn select_encodes_trino_types() {
    let base = start_default().await;
    let d = run(
        &base,
        "SELECT id, small, name, price, day, ts, flag, ratio FROM typed ORDER BY id",
        &[],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    let cols = d.columns.clone().unwrap();
    let types: Vec<&str> = cols
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        vec![
            "bigint",
            "integer",
            "varchar",
            "decimal(12,2)",
            "date",
            "timestamp(3)",
            "boolean",
            "double"
        ]
    );
    assert_eq!(cols[3]["typeSignature"]["rawType"], "decimal");
    assert_eq!(cols[3]["typeSignature"]["arguments"][0]["value"], 12);
    assert_eq!(cols[2]["typeSignature"]["arguments"][0]["kind"], "LONG");
    assert_eq!(
        d.rows,
        vec![
            json!([
                1,
                7,
                "alpha",
                "123.45",
                "1995-01-01",
                "2020-01-02 03:04:05.123",
                true,
                0.25
            ]),
            json!([2, null, null, null, null, null, null, null]),
        ]
    );
    let last = d.documents.last().unwrap();
    assert_eq!(last["stats"]["state"], "FINISHED");
    assert!(last["infoUri"].as_str().unwrap().contains("/v1/query/"));
}

#[tokio::test]
async fn results_are_paged_in_bounded_chunks() {
    let base = start_default().await;
    let d = run(&base, "SELECT n FROM numbers ORDER BY n", &[]).await;
    assert!(d.error.is_none(), "{:?}", d.error);
    let pages: Vec<usize> = d
        .documents
        .iter()
        .filter_map(|doc| doc.get("data").and_then(Value::as_array).map(Vec::len))
        .collect();
    assert_eq!(pages, vec![10, 10, 5], "page sizes");
    let values: Vec<i64> = first_column(&d)
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(values, (1..=25).collect::<Vec<_>>());
    // Every data page carries the columns, and intermediate pages say RUNNING.
    assert!(d
        .documents
        .iter()
        .filter(|doc| doc.get("data").is_some())
        .all(|doc| doc.get("columns").is_some()));
    assert_eq!(d.documents.last().unwrap()["stats"]["state"], "FINISHED");
}

#[tokio::test]
async fn refetching_a_token_is_idempotent_and_stale_tokens_are_gone() {
    let base = start_default().await;
    let client = reqwest::Client::new();
    let doc = post(&base, "SELECT n FROM numbers ORDER BY n", &[]).await;
    let first_uri = doc["nextUri"].as_str().unwrap().to_string();
    let mut page: Value = client
        .get(&first_uri)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    while page.get("data").is_none() {
        page = client
            .get(&first_uri)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
    }
    let again: Value = client
        .get(&first_uri)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["data"], again["data"], "same token → same page");
    let second_uri = page["nextUri"].as_str().unwrap().to_string();
    let _: Value = client
        .get(&second_uri)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stale = client.get(&first_uri).send().await.unwrap();
    assert_eq!(stale.status(), 410);
}

#[tokio::test]
async fn errors_are_reported_as_trino_query_errors() {
    let base = start_default().await;
    let d = run(&base, "SELECT * FROM no_such_table", &[]).await;
    let err = d.error.expect("error");
    assert_eq!(err["errorName"], "TABLE_NOT_FOUND");
    assert_eq!(err["errorCode"], 46);
    assert_eq!(err["errorType"], "USER_ERROR");
    assert!(err["message"].as_str().unwrap().contains("no_such_table"));
    assert_eq!(err["failureInfo"]["type"], "io.trino.spi.TrinoException");
    assert_eq!(d.documents.last().unwrap()["stats"]["state"], "FAILED");

    let d = run(&base, "SELEC 1", &[]).await;
    assert_eq!(d.error.unwrap()["errorName"], "SYNTAX_ERROR");
}

#[tokio::test]
async fn empty_statement_is_rejected() {
    let base = start_default().await;
    let resp = reqwest::Client::new()
        .post(format!("{base}/v1/statement"))
        .body("  ")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn session_catalog_and_schema_headers_resolve_unqualified_tables() {
    let base = start_default().await;
    let d = run(
        &base,
        "SELECT order_id FROM orders WHERE status = 'open' ORDER BY order_id",
        &[("X-Trino-Catalog", "lake"), ("X-Trino-Schema", "sales")],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(first_column(&d), vec![json!(10), json!(30)]);

    // Without the headers the server default (memory.default) applies.
    let d = run(&base, "SELECT order_id FROM orders", &[]).await;
    assert_eq!(d.error.unwrap()["errorName"], "TABLE_NOT_FOUND");
}

#[tokio::test]
async fn show_statements_browse_the_catalog() {
    let base = start_default().await;
    let d = run(&base, "SHOW CATALOGS", &[]).await;
    assert_eq!(column_names(&d), vec!["Catalog"]);
    assert_eq!(
        first_column(&d),
        vec![json!("lake"), json!("memory"), json!("system")]
    );

    let d = run(&base, "SHOW SCHEMAS FROM lake", &[]).await;
    assert_eq!(
        first_column(&d),
        vec![json!("information_schema"), json!("sales")]
    );

    let d = run(&base, "SHOW TABLES FROM lake.sales", &[]).await;
    assert_eq!(first_column(&d), vec![json!("orders")]);

    let d = run(&base, "SHOW TABLES LIKE 'n%'", &[]).await;
    assert_eq!(first_column(&d), vec![json!("numbers")]);

    let d = run(&base, "DESCRIBE typed", &[]).await;
    assert_eq!(column_names(&d), vec!["Column", "Type", "Extra", "Comment"]);
    assert_eq!(d.rows[3], json!(["price", "decimal(12,2)", "", ""]));

    let d = run(&base, "SHOW COLUMNS FROM lake.sales.orders", &[]).await;
    assert_eq!(d.rows.len(), 2);

    let d = run(&base, "SHOW SCHEMAS FROM nope", &[]).await;
    assert_eq!(d.error.unwrap()["errorName"], "CATALOG_NOT_FOUND");
}

#[tokio::test]
async fn information_schema_queries_run_through_the_engine() {
    let base = start_default().await;
    let d = run(
        &base,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'sales' AND table_type = 'BASE TABLE'",
        &[("X-Trino-Catalog", "lake")],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(first_column(&d), vec![json!("orders")]);

    // SQLAlchemy / Superset get_columns shape.
    let d = run(
        &base,
        "SELECT column_name, data_type, column_default, UPPER(is_nullable) AS is_nullable \
         FROM information_schema.columns \
         WHERE table_schema = 'default' AND table_name = 'typed' \
         ORDER BY ordinal_position ASC",
        &[("X-Trino-Catalog", "memory")],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(d.rows.len(), 8);
    assert_eq!(d.rows[0], json!(["id", "bigint", null, "NO"]));
    assert_eq!(d.rows[3], json!(["price", "decimal(12,2)", null, "YES"]));

    let d = run(
        &base,
        "SELECT schema_name FROM lake.information_schema.schemata ORDER BY schema_name",
        &[],
    )
    .await;
    assert_eq!(
        first_column(&d),
        vec![json!("information_schema"), json!("sales")]
    );
}

#[tokio::test]
async fn jdbc_metadata_tables_answer_driver_queries() {
    let base = start_default().await;
    // Shape of TrinoDatabaseMetaData.getTables().
    let d = run(
        &base,
        "SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS \
         FROM system.jdbc.tables \
         WHERE TABLE_CAT = 'lake' AND TABLE_SCHEM LIKE 'sal%' \
         ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME",
        &[],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(
        d.rows,
        vec![json!(["lake", "sales", "orders", "TABLE", null])]
    );

    let d = run(
        &base,
        "SELECT column_name, data_type, type_name, ordinal_position FROM system.jdbc.columns \
         WHERE table_cat = 'memory' AND table_name = 'typed' AND column_name = 'day'",
        &[],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(d.rows, vec![json!(["day", 91, "date", 5])]);

    let d = run(&base, "SELECT table_cat FROM system.jdbc.catalogs", &[]).await;
    assert_eq!(d.rows.len(), 3);
}

#[tokio::test]
async fn session_control_statements_return_client_headers() {
    let base = start_default().await;

    let d = run(&base, "USE lake.sales", &[]).await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(d.update_type.as_deref(), Some("USE"));
    assert!(d
        .headers
        .contains(&("x-trino-set-catalog".into(), "lake".into())));
    assert!(d
        .headers
        .contains(&("x-trino-set-schema".into(), "sales".into())));

    let d = run(&base, "USE lake.nope", &[]).await;
    assert_eq!(d.error.unwrap()["errorName"], "SCHEMA_NOT_FOUND");

    let d = run(&base, "SET SESSION query_max_run_time = '10m'", &[]).await;
    assert_eq!(d.update_type.as_deref(), Some("SET SESSION"));
    assert!(d.headers.contains(&(
        "x-trino-set-session".into(),
        "query_max_run_time=10m".into()
    )));

    let d = run(&base, "RESET SESSION query_max_run_time", &[]).await;
    assert!(d
        .headers
        .contains(&("x-trino-clear-session".into(), "query_max_run_time".into())));

    let d = run(
        &base,
        "SHOW SESSION",
        &[("X-Trino-Session", "query_max_run_time=10m")],
    )
    .await;
    assert_eq!(d.rows[0][0], "query_max_run_time");
    assert_eq!(d.rows[0][1], "10m");

    // Legacy Presto clients get X-Presto-* back.
    let client = reqwest::Client::new();
    let doc: Value = client
        .post(format!("{base}/v1/statement"))
        .header("X-Presto-User", "legacy")
        .body("USE lake.sales")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut next = doc["nextUri"].as_str().map(str::to_string);
    let mut presto_headers = Vec::new();
    while let Some(uri) = next {
        let resp = client.get(&uri).send().await.unwrap();
        collect_headers(resp.headers(), &mut presto_headers);
        let doc: Value = resp.json().await.unwrap();
        next = doc["nextUri"].as_str().map(str::to_string);
    }
    assert!(presto_headers.contains(&("x-presto-set-catalog".into(), "lake".into())));
}

#[tokio::test]
async fn prepared_statements_and_execute_immediate() {
    let base = start_default().await;

    let d = run(
        &base,
        "PREPARE q1 FROM SELECT n FROM numbers WHERE n > ? AND n < ? ORDER BY n",
        &[],
    )
    .await;
    assert_eq!(d.update_type.as_deref(), Some("PREPARE"));
    let added = d
        .headers
        .iter()
        .find(|(k, _)| k == "x-trino-added-prepare")
        .map(|(_, v)| v.clone())
        .expect("added-prepare header");
    assert!(added.starts_with("q1="));

    // Client echoes the prepared statement back on EXECUTE.
    let d = run(
        &base,
        "EXECUTE q1 USING 20, 23",
        &[("X-Trino-Prepared-Statement", added.as_str())],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(first_column(&d), vec![json!(21), json!(22)]);

    let d = run(&base, "EXECUTE missing USING 1", &[]).await;
    assert_eq!(d.error.unwrap()["errorName"], "NOT_FOUND");

    let d = run(
        &base,
        "EXECUTE IMMEDIATE 'SELECT n FROM numbers WHERE n = ?' USING 7",
        &[],
    )
    .await;
    assert!(d.error.is_none(), "{:?}", d.error);
    assert_eq!(first_column(&d), vec![json!(7)]);

    let d = run(&base, "DEALLOCATE PREPARE q1", &[]).await;
    assert!(d
        .headers
        .contains(&("x-trino-deallocated-prepare".into(), "q1".into())));
}

// ---------------------------------------------------------------------------
// Cancellation: a distributed executor that never finishes on its own.
// ---------------------------------------------------------------------------

struct HangingExecutor {
    dropped: Arc<AtomicBool>,
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl DistributedExecutor for HangingExecutor {
    async fn execute(
        &self,
        _plan: LogicalPlan,
        _exec_ctx: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, ArnebError> {
        let _guard = DropFlag(Arc::clone(&self.dropped));
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(Vec::new())
    }

    fn has_workers(&self) -> bool {
        true
    }
}

async fn start_hanging(config: TrinoConfig) -> (String, Arc<AtomicBool>, Arc<QueryTracker>) {
    let (cm, registry) = engine_state();
    let dropped = Arc::new(AtomicBool::new(false));
    let tracker = Arc::new(QueryTracker::new());
    let server = TrinoServer::new(config, cm, registry)
        .with_distributed_executor(Arc::new(HangingExecutor {
            dropped: Arc::clone(&dropped),
        }))
        .with_query_tracker(Arc::clone(&tracker));
    (start(server).await, dropped, tracker)
}

#[tokio::test]
async fn delete_cancels_a_running_query() {
    let (base, dropped, tracker) = start_hanging(test_config()).await;
    let client = reqwest::Client::new();
    let doc = post(&base, "SELECT n FROM numbers", &[]).await;
    let next = doc["nextUri"].as_str().unwrap().to_string();

    // Long-poll returns RUNNING with the same token while it executes.
    let polled: Value = client
        .get(&next)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(polled["stats"]["state"], "RUNNING");
    assert_eq!(polled["nextUri"], json!(next));
    let tracked = tracker.list_queries(None);
    assert_eq!(tracked.len(), 1);
    assert_eq!(tracked[0].state, QueryState::Running);

    let resp = client.delete(&next).send().await.unwrap();
    assert_eq!(resp.status(), 204);
    for _ in 0..50 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dropped.load(Ordering::SeqCst), "execution future dropped");
    assert_eq!(tracker.list_queries(None)[0].state, QueryState::Cancelled);
    let gone = client.get(&next).send().await.unwrap();
    assert_eq!(gone.status(), 404);
}

#[tokio::test]
async fn abandoned_queries_are_canceled() {
    let config = TrinoConfig {
        client_timeout: Duration::from_millis(200),
        ..test_config()
    };
    let (base, dropped, _tracker) = start_hanging(config).await;
    let doc = post(&base, "SELECT n FROM numbers", &[]).await;
    let next = doc["nextUri"].as_str().unwrap().to_string();
    for _ in 0..100 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dropped.load(Ordering::SeqCst), "abandoned query canceled");
    let gone = reqwest::get(&next).await.unwrap();
    assert_eq!(gone.status(), 404);
}

#[tokio::test]
async fn delete_between_pages_releases_the_query() {
    let base = start_default().await;
    let client = reqwest::Client::new();
    let doc = post(&base, "SELECT n FROM numbers ORDER BY n", &[]).await;
    let mut next = doc["nextUri"].as_str().unwrap().to_string();
    let page = loop {
        let page: Value = client
            .get(&next)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if page.get("data").is_some() {
            break page;
        }
        next = page["nextUri"].as_str().unwrap().to_string();
    };
    let second = page["nextUri"].as_str().unwrap().to_string();
    assert_eq!(client.delete(&second).send().await.unwrap().status(), 204);
    assert_eq!(client.get(&second).send().await.unwrap().status(), 404);
    let id = page["id"].as_str().unwrap();
    let info = client
        .get(format!("{base}/v1/query/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(info.status(), 404);
}
