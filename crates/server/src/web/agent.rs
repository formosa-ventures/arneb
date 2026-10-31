//! The agent query API under `/api/v2`: discovery, explain, and asynchronous
//! statements. A thin HTTP layer over [`AgentQueryService`]: it extracts the
//! caller, validates `wait_ms` against the configured ceiling (refusing, not
//! clamping), and renders [`AgentError`](arneb_protocol::agent::AgentError)s
//! as the shared error envelope. A statement that fails after submission is
//! a `200` status body with `state: "failed"`, not an HTTP error.

use std::time::Duration;

use arneb_protocol::agent::{
    AgentQueryService, ExplainRequest, Explanation, ListRequest, Listing, ResultPage,
    StatementStatus, SubmitRequest, TableDescription,
};
use axum::body::Bytes;
use axum::extract::rejection::{BytesRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::{Extension, Router};
use serde::Deserialize;

use super::auth::Identity;
use super::error::{ApiError, ErrorCode};
use super::v2::{bad_body, bad_query};
use super::WebState;

/// The agent routes, merged into the `/api/v2` router.
pub fn routes() -> Router<WebState> {
    Router::new()
        .route("/catalogs", get(list_catalogs))
        .route("/catalogs/{catalog}/schemas", get(list_schemas))
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables",
            get(list_tables),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}",
            get(describe_table),
        )
        .route(
            "/catalogs/{catalog}/schemas/{schema}/tables/{table}/sample",
            get(sample_table),
        )
        .route("/explain", post(explain))
        .route("/statements", post(submit))
        .route("/statements/{id}", get(status).delete(cancel))
        .route("/statements/{id}/results", get(results))
}

/// The service, or `not_found` where it is not served (workers).
pub(super) fn service(state: &WebState) -> Result<&AgentQueryService, ApiError> {
    state
        .agent
        .as_ref()
        .ok_or_else(|| ApiError::new(ErrorCode::NotFound, "no such endpoint"))
}

fn json_body<T: serde::de::DeserializeOwned>(
    body: Result<Bytes, BytesRejection>,
) -> Result<T, ApiError> {
    let body = body.map_err(bad_body)?;
    serde_json::from_slice(&body).map_err(|e| {
        ApiError::new(
            ErrorCode::InvalidArgument,
            format!("invalid request body: {e}"),
        )
        .with_hint("send a JSON object with a \"sql\" string")
    })
}

fn query<T>(params: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    params.map(|Query(p)| p).map_err(bad_query)
}

/// `wait_ms` (default 0), refused above the configured ceiling.
pub(super) fn wait(
    service: &AgentQueryService,
    wait_ms: Option<u64>,
) -> Result<Duration, ApiError> {
    let max = service.limits().max_wait;
    let wait = Duration::from_millis(wait_ms.unwrap_or(0));
    if wait > max {
        return Err(ApiError::new(
            ErrorCode::InvalidArgument,
            format!("wait_ms {} is above the maximum", wait.as_millis()),
        )
        .with_hint(format!("wait_ms must be at most {}", max.as_millis())));
    }
    Ok(wait)
}

#[derive(Deserialize, Default)]
struct WaitParams {
    wait_ms: Option<u64>,
}

#[derive(Deserialize, Default)]
struct ResultsParams {
    #[serde(default)]
    cursor: u64,
    max_bytes: Option<usize>,
}

#[derive(Deserialize, Default)]
struct DescribeParams {
    limit: Option<usize>,
}

#[derive(Deserialize, Default)]
struct SampleParams {
    rows: Option<usize>,
}

async fn list_catalogs(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<ListRequest>, QueryRejection>,
) -> Result<Json<Listing>, ApiError> {
    let req = query(params)?;
    Ok(Json(service(&state)?.list_catalogs(&who.user, &req).await?))
}

async fn list_schemas(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(catalog): Path<String>,
    params: Result<Query<ListRequest>, QueryRejection>,
) -> Result<Json<Listing>, ApiError> {
    let req = query(params)?;
    Ok(Json(
        service(&state)?
            .list_schemas(&who.user, &catalog, &req)
            .await?,
    ))
}

async fn list_tables(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path((catalog, schema)): Path<(String, String)>,
    params: Result<Query<ListRequest>, QueryRejection>,
) -> Result<Json<Listing>, ApiError> {
    let req = query(params)?;
    Ok(Json(
        service(&state)?
            .list_tables(&who.user, &catalog, &schema, &req)
            .await?,
    ))
}

async fn describe_table(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
    params: Result<Query<DescribeParams>, QueryRejection>,
) -> Result<Json<TableDescription>, ApiError> {
    let DescribeParams { limit } = query(params)?;
    Ok(Json(
        service(&state)?
            .describe_table(&who.user, &catalog, &schema, &table, limit)
            .await?,
    ))
}

async fn sample_table(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path((catalog, schema, table)): Path<(String, String, String)>,
    params: Result<Query<SampleParams>, QueryRejection>,
) -> Result<Json<StatementStatus>, ApiError> {
    let SampleParams { rows } = query(params)?;
    Ok(Json(
        service(&state)?
            .sample(&who.user, &catalog, &schema, &table, rows, None)
            .await?,
    ))
}

async fn explain(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<Explanation>, ApiError> {
    let service = service(&state)?;
    let req: ExplainRequest = json_body(body)?;
    Ok(Json(service.explain(&who.user, &req, None).await?))
}

async fn submit(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    params: Result<Query<WaitParams>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let service = service(&state)?;
    let wait = wait(service, query(params)?.wait_ms)?;
    let req: SubmitRequest = json_body(body)?;
    let status = service.submit(&who.user, req, wait, None).await?;
    let location = format!("/api/v2/statements/{}", status.id);
    let mut resp = (StatusCode::CREATED, Json(status)).into_response();
    if let Ok(value) = HeaderValue::from_str(&location) {
        resp.headers_mut().insert(header::LOCATION, value);
    }
    Ok(resp)
}

async fn status(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
    params: Result<Query<WaitParams>, QueryRejection>,
) -> Result<Json<StatementStatus>, ApiError> {
    let service = service(&state)?;
    let wait = wait(service, query(params)?.wait_ms)?;
    Ok(Json(service.status(&who.user, &id, wait, None).await?))
}

async fn results(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
    params: Result<Query<ResultsParams>, QueryRejection>,
) -> Result<Json<ResultPage>, ApiError> {
    let ResultsParams { cursor, max_bytes } = query(params)?;
    Ok(Json(
        service(&state)?.results(&who.user, &id, cursor, max_bytes)?,
    ))
}

async fn cancel(
    State(state): State<WebState>,
    Extension(who): Extension<Identity>,
    Path(id): Path<String>,
) -> Result<Json<StatementStatus>, ApiError> {
    Ok(Json(service(&state)?.cancel(&who.user, &id, None)?))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arneb_catalog::CatalogManager;
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::{
        MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
    };
    use arneb_connectors::ConnectorRegistry;
    use arneb_protocol::agent::{AgentLimits, AgentQueryService};
    use arneb_protocol::{CredentialSource, ScramVerifier};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde_json::{json, Value};
    use tower::ServiceExt as _;

    use crate::web::{build_router, SessionStore, WebState};

    /// `memory.default.lineitem(l_orderkey, l_quantity)`, two rows.
    fn engine_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", ArrowDataType::Int64, false),
            Field::new("l_quantity", ArrowDataType::Int64, false),
        ]));
        let infos = schema
            .fields()
            .iter()
            .map(|f| ColumnInfo {
                name: f.name().clone(),
                data_type: DataType::try_from(f.data_type().clone()).unwrap(),
                nullable: f.is_nullable(),
            })
            .collect();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![17, 36])),
        ];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let default_schema = Arc::new(MemorySchema::new());
        default_schema.register_table("lineitem", Arc::new(MemoryTable::new(infos, vec![batch])));
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", default_schema);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", memory.clone());
        let mut registry = ConnectorRegistry::new();
        registry.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(memory, "default")),
        );
        (Arc::new(cm), Arc::new(registry))
    }

    fn web_state(password: bool, with_agent: bool) -> WebState {
        let store = Arc::new(arneb_state::StateStore::open(None).unwrap());
        let v = ScramVerifier::from_password("secret", b"0123456789abcdef", 4096);
        store.seed_users(&[("alice".into(), v)], false).unwrap();
        let credentials: Option<Arc<dyn CredentialSource>> =
            password.then(|| store.clone() as Arc<dyn CredentialSource>);
        let tracker = Arc::new(arneb_scheduler::QueryTracker::new());
        let (cm, registry) = engine_state();
        let agent = with_agent
            .then(|| AgentQueryService::new(cm, registry, tracker.clone(), AgentLimits::default()));
        WebState {
            query_tracker: tracker,
            node_registry: arneb_scheduler::NodeRegistry::new(30),
            start_time: std::time::Instant::now(),
            role: "standalone".into(),
            admission: None,
            max_queued_queries: 8,
            memory_pool: Arc::new(arneb_common::memory_pool::GreedyMemoryPool::new(1 << 20)),
            memory_limit_bytes: None,
            basic: credentials
                .clone()
                .map(|c| arneb_protocol::BasicAuthenticator::new(c).unwrap()),
            credentials,
            state: password.then_some(store),
            access_rules: None,
            authorizer: crate::web::privilege::Authorizer::new(None, Vec::new(), password),
            metrics_auth: false,
            sessions: SessionStore::new(),
            agent,
            mcp: None,
        }
    }

    async fn call(
        state: &WebState,
        method: Method,
        path: &str,
        headers: &[(&str, String)],
        body: Option<Value>,
    ) -> (StatusCode, axum::http::HeaderMap, Value) {
        let mut req = axum::http::Request::builder().method(method).uri(path);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        let body = match body {
            Some(b) => {
                req = req.header(header::CONTENT_TYPE, "application/json");
                Body::from(b.to_string())
            }
            None => Body::empty(),
        };
        let resp = build_router(state.clone())
            .oneshot(req.body(body).unwrap())
            .await
            .unwrap();
        let (status, headers) = (resp.status(), resp.headers().clone());
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, headers, json)
    }

    fn user(name: &str) -> [(&'static str, String); 1] {
        [("x-arneb-user", name.to_string())]
    }

    #[tokio::test]
    async fn statements_require_authentication() {
        let state = web_state(true, true);
        for (method, path) in [
            (Method::POST, "/api/v2/statements"),
            (Method::GET, "/api/v2/catalogs"),
            (Method::POST, "/api/v2/explain"),
        ] {
            let (status, headers, body) =
                call(&state, method, path, &[], Some(json!({"sql": "SELECT 1"}))).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
            assert_eq!(body["error"]["code"], "unauthenticated");
            assert!(headers.contains_key(header::WWW_AUTHENTICATE));
        }
        let basic = format!("Basic {}", STANDARD.encode("alice:secret"));
        let (status, ..) = call(
            &state,
            Method::GET,
            "/api/v2/catalogs",
            &[("authorization", basic)],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn submit_poll_fetch_cancel_over_http() {
        let state = web_state(false, true);
        let me = user("alice");
        let (status, headers, body) = call(
            &state,
            Method::POST,
            "/api/v2/statements",
            &me,
            Some(json!({"sql": "SELECT 1"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let id = body["id"].as_str().unwrap().to_string();
        assert_eq!(
            headers[header::LOCATION],
            format!("/api/v2/statements/{id}").as_str()
        );

        let (status, _, body) = call(
            &state,
            Method::GET,
            &format!("/api/v2/statements/{id}?wait_ms=2000"),
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], "finished");

        let (status, _, page) = call(
            &state,
            Method::GET,
            &format!("/api/v2/statements/{id}/results"),
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page["rows"], json!([[1]]));

        // Cancel is idempotent and keeps the terminal state.
        let (status, _, body) = call(
            &state,
            Method::DELETE,
            &format!("/api/v2/statements/{id}"),
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], "finished");

        // Another user sees nothing.
        let (status, _, body) = call(
            &state,
            Method::GET,
            &format!("/api/v2/statements/{id}"),
            &user("bob"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn unknown_column_envelope_has_location_candidates_hint() {
        let state = web_state(false, true);
        let me = user("alice");
        let (status, _, body) = call(
            &state,
            Method::POST,
            "/api/v2/statements?wait_ms=2000",
            &me,
            Some(json!({"sql": "SELECT l_quantit FROM lineitem"})),
        )
        .await;
        // The request was fine; the statement failed.
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["state"], "failed");
        let err = &body["error"];
        assert_eq!(err["code"], "column_not_found");
        assert_eq!(err["location"], json!({"line": 1, "column": 8}));
        assert_eq!(err["candidates"][0], "l_quantity");
        assert!(err["hint"].as_str().unwrap().contains("l_quantity"));
    }

    #[tokio::test]
    async fn explain_error_is_http_400_with_envelope() {
        let state = web_state(false, true);
        let me = user("alice");
        let (status, _, body) = call(
            &state,
            Method::POST,
            "/api/v2/explain",
            &me,
            Some(json!({"sql": "SELECT l_quantit FROM lineitem"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "column_not_found");
        assert_eq!(body["error"]["candidates"][0], "l_quantity");

        let (status, _, body) = call(
            &state,
            Method::POST,
            "/api/v2/explain",
            &me,
            Some(json!({"sql": "SELECT l_quantity FROM lineitem"})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["plan"].is_string());
    }

    #[tokio::test]
    async fn wait_above_ceiling_is_invalid_argument() {
        let state = web_state(false, true);
        let me = user("alice");
        let (status, _, body) = call(
            &state,
            Method::POST,
            "/api/v2/statements?wait_ms=10001",
            &me,
            Some(json!({"sql": "SELECT 1"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "invalid_argument");
        assert!(body["error"]["hint"].as_str().unwrap().contains("10000"));

        // The ceiling itself is fine; so is a poll at it.
        let (status, ..) = call(
            &state,
            Method::POST,
            "/api/v2/statements?wait_ms=10000",
            &me,
            Some(json!({"sql": "SELECT 1"})),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, ..) = call(
            &state,
            Method::GET,
            "/api/v2/statements/nope?wait_ms=10001",
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_input_is_an_invalid_argument_envelope() {
        let state = web_state(false, true);
        let me = user("alice");
        let mut req = axum::http::Request::builder()
            .method(Method::POST)
            .uri("/api/v2/statements")
            .header("x-arneb-user", "alice")
            .body(Body::from("{not json"))
            .unwrap();
        req.headers_mut()
            .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let resp = build_router(state.clone()).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["code"], "invalid_argument");

        for path in [
            "/api/v2/statements/x?wait_ms=abc",
            "/api/v2/statements/x/results?cursor=-1",
            "/api/v2/catalogs?limit=many",
        ] {
            let (status, _, body) = call(&state, Method::GET, path, &me, None).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(body["error"]["code"], "invalid_argument", "{path}");
        }
    }

    #[tokio::test]
    async fn discovery_routes_under_auth_none_use_claimed_user() {
        let state = web_state(false, true);
        let me = user("alice");
        let (status, _, body) = call(&state, Method::GET, "/api/v2/catalogs", &me, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["items"][0]["name"], "memory");

        let (_, _, body) = call(
            &state,
            Method::GET,
            "/api/v2/catalogs/memory/schemas/default/tables?prefix=line",
            &me,
            None,
        )
        .await;
        assert_eq!(body["items"][0]["name"], "lineitem");

        let (status, _, body) = call(
            &state,
            Method::GET,
            "/api/v2/catalogs/memory/schemas/default/tables/lineitem",
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["columns"][1]["name"], "l_quantity");

        let (status, _, body) = call(
            &state,
            Method::GET,
            "/api/v2/catalogs/memory/schemas/default/tables/lineitem/sample?rows=1",
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], "finished");

        let (status, _, body) = call(
            &state,
            Method::GET,
            "/api/v2/catalogs/nope/schemas",
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "catalog_not_found");
    }

    #[tokio::test]
    async fn worker_role_has_no_agent_routes() {
        let state = web_state(false, false);
        let me = user("alice");
        for (method, path) in [
            (Method::GET, "/api/v2/catalogs"),
            (Method::POST, "/api/v2/statements"),
            (Method::GET, "/api/v2/statements/x"),
            (Method::POST, "/api/v2/explain"),
        ] {
            let (status, _, body) =
                call(&state, method, path, &me, Some(json!({"sql": "SELECT 1"}))).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            assert_eq!(body["error"]["code"], "not_found", "{path}");
        }
    }

    #[tokio::test]
    async fn statement_shows_in_queries_with_source_api() {
        let state = web_state(false, true);
        let me = user("alice");
        let (_, _, body) = call(
            &state,
            Method::POST,
            "/api/v2/statements?wait_ms=2000",
            &me,
            Some(json!({"sql": "SELECT 1"})),
        )
        .await;
        let id = body["id"].as_str().unwrap();
        let (status, _, q) = call(
            &state,
            Method::GET,
            &format!("/api/v2/queries/{id}"),
            &me,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(q["source"], "api");
        assert_eq!(q["user"], "alice");
    }
}
