//! Catalog sync between the coordinator and its workers (runtime-config D8).
//!
//! The coordinator pushes its live catalog set to a worker with the
//! `sync_catalogs` action ([`push_catalogs`]); a worker pulls it with the
//! `get_catalogs` action ([`fetch_catalogs`]) when a heartbeat reply names
//! another generation than the one it applied. Both run under the cluster
//! security of every Flight action.
//!
//! A set's specs carry raw catalog secrets, so a [`CatalogSet`] is never
//! logged: its `Debug` shows the generation and the catalog count only.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arrow_flight::Action;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::security::ClusterSecurity;
use crate::task_submission::connect;

/// A catalog generation and its specs, as JSON objects (the server crate's
/// `CatalogConfig`, raw secrets included). Encoded as JSON.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogSet {
    /// The epoch of the coordinator the set comes from, chosen at random at
    /// each start (Ruling R6). A set from another epoch than the one a
    /// worker holds replaces it even with a lower generation.
    #[serde(default)]
    pub epoch: u64,
    /// The catalog generation the set belongs to.
    pub generation: u64,
    /// One spec per catalog, each with the coordinator's global
    /// `[storage]` already merged in (Ruling R12), so a worker builds it
    /// without any storage config of its own.
    pub catalogs: Vec<serde_json::Value>,
    /// The coordinator's default catalog, which a worker resolves a scan
    /// with no catalog in (Ruling R12). `None` from an older coordinator:
    /// the worker keeps its own default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_catalog: Option<String>,
    /// The coordinator's default schema (Ruling R12); `None` as above.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_schema: Option<String>,
}

impl std::fmt::Debug for CatalogSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogSet")
            .field("epoch", &self.epoch)
            .field("generation", &self.generation)
            .field("catalogs", &self.catalogs.len())
            .field("default_catalog", &self.default_catalog)
            .field("default_schema", &self.default_schema)
            .finish()
    }
}

impl CatalogSet {
    /// Serialize to JSON.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("CatalogSet serialization should not fail")
    }

    /// Deserialize from JSON. The error never quotes the body.
    pub fn decode(data: &[u8]) -> Result<Self, ExecutionError> {
        serde_json::from_slice(data).map_err(|e| {
            ExecutionError::InvalidOperation(format!(
                "invalid catalog set ({:?} error at line {}, column {})",
                e.classify(),
                e.line(),
                e.column()
            ))
        })
    }
}

/// Worker-side handler for `sync_catalogs`: applies a pushed set and
/// returns the names of the catalogs that failed to build (Ruling R7). An
/// `Err` is returned to the coordinator as `Internal`.
pub type CatalogSyncCallback =
    Arc<dyn Fn(CatalogSet) -> BoxFuture<'static, Result<Vec<String>, String>> + Send + Sync>;

/// The worker's answer to `sync_catalogs`, encoded as JSON: the names of
/// the catalogs that failed to build (never their specs).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogSyncReply {
    #[serde(default)]
    pub(crate) failed: Vec<String>,
}

/// Coordinator-side source for `get_catalogs`: the live catalog set.
pub type CatalogSourceCallback = Arc<dyn Fn() -> CatalogSet + Send + Sync>;

/// Coordinator → worker: push `set` to the worker at `addr` and wait until
/// it applied it. `Ok` lists the catalogs that failed to build on the
/// worker (the set still applied; Ruling R7).
pub async fn push_catalogs(
    addr: &str,
    set: &CatalogSet,
    security: &ClusterSecurity,
) -> Result<Vec<String>, String> {
    use futures::StreamExt;
    let mut client = connect(addr, security).await?;
    let action = Action {
        r#type: "sync_catalogs".to_string(),
        body: set.encode().into(),
    };
    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("sync_catalogs RPC failed: {}", e.message()))?
        .into_inner();
    match stream.next().await {
        Some(Ok(result)) => Ok(serde_json::from_slice::<CatalogSyncReply>(&result.body)
            .unwrap_or_default()
            .failed),
        Some(Err(e)) => Err(format!("sync_catalogs response error: {}", e.message())),
        None => Ok(Vec::new()),
    }
}

/// How long [`fetch_catalogs`] waits for the coordinator, connect included.
pub const FETCH_CATALOGS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Worker → coordinator: fetch the coordinator's live catalog set. Gives up
/// after [`FETCH_CATALOGS_TIMEOUT`], so a black-holed coordinator never
/// leaves a pull pending.
pub async fn fetch_catalogs(
    coordinator: &str,
    security: &ClusterSecurity,
) -> Result<CatalogSet, String> {
    tokio::time::timeout(
        FETCH_CATALOGS_TIMEOUT,
        fetch_catalogs_inner(coordinator, security),
    )
    .await
    .unwrap_or_else(|_| {
        Err(format!(
            "get_catalogs from {coordinator} timed out after {} s",
            FETCH_CATALOGS_TIMEOUT.as_secs()
        ))
    })
}

async fn fetch_catalogs_inner(
    coordinator: &str,
    security: &ClusterSecurity,
) -> Result<CatalogSet, String> {
    use futures::StreamExt;
    let mut client = connect(coordinator, security).await?;
    let action = Action {
        r#type: "get_catalogs".to_string(),
        body: Default::default(),
    };
    let mut stream = client
        .do_action(action)
        .await
        .map_err(|e| format!("get_catalogs RPC failed: {}", e.message()))?
        .into_inner();
    let result = stream
        .next()
        .await
        .ok_or_else(|| "get_catalogs returned no body".to_string())?
        .map_err(|e| format!("get_catalogs response error: {}", e.message()))?;
    CatalogSet::decode(&result.body).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use arrow_flight::flight_service_server::FlightService;
    use arrow_flight::Action;

    use crate::flight_service::TrinoFlightService;
    use crate::{ClusterSecurity, FlightState};

    fn sample_set() -> CatalogSet {
        CatalogSet {
            epoch: 42,
            generation: 7,
            catalogs: vec![
                serde_json::json!({"name": "lake", "type": "hive", "metastore_uri": "h:9083"}),
                serde_json::json!({"name": "pg", "type": "postgresql",
                                   "connection_url": "postgresql://u@db/x"}),
            ],
            default_catalog: Some("lake".into()),
            default_schema: Some("tpch".into()),
        }
    }

    async fn serve(state: FlightState) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            crate::serve_flight(listener, state, &ClusterSecurity::default())
                .await
                .unwrap();
        });
        format!("http://{addr}")
    }

    #[test]
    fn catalog_set_epoch_defaults_to_zero() {
        let set = CatalogSet::decode(br#"{"generation":3,"catalogs":[]}"#).unwrap();
        assert_eq!((set.epoch, set.generation), (0, 3));
        // Ruling R12: a set from an older coordinator carries no defaults.
        assert_eq!((set.default_catalog, set.default_schema), (None, None));
    }

    #[test]
    fn catalog_set_round_trips() {
        let set = sample_set();
        let decoded = CatalogSet::decode(&set.encode()).unwrap();
        assert_eq!(decoded, set);
        assert!(CatalogSet::decode(b"ok").is_err());
        // Debug never prints the specs (they carry raw secrets).
        let debug = format!("{set:?}");
        assert!(!debug.contains("postgresql://"), "{debug}");
        assert!(debug.contains('7'), "{debug}");
    }

    #[tokio::test]
    async fn sync_catalogs_without_callback_is_unavailable() {
        let service = TrinoFlightService::new(FlightState::new());
        let action = Action {
            r#type: "sync_catalogs".into(),
            body: sample_set().encode().into(),
        };
        let status = match service.do_action(tonic::Request::new(action)).await {
            Err(s) => s,
            Ok(_) => panic!("expected sync_catalogs to fail"),
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let action = Action {
            r#type: "get_catalogs".into(),
            body: Default::default(),
        };
        let status = match service.do_action(tonic::Request::new(action)).await {
            Err(s) => s,
            Ok(_) => panic!("expected get_catalogs to fail"),
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn get_catalogs_returns_the_source() {
        let mut state = FlightState::new();
        state.set_catalog_source(Arc::new(sample_set));
        let addr = serve(state).await;
        let fetched = fetch_catalogs(&addr, &ClusterSecurity::default())
            .await
            .unwrap();
        assert_eq!(fetched, sample_set());
    }

    #[tokio::test]
    async fn heartbeat_reply_carries_the_catalog_generation() {
        let state = FlightState::with_heartbeat_callback(Arc::new(|_| {
            Ok(crate::HeartbeatReply {
                catalog_generation: Some(3),
                catalog_epoch: Some(42),
            })
        }));
        let addr = serve(state).await;
        let msg = crate::HeartbeatMessage::new("w1".into(), "http://localhost:1".into(), 8);
        let reply = crate::send_heartbeat_with(&addr, &msg, &ClusterSecurity::default())
            .await
            .unwrap();
        assert_eq!(reply.catalog_generation, Some(3));
        assert_eq!(reply.catalog_epoch, Some(42));
    }

    #[tokio::test]
    async fn push_catalogs_reaches_the_worker_callback() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let s = seen.clone();
        let mut state = FlightState::new();
        state.set_catalog_sync_callback(Arc::new(move |set: CatalogSet| {
            s.lock().unwrap().push(set);
            Box::pin(async { Ok(vec![]) })
        }));
        let addr = serve(state).await;
        let failed = push_catalogs(&addr, &sample_set(), &ClusterSecurity::default())
            .await
            .unwrap();
        assert!(failed.is_empty());
        assert_eq!(*seen.lock().unwrap(), vec![sample_set()]);

        // Ruling R7: an applied set with build failures is Ok, listing them.
        let mut state = FlightState::new();
        state.set_catalog_sync_callback(Arc::new(|_| Box::pin(async { Ok(vec!["pg".into()]) })));
        let addr = serve(state).await;
        let failed = push_catalogs(&addr, &sample_set(), &ClusterSecurity::default())
            .await
            .unwrap();
        assert_eq!(failed, vec!["pg".to_string()]);

        // A failed apply reaches the coordinator as an error.
        let mut state = FlightState::new();
        state.set_catalog_sync_callback(Arc::new(|_| Box::pin(async { Err("boom".into()) })));
        let addr = serve(state).await;
        let err = push_catalogs(&addr, &sample_set(), &ClusterSecurity::default())
            .await
            .unwrap_err();
        assert!(err.contains("boom"), "{err}");
    }
}
