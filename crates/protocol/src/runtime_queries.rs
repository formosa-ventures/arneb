//! `system.runtime.queries`: the [`QueryTracker`]'s running and recent
//! queries as a table, for pgwire and the Trino protocol.
//!
//! As with the Trino virtual metadata tables, a snapshot is materialized
//! into an in-memory catalog per statement and the client's SQL runs over it
//! through the regular planner and executor, so filters, ordering and
//! `LIMIT` behave as on any table.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arneb_catalog::CatalogManager;
use arneb_common::types::{ColumnInfo, DataType, TimeUnit};
use arneb_connectors::memory::{MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable};
use arneb_connectors::ConnectorRegistry;
use arneb_scheduler::{QueryInfo, QueryState, QueryTracker};
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};

const CATALOG: &str = "system";
const SCHEMA: &str = "runtime";
const TABLE: &str = "queries";

/// Does `sql` read `system.runtime.queries`?
pub fn references_runtime_queries(sql: &str) -> bool {
    let mut lower = sql.to_ascii_lowercase();
    if lower.contains('"') {
        lower.retain(|c| c != '"');
    }
    lower.contains("system.runtime.queries")
}

/// The `state` column: Trino has no `CANCELLED` state (a cancelled query is
/// `FAILED` with `USER_CANCELED`), so cancelled queries read as `FAILED`.
fn state_name(state: QueryState) -> String {
    match state {
        QueryState::Cancelled => QueryState::Failed.to_string(),
        other => other.to_string(),
    }
}

/// The `error_message` column; a cancelled query without one says so.
fn error_message(q: &QueryInfo) -> Option<String> {
    match (&q.error, q.state) {
        (Some(e), _) => Some(e.clone()),
        (None, QueryState::Cancelled) => Some("Query was canceled".to_string()),
        (None, _) => None,
    }
}

fn millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn elapsed_ms(from: SystemTime, to: SystemTime) -> i64 {
    to.duration_since(from)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn columns() -> Vec<ColumnInfo> {
    let ts = DataType::Timestamp {
        unit: TimeUnit::Millisecond,
        timezone: Some("UTC".to_string()),
    };
    [
        ("query_id", DataType::Utf8, false),
        ("state", DataType::Utf8, false),
        ("user", DataType::Utf8, true),
        ("source", DataType::Utf8, false),
        ("query", DataType::Utf8, false),
        ("queued_time_ms", DataType::Int64, true),
        ("elapsed_time_ms", DataType::Int64, false),
        ("rows", DataType::Int64, true),
        ("created", ts.clone(), false),
        ("started", ts.clone(), true),
        ("end", ts, true),
        ("error_message", DataType::Utf8, true),
    ]
    .into_iter()
    .map(|(name, data_type, nullable)| ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable,
    })
    .collect()
}

/// The tracker's queries as one batch, in [`columns`] order.
fn batch(queries: &[QueryInfo], now: SystemTime) -> RecordBatch {
    let text = |f: &dyn Fn(&QueryInfo) -> Option<String>| -> ArrayRef {
        Arc::new(StringArray::from(queries.iter().map(f).collect::<Vec<_>>()))
    };
    let int = |f: &dyn Fn(&QueryInfo) -> Option<i64>| -> ArrayRef {
        Arc::new(Int64Array::from(queries.iter().map(f).collect::<Vec<_>>()))
    };
    let time = |f: &dyn Fn(&QueryInfo) -> Option<SystemTime>| -> ArrayRef {
        Arc::new(
            TimestampMillisecondArray::from(
                queries.iter().map(|q| f(q).map(millis)).collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        )
    };
    let schema = Arc::new(arrow::datatypes::Schema::new(
        columns()
            .into_iter()
            .map(arrow::datatypes::Field::from)
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(
        schema,
        vec![
            text(&|q| Some(q.query_id.to_string())),
            text(&|q| Some(state_name(q.state))),
            text(&|q| q.user.clone()),
            text(&|q| Some(q.source.clone())),
            text(&|q| Some(q.sql.clone())),
            int(&|q| q.started.map(|s| elapsed_ms(q.created, s))),
            int(&|q| Some(elapsed_ms(q.created, q.ended.unwrap_or(now)))),
            int(&|q| q.rows.map(|r| r as i64)),
            time(&|q| Some(q.created)),
            time(&|q| q.started),
            time(&|q| q.ended),
            text(&|q| error_message(q)),
        ],
    )
    .expect("runtime.queries columns match their schema")
}

/// A catalog manager and connector registry holding only
/// `system.runtime.queries`, snapshotted from `tracker` now. `session` is the
/// reader's catalog view: only queries its user may see are listed
/// (file-based-access-control).
pub fn runtime_catalog(
    tracker: &QueryTracker,
    session: &CatalogManager,
) -> (CatalogManager, ConnectorRegistry) {
    let mut queries = tracker.list_queries(None);
    queries.retain(|q| session.can_view_query(q.user.as_deref().unwrap_or_default()));
    let schema = Arc::new(MemorySchema::new());
    schema.register_table(
        TABLE,
        Arc::new(MemoryTable::new(
            columns(),
            vec![batch(&queries, SystemTime::now())],
        )),
    );
    let catalog = Arc::new(MemoryCatalog::new());
    catalog.register_schema(SCHEMA, schema);
    let cm = CatalogManager::new(CATALOG, SCHEMA);
    cm.register_catalog(CATALOG, catalog.clone());
    let mut registry = ConnectorRegistry::new();
    registry.register(
        CATALOG,
        Arc::new(MemoryConnectorFactory::new(catalog, SCHEMA)),
    );
    (cm, registry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_qualified_and_quoted_references() {
        assert!(references_runtime_queries(
            "SELECT * FROM system.runtime.queries"
        ));
        assert!(references_runtime_queries(
            r#"select state from "system"."runtime"."queries""#
        ));
        assert!(!references_runtime_queries(
            "SELECT * FROM system.jdbc.tables"
        ));
    }

    #[tokio::test]
    async fn queries_are_selectable_with_filters_and_ordering() {
        let tracker = QueryTracker::with_history_limit(10);
        let done = tracker.create_query_from("SELECT 1".into(), "pgwire", Some("ann".into()));
        tracker.finish_query(&done, 1).unwrap();
        let failed = tracker.create_query_from("SELECT nope".into(), "trino", None);
        tracker
            .fail_query(&failed, "column nope not found".into())
            .unwrap();
        let running = tracker.create_query_from("SELECT 3".into(), "pgwire", None);
        tracker.advance_to(&running, QueryState::Running).unwrap();
        let cancelled = tracker.create_query_from("SELECT 4".into(), "trino", None);
        tracker.cancel_query(&cancelled).unwrap();

        let (cm, registry) = runtime_catalog(&tracker, &CatalogManager::new("memory", "default"));
        let pool: Arc<dyn arneb_execution::memory_pool::MemoryPool> =
            Arc::new(arneb_execution::memory_pool::UnboundedMemoryPool::new());
        let (_, batches) = crate::handler::execute_query(
            "SELECT state, source, rows, error_message FROM system.runtime.queries \
             WHERE state <> 'RUNNING' ORDER BY state, rows NULLS FIRST, error_message",
            &cm,
            &registry,
            None,
            &pool,
        )
        .await
        .unwrap();
        let b = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
        let col = |i: usize| {
            b.column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_string))
                .collect::<Vec<_>>()
        };
        let s = |v: &str| Some(v.to_string());
        assert_eq!(col(0), vec![s("FAILED"), s("FAILED"), s("FINISHED")]);
        assert_eq!(col(1), vec![s("trino"), s("trino"), s("pgwire")]);
        assert_eq!(
            col(3)[..2],
            [s("Query was canceled"), s("column nope not found")]
        );
        let rows = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(arrow::array::Array::is_null(rows, 0));
        assert_eq!(rows.value(2), 1);
    }
}
