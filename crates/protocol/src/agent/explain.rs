//! Explain for agents: plan a query without running it and report the
//! estimated scan size wherever the connectors know it. Not admitted and not
//! tracked; `EXPLAIN ANALYZE` is a statement, not an explain.

use serde::{Deserialize, Serialize};

use arneb_common::diagnostic::SourceFile;

use super::error::{AgentError, AgentErrorCode};
use super::service::AgentQueryService;
use crate::trino::{parse_control, ClientSession, TrinoError};

/// Body of an explain request.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExplainRequest {
    /// The query to plan.
    pub sql: String,
    /// Default catalog for unqualified names.
    pub catalog: Option<String>,
    /// Default schema for unqualified names.
    pub schema: Option<String>,
}

/// What one table scan in the plan is expected to read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ScanEstimate {
    /// Fully qualified `catalog.schema.table`.
    pub table: String,
    /// Approximate row count; null when the connector does not know.
    pub row_count: Option<u64>,
    /// Size in bytes; null when the connector does not know.
    pub size_bytes: Option<u64>,
}

/// A planned query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Explanation {
    /// The optimized logical plan, as `EXPLAIN` prints it, cut at
    /// `default_page_bytes` on a character boundary.
    pub plan: String,
    /// Whether `plan` was cut.
    pub plan_truncated: bool,
    /// One entry per table scan in the plan.
    pub scans: Vec<ScanEstimate>,
    /// Sum of the scans' `size_bytes`; null (unknown) unless every scan
    /// knows its size.
    pub estimated_scan_bytes: Option<u64>,
}

impl AgentQueryService {
    /// Plans `req.sql` for `user` without executing it.
    pub async fn explain(
        &self,
        user: &str,
        req: &ExplainRequest,
    ) -> Result<Explanation, AgentError> {
        let sql = req.sql.trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return Err(AgentError::new(
                AgentErrorCode::InvalidArgument,
                "sql is empty",
            ));
        }
        let not_a_query = || {
            AgentError::new(
                AgentErrorCode::InvalidArgument,
                "statement has no query plan",
            )
            .with_hint("explain applies to queries")
        };
        if parse_control(sql).is_some() {
            return Err(not_a_query());
        }
        match arneb_sql_parser::parse(sql) {
            Ok(arneb_sql_parser::ast::Statement::Query { .. }) => {}
            Ok(arneb_sql_parser::ast::Statement::Explain { .. }) => {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidArgument,
                    "statement is already an EXPLAIN",
                )
                .with_hint("send the query without EXPLAIN"))
            }
            Ok(_) => return Err(not_a_query()),
            Err(e) => {
                let source = SourceFile::new("<query>", sql);
                return Err(AgentError::from_trino(&TrinoError::from_arneb(
                    &e.into(),
                    Some(&source),
                )));
            }
        }
        let session = ClientSession::for_agent(user, req.catalog.clone(), req.schema.clone());
        let (plan, scans) = self
            .engine
            .plan_for_explain(&session, sql)
            .await
            .map_err(|e| AgentError::from_trino(&e))?;
        let (plan, plan_truncated) = cut(plan, self.limits.default_page_bytes);
        let estimated_scan_bytes = scans
            .iter()
            .map(|s| s.size_bytes)
            .try_fold(0u64, |sum, size| size.map(|n| sum.saturating_add(n)));
        Ok(Explanation {
            plan,
            plan_truncated,
            scans,
            estimated_scan_bytes,
        })
    }
}

/// `text` cut to at most `max_bytes` bytes on a character boundary, and
/// whether it was cut.
fn cut(mut text: String, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text, false);
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    (text, true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use arneb_catalog::{
        CatalogManager, CatalogProvider, SchemaProvider, TableProvider, TableStatistics,
    };
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::{
        MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
    };
    use arneb_connectors::ConnectorRegistry;
    use arneb_scheduler::QueryTracker;

    use super::*;
    use crate::agent::{AgentLimits, SubmitRequest};

    fn columns() -> Vec<ColumnInfo> {
        vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }]
    }

    #[derive(Debug)]
    struct SizedTable(u64);

    impl TableProvider for SizedTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            columns()
        }
        fn statistics(&self) -> Option<TableStatistics> {
            Some(TableStatistics {
                row_count: Some(self.0 / 10),
                size_bytes: Some(self.0),
                columns: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct SizedSchema;

    #[async_trait]
    impl SchemaProvider for SizedSchema {
        async fn table_names(&self) -> Vec<String> {
            vec!["a".into(), "b".into()]
        }
        async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
            match name {
                "a" => Some(Arc::new(SizedTable(1000))),
                "b" => Some(Arc::new(SizedTable(240))),
                _ => None,
            }
        }
    }

    #[derive(Debug)]
    struct SizedCatalog;

    #[async_trait]
    impl CatalogProvider for SizedCatalog {
        async fn schema_names(&self) -> Vec<String> {
            vec!["s".into()]
        }
        async fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
            (name == "s").then(|| Arc::new(SizedSchema) as Arc<dyn SchemaProvider>)
        }
    }

    fn service() -> (AgentQueryService, Arc<QueryTracker>) {
        service_with(AgentLimits::default())
    }

    fn service_with(limits: AgentLimits) -> (AgentQueryService, Arc<QueryTracker>) {
        let schema = Arc::new(MemorySchema::new());
        schema.register_table("mem", Arc::new(MemoryTable::new(columns(), vec![])));
        let memory = Arc::new(MemoryCatalog::new());
        memory.register_schema("default", schema);
        let cm = CatalogManager::new("memory", "default");
        cm.register_catalog("memory", memory.clone());
        cm.register_catalog("sized", Arc::new(SizedCatalog));
        let mut registry = ConnectorRegistry::new();
        registry.register(
            "memory",
            Arc::new(MemoryConnectorFactory::new(memory, "default")),
        );
        let tracker = Arc::new(QueryTracker::new());
        let svc = AgentQueryService::new(
            Arc::new(cm),
            Arc::new(registry),
            Arc::clone(&tracker),
            limits,
        );
        (svc, tracker)
    }

    fn req(sql: &str) -> ExplainRequest {
        ExplainRequest {
            sql: sql.into(),
            catalog: None,
            schema: None,
        }
    }

    #[tokio::test]
    async fn explain_text_matches_explain_statement() {
        let (svc, _) = service();
        let sql = "SELECT id FROM sized.s.a WHERE id > 3";
        let explained = svc.explain("alice", &req(sql)).await.unwrap();
        let status = svc
            .submit(
                "alice",
                SubmitRequest {
                    sql: format!("EXPLAIN {sql}"),
                    catalog: None,
                    schema: None,
                    max_rows: None,
                },
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        let page = svc.results("alice", &status.id, 0, None).unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0][0].as_str().unwrap(), explained.plan);
        assert!(!explained.plan_truncated);
    }

    #[tokio::test]
    async fn explain_plan_is_cut_at_the_page_budget() {
        // A multi-byte literal puts character boundaries off the budget.
        let sql = format!("SELECT '{}' AS t, id FROM sized.s.a", "測".repeat(40));
        for budget in 60..=72 {
            let (svc, _) = service_with(AgentLimits {
                default_page_bytes: budget,
                ..AgentLimits::default()
            });
            let e = svc.explain("alice", &req(&sql)).await.unwrap();
            assert!(e.plan_truncated, "{budget}: {e:?}");
            assert!(e.plan.len() <= budget, "{budget}: {} bytes", e.plan.len());
            assert!(
                e.plan.len() > budget - 3,
                "{budget}: {} bytes",
                e.plan.len()
            );
            assert!(std::str::from_utf8(e.plan.as_bytes()).is_ok());
            assert_eq!(e.scans.len(), 1, "scan estimates are kept");
        }
    }

    #[tokio::test]
    async fn explain_sums_known_scan_sizes() {
        let (svc, _) = service();
        let e = svc
            .explain(
                "alice",
                &req("SELECT a.id FROM sized.s.a a JOIN sized.s.b b ON a.id = b.id"),
            )
            .await
            .unwrap();
        assert_eq!(e.scans.len(), 2);
        assert!(e.scans.iter().all(|s| s.table.starts_with("sized.s.")));
        assert_eq!(e.estimated_scan_bytes, Some(1240));
        let a = e.scans.iter().find(|s| s.table == "sized.s.a").unwrap();
        assert_eq!((a.row_count, a.size_bytes), (Some(100), Some(1000)));
    }

    #[tokio::test]
    async fn explain_unknown_size_is_null() {
        let (svc, _) = service();
        let e = svc
            .explain(
                "alice",
                &req("SELECT a.id FROM sized.s.a a JOIN mem m ON a.id = m.id"),
            )
            .await
            .unwrap();
        assert_eq!(e.estimated_scan_bytes, None);
        let mem = e
            .scans
            .iter()
            .find(|s| s.table == "memory.default.mem")
            .unwrap();
        assert_eq!(mem.size_bytes, None);
        let a = e.scans.iter().find(|s| s.table == "sized.s.a").unwrap();
        assert_eq!(a.size_bytes, Some(1000));
    }

    #[tokio::test]
    async fn explain_reports_a_repeated_table_once_per_scan() {
        let (svc, _) = service();
        let e = svc
            .explain(
                "alice",
                &req("SELECT id FROM sized.s.b UNION ALL SELECT id FROM sized.s.b"),
            )
            .await
            .unwrap();
        assert_eq!(e.scans.len(), 2);
        assert_eq!(e.estimated_scan_bytes, Some(480));
    }

    #[tokio::test]
    async fn explain_refuses_ddl_and_nested_explain() {
        let (svc, _) = service();
        for sql in [
            "CREATE TABLE t (id BIGINT)",
            "USE memory.default",
            "DROP TABLE mem",
        ] {
            let err = svc.explain("alice", &req(sql)).await.unwrap_err();
            assert_eq!(err.code, AgentErrorCode::InvalidArgument, "{sql}");
            assert_eq!(
                err.hint.as_deref(),
                Some("explain applies to queries"),
                "{sql}"
            );
        }
        let err = svc
            .explain("alice", &req("EXPLAIN SELECT 1"))
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::InvalidArgument);
        assert_eq!(err.hint.as_deref(), Some("send the query without EXPLAIN"));
    }

    #[tokio::test]
    async fn explain_error_has_location() {
        let (svc, _) = service();
        let err = svc
            .explain("alice", &req("SELECT nope FROM sized.s.a"))
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::ColumnNotFound);
        assert!(err.location.is_some(), "{err:?}");
    }

    #[tokio::test]
    async fn explain_plans_virtual_catalogs_like_run() {
        let (svc, _) = service();
        for sql in [
            "SELECT table_name FROM information_schema.tables",
            "SELECT query_id FROM system.runtime.queries",
        ] {
            let e = svc.explain("alice", &req(sql)).await.unwrap();
            assert_eq!(e.scans.len(), 1, "{sql}: {e:?}");
        }
    }

    #[tokio::test]
    async fn explain_resolves_unqualified_tables_with_session_defaults() {
        let (svc, _) = service();
        let e = svc
            .explain(
                "alice",
                &ExplainRequest {
                    sql: "SELECT id FROM a".into(),
                    catalog: Some("sized".into()),
                    schema: Some("s".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(e.scans.len(), 1);
        assert_eq!(e.scans[0].table, "sized.s.a");
        assert_eq!(e.estimated_scan_bytes, Some(1000));
    }

    #[tokio::test]
    async fn explain_does_not_register_with_tracker() {
        let (svc, tracker) = service();
        svc.explain("alice", &req("SELECT id FROM sized.s.a"))
            .await
            .unwrap();
        assert!(tracker.list_queries(None).is_empty());
    }
}
