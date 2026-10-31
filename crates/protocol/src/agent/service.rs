//! The agent statement service: submit, poll and cancel statements that run
//! through the Trino-protocol engine, with an idle-timeout reaper.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;

use arneb_catalog::CatalogManager;
use arneb_connectors::ConnectorRegistry;
use arneb_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use arneb_scheduler::{AdmissionController, AdmissionPermit, QueryState, QueryTracker};

use super::error::{AgentError, AgentErrorCode};
use super::limits::AgentLimits;
use super::results::{retain, ResultPage};
use super::statement::{Column, FinishedOutput, Phase, Statement, StatementStatus, SubmitRequest};
use crate::handler::DistributedExecutor;
use crate::trino::{
    needs_admission, parse_control, with_deadline, ClientSession, ControlStatement, Engine,
    QueryOutput, TimeLimits, TrinoError,
};

/// Statements (running or finished) one user may hold; at the cap the least
/// recently used finished one is evicted.
const MAX_STATEMENTS_PER_USER: usize = 32;
/// Statements across all users, evicted the same way.
const MAX_STATEMENTS_TOTAL: usize = 1024;
/// Longest interval between reaper passes.
const MAX_REAPER_TICK: Duration = Duration::from_secs(30);

/// Hint for a refused session statement.
const SESSION_STATEMENT_HINT: &str = "pass \"catalog\" and \"schema\" in the request body";

/// Runs agent statements asynchronously: [`submit`](Self::submit) returns at
/// once (or within the requested wait), [`status`](Self::status) polls,
/// [`cancel`](Self::cancel) stops one. Statements belong to the user who
/// submitted them; any other user is told they do not exist.
#[derive(Clone)]
pub struct AgentQueryService {
    pub(super) engine: Arc<Engine>,
    tracker: Arc<QueryTracker>,
    pub(super) limits: Arc<AgentLimits>,
    admission: Option<AdmissionController>,
    statement_timeout: Option<Duration>,
    statements: Arc<Mutex<HashMap<String, Arc<Statement>>>>,
}

impl AgentQueryService {
    /// A service over the same catalog, connector and tracker state the
    /// other protocols use.
    pub fn new(
        catalog_manager: Arc<CatalogManager>,
        connector_registry: Arc<ConnectorRegistry>,
        query_tracker: Arc<QueryTracker>,
        limits: AgentLimits,
    ) -> Self {
        Self {
            engine: Arc::new(Engine {
                catalog_manager,
                connector_registry,
                distributed_executor: None,
                memory_pool: Arc::new(UnboundedMemoryPool::new()),
                query_memory_limit: None,
                query_tracker: Arc::clone(&query_tracker),
                access_rules: None,
            }),
            tracker: query_tracker,
            limits: Arc::new(limits),
            admission: None,
            statement_timeout: None,
            statements: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The limits every request is validated against.
    pub fn limits(&self) -> &AgentLimits {
        &self.limits
    }

    /// The access-scoped catalog root `user` sees (same visibility as
    /// `SHOW`), or the error that user is refused with.
    pub(super) fn scoped_root(&self, user: &str) -> Result<Arc<CatalogManager>, AgentError> {
        self.engine
            .session_root(&ClientSession::for_agent(user, None, None))
            .map_err(|e| AgentError::from_trino(&e))
    }

    /// Replace the engine with a copy changed by `change`.
    fn with_engine(mut self, change: impl FnOnce(&mut Engine)) -> Self {
        let e = &self.engine;
        let mut engine = Engine {
            catalog_manager: Arc::clone(&e.catalog_manager),
            connector_registry: Arc::clone(&e.connector_registry),
            distributed_executor: e.distributed_executor.clone(),
            memory_pool: Arc::clone(&e.memory_pool),
            query_memory_limit: e.query_memory_limit,
            query_tracker: Arc::clone(&e.query_tracker),
            access_rules: e.access_rules.clone(),
        };
        change(&mut engine);
        self.engine = Arc::new(engine);
        self
    }

    /// Routes queries through the coordinator's distributed executor.
    pub fn with_distributed_executor(self, executor: Arc<dyn DistributedExecutor>) -> Self {
        self.with_engine(|e| e.distributed_executor = Some(executor))
    }

    /// Memory pool threaded into every statement's execution context.
    pub fn with_memory_pool(self, pool: Arc<dyn MemoryPool>) -> Self {
        self.with_engine(|e| e.memory_pool = pool)
    }

    /// Limit each statement to `limit` bytes of the memory pool on this node
    /// (`query_max_memory_per_node`; default: no per-query limit).
    pub fn with_query_memory_limit(self, limit: Option<usize>) -> Self {
        self.with_engine(|e| e.query_memory_limit = limit)
    }

    /// Queue statements through this admission controller (shared with the
    /// other protocols). Default: none, every statement runs at once.
    pub fn with_admission(mut self, admission: Option<AdmissionController>) -> Self {
        self.admission = admission;
        self
    }

    /// Server time limit for every statement (`[admission]
    /// statement_timeout`), counted from submission so it covers queue time.
    /// Default: off.
    pub fn with_statement_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Check every statement against `rules` for the submitting user
    /// (file-based-access-control). Default: none, every user reads all.
    pub fn with_access_rules(self, rules: Option<Arc<arneb_catalog::AccessRules>>) -> Self {
        self.with_engine(|e| e.access_rules = rules)
    }

    /// Spawn the reaper: every `min(idle_timeout / 4, 30 s)` it drops
    /// terminal statements and cancels running ones (as abandoned) that no
    /// request has touched for `idle_timeout`.
    pub fn spawn_reaper(&self) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            let tick = (service.limits.idle_timeout / 4)
                .min(MAX_REAPER_TICK)
                .max(Duration::from_millis(1));
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                service.reap();
            }
        })
    }

    /// Submit a statement for `user`, waiting up to `wait` (capped at
    /// `max_wait`) for it to finish.
    pub async fn submit(
        &self,
        user: &str,
        req: SubmitRequest,
        wait: Duration,
    ) -> Result<StatementStatus, AgentError> {
        let sql = req.sql.trim();
        if sql.trim_end_matches(';').trim().is_empty() {
            return Err(AgentError::new(
                AgentErrorCode::InvalidArgument,
                "sql is empty",
            ));
        }
        refuse_session_statement(sql)?;
        let max_rows = match req.max_rows {
            None => self.limits.default_max_rows,
            Some(n) if (1..=self.limits.max_rows).contains(&n) => n,
            Some(n) => {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidArgument,
                    format!("max_rows {n} is out of range"),
                )
                .with_hint(format!("max_rows must be in 1..={}", self.limits.max_rows)))
            }
        };
        let statement = self.register(user, sql.to_string(), max_rows)?;
        let session = ClientSession::for_agent(user, req.catalog, req.schema);
        tracing::debug!(statement_id = %statement.id, user, "agent statement submitted");
        let handle = tokio::spawn(self.clone().run(Arc::clone(&statement), session));
        *lock(&statement.abort) = Some(handle.abort_handle());
        // Cancelled before the handle was stored: stop the task now.
        if statement.is_terminal() {
            if let Some(handle) = lock(&statement.abort).take() {
                handle.abort();
            }
        }
        self.wait_for_end(&statement, wait).await;
        Ok(statement.status(&self.limits))
    }

    /// The status of `user`'s statement `id`, waiting up to `wait` (capped
    /// at `max_wait`) for it to finish.
    pub async fn status(
        &self,
        user: &str,
        id: &str,
        wait: Duration,
    ) -> Result<StatementStatus, AgentError> {
        let statement = self.lookup(user, id)?;
        self.wait_for_end(&statement, wait).await;
        Ok(statement.status(&self.limits))
    }

    /// A page of `user`'s finished statement `id`, starting at row `cursor`,
    /// at most `max_bytes` (default `default_page_bytes`) of serialized rows.
    pub fn results(
        &self,
        user: &str,
        id: &str,
        cursor: u64,
        max_bytes: Option<usize>,
    ) -> Result<ResultPage, AgentError> {
        let statement = self.lookup(user, id)?;
        let max_bytes = match max_bytes {
            None => self.limits.default_page_bytes,
            Some(n) if (1..=self.limits.max_page_bytes).contains(&n) => n,
            Some(n) => {
                return Err(AgentError::new(
                    AgentErrorCode::InvalidArgument,
                    format!("max_bytes {n} is out of range"),
                )
                .with_hint(format!(
                    "max_bytes must be in 1..={}",
                    self.limits.max_page_bytes
                )))
            }
        };
        statement.page(cursor, max_bytes, self.limits.max_cell_bytes)
    }

    /// Cancel `user`'s statement `id`. Idempotent: a terminal statement
    /// keeps its state and its current status is returned.
    pub fn cancel(&self, user: &str, id: &str) -> Result<StatementStatus, AgentError> {
        let statement = self.lookup(user, id)?;
        self.cancel_statement(
            &statement,
            AgentError::new(AgentErrorCode::Cancelled, "Statement was cancelled"),
            "client",
        );
        Ok(statement.status(&self.limits))
    }

    /// Checks the caps and registers a new statement with the tracker and
    /// the statement map, atomically.
    fn register(
        &self,
        user: &str,
        sql: String,
        max_rows: usize,
    ) -> Result<Arc<Statement>, AgentError> {
        let mut statements = lock(&self.statements);
        let mine = statements.values().filter(|s| s.user == user).count();
        // At a cap, make room by evicting the least-recently-accessed
        // terminal statement (the user's own for the per-user cap); refuse
        // only when every counted statement is still queued or running.
        let victim = if mine >= MAX_STATEMENTS_PER_USER {
            let victim = lru_terminal(statements.values().filter(|s| s.user == user));
            Some(victim.ok_or_else(|| {
                AgentError::overloaded(format!(
                    "user {user} already has {MAX_STATEMENTS_PER_USER} statements running or queued"
                ))
            })?)
        } else if statements.len() >= MAX_STATEMENTS_TOTAL {
            let victim = lru_terminal(statements.values());
            Some(victim.ok_or_else(|| {
                AgentError::overloaded(format!(
                    "the server already has {MAX_STATEMENTS_TOTAL} statements running or queued"
                ))
            })?)
        } else {
            None
        };
        if let Some(id) = victim {
            statements.remove(&id);
            tracing::debug!(statement_id = %id, "agent statement evicted at the cap");
        }
        let tracker_id = self
            .tracker
            .create_query_from(sql.clone(), "api", Some(user.to_string()));
        let statement = Arc::new(Statement::new(tracker_id, user, sql, max_rows));
        statements.insert(statement.id.clone(), Arc::clone(&statement));
        Ok(statement)
    }

    /// `user`'s statement `id`, touched; any other user's is not found.
    fn lookup(&self, user: &str, id: &str) -> Result<Arc<Statement>, AgentError> {
        let statement = lock(&self.statements)
            .get(id)
            .filter(|s| s.user == user)
            .cloned()
            .ok_or_else(|| AgentError::statement_not_found(id))?;
        statement.touch();
        Ok(statement)
    }

    /// Waits up to `wait` (capped at `max_wait`) for `statement` to end.
    async fn wait_for_end(&self, statement: &Statement, wait: Duration) {
        let wait = wait.min(self.limits.max_wait);
        if wait.is_zero() || statement.is_terminal() {
            return;
        }
        let mut done = statement.done.subscribe();
        let _ = tokio::time::timeout(wait, done.wait_for(|d| *d)).await;
        statement.touch();
    }

    /// The statement task, in the order of the Trino submit path: (already
    /// registered with the tracker) admit, run under the time limit, record
    /// the outcome.
    async fn run(self, statement: Arc<Statement>, session: ClientSession) {
        if statement.is_terminal() {
            return; // cancelled before the task started
        }
        let limits = match TimeLimits::from_session(&session, self.statement_timeout) {
            Ok(l) => l,
            Err(err) => return self.complete(&statement, Err(err)),
        };
        // The run limit counts from submission and covers the queue wait.
        let run_deadline = limits.run.and_then(|d| statement.created.checked_add(d));
        let _permit = match with_deadline(run_deadline, self.admit(&statement, &session)).await {
            Some(Ok(permit)) => permit,
            Some(Err(err)) => return self.complete(&statement, Err(err)),
            None => {
                let err = limits.exceeded("query_max_run_time");
                return self.complete(&statement, Err(err));
            }
        };
        let exec_deadline = match (
            run_deadline,
            limits
                .execution
                .and_then(|d| tokio::time::Instant::now().checked_add(d)),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        {
            let mut inner = statement.lock();
            if matches!(inner.phase, Phase::Queued(_)) {
                inner.phase = Phase::Running;
            }
        }
        let _ = self
            .tracker
            .advance_to(&statement.tracker_id, QueryState::Running);
        let run = std::panic::AssertUnwindSafe(self.engine.run_statement_capped(
            &session,
            &statement.sql,
            statement.max_rows + 1,
        ))
        .catch_unwind();
        let result = match with_deadline(exec_deadline, run).await {
            Some(outcome) => outcome
                .unwrap_or_else(|_| Err(TrinoError::internal("statement execution panicked"))),
            None => Err(limits.exceeded_during_execution(run_deadline, exec_deadline)),
        };
        self.complete(&statement, result);
    }

    /// Takes an admission slot for a statement that executes a plan,
    /// reporting it as queued (with its position) while it waits.
    async fn admit(
        &self,
        statement: &Statement,
        session: &ClientSession,
    ) -> Result<Option<AdmissionPermit>, TrinoError> {
        let Some(controller) = &self.admission else {
            return Ok(None);
        };
        if !needs_admission(session, &statement.sql) {
            return Ok(None);
        }
        let pending = controller
            .admit_with(|| crate::handler::admission_label(&statement.sql))
            .map_err(|e| TrinoError::queue_full(e.to_string()))?;
        if let Some(position) = pending.queue_position() {
            if let Phase::Queued(p) = &mut statement.lock().phase {
                *p = Some(position);
            }
        }
        Ok(Some(pending.wait().await))
    }

    /// Records the outcome, unless the statement was cancelled meanwhile.
    fn complete(&self, statement: &Statement, result: Result<QueryOutput, TrinoError>) {
        match result {
            Ok(output) => {
                let retained = retain(output.batches, statement.max_rows);
                let rows = retained.kept_rows();
                let finished = FinishedOutput {
                    columns: output.columns.map(|cols| {
                        cols.iter()
                            .map(|c| Column {
                                name: c.name.clone(),
                                type_name: c.ty.display(),
                            })
                            .collect()
                    }),
                    update_type: output.update_type,
                    retained,
                };
                if statement.end(Phase::Finished(finished)) {
                    let _ = self.tracker.finish_query(&statement.tracker_id, rows);
                }
            }
            Err(err) => {
                if statement.end(Phase::Failed(AgentError::from_trino(&err))) {
                    let _ = self.tracker.fail_query(&statement.tracker_id, err.message);
                }
            }
        }
    }

    /// Cancels a statement that is still queued or running: aborts its task
    /// (dropping its admission slot) and marks it cancelled in the tracker.
    fn cancel_statement(&self, statement: &Statement, error: AgentError, reason: &str) {
        if !statement.end(Phase::Cancelled(error)) {
            return;
        }
        if let Some(handle) = lock(&statement.abort).take() {
            handle.abort();
        }
        let _ = self.tracker.cancel_query(&statement.tracker_id);
        tracing::debug!(statement_id = %statement.id, reason, "agent statement cancelled");
    }

    /// One reaper pass over statements idle for longer than `idle_timeout`.
    fn reap(&self) {
        let idle_timeout = self.limits.idle_timeout;
        let statements: Vec<Arc<Statement>> = lock(&self.statements).values().cloned().collect();
        for statement in statements {
            if statement.idle() <= idle_timeout {
                continue;
            }
            if statement.is_terminal() {
                let mut map = lock(&self.statements);
                // Re-check under the map lock: a request may just have come in.
                if statement.idle() > idle_timeout {
                    map.remove(&statement.id);
                }
            } else {
                let error = AgentError::new(
                    AgentErrorCode::Cancelled,
                    format!(
                        "Statement was abandoned: no request within the idle timeout of {}s",
                        idle_timeout.as_secs()
                    ),
                );
                self.cancel_statement(&statement, error, "abandoned");
                // Keep it for another idle period so a returning client
                // learns why it was cancelled.
                statement.touch();
            }
        }
    }
}

/// Id of the least-recently-accessed terminal statement among `statements`.
fn lru_terminal<'a>(statements: impl Iterator<Item = &'a Arc<Statement>>) -> Option<String> {
    statements
        .filter_map(|s| {
            let inner = s.lock();
            inner
                .phase
                .state()
                .is_terminal()
                .then(|| (inner.last_access, s.id.clone()))
        })
        .min()
        .map(|(_, id)| id)
}

/// Session statements (`USE`, `SET SESSION`, `PREPARE`, …) have no effect in
/// a stateless API; `SHOW` and `DESCRIBE` are ordinary queries.
fn refuse_session_statement(sql: &str) -> Result<(), AgentError> {
    let sql = sql.trim_end_matches(';').trim();
    match parse_control(sql) {
        None
        | Some(
            ControlStatement::ShowCatalogs { .. }
            | ControlStatement::ShowSchemas { .. }
            | ControlStatement::ShowTables { .. }
            | ControlStatement::ShowColumns { .. }
            | ControlStatement::ShowSession,
        ) => Ok(()),
        Some(_) => Err(AgentError::new(
            AgentErrorCode::InvalidArgument,
            "session statements are not supported: each statement runs in its own session",
        )
        .with_hint(SESSION_STATEMENT_HINT)),
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;

    use arneb_common::error::ArnebError;
    use arneb_common::identifiers::QueryId;
    use arneb_common::types::{ColumnInfo, DataType};
    use arneb_connectors::memory::{
        MemoryCatalog, MemoryConnectorFactory, MemorySchema, MemoryTable,
    };
    use arneb_execution::ExecutionContext;
    use arneb_planner::LogicalPlan;

    use crate::agent::statement::StatementState;
    use serde_json::{json, Value};

    /// `memory.default.lineitem(l_orderkey, l_quantity)`, two rows.
    fn engine_state() -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
        engine_state_with(Vec::new())
    }

    /// `engine_state` plus extra `memory.default` tables.
    fn engine_state_with(
        extra: Vec<(&str, RecordBatch)>,
    ) -> (Arc<CatalogManager>, Arc<ConnectorRegistry>) {
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
        for (name, batch) in extra {
            let infos = batch
                .schema()
                .fields()
                .iter()
                .map(|f| ColumnInfo {
                    name: f.name().clone(),
                    data_type: DataType::try_from(f.data_type().clone()).unwrap(),
                    nullable: f.is_nullable(),
                })
                .collect();
            default_schema.register_table(name, Arc::new(MemoryTable::new(infos, vec![batch])));
        }
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

    fn service_with(limits: AgentLimits) -> (AgentQueryService, Arc<QueryTracker>) {
        let (cm, registry) = engine_state();
        let tracker = Arc::new(QueryTracker::new());
        let svc = AgentQueryService::new(cm, registry, Arc::clone(&tracker), limits);
        (svc, tracker)
    }

    fn service_over(extra: Vec<(&str, RecordBatch)>, limits: AgentLimits) -> AgentQueryService {
        let (cm, registry) = engine_state_with(extra);
        AgentQueryService::new(cm, registry, Arc::new(QueryTracker::new()), limits)
    }

    /// `big(n)`: `n` rows 0..50_000.
    fn big_table() -> (&'static str, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "n",
            ArrowDataType::Int64,
            false,
        )]));
        let col: ArrayRef = Arc::new(Int64Array::from_iter_values(0..50_000));
        ("big", RecordBatch::try_new(schema, vec![col]).unwrap())
    }

    async fn run_to_end(
        svc: &AgentQueryService,
        sql: &str,
        max_rows: Option<usize>,
    ) -> StatementStatus {
        let mut r = req(sql);
        r.max_rows = max_rows;
        svc.submit("alice", r, Duration::from_secs(30))
            .await
            .unwrap()
    }

    fn assert_invalid(err: AgentError, hint: &str) {
        assert_eq!(err.code, AgentErrorCode::InvalidArgument);
        assert!(err.hint.as_deref().unwrap_or("").contains(hint), "{err:?}");
    }

    #[tokio::test]
    async fn large_result_is_truncated_without_total() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let s = run_to_end(&svc, "SELECT n FROM big", Some(1000)).await;
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert_eq!(s.total_rows, None);
        assert!(s.truncated);
        assert_eq!(s.max_rows, 1000);
        let mut rows = s.first_page.as_ref().unwrap().rows.len();
        let mut cursor = s.first_page.unwrap().next_cursor;
        while let Some(c) = cursor {
            let p = svc.results("alice", &s.id, c, None).unwrap();
            rows += p.rows.len();
            cursor = p.next_cursor;
        }
        assert_eq!(rows, 1000);
    }

    async fn all_rows(svc: &AgentQueryService, s: StatementStatus) -> Vec<Vec<Value>> {
        let first = s.first_page.unwrap();
        let mut rows = first.rows;
        let mut cursor = first.next_cursor;
        while let Some(c) = cursor {
            let p = svc.results("alice", &s.id, c, None).unwrap();
            rows.extend(p.rows);
            cursor = p.next_cursor;
        }
        rows
    }

    #[tokio::test]
    async fn execution_stops_after_max_rows_plus_one() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let session = ClientSession::for_agent("alice", None, None);
        for sql in [
            "SELECT n FROM big",
            "SELECT n + 1 AS m FROM big",
            "SELECT n FROM big ORDER BY n DESC",
            "SELECT n FROM big LIMIT 20000",
        ] {
            let out = svc
                .engine
                .run_statement_capped(&session, sql, 101)
                .await
                .unwrap();
            let produced: usize = out.batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(produced, 101, "{sql}");
        }
    }

    #[tokio::test]
    async fn truncated_order_by_keeps_the_top_rows() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let s = run_to_end(&svc, "SELECT n FROM big ORDER BY n DESC", Some(3)).await;
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert!(s.truncated);
        assert_eq!(s.total_rows, None);
        let rows = all_rows(&svc, s).await;
        assert_eq!(
            rows,
            vec![
                vec![json!(49_999)],
                vec![json!(49_998)],
                vec![json!(49_997)]
            ]
        );
    }

    #[tokio::test]
    async fn result_of_exactly_max_rows_is_not_truncated() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let s = run_to_end(&svc, "SELECT n FROM big LIMIT 100", Some(100)).await;
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert!(!s.truncated);
        assert_eq!(s.total_rows, Some(100));
        let s = run_to_end(&svc, "SELECT n FROM big LIMIT 101", Some(100)).await;
        assert!(s.truncated);
        assert_eq!(s.total_rows, None);
    }

    #[tokio::test]
    async fn sql_limit_and_offset_compose_with_max_rows() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let s = run_to_end(
            &svc,
            "SELECT n FROM big ORDER BY n LIMIT 10 OFFSET 5",
            Some(3),
        )
        .await;
        assert_eq!(s.state, StatementState::Finished, "{:?}", s.error);
        assert!(s.truncated);
        let rows = all_rows(&svc, s).await;
        assert_eq!(rows, vec![vec![json!(5)], vec![json!(6)], vec![json!(7)]]);
        let s = run_to_end(
            &svc,
            "SELECT n FROM big ORDER BY n LIMIT 2 OFFSET 5",
            Some(3),
        )
        .await;
        assert!(!s.truncated);
        assert_eq!(s.total_rows, Some(2));
        let s = run_to_end(&svc, "SELECT count(*) AS c FROM big", Some(1)).await;
        assert!(!s.truncated);
        assert_eq!(all_rows(&svc, s).await, vec![vec![json!(50_000)]]);
    }

    #[tokio::test]
    async fn pages_respect_max_bytes_and_cursor_walks_all_rows() {
        let svc = service_over(vec![big_table()], AgentLimits::default());
        let s = run_to_end(&svc, "SELECT n FROM big", Some(2000)).await;
        let mut seen = Vec::new();
        let mut cursor = Some(0);
        while let Some(c) = cursor {
            let p = svc.results("alice", &s.id, c, Some(1024)).unwrap();
            let bytes: usize = p
                .rows
                .iter()
                .map(|r| serde_json::to_string(r).unwrap().len() + 1)
                .sum();
            assert!(bytes <= 1024 + 16, "{bytes}");
            seen.extend(p.rows.iter().map(|r| r[0].as_i64().unwrap()));
            cursor = p.next_cursor;
        }
        assert_eq!(seen, (0..2000).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn oversized_cell_is_cut_and_counted() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            ArrowDataType::Utf8,
            false,
        )]));
        let col: ArrayRef = Arc::new(arrow::array::StringArray::from(vec!["x".repeat(100_000)]));
        let wide = ("wide", RecordBatch::try_new(schema, vec![col]).unwrap());
        let svc = service_over(vec![wide], AgentLimits::default());
        let s = run_to_end(&svc, "SELECT s FROM wide", None).await;
        let page = s.first_page.unwrap();
        let cell = page.rows[0][0].as_str().unwrap();
        assert_eq!(cell.len(), 8192 + "…".len());
        assert!(cell.ends_with('…'));
        assert_eq!(page.truncated_cells, 1);
    }

    #[tokio::test]
    async fn results_before_finish_is_invalid_argument() {
        let (svc, _) = service();
        let (svc, _dropped) = hanging(svc);
        let s = svc
            .submit("alice", req(HANGING_SQL), Duration::ZERO)
            .await
            .unwrap();
        let err = svc.results("alice", &s.id, 0, None).unwrap_err();
        assert_invalid(err, "finished");
    }

    #[tokio::test]
    async fn cursor_past_end_is_invalid_argument() {
        let (svc, _) = service();
        let s = run_to_end(&svc, "SELECT l_quantity FROM lineitem", None).await;
        assert!(svc
            .results("alice", &s.id, 2, None)
            .unwrap()
            .rows
            .is_empty());
        let err = svc.results("alice", &s.id, 3, None).unwrap_err();
        assert_invalid(err, "0..=2");
    }

    #[tokio::test]
    async fn max_rows_above_ceiling_is_refused() {
        let (svc, _) = service();
        for n in [0, 10_001] {
            let mut r = req("SELECT 1");
            r.max_rows = Some(n);
            let err = svc.submit("alice", r, Duration::ZERO).await.unwrap_err();
            assert_invalid(err, "1..=10000");
        }
        let (svc, _) = service();
        let s = run_to_end(&svc, "SELECT 1", None).await;
        for n in [0, 4_194_305] {
            let err = svc.results("alice", &s.id, 0, Some(n)).unwrap_err();
            assert_invalid(err, "1..=4194304");
        }
    }

    /// The Trino engine is read-only: DDL such as CTAS fails as a statement
    /// error, it does not finish with an `update_type`.
    #[tokio::test]
    async fn ddl_statement_fails_with_error_body() {
        let (svc, _) = service();
        let s = run_to_end(&svc, "CREATE TABLE memory.default.t AS SELECT 1 AS x", None).await;
        assert_eq!(s.state, StatementState::Failed);
        assert!(s.error.is_some() && s.first_page.is_none());
    }

    /// A finished statement without a result set reports its update type and
    /// an empty first page.
    #[test]
    fn update_statement_reports_update_type_without_rows() {
        let statement = Statement::new(QueryId::new(), "alice", "USE x".into(), 10);
        statement.end(Phase::Finished(FinishedOutput {
            columns: None,
            update_type: Some("USE".into()),
            retained: retain(Vec::new(), 10),
        }));
        let s = statement.status(&AgentLimits::default());
        assert_eq!(s.state, StatementState::Finished);
        assert_eq!(s.update_type.as_deref(), Some("USE"));
        assert_eq!(s.total_rows, Some(0));
        let page = s.first_page.unwrap();
        assert!(page.rows.is_empty() && page.next_cursor.is_none());
    }

    fn service() -> (AgentQueryService, Arc<QueryTracker>) {
        service_with(AgentLimits::default())
    }

    fn req(sql: &str) -> SubmitRequest {
        SubmitRequest {
            sql: sql.to_string(),
            catalog: None,
            schema: None,
            max_rows: None,
        }
    }

    /// Executes every plan by never returning.
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
            std::future::pending::<()>().await;
            Ok(Vec::new())
        }

        fn has_workers(&self) -> bool {
            true
        }
    }

    const HANGING_SQL: &str = "SELECT l_quantity FROM lineitem";

    fn hanging(svc: AgentQueryService) -> (AgentQueryService, Arc<AtomicBool>) {
        let dropped = Arc::new(AtomicBool::new(false));
        let svc = svc.with_distributed_executor(Arc::new(HangingExecutor {
            dropped: Arc::clone(&dropped),
        }));
        (svc, dropped)
    }

    /// Polls until `id` reaches `state`.
    async fn wait_for_state(svc: &AgentQueryService, id: &str, state: StatementState) {
        for _ in 0..100 {
            let s = svc
                .status("alice", id, Duration::from_millis(50))
                .await
                .unwrap();
            if s.state == state {
                return;
            }
        }
        panic!("statement {id} never reached {state:?}");
    }

    async fn yield_many() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn submit_then_poll_reaches_finished() {
        let (svc, _) = service();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        assert!(matches!(
            s.state,
            StatementState::Queued | StatementState::Running | StatementState::Finished
        ));
        let s = svc
            .status("alice", &s.id, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
        assert_eq!(s.total_rows, Some(1));
        assert!(!s.truncated);
        assert_eq!(s.columns.as_ref().map(Vec::len), Some(1));
        assert!(s.error.is_none());
    }

    #[tokio::test]
    async fn submit_with_wait_returns_finished_inline() {
        let (svc, _) = service();
        let s = svc
            .submit(
                "alice",
                req("SELECT l_quantity FROM lineitem"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
        assert_eq!(s.total_rows, Some(2));
        let columns = s.columns.unwrap();
        assert_eq!(columns[0].name, "l_quantity");
        assert_eq!(columns[0].type_name, "bigint");
    }

    #[tokio::test]
    async fn poll_returns_within_wait_while_queued() {
        let admission = AdmissionController::new(1, 10);
        let (svc, _) = service();
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        let start = std::time::Instant::now();
        let s = svc
            .status("alice", &s.id, Duration::from_millis(200))
            .await
            .unwrap();
        let took = start.elapsed();
        assert!(took >= Duration::from_millis(150), "{took:?}");
        assert!(took < Duration::from_millis(200 + 500), "{took:?}");
        assert_eq!(s.state, StatementState::Queued);
        assert_eq!(s.queue_position, Some(1));
    }

    #[tokio::test]
    async fn wait_is_capped_at_max_wait() {
        let admission = AdmissionController::new(1, 10);
        let (svc, _) = service_with(AgentLimits {
            max_wait: Duration::from_millis(100),
            ..AgentLimits::default()
        });
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        let start = std::time::Instant::now();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::from_secs(5))
            .await
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(s.state, StatementState::Queued);
    }

    #[tokio::test]
    async fn cancel_running_statement_marks_cancelled_and_tracker_cancelled() {
        let (svc, tracker) = service();
        let (svc, dropped) = hanging(svc);
        let s = svc
            .submit("alice", req(HANGING_SQL), Duration::ZERO)
            .await
            .unwrap();
        wait_for_state(&svc, &s.id, StatementState::Running).await;
        let c = svc.cancel("alice", &s.id).unwrap();
        assert_eq!(c.state, StatementState::Cancelled);
        assert_eq!(c.error.unwrap().code, AgentErrorCode::Cancelled);
        let info = tracker
            .list_queries(None)
            .into_iter()
            .find(|q| q.query_id.to_string() == s.id)
            .unwrap();
        assert_eq!(info.state, QueryState::Cancelled);
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(dropped.load(Ordering::SeqCst), "execution was not aborted");
    }

    #[tokio::test]
    async fn cancel_is_idempotent_on_terminal_statement() {
        let (svc, tracker) = service();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
        for _ in 0..2 {
            let c = svc.cancel("alice", &s.id).unwrap();
            assert_eq!(c.state, StatementState::Finished);
            assert_eq!(c.total_rows, Some(1));
        }
        let info = tracker.list_queries(None).pop().unwrap();
        assert_eq!(info.state, QueryState::Finished);

        // A cancelled statement stays cancelled.
        let (svc, _) = hanging(svc);
        let s = svc
            .submit("alice", req(HANGING_SQL), Duration::ZERO)
            .await
            .unwrap();
        let first = svc.cancel("alice", &s.id).unwrap();
        let second = svc.cancel("alice", &s.id).unwrap();
        assert_eq!(first.state, StatementState::Cancelled);
        assert_eq!(second.state, StatementState::Cancelled);
        assert_eq!(first.elapsed_ms, second.elapsed_ms);
    }

    #[tokio::test]
    async fn other_users_statement_is_not_found() {
        let (svc, _) = service();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        let random = QueryId::new().to_string();
        let err = svc.status("bob", &s.id, Duration::ZERO).await.unwrap_err();
        let missing = svc
            .status("bob", &random, Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
        assert_eq!(
            err.message,
            missing.message.replace(&random, &s.id),
            "same message shape as for a random id"
        );
        let err = svc.cancel("bob", &s.id).unwrap_err();
        let missing = svc.cancel("bob", &random).unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
        assert_eq!(err.message, missing.message.replace(&random, &s.id));
        // Alice still sees it, and bob's cancel did nothing.
        let s = svc
            .status("alice", &s.id, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
    }

    #[tokio::test]
    async fn session_statements_are_refused_with_hint() {
        let (svc, tracker) = service();
        for sql in [
            "USE tpch.tiny",
            "SET SESSION query_max_run_time = '1h'",
            "PREPARE q FROM SELECT 1",
            "COMMIT",
        ] {
            let err = svc
                .submit("alice", req(sql), Duration::ZERO)
                .await
                .unwrap_err();
            assert_eq!(err.code, AgentErrorCode::InvalidArgument, "{sql}");
            assert!(err.hint.as_deref().unwrap().contains("catalog"), "{sql}");
        }
        assert!(tracker.list_queries(None).is_empty());
        // SHOW is not a session statement.
        let s = svc
            .submit("alice", req("SHOW CATALOGS"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
    }

    #[tokio::test]
    async fn empty_sql_is_invalid_argument() {
        let (svc, tracker) = service();
        let err = svc
            .submit("alice", req("  ; "), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::InvalidArgument);
        assert!(tracker.list_queries(None).is_empty());
    }

    #[tokio::test]
    async fn failed_statement_reports_structured_error() {
        let (svc, tracker) = service();
        let s = svc
            .submit(
                "alice",
                req("SELECT l_quantiy FROM lineitem"),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Failed);
        let err = s.error.unwrap();
        assert_eq!(err.code, AgentErrorCode::ColumnNotFound);
        assert_eq!(
            err.location,
            Some(crate::agent::error::SqlLocation { line: 1, column: 8 })
        );
        assert_eq!(err.candidates[0], "l_quantity");
        assert!(err.hint.unwrap().starts_with("did you mean"));
        let info = tracker.list_queries(None).pop().unwrap();
        assert_eq!(info.state, QueryState::Failed);
    }

    #[tokio::test]
    async fn per_user_cap_returns_overloaded() {
        let admission = AdmissionController::new(1, 2000);
        let (svc, tracker) = service();
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        for _ in 0..MAX_STATEMENTS_PER_USER {
            svc.submit("alice", req("SELECT 1"), Duration::ZERO)
                .await
                .unwrap();
        }
        let err = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::Overloaded);
        assert_eq!(
            err.hint.as_deref(),
            Some("retry after a running statement finishes")
        );
        assert_eq!(tracker.list_queries(None).len(), MAX_STATEMENTS_PER_USER);
        // Another user is not affected.
        svc.submit("bob", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn total_cap_returns_overloaded() {
        let admission = AdmissionController::new(1, 2000);
        let (svc, tracker) = service();
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        for i in 0..MAX_STATEMENTS_TOTAL {
            let user = format!("user{}", i / MAX_STATEMENTS_PER_USER);
            svc.submit(&user, req("SELECT 1"), Duration::ZERO)
                .await
                .unwrap();
        }
        let err = svc
            .submit("newcomer", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::Overloaded);
        assert_eq!(tracker.list_queries(None).len(), MAX_STATEMENTS_TOTAL);
    }

    #[tokio::test]
    async fn terminal_statements_are_evicted_lru_when_user_cap_is_full() {
        let (svc, tracker) = service();
        let mut ids = Vec::new();
        for _ in 0..MAX_STATEMENTS_PER_USER {
            let s = svc
                .submit("alice", req("SELECT 1"), Duration::from_secs(5))
                .await
                .unwrap();
            assert_eq!(s.state, StatementState::Finished);
            ids.push(s.id);
        }
        // Touch all but the first, so the first is least recently accessed.
        for id in &ids[1..] {
            svc.status("alice", id, Duration::ZERO).await.unwrap();
        }
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(s.state, StatementState::Finished);
        let err = svc
            .status("alice", &ids[0], Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
        for id in &ids[1..] {
            svc.status("alice", id, Duration::ZERO).await.unwrap();
        }
        // Eviction only forgets the statement; the tracker keeps its history.
        assert_eq!(
            tracker.list_queries(None).len(),
            MAX_STATEMENTS_PER_USER + 1
        );
    }

    #[tokio::test]
    async fn user_cap_overloaded_only_when_all_live() {
        let admission = AdmissionController::new(1, 2000);
        let (svc, _) = service();
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        // One terminal statement (SHOW takes no admission slot) ...
        let shown = svc
            .submit("alice", req("SHOW CATALOGS"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(shown.state, StatementState::Finished);
        // ... and the rest live (queued behind the held slot).
        for _ in 1..MAX_STATEMENTS_PER_USER {
            let s = svc
                .submit("alice", req("SELECT 1"), Duration::ZERO)
                .await
                .unwrap();
            assert!(!s.state.is_terminal());
        }
        // At the cap with one terminal statement: it is evicted.
        svc.submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        let err = svc
            .status("alice", &shown.id, Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
        // Now all 32 are live: overloaded.
        let err = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::Overloaded);
    }

    #[tokio::test]
    async fn total_cap_evicts_any_users_terminal_statement() {
        let admission = AdmissionController::new(1, 2000);
        let (svc, _) = service();
        let svc = svc.with_admission(Some(admission.clone()));
        let _held = admission.admit("held").unwrap().wait().await;
        let shown = svc
            .submit("user0", req("SHOW CATALOGS"), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(shown.state, StatementState::Finished);
        for i in 1..MAX_STATEMENTS_TOTAL {
            let user = format!("user{}", i / MAX_STATEMENTS_PER_USER);
            svc.submit(&user, req("SELECT 1"), Duration::ZERO)
                .await
                .unwrap();
        }
        svc.submit("newcomer", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        let err = svc
            .status("user0", &shown.id, Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
        let err = svc
            .submit("other", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::Overloaded);
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_running_statement_is_cancelled_and_releases_permit() {
        let admission = AdmissionController::new(1, 10);
        let (svc, tracker) = service_with(AgentLimits {
            idle_timeout: Duration::from_secs(1),
            ..AgentLimits::default()
        });
        let (svc, dropped) = hanging(svc);
        let svc = svc.with_admission(Some(admission.clone()));
        let _reaper = svc.spawn_reaper();
        let s = svc
            .submit("alice", req(HANGING_SQL), Duration::ZERO)
            .await
            .unwrap();
        wait_for_state(&svc, &s.id, StatementState::Running).await;
        assert_eq!(admission.snapshot().running, 1);

        tokio::time::sleep(Duration::from_secs(2)).await;
        yield_many().await;

        assert_eq!(admission.snapshot().running, 0);
        assert!(dropped.load(Ordering::SeqCst));
        let info = tracker.list_queries(None).pop().unwrap();
        assert_eq!(info.state, QueryState::Cancelled);
        // Kept for another idle period so the agent can learn why.
        let st = svc.status("alice", &s.id, Duration::ZERO).await.unwrap();
        assert_eq!(st.state, StatementState::Cancelled);
        let err = st.error.unwrap();
        assert_eq!(err.code, AgentErrorCode::Cancelled);
        assert!(err.message.contains("abandoned"), "{}", err.message);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_finished_statement_is_dropped() {
        let (svc, _) = service_with(AgentLimits {
            idle_timeout: Duration::from_secs(1),
            ..AgentLimits::default()
        });
        let _reaper = svc.spawn_reaper();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::ZERO)
            .await
            .unwrap();
        wait_for_state(&svc, &s.id, StatementState::Finished).await;

        tokio::time::sleep(Duration::from_secs(2)).await;
        yield_many().await;

        let err = svc
            .status("alice", &s.id, Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.code, AgentErrorCode::NotFound);
    }

    #[tokio::test(start_paused = true)]
    async fn polled_statement_is_not_reaped() {
        let (svc, _) = service_with(AgentLimits {
            idle_timeout: Duration::from_secs(1),
            ..AgentLimits::default()
        });
        let (svc, _) = hanging(svc);
        let _reaper = svc.spawn_reaper();
        let s = svc
            .submit("alice", req(HANGING_SQL), Duration::ZERO)
            .await
            .unwrap();
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let st = svc.status("alice", &s.id, Duration::ZERO).await.unwrap();
            assert!(!st.state.is_terminal(), "{:?}", st.state);
        }
    }

    #[tokio::test]
    async fn statement_id_is_the_tracker_query_id() {
        let (svc, tracker) = service();
        let s = svc
            .submit("alice", req("SELECT 1"), Duration::from_secs(5))
            .await
            .unwrap();
        let queries = tracker.list_queries(None);
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].query_id.to_string(), s.id);
        assert_eq!(queries[0].source, "api");
        assert_eq!(queries[0].user.as_deref(), Some("alice"));
        assert_eq!(queries[0].rows, Some(1));
    }
}
