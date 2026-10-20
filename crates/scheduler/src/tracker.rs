//! Query tracker: active queries plus a bounded history of finished ones.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::SystemTime;

use arneb_common::error::ExecutionError;
use arneb_common::identifiers::QueryId;

use crate::state::{QueryState, QueryStateMachine};

/// Default for `ARNEB_QUERY_HISTORY_SIZE`.
pub const DEFAULT_QUERY_HISTORY_SIZE: usize = 100;

/// How many finished (terminal) queries a tracker keeps:
/// `ARNEB_QUERY_HISTORY_SIZE`, read and logged once.
pub fn query_history_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        let raw = std::env::var("ARNEB_QUERY_HISTORY_SIZE").ok();
        let size = match raw.as_deref().map(str::trim) {
            None => DEFAULT_QUERY_HISTORY_SIZE,
            Some(v) => v.parse::<usize>().unwrap_or_else(|_| {
                tracing::warn!(
                    target: "arneb::config",
                    value = v,
                    "invalid ARNEB_QUERY_HISTORY_SIZE, using the default"
                );
                DEFAULT_QUERY_HISTORY_SIZE
            }),
        };
        tracing::info!(
            target: "arneb::config",
            query_history_size = size,
            "ARNEB_QUERY_HISTORY_SIZE effective value (finished queries kept)"
        );
        size
    })
}

struct Entry {
    machine: QueryStateMachine,
    source: String,
    user: Option<String>,
    rows: Option<u64>,
}

struct Inner {
    queries: HashMap<QueryId, Entry>,
    /// Terminal queries, oldest first, for eviction.
    finished: VecDeque<QueryId>,
}

/// Tracks running queries and keeps the most recent `history_limit`
/// finished ones. A query is never evicted while it is still active.
#[derive(Clone)]
pub struct QueryTracker {
    inner: Arc<RwLock<Inner>>,
    history_limit: usize,
}

impl QueryTracker {
    /// A tracker keeping [`query_history_size`] finished queries.
    pub fn new() -> Self {
        Self::with_history_limit(query_history_size())
    }

    /// A tracker keeping at most `limit` finished queries.
    pub fn with_history_limit(limit: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                queries: HashMap::new(),
                finished: VecDeque::new(),
            })),
            history_limit: limit,
        }
    }

    /// Register a new query in `Queued` (source `unknown`, no user).
    pub fn create_query(&self, sql: String) -> QueryId {
        self.create_query_from(sql, "unknown", None)
    }

    /// Register a new query in `Queued`, recording which protocol (`source`)
    /// and user submitted it.
    pub fn create_query_from(&self, sql: String, source: &str, user: Option<String>) -> QueryId {
        let id = QueryId::new();
        self.inner.write().unwrap().queries.insert(
            id,
            Entry {
                machine: QueryStateMachine::new(id, sql),
                source: source.to_string(),
                user,
                rows: None,
            },
        );
        id
    }

    /// All tracked queries, newest first, optionally filtered by state.
    pub fn list_queries(&self, state_filter: Option<QueryState>) -> Vec<QueryInfo> {
        let inner = self.inner.read().unwrap();
        let mut out: Vec<QueryInfo> = inner
            .queries
            .values()
            .filter(|e| state_filter.is_none_or(|s| e.machine.state == s))
            .map(QueryInfo::from)
            .collect();
        out.sort_by(|a, b| b.created.cmp(&a.created));
        out
    }

    pub fn get_query(&self, id: &QueryId) -> Option<QueryInfo> {
        self.inner
            .read()
            .unwrap()
            .queries
            .get(id)
            .map(QueryInfo::from)
    }

    pub fn transition_query(&self, id: &QueryId, state: QueryState) -> Result<(), ExecutionError> {
        self.update(id, |m| m.transition(state))
    }

    /// Step through the lifecycle (`Queued → Planning → Starting → Running
    /// → Finishing → Finished`) until `target`; a no-op when the query is
    /// already there or past it.
    pub fn advance_to(&self, id: &QueryId, target: QueryState) -> Result<(), ExecutionError> {
        const CHAIN: [QueryState; 6] = [
            QueryState::Queued,
            QueryState::Planning,
            QueryState::Starting,
            QueryState::Running,
            QueryState::Finishing,
            QueryState::Finished,
        ];
        let Some(goal) = CHAIN.iter().position(|s| *s == target) else {
            return self.transition_query(id, target);
        };
        self.update(id, |m| {
            let Some(mut at) = CHAIN.iter().position(|s| *s == m.state) else {
                return Ok(());
            };
            while at < goal {
                at += 1;
                m.transition(CHAIN[at])?;
            }
            Ok(())
        })
    }

    /// The query returned `rows` rows: record them and mark it `Finished`.
    pub fn finish_query(&self, id: &QueryId, rows: u64) -> Result<(), ExecutionError> {
        if let Some(entry) = self.inner.write().unwrap().queries.get_mut(id) {
            entry.rows = Some(rows);
        }
        self.advance_to(id, QueryState::Finished)
    }

    pub fn cancel_query(&self, id: &QueryId) -> Result<(), ExecutionError> {
        self.update(id, |m| m.cancel())
    }

    pub fn fail_query(&self, id: &QueryId, error: String) -> Result<(), ExecutionError> {
        self.update(id, |m| m.fail(error))
    }

    pub fn active_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .queries
            .values()
            .filter(|e| !e.machine.state.is_terminal())
            .count()
    }

    /// Apply `f` to a query's state machine; when that makes it terminal,
    /// add it to the history and evict the oldest finished queries over the
    /// limit.
    fn update(
        &self,
        id: &QueryId,
        f: impl FnOnce(&mut QueryStateMachine) -> Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let mut inner = self.inner.write().unwrap();
        let entry = inner
            .queries
            .get_mut(id)
            .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
        let was_terminal = entry.machine.state.is_terminal();
        f(&mut entry.machine)?;
        if !was_terminal && entry.machine.state.is_terminal() {
            inner.finished.push_back(*id);
            while inner.finished.len() > self.history_limit {
                if let Some(old) = inner.finished.pop_front() {
                    inner.queries.remove(&old);
                }
            }
        }
        Ok(())
    }
}

impl Default for QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// A snapshot of one tracked query.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub query_id: QueryId,
    pub state: QueryState,
    pub sql: String,
    pub error: Option<String>,
    /// Protocol that submitted it (`pgwire`, `trino`, …).
    pub source: String,
    /// Authenticated (or client-declared) user, when known.
    pub user: Option<String>,
    pub created: SystemTime,
    pub started: Option<SystemTime>,
    pub ended: Option<SystemTime>,
    /// Rows returned, once finished.
    pub rows: Option<u64>,
}

impl From<&Entry> for QueryInfo {
    fn from(e: &Entry) -> Self {
        let m = &e.machine;
        Self {
            query_id: m.query_id,
            state: m.state,
            sql: m.sql.clone(),
            error: m.error.clone(),
            source: e.source.clone(),
            user: e.user.clone(),
            created: m.created,
            started: m.started,
            ended: m.ended,
            rows: e.rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_list_queries() {
        let tracker = QueryTracker::new();
        let id1 = tracker.create_query("SELECT 1".into());
        let id2 = tracker.create_query("SELECT 2".into());

        let all = tracker.list_queries(None);
        assert_eq!(all.len(), 2);

        let queued = tracker.list_queries(Some(QueryState::Queued));
        assert_eq!(queued.len(), 2);

        let running = tracker.list_queries(Some(QueryState::Running));
        assert_eq!(running.len(), 0);

        // Transition one to Planning.
        tracker
            .transition_query(&id1, QueryState::Planning)
            .unwrap();
        let planning = tracker.list_queries(Some(QueryState::Planning));
        assert_eq!(planning.len(), 1);
        assert_eq!(planning[0].query_id, id1);

        assert!(tracker.get_query(&id2).is_some());
    }

    #[test]
    fn cancel_query() {
        let tracker = QueryTracker::new();
        let id = tracker.create_query("SELECT 1".into());
        tracker.transition_query(&id, QueryState::Planning).unwrap();
        tracker.cancel_query(&id).unwrap();

        let info = tracker.get_query(&id).unwrap();
        assert_eq!(info.state, QueryState::Cancelled);
    }

    #[test]
    fn active_count() {
        let tracker = QueryTracker::new();
        let id1 = tracker.create_query("SELECT 1".into());
        let _id2 = tracker.create_query("SELECT 2".into());
        assert_eq!(tracker.active_count(), 2);

        tracker
            .transition_query(&id1, QueryState::Planning)
            .unwrap();
        tracker.fail_query(&id1, "error".into()).unwrap();
        assert_eq!(tracker.active_count(), 1);
    }

    #[test]
    fn finished_queries_are_bounded_and_active_ones_are_kept() {
        let tracker = QueryTracker::with_history_limit(2);
        let active = tracker.create_query_from("SELECT 0".into(), "pgwire", Some("ann".into()));
        let ids: Vec<_> = (1..=4)
            .map(|i| {
                let id = tracker.create_query_from(format!("SELECT {i}"), "trino", None);
                tracker.finish_query(&id, i).unwrap();
                id
            })
            .collect();
        // The two oldest finished queries are gone; the active one stays.
        assert!(tracker.get_query(&ids[0]).is_none());
        assert!(tracker.get_query(&ids[1]).is_none());
        assert_eq!(tracker.get_query(&ids[3]).unwrap().rows, Some(4));
        let kept = tracker.get_query(&active).unwrap();
        assert_eq!(
            (kept.source.as_str(), kept.user.as_deref()),
            ("pgwire", Some("ann"))
        );
        assert_eq!(tracker.list_queries(None).len(), 3);
    }

    #[test]
    fn advance_to_walks_the_lifecycle_and_stamps_times() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let running = tracker.get_query(&id).unwrap();
        assert_eq!(running.state, QueryState::Running);
        assert!(running.started.is_some() && running.ended.is_none());
        // Already past Planning: a no-op.
        tracker.advance_to(&id, QueryState::Planning).unwrap();
        tracker.finish_query(&id, 7).unwrap();
        let done = tracker.get_query(&id).unwrap();
        assert_eq!(done.state, QueryState::Finished);
        assert!(done.ended.unwrap() >= done.started.unwrap());
        assert!(done.started.unwrap() >= done.created);
    }

    #[test]
    fn list_is_newest_first() {
        let tracker = QueryTracker::with_history_limit(10);
        let first = tracker.create_query("SELECT 1".into());
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = tracker.create_query("SELECT 2".into());
        let ids: Vec<_> = tracker
            .list_queries(None)
            .into_iter()
            .map(|q| q.query_id)
            .collect();
        assert_eq!(ids, vec![second, first]);
    }
}
