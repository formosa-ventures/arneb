//! Query tracker: manages active queries.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use trino_common::error::ExecutionError;
use trino_common::identifiers::QueryId;

use crate::state::{QueryState, QueryStateMachine};

/// Thread-safe query tracker managing all active and completed queries.
#[derive(Clone)]
pub struct QueryTracker {
    queries: Arc<RwLock<HashMap<QueryId, QueryStateMachine>>>,
}

impl QueryTracker {
    /// Creates an empty tracker.
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new query and returns its ID.
    pub fn create_query(&self, sql: String) -> QueryId {
        let id = QueryId::new();
        let qsm = QueryStateMachine::new(id, sql);
        self.queries.write().unwrap().insert(id, qsm);
        id
    }

    /// Lists all queries, optionally filtered by state.
    pub fn list_queries(&self, state_filter: Option<QueryState>) -> Vec<QueryInfo> {
        let queries = self.queries.read().unwrap();
        queries
            .values()
            .filter(|q| state_filter.is_none_or(|s| q.state == s))
            .map(|q| QueryInfo {
                query_id: q.query_id,
                state: q.state,
                sql: q.sql.clone(),
                error: q.error.clone(),
            })
            .collect()
    }

    /// Gets info about a specific query.
    pub fn get_query(&self, id: &QueryId) -> Option<QueryInfo> {
        let queries = self.queries.read().unwrap();
        queries.get(id).map(|q| QueryInfo {
            query_id: q.query_id,
            state: q.state,
            sql: q.sql.clone(),
            error: q.error.clone(),
        })
    }

    /// Transitions a query to a new state.
    pub fn transition_query(&self, id: &QueryId, state: QueryState) -> Result<(), ExecutionError> {
        let mut queries = self.queries.write().unwrap();
        let qsm = queries
            .get_mut(id)
            .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
        qsm.transition(state)
    }

    /// Cancels a query.
    pub fn cancel_query(&self, id: &QueryId) -> Result<(), ExecutionError> {
        let mut queries = self.queries.write().unwrap();
        let qsm = queries
            .get_mut(id)
            .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
        qsm.cancel()
    }

    /// Fails a query with an error message.
    pub fn fail_query(&self, id: &QueryId, error: String) -> Result<(), ExecutionError> {
        let mut queries = self.queries.write().unwrap();
        let qsm = queries
            .get_mut(id)
            .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
        qsm.fail(error)
    }

    /// Returns the number of active (non-terminal) queries.
    pub fn active_count(&self) -> usize {
        let queries = self.queries.read().unwrap();
        queries.values().filter(|q| !q.state.is_terminal()).count()
    }
}

impl Default for QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only snapshot of a query's state.
#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub query_id: QueryId,
    pub state: QueryState,
    pub sql: String,
    pub error: Option<String>,
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
}
