//! Query tracker: active queries plus a bounded history of finished ones.

use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use arneb_common::error::ExecutionError;
use arneb_common::identifiers::QueryId;
use arneb_common::metrics::{self, QueryEnd, QueryOutcome};

use crate::state::{QueryState, QueryStateMachine};

/// Default number of finished queries a tracker keeps
/// (`[observability] query_history_size`).
pub const DEFAULT_QUERY_HISTORY_SIZE: usize = 100;

/// The non-terminal lifecycle, in order, ending in `Finished`.
const CHAIN: [QueryState; 6] = [
    QueryState::Queued,
    QueryState::Planning,
    QueryState::Starting,
    QueryState::Running,
    QueryState::Finishing,
    QueryState::Finished,
];

/// Step `m` through [`CHAIN`] until `target`; a no-op when it is already
/// there or past it. A query that already failed or was cancelled cannot
/// advance.
fn advance_machine(m: &mut QueryStateMachine, target: QueryState) -> Result<(), ExecutionError> {
    let Some(goal) = CHAIN.iter().position(|s| *s == target) else {
        return m.transition(target);
    };
    let Some(mut at) = CHAIN.iter().position(|s| *s == m.state) else {
        return Err(ExecutionError::InvalidOperation(format!(
            "query {} is already {}",
            m.query_id, m.state
        )));
    };
    while at < goal {
        at += 1;
        m.transition(CHAIN[at])?;
    }
    Ok(())
}

/// Stops a running query on the protocol that runs it. The argument is the
/// name of the user who killed it. Called at most once, never under the
/// tracker's lock, so it may call back into the tracker.
pub type Canceller = Box<dyn FnOnce(&str) + Send + Sync>;

/// What [`QueryTracker::kill_query`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    /// The query was stopped; `was` is its state before the kill.
    Killed { was: QueryState },
    /// The query had already ended (or ended on its own during the kill)
    /// in this state; nothing was changed.
    AlreadyEnded(QueryState),
}

struct Entry {
    machine: QueryStateMachine,
    source: String,
    user: Option<String>,
    rows: Option<u64>,
    /// Dropped (uncalled) when the query turns terminal.
    canceller: Option<Canceller>,
    /// Who killed it through [`QueryTracker::kill_query`].
    killed_by: Option<String>,
}

struct Inner {
    queries: HashMap<QueryId, Entry>,
    /// Terminal queries, oldest first, for eviction.
    finished: VecDeque<QueryId>,
}

/// Tracks running queries and keeps the most recent `history_limit`
/// finished ones. A query is never evicted while it is still active.
///
/// The tracker owns each statement's lifecycle, so it also feeds the
/// process-wide query metrics: a query counts as running once it reaches
/// `Running`, and by outcome and duration when it turns terminal.
#[derive(Clone)]
pub struct QueryTracker {
    inner: Arc<RwLock<Inner>>,
    history_limit: usize,
}

impl QueryTracker {
    /// A tracker keeping [`DEFAULT_QUERY_HISTORY_SIZE`] finished queries.
    pub fn new() -> Self {
        Self::with_history_limit(DEFAULT_QUERY_HISTORY_SIZE)
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
                canceller: None,
                killed_by: None,
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
        out.sort_by_key(|q| Reverse(q.created));
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
        self.update(id, |e| e.machine.transition(state))
    }

    /// Step through the lifecycle (`Queued → Planning → Starting → Running
    /// → Finishing → Finished`) until `target`; a no-op when the query is
    /// already there or past it.
    pub fn advance_to(&self, id: &QueryId, target: QueryState) -> Result<(), ExecutionError> {
        self.update(id, |e| advance_machine(&mut e.machine, target))
    }

    /// The query returned `rows` rows: record them and mark it `Finished`.
    pub fn finish_query(&self, id: &QueryId, rows: u64) -> Result<(), ExecutionError> {
        self.update(id, |e| {
            advance_machine(&mut e.machine, QueryState::Finished)?;
            e.rows = Some(rows);
            Ok(())
        })
    }

    pub fn cancel_query(&self, id: &QueryId) -> Result<(), ExecutionError> {
        self.update(id, |e| e.machine.cancel())
    }

    pub fn fail_query(&self, id: &QueryId, error: String) -> Result<(), ExecutionError> {
        self.update(id, |e| e.machine.fail(error))
    }

    /// Register how to stop query `id` on the protocol running it. A query
    /// already cancelled (killed before its protocol registered) has the
    /// canceller called at once, outside the lock, with the killer's name
    /// (`the server` when it was not killed by a user). Dropped without
    /// being called when the query is unknown or ended otherwise.
    pub fn set_canceller(&self, id: &QueryId, canceller: Canceller) {
        let mut inner = self.inner.write().unwrap();
        let killed_by = match inner.queries.get_mut(id) {
            Some(e) if !e.machine.state.is_terminal() => {
                e.canceller = Some(canceller);
                return;
            }
            Some(e) if e.machine.state == QueryState::Cancelled => e
                .killed_by
                .clone()
                .unwrap_or_else(|| "the server".to_owned()),
            _ => {
                drop(inner);
                drop(canceller);
                return;
            }
        };
        drop(inner);
        canceller(&killed_by);
    }

    /// Stop query `id` on behalf of user `by`: call its canceller (outside
    /// the lock) and mark it cancelled. A query that already ended, or ends
    /// on its own during the kill, is [`KillOutcome::AlreadyEnded`]. The
    /// first kill claims the query under the lock; a concurrent kill of the
    /// same query is `AlreadyEnded(Cancelled)`, so only one reports a kill.
    pub fn kill_query(&self, id: &QueryId, by: &str) -> Result<KillOutcome, ExecutionError> {
        let (was, canceller) = {
            let mut inner = self.inner.write().unwrap();
            let entry = inner
                .queries
                .get_mut(id)
                .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
            let was = entry.machine.state;
            if was.is_terminal() {
                return Ok(KillOutcome::AlreadyEnded(was));
            }
            if entry.killed_by.is_some() {
                // Another kill claimed it and is stopping it now.
                return Ok(KillOutcome::AlreadyEnded(QueryState::Cancelled));
            }
            entry.killed_by = Some(by.to_owned());
            (was, entry.canceller.take())
        };
        if let Some(cancel) = canceller {
            cancel(by);
        }
        if self.cancel_query(id).is_ok() {
            return Ok(KillOutcome::Killed { was });
        }
        // Already terminal: cancelled by the canceller itself (or by the
        // server), or it ended on its own meanwhile.
        Ok(match self.get_query(id).map(|q| q.state) {
            Some(state) if state != QueryState::Cancelled => KillOutcome::AlreadyEnded(state),
            _ => KillOutcome::Killed { was },
        })
    }

    pub fn active_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .queries
            .values()
            .filter(|e| !e.machine.state.is_terminal())
            .count()
    }

    /// Apply `f` to a query's entry and report what changed to the
    /// metrics. When that makes it terminal, add it to the history and
    /// evict the oldest finished queries over the limit.
    fn update(
        &self,
        id: &QueryId,
        f: impl FnOnce(&mut Entry) -> Result<(), ExecutionError>,
    ) -> Result<(), ExecutionError> {
        let mut inner = self.inner.write().unwrap();
        let entry = inner
            .queries
            .get_mut(id)
            .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
        let was_terminal = entry.machine.state.is_terminal();
        // A terminal query's canceller is dropped after the lock is released.
        let mut dropped_canceller = None;
        let was_started = entry.machine.started.is_some();
        // A failed step may still have moved the machine; report either way.
        let result = f(entry);
        let m = &entry.machine;
        if !was_started && m.started.is_some() {
            metrics::query_started();
        }
        let ended = !was_terminal && m.state.is_terminal();
        if ended {
            report_end(entry);
            dropped_canceller = entry.canceller.take();
        }
        if let Err(e) = result {
            drop(inner);
            drop(dropped_canceller);
            return Err(e);
        }
        if ended {
            inner.finished.push_back(*id);
            while inner.finished.len() > self.history_limit {
                if let Some(old) = inner.finished.pop_front() {
                    inner.queries.remove(&old);
                }
            }
        }
        drop(inner);
        drop(dropped_canceller);
        Ok(())
    }
}

fn report_end(entry: &Entry) {
    let m = &entry.machine;
    let outcome = match m.state {
        QueryState::Finished => QueryOutcome::Finished,
        QueryState::Failed => QueryOutcome::Failed,
        _ => QueryOutcome::Cancelled,
    };
    let from = m.started.unwrap_or(m.created);
    let elapsed = m
        .ended
        .and_then(|end| end.duration_since(from).ok())
        .unwrap_or(Duration::ZERO);
    metrics::query_ended(QueryEnd {
        outcome,
        elapsed,
        was_running: m.started.is_some(),
        rows: entry.rows,
        error: m.error.as_deref(),
        sql: &m.sql,
    });
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

    /// The tracker feeds `/metrics`. The registry is process-wide and tests
    /// run in parallel, so this checks lower bounds.
    #[test]
    fn terminal_transitions_are_counted_by_outcome() {
        fn total(outcome: &str) -> f64 {
            let series = format!("arneb_queries_total{{outcome=\"{outcome}\"}} ");
            arneb_common::metrics::render(&[])
                .lines()
                .find_map(|l| l.strip_prefix(series.as_str()).map(|v| v.parse().unwrap()))
                .unwrap()
        }
        let before = ["finished", "failed", "cancelled"].map(total);
        let tracker = QueryTracker::with_history_limit(10);
        let done = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&done, QueryState::Running).unwrap();
        tracker.finish_query(&done, 1).unwrap();
        // Refused before admission: counted, never running.
        let refused = tracker.create_query("SELECT 2".into());
        tracker.fail_query(&refused, "queue full".into()).unwrap();
        let cancelled = tracker.create_query("SELECT 3".into());
        tracker.advance_to(&cancelled, QueryState::Running).unwrap();
        tracker.cancel_query(&cancelled).unwrap();
        // A second terminal transition is rejected and not counted again.
        assert!(tracker.finish_query(&cancelled, 5).is_err());
        let after = ["finished", "failed", "cancelled"].map(total);
        for i in 0..3 {
            assert!(after[i] >= before[i] + 1.0, "{i}: {before:?} -> {after:?}");
        }
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

    #[test]
    fn kill_calls_the_canceller_once_and_marks_cancelled() {
        use std::sync::Mutex;
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &id,
            Box::new(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(*calls.lock().unwrap(), ["root"]);
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Cancelled)
        );
        assert_eq!(calls.lock().unwrap().len(), 1, "called once");
        let missing = QueryId::new();
        assert!(tracker.kill_query(&missing, "root").is_err());
    }

    /// Two kills of one query at once: the one that claims it reports the
    /// kill; the other, landing while the canceller runs, already ended.
    #[test]
    fn concurrent_kills_report_one_kill() {
        use std::sync::mpsc;
        let tracker = Arc::new(QueryTracker::with_history_limit(10));
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let (inside_tx, inside_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = std::sync::Mutex::new(release_rx);
        tracker.set_canceller(
            &id,
            Box::new(move |_| {
                inside_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }),
        );
        let first = std::thread::spawn({
            let tracker = Arc::clone(&tracker);
            move || tracker.kill_query(&id, "root").unwrap()
        });
        inside_rx.recv().unwrap();
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Running);
        assert_eq!(
            tracker.kill_query(&id, "ops").unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Cancelled)
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            first.join().unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
    }

    #[test]
    fn terminal_query_drops_its_canceller() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct DropGuard(Arc<AtomicBool>);
        impl Drop for DropGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        let (dropped, called) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let guard = DropGuard(Arc::clone(&dropped));
        let flag = Arc::clone(&called);
        tracker.set_canceller(
            &id,
            Box::new(move |_| {
                let _guard = &guard;
                flag.store(true, Ordering::SeqCst);
            }),
        );
        assert!(!dropped.load(Ordering::SeqCst));
        tracker.finish_query(&id, 3).unwrap();
        assert!(
            dropped.load(Ordering::SeqCst),
            "dropped at the terminal turn"
        );
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Finished)
        );
        assert!(!called.load(Ordering::SeqCst), "never called");
        // A canceller set on a terminal query is dropped at once.
        let late = Arc::new(AtomicBool::new(false));
        let guard = DropGuard(Arc::clone(&late));
        tracker.set_canceller(&id, Box::new(move |_| drop(guard)));
        assert!(late.load(Ordering::SeqCst));
    }

    #[test]
    fn kill_before_registration_runs_the_canceller_on_registration() {
        use std::sync::Mutex;
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::Killed {
                was: QueryState::Queued
            }
        );
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &id,
            Box::new(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(*calls.lock().unwrap(), ["root"], "run on registration");
        // Cancelled without a kill: the canceller still runs, by the server.
        let other = tracker.create_query("SELECT 2".into());
        tracker.cancel_query(&other).unwrap();
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &other,
            Box::new(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(*calls.lock().unwrap(), ["root", "the server"]);
    }

    #[test]
    fn query_ending_on_its_own_during_the_kill_already_ended() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let inner = tracker.clone();
        tracker.set_canceller(&id, Box::new(move |_| inner.finish_query(&id, 1).unwrap()));
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Finished)
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Finished);
    }

    #[test]
    fn canceller_may_reenter_the_tracker() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let inner = tracker.clone();
        tracker.set_canceller(
            &id,
            Box::new(move |_| {
                inner.cancel_query(&id).unwrap();
            }),
        );
        assert_eq!(
            tracker.kill_query(&id, "root").unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
    }
}
