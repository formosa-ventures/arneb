//! Query tracker: active queries plus a bounded history of finished ones.

use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::Poll;
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

/// Whether a [`Canceller`] stopped its query: `false` when the query's work
/// had already completed, so the kill stopped nothing (see
/// [`crate::KillSwitch`]). A [`crate::KillSwitch`] canceller resolves it
/// once a poll of the work in flight ends; it never blocks a thread.
pub type CancelFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

/// Stops a running query on the protocol that runs it. The argument is the
/// name of the user who killed it; the kill is requested when it is called,
/// and the returned [`CancelFuture`] says whether it was applied. Called at
/// most once, never under the tracker's lock, so it may call back into the
/// tracker.
pub type Canceller = Box<dyn FnOnce(&str) -> CancelFuture + Send + Sync>;

/// What [`QueryTracker::kill_query`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillOutcome {
    /// The query was stopped; `was` is its state before the kill.
    Killed { was: QueryState },
    /// The query had already ended (or ended on its own during the kill)
    /// in this state; nothing was changed.
    AlreadyEnded(QueryState),
    /// The query's work had already completed when the kill landed, but its
    /// protocol had not yet recorded the outcome: nothing was stopped and
    /// its result stands. `state` is its (still non-terminal) state then.
    NotApplied { state: QueryState },
}

/// The first kill's outcome, shared with kills of the same query that
/// arrive while it is being settled.
type KillWatch = tokio::sync::watch::Receiver<Option<Settled>>;

/// How the first kill of a query settled.
#[derive(Debug, Clone, Copy)]
enum Settled {
    Outcome(KillOutcome),
    /// The query left the history meanwhile: its state is unknown.
    Gone,
}

/// How to stop a query, as kept on its entry: its canceller, until a kill
/// calls it, then the future of that call, until it resolves. A kill that
/// ends without an outcome hands back whichever it held, so a later kill
/// asks the same canceller (or awaits the same call) whether the work was
/// stopped, instead of assuming it was.
enum Cancel {
    Uncalled(Canceller),
    /// In a mutex only so the entry stays `Sync`; polled through
    /// `get_mut`, never locked.
    Called(std::sync::Mutex<CancelFuture>),
}

impl Cancel {
    /// Whether this stopped the query's work, calling the canceller first
    /// when it has not been called yet. Resolves once.
    fn poll_applied(&mut self, by: &str, cx: &mut std::task::Context<'_>) -> Poll<bool> {
        if let Self::Uncalled(_) = self {
            let pending: CancelFuture = Box::pin(std::future::pending());
            let Self::Uncalled(canceller) =
                std::mem::replace(self, Self::Called(std::sync::Mutex::new(pending)))
            else {
                unreachable!("checked above")
            };
            *self = Self::Called(std::sync::Mutex::new(canceller(by)));
        }
        match self {
            Self::Called(future) => future
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_mut()
                .poll(cx),
            Self::Uncalled(_) => unreachable!("called above"),
        }
    }
}

/// The first kill's claim on a query while its task settles it. Dropped
/// without publishing (the runtime shut down mid-kill, or the canceller
/// panicked), it releases the claim, so a later kill can try again instead
/// of waiting on a kill that will never settle; waiting kills then fail.
/// It also hands the query's [`Cancel`] back to the entry, so the later
/// kill learns from it whether this one already stopped the work (a kill
/// interrupted while it waited on the poll that completed a write must not
/// let a retry report that write cancelled).
///
/// Never dropped under the tracker's lock: its drop takes it.
struct Claim {
    tracker: QueryTracker,
    id: QueryId,
    tx: Option<tokio::sync::watch::Sender<Option<Settled>>>,
    /// Taken from the entry by the kill; `None` when nothing was registered
    /// yet, or once it resolved (or panicked).
    cancel: Option<Cancel>,
}

impl Claim {
    fn publish(mut self, settled: Settled) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(Some(settled));
        }
    }

    /// Whether the kill stopped the query's work: asks its [`Cancel`], or
    /// `true` when the query had none (its protocol has not registered one
    /// yet: it calls it on registration, see
    /// [`QueryTracker::set_canceller`]). A canceller that panics is dropped
    /// (a [`crate::KillSwitch`] canceller can panic only in its `on_kill`,
    /// after the kill was applied) and the panic continues.
    async fn applied(&mut self, by: &str) -> bool {
        std::future::poll_fn(|cx| {
            let Some(cancel) = self.cancel.as_mut() else {
                return Poll::Ready(true);
            };
            let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cancel.poll_applied(by, cx)
            }));
            match polled {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(applied)) => {
                    self.cancel = None;
                    Poll::Ready(applied)
                }
                Err(panic) => {
                    self.cancel = None;
                    std::panic::resume_unwind(panic)
                }
            }
        })
        .await
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if self.tx.is_none() {
            return;
        }
        tracing::warn!(query_id = %self.id, "a kill ended without an outcome; releasing its claim");
        let mut cancel = self.cancel.take();
        {
            let mut inner = self
                .tracker
                .inner
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(e) = inner.queries.get_mut(&self.id) {
                e.killed_by = None;
                e.kill = None;
                if !e.machine.state.is_terminal() && e.cancel.is_none() {
                    e.cancel = cancel.take();
                }
            }
        }
        // Not handed back (the query ended or left): dropped outside the
        // lock, since dropping a canceller may call back into the tracker.
        drop(cancel);
    }
}

struct Entry {
    machine: QueryStateMachine,
    source: String,
    user: Option<String>,
    rows: Option<u64>,
    /// How to stop it; dropped when the query turns terminal.
    cancel: Option<Cancel>,
    /// Who killed it through [`QueryTracker::kill_query`].
    killed_by: Option<String>,
    /// The first kill's outcome, once a kill claimed the query; kept after a
    /// kill that was not applied, so a later kill reports the same.
    kill: Option<KillWatch>,
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
                cancel: None,
                killed_by: None,
                kill: None,
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

    /// Who cancelled query `id`, once it is `Cancelled`: the killer's name
    /// ([`Self::kill_query`]), or `the server`. `None` while it has not
    /// been cancelled (or is unknown).
    ///
    /// A protocol whose statement ends after a kill marked it cancelled
    /// (the kill landed before its canceller was registered) checks this
    /// when [`Self::finish_query`] fails, and reports the statement
    /// cancelled instead of returning its rows.
    pub fn cancelled_by(&self, id: &QueryId) -> Option<String> {
        let inner = self.inner.read().unwrap();
        let e = inner.queries.get(id)?;
        (e.machine.state == QueryState::Cancelled).then(|| {
            e.killed_by
                .clone()
                .unwrap_or_else(|| "the server".to_owned())
        })
    }

    /// The query returned `rows` rows: record them and mark it `Finished`.
    /// Fails, changing nothing, when the query already ended (a cancelled
    /// query never turns `Finished`).
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
    /// already cancelled (killed before its protocol registered), or one a
    /// kill has claimed but not yet marked cancelled, has the canceller
    /// called at once, outside the lock, with the killer's name (`the
    /// server` when it was not killed by a user). Dropped without being
    /// called when the query is unknown or ended otherwise.
    pub fn set_canceller(&self, id: &QueryId, canceller: Canceller) {
        let mut inner = self.inner.write().unwrap();
        let killed_by = match inner.queries.get_mut(id) {
            // A kill in progress: stored, it would be dropped uncalled when
            // the kill marks the query cancelled.
            Some(e) if !e.machine.state.is_terminal() && e.killed_by.is_some() => {
                e.killed_by.clone().unwrap_or_default()
            }
            Some(e) if !e.machine.state.is_terminal() => {
                e.cancel = Some(Cancel::Uncalled(canceller));
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
        // The work cannot have completed: it starts only after registering,
        // so a kill switch canceller resolves at its first poll.
        let mut cancel = canceller(&killed_by);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        if cancel.as_mut().poll(&mut cx).is_pending() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(cancel);
            }
        }
    }

    /// Stop query `id` on behalf of user `by`: call its canceller (outside
    /// the lock), wait for whether that stopped the work, and mark it
    /// cancelled. A query that already ended, or ends on its own during the
    /// kill, is [`KillOutcome::AlreadyEnded`]; one whose canceller reports
    /// that its work had already completed is [`KillOutcome::NotApplied`]
    /// with its real state (nothing is marked; the protocol records the
    /// outcome).
    ///
    /// The first kill claims the query under the lock. A kill of the same
    /// query while that one is settled waits for it, then reports what the
    /// query is: `AlreadyEnded` with its terminal state, or `NotApplied`
    /// while the protocol has not yet recorded a completed outcome, so it
    /// never reports a cancel that did not happen.
    ///
    /// A query that leaves the history while the kill settles is not found.
    /// A kill whose task ended without an outcome (the runtime shut down
    /// mid-kill, or its canceller panicked) fails with "interrupted" and
    /// releases its claim, so a later kill can try again; that kill asks
    /// the same canceller (or the same pending call of it) whether the work
    /// was stopped, so it never reports a completed write cancelled.
    ///
    /// The kill is settled in a task of its own, so dropping this future
    /// does not leave it half done. It waits for at most one poll of the
    /// query's work and never blocks a thread.
    pub async fn kill_query(&self, id: &QueryId, by: &str) -> Result<KillOutcome, ExecutionError> {
        let (claim, watch) = {
            let mut inner = self.inner.write().unwrap();
            let entry = inner
                .queries
                .get_mut(id)
                .ok_or_else(|| ExecutionError::InvalidOperation(format!("query {id} not found")))?;
            let was = entry.machine.state;
            if was.is_terminal() {
                return Ok(KillOutcome::AlreadyEnded(was));
            }
            match &entry.kill {
                // Another kill claimed it: share its outcome.
                Some(watch) => (None, watch.clone()),
                None => {
                    entry.killed_by = Some(by.to_owned());
                    let (tx, rx) = tokio::sync::watch::channel(None);
                    entry.kill = Some(rx.clone());
                    let claim = Claim {
                        tracker: self.clone(),
                        id: *id,
                        tx: Some(tx),
                        cancel: entry.cancel.take(),
                    };
                    (Some((claim, was)), rx)
                }
            }
        };
        // Spawned after the lock is released: a runtime that is shutting
        // down drops the task at once, and the claim's drop takes the lock.
        let first = claim.is_some();
        if let Some((claim, was)) = claim {
            tokio::spawn(self.clone().settle_kill(claim, by.to_owned(), was));
        }
        let not_found = || ExecutionError::InvalidOperation(format!("query {id} not found"));
        let outcome = match wait_for_kill(watch).await {
            Some(Settled::Outcome(outcome)) => outcome,
            Some(Settled::Gone) => return Err(not_found()),
            None => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "the kill of query {id} was interrupted"
                )))
            }
        };
        if first {
            return Ok(outcome);
        }
        // What the query is now, after the first kill settled.
        match self.get_query(id).map(|q| q.state) {
            Some(state) if state.is_terminal() => Ok(KillOutcome::AlreadyEnded(state)),
            Some(state) => Ok(KillOutcome::NotApplied { state }),
            None => Err(not_found()),
        }
    }

    /// The claimed kill of `id`: ask its [`Cancel`] whether it stopped the
    /// work, then record and publish the outcome.
    async fn settle_kill(self, mut claim: Claim, by: String, was: QueryState) {
        let id = claim.id;
        let applied = claim.applied(&by).await;
        let outcome = if !applied {
            // Too late: the work completed. Release the claim so the
            // protocol's finish stands; keep the outcome for later kills.
            let mut inner = self.inner.write().unwrap();
            let state = inner.queries.get_mut(&id).map(|e| {
                e.killed_by = None;
                e.machine.state
            });
            match state {
                Some(state) if !state.is_terminal() => KillOutcome::NotApplied { state },
                Some(state) => KillOutcome::AlreadyEnded(state),
                None => {
                    drop(inner);
                    return claim.publish(Settled::Gone);
                }
            }
        } else if self.cancel_query(&id).is_ok() {
            KillOutcome::Killed { was }
        } else {
            // Already terminal: cancelled by the canceller itself (or by
            // the server), or it ended on its own meanwhile.
            match self.get_query(&id).map(|q| q.state) {
                Some(state) if state != QueryState::Cancelled => KillOutcome::AlreadyEnded(state),
                _ => KillOutcome::Killed { was },
            }
        };
        claim.publish(Settled::Outcome(outcome));
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
            dropped_canceller = entry.cancel.take();
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

/// The outcome published on `watch`; `None` when its kill task ended
/// without one (a panicking canceller, or the runtime shutting down).
async fn wait_for_kill(mut watch: KillWatch) -> Option<Settled> {
    let outcome = watch.wait_for(Option::is_some).await.ok()?;
    *outcome
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

    use std::time::Duration;
    use tokio::sync::oneshot;

    /// A canceller that always stops the query (its work never completed).
    fn stopping(f: impl FnOnce(&str) + Send + Sync + 'static) -> Canceller {
        Box::new(move |by| {
            f(by);
            Box::pin(async { true })
        })
    }

    /// A canceller reporting that the work already completed: the kill is
    /// not applied, the claim is released, and the protocol's finish stands.
    #[tokio::test]
    async fn kill_after_the_work_completed_is_not_applied() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("INSERT INTO t SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        tracker.set_canceller(&id, Box::new(|_| Box::pin(async { false })));
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::NotApplied {
                state: QueryState::Running
            }
        );
        // A later kill reports the same, not a cancel.
        assert_eq!(
            tracker.kill_query(&id, "ops").await.unwrap(),
            KillOutcome::NotApplied {
                state: QueryState::Running
            }
        );
        assert_eq!(tracker.cancelled_by(&id), None);
        tracker.finish_query(&id, 1).unwrap();
        let q = tracker.get_query(&id).unwrap();
        assert_eq!(q.state, QueryState::Finished);
        assert_eq!(q.rows, Some(1));
    }

    /// A kill that lands before the protocol registers its canceller: the
    /// query is marked cancelled, cannot then finish, and the canceller
    /// registered afterwards runs at once with the killer's name.
    #[tokio::test]
    async fn kill_before_registration_cannot_finish_and_cancels_on_register() {
        let tracker = QueryTracker::new();
        let id = tracker.create_query_from("SELECT 1".into(), "trino", Some("bob".into()));
        tracker.advance_to(&id, QueryState::Running).unwrap();
        assert_eq!(tracker.cancelled_by(&id), None);
        // 1. The kill: nothing registered, nothing stopped yet.
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        // 2. The task finishes before registering: it cannot report rows.
        assert!(tracker.finish_query(&id, 5).is_err());
        let q = tracker.get_query(&id).unwrap();
        assert_eq!(q.state, QueryState::Cancelled);
        assert_eq!(q.rows, None);
        assert_eq!(tracker.cancelled_by(&id).as_deref(), Some("root"));
        // 3. The registration runs the canceller at once.
        let called = Arc::new(std::sync::Mutex::new(None));
        tracker.set_canceller(
            &id,
            stopping({
                let called = Arc::clone(&called);
                move |by| *called.lock().unwrap() = Some(by.to_owned())
            }),
        );
        assert_eq!(called.lock().unwrap().as_deref(), Some("root"));
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
    }

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

    #[tokio::test]
    async fn kill_calls_the_canceller_once_and_marks_cancelled() {
        use std::sync::Mutex;
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &id,
            stopping(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(*calls.lock().unwrap(), ["root"]);
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Cancelled)
        );
        assert_eq!(calls.lock().unwrap().len(), 1, "called once");
        let missing = QueryId::new();
        assert!(tracker.kill_query(&missing, "root").await.is_err());
    }

    /// A canceller that signals `inside` when called, then waits for
    /// `release` and resolves to `applied`.
    fn gated(applied: bool) -> (Canceller, oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (inside_tx, inside_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let canceller: Canceller = Box::new(move |_| {
            let _ = inside_tx.send(());
            Box::pin(async move {
                let _ = release_rx.await;
                applied
            })
        });
        (canceller, inside_rx, release_tx)
    }

    /// Two kills of one query at once: the one that claims it reports the
    /// kill; the other waits for it, then reports the query cancelled.
    #[tokio::test]
    async fn concurrent_kills_report_one_kill() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let (canceller, inside, release) = gated(true);
        tracker.set_canceller(&id, canceller);
        let first = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "root").await.unwrap() }
        });
        inside.await.unwrap();
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Running);
        let mut second = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "ops").await.unwrap() }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "the second kill waits for the first"
        );
        release.send(()).unwrap();
        assert_eq!(
            first.await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(
            second.await.unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Cancelled)
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
    }

    /// A kill concurrent with one that is not applied (the work completed)
    /// does not report a cancel: it shares that outcome with the query's
    /// real state, which then finishes normally.
    #[tokio::test]
    async fn a_kill_concurrent_with_one_not_applied_is_not_applied() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("INSERT INTO t SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let (canceller, inside, release) = gated(false);
        tracker.set_canceller(&id, canceller);
        let first = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "root").await.unwrap() }
        });
        inside.await.unwrap();
        let second = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "ops").await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        release.send(()).unwrap();
        let running = KillOutcome::NotApplied {
            state: QueryState::Running,
        };
        assert_eq!(first.await.unwrap(), running);
        assert_eq!(second.await.unwrap(), running);
        assert_eq!(tracker.cancelled_by(&id), None);
        tracker.finish_query(&id, 1).unwrap();
        assert_eq!(
            tracker.kill_query(&id, "ops").await.unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Finished)
        );
    }

    /// Dropping a kill's future does not leave the query claimed: the kill
    /// is settled in its own task.
    #[tokio::test]
    async fn a_dropped_kill_is_still_settled() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let (canceller, inside, release) = gated(true);
        tracker.set_canceller(&id, canceller);
        let kill = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "root").await }
        });
        inside.await.unwrap();
        kill.abort();
        release.send(()).unwrap();
        for _ in 0..100 {
            if tracker.get_query(&id).unwrap().state == QueryState::Cancelled {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the dropped kill was never settled");
    }

    /// A kill whose canceller panics fails as interrupted and releases its
    /// claim: the next kill is not stuck behind it.
    #[tokio::test]
    async fn a_kill_whose_canceller_panics_releases_its_claim() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        tracker.set_canceller(
            &id,
            Box::new(|_| Box::pin(async { panic!("canceller panicked") })),
        );
        let err = tracker.kill_query(&id, "root").await.unwrap_err();
        assert!(err.to_string().contains("interrupted"), "{err}");
        assert_eq!(tracker.cancelled_by(&id), None);
        assert_eq!(
            tracker.kill_query(&id, "ops").await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
    }

    /// A canceller registered while a kill is in progress (claimed, not yet
    /// cancelled) is called at once, not stored and then dropped uncalled.
    #[tokio::test]
    async fn canceller_registered_during_a_kill_is_called() {
        use std::sync::Mutex;
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let (canceller, inside, release) = gated(true);
        tracker.set_canceller(&id, canceller);
        let kill = tokio::spawn({
            let tracker = tracker.clone();
            async move { tracker.kill_query(&id, "root").await.unwrap() }
        });
        inside.await.unwrap();
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &id,
            stopping(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(*calls.lock().unwrap(), ["root"], "called during the kill");
        release.send(()).unwrap();
        assert_eq!(
            kill.await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
    }

    #[tokio::test]
    async fn terminal_query_drops_its_canceller() {
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
            stopping(move |_| {
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
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Finished)
        );
        assert!(!called.load(Ordering::SeqCst), "never called");
        // A canceller set on a terminal query is dropped at once.
        let late = Arc::new(AtomicBool::new(false));
        let guard = DropGuard(Arc::clone(&late));
        tracker.set_canceller(&id, stopping(move |_| drop(guard)));
        assert!(late.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn kill_before_registration_runs_the_canceller_on_registration() {
        use std::sync::Mutex;
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Queued
            }
        );
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &id,
            stopping(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(*calls.lock().unwrap(), ["root"], "run on registration");
        // Cancelled without a kill: the canceller still runs, by the server.
        let other = tracker.create_query("SELECT 2".into());
        tracker.cancel_query(&other).unwrap();
        let seen = Arc::clone(&calls);
        tracker.set_canceller(
            &other,
            stopping(move |by| seen.lock().unwrap().push(by.to_owned())),
        );
        assert_eq!(*calls.lock().unwrap(), ["root", "the server"]);
    }

    #[tokio::test]
    async fn query_ending_on_its_own_during_the_kill_already_ended() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let inner = tracker.clone();
        tracker.set_canceller(&id, stopping(move |_| inner.finish_query(&id, 1).unwrap()));
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::AlreadyEnded(QueryState::Finished)
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Finished);
    }

    #[tokio::test]
    async fn canceller_may_reenter_the_tracker() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let inner = tracker.clone();
        tracker.set_canceller(
            &id,
            stopping(move |_| {
                inner.cancel_query(&id).unwrap();
            }),
        );
        assert_eq!(
            tracker.kill_query(&id, "root").await.unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Cancelled);
    }

    /// A current-thread runtime for the tests that shut one down.
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
    }

    /// A kill on a runtime that is shutting down: the runtime drops its
    /// settle task as it is spawned, and the claim's drop takes the
    /// tracker's lock, so the kill must not spawn under that lock (it
    /// deadlocked, or panicked on the re-entrant lock). The kill fails as
    /// interrupted, and the canceller it had not called yet is handed back:
    /// a retry calls it.
    #[test]
    fn a_kill_on_a_closing_runtime_releases_its_claim_without_deadlock() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let called = Arc::new(AtomicBool::new(false));
        tracker.set_canceller(
            &id,
            stopping({
                let called = Arc::clone(&called);
                move |_| called.store(true, Ordering::SeqCst)
            }),
        );
        let closed = runtime();
        let handle = closed.handle().clone();
        drop(closed);
        let (tx, rx) = std::sync::mpsc::channel();
        let t = tracker.clone();
        std::thread::spawn(move || {
            let out = handle.block_on(t.kill_query(&id, "root"));
            let _ = tx.send(out.map_err(|e| e.to_string()));
        });
        let out = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the kill deadlocked (or panicked) on a closing runtime");
        assert!(out.unwrap_err().contains("interrupted"));
        assert!(!called.load(Ordering::SeqCst));
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Running);
        assert_eq!(
            runtime().block_on(tracker.kill_query(&id, "ops")).unwrap(),
            KillOutcome::Killed {
                was: QueryState::Running
            }
        );
        assert!(called.load(Ordering::SeqCst), "the retry called it");
    }

    /// The reviewer's example: a kill's settle task is dropped (the runtime
    /// shut down) while its kill waits on the poll that completes an
    /// INSERT, so the switch ends `Done`. A retried kill must ask that
    /// switch, through the pending call handed back, and report the kill as
    /// not applied, not mark the committed write cancelled.
    #[test]
    fn a_retry_after_an_interrupted_kill_does_not_relabel_a_completed_write() {
        let tracker = QueryTracker::with_history_limit(10);
        let id = tracker.create_query("INSERT INTO t SELECT 1".into());
        tracker.advance_to(&id, QueryState::Running).unwrap();
        let switch = crate::KillSwitch::new();
        tracker.set_canceller(&id, switch.canceller(|_| panic!("stopped completed work")));
        let first = runtime();
        let t = tracker.clone();
        let work = switch.guard(async move {
            // Inside the poll that completes the work: the first kill
            // calls the canceller, and its kill is pending on this poll.
            first.block_on(async move {
                let kill = tokio::spawn(async move { t.kill_query(&id, "root").await });
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                assert!(!kill.is_finished(), "the kill waits for the poll");
            });
            // The runtime shuts down: the settle task is dropped mid-kill.
            drop(first);
            "inserted"
        });
        let mut work = std::pin::pin!(work);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert_eq!(work.as_mut().poll(&mut cx), Poll::Ready(Some("inserted")));
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Running);
        assert_eq!(
            runtime().block_on(tracker.kill_query(&id, "ops")).unwrap(),
            KillOutcome::NotApplied {
                state: QueryState::Running
            }
        );
        assert_eq!(tracker.cancelled_by(&id), None);
        tracker.finish_query(&id, 1).unwrap();
        assert_eq!(tracker.get_query(&id).unwrap().state, QueryState::Finished);
    }
}
