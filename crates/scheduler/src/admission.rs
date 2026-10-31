//! Admission control: a cap on concurrently running queries with a bounded
//! FIFO queue for the rest (admission-control D5).
//!
//! One controller serves a coordinator or standalone process, shared by the
//! pgwire and Trino listeners. A statement that executes a plan calls
//! [`AdmissionController::admit`] before planning:
//!
//! - under the cap, it gets an [`AdmissionPermit`] at once;
//! - over the cap, it waits in a FIFO queue ([`Pending::wait`]);
//! - with the queue full, it fails with [`AdmissionError::QueueFull`].
//!
//! Dropping a permit hands its slot straight to the head of the queue, so a
//! later arrival can never overtake a queued query. A queued query whose
//! future is dropped (client gone, timeout) leaves the queue, and a slot
//! handed to it is passed on.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

/// Why a query was not admitted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionError {
    /// The queue already holds `max_queued_queries` queries.
    #[error("query queue is full ({running} running, {queued} queued)")]
    QueueFull { running: usize, queued: usize },
}

/// Counters for logs and the Web UI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub running: usize,
    pub queued: usize,
    /// `0` means unlimited.
    pub max_running: usize,
    pub max_queued: usize,
}

struct Waiter {
    id: u64,
    grant: oneshot::Sender<()>,
}

struct State {
    running: usize,
    queue: VecDeque<Waiter>,
    next_id: u64,
    /// `0` means unlimited. Changed at runtime by `set_limits`.
    max_running: usize,
    max_queued: usize,
}

impl State {
    fn has_room(&self) -> bool {
        self.max_running == 0 || self.running < self.max_running
    }

    /// Grant waiters in FIFO order while the cap has room. A granted waiter
    /// counts as running from the hand-off on.
    fn grant_waiters(&mut self) {
        while self.has_room() {
            let Some(waiter) = self.queue.pop_front() else {
                return;
            };
            if waiter.grant.send(()).is_ok() {
                self.running += 1;
            }
        }
    }

    fn snapshot(&self) -> AdmissionSnapshot {
        AdmissionSnapshot {
            running: self.running,
            queued: self.queue.len(),
            max_running: self.max_running,
            max_queued: self.max_queued,
        }
    }
}

struct Shared {
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Return a freed slot and hand it to the first waiter still listening,
    /// but only while fewer than the (possibly lowered) cap run.
    fn release(&self) {
        let mut state = self.lock();
        state.running = state.running.saturating_sub(1);
        state.grant_waiters();
    }
}

/// Caps running queries and queues the rest.
#[derive(Clone)]
pub struct AdmissionController {
    shared: Arc<Shared>,
}

impl AdmissionController {
    /// `max_running = 0` disables the cap (every query is admitted at once).
    pub fn new(max_running: usize, max_queued: usize) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    running: 0,
                    queue: VecDeque::new(),
                    next_id: 0,
                    max_running,
                    max_queued,
                }),
            }),
        }
    }

    /// Ask to run a query (`label` names it in the `arneb::admission`
    /// log). Returns at once: either admitted, or queued at a position, or
    /// rejected because the queue is full. Await [`Pending::wait`] for the
    /// permit.
    pub fn admit(&self, label: &str) -> Result<Pending, AdmissionError> {
        self.admit_with(|| label.to_owned())
    }

    /// [`Self::admit`] with the label built only when the query is queued,
    /// the one case that logs it: a query admitted at once costs no
    /// allocation for its label.
    pub fn admit_with(&self, label: impl FnOnce() -> String) -> Result<Pending, AdmissionError> {
        let mut state = self.shared.lock();
        let unlimited = state.max_running == 0;
        if unlimited || (state.running < state.max_running && state.queue.is_empty()) {
            state.running += 1;
            return Ok(Pending {
                shared: Arc::clone(&self.shared),
                inner: PendingInner::Admitted,
                label: String::new(),
                since: Instant::now(),
            });
        }
        if state.queue.len() >= state.max_queued {
            return Err(AdmissionError::QueueFull {
                running: state.running,
                queued: state.queue.len(),
            });
        }
        let label = label();
        let (tx, rx) = oneshot::channel();
        let id = state.next_id;
        state.next_id += 1;
        state.queue.push_back(Waiter { id, grant: tx });
        let position = state.queue.len();
        tracing::info!(
            target: "arneb::admission",
            query = %label,
            position,
            running = state.running,
            max_running = state.max_running,
            "query queued"
        );
        Ok(Pending {
            shared: Arc::clone(&self.shared),
            inner: PendingInner::Queued {
                id,
                grant: rx,
                position,
            },
            label,
            since: Instant::now(),
        })
    }

    /// Current counters and limits.
    pub fn snapshot(&self) -> AdmissionSnapshot {
        self.shared.lock().snapshot()
    }

    /// Change the limits at runtime (manage-api D9) and return the counters
    /// after the change. `max_running = 0` means unlimited.
    ///
    /// - Lowering the cap below the running count stops nothing: new queries
    ///   queue until fewer than the new cap run.
    /// - Raising it (or setting `0`) grants queued queries at once, in FIFO
    ///   order, up to the new cap.
    /// - A shorter queue keeps the queries already queued; new arrivals are
    ///   refused while the queue is at or over the new limit.
    pub fn set_limits(&self, max_running: usize, max_queued: usize) -> AdmissionSnapshot {
        let mut state = self.shared.lock();
        state.max_running = max_running;
        state.max_queued = max_queued;
        state.grant_waiters();
        state.snapshot()
    }

    /// Log a DEBUG snapshot on `arneb::admission` every `every` while any
    /// query is queued. Call once from inside a tokio runtime.
    pub fn spawn_snapshot_logger(&self, every: Duration) {
        let controller = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                let s = controller.snapshot();
                if s.queued > 0 {
                    tracing::debug!(
                        target: "arneb::admission",
                        running = s.running,
                        queued = s.queued,
                        max_running = s.max_running,
                        max_queued = s.max_queued,
                        "admission snapshot"
                    );
                }
            }
        });
    }
}

enum PendingInner {
    Admitted,
    Queued {
        id: u64,
        grant: oneshot::Receiver<()>,
        position: usize,
    },
    /// The permit was handed out (or the slot passed on).
    Done,
}

/// A query that passed admission or is waiting in the queue.
pub struct Pending {
    shared: Arc<Shared>,
    inner: PendingInner,
    /// Empty when the query was admitted at once.
    label: String,
    since: Instant,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("label", &self.label)
            .field("queue_position", &self.queue_position())
            .finish()
    }
}

impl Pending {
    /// Queue position at admission time (1 = next), or `None` when the
    /// query was admitted at once.
    pub fn queue_position(&self) -> Option<usize> {
        match &self.inner {
            PendingInner::Queued { position, .. } => Some(*position),
            _ => None,
        }
    }

    /// A handle that reads this query's live queue position while
    /// [`Self::wait`] is pending, or `None` when it was admitted at once.
    pub fn ticket(&self) -> Option<QueueTicket> {
        match &self.inner {
            PendingInner::Queued { id, .. } => Some(QueueTicket {
                shared: Arc::clone(&self.shared),
                id: *id,
            }),
            _ => None,
        }
    }

    /// Wait for a slot. Cancel-safe: dropping this future leaves the queue
    /// (and passes on a slot granted in the meantime).
    pub async fn wait(mut self) -> AdmissionPermit {
        if let PendingInner::Queued { grant, .. } = &mut self.inner {
            // The sender is only dropped together with the controller.
            let _ = grant.await;
            tracing::info!(
                target: "arneb::admission",
                query = %self.label,
                waited_ms = self.since.elapsed().as_millis() as u64,
                "query admitted"
            );
        }
        self.inner = PendingInner::Done;
        AdmissionPermit {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        match std::mem::replace(&mut self.inner, PendingInner::Done) {
            PendingInner::Done => {}
            // Admitted but never waited on: give the slot back.
            PendingInner::Admitted => self.shared.release(),
            PendingInner::Queued { id, mut grant, .. } => {
                let still_queued = {
                    let mut state = self.shared.lock();
                    let before = state.queue.len();
                    state.queue.retain(|w| w.id != id);
                    state.queue.len() != before
                };
                // Granted between the hand-off and this drop: pass it on.
                if !still_queued && grant.try_recv().is_ok() {
                    self.shared.release();
                }
            }
        }
    }
}

/// A queued query's place in the queue, readable from outside the task
/// that awaits [`Pending::wait`] (e.g. by a status request).
#[derive(Clone)]
pub struct QueueTicket {
    shared: Arc<Shared>,
    id: u64,
}

impl std::fmt::Debug for QueueTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueTicket")
            .field("position", &self.position())
            .finish()
    }
}

impl QueueTicket {
    /// Current queue position (1 = next), or `None` once the query has left
    /// the queue (admitted, or its wait was dropped). One scan of the
    /// bounded queue under the controller lock.
    pub fn position(&self) -> Option<usize> {
        self.shared
            .lock()
            .queue
            .iter()
            .position(|w| w.id == self.id)
            .map(|i| i + 1)
    }
}

/// A running query's slot; dropping it admits the next queued query.
pub struct AdmissionPermit {
    shared: Arc<Shared>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.shared.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn admitted(c: &AdmissionController) -> AdmissionPermit {
        c.admit("q").unwrap().wait().await
    }

    /// The label is built only for a query that has to queue.
    #[tokio::test]
    async fn label_is_built_only_when_queued() {
        let c = AdmissionController::new(1, 10);
        let built = std::cell::Cell::new(0);
        let label = || {
            built.set(built.get() + 1);
            "q".to_owned()
        };
        let _running = c.admit_with(label).unwrap().wait().await;
        assert_eq!(built.get(), 0);
        let queued = c.admit_with(label).unwrap();
        assert_eq!(queued.queue_position(), Some(1));
        assert_eq!(built.get(), 1);

        let unlimited = AdmissionController::new(0, 0);
        let _a = unlimited.admit_with(label).unwrap();
        assert_eq!(built.get(), 1);
    }

    #[tokio::test]
    async fn ticket_tracks_the_live_position() {
        let c = AdmissionController::new(1, 10);
        let first = admitted(&c).await;
        let q1 = c.admit("q1").unwrap();
        let q2 = c.admit("q2").unwrap();
        let (t1, t2) = (q1.ticket().unwrap(), q2.ticket().unwrap());
        assert_eq!((t1.position(), t2.position()), (Some(1), Some(2)));
        drop(first); // the slot goes to q1
        let p1 = q1.wait().await;
        assert_eq!((t1.position(), t2.position()), (None, Some(1)));
        drop(q2); // gave up waiting
        assert_eq!(t2.position(), None);
        drop(p1);
        let unlimited = AdmissionController::new(0, 0);
        assert!(unlimited.admit("x").unwrap().ticket().is_none());
    }

    #[tokio::test]
    async fn caps_running_queries() {
        let c = AdmissionController::new(2, 10);
        let a = admitted(&c).await;
        let _b = admitted(&c).await;
        let third = c.admit("third").unwrap();
        assert_eq!(third.queue_position(), Some(1));
        assert_eq!(
            c.snapshot(),
            AdmissionSnapshot {
                running: 2,
                queued: 1,
                max_running: 2,
                max_queued: 10
            }
        );
        drop(a);
        let _c = tokio::time::timeout(Duration::from_secs(1), third.wait())
            .await
            .expect("slot handed to the queued query");
        assert_eq!(c.snapshot().running, 2);
        assert_eq!(c.snapshot().queued, 0);
    }

    #[tokio::test]
    async fn fifo_order_and_no_overtaking() {
        let c = AdmissionController::new(1, 10);
        let first = admitted(&c).await;
        let q1 = c.admit("q1").unwrap();
        let q2 = c.admit("q2").unwrap();
        assert_eq!(
            (q1.queue_position(), q2.queue_position()),
            (Some(1), Some(2))
        );
        let order = Arc::new(Mutex::new(Vec::new()));
        let (o1, o2) = (order.clone(), order.clone());
        let h2 = tokio::spawn(async move {
            let p = q2.wait().await;
            o2.lock().unwrap().push(2);
            drop(p);
        });
        let h1 = tokio::spawn(async move {
            let p = q1.wait().await;
            o1.lock().unwrap().push(1);
            tokio::time::sleep(Duration::from_millis(20)).await;
            drop(p);
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        // A late arrival queues behind the waiters even though a slot is
        // about to free up.
        assert_eq!(c.admit("late").unwrap().queue_position(), Some(3));
        drop(first);
        h1.await.unwrap();
        h2.await.unwrap();
        assert_eq!(*order.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn dropped_waiter_is_skipped() {
        let c = AdmissionController::new(1, 10);
        let first = admitted(&c).await;
        let gone = c.admit("gone").unwrap();
        let next = c.admit("next").unwrap();
        drop(gone);
        assert_eq!(c.snapshot().queued, 1);
        drop(first);
        let _permit = tokio::time::timeout(Duration::from_secs(1), next.wait())
            .await
            .expect("skips the dropped waiter");
        assert_eq!(c.snapshot().running, 1);
    }

    #[tokio::test]
    async fn slot_granted_to_a_dropped_waiter_is_passed_on() {
        let c = AdmissionController::new(1, 10);
        let first = admitted(&c).await;
        let granted_then_dropped = c.admit("a").unwrap();
        let next = c.admit("b").unwrap();
        drop(first); // grants the slot to `a` before it is ever awaited
        drop(granted_then_dropped);
        let _permit = tokio::time::timeout(Duration::from_secs(1), next.wait())
            .await
            .expect("the slot moved on to b");
        assert_eq!(c.snapshot().running, 1);
    }

    #[tokio::test]
    async fn queue_full_is_rejected() {
        let c = AdmissionController::new(1, 1);
        let _first = admitted(&c).await;
        let _queued = c.admit("queued").unwrap();
        assert_eq!(
            c.admit("rejected").unwrap_err(),
            AdmissionError::QueueFull {
                running: 1,
                queued: 1
            }
        );
    }

    #[tokio::test]
    async fn zero_means_unlimited() {
        let c = AdmissionController::new(0, 0);
        let permits: Vec<_> = futures_join(&c, 50).await;
        assert_eq!(c.snapshot().running, 50);
        drop(permits);
        assert_eq!(c.snapshot().running, 0);
    }

    async fn futures_join(c: &AdmissionController, n: usize) -> Vec<AdmissionPermit> {
        let mut out = Vec::new();
        for _ in 0..n {
            let p = c.admit("q").unwrap();
            assert_eq!(p.queue_position(), None);
            out.push(p.wait().await);
        }
        out
    }

    #[tokio::test]
    async fn admitted_but_never_awaited_returns_the_slot() {
        let c = AdmissionController::new(1, 1);
        drop(c.admit("q").unwrap());
        assert_eq!(c.snapshot().running, 0);
    }

    async fn granted(p: Pending) -> AdmissionPermit {
        tokio::time::timeout(Duration::from_secs(1), p.wait())
            .await
            .expect("the queued query is granted")
    }

    #[tokio::test]
    async fn raising_the_cap_admits_the_queue() {
        let c = AdmissionController::new(1, 10);
        let _first = admitted(&c).await;
        let q1 = c.admit("q1").unwrap();
        let q2 = c.admit("q2").unwrap();
        let snap = c.set_limits(3, 10);
        assert_eq!((snap.running, snap.queued, snap.max_running), (3, 0, 3));
        let _p1 = granted(q1).await;
        let _p2 = granted(q2).await;
        assert_eq!(c.snapshot().running, 3);
    }

    #[tokio::test]
    async fn lowering_below_running_queues_new_queries() {
        let c = AdmissionController::new(4, 10);
        let mut running = Vec::new();
        for _ in 0..4 {
            running.push(admitted(&c).await);
        }
        let snap = c.set_limits(2, 10);
        assert_eq!((snap.running, snap.max_running), (4, 2));
        let q1 = c.admit("q1").unwrap();
        let q2 = c.admit("q2").unwrap();
        assert_eq!(
            (q1.queue_position(), q2.queue_position()),
            (Some(1), Some(2))
        );
        let (t1, t2) = (q1.ticket().unwrap(), q2.ticket().unwrap());

        // A granted waiter leaves the queue, so its ticket reads `None`.
        drop(running.pop()); // 3 running, still >= 2
        assert_eq!((t1.position(), t2.position()), (Some(1), Some(2)));
        assert_eq!(c.snapshot().running, 3);
        drop(running.pop()); // 2 running, still >= 2
        assert_eq!((t1.position(), t2.position()), (Some(1), Some(2)));
        assert_eq!(c.snapshot().running, 2);
        drop(running.pop()); // 1 running: the head of the queue goes first
        assert_eq!((t1.position(), t2.position()), (None, Some(1)));
        let _p1 = granted(q1).await;
        assert_eq!(c.snapshot().running, 2);

        // Raising the cap again grants the rest of the queue at once.
        c.set_limits(4, 10);
        let _p2 = granted(q2).await;
        assert_eq!(t2.position(), None);
        assert_eq!(c.snapshot().running, 3);
    }

    #[tokio::test]
    async fn zero_means_unlimited_at_runtime() {
        let c = AdmissionController::new(1, 10);
        let _first = admitted(&c).await;
        let queued: Vec<_> = (0..5).map(|_| c.admit("q").unwrap()).collect();
        let snap = c.set_limits(0, 10);
        assert_eq!((snap.running, snap.queued), (6, 0));
        for q in queued {
            granted(q).await;
        }
        assert_eq!(c.admit("new").unwrap().queue_position(), None);
    }

    #[tokio::test]
    async fn shrinking_the_queue_keeps_queued_queries() {
        let c = AdmissionController::new(1, 10);
        let _first = admitted(&c).await;
        let queued: Vec<_> = (0..5).map(|_| c.admit("q").unwrap()).collect();
        let snap = c.set_limits(1, 2);
        assert_eq!((snap.running, snap.queued, snap.max_queued), (1, 5, 2));
        assert_eq!(
            c.admit("new").unwrap_err(),
            AdmissionError::QueueFull {
                running: 1,
                queued: 5
            }
        );
        let positions: Vec<_> = queued
            .iter()
            .map(|q| q.ticket().unwrap().position())
            .collect();
        assert_eq!(positions, (1..=5).map(Some).collect::<Vec<_>>());
    }
}
