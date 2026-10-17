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
}

struct Shared {
    max_running: usize,
    max_queued: usize,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Give a freed slot to the first waiter still listening, or return it.
    fn release(&self) {
        let mut state = self.lock();
        while let Some(waiter) = state.queue.pop_front() {
            if waiter.grant.send(()).is_ok() {
                return; // the slot moves to the waiter; `running` is unchanged
            }
        }
        state.running = state.running.saturating_sub(1);
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
                max_running,
                max_queued,
                state: Mutex::new(State {
                    running: 0,
                    queue: VecDeque::new(),
                    next_id: 0,
                }),
            }),
        }
    }

    /// Ask to run a query (`label` names it in the `arneb::admission`
    /// log). Returns at once: either admitted, or queued at a position, or
    /// rejected because the queue is full. Await [`Pending::wait`] for the
    /// permit.
    pub fn admit(&self, label: &str) -> Result<Pending, AdmissionError> {
        let mut state = self.shared.lock();
        let unlimited = self.shared.max_running == 0;
        if unlimited || (state.running < self.shared.max_running && state.queue.is_empty()) {
            state.running += 1;
            return Ok(Pending {
                shared: Arc::clone(&self.shared),
                inner: PendingInner::Admitted,
                label: label.to_owned(),
                since: Instant::now(),
            });
        }
        if state.queue.len() >= self.shared.max_queued {
            return Err(AdmissionError::QueueFull {
                running: state.running,
                queued: state.queue.len(),
            });
        }
        let (tx, rx) = oneshot::channel();
        let id = state.next_id;
        state.next_id += 1;
        state.queue.push_back(Waiter { id, grant: tx });
        let position = state.queue.len();
        tracing::info!(
            target: "arneb::admission",
            query = label,
            position,
            running = state.running,
            max_running = self.shared.max_running,
            "query queued"
        );
        Ok(Pending {
            shared: Arc::clone(&self.shared),
            inner: PendingInner::Queued {
                id,
                grant: rx,
                position,
            },
            label: label.to_owned(),
            since: Instant::now(),
        })
    }

    /// Current counters.
    pub fn snapshot(&self) -> AdmissionSnapshot {
        let state = self.shared.lock();
        AdmissionSnapshot {
            running: state.running,
            queued: state.queue.len(),
            max_running: self.shared.max_running,
            max_queued: self.shared.max_queued,
        }
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
}
