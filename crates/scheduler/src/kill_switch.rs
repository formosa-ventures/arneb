//! A statement's kill switch: decides, atomically with the statement's
//! progress, whether a kill stopped its work or came after it completed.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::tracker::Canceller;

#[derive(Default)]
enum Progress {
    /// Not started, or still running.
    #[default]
    Running,
    /// The guarded work completed (successfully or not): a kill is too
    /// late and stops nothing.
    Done,
    /// Killed before the work completed: it is never polled again.
    Killed,
}

#[derive(Default)]
struct Shared {
    progress: Progress,
    waker: Option<Waker>,
}

/// Stops a statement's work on a kill, or reports that the kill came too
/// late. A protocol creates one per statement, registers
/// [`Self::canceller`] with the tracker *before* the work starts, and runs
/// the work through [`Self::guard`].
///
/// The guard polls the work under the switch's lock, and the canceller
/// takes the same lock, so a kill never lands in the middle of a poll:
///
/// - a kill before the work starts prevents it: the work is never polled;
/// - a kill after the work returned is not applied (the canceller returns
///   `false`), so the statement reports its real result;
/// - a kill during the work drops it at its next poll. A write may already
///   have taken effect in an earlier poll, or in a spawned task the root
///   future does not own, and is then reported as cancelled although part
///   or all of it happened, as with an abort handle.
///
/// Blocking: a kill waits for an in-flight poll of the work to end, so call
/// the canceller off async workers. The lock is not reentrant: the work
/// must never kill its own statement from inside its poll (it would
/// deadlock); no such path exists.
#[derive(Clone, Default)]
pub struct KillSwitch(Arc<Mutex<Shared>>);

impl KillSwitch {
    /// A switch whose work has not started.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Shared> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A tracker canceller for this switch. When the work has not completed
    /// it marks the switch killed, wakes the guard (which drops the work),
    /// then calls `on_kill` with the killer's name outside the lock, and
    /// returns `true`. Once the work completed it does nothing and returns
    /// `false`.
    pub fn canceller(&self, on_kill: impl FnOnce(&str) + Send + Sync + 'static) -> Canceller {
        let switch = self.clone();
        Box::new(move |by| {
            if !switch.kill() {
                return false;
            }
            on_kill(by);
            true
        })
    }

    /// Mark the switch killed; `false` when the work already completed.
    fn kill(&self) -> bool {
        let waker = {
            let mut shared = self.lock();
            match shared.progress {
                Progress::Done => return false,
                Progress::Killed => return true,
                Progress::Running => {}
            }
            shared.progress = Progress::Killed;
            shared.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    }

    /// A non-blocking kill for a protocol's own cancel (a client cancel, a
    /// reaper), which runs on async workers. `Some(false)` when the work
    /// already returned (leave the outcome to the protocol), `Some(true)`
    /// when it is now killed, and `None` when a poll is in flight, so the
    /// outcome is not known without waiting (cancel as before: abort the
    /// task).
    pub fn try_kill(&self) -> Option<bool> {
        let mut shared = match self.0.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(p)) => p.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        match shared.progress {
            Progress::Done => return Some(false),
            Progress::Killed => return Some(true),
            Progress::Running => {}
        }
        shared.progress = Progress::Killed;
        let waker = shared.waker.take();
        drop(shared);
        if let Some(waker) = waker {
            waker.wake();
        }
        Some(true)
    }

    /// Whether a kill stopped the work.
    pub fn is_killed(&self) -> bool {
        matches!(self.lock().progress, Progress::Killed)
    }

    /// Run `work` under the switch: `Some(output)` when it completed, or
    /// `None` when a kill came first (the work is then dropped, unpolled
    /// from then on).
    pub fn guard<F: Future>(&self, work: F) -> Guarded<F> {
        Guarded {
            switch: self.clone(),
            work: Some(Box::pin(work)),
        }
    }
}

/// The future returned by [`KillSwitch::guard`].
pub struct Guarded<F> {
    switch: KillSwitch,
    work: Option<Pin<Box<F>>>,
}

impl<F: Future> Future for Guarded<F> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(work) = this.work.as_mut() else {
            return Poll::Ready(None);
        };
        let mut shared = this.switch.lock();
        match shared.progress {
            Progress::Killed => {
                drop(shared);
                this.work = None;
                return Poll::Ready(None);
            }
            Progress::Done => {
                // Polled again after completing: nothing left to run.
                drop(shared);
                return Poll::Ready(None);
            }
            Progress::Running => {}
        }
        // Polled under the lock: a kill waits for this poll to end.
        match work.as_mut().poll(cx) {
            Poll::Ready(output) => {
                shared.progress = Progress::Done;
                shared.waker = None;
                drop(shared);
                this.work = None;
                Poll::Ready(Some(output))
            }
            Poll::Pending => {
                shared.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// A kill before the work starts: the work is never polled, and the
    /// canceller reports the kill as applied.
    #[tokio::test]
    async fn a_kill_before_the_start_stops_all_the_work() {
        let switch = KillSwitch::new();
        let called = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&called);
        let cancel = switch.canceller(move |by| {
            assert_eq!(by, "root");
            seen.store(true, Ordering::SeqCst);
        });
        assert!(cancel("root"));
        assert!(called.load(Ordering::SeqCst));
        let ran = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&ran);
        let out = switch
            .guard(async move {
                flag.store(true, Ordering::SeqCst);
                1
            })
            .await;
        assert_eq!(out, None);
        assert!(!ran.load(Ordering::SeqCst), "the killed work ran");
        assert!(switch.is_killed());
    }

    /// A kill after the work completed stops nothing: the canceller returns
    /// `false` and does not call `on_kill`.
    #[tokio::test]
    async fn a_kill_after_completion_is_not_applied() {
        let switch = KillSwitch::new();
        let cancel = switch.canceller(|_| panic!("on_kill after completion"));
        assert_eq!(switch.guard(async { 7 }).await, Some(7));
        assert!(!cancel("root"));
        assert!(!switch.is_killed());
    }

    /// A kill while the work waits wakes the guard, which drops the work.
    #[tokio::test]
    async fn a_kill_while_running_drops_the_work() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let switch = KillSwitch::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(Arc::clone(&dropped));
        let task = tokio::spawn(switch.guard(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        }));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(switch.canceller(|_| {})("root"));
        let out = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the guard returned after the kill")
            .unwrap();
        assert_eq!(out, None);
        assert!(dropped.load(Ordering::SeqCst));
    }

    /// A kill that arrives during the poll that completes the work waits
    /// for that poll (proven: it has not returned while the poll still
    /// runs), then finds the work done: not applied. A non-blocking
    /// `try_kill` during the poll reports `None`.
    #[tokio::test]
    async fn a_kill_during_the_completing_poll_waits_and_is_not_applied() {
        use std::sync::mpsc;
        let switch = KillSwitch::new();
        let cancel = switch.canceller(|_| panic!("on_kill after completion"));
        let probe = switch.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let killer = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&killer);
        let out = switch
            .guard(async move {
                // Inside the poll: the switch is locked.
                assert_eq!(probe.try_kill(), None, "a poll is in flight");
                let handle = std::thread::spawn(move || {
                    started_tx.send(()).unwrap();
                    let applied = cancel("root");
                    done_tx.send(()).unwrap();
                    applied
                });
                started_rx.recv().unwrap();
                // The killer started, yet cannot return while we poll.
                assert!(
                    done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                    "the kill returned during the poll"
                );
                *slot.lock().unwrap() = Some((handle, done_rx));
                "inserted"
            })
            .await;
        assert_eq!(out, Some("inserted"));
        let (handle, done_rx) = killer.lock().unwrap().take().unwrap();
        assert!(!handle.join().unwrap(), "the kill must not apply");
        assert!(done_rx.try_recv().is_ok());
        assert_eq!(switch.try_kill(), Some(false), "done: nothing to kill");
    }

    /// `try_kill` between polls kills without blocking and wakes the guard.
    #[tokio::test]
    async fn try_kill_between_polls_kills() {
        let switch = KillSwitch::new();
        let task = tokio::spawn(switch.guard(std::future::pending::<()>()));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(switch.try_kill(), Some(true));
        let out = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, None);
    }
}
