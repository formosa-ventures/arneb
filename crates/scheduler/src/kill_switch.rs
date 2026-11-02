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
/// takes the same lock, so a kill never lands in the middle of a poll: it
/// either comes first (the work is dropped and never polled again, so a
/// kill that arrives before the work starts stops all of it) or after the
/// poll that completed the work (the canceller returns `false`, and the
/// tracker reports the kill as not applied). A completed write is
/// therefore never reported as cancelled. A kill waits at most one poll of
/// the work.
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
    /// for that poll, then finds the work done: not applied.
    #[tokio::test]
    async fn a_kill_during_the_completing_poll_waits_and_is_not_applied() {
        let switch = KillSwitch::new();
        let cancel = switch.canceller(|_| panic!("on_kill after completion"));
        let killer = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&killer);
        let out = switch
            .guard(async move {
                // The kill starts while this poll runs and blocks on the lock.
                let handle = std::thread::spawn(move || cancel("root"));
                std::thread::sleep(Duration::from_millis(20));
                *slot.lock().unwrap() = Some(handle);
                "inserted"
            })
            .await;
        assert_eq!(out, Some("inserted"));
        let handle = killer.lock().unwrap().take().unwrap();
        assert!(!handle.join().unwrap(), "the kill must not apply");
    }
}
