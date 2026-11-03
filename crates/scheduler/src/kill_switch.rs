//! A statement's kill switch: decides, atomically with the statement's
//! progress, whether a kill stopped its work or came after it completed.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::tracker::Canceller;

#[derive(Default)]
enum Progress {
    /// Not started, or waiting between two polls.
    #[default]
    Idle,
    /// A poll of the work is in flight: a kill is recorded as pending and
    /// settled by the guard when that poll ends.
    Polling,
    /// The guarded work completed (successfully or not): a kill is too
    /// late and stops nothing.
    Done,
    /// Killed before the work completed: it is never polled again.
    Killed,
}

#[derive(Default)]
struct Shared {
    progress: Progress,
    /// A kill arrived during the poll in flight.
    kill_pending: bool,
    /// The guard's waker, while it waits between polls.
    waker: Option<Waker>,
    /// Kills waiting for the poll in flight to settle them.
    killers: Vec<Waker>,
}

/// Stops a statement's work on a kill, or reports that the kill came too
/// late. A protocol creates one per statement, registers
/// [`Self::canceller`] with the tracker *before* the work starts, runs the
/// work through [`Self::guard`], and routes its own cancels (a client
/// cancel, a reaper) through [`Self::kill`].
///
/// A kill never lands in the middle of a poll of the work. A kill during a
/// poll is recorded as pending and settled by the guard when that poll
/// ends; the kill waits (asynchronously, holding no lock) for that outcome:
///
/// - a kill before the work starts prevents it: the work is never polled;
/// - a kill that lands while the poll that completes the work is in
///   flight, or after the work returned, is not applied, so the statement
///   reports its real result;
/// - otherwise the work is dropped between two polls and the kill is
///   applied. This decides only whether the *root* future completed: work
///   already done in an earlier poll, or in a spawned task the root future
///   does not own, is not undone, and is reported as cancelled, as with an
///   abort handle. A statement whose effect happens partway through its
///   future would need its own "write started" point to report such a
///   kill as not applied; none exists today, since the execution engine
///   does not run DDL or DML plans.
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

    /// A tracker canceller for this switch: [`Self::kill`], then, when the
    /// kill was applied, `on_kill` with the killer's name. Its future
    /// resolves to whether the kill was applied. The kill is requested when
    /// the canceller is called, before its future is first polled.
    pub fn canceller(&self, on_kill: impl FnOnce(&str) + Send + Sync + 'static) -> Canceller {
        let switch = self.clone();
        Box::new(move |by| {
            let by = by.to_owned();
            let kill = switch.kill();
            Box::pin(async move {
                let applied = kill.await;
                if applied {
                    on_kill(&by);
                }
                applied
            })
        })
    }

    /// Kill the work. The kill is requested at once; the returned future
    /// resolves to `true` when it stopped the work (or the work was already
    /// killed), and to `false` when the work completed first, so the
    /// statement keeps its real outcome. During a poll it resolves when
    /// that poll ends. It never blocks a thread, and dropping it does not
    /// withdraw the kill.
    pub fn kill(&self) -> Kill {
        let waker = {
            let mut shared = self.lock();
            match shared.progress {
                Progress::Idle => {
                    shared.progress = Progress::Killed;
                    shared.waker.take()
                }
                Progress::Polling => {
                    shared.kill_pending = true;
                    None
                }
                Progress::Done | Progress::Killed => None,
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Kill {
            switch: self.clone(),
        }
    }

    #[cfg(test)]
    fn is_killed(&self) -> bool {
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

    /// End the poll in flight: `Done` when the work completed, otherwise
    /// `Killed` when a kill arrived during the poll, else `Idle`. Returns
    /// whether the work is now killed. Wakes the waiting kills.
    fn settle(&self, completed: bool, waker: Option<&Waker>) -> bool {
        let (killers, killed) = {
            let mut shared = self.lock();
            let pending = std::mem::take(&mut shared.kill_pending);
            shared.progress = if completed {
                Progress::Done
            } else if pending {
                Progress::Killed
            } else {
                Progress::Idle
            };
            match waker {
                // Skip the clone when the stored waker would wake this task.
                Some(w) if !completed && !pending => {
                    if !shared.waker.as_ref().is_some_and(|old| old.will_wake(w)) {
                        shared.waker = Some(w.clone());
                    }
                }
                _ => shared.waker = None,
            }
            (std::mem::take(&mut shared.killers), pending && !completed)
        };
        for killer in killers {
            killer.wake();
        }
        killed
    }
}

/// The future returned by [`KillSwitch::kill`]: whether the kill was
/// applied.
pub struct Kill {
    switch: KillSwitch,
}

impl Future for Kill {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let mut shared = self.switch.lock();
        match shared.progress {
            Progress::Done => Poll::Ready(false),
            Progress::Killed => Poll::Ready(true),
            Progress::Polling => {
                // Settled by the guard when the poll ends.
                shared.kill_pending = true;
                if !shared.killers.iter().any(|w| w.will_wake(cx.waker())) {
                    shared.killers.push(cx.waker().clone());
                }
                Poll::Pending
            }
            Progress::Idle => {
                // Between polls (e.g. the kill was requested before a poll
                // whose work panicked): kill now.
                shared.progress = Progress::Killed;
                let waker = shared.waker.take();
                drop(shared);
                if let Some(waker) = waker {
                    waker.wake();
                }
                Poll::Ready(true)
            }
        }
    }
}

/// The future returned by [`KillSwitch::guard`].
pub struct Guarded<F> {
    switch: KillSwitch,
    work: Option<Pin<Box<F>>>,
}

/// Settles a poll that unwinds (the work panicked): a pending kill is
/// applied, since the work did not complete; otherwise the switch is left
/// idle, so a later kill applies too.
struct SettleOnUnwind<'a>(&'a KillSwitch);

impl Drop for SettleOnUnwind<'_> {
    fn drop(&mut self) {
        self.0.settle(false, None);
    }
}

impl<F: Future> Future for Guarded<F> {
    type Output = Option<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(work) = this.work.as_mut() else {
            return Poll::Ready(None);
        };
        {
            let mut shared = this.switch.lock();
            match shared.progress {
                Progress::Killed => {
                    drop(shared);
                    this.work = None;
                    return Poll::Ready(None);
                }
                // Polled again after completing: nothing left to run.
                Progress::Done => return Poll::Ready(None),
                Progress::Idle | Progress::Polling => shared.progress = Progress::Polling,
            }
        }
        // Polled without the lock: a kill meanwhile is recorded as pending
        // and settled right below.
        let unwind = SettleOnUnwind(&this.switch);
        let polled = work.as_mut().poll(cx);
        std::mem::forget(unwind);
        match polled {
            Poll::Ready(output) => {
                this.switch.settle(true, None);
                this.work = None;
                Poll::Ready(Some(output))
            }
            Poll::Pending => {
                if this.switch.settle(false, Some(cx.waker())) {
                    // Killed during this poll, which did not complete it.
                    this.work = None;
                    return Poll::Ready(None);
                }
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

    /// Polls `fut` once with a no-op waker.
    fn poll_once<F: Future + Unpin>(fut: &mut F) -> Poll<F::Output> {
        Pin::new(fut).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// Yields once: `Pending` on the first poll, then `Ready`.
    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(|cx| {
            if yielded {
                return Poll::Ready(());
            }
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        })
        .await
    }

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
        assert!(cancel("root").await);
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
        assert!(!cancel("root").await);
        assert!(!switch.kill().await);
        assert!(!switch.is_killed());
    }

    /// A kill while the work waits between polls wakes the guard, which
    /// drops the work.
    #[tokio::test]
    async fn a_kill_between_polls_drops_the_work() {
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
        assert!(switch.canceller(|_| {})("root").await);
        let out = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("the guard returned after the kill")
            .unwrap();
        assert_eq!(out, None);
        assert!(dropped.load(Ordering::SeqCst));
    }

    /// A kill that arrives during the poll that completes the work is
    /// pending until that poll ends (it blocks no thread meanwhile), then
    /// settled as not applied; so is a second kill waiting with it.
    #[tokio::test]
    async fn a_kill_during_the_completing_poll_is_not_applied() {
        let switch = KillSwitch::new();
        let cancel = switch.canceller(|_| panic!("on_kill after completion"));
        let probe = switch.clone();
        let kills = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&kills);
        let out = switch
            .guard(async move {
                // Inside the poll: both kills are pending, not blocked.
                let mut first = cancel("root");
                let mut second = probe.kill();
                assert!(poll_once(&mut first).is_pending(), "a poll is in flight");
                assert!(poll_once(&mut second).is_pending());
                *slot.lock().unwrap() = Some((first, second));
                "inserted"
            })
            .await;
        assert_eq!(out, Some("inserted"));
        let (first, second) = kills.lock().unwrap().take().unwrap();
        assert!(!first.await, "the kill must not apply");
        assert!(!second.await, "a concurrent kill shares the outcome");
        assert!(!switch.is_killed());
    }

    /// A kill during a poll that does not complete the work is applied when
    /// that poll ends: the work is dropped and never polled again.
    #[tokio::test]
    async fn a_kill_during_a_poll_that_does_not_complete_is_applied() {
        let switch = KillSwitch::new();
        let probe = switch.clone();
        let kill = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&kill);
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = Arc::clone(&polls);
        let out = switch
            .guard(async move {
                count.fetch_add(1, Ordering::SeqCst);
                *slot.lock().unwrap() = Some(probe.kill());
                yield_once().await;
                count.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        assert_eq!(out, None);
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "polled again after the kill"
        );
        let kill = kill.lock().unwrap().take().unwrap();
        assert!(kill.await);
        assert!(switch.is_killed());
    }

    /// A kill pending when the work panics is applied: the work did not
    /// complete.
    #[tokio::test]
    async fn a_kill_pending_when_the_work_panics_is_applied() {
        let switch = KillSwitch::new();
        let probe = switch.clone();
        let kill = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&kill);
        let task = tokio::spawn(switch.guard(async move {
            *slot.lock().unwrap() = Some(probe.kill());
            panic!("work panicked");
        }));
        assert!(task.await.unwrap_err().is_panic());
        let kill = kill.lock().unwrap().take().unwrap();
        assert!(kill.await);
    }
}
