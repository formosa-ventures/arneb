//! Tasks an operator spawns that stop when nothing needs them any more.
//!
//! A task started with `tokio::spawn` keeps running after its `JoinHandle`
//! is dropped. An operator that drives its input from such a task would
//! then keep that input (and every reservation under it) alive after the
//! query is cancelled, until the input happens to produce a batch and the
//! send fails (#335). These wrappers abort the task instead.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::task::{AbortHandle, JoinError, JoinHandle};

/// A spawned task that is aborted when this handle is dropped. Awaiting it
/// gives the task's result, like a [`JoinHandle`].
#[derive(Debug)]
pub(crate) struct AbortOnDrop<T>(JoinHandle<T>);

impl<T: Send + 'static> AbortOnDrop<T> {
    /// Spawn `future` on the current runtime.
    pub(crate) fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        Self(tokio::spawn(future))
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Tasks that are aborted together when the group is dropped. Share it
/// with an `Arc` to abort them when the last owner goes away.
#[derive(Debug, Default)]
pub(crate) struct TaskGroup(Vec<AbortHandle>);

impl TaskGroup {
    /// Spawn `future` as a member of the group.
    pub(crate) fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.0.push(tokio::spawn(future).abort_handle());
    }
}

impl Drop for TaskGroup {
    fn drop(&mut self) {
        for handle in &self.0 {
            handle.abort();
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// Sets its flag when dropped, so a test can tell that the future
    /// holding it was dropped.
    pub(crate) struct DropFlag(pub(crate) Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Wait (up to a second) for `flag` to be set.
    pub(crate) async fn dropped_soon(flag: &AtomicBool) -> bool {
        for _ in 0..100 {
            if flag.load(Ordering::SeqCst) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    /// An input whose `execute` never returns, like an operator still
    /// building from an upstream that went quiet. It marks `dropped` once
    /// every pending `execute` future has been dropped.
    #[derive(Debug)]
    pub(crate) struct StalledExec {
        partitions: usize,
        dropped: Arc<AtomicBool>,
        live: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StalledExec {
        pub(crate) fn new(partitions: usize) -> (Arc<Self>, Arc<AtomicBool>) {
            let dropped = Arc::new(AtomicBool::new(false));
            let exec = Arc::new(Self {
                partitions,
                dropped: Arc::clone(&dropped),
                live: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            });
            (exec, dropped)
        }
    }

    struct LiveExecute {
        live: Arc<std::sync::atomic::AtomicUsize>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for LiveExecute {
        fn drop(&mut self) {
            if self.live.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::operator::ExecutionPlan for StalledExec {
        fn schema(&self) -> Vec<arneb_common::types::ColumnInfo> {
            vec![arneb_common::types::ColumnInfo {
                name: "id".to_string(),
                data_type: arneb_common::types::DataType::Int64,
                nullable: true,
            }]
        }

        async fn execute(
            &self,
            _partition: usize,
        ) -> Result<
            arneb_common::stream::SendableRecordBatchStream,
            arneb_common::error::ExecutionError,
        > {
            self.live.fetch_add(1, Ordering::SeqCst);
            let _live = LiveExecute {
                live: Arc::clone(&self.live),
                dropped: Arc::clone(&self.dropped),
            };
            std::future::pending().await
        }

        fn display_name(&self) -> &str {
            "StalledExec"
        }

        fn output_partitioning(&self) -> crate::partitioning::Partitioning {
            crate::partitioning::Partitioning::UnknownPartitioning(self.partitions)
        }
    }

    fn pending_task(flag: &Arc<AtomicBool>) -> impl Future<Output = ()> + Send + 'static {
        let guard = DropFlag(Arc::clone(flag));
        async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        }
    }

    #[tokio::test]
    async fn dropping_an_abort_on_drop_handle_stops_its_task() {
        let flag = Arc::new(AtomicBool::new(false));
        let handle = AbortOnDrop::spawn(pending_task(&flag));
        drop(handle);
        assert!(dropped_soon(&flag).await);
    }

    #[tokio::test]
    async fn an_abort_on_drop_handle_returns_the_result() {
        let handle = AbortOnDrop::spawn(async { 7 });
        assert_eq!(handle.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn dropping_a_task_group_stops_every_member() {
        let flags: Vec<_> = (0..3).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let mut group = TaskGroup::default();
        for flag in &flags {
            group.spawn(pending_task(flag));
        }
        drop(group);
        for flag in &flags {
            assert!(dropped_soon(flag).await);
        }
    }
}
