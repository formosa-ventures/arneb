//! Coordinated byte-denominated in-flight back-pressure budget.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// Shared byte budget for batches buffered between operators.
///
/// A limit of `0` disables the budget and preserves count-only channel
/// back-pressure. When enabled, producers wait before enqueueing a batch if
/// that batch would push the shared in-flight bytes over `limit`. A single
/// oversized batch is allowed when no other bytes are in flight so the system
/// cannot deadlock on a batch larger than the configured ceiling.
#[derive(Debug)]
pub struct InflightBudget {
    used: AtomicU64,
    limit: u64,
    notify: Notify,
}

impl InflightBudget {
    /// Create a new budget with `limit` bytes. `0` disables gating.
    pub fn new(limit: u64) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit,
            notify: Notify::new(),
        }
    }

    /// Wait until `bytes` can be admitted, then reserve them.
    pub async fn acquire(&self, bytes: u64) {
        if self.limit == 0 || bytes == 0 {
            return;
        }

        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let used = self.used.load(Ordering::Acquire);
            let allowed = used == 0
                || used
                    .checked_add(bytes)
                    .is_some_and(|next| next <= self.limit);
            if allowed {
                if self
                    .used
                    .compare_exchange_weak(
                        used,
                        used.saturating_add(bytes),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return;
                }
                continue;
            }

            notified.await;
        }
    }

    /// Release bytes previously admitted by [`Self::acquire`].
    pub fn release(&self, bytes: u64) {
        if self.limit == 0 || bytes == 0 {
            return;
        }
        let previous = self.used.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(
            previous >= bytes,
            "InflightBudget release exceeded acquired bytes"
        );
        self.notify.notify_waiters();
    }

    /// Configured byte limit. `0` means disabled.
    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn acquire_blocks_until_release_wakes_waiter() {
        let budget = Arc::new(InflightBudget::new(10));
        budget.acquire(7).await;

        let waiter_budget = Arc::clone(&budget);
        let mut waiter = tokio::spawn(async move {
            waiter_budget.acquire(4).await;
        });

        assert!(timeout(Duration::from_millis(25), &mut waiter)
            .await
            .is_err());
        budget.release(7);
        timeout(Duration::from_millis(250), waiter)
            .await
            .expect("release should wake blocked acquire")
            .expect("waiter task should not panic");
    }

    #[tokio::test]
    async fn used_zero_allows_single_oversized_batch() {
        let budget = InflightBudget::new(10);
        timeout(Duration::from_millis(25), budget.acquire(100))
            .await
            .expect("single oversized batch must not deadlock");
    }

    #[tokio::test]
    async fn limit_zero_never_blocks() {
        let budget = InflightBudget::new(0);
        budget.acquire(u64::MAX).await;
        budget.acquire(u64::MAX).await;
    }
}
