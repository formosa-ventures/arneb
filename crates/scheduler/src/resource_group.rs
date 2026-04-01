//! Resource groups for query admission control.

use std::collections::VecDeque;

use arneb_common::identifiers::QueryId;

/// Basic admission control: limits concurrent and queued queries.
#[derive(Debug)]
pub struct ResourceGroup {
    /// Maximum number of concurrently running queries.
    pub max_running: usize,
    /// Maximum number of queued queries.
    pub max_queued: usize,
    /// Currently running query count.
    running: usize,
    /// Waiting queue.
    queue: VecDeque<QueryId>,
}

impl ResourceGroup {
    /// Creates a resource group with the given limits.
    pub fn new(max_running: usize, max_queued: usize) -> Self {
        Self {
            max_running,
            max_queued,
            running: 0,
            queue: VecDeque::new(),
        }
    }

    /// Try to acquire a slot for the query. Returns true if the query can run
    /// immediately, false if it was queued. Returns Err if the queue is full.
    pub fn try_acquire(&mut self, query_id: QueryId) -> Result<bool, String> {
        if self.running < self.max_running {
            self.running += 1;
            return Ok(true);
        }
        if self.queue.len() >= self.max_queued {
            return Err("resource group queue is full".into());
        }
        self.queue.push_back(query_id);
        Ok(false)
    }

    /// Release a slot when a query finishes. Returns the next queued query
    /// that should be admitted, if any.
    pub fn release(&mut self) -> Option<QueryId> {
        if self.running > 0 {
            self.running -= 1;
        }
        if let Some(next) = self.queue.pop_front() {
            self.running += 1;
            Some(next)
        } else {
            None
        }
    }

    /// Returns the number of currently running queries.
    pub fn running_count(&self) -> usize {
        self.running
    }

    /// Returns the number of queued queries.
    pub fn queued_count(&self) -> usize {
        self.queue.len()
    }
}

impl Default for ResourceGroup {
    fn default() -> Self {
        Self::new(100, 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_within_limit() {
        let mut rg = ResourceGroup::new(2, 10);
        let q1 = QueryId::new();
        let q2 = QueryId::new();
        assert!(rg.try_acquire(q1).unwrap()); // runs immediately
        assert!(rg.try_acquire(q2).unwrap()); // runs immediately
        assert_eq!(rg.running_count(), 2);
    }

    #[test]
    fn acquire_queues_when_full() {
        let mut rg = ResourceGroup::new(1, 10);
        let q1 = QueryId::new();
        let q2 = QueryId::new();
        assert!(rg.try_acquire(q1).unwrap()); // runs
        assert!(!rg.try_acquire(q2).unwrap()); // queued
        assert_eq!(rg.running_count(), 1);
        assert_eq!(rg.queued_count(), 1);
    }

    #[test]
    fn acquire_rejects_when_queue_full() {
        let mut rg = ResourceGroup::new(1, 1);
        let q1 = QueryId::new();
        let q2 = QueryId::new();
        let q3 = QueryId::new();
        assert!(rg.try_acquire(q1).unwrap()); // runs
        assert!(!rg.try_acquire(q2).unwrap()); // queued
        assert!(rg.try_acquire(q3).is_err()); // rejected
    }

    #[test]
    fn release_admits_next() {
        let mut rg = ResourceGroup::new(1, 10);
        let q1 = QueryId::new();
        let q2 = QueryId::new();
        rg.try_acquire(q1).unwrap();
        rg.try_acquire(q2).unwrap(); // queued
        let next = rg.release();
        assert_eq!(next, Some(q2));
        assert_eq!(rg.running_count(), 1); // q2 now running
    }

    #[test]
    fn release_no_queued() {
        let mut rg = ResourceGroup::new(2, 10);
        let q1 = QueryId::new();
        rg.try_acquire(q1).unwrap();
        let next = rg.release();
        assert!(next.is_none());
        assert_eq!(rg.running_count(), 0);
    }
}
