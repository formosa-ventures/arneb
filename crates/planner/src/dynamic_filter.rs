//! Plan-time allocation of dynamic filter identifiers.

use std::sync::atomic::{AtomicU32, Ordering};

use arneb_common::DynamicFilterId;

/// Allocates dense `DynamicFilterId` values for one query plan.
///
/// One allocator per `QueryPlanner` invocation. IDs are guaranteed
/// monotonically increasing within a single allocator so that downstream
/// data structures (e.g. coordinator-side `DynamicFilterService`) can use
/// dense Vec indexing keyed by `id.0 as usize`.
#[derive(Debug, Default)]
pub struct DynamicFilterIdAllocator {
    next: AtomicU32,
}

impl DynamicFilterIdAllocator {
    /// Returns a new allocator starting from id 0.
    pub fn new() -> Self {
        Self {
            next: AtomicU32::new(0),
        }
    }

    /// Returns a new allocator starting from `first_id`.
    pub fn new_starting_at(first_id: u32) -> Self {
        Self {
            next: AtomicU32::new(first_id),
        }
    }

    /// Allocates and returns the next `DynamicFilterId`.
    pub fn allocate(&self) -> DynamicFilterId {
        DynamicFilterId(self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns how many IDs have been allocated so far.
    pub fn count(&self) -> u32 {
        self.next.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_yields_dense_ids() {
        let alloc = DynamicFilterIdAllocator::new();
        assert_eq!(alloc.allocate(), DynamicFilterId(0));
        assert_eq!(alloc.allocate(), DynamicFilterId(1));
        assert_eq!(alloc.allocate(), DynamicFilterId(2));
        assert_eq!(alloc.count(), 3);
    }

    #[test]
    fn separate_allocators_are_independent() {
        let a = DynamicFilterIdAllocator::new();
        let b = DynamicFilterIdAllocator::new();
        assert_eq!(a.allocate(), DynamicFilterId(0));
        assert_eq!(a.allocate(), DynamicFilterId(1));
        assert_eq!(b.allocate(), DynamicFilterId(0));
        assert_eq!(a.count(), 2);
        assert_eq!(b.count(), 1);
    }
}
