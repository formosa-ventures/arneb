//! Partitioning metadata for physical operators.
//!
//! Phase 3.1 introduces the [`Partitioning`] enum and the
//! [`ExecutionPlan::output_partitioning`] / [`ExecutionPlan::required_input_partitioning`]
//! trait methods. All operators default to `UnknownPartitioning(1)` (a
//! single sequential partition) so the existing single-threaded
//! execution path keeps working unchanged. Subsequent phases enable
//! multi-partition execution by overriding these methods on individual
//! operators and inserting `RepartitionExec`/`CoalescePartitionsExec`
//! via the physical planner.

use arneb_planner::PlanExpr;

/// How an operator's output rows are distributed across `n` independent
/// streams.
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    /// `n` partitions, no order or content guarantee within or across them.
    /// The single-partition default `UnknownPartitioning(1)` mirrors the
    /// pre-3.1 behaviour: one sequential stream of batches.
    UnknownPartitioning(usize),
    /// `n` partitions filled round-robin from upstream batches. Useful
    /// as a load-balancer ahead of stateless operators.
    RoundRobinBatch(usize),
    /// `n` partitions where rows with the same hash of the given
    /// expressions land in the same partition. Required for shuffle
    /// joins and partitioned aggregates.
    Hash(Vec<PlanExpr>, usize),
}

impl Partitioning {
    /// Returns the partition count carried by this `Partitioning`.
    pub fn partition_count(&self) -> usize {
        match self {
            Partitioning::UnknownPartitioning(n)
            | Partitioning::RoundRobinBatch(n)
            | Partitioning::Hash(_, n) => *n,
        }
    }

    /// Returns `true` when this partitioning satisfies the partitioning
    /// shape required by a downstream operator.
    ///
    /// Compatibility rules:
    /// - `RoundRobinBatch(n)` satisfies `UnknownPartitioning(n)`.
    /// - `Hash(_, n)` satisfies `UnknownPartitioning(n)`.
    /// - `Hash(a, n)` satisfies `Hash(b, n)` iff `a == b`.
    /// - Any partitioning satisfies a same-count `UnknownPartitioning`.
    /// - Partitioning counts that differ are never compatible.
    pub fn satisfies(&self, required: &Partitioning) -> bool {
        if self.partition_count() != required.partition_count() {
            return false;
        }
        match (self, required) {
            (_, Partitioning::UnknownPartitioning(_)) => true,
            (Partitioning::Hash(a, _), Partitioning::Hash(b, _)) => a == b,
            (Partitioning::RoundRobinBatch(_), Partitioning::RoundRobinBatch(_)) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> PlanExpr {
        PlanExpr::Column {
            index: 0,
            name: name.to_string(),
            span: None,
        }
    }

    #[test]
    fn partition_count_returns_carried_n() {
        assert_eq!(Partitioning::UnknownPartitioning(14).partition_count(), 14);
        assert_eq!(Partitioning::RoundRobinBatch(4).partition_count(), 4);
        assert_eq!(Partitioning::Hash(vec![col("k")], 8).partition_count(), 8);
    }

    #[test]
    fn round_robin_satisfies_unknown_same_count() {
        assert!(Partitioning::RoundRobinBatch(14).satisfies(&Partitioning::UnknownPartitioning(14)));
    }

    #[test]
    fn hash_satisfies_unknown_same_count() {
        assert!(Partitioning::Hash(vec![col("k")], 14)
            .satisfies(&Partitioning::UnknownPartitioning(14)));
    }

    #[test]
    fn hash_satisfies_hash_when_keys_equal() {
        let a = Partitioning::Hash(vec![col("k")], 14);
        let b = Partitioning::Hash(vec![col("k")], 14);
        assert!(a.satisfies(&b));
    }

    #[test]
    fn hash_with_different_keys_does_not_satisfy() {
        let a = Partitioning::Hash(vec![col("a")], 14);
        let b = Partitioning::Hash(vec![col("b")], 14);
        assert!(!a.satisfies(&b));
    }

    #[test]
    fn mismatched_partition_count_never_satisfies() {
        assert!(!Partitioning::RoundRobinBatch(4).satisfies(&Partitioning::UnknownPartitioning(14)));
        assert!(!Partitioning::Hash(vec![col("k")], 4)
            .satisfies(&Partitioning::Hash(vec![col("k")], 14)));
    }

    #[test]
    fn unknown_does_not_satisfy_hash() {
        // A plain `UnknownPartitioning(N)` does NOT satisfy a `Hash(_, N)`
        // requirement — the planner must insert a `RepartitionExec(Hash)`.
        assert!(!Partitioning::UnknownPartitioning(14)
            .satisfies(&Partitioning::Hash(vec![col("k")], 14)));
    }

    #[test]
    fn default_unknown_partitioning_one_satisfies_itself() {
        let p = Partitioning::UnknownPartitioning(1);
        assert!(p.satisfies(&p));
    }
}
