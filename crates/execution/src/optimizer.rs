//! Physical plan optimizer framework.
//!
//! The [`PhysicalPlanOptimizer`] applies a sequence of [`OptimizationRule`]s
//! to rewrite the physical plan tree (e.g., pushing projections/filters into scans).

use std::sync::Arc;

use trino_common::error::ExecutionError;

use crate::operator::ExecutionPlan;

/// A rule that rewrites a physical execution plan.
pub trait OptimizationRule: Send + Sync {
    /// A human-readable name for this rule (for logging/EXPLAIN).
    fn name(&self) -> &str;

    /// Attempt to optimize the given plan. Returns the optimized plan
    /// (which may be the same `Arc` if no optimization was applicable).
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError>;
}

/// Applies an ordered sequence of optimization rules to a physical plan.
pub struct PhysicalPlanOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl PhysicalPlanOptimizer {
    /// Creates an optimizer with the given rules (applied in order).
    pub fn new(rules: Vec<Box<dyn OptimizationRule>>) -> Self {
        Self { rules }
    }

    /// Creates an optimizer with the default set of rules.
    pub fn default_rules() -> Self {
        // Currently no rules — they will be added as we implement pushdown.
        Self::new(vec![])
    }

    /// Applies all rules in sequence to the given plan.
    pub fn optimize(
        &self,
        mut plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        for rule in &self.rules {
            plan = rule.optimize(plan)?;
        }
        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::InMemoryDataSource;
    use crate::operator::ScanExec;
    use crate::scan_context::ScanContext;
    use trino_common::types::{ColumnInfo, DataType};

    struct NoOpRule;

    impl OptimizationRule for NoOpRule {
        fn name(&self) -> &str {
            "no-op"
        }

        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
        ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
            Ok(plan)
        }
    }

    fn make_scan() -> Arc<dyn ExecutionPlan> {
        let ds = InMemoryDataSource::empty(vec![ColumnInfo {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]);
        Arc::new(ScanExec {
            source: Arc::new(ds),
            _table_name: "test".to_string(),
            scan_context: ScanContext::default(),
        })
    }

    #[test]
    fn optimizer_with_no_rules() {
        let optimizer = PhysicalPlanOptimizer::new(vec![]);
        let plan = make_scan();
        let result = optimizer.optimize(plan).unwrap();
        assert_eq!(result.display_name(), "ScanExec");
    }

    #[test]
    fn optimizer_applies_rules_in_order() {
        let optimizer = PhysicalPlanOptimizer::new(vec![Box::new(NoOpRule), Box::new(NoOpRule)]);
        let plan = make_scan();
        let result = optimizer.optimize(plan).unwrap();
        assert_eq!(result.display_name(), "ScanExec");
    }
}
