//! Query planner for the trino-alt query engine.
//!
//! Converts parsed SQL AST into a logical query plan tree that the
//! optimizer and execution engine consume.

pub mod fragment;
mod optimizer;
mod plan;
mod planner;

pub use fragment::{FragmentType, PartitioningScheme, PlanFragment, PlanFragmenter, QueryStage};
pub use optimizer::{LogicalOptimizer, LogicalRule};
pub use plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr};
pub use planner::QueryPlanner;
