//! Query planner for the trino-alt query engine.
//!
//! Converts parsed SQL AST into a logical query plan tree that the
//! optimizer and execution engine consume.

mod plan;
mod planner;

pub use plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr};
pub use planner::QueryPlanner;
