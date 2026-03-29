#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Execution engine for the trino-alt query engine.
//!
//! Evaluates [`LogicalPlan`] trees against actual data, producing Arrow
//! [`RecordBatch`] results. This crate defines the [`DataSource`] trait
//! for data providers and converts logical plans into physical execution
//! operators.

mod aggregate;
mod datasource;
pub mod distributed;
mod expression;
pub mod functions;
mod hash_join;
mod operator;
mod optimizer;
mod planner;
mod scalar_subquery;
mod scan_context;
mod semi_join;
mod set_ops;
mod window;

pub use aggregate::{
    Accumulator, AvgAccumulator, CountAccumulator, MaxAccumulator, MinAccumulator, SumAccumulator,
};
pub use datasource::{DataSource, InMemoryDataSource};
pub use functions::{default_registry, FunctionRegistry, ScalarFunction};
pub use operator::ExecutionPlan;
pub use optimizer::{OptimizationRule, PhysicalPlanOptimizer};
pub use planner::ExecutionContext;
pub use scan_context::{ConnectorCapabilities, ScanContext};
