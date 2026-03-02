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
mod expression;
mod operator;
mod planner;

pub use aggregate::{
    Accumulator, AvgAccumulator, CountAccumulator, MaxAccumulator, MinAccumulator, SumAccumulator,
};
pub use datasource::{DataSource, InMemoryDataSource};
pub use operator::ExecutionPlan;
pub use planner::ExecutionContext;
