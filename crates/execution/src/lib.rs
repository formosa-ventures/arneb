#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![deny(unsafe_code)]

//! Execution engine for the arneb query engine.
//!
//! Evaluates [`LogicalPlan`] trees against actual data, producing Arrow
//! [`RecordBatch`] results. This crate defines the [`DataSource`] trait
//! for data providers and converts logical plans into physical execution
//! operators.

mod aggregate;
pub mod coalesce;
mod datasource;
pub mod distributed;
pub mod dynamic_filter_collector;
pub mod dynamic_filter_publisher;
mod expression;
mod fast_hash;
pub mod functions;
mod group_by_hash;
mod group_key;
mod hash_join;
pub mod inflight_budget;
/// Re-export of the memory-budget framework, relocated to `arneb_common`
/// so non-execution crates (e.g. `arneb_rpc`) can account allocations
/// against the same pool. Internal `crate::memory_pool::*` paths and
/// external `arneb_execution::memory_pool::*` paths keep working.
pub use arneb_common::memory_pool;
mod operator;
mod optimizer;
pub mod partitioning;
mod planner;
pub mod repartition;
pub mod spill;
// query_coordinator lives in server crate to avoid circular deps
mod scalar_subquery;
mod scan_context;
mod semi_join;
mod set_ops;
mod window;

pub use aggregate::{
    Accumulator, AvgAccumulator, CountAccumulator, GroupedAccumulator, GroupedAvgAccumulator,
    GroupedCountAccumulator, GroupedMaxAccumulator, GroupedMinAccumulator, GroupedSumAccumulator,
    MaxAccumulator, MinAccumulator, SumAccumulator,
};
pub use coalesce::CoalescePartitionsExec;
pub use datasource::{column_info_to_arrow_schema, DataSource, InMemoryDataSource};
pub use dynamic_filter_collector::DynamicFilterCollector;
pub use dynamic_filter_publisher::{
    build_partition_domain_for_column, DynamicFilterPublisher, DynamicFilterPublisherRef,
};
pub use functions::{default_registry, FunctionRegistry, ScalarFunction};
pub use operator::ExecutionPlan;
pub use optimizer::{OptimizationRule, PhysicalPlanOptimizer};
pub use partitioning::Partitioning;
pub use planner::{prune_for_columns, ExecutionContext, DEFAULT_DYNAMIC_FILTERING_WAIT_TIMEOUT};
pub use repartition::RepartitionExec;
// QueryCoordinator is in the server crate (avoids execution↔rpc cycle)
pub use scan_context::{ConnectorCapabilities, DynamicFilterDomain, ScanContext};
