//! Query planner for the arneb query engine.
//!
//! Converts parsed SQL AST into a logical query plan tree that the
//! optimizer and execution engine consume.

pub mod analyzer;
pub mod cost;
pub mod dynamic_filter;
pub mod explain;
pub mod fragment;
mod optimizer;
mod plan;
mod planner;
pub mod properties;
pub mod selectivity;

pub use analyzer::{parse_hints, AnalysisPass, Analyzer, AnalyzerContext, Hint, HintSet};
pub use cost::{
    estimate_row_width_bytes, estimated_bytes, estimated_cardinality, CatalogStats, Cost,
    DEFAULT_TABLE_SIZE,
};
pub use dynamic_filter::DynamicFilterIdAllocator;
pub use explain::format_plan_with_estimates;
pub use fragment::{
    Distribution, FragmentType, PartitioningScheme, PlanFragment, PlanFragmenter, QueryStage,
    DEFAULT_HASH_PARTITION_TARGET_ROWS, DEFAULT_MAX_HASH_PARTITIONS,
};
pub use optimizer::{LogicalOptimizer, LogicalRule};
pub use plan::{
    DynamicFilterConsumer, DynamicFilterProducer, JoinCondition, LogicalPlan, PlanExpr, SortExpr,
    WindowFunctionDef,
};
pub use planner::QueryPlanner;
pub use properties::ActualProperties;
pub use selectivity::{selectivity, ColumnStatsLookup, EmptyLookup};
