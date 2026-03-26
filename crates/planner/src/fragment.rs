//! Plan fragmentation for distributed execution.
//!
//! Splits an optimized [`LogicalPlan`] into distributable fragments separated
//! by exchange boundaries. Each fragment can execute independently on a worker.

use std::fmt;

use trino_common::identifiers::StageId;

use crate::plan::LogicalPlan;

// ===========================================================================
// PartitioningScheme
// ===========================================================================

/// How data is redistributed between fragments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitioningScheme {
    /// All data goes to a single node (gather).
    Single,
    /// Data is hash-partitioned by the given column indices.
    Hash { columns: Vec<usize> },
    /// Data is distributed evenly across nodes.
    RoundRobin,
    /// Data is replicated to all nodes.
    Broadcast,
}

impl fmt::Display for PartitioningScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single => write!(f, "SINGLE"),
            Self::Hash { columns } => write!(f, "HASH({columns:?})"),
            Self::RoundRobin => write!(f, "ROUND_ROBIN"),
            Self::Broadcast => write!(f, "BROADCAST"),
        }
    }
}

// ===========================================================================
// FragmentType
// ===========================================================================

/// Classification of a plan fragment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FragmentType {
    /// Reads from a data source (leaf fragment).
    Source,
    /// Runs as a single instance (e.g., final aggregation, coordinator output).
    Fixed,
    /// Distributed by hash partitioning.
    HashPartitioned,
    /// Distributed round-robin.
    RoundRobin,
}

impl fmt::Display for FragmentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => write!(f, "SOURCE"),
            Self::Fixed => write!(f, "FIXED"),
            Self::HashPartitioned => write!(f, "HASH_PARTITIONED"),
            Self::RoundRobin => write!(f, "ROUND_ROBIN"),
        }
    }
}

// ===========================================================================
// PlanFragment
// ===========================================================================

/// A distributable unit of a query plan.
///
/// Each fragment contains a sub-tree of the logical plan that can execute
/// on a single node. Exchange boundaries separate fragments.
#[derive(Debug, Clone)]
pub struct PlanFragment {
    /// Unique stage identifier.
    pub id: StageId,
    /// Classification of this fragment.
    pub fragment_type: FragmentType,
    /// The root logical plan node for this fragment.
    pub root: LogicalPlan,
    /// How this fragment's output is partitioned.
    pub output_partitioning: PartitioningScheme,
    /// Child fragments that feed into this fragment via exchanges.
    pub source_fragments: Vec<PlanFragment>,
}

impl fmt::Display for PlanFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Fragment[id={}, type={}, output={}, sources={}]",
            self.id,
            self.fragment_type,
            self.output_partitioning,
            self.source_fragments.len()
        )
    }
}

// ===========================================================================
// QueryStage
// ===========================================================================

/// Scheduling metadata for a fragment.
#[derive(Debug, Clone)]
pub struct QueryStage {
    /// The fragment to execute.
    pub fragment: PlanFragment,
    /// Desired parallelism (number of tasks).
    pub parallelism: usize,
}

// ===========================================================================
// PlanFragmenter
// ===========================================================================

/// Splits an optimized logical plan into a tree of fragments.
///
/// Rules:
/// - Each `TableScan` becomes a SOURCE fragment.
/// - Each side of a `Join` becomes a separate fragment (exchange boundary).
/// - `Aggregate` is split into PartialAggregate + Exchange + FinalAggregate.
/// - The root fragment is FIXED (coordinator output).
pub struct PlanFragmenter {
    next_stage_id: u32,
}

impl PlanFragmenter {
    /// Creates a new fragmenter.
    pub fn new() -> Self {
        Self { next_stage_id: 0 }
    }

    fn next_id(&mut self) -> StageId {
        let id = StageId(self.next_stage_id);
        self.next_stage_id += 1;
        id
    }

    /// Fragments the given logical plan into a tree of [`PlanFragment`]s.
    pub fn fragment(&mut self, plan: LogicalPlan) -> PlanFragment {
        let (root_plan, source_fragments) = self.split(plan);
        PlanFragment {
            id: self.next_id(),
            fragment_type: FragmentType::Fixed,
            root: root_plan,
            output_partitioning: PartitioningScheme::Single,
            source_fragments,
        }
    }

    /// Recursively split the plan. Returns (local plan, child fragments).
    fn split(&mut self, plan: LogicalPlan) -> (LogicalPlan, Vec<PlanFragment>) {
        match plan {
            LogicalPlan::TableScan {
                table,
                schema,
                alias,
            } => {
                // TableScan becomes a SOURCE fragment.
                let scan_plan = LogicalPlan::TableScan {
                    table: table.clone(),
                    schema: schema.clone(),
                    alias: alias.clone(),
                };
                let fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::Source,
                    root: scan_plan,
                    output_partitioning: PartitioningScheme::RoundRobin,
                    source_fragments: vec![],
                };
                // Replace with an ExchangeNode placeholder in the parent.
                let exchange = LogicalPlan::ExchangeNode {
                    stage_id: fragment.id,
                    schema: schema.clone(),
                };
                (exchange, vec![fragment])
            }

            LogicalPlan::Filter { input, predicate } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Filter {
                        input: Box::new(input_plan),
                        predicate,
                    },
                    frags,
                )
            }

            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Projection {
                        input: Box::new(input_plan),
                        exprs,
                        schema,
                    },
                    frags,
                )
            }

            LogicalPlan::Sort { input, order_by } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Sort {
                        input: Box::new(input_plan),
                        order_by,
                    },
                    frags,
                )
            }

            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Limit {
                        input: Box::new(input_plan),
                        limit,
                        offset,
                    },
                    frags,
                )
            }

            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                let (left_plan, mut left_frags) = self.split(*left);
                let (right_plan, mut right_frags) = self.split(*right);
                left_frags.append(&mut right_frags);
                (
                    LogicalPlan::Join {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                        join_type,
                        condition,
                    },
                    left_frags,
                )
            }

            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => {
                // Two-phase aggregation: Partial → Exchange(HASH on group keys) → Final
                let (input_plan, input_frags) = self.split(*input);

                if group_by.is_empty() {
                    // No group-by: single-phase is fine (gather to one node).
                    return (
                        LogicalPlan::Aggregate {
                            input: Box::new(input_plan),
                            group_by,
                            aggr_exprs,
                            schema,
                        },
                        input_frags,
                    );
                }

                // Create partial aggregate fragment.
                let partial_plan = LogicalPlan::PartialAggregate {
                    input: Box::new(input_plan),
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    schema: schema.clone(),
                };

                let group_col_indices: Vec<usize> = (0..group_by.len()).collect();
                let partial_fragment = PlanFragment {
                    id: self.next_id(),
                    fragment_type: FragmentType::HashPartitioned,
                    root: partial_plan,
                    output_partitioning: PartitioningScheme::Hash {
                        columns: group_col_indices,
                    },
                    source_fragments: input_frags,
                };

                // Exchange node for the partial aggregate output.
                let exchange = LogicalPlan::ExchangeNode {
                    stage_id: partial_fragment.id,
                    schema: schema.clone(),
                };

                // Final aggregate reads from the exchange.
                let final_plan = LogicalPlan::FinalAggregate {
                    input: Box::new(exchange),
                    group_by,
                    aggr_exprs,
                    schema,
                };

                (final_plan, vec![partial_fragment])
            }

            LogicalPlan::Explain { input } => {
                let (input_plan, frags) = self.split(*input);
                (
                    LogicalPlan::Explain {
                        input: Box::new(input_plan),
                    },
                    frags,
                )
            }

            // Pass through nodes that don't need fragmentation.
            other => (other, vec![]),
        }
    }
}

impl Default for PlanFragmenter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanExpr;
    use trino_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
    use trino_sql_parser::ast;

    fn scan(name: &str) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema: vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
        }
    }

    #[test]
    fn fragment_simple_scan() {
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(scan("t"));
        // Root is FIXED, with one SOURCE child.
        assert_eq!(result.fragment_type, FragmentType::Fixed);
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].fragment_type,
            FragmentType::Source
        );
        assert!(matches!(result.root, LogicalPlan::ExchangeNode { .. }));
    }

    #[test]
    fn fragment_filter_scan() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan("t")),
            predicate: PlanExpr::Literal(ScalarValue::Boolean(true)),
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        assert_eq!(result.fragment_type, FragmentType::Fixed);
        assert_eq!(result.source_fragments.len(), 1);
        assert!(matches!(result.root, LogicalPlan::Filter { .. }));
    }

    #[test]
    fn fragment_join_creates_two_source_fragments() {
        let plan = LogicalPlan::Join {
            left: Box::new(scan("left_table")),
            right: Box::new(scan("right_table")),
            join_type: ast::JoinType::Inner,
            condition: crate::plan::JoinCondition::None,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Root fragment should have 2 source fragments (one per table).
        assert_eq!(result.source_fragments.len(), 2);
    }

    #[test]
    fn fragment_aggregate_with_group_by_creates_two_phase() {
        let schema = vec![
            ColumnInfo {
                name: "key".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "count".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![PlanExpr::Column {
                index: 0,
                name: "key".into(),
            }],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![],
                distinct: false,
            }],
            schema,
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Root has FinalAggregate, with one HashPartitioned child (partial agg).
        assert!(matches!(result.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(result.source_fragments.len(), 1);
        assert_eq!(
            result.source_fragments[0].fragment_type,
            FragmentType::HashPartitioned
        );
        // The partial fragment has one SOURCE child.
        assert_eq!(result.source_fragments[0].source_fragments.len(), 1);
    }

    #[test]
    fn fragment_aggregate_no_group_by_stays_single_phase() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan("t")),
            group_by: vec![],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".into(),
                args: vec![],
                distinct: false,
            }],
            schema: vec![ColumnInfo {
                name: "count".into(),
                data_type: DataType::Int64,
                nullable: false,
            }],
        };
        let mut frag = PlanFragmenter::new();
        let result = frag.fragment(plan);
        // Should remain single-phase Aggregate (not split).
        assert!(matches!(result.root, LogicalPlan::Aggregate { .. }));
    }

    #[test]
    fn partitioning_scheme_display() {
        assert_eq!(PartitioningScheme::Single.to_string(), "SINGLE");
        assert_eq!(
            PartitioningScheme::Hash {
                columns: vec![0, 1]
            }
            .to_string(),
            "HASH([0, 1])"
        );
        assert_eq!(PartitioningScheme::RoundRobin.to_string(), "ROUND_ROBIN");
        assert_eq!(PartitioningScheme::Broadcast.to_string(), "BROADCAST");
    }

    #[test]
    fn fragment_type_display() {
        assert_eq!(FragmentType::Source.to_string(), "SOURCE");
        assert_eq!(FragmentType::Fixed.to_string(), "FIXED");
    }

    #[test]
    fn plan_fragment_display() {
        let frag = PlanFragment {
            id: StageId(0),
            fragment_type: FragmentType::Source,
            root: scan("t"),
            output_partitioning: PartitioningScheme::RoundRobin,
            source_fragments: vec![],
        };
        assert!(frag.to_string().contains("Fragment[id=0"));
    }
}
