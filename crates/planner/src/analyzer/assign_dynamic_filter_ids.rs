//! `AssignDynamicFilterIds` analyzer pass.
//!
//! Walks the plan tree after `JoinReorder` and tags eligible joins with
//! [`DynamicFilterProducer`] annotations, plus matching probe-side
//! [`TableScan`]s with [`DynamicFilterConsumer`] annotations. This pass
//! does NOT change runtime behavior on its own — it only populates the
//! `dynamic_filter_ids` and `dynamic_filters_consumed` fields. Downstream
//! phases (A1.3 RPC, A1.4 scan-side wait, A1.5 build-side emit) consume
//! these annotations to drive the cross-fragment dynamic filter flow.
//!
//! ## Eligibility
//!
//! A join's equi-key produces a DF id when:
//! - `join_type == Inner` (LEFT/RIGHT/FULL OUTER and AntiJoin produce
//!   incompatible build-side semantics)
//! - the equi-key is `Column(L) = Column(R)` (no expressions on either
//!   side)
//! - the LEFT (probe) child's column at index `L` traces back through
//!   pass-through operators (Filter / Sort / Limit / no-op Projection)
//!   to a `TableScan` — required because the consumer annotation lives
//!   on the scan node itself
//!
//! SemiJoins follow the same rules using `left_key` / `right_key`.
//!
//! ## What's intentionally NOT here yet
//!
//! - Build-cardinality cap (`partitioned_max_distinct_values`, default
//!   20K). Adding the annotation has no runtime cost when the feature
//!   flag is off; the cap kicks in when A1.5 actually builds Domains.
//! - Multi-DF coalescing (same probe column from multiple joins).
//! - Tracing through ExchangeNode boundaries — handled later by the
//!   fragmenter / column-pruning pass when it preserves annotations.

use arneb_common::error::PlanError;
use arneb_sql_parser::ast::BinaryOp;
use std::sync::OnceLock;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::dynamic_filter::DynamicFilterIdAllocator;
use crate::plan::{
    DynamicFilterConsumer, DynamicFilterProducer, JoinCondition, LogicalPlan, PlanExpr,
};

#[cfg(test)]
static DF_THROUGH_JOINS_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static DF_THROUGH_JOINS_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct DfThroughJoinsOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for DfThroughJoinsOverride {
    fn drop(&mut self) {
        *DF_THROUGH_JOINS_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("DF_THROUGH_JOINS_TEST_OVERRIDE mutex poisoned") = None;
    }
}

#[cfg(test)]
pub(crate) fn set_df_through_joins_for_test(enabled: bool) -> DfThroughJoinsOverride {
    let guard = DF_THROUGH_JOINS_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("DF_THROUGH_JOINS_TEST_LOCK mutex poisoned");
    *DF_THROUGH_JOINS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("DF_THROUGH_JOINS_TEST_OVERRIDE mutex poisoned") = Some(enabled);
    DfThroughJoinsOverride { _guard: guard }
}

/// Pass that allocates [`DynamicFilterId`](arneb_common::DynamicFilterId)
/// values for eligible joins and tags both producer (join) and consumer
/// (scan) nodes.
#[derive(Debug, Default)]
pub struct AssignDynamicFilterIds;

impl AssignDynamicFilterIds {
    /// Returns a new instance of the pass.
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for AssignDynamicFilterIds {
    fn name(&self) -> &'static str {
        "AssignDynamicFilterIds"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        let allocator = DynamicFilterIdAllocator::new();
        Ok(rewrite(plan, &allocator))
    }
}

// ---------------------------------------------------------------------------
// Walker
// ---------------------------------------------------------------------------

fn rewrite(plan: LogicalPlan, alloc: &DynamicFilterIdAllocator) -> LogicalPlan {
    // Recurse bottom-up so nested joins are tagged first; the LEFT child
    // we tag for each parent join is already post-rewrite.
    let plan = recurse_children(plan, alloc);
    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } if matches!(join_type, arneb_sql_parser::ast::JoinType::Inner)
            && dynamic_filter_ids.is_empty() =>
        {
            assign_for_join(*left, *right, join_type, condition, alloc)
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } if dynamic_filter_ids.is_empty() && residual.is_none() => {
            assign_for_semi_join(*left, *right, left_key, right_key, alloc)
        }
        other => other,
    }
}

fn recurse_children(plan: LogicalPlan, alloc: &DynamicFilterIdAllocator) -> LogicalPlan {
    use LogicalPlan as L;
    match plan {
        L::TableScan { .. } | L::ExchangeNode { .. } | L::OneRow => plan,
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(rewrite(*input, alloc)),
            exprs,
            schema,
        },
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(rewrite(*input, alloc)),
            predicate,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(rewrite(*input, alloc)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(rewrite(*input, alloc)),
            limit,
            offset,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(rewrite(*input, alloc)),
            analyze,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(rewrite(*input, alloc)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(rewrite(*input, alloc)),
            functions,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(rewrite(*input, alloc)),
            id_column,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(rewrite(*input, alloc)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(rewrite(*input, alloc)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::FinalAggregate {
            input: Box::new(rewrite(*input, alloc)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => L::Join {
            left: Box::new(rewrite(*left, alloc)),
            right: Box::new(rewrite(*right, alloc)),
            join_type,
            condition,
            dynamic_filter_ids,
        },
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => L::SemiJoin {
            left: Box::new(rewrite(*left, alloc)),
            right: Box::new(rewrite(*right, alloc)),
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        },
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => L::AntiJoin {
            left: Box::new(rewrite(*left, alloc)),
            right: Box::new(rewrite(*right, alloc)),
            left_key,
            right_key,
            residual,
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(rewrite(*subplan, alloc)),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs.into_iter().map(|p| rewrite(p, alloc)).collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(rewrite(*left, alloc)),
            right: Box::new(rewrite(*right, alloc)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(rewrite(*left, alloc)),
            right: Box::new(rewrite(*right, alloc)),
        },
        L::CreateTableAsSelect { name, source } => L::CreateTableAsSelect {
            name,
            source: Box::new(rewrite(*source, alloc)),
        },
        L::InsertInto { table, source } => L::InsertInto {
            table,
            source: Box::new(rewrite(*source, alloc)),
        },
        L::CreateView { name, sql, plan } => L::CreateView {
            name,
            sql,
            plan: Box::new(rewrite(*plan, alloc)),
        },
        L::CreateTable { .. } | L::DropTable { .. } | L::DeleteFrom { .. } | L::DropView { .. } => {
            plan
        }
    }
}

// ---------------------------------------------------------------------------
// Per-join assignment
// ---------------------------------------------------------------------------

pub(crate) fn assign_for_join(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: arneb_sql_parser::ast::JoinType,
    condition: JoinCondition,
    alloc: &DynamicFilterIdAllocator,
) -> LogicalPlan {
    let equi_pairs = extract_pure_column_equi_pairs(&condition, left.schema().len());
    let left_schema = left.schema();
    let right_schema = right.schema();

    let mut producers = Vec::new();
    let mut new_left = left;
    for (left_idx, right_idx) in equi_pairs {
        let id = alloc.allocate();
        let column_name = right_schema
            .get(right_idx)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("col_{right_idx}"));
        // Try to tag the probe-side TableScan with the consumer
        // annotation. If we can't trace the column down to a single
        // scan (e.g. it traces through an aggregate or set op), drop
        // the annotation on the producer side too — without a matching
        // consumer there's no point producing the DF.
        match tag_consumer(new_left, left_idx, id, column_name.clone()) {
            Ok(tagged) => {
                new_left = tagged;
                producers.push(DynamicFilterProducer {
                    id,
                    build_index: right_idx,
                    probe_index: left_idx,
                    column_name,
                });
            }
            Err(untagged) => {
                new_left = untagged;
            }
        }
    }

    let _ = left_schema; // suppress unused if no equi_pairs
    LogicalPlan::Join {
        left: Box::new(new_left),
        right: Box::new(right),
        join_type,
        condition,
        dynamic_filter_ids: producers,
    }
}

fn assign_for_semi_join(
    left: LogicalPlan,
    right: LogicalPlan,
    left_key: PlanExpr,
    right_key: PlanExpr,
    alloc: &DynamicFilterIdAllocator,
) -> LogicalPlan {
    let (
        PlanExpr::Column {
            index: left_idx, ..
        },
        PlanExpr::Column {
            index: right_idx,
            name,
            ..
        },
    ) = (&left_key, &right_key)
    else {
        return LogicalPlan::SemiJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key,
            right_key,
            residual: None,
            dynamic_filter_ids: Vec::new(),
        };
    };
    let (left_idx, right_idx, name) = (*left_idx, *right_idx, name.clone());

    let id = alloc.allocate();
    let (new_left, producers) = match tag_consumer(left, left_idx, id, name.clone()) {
        Ok(tagged) => (
            tagged,
            vec![DynamicFilterProducer {
                id,
                build_index: right_idx,
                probe_index: left_idx,
                column_name: name,
            }],
        ),
        Err(untagged) => (untagged, Vec::new()),
    };

    LogicalPlan::SemiJoin {
        left: Box::new(new_left),
        right: Box::new(right),
        left_key,
        right_key,
        residual: None,
        dynamic_filter_ids: producers,
    }
}

// ---------------------------------------------------------------------------
// Column → TableScan tracing
// ---------------------------------------------------------------------------

fn df_through_joins_enabled() -> bool {
    #[cfg(test)]
    if let Some(override_value) = DF_THROUGH_JOINS_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("DF_THROUGH_JOINS_TEST_OVERRIDE mutex poisoned")
        .as_ref()
    {
        return *override_value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_DF_THROUGH_JOINS").is_ok_and(|v| v == "1");
        tracing::info!(
            target: "arneb::config",
            ARNEB_DF_THROUGH_JOINS = enabled,
            "dynamic-filter tracing through joins configured"
        );
        enabled
    })
}

/// Tries to attach a [`DynamicFilterConsumer`] annotation to the
/// `TableScan` that produces the column at `idx` in `plan`'s output
/// schema. Returns `Ok(modified_plan)` on success, `Err(original_plan)`
/// when the column traces through a node we don't yet know how to
/// remap (Aggregate, UnionAll, Join, ExchangeNode, etc.).
#[allow(clippy::result_large_err)]
fn tag_consumer(
    plan: LogicalPlan,
    idx: usize,
    id: arneb_common::DynamicFilterId,
    column_name: String,
) -> Result<LogicalPlan, LogicalPlan> {
    use LogicalPlan as L;
    match plan {
        L::TableScan {
            table,
            schema,
            alias,
            properties,
            mut dynamic_filters_consumed,
        } => {
            dynamic_filters_consumed.push(DynamicFilterConsumer {
                id,
                column_index: idx,
                column_name,
            });
            Ok(L::TableScan {
                table,
                schema,
                alias,
                properties,
                dynamic_filters_consumed,
            })
        }
        L::Filter { input, predicate } => match tag_consumer(*input, idx, id, column_name) {
            Ok(tagged) => Ok(L::Filter {
                input: Box::new(tagged),
                predicate,
            }),
            Err(untagged) => Err(L::Filter {
                input: Box::new(untagged),
                predicate,
            }),
        },
        L::Sort { input, order_by } => match tag_consumer(*input, idx, id, column_name) {
            Ok(tagged) => Ok(L::Sort {
                input: Box::new(tagged),
                order_by,
            }),
            Err(untagged) => Err(L::Sort {
                input: Box::new(untagged),
                order_by,
            }),
        },
        L::Projection {
            input,
            exprs,
            schema,
        } => match exprs.get(idx) {
            Some(PlanExpr::Column {
                index: child_idx, ..
            }) => {
                let ci = *child_idx;
                match tag_consumer(*input, ci, id, column_name) {
                    Ok(tagged) => Ok(L::Projection {
                        input: Box::new(tagged),
                        exprs,
                        schema,
                    }),
                    Err(untagged) => Err(L::Projection {
                        input: Box::new(untagged),
                        exprs,
                        schema,
                    }),
                }
            }
            _ => Err(L::Projection {
                input,
                exprs,
                schema,
            }),
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } if matches!(join_type, arneb_sql_parser::ast::JoinType::Inner)
            && df_through_joins_enabled() =>
        {
            let left_width = left.schema().len();
            if idx < left_width {
                match tag_consumer(*left, idx, id, column_name) {
                    Ok(tagged_left) => Ok(L::Join {
                        left: Box::new(tagged_left),
                        right,
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    }),
                    Err(untagged_left) => Err(L::Join {
                        left: Box::new(untagged_left),
                        right,
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    }),
                }
            } else {
                match tag_consumer(*right, idx - left_width, id, column_name) {
                    Ok(tagged_right) => Ok(L::Join {
                        left,
                        right: Box::new(tagged_right),
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    }),
                    Err(untagged_right) => Err(L::Join {
                        left,
                        right: Box::new(untagged_right),
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    }),
                }
            }
        }
        // Operators we can't trace through cleanly — give up and
        // return the unmodified plan.
        other => Err(other),
    }
}

// ---------------------------------------------------------------------------
// Equi-key extraction (Column = Column only)
// ---------------------------------------------------------------------------

/// Returns `(left_idx, right_idx_in_right_schema)` pairs for every
/// `Column(L) = Column(R)` conjunct in the join condition.
fn extract_pure_column_equi_pairs(
    condition: &JoinCondition,
    left_col_count: usize,
) -> Vec<(usize, usize)> {
    let JoinCondition::On(expr) = condition else {
        return Vec::new();
    };
    let mut out = Vec::new();
    collect(expr, left_col_count, &mut out);
    out
}

fn collect(expr: &PlanExpr, left_col_count: usize, out: &mut Vec<(usize, usize)>) {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect(left, left_col_count, out);
            collect(right, left_col_count, out);
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            if let (PlanExpr::Column { index: l, .. }, PlanExpr::Column { index: r, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if *l < left_col_count && *r >= left_col_count {
                    out.push((*l, *r - left_col_count));
                } else if *r < left_col_count && *l >= left_col_count {
                    out.push((*r, *l - left_col_count));
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, DataType, TableReference};
    use arneb_sql_parser::ast;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan(name: &str, columns: Vec<&str>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(name),
            schema: columns.into_iter().map(col).collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col_expr(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    fn eq_expr(left_idx: usize, left_name: &str, right_idx: usize, right_name: &str) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(col_expr(left_idx, left_name)),
            op: BinaryOp::Eq,
            right: Box::new(col_expr(right_idx, right_name)),
            span: None,
        }
    }

    fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: ast::JoinType,
        condition: JoinCondition,
    ) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            dynamic_filter_ids: Vec::new(),
        }
    }

    fn scan_consumers(plan: &LogicalPlan, table_name: &str) -> Vec<DynamicFilterConsumer> {
        match plan {
            LogicalPlan::TableScan {
                table,
                dynamic_filters_consumed,
                ..
            } if table.table == table_name => dynamic_filters_consumed.clone(),
            LogicalPlan::TableScan { .. } => Vec::new(),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Projection { input, .. } => scan_consumers(input, table_name),
            LogicalPlan::Join { left, right, .. } => {
                let mut consumers = scan_consumers(left, table_name);
                consumers.extend(scan_consumers(right, table_name));
                consumers
            }
            other => panic!("unexpected plan in scan_consumers: {other:?}"),
        }
    }

    #[test]
    fn df_through_inner_join_gate_on_tags_deep_left_scan() {
        let _override = set_df_through_joins_for_test(true);
        let lower = join(
            scan("a", vec!["ak", "a_payload"]),
            scan("b", vec!["bk"]),
            ast::JoinType::Inner,
            JoinCondition::None,
        );
        let plan = join(
            lower,
            scan("c", vec!["ck"]),
            ast::JoinType::Inner,
            JoinCondition::On(eq_expr(0, "ak", 3, "ck")),
        );

        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Join {
            dynamic_filter_ids, ..
        } = &rewritten
        else {
            panic!("expected Join at root");
        };
        assert_eq!(dynamic_filter_ids.len(), 1);
        assert_eq!(dynamic_filter_ids[0].probe_index, 0);
        assert_eq!(dynamic_filter_ids[0].build_index, 0);

        let consumers = scan_consumers(&rewritten, "a");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].column_index, 0);
        assert_eq!(consumers[0].column_name, "ck");
    }

    #[test]
    fn df_through_inner_join_gate_off_does_not_tag_deep_scan() {
        let _override = set_df_through_joins_for_test(false);
        let lower = join(
            scan("a", vec!["ak", "a_payload"]),
            scan("b", vec!["bk"]),
            ast::JoinType::Inner,
            JoinCondition::None,
        );
        let plan = join(
            lower,
            scan("c", vec!["ck"]),
            ast::JoinType::Inner,
            JoinCondition::On(eq_expr(0, "ak", 3, "ck")),
        );

        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Join {
            dynamic_filter_ids, ..
        } = &rewritten
        else {
            panic!("expected Join at root");
        };
        assert!(dynamic_filter_ids.is_empty());
        assert!(scan_consumers(&rewritten, "a").is_empty());
    }

    #[test]
    fn df_through_left_join_gate_on_does_not_trace() {
        let _override = set_df_through_joins_for_test(true);
        let lower = join(
            scan("a", vec!["ak", "a_payload"]),
            scan("b", vec!["bk"]),
            ast::JoinType::Left,
            JoinCondition::None,
        );
        let plan = join(
            lower,
            scan("c", vec!["ck"]),
            ast::JoinType::Inner,
            JoinCondition::On(eq_expr(0, "ak", 3, "ck")),
        );

        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Join {
            dynamic_filter_ids, ..
        } = &rewritten
        else {
            panic!("expected Join at root");
        };
        assert!(dynamic_filter_ids.is_empty());
        assert!(scan_consumers(&rewritten, "a").is_empty());
    }

    #[test]
    fn df_through_inner_join_remaps_right_child_index() {
        let _override = set_df_through_joins_for_test(true);
        let lower = join(
            scan("a", vec!["ak"]),
            scan("b", vec!["bk", "b_payload"]),
            ast::JoinType::Inner,
            JoinCondition::None,
        );
        let plan = join(
            lower,
            scan("c", vec!["ck"]),
            ast::JoinType::Inner,
            JoinCondition::On(eq_expr(1, "bk", 3, "ck")),
        );

        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        let consumers = scan_consumers(&rewritten, "b");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].column_index, 0);
        assert_eq!(consumers[0].column_name, "ck");
        assert!(scan_consumers(&rewritten, "a").is_empty());
    }

    #[test]
    fn inner_join_with_column_equi_key_gets_annotated() {
        // SELECT * FROM nation INNER JOIN supplier ON nation.nationkey = supplier.nationkey
        // nation: [nationkey, name]; supplier: [suppkey, name, nationkey]
        let nation = scan("nation", vec!["nationkey", "name"]);
        let supplier = scan("supplier", vec!["suppkey", "name", "nationkey"]);
        // left has 2 cols, so right's nationkey lives at joined-schema index 2+2=4
        let condition = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(col_expr(0, "nationkey")),
            op: BinaryOp::Eq,
            right: Box::new(col_expr(4, "nationkey")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(nation),
            right: Box::new(supplier),
            join_type: ast::JoinType::Inner,
            condition,
            dynamic_filter_ids: Vec::new(),
        };

        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Join {
            left,
            dynamic_filter_ids,
            ..
        } = rewritten
        else {
            panic!("expected Join at root");
        };
        assert_eq!(dynamic_filter_ids.len(), 1);
        let p = &dynamic_filter_ids[0];
        assert_eq!(p.probe_index, 0);
        assert_eq!(p.build_index, 2); // supplier.nationkey is right-side col index 2
        assert_eq!(p.column_name, "nationkey");

        let LogicalPlan::TableScan {
            dynamic_filters_consumed,
            ..
        } = *left
        else {
            panic!("expected TableScan on left");
        };
        assert_eq!(dynamic_filters_consumed.len(), 1);
        assert_eq!(dynamic_filters_consumed[0].column_index, 0);
        assert_eq!(dynamic_filters_consumed[0].column_name, "nationkey");
    }

    #[test]
    fn outer_join_not_annotated() {
        let nation = scan("nation", vec!["nationkey", "name"]);
        let supplier = scan("supplier", vec!["nationkey"]);
        let condition = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(col_expr(0, "nationkey")),
            op: BinaryOp::Eq,
            right: Box::new(col_expr(2, "nationkey")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(nation),
            right: Box::new(supplier),
            join_type: ast::JoinType::Left,
            condition,
            dynamic_filter_ids: Vec::new(),
        };
        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();
        let LogicalPlan::Join {
            dynamic_filter_ids, ..
        } = rewritten
        else {
            panic!("expected Join");
        };
        assert!(
            dynamic_filter_ids.is_empty(),
            "LEFT OUTER joins should not produce DF"
        );
    }

    #[test]
    fn trace_through_filter_to_scan() {
        let scan_plan = scan("orders", vec!["orderkey", "custkey"]);
        let filtered = LogicalPlan::Filter {
            input: Box::new(scan_plan),
            predicate: PlanExpr::Literal {
                value: arneb_common::types::ScalarValue::Boolean(true),
                span: None,
            },
        };
        let supplier = scan("customer", vec!["custkey"]);
        let condition = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(col_expr(1, "custkey")),
            op: BinaryOp::Eq,
            right: Box::new(col_expr(2, "custkey")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(filtered),
            right: Box::new(supplier),
            join_type: ast::JoinType::Inner,
            condition,
            dynamic_filter_ids: Vec::new(),
        };
        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();

        // Producer should be present on the Join.
        let LogicalPlan::Join {
            left,
            dynamic_filter_ids,
            ..
        } = rewritten
        else {
            panic!();
        };
        assert_eq!(dynamic_filter_ids.len(), 1);

        // Consumer should be on the TableScan beneath the Filter.
        let LogicalPlan::Filter { input, .. } = *left else {
            panic!("expected Filter on left");
        };
        let LogicalPlan::TableScan {
            dynamic_filters_consumed,
            ..
        } = *input
        else {
            panic!("expected TableScan beneath Filter");
        };
        assert_eq!(dynamic_filters_consumed.len(), 1);
    }

    #[test]
    fn non_column_equi_key_skipped() {
        let nation = scan("nation", vec!["nationkey"]);
        let supplier = scan("supplier", vec!["nationkey"]);
        // ON nation.nationkey + 1 = supplier.nationkey — not pure column equi
        let condition = JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::BinaryOp {
                left: Box::new(col_expr(0, "nationkey")),
                op: BinaryOp::Plus,
                right: Box::new(PlanExpr::Literal {
                    value: arneb_common::types::ScalarValue::Int64(1),
                    span: None,
                }),
                span: None,
            }),
            op: BinaryOp::Eq,
            right: Box::new(col_expr(1, "nationkey")),
            span: None,
        });
        let plan = LogicalPlan::Join {
            left: Box::new(nation),
            right: Box::new(supplier),
            join_type: ast::JoinType::Inner,
            condition,
            dynamic_filter_ids: Vec::new(),
        };
        let pass = AssignDynamicFilterIds::new();
        let mut ctx = AnalyzerContext::default();
        let rewritten = pass.analyze(plan, &mut ctx).unwrap();
        let LogicalPlan::Join {
            dynamic_filter_ids, ..
        } = rewritten
        else {
            panic!();
        };
        assert!(dynamic_filter_ids.is_empty());
    }
}
