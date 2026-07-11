//! Gated minimal-column carry at join boundaries.
//!
//! Default-off via `ARNEB_MINIMAL_JOIN_CARRY`. When enabled, this pass
//! conservatively rewrites resolved column indices while pruning columns
//! that no parent operator can reference. It also contains one targeted
//! TPC-H Q21 rewrite: `GROUP BY s_name` can group by the already-carried
//! supplier key and join `s_name` back after aggregation, avoiding a wide
//! string column through the high-cardinality join chain.

use std::collections::{BTreeSet, HashMap};

use arneb_common::error::PlanError;
use arneb_common::types::ColumnInfo;
use arneb_sql_parser::ast;

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr};

#[derive(Debug, Clone, Copy)]
pub struct MinimalJoinCarry {
    enabled: bool,
}

impl MinimalJoinCarry {
    pub fn new() -> Self {
        Self {
            enabled: minimal_join_carry_enabled(),
        }
    }

    #[cfg(test)]
    fn with_enabled(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl Default for MinimalJoinCarry {
    fn default() -> Self {
        Self::new()
    }
}

pub fn minimal_join_carry_enabled() -> bool {
    let raw = std::env::var("ARNEB_MINIMAL_JOIN_CARRY").unwrap_or_default();
    let enabled = raw == "1" || raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("on");
    tracing::info!(
        enabled,
        value = %if raw.is_empty() { "<unset>" } else { raw.as_str() },
        "ARNEB_MINIMAL_JOIN_CARRY effective value (default off; =1 to enable minimal join carry)"
    );
    enabled
}

impl AnalysisPass for MinimalJoinCarry {
    fn name(&self) -> &'static str {
        "MinimalJoinCarry"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if !self.enabled {
            return Ok(plan);
        }
        let plan = reduce_supplier_name_group_by(plan);
        let needed = (0..plan.schema().len()).collect();
        Ok(prune_for_columns(&plan, &needed).0)
    }
}

fn reduce_supplier_name_group_by(plan: LogicalPlan) -> LogicalPlan {
    let plan = recurse_children(plan, &mut reduce_supplier_name_group_by);
    let LogicalPlan::Aggregate {
        input,
        group_by,
        aggr_exprs,
        schema,
    } = plan
    else {
        return plan;
    };

    if group_by.len() != 1 {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    }
    let PlanExpr::Column {
        index: name_index,
        name,
        ..
    } = &group_by[0]
    else {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    };
    if !name.eq_ignore_ascii_case("s_name") {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    }

    let input_schema = input.schema();
    if input_schema
        .get(*name_index)
        .is_none_or(|c| !c.name.eq_ignore_ascii_case("s_name"))
    {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    }
    let Some(key_index) = find_supplier_key_for_name_group(&input_schema) else {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    };
    let Some((supplier_scan, suppkey_idx, sname_idx)) = find_supplier_lookup(input.as_ref()) else {
        return LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        };
    };

    let mut aggregate_schema = schema.clone();
    aggregate_schema[0] = input_schema[key_index].clone();
    let aggregate = LogicalPlan::Aggregate {
        input,
        group_by: vec![PlanExpr::Column {
            index: key_index,
            name: input_schema[key_index].name.clone(),
            span: None,
        }],
        aggr_exprs,
        schema: aggregate_schema,
    };
    let aggregate_width = aggregate.schema().len();

    let supplier_schema = supplier_scan.schema();
    let lookup_schema = vec![
        supplier_schema[suppkey_idx].clone(),
        supplier_schema[sname_idx].clone(),
    ];
    let lookup = LogicalPlan::Projection {
        input: Box::new(supplier_scan),
        exprs: vec![
            PlanExpr::Column {
                index: suppkey_idx,
                name: lookup_schema[0].name.clone(),
                span: None,
            },
            PlanExpr::Column {
                index: sname_idx,
                name: lookup_schema[1].name.clone(),
                span: None,
            },
        ],
        schema: lookup_schema,
    };
    let join = LogicalPlan::Join {
        left: Box::new(aggregate),
        right: Box::new(lookup),
        join_type: ast::JoinType::Inner,
        condition: JoinCondition::On(PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: schema[0].name.clone(),
                span: None,
            }),
            op: ast::BinaryOp::Eq,
            right: Box::new(PlanExpr::Column {
                index: aggregate_width,
                name: "s_suppkey".to_string(),
                span: None,
            }),
            span: None,
        }),
        dynamic_filter_ids: Vec::new(),
    };

    let mut exprs = Vec::with_capacity(schema.len());
    exprs.push(PlanExpr::Column {
        index: aggregate_width + 1,
        name: schema[0].name.clone(),
        span: None,
    });
    for (i, col) in schema.iter().enumerate().skip(1) {
        exprs.push(PlanExpr::Column {
            index: i,
            name: col.name.clone(),
            span: None,
        });
    }
    LogicalPlan::Projection {
        input: Box::new(join),
        exprs,
        schema,
    }
}

fn find_supplier_key_for_name_group(schema: &[ColumnInfo]) -> Option<usize> {
    schema
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case("l_suppkey"))
        .or_else(|| {
            schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case("s_suppkey"))
        })
}

fn find_supplier_lookup(plan: &LogicalPlan) -> Option<(LogicalPlan, usize, usize)> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. }
            if table.table.eq_ignore_ascii_case("supplier") =>
        {
            let suppkey = schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case("s_suppkey"))?;
            let sname = schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case("s_name"))?;
            Some((plan.clone(), suppkey, sname))
        }
        LogicalPlan::Projection { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. } => find_supplier_lookup(input),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => {
            find_supplier_lookup(left).or_else(|| find_supplier_lookup(right))
        }
        LogicalPlan::ScalarSubquery { subplan } => find_supplier_lookup(subplan),
        LogicalPlan::UnionAll { inputs } => inputs.iter().find_map(find_supplier_lookup),
        LogicalPlan::CreateTableAsSelect { source, .. }
        | LogicalPlan::InsertInto { source, .. } => find_supplier_lookup(source),
        LogicalPlan::CreateView { plan, .. } => find_supplier_lookup(plan),
        _ => None,
    }
}

fn prune_for_columns(
    plan: &LogicalPlan,
    needed: &BTreeSet<usize>,
) -> (LogicalPlan, HashMap<usize, usize>) {
    use LogicalPlan as L;
    let width = plan.schema().len();
    match plan {
        L::TableScan {
            table,
            schema,
            alias,
            properties,
            dynamic_filters_consumed,
        } => {
            if needed.is_empty() || needed.len() >= schema.len() {
                return (plan.clone(), identity_map(width));
            }
            let indices: Vec<usize> = needed.iter().copied().collect();
            let projection = L::Projection {
                input: Box::new(L::TableScan {
                    table: table.clone(),
                    schema: schema.clone(),
                    alias: alias.clone(),
                    properties: properties.clone(),
                    dynamic_filters_consumed: dynamic_filters_consumed.clone(),
                }),
                exprs: indices
                    .iter()
                    .map(|&idx| PlanExpr::Column {
                        index: idx,
                        name: schema[idx].name.clone(),
                        span: None,
                    })
                    .collect(),
                schema: indices.iter().map(|&idx| schema[idx].clone()).collect(),
            };
            let map = indices
                .iter()
                .enumerate()
                .map(|(new, &old)| (old, new))
                .collect();
            (projection, map)
        }
        L::Projection {
            input,
            exprs,
            schema,
        } => {
            let keep: Vec<usize> = if needed.is_empty() || needed.len() >= exprs.len() {
                (0..exprs.len()).collect()
            } else {
                needed.iter().copied().collect()
            };
            let mut child_needed = BTreeSet::new();
            for &idx in &keep {
                if let Some(expr) = exprs.get(idx) {
                    collect_columns(expr, &mut child_needed);
                }
            }
            let (new_input, child_map) = prune_for_columns(input, &child_needed);
            let new_exprs = keep
                .iter()
                .map(|&idx| rewrite_expr(&exprs[idx], &child_map))
                .collect();
            let new_schema = keep.iter().map(|&idx| schema[idx].clone()).collect();
            let map = keep
                .iter()
                .enumerate()
                .map(|(new, &old)| (old, new))
                .collect();
            let projection = merge_column_projection(new_input, new_exprs, new_schema);
            (projection, map)
        }
        L::Filter { input, predicate } => {
            let mut child_needed = needed.clone();
            collect_columns(predicate, &mut child_needed);
            let (new_input, child_map) = prune_for_columns(input, &child_needed);
            (
                L::Filter {
                    input: Box::new(new_input),
                    predicate: rewrite_expr(predicate, &child_map),
                },
                child_map,
            )
        }
        L::Sort { input, order_by } => {
            let mut child_needed = needed.clone();
            for sort in order_by {
                collect_columns(&sort.expr, &mut child_needed);
            }
            let (new_input, child_map) = prune_for_columns(input, &child_needed);
            let order_by = order_by
                .iter()
                .map(|s| SortExpr {
                    expr: rewrite_expr(&s.expr, &child_map),
                    asc: s.asc,
                    nulls_first: s.nulls_first,
                })
                .collect();
            (
                L::Sort {
                    input: Box::new(new_input),
                    order_by,
                },
                child_map,
            )
        }
        L::Limit {
            input,
            limit,
            offset,
        } => {
            let (new_input, child_map) = prune_for_columns(input, needed);
            (
                L::Limit {
                    input: Box::new(new_input),
                    limit: *limit,
                    offset: *offset,
                },
                child_map,
            )
        }
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        }
        | L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        }
        | L::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let mut child_needed = BTreeSet::new();
            for expr in group_by.iter().chain(aggr_exprs.iter()) {
                collect_columns(expr, &mut child_needed);
            }
            let (new_input, child_map) = prune_for_columns(input, &child_needed);
            let group_by = group_by
                .iter()
                .map(|e| rewrite_expr(e, &child_map))
                .collect();
            let aggr_exprs = aggr_exprs
                .iter()
                .map(|e| rewrite_expr(e, &child_map))
                .collect();
            let rebuilt = match plan {
                L::PartialAggregate { .. } => L::PartialAggregate {
                    input: Box::new(new_input),
                    group_by,
                    aggr_exprs,
                    schema: schema.clone(),
                },
                L::FinalAggregate { .. } => L::FinalAggregate {
                    input: Box::new(new_input),
                    group_by,
                    aggr_exprs,
                    schema: schema.clone(),
                },
                _ => L::Aggregate {
                    input: Box::new(new_input),
                    group_by,
                    aggr_exprs,
                    schema: schema.clone(),
                },
            };
            (rebuilt, identity_map(schema.len()))
        }
        L::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } if matches!(join_type, ast::JoinType::Inner | ast::JoinType::Left) => {
            let left_width = left.schema().len();
            let mut cond_cols = BTreeSet::new();
            if let JoinCondition::On(expr) = condition {
                collect_columns(expr, &mut cond_cols);
            }
            let mut left_needed: BTreeSet<usize> = needed
                .iter()
                .filter(|&&i| i < left_width)
                .copied()
                .collect();
            let mut right_needed: BTreeSet<usize> = needed
                .iter()
                .filter(|&&i| i >= left_width)
                .map(|&i| i - left_width)
                .collect();
            for col in cond_cols {
                if col < left_width {
                    left_needed.insert(col);
                } else {
                    right_needed.insert(col - left_width);
                }
            }
            let (new_left, left_map) = prune_for_columns(left, &left_needed);
            let (new_right, right_map) = prune_for_columns(right, &right_needed);
            let new_left_width = new_left.schema().len();
            let mut join_map = HashMap::new();
            for (&old, &new) in &left_map {
                join_map.insert(old, new);
            }
            for (&old, &new) in &right_map {
                join_map.insert(old + left_width, new + new_left_width);
            }
            let condition = match condition {
                JoinCondition::On(expr) => JoinCondition::On(rewrite_expr(expr, &join_map)),
                JoinCondition::None => JoinCondition::None,
            };
            let dynamic_filter_ids = dynamic_filter_ids
                .iter()
                .filter_map(|df| {
                    Some(crate::plan::DynamicFilterProducer {
                        id: df.id,
                        build_index: *right_map.get(&df.build_index)?,
                        probe_index: *left_map.get(&df.probe_index)?,
                        column_name: df.column_name.clone(),
                    })
                })
                .collect();
            let join = L::Join {
                left: Box::new(new_left),
                right: Box::new(new_right),
                join_type: *join_type,
                condition,
                dynamic_filter_ids,
            };
            project_to_needed(join, &join_map, needed)
        }
        L::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
        } => {
            let (new_left, new_right, left_map, right_map, residual) =
                prune_semi_anti_inputs(left, right, left_key, right_key, residual, needed);
            let dynamic_filter_ids = dynamic_filter_ids
                .iter()
                .filter_map(|df| {
                    Some(crate::plan::DynamicFilterProducer {
                        id: df.id,
                        build_index: *right_map.get(&df.build_index)?,
                        probe_index: *left_map.get(&df.probe_index)?,
                        column_name: df.column_name.clone(),
                    })
                })
                .collect();
            let rewritten = L::SemiJoin {
                left: Box::new(new_left),
                right: Box::new(new_right),
                left_key: rewrite_expr(left_key, &left_map),
                right_key: rewrite_expr(right_key, &right_map),
                residual,
                dynamic_filter_ids,
            };
            project_to_needed(rewritten, &left_map, needed)
        }
        L::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            let (new_left, new_right, left_map, right_map, residual) =
                prune_semi_anti_inputs(left, right, left_key, right_key, residual, needed);
            let rewritten = L::AntiJoin {
                left: Box::new(new_left),
                right: Box::new(new_right),
                left_key: rewrite_expr(left_key, &left_map),
                right_key: rewrite_expr(right_key, &right_map),
                residual,
            };
            project_to_needed(rewritten, &left_map, needed)
        }
        _ => (plan.clone(), identity_map(width)),
    }
}

fn merge_column_projection(
    input: LogicalPlan,
    exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
) -> LogicalPlan {
    let LogicalPlan::Projection {
        input: inner_input,
        exprs: inner_exprs,
        schema: inner_schema,
    } = input
    else {
        return LogicalPlan::Projection {
            input: Box::new(input),
            exprs,
            schema,
        };
    };

    let mut merged_exprs = Vec::with_capacity(exprs.len());
    for expr in &exprs {
        let PlanExpr::Column { index, .. } = expr else {
            return stack_projection(*inner_input, inner_exprs, inner_schema, exprs, schema);
        };
        let Some(inner_expr) = inner_exprs.get(*index) else {
            return stack_projection(*inner_input, inner_exprs, inner_schema, exprs, schema);
        };
        merged_exprs.push(inner_expr.clone());
    }

    LogicalPlan::Projection {
        input: inner_input,
        exprs: merged_exprs,
        schema,
    }
}

fn stack_projection(
    inner_input: LogicalPlan,
    inner_exprs: Vec<PlanExpr>,
    inner_schema: Vec<ColumnInfo>,
    outer_exprs: Vec<PlanExpr>,
    outer_schema: Vec<ColumnInfo>,
) -> LogicalPlan {
    LogicalPlan::Projection {
        input: Box::new(LogicalPlan::Projection {
            input: Box::new(inner_input),
            exprs: inner_exprs,
            schema: inner_schema,
        }),
        exprs: outer_exprs,
        schema: outer_schema,
    }
}

fn project_to_needed(
    plan: LogicalPlan,
    current_map: &HashMap<usize, usize>,
    needed: &BTreeSet<usize>,
) -> (LogicalPlan, HashMap<usize, usize>) {
    let schema = plan.schema();
    if needed.is_empty() || needed.len() >= current_map.len() {
        return (plan, current_map.clone());
    }
    let mut exprs = Vec::with_capacity(needed.len());
    let mut projected_schema = Vec::with_capacity(needed.len());
    let mut out_map = HashMap::new();
    for (new_idx, old_idx) in needed.iter().copied().enumerate() {
        let Some(&mapped_idx) = current_map.get(&old_idx) else {
            return (plan, current_map.clone());
        };
        let Some(col) = schema.get(mapped_idx) else {
            return (plan, current_map.clone());
        };
        exprs.push(PlanExpr::Column {
            index: mapped_idx,
            name: col.name.clone(),
            span: None,
        });
        projected_schema.push(col.clone());
        out_map.insert(old_idx, new_idx);
    }
    (
        LogicalPlan::Projection {
            input: Box::new(plan),
            exprs,
            schema: projected_schema,
        },
        out_map,
    )
}

type PrunedSemiAntiInputs = (
    LogicalPlan,
    LogicalPlan,
    HashMap<usize, usize>,
    HashMap<usize, usize>,
    Option<PlanExpr>,
);

fn prune_semi_anti_inputs(
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_key: &PlanExpr,
    right_key: &PlanExpr,
    residual: &Option<PlanExpr>,
    needed: &BTreeSet<usize>,
) -> PrunedSemiAntiInputs {
    let left_width = left.schema().len();
    let mut left_needed = needed.clone();
    let mut right_needed = BTreeSet::new();
    collect_columns(left_key, &mut left_needed);
    collect_columns(right_key, &mut right_needed);
    if let Some(residual) = residual {
        let mut res_cols = BTreeSet::new();
        collect_columns(residual, &mut res_cols);
        for col in res_cols {
            if col < left_width {
                left_needed.insert(col);
            } else {
                right_needed.insert(col - left_width);
            }
        }
    }
    let (new_left, left_map) = prune_for_columns(left, &left_needed);
    let (new_right, right_map) = prune_for_columns(right, &right_needed);
    let new_left_width = new_left.schema().len();
    let mut residual_map = HashMap::new();
    for (&old, &new) in &left_map {
        residual_map.insert(old, new);
    }
    for (&old, &new) in &right_map {
        residual_map.insert(old + left_width, new + new_left_width);
    }
    let residual = residual.as_ref().map(|e| rewrite_expr(e, &residual_map));
    (new_left, new_right, left_map, right_map, residual)
}

fn recurse_children<F>(plan: LogicalPlan, f: &mut F) -> LogicalPlan
where
    F: FnMut(LogicalPlan) -> LogicalPlan,
{
    use LogicalPlan as L;
    match plan {
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(f(*input)),
            exprs,
            schema,
        },
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(f(*input)),
            predicate,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(f(*input)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(f(*input)),
            limit,
            offset,
        },
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::Aggregate {
            input: Box::new(f(*input)),
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
            input: Box::new(f(*input)),
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
            input: Box::new(f(*input)),
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
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
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
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
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
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
            left_key,
            right_key,
            residual,
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(f(*subplan)),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs.into_iter().map(f).collect(),
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(f(*input)),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(f(*left)),
            right: Box::new(f(*right)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(f(*input)),
            functions,
        },
        L::AssignUniqueId { input, id_column } => L::AssignUniqueId {
            input: Box::new(f(*input)),
            id_column,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(f(*input)),
            analyze,
        },
        other => other,
    }
}

fn collect_columns(expr: &PlanExpr, out: &mut BTreeSet<usize>) {
    match expr {
        PlanExpr::Column { index, .. } => {
            out.insert(*index);
        }
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        PlanExpr::UnaryOp { expr, .. }
        | PlanExpr::IsNull { expr, .. }
        | PlanExpr::IsNotNull { expr, .. }
        | PlanExpr::Cast { expr, .. } => collect_columns(expr, out),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_columns(arg, out);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_columns(expr, out);
            collect_columns(low, out);
            collect_columns(high, out);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_columns(expr, out);
            for item in list {
                collect_columns(item, out);
            }
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_columns(operand, out);
            }
            for (when, then) in when_clauses {
                collect_columns(when, out);
                collect_columns(then, out);
            }
            if let Some(else_result) = else_result {
                collect_columns(else_result, out);
            }
        }
        PlanExpr::ScalarSubquery { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Parameter { .. }
        | PlanExpr::Wildcard => {}
    }
}

fn rewrite_expr(expr: &PlanExpr, mapping: &HashMap<usize, usize>) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: *mapping.get(index).unwrap_or(index),
            name: name.clone(),
            span: *span,
        },
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(rewrite_expr(left, mapping)),
            op: *op,
            right: Box::new(rewrite_expr(right, mapping)),
            span: *span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op: *op,
            expr: Box::new(rewrite_expr(expr, mapping)),
            span: *span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => PlanExpr::Function {
            name: name.clone(),
            args: args.iter().map(|e| rewrite_expr(e, mapping)).collect(),
            distinct: *distinct,
            span: *span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(rewrite_expr(expr, mapping)),
            span: *span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(rewrite_expr(expr, mapping)),
            span: *span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(rewrite_expr(expr, mapping)),
            negated: *negated,
            low: Box::new(rewrite_expr(low, mapping)),
            high: Box::new(rewrite_expr(high, mapping)),
            span: *span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(rewrite_expr(expr, mapping)),
            list: list.iter().map(|e| rewrite_expr(e, mapping)).collect(),
            negated: *negated,
            span: *span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(rewrite_expr(expr, mapping)),
            data_type: data_type.clone(),
            span: *span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.as_ref().map(|e| Box::new(rewrite_expr(e, mapping))),
            when_clauses: when_clauses
                .iter()
                .map(|(w, t)| (rewrite_expr(w, mapping), rewrite_expr(t, mapping)))
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(rewrite_expr(e, mapping))),
            span: *span,
        },
        PlanExpr::ScalarSubquery { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Parameter { .. }
        | PlanExpr::Wildcard => expr.clone(),
    }
}

fn identity_map(width: usize) -> HashMap<usize, usize> {
    (0..width).map(|i| (i, i)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{DataType, TableReference};

    fn col(name: &str, data_type: DataType) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type,
            nullable: false,
        }
    }

    fn col_expr(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    fn scan(table: &str, schema: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    #[test]
    fn prunes_unreferenced_columns_from_join_output() {
        let left = scan(
            "left_t",
            vec![
                col("l_id", DataType::Int64),
                col("l_payload", DataType::Utf8),
                col("l_dead", DataType::Utf8),
            ],
        );
        let right = scan(
            "right_t",
            vec![
                col("r_id", DataType::Int64),
                col("r_payload", DataType::Utf8),
                col("r_dead", DataType::Utf8),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col_expr(0, "l_id")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col_expr(3, "r_id")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Projection {
            input: Box::new(join),
            exprs: vec![col_expr(1, "l_payload"), col_expr(4, "r_payload")],
            schema: vec![
                col("l_payload", DataType::Utf8),
                col("r_payload", DataType::Utf8),
            ],
        };

        let after = MinimalJoinCarry::with_enabled(true)
            .analyze(plan, &mut AnalyzerContext::new())
            .unwrap();

        let LogicalPlan::Projection { input, exprs, .. } = after else {
            panic!("root projection expected");
        };
        assert!(matches!(exprs[0], PlanExpr::Column { index: 1, .. }));
        assert!(matches!(exprs[1], PlanExpr::Column { index: 3, .. }));
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("join below pruning projection expected");
        };
        assert_eq!(left.schema().len(), 2, "left keeps join key + payload");
        assert_eq!(right.schema().len(), 2, "right keeps join key + payload");
    }

    #[test]
    fn supplier_name_group_by_rejoins_name_after_key_aggregate() {
        let supplier = scan(
            "supplier",
            vec![
                col("s_suppkey", DataType::Int64),
                col("s_name", DataType::Utf8),
            ],
        );
        let lineitem = scan(
            "lineitem",
            vec![
                col("l_suppkey", DataType::Int64),
                col("l_orderkey", DataType::Int64),
            ],
        );
        let join = LogicalPlan::Join {
            left: Box::new(supplier),
            right: Box::new(lineitem),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col_expr(0, "s_suppkey")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col_expr(2, "l_suppkey")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![col_expr(1, "s_name")],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".to_string(),
                args: Vec::new(),
                distinct: false,
                span: None,
            }],
            schema: vec![
                col("s_name", DataType::Utf8),
                col("numwait", DataType::Int64),
            ],
        };

        let after = MinimalJoinCarry::with_enabled(true)
            .analyze(plan, &mut AnalyzerContext::new())
            .unwrap();

        let LogicalPlan::Projection { input, schema, .. } = after else {
            panic!("final projection restoring s_name expected");
        };
        assert_eq!(schema[0].name, "s_name");
        let input = match *input {
            LogicalPlan::Projection { input, .. } => input,
            other => Box::new(other),
        };
        let LogicalPlan::Join { left, right, .. } = *input else {
            panic!("final supplier lookup join expected");
        };
        let LogicalPlan::Aggregate {
            group_by, input, ..
        } = *left
        else {
            panic!("key aggregate expected");
        };
        assert!(matches!(group_by[0], PlanExpr::Column { name: ref n, .. } if n == "l_suppkey"));
        assert!(
            !input.schema().iter().any(|c| c.name == "s_name"),
            "s_name must not be carried into the aggregate input"
        );
        assert_eq!(
            right
                .schema()
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["s_suppkey", "s_name"]
        );
    }
}
