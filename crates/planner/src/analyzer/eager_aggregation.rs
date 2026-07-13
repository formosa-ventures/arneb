//! Gated eager aggregation through joins.
//!
//! This pass handles the conservative TPC-H Q13 shape:
//!
//! ```text
//! Aggregate(group_by = preserved join key, aggr = COUNT/SUM/MIN/MAX(other side col))
//!   Join(Inner|Left, preserved, other, preserved.key = other.key)
//! ```
//!
//! It pre-aggregates the non-preserved side by its join key so the
//! large one-to-many join output is never materialized. A top
//! aggregate remains to preserve correctness when the preserved side
//! itself has duplicate join keys.

use std::sync::OnceLock;

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, ScalarValue};
use arneb_sql_parser::ast::{self, BinaryOp};

use crate::analyzer::{AnalysisPass, AnalyzerContext};
use crate::cost::{estimated_cardinality, CatalogStats};
use crate::plan::{DynamicFilterProducer, JoinCondition, LogicalPlan, PlanExpr};

pub struct EagerAggregation;

impl EagerAggregation {
    pub fn new() -> Self {
        Self
    }
}

impl Default for EagerAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalysisPass for EagerAggregation {
    fn name(&self) -> &'static str {
        "EagerAggregation"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        if !eager_aggregation_enabled() {
            return Ok(plan);
        }
        Ok(rewrite(plan, ctx.catalog_stats.as_ref()))
    }
}

fn eager_aggregation_enabled() -> bool {
    #[cfg(test)]
    if let Some(enabled) = eager_aggregation_test_override() {
        return enabled;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_EAGER_AGG")
            .map(|v| v == "1")
            .unwrap_or(false);
        tracing::info!(
            target: "arneb::config",
            ARNEB_EAGER_AGG = enabled,
            "ARNEB_EAGER_AGG effective value (default off; =1 to enable eager aggregation)"
        );
        enabled
    })
}

#[cfg(test)]
static EAGER_AGG_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> = OnceLock::new();
#[cfg(test)]
static EAGER_AGG_TEST_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct EagerAggregationOverride {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for EagerAggregationOverride {
    fn drop(&mut self) {
        *EAGER_AGG_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("eager aggregation test override lock poisoned") = None;
    }
}

#[cfg(test)]
fn eager_aggregation_test_override() -> Option<bool> {
    *EAGER_AGG_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("eager aggregation test override lock poisoned")
}

#[cfg(test)]
fn set_eager_aggregation_for_test(enabled: bool) -> EagerAggregationOverride {
    let guard = EAGER_AGG_TEST_LOCK
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("eager aggregation test lock poisoned");
    *EAGER_AGG_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("eager aggregation test override lock poisoned") = Some(enabled);
    EagerAggregationOverride { _guard: guard }
}

fn rewrite(plan: LogicalPlan, stats: &CatalogStats) -> LogicalPlan {
    use LogicalPlan as L;

    match plan {
        L::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = rewrite(*input, stats);
            try_rewrite_aggregate(
                input.clone(),
                group_by.clone(),
                aggr_exprs.clone(),
                schema.clone(),
                stats,
            )
            .unwrap_or(L::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        L::Filter { input, predicate } => L::Filter {
            input: Box::new(rewrite(*input, stats)),
            predicate,
        },
        L::Projection {
            input,
            exprs,
            schema,
        } => L::Projection {
            input: Box::new(rewrite(*input, stats)),
            exprs,
            schema,
        },
        L::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => L::PartialAggregate {
            input: Box::new(rewrite(*input, stats)),
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
            input: Box::new(rewrite(*input, stats)),
            group_by,
            aggr_exprs,
            schema,
        },
        L::Sort { input, order_by } => L::Sort {
            input: Box::new(rewrite(*input, stats)),
            order_by,
        },
        L::Limit {
            input,
            limit,
            offset,
        } => L::Limit {
            input: Box::new(rewrite(*input, stats)),
            limit,
            offset,
        },
        L::Distinct { input } => L::Distinct {
            input: Box::new(rewrite(*input, stats)),
        },
        L::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => L::Join {
            left: Box::new(rewrite(*left, stats)),
            right: Box::new(rewrite(*right, stats)),
            join_type,
            condition,
            dynamic_filter_ids: Vec::new(),
        },
        L::UnionAll { inputs } => L::UnionAll {
            inputs: inputs
                .into_iter()
                .map(|input| rewrite(input, stats))
                .collect(),
        },
        L::Intersect { left, right } => L::Intersect {
            left: Box::new(rewrite(*left, stats)),
            right: Box::new(rewrite(*right, stats)),
        },
        L::Except { left, right } => L::Except {
            left: Box::new(rewrite(*left, stats)),
            right: Box::new(rewrite(*right, stats)),
        },
        L::ScalarSubquery { subplan } => L::ScalarSubquery {
            subplan: Box::new(rewrite(*subplan, stats)),
        },
        L::Window { input, functions } => L::Window {
            input: Box::new(rewrite(*input, stats)),
            functions,
        },
        L::Explain { input, analyze } => L::Explain {
            input: Box::new(rewrite(*input, stats)),
            analyze,
        },
        L::CreateTableAsSelect { name, source } => L::CreateTableAsSelect {
            name,
            source: Box::new(rewrite(*source, stats)),
        },
        L::InsertInto { table, source } => L::InsertInto {
            table,
            source: Box::new(rewrite(*source, stats)),
        },
        L::CreateView { name, sql, plan } => L::CreateView {
            name,
            sql,
            plan: Box::new(rewrite(*plan, stats)),
        },
        other => other,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

#[derive(Clone)]
struct JoinKeys {
    left: PlanExpr,
    right: PlanExpr,
}

#[derive(Clone)]
struct JoinAnalysis {
    keys: JoinKeys,
    residuals: Vec<PlanExpr>,
}

fn try_rewrite_aggregate(
    input: LogicalPlan,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
    stats: &CatalogStats,
) -> Option<LogicalPlan> {
    if let Some(input) = unwrap_identity_projection(input.clone()) {
        return try_rewrite_join_aggregate(input, group_by, aggr_exprs, schema, stats);
    }

    if let LogicalPlan::SemiJoin {
        left,
        right,
        left_key,
        right_key,
        residual: None,
        dynamic_filter_ids,
    } = input.clone()
    {
        return try_rewrite_semijoin_left_aggregate(
            *left,
            *right,
            left_key,
            right_key,
            dynamic_filter_ids,
            group_by,
            aggr_exprs,
            schema,
            stats,
        );
    }

    let LogicalPlan::Projection {
        input,
        exprs,
        schema: projection_schema,
    } = input
    else {
        return None;
    };
    if !is_simple_column_projection(&exprs) {
        return None;
    }

    let remapped_group_by = group_by
        .iter()
        .map(|expr| remap_expr_through_projection(expr, &exprs))
        .collect::<Option<Vec<_>>>()?;
    let remapped_aggr_exprs = aggr_exprs
        .iter()
        .map(|expr| remap_expr_through_projection(expr, &exprs))
        .collect::<Option<Vec<_>>>()?;

    let rewritten = try_rewrite_join_aggregate(
        *input,
        remapped_group_by.clone(),
        remapped_aggr_exprs.clone(),
        schema.clone(),
        stats,
    )?;
    rewrap_simple_projection_rewrite(
        rewritten,
        exprs,
        projection_schema,
        group_by,
        aggr_exprs,
        remapped_group_by,
        remapped_aggr_exprs,
        schema,
    )
}

#[allow(clippy::too_many_arguments)]
fn try_rewrite_semijoin_left_aggregate(
    left: LogicalPlan,
    right: LogicalPlan,
    left_key: PlanExpr,
    right_key: PlanExpr,
    dynamic_filter_ids: Vec<DynamicFilterProducer>,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
    stats: &CatalogStats,
) -> Option<LogicalPlan> {
    let rewritten =
        try_rewrite_join_aggregate(left, group_by.clone(), aggr_exprs, schema.clone(), stats)?;
    let LogicalPlan::Aggregate {
        input,
        group_by: final_group_by,
        aggr_exprs: final_aggr_exprs,
        ..
    } = rewritten
    else {
        return None;
    };
    let semijoin_left_key = group_by
        .iter()
        .position(|group_expr| expr_eq(group_expr, &left_key))
        .and_then(|index| {
            Some(PlanExpr::Column {
                index,
                name: column_name(&left_key)?,
                span: None,
            })
        })?;

    Some(LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::SemiJoin {
            left: input,
            right: Box::new(right),
            left_key: semijoin_left_key,
            right_key,
            residual: None,
            dynamic_filter_ids,
        }),
        group_by: final_group_by,
        aggr_exprs: final_aggr_exprs,
        schema,
    })
}

fn column_name(expr: &PlanExpr) -> Option<String> {
    match expr {
        PlanExpr::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

fn try_rewrite_join_aggregate(
    input: LogicalPlan,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
    stats: &CatalogStats,
) -> Option<LogicalPlan> {
    let LogicalPlan::Join {
        left,
        right,
        join_type,
        condition,
        ..
    } = input
    else {
        return None;
    };

    if group_by.is_empty() || aggr_exprs.is_empty() {
        return None;
    }
    if !matches!(join_type, ast::JoinType::Inner | ast::JoinType::Left) {
        return None;
    }

    let left_schema = left.schema();
    let right_schema = right.schema();
    let left_width = left_schema.len();
    let join_analysis = analyze_join_condition(&condition, left_width, right_schema.len())?;

    if matches!(join_type, ast::JoinType::Left) {
        return build_rewrite(
            *left,
            *right,
            join_type,
            group_by,
            aggr_exprs,
            schema,
            left_schema,
            right_schema,
            left_width,
            join_analysis,
            Side::Left,
            Side::Right,
            stats,
        );
    }

    build_rewrite(
        *left.clone(),
        *right.clone(),
        join_type,
        group_by.clone(),
        aggr_exprs.clone(),
        schema.clone(),
        left_schema.clone(),
        right_schema.clone(),
        left_width,
        join_analysis.clone(),
        Side::Left,
        Side::Right,
        stats,
    )
    .or_else(|| {
        build_rewrite(
            *left,
            *right,
            join_type,
            group_by,
            aggr_exprs,
            schema,
            left_schema,
            right_schema,
            left_width,
            join_analysis,
            Side::Right,
            Side::Left,
            stats,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn rewrap_simple_projection_rewrite(
    rewritten: LogicalPlan,
    projection_exprs: Vec<PlanExpr>,
    projection_schema: Vec<ColumnInfo>,
    original_group_by: Vec<PlanExpr>,
    original_aggr_exprs: Vec<PlanExpr>,
    remapped_group_by: Vec<PlanExpr>,
    remapped_aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
) -> Option<LogicalPlan> {
    let LogicalPlan::Aggregate {
        input,
        group_by: final_group_by,
        aggr_exprs: final_aggr_exprs,
        ..
    } = rewritten
    else {
        return None;
    };
    if final_group_by.len() != original_group_by.len()
        || final_aggr_exprs.len() != original_aggr_exprs.len()
    {
        return None;
    }
    let rewritten_input_schema = input.schema();

    let mut projection_over_rewrite = Vec::with_capacity(projection_exprs.len());
    for (projection_index, projection_expr) in projection_exprs.iter().enumerate() {
        let rewritten_index = remapped_group_by
            .iter()
            .position(|group_expr| expr_eq(projection_expr, group_expr))
            .or_else(|| {
                remapped_aggr_exprs
                    .iter()
                    .position(|aggr_expr| {
                        aggregate_single_column_arg_eq(aggr_expr, projection_expr)
                    })
                    .map(|aggr_index| remapped_group_by.len() + aggr_index)
            })
            .or_else(|| {
                let projected_type = &projection_schema[projection_index].data_type;
                rewritten_input_schema
                    .iter()
                    .position(|col| &col.data_type == projected_type)
            })?;
        projection_over_rewrite.push(PlanExpr::Column {
            index: rewritten_index,
            name: projection_schema[projection_index].name.clone(),
            span: None,
        });
    }

    let final_aggr_exprs = final_aggr_exprs
        .into_iter()
        .map(|expr| {
            remap_expr_columns(&expr, &mut |index| {
                let projection_index = if index < remapped_group_by.len() {
                    original_group_by.get(index).and_then(column_index)
                } else {
                    let aggr_index = index - remapped_group_by.len();
                    let arg = original_aggr_exprs.get(aggr_index)?;
                    aggregate_single_column_arg_index(arg)
                }?;
                Some(PlanExpr::Column {
                    index: projection_index,
                    name: projection_schema[projection_index].name.clone(),
                    span: None,
                })
            })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(LogicalPlan::Aggregate {
        input: Box::new(LogicalPlan::Projection {
            input,
            exprs: projection_over_rewrite,
            schema: projection_schema,
        }),
        group_by: original_group_by,
        aggr_exprs: final_aggr_exprs,
        schema,
    })
}

#[derive(Clone)]
enum GroupKeyMatch {
    Exact,
    PreservedSuperset,
}

#[allow(clippy::too_many_arguments)]
fn build_rewrite(
    left: LogicalPlan,
    right: LogicalPlan,
    join_type: ast::JoinType,
    group_by: Vec<PlanExpr>,
    aggr_exprs: Vec<PlanExpr>,
    schema: Vec<ColumnInfo>,
    left_schema: Vec<ColumnInfo>,
    right_schema: Vec<ColumnInfo>,
    left_width: usize,
    join_analysis: JoinAnalysis,
    preserved_side: Side,
    preagg_side: Side,
    stats: &CatalogStats,
) -> Option<LogicalPlan> {
    let JoinAnalysis {
        keys: join_keys,
        residuals,
    } = join_analysis;
    let preserved_key = match preserved_side {
        Side::Left => &join_keys.left,
        Side::Right => &join_keys.right,
    };
    let preagg_key = match preagg_side {
        Side::Left => &join_keys.left,
        Side::Right => &join_keys.right,
    };

    let group_key_match = match_group_keys(
        &group_by,
        preserved_key,
        preserved_side,
        preagg_side,
        left_width,
        right_schema.len(),
    )?;
    if matches!(group_key_match, GroupKeyMatch::PreservedSuperset)
        && !preserved_side_unique_on_key(
            match preserved_side {
                Side::Left => &left,
                Side::Right => &right,
            },
            preserved_key,
            preserved_side,
            left_width,
            stats,
        )
    {
        return None;
    }

    let preagg_key_index = side_column_index(preagg_key, preagg_side, left_width)?;
    let preagg_schema = match preagg_side {
        Side::Left => &left_schema,
        Side::Right => &right_schema,
    };
    let mut partial_aggr_exprs = Vec::with_capacity(aggr_exprs.len());
    let mut partial_aggr_kinds = Vec::with_capacity(aggr_exprs.len());
    for agg in &aggr_exprs {
        let kind = classify_aggregate(agg)?;
        if aggregate_arg_side(agg, left_width)? != preagg_side {
            return None;
        }
        partial_aggr_exprs.push(remap_expr_to_side(agg, preagg_side, left_width)?);
        partial_aggr_kinds.push(kind);
    }

    let preagg_filter_predicate = residuals
        .iter()
        .map(|predicate| {
            if expr_side(predicate, left_width, right_schema.len())? != preagg_side {
                return None;
            }
            remap_expr_to_side(predicate, preagg_side, left_width)
        })
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .reduce(and_expr);

    let preagg_key_col = preagg_schema.get(preagg_key_index)?.clone();
    let preagg_group_by = vec![PlanExpr::Column {
        index: preagg_key_index,
        name: preagg_key_col.name.clone(),
        span: None,
    }];
    let mut preagg_output_schema = vec![preagg_key_col];
    preagg_output_schema.extend(schema.iter().skip(group_by.len()).cloned());

    let (preserved_input, preagg_input) = match (preserved_side, preagg_side) {
        (Side::Left, Side::Right) => (left, right),
        (Side::Right, Side::Left) => (right, left),
        _ => return None,
    };
    let preagg_input = if let Some(predicate) = preagg_filter_predicate {
        LogicalPlan::Filter {
            input: Box::new(preagg_input),
            predicate,
        }
    } else {
        preagg_input
    };
    let preaggregated = LogicalPlan::Aggregate {
        input: Box::new(preagg_input),
        group_by: preagg_group_by,
        aggr_exprs: partial_aggr_exprs,
        schema: preagg_output_schema,
    };

    let (new_left, new_right, new_condition, new_join_left_width) = match preagg_side {
        Side::Right => {
            let cond = eq_expr(
                preserved_key.clone(),
                PlanExpr::Column {
                    index: left_width,
                    name: preagg_schema[preagg_key_index].name.clone(),
                    span: None,
                },
            );
            (preserved_input, preaggregated, cond, left_width)
        }
        Side::Left => {
            let cond = eq_expr(
                PlanExpr::Column {
                    index: 0,
                    name: preagg_schema[preagg_key_index].name.clone(),
                    span: None,
                },
                remap_expr_from_join_to_side(preserved_key, Side::Right, left_width)?,
            );
            (preaggregated, preserved_input, cond, 1 + aggr_exprs.len())
        }
    };

    let join = LogicalPlan::Join {
        left: Box::new(new_left),
        right: Box::new(new_right),
        join_type,
        condition: JoinCondition::On(new_condition),
        dynamic_filter_ids: Vec::new(),
    };

    let mut projected_schema = Vec::with_capacity(schema.len());
    projected_schema.extend(schema.iter().take(group_by.len()).cloned());
    projected_schema.extend(schema.iter().skip(group_by.len()).cloned());

    let mut proj_exprs = Vec::with_capacity(schema.len());
    for group_expr in &group_by {
        proj_exprs.push(project_preserved_expr_after_join(
            group_expr,
            preserved_side,
            preagg_side,
            left_width,
            new_join_left_width,
        )?);
    }
    for (i, kind) in partial_aggr_kinds.iter().enumerate() {
        let partial_idx = match preagg_side {
            Side::Right => new_join_left_width + 1 + i,
            Side::Left => 1 + i,
        };
        let col = PlanExpr::Column {
            index: partial_idx,
            name: schema[group_by.len() + i].name.clone(),
            span: None,
        };
        if matches!(join_type, ast::JoinType::Left) && matches!(kind, AggKind::Count) {
            proj_exprs.push(zero_if_null(col));
        } else {
            proj_exprs.push(col);
        }
    }

    let projected = LogicalPlan::Projection {
        input: Box::new(join),
        exprs: proj_exprs,
        schema: projected_schema.clone(),
    };

    let final_group_by = schema
        .iter()
        .take(group_by.len())
        .enumerate()
        .map(|(index, col)| PlanExpr::Column {
            index,
            name: col.name.clone(),
            span: None,
        })
        .collect();
    let final_aggr_exprs = partial_aggr_kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            let idx = group_by.len() + i;
            let name = match kind {
                AggKind::Count | AggKind::Sum => "SUM",
                AggKind::Min => "MIN",
                AggKind::Max => "MAX",
            };
            PlanExpr::Function {
                name: name.to_string(),
                args: vec![PlanExpr::Column {
                    index: idx,
                    name: projected_schema[idx].name.clone(),
                    span: None,
                }],
                distinct: false,
                span: None,
            }
        })
        .collect();

    Some(LogicalPlan::Aggregate {
        input: Box::new(projected),
        group_by: final_group_by,
        aggr_exprs: final_aggr_exprs,
        schema,
    })
}

fn unwrap_identity_projection(input: LogicalPlan) -> Option<LogicalPlan> {
    match input {
        LogicalPlan::Join { .. } => Some(input),
        LogicalPlan::Projection { input, exprs, .. } if is_identity_projection(&exprs) => {
            Some(*input)
        }
        _ => None,
    }
}

fn is_simple_column_projection(exprs: &[PlanExpr]) -> bool {
    exprs
        .iter()
        .all(|expr| matches!(expr, PlanExpr::Column { .. }))
}

fn remap_expr_through_projection(
    expr: &PlanExpr,
    projection_exprs: &[PlanExpr],
) -> Option<PlanExpr> {
    remap_expr_columns(expr, &mut |index| projection_exprs.get(index).cloned())
}

fn remap_expr_columns<F>(expr: &PlanExpr, f: &mut F) -> Option<PlanExpr>
where
    F: FnMut(usize) -> Option<PlanExpr>,
{
    Some(match expr {
        PlanExpr::Column { index, .. } => f(*index)?,
        PlanExpr::Literal { value, .. } => PlanExpr::Literal {
            value: value.clone(),
            span: None,
        },
        PlanExpr::Parameter {
            index,
            type_hint,
            span,
        } => PlanExpr::Parameter {
            index: *index,
            type_hint: type_hint.clone(),
            span: *span,
        },
        PlanExpr::Wildcard => PlanExpr::Wildcard,
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_expr_columns(left, f)?),
            op: *op,
            right: Box::new(remap_expr_columns(right, f)?),
            span: None,
        },
        PlanExpr::UnaryOp { op, expr, .. } => PlanExpr::UnaryOp {
            op: *op,
            expr: Box::new(remap_expr_columns(expr, f)?),
            span: None,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            ..
        } => PlanExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_expr_columns(arg, f))
                .collect::<Option<Vec<_>>>()?,
            distinct: *distinct,
            span: None,
        },
        PlanExpr::IsNull { expr, .. } => PlanExpr::IsNull {
            expr: Box::new(remap_expr_columns(expr, f)?),
            span: None,
        },
        PlanExpr::IsNotNull { expr, .. } => PlanExpr::IsNotNull {
            expr: Box::new(remap_expr_columns(expr, f)?),
            span: None,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            ..
        } => PlanExpr::InList {
            expr: Box::new(remap_expr_columns(expr, f)?),
            list: list
                .iter()
                .map(|item| remap_expr_columns(item, f))
                .collect::<Option<Vec<_>>>()?,
            negated: *negated,
            span: None,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            ..
        } => PlanExpr::Between {
            expr: Box::new(remap_expr_columns(expr, f)?),
            negated: *negated,
            low: Box::new(remap_expr_columns(low, f)?),
            high: Box::new(remap_expr_columns(high, f)?),
            span: None,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => PlanExpr::CaseExpr {
            operand: match operand {
                Some(operand) => Some(Box::new(remap_expr_columns(operand, f)?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    Some((remap_expr_columns(when, f)?, remap_expr_columns(then, f)?))
                })
                .collect::<Option<Vec<_>>>()?,
            else_result: match else_result {
                Some(expr) => Some(Box::new(remap_expr_columns(expr, f)?)),
                None => None,
            },
            span: None,
        },
        PlanExpr::Cast {
            expr, data_type, ..
        } => PlanExpr::Cast {
            expr: Box::new(remap_expr_columns(expr, f)?),
            data_type: data_type.clone(),
            span: None,
        },
        PlanExpr::ScalarSubquery { .. } => return None,
    })
}

fn column_index(expr: &PlanExpr) -> Option<usize> {
    match expr {
        PlanExpr::Column { index, .. } => Some(*index),
        _ => None,
    }
}

fn aggregate_single_column_arg_eq(aggr_expr: &PlanExpr, expr: &PlanExpr) -> bool {
    let Some(arg) = aggregate_single_column_arg(aggr_expr) else {
        return false;
    };
    expr_eq(arg, expr)
}

fn aggregate_single_column_arg_index(aggr_expr: &PlanExpr) -> Option<usize> {
    aggregate_single_column_arg(aggr_expr).and_then(column_index)
}

fn aggregate_single_column_arg(aggr_expr: &PlanExpr) -> Option<&PlanExpr> {
    let PlanExpr::Function { args, .. } = aggr_expr else {
        return None;
    };
    let [arg] = args.as_slice() else {
        return None;
    };
    if matches!(arg, PlanExpr::Column { .. }) {
        Some(arg)
    } else {
        None
    }
}

fn match_group_keys(
    group_by: &[PlanExpr],
    preserved_key: &PlanExpr,
    preserved_side: Side,
    preagg_side: Side,
    left_width: usize,
    right_width: usize,
) -> Option<GroupKeyMatch> {
    if group_by.len() == 1 && expr_eq(&group_by[0], preserved_key) {
        return Some(GroupKeyMatch::Exact);
    }
    if group_by.is_empty()
        || !group_by.iter().any(|expr| expr_eq(expr, preserved_key))
        || group_by.iter().any(|expr| {
            expr_side(expr, left_width, right_width) != Some(preserved_side)
                || expr_side(expr, left_width, right_width) == Some(preagg_side)
        })
    {
        return None;
    }
    Some(GroupKeyMatch::PreservedSuperset)
}

fn preserved_side_unique_on_key(
    preserved_input: &LogicalPlan,
    preserved_key: &PlanExpr,
    preserved_side: Side,
    left_width: usize,
    stats: &CatalogStats,
) -> bool {
    let Some(key_name) = preserved_key_column_name(preserved_key, preserved_side, left_width)
    else {
        return false;
    };
    if !all_scans_have_row_count(preserved_input, stats) {
        return false;
    }
    let Some(ndv) = lookup_column_ndv_strict(&key_name, preserved_input, stats) else {
        return false;
    };
    let row_count = estimated_cardinality(preserved_input, stats);
    row_count.is_finite() && row_count > 0.0 && ndv as f64 >= row_count * 0.99
}

fn preserved_key_column_name(
    preserved_key: &PlanExpr,
    preserved_side: Side,
    left_width: usize,
) -> Option<String> {
    let PlanExpr::Column { index, name, .. } = preserved_key else {
        return None;
    };
    match preserved_side {
        Side::Left if *index < left_width => Some(name.clone()),
        Side::Right if *index >= left_width => Some(name.clone()),
        _ => None,
    }
}

fn lookup_column_ndv_strict(
    column_name: &str,
    plan: &LogicalPlan,
    stats: &CatalogStats,
) -> Option<u64> {
    match plan {
        LogicalPlan::TableScan { table, schema, .. } => schema
            .iter()
            .any(|col| col.name == column_name)
            .then(|| {
                stats
                    .get(table)
                    .and_then(|table_stats| table_stats.columns.get(column_name))
                    .and_then(|column_stats| column_stats.ndv)
            })
            .flatten(),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => {
            lookup_column_ndv_strict(column_name, input, stats)
        }
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => lookup_column_ndv_strict(column_name, left, stats)
            .or_else(|| lookup_column_ndv_strict(column_name, right, stats)),
        LogicalPlan::UnionAll { inputs } => inputs
            .iter()
            .find_map(|input| lookup_column_ndv_strict(column_name, input, stats)),
        _ => None,
    }
}

fn all_scans_have_row_count(plan: &LogicalPlan, stats: &CatalogStats) -> bool {
    match plan {
        LogicalPlan::TableScan { table, .. } => {
            stats.get(table).and_then(|s| s.row_count).is_some()
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Projection { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Explain { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::PartialAggregate { input, .. }
        | LogicalPlan::FinalAggregate { input, .. }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::AssignUniqueId { input, .. } => all_scans_have_row_count(input, stats),
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::SemiJoin { left, right, .. }
        | LogicalPlan::AntiJoin { left, right, .. }
        | LogicalPlan::Intersect { left, right }
        | LogicalPlan::Except { left, right } => {
            all_scans_have_row_count(left, stats) && all_scans_have_row_count(right, stats)
        }
        LogicalPlan::UnionAll { inputs } => inputs
            .iter()
            .all(|input| all_scans_have_row_count(input, stats)),
        _ => true,
    }
}

fn project_preserved_expr_after_join(
    expr: &PlanExpr,
    preserved_side: Side,
    preagg_side: Side,
    left_width: usize,
    new_join_left_width: usize,
) -> Option<PlanExpr> {
    match (preserved_side, preagg_side) {
        (Side::Left, Side::Right) => Some(expr.clone()),
        (Side::Right, Side::Left) => {
            let side_expr = remap_expr_to_side(expr, Side::Right, left_width)?;
            Some(shift_column_indices(side_expr, new_join_left_width))
        }
        _ => None,
    }
}

fn shift_column_indices(expr: PlanExpr, offset: usize) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: index + offset,
            name,
            span,
        },
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(shift_column_indices(*left, offset)),
            op,
            right: Box::new(shift_column_indices(*right, offset)),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(shift_column_indices(*expr, offset)),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => PlanExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|arg| shift_column_indices(arg, offset))
                .collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(shift_column_indices(*expr, offset)),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(shift_column_indices(*expr, offset)),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(shift_column_indices(*expr, offset)),
            list: list
                .into_iter()
                .map(|item| shift_column_indices(item, offset))
                .collect(),
            negated,
            span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(shift_column_indices(*expr, offset)),
            negated,
            low: Box::new(shift_column_indices(*low, offset)),
            high: Box::new(shift_column_indices(*high, offset)),
            span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|operand| Box::new(shift_column_indices(*operand, offset))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(when, then)| {
                    (
                        shift_column_indices(when, offset),
                        shift_column_indices(then, offset),
                    )
                })
                .collect(),
            else_result: else_result.map(|expr| Box::new(shift_column_indices(*expr, offset))),
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(shift_column_indices(*expr, offset)),
            data_type,
            span,
        },
        other => other,
    }
}

fn is_identity_projection(exprs: &[PlanExpr]) -> bool {
    exprs.iter().enumerate().all(
        |(expected, expr)| matches!(expr, PlanExpr::Column { index, .. } if *index == expected),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AggKind {
    Count,
    Sum,
    Min,
    Max,
}

fn classify_aggregate(expr: &PlanExpr) -> Option<AggKind> {
    let PlanExpr::Function {
        name,
        args,
        distinct,
        ..
    } = expr
    else {
        return None;
    };
    if *distinct || args.len() != 1 {
        return None;
    }
    match name.to_uppercase().as_str() {
        "COUNT" => Some(AggKind::Count),
        "SUM" => Some(AggKind::Sum),
        "MIN" => Some(AggKind::Min),
        "MAX" => Some(AggKind::Max),
        _ => None,
    }
}

fn aggregate_arg_side(expr: &PlanExpr, left_width: usize) -> Option<Side> {
    let PlanExpr::Function { args, .. } = expr else {
        return None;
    };
    let mut side = None;
    let mut mixed = false;
    collect_column_indices(&args[0], &mut |idx| {
        let this_side = if idx < left_width {
            Side::Left
        } else {
            Side::Right
        };
        if let Some(prev) = side {
            if prev != this_side {
                mixed = true;
            }
        } else {
            side = Some(this_side);
        }
    });
    if mixed {
        None
    } else {
        side
    }
}

fn analyze_join_condition(
    condition: &JoinCondition,
    left_width: usize,
    right_width: usize,
) -> Option<JoinAnalysis> {
    let JoinCondition::On(expr) = condition else {
        return None;
    };
    let mut keys = Vec::new();
    let mut residuals = Vec::new();
    collect_join_condition_parts(expr, left_width, right_width, &mut keys, &mut residuals)?;
    if keys.len() == 1 {
        Some(JoinAnalysis {
            keys: keys.pop()?,
            residuals,
        })
    } else {
        None
    }
}

fn collect_join_condition_parts(
    expr: &PlanExpr,
    left_width: usize,
    right_width: usize,
    keys: &mut Vec<JoinKeys>,
    residuals: &mut Vec<PlanExpr>,
) -> Option<()> {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => {
            collect_join_condition_parts(left, left_width, right_width, keys, residuals)?;
            collect_join_condition_parts(right, left_width, right_width, keys, residuals)
        }
        PlanExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } => {
            let l_side = expr_side(left, left_width, right_width);
            let r_side = expr_side(right, left_width, right_width);
            match (l_side, r_side) {
                (Some(Side::Left), Some(Side::Right)) => keys.push(JoinKeys {
                    left: (**left).clone(),
                    right: (**right).clone(),
                }),
                (Some(Side::Right), Some(Side::Left)) => keys.push(JoinKeys {
                    left: (**right).clone(),
                    right: (**left).clone(),
                }),
                _ => residuals.push(expr.clone()),
            }
            Some(())
        }
        _ => {
            residuals.push(expr.clone());
            Some(())
        }
    }
}

fn expr_side(expr: &PlanExpr, left_width: usize, right_width: usize) -> Option<Side> {
    let mut side = None;
    collect_column_indices(expr, &mut |idx| {
        let this_side = if idx < left_width {
            Some(Side::Left)
        } else if idx < left_width + right_width {
            Some(Side::Right)
        } else {
            None
        };
        side = match (side, this_side) {
            (None, Some(s)) => Some(s),
            (Some(prev), Some(s)) if prev == s => Some(prev),
            _ => None,
        };
    });
    side
}

fn side_column_index(expr: &PlanExpr, side: Side, left_width: usize) -> Option<usize> {
    let PlanExpr::Column { index, .. } = expr else {
        return None;
    };
    match side {
        Side::Left if *index < left_width => Some(*index),
        Side::Right if *index >= left_width => Some(*index - left_width),
        _ => None,
    }
}

fn remap_expr_to_side(expr: &PlanExpr, side: Side, left_width: usize) -> Option<PlanExpr> {
    Some(match expr {
        PlanExpr::Column { index, name, .. } => {
            let new_index = match side {
                Side::Left if *index < left_width => *index,
                Side::Right if *index >= left_width => *index - left_width,
                _ => return None,
            };
            PlanExpr::Column {
                index: new_index,
                name: name.clone(),
                span: None,
            }
        }
        PlanExpr::Literal { value, .. } => PlanExpr::Literal {
            value: value.clone(),
            span: None,
        },
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => PlanExpr::BinaryOp {
            left: Box::new(remap_expr_to_side(left, side, left_width)?),
            op: *op,
            right: Box::new(remap_expr_to_side(right, side, left_width)?),
            span: None,
        },
        PlanExpr::UnaryOp { op, expr, .. } => PlanExpr::UnaryOp {
            op: *op,
            expr: Box::new(remap_expr_to_side(expr, side, left_width)?),
            span: None,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            ..
        } => PlanExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_expr_to_side(arg, side, left_width))
                .collect::<Option<Vec<_>>>()?,
            distinct: *distinct,
            span: None,
        },
        _ => return None,
    })
}

fn remap_expr_from_join_to_side(
    expr: &PlanExpr,
    side: Side,
    left_width: usize,
) -> Option<PlanExpr> {
    remap_expr_to_side(expr, side, left_width)
}

fn collect_column_indices<F: FnMut(usize)>(expr: &PlanExpr, cb: &mut F) {
    match expr {
        PlanExpr::Column { index, .. } => cb(*index),
        PlanExpr::Literal { .. } | PlanExpr::Parameter { .. } | PlanExpr::Wildcard => {}
        PlanExpr::BinaryOp { left, right, .. } => {
            collect_column_indices(left, cb);
            collect_column_indices(right, cb);
        }
        PlanExpr::UnaryOp { expr, .. } => collect_column_indices(expr, cb),
        PlanExpr::Function { args, .. } => {
            for arg in args {
                collect_column_indices(arg, cb);
            }
        }
        PlanExpr::IsNull { expr, .. } | PlanExpr::IsNotNull { expr, .. } => {
            collect_column_indices(expr, cb);
        }
        PlanExpr::InList { expr, list, .. } => {
            collect_column_indices(expr, cb);
            for item in list {
                collect_column_indices(item, cb);
            }
        }
        PlanExpr::Between {
            expr, low, high, ..
        } => {
            collect_column_indices(expr, cb);
            collect_column_indices(low, cb);
            collect_column_indices(high, cb);
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                collect_column_indices(operand, cb);
            }
            for (when, then) in when_clauses {
                collect_column_indices(when, cb);
                collect_column_indices(then, cb);
            }
            if let Some(else_result) = else_result {
                collect_column_indices(else_result, cb);
            }
        }
        PlanExpr::Cast { expr, .. } => collect_column_indices(expr, cb),
        PlanExpr::ScalarSubquery { .. } => {}
    }
}

fn eq_expr(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::Eq,
        right: Box::new(right),
        span: None,
    }
}

fn and_expr(left: PlanExpr, right: PlanExpr) -> PlanExpr {
    PlanExpr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
        span: None,
    }
}

fn zero_if_null(expr: PlanExpr) -> PlanExpr {
    PlanExpr::CaseExpr {
        operand: None,
        when_clauses: vec![(
            PlanExpr::IsNull {
                expr: Box::new(expr.clone()),
                span: None,
            },
            PlanExpr::Literal {
                value: ScalarValue::Int64(0),
                span: None,
            },
        )],
        else_result: Some(Box::new(expr)),
        span: None,
    }
}

fn expr_eq(left: &PlanExpr, right: &PlanExpr) -> bool {
    format!("{left}") == format!("{right}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arneb_catalog::{
        CatalogManager, ColumnStatistics, MemoryCatalog, MemorySchema, MemoryTable, TableProvider,
        TableStatistics,
    };
    use arneb_common::types::{DataType, TableReference};
    use arneb_sql_parser::ast;

    use crate::QueryPlanner;

    const Q13_SQL: &str = include_str!("../../../../benchmarks/tpch/queries/q13.sql");
    const Q18_SQL: &str = include_str!("../../../../benchmarks/tpch/queries/q18.sql");

    #[derive(Debug)]
    struct StatsTable {
        schema: Vec<ColumnInfo>,
        stats: TableStatistics,
    }

    impl TableProvider for StatsTable {
        fn schema(&self) -> Vec<ColumnInfo> {
            self.schema.clone()
        }

        fn statistics(&self) -> Option<TableStatistics> {
            Some(self.stats.clone())
        }
    }

    fn ci(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn scan(table: &str, cols: &[&str]) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table(table),
            schema: cols.iter().map(|name| ci(name)).collect(),
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn col(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        }
    }

    fn string_lit(value: &str) -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Utf8(value.to_string()),
            span: None,
        }
    }

    fn gt_expr(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Gt,
            right: Box::new(right),
            span: None,
        }
    }

    fn plus_expr(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Plus,
            right: Box::new(right),
            span: None,
        }
    }

    fn not_like_expr(left: PlanExpr, right: PlanExpr) -> PlanExpr {
        PlanExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::NotLike,
            right: Box::new(right),
            span: None,
        }
    }

    fn q13_shape(function_name: &str, group_by: PlanExpr) -> LogicalPlan {
        let customer = scan("customer", &["c_custkey"]);
        let orders = scan("orders", &["o_custkey", "o_orderkey"]);
        let join = LogicalPlan::Join {
            left: Box::new(customer),
            right: Box::new(orders),
            join_type: ast::JoinType::Left,
            condition: JoinCondition::On(eq_expr(col(0, "c_custkey"), col(1, "o_custkey"))),
            dynamic_filter_ids: Vec::new(),
        };
        LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![group_by],
            aggr_exprs: vec![PlanExpr::Function {
                name: function_name.to_string(),
                args: vec![col(2, "o_orderkey")],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("c_custkey"), ci("COUNT(o_orderkey)")],
        }
    }

    fn q13_shape_with_join_predicate(predicate: PlanExpr) -> LogicalPlan {
        let customer = scan("customer", &["c_custkey"]);
        let orders = scan("orders", &["o_custkey", "o_orderkey", "o_comment"]);
        let join = LogicalPlan::Join {
            left: Box::new(customer),
            right: Box::new(orders),
            join_type: ast::JoinType::Left,
            condition: JoinCondition::On(and_expr(
                eq_expr(col(0, "c_custkey"), col(1, "o_custkey")),
                predicate,
            )),
            dynamic_filter_ids: Vec::new(),
        };
        LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by: vec![col(0, "c_custkey")],
            aggr_exprs: vec![PlanExpr::Function {
                name: "COUNT".to_string(),
                args: vec![col(2, "o_orderkey")],
                distinct: false,
                span: None,
            }],
            schema: vec![ci("c_custkey"), ci("COUNT(o_orderkey)")],
        }
    }

    fn sum_expr(arg: PlanExpr) -> PlanExpr {
        PlanExpr::Function {
            name: "SUM".to_string(),
            args: vec![arg],
            distinct: false,
            span: None,
        }
    }

    fn superset_group_by_shape(group_by: Vec<PlanExpr>) -> LogicalPlan {
        let preserved = scan("orders", &["o_orderkey", "o_orderdate", "o_totalprice"]);
        let preagg = scan("lineitem", &["l_orderkey", "l_quantity"]);
        let join = LogicalPlan::Join {
            left: Box::new(preserved),
            right: Box::new(preagg),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(eq_expr(col(0, "o_orderkey"), col(3, "l_orderkey"))),
            dynamic_filter_ids: Vec::new(),
        };
        LogicalPlan::Aggregate {
            input: Box::new(join),
            group_by,
            aggr_exprs: vec![sum_expr(col(4, "l_quantity"))],
            schema: vec![
                ci("o_orderkey"),
                ci("o_orderdate"),
                ci("o_totalprice"),
                ci("SUM(l_quantity)"),
            ],
        }
    }

    fn projection_superset_group_by_shape(computed_projection: bool) -> LogicalPlan {
        let preserved = scan("p", &["p_key", "p_other"]);
        let preagg = scan("r", &["r_key", "r_val"]);
        let join = LogicalPlan::Join {
            left: Box::new(preserved),
            right: Box::new(preagg),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(eq_expr(col(0, "p_key"), col(2, "r_key"))),
            dynamic_filter_ids: Vec::new(),
        };
        let p_key_expr = if computed_projection {
            plus_expr(
                col(0, "p_key"),
                PlanExpr::Literal {
                    value: ScalarValue::Int64(0),
                    span: None,
                },
            )
        } else {
            col(0, "p_key")
        };
        let projection = LogicalPlan::Projection {
            input: Box::new(join),
            exprs: vec![col(1, "p_other"), p_key_expr, col(3, "r_val")],
            schema: vec![ci("p_other"), ci("p_key"), ci("r_val")],
        };
        LogicalPlan::Aggregate {
            input: Box::new(projection),
            group_by: vec![col(1, "p_key"), col(0, "p_other")],
            aggr_exprs: vec![sum_expr(col(2, "r_val"))],
            schema: vec![ci("p_key"), ci("p_other"), ci("SUM(r_val)")],
        }
    }

    fn stats_for(table: &str, row_count: u64, column_ndvs: &[(&str, u64)]) -> TableStatistics {
        let mut columns = HashMap::new();
        for (col, ndv) in column_ndvs {
            columns.insert(
                col.to_string(),
                ColumnStatistics {
                    ndv: Some(*ndv),
                    ..ColumnStatistics::default()
                },
            );
        }
        let _ = table;
        TableStatistics {
            row_count: Some(row_count),
            size_bytes: None,
            columns,
        }
    }

    fn superset_stats(orderkey_ndv: u64) -> Arc<CatalogStats> {
        let mut stats = CatalogStats::new();
        stats.insert(
            TableReference::table("orders"),
            stats_for("orders", 1_000, &[("o_orderkey", orderkey_ndv)]),
        );
        stats.insert(
            TableReference::table("lineitem"),
            stats_for("lineitem", 6_000, &[("l_orderkey", 1_000)]),
        );
        Arc::new(stats)
    }

    fn projection_superset_stats() -> Arc<CatalogStats> {
        let mut stats = CatalogStats::new();
        stats.insert(
            TableReference::table("p"),
            stats_for("p", 1_000, &[("p_key", 1_000)]),
        );
        stats.insert(
            TableReference::table("r"),
            stats_for("r", 6_000, &[("r_key", 1_000)]),
        );
        Arc::new(stats)
    }

    #[test]
    fn eager_aggregation_pushes_q13_count_below_left_join() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape("COUNT", col(0, "c_custkey"));

        let after = EagerAggregation::new().analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Aggregate {
            input,
            schema,
            aggr_exprs,
            ..
        } = after
        else {
            panic!("expected top aggregate after eager aggregation");
        };
        assert_eq!(schema[0].name, "c_custkey");
        assert_eq!(schema[1].name, "COUNT(o_orderkey)");
        match &aggr_exprs[0] {
            PlanExpr::Function { name, .. } => assert_eq!(name, "SUM"),
            other => panic!("expected final SUM for COUNT partial, got {other:?}"),
        }

        let LogicalPlan::Projection { input, exprs, .. } = *input else {
            panic!("expected projection below top aggregate");
        };
        assert!(matches!(exprs[1], PlanExpr::CaseExpr { .. }));

        let LogicalPlan::Join {
            left,
            right,
            join_type,
            ..
        } = *input
        else {
            panic!("expected join below projection");
        };
        assert!(matches!(join_type, ast::JoinType::Left));
        assert!(matches!(*left, LogicalPlan::TableScan { .. }));
        assert!(matches!(*right, LogicalPlan::Aggregate { .. }));

        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } = *right
        else {
            panic!("expected aggregate on orders side");
        };
        assert!(matches!(*input, LogicalPlan::TableScan { .. }));
        assert_eq!(group_by, vec![col(0, "o_custkey")]);
        assert_eq!(aggr_exprs.len(), 1);
        assert_eq!(schema[0].name, "o_custkey");
        assert_eq!(schema[1].name, "COUNT(o_orderkey)");
    }

    #[test]
    fn test_superset_group_by_fires() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::with_stats(superset_stats(1_000));
        let plan = superset_group_by_shape(vec![
            col(0, "o_orderkey"),
            col(1, "o_orderdate"),
            col(2, "o_totalprice"),
        ]);

        let after = EagerAggregation::new().analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } = after
        else {
            panic!("expected top aggregate after eager aggregation");
        };
        assert_eq!(
            group_by,
            vec![
                col(0, "o_orderkey"),
                col(1, "o_orderdate"),
                col(2, "o_totalprice")
            ]
        );
        match &aggr_exprs[0] {
            PlanExpr::Function { name, args, .. } => {
                assert_eq!(name, "SUM");
                assert_eq!(args, &vec![col(3, "SUM(l_quantity)")]);
            }
            other => panic!("expected final SUM over partial, got {other:?}"),
        }

        let LogicalPlan::Projection { input, exprs, .. } = *input else {
            panic!("expected projection below top aggregate");
        };
        assert_eq!(
            exprs,
            vec![
                col(0, "o_orderkey"),
                col(1, "o_orderdate"),
                col(2, "o_totalprice"),
                col(4, "SUM(l_quantity)")
            ]
        );

        let LogicalPlan::Join { right, .. } = *input else {
            panic!("expected join below projection");
        };
        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } = *right
        else {
            panic!("expected aggregate on lineitem side");
        };
        assert!(matches!(
            input.as_ref(),
            LogicalPlan::TableScan { table, .. } if table.table == "lineitem"
        ));
        assert_eq!(group_by, vec![col(0, "l_orderkey")]);
        assert_eq!(aggr_exprs, vec![sum_expr(col(1, "l_quantity"))]);
    }

    #[test]
    fn test_superset_group_by_non_unique_no_rewrite() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::with_stats(superset_stats(1));
        let plan = superset_group_by_shape(vec![
            col(0, "o_orderkey"),
            col(1, "o_orderdate"),
            col(2, "o_totalprice"),
        ]);

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn test_superset_group_by_extra_key_from_r_no_rewrite() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::with_stats(superset_stats(1_000));
        let plan = superset_group_by_shape(vec![
            col(0, "o_orderkey"),
            col(1, "o_orderdate"),
            col(4, "l_quantity"),
        ]);

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn eager_aggregation_pushes_right_only_join_predicate_below_preaggregate() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape_with_join_predicate(not_like_expr(
            col(3, "o_comment"),
            string_lit("%special%requests%"),
        ));

        let after = EagerAggregation::new().analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Aggregate { input, .. } = after else {
            panic!("expected top aggregate after eager aggregation");
        };
        let LogicalPlan::Projection { input, .. } = *input else {
            panic!("expected projection below top aggregate");
        };
        let LogicalPlan::Join {
            left,
            right,
            condition,
            ..
        } = *input
        else {
            panic!("expected join below projection");
        };
        assert!(matches!(*left, LogicalPlan::TableScan { .. }));
        let JoinCondition::On(PlanExpr::BinaryOp {
            op: BinaryOp::Eq, ..
        }) = condition
        else {
            panic!("expected residual predicate removed from join condition");
        };

        let LogicalPlan::Aggregate { input, .. } = *right else {
            panic!("expected aggregate on orders side");
        };
        let LogicalPlan::Filter { input, predicate } = *input else {
            panic!("expected right-only predicate as filter below pre-aggregate");
        };
        assert!(matches!(*input, LogicalPlan::TableScan { .. }));
        assert_eq!(
            format!("{predicate}"),
            "o_comment NOT LIKE '%special%requests%'"
        );
        assert_eq!(expr_side(&predicate, 0, 3), Some(Side::Right));
    }

    #[test]
    fn eager_aggregation_rejects_preserved_side_extra_join_predicate() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape_with_join_predicate(gt_expr(
            col(0, "c_custkey"),
            PlanExpr::Literal {
                value: ScalarValue::Int64(0),
                span: None,
            },
        ));

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn eager_aggregation_accepts_identity_project_between_aggregate_and_join() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape("COUNT", col(0, "c_custkey"));
        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } = plan
        else {
            panic!("expected aggregate");
        };
        let identity_schema = input.schema();
        let identity_project = LogicalPlan::Projection {
            input,
            exprs: vec![
                col(0, "c_custkey"),
                col(1, "o_custkey"),
                col(2, "o_orderkey"),
            ],
            schema: identity_schema,
        };
        let plan = LogicalPlan::Aggregate {
            input: Box::new(identity_project),
            group_by,
            aggr_exprs,
            schema,
        };

        let after = EagerAggregation::new().analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Aggregate { input, .. } = after else {
            panic!("expected top aggregate after eager aggregation");
        };
        let LogicalPlan::Projection { input, .. } = *input else {
            panic!("expected projection below top aggregate");
        };
        let LogicalPlan::Join { right, .. } = *input else {
            panic!("expected join below projection");
        };
        assert!(matches!(*right, LogicalPlan::Aggregate { .. }));
    }

    #[test]
    fn eager_aggregation_accepts_simple_column_project_between_aggregate_and_join() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::with_stats(projection_superset_stats());
        let plan = projection_superset_group_by_shape(false);

        let after = EagerAggregation::new().analyze(plan, &mut ctx).unwrap();

        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } = after
        else {
            panic!("expected top aggregate after eager aggregation");
        };
        assert_eq!(group_by, vec![col(1, "p_key"), col(0, "p_other")]);
        assert_eq!(aggr_exprs, vec![sum_expr(col(2, "r_val"))]);

        let LogicalPlan::Projection { input, exprs, .. } = *input else {
            panic!("expected original projection rewrapped below top aggregate");
        };
        assert_eq!(
            exprs,
            vec![col(1, "p_other"), col(0, "p_key"), col(2, "r_val")]
        );

        let LogicalPlan::Projection { input, .. } = *input else {
            panic!("expected eager aggregation projection below rewrapped projection");
        };
        let LogicalPlan::Join { right, .. } = *input else {
            panic!("expected join below eager aggregation projection");
        };
        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } = *right
        else {
            panic!("expected aggregate on r side");
        };
        assert!(matches!(
            input.as_ref(),
            LogicalPlan::TableScan { table, .. } if table.table == "r"
        ));
        assert_eq!(group_by, vec![col(0, "r_key")]);
        assert_eq!(aggr_exprs, vec![sum_expr(col(1, "r_val"))]);
    }

    #[test]
    fn eager_aggregation_rejects_computed_project_between_aggregate_and_join() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::with_stats(projection_superset_stats());
        let plan = projection_superset_group_by_shape(true);

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn eager_aggregation_rejects_avg() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape("AVG", col(0, "c_custkey"));

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn eager_aggregation_rejects_non_join_group_key() {
        let _override = set_eager_aggregation_for_test(true);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape("COUNT", col(2, "o_orderkey"));

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[test]
    fn eager_aggregation_gate_off_is_noop() {
        let _override = set_eager_aggregation_for_test(false);
        let mut ctx = AnalyzerContext::new();
        let plan = q13_shape("COUNT", col(0, "c_custkey"));

        let after = EagerAggregation::new()
            .analyze(plan.clone(), &mut ctx)
            .unwrap();

        assert!(matches!(after, LogicalPlan::Aggregate { .. }));
        assert_eq!(format!("{after}"), format!("{plan}"));
    }

    #[tokio::test]
    async fn eager_aggregation_rewrites_actual_tpch_q13_sql() {
        let _override = set_eager_aggregation_for_test(true);
        let catalog = q13_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(Q13_SQL).expect("parse q13");

        let plan = planner.plan_statement(&stmt).await.expect("plan q13");

        assert!(
            has_orders_preaggregate_with_comment_filter(&plan),
            "expected q13 orders side to be filtered and pre-aggregated; plan:\n{plan}"
        );
    }

    #[tokio::test]
    async fn test_actual_tpch_q18_sql_triggers_superset_rewrite() {
        let _override = set_eager_aggregation_for_test(true);
        let catalog = q18_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(Q18_SQL).expect("parse q18");

        let plan = planner.plan_statement(&stmt).await.expect("plan q18");

        assert!(
            count_lineitem_sum_by_orderkey_aggregates(&plan) >= 2,
            "expected q18 to contain the original HAVING aggregate plus an eager lineitem pre-aggregate; plan:\n{plan}"
        );
    }

    fn q13_catalog() -> CatalogManager {
        let manager = CatalogManager::new("default", "public");
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());
        schema.register_table(
            "customer",
            Arc::new(MemoryTable::new(vec![ci("c_custkey")]))
                as Arc<dyn arneb_catalog::TableProvider>,
        );
        schema.register_table(
            "orders",
            Arc::new(MemoryTable::new(vec![
                ci("o_orderkey"),
                ci("o_custkey"),
                ColumnInfo {
                    name: "o_comment".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ])) as Arc<dyn arneb_catalog::TableProvider>,
        );
        catalog.register_schema("public", schema);
        manager.register_catalog("default", catalog);
        manager
    }

    fn q18_catalog() -> CatalogManager {
        let manager = CatalogManager::new("default", "public");
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());
        schema.register_table(
            "customer",
            Arc::new(StatsTable {
                schema: vec![
                    ColumnInfo {
                        name: "c_name".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                    },
                    ci("c_custkey"),
                ],
                stats: stats_for("customer", 150_000, &[("c_custkey", 150_000)]),
            }) as Arc<dyn TableProvider>,
        );
        schema.register_table(
            "orders",
            Arc::new(StatsTable {
                schema: vec![
                    ci("o_orderkey"),
                    ci("o_custkey"),
                    ci("o_orderdate"),
                    ci("o_totalprice"),
                ],
                stats: stats_for(
                    "orders",
                    1_500_000,
                    &[("o_orderkey", 1_500_000), ("o_custkey", 150_000)],
                ),
            }) as Arc<dyn TableProvider>,
        );
        schema.register_table(
            "lineitem",
            Arc::new(StatsTable {
                schema: vec![ci("l_orderkey"), ci("l_quantity")],
                stats: stats_for(
                    "lineitem",
                    6_000_000,
                    &[("l_orderkey", 1_500_000), ("l_quantity", 50)],
                ),
            }) as Arc<dyn TableProvider>,
        );
        catalog.register_schema("public", schema);
        manager.register_catalog("default", catalog);
        manager
    }

    fn has_orders_preaggregate_with_comment_filter(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Aggregate {
                input, group_by, ..
            } if group_by.iter().any(
                |expr| matches!(expr, PlanExpr::Column { name, .. } if name == "o_custkey"),
            ) =>
            {
                matches!(
                    input.as_ref(),
                    LogicalPlan::Filter {
                        input,
                        predicate: PlanExpr::BinaryOp {
                            op: BinaryOp::NotLike,
                            ..
                        },
                    } if matches!(
                        input.as_ref(),
                        LogicalPlan::TableScan { table, .. } if table.table == "orders"
                    )
                )
            }
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::CreateTableAsSelect { source: input, .. }
            | LogicalPlan::InsertInto { source: input, .. }
            | LogicalPlan::CreateView { plan: input, .. }
            | LogicalPlan::ScalarSubquery { subplan: input } => {
                has_orders_preaggregate_with_comment_filter(input)
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                has_orders_preaggregate_with_comment_filter(left)
                    || has_orders_preaggregate_with_comment_filter(right)
            }
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .any(has_orders_preaggregate_with_comment_filter),
            _ => false,
        }
    }

    fn count_lineitem_sum_by_orderkey_aggregates(plan: &LogicalPlan) -> usize {
        let here = match plan {
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                ..
            } if group_by.iter().any(
                |expr| matches!(expr, PlanExpr::Column { name, .. } if name == "l_orderkey"),
            ) && aggr_exprs.iter().any(|expr| {
                matches!(
                    expr,
                    PlanExpr::Function { name, args, .. }
                        if name == "SUM"
                            && args.iter().any(|arg| matches!(
                                arg,
                                PlanExpr::Column { name, .. } if name == "l_quantity"
                            ))
                )
            }) && contains_table_scan(input, "lineitem") =>
            {
                1
            }
            _ => 0,
        };

        here + match plan {
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::CreateTableAsSelect { source: input, .. }
            | LogicalPlan::InsertInto { source: input, .. }
            | LogicalPlan::CreateView { plan: input, .. }
            | LogicalPlan::ScalarSubquery { subplan: input } => {
                count_lineitem_sum_by_orderkey_aggregates(input)
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                count_lineitem_sum_by_orderkey_aggregates(left)
                    + count_lineitem_sum_by_orderkey_aggregates(right)
            }
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .map(count_lineitem_sum_by_orderkey_aggregates)
                .sum(),
            _ => 0,
        }
    }

    fn contains_table_scan(plan: &LogicalPlan, table_name: &str) -> bool {
        match plan {
            LogicalPlan::TableScan { table, .. } => table.table == table_name,
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::CreateTableAsSelect { source: input, .. }
            | LogicalPlan::InsertInto { source: input, .. }
            | LogicalPlan::CreateView { plan: input, .. }
            | LogicalPlan::ScalarSubquery { subplan: input } => {
                contains_table_scan(input, table_name)
            }
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => {
                contains_table_scan(left, table_name) || contains_table_scan(right, table_name)
            }
            LogicalPlan::UnionAll { inputs } => inputs
                .iter()
                .any(|input| contains_table_scan(input, table_name)),
            _ => false,
        }
    }
}
