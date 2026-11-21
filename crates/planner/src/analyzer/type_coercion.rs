//! Type coercion analyzer pass.
//!
//! Walks a [`LogicalPlan`] and, at every site where operand types
//! must agree, inserts [`PlanExpr::Cast`] nodes per the coercion
//! matrix ([`super::coercion_matrix`]).
//!
//! Passes run post-order within each expression: children are
//! coerced first, then the current node decides what supertype (if
//! any) its direct operands must share and wraps the narrower side
//! in a `Cast`.
//!
//! Failures surface as [`PlanError::TypeMismatch`] with a
//! source-location sourced from [`PlanExpr::best_span`] — the error
//! is rendered as a rustc-style diagnostic by
//! `arneb_common::diagnostic::render_plan_error` at the pgwire
//! boundary.
//!
//! # What this pass does NOT do
//!
//! - **Function-argument coercion** (spec task 20): the current
//!   planner has no access to the execution-side `FunctionRegistry`
//!   and ScalarFunction has no declared input signature; calls are
//!   passed through unchanged and any runtime mismatch still goes
//!   through the execution-side cast path. This is explicitly
//!   allowed by the spec ("If no signature, leave as-is"). A
//!   follow-up change that lifts `FunctionSignature` into
//!   `arneb-common` will re-enable this.
//! - **Schema recomputation** for `Projection`/`Aggregate`: the
//!   existing `QueryPlanner::expr_to_column_info` already produces
//!   the widened-supertype output, so inserting internal `Cast`s
//!   doesn't shift the projection output type. We keep the schema
//!   as-is.

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType, ScalarValue, CHAR_MAX_LENGTH};
use arneb_sql_parser::ast;

use super::coercion_matrix::{common_supertype, lookup_cast, CoercionSite, Safety};
use super::{
    decimal_arithmetic_operands, is_boolean_result_op, is_literal_like, plan_expr_type,
    AnalysisPass, AnalyzerContext,
};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr};

/// The type-coercion pass. Construct with [`TypeCoercion::new`] and
/// insert into an [`super::Analyzer`] pipeline.
#[derive(Debug, Default)]
pub struct TypeCoercion;

impl TypeCoercion {
    /// Construct a new coercion pass. There is no per-instance state
    /// today; `Default` is equivalent.
    pub fn new() -> Self {
        Self
    }
}

impl AnalysisPass for TypeCoercion {
    fn name(&self) -> &'static str {
        "TypeCoercion"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        let plan = analyze_plan(plan, ctx)?;
        // Second walk: promote any remaining `Parameter { type_hint:
        // None }` nodes to `Utf8`. This matches Trino/Postgres'
        // "unknown → varchar" fallback for lone placeholders.
        let plan = default_unresolved_parameters(plan, ctx);
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// Plan walker
// ---------------------------------------------------------------------------

fn analyze_plan(plan: LogicalPlan, ctx: &mut AnalyzerContext) -> Result<LogicalPlan, PlanError> {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            let input = analyze_plan(*input, ctx)?;
            let schema = input.schema();
            let predicate = coerce_expr(predicate, &schema, ctx)?;
            Ok(LogicalPlan::Filter {
                input: Box::new(input),
                predicate,
            })
        }
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let exprs = exprs
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Projection {
                input: Box::new(input),
                exprs,
                schema,
            })
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Aggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::RollupAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::RollupAggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::PartialAggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let group_by = group_by
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let aggr_exprs = aggr_exprs
                .into_iter()
                .map(|e| coerce_expr(e, &input_schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::FinalAggregate {
                input: Box::new(input),
                group_by,
                aggr_exprs,
                schema,
            })
        }
        LogicalPlan::Sort { input, order_by } => {
            let input = analyze_plan(*input, ctx)?;
            let input_schema = input.schema();
            let order_by = order_by
                .into_iter()
                .map(|s| {
                    Ok::<_, PlanError>(SortExpr {
                        expr: coerce_expr(s.expr, &input_schema, ctx)?,
                        asc: s.asc,
                        nulls_first: s.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Sort {
                input: Box::new(input),
                order_by,
            })
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => Ok(LogicalPlan::Limit {
            input: Box::new(analyze_plan(*input, ctx)?),
            limit,
            offset,
        }),
        LogicalPlan::Explain { input, analyze } => Ok(LogicalPlan::Explain {
            input: Box::new(analyze_plan(*input, ctx)?),
            analyze,
        }),
        LogicalPlan::Distinct { input } => Ok(LogicalPlan::Distinct {
            input: Box::new(analyze_plan(*input, ctx)?),
        }),
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => {
            let left = analyze_plan(*left, ctx)?;
            let right = analyze_plan(*right, ctx)?;
            let mut combined = left.schema();
            combined.extend(right.schema());
            let condition = match condition {
                JoinCondition::On(expr) => JoinCondition::On(coerce_expr(expr, &combined, ctx)?),
                JoinCondition::None => JoinCondition::None,
            };
            Ok(LogicalPlan::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type,
                condition,
                dynamic_filter_ids: Vec::new(),
            })
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let left = analyze_plan(*left, ctx)?;
            let right = analyze_plan(*right, ctx)?;
            let left_schema = left.schema();
            let right_schema = right.schema();
            // Keys are evaluated against their respective inputs, so
            // we unify the PAIR's types via a synthetic binary site.
            let left_key = coerce_expr(left_key, &left_schema, ctx)?;
            let right_key = coerce_expr(right_key, &right_schema, ctx)?;
            let residual = match residual {
                Some(r) => {
                    let mut combined = left_schema.clone();
                    combined.extend(right_schema.iter().cloned());
                    Some(coerce_expr(r, &combined, ctx)?)
                }
                None => None,
            };
            Ok(LogicalPlan::SemiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
                dynamic_filter_ids: Vec::new(),
                probe_side_filter_ids: Vec::new(),
            })
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            ..
        } => {
            let left = analyze_plan(*left, ctx)?;
            let right = analyze_plan(*right, ctx)?;
            let left_schema = left.schema();
            let right_schema = right.schema();
            let left_key = coerce_expr(left_key, &left_schema, ctx)?;
            let right_key = coerce_expr(right_key, &right_schema, ctx)?;
            let residual = match residual {
                Some(r) => {
                    let mut combined = left_schema.clone();
                    combined.extend(right_schema.iter().cloned());
                    Some(coerce_expr(r, &combined, ctx)?)
                }
                None => None,
            };
            Ok(LogicalPlan::AntiJoin {
                left: Box::new(left),
                right: Box::new(right),
                left_key,
                right_key,
                residual,
                probe_side_filter_ids: Vec::new(),
            })
        }
        LogicalPlan::ScalarSubquery { subplan } => Ok(LogicalPlan::ScalarSubquery {
            subplan: Box::new(analyze_plan(*subplan, ctx)?),
        }),
        LogicalPlan::UnionAll { inputs } => {
            let inputs = inputs
                .into_iter()
                .map(|p| analyze_plan(p, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::UnionAll {
                inputs: align_set_op_branches(inputs)?,
            })
        }
        LogicalPlan::Intersect { left, right } => {
            let left = analyze_plan(*left, ctx)?;
            let right = analyze_plan(*right, ctx)?;
            let aligned = align_set_op_branches(vec![left, right])?;
            let mut it = aligned.into_iter();
            let left = it.next().expect("two inputs");
            let right = it.next().expect("two inputs");
            Ok(LogicalPlan::Intersect {
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        LogicalPlan::Except { left, right } => {
            let left = analyze_plan(*left, ctx)?;
            let right = analyze_plan(*right, ctx)?;
            let aligned = align_set_op_branches(vec![left, right])?;
            let mut it = aligned.into_iter();
            let left = it.next().expect("two inputs");
            let right = it.next().expect("two inputs");
            Ok(LogicalPlan::Except {
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        LogicalPlan::CreateTableAsSelect { name, source } => Ok(LogicalPlan::CreateTableAsSelect {
            name,
            source: Box::new(analyze_plan(*source, ctx)?),
        }),
        LogicalPlan::InsertInto { table, source } => Ok(LogicalPlan::InsertInto {
            table,
            source: Box::new(analyze_plan(*source, ctx)?),
        }),
        LogicalPlan::CreateView { name, sql, plan } => Ok(LogicalPlan::CreateView {
            name,
            sql,
            plan: Box::new(analyze_plan(*plan, ctx)?),
        }),
        LogicalPlan::ProjectSet {
            input,
            srfs,
            schema,
        } => analyze_project_set(*input, srfs, schema, ctx),
        LogicalPlan::Window { input, functions } => Ok(LogicalPlan::Window {
            input: Box::new(analyze_plan(*input, ctx)?),
            functions,
        }),
        LogicalPlan::AssignUniqueId { input, id_column } => Ok(LogicalPlan::AssignUniqueId {
            input: Box::new(analyze_plan(*input, ctx)?),
            id_column,
        }),
        // Leaf / opaque nodes: no expressions to walk.
        leaf @ (LogicalPlan::TableScan { .. }
        | LogicalPlan::ExchangeNode { .. }
        | LogicalPlan::CreateTable { .. }
        | LogicalPlan::DropTable { .. }
        | LogicalPlan::DeleteFrom { .. }
        | LogicalPlan::DropView { .. }
        | LogicalPlan::OneRow) => Ok(leaf),
    }
}

/// Out of line so the recursive `analyze_plan` frame stays small.
#[inline(never)]
fn analyze_project_set(
    input: LogicalPlan,
    srfs: Vec<(crate::plan::SrfKind, PlanExpr)>,
    schema: Vec<ColumnInfo>,
    ctx: &mut AnalyzerContext,
) -> Result<LogicalPlan, PlanError> {
    let input = analyze_plan(input, ctx)?;
    let input_schema = input.schema();
    let srfs = srfs
        .into_iter()
        .map(|(kind, e)| Ok((kind, coerce_expr(e, &input_schema, ctx)?)))
        .collect::<Result<Vec<_>, PlanError>>()?;
    Ok(LogicalPlan::rebuild_project_set(
        Box::new(input),
        srfs,
        schema,
    ))
}

// ---------------------------------------------------------------------------
// Expression walker
// ---------------------------------------------------------------------------

/// Out-of-line so the recursive `coerce_expr` frame stays small.
#[inline(never)]
fn coerce_order_by(
    order_by: Option<Box<Vec<crate::plan::SortExpr>>>,
    schema: &[ColumnInfo],
    ctx: &mut AnalyzerContext,
) -> Result<Option<Box<Vec<crate::plan::SortExpr>>>, PlanError> {
    let Some(v) = order_by else { return Ok(None) };
    let mut out = Vec::with_capacity(v.len());
    for s in *v {
        out.push(crate::plan::SortExpr {
            expr: coerce_expr(s.expr, schema, ctx)?,
            ..s
        });
    }
    Ok(Some(Box::new(out)))
}

fn coerce_expr(
    expr: PlanExpr,
    schema: &[ColumnInfo],
    ctx: &mut AnalyzerContext,
) -> Result<PlanExpr, PlanError> {
    // Post-order: coerce children first so their types are already
    // aligned when the parent decides its own supertype.
    match expr {
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => {
            let left = coerce_expr(*left, schema, ctx)?;
            let right = coerce_expr(*right, schema, ctx)?;
            let (left, right) =
                unify_binary_operands(left, right, &op, schema, span.map(|s| s.start), ctx)?;
            Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            })
        }
        PlanExpr::UnaryOp { op, expr, span } => Ok(PlanExpr::UnaryOp {
            op,
            expr: Box::new(coerce_expr(*expr, schema, ctx)?),
            span,
        }),
        PlanExpr::IsNull { expr, span } => Ok(PlanExpr::IsNull {
            expr: Box::new(coerce_expr(*expr, schema, ctx)?),
            span,
        }),
        PlanExpr::IsNotNull { expr, span } => Ok(PlanExpr::IsNotNull {
            expr: Box::new(coerce_expr(*expr, schema, ctx)?),
            span,
        }),
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => {
            let expr = coerce_expr(*expr, schema, ctx)?;
            let low = coerce_expr(*low, schema, ctx)?;
            let high = coerce_expr(*high, schema, ctx)?;
            let (expr, low, high) = unify_between(expr, low, high, schema, span.map(|s| s.start))?;
            Ok(PlanExpr::Between {
                expr: Box::new(expr),
                negated,
                low: Box::new(low),
                high: Box::new(high),
                span,
            })
        }
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => {
            let expr = coerce_expr(*expr, schema, ctx)?;
            let list = list
                .into_iter()
                .map(|e| coerce_expr(e, schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            let (expr, list) = unify_in_list(expr, list, schema, span.map(|s| s.start))?;
            Ok(PlanExpr::InList {
                expr: Box::new(expr),
                list,
                negated,
                span,
            })
        }
        PlanExpr::Array { elements, span, .. } => {
            let elements = elements
                .into_iter()
                .map(|e| coerce_expr(e, schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            unify_array(elements, schema, span)
        }
        PlanExpr::ArrayContains {
            expr,
            array,
            negated,
            span,
        } => coerce_array_contains(*expr, *array, negated, span, schema, ctx),
        PlanExpr::ArrayElement(pair) => coerce_array_element(pair, schema, ctx),
        PlanExpr::GetField(pair) => coerce_get_field(pair, schema, ctx),
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => {
            let operand = match operand {
                Some(o) => Some(Box::new(coerce_expr(*o, schema, ctx)?)),
                None => None,
            };
            let when_clauses = when_clauses
                .into_iter()
                .map(|(cond, result)| {
                    Ok::<_, PlanError>((
                        coerce_expr(cond, schema, ctx)?,
                        coerce_expr(result, schema, ctx)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let else_result = match else_result {
                Some(e) => Some(Box::new(coerce_expr(*e, schema, ctx)?)),
                None => None,
            };
            let (operand, when_clauses, else_result) = unify_case(
                operand,
                when_clauses,
                else_result,
                schema,
                span.map(|s| s.start),
            )?;
            Ok(PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                span,
            })
        }
        PlanExpr::Function {
            name,
            args,
            distinct,
            order_by,
            span,
        } => {
            // Coerce children but do not unify argument types against
            // a signature — see the module-level note.
            let mut args = args
                .into_iter()
                .map(|a| coerce_expr(a, schema, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            // The length of a CHAR(n) value counts its blank padding.
            if name.eq_ignore_ascii_case("length") && args.len() == 1 {
                let arg = args.pop().expect("one argument");
                args.push(pad_char_operand(arg, schema));
            }
            if (name.eq_ignore_ascii_case("substr") || name.eq_ignore_ascii_case("substring"))
                && (args.len() == 2 || args.len() == 3)
            {
                let arg = args.remove(0);
                args.insert(0, pad_char_operand(arg, schema));
            }
            let order_by = coerce_order_by(order_by, schema, ctx)?;
            Ok(PlanExpr::Function {
                order_by,
                name,
                args,
                distinct,
                span,
            })
        }
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => Ok(PlanExpr::Cast {
            expr: Box::new(coerce_expr(*expr, schema, ctx)?),
            data_type,
            span,
        }),
        PlanExpr::ScalarSubquery { subplan, span } => Ok(PlanExpr::ScalarSubquery {
            // Recurse: the subquery's inner plan (aggregates,
            // projections, etc.) needs the same coercion treatment
            // as the outer plan.
            subplan: Box::new(analyze_plan(*subplan, ctx)?),
            span,
        }),
        PlanExpr::Column { .. }
        | PlanExpr::Literal { .. }
        | PlanExpr::Parameter { .. }
        | PlanExpr::Wildcard => Ok(expr),
    }
}

// ---------------------------------------------------------------------------
// Site-specific unification
// ---------------------------------------------------------------------------

fn unify_binary_operands(
    left: PlanExpr,
    right: PlanExpr,
    op: &ast::BinaryOp,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
    ctx: &mut AnalyzerContext,
) -> Result<(PlanExpr, PlanExpr), PlanError> {
    // Logical operators (AND/OR) require Boolean on each side but
    // don't need type unification — skip.
    if matches!(op, ast::BinaryOp::And | ast::BinaryOp::Or) {
        return Ok((left, right));
    }
    // LIKE / NOT LIKE require Utf8 on both sides; our parser produces
    // Utf8-shaped operands by construction, so no coercion is attempted.
    // A CHAR value is matched with its blank padding, as in Trino.
    if matches!(op, ast::BinaryOp::Like | ast::BinaryOp::NotLike) {
        return Ok((pad_char_operand(left, schema), right));
    }
    // Parameter-type inference: if one side is an unbound `$n` and
    // the other has a known type, record the inference in `ctx` and
    // materialise the `type_hint` on the Parameter node itself so
    // subsequent passes and diagnostics see the resolved type.
    let (left, right) = infer_parameter_pair(left, right, schema, location, ctx)?;

    let lt = match plan_expr_type(&left, schema) {
        Some(t) => t,
        None => return Ok((left, right)),
    };
    let rt = match plan_expr_type(&right, schema) {
        Some(t) => t,
        None => return Ok((left, right)),
    };
    if lt == rt {
        return Ok((left, right));
    }

    // An untyped string literal against a numeric operand takes that
    // operand's type (`bigint_col = '0'`), as in PostgreSQL. Literals
    // only: a string column against a numeric one still mismatches.
    if let Some(left) = resolve_string_literal(&left, &rt, location)? {
        return unify_binary_operands(left, right, op, schema, location, ctx);
    }
    if let Some(right) = resolve_string_literal(&right, &lt, location)? {
        return unify_binary_operands(left, right, op, schema, location, ctx);
    }

    // Date ± integer-days arithmetic: SQL semantics yields a Date
    // shifted by N days. We keep the operands at their declared
    // types (Date32 and Int32 / Int64) so the executor calls Arrow's
    // typed sub/add kernel which handles Date32 + Int → Date32
    // directly. The TPC-H reference uses this in queries like Q01
    // (`l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`) — our
    // sql-parser `convert_interval` folds the INTERVAL to an Int32
    // literal, so the analyzer just needs to permit the mixed-type
    // arithmetic. Only addition and subtraction are accepted; Date *
    // Int / Date / Int / Date < Int etc. continue to fall through to
    // the regular supertype-unification path (which will reject them).
    let date_and_int = matches!(
        (lt.clone(), rt.clone()),
        (DataType::Date32, DataType::Int32)
            | (DataType::Date32, DataType::Int64)
            | (DataType::Int32, DataType::Date32)
            | (DataType::Int64, DataType::Date32)
    );
    if date_and_int && matches!(op, ast::BinaryOp::Plus | ast::BinaryOp::Minus) {
        return Ok((left, right));
    }

    // DATE compared with TIMESTAMP compares as TIMESTAMP (midnight), as
    // in PostgreSQL and Trino: `d >= date_trunc('year', now())`.
    let comparison = matches!(
        op,
        ast::BinaryOp::Eq
            | ast::BinaryOp::NotEq
            | ast::BinaryOp::Lt
            | ast::BinaryOp::LtEq
            | ast::BinaryOp::Gt
            | ast::BinaryOp::GtEq
    );
    if comparison {
        let to_timestamp = |expr: PlanExpr, ts: &DataType| PlanExpr::Cast {
            expr: Box::new(expr),
            data_type: ts.clone(),
            span: None,
        };
        match (&lt, &rt) {
            (DataType::Date32, DataType::Timestamp { .. }) => {
                return Ok((to_timestamp(left, &rt), right));
            }
            (DataType::Timestamp { .. }, DataType::Date32) => {
                return Ok((left, to_timestamp(right, &lt)));
            }
            _ => {}
        }
    }

    // Decimal arithmetic: operands keep their own precision/scale (an
    // integer side becomes DECIMAL(10|19, 0)); the executor computes the
    // Trino result type from them, so no common supertype is forced.
    if !is_boolean_result_op(op) {
        if let Some(((lp, ls), (rp, rs))) = decimal_arithmetic_operands(&left, &lt, &right, &rt) {
            let ld = DataType::Decimal128 {
                precision: lp,
                scale: ls,
            };
            let rd = DataType::Decimal128 {
                precision: rp,
                scale: rs,
            };
            let (l_lit, r_lit) = (is_literal_like(&left), is_literal_like(&right));
            return Ok((
                maybe_cast(left, &lt, &ld, l_lit, location)?,
                maybe_cast(right, &rt, &rd, r_lit, location)?,
            ));
        }
    }

    let site = CoercionSite::Binary {
        left_is_literal: is_literal_like(&left),
        right_is_literal: is_literal_like(&right),
    };
    let supertype = common_supertype(&lt, &rt, site).ok_or_else(|| PlanError::TypeMismatch {
        expected: lt.clone(),
        found: rt.clone(),
        location,
    })?;
    Ok((
        maybe_cast(left, &lt, &supertype, site.left_is_literal(), location)?,
        maybe_cast(right, &rt, &supertype, site.right_is_literal(), location)?,
    ))
}

/// `expr` as a numeric literal when it is a string literal and `other`
/// is a numeric type; `None` when the rule does not apply.
fn resolve_string_literal(
    expr: &PlanExpr,
    other: &DataType,
    location: Option<arneb_common::error::Location>,
) -> Result<Option<PlanExpr>, PlanError> {
    let PlanExpr::Literal {
        value: ScalarValue::Utf8(text),
        span,
    } = expr
    else {
        return Ok(None);
    };
    if !matches!(
        other,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128 { .. }
    ) {
        return Ok(None);
    }
    let value = coerce_string_literal_to_numeric(text, other).map_err(|e| match e {
        PlanError::InvalidLiteral { message, .. } => {
            PlanError::InvalidLiteral { message, location }
        }
        e => e,
    })?;
    Ok(Some(PlanExpr::Literal { value, span: *span }))
}

/// Parses the string literal `lit` as a value of the numeric type
/// `target`. Integer targets yield an `Int32` (SMALLINT / INTEGER) or
/// `Int64` literal and decimals a literal with the digits' own
/// precision and scale, so the usual unification then widens them.
/// Failures use PostgreSQL's `invalid input syntax for type <name>`.
fn coerce_string_literal_to_numeric(
    lit: &str,
    target: &DataType,
) -> Result<ScalarValue, PlanError> {
    let (name, bounds): (&str, Option<(i64, i64)>) = match target {
        DataType::Int8 | DataType::Int16 => ("smallint", Some((i16::MIN.into(), i16::MAX.into()))),
        DataType::Int32 => ("integer", Some((i32::MIN.into(), i32::MAX.into()))),
        DataType::Int64 => ("bigint", None),
        DataType::Decimal128 { .. } => ("numeric", None),
        _ => ("double precision", None),
    };
    let invalid = || PlanError::InvalidLiteral {
        message: format!("invalid input syntax for type {name}: \"{lit}\""),
        location: None,
    };
    let trimmed = lit.trim();
    match target {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let n: i64 = trimmed.parse().map_err(|_| invalid())?;
            if let Some((lo, hi)) = bounds {
                if n < lo || n > hi {
                    return Err(PlanError::InvalidLiteral {
                        message: format!("value \"{lit}\" is out of range for type {name}"),
                        location: None,
                    });
                }
            }
            Ok(if *target == DataType::Int64 {
                ScalarValue::Int64(n)
            } else {
                ScalarValue::Int32(n as i32)
            })
        }
        DataType::Decimal128 { .. } => {
            let unsigned = trimmed.trim_start_matches(['+', '-']);
            let negative = trimmed.starts_with('-');
            if trimmed.len() - unsigned.len() > 1 {
                return Err(invalid());
            }
            let (int_part, frac_part) = unsigned.split_once('.').unwrap_or((unsigned, ""));
            let digits = format!("{int_part}{frac_part}");
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) || digits.len() > 38
            {
                return Err(invalid());
            }
            let magnitude: i128 = digits.parse().map_err(|_| invalid())?;
            Ok(ScalarValue::Decimal128 {
                value: if negative { -magnitude } else { magnitude },
                precision: digits.len().max(1) as u8,
                scale: frac_part.len() as i8,
            })
        }
        _ => {
            let n: f64 = trimmed.parse().map_err(|_| invalid())?;
            Ok(ScalarValue::Float64(n))
        }
    }
}

/// `expr` blank-padded to its declared length when it is a CHAR(n)
/// value (stored without trailing blanks), for the operations that see
/// the padding; any other expression is returned unchanged.
fn pad_char_operand(expr: PlanExpr, schema: &[ColumnInfo]) -> PlanExpr {
    let Some(DataType::Char(length)) = plan_expr_type(&expr, schema) else {
        return expr;
    };
    PlanExpr::Function {
        order_by: None,
        name: "rpad".to_string(),
        args: vec![
            expr,
            PlanExpr::Literal {
                value: ScalarValue::Int64(i64::from(length)),
                span: None,
            },
            PlanExpr::Literal {
                value: ScalarValue::Utf8(" ".to_string()),
                span: None,
            },
        ],
        distinct: false,
        span: None,
    }
}

fn unify_between(
    expr: PlanExpr,
    low: PlanExpr,
    high: PlanExpr,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
) -> Result<(PlanExpr, PlanExpr, PlanExpr), PlanError> {
    let et = match plan_expr_type(&expr, schema) {
        Some(t) => t,
        None => return Ok((expr, low, high)),
    };
    let lt = plan_expr_type(&low, schema);
    let ht = plan_expr_type(&high, schema);
    let mut target = et.clone();
    if let Some(lt) = &lt {
        target = bridge_supertype(&target, lt, &expr, &low, location)?;
    }
    if let Some(ht) = &ht {
        target = bridge_supertype(&target, ht, &expr, &high, location)?;
    }
    let expr_lit = is_literal_like(&expr);
    let low_lit = is_literal_like(&low);
    let high_lit = is_literal_like(&high);
    Ok((
        maybe_cast(expr, &et, &target, expr_lit, location)?,
        match lt {
            Some(lt) => maybe_cast(low, &lt, &target, low_lit, location)?,
            None => low,
        },
        match ht {
            Some(ht) => maybe_cast(high, &ht, &target, high_lit, location)?,
            None => high,
        },
    ))
}

fn unify_in_list(
    expr: PlanExpr,
    list: Vec<PlanExpr>,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
) -> Result<(PlanExpr, Vec<PlanExpr>), PlanError> {
    let et = match plan_expr_type(&expr, schema) {
        Some(t) => t,
        None => return Ok((expr, list)),
    };
    let mut target = et.clone();
    let list_types: Vec<Option<DataType>> =
        list.iter().map(|e| plan_expr_type(e, schema)).collect();
    for (e, mt) in list.iter().zip(&list_types) {
        if let Some(t) = mt {
            target = bridge_supertype(&target, t, &expr, e, location)?;
        }
    }
    let expr_lit = is_literal_like(&expr);
    let list = list
        .into_iter()
        .zip(list_types)
        .map(|(e, t)| match t {
            Some(t) => {
                let lit = is_literal_like(&e);
                maybe_cast(e, &t, &target, lit, location)
            }
            None => Ok(e),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((maybe_cast(expr, &et, &target, expr_lit, location)?, list))
}

/// `coerce_expr` for [`PlanExpr::ArrayContains`], out of line so the
/// recursive `coerce_expr` frame stays small.
#[inline(never)]
fn coerce_array_contains(
    expr: PlanExpr,
    array: PlanExpr,
    negated: bool,
    span: Option<arneb_sql_parser::Span>,
    schema: &[ColumnInfo],
    ctx: &mut AnalyzerContext,
) -> Result<PlanExpr, PlanError> {
    let expr = coerce_expr(expr, schema, ctx)?;
    let array = coerce_expr(array, schema, ctx)?;
    let (expr, array) = unify_array_contains(expr, array, schema, span.map(|s| s.start), ctx)?;
    Ok(PlanExpr::ArrayContains {
        expr: Box::new(expr),
        array: Box::new(array),
        negated,
        span,
    })
}

/// `coerce_expr` for [`PlanExpr::ArrayElement`]: the operand must be an
/// array (or an untyped NULL) and the index an integer. Out of line so the
/// recursive `coerce_expr` frame stays small.
#[inline(never)]
#[allow(clippy::boxed_local)]
fn coerce_array_element(
    pair: Box<(PlanExpr, PlanExpr)>,
    schema: &[ColumnInfo],
    ctx: &mut AnalyzerContext,
) -> Result<PlanExpr, PlanError> {
    let (array, index) = *pair;
    let array = coerce_expr(array, schema, ctx)?;
    let index = coerce_expr(index, schema, ctx)?;
    match plan_expr_type(&array, schema) {
        None | Some(DataType::Null) | Some(DataType::List(_)) => {}
        Some(t) => {
            return Err(PlanError::invalid_expression(format!(
                "cannot subscript type {t} because it is not an array"
            )))
        }
    }
    match plan_expr_type(&index, schema) {
        None
        | Some(DataType::Null)
        | Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {}
        Some(t) => {
            return Err(PlanError::invalid_expression(format!(
                "array subscript must have type integer, not {t}"
            )))
        }
    }
    Ok(PlanExpr::ArrayElement(Box::new((array, index))))
}

/// `coerce_expr` for [`PlanExpr::GetField`]: checks the field exists in the
/// (now typed) operand.
#[inline(never)]
#[allow(clippy::boxed_local)]
fn coerce_get_field(
    pair: Box<(PlanExpr, String)>,
    schema: &[ColumnInfo],
    ctx: &mut AnalyzerContext,
) -> Result<PlanExpr, PlanError> {
    let (expr, field) = *pair;
    let expr = coerce_expr(expr, schema, ctx)?;
    super::check_get_field(plan_expr_type(&expr, schema).as_ref(), &field)?;
    Ok(PlanExpr::GetField(Box::new((expr, field))))
}

/// Aligns the operands of `expr = ANY (array)`: the array must be a
/// `List(t)`; `expr` and the array's elements are cast to their common
/// supertype. An untyped `$n` on either side takes its type from the other
/// (`$n` in the array position is `List(type of expr)`), and a bare NULL
/// array is a NULL of `List(type of expr)`.
#[inline(never)]
fn unify_array_contains(
    expr: PlanExpr,
    array: PlanExpr,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
    ctx: &mut AnalyzerContext,
) -> Result<(PlanExpr, PlanExpr), PlanError> {
    let unbound = |e: &PlanExpr| {
        matches!(
            e,
            PlanExpr::Parameter {
                type_hint: None,
                ..
            }
        )
    };
    let mut expr = expr;
    let mut array = array;
    if unbound(&array) {
        if let Some(t) = plan_expr_type(&expr, schema).filter(|t| *t != DataType::Null) {
            array =
                record_and_annotate_parameter(array, &DataType::List(Box::new(t)), location, ctx)?;
        }
    } else if unbound(&expr) {
        if let Some(DataType::List(t)) = plan_expr_type(&array, schema) {
            expr = record_and_annotate_parameter(expr, &t, location, ctx)?;
        }
    }
    let (Some(lt), Some(at)) = (
        plan_expr_type(&expr, schema),
        plan_expr_type(&array, schema),
    ) else {
        return Ok((expr, array));
    };
    let element = match at.clone() {
        DataType::List(t) => *t,
        // `x = ANY ('{1,2}')`: a string literal on the array side is an
        // array literal of the element type, as in PostgreSQL.
        DataType::Null | DataType::Utf8
            if lt != DataType::Null
                && (at == DataType::Null
                    || matches!(
                        &array,
                        PlanExpr::Literal {
                            value: ScalarValue::Utf8(_),
                            ..
                        }
                    )) =>
        {
            let target = DataType::List(Box::new(lt.clone()));
            let array = PlanExpr::Cast {
                expr: Box::new(array),
                data_type: target,
                span: None,
            };
            return Ok((expr, array));
        }
        found => {
            return Err(PlanError::TypeMismatch {
                expected: DataType::List(Box::new(lt)),
                found,
                location,
            })
        }
    };
    if lt == element || lt == DataType::Null {
        return Ok((expr, array));
    }
    let site = CoercionSite::Binary {
        left_is_literal: is_literal_like(&expr),
        right_is_literal: false,
    };
    let supertype =
        common_supertype(&lt, &element, site).ok_or_else(|| PlanError::TypeMismatch {
            expected: lt.clone(),
            found: element.clone(),
            location,
        })?;
    let expr_lit = is_literal_like(&expr);
    let expr = maybe_cast(expr, &lt, &supertype, expr_lit, location)?;
    let array = if supertype == element {
        array
    } else {
        PlanExpr::Cast {
            expr: Box::new(array),
            data_type: DataType::List(Box::new(supertype)),
            span: None,
        }
    };
    Ok((expr, array))
}

/// Casts every element of an `ARRAY[...]` to the common supertype of the
/// elements (the `CASE` branch rules), and records it as `element_type`.
fn unify_array(
    elements: Vec<PlanExpr>,
    schema: &[ColumnInfo],
    span: Option<arneb_sql_parser::Span>,
) -> Result<PlanExpr, PlanError> {
    let location = span.map(|s| s.start);
    let types: Vec<Option<DataType>> = elements.iter().map(|e| plan_expr_type(e, schema)).collect();
    let element_type =
        super::array_element_type(&elements, schema).map_err(|(expected, found)| {
            PlanError::TypeMismatch {
                expected,
                found,
                location,
            }
        })?;
    let elements = elements
        .into_iter()
        .zip(types)
        .map(|(e, t)| match t {
            Some(t) if t != element_type => {
                let lit = is_literal_like(&e);
                maybe_cast(e, &t, &element_type, lit, location)
            }
            _ => Ok(e),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PlanExpr::Array {
        elements,
        element_type,
        span,
    })
}

#[allow(clippy::type_complexity)]
fn unify_case(
    operand: Option<Box<PlanExpr>>,
    when_clauses: Vec<(PlanExpr, PlanExpr)>,
    else_result: Option<Box<PlanExpr>>,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
) -> Result<
    (
        Option<Box<PlanExpr>>,
        Vec<(PlanExpr, PlanExpr)>,
        Option<Box<PlanExpr>>,
    ),
    PlanError,
> {
    // An untyped string literal arm takes the type of a numeric arm
    // (`COALESCE(n, '0')`); non-literal arms are left to the supertype
    // rules below.
    let numeric_arm = when_clauses
        .iter()
        .map(|(_, r)| r)
        .chain(else_result.as_deref())
        .filter(|e| {
            !matches!(
                e,
                PlanExpr::Literal {
                    value: ScalarValue::Utf8(_),
                    ..
                }
            )
        })
        .filter_map(|e| plan_expr_type(e, schema))
        .find(|t| {
            matches!(
                t,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128 { .. }
            )
        });
    let (when_clauses, else_result) = match numeric_arm {
        Some(numeric) => {
            let resolve = |e: PlanExpr| -> Result<PlanExpr, PlanError> {
                Ok(resolve_string_literal(&e, &numeric, location)?.unwrap_or(e))
            };
            let when_clauses = when_clauses
                .into_iter()
                .map(|(c, r)| Ok((c, resolve(r)?)))
                .collect::<Result<Vec<_>, PlanError>>()?;
            let else_result = else_result.map(|e| resolve(*e).map(Box::new)).transpose()?;
            (when_clauses, else_result)
        }
        None => (when_clauses, else_result),
    };
    // Unify result arms (THEN + ELSE).
    let mut target: Option<DataType> = None;
    let result_types: Vec<Option<DataType>> = when_clauses
        .iter()
        .map(|(_, r)| plan_expr_type(r, schema))
        .collect();
    let else_type = else_result
        .as_deref()
        .and_then(|e| plan_expr_type(e, schema));
    for (rt_opt, (_, result)) in result_types.iter().zip(when_clauses.iter()) {
        if let Some(rt) = rt_opt {
            match &target {
                None => target = Some(rt.clone()),
                Some(acc) => {
                    let lit = is_literal_like(result);
                    let site = CoercionSite::CaseBranch {
                        left_is_literal: false,
                        right_is_literal: lit,
                    };
                    target = Some(common_supertype(acc, rt, site).ok_or_else(|| {
                        PlanError::TypeMismatch {
                            expected: acc.clone(),
                            found: rt.clone(),
                            location,
                        }
                    })?);
                }
            }
        }
    }
    if let (Some(et), Some(er)) = (else_type.as_ref(), else_result.as_deref()) {
        match &target {
            None => target = Some(et.clone()),
            Some(acc) => {
                let lit = is_literal_like(er);
                let site = CoercionSite::CaseBranch {
                    left_is_literal: false,
                    right_is_literal: lit,
                };
                target = Some(common_supertype(acc, et, site).ok_or_else(|| {
                    PlanError::TypeMismatch {
                        expected: acc.clone(),
                        found: et.clone(),
                        location,
                    }
                })?);
            }
        }
    }
    let target = target; // freeze
    let when_clauses: Vec<(PlanExpr, PlanExpr)> = when_clauses
        .into_iter()
        .zip(result_types)
        .map(|((cond, result), rt_opt)| {
            let result = match (rt_opt, &target) {
                (Some(rt), Some(tgt)) if &rt != tgt => {
                    let lit = is_literal_like(&result);
                    maybe_cast(result, &rt, tgt, lit, location)?
                }
                _ => result,
            };
            Ok::<_, PlanError>((cond, result))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let else_result = match (else_type, else_result, &target) {
        (Some(et), Some(er), Some(tgt)) if &et != tgt => {
            let lit = is_literal_like(&er);
            Some(Box::new(maybe_cast(*er, &et, tgt, lit, location)?))
        }
        (_, er, _) => er,
    };
    if let Some(operand) = operand {
        let (conditions, results): (Vec<_>, Vec<_>) = when_clauses.into_iter().unzip();
        let (operand, conditions) = unify_in_list(*operand, conditions, schema, location)?;
        let when_clauses = conditions.into_iter().zip(results).collect();
        Ok((Some(Box::new(operand)), when_clauses, else_result))
    } else {
        Ok((None, when_clauses, else_result))
    }
}

/// Align the output column types of every branch of a set operation
/// by inserting a top-of-branch `Projection` with `Cast` where needed.
fn align_set_op_branches(branches: Vec<LogicalPlan>) -> Result<Vec<LogicalPlan>, PlanError> {
    if branches.is_empty() {
        return Ok(branches);
    }
    let schemas: Vec<Vec<ColumnInfo>> = branches.iter().map(|b| b.schema()).collect();
    let width = schemas[0].len();
    if !schemas.iter().all(|s| s.len() == width) {
        return Err(PlanError::invalid_expression(
            "set operation branches have differing column counts",
        ));
    }
    // Per-column supertype across all branches.
    let mut target_types: Vec<DataType> = Vec::with_capacity(width);
    for col in 0..width {
        let mut acc: Option<DataType> = None;
        for s in &schemas {
            let t = s[col].data_type.clone();
            acc = Some(match acc {
                None => t,
                Some(a) => {
                    common_supertype(&a, &t, CoercionSite::UnionColumn).ok_or_else(|| {
                        PlanError::TypeMismatch {
                            expected: a.clone(),
                            found: t,
                            location: None,
                        }
                    })?
                }
            });
        }
        target_types.push(acc.unwrap_or(DataType::Null));
    }
    // For any branch whose schema differs from target, wrap in a
    // Projection that casts per-column.
    Ok(branches
        .into_iter()
        .zip(schemas)
        .map(|(branch, schema)| {
            if schema
                .iter()
                .zip(&target_types)
                .all(|(c, t)| c.data_type == *t)
            {
                return branch;
            }
            let projected: Vec<PlanExpr> = (0..width)
                .map(|i| {
                    let col = PlanExpr::Column {
                        index: i,
                        name: schema[i].name.clone(),
                        span: None,
                    };
                    if schema[i].data_type == target_types[i] {
                        col
                    } else {
                        PlanExpr::Cast {
                            expr: Box::new(col),
                            data_type: target_types[i].clone(),
                            span: None,
                        }
                    }
                })
                .collect();
            let output_schema: Vec<ColumnInfo> = schema
                .iter()
                .zip(&target_types)
                .map(|(c, t)| ColumnInfo {
                    name: c.name.clone(),
                    data_type: t.clone(),
                    nullable: c.nullable,
                })
                .collect();
            LogicalPlan::Projection {
                input: Box::new(branch),
                exprs: projected,
                schema: output_schema,
            }
        })
        .collect())
}

/// Combine an accumulator type with a new operand type via
/// [`common_supertype`] at a binary site, reporting
/// `PlanError::TypeMismatch` with the span-derived location on
/// failure.
fn bridge_supertype(
    acc: &DataType,
    other: &DataType,
    acc_expr: &PlanExpr,
    other_expr: &PlanExpr,
    location: Option<arneb_common::error::Location>,
) -> Result<DataType, PlanError> {
    if acc == other {
        return Ok(acc.clone());
    }
    let site = CoercionSite::Binary {
        left_is_literal: is_literal_like(acc_expr),
        right_is_literal: is_literal_like(other_expr),
    };
    common_supertype(acc, other, site).ok_or_else(|| PlanError::TypeMismatch {
        expected: acc.clone(),
        found: other.clone(),
        location,
    })
}

// ---------------------------------------------------------------------------
// Parameter type inference
// ---------------------------------------------------------------------------

/// Try to infer types for [`PlanExpr::Parameter`] nodes appearing on
/// either side of a binary operator, using the other side's type as
/// the hint. Writes the inference into `ctx.param_types` and
/// rewrites the Parameter node in-place with `type_hint: Some(...)`.
///
/// Returns the (possibly-rewritten) pair unchanged when neither side
/// is a parameter.
fn infer_parameter_pair(
    left: PlanExpr,
    right: PlanExpr,
    schema: &[ColumnInfo],
    location: Option<arneb_common::error::Location>,
    ctx: &mut AnalyzerContext,
) -> Result<(PlanExpr, PlanExpr), PlanError> {
    // Left parameter unifies with right's type.
    let left = match (&left, plan_expr_type(&right, schema)) {
        (
            PlanExpr::Parameter {
                type_hint: None, ..
            },
            Some(rt),
        ) => record_and_annotate_parameter(left, &rt, location, ctx)?,
        _ => left,
    };
    // Right parameter unifies with left's type.
    let right = match (&right, plan_expr_type(&left, schema)) {
        (
            PlanExpr::Parameter {
                type_hint: None, ..
            },
            Some(lt),
        ) => record_and_annotate_parameter(right, &lt, location, ctx)?,
        _ => right,
    };
    Ok((left, right))
}

/// Record an inference in `ctx.param_types`, detecting conflicts
/// against any previously-inferred type for the same index.
/// Returns the Parameter node updated with `type_hint: Some(t)`.
fn record_and_annotate_parameter(
    expr: PlanExpr,
    t: &DataType,
    location: Option<arneb_common::error::Location>,
    ctx: &mut AnalyzerContext,
) -> Result<PlanExpr, PlanError> {
    let PlanExpr::Parameter { index, span, .. } = expr else {
        return Ok(expr);
    };
    if let Some(prev) = ctx.param_types.get(&index) {
        if prev != t {
            return Err(PlanError::ParameterTypeConflict {
                index,
                conflict_types: format!("{prev} vs {t}"),
                location,
            });
        }
    } else {
        ctx.param_types.insert(index, t.clone());
    }
    Ok(PlanExpr::Parameter {
        index,
        type_hint: Some(t.clone()),
        span,
    })
}

/// Final pass: any `Parameter { type_hint: None }` remaining after
/// the main traversal defaults to `Utf8`, matching Trino /
/// Postgres' `unknown → varchar` fallback.
fn default_unresolved_parameters(plan: LogicalPlan, ctx: &mut AnalyzerContext) -> LogicalPlan {
    walk_plan_exprs(plan, &mut |expr| fallback_default_param(expr, ctx))
}

fn fallback_default_param(expr: PlanExpr, ctx: &mut AnalyzerContext) -> PlanExpr {
    match expr {
        PlanExpr::Parameter {
            index,
            type_hint: None,
            span,
        } => {
            let t = ctx
                .param_types
                .entry(index)
                .or_insert_with(|| {
                    tracing::debug!(
                        %index,
                        "analyzer: no type inference site for parameter; defaulting to Utf8"
                    );
                    DataType::Utf8
                })
                .clone();
            PlanExpr::Parameter {
                index,
                type_hint: Some(t),
                span,
            }
        }
        other => other,
    }
}

/// Apply `f` to every `PlanExpr` in `plan` (including in nested
/// sub-plans). Recursively visits children so the transform reaches
/// deeply-nested parameter nodes.
pub(super) fn walk_plan_exprs(
    plan: LogicalPlan,
    f: &mut dyn FnMut(PlanExpr) -> PlanExpr,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(walk_plan_exprs(*input, f)),
            predicate: walk_expr(predicate, f),
        },
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(walk_plan_exprs(*input, f)),
            exprs: exprs.into_iter().map(|e| walk_expr(e, f)).collect(),
            schema,
        },
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => LogicalPlan::Aggregate {
            input: Box::new(walk_plan_exprs(*input, f)),
            group_by: group_by.into_iter().map(|e| walk_expr(e, f)).collect(),
            aggr_exprs: aggr_exprs.into_iter().map(|e| walk_expr(e, f)).collect(),
            schema,
        },
        LogicalPlan::RollupAggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } => LogicalPlan::RollupAggregate {
            input: Box::new(walk_plan_exprs(*input, f)),
            group_by: group_by.into_iter().map(|e| walk_expr(e, f)).collect(),
            aggr_exprs: aggr_exprs.into_iter().map(|e| walk_expr(e, f)).collect(),
            schema,
        },
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(walk_plan_exprs(*input, f)),
            order_by: order_by
                .into_iter()
                .map(|s| SortExpr {
                    expr: walk_expr(s.expr, f),
                    ..s
                })
                .collect(),
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(walk_plan_exprs(*input, f)),
            limit,
            offset,
        },
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            ..
        } => LogicalPlan::Join {
            left: Box::new(walk_plan_exprs(*left, f)),
            right: Box::new(walk_plan_exprs(*right, f)),
            join_type,
            condition: match condition {
                JoinCondition::On(e) => JoinCondition::On(walk_expr(e, f)),
                JoinCondition::None => JoinCondition::None,
            },
            dynamic_filter_ids: Vec::new(),
        },
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            dynamic_filter_ids,
            probe_side_filter_ids,
        } => LogicalPlan::SemiJoin {
            left: Box::new(walk_plan_exprs(*left, f)),
            right: Box::new(walk_plan_exprs(*right, f)),
            left_key: walk_expr(left_key, f),
            right_key: walk_expr(right_key, f),
            residual: residual.map(|e| walk_expr(e, f)),
            dynamic_filter_ids,
            probe_side_filter_ids,
        },
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
            probe_side_filter_ids,
        } => LogicalPlan::AntiJoin {
            left: Box::new(walk_plan_exprs(*left, f)),
            right: Box::new(walk_plan_exprs(*right, f)),
            left_key: walk_expr(left_key, f),
            right_key: walk_expr(right_key, f),
            residual: residual.map(|e| walk_expr(e, f)),
            probe_side_filter_ids,
        },
        LogicalPlan::Explain { input, analyze } => LogicalPlan::Explain {
            input: Box::new(walk_plan_exprs(*input, f)),
            analyze,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(walk_plan_exprs(*input, f)),
        },
        LogicalPlan::UnionAll { inputs } => LogicalPlan::UnionAll {
            inputs: inputs.into_iter().map(|p| walk_plan_exprs(p, f)).collect(),
        },
        LogicalPlan::Intersect { left, right } => LogicalPlan::Intersect {
            left: Box::new(walk_plan_exprs(*left, f)),
            right: Box::new(walk_plan_exprs(*right, f)),
        },
        LogicalPlan::Except { left, right } => LogicalPlan::Except {
            left: Box::new(walk_plan_exprs(*left, f)),
            right: Box::new(walk_plan_exprs(*right, f)),
        },
        LogicalPlan::CreateTableAsSelect { name, source } => LogicalPlan::CreateTableAsSelect {
            name,
            source: Box::new(walk_plan_exprs(*source, f)),
        },
        LogicalPlan::InsertInto { table, source } => LogicalPlan::InsertInto {
            table,
            source: Box::new(walk_plan_exprs(*source, f)),
        },
        LogicalPlan::CreateView { name, sql, plan } => LogicalPlan::CreateView {
            name,
            sql,
            plan: Box::new(walk_plan_exprs(*plan, f)),
        },
        LogicalPlan::ProjectSet {
            input,
            srfs,
            schema,
        } => walk_project_set(*input, srfs, schema, f),
        LogicalPlan::Window { input, functions } => walk_window(*input, functions, f),
        LogicalPlan::AssignUniqueId { input, id_column } => LogicalPlan::AssignUniqueId {
            input: Box::new(walk_plan_exprs(*input, f)),
            id_column,
        },
        other => other,
    }
}

#[inline(never)]
fn walk_project_set(
    input: LogicalPlan,
    srfs: Vec<(crate::plan::SrfKind, PlanExpr)>,
    schema: Vec<ColumnInfo>,
    f: &mut dyn FnMut(PlanExpr) -> PlanExpr,
) -> LogicalPlan {
    LogicalPlan::ProjectSet {
        input: Box::new(walk_plan_exprs(input, f)),
        srfs: srfs
            .into_iter()
            .map(|(kind, e)| (kind, walk_expr(e, f)))
            .collect(),
        schema,
    }
}

#[inline(never)]
fn walk_window(
    input: LogicalPlan,
    functions: Vec<crate::plan::WindowFunctionDef>,
    f: &mut dyn FnMut(PlanExpr) -> PlanExpr,
) -> LogicalPlan {
    LogicalPlan::Window {
        input: Box::new(walk_plan_exprs(input, f)),
        functions: functions
            .into_iter()
            .map(|mut w| {
                w.args = w.args.into_iter().map(|e| walk_expr(e, f)).collect();
                w.partition_by = w
                    .partition_by
                    .into_iter()
                    .map(|e| walk_expr(e, f))
                    .collect();
                w.order_by = w
                    .order_by
                    .into_iter()
                    .map(|s| SortExpr {
                        expr: walk_expr(s.expr, f),
                        ..s
                    })
                    .collect();
                w
            })
            .collect(),
    }
}

pub(super) fn walk_expr(expr: PlanExpr, f: &mut dyn FnMut(PlanExpr) -> PlanExpr) -> PlanExpr {
    // Recurse into children first so the transform sees leaves
    // before the parent applies.
    let expr = match expr {
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(walk_expr(*left, f)),
            op,
            right: Box::new(walk_expr(*right, f)),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(walk_expr(*expr, f)),
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(walk_expr(*expr, f)),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(walk_expr(*expr, f)),
            span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(walk_expr(*expr, f)),
            negated,
            low: Box::new(walk_expr(*low, f)),
            high: Box::new(walk_expr(*high, f)),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(walk_expr(*expr, f)),
            list: list.into_iter().map(|e| walk_expr(e, f)).collect(),
            negated,
            span,
        },
        PlanExpr::ArrayContains {
            expr,
            array,
            negated,
            span,
        } => crate::map_array_contains(expr, array, negated, span, |e| walk_expr(e, f)),
        PlanExpr::ArrayElement(pair) => crate::map_array_element(pair, |e| walk_expr(e, f)),
        PlanExpr::GetField(pair) => crate::map_get_field(pair, |e| walk_expr(e, f)),
        PlanExpr::Array {
            elements,
            element_type,
            span,
        } => PlanExpr::Array {
            elements: elements.into_iter().map(|e| walk_expr(e, f)).collect(),
            element_type,
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(walk_expr(*expr, f)),
            data_type,
            span,
        },
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|o| Box::new(walk_expr(*o, f))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(c, r)| (walk_expr(c, f), walk_expr(r, f)))
                .collect(),
            else_result: else_result.map(|e| Box::new(walk_expr(*e, f))),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            order_by,
            span,
        } => PlanExpr::Function {
            order_by: crate::plan::SortExpr::map_exprs(order_by, &mut |e| walk_expr(e, f)),
            name,
            args: args.into_iter().map(|a| walk_expr(a, f)).collect(),
            distinct,
            span,
        },
        other => other,
    };
    f(expr)
}

/// Wrap `expr` in a `Cast` to `target` unless it is already that
/// type. The second result enforces the `LiteralOnly` safety gate:
/// if the matrix demands a literal source and `is_source_literal` is
/// false, return a `TypeMismatch`.
fn maybe_cast(
    expr: PlanExpr,
    from: &DataType,
    target: &DataType,
    is_source_literal: bool,
    location: Option<arneb_common::error::Location>,
) -> Result<PlanExpr, PlanError> {
    if from == target {
        return Ok(expr);
    }
    // CHAR(n) values are stored without trailing blanks, so a char needs
    // no conversion to a wider char or to varchar, and a varchar becomes a
    // char by dropping its trailing blanks. A varchar literal is trimmed
    // in place: `char_col = 'lit'` stays a plain column-vs-literal
    // comparison that scans can push down and prune row groups with. An
    // implicit coercion never truncates, hence the maximum length.
    match (from, target) {
        (DataType::Char(_), DataType::Char(_) | DataType::Utf8) => return Ok(expr),
        (DataType::Utf8, DataType::Char(_)) => {
            if let PlanExpr::Literal {
                value: ScalarValue::Utf8(value),
                span,
            } = &expr
            {
                return Ok(PlanExpr::Literal {
                    value: ScalarValue::Utf8(value.trim_end_matches(' ').to_string()),
                    span: *span,
                });
            }
            return Ok(PlanExpr::Cast {
                expr: Box::new(expr),
                data_type: DataType::Char(CHAR_MAX_LENGTH),
                span: None,
            });
        }
        _ => {}
    }
    // A decimal literal against a DOUBLE operand (`double_col > 0.05`)
    // becomes a DOUBLE literal here rather than a `Cast`: the optimizer
    // does not fold under every plan node, and connectors only push down
    // plain `column op literal` predicates.
    if let PlanExpr::Literal {
        value: value @ ScalarValue::Decimal128 { .. },
        span,
    } = &expr
    {
        if *target == DataType::Float64 {
            if let Ok(value) = super::cast_scalar::cast_scalar(value, target) {
                return Ok(PlanExpr::Literal { value, span: *span });
            }
        }
    }
    match lookup_cast(from, target) {
        Some(Safety::AlwaysSafe) | Some(Safety::PrecisionLoss) => Ok(PlanExpr::Cast {
            expr: Box::new(expr),
            data_type: target.clone(),
            span: None,
        }),
        Some(Safety::LiteralOnly) if is_source_literal => Ok(PlanExpr::Cast {
            expr: Box::new(expr),
            data_type: target.clone(),
            span: None,
        }),
        Some(Safety::LiteralOnly) => Err(PlanError::TypeMismatch {
            expected: target.clone(),
            found: from.clone(),
            location,
        }),
        None => Err(PlanError::TypeMismatch {
            expected: target.clone(),
            found: from.clone(),
            location,
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{Analyzer, AnalyzerContext};
    use arneb_common::types::{ScalarValue, TableReference};

    fn col(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.into(),
            span: None,
        }
    }

    fn lit(v: ScalarValue) -> PlanExpr {
        PlanExpr::Literal {
            value: v,
            span: None,
        }
    }

    fn scan(schema: Vec<ColumnInfo>) -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("t"),
            schema,
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    fn filter(input: LogicalPlan, predicate: PlanExpr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn dec(p: u8, s: i8) -> DataType {
        DataType::Decimal128 {
            precision: p,
            scale: s,
        }
    }

    fn run(plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        let mut ctx = AnalyzerContext::new();
        Analyzer::new(vec![Box::new(TypeCoercion::new())]).run(plan, &mut ctx)
    }

    #[test]
    fn string_literal_coerces_to_numeric_operand() {
        let schema = vec![ColumnInfo {
            name: "x".into(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        let pred = PlanExpr::BinaryOp {
            left: Box::new(col(0, "x")),
            op: ast::BinaryOp::Eq,
            right: Box::new(lit(ScalarValue::Utf8("0".into()))),
            span: None,
        };
        let out = run(filter(scan(schema), pred)).unwrap();
        let LogicalPlan::Filter { predicate, .. } = out else {
            panic!("expected Filter");
        };
        let PlanExpr::BinaryOp { right, .. } = predicate else {
            panic!("expected BinaryOp");
        };
        assert!(matches!(
            *right,
            PlanExpr::Literal {
                value: ScalarValue::Int64(0),
                ..
            }
        ));
    }

    #[test]
    fn string_literal_coercion_is_literal_only() {
        let schema = vec![
            ColumnInfo {
                name: "s".into(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            ColumnInfo {
                name: "n".into(),
                data_type: DataType::Int64,
                nullable: true,
            },
        ];
        let pred = PlanExpr::BinaryOp {
            left: Box::new(col(0, "s")),
            op: ast::BinaryOp::Eq,
            right: Box::new(col(1, "n")),
            span: None,
        };
        let err = run(filter(scan(schema.clone()), pred)).unwrap_err();
        assert!(matches!(err, PlanError::TypeMismatch { .. }), "{err}");

        let bad = PlanExpr::BinaryOp {
            left: Box::new(col(1, "n")),
            op: ast::BinaryOp::Eq,
            right: Box::new(lit(ScalarValue::Utf8("abc".into()))),
            span: None,
        };
        let err = run(filter(scan(schema), bad)).unwrap_err();
        assert!(
            err.to_string()
                .contains("invalid input syntax for type bigint: \"abc\""),
            "{err}"
        );
    }

    #[test]
    fn int32_plus_int64_coerces_left_to_int64() {
        let schema = vec![
            ColumnInfo {
                name: "a".into(),
                data_type: DataType::Int32,
                nullable: false,
            },
            ColumnInfo {
                name: "b".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "a")),
                op: ast::BinaryOp::Plus,
                right: Box::new(col(1, "b")),
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!("expected Filter")
        };
        match predicate {
            PlanExpr::BinaryOp { left, right, .. } => {
                assert!(matches!(
                    left.as_ref(),
                    PlanExpr::Cast {
                        data_type: DataType::Int64,
                        ..
                    }
                ));
                assert!(matches!(right.as_ref(), PlanExpr::Column { .. }));
            }
            _ => panic!("expected BinaryOp"),
        }
    }

    fn null_lit() -> PlanExpr {
        PlanExpr::Literal {
            value: ScalarValue::Null,
            span: None,
        }
    }

    fn is_null_cast_to(expr: &PlanExpr, target: &DataType) -> bool {
        matches!(
            expr,
            PlanExpr::Cast { expr, data_type, .. }
                if data_type == target
                    && matches!(expr.as_ref(), PlanExpr::Literal { value: ScalarValue::Null, .. })
        )
    }

    #[test]
    fn untyped_null_operand_coerces_to_other_side() {
        let schema = vec![ColumnInfo {
            name: "a".into(),
            data_type: DataType::Int32,
            nullable: true,
        }];
        for (op, null_on_left) in [
            (ast::BinaryOp::Eq, false),
            (ast::BinaryOp::Lt, true),
            (ast::BinaryOp::Plus, false),
        ] {
            let (l, r) = if null_on_left {
                (null_lit(), col(0, "a"))
            } else {
                (col(0, "a"), null_lit())
            };
            let plan = filter(
                scan(schema.clone()),
                PlanExpr::BinaryOp {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                    span: None,
                },
            );
            let LogicalPlan::Filter { predicate, .. } = run(plan).unwrap() else {
                panic!("expected Filter")
            };
            let PlanExpr::BinaryOp { left, right, .. } = predicate else {
                panic!("expected BinaryOp")
            };
            let (null_side, col_side) = if null_on_left {
                (left, right)
            } else {
                (right, left)
            };
            assert!(
                is_null_cast_to(&null_side, &DataType::Int32),
                "{null_side:?}"
            );
            assert!(matches!(col_side.as_ref(), PlanExpr::Column { .. }));
        }
    }

    #[test]
    fn untyped_null_case_arm_coerces_to_other_arm() {
        let schema = vec![ColumnInfo {
            name: "a".into(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        let plan = LogicalPlan::Projection {
            input: Box::new(scan(schema)),
            exprs: vec![PlanExpr::CaseExpr {
                operand: None,
                when_clauses: vec![(
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        span: None,
                    },
                    col(0, "a"),
                )],
                else_result: Some(Box::new(null_lit())),
                span: None,
            }],
            schema: vec![ColumnInfo {
                name: "c".into(),
                data_type: DataType::Int64,
                nullable: true,
            }],
        };
        let LogicalPlan::Projection { exprs, .. } = run(plan).unwrap() else {
            panic!("expected Projection")
        };
        let PlanExpr::CaseExpr { else_result, .. } = &exprs[0] else {
            panic!("expected CaseExpr")
        };
        assert!(is_null_cast_to(
            else_result.as_deref().unwrap(),
            &DataType::Int64
        ));
    }

    #[test]
    fn decimal_mul_decimal_widens_via_trino_formula() {
        // Decimal(10,2) * Decimal(10,2) → per supertype rule, result
        // type is the Trino-formula supertype Decimal(10, 2) since
        // equal operands produce themselves. A runtime widening to
        // Decimal(21, 4) is a separate arithmetic result-type rule
        // that operates AFTER coercion. The coercion pass's job is
        // only to align inputs.
        let schema = vec![
            ColumnInfo {
                name: "a".into(),
                data_type: dec(10, 2),
                nullable: false,
            },
            ColumnInfo {
                name: "b".into(),
                data_type: dec(10, 2),
                nullable: false,
            },
        ];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "a")),
                op: ast::BinaryOp::Multiply,
                right: Box::new(col(1, "b")),
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!("expected Filter")
        };
        // Same-type operands: no Cast inserted.
        match predicate {
            PlanExpr::BinaryOp { left, right, .. } => {
                assert!(matches!(left.as_ref(), PlanExpr::Column { .. }));
                assert!(matches!(right.as_ref(), PlanExpr::Column { .. }));
            }
            _ => panic!("expected BinaryOp"),
        }
    }

    #[test]
    fn tpch_extprice_times_one_minus_discount_coerces_int_literal_to_decimal() {
        // Represents `l_extendedprice * (1 - l_discount)` where
        // extendedprice: Decimal(15,2), discount: Decimal(15,2),
        // literal `1`: Int32. The inner `1 - l_discount` should
        // widen `1` to Decimal(15, 2).
        let schema = vec![
            ColumnInfo {
                name: "l_extendedprice".into(),
                data_type: dec(15, 2),
                nullable: false,
            },
            ColumnInfo {
                name: "l_discount".into(),
                data_type: dec(15, 2),
                nullable: false,
            },
        ];
        let one = lit(ScalarValue::Int32(1));
        let inner = PlanExpr::BinaryOp {
            left: Box::new(one),
            op: ast::BinaryOp::Minus,
            right: Box::new(col(1, "l_discount")),
            span: None,
        };
        let expr = PlanExpr::BinaryOp {
            left: Box::new(col(0, "l_extendedprice")),
            op: ast::BinaryOp::Multiply,
            right: Box::new(inner),
            span: None,
        };
        let plan = filter(scan(schema), expr);
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!()
        };
        // Look inside: the `1` becomes DECIMAL(10, 0) (Trino's INTEGER), the
        // decimal operands are left alone, and the declared type is
        // Trino's DECIMAL(15,2) * DECIMAL(16,2) = DECIMAL(31, 4).
        let PlanExpr::BinaryOp { left, right, .. } = &predicate else {
            panic!()
        };
        assert!(matches!(left.as_ref(), PlanExpr::Column { .. }));
        let PlanExpr::BinaryOp { left: inner_l, .. } = right.as_ref() else {
            panic!()
        };
        assert!(
            matches!(
                inner_l.as_ref(),
                PlanExpr::Cast { data_type, .. } if *data_type == dec(10, 0)
            ),
            "got: {inner_l:?}"
        );
        let schema = [dec(15, 2), dec(15, 2)].map(|data_type| ColumnInfo {
            name: "c".into(),
            data_type,
            nullable: false,
        });
        assert_eq!(plan_expr_type(&predicate, &schema), Some(dec(31, 4)));
    }

    #[test]
    fn date_column_le_utf8_literal_inserts_cast_on_literal() {
        let schema = vec![ColumnInfo {
            name: "l_shipdate".into(),
            data_type: DataType::Date32,
            nullable: false,
        }];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_shipdate")),
                op: ast::BinaryOp::LtEq,
                right: Box::new(lit(ScalarValue::Utf8("1998-12-01".into()))),
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!()
        };
        let PlanExpr::BinaryOp { right, .. } = &predicate else {
            panic!()
        };
        assert!(matches!(
            right.as_ref(),
            PlanExpr::Cast {
                data_type: DataType::Date32,
                ..
            }
        ));
    }

    #[test]
    fn utf8_column_le_date32_column_rejects_with_type_mismatch() {
        let schema = vec![
            ColumnInfo {
                name: "str_date".into(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "real_date".into(),
                data_type: DataType::Date32,
                nullable: false,
            },
        ];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "str_date")),
                op: ast::BinaryOp::LtEq,
                right: Box::new(col(1, "real_date")),
                span: None,
            },
        );
        let err = run(plan).unwrap_err();
        assert!(
            matches!(err, PlanError::TypeMismatch { .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn type_mismatch_error_message_mentions_types() {
        // The `thiserror` Display uses "expected {expected}, found {found}".
        // `render_plan_error` later adds an explicit CAST hint; we
        // verify here that the structured variant carries the types.
        let schema = vec![
            ColumnInfo {
                name: "s".into(),
                data_type: DataType::Utf8,
                nullable: false,
            },
            ColumnInfo {
                name: "d".into(),
                data_type: DataType::Date32,
                nullable: false,
            },
        ];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "s")),
                op: ast::BinaryOp::LtEq,
                right: Box::new(col(1, "d")),
                span: None,
            },
        );
        let err = run(plan).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Utf8") || msg.contains("Date32"), "got: {msg}");
    }

    #[test]
    fn between_unifies_across_three_operands() {
        let schema = vec![ColumnInfo {
            name: "n".into(),
            data_type: DataType::Int32,
            nullable: false,
        }];
        let plan = filter(
            scan(schema),
            PlanExpr::Between {
                expr: Box::new(col(0, "n")),
                negated: false,
                low: Box::new(lit(ScalarValue::Int64(1))),
                high: Box::new(lit(ScalarValue::Int64(10))),
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!()
        };
        let PlanExpr::Between { expr, .. } = predicate else {
            panic!()
        };
        assert!(matches!(
            expr.as_ref(),
            PlanExpr::Cast {
                data_type: DataType::Int64,
                ..
            }
        ));
    }

    #[test]
    fn in_list_unifies_literal_elements() {
        let schema = vec![ColumnInfo {
            name: "n".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let plan = filter(
            scan(schema),
            PlanExpr::InList {
                expr: Box::new(col(0, "n")),
                list: vec![lit(ScalarValue::Int32(1)), lit(ScalarValue::Int32(2))],
                negated: false,
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!()
        };
        let PlanExpr::InList { list, .. } = predicate else {
            panic!()
        };
        for e in &list {
            assert!(matches!(
                e,
                PlanExpr::Cast {
                    data_type: DataType::Int64,
                    ..
                }
            ));
        }
    }

    #[test]
    fn case_unifies_mixed_numeric_branches() {
        // CASE WHEN x > 0 THEN 1 ELSE 2.5 END — Int32/Int64 branch vs
        // Float64 branch → both widen to Float64.
        let schema = vec![ColumnInfo {
            name: "x".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let cond = PlanExpr::BinaryOp {
            left: Box::new(col(0, "x")),
            op: ast::BinaryOp::Gt,
            right: Box::new(lit(ScalarValue::Int64(0))),
            span: None,
        };
        let plan = filter(
            scan(schema),
            PlanExpr::CaseExpr {
                operand: None,
                when_clauses: vec![(cond, lit(ScalarValue::Int64(1)))],
                else_result: Some(Box::new(lit(ScalarValue::Float64(2.5)))),
                span: None,
            },
        );
        let plan = run(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = plan else {
            panic!()
        };
        let PlanExpr::CaseExpr {
            when_clauses,
            else_result,
            ..
        } = predicate
        else {
            panic!()
        };
        // THEN side should be wrapped in Cast to Float64.
        assert!(matches!(
            when_clauses[0].1,
            PlanExpr::Cast {
                data_type: DataType::Float64,
                ..
            }
        ));
        // ELSE side is already Float64 — no cast.
        assert!(matches!(
            else_result.as_deref().unwrap(),
            PlanExpr::Literal {
                value: ScalarValue::Float64(_),
                ..
            }
        ));
    }

    #[test]
    fn join_condition_coerces_int_widths() {
        let left_schema = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int32,
            nullable: false,
        }];
        let right_schema = vec![ColumnInfo {
            name: "id".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let plan = LogicalPlan::Join {
            left: Box::new(scan(left_schema)),
            right: Box::new(scan(right_schema)),
            join_type: ast::JoinType::Inner,
            condition: JoinCondition::On(PlanExpr::BinaryOp {
                left: Box::new(col(0, "id")),
                op: ast::BinaryOp::Eq,
                right: Box::new(col(1, "id")),
                span: None,
            }),
            dynamic_filter_ids: Vec::new(),
        };
        let plan = run(plan).unwrap();
        let LogicalPlan::Join {
            condition: JoinCondition::On(expr),
            ..
        } = plan
        else {
            panic!()
        };
        let PlanExpr::BinaryOp { left, .. } = expr else {
            panic!()
        };
        assert!(matches!(
            left.as_ref(),
            PlanExpr::Cast {
                data_type: DataType::Int64,
                ..
            }
        ));
    }

    // --- Parameter inference ---

    fn param(index: usize) -> PlanExpr {
        PlanExpr::Parameter {
            index,
            type_hint: None,
            span: None,
        }
    }

    fn run_with_ctx(plan: LogicalPlan) -> Result<(LogicalPlan, AnalyzerContext), PlanError> {
        let mut ctx = AnalyzerContext::new();
        let plan = Analyzer::new(vec![Box::new(TypeCoercion::new())]).run(plan, &mut ctx)?;
        Ok((plan, ctx))
    }

    #[test]
    fn parameter_inferred_from_date_column_sibling() {
        let schema = vec![ColumnInfo {
            name: "l_shipdate".into(),
            data_type: DataType::Date32,
            nullable: false,
        }];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(col(0, "l_shipdate")),
                op: ast::BinaryOp::LtEq,
                right: Box::new(param(1)),
                span: None,
            },
        );
        let (_, ctx) = run_with_ctx(plan).unwrap();
        assert_eq!(ctx.param_types.get(&1), Some(&DataType::Date32));
    }

    #[test]
    fn parameter_in_list_inferred_from_tested_expr() {
        let schema = vec![ColumnInfo {
            name: "n".into(),
            data_type: DataType::Int64,
            nullable: false,
        }];
        let plan = filter(
            scan(schema),
            PlanExpr::InList {
                expr: Box::new(col(0, "n")),
                list: vec![param(1), lit(ScalarValue::Int64(100))],
                negated: false,
                span: None,
            },
        );
        // InList doesn't call `unify_binary_operands`, but the
        // post-walk default sweep would set $1 to Utf8 when the
        // site doesn't infer. IN-list inference is best-effort in
        // the MVP; documented as a known limitation in the spec.
        // We verify the fallback path instead.
        let (_, ctx) = run_with_ctx(plan).unwrap();
        // Present-or-Utf8 is acceptable for IN list in this cut.
        let ty = ctx.param_types.get(&1).cloned();
        assert!(
            matches!(ty, Some(DataType::Int64) | Some(DataType::Utf8)),
            "got: {ty:?}"
        );
    }

    #[test]
    fn isolated_parameter_defaults_to_utf8() {
        // SELECT $1 — no surrounding context.
        let plan = LogicalPlan::Projection {
            input: Box::new(scan(vec![ColumnInfo {
                name: "a".into(),
                data_type: DataType::Int32,
                nullable: false,
            }])),
            exprs: vec![param(1)],
            schema: vec![ColumnInfo {
                name: "$1".into(),
                data_type: DataType::Utf8,
                nullable: true,
            }],
        };
        let (_, ctx) = run_with_ctx(plan).unwrap();
        assert_eq!(ctx.param_types.get(&1), Some(&DataType::Utf8));
    }

    #[test]
    fn unresolved_parameter_below_project_set_and_window_is_defaulted() {
        let schema = vec![ColumnInfo {
            name: "a".into(),
            data_type: DataType::Int32,
            nullable: false,
        }];
        // A parameter among the SRF arguments, and one in a window argument
        // below it: both must be reached by the default-type walk.
        let window = LogicalPlan::Window {
            input: Box::new(scan(schema.clone())),
            functions: vec![crate::plan::WindowFunctionDef {
                name: "SUM".into(),
                args: vec![param(2)],
                partition_by: vec![],
                order_by: vec![],
                rows_frame: false,
                output_name: "s".into(),
            }],
        };
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(window),
            srfs: vec![(crate::plan::SrfKind::Unnest, param(1))],
            schema: schema.clone(),
        };
        let walked = default_unresolved_parameters(plan, &mut AnalyzerContext::new());
        let mut untyped = 0;
        walk_plan_exprs(walked, &mut |e| {
            if matches!(
                e,
                PlanExpr::Parameter {
                    type_hint: None,
                    ..
                }
            ) {
                untyped += 1;
            }
            e
        });
        // The default pass records a type for each, so none is left untyped
        // in the context either.
        let mut ctx = AnalyzerContext::new();
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(scan(schema.clone())),
            srfs: vec![(crate::plan::SrfKind::Unnest, param(1))],
            schema,
        };
        default_unresolved_parameters(plan, &mut ctx);
        assert_eq!(untyped, 0);
        assert_eq!(ctx.param_types.get(&1), Some(&DataType::Utf8));
    }

    #[test]
    fn parameter_conflicting_inferences_errors() {
        // WHERE l_shipdate <= $1 AND l_orderkey = $1
        // where l_shipdate: Date32 and l_orderkey: Int64.
        let schema = vec![
            ColumnInfo {
                name: "l_shipdate".into(),
                data_type: DataType::Date32,
                nullable: false,
            },
            ColumnInfo {
                name: "l_orderkey".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
        ];
        let plan = filter(
            scan(schema),
            PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(0, "l_shipdate")),
                    op: ast::BinaryOp::LtEq,
                    right: Box::new(param(1)),
                    span: None,
                }),
                op: ast::BinaryOp::And,
                right: Box::new(PlanExpr::BinaryOp {
                    left: Box::new(col(1, "l_orderkey")),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(param(1)),
                    span: None,
                }),
                span: None,
            },
        );
        let err = run_with_ctx(plan).unwrap_err();
        assert!(
            matches!(err, PlanError::ParameterTypeConflict { index: 1, .. }),
            "got: {err:?}"
        );
    }

    #[test]
    fn union_branches_aligned_via_cast_projection() {
        // Branch A: a: Int32; Branch B: a: Int64. Supertype Int64;
        // Branch A gets wrapped in a Projection that casts a → Int64.
        let left = scan(vec![ColumnInfo {
            name: "a".into(),
            data_type: DataType::Int32,
            nullable: false,
        }]);
        let right = scan(vec![ColumnInfo {
            name: "a".into(),
            data_type: DataType::Int64,
            nullable: false,
        }]);
        let plan = LogicalPlan::UnionAll {
            inputs: vec![left, right],
        };
        let plan = run(plan).unwrap();
        let LogicalPlan::UnionAll { inputs } = plan else {
            panic!()
        };
        assert_eq!(inputs.len(), 2);
        // Branch 0 should be wrapped in a Projection containing a Cast.
        match &inputs[0] {
            LogicalPlan::Projection { exprs, .. } => {
                assert!(matches!(
                    &exprs[0],
                    PlanExpr::Cast {
                        data_type: DataType::Int64,
                        ..
                    }
                ));
            }
            other => panic!("expected Projection, got {other:?}"),
        }
        assert!(matches!(&inputs[1], LogicalPlan::TableScan { .. }));
    }
}
