//! Correlated scalar subqueries in `SELECT` lists.
//!
//! [`QueryPlanner::rewrite_select_list_subqueries`] runs before the
//! projection is planned. It finds every `(SELECT ...)` in the select items,
//! at any depth (inside `CASE` conditions and branches, function arguments,
//! comparisons, casts, arithmetic), and replaces each *correlated* one with
//! an expression over the columns of a LEFT JOIN that
//! [`QueryPlanner::attach_select_subquery_joins`] adds to the input:
//!
//! - The subquery's `WHERE` must be a conjunction of correlation equalities
//!   `inner_expr = outer_expr` (either side may be an expression), inner-only
//!   predicates (kept in the subquery) and outer-only predicates (lifted out
//!   as a guard).
//! - An aggregate subquery reuses the WHERE-side [`LeftJoinSpec`] build, so
//!   an unmatched or guarded-out row gets the empty-set value (COUNT → 0).
//! - A non-aggregate subquery is grouped by its keys, carrying each inner
//!   column its value reads (`min`, or `bool_and` for booleans) and the
//!   group's row count. Its value is then evaluated over the outer row as
//!   `CASE WHEN <guard> AND rows IS NOT NULL THEN __single_row_check(rows,
//!   value) END`. `__single_row_check` raises PostgreSQL's multi-row error;
//!   since `CASE` evaluates a branch only on the rows that take it, a
//!   subquery under a branch that is not taken never raises it.
//!
//! Uncorrelated subqueries are left alone and keep the execute-once-and-fold
//! path. Other correlated shapes fail with a "correlated subquery ... is not
//! supported" error instead of "column not found".

use super::*;

/// Planning-context qualifier of the columns a decorrelated SELECT-list
/// subquery join appends. Not a user column, so `*` skips them.
pub(super) const SELECT_SUBQUERY_QUALIFIER: &str = "\u{0}subquery";

/// Name of the internal function that enforces a scalar subquery's
/// single-row rule on the rows that evaluate it.
const SINGLE_ROW_CHECK: &str = "__single_row_check";

/// A LEFT JOIN to add below the projection for one decorrelated subquery.
pub(super) struct SelectSubqueryJoin {
    /// Right side. Its first `outer_keys.len()` columns are the inner keys.
    plan: LogicalPlan,
    /// Outer key expressions, paired by position with the inner keys.
    outer_keys: Vec<ast::Expr>,
}

/// Select items with their correlated subqueries rewritten, plus the joins
/// those rewrites read from.
pub(super) type RewrittenSelectList = (Vec<ast::SelectItem>, Vec<SelectSubqueryJoin>);

impl QueryPlanner<'_> {
    /// Rewrites the correlated scalar subqueries in `items` (see the module
    /// docs). `None` when there is nothing to rewrite, so the caller plans
    /// the items exactly as before.
    pub(super) async fn rewrite_select_list_subqueries(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
        num_group_by: Option<usize>,
    ) -> Result<Option<RewrittenSelectList>, PlanError> {
        let mut found: Vec<(ast::Query, arneb_sql_parser::Span)> = Vec::new();
        for item in items {
            if let ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. } = item
            {
                for_each_scalar_subquery(expr, &mut |e| {
                    if let ast::Expr::Subquery { query, span } = e {
                        found.push(((**query).clone(), *span));
                    }
                });
            }
        }
        if found.is_empty() {
            return Ok(None);
        }

        let mut replacements: Vec<Option<ast::Expr>> = Vec::with_capacity(found.len());
        let mut joins = Vec::new();
        for (n, (query, span)) in found.iter().enumerate() {
            match self
                .decorrelate_select_subquery(query, *span, ctx, n)
                .await
                .map_err(|e| outer_reference_error(e, ctx))?
            {
                Some((replacement, join)) => {
                    replacements.push(Some(replacement));
                    joins.extend(join);
                }
                None => replacements.push(None),
            }
        }
        if replacements.iter().all(Option::is_none) {
            return Ok(None);
        }

        let mut rewritten = Vec::with_capacity(items.len());
        let mut next = replacements.into_iter();
        for item in items {
            let (mut expr, alias) = match item {
                ast::SelectItem::UnnamedExpr(expr) => (expr.clone(), None),
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    (expr.clone(), Some(alias.clone()))
                }
                other => {
                    rewritten.push(other.clone());
                    continue;
                }
            };
            // The item's name, as the uncorrelated path would give it.
            let mut named = expr.clone();
            let mut replaced = false;
            for_each_scalar_subquery_mut(&mut expr, &mut |e| {
                if let Some(Some(replacement)) = next.next() {
                    *e = replacement;
                    replaced = true;
                }
            });
            let alias = match alias {
                Some(alias) => Some(alias),
                None if replaced => {
                    Some(self.unnamed_item_name(&mut named, ctx, num_group_by).await)
                }
                None => None,
            };
            rewritten.push(match alias {
                Some(alias) => ast::SelectItem::ExprWithAlias { expr, alias },
                None => ast::SelectItem::UnnamedExpr(expr),
            });
        }
        let items = rewritten;
        Ok(Some((items, joins)))
    }

    /// The output name an unnamed item gets when its subqueries are planned
    /// on the uncorrelated path: each `(SELECT ...)` displays as
    /// `(scalar_subquery)`. Planned with every subquery swapped for
    /// `(SELECT NULL)`, so no helper column name leaks into it.
    async fn unnamed_item_name(
        &self,
        expr: &mut ast::Expr,
        ctx: &PlanningContext,
        num_group_by: Option<usize>,
    ) -> String {
        let span = expr.span();
        let null_query = ast::Query {
            ctes: Vec::new(),
            body: ast::QueryBody::Select(ast::SelectBody {
                distinct: false,
                projection: vec![ast::SelectItem::UnnamedExpr(ast::Expr::Literal {
                    value: ScalarValue::Null,
                    span,
                })],
                from: Vec::new(),
                selection: None,
                group_by: Vec::new(),
                having: None,
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        for_each_scalar_subquery_mut(expr, &mut |e| {
            *e = ast::Expr::Subquery {
                query: Box::new(null_query.clone()),
                span,
            };
        });
        if let Some(num_group_by) = num_group_by {
            if self.contains_aggregate(expr) {
                *expr = self.rewrite_aggregates_as_columns(expr, ctx, num_group_by);
            }
        }
        match self.plan_expr(expr, ctx).await {
            Ok(planned) => self.expr_to_column_info(&planned, ctx).name,
            Err(_) => "(scalar_subquery)".to_string(),
        }
    }

    /// LEFT JOINs each decorrelated subquery onto `plan` and appends its
    /// columns to `ctx` under [`SELECT_SUBQUERY_QUALIFIER`].
    pub(super) async fn attach_select_subquery_joins(
        &self,
        mut plan: LogicalPlan,
        ctx: &mut PlanningContext,
        joins: Vec<SelectSubqueryJoin>,
    ) -> Result<LogicalPlan, PlanError> {
        for join in joins {
            // Outer keys resolve against the outer columns only.
            let mut outer_keys = Vec::with_capacity(join.outer_keys.len());
            for key in &join.outer_keys {
                outer_keys.push(self.plan_expr(key, ctx).await?);
            }
            let offset = ctx.columns.len();
            let schema = join.plan.schema();
            for column in &schema {
                ctx.columns.push((
                    Some(SELECT_SUBQUERY_QUALIFIER.to_string()),
                    ColumnInfo {
                        nullable: true,
                        ..column.clone()
                    },
                ));
            }
            let condition = outer_keys
                .into_iter()
                .enumerate()
                .map(|(i, outer)| PlanExpr::BinaryOp {
                    left: Box::new(outer),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(PlanExpr::Column {
                        index: offset + i,
                        name: schema[i].name.clone(),
                        span: None,
                    }),
                    span: None,
                })
                .reduce(|left, right| PlanExpr::BinaryOp {
                    left: Box::new(left),
                    op: ast::BinaryOp::And,
                    right: Box::new(right),
                    span: None,
                })
                .expect("a decorrelated subquery has at least one key");
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(join.plan),
                join_type: ast::JoinType::Left,
                condition: JoinCondition::On(condition),
                dynamic_filter_ids: Vec::new(),
            };
        }
        Ok(plan)
    }

    /// Decorrelates one SELECT-list subquery. `None` when it is not
    /// correlated (or cannot be analysed here, e.g. its FROM names one of
    /// its own CTEs), so the existing path plans it. Otherwise the
    /// replacement expression, plus the join it reads from (none when the
    /// subquery has no FROM and is inlined).
    async fn decorrelate_select_subquery(
        &self,
        query: &ast::Query,
        span: arneb_sql_parser::Span,
        outer: &PlanningContext,
        n: usize,
    ) -> Result<Option<(ast::Expr, Option<SelectSubqueryJoin>)>, PlanError> {
        let ast::QueryBody::Select(body) = &query.body else {
            return Ok(None);
        };
        let Ok((inner_plan, inner)) = self.plan_from(&body.from).await else {
            return Ok(None);
        };

        let scope = |c: &ast::ColumnRef| ref_scope(c, outer, &inner);
        let mut correlated = false;
        for expr in query_clause_exprs(query) {
            for_each_column(expr, &mut |c| correlated |= scope(c) == RefScope::Outer);
        }
        let nested = nested_outer_reference(query, outer, &inner);
        if !correlated && !nested {
            return Ok(None);
        }

        let unsupported = |what: String| PlanError::UnsupportedExpression {
            message: format!("correlated subquery {what} is not supported"),
            location: Some(span.start),
        };
        if nested {
            return Err(unsupported(
                "with a correlation nested more than one level deep".into(),
            ));
        }
        let clauses = [
            (!query.ctes.is_empty(), "WITH"),
            (body.distinct, "DISTINCT"),
            (!body.group_by.is_empty(), "GROUP BY"),
            (body.having.is_some(), "HAVING"),
            (!query.order_by.is_empty(), "ORDER BY"),
            (
                query.limit.is_some() || query.offset.is_some(),
                "LIMIT/OFFSET",
            ),
        ];
        if let Some((_, clause)) = clauses.iter().find(|(present, _)| *present) {
            return Err(unsupported(format!("with {clause}")));
        }
        let value = match body.projection.as_slice() {
            [ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. }] => {
                expr.clone()
            }
            _ => {
                return Err(PlanError::invalid_expression(
                    "subquery must return only one column",
                ))
            }
        };
        if contains_query_or_window(&value) {
            return Err(unsupported(
                "with a subquery or window function in its select list".into(),
            ));
        }

        if body.from.is_empty() {
            // `(SELECT <outer expr>)`: the value itself.
            if body.selection.is_none() && !self.contains_aggregate(&value) {
                return Ok(Some((
                    ast::Expr::Nested {
                        expr: Box::new(value),
                        span,
                    },
                    None,
                )));
            }
            return Err(unsupported("without FROM".into()));
        }

        // Split the WHERE conjuncts.
        let mut keys: Vec<(ast::Expr, ast::Expr)> = Vec::new(); // (outer, inner)
        let mut inner_only: Vec<ast::Expr> = Vec::new();
        let mut guards: Vec<ast::Expr> = Vec::new();
        if let Some(selection) = &body.selection {
            for part in split_and(selection) {
                let (has_inner, has_outer) = side_scopes(&part, &scope);
                if !has_outer {
                    inner_only.push(part);
                } else if !has_inner {
                    guards.push(part);
                } else if let Some(pair) = equality_key(&part, &scope) {
                    keys.push(pair);
                } else if let Some((common, remainder)) =
                    factor_or_correlations(&part, outer, &inner)
                {
                    keys.extend(common);
                    inner_only.extend(remainder);
                } else {
                    return Err(unsupported(format!(
                        "correlated through anything but an equality (`{part}`)"
                    )));
                }
            }
        }
        if keys.is_empty() {
            return Err(unsupported("without an equality correlation".into()));
        }

        let mut filtered = inner_plan;
        if let Some(predicate) = combine_and(inner_only) {
            filtered = LogicalPlan::Filter {
                input: Box::new(filtered),
                predicate: self.plan_expr(&predicate, &inner).await?,
            };
        }
        let outer_keys: Vec<ast::Expr> = keys.iter().map(|(o, _)| o.clone()).collect();
        let column = |name: String| ast::Expr::Column {
            col_ref: ast::ColumnRef {
                name,
                table: Some(SELECT_SUBQUERY_QUALIFIER.to_string()),
                span,
            },
            span,
        };
        let with_guards = |cond: ast::Expr| {
            let mut parts = guards.clone();
            parts.push(cond);
            combine_and(parts).expect("non-empty")
        };

        if self.contains_aggregate(&value) {
            let mut outer_in_value = false;
            for_each_column(&value, &mut |c| {
                outer_in_value |= scope(c) == RefScope::Outer;
            });
            if outer_in_value {
                return Err(unsupported(
                    "with an outer column in an aggregate select list".into(),
                ));
            }
            let Some(empty_value) = aggregate_projection_on_empty(&value) else {
                return Err(unsupported(
                    "mixing aggregates and ungrouped columns".into(),
                ));
            };
            let Some(spec) = self
                .build_aggregate_leftjoin_spec(
                    filtered,
                    &inner,
                    keys,
                    &value,
                    empty_value.clone(),
                    &format!("sel{n}"),
                )
                .await?
            else {
                return Err(unsupported("of this shape".into()));
            };
            let schema = spec.plan.schema();
            let key = column(schema[0].name.clone());
            let result = column(schema.last().expect("value column").name.clone());
            // A guarded-out or unmatched row aggregates the empty set.
            let replacement = ast::Expr::Case {
                operand: None,
                conditions: vec![with_guards(ast::Expr::IsNotNull {
                    expr: Box::new(key),
                    span,
                })],
                results: vec![result],
                else_result: Some(Box::new(empty_value)),
                span,
            };
            let join = SelectSubqueryJoin {
                plan: spec.plan,
                outer_keys,
            };
            return Ok(Some((replacement, Some(join))));
        }

        // Non-aggregate: carry the inner columns the value reads.
        let mut carried: Vec<(usize, ast::Expr, ColumnInfo)> = Vec::new();
        let mut resolve_error = None;
        for_each_column(&value, &mut |c| {
            if scope(c) != RefScope::Inner {
                return;
            }
            match inner.resolve_column(&c.name, c.table.as_deref(), Some(c.span.start)) {
                Ok((index, info)) => {
                    if !carried.iter().any(|(i, _, _)| *i == index) {
                        carried.push((index, column_expr(c), info));
                    }
                }
                Err(PlanError::ColumnNotFound { .. }) => {}
                Err(e) => resolve_error = Some(e),
            }
        });
        if let Some(e) = resolve_error {
            return Err(e);
        }

        let mut group_by = Vec::with_capacity(keys.len());
        let mut agg_schema = Vec::new();
        for (_, inner_key) in &keys {
            let planned = self.plan_expr(inner_key, &inner).await?;
            agg_schema.push(self.expr_to_column_info(&planned, &inner));
            group_by.push(planned);
        }
        let mut aggr_exprs = Vec::with_capacity(carried.len() + 1);
        let mut casts_back: Vec<Option<DataType>> = Vec::with_capacity(carried.len());
        for (_, col, info) in &carried {
            let (function, arg, cast_back) = match &info.data_type {
                DataType::Boolean => ("bool_and", col.clone(), None),
                DataType::Int8 | DataType::Int16 => (
                    "min",
                    ast::Expr::Cast {
                        expr: Box::new(col.clone()),
                        data_type: DataType::Int32,
                        span,
                    },
                    Some(info.data_type.clone()),
                ),
                DataType::Null
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128 { .. }
                | DataType::Utf8
                | DataType::Char(_)
                | DataType::Date32
                | DataType::Timestamp { .. } => ("min", col.clone(), None),
                other => {
                    return Err(unsupported(format!("returning a value of type {other}")));
                }
            };
            let call = ast::Expr::Function {
                name: function.to_string(),
                args: vec![ast::FunctionArg::Unnamed(arg)],
                distinct: false,
                order_by: Vec::new(),
                span,
            };
            let planned = self.plan_expr(&call, &inner).await?;
            agg_schema.push(self.expr_to_column_info(&planned, &inner));
            aggr_exprs.push(planned);
            casts_back.push(cast_back);
        }
        let count = ast::Expr::Function {
            name: "count".to_string(),
            args: vec![ast::FunctionArg::Wildcard],
            distinct: false,
            order_by: Vec::new(),
            span,
        };
        let planned = self.plan_expr(&count, &inner).await?;
        agg_schema.push(self.expr_to_column_info(&planned, &inner));
        aggr_exprs.push(planned);

        // Rename every output to a synthetic name: the inner names often
        // repeat outer ones.
        let key_count = group_by.len();
        let names: Vec<String> = (0..key_count)
            .map(|i| format!("__sel{n}_key{i}"))
            .chain((0..carried.len()).map(|j| format!("__sel{n}_c{j}")))
            .chain(std::iter::once(format!("__sel{n}_rows")))
            .collect();
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(filtered),
            group_by,
            aggr_exprs,
            schema: agg_schema.clone(),
        };
        let plan = LogicalPlan::Projection {
            input: Box::new(aggregate),
            exprs: agg_schema
                .iter()
                .enumerate()
                .map(|(index, c)| PlanExpr::Column {
                    index,
                    name: c.name.clone(),
                    span: None,
                })
                .collect(),
            schema: agg_schema
                .iter()
                .zip(&names)
                .map(|(c, name)| ColumnInfo {
                    name: name.clone(),
                    ..c.clone()
                })
                .collect(),
        };

        let mut rewritten = value;
        replace_columns(&mut rewritten, &mut |c| {
            if scope(c) != RefScope::Inner {
                return None;
            }
            let (index, _) = inner
                .resolve_column(&c.name, c.table.as_deref(), None)
                .ok()?;
            let j = carried.iter().position(|(i, _, _)| *i == index)?;
            let carrier = column(names[key_count + j].clone());
            Some(match &casts_back[j] {
                Some(data_type) => ast::Expr::Cast {
                    expr: Box::new(carrier),
                    data_type: data_type.clone(),
                    span,
                },
                None => carrier,
            })
        });
        let rows = column(format!("__sel{n}_rows"));
        let replacement = ast::Expr::Case {
            operand: None,
            conditions: vec![with_guards(ast::Expr::IsNotNull {
                expr: Box::new(rows.clone()),
                span,
            })],
            results: vec![ast::Expr::Function {
                name: SINGLE_ROW_CHECK.to_string(),
                args: vec![
                    ast::FunctionArg::Unnamed(rows),
                    ast::FunctionArg::Unnamed(rewritten),
                ],
                distinct: false,
                order_by: Vec::new(),
                span,
            }],
            else_result: None,
            span,
        };
        Ok(Some((
            replacement,
            Some(SelectSubqueryJoin { plan, outer_keys }),
        )))
    }
}

/// Turns a "column not found" raised while planning a subquery into the
/// unsupported-correlation error when the column belongs to the enclosing
/// scope `outer`: the subquery is correlated in a shape that is not
/// decorrelated (its own CTE, a derived table, a correlation two levels
/// deep, ...). Other errors pass through.
#[inline(never)]
pub(super) fn outer_reference_error(err: PlanError, outer: &PlanningContext) -> PlanError {
    reference_error(err, outer, "correlated subquery")
}

/// [`outer_reference_error`] for a FROM item that reads an earlier FROM item
/// (`LATERAL`).
#[inline(never)]
pub(super) fn lateral_reference_error(err: PlanError, left: &PlanningContext) -> PlanError {
    reference_error(err, left, "correlated subquery (LATERAL)")
}

fn reference_error(err: PlanError, outer: &PlanningContext, what: &str) -> PlanError {
    let PlanError::ColumnNotFound { name, location, .. } = &err else {
        return err;
    };
    let (table, column) = match name.rsplit_once('.') {
        Some((table, column)) => (Some(table), column),
        None => (None, name.as_str()),
    };
    if matches!(
        outer.resolve_column(column, table, None),
        Err(PlanError::ColumnNotFound { .. })
    ) {
        return err;
    }
    PlanError::UnsupportedExpression {
        message: format!("{what} referencing outer column {name} here is not supported"),
        location: *location,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RefScope {
    /// Resolves in the subquery's FROM (innermost scope wins), or nowhere:
    /// an unresolvable name is left for the subquery's own planning to
    /// report.
    Inner,
    /// Resolves only in the enclosing query.
    Outer,
}

fn ref_scope(c: &ast::ColumnRef, outer: &PlanningContext, inner: &PlanningContext) -> RefScope {
    let resolves = |ctx: &PlanningContext| {
        !matches!(
            ctx.resolve_column(&c.name, c.table.as_deref(), None),
            Err(PlanError::ColumnNotFound { .. })
        )
    };
    if !resolves(inner) && resolves(outer) {
        RefScope::Outer
    } else {
        RefScope::Inner
    }
}

fn column_expr(c: &ast::ColumnRef) -> ast::Expr {
    ast::Expr::Column {
        col_ref: c.clone(),
        span: c.span,
    }
}

/// `(references an inner column, references an outer column)`.
fn side_scopes(expr: &ast::Expr, scope: &impl Fn(&ast::ColumnRef) -> RefScope) -> (bool, bool) {
    let (mut inner, mut outer) = (false, false);
    for_each_column(expr, &mut |c| match scope(c) {
        RefScope::Inner => inner = true,
        RefScope::Outer => outer = true,
    });
    (inner, outer)
}

/// `inner_expr = outer_expr` (either order) → `(outer_expr, inner_expr)`.
fn equality_key(
    part: &ast::Expr,
    scope: &impl Fn(&ast::ColumnRef) -> RefScope,
) -> Option<(ast::Expr, ast::Expr)> {
    let ast::Expr::BinaryOp {
        left,
        op: ast::BinaryOp::Eq,
        right,
        ..
    } = part
    else {
        return None;
    };
    match (side_scopes(left, scope), side_scopes(right, scope)) {
        ((true, false), (false, true)) => Some(((**right).clone(), (**left).clone())),
        ((false, true), (true, false)) => Some(((**left).clone(), (**right).clone())),
        _ => None,
    }
}

/// The expressions of a query's own clauses (not those of nested queries).
fn query_clause_exprs(query: &ast::Query) -> Vec<&ast::Expr> {
    let mut out: Vec<&ast::Expr> = Vec::new();
    if let ast::QueryBody::Select(body) = &query.body {
        for item in &body.projection {
            if let ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. } = item
            {
                out.push(expr);
            }
        }
        out.extend(body.selection.as_deref());
        out.extend(&body.group_by);
        out.extend(body.having.as_deref());
    }
    out.extend(query.order_by.iter().map(|o| &o.expr));
    out.extend(query.limit.as_deref());
    out.extend(query.offset.as_deref());
    out
}

/// Whether a query nested inside `query`'s clauses references a column of
/// the enclosing query by its table qualifier (a correlation two levels
/// deep). Only qualified references are recognised: an unqualified one
/// cannot be attributed without planning the nested query.
fn nested_outer_reference(
    query: &ast::Query,
    outer: &PlanningContext,
    inner: &PlanningContext,
) -> bool {
    let mut nested: Vec<&ast::Query> = Vec::new();
    for expr in query_clause_exprs(query) {
        collect_nested_queries(expr, &mut nested);
    }
    let mut stack = nested;
    let mut declared: Vec<String> = Vec::new();
    let mut refs: Vec<ast::ColumnRef> = Vec::new();
    while let Some(q) = stack.pop() {
        if let ast::QueryBody::Select(body) = &q.body {
            for twj in &body.from {
                declared_names(&twj.relation, &mut declared);
                for join in &twj.joins {
                    declared_names(&join.relation, &mut declared);
                }
            }
        }
        for expr in query_clause_exprs(q) {
            for_each_column(expr, &mut |c| refs.push(c.clone()));
            collect_nested_queries(expr, &mut stack);
        }
    }
    let has_qualifier = |ctx: &PlanningContext, t: &str| {
        ctx.columns
            .iter()
            .any(|(q, _)| q.as_deref().is_some_and(|q| q.eq_ignore_ascii_case(t)))
    };
    refs.iter().any(|c| {
        let Some(t) = c.table.as_deref() else {
            return false;
        };
        !declared.iter().any(|d| d.eq_ignore_ascii_case(t))
            && !has_qualifier(inner, t)
            && outer.resolve_column(&c.name, Some(t), None).is_ok()
    })
}

fn declared_names(factor: &ast::TableFactor, out: &mut Vec<String>) {
    match factor {
        ast::TableFactor::Table { name, alias, .. } => {
            out.push(alias.clone().unwrap_or_else(|| name.table.clone()));
        }
        ast::TableFactor::Subquery { alias, .. } => out.push(alias.clone()),
        ast::TableFactor::NestedJoin {
            relation, alias, ..
        } => {
            out.extend(alias.clone());
            declared_names(&relation.relation, out);
            for join in &relation.joins {
                declared_names(&join.relation, out);
            }
        }
    }
}

/// The direct children of an expression, not descending into the bodies of
/// nested queries.
fn children(expr: &ast::Expr) -> Vec<&ast::Expr> {
    use ast::Expr as E;
    match expr {
        E::Column { .. }
        | E::Literal { .. }
        | E::Parameter { .. }
        | E::Interval { .. }
        | E::Subquery { .. }
        | E::Exists { .. } => Vec::new(),
        E::BinaryOp { left, right, .. } => vec![left, right],
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. }
        | E::Nested { expr, .. }
        | E::InSubquery { expr, .. } => vec![expr],
        E::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        E::InList { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
        E::Function { args, order_by, .. } => args
            .iter()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(e) => Some(e),
                ast::FunctionArg::Wildcard => None,
            })
            .chain(order_by.iter().map(|o| &o.expr))
            .collect(),
        E::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => args
            .iter()
            .chain(partition_by)
            .chain(order_by.iter().map(|o| &o.expr))
            .collect(),
        E::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => operand
            .as_deref()
            .into_iter()
            .chain(conditions)
            .chain(results)
            .chain(else_result.as_deref())
            .collect(),
        E::Array { elements, .. } => elements.iter().collect(),
        E::AnyAll { expr, array, .. } => vec![expr, array],
    }
}

/// [`children`], mutably.
fn children_mut(expr: &mut ast::Expr) -> Vec<&mut ast::Expr> {
    use ast::Expr as E;
    match expr {
        E::Column { .. }
        | E::Literal { .. }
        | E::Parameter { .. }
        | E::Interval { .. }
        | E::Subquery { .. }
        | E::Exists { .. } => Vec::new(),
        E::BinaryOp { left, right, .. } => vec![left, right],
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. }
        | E::Nested { expr, .. }
        | E::InSubquery { expr, .. } => vec![expr],
        E::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        E::InList { expr, list, .. } => std::iter::once(&mut **expr).chain(list).collect(),
        E::Function { args, order_by, .. } => args
            .iter_mut()
            .filter_map(|a| match a {
                ast::FunctionArg::Unnamed(e) => Some(e),
                ast::FunctionArg::Wildcard => None,
            })
            .chain(order_by.iter_mut().map(|o| &mut o.expr))
            .collect(),
        E::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => args
            .iter_mut()
            .chain(partition_by)
            .chain(order_by.iter_mut().map(|o| &mut o.expr))
            .collect(),
        E::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => operand
            .as_deref_mut()
            .into_iter()
            .chain(conditions)
            .chain(results)
            .chain(else_result.as_deref_mut())
            .collect(),
        E::Array { elements, .. } => elements.iter_mut().collect(),
        E::AnyAll { expr, array, .. } => vec![expr, array],
    }
}

#[inline(never)]
fn for_each_column(expr: &ast::Expr, f: &mut dyn FnMut(&ast::ColumnRef)) {
    if let ast::Expr::Column { col_ref, .. } = expr {
        f(col_ref);
    }
    for child in children(expr) {
        for_each_column(child, f);
    }
}

#[inline(never)]
fn replace_columns(expr: &mut ast::Expr, f: &mut dyn FnMut(&ast::ColumnRef) -> Option<ast::Expr>) {
    if let ast::Expr::Column { col_ref, .. } = expr {
        if let Some(replacement) = f(col_ref) {
            *expr = replacement;
        }
        return;
    }
    for child in children_mut(expr) {
        replace_columns(child, f);
    }
}

/// Visits every scalar `(SELECT ...)` node, in a fixed order.
#[inline(never)]
fn for_each_scalar_subquery(expr: &ast::Expr, f: &mut dyn FnMut(&ast::Expr)) {
    if matches!(expr, ast::Expr::Subquery { .. }) {
        f(expr);
        return;
    }
    for child in children(expr) {
        for_each_scalar_subquery(child, f);
    }
}

/// [`for_each_scalar_subquery`], mutably and in the same order.
#[inline(never)]
fn for_each_scalar_subquery_mut(expr: &mut ast::Expr, f: &mut dyn FnMut(&mut ast::Expr)) {
    if matches!(expr, ast::Expr::Subquery { .. }) {
        f(expr);
        return;
    }
    for child in children_mut(expr) {
        for_each_scalar_subquery_mut(child, f);
    }
}

/// Pushes the queries of the `Subquery` / `InSubquery` / `Exists` nodes in
/// `expr` (not descending into them).
#[inline(never)]
fn collect_nested_queries<'q>(expr: &'q ast::Expr, out: &mut Vec<&'q ast::Query>) {
    match expr {
        ast::Expr::Subquery { query, .. } => out.push(query),
        ast::Expr::Exists { subquery, .. } => out.push(subquery),
        ast::Expr::InSubquery { subquery, .. } => out.push(subquery),
        _ => {}
    }
    for child in children(expr) {
        collect_nested_queries(child, out);
    }
}

fn contains_query_or_window(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Subquery { .. }
            | ast::Expr::InSubquery { .. }
            | ast::Expr::Exists { .. }
            | ast::Expr::WindowFunction { .. }
    ) || children(expr).into_iter().any(contains_query_or_window)
}
