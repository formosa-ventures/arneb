//! Query planner that converts a parsed SQL AST into a logical plan.

use async_recursion::async_recursion;
use std::sync::OnceLock;

use arneb_catalog::CatalogManager;
use arneb_common::error::{CatalogError, PlanError};
use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_sql_parser::ast;

use crate::analyzer::{plan_expr_type, Analyzer, AnalyzerContext};
use crate::plan::{
    null_extend, JoinCondition, LogicalPlan, PlanExpr, SortExpr, SrfKind, WindowFunctionDef,
};

mod select_subquery;
use select_subquery::SELECT_SUBQUERY_QUALIFIER;

static CTE_SELF_AGG_WINDOW_TEST_OVERRIDE: OnceLock<std::sync::Mutex<Option<bool>>> =
    OnceLock::new();

fn cte_self_agg_window_enabled() -> bool {
    if let Some(value) = *CTE_SELF_AGG_WINDOW_TEST_OVERRIDE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("CTE self-agg window test override lock poisoned")
    {
        return value;
    }

    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let enabled = std::env::var("ARNEB_CTE_SELF_AGG_WINDOW")
            .map(|v| v != "0" && !v.is_empty())
            .unwrap_or(true);
        tracing::info!(
            ARNEB_CTE_SELF_AGG_WINDOW = enabled,
            "ARNEB_CTE_SELF_AGG_WINDOW effective value (default on; =0 to disable)"
        );
        enabled
    })
}

/// Converts parsed SQL statements into logical query plans.
pub struct QueryPlanner<'a> {
    catalog: &'a CatalogManager,
    /// CTE registry, populated as each `WITH name AS (...)` is encountered
    /// in [`Self::plan_query`] and consulted by [`Self::plan_table_factor`]
    /// when resolving an unqualified table name. Single-part names beat
    /// the catalog so the CTE shadows any same-named base table inside its
    /// scope. Scoping is enforced via snapshot-restore around each
    /// `plan_query` call.
    cte_plans: std::sync::Mutex<std::collections::HashMap<String, LogicalPlan>>,
    /// `$N` placeholders found in LIMIT/OFFSET. They leave no node in the
    /// plan, so the analyzer never sees them; they are seeded into the
    /// analyzer context as int8, which is what PostgreSQL reports.
    limit_params: std::sync::Mutex<std::collections::HashMap<usize, DataType>>,
}

/// Tracks the available columns from resolved tables during planning.
/// Each entry is (optional_qualifier, column_info, global_index).
struct PlanningContext {
    /// All columns available in the current scope.
    /// (qualifier, column_info)
    columns: Vec<(Option<String>, ColumnInfo)>,
    /// Right-hand copies of `JOIN ... USING` columns: reachable only with
    /// their table qualifier, and left out of `*`, so the joined column
    /// appears once (the left copy), as in PostgreSQL and Trino.
    merged: std::collections::HashSet<usize>,
    /// The `GROUP BY` keys of the aggregate whose output this context
    /// describes (empty otherwise), for `GROUPING(...)` under a plain
    /// `GROUP BY`.
    group_by: Vec<ast::Expr>,
}

impl PlanningContext {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
            merged: std::collections::HashSet::new(),
            group_by: Vec::new(),
        }
    }

    /// Add columns from a table scan into the context.
    fn add_table_columns(&mut self, qualifier: Option<&str>, schema: &[ColumnInfo]) {
        for col in schema {
            self.columns
                .push((qualifier.map(|s| s.to_string()), col.clone()));
        }
    }

    /// Resolve a column reference to a (global_index, ColumnInfo) pair.
    ///
    /// The optional `location` lets error variants carry the source
    /// position of the column reference. Call sites that happen to have
    /// an AST node handy (and therefore a `Span`) should thread its
    /// `start` location through here so diagnostics can point at the
    /// typo. Call sites with no position context pass `None`.
    fn resolve_column(
        &self,
        name: &str,
        table: Option<&str>,
        location: Option<arneb_common::error::Location>,
    ) -> Result<(usize, ColumnInfo), PlanError> {
        let mut found = None;
        for (i, (qualifier, col)) in self.columns.iter().enumerate() {
            if table.is_none() && self.merged.contains(&i) {
                continue;
            }
            let name_matches = col.name.eq_ignore_ascii_case(name);
            let qualifier_matches = match (table, qualifier) {
                (Some(t), Some(q)) => t.eq_ignore_ascii_case(q),
                (Some(_), None) => false,
                (None, _) => true,
            };
            if name_matches && qualifier_matches {
                if found.is_some() {
                    return Err(PlanError::AmbiguousReference {
                        name: name.to_string(),
                        location,
                    });
                }
                found = Some((i, col.clone()));
            }
        }
        found.ok_or_else(|| PlanError::ColumnNotFound {
            name: table.map_or_else(|| name.to_string(), |table| format!("{table}.{name}")),
            location,
            candidates: arneb_common::suggest::nearest(
                name,
                self.columns.iter().map(|(_, c)| c.name.as_str()),
                3,
            ),
        })
    }

    /// Return all columns as ColumnInfo.
    #[allow(dead_code)]
    fn all_columns(&self) -> Vec<ColumnInfo> {
        self.columns.iter().map(|(_, c)| c.clone()).collect()
    }

    /// Return columns matching a qualifier.
    fn columns_for_qualifier(&self, qualifier: &str) -> Vec<(usize, ColumnInfo)> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, (q, _))| {
                q.as_ref()
                    .map(|q| q.eq_ignore_ascii_case(qualifier))
                    .unwrap_or(false)
            })
            .map(|(i, (_, c))| (i, c.clone()))
            .collect()
    }
}

/// Gives both inputs of a set operation the same column types now, at
/// planning time, so whatever is planned on top of it (an outer query over
/// a `UNION ALL` in a derived table, say) sees the unified type rather than
/// the left input's. The analyzer aligns them again after coercion; that
/// pass is then a no-op. Inputs with an untyped (NULL or parameter) column
/// are left for the analyzer, which infers those types first.
fn align_set_op_inputs(
    left: LogicalPlan,
    right: LogicalPlan,
) -> Result<(LogicalPlan, LogicalPlan), PlanError> {
    let untyped = |plan: &LogicalPlan| plan.schema().iter().any(|c| c.data_type == DataType::Null);
    if untyped(&left) || untyped(&right) || left.schema().len() != right.schema().len() {
        return Ok((left, right));
    }
    let mut aligned =
        crate::analyzer::type_coercion::align_set_op_branches(vec![left, right])?.into_iter();
    let left = aligned.next().expect("two inputs");
    let right = aligned.next().expect("two inputs");
    Ok((left, right))
}

fn apply_table_column_aliases(
    plan: LogicalPlan,
    column_aliases: &[String],
    qualifier: &str,
) -> Result<LogicalPlan, PlanError> {
    if column_aliases.is_empty() {
        return Ok(plan);
    }
    let schema = plan.schema();
    if column_aliases.len() != schema.len() {
        return Err(PlanError::invalid_expression(format!(
            "Column alias list has {} entries but '{qualifier}' has {} columns available",
            column_aliases.len(),
            schema.len()
        )));
    }
    let exprs = schema
        .iter()
        .enumerate()
        .map(|(index, column)| PlanExpr::Column {
            index,
            name: column.name.clone(),
            span: None,
        })
        .collect();
    let schema = schema
        .iter()
        .zip(column_aliases)
        .map(|(column, alias)| ColumnInfo {
            name: alias.clone(),
            data_type: column.data_type.clone(),
            nullable: column.nullable,
        })
        .collect();
    Ok(LogicalPlan::Projection {
        input: Box::new(plan),
        exprs,
        schema,
    })
}

impl<'a> QueryPlanner<'a> {
    pub fn new(catalog: &'a CatalogManager) -> Self {
        Self {
            catalog,
            cte_plans: std::sync::Mutex::new(std::collections::HashMap::new()),
            limit_params: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Plan a top-level SQL statement and return just the plan.
    ///
    /// This is the convenience entrypoint for code paths that don't
    /// need the [`AnalyzerContext`] — query execution, fragmenters,
    /// and tests. See [`Self::plan_statement_with_context`] when you
    /// need the inferred parameter types (e.g., pgwire
    /// `ParameterDescription`).
    pub async fn plan_statement(&self, stmt: &ast::Statement) -> Result<LogicalPlan, PlanError> {
        let (plan, _ctx) = self.plan_statement_with_context(stmt).await?;
        Ok(plan)
    }

    /// Plan a top-level SQL statement and return the plan plus the
    /// [`AnalyzerContext`] produced while running the default analyzer
    /// pipeline.
    ///
    /// The pipeline runs exactly once per top-level statement — not
    /// once per recursive subquery — so the returned context reflects
    /// the entire query.
    pub async fn plan_statement_with_context(
        &self,
        stmt: &ast::Statement,
    ) -> Result<(LogicalPlan, AnalyzerContext), PlanError> {
        let plan = self.plan_statement_inner(stmt).await?;
        let mut ctx = AnalyzerContext::new();
        if let Ok(limit_params) = self.limit_params.lock() {
            ctx.param_types.extend(limit_params.clone());
        }
        // Populate per-query statistics for cost-based passes. Walks
        // every `TableScan` in the plan, asks each `TableProvider` for
        // its statistics, and seeds `AnalyzerContext::catalog_stats`.
        // Missing stats fall back to defaults inside the cost model;
        // resolve failures are logged but do not block planning.
        ctx.catalog_stats = std::sync::Arc::new(self.collect_catalog_statistics(&plan).await);
        let plan = Analyzer::default_pipeline().run(plan, &mut ctx)?;
        Ok((plan, ctx))
    }

    /// Walks `plan` collecting every `TableScan`'s qualified reference,
    /// resolves the underlying `TableProvider` through `CatalogManager`,
    /// calls `statistics()`, and assembles a [`crate::CatalogStats`]
    /// snapshot for the analyzer. Errors during catalog resolution are
    /// logged at `tracing::warn` and the table is skipped — planning
    /// degrades gracefully to row-count defaults.
    async fn collect_catalog_statistics(&self, plan: &LogicalPlan) -> crate::CatalogStats {
        let mut refs: Vec<arneb_common::types::TableReference> = Vec::new();
        collect_table_scan_refs(plan, &mut refs);
        let mut stats = crate::CatalogStats::new();
        for reference in refs {
            match self.catalog.resolve_table(&reference).await {
                Ok(provider) => {
                    if let Some(t_stats) = provider.statistics() {
                        stats.insert(reference, t_stats);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "collect_catalog_statistics: resolve_table({}) failed: {}",
                        reference,
                        e
                    );
                }
            }
        }
        stats
    }

    /// Raw AST → LogicalPlan translation. Runs the analyzer — callers
    /// use [`Self::plan_statement`] or [`Self::plan_statement_with_context`].
    #[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
    #[async_recursion]
    async fn plan_statement_inner(&self, stmt: &ast::Statement) -> Result<LogicalPlan, PlanError> {
        match stmt {
            ast::Statement::Query { query, .. } => self.plan_query(query).await,
            ast::Statement::Explain {
                stmt: inner,
                analyze,
                ..
            } => {
                let plan = self.plan_statement_inner(inner).await?;
                Ok(LogicalPlan::Explain {
                    input: Box::new(plan),
                    analyze: *analyze,
                })
            }
            ast::Statement::CreateTable { name, columns, .. } => {
                let schema: Vec<ColumnInfo> = columns
                    .iter()
                    .map(|c| ColumnInfo {
                        name: c.name.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect();
                Ok(LogicalPlan::CreateTable {
                    name: name.clone(),
                    schema,
                })
            }
            ast::Statement::DropTable {
                name, if_exists, ..
            } => Ok(LogicalPlan::DropTable {
                name: name.clone(),
                if_exists: *if_exists,
            }),
            ast::Statement::CreateTableAsSelect { name, query, .. } => {
                let source = self.plan_query(query).await?;
                Ok(LogicalPlan::CreateTableAsSelect {
                    name: name.clone(),
                    source: Box::new(source),
                })
            }
            ast::Statement::InsertInto {
                table,
                columns: _,
                source,
                ..
            } => {
                let source_plan = match source {
                    ast::InsertSource::Query(q) => self.plan_query(q).await?,
                    ast::InsertSource::Values(_rows) => {
                        return Err(PlanError::invalid_expression(
                            "INSERT INTO ... VALUES not yet supported in planner; use INSERT INTO ... SELECT"
                                .to_string(),
                        ));
                    }
                };
                Ok(LogicalPlan::InsertInto {
                    table: table.clone(),
                    source: Box::new(source_plan),
                })
            }
            ast::Statement::DeleteFrom {
                table, predicate, ..
            } => {
                let pred_str = predicate.as_ref().map(|p| format!("{p}"));
                Ok(LogicalPlan::DeleteFrom {
                    table: table.clone(),
                    predicate: pred_str,
                })
            }
            ast::Statement::CreateView { name, query, .. } => {
                let plan = self.plan_query(query).await?;
                Ok(LogicalPlan::CreateView {
                    name: name.clone(),
                    sql: format!(
                        "{}",
                        ast::Statement::Query {
                            query: query.clone(),
                            span: stmt.span(),
                        }
                    ),
                    plan: Box::new(plan),
                })
            }
            ast::Statement::DropView {
                name, if_exists, ..
            } => Ok(LogicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            }),
        }
    }

    /// Plan a Query (CTEs + body + ORDER BY + LIMIT/OFFSET).
    #[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
    #[async_recursion]
    async fn plan_query(&self, query: &ast::Query) -> Result<LogicalPlan, PlanError> {
        // Snapshot the CTE registry on entry and restore it on exit so
        // a CTE defined in a nested WITH doesn't leak into the outer
        // scope and so two sibling queries that reuse the same CTE
        // name don't collide.
        let saved_ctes = self.cte_plans.lock().unwrap().clone();

        for cte in &query.ctes {
            let cte_plan = self.plan_query(&cte.query).await?;
            // Apply column aliases by wrapping the CTE plan in a
            // projection that renames the output columns to match the
            // declared names. We keep the original PlanExpr::Column
            // references untouched — only the ColumnInfo names are
            // rewritten so downstream `qualifier.col` resolution lands
            // on the alias the user typed in the WITH list.
            let cte_plan = if cte.column_aliases.is_empty() {
                cte_plan
            } else {
                let schema = cte_plan.schema();
                if cte.column_aliases.len() != schema.len() {
                    return Err(PlanError::invalid_expression(format!(
                        "CTE `{}` declares {} column aliases but produces {} columns",
                        cte.name,
                        cte.column_aliases.len(),
                        schema.len()
                    )));
                }
                let exprs: Vec<PlanExpr> = schema
                    .iter()
                    .enumerate()
                    .map(|(i, c)| PlanExpr::Column {
                        index: i,
                        name: c.name.clone(),
                        span: None,
                    })
                    .collect();
                let renamed_schema: Vec<ColumnInfo> = schema
                    .iter()
                    .zip(cte.column_aliases.iter())
                    .map(|(c, alias)| ColumnInfo {
                        name: alias.clone(),
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect();
                LogicalPlan::Projection {
                    input: Box::new(cte_plan),
                    exprs,
                    schema: renamed_schema,
                }
            };
            self.cte_plans
                .lock()
                .unwrap()
                .insert(cte.name.clone(), cte_plan);
        }

        let mut plan = self.plan_query_body(&query.body, &query.order_by).await?;

        // ORDER BY
        if !query.order_by.is_empty() {
            let ctx = self.context_from_plan(&plan);

            // Build alias→index mapping from SELECT list for ORDER BY resolution
            let select_items = match &query.body {
                ast::QueryBody::Select(body) => Some(&body.projection),
                _ => None,
            };

            let mut sort_exprs = Vec::with_capacity(query.order_by.len());
            let mut unresolved = None;
            for ob in &query.order_by {
                let ordinal = select_ordinal(&ob.expr, ctx.columns.len(), "ORDER BY")?;
                let resolved = match ordinal {
                    Some(index) => Some(PlanExpr::Column {
                        index,
                        name: ctx.columns[index].1.name.clone(),
                        span: None,
                    }),
                    None => self.resolve_order_by_expr_with_select(&ob.expr, &ctx, select_items),
                };
                let expr = match resolved {
                    Some(resolved) => resolved,
                    None => match self.plan_expr(&ob.expr, &ctx).await {
                        Ok(expr) => expr,
                        Err(e) => {
                            unresolved = Some(e);
                            break;
                        }
                    },
                };
                sort_exprs.push(SortExpr {
                    expr,
                    asc: ob.asc.unwrap_or(true),
                    nulls_first: ob.nulls_first.unwrap_or(false),
                });
            }
            plan = match unresolved {
                None => LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: sort_exprs,
                },
                // ORDER BY a column that is not in the SELECT list (e.g.
                // `SELECT name FROM t ORDER BY position`): sort beneath the
                // projection, where every input column is still visible.
                Some(err) => match plan {
                    LogicalPlan::Projection {
                        input,
                        exprs,
                        schema,
                    } => {
                        let order_by = self
                            .order_by_below_projection(query, &ctx, select_items, &input, &exprs)
                            .await
                            .map_err(|_| err)?;
                        LogicalPlan::Projection {
                            input: Box::new(LogicalPlan::Sort { input, order_by }),
                            exprs,
                            schema,
                        }
                    }
                    // SELECT DISTINCT wraps the projection in an Aggregate,
                    // so a key outside the select list can't be sorted on.
                    _ if matches!(&query.body, ast::QueryBody::Select(b) if b.distinct) => {
                        return Err(PlanError::invalid_expression(
                            "For SELECT DISTINCT, ORDER BY expressions must appear in select list",
                        ))
                    }
                    _ => return Err(err),
                },
            };
        }

        // LIMIT / OFFSET
        let limit = self.eval_limit_expr(query.limit.as_deref()).await?;
        let offset = self.eval_limit_expr(query.offset.as_deref()).await?;
        if limit.is_some() || offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit,
                offset,
            };
        }

        // Restore the outer CTE scope.
        *self.cte_plans.lock().unwrap() = saved_ctes;

        Ok(plan)
    }

    /// Resolves ORDER BY keys against the input of the final projection.
    /// Keys that name a SELECT output column are rewritten to that column's
    /// projection expression, so aliases keep working below the projection.
    async fn order_by_below_projection(
        &self,
        query: &ast::Query,
        output_ctx: &PlanningContext,
        select_items: Option<&Vec<ast::SelectItem>>,
        input: &LogicalPlan,
        exprs: &[PlanExpr],
    ) -> Result<Vec<SortExpr>, PlanError> {
        let mut input_ctx = self.context_from_plan(input);
        if let ast::QueryBody::Select(body) = &query.body {
            let group_by = resolve_group_by_ordinals(body)?;
            let group_by = rollup_group_by_keys(&group_by)?.unwrap_or(group_by);
            if !group_by.is_empty() {
                for (col, key) in input_ctx.columns.iter_mut().zip(&group_by) {
                    col.0 = group_by_qualifier(key);
                }
            } else if !matches!(input, LogicalPlan::Aggregate { .. }) {
                let (_, from_ctx) = self.plan_from(&body.from).await?;
                // Columns appended above the FROM (decorrelated SELECT-list
                // subqueries) leave the FROM columns as a prefix.
                if input_ctx.columns.len() >= from_ctx.columns.len() {
                    for (col, source) in input_ctx.columns.iter_mut().zip(from_ctx.columns) {
                        col.0 = source.0;
                    }
                }
            }
        }
        // Restore the qualifiers of selected columns. A wildcard expands to
        // several expressions, so items and exprs only line up without one.
        if let Some(items) = select_items.filter(|items| {
            items.len() == exprs.len()
                && !items.iter().any(|item| {
                    matches!(
                        item,
                        ast::SelectItem::Wildcard | ast::SelectItem::QualifiedWildcard(_)
                    )
                })
        }) {
            for (item, expr) in items.iter().zip(exprs) {
                if let (
                    ast::SelectItem::UnnamedExpr(ast::Expr::Column { col_ref, .. }),
                    PlanExpr::Column { index, .. },
                ) = (item, expr)
                {
                    if let Some(col) = input_ctx.columns.get_mut(*index) {
                        col.0 = col_ref.table.clone();
                    }
                }
            }
        }
        let mut order_by = Vec::with_capacity(query.order_by.len());
        for ob in &query.order_by {
            let resolved = match select_ordinal(&ob.expr, exprs.len(), "ORDER BY")? {
                Some(index) => Some(index),
                None => {
                    match self.resolve_order_by_expr_with_select(&ob.expr, output_ctx, select_items)
                    {
                        Some(PlanExpr::Column { index, .. }) if index < exprs.len() => Some(index),
                        _ => None,
                    }
                }
            };
            let expr = match resolved {
                Some(index) => exprs[index].clone(),
                None => {
                    let rewritten = match &query.body {
                        ast::QueryBody::Select(body) => self.rewrite_aggregates_as_columns(
                            &ob.expr,
                            &input_ctx,
                            rollup_group_by_keys(&resolve_group_by_ordinals(body)?)?
                                .map_or_else(|| body.group_by.len(), |keys| keys.len()),
                        ),
                        _ => ob.expr.clone(),
                    };
                    self.plan_expr(&rewritten, &input_ctx).await?
                }
            };
            order_by.push(SortExpr {
                expr,
                asc: ob.asc.unwrap_or(true),
                nulls_first: ob.nulls_first.unwrap_or(false),
            });
        }
        Ok(order_by)
    }

    /// Plan a QueryBody (SELECT or set operation).
    #[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
    #[async_recursion]
    async fn plan_query_body(
        &self,
        body: &ast::QueryBody,
        order_by: &[ast::OrderByExpr],
    ) -> Result<LogicalPlan, PlanError> {
        match body {
            ast::QueryBody::Select(select) => self.plan_select(select, order_by).await,
            ast::QueryBody::Query(query) => self.plan_query(query).await,
            ast::QueryBody::SetOperation { op, left, right } => {
                let left_plan = self.plan_query_body(left, &[]).await?;
                let right_plan = self.plan_query_body(right, &[]).await?;
                let (left_plan, right_plan) = align_set_op_inputs(left_plan, right_plan)?;
                match op {
                    ast::SetOperator::UnionAll => Ok(LogicalPlan::UnionAll {
                        inputs: vec![left_plan, right_plan],
                    }),
                    ast::SetOperator::Union => Ok(LogicalPlan::Distinct {
                        input: Box::new(LogicalPlan::UnionAll {
                            inputs: vec![left_plan, right_plan],
                        }),
                    }),
                    ast::SetOperator::Intersect => Ok(LogicalPlan::Intersect {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                    }),
                    ast::SetOperator::Except => Ok(LogicalPlan::Except {
                        left: Box::new(left_plan),
                        right: Box::new(right_plan),
                    }),
                }
            }
        }
    }

    /// Plan a SelectBody: FROM → WHERE → GROUP BY/HAVING → SELECT projection.
    async fn plan_select(
        &self,
        body: &ast::SelectBody,
        order_by: &[ast::OrderByExpr],
    ) -> Result<LogicalPlan, PlanError> {
        // 1. FROM clause → base plan + context
        let (mut plan, mut ctx) = self.plan_from(&body.from).await?;

        // 2. WHERE
        if let Some(selection) = &body.selection {
            // Pre-pass: extract `EXISTS (...)` / `NOT EXISTS (...)`
            // sub-predicates from the WHERE AND-tree and rewrite each
            // one as a SemiJoin / AntiJoin against the outer plan.
            // Nested subqueries in the remainder are planned separately.
            //
            // Correlation handling: each EXISTS subquery's inner
            // WHERE is split into AND parts; the one shape we
            // currently decorrelate is `outer_col = inner_col` (or
            // mirror), which translates 1:1 to SemiJoin's single
            // `(left_key, right_key)` pair. Other parts of the inner
            // WHERE become a Filter applied to the inner plan before
            // the SemiJoin. Anything more complex (multi-correlation,
            // non-equi correlation, correlation in JOIN ON, …) bails
            // out as `UnsupportedFeature`.
            let (residual, semi_specs) = self.extract_exists_subqueries(selection, &ctx).await?;

            for spec in semi_specs {
                plan = match spec.kind {
                    SemiJoinKind::Semi => LogicalPlan::SemiJoin {
                        left: Box::new(plan),
                        right: Box::new(spec.right_plan),
                        left_key: spec.left_key,
                        right_key: spec.right_key,
                        residual: spec.residual,
                        dynamic_filter_ids: Vec::new(),
                        probe_side_filter_ids: Vec::new(),
                    },
                    SemiJoinKind::Anti => LogicalPlan::AntiJoin {
                        left: Box::new(plan),
                        right: Box::new(spec.right_plan),
                        left_key: spec.left_key,
                        right_key: spec.right_key,
                        residual: spec.residual,
                        probe_side_filter_ids: Vec::new(),
                    },
                };
            }

            // Second pre-pass: extract correlated scalar subqueries
            // (e.g. TPC-H Q17's `l_quantity < (SELECT 0.2 * AVG(...)
            // FROM lineitem WHERE l_partkey = p_partkey)`). Each one
            // becomes a LEFT JOIN onto a decorrelated aggregate plus a
            // residual rewrite that swaps the subquery for a column
            // reference to the joined value.
            let residual = if let Some(rem) = residual {
                let (rem, leftjoin_specs) =
                    self.extract_correlated_scalar_subqueries(rem, &ctx).await?;
                for spec in leftjoin_specs {
                    // Plan each outer correlation against the current
                    // ctx BEFORE appending spec's columns (so outer
                    // resolves only against outer-scope columns).
                    let mut outer_keys: Vec<PlanExpr> = Vec::with_capacity(spec.correlations.len());
                    for (outer_ast, _) in &spec.correlations {
                        outer_keys.push(self.plan_expr(outer_ast, &ctx).await?);
                    }
                    let inner_offset = ctx.columns.len();
                    let spec_schema = spec.plan.schema();
                    for c in &spec_schema {
                        ctx.columns.push((None, c.clone()));
                    }
                    // Build the conjunctive equi-join condition: every
                    // (outer_key_i = inner_key_i) AND-ed together.
                    let mut cond: Option<PlanExpr> = None;
                    for (i, (_, inner_idx)) in spec.correlations.iter().enumerate() {
                        let inner_key = PlanExpr::Column {
                            index: inner_offset + inner_idx,
                            name: spec_schema[*inner_idx].name.clone(),
                            span: None,
                        };
                        let eq = PlanExpr::BinaryOp {
                            left: Box::new(outer_keys[i].clone()),
                            op: ast::BinaryOp::Eq,
                            right: Box::new(inner_key),
                            span: None,
                        };
                        cond = Some(match cond {
                            None => eq,
                            Some(prev) => PlanExpr::BinaryOp {
                                left: Box::new(prev),
                                op: ast::BinaryOp::And,
                                right: Box::new(eq),
                                span: None,
                            },
                        });
                    }
                    let condition = JoinCondition::On(cond.expect("at least one correlation"));
                    plan = LogicalPlan::Join {
                        left: Box::new(plan),
                        right: Box::new(spec.plan),
                        join_type: ast::JoinType::Left,
                        condition,
                        dynamic_filter_ids: Vec::new(),
                    };
                }
                rem
            } else {
                None
            };

            if let Some(remaining) = residual {
                if contains_nested_subquery(&remaining) {
                    let parts = split_and(&remaining);
                    if parts.iter().any(|part| {
                        matches!(
                            part,
                            ast::Expr::BinaryOp {
                                op: ast::BinaryOp::Or,
                                ..
                            }
                        ) && contains_nested_subquery(part)
                    }) {
                        let (ordinary, nested): (Vec<_>, Vec<_>) = parts
                            .into_iter()
                            .partition(|part| !contains_nested_subquery(part));
                        if let Some(ordinary) = combine_and(ordinary) {
                            let predicate = self.plan_expr(&ordinary, &ctx).await?;
                            plan = LogicalPlan::Filter {
                                input: Box::new(plan),
                                predicate,
                            };
                        }
                        if let Some(nested) = combine_and(nested) {
                            plan = self
                                .filter_nested_subqueries(plan, &nested, &ctx, PredicateTruth::True)
                                .await?;
                        }
                    } else {
                        plan = self
                            .filter_nested_subqueries(plan, &remaining, &ctx, PredicateTruth::True)
                            .await?;
                    }
                } else {
                    let predicate = self.plan_expr(&remaining, &ctx).await?;
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate,
                    };
                }
            }
        }

        // 3. GROUP BY / HAVING → Aggregate
        // Also handle implicit aggregate: SELECT SUM(x) FROM t (no GROUP BY but has aggregates)
        let mut group_by_ast = resolve_group_by_ordinals(body)?;
        let rollup = rollup_group_by_keys(&group_by_ast)?;
        if let Some(keys) = &rollup {
            group_by_ast = keys.clone();
        }
        let has_group_by = !group_by_ast.is_empty();
        let mut aggr_exprs = self.collect_aggregates(&body.projection, &ctx).await?;
        // Aggregates can appear in HAVING without being in SELECT (e.g.
        // `... GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`).
        // Pick those up too so the Aggregate node materializes them and
        // the HAVING rewrite can replace the call with a column ref.
        if let Some(having) = &body.having {
            self.extract_aggregates(having, &ctx, &mut aggr_exprs)
                .await?;
        }
        for item in order_by {
            self.extract_aggregates(&item.expr, &ctx, &mut aggr_exprs)
                .await?;
        }
        let has_aggregates = has_group_by || !aggr_exprs.is_empty();

        if has_aggregates {
            let mut group_by = Vec::with_capacity(group_by_ast.len());
            for e in &group_by_ast {
                group_by.push(self.plan_expr(e, &ctx).await?);
            }

            // Build output schema: group-by columns + aggregate results
            let mut schema = Vec::new();
            for gb in &group_by {
                schema.push(self.expr_to_column_info(gb, &ctx));
            }
            for agg in &aggr_exprs {
                schema.push(self.expr_to_column_info(agg, &ctx));
            }

            plan = if rollup.is_some() {
                let key_count = group_by.len();
                let mut output_schema = schema.clone();
                for col in output_schema.iter_mut().take(key_count) {
                    col.nullable = true;
                }
                for key in &group_by_ast {
                    output_schema.push(ColumnInfo {
                        name: format!("GROUPING({})", format_ast_unqualified(key)),
                        data_type: DataType::Int64,
                        nullable: false,
                    });
                }
                schema = output_schema;
                LogicalPlan::RollupAggregate {
                    input: Box::new(plan),
                    group_by,
                    aggr_exprs: aggr_exprs.clone(),
                    schema: schema.clone(),
                }
            } else {
                LogicalPlan::Aggregate {
                    input: Box::new(plan),
                    group_by: group_by.clone(),
                    aggr_exprs: aggr_exprs.clone(),
                    schema: schema.clone(),
                }
            };

            // Update context to reflect aggregate output.
            //
            // For group-by columns, carry the source qualifier forward
            // so that later `n1.n_name` vs `n2.n_name` references in
            // the SELECT/HAVING list resolve to the correct slot. Without
            // this, self-joins whose SELECT projects both aliases of a
            // shared column (`SELECT n1.n_name, n2.n_name ...`) collapse
            // onto the first group-by slot.
            ctx = PlanningContext::new();
            for (i, col) in schema.iter().enumerate() {
                let qualifier = if i < group_by_ast.len() {
                    group_by_qualifier(&group_by_ast[i])
                } else {
                    None
                };
                ctx.columns.push((qualifier, col.clone()));
            }
            ctx.group_by = group_by_ast.clone();

            // HAVING (applied after aggregation)
            // Rewrite aggregate expressions in HAVING to column references
            if let Some(having) = &body.having {
                let num_group_by = group_by_ast.len();
                let rewritten = self.rewrite_aggregates_as_columns(having, &ctx, num_group_by);
                let predicate = self.plan_expr(&rewritten, &ctx).await?;
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        // 3a. Correlated scalar subqueries in the SELECT list: each becomes
        // a LEFT JOIN below the projection, and the item reads its columns.
        let decorrelated_items;
        let select_items: &[ast::SelectItem] = match Box::pin(self.rewrite_select_list_subqueries(
            &body.projection,
            &ctx,
            has_aggregates.then_some(group_by_ast.len()),
        ))
        .await?
        {
            Some((items, joins)) => {
                plan = Box::pin(self.attach_select_subquery_joins(plan, &mut ctx, joins)).await?;
                decorrelated_items = items;
                &decorrelated_items
            }
            None => &body.projection,
        };

        // 3b. Window functions: computed after WHERE / GROUP BY / HAVING and
        // before the projection (ORDER BY / LIMIT are applied above it).
        let mut window_calls = Vec::new();
        for item in select_items {
            if let ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. } = item
            {
                collect_window_calls(expr, &mut window_calls);
            }
        }
        if !window_calls.is_empty() {
            let num_group_by = has_aggregates.then_some(group_by_ast.len());
            plan = self
                .plan_windows(plan, &mut ctx, &window_calls, num_group_by)
                .await?;
        }

        // 3c. Set-returning functions (`unnest`, `generate_subscripts`) as
        // whole SELECT items: evaluated by a ProjectSet below the projection,
        // which then reads each one as a column.
        let srf_items;
        let projection_items = if select_items.iter().any(|i| srf_call(i).is_some()) {
            let num_group_by = has_aggregates.then_some(group_by_ast.len());
            let (set_plan, items) =
                Box::pin(self.plan_project_set(plan, &mut ctx, select_items, num_group_by)).await?;
            plan = set_plan;
            srf_items = items;
            &srf_items
        } else {
            select_items
        };

        // 4. Projection (SELECT list)
        // After aggregate, SELECT expressions that ARE aggregate functions should reference
        // the aggregate output columns by index, not re-resolve their arguments.
        let (proj_exprs, proj_schema) = if has_aggregates {
            self.plan_aggregate_projection(projection_items, &ctx, &group_by_ast)
                .await?
        } else {
            self.plan_projection(projection_items, &ctx).await?
        };

        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            exprs: proj_exprs,
            schema: proj_schema,
        };

        if cte_self_agg_window_enabled() {
            plan = rewrite_cte_self_agg_scalar_to_window(plan);
        }

        // 5. SELECT DISTINCT → Aggregate grouping by every output column,
        // planned and fragmented exactly like a GROUP BY.
        if body.distinct {
            let schema = plan.schema();
            let group_by = schema
                .iter()
                .enumerate()
                .map(|(index, c)| PlanExpr::Column {
                    index,
                    name: c.name.clone(),
                    span: None,
                })
                .collect();
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by,
                aggr_exprs: Vec::new(),
                schema,
            };
        }

        Ok(plan)
    }

    /// Plans the SELECT list's window-function calls above `plan`: one
    /// `Sort` (PARTITION BY keys, then ORDER BY keys) + `Window` pair per
    /// distinct OVER spec. Each result column is named by the call's display
    /// string and appended to `ctx`, which is how `plan_expr` resolves the
    /// call in the projection and in ORDER BY. `num_group_by` is set when the
    /// SELECT aggregates, so `rank() OVER (ORDER BY sum(x))` reads the
    /// Aggregate's output column.
    async fn plan_windows(
        &self,
        mut plan: LogicalPlan,
        ctx: &mut PlanningContext,
        calls: &[ast::Expr],
        num_group_by: Option<usize>,
    ) -> Result<LogicalPlan, PlanError> {
        // (OVER-spec key, functions sharing it), in first-seen order.
        let mut specs: Vec<(String, Vec<WindowFunctionDef>)> = Vec::new();
        for call in calls {
            let ast::Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                rows_frame,
                ..
            } = call
            else {
                continue;
            };
            let upper = name.to_ascii_uppercase();
            let arity_ok = match upper.as_str() {
                "ROW_NUMBER" | "RANK" | "DENSE_RANK" => args.is_empty(),
                "COUNT" => args.len() <= 1,
                "SUM" | "AVG" | "MIN" | "MAX" => args.len() == 1,
                _ => {
                    return Err(PlanError::UnsupportedExpression {
                        message: format!(
                            "window function {name} is not supported (supported: ROW_NUMBER, \
                             RANK, DENSE_RANK, SUM, AVG, COUNT, MIN, MAX)"
                        ),
                        location: None,
                    })
                }
            };
            if !arity_ok {
                return Err(PlanError::invalid_expression(format!(
                    "wrong number of arguments to window function {name}"
                )));
            }

            let operand = |e: &ast::Expr| match num_group_by {
                Some(n) => self.rewrite_aggregates_as_columns(e, ctx, n),
                None => e.clone(),
            };
            let mut fn_args = Vec::with_capacity(args.len());
            for a in args {
                fn_args.push(self.plan_expr(&operand(a), ctx).await?);
            }
            let mut fn_partition = Vec::with_capacity(partition_by.len());
            for p in partition_by {
                fn_partition.push(self.plan_expr(&operand(p), ctx).await?);
            }
            let mut fn_order = Vec::with_capacity(order_by.len());
            for o in order_by {
                fn_order.push(SortExpr {
                    expr: self.plan_expr(&operand(&o.expr), ctx).await?,
                    asc: o.asc.unwrap_or(true),
                    nulls_first: o.nulls_first.unwrap_or(false),
                });
            }

            let mut key = String::new();
            for p in &fn_partition {
                key.push_str(&format!("{p},"));
            }
            key.push('|');
            for s in &fn_order {
                key.push_str(&format!("{} {} {},", s.expr, s.asc, s.nulls_first));
            }
            let def = WindowFunctionDef {
                name: upper,
                args: fn_args,
                partition_by: fn_partition,
                order_by: fn_order,
                rows_frame: *rows_frame,
                output_name: call.to_string(),
            };
            match specs.iter_mut().find(|(k, _)| *k == key) {
                Some((_, functions)) => functions.push(def),
                None => specs.push((key, vec![def])),
            }
        }

        let base_width = plan.schema().len();
        for (_, functions) in specs {
            // WindowExec detects partitions and peers as runs of adjacent
            // rows, so its input must be sorted by the spec's keys.
            let sort_keys: Vec<SortExpr> = functions[0]
                .partition_by
                .iter()
                .map(|e| SortExpr {
                    expr: e.clone(),
                    asc: true,
                    nulls_first: false,
                })
                .chain(functions[0].order_by.iter().cloned())
                .collect();
            let input = if sort_keys.is_empty() {
                plan
            } else {
                LogicalPlan::Sort {
                    input: Box::new(plan),
                    order_by: sort_keys,
                }
            };
            plan = LogicalPlan::Window {
                input: Box::new(input),
                functions,
            };
        }
        for col in plan.schema().into_iter().skip(base_width) {
            ctx.columns.push((Some(WINDOW_QUALIFIER.to_string()), col));
        }
        Ok(plan)
    }

    /// `generate_subscripts`' dimension must be a constant that folds to 1
    /// (`1`, `1::INTEGER`, ...): Arneb arrays have one dimension. Out of line
    /// so `plan_project_set`'s future stays small.
    #[inline(never)]
    async fn check_generate_subscripts_dim(
        &self,
        dim: &ast::Expr,
        ctx: &PlanningContext,
    ) -> Result<(), PlanError> {
        let planned = Box::pin(self.plan_expr(dim, ctx)).await?;
        let is_one = |e: &PlanExpr| {
            matches!(
                e,
                PlanExpr::Literal {
                    value: ScalarValue::Int32(1) | ScalarValue::Int64(1),
                    ..
                }
            )
        };
        match crate::optimizer::fold_constants(planned) {
            Ok(e) if is_one(&e) => Ok(()),
            // `1::SMALLINT`: there is no SMALLINT literal, so the fold keeps
            // the (range-checked) cast over a BIGINT literal.
            Ok(PlanExpr::Cast {
                expr,
                data_type: DataType::Int8 | DataType::Int16,
                ..
            }) if is_one(&expr) => Ok(()),
            _ => Err(generate_subscripts_dim_error()),
        }
    }

    /// Plans the SELECT items that are set-returning function calls into a
    /// `ProjectSet` above `plan`. Each SRF becomes a column appended to `ctx`
    /// (under [`SRF_QUALIFIER`]), and the returned SELECT list reads it as a
    /// column reference, named like PostgreSQL does (`unnest`) unless the
    /// item has an alias. `num_group_by` is set when the SELECT aggregates.
    async fn plan_project_set(
        &self,
        plan: LogicalPlan,
        ctx: &mut PlanningContext,
        items: &[ast::SelectItem],
        num_group_by: Option<usize>,
    ) -> Result<(LogicalPlan, Vec<ast::SelectItem>), PlanError> {
        let input_schema = plan.schema();
        let mut srfs: Vec<(SrfKind, PlanExpr)> = Vec::new();
        let mut schema = input_schema.clone();
        let mut rewritten = Vec::with_capacity(items.len());
        for item in items {
            let Some((kind, call)) = srf_call(item) else {
                rewritten.push(item.clone());
                continue;
            };
            let array = srf_array_argument(kind, call)?;
            if let Some(dim) =
                generate_subscripts_dim(call).filter(|_| kind == SrfKind::GenerateSubscripts)
            {
                self.check_generate_subscripts_dim(dim, ctx).await?;
            }
            let array = match num_group_by {
                Some(n) => self.rewrite_aggregates_as_columns(array, ctx, n),
                None => array.clone(),
            };
            let planned = self.plan_expr(&array, ctx).await?;
            // Typed against the planning context the argument was resolved in.
            let ctx_schema: Vec<ColumnInfo> = ctx.columns.iter().map(|(_, c)| c.clone()).collect();
            let array_type = match plan_expr_type(&planned, &ctx_schema) {
                Some(t @ DataType::List(_)) => t,
                // An untyped NULL or parameter: the element type is unknown,
                // as PostgreSQL also refuses. `CAST(NULL AS int[])` is typed.
                None | Some(DataType::Null) => {
                    return Err(PlanError::invalid_expression(format!(
                        "could not determine the type of the argument of {}(); cast it to an \
                         array type, e.g. CAST(NULL AS integer[])",
                        kind.name()
                    )))
                }
                Some(other) => {
                    return Err(PlanError::invalid_expression(format!(
                        "function {}({other}) does not exist: {} takes an array",
                        kind.name(),
                        kind.name()
                    )))
                }
            };
            let mut name = kind.display(&planned);
            if schema.iter().any(|c| c.name.eq_ignore_ascii_case(&name)) {
                name = format!("{name}#{}", srfs.len() + 1);
            }
            let column = ColumnInfo {
                name: name.clone(),
                data_type: kind.output_type(&array_type),
                nullable: true,
            };
            ctx.columns
                .push((Some(SRF_QUALIFIER.to_string()), column.clone()));
            schema.push(column);
            srfs.push((kind, planned));
            let reference = ast::Expr::Column {
                col_ref: ast::ColumnRef {
                    name,
                    table: Some(SRF_QUALIFIER.to_string()),
                    span: arneb_sql_parser::Span::empty(),
                },
                span: arneb_sql_parser::Span::empty(),
            };
            let (ast::SelectItem::UnnamedExpr(item_expr)
            | ast::SelectItem::ExprWithAlias {
                expr: item_expr, ..
            }) = item
            else {
                unreachable!("srf_call matches expression items");
            };
            let alias = match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => alias.clone(),
                _ => srf_item_field(item_expr).unwrap_or(kind.name()).to_string(),
            };
            rewritten.push(ast::SelectItem::ExprWithAlias {
                expr: replace_srf_call(item_expr, reference),
                alias,
            });
        }
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(plan),
            srfs,
            schema,
        };
        Ok((plan, rewritten))
    }

    /// Split the WHERE-clause AND-tree, pull every `EXISTS (...)` /
    /// `NOT EXISTS (...)` sub-predicate out as a [`SemiJoinSpec`],
    /// and return `(residual_where, specs)` where `residual_where`
    /// is the AND-combination of the remaining non-EXISTS branches.
    ///
    /// Each EXISTS subquery is decorrelated against `outer_ctx`: its
    /// inner WHERE is split on AND, the equi-correlation
    /// `outer_col = inner_col` is extracted as the SemiJoin key
    /// pair, and any remaining inner-only predicates wrap the inner
    /// plan as a Filter. The shape arneb decorrelates today is one
    /// equi-correlation per EXISTS — covers TPC-H Q04/Q21/Q22's
    /// pattern. Unsupported shapes (multi-correlation, non-equi
    /// correlation, JOIN-ON correlation) surface as
    /// `UnsupportedFeature`.
    async fn extract_exists_subqueries(
        &self,
        selection: &ast::Expr,
        outer_ctx: &PlanningContext,
    ) -> Result<(Option<ast::Expr>, Vec<SemiJoinSpec>), PlanError> {
        let parts = split_and(selection);
        let mut residual: Vec<ast::Expr> = Vec::new();
        let mut specs: Vec<SemiJoinSpec> = Vec::new();
        for part in parts {
            match part {
                ast::Expr::Exists {
                    subquery, negated, ..
                } => {
                    let spec = self
                        .decorrelate_exists(*subquery, outer_ctx, negated)
                        .await?;
                    specs.push(spec);
                }
                ast::Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                    ..
                } => {
                    let in_specs = self
                        .decorrelate_in_subquery(*expr, *subquery, outer_ctx, negated)
                        .await?;
                    specs.extend(in_specs);
                }
                other => residual.push(other),
            }
        }
        Ok((combine_and(residual), specs))
    }

    #[async_recursion]
    async fn filter_nested_subqueries(
        &self,
        input: LogicalPlan,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        truth: PredicateTruth,
    ) -> Result<LogicalPlan, PlanError> {
        match expr {
            ast::Expr::Nested { expr, .. } => {
                self.filter_nested_subqueries(input, expr, ctx, truth).await
            }
            ast::Expr::UnaryOp {
                op: ast::UnaryOp::Not,
                expr,
                ..
            } => {
                let inner_truth = match truth {
                    PredicateTruth::True => PredicateTruth::False,
                    PredicateTruth::False => PredicateTruth::True,
                    PredicateTruth::NotTrue => PredicateTruth::NotFalse,
                    PredicateTruth::NotFalse => PredicateTruth::NotTrue,
                };
                self.filter_nested_subqueries(input, expr, ctx, inner_truth)
                    .await
            }
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOp::And,
                right,
                ..
            } => match truth {
                PredicateTruth::True | PredicateTruth::NotTrue => {
                    if truth == PredicateTruth::True {
                        let input = self
                            .filter_nested_subqueries(input, left, ctx, truth)
                            .await?;
                        self.filter_nested_subqueries(input, right, ctx, truth)
                            .await
                    } else {
                        let first = self
                            .filter_nested_subqueries(input.clone(), left, ctx, truth)
                            .await?;
                        let rest = self
                            .filter_nested_subqueries(input, left, ctx, PredicateTruth::True)
                            .await?;
                        let second = self
                            .filter_nested_subqueries(rest, right, ctx, truth)
                            .await?;
                        Ok(LogicalPlan::UnionAll {
                            inputs: vec![first, second],
                        })
                    }
                }
                PredicateTruth::False => {
                    let first = self
                        .filter_nested_subqueries(input.clone(), left, ctx, truth)
                        .await?;
                    let rest = self
                        .filter_nested_subqueries(input, left, ctx, PredicateTruth::NotFalse)
                        .await?;
                    let second = self
                        .filter_nested_subqueries(rest, right, ctx, truth)
                        .await?;
                    Ok(LogicalPlan::UnionAll {
                        inputs: vec![first, second],
                    })
                }
                PredicateTruth::NotFalse => {
                    let input = self
                        .filter_nested_subqueries(input, left, ctx, truth)
                        .await?;
                    self.filter_nested_subqueries(input, right, ctx, truth)
                        .await
                }
            },
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOp::Or,
                right,
                ..
            } => match truth {
                PredicateTruth::True | PredicateTruth::NotFalse => {
                    let first = self
                        .filter_nested_subqueries(input.clone(), left, ctx, truth)
                        .await?;
                    let rest = self
                        .filter_nested_subqueries(
                            input,
                            left,
                            ctx,
                            if truth == PredicateTruth::True {
                                PredicateTruth::NotTrue
                            } else {
                                PredicateTruth::False
                            },
                        )
                        .await?;
                    let second = self
                        .filter_nested_subqueries(rest, right, ctx, truth)
                        .await?;
                    Ok(LogicalPlan::UnionAll {
                        inputs: vec![first, second],
                    })
                }
                PredicateTruth::False | PredicateTruth::NotTrue => {
                    let input = self
                        .filter_nested_subqueries(input, left, ctx, truth)
                        .await?;
                    self.filter_nested_subqueries(input, right, ctx, truth)
                        .await
                }
            },
            ast::Expr::Exists {
                subquery, negated, ..
            } => {
                let anti = matches!(truth, PredicateTruth::False | PredicateTruth::NotTrue);
                let spec = self
                    .decorrelate_exists(*subquery.clone(), ctx, *negated ^ anti)
                    .await?;
                Ok(apply_semi_join(input, spec))
            }
            ast::Expr::InSubquery {
                expr,
                subquery,
                negated,
                ..
            } => {
                let requested = if *negated {
                    match truth {
                        PredicateTruth::True => PredicateTruth::False,
                        PredicateTruth::False => PredicateTruth::True,
                        PredicateTruth::NotTrue => PredicateTruth::NotFalse,
                        PredicateTruth::NotFalse => PredicateTruth::NotTrue,
                    }
                } else {
                    truth
                };
                let specs = match requested {
                    PredicateTruth::True | PredicateTruth::False => {
                        self.decorrelate_in_subquery(
                            *expr.clone(),
                            *subquery.clone(),
                            ctx,
                            requested == PredicateTruth::False,
                        )
                        .await?
                    }
                    PredicateTruth::NotTrue => {
                        let mut specs = self
                            .decorrelate_in_subquery(*expr.clone(), *subquery.clone(), ctx, false)
                            .await?;
                        specs[0].kind = SemiJoinKind::Anti;
                        specs
                    }
                    PredicateTruth::NotFalse => {
                        return self.in_subquery_not_false(input, expr, subquery, ctx).await;
                    }
                };
                Ok(specs.into_iter().fold(input, apply_semi_join))
            }
            _ => {
                let predicate = self.plan_expr(expr, ctx).await?;
                let predicate = match truth {
                    PredicateTruth::True => predicate,
                    PredicateTruth::False => PlanExpr::UnaryOp {
                        op: ast::UnaryOp::Not,
                        expr: Box::new(predicate),
                        span: None,
                    },
                    PredicateTruth::NotTrue | PredicateTruth::NotFalse => {
                        let null = PlanExpr::IsNull {
                            expr: Box::new(predicate.clone()),
                            span: None,
                        };
                        let known = if truth == PredicateTruth::NotTrue {
                            PlanExpr::UnaryOp {
                                op: ast::UnaryOp::Not,
                                expr: Box::new(predicate),
                                span: None,
                            }
                        } else {
                            predicate
                        };
                        PlanExpr::BinaryOp {
                            left: Box::new(known),
                            op: ast::BinaryOp::Or,
                            right: Box::new(null),
                            span: None,
                        }
                    }
                };
                Ok(LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }
        }
    }

    /// The rows of `input` where `<expr> IN (subquery)` is TRUE or NULL, i.e.
    /// not FALSE (an `x NOT IN (...) OR ...` / `NOT (x IN (...) AND ...)`
    /// operand). Three disjoint parts, joined with `UNION ALL`:
    ///
    /// - rows with a match (TRUE);
    /// - rows without one, when the subquery yields a NULL (NULL);
    /// - rows without one whose `<expr>` is NULL, when the subquery yields
    ///   rows but no NULL (NULL).
    ///
    /// A row of a NULL `<expr>` over an empty subquery is FALSE and is left
    /// out. A part that cannot have rows (non-nullable column) is skipped.
    async fn in_subquery_not_false(
        &self,
        input: LogicalPlan,
        expr: &ast::Expr,
        subquery: &ast::Query,
        ctx: &PlanningContext,
    ) -> Result<LogicalPlan, PlanError> {
        let mut specs = self
            .decorrelate_in_subquery(expr.clone(), subquery.clone(), ctx, false)
            .await?;
        let matched = specs.remove(0);
        let sub_plan = matched.right_plan.clone();
        let sub_column = sub_plan.schema().remove(0);
        let outer_nullable = match &matched.left_key {
            PlanExpr::Column { index, .. } => ctx.columns[*index].1.nullable,
            PlanExpr::Literal { value, .. } => matches!(value, ScalarValue::Null),
            _ => true,
        };
        let unmatched = apply_semi_join(
            input.clone(),
            SemiJoinSpec {
                kind: SemiJoinKind::Anti,
                right_plan: matched.right_plan.clone(),
                left_key: matched.left_key.clone(),
                right_key: matched.right_key.clone(),
                residual: None,
            },
        );
        let outer_key = matched.left_key.clone();
        let mut parts = vec![apply_semi_join(input, matched)];

        let constant_key = PlanExpr::Literal {
            value: ScalarValue::Int32(1),
            span: None,
        };
        let sub_value = |index: usize| PlanExpr::Column {
            index,
            name: sub_column.name.clone(),
            span: None,
        };
        // Some row of the subquery is NULL (a semi or anti join on a
        // constant key against `subquery WHERE value IS NULL LIMIT 1`).
        let subquery_has_null = |kind: SemiJoinKind| SemiJoinSpec {
            kind,
            right_plan: LogicalPlan::Limit {
                input: Box::new(LogicalPlan::Filter {
                    input: Box::new(sub_plan.clone()),
                    predicate: PlanExpr::IsNull {
                        expr: Box::new(sub_value(0)),
                        span: None,
                    },
                }),
                limit: Some(1),
                offset: None,
            },
            left_key: constant_key.clone(),
            right_key: constant_key.clone(),
            residual: None,
        };
        if sub_column.nullable {
            parts.push(apply_semi_join(
                unmatched.clone(),
                subquery_has_null(SemiJoinKind::Semi),
            ));
        }
        if outer_nullable {
            let without_null = if sub_column.nullable {
                apply_semi_join(unmatched, subquery_has_null(SemiJoinKind::Anti))
            } else {
                unmatched
            };
            // `<expr> IS NULL` and the subquery has a row. The residual
            // names the subquery's column (an always-true test of it), as
            // the NOT IN emptiness check does.
            let inner = sub_value(ctx.columns.len());
            parts.push(apply_semi_join(
                without_null,
                SemiJoinSpec {
                    kind: SemiJoinKind::Semi,
                    right_plan: LogicalPlan::Limit {
                        input: Box::new(sub_plan),
                        limit: Some(1),
                        offset: None,
                    },
                    left_key: constant_key.clone(),
                    right_key: constant_key,
                    residual: Some(PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::IsNull {
                            expr: Box::new(outer_key),
                            span: None,
                        }),
                        op: ast::BinaryOp::And,
                        right: Box::new(PlanExpr::BinaryOp {
                            left: Box::new(PlanExpr::IsNull {
                                expr: Box::new(inner.clone()),
                                span: None,
                            }),
                            op: ast::BinaryOp::Or,
                            right: Box::new(PlanExpr::IsNotNull {
                                expr: Box::new(inner),
                                span: None,
                            }),
                            span: None,
                        }),
                        span: None,
                    }),
                },
            ));
        }
        Ok(if parts.len() == 1 {
            parts.remove(0)
        } else {
            LogicalPlan::UnionAll { inputs: parts }
        })
    }

    /// `GROUPING(k1, ..., kn)`, as in Trino: a BIGINT bit set with one bit per
    /// argument, the first argument the most significant, set when that key
    /// is rolled up (not part of the row's grouping). Under `ROLLUP` each
    /// key's bit is its `GROUPING(key)` marker column; under a plain
    /// `GROUP BY` every key is always grouped, so its bit is 0. An argument
    /// that is not a grouping key is an error.
    async fn plan_grouping(
        &self,
        args: &[ast::FunctionArg],
        ctx: &PlanningContext,
        span: Option<arneb_sql_parser::Span>,
    ) -> Result<PlanExpr, PlanError> {
        if args.is_empty() {
            return Err(PlanError::invalid_expression(
                "GROUPING requires at least one grouping expression",
            ));
        }
        let mut bits = Vec::with_capacity(args.len());
        for arg in args {
            let ast::FunctionArg::Unnamed(key) = arg else {
                return Err(PlanError::invalid_expression(
                    "GROUPING takes grouping expressions as arguments",
                ));
            };
            let marker = format!("GROUPING({})", format_ast_unqualified(key));
            if let Some((index, (_, col))) = ctx
                .columns
                .iter()
                .enumerate()
                .find(|(_, (_, col))| col.name.eq_ignore_ascii_case(&marker))
            {
                bits.push(Some(PlanExpr::Column {
                    index,
                    name: col.name.clone(),
                    span,
                }));
                continue;
            }
            // Not a ROLLUP key: it must be a key of a plain GROUP BY.
            if ctx.group_by.is_empty() || !self.is_group_by_expr(key, &ctx.group_by) {
                return Err(PlanError::invalid_expression(format!(
                    "GROUPING argument {} is not a grouping expression",
                    format_ast_unqualified(key)
                )));
            }
            bits.push(None);
        }
        let n = bits.len();
        let literal = |value: i64| PlanExpr::Literal {
            value: ScalarValue::Int64(value),
            span,
        };
        let mut result: Option<PlanExpr> = None;
        for (i, bit) in bits.into_iter().enumerate() {
            let Some(marker) = bit else { continue };
            let weight = 1i64 << (n - 1 - i);
            let term = if weight == 1 {
                marker
            } else {
                PlanExpr::BinaryOp {
                    left: Box::new(marker),
                    op: ast::BinaryOp::Multiply,
                    right: Box::new(literal(weight)),
                    span,
                }
            };
            result = Some(match result {
                None => term,
                Some(acc) => PlanExpr::BinaryOp {
                    left: Box::new(acc),
                    op: ast::BinaryOp::Plus,
                    right: Box::new(term),
                    span,
                },
            });
        }
        Ok(result.unwrap_or_else(|| literal(0)))
    }

    /// Decorrelate `<expr> IN (subquery)` / `<expr> NOT IN (subquery)`.
    /// Currently only the **uncorrelated** form is supported — the
    /// subquery may reference its own FROM tables but not the outer
    /// scope. The subquery must produce exactly one column.
    ///
    /// Maps to `SemiJoin` (IN) / `AntiJoin` (NOT IN). Ordinary joins use
    /// the outer expression and subquery output as their keys.
    ///
    /// Nullable NOT IN also rejects rows when the subquery contains NULL,
    /// and rejects a NULL outer key when the subquery is nonempty.
    async fn decorrelate_in_subquery(
        &self,
        outer_expr: ast::Expr,
        subquery: ast::Query,
        outer_ctx: &PlanningContext,
        negated: bool,
    ) -> Result<Vec<SemiJoinSpec>, PlanError> {
        let subplan = self.plan_query(&subquery).await?;
        let sub_schema = subplan.schema();
        if sub_schema.len() != 1 {
            return Err(PlanError::invalid_expression(format!(
                "IN subquery must produce exactly one column, got {}",
                sub_schema.len()
            )));
        }

        let left_key = self.plan_expr(&outer_expr, outer_ctx).await?;
        let right_key = PlanExpr::Column {
            index: 0,
            name: sub_schema[0].name.clone(),
            span: None,
        };

        let outer_nullable = match &left_key {
            PlanExpr::Column { index, .. } => outer_ctx.columns[*index].1.nullable,
            PlanExpr::Literal { value, .. } => matches!(value, ScalarValue::Null),
            _ => true,
        };
        let mut specs = Vec::new();
        let null_check_plan = if negated && sub_schema[0].nullable {
            Some(subplan.clone())
        } else {
            None
        };
        let nonempty_check_plan = if negated && outer_nullable {
            Some(subplan.clone())
        } else {
            None
        };
        specs.push(SemiJoinSpec {
            kind: if negated {
                SemiJoinKind::Anti
            } else {
                SemiJoinKind::Semi
            },
            right_plan: subplan,
            left_key: left_key.clone(),
            right_key,
            residual: None,
        });

        let constant_key = PlanExpr::Literal {
            value: ScalarValue::Int32(1),
            span: None,
        };
        if let Some(plan) = null_check_plan {
            specs.push(SemiJoinSpec {
                kind: SemiJoinKind::Anti,
                right_plan: LogicalPlan::Limit {
                    input: Box::new(LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: PlanExpr::IsNull {
                            expr: Box::new(PlanExpr::Column {
                                index: 0,
                                name: sub_schema[0].name.clone(),
                                span: None,
                            }),
                            span: None,
                        },
                    }),
                    limit: Some(1),
                    offset: None,
                },
                left_key: constant_key.clone(),
                right_key: constant_key.clone(),
                residual: None,
            });
        }
        if let Some(plan) = nonempty_check_plan {
            specs.push(SemiJoinSpec {
                kind: SemiJoinKind::Anti,
                right_plan: LogicalPlan::Limit {
                    input: Box::new(plan),
                    limit: Some(1),
                    offset: None,
                },
                left_key: constant_key.clone(),
                right_key: constant_key,
                residual: Some(PlanExpr::BinaryOp {
                    left: Box::new(PlanExpr::IsNull {
                        expr: Box::new(left_key),
                        span: None,
                    }),
                    op: ast::BinaryOp::And,
                    right: Box::new(PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::IsNull {
                            expr: Box::new(PlanExpr::Column {
                                index: outer_ctx.columns.len(),
                                name: sub_schema[0].name.clone(),
                                span: None,
                            }),
                            span: None,
                        }),
                        op: ast::BinaryOp::Or,
                        right: Box::new(PlanExpr::IsNotNull {
                            expr: Box::new(PlanExpr::Column {
                                index: outer_ctx.columns.len(),
                                name: sub_schema[0].name.clone(),
                                span: None,
                            }),
                            span: None,
                        }),
                        span: None,
                    }),
                    span: None,
                }),
            });
        }
        Ok(specs)
    }

    /// Walk the WHERE AND-tree, replacing correlated scalar subqueries
    /// within comparison operands with values from [`LeftJoinSpec`]s.
    ///
    /// Conjuncts that don't match the shape are passed through
    /// unchanged. Returns `(residual_where, specs)`.
    async fn extract_correlated_scalar_subqueries(
        &self,
        selection: ast::Expr,
        outer_ctx: &PlanningContext,
    ) -> Result<(Option<ast::Expr>, Vec<LeftJoinSpec>), PlanError> {
        let parts = split_and(&selection);
        let mut specs: Vec<LeftJoinSpec> = Vec::new();
        let mut residual: Vec<ast::Expr> = Vec::new();
        for part in parts {
            let rewritten = self
                .try_rewrite_scalar_subquery_predicate(part, outer_ctx, &mut specs)
                .await?;
            residual.push(rewritten);
        }
        Ok((combine_and(residual), specs))
    }

    /// Rewrite scalar subqueries within the arithmetic operands of a comparison.
    async fn try_rewrite_scalar_subquery_predicate(
        &self,
        part: ast::Expr,
        outer_ctx: &PlanningContext,
        specs: &mut Vec<LeftJoinSpec>,
    ) -> Result<ast::Expr, PlanError> {
        let (left, op, right, span) = match part {
            ast::Expr::BinaryOp {
                left,
                op,
                right,
                span,
            } => (left, op, right, span),
            other => return Ok(other),
        };

        if !matches!(
            op,
            ast::BinaryOp::Eq
                | ast::BinaryOp::NotEq
                | ast::BinaryOp::Lt
                | ast::BinaryOp::LtEq
                | ast::BinaryOp::Gt
                | ast::BinaryOp::GtEq
        ) {
            return Ok(ast::Expr::BinaryOp {
                left,
                op,
                right,
                span,
            });
        }

        let left = self.rewrite_scalar_operand(*left, outer_ctx, specs).await?;
        let right = self
            .rewrite_scalar_operand(*right, outer_ctx, specs)
            .await?;
        Ok(ast::Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            span,
        })
    }

    #[async_recursion]
    async fn rewrite_scalar_operand(
        &self,
        expr: ast::Expr,
        outer_ctx: &PlanningContext,
        specs: &mut Vec<LeftJoinSpec>,
    ) -> Result<ast::Expr, PlanError> {
        let (subquery, span) = match expr {
            ast::Expr::Subquery { query, span } => (*query, span),
            ast::Expr::BinaryOp {
                left,
                op,
                right,
                span,
            } if matches!(
                op,
                ast::BinaryOp::Plus
                    | ast::BinaryOp::Minus
                    | ast::BinaryOp::Multiply
                    | ast::BinaryOp::Divide
                    | ast::BinaryOp::Modulo
            ) =>
            {
                let left = self.rewrite_scalar_operand(*left, outer_ctx, specs).await?;
                let right = self
                    .rewrite_scalar_operand(*right, outer_ctx, specs)
                    .await?;
                return Ok(ast::Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                    span,
                });
            }
            ast::Expr::Nested { expr, span } => {
                let expr = self.rewrite_scalar_operand(*expr, outer_ctx, specs).await?;
                return Ok(ast::Expr::Nested {
                    expr: Box::new(expr),
                    span,
                });
            }
            other => return Ok(other),
        };

        let Some(spec) = self
            .try_make_leftjoin_spec(subquery.clone(), outer_ctx, specs.len())
            .await?
        else {
            return Ok(ast::Expr::Subquery {
                query: Box::new(subquery),
                span,
            });
        };

        let value_col_name = spec
            .plan
            .schema()
            .last()
            .expect("decorrelated plan has at least the value column")
            .name
            .clone();
        let placeholder = ast::Expr::Column {
            col_ref: ast::ColumnRef {
                name: value_col_name,
                table: None,
                span,
            },
            span,
        };
        let match_key = ast::Expr::Column {
            col_ref: ast::ColumnRef {
                name: spec.plan.schema()[0].name.clone(),
                table: None,
                span,
            },
            span,
        };
        let placeholder = ast::Expr::Case {
            operand: None,
            conditions: vec![ast::Expr::IsNotNull {
                expr: Box::new(match_key),
                span,
            }],
            results: vec![placeholder],
            else_result: Some(Box::new(spec.empty_value.clone())),
            span,
        };
        specs.push(spec);
        Ok(placeholder)
    }

    /// Try to decorrelate one scalar subquery body into a `[corr_key,
    /// value]` plan. Returns `None` if the subquery isn't in the
    /// supported shape (single SELECT item, single equi-correlation,
    /// no nested CTE/DISTINCT/GROUP BY/HAVING).
    async fn try_make_leftjoin_spec(
        &self,
        subquery: ast::Query,
        outer_ctx: &PlanningContext,
        suffix: usize,
    ) -> Result<Option<LeftJoinSpec>, PlanError> {
        let body = match subquery.body {
            ast::QueryBody::Select(s) => s,
            _ => return Ok(None),
        };
        if !subquery.ctes.is_empty()
            || body.distinct
            || !body.group_by.is_empty()
            || body.having.is_some()
        {
            return Ok(None);
        }
        if body.projection.len() != 1 {
            return Ok(None);
        }
        let proj_expr = match &body.projection[0] {
            ast::SelectItem::UnnamedExpr(e) => e.clone(),
            ast::SelectItem::ExprWithAlias { expr, .. } => expr.clone(),
            _ => return Ok(None),
        };
        let Some(empty_value) = aggregate_projection_on_empty(&proj_expr) else {
            return Ok(None);
        };
        let inner_where = match body.selection.clone() {
            Some(w) => w,
            None => return Ok(None),
        };

        let (inner_plan, inner_ctx) = self.plan_from(&body.from).await?;

        let parts = split_and(&inner_where);
        let mut inner_only_parts: Vec<ast::Expr> = Vec::new();
        // Multiple equi-correlations are joined as AND in the final
        // LEFT JOIN condition. Mixed predicates that reference both
        // outer and inner without being equi (e.g. `<>`) aren't
        // supported here — bail out and leave the predicate as a
        // (slow) ScalarSubquery.
        let mut correlations: Vec<(ast::Expr, ast::Expr)> = Vec::new();
        for part in parts {
            if let Some(pair) = try_match_correlation(&part, outer_ctx, &inner_ctx) {
                correlations.push(pair);
            } else if let Some((common, remainder)) =
                factor_or_correlations(&part, outer_ctx, &inner_ctx)
            {
                correlations.extend(common);
                if let Some(remainder) = remainder {
                    inner_only_parts.push(remainder);
                }
            } else if references_outer(&part, &inner_ctx) {
                return Ok(None);
            } else {
                inner_only_parts.push(part);
            }
        }
        if correlations.is_empty() {
            return Ok(None);
        }

        // Inner-only filters apply BEFORE the aggregation.
        let mut filtered_inner = inner_plan;
        if !inner_only_parts.is_empty() {
            let combined = combine_and(inner_only_parts).expect("non-empty");
            let pred = self.plan_expr(&combined, &inner_ctx).await?;
            filtered_inner = LogicalPlan::Filter {
                input: Box::new(filtered_inner),
                predicate: pred,
            };
        }

        self.build_aggregate_leftjoin_spec(
            filtered_inner,
            &inner_ctx,
            correlations,
            &proj_expr,
            empty_value,
            &suffix.to_string(),
        )
        .await
    }

    /// Group a filtered correlated subquery input by its inner correlation
    /// keys and project `[corr_key_0, ..., value]` over the aggregates of
    /// `proj_expr`. `correlations` are `(outer, inner)` pairs. `None` when
    /// the projection has no aggregate.
    async fn build_aggregate_leftjoin_spec(
        &self,
        filtered_inner: LogicalPlan,
        inner_ctx: &PlanningContext,
        correlations: Vec<(ast::Expr, ast::Expr)>,
        proj_expr: &ast::Expr,
        empty_value: ast::Expr,
        suffix: &str,
    ) -> Result<Option<LeftJoinSpec>, PlanError> {
        // Decorrelate by GROUP BY-ing on every inner correlation
        // column. Multi-correlation (Q20) lands here naturally — the
        // outer LEFT JOIN later AND-s all the key pairs.
        let mut group_by_exprs: Vec<PlanExpr> = Vec::with_capacity(correlations.len());
        let mut inner_corr_infos: Vec<ColumnInfo> = Vec::with_capacity(correlations.len());
        for (_, inner_ast) in &correlations {
            let p = self.plan_expr(inner_ast, inner_ctx).await?;
            inner_corr_infos.push(self.expr_to_column_info(&p, inner_ctx));
            group_by_exprs.push(p);
        }

        // Collect aggregates referenced by the projection expression.
        let mut aggrs: Vec<PlanExpr> = Vec::new();
        self.extract_aggregates(proj_expr, inner_ctx, &mut aggrs)
            .await?;
        if aggrs.is_empty() {
            // No aggregate ⇒ the scalar subquery isn't a single-row
            // aggregation we can decorrelate this simply.
            return Ok(None);
        }

        let mut agg_schema: Vec<ColumnInfo> = inner_corr_infos.clone();
        for a in &aggrs {
            agg_schema.push(self.expr_to_column_info(a, inner_ctx));
        }
        let agg_plan = LogicalPlan::Aggregate {
            input: Box::new(filtered_inner),
            group_by: group_by_exprs.clone(),
            aggr_exprs: aggrs.clone(),
            schema: agg_schema.clone(),
        };

        let mut post_agg_ctx = PlanningContext::new();
        for c in &agg_schema {
            post_agg_ctx.columns.push((None, c.clone()));
        }
        let num_group_by = correlations.len();
        let rewritten_proj =
            self.rewrite_aggregates_as_columns(proj_expr, &post_agg_ctx, num_group_by);
        let projected_scalar = self.plan_expr(&rewritten_proj, &post_agg_ctx).await?;
        let value_name = format!("__corr_scalar_{suffix}");
        let scalar_info = ColumnInfo {
            name: value_name.clone(),
            data_type: self
                .expr_to_column_info(&projected_scalar, &post_agg_ctx)
                .data_type,
            nullable: true,
        };

        // Final projection: [corr_key_0, corr_key_1, ..., scalar_value]
        let mut proj_exprs: Vec<PlanExpr> = Vec::with_capacity(num_group_by + 1);
        let mut proj_schema: Vec<ColumnInfo> = Vec::with_capacity(num_group_by + 1);
        for i in 0..num_group_by {
            proj_exprs.push(PlanExpr::Column {
                index: i,
                name: agg_schema[i].name.clone(),
                span: None,
            });
            // Rename to a synthesized key name so the column can be
            // referenced without ambiguity from the join condition.
            // The original inner column name (e.g. `l_partkey`) often
            // duplicates an outer column name; the LeftJoinSpec
            // consumer addresses by index anyway, but the name guards
            // against future name-based resolution.
            proj_schema.push(ColumnInfo {
                name: format!("__corr_key_{suffix}_{i}"),
                data_type: inner_corr_infos[i].data_type.clone(),
                nullable: inner_corr_infos[i].nullable,
            });
        }
        proj_exprs.push(projected_scalar);
        proj_schema.push(scalar_info);

        let proj_node = LogicalPlan::Projection {
            input: Box::new(agg_plan),
            exprs: proj_exprs,
            schema: proj_schema,
        };

        let mut spec_correlations: Vec<(ast::Expr, usize)> = Vec::with_capacity(correlations.len());
        for (i, (outer_ast, _)) in correlations.into_iter().enumerate() {
            spec_correlations.push((outer_ast, i));
        }

        Ok(Some(LeftJoinSpec {
            plan: proj_node,
            correlations: spec_correlations,
            empty_value,
        }))
    }

    /// Decorrelate one `EXISTS (...) / NOT EXISTS (...)` subquery.
    /// Currently handles the single-equi-correlation shape (the only
    /// one needed by TPC-H Q04/Q21/Q22). The inner subquery's body
    /// MUST be a SELECT with a FROM clause and at least one
    /// `outer_col = inner_col` AND-conjunct in its WHERE.
    async fn decorrelate_exists(
        &self,
        subquery: ast::Query,
        outer_ctx: &PlanningContext,
        negated: bool,
    ) -> Result<SemiJoinSpec, PlanError> {
        let body = match subquery.body {
            ast::QueryBody::Select(s) => s,
            _ => {
                return Err(PlanError::invalid_expression(
                    "EXISTS subquery body must be a SELECT (no UNION/INTERSECT/EXCEPT)".to_string(),
                ))
            }
        };
        if !subquery.ctes.is_empty() {
            return Err(PlanError::invalid_expression(
                "EXISTS subquery with CTE not yet supported".to_string(),
            ));
        }
        if body.distinct {
            return Err(PlanError::invalid_expression(
                "EXISTS subquery with DISTINCT not yet supported".to_string(),
            ));
        }
        if !body.group_by.is_empty() || body.having.is_some() {
            return Err(PlanError::invalid_expression(
                "EXISTS subquery with GROUP BY / HAVING not yet supported".to_string(),
            ));
        }

        let (inner_plan, inner_ctx) = self.plan_from(&body.from).await?;

        let inner_where = body.selection.ok_or_else(|| {
            PlanError::invalid_expression(
                "EXISTS subquery without WHERE — no correlation predicate to extract".to_string(),
            )
        })?;
        let parts = split_and(&inner_where);

        let mut inner_only_parts: Vec<ast::Expr> = Vec::new();
        let mut residual_parts: Vec<ast::Expr> = Vec::new();
        let mut correlation: Option<(ast::Expr, ast::Expr)> = None;
        for part in parts {
            if let Some(pair) = try_match_correlation(&part, outer_ctx, &inner_ctx) {
                if correlation.is_some() {
                    return Err(PlanError::invalid_expression(
                        "EXISTS subquery with multiple correlation predicates not yet supported"
                            .to_string(),
                    ));
                }
                correlation = Some(pair);
            } else if references_outer(&part, &inner_ctx) {
                // Mixed outer-and-inner predicate that isn't an
                // equi-correlation. Carry as SemiJoin residual; it
                // gets evaluated on each (outer_row, inner_row) match
                // candidate at execution time. Covers TPC-H Q21's
                // `l2.l_suppkey <> l1.l_suppkey`.
                residual_parts.push(part);
            } else {
                inner_only_parts.push(part);
            }
        }

        let (outer_key_ast, inner_key_ast) = correlation.ok_or_else(|| {
            PlanError::invalid_expression(
                "EXISTS subquery has no equi-correlation predicate (`outer.col = inner.col`)"
                    .to_string(),
            )
        })?;

        let left_key = self.plan_expr(&outer_key_ast, outer_ctx).await?;
        let right_key = self.plan_expr(&inner_key_ast, &inner_ctx).await?;

        let mut final_inner = inner_plan;
        if !inner_only_parts.is_empty() {
            let combined = combine_and(inner_only_parts).expect("non-empty");
            let pred = self.plan_expr(&combined, &inner_ctx).await?;
            final_inner = LogicalPlan::Filter {
                input: Box::new(final_inner),
                predicate: pred,
            };
        }

        // Plan the residual against a combined ctx where outer columns
        // sit at indices 0..outer_width and inner columns shift up to
        // start at outer_width. This matches the joined-batch column
        // layout that `SemiJoinExec` materializes at runtime.
        let residual = if residual_parts.is_empty() {
            None
        } else {
            let outer_width = outer_ctx.columns.len();
            let mut combined_ctx = PlanningContext::new();
            for (q, c) in &outer_ctx.columns {
                combined_ctx.columns.push((q.clone(), c.clone()));
            }
            for (q, c) in &inner_ctx.columns {
                combined_ctx.columns.push((q.clone(), c.clone()));
            }
            let _ = outer_width; // documents the layout; used implicitly via combined_ctx ordering
            let combined = combine_and(residual_parts).expect("non-empty");
            Some(self.plan_expr(&combined, &combined_ctx).await?)
        };

        Ok(SemiJoinSpec {
            kind: if negated {
                SemiJoinKind::Anti
            } else {
                SemiJoinKind::Semi
            },
            right_plan: final_inner,
            left_key,
            right_key,
            residual,
        })
    }

    /// Plan the FROM clause: resolve tables, build join tree.
    async fn plan_from(
        &self,
        from: &[ast::TableWithJoins],
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        if from.is_empty() {
            // `SELECT <expr>, ...` with no FROM: emit a synthetic one-row
            // source so the surrounding Projection can evaluate literal
            // / constant expressions against it. Enables `SELECT 1`,
            // `SELECT 1 + 1`, health checks (`pg_isready`-style probes),
            // and PostgreSQL-compatible introspection queries.
            return Ok((LogicalPlan::OneRow, PlanningContext::new()));
        }

        let (mut plan, mut ctx) = self.plan_table_with_joins(&from[0]).await?;

        // Multiple FROM items → implicit CROSS JOIN
        for twj in &from[1..] {
            let (right_plan, right_ctx) = self
                .plan_table_with_joins(twj)
                .await
                .map_err(|e| select_subquery::lateral_reference_error(e, &ctx))?;
            ctx.columns.extend(right_ctx.columns);
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: ast::JoinType::Cross,
                condition: JoinCondition::None,
                dynamic_filter_ids: Vec::new(),
            };
        }

        Ok((plan, ctx))
    }

    /// Plan a single FROM item with its joins.
    async fn plan_table_with_joins(
        &self,
        twj: &ast::TableWithJoins,
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        let (mut plan, mut ctx) = self.plan_table_factor(&twj.relation).await?;

        for join in &twj.joins {
            let (right_plan, right_ctx) = self
                .plan_table_factor(&join.relation)
                .await
                .map_err(|e| select_subquery::lateral_reference_error(e, &ctx))?;
            let left_len = ctx.columns.len();
            let using = match &join.condition {
                ast::JoinCondition::Using(columns) => {
                    using_pairs(&ctx, &right_ctx, columns, join.join_type)?
                }
                _ => Vec::new(),
            };
            ctx.merged
                .extend(right_ctx.merged.iter().map(|i| i + left_len));
            ctx.columns.extend(right_ctx.columns);

            let condition = match &join.condition {
                ast::JoinCondition::On(expr) => {
                    let plan_expr = self.plan_expr(expr, &ctx).await?;
                    JoinCondition::On(plan_expr)
                }
                ast::JoinCondition::Using(_) => {
                    // `USING (a, b)` = `ON l.a = r.a AND l.b = r.b`.
                    ctx.merged
                        .extend(using.iter().map(|&(_, right)| right + left_len));
                    let column = |index: usize| {
                        Box::new(PlanExpr::Column {
                            index,
                            name: ctx.columns[index].1.name.clone(),
                            span: None,
                        })
                    };
                    let on = using
                        .iter()
                        .map(|&(left, right)| PlanExpr::BinaryOp {
                            left: column(left),
                            op: ast::BinaryOp::Eq,
                            right: column(right + left_len),
                            span: None,
                        })
                        .reduce(|acc, eq| PlanExpr::BinaryOp {
                            left: Box::new(acc),
                            op: ast::BinaryOp::And,
                            right: Box::new(eq),
                            span: None,
                        })
                        .ok_or_else(|| {
                            PlanError::invalid_expression(
                                "USING needs at least one column".to_string(),
                            )
                        })?;
                    JoinCondition::On(on)
                }
                ast::JoinCondition::None => JoinCondition::None,
            };

            let (left_cols, right_cols) = ctx.columns.split_at_mut(left_len);
            null_extend(
                join.join_type,
                left_cols.iter_mut().map(|(_, col)| col),
                right_cols.iter_mut().map(|(_, col)| col),
            );

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: join.join_type,
                condition,
                dynamic_filter_ids: Vec::new(),
            };
        }

        Ok((plan, ctx))
    }

    /// Resolve a single table factor (table name or subquery).
    async fn plan_table_factor(
        &self,
        factor: &ast::TableFactor,
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        match factor {
            ast::TableFactor::Table {
                name,
                alias,
                column_aliases,
            } => {
                // Single-part names may resolve to a CTE before falling
                // back to the catalog. When a CTE is referenced more than
                // once in the same scope the plan is cloned (re-executed);
                // we accept the duplicated work in v1 — TPC-H Q15's
                // revenue0 CTE is small enough that it doesn't matter.
                if name.catalog.is_none() && name.schema.is_none() {
                    let cte_plan = self.cte_plans.lock().unwrap().get(&name.table).cloned();
                    if let Some(plan) = cte_plan {
                        let plan = apply_table_column_aliases(
                            plan,
                            column_aliases,
                            alias.as_deref().unwrap_or(&name.table),
                        )?;
                        let schema = plan.schema();
                        let qualifier = alias.as_deref().unwrap_or(&name.table);
                        let mut ctx = PlanningContext::new();
                        ctx.add_table_columns(Some(qualifier), &schema);
                        return Ok((plan, ctx));
                    }
                }

                let to_plan_error =
                    |e: CatalogError, name: &arneb_common::types::TableReference| match e {
                        CatalogError::AccessDenied(denial) => PlanError::AccessDenied(denial),
                        // An outage is neither a missing table nor an
                        // engine bug.
                        CatalogError::Unavailable(why) => PlanError::SourceUnavailable(why),
                        _ => PlanError::TableNotFound(name.to_string()),
                    };
                let name = &self
                    .catalog
                    .fold_reference_case(name)
                    .await
                    .map_err(|e| to_plan_error(e, name))?;
                let table_provider = self
                    .catalog
                    .resolve_table(name)
                    .await
                    .map_err(|e| to_plan_error(e, name))?;
                let schema = table_provider.schema();
                let qualifier = alias.as_deref().unwrap_or(&name.table);

                let properties = table_provider.properties();
                // Identity on the server's root catalog manager; on a
                // per-session view (Trino `X-Trino-Catalog`/`-Schema`) the
                // reference is fully qualified so connector lookup and
                // workers resolve the table the session meant.
                let plan = LogicalPlan::TableScan {
                    table: self.catalog.qualify_table_reference(name),
                    schema,
                    alias: alias.clone(),
                    properties,
                    dynamic_filters_consumed: Vec::new(),
                };

                let plan = apply_table_column_aliases(plan, column_aliases, qualifier)?;
                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(qualifier), &plan.schema());

                Ok((plan, ctx))
            }
            ast::TableFactor::Subquery {
                query,
                alias,
                column_aliases,
            } => {
                let plan = self.plan_query(query).await?;
                let plan = apply_table_column_aliases(plan, column_aliases, alias)?;
                let schema = plan.schema();
                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(alias.as_str()), &schema);
                Ok((plan, ctx))
            }
            ast::TableFactor::NestedJoin {
                relation,
                alias,
                column_aliases,
            } => {
                let (mut plan, mut ctx) = Box::pin(self.plan_table_with_joins(relation)).await?;
                if let Some(alias) = alias {
                    for (index, (qualifier, _)) in ctx.columns.iter_mut().enumerate() {
                        *qualifier = (!ctx.merged.contains(&index)).then(|| alias.clone());
                    }
                }
                if !column_aliases.is_empty() {
                    let schema = plan.schema();
                    let visible: Vec<usize> = (0..schema.len())
                        .filter(|index| !ctx.merged.contains(index))
                        .collect();
                    if column_aliases.len() != visible.len() {
                        return Err(PlanError::invalid_expression(format!(
                            "nested join alias declares {} columns but produces {} columns",
                            column_aliases.len(),
                            visible.len()
                        )));
                    }
                    let exprs = visible
                        .iter()
                        .map(|&index| PlanExpr::Column {
                            index,
                            name: schema[index].name.clone(),
                            span: None,
                        })
                        .collect();
                    let renamed_schema: Vec<ColumnInfo> = visible
                        .iter()
                        .zip(column_aliases)
                        .map(|(&index, alias)| {
                            let mut column = schema[index].clone();
                            column.name = alias.clone();
                            column
                        })
                        .collect();
                    ctx = PlanningContext::new();
                    ctx.add_table_columns(alias.as_deref(), &renamed_schema);
                    plan = LogicalPlan::Projection {
                        input: Box::new(plan),
                        exprs,
                        schema: renamed_schema,
                    };
                }
                Ok((plan, ctx))
            }
        }
    }

    /// Convert an AST expression to a PlanExpr, resolving column references.
    /// Plans an aggregate call's `ORDER BY`. Kept out of `plan_expr` so the
    /// recursive future (and with it the stack depth) stays small.
    #[async_recursion]
    async fn plan_call_order_by(
        &self,
        order_by: &[ast::OrderByExpr],
        ctx: &PlanningContext,
    ) -> Result<Vec<SortExpr>, PlanError> {
        let mut out = Vec::with_capacity(order_by.len());
        for ob in order_by {
            let asc = ob.asc.unwrap_or(true);
            out.push(SortExpr {
                expr: self.plan_expr(&ob.expr, ctx).await?,
                asc,
                // PostgreSQL: NULLs sort as larger than any value, so they
                // come last for ASC and first for DESC unless stated.
                nulls_first: ob.nulls_first.unwrap_or(!asc),
            });
        }
        Ok(out)
    }

    #[allow(clippy::only_used_in_recursion)]
    #[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
    #[async_recursion]
    async fn plan_expr(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
    ) -> Result<PlanExpr, PlanError> {
        let node_span = Some(expr.span());
        match expr {
            ast::Expr::Column { col_ref, .. } => {
                let resolved = ctx.resolve_column(
                    &col_ref.name,
                    col_ref.table.as_deref(),
                    Some(col_ref.span.start),
                );
                // The parser reads a bare `current_schema` as a column.
                if resolved.is_err() && col_ref.table.is_none() {
                    if let Some(literal) =
                        session_literal(self.catalog, &col_ref.name, false, node_span)
                    {
                        return Ok(literal);
                    }
                }
                let (index, col_info) = resolved?;
                Ok(PlanExpr::Column {
                    index,
                    name: col_info.name,
                    span: node_span,
                })
            }
            ast::Expr::Literal { value, .. } => Ok(PlanExpr::Literal {
                value: value.clone(),
                span: node_span,
            }),
            ast::Expr::BinaryOp {
                left, op, right, ..
            } => {
                // `x ± INTERVAL` and `INTERVAL + x`: lowered by `x`'s type.
                let interval = match (as_interval(left), op, as_interval(right)) {
                    (None, ast::BinaryOp::Plus | ast::BinaryOp::Minus, Some((value, unit, span))) => {
                        Some((left.as_ref(), value, unit, span, false))
                    }
                    (Some((value, unit, span)), ast::BinaryOp::Plus, None) => {
                        Some((right.as_ref(), value, unit, span, true))
                    }
                    _ => None,
                };
                if let Some((operand, value, unit, interval_span, interval_first)) = interval {
                    let operand = self.plan_expr(operand, ctx).await?;
                    return plan_interval_arithmetic(
                        operand,
                        IntervalOperand {
                            op: *op,
                            value,
                            unit,
                            span: interval_span,
                            first: interval_first,
                        },
                        ctx,
                        node_span,
                    );
                }
                Ok(PlanExpr::BinaryOp {
                    left: Box::new(self.plan_expr(left, ctx).await?),
                    op: *op,
                    right: Box::new(self.plan_expr(right, ctx).await?),
                    span: node_span,
                })
            }
            ast::Expr::UnaryOp { op, expr, .. } => Ok(PlanExpr::UnaryOp {
                op: *op,
                expr: Box::new(self.plan_expr(expr, ctx).await?),
                span: node_span,
            }),
            ast::Expr::Function {
                name,
                args,
                distinct,
                order_by,
                ..
            } => {
                if SrfKind::from_name(name).is_some() {
                    return Err(srf_not_supported_here(name, node_span));
                }
                if !order_by.is_empty() {
                    check_aggregate_order_by(name)?;
                }
                if *distinct && name.eq_ignore_ascii_case("ARRAY_AGG") {
                    return Err(PlanError::invalid_expression(
                        "DISTINCT is not supported in array_agg",
                    ));
                }
                if name.eq_ignore_ascii_case("GROUPING") {
                    return self.plan_grouping(args, ctx, node_span).await;
                }
                if args.is_empty() {
                    if let Some(literal) = session_literal(self.catalog, name, true, node_span) {
                        return Ok(literal);
                    }
                }
                if name.eq_ignore_ascii_case("regclass") {
                    return Box::pin(self.plan_regclass(args, ctx, node_span)).await;
                }
                if name.eq_ignore_ascii_case("json_build_object") && !args.len().is_multiple_of(2) {
                    return Err(odd_json_build_object_args(node_span));
                }
                if name.eq_ignore_ascii_case("regtype") {
                    return match args.as_slice() {
                        // `'int4'::regtype`: the type's oid.
                        [ast::FunctionArg::Unnamed(ast::Expr::Literal {
                            value: ScalarValue::Utf8(type_name),
                            ..
                        })] => arneb_common::pg_oid::type_oid(type_name)
                            .map(|oid| PlanExpr::Literal {
                                value: ScalarValue::Int64(oid),
                                span: node_span,
                            })
                            .ok_or_else(|| PlanError::InvalidLiteral {
                                message: format!("type \"{type_name}\" does not exist"),
                                location: node_span.map(|s| s.start),
                            }),
                        // `typoid::regtype`: shown as the type's name.
                        [ast::FunctionArg::Unnamed(e)] => Ok(PlanExpr::Function {
                            order_by: None,
                            name: "format_type".to_string(),
                            args: vec![
                                self.plan_expr(e, ctx).await?,
                                PlanExpr::Literal {
                                    value: ScalarValue::Null,
                                    span: node_span,
                                },
                            ],
                            distinct: false,
                            span: node_span,
                        }),
                        _ => Err(PlanError::UnsupportedExpression {
                            message: "regtype takes one argument".to_string(),
                            location: node_span.map(|s| s.start),
                        }),
                    };
                }
                // `TRY_CAST('x' AS regclass)` is `TRY(regclass('x'))`, which is
                // resolved here at plan time, so a name it cannot parse is
                // NULL now rather than an error TRY never sees.
                if name.eq_ignore_ascii_case("TRY") {
                    if let [ast::FunctionArg::Unnamed(
                        inner @ ast::Expr::Function {
                            name: inner_name, ..
                        },
                    )] = args.as_slice()
                    {
                        if inner_name.eq_ignore_ascii_case("regclass")
                            || inner_name.eq_ignore_ascii_case("regtype")
                        {
                            return match self.plan_expr(inner, ctx).await {
                                // Both resolve to an oid (BIGINT).
                                Err(PlanError::InvalidLiteral { .. }) => Ok(PlanExpr::Cast {
                                    expr: Box::new(PlanExpr::Literal {
                                        value: ScalarValue::Null,
                                        span: node_span,
                                    }),
                                    data_type: DataType::Int64,
                                    span: node_span,
                                }),
                                other => other,
                            };
                        }
                    }
                }
                let mut plan_args = Vec::with_capacity(args.len());
                for a in args {
                    let plan_arg = match a {
                        ast::FunctionArg::Unnamed(e) => self.plan_expr(e, ctx).await?,
                        ast::FunctionArg::Wildcard => PlanExpr::Wildcard,
                    };
                    plan_args.push(plan_arg);
                }
                if name.eq_ignore_ascii_case("current_schemas") {
                    if plan_args.len() != 1 {
                        return Err(current_schemas_arity_error(plan_args.len(), node_span));
                    }
                    // The session's search path, as `current_schema` reads it.
                    plan_args.push(PlanExpr::Literal {
                        value: ScalarValue::Utf8(self.catalog.default_schema().to_string()),
                        span: None,
                    });
                }
                if let Some(kind) = privilege_object_kind(name) {
                    return Box::pin(self.plan_has_privilege(kind, name, plan_args, node_span))
                        .await;
                }
                let plan_order_by = if order_by.is_empty() {
                    None
                } else {
                    Some(Box::new(self.plan_call_order_by(order_by, ctx).await?))
                };
                if is_percentile(name) {
                    let schema: Vec<ColumnInfo> =
                        ctx.columns.iter().map(|(_, col)| col.clone()).collect();
                    check_percentile_call(name, &plan_args, &schema, node_span)?;
                }
                Ok(PlanExpr::Function {
                    name: name.clone(),
                    args: plan_args,
                    distinct: *distinct,
                    order_by: plan_order_by,
                    span: node_span,
                })
            }
            ast::Expr::IsNull { expr: inner, .. } => Ok(PlanExpr::IsNull {
                expr: Box::new(self.plan_expr(inner, ctx).await?),
                span: node_span,
            }),
            ast::Expr::IsNotNull { expr: inner, .. } => Ok(PlanExpr::IsNotNull {
                expr: Box::new(self.plan_expr(inner, ctx).await?),
                span: node_span,
            }),
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
                ..
            } => Ok(PlanExpr::Between {
                expr: Box::new(self.plan_expr(expr, ctx).await?),
                negated: *negated,
                low: Box::new(self.plan_expr(low, ctx).await?),
                high: Box::new(self.plan_expr(high, ctx).await?),
                span: node_span,
            }),
            ast::Expr::InList {
                expr,
                list,
                negated,
                ..
            } => {
                let mut plan_list = Vec::with_capacity(list.len());
                for e in list {
                    plan_list.push(self.plan_expr(e, ctx).await?);
                }
                Ok(PlanExpr::InList {
                    expr: Box::new(self.plan_expr(expr, ctx).await?),
                    list: plan_list,
                    negated: *negated,
                    span: node_span,
                })
            }
            ast::Expr::Cast {
                expr, data_type, ..
            } => {
                if is_regclass_to_text(expr, data_type) {
                    return Box::pin(self.plan_regclass_name(expr, ctx, node_span)).await;
                }
                Ok(PlanExpr::Cast {
                    expr: Box::new(self.plan_expr(expr, ctx).await?),
                    data_type: data_type.clone(),
                    span: node_span,
                })
            }
            ast::Expr::Nested { expr: inner, .. } => self.plan_expr(inner, ctx).await,
            ast::Expr::Subquery { query, .. } => {
                let subplan = self
                    .plan_query(query)
                    .await
                    .map_err(|e| select_subquery::outer_reference_error(e, ctx))?;
                Ok(PlanExpr::ScalarSubquery {
                    subplan: Box::new(subplan),
                    span: node_span,
                })
            }
            ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                let op = match operand {
                    Some(expr) => Some(Box::new(self.plan_expr(expr, ctx).await?)),
                    None => None,
                };
                let mut when_clauses = Vec::with_capacity(conditions.len());
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    when_clauses.push((self.plan_expr(cond, ctx).await?, self.plan_expr(res, ctx).await?));
                }
                let el = match else_result {
                    Some(expr) => Some(Box::new(self.plan_expr(expr, ctx).await?)),
                    None => None,
                };
                Ok(PlanExpr::CaseExpr {
                    operand: op,
                    when_clauses,
                    else_result: el,
                    span: node_span,
                })
            }
            ast::Expr::InSubquery { .. } | ast::Expr::Exists { .. } => {
                Err(PlanError::invalid_expression(
                    "IN/EXISTS subquery expressions are handled at the plan level, not in plan_expr"
                        .to_string(),
                ))
            }
            ast::Expr::WindowFunction { .. } => {
                // Computed by a Window node below the projection
                // (`plan_windows`), which exposes each call as a column
                // named by the call's display string.
                let (index, col_info) = ctx
                    .resolve_column(&expr.to_string(), None, None)
                    .map_err(|_| PlanError::UnsupportedExpression {
                        message: format!(
                            "window function {expr} is only supported in the SELECT list \
                             (and in ORDER BY when it also appears in the SELECT list)"
                        ),
                        location: None,
                    })?;
                Ok(PlanExpr::Column {
                    index,
                    name: col_info.name,
                    span: node_span,
                })
            }
            ast::Expr::Array { elements, .. } => {
                if elements.is_empty() {
                    return Err(PlanError::invalid_expression(
                        "cannot determine the type of an empty ARRAY[]; cast it, e.g. \
                         ARRAY[]::integer[]"
                            .to_string(),
                    ));
                }
                let mut planned = Vec::with_capacity(elements.len());
                for e in elements {
                    planned.push(self.plan_expr(e, ctx).await?);
                }
                Ok(PlanExpr::Array {
                    elements: planned,
                    // Filled in by the type-coercion pass.
                    element_type: DataType::Null,
                    span: node_span,
                })
            }
            ast::Expr::AnyAll {
                expr, op, array, ..
            } => Box::pin(self.plan_any_all(expr, *op, array, node_span, ctx)).await,
            ast::Expr::Parameter { index, .. } => Ok(PlanExpr::Parameter {
                index: *index,
                type_hint: None,
                span: node_span,
            }),
            ast::Expr::Interval { .. } => Err(interval_outside_arithmetic(node_span)),
            ast::Expr::Subscript { expr, index, .. } => {
                Box::pin(self.plan_subscript(expr, index, ctx)).await
            }
            ast::Expr::FieldAccess { expr, field, .. } => {
                Box::pin(self.plan_field_access(expr, field, ctx)).await
            }
        }
    }

    /// Build projection expressions and output schema from SELECT items.
    async fn plan_projection(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
    ) -> Result<(Vec<PlanExpr>, Vec<ColumnInfo>), PlanError> {
        let mut exprs = Vec::new();
        let mut schema = Vec::new();

        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let plan_expr = self.plan_expr(expr, ctx).await?;
                    let mut col_info = self.expr_to_column_info(&plan_expr, ctx);
                    if matches!(plan_expr, PlanExpr::Literal { .. }) {
                        if let Some(name) = session_function_name(expr) {
                            col_info.name = name;
                        }
                    }
                    exprs.push(plan_expr);
                    schema.push(col_info);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let plan_expr = self.plan_expr(expr, ctx).await?;
                    let mut col_info = self.expr_to_column_info(&plan_expr, ctx);
                    col_info.name = alias.clone();
                    exprs.push(plan_expr);
                    schema.push(col_info);
                }
                ast::SelectItem::Wildcard => {
                    // Expand * to all columns (not the window results)
                    for (i, (q, col)) in ctx.columns.iter().enumerate() {
                        if q.as_deref() == Some(WINDOW_QUALIFIER)
                            || q.as_deref() == Some(SRF_QUALIFIER)
                            || q.as_deref() == Some(SELECT_SUBQUERY_QUALIFIER)
                            || ctx.merged.contains(&i)
                        {
                            continue;
                        }
                        exprs.push(PlanExpr::Column {
                            index: i,
                            name: col.name.clone(),
                            span: None,
                        });
                        schema.push(col.clone());
                    }
                }
                ast::SelectItem::QualifiedWildcard(table_ref) => {
                    let qualifier = &table_ref.table;
                    let qualified_cols = ctx.columns_for_qualifier(qualifier);
                    if qualified_cols.is_empty() {
                        return Err(PlanError::TableNotFound(qualifier.clone()));
                    }
                    for (i, col) in qualified_cols {
                        exprs.push(PlanExpr::Column {
                            index: i,
                            name: col.name.clone(),
                            span: None,
                        });
                        schema.push(col);
                    }
                }
            }
        }

        Ok((exprs, schema))
    }

    /// Build projection for a SELECT with aggregation.
    ///
    /// After GROUP BY, the context only has group-by columns + aggregate outputs.
    /// Expressions in SELECT that are aggregate functions must be mapped to the
    /// aggregate output column by index, not re-resolved.
    async fn plan_aggregate_projection(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
        group_by_exprs: &[ast::Expr],
    ) -> Result<(Vec<PlanExpr>, Vec<ColumnInfo>), PlanError> {
        let num_group_by = group_by_exprs.len();
        let mut exprs = Vec::new();
        let mut schema = Vec::new();

        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    let alias = match item {
                        ast::SelectItem::ExprWithAlias { alias, .. } => Some(alias.clone()),
                        _ => None,
                    };

                    // Check if this expression is an aggregate function — match by display string
                    if let Some(agg_idx) = self.find_aggregate_index(expr, ctx, num_group_by) {
                        let col_info = ctx.columns.get(agg_idx).map(|(_, c)| c.clone());
                        let mut ci = col_info.unwrap_or(ColumnInfo {
                            name: format!("{expr}"),
                            data_type: DataType::Null,
                            nullable: true,
                        });
                        if let Some(a) = alias {
                            ci.name = a;
                        }
                        exprs.push(PlanExpr::Column {
                            index: agg_idx,
                            name: ci.name.clone(),
                            span: None,
                        });
                        schema.push(ci);
                    } else if self.is_group_by_expr(expr, group_by_exprs) {
                        // It's a group-by column — resolve in post-aggregate ctx
                        // Try normal resolution first; if that fails (qualified ref vs
                        // unqualified ctx), try by column name only
                        let plan_expr = match self.plan_expr(expr, ctx).await {
                            Ok(pe) => pe,
                            Err(_) => {
                                // Try matching by unqualified column name first.
                                if let ast::Expr::Column { col_ref, .. } = expr {
                                    let col_name = &col_ref.name;
                                    let mut found = None;
                                    for (i, (_, c)) in ctx.columns.iter().enumerate() {
                                        if c.name.eq_ignore_ascii_case(col_name) {
                                            found = Some(PlanExpr::Column {
                                                index: i,
                                                name: c.name.clone(),
                                                span: None,
                                            });
                                            break;
                                        }
                                    }
                                    found.ok_or_else(|| {
                                        PlanError::column_not_found(col_name.clone())
                                    })?
                                } else {
                                    // Non-Column group-by expr (e.g.
                                    // `EXTRACT(YEAR FROM l_shipdate)`, a CASE
                                    // expression, an arithmetic expression).
                                    // The group-by columns are laid out at
                                    // indices [0..group_by_exprs.len()) in the
                                    // post-aggregate ctx, in the order they
                                    // appear in the GROUP BY clause. Find this
                                    // expr's position there by structural
                                    // equality (same Display).
                                    let expr_str = format!("{expr}");
                                    let position = group_by_exprs
                                        .iter()
                                        .position(|gb| format!("{gb}") == expr_str)
                                        .ok_or_else(|| {
                                            PlanError::invalid_expression(format!(
                                                "cannot resolve group-by expr: {expr}"
                                            ))
                                        })?;
                                    let col = ctx
                                        .columns
                                        .get(position)
                                        .map(|(_, c)| c.clone())
                                        .unwrap_or(ColumnInfo {
                                            name: expr_str.clone(),
                                            data_type: DataType::Null,
                                            nullable: true,
                                        });
                                    PlanExpr::Column {
                                        index: position,
                                        name: col.name,
                                        span: None,
                                    }
                                }
                            }
                        };
                        let mut ci = self.expr_to_column_info(&plan_expr, ctx);
                        if let Some(a) = alias {
                            ci.name = a;
                        }
                        exprs.push(plan_expr);
                        schema.push(ci);
                    } else if self.contains_aggregate(expr) {
                        // Expression containing aggregate (e.g., 100 * SUM(x) / SUM(y))
                        // Replace aggregate sub-expressions with column refs, then plan the rest
                        let rewritten = self.rewrite_aggregates_as_columns(expr, ctx, num_group_by);
                        let plan_expr = self.plan_expr(&rewritten, ctx).await?;
                        let mut ci = self.expr_to_column_info(&plan_expr, ctx);
                        if let Some(a) = alias.clone() {
                            ci.name = a;
                        }
                        exprs.push(plan_expr);
                        schema.push(ci);
                    } else {
                        // Non-aggregate, non-group-by expression — resolve normally
                        let plan_expr = self.plan_expr(expr, ctx).await?;
                        let mut ci = self.expr_to_column_info(&plan_expr, ctx);
                        if let Some(a) = alias {
                            ci.name = a;
                        }
                        exprs.push(plan_expr);
                        schema.push(ci);
                    }
                }
                ast::SelectItem::Wildcard => {
                    for (i, (q, col)) in ctx.columns.iter().enumerate() {
                        if q.as_deref() == Some(WINDOW_QUALIFIER)
                            || q.as_deref() == Some(SRF_QUALIFIER)
                            || q.as_deref() == Some(SELECT_SUBQUERY_QUALIFIER)
                            || ctx.merged.contains(&i)
                        {
                            continue;
                        }
                        exprs.push(PlanExpr::Column {
                            index: i,
                            name: col.name.clone(),
                            span: None,
                        });
                        schema.push(col.clone());
                    }
                }
                ast::SelectItem::QualifiedWildcard(table_ref) => {
                    let qualifier = &table_ref.table;
                    let qualified_cols = ctx.columns_for_qualifier(qualifier);
                    if qualified_cols.is_empty() {
                        return Err(PlanError::TableNotFound(qualifier.clone()));
                    }
                    for (i, col) in qualified_cols {
                        exprs.push(PlanExpr::Column {
                            index: i,
                            name: col.name.clone(),
                            span: None,
                        });
                        schema.push(col);
                    }
                }
            }
        }

        Ok((exprs, schema))
    }

    /// Find the index in the post-aggregate context for an aggregate expression.
    ///
    /// Matches by display string, but strips any column qualifiers from
    /// the AST first — the stored aggregate column names come from
    /// `PlanExpr` Display which is always unqualified, so the AST's
    /// `SUM(t.col)` must normalize to `SUM(col)` for the comparison to
    /// work. Without this normalization, two aggregates sharing a
    /// function name (e.g. `SUM(CASE..)` and `SUM(t.col)`) would both
    /// collapse onto the first slot once the historical name-prefix
    /// fallback kicked in.
    fn find_aggregate_index(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        num_group_by: usize,
    ) -> Option<usize> {
        if let ast::Expr::Function { name, .. } = expr {
            if is_aggregate_function(name) {
                let expr_str = format_ast_unqualified(expr);
                for (i, (_, col)) in ctx.columns.iter().enumerate().skip(num_group_by) {
                    if col.name == expr_str || col.name.eq_ignore_ascii_case(&expr_str) {
                        return Some(i);
                    }
                }
                // Last-ditch: exactly one aggregate slot — no ambiguity,
                // safe to assume it is the target.
                let agg_count = ctx
                    .columns
                    .iter()
                    .skip(num_group_by)
                    .filter(|(q, _)| q.as_deref() != Some(WINDOW_QUALIFIER))
                    .count();
                if agg_count == 1 {
                    return Some(num_group_by);
                }
            }
        }
        None
    }

    /// Check if an expression matches any group-by expression.
    fn is_group_by_expr(&self, expr: &ast::Expr, group_by_exprs: &[ast::Expr]) -> bool {
        let s = format!("{expr}");
        if group_by_exprs.iter().any(|gb| format!("{gb}") == s) {
            return true;
        }
        // For qualified column refs (e.g., n1.n_name), check if unqualified name
        // matches a group-by expression
        if let ast::Expr::Column { col_ref: col, .. } = expr {
            let unqualified = &col.name;
            return group_by_exprs.iter().any(|gb| {
                if let ast::Expr::Column {
                    col_ref: gb_col, ..
                } = gb
                {
                    gb_col.name.eq_ignore_ascii_case(unqualified)
                } else {
                    false
                }
            });
        }
        false
    }

    /// `array[index]`. The operand and index types are checked by the
    /// type-coercion pass. Out of line so the recursive `plan_expr` future
    /// stays small.
    async fn plan_subscript(
        &self,
        array: &ast::Expr,
        index: &ast::Expr,
        ctx: &PlanningContext,
    ) -> Result<PlanExpr, PlanError> {
        let array = self.plan_expr(array, ctx).await?;
        let index = self.plan_expr(index, ctx).await?;
        Ok(PlanExpr::ArrayElement(Box::new((array, index))))
    }

    /// `(expr).field`: an unknown field of a typed struct operand is a
    /// plan-time error. Out of line so the recursive `plan_expr` future
    /// stays small.
    async fn plan_field_access(
        &self,
        expr: &ast::Expr,
        field: &str,
        ctx: &PlanningContext,
    ) -> Result<PlanExpr, PlanError> {
        let planned = self.plan_expr(expr, ctx).await?;
        let schema: Vec<ColumnInfo> = ctx.columns.iter().map(|(_, c)| c.clone()).collect();
        crate::analyzer::check_get_field(plan_expr_type(&planned, &schema).as_ref(), field)?;
        Ok(PlanExpr::GetField(Box::new((planned, field.to_string()))))
    }

    /// `expr = ANY (array)` / `expr <> ALL (array)` over an array-valued
    /// expression. Out of line so the recursive `plan_expr` future stays small.
    async fn plan_any_all(
        &self,
        expr: &ast::Expr,
        op: ast::AnyAllOp,
        array: &ast::Expr,
        span: Option<arneb_sql_parser::Span>,
        ctx: &PlanningContext,
    ) -> Result<PlanExpr, PlanError> {
        Ok(PlanExpr::ArrayContains {
            expr: Box::new(self.plan_expr(expr, ctx).await?),
            array: Box::new(self.plan_expr(array, ctx).await?),
            negated: matches!(op, ast::AnyAllOp::NotEqAll),
            span,
        })
    }

    /// Check if an expression contains any aggregate function.
    fn contains_aggregate(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function { name, args, .. } => {
                is_aggregate_function(name)
                    || args.iter().any(|arg| match arg {
                        ast::FunctionArg::Unnamed(expr) => self.contains_aggregate(expr),
                        ast::FunctionArg::Wildcard => false,
                    })
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate(left) || self.contains_aggregate(right)
            }
            ast::Expr::UnaryOp { expr, .. }
            | ast::Expr::IsNull { expr, .. }
            | ast::Expr::IsNotNull { expr, .. }
            | ast::Expr::Cast { expr, .. }
            | ast::Expr::Nested { expr, .. } => self.contains_aggregate(expr),
            ast::Expr::Between {
                expr, low, high, ..
            } => {
                self.contains_aggregate(expr)
                    || self.contains_aggregate(low)
                    || self.contains_aggregate(high)
            }
            ast::Expr::InList { expr, list, .. } => {
                self.contains_aggregate(expr) || list.iter().any(|e| self.contains_aggregate(e))
            }
            ast::Expr::Array { elements, .. } => {
                elements.iter().any(|e| self.contains_aggregate(e))
            }
            ast::Expr::AnyAll { expr, array, .. } => {
                self.contains_aggregate(expr) || self.contains_aggregate(array)
            }
            ast::Expr::Subscript { expr, index, .. } => {
                self.contains_aggregate(expr) || self.contains_aggregate(index)
            }
            ast::Expr::FieldAccess { expr, .. } => self.contains_aggregate(expr),
            ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => operand
                .iter()
                .chain(else_result.iter())
                .map(|e| &**e)
                .chain(conditions)
                .chain(results)
                .any(|e| self.contains_aggregate(e)),
            _ => false,
        }
    }

    /// Try to resolve an ORDER BY expression that references an aggregate or alias.
    /// Matches by display string against context column names.
    /// Resolve ORDER BY expression, considering SELECT aliases.
    fn resolve_order_by_expr_with_select(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        select_items: Option<&Vec<ast::SelectItem>>,
    ) -> Option<PlanExpr> {
        // First try normal resolution
        if let Some(resolved) = self.resolve_order_by_expr(expr, ctx) {
            return Some(resolved);
        }
        // Then check if the expression matches a SELECT item's pre-alias expression
        if let Some(items) = select_items {
            let expr_str = format!("{expr}");
            for (idx, item) in items.iter().enumerate() {
                if let ast::SelectItem::ExprWithAlias {
                    expr: select_expr, ..
                } = item
                {
                    let select_expr_str = format!("{select_expr}");
                    if (select_expr_str == expr_str
                        || select_expr_str.eq_ignore_ascii_case(&expr_str))
                        && idx < ctx.columns.len()
                    {
                        return Some(PlanExpr::Column {
                            index: idx,
                            name: ctx.columns[idx].1.name.clone(),
                            span: None,
                        });
                    }
                }
                // Also match by column name for aliased items
                if let (
                    ast::Expr::Column { col_ref, .. },
                    ast::SelectItem::ExprWithAlias {
                        expr:
                            ast::Expr::Column {
                                col_ref: sel_col, ..
                            },
                        ..
                    },
                ) = (expr, item)
                {
                    if sel_col.name.eq_ignore_ascii_case(&col_ref.name) && idx < ctx.columns.len() {
                        return Some(PlanExpr::Column {
                            index: idx,
                            name: ctx.columns[idx].1.name.clone(),
                            span: None,
                        });
                    }
                }
            }
        }
        None
    }

    fn resolve_order_by_expr(&self, expr: &ast::Expr, ctx: &PlanningContext) -> Option<PlanExpr> {
        let expr_str = format!("{expr}");
        // Match by exact display string
        for (i, (_, col)) in ctx.columns.iter().enumerate() {
            if col.name == expr_str || col.name.eq_ignore_ascii_case(&expr_str) {
                return Some(PlanExpr::Column {
                    index: i,
                    name: col.name.clone(),
                    span: None,
                });
            }
        }
        // For column references, also match by unqualified name (handles aliased columns)
        if let ast::Expr::Column { col_ref, .. } = expr {
            for (i, (_, col)) in ctx.columns.iter().enumerate() {
                if col.name.eq_ignore_ascii_case(&col_ref.name) {
                    return Some(PlanExpr::Column {
                        index: i,
                        name: col.name.clone(),
                        span: None,
                    });
                }
            }
        }
        // For aggregate functions, try matching by function name
        if let ast::Expr::Function { name, .. } = expr {
            if is_aggregate_function(name) {
                let name_upper = name.to_uppercase();
                for (i, (_, col)) in ctx.columns.iter().enumerate() {
                    if col.name.to_uppercase().starts_with(&name_upper) {
                        return Some(PlanExpr::Column {
                            index: i,
                            name: col.name.clone(),
                            span: None,
                        });
                    }
                }
            }
        }
        None
    }

    /// Rewrite an expression by replacing aggregate function calls with column references
    /// to the aggregate output. Non-aggregate parts are left as-is.
    fn rewrite_aggregates_as_columns(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        num_group_by: usize,
    ) -> ast::Expr {
        match expr {
            ast::Expr::Function { name, span, .. } if is_aggregate_function(name) => {
                // Replace with a column reference to the aggregate output
                if let Some(idx) = self.find_aggregate_index(expr, ctx, num_group_by) {
                    if let Some((_, col)) = ctx.columns.get(idx) {
                        return ast::Expr::Column {
                            col_ref: ast::ColumnRef {
                                name: col.name.clone(),
                                table: None,
                                span: *span,
                            },
                            span: *span,
                        };
                    }
                }
                expr.clone()
            }
            ast::Expr::BinaryOp {
                left,
                op,
                right,
                span,
            } => ast::Expr::BinaryOp {
                left: Box::new(self.rewrite_aggregates_as_columns(left, ctx, num_group_by)),
                op: *op,
                right: Box::new(self.rewrite_aggregates_as_columns(right, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::UnaryOp {
                op,
                expr: inner,
                span,
            } => ast::Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::Nested { expr: inner, span } => ast::Expr::Nested {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::Cast {
                expr: inner,
                data_type,
                span,
            } => ast::Expr::Cast {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                data_type: data_type.clone(),
                span: *span,
            },
            ast::Expr::IsNull { expr: inner, span } => ast::Expr::IsNull {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::IsNotNull { expr: inner, span } => ast::Expr::IsNotNull {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::Between {
                expr: inner,
                negated,
                low,
                high,
                span,
            } => ast::Expr::Between {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                negated: *negated,
                low: Box::new(self.rewrite_aggregates_as_columns(low, ctx, num_group_by)),
                high: Box::new(self.rewrite_aggregates_as_columns(high, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::InList {
                expr: inner,
                list,
                negated,
                span,
            } => ast::Expr::InList {
                expr: Box::new(self.rewrite_aggregates_as_columns(inner, ctx, num_group_by)),
                list: list
                    .iter()
                    .map(|e| self.rewrite_aggregates_as_columns(e, ctx, num_group_by))
                    .collect(),
                negated: *negated,
                span: *span,
            },
            ast::Expr::Array { elements, span } => ast::Expr::Array {
                elements: elements
                    .iter()
                    .map(|e| self.rewrite_aggregates_as_columns(e, ctx, num_group_by))
                    .collect(),
                span: *span,
            },
            ast::Expr::AnyAll {
                expr,
                op,
                array,
                span,
            } => ast::Expr::AnyAll {
                expr: Box::new(self.rewrite_aggregates_as_columns(expr, ctx, num_group_by)),
                op: *op,
                array: Box::new(self.rewrite_aggregates_as_columns(array, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::Subscript { expr, index, span } => ast::Expr::Subscript {
                expr: Box::new(self.rewrite_aggregates_as_columns(expr, ctx, num_group_by)),
                index: Box::new(self.rewrite_aggregates_as_columns(index, ctx, num_group_by)),
                span: *span,
            },
            ast::Expr::FieldAccess { expr, field, span } => ast::Expr::FieldAccess {
                expr: Box::new(self.rewrite_aggregates_as_columns(expr, ctx, num_group_by)),
                field: field.clone(),
                span: *span,
            },
            ast::Expr::Function {
                name,
                args,
                distinct,
                order_by,
                span,
            } => ast::Expr::Function {
                order_by: order_by
                    .iter()
                    .map(|ob| ast::OrderByExpr {
                        expr: self.rewrite_aggregates_as_columns(&ob.expr, ctx, num_group_by),
                        asc: ob.asc,
                        nulls_first: ob.nulls_first,
                    })
                    .collect(),
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| match arg {
                        ast::FunctionArg::Unnamed(e) => ast::FunctionArg::Unnamed(
                            self.rewrite_aggregates_as_columns(e, ctx, num_group_by),
                        ),
                        ast::FunctionArg::Wildcard => ast::FunctionArg::Wildcard,
                    })
                    .collect(),
                distinct: *distinct,
                span: *span,
            },
            ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                span,
            } => ast::Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(self.rewrite_aggregates_as_columns(e, ctx, num_group_by))),
                conditions: conditions
                    .iter()
                    .map(|e| self.rewrite_aggregates_as_columns(e, ctx, num_group_by))
                    .collect(),
                results: results
                    .iter()
                    .map(|e| self.rewrite_aggregates_as_columns(e, ctx, num_group_by))
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(self.rewrite_aggregates_as_columns(e, ctx, num_group_by))),
                span: *span,
            },
            _ => expr.clone(),
        }
    }

    /// Collect aggregate function expressions from the SELECT list.
    async fn collect_aggregates(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
    ) -> Result<Vec<PlanExpr>, PlanError> {
        let mut aggregates = Vec::new();
        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    self.extract_aggregates(expr, ctx, &mut aggregates).await?;
                }
                _ => {}
            }
        }
        Ok(aggregates)
    }

    /// Recursively extract aggregate functions from an expression.
    #[allow(clippy::double_must_use)] // async_recursion emits a bare #[must_use]
    #[async_recursion]
    async fn extract_aggregates(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        out: &mut Vec<PlanExpr>,
    ) -> Result<(), PlanError> {
        match expr {
            ast::Expr::Function { name, .. } if is_aggregate_function(name) => {
                let plan_expr = self.plan_expr(expr, ctx).await?;
                // Avoid duplicates
                if !out.iter().any(|e| format!("{e}") == format!("{plan_expr}")) {
                    out.push(plan_expr);
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates(left, ctx, out).await?;
                self.extract_aggregates(right, ctx, out).await?;
            }
            ast::Expr::UnaryOp { expr, .. } => {
                self.extract_aggregates(expr, ctx, out).await?;
            }
            ast::Expr::Nested { expr: inner, .. } => {
                self.extract_aggregates(inner, ctx, out).await?;
            }
            ast::Expr::Cast { expr, .. }
            | ast::Expr::IsNull { expr, .. }
            | ast::Expr::IsNotNull { expr, .. } => {
                self.extract_aggregates(expr, ctx, out).await?;
            }
            ast::Expr::Function { args, .. } => {
                for arg in args {
                    if let ast::FunctionArg::Unnamed(e) = arg {
                        self.extract_aggregates(e, ctx, out).await?;
                    }
                }
            }
            ast::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                for e in operand
                    .iter()
                    .chain(else_result.iter())
                    .map(|e| &**e)
                    .chain(conditions)
                    .chain(results)
                {
                    self.extract_aggregates(e, ctx, out).await?;
                }
            }
            ast::Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_aggregates(expr, ctx, out).await?;
                self.extract_aggregates(low, ctx, out).await?;
                self.extract_aggregates(high, ctx, out).await?;
            }
            ast::Expr::InList { expr, list, .. } => {
                self.extract_aggregates(expr, ctx, out).await?;
                for e in list {
                    self.extract_aggregates(e, ctx, out).await?;
                }
            }
            ast::Expr::Array { elements, .. } => {
                for e in elements {
                    self.extract_aggregates(e, ctx, out).await?;
                }
            }
            ast::Expr::AnyAll { expr, array, .. }
            | ast::Expr::Subscript {
                expr, index: array, ..
            } => {
                for e in [&**expr, &**array] {
                    self.extract_aggregates(e, ctx, out).await?;
                }
            }
            ast::Expr::FieldAccess { expr, .. } => {
                self.extract_aggregates(expr, ctx, out).await?;
            }
            // `rank() OVER (ORDER BY sum(x))`: the inner aggregate is
            // computed by the Aggregate node below the Window.
            ast::Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for e in args
                    .iter()
                    .chain(partition_by)
                    .chain(order_by.iter().map(|o| &o.expr))
                {
                    self.extract_aggregates(e, ctx, out).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Derive a ColumnInfo from a PlanExpr (best effort name + type inference).
    fn expr_to_column_info(&self, expr: &PlanExpr, ctx: &PlanningContext) -> ColumnInfo {
        let input_schema: Vec<ColumnInfo> =
            ctx.columns.iter().map(|(_, col)| col.clone()).collect();
        let inferred_type = || plan_expr_type(expr, &input_schema).unwrap_or(DataType::Null);
        match expr {
            PlanExpr::Column { index, name, .. } => {
                if let Some((_, col)) = ctx.columns.get(*index) {
                    ColumnInfo {
                        name: name.clone(),
                        data_type: col.data_type.clone(),
                        nullable: col.nullable,
                    }
                } else {
                    ColumnInfo {
                        name: name.clone(),
                        data_type: DataType::Null,
                        nullable: true,
                    }
                }
            }
            PlanExpr::Literal { value: val, .. } => ColumnInfo {
                name: val.to_string(),
                data_type: inferred_type(),
                nullable: matches!(val, ScalarValue::Null),
            },
            PlanExpr::Function { .. } => {
                // Use full display string as column name to disambiguate (e.g., "SUM(age)" not "SUM")
                let display_name = format!("{expr}");
                ColumnInfo {
                    name: display_name,
                    data_type: inferred_type(),
                    nullable: true,
                }
            }
            PlanExpr::BinaryOp {
                left, op, right, ..
            } => ColumnInfo {
                name: format!("{left} {op} {right}"),
                data_type: inferred_type(),
                nullable: true,
            },
            PlanExpr::Cast {
                data_type, expr, ..
            } => ColumnInfo {
                name: format!("CAST({expr} AS {data_type})"),
                data_type: data_type.clone(),
                nullable: true,
            },
            PlanExpr::CaseExpr { .. } => ColumnInfo {
                name: format!("{expr}"),
                data_type: inferred_type(),
                nullable: true,
            },
            PlanExpr::UnaryOp { .. }
            | PlanExpr::IsNull { .. }
            | PlanExpr::IsNotNull { .. }
            | PlanExpr::Between { .. }
            | PlanExpr::InList { .. }
            | PlanExpr::ArrayContains { .. }
            | PlanExpr::Parameter { .. }
            | PlanExpr::ScalarSubquery { .. } => ColumnInfo {
                name: expr.to_string(),
                data_type: inferred_type(),
                nullable: true,
            },
            // PostgreSQL names `(expr).field` after the field.
            PlanExpr::GetField(pair) => ColumnInfo {
                name: pair.1.clone(),
                data_type: inferred_type(),
                nullable: true,
            },
            _ => ColumnInfo {
                name: expr.to_string(),
                data_type: inferred_type(),
                nullable: true,
            },
        }
    }

    /// Build a PlanningContext from an existing plan's output schema.
    fn context_from_plan(&self, plan: &LogicalPlan) -> PlanningContext {
        let mut ctx = PlanningContext::new();
        for col in plan.schema() {
            ctx.columns.push((None, col));
        }
        ctx
    }

    /// Evaluate a LIMIT/OFFSET expression to a usize. Any constant that
    /// folds to an integer is accepted, such as `3`, `CAST(3 AS INTEGER)`
    /// or `$1::INTEGER` with the parameter already substituted.
    async fn eval_limit_expr(&self, expr: Option<&ast::Expr>) -> Result<Option<usize>, PlanError> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        let not_constant = || {
            PlanError::invalid_expression(
                "LIMIT/OFFSET must be a constant integer expression".to_string(),
            )
        };
        let planned = self
            .plan_expr(expr, &PlanningContext::new())
            .await
            .map_err(|_| not_constant())?;
        let folded = crate::optimizer::fold_constants(planned)?;
        // Describe plans the statement before Bind with its placeholders
        // still in place, and only reports the output columns. Execution
        // substitutes the bound text first, so it never reaches this arm.
        let param_index = |e: &PlanExpr| match e {
            PlanExpr::Parameter { index, .. } => Some(*index),
            _ => None,
        };
        let placeholder = param_index(&folded).or_else(|| match &folded {
            PlanExpr::Cast { expr, .. } => param_index(expr),
            _ => None,
        });
        if let Some(index) = placeholder {
            if let Ok(mut limit_params) = self.limit_params.lock() {
                limit_params.insert(index, DataType::Int64);
            }
            return Ok(None);
        }
        // `1::SMALLINT` keeps its range-checked cast over the literal.
        let value = match &folded {
            PlanExpr::Literal { value, .. } => value,
            PlanExpr::Cast { expr, .. } => match &**expr {
                PlanExpr::Literal { value, .. } => value,
                _ => return Err(not_constant()),
            },
            _ => return Err(not_constant()),
        };
        let n = match value {
            ScalarValue::Int32(n) => i64::from(*n),
            ScalarValue::Int64(n) => *n,
            _ => {
                return Err(PlanError::invalid_expression(
                    "LIMIT/OFFSET must be an integer".to_string(),
                ))
            }
        };
        if n < 0 {
            return Err(PlanError::invalid_expression(
                "LIMIT/OFFSET must be non-negative".to_string(),
            ));
        }
        Ok(Some(n as usize))
    }
}

/// Check if a function name is a known aggregate function.
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "BOOL_OR"
            | "BOOL_AND"
            | "ARRAY_AGG"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "STDDEV_SAMP"
            | "STDDEV"
            | "STDDEV_POP"
            | "VAR_SAMP"
            | "VARIANCE"
            | "VAR_POP"
    )
}

/// Direction of an EXISTS/NOT EXISTS rewrite.
#[derive(Debug)]
enum SemiJoinKind {
    Semi,
    Anti,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PredicateTruth {
    True,
    False,
    NotTrue,
    NotFalse,
}

fn apply_semi_join(input: LogicalPlan, spec: SemiJoinSpec) -> LogicalPlan {
    match spec.kind {
        SemiJoinKind::Semi => LogicalPlan::SemiJoin {
            left: Box::new(input),
            right: Box::new(spec.right_plan),
            left_key: spec.left_key,
            right_key: spec.right_key,
            residual: spec.residual,
            dynamic_filter_ids: Vec::new(),
            probe_side_filter_ids: Vec::new(),
        },
        SemiJoinKind::Anti => LogicalPlan::AntiJoin {
            left: Box::new(input),
            right: Box::new(spec.right_plan),
            left_key: spec.left_key,
            right_key: spec.right_key,
            residual: spec.residual,
            probe_side_filter_ids: Vec::new(),
        },
    }
}

/// The interval side of `x ± INTERVAL` / `INTERVAL + x`.
struct IntervalOperand {
    op: ast::BinaryOp,
    value: i64,
    unit: ast::IntervalUnit,
    span: arneb_sql_parser::Span,
    /// The interval is the left operand (`INTERVAL + x`).
    first: bool,
}

/// The count, unit and span of an interval literal, looking through
/// parentheses: `d + (INTERVAL '1 day')`.
fn as_interval(expr: &ast::Expr) -> Option<(i64, ast::IntervalUnit, arneb_sql_parser::Span)> {
    match expr {
        ast::Expr::Interval { value, unit, span } => Some((*value, *unit, *span)),
        ast::Expr::Nested { expr, .. } => as_interval(expr),
        _ => None,
    }
}

fn interval_outside_arithmetic(span: Option<arneb_sql_parser::Span>) -> PlanError {
    PlanError::UnsupportedExpression {
        message: "interval outside date/timestamp arithmetic".to_string(),
        location: span.map(|s| s.start),
    }
}

/// Lowers `x ± INTERVAL` by the planned type of `x`:
/// - DATE ± days stays integer-day arithmetic (`d - 90`), the form
///   TPC-H plans with, so it is still DATE;
/// - DATE ± years/quarters/months/weeks is `date_add(unit, ±n, d)`, DATE;
/// - DATE ± hours/minutes/seconds is
///   `date_add(unit, ±n, CAST(d AS TIMESTAMP))`, TIMESTAMP;
/// - TIMESTAMP ± any unit is `date_add(unit, ±n, ts)`, TIMESTAMP.
///
/// Any other operand type is unsupported.
#[inline(never)]
fn plan_interval_arithmetic(
    operand: PlanExpr,
    interval: IntervalOperand,
    ctx: &PlanningContext,
    span: Option<arneb_sql_parser::Span>,
) -> Result<PlanExpr, PlanError> {
    use ast::IntervalUnit;
    let schema: Vec<ColumnInfo> = ctx.columns.iter().map(|(_, col)| col.clone()).collect();
    let operand_type = plan_expr_type(&operand, &schema);
    let out_of_range = || PlanError::UnsupportedExpression {
        message: format!("interval value out of range: {}", interval.value),
        location: Some(interval.span.start),
    };
    let target = match (&operand_type, interval.unit) {
        // An operand of unknown type (a parameter on the describe path)
        // keeps the integer-day form a DAY interval always had.
        (Some(DataType::Date32) | None, IntervalUnit::Day) => {
            let days = PlanExpr::Literal {
                value: ScalarValue::Int32(
                    i32::try_from(interval.value).map_err(|_| out_of_range())?,
                ),
                span: Some(interval.span),
            };
            let (left, right) = if interval.first {
                (days, operand)
            } else {
                (operand, days)
            };
            return Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op: interval.op,
                right: Box::new(right),
                span,
            });
        }
        (
            Some(DataType::Date32),
            IntervalUnit::Hour | IntervalUnit::Minute | IntervalUnit::Second,
        ) => PlanExpr::Cast {
            expr: Box::new(operand),
            data_type: DataType::Timestamp {
                unit: arneb_common::types::TimeUnit::Microsecond,
                timezone: None,
            },
            span: None,
        },
        (Some(DataType::Date32 | DataType::Timestamp { .. }), _) => operand,
        (other, _) => {
            return Err(PlanError::UnsupportedExpression {
                message: format!(
                    "interval arithmetic needs a DATE or TIMESTAMP operand, got {}",
                    other
                        .as_ref()
                        .map_or_else(|| "an unknown type".to_string(), |t| t.to_string())
                ),
                location: span.map(|s| s.start),
            });
        }
    };
    let count = match interval.op {
        ast::BinaryOp::Minus => interval.value.checked_neg().ok_or_else(out_of_range)?,
        _ => interval.value,
    };
    Ok(PlanExpr::Function {
        name: "date_add".to_string(),
        args: vec![
            PlanExpr::Literal {
                value: ScalarValue::Utf8(interval.unit.name().to_string()),
                span: None,
            },
            PlanExpr::Literal {
                value: ScalarValue::Int64(count),
                span: Some(interval.span),
            },
            target,
        ],
        distinct: false,
        order_by: None,
        span,
    })
}

fn contains_nested_subquery(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. } => true,
        ast::Expr::BinaryOp { left, right, .. } => {
            contains_nested_subquery(left) || contains_nested_subquery(right)
        }
        ast::Expr::UnaryOp { expr, .. } | ast::Expr::Nested { expr, .. } => {
            contains_nested_subquery(expr)
        }
        _ => false,
    }
}

fn aggregate_projection_on_empty(expr: &ast::Expr) -> Option<ast::Expr> {
    match expr {
        ast::Expr::Function { name, span, .. } if is_aggregate_function(name) => {
            Some(ast::Expr::Literal {
                value: if name.eq_ignore_ascii_case("count") {
                    ScalarValue::Int64(0)
                } else {
                    ScalarValue::Null
                },
                span: *span,
            })
        }
        ast::Expr::Function {
            name,
            args,
            distinct,
            order_by,
            span,
        } => Some(ast::Expr::Function {
            order_by: order_by
                .iter()
                .map(|ob| {
                    aggregate_projection_on_empty(&ob.expr).map(|expr| ast::OrderByExpr {
                        expr,
                        asc: ob.asc,
                        nulls_first: ob.nulls_first,
                    })
                })
                .collect::<Option<Vec<_>>>()?,
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| match arg {
                    ast::FunctionArg::Unnamed(expr) => {
                        aggregate_projection_on_empty(expr).map(ast::FunctionArg::Unnamed)
                    }
                    ast::FunctionArg::Wildcard => Some(ast::FunctionArg::Wildcard),
                })
                .collect::<Option<Vec<_>>>()?,
            distinct: *distinct,
            span: *span,
        }),
        ast::Expr::BinaryOp {
            left,
            op,
            right,
            span,
        } => Some(ast::Expr::BinaryOp {
            left: Box::new(aggregate_projection_on_empty(left)?),
            op: *op,
            right: Box::new(aggregate_projection_on_empty(right)?),
            span: *span,
        }),
        ast::Expr::UnaryOp { op, expr, span } => Some(ast::Expr::UnaryOp {
            op: *op,
            expr: Box::new(aggregate_projection_on_empty(expr)?),
            span: *span,
        }),
        ast::Expr::Nested { expr, span } => Some(ast::Expr::Nested {
            expr: Box::new(aggregate_projection_on_empty(expr)?),
            span: *span,
        }),
        ast::Expr::Cast {
            expr,
            data_type,
            span,
        } => Some(ast::Expr::Cast {
            expr: Box::new(aggregate_projection_on_empty(expr)?),
            data_type: data_type.clone(),
            span: *span,
        }),
        ast::Expr::IsNull { expr, span } => Some(ast::Expr::IsNull {
            expr: Box::new(aggregate_projection_on_empty(expr)?),
            span: *span,
        }),
        ast::Expr::IsNotNull { expr, span } => Some(ast::Expr::IsNotNull {
            expr: Box::new(aggregate_projection_on_empty(expr)?),
            span: *span,
        }),
        ast::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            span,
        } => Some(ast::Expr::Case {
            operand: match operand {
                Some(expr) => Some(Box::new(aggregate_projection_on_empty(expr)?)),
                None => None,
            },
            conditions: conditions
                .iter()
                .map(aggregate_projection_on_empty)
                .collect::<Option<Vec<_>>>()?,
            results: results
                .iter()
                .map(aggregate_projection_on_empty)
                .collect::<Option<Vec<_>>>()?,
            else_result: match else_result {
                Some(expr) => Some(Box::new(aggregate_projection_on_empty(expr)?)),
                None => None,
            },
            span: *span,
        }),
        ast::Expr::Literal { .. } => Some(expr.clone()),
        _ => None,
    }
}

/// A pending LEFT JOIN extracted from a correlated scalar subquery in
/// WHERE (e.g. TPC-H Q17's `< (SELECT 0.2 * AVG(...) FROM lineitem
/// WHERE l_partkey = p_partkey)`). The inner is decorrelated by
/// adding the correlation column(s) to GROUP BY, so the join attaches
/// a per-correlation-key scalar value to each outer row. Outer rows
/// without a matching key recover the projection's empty-set value in
/// the residual predicate.
///
/// Supports multi-column correlations (TPC-H Q20: scalar subquery
/// correlated on both `ps_partkey` AND `ps_suppkey`).
#[derive(Debug)]
struct LeftJoinSpec {
    /// Decorrelated subquery plan. Output schema:
    /// `[corr_key_0, corr_key_1, ..., value]`.
    plan: LogicalPlan,
    /// One entry per correlation, paired with its index in
    /// `plan.schema()`.
    correlations: Vec<(ast::Expr, usize)>,
    /// Value of the scalar projection when the correlated input is empty.
    empty_value: ast::Expr,
}

struct CteSelfAggWindowSpec {
    scalar_subplan: LogicalPlan,
    cte_plan: LogicalPlan,
    cte_col_name: String,
    agg_func: String,
    arith: Option<(ast::BinaryOp, PlanExpr)>,
}

fn rewrite_cte_self_agg_scalar_to_window(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } => LogicalPlan::Projection {
            input: Box::new(rewrite_cte_self_agg_scalar_to_window(*input)),
            exprs,
            schema,
        },
        LogicalPlan::Filter { input, predicate } => {
            if let Some(spec) = find_cte_self_agg_window_spec(&predicate) {
                if let Some((rewritten_input, window_index)) =
                    wrap_matching_cte_with_window((*input).clone(), &spec)
                {
                    let replacement = cte_window_replacement_expr(&spec, window_index);
                    let predicate = rewrite_expr_after_column_insert(
                        predicate,
                        window_index,
                        &spec.scalar_subplan,
                        &replacement,
                    );
                    return LogicalPlan::Filter {
                        input: Box::new(rewritten_input),
                        predicate,
                    };
                }
            }

            LogicalPlan::Filter {
                input: Box::new(rewrite_cte_self_agg_scalar_to_window(*input)),
                predicate,
            }
        }
        LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
            input: Box::new(rewrite_cte_self_agg_scalar_to_window(*input)),
            order_by,
        },
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(rewrite_cte_self_agg_scalar_to_window(*input)),
            limit,
            offset,
        },
        other => other,
    }
}

fn find_cte_self_agg_window_spec(predicate: &PlanExpr) -> Option<CteSelfAggWindowSpec> {
    match predicate {
        PlanExpr::BinaryOp {
            left,
            op: ast::BinaryOp::And,
            right,
            ..
        } => find_cte_self_agg_window_spec(left).or_else(|| find_cte_self_agg_window_spec(right)),
        PlanExpr::BinaryOp {
            left,
            op:
                ast::BinaryOp::Eq
                | ast::BinaryOp::Lt
                | ast::BinaryOp::LtEq
                | ast::BinaryOp::Gt
                | ast::BinaryOp::GtEq,
            right,
            ..
        } => match (&**left, &**right) {
            (PlanExpr::Column { name, .. }, PlanExpr::ScalarSubquery { subplan, .. })
            | (PlanExpr::ScalarSubquery { subplan, .. }, PlanExpr::Column { name, .. }) => {
                scalar_subquery_self_agg_spec(subplan, name)
            }
            _ => None,
        },
        _ => None,
    }
}

fn scalar_subquery_self_agg_spec(
    subplan: &LogicalPlan,
    outer_col_name: &str,
) -> Option<CteSelfAggWindowSpec> {
    let LogicalPlan::Projection { input, exprs, .. } = subplan else {
        return None;
    };
    if exprs.len() != 1 {
        return None;
    }

    let (agg_ref, arith) = scalar_projection_agg_ref(&exprs[0])?;
    let LogicalPlan::Aggregate {
        input: cte_plan,
        group_by,
        aggr_exprs,
        ..
    } = &**input
    else {
        return None;
    };
    if !group_by.is_empty() || aggr_exprs.len() != 1 || agg_ref != 0 {
        return None;
    }

    let PlanExpr::Function {
        name,
        args,
        distinct,
        ..
    } = &aggr_exprs[0]
    else {
        return None;
    };
    let agg_name = name.to_ascii_uppercase();
    if *distinct || !matches!(agg_name.as_str(), "MAX" | "MIN") || args.len() != 1 {
        return None;
    }
    let PlanExpr::Column {
        name: cte_col_name, ..
    } = &args[0]
    else {
        return None;
    };
    if !cte_col_name.eq_ignore_ascii_case(outer_col_name) {
        return None;
    }

    Some(CteSelfAggWindowSpec {
        scalar_subplan: subplan.clone(),
        cte_plan: (**cte_plan).clone(),
        cte_col_name: cte_col_name.clone(),
        agg_func: agg_name,
        arith,
    })
}

fn scalar_projection_agg_ref(
    expr: &PlanExpr,
) -> Option<(usize, Option<(ast::BinaryOp, PlanExpr)>)> {
    match expr {
        PlanExpr::Column { index, .. } => Some((*index, None)),
        PlanExpr::BinaryOp {
            left, op, right, ..
        } if matches!(op, ast::BinaryOp::Plus | ast::BinaryOp::Minus) => {
            let PlanExpr::Column { index, .. } = &**left else {
                return None;
            };
            if !matches!(**right, PlanExpr::Literal { .. }) {
                return None;
            }
            Some((*index, Some((*op, (**right).clone()))))
        }
        _ => None,
    }
}

fn cte_window_replacement_expr(spec: &CteSelfAggWindowSpec, window_index: usize) -> PlanExpr {
    let window_col = PlanExpr::Column {
        index: window_index,
        name: "__cte_max".to_string(),
        span: None,
    };
    match &spec.arith {
        Some((op, lit)) => PlanExpr::BinaryOp {
            left: Box::new(window_col),
            op: *op,
            right: Box::new(lit.clone()),
            span: None,
        },
        None => window_col,
    }
}

fn wrap_matching_cte_with_window(
    plan: LogicalPlan,
    spec: &CteSelfAggWindowSpec,
) -> Option<(LogicalPlan, usize)> {
    let schema = plan.schema();
    if logical_plan_eq_ignoring_spans(&plan, &spec.cte_plan) {
        let cte_col_index = schema
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&spec.cte_col_name))?;
        let window_index = schema.len();
        return Some((
            LogicalPlan::Window {
                input: Box::new(plan),
                functions: vec![WindowFunctionDef {
                    name: spec.agg_func.clone(),
                    args: vec![PlanExpr::Column {
                        index: cte_col_index,
                        name: spec.cte_col_name.clone(),
                        span: None,
                    }],
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    rows_frame: false,
                    output_name: "__cte_max".to_string(),
                }],
            },
            window_index,
        ));
    }

    match plan {
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
            dynamic_filter_ids,
        } => {
            let left_width = left.schema().len();
            let right_width = right.schema().len();
            if let Some((new_left, local_index)) =
                wrap_matching_cte_with_window((*left).clone(), spec)
            {
                let condition = shift_join_condition_after_insert(condition, local_index);
                return Some((
                    LogicalPlan::Join {
                        left: Box::new(new_left),
                        right,
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    },
                    local_index,
                ));
            }
            if let Some((new_right, local_index)) =
                wrap_matching_cte_with_window((*right).clone(), spec)
            {
                let global_index = left_width + local_index;
                let condition = if local_index < right_width {
                    shift_join_condition_after_insert(condition, global_index)
                } else {
                    condition
                };
                return Some((
                    LogicalPlan::Join {
                        left,
                        right: Box::new(new_right),
                        join_type,
                        condition,
                        dynamic_filter_ids,
                    },
                    global_index,
                ));
            }
            None
        }
        _ => None,
    }
}

/// The `Array` arm of [`rewrite_expr_after_column_insert`], out of line so
/// the recursive function's frame stays small.
#[inline(never)]
fn rewrite_array_after_column_insert(
    expr: PlanExpr,
    inserted_index: usize,
    scalar_subplan: &LogicalPlan,
    scalar_replacement: &PlanExpr,
) -> PlanExpr {
    let PlanExpr::Array {
        elements,
        element_type,
        span,
    } = expr
    else {
        return expr;
    };
    PlanExpr::Array {
        elements: elements
            .into_iter()
            .map(|e| {
                rewrite_expr_after_column_insert(
                    e,
                    inserted_index,
                    scalar_subplan,
                    scalar_replacement,
                )
            })
            .collect(),
        element_type,
        span,
    }
}

fn rewrite_expr_after_column_insert(
    expr: PlanExpr,
    inserted_index: usize,
    scalar_subplan: &LogicalPlan,
    scalar_replacement: &PlanExpr,
) -> PlanExpr {
    match expr {
        PlanExpr::Column { index, name, span } => PlanExpr::Column {
            index: if index >= inserted_index {
                index + 1
            } else {
                index
            },
            name,
            span,
        },
        PlanExpr::ScalarSubquery { subplan, span } => {
            if logical_plan_eq_ignoring_spans(&subplan, scalar_subplan) {
                scalar_replacement.clone()
            } else {
                PlanExpr::ScalarSubquery { subplan, span }
            }
        }
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => PlanExpr::BinaryOp {
            left: Box::new(rewrite_expr_after_column_insert(
                *left,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            op,
            right: Box::new(rewrite_expr_after_column_insert(
                *right,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            span,
        },
        PlanExpr::UnaryOp { op, expr, span } => PlanExpr::UnaryOp {
            op,
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            span,
        },
        PlanExpr::Function {
            name,
            args,
            distinct,
            order_by,
            span,
        } => PlanExpr::Function {
            order_by: SortExpr::map_exprs(order_by, &mut |e| {
                rewrite_expr_after_column_insert(
                    e,
                    inserted_index,
                    scalar_subplan,
                    scalar_replacement,
                )
            }),
            name,
            args: args
                .into_iter()
                .map(|a| {
                    rewrite_expr_after_column_insert(
                        a,
                        inserted_index,
                        scalar_subplan,
                        scalar_replacement,
                    )
                })
                .collect(),
            distinct,
            span,
        },
        PlanExpr::IsNull { expr, span } => PlanExpr::IsNull {
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            span,
        },
        PlanExpr::IsNotNull { expr, span } => PlanExpr::IsNotNull {
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            span,
        },
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => PlanExpr::Between {
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            negated,
            low: Box::new(rewrite_expr_after_column_insert(
                *low,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            high: Box::new(rewrite_expr_after_column_insert(
                *high,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            span,
        },
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => PlanExpr::InList {
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            list: list
                .into_iter()
                .map(|e| {
                    rewrite_expr_after_column_insert(
                        e,
                        inserted_index,
                        scalar_subplan,
                        scalar_replacement,
                    )
                })
                .collect(),
            negated,
            span,
        },
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => PlanExpr::Cast {
            expr: Box::new(rewrite_expr_after_column_insert(
                *expr,
                inserted_index,
                scalar_subplan,
                scalar_replacement,
            )),
            data_type,
            span,
        },
        PlanExpr::ArrayContains {
            expr,
            array,
            negated,
            span,
        } => crate::map_array_contains(expr, array, negated, span, |e| {
            rewrite_expr_after_column_insert(e, inserted_index, scalar_subplan, scalar_replacement)
        }),
        PlanExpr::ArrayElement(pair) => crate::map_array_element(pair, |e| {
            rewrite_expr_after_column_insert(e, inserted_index, scalar_subplan, scalar_replacement)
        }),
        PlanExpr::GetField(pair) => crate::map_get_field(pair, |e| {
            rewrite_expr_after_column_insert(e, inserted_index, scalar_subplan, scalar_replacement)
        }),
        PlanExpr::Array { .. } => rewrite_array_after_column_insert(
            expr,
            inserted_index,
            scalar_subplan,
            scalar_replacement,
        ),
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => PlanExpr::CaseExpr {
            operand: operand.map(|e| {
                Box::new(rewrite_expr_after_column_insert(
                    *e,
                    inserted_index,
                    scalar_subplan,
                    scalar_replacement,
                ))
            }),
            when_clauses: when_clauses
                .into_iter()
                .map(|(c, r)| {
                    (
                        rewrite_expr_after_column_insert(
                            c,
                            inserted_index,
                            scalar_subplan,
                            scalar_replacement,
                        ),
                        rewrite_expr_after_column_insert(
                            r,
                            inserted_index,
                            scalar_subplan,
                            scalar_replacement,
                        ),
                    )
                })
                .collect(),
            else_result: else_result.map(|e| {
                Box::new(rewrite_expr_after_column_insert(
                    *e,
                    inserted_index,
                    scalar_subplan,
                    scalar_replacement,
                ))
            }),
            span,
        },
        other => other,
    }
}

fn shift_join_condition_after_insert(
    condition: JoinCondition,
    inserted_index: usize,
) -> JoinCondition {
    match condition {
        JoinCondition::On(expr) => JoinCondition::On(shift_plan_expr_columns(expr, inserted_index)),
        JoinCondition::None => JoinCondition::None,
    }
}

fn shift_plan_expr_columns(expr: PlanExpr, inserted_index: usize) -> PlanExpr {
    let replacement = PlanExpr::Literal {
        value: ScalarValue::Null,
        span: None,
    };
    rewrite_expr_after_column_insert(expr, inserted_index, &LogicalPlan::OneRow, &replacement)
}

fn logical_plan_eq_ignoring_spans(left: &LogicalPlan, right: &LogicalPlan) -> bool {
    serde_json::to_value(left).ok() == serde_json::to_value(right).ok()
}

/// A pending SemiJoin/AntiJoin extracted from a WHERE-clause `EXISTS`
/// (or `NOT EXISTS`) sub-predicate. Carries the decorrelated inner
/// plan plus the single equi-correlation key pair and an optional
/// residual predicate (for non-equi correlations that reference both
/// outer and inner — e.g. TPC-H Q21's `l2.l_suppkey <> l1.l_suppkey`).
#[derive(Debug)]
struct SemiJoinSpec {
    kind: SemiJoinKind,
    right_plan: LogicalPlan,
    left_key: PlanExpr,
    right_key: PlanExpr,
    /// Residual predicate planned against the concatenated
    /// (outer ++ inner) column index space.
    residual: Option<PlanExpr>,
}

/// Flatten an AND-tree into its leaf predicates.
fn split_and(expr: &ast::Expr) -> Vec<ast::Expr> {
    let mut out = Vec::new();
    fn go(e: &ast::Expr, out: &mut Vec<ast::Expr>) {
        match e {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOp::And,
                right,
                ..
            } => {
                go(left, out);
                go(right, out);
            }
            ast::Expr::Nested { expr, .. } => go(expr, out),
            other => out.push(other.clone()),
        }
    }
    go(expr, &mut out);
    out
}

/// Reduce a list of conjuncts to a single expression using AND.
/// Returns `None` for an empty list (i.e. no remaining predicate).
fn combine_and(parts: Vec<ast::Expr>) -> Option<ast::Expr> {
    parts.into_iter().reduce(|acc, e| ast::Expr::BinaryOp {
        left: Box::new(acc),
        op: ast::BinaryOp::And,
        right: Box::new(e),
        span: arneb_sql_parser::Span::empty(),
    })
}

fn split_or(expr: &ast::Expr) -> Vec<ast::Expr> {
    let mut out = Vec::new();
    fn go(expr: &ast::Expr, out: &mut Vec<ast::Expr>) {
        match expr {
            ast::Expr::BinaryOp {
                left,
                op: ast::BinaryOp::Or,
                right,
                ..
            } => {
                go(left, out);
                go(right, out);
            }
            ast::Expr::Nested { expr, .. } => go(expr, out),
            other => out.push(other.clone()),
        }
    }
    go(expr, &mut out);
    out
}

fn combine_or(parts: Vec<ast::Expr>) -> Option<ast::Expr> {
    parts.into_iter().reduce(|acc, expr| ast::Expr::BinaryOp {
        left: Box::new(acc),
        op: ast::BinaryOp::Or,
        right: Box::new(expr),
        span: arneb_sql_parser::Span::empty(),
    })
}

type CorrelationPair = (ast::Expr, ast::Expr);

fn same_correlation(
    left: &CorrelationPair,
    right: &CorrelationPair,
    outer_ctx: &PlanningContext,
    inner_ctx: &PlanningContext,
) -> bool {
    fn column_index(expr: &ast::Expr, ctx: &PlanningContext) -> Option<usize> {
        let ast::Expr::Column { col_ref, .. } = expr else {
            return None;
        };
        ctx.resolve_column(&col_ref.name, col_ref.table.as_deref(), None)
            .ok()
            .map(|(index, _)| index)
    }
    column_index(&left.0, outer_ctx) == column_index(&right.0, outer_ctx)
        && column_index(&left.1, inner_ctx) == column_index(&right.1, inner_ctx)
}

/// Pull equi-correlations shared by every OR branch above the disjunction.
fn factor_or_correlations(
    expr: &ast::Expr,
    outer_ctx: &PlanningContext,
    inner_ctx: &PlanningContext,
) -> Option<(Vec<CorrelationPair>, Option<ast::Expr>)> {
    let branches = split_or(expr);
    if branches.len() < 2 {
        return None;
    }
    let branch_parts: Vec<Vec<ast::Expr>> = branches.iter().map(split_and).collect();
    let mut common: Vec<CorrelationPair> = branch_parts[0]
        .iter()
        .filter_map(|part| try_match_correlation(part, outer_ctx, inner_ctx))
        .collect();
    common.retain(|candidate| {
        branch_parts.iter().skip(1).all(|parts| {
            parts.iter().any(|part| {
                try_match_correlation(part, outer_ctx, inner_ctx)
                    .is_some_and(|pair| same_correlation(candidate, &pair, outer_ctx, inner_ctx))
            })
        })
    });
    if common.is_empty() {
        return None;
    }
    let mut remainders = Vec::new();
    for mut parts in branch_parts {
        for pair in &common {
            let index = parts.iter().position(|part| {
                try_match_correlation(part, outer_ctx, inner_ctx).is_some_and(|candidate| {
                    same_correlation(pair, &candidate, outer_ctx, inner_ctx)
                })
            })?;
            parts.remove(index);
        }
        match combine_and(parts) {
            Some(remainder) => remainders.push(remainder),
            None => return Some((common, None)),
        }
    }
    let remainder = combine_or(remainders);
    if remainder
        .as_ref()
        .is_some_and(|expr| references_outer(expr, inner_ctx))
    {
        return None;
    }
    Some((common, remainder))
}

/// Return `Some((outer_key, inner_key))` when `expr` is an equi
/// predicate `<column on one side> = <column on other side>` with
/// one side resolving in `outer_ctx` only and the other side
/// resolving in `inner_ctx` only — the shape we can pull out as a
/// SemiJoin's single key pair.
fn try_match_correlation(
    expr: &ast::Expr,
    outer_ctx: &PlanningContext,
    inner_ctx: &PlanningContext,
) -> Option<(ast::Expr, ast::Expr)> {
    let (left_expr, right_expr) = match expr {
        ast::Expr::BinaryOp {
            left,
            op: ast::BinaryOp::Eq,
            right,
            ..
        } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };
    let lc = match left_expr {
        ast::Expr::Column { col_ref, .. } => col_ref,
        _ => return None,
    };
    let rc = match right_expr {
        ast::Expr::Column { col_ref, .. } => col_ref,
        _ => return None,
    };

    let left_in_inner = inner_ctx
        .resolve_column(&lc.name, lc.table.as_deref(), None)
        .is_ok();
    let left_in_outer = outer_ctx
        .resolve_column(&lc.name, lc.table.as_deref(), None)
        .is_ok();
    let right_in_inner = inner_ctx
        .resolve_column(&rc.name, rc.table.as_deref(), None)
        .is_ok();
    let right_in_outer = outer_ctx
        .resolve_column(&rc.name, rc.table.as_deref(), None)
        .is_ok();

    // SQL resolves unqualified columns in the innermost scope first,
    // falling back to outer scopes. So a name appearing in both is
    // treated as inner. We classify each side accordingly and look
    // for the (Inner, Outer) / (Outer, Inner) shapes that map to a
    // semi-join key pair. Cases where a side resolves in NEITHER scope
    // would be a planning error caught later; we just decline to
    // decorrelate here.
    #[derive(Eq, PartialEq)]
    enum Scope {
        Inner,
        Outer,
        Neither,
    }
    let left_scope = if left_in_inner {
        Scope::Inner
    } else if left_in_outer {
        Scope::Outer
    } else {
        Scope::Neither
    };
    let right_scope = if right_in_inner {
        Scope::Inner
    } else if right_in_outer {
        Scope::Outer
    } else {
        Scope::Neither
    };
    match (left_scope, right_scope) {
        (Scope::Inner, Scope::Outer) => Some((right_expr.clone(), left_expr.clone())),
        (Scope::Outer, Scope::Inner) => Some((left_expr.clone(), right_expr.clone())),
        _ => None,
    }
}

/// Returns `true` if `expr` references any column that does NOT
/// resolve in `inner_ctx` (and therefore must come from an outer
/// scope). Used to reject inner-WHERE predicates we can't yet
/// decorrelate.
fn references_outer(expr: &ast::Expr, inner_ctx: &PlanningContext) -> bool {
    let mut found = false;
    walk_columns(expr, &mut |col_ref| {
        if inner_ctx
            .resolve_column(&col_ref.name, col_ref.table.as_deref(), None)
            .is_err()
        {
            found = true;
        }
    });
    found
}

/// Planning-context qualifier for window-function result columns. They are
/// resolvable unqualified (by the call's display string) but are not user
/// columns, so `*` expansion skips them.
const WINDOW_QUALIFIER: &str = "\u{0}window";

/// Context qualifier of the columns a `ProjectSet` appends (one per SRF).
const SRF_QUALIFIER: &str = "\u{0}srf";

/// The SRF and its arguments when a SELECT item is a set-returning function
/// call (possibly parenthesized, possibly aliased).
fn srf_call(item: &ast::SelectItem) -> Option<(SrfKind, &ast::Expr)> {
    let (ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. }) = item
    else {
        return None;
    };
    srf_call_expr(expr)
}

/// The SRF call `expr` is, possibly parenthesized and under field accesses
/// (`(_pg_expandarray(a)).n`).
fn srf_call_expr(expr: &ast::Expr) -> Option<(SrfKind, &ast::Expr)> {
    match expr {
        ast::Expr::Nested { expr: inner, .. } | ast::Expr::FieldAccess { expr: inner, .. } => {
            srf_call_expr(inner)
        }
        ast::Expr::Function { name, .. } => SrfKind::from_name(name).map(|kind| (kind, expr)),
        _ => None,
    }
}

/// `expr` with its SRF call (see [`srf_call_expr`]) replaced by `column`,
/// keeping the field accesses above it.
fn replace_srf_call(expr: &ast::Expr, column: ast::Expr) -> ast::Expr {
    match expr {
        ast::Expr::Nested { expr: inner, span } => ast::Expr::Nested {
            expr: Box::new(replace_srf_call(inner, column)),
            span: *span,
        },
        ast::Expr::FieldAccess {
            expr: inner,
            field,
            span,
        } => ast::Expr::FieldAccess {
            expr: Box::new(replace_srf_call(inner, column)),
            field: field.clone(),
            span: *span,
        },
        _ => column,
    }
}

/// The outermost field an SRF SELECT item reads (`n` for
/// `(_pg_expandarray(a)).n`), PostgreSQL's name for the column.
fn srf_item_field(expr: &ast::Expr) -> Option<&str> {
    match expr {
        ast::Expr::Nested { expr, .. } => srf_item_field(expr),
        ast::Expr::FieldAccess { field, .. } => Some(field),
        _ => None,
    }
}

/// The array argument of an SRF call, checking its shape: `unnest(array)`
/// and `generate_subscripts(array, 1)`.
fn srf_array_argument(kind: SrfKind, call: &ast::Expr) -> Result<&ast::Expr, PlanError> {
    let ast::Expr::Function {
        args,
        distinct,
        order_by,
        ..
    } = call
    else {
        unreachable!("srf_call returns function calls");
    };
    if *distinct || !order_by.is_empty() {
        return Err(PlanError::UnsupportedExpression {
            message: format!(
                "DISTINCT and ORDER BY are not supported in {}()",
                kind.name()
            ),
            location: None,
        });
    }
    match (kind, args.as_slice()) {
        (SrfKind::Unnest, [ast::FunctionArg::Unnamed(array)]) => Ok(array),
        (SrfKind::Unnest, _) => Err(PlanError::UnsupportedExpression {
            message: "unnest takes exactly one array argument".to_string(),
            location: None,
        }),
        // The dimension is checked once planned: any constant folding to 1.
        (
            SrfKind::GenerateSubscripts,
            [ast::FunctionArg::Unnamed(array), ast::FunctionArg::Unnamed(_)],
        ) => Ok(array),
        (SrfKind::GenerateSubscripts, _) => Err(generate_subscripts_dim_error()),
        (SrfKind::ExpandArray, [ast::FunctionArg::Unnamed(array)]) => Ok(array),
        (SrfKind::ExpandArray, _) => Err(PlanError::UnsupportedExpression {
            message: "_pg_expandarray takes exactly one array argument".to_string(),
            location: None,
        }),
    }
}

/// The dimension argument of a `generate_subscripts(array, dim)` call.
fn generate_subscripts_dim(call: &ast::Expr) -> Option<&ast::Expr> {
    match call {
        ast::Expr::Function { args, .. } => match args.as_slice() {
            [_, ast::FunctionArg::Unnamed(dim)] => Some(dim),
            _ => None,
        },
        _ => None,
    }
}

fn generate_subscripts_dim_error() -> PlanError {
    PlanError::UnsupportedExpression {
        message: "generate_subscripts is supported only as generate_subscripts(array, 1): \
                  arrays have dimension 1"
            .to_string(),
        location: None,
    }
}

/// `current_schemas` takes exactly one boolean argument (PostgreSQL). Out of
/// line so `plan_expr`'s recursive frame stays small.
#[inline(never)]
fn current_schemas_arity_error(args: usize, span: Option<arneb_sql_parser::Span>) -> PlanError {
    PlanError::UnsupportedExpression {
        message: format!("function current_schemas takes exactly one boolean argument, got {args}"),
        location: span.map(|s| s.start),
    }
}

/// The error for an SRF call anywhere but as a whole SELECT item. Out of
/// line so `plan_expr`'s recursive frame stays small.
#[inline(never)]
fn srf_not_supported_here(name: &str, span: Option<arneb_sql_parser::Span>) -> PlanError {
    PlanError::UnsupportedExpression {
        message: format!(
            "set-returning function {}() is not supported here; it is supported only as a \
             whole SELECT-list item",
            name.to_ascii_lowercase()
        ),
        location: span.map(|s| s.start),
    }
}

/// Collects the distinct (by display string) window-function calls in `expr`.
fn collect_window_calls(expr: &ast::Expr, out: &mut Vec<ast::Expr>) {
    use ast::Expr as E;
    match expr {
        E::WindowFunction { .. } => {
            let s = expr.to_string();
            if !out.iter().any(|e| e.to_string() == s) {
                out.push(expr.clone());
            }
        }
        E::BinaryOp { left, right, .. } => {
            collect_window_calls(left, out);
            collect_window_calls(right, out);
        }
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. }
        | E::Nested { expr, .. }
        | E::FieldAccess { expr, .. } => collect_window_calls(expr, out),
        E::Subscript { expr, index, .. } => {
            collect_window_calls(expr, out);
            collect_window_calls(index, out);
        }
        E::Between {
            expr, low, high, ..
        } => {
            collect_window_calls(expr, out);
            collect_window_calls(low, out);
            collect_window_calls(high, out);
        }
        E::InList { expr, list, .. } => {
            collect_window_calls(expr, out);
            for e in list {
                collect_window_calls(e, out);
            }
        }
        E::Function { args, order_by, .. } => {
            for a in args {
                if let ast::FunctionArg::Unnamed(e) = a {
                    collect_window_calls(e, out);
                }
            }
            for o in order_by {
                collect_window_calls(&o.expr, out);
            }
        }
        E::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            for e in operand
                .iter()
                .chain(else_result.iter())
                .map(|b| &**b)
                .chain(conditions)
                .chain(results)
            {
                collect_window_calls(e, out);
            }
        }
        _ => {}
    }
}

fn walk_columns(expr: &ast::Expr, cb: &mut impl FnMut(&ast::ColumnRef)) {
    use ast::Expr as E;
    match expr {
        E::Column { col_ref, .. } => cb(col_ref),
        E::BinaryOp { left, right, .. } => {
            walk_columns(left, cb);
            walk_columns(right, cb);
        }
        E::UnaryOp { expr, .. }
        | E::IsNull { expr, .. }
        | E::IsNotNull { expr, .. }
        | E::Cast { expr, .. }
        | E::Nested { expr, .. }
        | E::FieldAccess { expr, .. } => walk_columns(expr, cb),
        E::Subscript { expr, index, .. } => {
            walk_columns(expr, cb);
            walk_columns(index, cb);
        }
        E::Between {
            expr, low, high, ..
        } => {
            walk_columns(expr, cb);
            walk_columns(low, cb);
            walk_columns(high, cb);
        }
        E::InList { expr, list, .. } => {
            walk_columns(expr, cb);
            for e in list {
                walk_columns(e, cb);
            }
        }
        E::Function { args, order_by, .. } => {
            for a in args {
                if let ast::FunctionArg::Unnamed(e) = a {
                    walk_columns(e, cb);
                }
            }
            for o in order_by {
                walk_columns(&o.expr, cb);
            }
        }
        E::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            if let Some(o) = operand {
                walk_columns(o, cb);
            }
            for c in conditions {
                walk_columns(c, cb);
            }
            for r in results {
                walk_columns(r, cb);
            }
            if let Some(e) = else_result {
                walk_columns(e, cb);
            }
        }
        _ => {}
    }
}

/// Walks `plan` and pushes every `TableScan`'s qualified reference into
/// `sink`. Deduplicated by reference so each table only contributes one
/// `TableStatistics` fetch per query.
fn collect_table_scan_refs(
    plan: &LogicalPlan,
    sink: &mut Vec<arneb_common::types::TableReference>,
) {
    use LogicalPlan as L;
    match plan {
        L::TableScan { table, .. } => {
            if !sink.iter().any(|r| r == table) {
                sink.push(table.clone());
            }
        }
        L::Projection { input, .. }
        | L::Filter { input, .. }
        | L::Sort { input, .. }
        | L::Limit { input, .. }
        | L::Explain { input, .. }
        | L::Aggregate { input, .. }
        | L::RollupAggregate { input, .. }
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Distinct { input }
        | L::Window { input, .. }
        | L::ProjectSet { input, .. }
        | L::AssignUniqueId { input, .. } => collect_table_scan_refs(input, sink),
        L::Join { left, right, .. }
        | L::SemiJoin { left, right, .. }
        | L::AntiJoin { left, right, .. }
        | L::Intersect { left, right }
        | L::Except { left, right } => {
            collect_table_scan_refs(left, sink);
            collect_table_scan_refs(right, sink);
        }
        L::UnionAll { inputs } => {
            for input in inputs {
                collect_table_scan_refs(input, sink);
            }
        }
        L::ScalarSubquery { subplan } => collect_table_scan_refs(subplan, sink),
        L::CreateTableAsSelect { source, .. } | L::InsertInto { source, .. } => {
            collect_table_scan_refs(source, sink);
        }
        L::CreateView { plan, .. } => collect_table_scan_refs(plan, sink),
        L::ExchangeNode { .. }
        | L::CreateTable { .. }
        | L::DropTable { .. }
        | L::DeleteFrom { .. }
        | L::DropView { .. }
        | L::OneRow => {}
    }
}

/// `ORDER BY 2` / `GROUP BY 2`: an integer literal names a 1-based
/// SELECT-list position (Trino semantics). Returns the 0-based index, or
/// `None` when `expr` is not an integer literal.
fn select_ordinal(
    expr: &ast::Expr,
    width: usize,
    clause: &str,
) -> Result<Option<usize>, PlanError> {
    let ast::Expr::Literal {
        value: ScalarValue::Int64(n),
        ..
    } = expr
    else {
        return Ok(None);
    };
    match usize::try_from(*n) {
        Ok(pos) if (1..=width).contains(&pos) => Ok(Some(pos - 1)),
        _ => Err(PlanError::invalid_expression(format!(
            "{clause} position {n} is not in select list"
        ))),
    }
}

/// Replace `GROUP BY <ordinal>` items with the SELECT-list expression they
/// name, leaving every other GROUP BY expression untouched.
fn resolve_group_by_ordinals(body: &ast::SelectBody) -> Result<Vec<ast::Expr>, PlanError> {
    // A wildcard expands to an unknown number of columns, so positions
    // after it can't be mapped from the AST select list.
    let has_wildcard = body.projection.iter().any(|item| {
        matches!(
            item,
            ast::SelectItem::Wildcard | ast::SelectItem::QualifiedWildcard(_)
        )
    });
    let mut out = Vec::with_capacity(body.group_by.len());
    for e in &body.group_by {
        let Some(index) = select_ordinal(e, body.projection.len(), "GROUP BY")? else {
            out.push(e.clone());
            continue;
        };
        match &body.projection[index] {
            ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. }
                if !has_wildcard =>
            {
                out.push(expr.clone())
            }
            _ => {
                return Err(PlanError::invalid_expression(
                    "GROUP BY position with SELECT * is not supported",
                ))
            }
        }
    }
    Ok(out)
}

fn rollup_group_by_keys(group_by: &[ast::Expr]) -> Result<Option<Vec<ast::Expr>>, PlanError> {
    let [ast::Expr::Function { name, args, .. }] = group_by else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case("ROLLUP") {
        return Ok(None);
    }
    args.iter()
        .map(|arg| match arg {
            ast::FunctionArg::Unnamed(expr) => Ok(expr.clone()),
            _ => Err(PlanError::invalid_expression("ROLLUP requires expressions")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

/// If a GROUP BY AST expression is a qualified column reference
/// (`t.col`), return its table qualifier. Used when rebuilding the
/// post-aggregate context so a subsequent `SELECT t.col` resolves to
/// the correct slot even when two aliases share a column name (e.g.
/// self-joined `nation n1` / `nation n2`).
fn group_by_qualifier(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Column { col_ref, .. } => col_ref.table.clone(),
        _ => None,
    }
}

/// An aggregate call's `ORDER BY`, written as `PlanExpr`'s Display writes it
/// (direction and NULLS placement spelled out, with `plan_call_order_by`'s
/// defaults), so `find_aggregate_index` matches the aggregate's output name.
fn write_call_order_by_unqualified(out: &mut String, order_by: &[ast::OrderByExpr]) {
    for (i, ob) in order_by.iter().enumerate() {
        out.push_str(if i == 0 { " ORDER BY " } else { ", " });
        write_ast_unqualified(out, &ob.expr);
        let asc = ob.asc.unwrap_or(true);
        out.push_str(if asc { " ASC" } else { " DESC" });
        out.push_str(if ob.nulls_first.unwrap_or(!asc) {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
}

/// Format an AST expression, stripping column qualifiers and unwrapping
/// parenthesized `Nested` nodes. Produces a string that matches the
/// Display of an equivalent `PlanExpr`:
///
/// - `PlanExpr::Column` carries only the column name, not a qualifier,
///   so we drop the `table.` prefix.
/// - `plan_expr` unwraps `ast::Expr::Nested` into its inner node (there
///   is no `PlanExpr::Nested` variant), so its Display loses the
///   parentheses. We mirror that here; otherwise a SELECT expression
///   like `SUM(x * (1 - y))` formats differently from its stored
///   aggregate column name (`SUM(x * 1 - y)`) and misses the
///   exact-match loop in `find_aggregate_index`.
fn format_ast_unqualified(expr: &ast::Expr) -> String {
    let mut out = String::new();
    write_ast_unqualified(&mut out, expr);
    out
}

fn write_ast_unqualified(out: &mut String, expr: &ast::Expr) {
    use std::fmt::Write;
    match expr {
        ast::Expr::Nested { expr: inner, .. } => {
            write_ast_unqualified(out, inner);
        }
        ast::Expr::Column { col_ref, .. } => {
            let _ = write!(out, "{}", col_ref.name);
        }
        ast::Expr::Literal { value, .. } => {
            let _ = write!(out, "{value}");
        }
        ast::Expr::BinaryOp {
            left, op, right, ..
        } => {
            write_ast_unqualified(out, left);
            let _ = write!(out, " {op} ");
            write_ast_unqualified(out, right);
        }
        ast::Expr::UnaryOp { op, expr, .. } => {
            let _ = write!(out, "{op} ");
            write_ast_unqualified(out, expr);
        }
        ast::Expr::Function {
            name,
            args,
            distinct,
            order_by,
            ..
        } => {
            let _ = write!(out, "{name}(");
            if *distinct {
                let _ = write!(out, "DISTINCT ");
            }
            for (i, a) in args.iter().enumerate() {
                if i > 0 {
                    let _ = write!(out, ", ");
                }
                match a {
                    ast::FunctionArg::Unnamed(e) => write_ast_unqualified(out, e),
                    ast::FunctionArg::Wildcard => {
                        let _ = write!(out, "*");
                    }
                }
            }
            write_call_order_by_unqualified(out, order_by);
            let _ = write!(out, ")");
        }
        ast::Expr::IsNull { expr, .. } => {
            write_ast_unqualified(out, expr);
            let _ = write!(out, " IS NULL");
        }
        ast::Expr::IsNotNull { expr, .. } => {
            write_ast_unqualified(out, expr);
            let _ = write!(out, " IS NOT NULL");
        }
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
            ..
        } => {
            write_ast_unqualified(out, expr);
            if *negated {
                let _ = write!(out, " NOT BETWEEN ");
            } else {
                let _ = write!(out, " BETWEEN ");
            }
            write_ast_unqualified(out, low);
            let _ = write!(out, " AND ");
            write_ast_unqualified(out, high);
        }
        ast::Expr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            write_ast_unqualified(out, expr);
            if *negated {
                let _ = write!(out, " NOT");
            }
            let _ = write!(out, " IN (");
            for (i, item) in list.iter().enumerate() {
                if i > 0 {
                    let _ = write!(out, ", ");
                }
                write_ast_unqualified(out, item);
            }
            let _ = write!(out, ")");
        }
        ast::Expr::AnyAll {
            expr, op, array, ..
        } => {
            write_ast_unqualified(out, expr);
            let _ = write!(
                out,
                "{}",
                match op {
                    ast::AnyAllOp::EqAny => " = ANY (",
                    ast::AnyAllOp::NotEqAll => " <> ALL (",
                }
            );
            write_ast_unqualified(out, array);
            let _ = write!(out, ")");
        }
        ast::Expr::Subscript { expr, index, .. } => {
            write_ast_unqualified(out, expr);
            let _ = write!(out, "[");
            write_ast_unqualified(out, index);
            let _ = write!(out, "]");
        }
        ast::Expr::FieldAccess { expr, field, .. } => {
            write_ast_unqualified(out, expr);
            let _ = write!(out, ".{field}");
        }
        ast::Expr::Array { elements, .. } => {
            let _ = write!(out, "ARRAY[");
            for (i, item) in elements.iter().enumerate() {
                if i > 0 {
                    let _ = write!(out, ", ");
                }
                write_ast_unqualified(out, item);
            }
            let _ = write!(out, "]");
        }
        ast::Expr::Cast {
            expr, data_type, ..
        } => {
            let _ = write!(out, "CAST(");
            write_ast_unqualified(out, expr);
            let _ = write!(out, " AS {data_type})");
        }
        ast::Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => {
            let _ = write!(out, "CASE");
            if let Some(op) = operand {
                let _ = write!(out, " ");
                write_ast_unqualified(out, op);
            }
            for (cond, res) in conditions.iter().zip(results.iter()) {
                let _ = write!(out, " WHEN ");
                write_ast_unqualified(out, cond);
                let _ = write!(out, " THEN ");
                write_ast_unqualified(out, res);
            }
            if let Some(el) = else_result {
                let _ = write!(out, " ELSE ");
                write_ast_unqualified(out, el);
            }
            let _ = write!(out, " END");
        }
        // Fall back to Display for node kinds whose qualifiers we
        // don't need to strip (subqueries, parameters, etc.).
        other => {
            let _ = write!(out, "{other}");
        }
    }
}

/// The (left index, right index) of each `JOIN ... USING` column, each
/// resolved without a qualifier on its own side. The merged column takes
/// the left value, which is only right for inner and left joins.
fn using_pairs(
    left: &PlanningContext,
    right: &PlanningContext,
    columns: &[String],
    join_type: ast::JoinType,
) -> Result<Vec<(usize, usize)>, PlanError> {
    if !matches!(join_type, ast::JoinType::Inner | ast::JoinType::Left) {
        return Err(PlanError::invalid_expression(format!(
            "USING is only supported with INNER and LEFT joins, not {join_type:?}"
        )));
    }
    columns
        .iter()
        .map(|c| {
            let (l, _) = left.resolve_column(c, None, None)?;
            let (r, _) = right.resolve_column(c, None, None)?;
            Ok((l, r))
        })
        .collect()
}

/// A session function the planner folds to a literal: `current_catalog` and
/// `current_schema` (Trino's niladic forms) and `current_database()` (a
/// call, `call`), from the session's defaults; `current_user`,
/// `session_user` and `current_role` from the session's user when the
/// catalog knows it. `None` for any other name.
fn session_literal(
    catalog: &CatalogManager,
    name: &str,
    call: bool,
    span: Option<arneb_sql_parser::Span>,
) -> Option<PlanExpr> {
    let is = |n: &str| name.eq_ignore_ascii_case(n);
    let value = if is("current_catalog") || (call && is("current_database")) {
        catalog.default_catalog()
    } else if is("current_schema") {
        catalog.default_schema()
    } else if is("current_user") || is("session_user") || is("current_role") {
        catalog.session_user()?
    } else {
        return None;
    };
    Some(PlanExpr::Literal {
        value: ScalarValue::Utf8(value.to_string()),
        span,
    })
}

/// The column name PostgreSQL gives an unaliased niladic session function
/// (`current_catalog`, `current_database()`, ...) that the planner folds to
/// a literal: the function's name, not the literal's text.
fn session_function_name(expr: &ast::Expr) -> Option<String> {
    let (name, call) = match expr {
        ast::Expr::Column { col_ref, .. } if col_ref.table.is_none() => (&col_ref.name, false),
        ast::Expr::Function { name, args, .. } if args.is_empty() => (name, true),
        _ => return None,
    };
    let lower = name.to_ascii_lowercase();
    let folds = matches!(
        lower.as_str(),
        "current_catalog" | "current_schema" | "current_user" | "session_user" | "current_role"
    ) || (call && lower == "current_database");
    folds.then_some(lower)
}

/// The object kind `__has_privilege` checks for a privilege function.
fn privilege_object_kind(name: &str) -> Option<&'static str> {
    [
        ("has_table_privilege", "table"),
        ("has_any_column_privilege", "any_column"),
        ("has_schema_privilege", "schema"),
    ]
    .into_iter()
    .find(|(n, _)| name.eq_ignore_ascii_case(n))
    .map(|(_, kind)| kind)
}

/// `CAST(<regclass expr> AS TEXT)`: the operand is a `regclass` conversion
/// (possibly parenthesized) and the target a string type.
fn is_regclass_to_text(expr: &ast::Expr, target: &DataType) -> bool {
    if !matches!(target, DataType::Utf8 | DataType::LargeUtf8) {
        return false;
    }
    match expr {
        ast::Expr::Nested { expr, .. } => is_regclass_to_text(expr, target),
        ast::Expr::Function { name, .. } => name.eq_ignore_ascii_case("regclass"),
        _ => false,
    }
}

fn odd_json_build_object_args(span: Option<arneb_sql_parser::Span>) -> PlanError {
    PlanError::UnsupportedExpression {
        message: "json_build_object requires an even number of arguments".to_string(),
        location: span.map(|s| s.start),
    }
}

impl QueryPlanner<'_> {
    /// `<expr>::regclass`. A string literal folds to its oid at plan time
    /// (no existence check). A text expression becomes
    /// `__regclass_oid(name, ...)`, which checks the name exists when it
    /// runs; anything else is already the oid.
    #[inline(never)]
    async fn plan_regclass(
        &self,
        args: &[ast::FunctionArg],
        ctx: &PlanningContext,
        span: Option<arneb_sql_parser::Span>,
    ) -> Result<PlanExpr, PlanError> {
        let [ast::FunctionArg::Unnamed(operand)] = args else {
            return Err(PlanError::UnsupportedExpression {
                message: "regclass takes one argument".to_string(),
                location: span.map(|s| s.start),
            });
        };
        if matches!(
            operand,
            ast::Expr::Literal {
                value: ScalarValue::Utf8(_),
                ..
            }
        ) {
            return regclass_literal(self.catalog, args, span);
        }
        let planned = self.plan_expr(operand, ctx).await?;
        let ctx_schema: Vec<ColumnInfo> = ctx.columns.iter().map(|(_, c)| c.clone()).collect();
        match plan_expr_type(&planned, &ctx_schema) {
            Some(DataType::Utf8 | DataType::LargeUtf8) => {
                let mut call_args = vec![planned];
                call_args.extend(self.regclass_scope(span).await);
                Ok(PlanExpr::Function {
                    order_by: None,
                    name: "__regclass_oid".to_string(),
                    args: call_args,
                    distinct: false,
                    span,
                })
            }
            Some(DataType::Int64) => Ok(planned),
            _ => Ok(PlanExpr::Cast {
                expr: Box::new(planned),
                data_type: DataType::Int64,
                span,
            }),
        }
    }

    /// `CAST(<regclass expr> AS TEXT)`: the relation's name, from the
    /// statement's catalog (`__regclass_name`).
    #[inline(never)]
    async fn plan_regclass_name(
        &self,
        operand: &ast::Expr,
        ctx: &PlanningContext,
        span: Option<arneb_sql_parser::Span>,
    ) -> Result<PlanExpr, PlanError> {
        let mut call_args = vec![self.plan_expr(operand, ctx).await?];
        call_args.extend(self.regclass_scope(span).await);
        Ok(PlanExpr::Function {
            order_by: None,
            name: "__regclass_name".to_string(),
            args: call_args,
            distinct: false,
            span,
        })
    }

    /// `has_table_privilege`, `has_any_column_privilege` or
    /// `has_schema_privilege` (`([role,] object, privilege)`) as
    /// `__has_privilege`, with the session's user and privileges as
    /// literals: the runtime check cannot reach the catalog.
    #[inline(never)]
    async fn plan_has_privilege(
        &self,
        kind: &str,
        name: &str,
        mut args: Vec<PlanExpr>,
        span: Option<arneb_sql_parser::Span>,
    ) -> Result<PlanExpr, PlanError> {
        let literal = |v: String| PlanExpr::Literal {
            value: ScalarValue::Utf8(v),
            span,
        };
        let session_user = self.catalog.session_user().unwrap_or_default().to_string();
        match args.len() {
            2 => args.insert(0, literal(session_user.clone())),
            3 => {}
            n => {
                return Err(PlanError::UnsupportedExpression {
                    message: format!("function {name} takes 2 or 3 arguments, got {n}"),
                    location: span.map(|s| s.start),
                })
            }
        }
        let privileges = self.catalog.session_privileges().await;
        let schemas = privileges
            .schemas
            .join(&arneb_common::pg_oid::RELATION_ENTRY_SEP.to_string());
        let field = arneb_common::pg_oid::RELATION_FIELD_SEP;
        let tables = privileges
            .tables
            .iter()
            .map(|(schema, table, held)| {
                let letters: String = arneb_common::pg_oid::TABLE_PRIVILEGE_LETTERS
                    .iter()
                    .filter(|(_, p)| held.iter().any(|h| h.to_string() == *p))
                    .map(|(letter, _)| *letter)
                    .collect();
                format!("{schema}{field}{table}{field}{letters}")
            })
            .collect::<Vec<_>>()
            .join(&arneb_common::pg_oid::RELATION_ENTRY_SEP.to_string());
        let mut call_args = vec![literal(kind.to_string())];
        call_args.extend(args);
        call_args.extend([
            literal(session_user),
            literal(self.catalog.default_catalog().to_string()),
            literal(self.catalog.default_schema().to_string()),
            literal(schemas),
            literal(tables),
        ]);
        Ok(PlanExpr::Function {
            order_by: None,
            name: "__has_privilege".to_string(),
            args: call_args,
            distinct: false,
            span,
        })
    }

    /// The literal arguments the runtime `regclass` functions take after
    /// their operand: the session catalog and default schema, and the
    /// relations the statement's catalog lists (see
    /// [`arneb_common::pg_oid::RELATION_ENTRY_SEP`]).
    async fn regclass_scope(&self, span: Option<arneb_sql_parser::Span>) -> Vec<PlanExpr> {
        let database = self.catalog.default_catalog().to_string();
        let mut list = String::new();
        let mut push = |schema: &str, table: &str| {
            if !list.is_empty() {
                list.push(arneb_common::pg_oid::RELATION_ENTRY_SEP);
            }
            list.push_str(schema);
            list.push(arneb_common::pg_oid::RELATION_FIELD_SEP);
            list.push_str(table);
        };
        if let Some(relations) = self.catalog.regclass_relations() {
            for (schema, table) in relations {
                push(schema, table);
            }
        } else {
            for schema in self.catalog.visible_schema_names(&database).await {
                for table in self.catalog.visible_table_names(&database, &schema).await {
                    push(&schema, &table);
                }
            }
        }
        [database, self.catalog.default_schema().to_string(), list]
            .into_iter()
            .map(|v| PlanExpr::Literal {
                value: ScalarValue::Utf8(v),
                span,
            })
            .collect()
    }
}

/// `'<name>'::regclass` as the relation's oid (pg-catalog-tables), resolved
/// against the session's catalog and default schema. Only a string literal
/// can be resolved at plan time.
fn regclass_literal(
    catalog: &CatalogManager,
    args: &[ast::FunctionArg],
    span: Option<arneb_sql_parser::Span>,
) -> Result<PlanExpr, PlanError> {
    let oid = match args {
        [ast::FunctionArg::Unnamed(ast::Expr::Literal {
            value: ScalarValue::Utf8(name),
            ..
        })] => arneb_common::pg_oid::regclass_oid(
            name,
            catalog.default_catalog(),
            catalog.default_schema(),
        )
        .ok_or_else(|| PlanError::InvalidLiteral {
            message: format!("invalid relation name for regclass: '{name}'"),
            location: span.map(|s| s.start),
        })?,
        _ => {
            return Err(PlanError::UnsupportedExpression {
                message: "regclass of anything but a string literal".to_string(),
                location: span.map(|s| s.start),
            })
        }
    };
    Ok(PlanExpr::Literal {
        value: ScalarValue::Int64(oid),
        span,
    })
}

/// `percentile_cont` / `percentile_disc`, the ordered-set aggregates.
fn is_percentile(name: &str) -> bool {
    name.eq_ignore_ascii_case("PERCENTILE_CONT") || name.eq_ignore_ascii_case("PERCENTILE_DISC")
}

/// Validates `percentile_*(f, x)` (the parser's form of `percentile_*(f)
/// WITHIN GROUP (ORDER BY x)`): `f` is a constant number in [0, 1] (the
/// executor casts it to DOUBLE); `percentile_cont` needs a numeric `x`.
fn check_percentile_call(
    name: &str,
    args: &[PlanExpr],
    schema: &[ColumnInfo],
    span: Option<arneb_sql_parser::Span>,
) -> Result<(), PlanError> {
    let location = span.map(|s| s.start);
    let [fraction, key] = args else {
        return Err(PlanError::InvalidExpression {
            message: format!("{name} requires WITHIN GROUP (ORDER BY ...)"),
            location,
        });
    };
    let value = match fraction {
        PlanExpr::Literal { value, .. } => scalar_as_f64(value),
        PlanExpr::UnaryOp {
            op: ast::UnaryOp::Minus,
            expr,
            ..
        } => match &**expr {
            PlanExpr::Literal { value, .. } => scalar_as_f64(value).map(|v| -v),
            _ => None,
        },
        _ => None,
    };
    let Some(value) = value else {
        return Err(PlanError::InvalidExpression {
            message: format!("{name} requires a constant numeric fraction"),
            location,
        });
    };
    if !(0.0..=1.0).contains(&value) {
        return Err(PlanError::InvalidExpression {
            message: format!("percentile value {value} is not between 0 and 1"),
            location,
        });
    }
    if name.eq_ignore_ascii_case("PERCENTILE_CONT") {
        let numeric = match plan_expr_type(key, schema) {
            None => true,
            Some(t) => matches!(
                t,
                DataType::Null
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128 { .. }
            ),
        };
        if !numeric {
            return Err(PlanError::InvalidExpression {
                message: format!("{name} requires a numeric WITHIN GROUP expression"),
                location,
            });
        }
    }
    Ok(())
}

/// A numeric literal as `f64`.
fn scalar_as_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int32(v) => Some(f64::from(*v)),
        ScalarValue::Int64(v) => Some(*v as f64),
        ScalarValue::Float32(v) => Some(f64::from(*v)),
        ScalarValue::Float64(v) => Some(*v),
        ScalarValue::Decimal128 { value, scale, .. } => {
            Some(*value as f64 / 10f64.powi(i32::from(*scale)))
        }
        _ => None,
    }
}

/// Aggregate `ORDER BY` is accepted only in `array_agg`, and as the
/// `WITHIN GROUP` key of `percentile_cont` / `percentile_disc`.
fn check_aggregate_order_by(name: &str) -> Result<(), PlanError> {
    if name.eq_ignore_ascii_case("ARRAY_AGG") || is_percentile(name) {
        Ok(())
    } else {
        Err(PlanError::invalid_expression(format!(
            "ORDER BY inside {name}(...) is not supported"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::{CatalogManager, MemoryCatalog, MemorySchema, MemoryTable};
    use arneb_common::types::ColumnInfo;
    use std::sync::Arc;

    #[test]
    fn column_insert_shifts_columns_inside_arrays_and_array_contains() {
        let col = |index: usize| PlanExpr::Column {
            index,
            name: format!("c{index}"),
            span: None,
        };
        let array = PlanExpr::Array {
            elements: vec![col(0), col(2)],
            element_type: DataType::Int64,
            span: None,
        };
        // A column inserted at index 1 moves column 2 to 3 and leaves 0.
        let shifted = shift_plan_expr_columns(array.clone(), 1);
        let PlanExpr::Array { elements, .. } = &shifted else {
            panic!("expected Array, got {shifted}");
        };
        assert!(matches!(elements[0], PlanExpr::Column { index: 0, .. }));
        assert!(matches!(elements[1], PlanExpr::Column { index: 3, .. }));
        let contains = PlanExpr::ArrayContains {
            expr: Box::new(col(5)),
            array: Box::new(array),
            negated: false,
            span: None,
        };
        let shifted = shift_plan_expr_columns(contains, 1);
        let PlanExpr::ArrayContains { expr, array, .. } = &shifted else {
            panic!("expected ArrayContains");
        };
        assert!(matches!(**expr, PlanExpr::Column { index: 6, .. }));
        let PlanExpr::Array { elements, .. } = &**array else {
            panic!("expected Array");
        };
        assert!(matches!(elements[1], PlanExpr::Column { index: 3, .. }));
    }

    #[test]
    fn column_insert_shifts_columns_inside_subscripts_and_field_access() {
        let col = |index: usize| PlanExpr::Column {
            index,
            name: format!("c{index}"),
            span: None,
        };
        let element = PlanExpr::ArrayElement(Box::new((col(2), col(0))));
        let field = PlanExpr::GetField(Box::new((element, "x".to_string())));
        let shifted = shift_plan_expr_columns(field, 1);
        let PlanExpr::GetField(pair) = &shifted else {
            panic!("expected GetField, got {shifted}");
        };
        assert_eq!(pair.1, "x");
        let PlanExpr::ArrayElement(inner) = &pair.0 else {
            panic!("expected ArrayElement");
        };
        assert!(matches!(inner.0, PlanExpr::Column { index: 3, .. }));
        assert!(matches!(inner.1, PlanExpr::Column { index: 0, .. }));
    }

    const Q15_SQL: &str = include_str!("../../../benchmarks/tpch/queries/q15.sql");

    /// Create a CatalogManager with a "users" table (id: Int64, name: Utf8, age: Int32).
    fn test_catalog() -> CatalogManager {
        let mgr = CatalogManager::new("default", "public");
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());

        let users = Arc::new(MemoryTable::new(vec![
            ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "name".into(),
                data_type: DataType::Utf8,
                nullable: true,
            },
            ColumnInfo {
                name: "age".into(),
                data_type: DataType::Int32,
                nullable: true,
            },
        ]));
        schema.register_table("users", users);

        let orders = Arc::new(MemoryTable::new(vec![
            ColumnInfo {
                name: "order_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "user_id".into(),
                data_type: DataType::Int64,
                nullable: false,
            },
            ColumnInfo {
                name: "amount".into(),
                data_type: DataType::Float64,
                nullable: true,
            },
        ]));
        schema.register_table("orders", orders);

        catalog.register_schema("public", schema);
        mgr.register_catalog("default", catalog);
        mgr
    }

    fn q15_catalog() -> CatalogManager {
        let mgr = CatalogManager::new("default", "public");
        let catalog = Arc::new(MemoryCatalog::new());
        let schema = Arc::new(MemorySchema::new());

        schema.register_table(
            "lineitem",
            Arc::new(MemoryTable::new(vec![
                ColumnInfo {
                    name: "l_suppkey".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "l_extendedprice".into(),
                    data_type: DataType::Float64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "l_discount".into(),
                    data_type: DataType::Float64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "l_shipdate".into(),
                    data_type: DataType::Date32,
                    nullable: false,
                },
            ])),
        );
        schema.register_table(
            "supplier",
            Arc::new(MemoryTable::new(vec![
                ColumnInfo {
                    name: "s_suppkey".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                ColumnInfo {
                    name: "s_name".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
                ColumnInfo {
                    name: "s_address".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
                ColumnInfo {
                    name: "s_phone".into(),
                    data_type: DataType::Utf8,
                    nullable: false,
                },
            ])),
        );

        catalog.register_schema("public", schema);
        mgr.register_catalog("default", catalog);
        mgr
    }

    async fn plan_sql(sql: &str) -> Result<LogicalPlan, PlanError> {
        let catalog = test_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(sql).expect("parse failed");
        planner.plan_statement(&stmt).await
    }

    async fn plan_q15_with_cte_window_gate(enabled: bool) -> LogicalPlan {
        let _guard = cte_self_agg_window_test_lock().lock().await;
        *CTE_SELF_AGG_WINDOW_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("CTE self-agg window override lock poisoned") = Some(enabled);

        let catalog = q15_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(Q15_SQL).expect("parse q15");
        let plan = planner.plan_statement(&stmt).await.expect("plan q15");

        *CTE_SELF_AGG_WINDOW_TEST_OVERRIDE
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("CTE self-agg window override lock poisoned") = None;
        plan
    }

    /// The projection expressions of `sql` over the q15 `lineitem`
    /// fixture (`l_shipdate DATE`).
    async fn lineitem_projection(sql: &str) -> Result<Vec<PlanExpr>, PlanError> {
        let catalog = q15_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = arneb_sql_parser::parse(sql).expect("parse");
        match planner.plan_statement(&stmt).await? {
            LogicalPlan::Projection { exprs, .. } => Ok(exprs),
            other => panic!("expected a projection, got {other:?}"),
        }
    }

    #[test]
    fn interval_day_on_untyped_operand_keeps_integer_days() {
        let parameter = || PlanExpr::Parameter {
            index: 1,
            type_hint: None,
            span: None,
        };
        let interval = |unit| IntervalOperand {
            op: ast::BinaryOp::Minus,
            value: 90,
            unit,
            span: arneb_sql_parser::Span::empty(),
            first: false,
        };
        let ctx = PlanningContext::new();
        let planned =
            plan_interval_arithmetic(parameter(), interval(ast::IntervalUnit::Day), &ctx, None)
                .unwrap();
        match planned {
            PlanExpr::BinaryOp {
                left, op, right, ..
            } => {
                assert_eq!(op, ast::BinaryOp::Minus);
                assert!(matches!(*left, PlanExpr::Parameter { index: 1, .. }));
                assert!(matches!(
                    *right,
                    PlanExpr::Literal {
                        value: ScalarValue::Int32(90),
                        ..
                    }
                ));
            }
            other => panic!("expected $1 - 90, got {other}"),
        }
        // Other units still need a known DATE or TIMESTAMP operand.
        let err =
            plan_interval_arithmetic(parameter(), interval(ast::IntervalUnit::Month), &ctx, None)
                .unwrap_err()
                .to_string();
        assert!(err.contains("interval"), "{err}");
    }

    #[tokio::test]
    async fn interval_arithmetic_lowering() {
        let exprs = lineitem_projection(
            "SELECT l_shipdate - INTERVAL '90' DAY, l_shipdate + INTERVAL '90 days', \
             INTERVAL '1' DAY + l_shipdate, l_shipdate - INTERVAL '1 month', \
             l_shipdate + INTERVAL '2' HOUR FROM lineitem",
        )
        .await
        .unwrap();
        // DATE ± days stays integer-day arithmetic (the TPC-H form).
        for (expr, op) in [
            (&exprs[0], ast::BinaryOp::Minus),
            (&exprs[1], ast::BinaryOp::Plus),
        ] {
            match expr {
                PlanExpr::BinaryOp {
                    left, op: o, right, ..
                } => {
                    assert_eq!(*o, op);
                    assert!(matches!(left.as_ref(), PlanExpr::Column { .. }), "{expr}");
                    assert!(
                        matches!(
                            right.as_ref(),
                            PlanExpr::Literal {
                                value: ScalarValue::Int32(90),
                                ..
                            }
                        ),
                        "{expr}"
                    );
                }
                other => panic!("expected date - days, got {other}"),
            }
        }
        assert!(
            matches!(
                &exprs[2],
                PlanExpr::BinaryOp { left, .. }
                    if matches!(left.as_ref(), PlanExpr::Literal { value: ScalarValue::Int32(1), .. })
            ),
            "{}",
            exprs[2]
        );
        // Other units call date_add, negated for `-`.
        let PlanExpr::Function { name, args, .. } = &exprs[3] else {
            panic!("expected date_add, got {}", exprs[3]);
        };
        assert_eq!(name, "date_add");
        assert!(
            matches!(&args[0], PlanExpr::Literal { value: ScalarValue::Utf8(u), .. } if u == "month")
        );
        assert!(matches!(
            &args[1],
            PlanExpr::Literal {
                value: ScalarValue::Int64(-1),
                ..
            }
        ));
        assert!(matches!(&args[2], PlanExpr::Column { .. }));
        // A sub-day unit on a DATE goes through TIMESTAMP.
        let PlanExpr::Function { args, .. } = &exprs[4] else {
            panic!("expected date_add, got {}", exprs[4]);
        };
        assert!(matches!(
            &args[2],
            PlanExpr::Cast {
                data_type: DataType::Timestamp { .. },
                ..
            }
        ));

        // Parentheses around the interval change nothing.
        let nested = lineitem_projection("SELECT l_shipdate + (INTERVAL '1 month') FROM lineitem")
            .await
            .unwrap();
        assert!(
            matches!(&nested[0], PlanExpr::Function { name, .. } if name == "date_add"),
            "{}",
            nested[0]
        );

        for sql in [
            "SELECT INTERVAL '1 day' FROM lineitem",
            "SELECT INTERVAL '1 day' - l_shipdate FROM lineitem",
            "SELECT l_suppkey + INTERVAL '1 day' FROM lineitem",
            "SELECT l_shipdate * INTERVAL '1 day' FROM lineitem",
        ] {
            let err = lineitem_projection(sql).await.unwrap_err().to_string();
            assert!(err.contains("interval"), "{sql}: {err}");
        }
    }

    fn cte_self_agg_window_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[tokio::test]
    async fn q15_cte_self_agg_window_gate_controls_revenue0_recompute() {
        let off = plan_q15_with_cte_window_gate(false).await;
        assert_eq!(
            count_table_scans(&off, "lineitem"),
            2,
            "gate off should preserve today's cloned CTE recompute"
        );
        assert_eq!(count_max_windows(&off), 0);

        let on = plan_q15_with_cte_window_gate(true).await;
        assert_eq!(
            count_table_scans(&on, "lineitem"),
            1,
            "gate on should plan the revenue0 lineitem scan once"
        );
        assert_eq!(
            count_max_windows(&on),
            1,
            "gate on should add one global MAX window"
        );
        assert_eq!(
            count_scalar_subqueries(&on),
            0,
            "gate on should remove the scalar subquery clone"
        );
    }

    #[tokio::test]
    async fn q15_cte_self_agg_window_rewrite_preserves_tolerance() {
        let plan = plan_q15_with_cte_window_gate(true).await;
        assert!(
            contains_cte_max_minus_cent(&plan),
            "rewritten q15 predicate must preserve MAX(total_revenue) - 0.01"
        );
    }

    fn count_table_scans(plan: &LogicalPlan, table_name: &str) -> usize {
        let here = match plan {
            LogicalPlan::TableScan { table, .. }
                if table.table.eq_ignore_ascii_case(table_name) =>
            {
                1
            }
            _ => 0,
        };
        here + plan_children(plan)
            .into_iter()
            .map(|child| count_table_scans(child, table_name))
            .sum::<usize>()
            + plan_exprs(plan)
                .into_iter()
                .map(|expr| count_table_scans_in_expr(expr, table_name))
                .sum::<usize>()
    }

    fn count_table_scans_in_expr(expr: &PlanExpr, table_name: &str) -> usize {
        let here = match expr {
            PlanExpr::ScalarSubquery { subplan, .. } => count_table_scans(subplan, table_name),
            _ => 0,
        };
        here + expr_children(expr)
            .into_iter()
            .map(|child| count_table_scans_in_expr(child, table_name))
            .sum::<usize>()
    }

    fn count_max_windows(plan: &LogicalPlan) -> usize {
        let here = match plan {
            LogicalPlan::Window { functions, .. } => functions
                .iter()
                .filter(|f| f.name.eq_ignore_ascii_case("MAX"))
                .count(),
            _ => 0,
        };
        here + plan_children(plan)
            .into_iter()
            .map(count_max_windows)
            .sum::<usize>()
    }

    fn count_scalar_subqueries(plan: &LogicalPlan) -> usize {
        plan_children(plan)
            .into_iter()
            .map(count_scalar_subqueries)
            .sum::<usize>()
            + plan_exprs(plan)
                .into_iter()
                .map(count_scalar_subqueries_in_expr)
                .sum::<usize>()
    }

    fn count_scalar_subqueries_in_expr(expr: &PlanExpr) -> usize {
        let here = usize::from(matches!(expr, PlanExpr::ScalarSubquery { .. }));
        here + expr_children(expr)
            .into_iter()
            .map(count_scalar_subqueries_in_expr)
            .sum::<usize>()
    }

    fn contains_cte_max_minus_cent(plan: &LogicalPlan) -> bool {
        plan_exprs(plan)
            .into_iter()
            .any(expr_contains_cte_max_minus_cent)
            || plan_children(plan)
                .into_iter()
                .any(contains_cte_max_minus_cent)
    }

    fn expr_contains_cte_max_minus_cent(expr: &PlanExpr) -> bool {
        match expr {
            PlanExpr::BinaryOp {
                left,
                op: ast::BinaryOp::Minus,
                right,
                ..
            } => {
                matches!(
                    &**left,
                    PlanExpr::Column { name, .. } if name == "__cte_max"
                ) && matches!(
                    &**right,
                    PlanExpr::Literal {
                        value: ScalarValue::Float64(v),
                        ..
                    } if (*v - 0.01).abs() < f64::EPSILON
                )
            }
            _ => expr_children(expr)
                .into_iter()
                .any(expr_contains_cte_max_minus_cent),
        }
    }

    fn plan_children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
        match plan {
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::SemiJoin { left, right, .. }
            | LogicalPlan::AntiJoin { left, right, .. }
            | LogicalPlan::Intersect { left, right }
            | LogicalPlan::Except { left, right } => vec![left, right],
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Explain { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::RollupAggregate { input, .. }
            | LogicalPlan::PartialAggregate { input, .. }
            | LogicalPlan::FinalAggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. } => vec![input],
            LogicalPlan::ScalarSubquery { subplan } => vec![subplan],
            LogicalPlan::UnionAll { inputs } => inputs.iter().collect(),
            LogicalPlan::CreateTableAsSelect { source, .. }
            | LogicalPlan::InsertInto { source, .. }
            | LogicalPlan::CreateView { plan: source, .. } => vec![source],
            _ => vec![],
        }
    }

    fn plan_exprs(plan: &LogicalPlan) -> Vec<&PlanExpr> {
        match plan {
            LogicalPlan::Projection { exprs, .. } => exprs.iter().collect(),
            LogicalPlan::Filter { predicate, .. } => vec![predicate],
            LogicalPlan::Join {
                condition: JoinCondition::On(expr),
                ..
            } => vec![expr],
            LogicalPlan::Aggregate {
                group_by,
                aggr_exprs,
                ..
            }
            | LogicalPlan::PartialAggregate {
                group_by,
                aggr_exprs,
                ..
            }
            | LogicalPlan::FinalAggregate {
                group_by,
                aggr_exprs,
                ..
            } => group_by.iter().chain(aggr_exprs.iter()).collect(),
            LogicalPlan::Sort { order_by, .. } => order_by.iter().map(|s| &s.expr).collect(),
            LogicalPlan::SemiJoin {
                left_key,
                right_key,
                residual,
                ..
            }
            | LogicalPlan::AntiJoin {
                left_key,
                right_key,
                residual,
                ..
            } => {
                let mut exprs = vec![left_key, right_key];
                if let Some(residual) = residual {
                    exprs.push(residual);
                }
                exprs
            }
            LogicalPlan::Window { functions, .. } => functions
                .iter()
                .flat_map(|f| {
                    f.args
                        .iter()
                        .chain(f.partition_by.iter())
                        .chain(f.order_by.iter().map(|s| &s.expr))
                })
                .collect(),
            _ => vec![],
        }
    }

    fn expr_children(expr: &PlanExpr) -> Vec<&PlanExpr> {
        match expr {
            PlanExpr::BinaryOp { left, right, .. } => vec![left, right],
            PlanExpr::UnaryOp { expr, .. }
            | PlanExpr::IsNull { expr, .. }
            | PlanExpr::IsNotNull { expr, .. }
            | PlanExpr::Cast { expr, .. } => vec![expr],
            PlanExpr::Function { args, .. } => args.iter().collect(),
            PlanExpr::Between {
                expr, low, high, ..
            } => vec![expr, low, high],
            PlanExpr::InList { expr, list, .. } => {
                let mut out = vec![expr.as_ref()];
                out.extend(list);
                out
            }
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
            } => {
                let mut out = Vec::new();
                if let Some(operand) = operand {
                    out.push(operand.as_ref());
                }
                for (condition, result) in when_clauses {
                    out.push(condition);
                    out.push(result);
                }
                if let Some(else_result) = else_result {
                    out.push(else_result.as_ref());
                }
                out
            }
            _ => vec![],
        }
    }

    // ---------------------------------------------------------------
    // Display tests (tasks 4.1, 4.2)
    // ---------------------------------------------------------------

    #[test]
    fn test_plan_expr_display() {
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".into(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int64(1),
                span: None,
            }),
            span: None,
        };
        assert_eq!(expr.to_string(), "a > 1");
    }

    #[test]
    fn test_plan_expr_display_function() {
        let expr = PlanExpr::Function {
            order_by: None,
            name: "COUNT".into(),
            args: vec![PlanExpr::Wildcard],
            distinct: false,
            span: None,
        };
        assert_eq!(expr.to_string(), "COUNT(*)");
    }

    #[test]
    fn test_plan_expr_display_between() {
        let expr = PlanExpr::Between {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "x".into(),
                span: None,
            }),
            negated: false,
            low: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int64(1),
                span: None,
            }),
            high: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int64(10),
                span: None,
            }),
            span: None,
        };
        assert_eq!(expr.to_string(), "x BETWEEN 1 AND 10");
    }

    #[tokio::test]
    async fn test_logical_plan_display() {
        let plan = plan_sql("SELECT name FROM users WHERE id > 10")
            .await
            .unwrap();
        let display = plan.to_string();
        assert!(display.contains("Projection"));
        assert!(display.contains("Filter"));
        assert!(display.contains("TableScan"));
    }

    // ---------------------------------------------------------------
    // Simple SELECT (task 4.3)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_simple_select() {
        let plan = plan_sql("SELECT id, name FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, schema, .. } => {
                assert_eq!(exprs.len(), 2);
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0].name, "id");
                assert_eq!(schema[1].name, "name");
            }
            _ => panic!("expected Projection, got: {plan:?}"),
        }
    }

    // ---------------------------------------------------------------
    // SELECT with WHERE (task 4.4)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_select_with_where() {
        let plan = plan_sql("SELECT name FROM users WHERE id > 10")
            .await
            .unwrap();
        // Should be Projection(Filter(TableScan))
        match &plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Filter { predicate, input } => {
                    assert!(predicate.to_string().contains(">"));
                    assert!(matches!(input.as_ref(), LogicalPlan::TableScan { .. }));
                }
                _ => panic!("expected Filter under Projection"),
            },
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // SELECT * wildcard expansion (task 4.5)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_select_wildcard() {
        let plan = plan_sql("SELECT * FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, schema, .. } => {
                assert_eq!(exprs.len(), 3, "users has 3 columns");
                assert_eq!(schema[0].name, "id");
                assert_eq!(schema[1].name, "name");
                assert_eq!(schema[2].name, "age");
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // SELECT with JOIN (task 4.6)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_select_with_join() {
        let plan = plan_sql(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
        )
        .await
        .unwrap();

        match &plan {
            LogicalPlan::Projection { input, schema, .. } => {
                assert_eq!(schema.len(), 2);
                assert_eq!(schema[0].name, "name");
                assert_eq!(schema[1].name, "amount");
                assert!(matches!(input.as_ref(), LogicalPlan::Join { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // SELECT with GROUP BY (task 4.7)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_select_distinct_plans_as_group_by_all_columns() {
        let plan = plan_sql("SELECT DISTINCT name FROM users").await.unwrap();
        let LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            schema,
        } = &plan
        else {
            panic!("expected Aggregate, got {plan:?}");
        };
        assert!(matches!(input.as_ref(), LogicalPlan::Projection { .. }));
        assert_eq!(group_by.len(), 1);
        assert!(aggr_exprs.is_empty());
        assert_eq!(schema[0].name, "name");

        // Distributed: like any GROUP BY with no aggregate functions, each
        // source partition de-duplicates its rows and the root de-duplicates
        // the partials.
        let root = crate::fragment::PlanFragmenter::new().fragment(plan);
        assert_eq!(
            root.output_partitioning,
            crate::fragment::PartitioningScheme::Single
        );
        assert!(matches!(root.root, LogicalPlan::FinalAggregate { .. }));
        assert_eq!(root.source_fragments.len(), 1);
        assert!(matches!(
            root.source_fragments[0].root,
            LogicalPlan::PartialAggregate { .. }
        ));
    }

    #[tokio::test]
    async fn test_select_with_group_by() {
        let plan = plan_sql("SELECT name, COUNT(*) FROM users GROUP BY name")
            .await
            .unwrap();

        // Should be Projection(Aggregate(TableScan))
        match &plan {
            LogicalPlan::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // SELECT with ORDER BY, LIMIT, OFFSET (task 4.8)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_select_with_order_by() {
        let plan = plan_sql("SELECT id, name FROM users ORDER BY id DESC")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Sort { order_by, .. } => {
                assert_eq!(order_by.len(), 1);
                assert!(!order_by[0].asc);
            }
            _ => panic!("expected Sort at top"),
        }
    }

    #[tokio::test]
    async fn test_order_by_column_not_in_select_list_sorts_below_projection() {
        let plan = plan_sql("SELECT name FROM users ORDER BY id DESC")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { input, schema, .. } => {
                assert_eq!(schema.len(), 1);
                assert_eq!(schema[0].name, "name");
                match input.as_ref() {
                    LogicalPlan::Sort { order_by, .. } => {
                        assert_eq!(order_by.len(), 1);
                        assert!(!order_by[0].asc);
                    }
                    other => panic!("expected Sort below Projection, got {other:?}"),
                }
            }
            other => panic!("expected Projection at top, got {other:?}"),
        }
        // Aliased output columns still resolve when sorting below.
        plan_sql("SELECT name AS n FROM users ORDER BY n, id")
            .await
            .unwrap();
        // Genuinely unknown columns still fail.
        assert!(plan_sql("SELECT name FROM users ORDER BY nope")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_table_scan_qualified_only_on_session_view() {
        fn scan_ref(plan: &LogicalPlan) -> arneb_common::types::TableReference {
            match plan {
                LogicalPlan::TableScan { table, .. } => table.clone(),
                LogicalPlan::Projection { input, .. } => scan_ref(input),
                other => panic!("unexpected node {other:?}"),
            }
        }
        let root = test_catalog();
        let stmt = arneb_sql_parser::parse("SELECT id FROM users").unwrap();

        let plan = QueryPlanner::new(&root)
            .plan_statement(&stmt)
            .await
            .unwrap();
        let table = scan_ref(&plan);
        assert_eq!((table.catalog, table.schema), (None, None));

        // Same defaults, but a session view: the reference is qualified.
        let view = root.with_session_defaults("default", "public");
        let plan = QueryPlanner::new(&view)
            .plan_statement(&stmt)
            .await
            .unwrap();
        let table = scan_ref(&plan);
        assert_eq!(table.catalog.as_deref(), Some("default"));
        assert_eq!(table.schema.as_deref(), Some("public"));
        assert_eq!(table.table, "users");
    }

    #[tokio::test]
    async fn test_select_with_limit_offset() {
        let plan = plan_sql("SELECT id FROM users LIMIT 10 OFFSET 5")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Limit { limit, offset, .. } => {
                assert_eq!(*limit, Some(10));
                assert_eq!(*offset, Some(5));
            }
            _ => panic!("expected Limit at top"),
        }
    }

    #[tokio::test]
    async fn limit_offset_accept_constant_integer_expressions() {
        for (sql, want_limit, want_offset) in [
            ("SELECT id FROM users LIMIT CAST(2 AS INTEGER)", 2, None),
            (
                "SELECT id FROM users LIMIT 2::BIGINT OFFSET 1::INTEGER",
                2,
                Some(1),
            ),
            ("SELECT id FROM users LIMIT 1 + 2", 3, None),
        ] {
            match plan_sql(sql).await.unwrap() {
                LogicalPlan::Limit { limit, offset, .. } => {
                    assert_eq!(limit, Some(want_limit), "{sql}");
                    assert_eq!(offset, want_offset, "{sql}");
                }
                other => panic!("expected Limit for {sql}, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn limit_rejects_non_constant_and_negative() {
        for sql in [
            "SELECT id FROM users LIMIT id",
            "SELECT id FROM users LIMIT -1",
            "SELECT id FROM users LIMIT 'x'",
        ] {
            let err = plan_sql(sql).await.unwrap_err().to_string();
            assert!(err.contains("LIMIT/OFFSET must be"), "{sql}: {err}");
        }
    }

    // ---------------------------------------------------------------
    // EXPLAIN (task 4.9)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_explain() {
        let plan = plan_sql("EXPLAIN SELECT id FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Explain { input, analyze } => {
                assert!(!analyze, "plain EXPLAIN should not set analyze");
                assert!(matches!(input.as_ref(), LogicalPlan::Projection { .. }));
            }
            _ => panic!("expected Explain"),
        }
    }

    #[tokio::test]
    async fn test_explain_analyze_flag() {
        let plan = plan_sql("EXPLAIN ANALYZE SELECT id FROM users")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Explain { input, analyze } => {
                assert!(*analyze, "EXPLAIN ANALYZE should set analyze");
                assert!(matches!(input.as_ref(), LogicalPlan::Projection { .. }));
            }
            _ => panic!("expected Explain"),
        }
    }

    /// The dimension of `generate_subscripts` is any constant that folds to 1
    /// (psycopg 3 sends `1::INTEGER`); anything else is refused.
    #[tokio::test]
    async fn generate_subscripts_dim_is_a_constant_folding_to_1() {
        for dim in [
            "1",
            "1::INTEGER",
            "CAST(1 AS SMALLINT)",
            "(1)",
            "2 - 1",
            "CAST(1.2 AS INTEGER)",
        ] {
            let sql = format!("EXPLAIN SELECT generate_subscripts(ARRAY[id], {dim}) FROM users");
            let plan = plan_sql(&sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(
                plan.to_string()
                    .contains("ProjectSet: [generate_subscripts(ARRAY[id], 1)]"),
                "{sql}: {plan}"
            );
        }
        for dim in ["2", "2::INTEGER", "id", "NULL"] {
            let sql = format!("SELECT generate_subscripts(ARRAY[id], {dim}) FROM users");
            let err = plan_sql(&sql).await.unwrap_err().to_string();
            assert!(err.contains("dimension 1"), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn srfs_plan_a_project_set_below_the_projection() {
        let plan = plan_sql(
            "EXPLAIN SELECT id, unnest(ARRAY[id, age]) AS v, generate_subscripts(ARRAY[id], 1) \
             FROM users",
        )
        .await
        .unwrap();
        let text = plan.to_string();
        assert!(
            text.contains(
                "ProjectSet: [unnest(ARRAY[id, CAST(age AS Int64)]), \
                 generate_subscripts(ARRAY[id], 1)]"
            ),
            "{text}"
        );
        let LogicalPlan::Explain { input, .. } = &plan else {
            panic!("expected Explain");
        };
        let LogicalPlan::Projection { input, schema, .. } = input.as_ref() else {
            panic!("expected Projection, got {input}");
        };
        assert_eq!(schema[1].name, "v");
        let LogicalPlan::ProjectSet {
            srfs,
            schema: set_schema,
            ..
        } = input.as_ref()
        else {
            panic!("expected ProjectSet, got {input}");
        };
        assert_eq!(srfs.len(), 2);
        assert_eq!(srfs[0].0, SrfKind::Unnest);
        assert_eq!(srfs[1].0, SrfKind::GenerateSubscripts);
        // users(id, name, age), then one column per SRF.
        assert_eq!(set_schema.len(), 5);
        assert_eq!(set_schema[3].data_type, DataType::Int64);
        assert!(set_schema[3].nullable);
        assert_eq!(set_schema[4].data_type, DataType::Int32);
        // Workers receive plans as JSON.
        let json = serde_json::to_string(input.as_ref()).unwrap();
        let back: LogicalPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(back.to_string(), input.to_string());

        // Queries without SRFs keep their plan shape.
        let plain = plan_sql("SELECT id FROM users").await.unwrap();
        assert!(!plain.to_string().contains("ProjectSet"), "{plain}");
    }

    #[tokio::test]
    async fn expandarray_yields_a_struct_read_by_field() {
        let plan = plan_sql(
            "EXPLAIN SELECT (information_schema._pg_expandarray(ARRAY[id])).n, \
             information_schema._pg_expandarray(ARRAY[id]) AS k FROM users",
        )
        .await
        .unwrap();
        let LogicalPlan::Explain { input, .. } = &plan else {
            panic!("expected Explain");
        };
        let LogicalPlan::Projection {
            input,
            exprs,
            schema,
        } = input.as_ref()
        else {
            panic!("expected Projection, got {input}");
        };
        // PostgreSQL names `(srf).n` after the field.
        assert_eq!(schema[0].name, "n");
        assert_eq!(schema[0].data_type, DataType::Int32);
        assert!(matches!(exprs[0], PlanExpr::GetField(_)), "{}", exprs[0]);
        let element = DataType::Struct(vec![
            ("x".to_string(), DataType::Int64),
            ("n".to_string(), DataType::Int32),
        ]);
        assert_eq!(schema[1].data_type, element);
        let LogicalPlan::ProjectSet { srfs, .. } = input.as_ref() else {
            panic!("expected ProjectSet, got {input}");
        };
        assert!(srfs.iter().all(|(k, _)| *k == SrfKind::ExpandArray));
        // Workers receive plans as JSON.
        let json = serde_json::to_string(input.as_ref()).unwrap();
        let back: LogicalPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(back.to_string(), input.to_string());
    }

    #[tokio::test]
    async fn subscripts_and_field_access_are_typed() {
        let plan = plan_sql("SELECT (ARRAY[id, age])[2], (ARRAY[name])[id] FROM users")
            .await
            .unwrap();
        let LogicalPlan::Projection { exprs, schema, .. } = &plan else {
            panic!("expected Projection, got {plan}");
        };
        assert!(
            matches!(exprs[0], PlanExpr::ArrayElement(_)),
            "{}",
            exprs[0]
        );
        assert_eq!(schema[0].data_type, DataType::Int64);
        assert_eq!(schema[1].data_type, DataType::Utf8);
        let json = serde_json::to_string(&plan).unwrap();
        let back: LogicalPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(back.to_string(), plan.to_string());

        for (sql, want) in [
            (
                "SELECT (information_schema._pg_expandarray(ARRAY[1])).zz",
                "column \"zz\" not found in data type",
            ),
            ("SELECT (id).x FROM users", "not a composite type"),
            ("SELECT (id)[1] FROM users", "not an array"),
            (
                "SELECT (ARRAY[1])[name] FROM users",
                "array subscript must have type integer",
            ),
        ] {
            let err = plan_sql(sql).await.unwrap_err().to_string();
            assert!(err.contains(want), "{sql}: {err}");
        }
    }

    async fn plan_with(catalog: &CatalogManager, sql: &str) -> Result<LogicalPlan, PlanError> {
        let planner = QueryPlanner::new(catalog);
        planner
            .plan_statement(&arneb_sql_parser::parse(sql).expect("parse failed"))
            .await
    }

    /// The projected expressions and column names of a plain `SELECT`.
    fn projection(plan: &LogicalPlan) -> (Vec<String>, Vec<String>) {
        let LogicalPlan::Projection { exprs, schema, .. } = plan else {
            panic!("expected Projection, got {plan}");
        };
        (
            exprs.iter().map(ToString::to_string).collect(),
            schema.iter().map(|c| c.name.clone()).collect(),
        )
    }

    #[tokio::test]
    async fn session_functions_fold_and_keep_their_names() {
        let catalog = test_catalog().with_session_user("alice");
        let plan = plan_with(
            &catalog,
            "SELECT current_database(), current_catalog, current_user, session_user, \
             current_schema, current_database() AS db",
        )
        .await
        .unwrap();
        let db = format!("'{}'", catalog.default_catalog());
        let schema = format!("'{}'", catalog.default_schema());
        assert_eq!(
            projection(&plan),
            (
                vec![
                    db.clone(),
                    db.clone(),
                    "'alice'".to_string(),
                    "'alice'".to_string(),
                    schema,
                    db
                ],
                [
                    "current_database",
                    "current_catalog",
                    "current_user",
                    "session_user",
                    "current_schema",
                    "db"
                ]
                .map(String::from)
                .to_vec()
            )
        );
        // Without a known user `current_user` does not fold; a bare
        // `current_database` is a column reference, as in PostgreSQL.
        let (exprs, _) = projection(&plan_sql("SELECT current_user").await.unwrap());
        assert!(!exprs[0].starts_with('\''), "{exprs:?}");
        assert!(plan_sql("SELECT current_database").await.is_err());
    }

    #[tokio::test]
    async fn privilege_functions_carry_the_session_privileges() {
        let catalog = test_catalog().with_session_user("alice");
        let plan = plan_with(
            &catalog,
            "SELECT has_table_privilege('t', 'select'), \
             has_any_column_privilege(current_user, 't', 'update'), \
             has_schema_privilege('alice', 's', 'usage')",
        )
        .await
        .unwrap();
        let LogicalPlan::Projection { exprs, schema, .. } = &plan else {
            panic!("expected Projection, got {plan}");
        };
        let privileges = catalog.session_privileges().await;
        for (expr, (kind, role)) in exprs.iter().zip([
            ("table", "alice"),
            ("any_column", "alice"),
            ("schema", "alice"),
        ]) {
            let PlanExpr::Function { name, args, .. } = expr else {
                panic!("expected a call, got {expr}");
            };
            assert_eq!(name, "__has_privilege");
            assert_eq!(args.len(), 9, "{expr}");
            assert_eq!(args[0].to_string(), format!("'{kind}'"));
            assert_eq!(args[1].to_string(), format!("'{role}'"));
            assert_eq!(args[4].to_string(), "'alice'");
            assert_eq!(
                args[5].to_string(),
                format!("'{}'", catalog.default_catalog())
            );
            // Every visible table is listed with all four privileges.
            let PlanExpr::Literal {
                value: ScalarValue::Utf8(tables),
                ..
            } = &args[8]
            else {
                panic!("tables literal: {}", args[8]);
            };
            let entries = tables
                .split(arneb_common::pg_oid::RELATION_ENTRY_SEP)
                .filter(|e| !e.is_empty())
                .count();
            assert_eq!(entries, privileges.tables.len());
            assert!(tables.ends_with("SIUD"), "{tables:?}");
        }
        assert!(schema.iter().all(|c| c.data_type == DataType::Boolean));
        let err = plan_sql("SELECT has_table_privilege('t')")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("takes 2 or 3 arguments"), "{err}");
    }

    #[tokio::test]
    async fn current_schemas_reads_the_session_search_path() {
        let plan = plan_sql("SELECT current_schemas(true)").await.unwrap();
        let LogicalPlan::Projection { exprs, schema, .. } = &plan else {
            panic!("expected Projection, got {plan}");
        };
        let PlanExpr::Function { args, .. } = &exprs[0] else {
            panic!("expected a call, got {}", exprs[0]);
        };
        // The catalog's default schema rides along as the search path.
        assert_eq!(
            args[1].to_string(),
            format!("'{}'", test_catalog().default_schema())
        );
        assert_eq!(
            schema[0].data_type,
            DataType::List(Box::new(DataType::Utf8))
        );
        // PostgreSQL has only current_schemas(boolean).
        for sql in [
            "SELECT current_schemas(true, 'x')",
            "SELECT current_schemas()",
        ] {
            let err = plan_sql(sql).await.unwrap_err().to_string();
            assert!(err.contains("exactly one boolean argument"), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn srf_outside_a_select_item_is_refused() {
        for sql in [
            "SELECT id FROM users WHERE unnest(ARRAY[id]) = 1",
            "SELECT unnest(ARRAY[id]) + 1 FROM users",
            "SELECT id FROM users ORDER BY unnest(ARRAY[id])",
        ] {
            let err = plan_sql(sql).await.expect_err(sql).to_string();
            assert!(
                err.contains("set-returning function") && err.contains("not supported here"),
                "{sql}: {err}"
            );
        }
    }

    /// EXPLAIN output MUST be position-independent: two parses of the
    /// same query with different whitespace should produce byte-identical
    /// plan text (and JSON). This guards D7 — `#[serde(skip)]` on
    /// `PlanExpr.span`.
    #[tokio::test]
    async fn explain_is_position_independent() {
        let a = plan_sql("SELECT id FROM users WHERE id > 1").await.unwrap();
        let b = plan_sql("SELECT id  FROM  users  WHERE  id > 1")
            .await
            .unwrap();
        assert_eq!(a.to_string(), b.to_string(), "plan Display must be stable");

        // Serde JSON must also be stable across whitespace differences;
        // spans are skipped, so only logical fields serialize.
        let ja = serde_json::to_string(&a).unwrap();
        let jb = serde_json::to_string(&b).unwrap();
        assert_eq!(ja, jb, "plan JSON must be stable");
    }

    // ---------------------------------------------------------------
    // Error cases (task 4.10)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_table_not_found() {
        let err = plan_sql("SELECT * FROM nonexistent").await.unwrap_err();
        match err {
            PlanError::TableNotFound(name) => assert_eq!(name, "nonexistent"),
            _ => panic!("expected TableNotFound, got: {err:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_column_has_location_and_candidates() {
        let err = plan_sql("SELECT nmae FROM users").await.unwrap_err();
        match err {
            PlanError::ColumnNotFound {
                name,
                location,
                candidates,
            } => {
                assert_eq!(name, "nmae");
                assert_eq!(
                    location,
                    Some(arneb_common::error::Location { line: 1, column: 8 })
                );
                assert_eq!(candidates.first().map(String::as_str), Some("name"));
            }
            _ => panic!("expected ColumnNotFound, got: {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_column_not_found() {
        let err = plan_sql("SELECT nonexistent FROM users").await.unwrap_err();
        match err {
            PlanError::ColumnNotFound { name, .. } => assert_eq!(name, "nonexistent"),
            _ => panic!("expected ColumnNotFound, got: {err:?}"),
        }
    }

    // ---------------------------------------------------------------
    // Aliases, qualified refs, expressions (task 4.11)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_alias_in_projection() {
        let plan = plan_sql("SELECT name AS user_name FROM users")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "user_name");
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_qualified_column_reference() {
        let plan = plan_sql("SELECT users.name FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "name");
                assert_eq!(schema[0].data_type, DataType::Utf8);
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_expression_in_projection() {
        let plan = plan_sql("SELECT id + 1 FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert!(matches!(exprs[0], PlanExpr::BinaryOp { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn plan_expr_type_matches_expr_to_column_info_for_complex_expressions() {
        let catalog = test_catalog();
        let planner = QueryPlanner::new(&catalog);
        let mut ctx = PlanningContext::new();
        ctx.columns.push((
            None,
            ColumnInfo {
                name: "price".into(),
                data_type: DataType::Decimal128 {
                    precision: 15,
                    scale: 2,
                },
                nullable: true,
            },
        ));
        ctx.columns.push((
            None,
            ColumnInfo {
                name: "discount".into(),
                data_type: DataType::Decimal128 {
                    precision: 15,
                    scale: 2,
                },
                nullable: true,
            },
        ));
        ctx.columns.push((
            None,
            ColumnInfo {
                name: "p_type".into(),
                data_type: DataType::Utf8,
                nullable: true,
            },
        ));
        let schema: Vec<ColumnInfo> = ctx.columns.iter().map(|(_, col)| col.clone()).collect();
        let dec_col = |index: usize, name: &str| PlanExpr::Column {
            index,
            name: name.to_string(),
            span: None,
        };
        let dec_lit = |value| PlanExpr::Literal {
            value: ScalarValue::Decimal128 {
                value,
                precision: 15,
                scale: 2,
            },
            span: None,
        };
        let promo_predicate = PlanExpr::BinaryOp {
            left: Box::new(dec_col(2, "p_type")),
            op: ast::BinaryOp::Like,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Utf8("PROMO%".into()),
                span: None,
            }),
            span: None,
        };
        let discounted_price = PlanExpr::BinaryOp {
            left: Box::new(dec_col(0, "price")),
            op: ast::BinaryOp::Multiply,
            right: Box::new(PlanExpr::BinaryOp {
                left: Box::new(dec_lit(100)),
                op: ast::BinaryOp::Minus,
                right: Box::new(dec_col(1, "discount")),
                span: None,
            }),
            span: None,
        };
        let promo_case = PlanExpr::CaseExpr {
            operand: None,
            when_clauses: vec![(promo_predicate, discounted_price.clone())],
            else_result: Some(Box::new(PlanExpr::Literal {
                value: ScalarValue::Int64(0),
                span: None,
            })),
            span: None,
        };
        let scalar_subquery = PlanExpr::ScalarSubquery {
            subplan: Box::new(LogicalPlan::Projection {
                input: Box::new(LogicalPlan::OneRow),
                exprs: vec![PlanExpr::Literal {
                    value: ScalarValue::Int64(1),
                    span: None,
                }],
                schema: vec![ColumnInfo {
                    name: "scalar".into(),
                    data_type: DataType::Int64,
                    nullable: false,
                }],
            }),
            span: None,
        };
        let expressions = vec![
            discounted_price,
            promo_case.clone(),
            PlanExpr::Function {
                order_by: None,
                name: "SUM".into(),
                args: vec![promo_case],
                distinct: false,
                span: None,
            },
            PlanExpr::UnaryOp {
                op: ast::UnaryOp::Minus,
                expr: Box::new(dec_col(0, "price")),
                span: None,
            },
            PlanExpr::Cast {
                expr: Box::new(dec_col(0, "price")),
                data_type: DataType::Float64,
                span: None,
            },
            PlanExpr::Function {
                order_by: None,
                name: "COUNT".into(),
                args: vec![PlanExpr::Wildcard],
                distinct: false,
                span: None,
            },
            scalar_subquery,
        ];

        for expr in expressions {
            assert_eq!(
                crate::analyzer::plan_expr_type(&expr, &schema).unwrap_or(DataType::Null),
                planner.expr_to_column_info(&expr, &ctx).data_type,
                "type mismatch for {expr}"
            );
        }
    }

    #[tokio::test]
    async fn test_table_alias() {
        let plan = plan_sql("SELECT u.name FROM users u").await.unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "name");
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_cross_join_implicit() {
        let plan = plan_sql("SELECT * FROM users, orders").await.unwrap();
        match &plan {
            LogicalPlan::Projection { input, exprs, .. } => {
                assert_eq!(exprs.len(), 6); // 3 + 3
                assert!(matches!(
                    input.as_ref(),
                    LogicalPlan::Join {
                        join_type: ast::JoinType::Cross,
                        ..
                    }
                ));
            }
            _ => panic!("expected Projection"),
        }
    }

    // -- comma joins inside other operators and in an order with no
    // predicate between the first two tables (TPC-DS shapes) --

    fn cross_joins(plan: &LogicalPlan) -> usize {
        plan.to_string().matches("Join: Cross").count()
    }

    #[tokio::test]
    async fn comma_join_under_a_window_becomes_inner() {
        let plan = plan_sql(
            "SELECT u.name, sum(o.amount) OVER (PARTITION BY u.name) \
             FROM users u, orders o WHERE u.id = o.user_id",
        )
        .await
        .unwrap();
        assert_eq!(cross_joins(&plan), 0, "{plan}");
    }

    #[tokio::test]
    async fn comma_joins_in_both_union_all_branches_become_inner() {
        let plan = plan_sql(
            "SELECT u.name FROM users u, orders o WHERE u.id = o.user_id \
             UNION ALL \
             SELECT u.name FROM users u, orders o WHERE o.user_id = u.id",
        )
        .await
        .unwrap();
        assert_eq!(cross_joins(&plan), 0, "{plan}");
    }

    #[tokio::test]
    async fn comma_join_whose_first_two_tables_share_no_predicate_is_reordered() {
        // o1 and u are only connected through o2: FROM order alone would
        // cross o1 with u first.
        let plan = plan_sql(
            "SELECT u.name FROM orders o1, users u, orders o2 \
             WHERE o1.user_id = o2.order_id AND u.id = o2.user_id",
        )
        .await
        .unwrap();
        assert_eq!(cross_joins(&plan), 0, "{plan}");
        assert_eq!(plan.to_string().matches("Join: Inner").count(), 2, "{plan}");
    }

    #[tokio::test]
    async fn comma_join_reorder_ignores_predicate_order() {
        let plan = plan_sql(
            "SELECT u.name FROM orders o1, users u, orders o2 \
             WHERE u.id = o2.user_id AND o1.user_id = o2.order_id",
        )
        .await
        .unwrap();
        assert_eq!(cross_joins(&plan), 0, "{plan}");
    }

    #[tokio::test]
    async fn comma_join_without_a_join_predicate_stays_cross() {
        // Only one-sided predicates: this really is a cross product, and
        // no condition may be invented.
        for sql in [
            "SELECT u.name FROM users u, orders o",
            "SELECT u.name FROM users u, orders o WHERE u.age > 30 AND o.amount > 1",
        ] {
            let plan = plan_sql(sql).await.unwrap();
            assert_eq!(cross_joins(&plan), 1, "{sql}\n{plan}");
        }
    }

    #[tokio::test]
    async fn test_qualified_wildcard() {
        let plan = plan_sql("SELECT users.* FROM users JOIN orders ON users.id = orders.user_id")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, schema, .. } => {
                assert_eq!(exprs.len(), 3); // only users columns
                assert_eq!(schema[0].name, "id");
                assert_eq!(schema[1].name, "name");
                assert_eq!(schema[2].name, "age");
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_schema_propagation_through_filter() {
        let plan = plan_sql("SELECT * FROM users WHERE age > 18")
            .await
            .unwrap();
        let schema = plan.schema();
        assert_eq!(schema.len(), 3);
    }

    // ---------------------------------------------------------------
    // CASE expression planner tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_searched_case_in_select() {
        let plan = plan_sql("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert!(matches!(
                    &exprs[0],
                    PlanExpr::CaseExpr { operand: None, .. }
                ));
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_simple_case_in_select() {
        let plan = plan_sql(
            "SELECT CASE age WHEN 18 THEN 'eighteen' WHEN 21 THEN 'twenty-one' END FROM users",
        )
        .await
        .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => match &exprs[0] {
                PlanExpr::CaseExpr {
                    operand,
                    when_clauses,
                    ..
                } => {
                    assert!(operand.is_some());
                    assert_eq!(when_clauses.len(), 2);
                }
                other => panic!("expected CaseExpr, got {other:?}"),
            },
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_coalesce_in_select() {
        let plan = plan_sql("SELECT COALESCE(name, 'unknown') FROM users")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert!(matches!(&exprs[0], PlanExpr::CaseExpr { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_case_in_where() {
        let plan =
            plan_sql("SELECT name FROM users WHERE CASE WHEN age > 18 THEN true ELSE false END")
                .await
                .unwrap();
        match &plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Filter { predicate, .. } => {
                    assert!(matches!(predicate, PlanExpr::CaseExpr { .. }));
                }
                _ => panic!("expected Filter"),
            },
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // HAVING clause planner tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_having_adds_filter_after_aggregate() {
        let plan = plan_sql("SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1")
            .await
            .unwrap();
        // Should be Projection(Filter(Aggregate(TableScan)))
        match &plan {
            LogicalPlan::Projection { input, .. } => match input.as_ref() {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
                }
                _ => panic!("expected Filter after Aggregate"),
            },
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_no_having_no_extra_filter() {
        let plan = plan_sql("SELECT name, COUNT(*) FROM users GROUP BY name")
            .await
            .unwrap();
        // Should be Projection(Aggregate(TableScan)) — no Filter
        match &plan {
            LogicalPlan::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // Bug fix regression tests
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn test_sum_non_group_by_column() {
        // Bug: SUM(age) failed with "column not found: age" after GROUP BY
        let plan = plan_sql("SELECT name, SUM(age) FROM users GROUP BY name")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_multiple_aggregates_with_group_by() {
        let plan = plan_sql("SELECT name, SUM(age), COUNT(*) FROM users GROUP BY name")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs.len(), 3);
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_implicit_aggregate_no_group_by() {
        // Bug: SELECT SUM(age) FROM users failed — SUM treated as scalar function
        let plan = plan_sql("SELECT SUM(age) FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
                if let LogicalPlan::Aggregate { group_by, .. } = input.as_ref() {
                    assert!(
                        group_by.is_empty(),
                        "implicit aggregate should have empty group_by"
                    );
                }
            }
            _ => panic!("expected Projection(Aggregate)"),
        }
    }

    #[tokio::test]
    async fn test_count_star_no_group_by() {
        let plan = plan_sql("SELECT COUNT(*) FROM users").await.unwrap();
        match &plan {
            LogicalPlan::Projection { input, .. } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Aggregate { .. }));
            }
            _ => panic!("expected Projection(Aggregate)"),
        }
    }

    #[tokio::test]
    async fn test_order_by_aggregate() {
        // Bug: ORDER BY SUM(age) failed with "column not found"
        let plan =
            plan_sql("SELECT name, SUM(age) FROM users GROUP BY name ORDER BY SUM(age) DESC")
                .await
                .unwrap();
        match &plan {
            LogicalPlan::Sort { order_by, .. } => {
                assert_eq!(order_by.len(), 1);
                assert!(!order_by[0].asc);
            }
            _ => panic!("expected Sort at top"),
        }
    }

    #[tokio::test]
    async fn test_join_with_aggregate() {
        let plan = plan_sql(
            "SELECT users.name, SUM(orders.amount) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name",
        ).await.unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // PB-001: self-join alias collapse in SELECT after GROUP BY
    // ---------------------------------------------------------------

    /// Regression for PB-001. When a self-join aliases the same table
    /// twice (`users u1`, `users u2`) and the SELECT list projects both
    /// aliases' instances of a shared column (`name`), the projection
    /// must point at two *different* aggregate-output slots — one per
    /// alias. Historically both collapsed to the first alias's slot.
    #[tokio::test]
    async fn test_self_join_alias_projection_does_not_collapse() {
        let plan = plan_sql(
            "SELECT u1.name, u2.name, COUNT(*) \
             FROM users u1 JOIN users u2 ON u1.id = u2.id \
             GROUP BY u1.name, u2.name",
        )
        .await
        .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, schema, .. } => {
                assert_eq!(exprs.len(), 3, "three projected columns");
                // First two projections must resolve to *distinct* group-by
                // slots (index 0 for u1.name, index 1 for u2.name). The
                // historical bug collapsed both onto index 0.
                let idx0 = match &exprs[0] {
                    PlanExpr::Column { index, .. } => *index,
                    other => panic!("expected Column at exprs[0], got {other:?}"),
                };
                let idx1 = match &exprs[1] {
                    PlanExpr::Column { index, .. } => *index,
                    other => panic!("expected Column at exprs[1], got {other:?}"),
                };
                assert_ne!(
                    idx0, idx1,
                    "u1.name and u2.name must project to different slots, got {idx0} and {idx1}",
                );
                assert_eq!(schema.len(), 3);
            }
            _ => panic!("expected Projection"),
        }
    }

    // ---------------------------------------------------------------
    // PB-002: multiple same-function aggregates collide
    // ---------------------------------------------------------------

    /// Regression for PB-002. When a SELECT contains two aggregates
    /// sharing the same function name (both `SUM`), each must resolve
    /// to its own aggregate-output slot. Historically a name-prefix
    /// fallback in `find_aggregate_index` returned the first slot for
    /// *every* `SUM(...)` projection.
    /// Regression: AST `Nested(...)` nodes (parenthesized sub-expressions)
    /// must be unwrapped when normalizing the SELECT-list aggregate for
    /// slot lookup, because `plan_expr` strips them and the stored
    /// PlanExpr display has no parens. TPC-H Q08/Q14 use
    /// `SUM(x * (1 - y))` which previously mismatched and fell into the
    /// post-aggregate resolution branch, failing with `column not found`.
    #[tokio::test]
    async fn test_aggregate_with_nested_parens_resolves() {
        let plan = plan_sql("SELECT SUM(id * (1 - id)) AS s, SUM(age) AS a FROM users")
            .await
            .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert_eq!(exprs.len(), 2);
                // Both SELECT items must resolve to aggregate-output
                // column references, not re-plan their arguments.
                for (i, e) in exprs.iter().enumerate() {
                    match e {
                        PlanExpr::Column { .. } => {}
                        other => panic!("exprs[{i}] expected Column, got {other:?}"),
                    }
                }
            }
            _ => panic!("expected Projection"),
        }
    }

    #[tokio::test]
    async fn test_two_same_function_aggregates_do_not_collide() {
        // Qualified column reference inside the second SUM is the
        // real trigger for PB-002: the AST Display of
        // `SUM(users.id)` differs from the PlanExpr Display of the
        // resolved aggregate (`SUM(id)`), so the exact-match loop in
        // `find_aggregate_index` misses and the name-prefix fallback
        // collapses onto the *first* SUM.
        let plan = plan_sql(
            "SELECT SUM(CASE WHEN age > 18 THEN id ELSE 0 END) AS guarded, \
                    SUM(users.id) AS total \
             FROM users",
        )
        .await
        .unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, input, .. } => {
                assert_eq!(exprs.len(), 2);
                let idx0 = match &exprs[0] {
                    PlanExpr::Column { index, .. } => *index,
                    other => panic!("expected Column at exprs[0], got {other:?}"),
                };
                let idx1 = match &exprs[1] {
                    PlanExpr::Column { index, .. } => *index,
                    other => panic!("expected Column at exprs[1], got {other:?}"),
                };
                assert_ne!(
                    idx0, idx1,
                    "SUM(CASE..) and SUM(id) must resolve to different aggregate slots",
                );
                // Underlying Aggregate must actually carry two distinct
                // aggregate expressions — otherwise both projection
                // indices point into a schema of only one aggregate.
                match input.as_ref() {
                    LogicalPlan::Aggregate { aggr_exprs, .. } => {
                        assert_eq!(
                            aggr_exprs.len(),
                            2,
                            "two SUM(...) aggregates should be preserved, got {}",
                            aggr_exprs.len()
                        );
                    }
                    other => panic!("expected Aggregate input, got {other:?}"),
                }
            }
            _ => panic!("expected Projection"),
        }
    }

    /// An aggregate with `ORDER BY` beside another aggregate: the SELECT item
    /// must find its own aggregate slot, whose name carries the planned
    /// `ORDER BY` (SQLAlchemy's pk/unique/index reflection queries).
    #[tokio::test]
    async fn test_ordered_aggregate_beside_another_resolves() {
        for sql in [
            "SELECT array_agg(u.name ORDER BY u.id) AS a, count(*) AS c FROM users u",
            "SELECT count(*) AS c, array_agg(name ORDER BY id DESC) AS a, \
                    array_agg(name ORDER BY id DESC NULLS LAST) AS b FROM users",
        ] {
            let plan = plan_sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            let LogicalPlan::Projection { exprs, input, .. } = &plan else {
                panic!("expected Projection: {plan:?}");
            };
            let LogicalPlan::Aggregate { aggr_exprs, .. } = input.as_ref() else {
                panic!("expected Aggregate input");
            };
            assert_eq!(aggr_exprs.len(), exprs.len(), "{sql}");
            let mut slots: Vec<usize> = exprs
                .iter()
                .map(|e| match e {
                    PlanExpr::Column { index, .. } => *index,
                    other => panic!("{sql}: expected Column, got {other:?}"),
                })
                .collect();
            slots.sort_unstable();
            slots.dedup();
            assert_eq!(slots.len(), exprs.len(), "{sql}: slots collide");
        }
    }

    /// `percentile_*(f) WITHIN GROUP (ORDER BY x)`: the fraction is a
    /// constant in [0, 1], `percentile_cont` is DOUBLE over a
    /// numeric key, `percentile_disc` keeps the key's type.
    #[tokio::test]
    async fn test_percentile_calls_are_validated_and_typed() {
        let plan = plan_sql(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id), \
                    percentile_disc(1) WITHIN GROUP (ORDER BY name DESC) FROM users",
        )
        .await
        .unwrap();
        let LogicalPlan::Projection { input, schema, .. } = &plan else {
            panic!("expected Projection: {plan:?}");
        };
        assert_eq!(schema[0].data_type, DataType::Float64);
        assert_eq!(schema[1].data_type, DataType::Utf8);
        let LogicalPlan::Aggregate { aggr_exprs, .. } = input.as_ref() else {
            panic!("expected Aggregate input");
        };
        // `percentile_disc(f, name)` ordered by `name DESC`.
        let PlanExpr::Function { args, order_by, .. } = &aggr_exprs[1] else {
            panic!("expected Function");
        };
        assert_eq!(args.len(), 2);
        assert!(matches!(&args[1], PlanExpr::Column { name, .. } if name == "name"));
        let order_by = order_by.as_deref().expect("WITHIN GROUP key");
        assert_eq!(order_by.len(), 1);
        assert!(!order_by[0].asc);
        for (sql, want) in [
            (
                "SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY id) FROM users",
                "between 0 and 1",
            ),
            (
                "SELECT percentile_disc(-0.1) WITHIN GROUP (ORDER BY id) FROM users",
                "between 0 and 1",
            ),
            (
                "SELECT percentile_cont(id) WITHIN GROUP (ORDER BY id) FROM users",
                "constant numeric fraction",
            ),
            (
                "SELECT percentile_cont(ARRAY[0.5]) WITHIN GROUP (ORDER BY id) FROM users",
                "constant numeric fraction",
            ),
            (
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY name) FROM users",
                "numeric",
            ),
        ] {
            let err = plan_sql(sql).await.expect_err(sql).to_string();
            assert!(err.contains(want), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn test_logical_plan_serialization_roundtrip() {
        let plan = plan_sql(
            "SELECT users.name, SUM(orders.amount) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name",
        ).await.unwrap();

        let json = serde_json::to_string(&plan).unwrap();
        assert!(!json.is_empty());

        let deserialized: LogicalPlan = serde_json::from_str(&json).unwrap();
        // Verify structure preserved
        let schema = deserialized.schema();
        assert_eq!(schema.len(), plan.schema().len());
    }
}
