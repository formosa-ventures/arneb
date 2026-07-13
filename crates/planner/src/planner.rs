//! Query planner that converts a parsed SQL AST into a logical plan.

use async_recursion::async_recursion;
use std::sync::OnceLock;

use arneb_catalog::CatalogManager;
use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType, ScalarValue};
use arneb_sql_parser::ast;

use crate::analyzer::{plan_expr_type, Analyzer, AnalyzerContext};
use crate::plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr, WindowFunctionDef};

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
        let enabled = std::env::var("ARNEB_CTE_SELF_AGG_WINDOW").is_ok_and(|v| v == "1");
        tracing::info!(
            ARNEB_CTE_SELF_AGG_WINDOW = enabled,
            "ARNEB_CTE_SELF_AGG_WINDOW effective value (default off; =1 to rewrite CTE self-aggregate scalar subqueries to windows)"
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
}

/// Tracks the available columns from resolved tables during planning.
/// Each entry is (optional_qualifier, column_info, global_index).
struct PlanningContext {
    /// All columns available in the current scope.
    /// (qualifier, column_info)
    columns: Vec<(Option<String>, ColumnInfo)>,
}

impl PlanningContext {
    fn new() -> Self {
        Self {
            columns: Vec::new(),
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
            name: name.to_string(),
            location,
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

impl<'a> QueryPlanner<'a> {
    pub fn new(catalog: &'a CatalogManager) -> Self {
        Self {
            catalog,
            cte_plans: std::sync::Mutex::new(std::collections::HashMap::new()),
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

        let mut plan = self.plan_query_body(&query.body).await?;

        // ORDER BY
        if !query.order_by.is_empty() {
            let ctx = self.context_from_plan(&plan);

            // Build alias→index mapping from SELECT list for ORDER BY resolution
            let select_items = match &query.body {
                ast::QueryBody::Select(body) => Some(&body.projection),
                _ => None,
            };

            let mut sort_exprs = Vec::with_capacity(query.order_by.len());
            for ob in &query.order_by {
                let expr =
                    match self.resolve_order_by_expr_with_select(&ob.expr, &ctx, select_items) {
                        Some(resolved) => resolved,
                        None => self.plan_expr(&ob.expr, &ctx).await?,
                    };
                sort_exprs.push(SortExpr {
                    expr,
                    asc: ob.asc.unwrap_or(true),
                    nulls_first: ob.nulls_first.unwrap_or(false),
                });
            }
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: sort_exprs,
            };
        }

        // LIMIT / OFFSET
        let limit = self.eval_limit_expr(query.limit.as_deref())?;
        let offset = self.eval_limit_expr(query.offset.as_deref())?;
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

    /// Plan a QueryBody (SELECT or set operation).
    #[async_recursion]
    async fn plan_query_body(&self, body: &ast::QueryBody) -> Result<LogicalPlan, PlanError> {
        match body {
            ast::QueryBody::Select(select) => self.plan_select(select).await,
            ast::QueryBody::SetOperation { op, left, right } => {
                let left_plan = self.plan_query_body(left).await?;
                let right_plan = self.plan_query_body(right).await?;
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
    async fn plan_select(&self, body: &ast::SelectBody) -> Result<LogicalPlan, PlanError> {
        // 1. FROM clause → base plan + context
        let (mut plan, mut ctx) = self.plan_from(&body.from).await?;

        // 2. WHERE
        if let Some(selection) = &body.selection {
            // Pre-pass: extract `EXISTS (...)` / `NOT EXISTS (...)`
            // sub-predicates from the WHERE AND-tree and rewrite each
            // one as a SemiJoin / AntiJoin against the outer plan.
            // What remains becomes a plain FilterExec predicate.
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
                    },
                    SemiJoinKind::Anti => LogicalPlan::AntiJoin {
                        left: Box::new(plan),
                        right: Box::new(spec.right_plan),
                        left_key: spec.left_key,
                        right_key: spec.right_key,
                        residual: spec.residual,
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
                let predicate = self.plan_expr(&remaining, &ctx).await?;
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        // 3. GROUP BY / HAVING → Aggregate
        // Also handle implicit aggregate: SELECT SUM(x) FROM t (no GROUP BY but has aggregates)
        let has_group_by = !body.group_by.is_empty();
        let mut aggr_exprs = self.collect_aggregates(&body.projection, &ctx).await?;
        // Aggregates can appear in HAVING without being in SELECT (e.g.
        // `... GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`).
        // Pick those up too so the Aggregate node materializes them and
        // the HAVING rewrite can replace the call with a column ref.
        if let Some(having) = &body.having {
            self.extract_aggregates(having, &ctx, &mut aggr_exprs)
                .await?;
        }
        let has_aggregates = has_group_by || !aggr_exprs.is_empty();

        if has_aggregates {
            let mut group_by = Vec::with_capacity(body.group_by.len());
            for e in &body.group_by {
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

            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_by.clone(),
                aggr_exprs: aggr_exprs.clone(),
                schema: schema.clone(),
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
                let qualifier = if i < body.group_by.len() {
                    group_by_qualifier(&body.group_by[i])
                } else {
                    None
                };
                ctx.columns.push((qualifier, col.clone()));
            }

            // HAVING (applied after aggregation)
            // Rewrite aggregate expressions in HAVING to column references
            if let Some(having) = &body.having {
                let num_group_by = body.group_by.len();
                let rewritten = self.rewrite_aggregates_as_columns(having, &ctx, num_group_by);
                let predicate = self.plan_expr(&rewritten, &ctx).await?;
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        // 4. Projection (SELECT list)
        // After aggregate, SELECT expressions that ARE aggregate functions should reference
        // the aggregate output columns by index, not re-resolve their arguments.
        let (proj_exprs, proj_schema) = if has_aggregates {
            self.plan_aggregate_projection(&body.projection, &ctx, &body.group_by)
                .await?
        } else {
            self.plan_projection(&body.projection, &ctx).await?
        };

        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            exprs: proj_exprs,
            schema: proj_schema,
        };

        if cte_self_agg_window_enabled() {
            plan = rewrite_cte_self_agg_scalar_to_window(plan);
        }

        Ok(plan)
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
                    let spec = self
                        .decorrelate_in_subquery(*expr, *subquery, outer_ctx, negated)
                        .await?;
                    specs.push(spec);
                }
                other => residual.push(other),
            }
        }
        Ok((combine_and(residual), specs))
    }

    /// Decorrelate `<expr> IN (subquery)` / `<expr> NOT IN (subquery)`.
    /// Currently only the **uncorrelated** form is supported — the
    /// subquery may reference its own FROM tables but not the outer
    /// scope. The subquery must produce exactly one column.
    ///
    /// Maps to `SemiJoin` (IN) / `AntiJoin` (NOT IN) with
    /// `left_key = plan_expr(outer_expr, outer_ctx)` and
    /// `right_key = Column(0)` of the subquery output.
    ///
    /// NOT IN NULL semantics: SQL defines `x NOT IN (S)` as NULL when
    /// `x` is NULL or any element of `S` is NULL. AntiJoin treats both
    /// as false (no-match → keep). For TPC-H Q18-style use against
    /// non-nullable key columns the two coincide; nullable inputs
    /// would silently disagree with strict SQL, so we accept the
    /// approximation rather than synthesize the extra NULL-guard.
    async fn decorrelate_in_subquery(
        &self,
        outer_expr: ast::Expr,
        subquery: ast::Query,
        outer_ctx: &PlanningContext,
        negated: bool,
    ) -> Result<SemiJoinSpec, PlanError> {
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

        Ok(SemiJoinSpec {
            kind: if negated {
                SemiJoinKind::Anti
            } else {
                SemiJoinKind::Semi
            },
            right_plan: subplan,
            left_key,
            right_key,
            residual: None,
        })
    }

    /// Walk the WHERE AND-tree pulling out top-level conjuncts of the
    /// shape `<expr> <cmp> (correlated_scalar_subquery)` (and its
    /// mirror). Each match is decorrelated into a [`LeftJoinSpec`] and
    /// the AST node is rewritten to reference a synthesized column.
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

    /// If `part` is a comparison whose RHS (or LHS) is a correlated
    /// scalar subquery we can decorrelate, return the predicate with
    /// the subquery substituted for a placeholder column reference and
    /// push a `LeftJoinSpec` onto `specs`. Otherwise return `part`
    /// unchanged.
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

        let lhs_expr = *left;
        let rhs_expr = *right;
        let (sub_on_right, other, subquery) = if let ast::Expr::Subquery { query, .. } = &rhs_expr {
            (true, lhs_expr.clone(), (**query).clone())
        } else if let ast::Expr::Subquery { query, .. } = &lhs_expr {
            (false, rhs_expr.clone(), (**query).clone())
        } else {
            return Ok(ast::Expr::BinaryOp {
                left: Box::new(lhs_expr),
                op,
                right: Box::new(rhs_expr),
                span,
            });
        };

        let spec = match self
            .try_make_leftjoin_spec(subquery, outer_ctx, specs.len())
            .await?
        {
            Some(s) => s,
            None => {
                return Ok(ast::Expr::BinaryOp {
                    left: Box::new(lhs_expr),
                    op,
                    right: Box::new(rhs_expr),
                    span,
                });
            }
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
        specs.push(spec);

        let (new_left, new_right) = if sub_on_right {
            (Box::new(other), Box::new(placeholder))
        } else {
            (Box::new(placeholder), Box::new(other))
        };
        Ok(ast::Expr::BinaryOp {
            left: new_left,
            op,
            right: new_right,
            span,
        })
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

        // Decorrelate by GROUP BY-ing on every inner correlation
        // column. Multi-correlation (Q20) lands here naturally — the
        // outer LEFT JOIN later AND-s all the key pairs.
        let mut group_by_exprs: Vec<PlanExpr> = Vec::with_capacity(correlations.len());
        let mut inner_corr_infos: Vec<ColumnInfo> = Vec::with_capacity(correlations.len());
        for (_, inner_ast) in &correlations {
            let p = self.plan_expr(inner_ast, &inner_ctx).await?;
            inner_corr_infos.push(self.expr_to_column_info(&p, &inner_ctx));
            group_by_exprs.push(p);
        }

        // Collect aggregates referenced by the projection expression.
        let mut aggrs: Vec<PlanExpr> = Vec::new();
        self.extract_aggregates(&proj_expr, &inner_ctx, &mut aggrs)
            .await?;
        if aggrs.is_empty() {
            // No aggregate ⇒ the scalar subquery isn't a single-row
            // aggregation we can decorrelate this simply.
            return Ok(None);
        }

        let mut agg_schema: Vec<ColumnInfo> = inner_corr_infos.clone();
        for a in &aggrs {
            agg_schema.push(self.expr_to_column_info(a, &inner_ctx));
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
            self.rewrite_aggregates_as_columns(&proj_expr, &post_agg_ctx, num_group_by);
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
            let (right_plan, right_ctx) = self.plan_table_with_joins(twj).await?;
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
            let (right_plan, right_ctx) = self.plan_table_factor(&join.relation).await?;
            ctx.columns.extend(right_ctx.columns);

            let condition = match &join.condition {
                ast::JoinCondition::On(expr) => {
                    let plan_expr = self.plan_expr(expr, &ctx).await?;
                    JoinCondition::On(plan_expr)
                }
                ast::JoinCondition::Using(_) => {
                    return Err(PlanError::invalid_expression(
                        "USING join condition not yet supported".to_string(),
                    ));
                }
                ast::JoinCondition::None => JoinCondition::None,
            };

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
            ast::TableFactor::Table { name, alias } => {
                // Single-part names may resolve to a CTE before falling
                // back to the catalog. When a CTE is referenced more than
                // once in the same scope the plan is cloned (re-executed);
                // we accept the duplicated work in v1 — TPC-H Q15's
                // revenue0 CTE is small enough that it doesn't matter.
                if name.catalog.is_none() && name.schema.is_none() {
                    let cte_plan = self.cte_plans.lock().unwrap().get(&name.table).cloned();
                    if let Some(plan) = cte_plan {
                        let schema = plan.schema();
                        let qualifier = alias.as_deref().unwrap_or(&name.table);
                        let mut ctx = PlanningContext::new();
                        ctx.add_table_columns(Some(qualifier), &schema);
                        return Ok((plan, ctx));
                    }
                }

                let table_provider = self
                    .catalog
                    .resolve_table(name)
                    .await
                    .map_err(|_| PlanError::TableNotFound(name.to_string()))?;
                let schema = table_provider.schema();
                let qualifier = alias.as_deref().unwrap_or(&name.table);

                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(qualifier), &schema);

                let properties = table_provider.properties();
                let plan = LogicalPlan::TableScan {
                    table: name.clone(),
                    schema,
                    alias: alias.clone(),
                    properties,
                    dynamic_filters_consumed: Vec::new(),
                };

                Ok((plan, ctx))
            }
            ast::TableFactor::Subquery { query, alias } => {
                let plan = self.plan_query(query).await?;
                let schema = plan.schema();
                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(alias.as_str()), &schema);
                Ok((plan, ctx))
            }
        }
    }

    /// Convert an AST expression to a PlanExpr, resolving column references.
    #[allow(clippy::only_used_in_recursion)]
    #[async_recursion]
    async fn plan_expr(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
    ) -> Result<PlanExpr, PlanError> {
        let node_span = Some(expr.span());
        match expr {
            ast::Expr::Column { col_ref, .. } => {
                let (index, col_info) = ctx.resolve_column(
                    &col_ref.name,
                    col_ref.table.as_deref(),
                    Some(col_ref.span.start),
                )?;
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
            } => Ok(PlanExpr::BinaryOp {
                left: Box::new(self.plan_expr(left, ctx).await?),
                op: *op,
                right: Box::new(self.plan_expr(right, ctx).await?),
                span: node_span,
            }),
            ast::Expr::UnaryOp { op, expr, .. } => Ok(PlanExpr::UnaryOp {
                op: *op,
                expr: Box::new(self.plan_expr(expr, ctx).await?),
                span: node_span,
            }),
            ast::Expr::Function {
                name,
                args,
                distinct,
                ..
            } => {
                let mut plan_args = Vec::with_capacity(args.len());
                for a in args {
                    let plan_arg = match a {
                        ast::FunctionArg::Unnamed(e) => self.plan_expr(e, ctx).await?,
                        ast::FunctionArg::Wildcard => PlanExpr::Wildcard,
                    };
                    plan_args.push(plan_arg);
                }
                Ok(PlanExpr::Function {
                    name: name.clone(),
                    args: plan_args,
                    distinct: *distinct,
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
            } => Ok(PlanExpr::Cast {
                expr: Box::new(self.plan_expr(expr, ctx).await?),
                data_type: data_type.clone(),
                span: node_span,
            }),
            ast::Expr::Nested { expr: inner, .. } => self.plan_expr(inner, ctx).await,
            ast::Expr::Subquery { query, .. } => {
                let subplan = self.plan_query(query).await?;
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
                // Window functions are handled at the plan level (Window node), not in plan_expr
                Err(PlanError::invalid_expression(
                    "window functions are handled at the plan level, not in plan_expr".to_string(),
                ))
            }
            ast::Expr::Parameter { index, .. } => Ok(PlanExpr::Parameter {
                index: *index,
                type_hint: None,
                span: node_span,
            }),
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
                    let col_info = self.expr_to_column_info(&plan_expr, ctx);
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
                    // Expand * to all columns
                    for (i, (_, col)) in ctx.columns.iter().enumerate() {
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
                    for (i, (_, col)) in ctx.columns.iter().enumerate() {
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
                let agg_count = ctx.columns.len() - num_group_by;
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

    /// Check if an expression contains any aggregate function.
    fn contains_aggregate(&self, expr: &ast::Expr) -> bool {
        match expr {
            ast::Expr::Function { name, .. } => is_aggregate_function(name),
            ast::Expr::BinaryOp { left, right, .. } => {
                self.contains_aggregate(left) || self.contains_aggregate(right)
            }
            ast::Expr::UnaryOp { expr, .. } => self.contains_aggregate(expr),
            ast::Expr::Nested { expr: inner, .. } => self.contains_aggregate(inner),
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
            | PlanExpr::Parameter { .. }
            | PlanExpr::ScalarSubquery { .. } => ColumnInfo {
                name: expr.to_string(),
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

    /// Evaluate a LIMIT/OFFSET expression to a usize.
    fn eval_limit_expr(&self, expr: Option<&ast::Expr>) -> Result<Option<usize>, PlanError> {
        match expr {
            None => Ok(None),
            Some(ast::Expr::Literal {
                value: ScalarValue::Int64(n),
                ..
            }) => {
                if *n < 0 {
                    return Err(PlanError::invalid_expression(
                        "LIMIT/OFFSET must be non-negative".to_string(),
                    ));
                }
                Ok(Some(*n as usize))
            }
            Some(ast::Expr::Literal {
                value: ScalarValue::Int32(n),
                ..
            }) => {
                if *n < 0 {
                    return Err(PlanError::invalid_expression(
                        "LIMIT/OFFSET must be non-negative".to_string(),
                    ));
                }
                Ok(Some(*n as usize))
            }
            Some(_) => Err(PlanError::invalid_expression(
                "LIMIT/OFFSET must be an integer literal".to_string(),
            )),
        }
    }
}

/// Check if a function name is a known aggregate function.
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "BOOL_OR"
    )
}

/// Direction of an EXISTS/NOT EXISTS rewrite.
#[derive(Debug)]
enum SemiJoinKind {
    Semi,
    Anti,
}

/// A pending LEFT JOIN extracted from a correlated scalar subquery in
/// WHERE (e.g. TPC-H Q17's `< (SELECT 0.2 * AVG(...) FROM lineitem
/// WHERE l_partkey = p_partkey)`). The inner is decorrelated by
/// adding the correlation column(s) to GROUP BY, so the join attaches
/// a per-correlation-key scalar value to each outer row. Outer rows
/// without a matching key get NULL — matching the SQL semantics of an
/// uncorrelated empty scalar subquery.
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
            span,
        } => PlanExpr::Function {
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
        | E::Nested { expr, .. } => walk_columns(expr, cb),
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
        E::Function { args, .. } => {
            for a in args {
                if let ast::FunctionArg::Unnamed(e) = a {
                    walk_columns(e, cb);
                }
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
        | L::PartialAggregate { input, .. }
        | L::FinalAggregate { input, .. }
        | L::Distinct { input }
        | L::Window { input, .. }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_catalog::{CatalogManager, MemoryCatalog, MemorySchema, MemoryTable};
    use arneb_common::types::ColumnInfo;
    use std::sync::Arc;

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
