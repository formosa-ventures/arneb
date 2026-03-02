//! Query planner that converts a parsed SQL AST into a logical plan.

use trino_catalog::CatalogManager;
use trino_common::error::PlanError;
use trino_common::types::{ColumnInfo, DataType, ScalarValue};
use trino_sql_parser::ast;

use crate::plan::{JoinCondition, LogicalPlan, PlanExpr, SortExpr};

/// Converts parsed SQL statements into logical query plans.
pub struct QueryPlanner<'a> {
    catalog: &'a CatalogManager,
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
    fn resolve_column(
        &self,
        name: &str,
        table: Option<&str>,
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
                    return Err(PlanError::InvalidExpression(format!(
                        "ambiguous column reference: {name}"
                    )));
                }
                found = Some((i, col.clone()));
            }
        }
        found.ok_or_else(|| PlanError::ColumnNotFound(name.to_string()))
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
        Self { catalog }
    }

    /// Plan a top-level SQL statement.
    pub fn plan_statement(&self, stmt: &ast::Statement) -> Result<LogicalPlan, PlanError> {
        match stmt {
            ast::Statement::Query(query) => self.plan_query(query),
            ast::Statement::Explain(inner) => {
                let plan = self.plan_statement(inner)?;
                Ok(LogicalPlan::Explain {
                    input: Box::new(plan),
                })
            }
        }
    }

    /// Plan a Query (body + ORDER BY + LIMIT/OFFSET).
    fn plan_query(&self, query: &ast::Query) -> Result<LogicalPlan, PlanError> {
        let mut plan = self.plan_select(&query.body)?;

        // ORDER BY
        if !query.order_by.is_empty() {
            let ctx = self.context_from_plan(&plan);
            let mut sort_exprs = Vec::with_capacity(query.order_by.len());
            for ob in &query.order_by {
                let expr = self.plan_expr(&ob.expr, &ctx)?;
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

        Ok(plan)
    }

    /// Plan a SelectBody: FROM → WHERE → GROUP BY/HAVING → SELECT projection.
    fn plan_select(&self, body: &ast::SelectBody) -> Result<LogicalPlan, PlanError> {
        // 1. FROM clause → base plan + context
        let (mut plan, mut ctx) = self.plan_from(&body.from)?;

        // 2. WHERE
        if let Some(selection) = &body.selection {
            let predicate = self.plan_expr(selection, &ctx)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // 3. GROUP BY / HAVING → Aggregate
        if !body.group_by.is_empty() {
            let group_by: Vec<PlanExpr> = body
                .group_by
                .iter()
                .map(|e| self.plan_expr(e, &ctx))
                .collect::<Result<_, _>>()?;

            // Collect aggregate function calls from projection
            let aggr_exprs = self.collect_aggregates(&body.projection, &ctx)?;

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
                group_by,
                aggr_exprs,
                schema: schema.clone(),
            };

            // Update context to reflect aggregate output
            ctx = PlanningContext::new();
            for col in &schema {
                ctx.columns.push((None, col.clone()));
            }

            // HAVING (applied after aggregation)
            if let Some(having) = &body.having {
                let predicate = self.plan_expr(having, &ctx)?;
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        // 4. Projection (SELECT list)
        let (proj_exprs, proj_schema) = self.plan_projection(&body.projection, &ctx)?;

        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            exprs: proj_exprs,
            schema: proj_schema,
        };

        Ok(plan)
    }

    /// Plan the FROM clause: resolve tables, build join tree.
    fn plan_from(
        &self,
        from: &[ast::TableWithJoins],
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        if from.is_empty() {
            return Err(PlanError::InvalidExpression(
                "SELECT without FROM is not supported".to_string(),
            ));
        }

        let (mut plan, mut ctx) = self.plan_table_with_joins(&from[0])?;

        // Multiple FROM items → implicit CROSS JOIN
        for twj in &from[1..] {
            let (right_plan, right_ctx) = self.plan_table_with_joins(twj)?;
            ctx.columns.extend(right_ctx.columns);
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: ast::JoinType::Cross,
                condition: JoinCondition::None,
            };
        }

        Ok((plan, ctx))
    }

    /// Plan a single FROM item with its joins.
    fn plan_table_with_joins(
        &self,
        twj: &ast::TableWithJoins,
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        let (mut plan, mut ctx) = self.plan_table_factor(&twj.relation)?;

        for join in &twj.joins {
            let (right_plan, right_ctx) = self.plan_table_factor(&join.relation)?;
            ctx.columns.extend(right_ctx.columns);

            let condition = match &join.condition {
                ast::JoinCondition::On(expr) => {
                    let plan_expr = self.plan_expr(expr, &ctx)?;
                    JoinCondition::On(plan_expr)
                }
                ast::JoinCondition::Using(_) => {
                    return Err(PlanError::InvalidExpression(
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
            };
        }

        Ok((plan, ctx))
    }

    /// Resolve a single table factor (table name or subquery).
    fn plan_table_factor(
        &self,
        factor: &ast::TableFactor,
    ) -> Result<(LogicalPlan, PlanningContext), PlanError> {
        match factor {
            ast::TableFactor::Table { name, alias } => {
                let table_provider = self
                    .catalog
                    .resolve_table(name)
                    .map_err(|_| PlanError::TableNotFound(name.to_string()))?;
                let schema = table_provider.schema();
                let qualifier = alias.as_deref().unwrap_or(&name.table);

                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(qualifier), &schema);

                let plan = LogicalPlan::TableScan {
                    table: name.clone(),
                    schema,
                    alias: alias.clone(),
                };

                Ok((plan, ctx))
            }
            ast::TableFactor::Subquery { query, alias } => {
                let plan = self.plan_query(query)?;
                let schema = plan.schema();
                let mut ctx = PlanningContext::new();
                ctx.add_table_columns(Some(alias.as_str()), &schema);
                Ok((plan, ctx))
            }
        }
    }

    /// Convert an AST expression to a PlanExpr, resolving column references.
    #[allow(clippy::only_used_in_recursion)]
    fn plan_expr(&self, expr: &ast::Expr, ctx: &PlanningContext) -> Result<PlanExpr, PlanError> {
        match expr {
            ast::Expr::Column(col_ref) => {
                let (index, col_info) =
                    ctx.resolve_column(&col_ref.name, col_ref.table.as_deref())?;
                Ok(PlanExpr::Column {
                    index,
                    name: col_info.name,
                })
            }
            ast::Expr::Literal(val) => Ok(PlanExpr::Literal(val.clone())),
            ast::Expr::BinaryOp { left, op, right } => Ok(PlanExpr::BinaryOp {
                left: Box::new(self.plan_expr(left, ctx)?),
                op: *op,
                right: Box::new(self.plan_expr(right, ctx)?),
            }),
            ast::Expr::UnaryOp { op, expr } => Ok(PlanExpr::UnaryOp {
                op: *op,
                expr: Box::new(self.plan_expr(expr, ctx)?),
            }),
            ast::Expr::Function {
                name,
                args,
                distinct,
            } => {
                let plan_args = args
                    .iter()
                    .map(|a| match a {
                        ast::FunctionArg::Unnamed(e) => self.plan_expr(e, ctx),
                        ast::FunctionArg::Wildcard => Ok(PlanExpr::Wildcard),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanExpr::Function {
                    name: name.clone(),
                    args: plan_args,
                    distinct: *distinct,
                })
            }
            ast::Expr::IsNull(inner) => Ok(PlanExpr::IsNull(Box::new(self.plan_expr(inner, ctx)?))),
            ast::Expr::IsNotNull(inner) => {
                Ok(PlanExpr::IsNotNull(Box::new(self.plan_expr(inner, ctx)?)))
            }
            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(PlanExpr::Between {
                expr: Box::new(self.plan_expr(expr, ctx)?),
                negated: *negated,
                low: Box::new(self.plan_expr(low, ctx)?),
                high: Box::new(self.plan_expr(high, ctx)?),
            }),
            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let plan_list = list
                    .iter()
                    .map(|e| self.plan_expr(e, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PlanExpr::InList {
                    expr: Box::new(self.plan_expr(expr, ctx)?),
                    list: plan_list,
                    negated: *negated,
                })
            }
            ast::Expr::Cast { expr, data_type } => Ok(PlanExpr::Cast {
                expr: Box::new(self.plan_expr(expr, ctx)?),
                data_type: data_type.clone(),
            }),
            ast::Expr::Nested(inner) => self.plan_expr(inner, ctx),
            ast::Expr::Subquery(_) => Err(PlanError::InvalidExpression(
                "subquery expressions not yet supported".to_string(),
            )),
        }
    }

    /// Build projection expressions and output schema from SELECT items.
    fn plan_projection(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
    ) -> Result<(Vec<PlanExpr>, Vec<ColumnInfo>), PlanError> {
        let mut exprs = Vec::new();
        let mut schema = Vec::new();

        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let plan_expr = self.plan_expr(expr, ctx)?;
                    let col_info = self.expr_to_column_info(&plan_expr, ctx);
                    exprs.push(plan_expr);
                    schema.push(col_info);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let plan_expr = self.plan_expr(expr, ctx)?;
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
                        });
                        schema.push(col);
                    }
                }
            }
        }

        Ok((exprs, schema))
    }

    /// Collect aggregate function expressions from the SELECT list.
    fn collect_aggregates(
        &self,
        items: &[ast::SelectItem],
        ctx: &PlanningContext,
    ) -> Result<Vec<PlanExpr>, PlanError> {
        let mut aggregates = Vec::new();
        for item in items {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => {
                    self.extract_aggregates(expr, ctx, &mut aggregates)?;
                }
                _ => {}
            }
        }
        Ok(aggregates)
    }

    /// Recursively extract aggregate functions from an expression.
    fn extract_aggregates(
        &self,
        expr: &ast::Expr,
        ctx: &PlanningContext,
        out: &mut Vec<PlanExpr>,
    ) -> Result<(), PlanError> {
        match expr {
            ast::Expr::Function { name, .. } if is_aggregate_function(name) => {
                let plan_expr = self.plan_expr(expr, ctx)?;
                // Avoid duplicates
                if !out.iter().any(|e| format!("{e}") == format!("{plan_expr}")) {
                    out.push(plan_expr);
                }
            }
            ast::Expr::BinaryOp { left, right, .. } => {
                self.extract_aggregates(left, ctx, out)?;
                self.extract_aggregates(right, ctx, out)?;
            }
            ast::Expr::UnaryOp { expr, .. } => {
                self.extract_aggregates(expr, ctx, out)?;
            }
            ast::Expr::Nested(inner) => {
                self.extract_aggregates(inner, ctx, out)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Derive a ColumnInfo from a PlanExpr (best effort name + type inference).
    fn expr_to_column_info(&self, expr: &PlanExpr, ctx: &PlanningContext) -> ColumnInfo {
        match expr {
            PlanExpr::Column { index, name } => {
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
            PlanExpr::Literal(val) => ColumnInfo {
                name: val.to_string(),
                data_type: val.data_type(),
                nullable: matches!(val, ScalarValue::Null),
            },
            PlanExpr::Function { name, .. } => ColumnInfo {
                name: name.clone(),
                data_type: DataType::Null, // type unknown at planning time
                nullable: true,
            },
            PlanExpr::BinaryOp { left, op, right } => ColumnInfo {
                name: format!("{left} {op} {right}"),
                data_type: DataType::Null,
                nullable: true,
            },
            PlanExpr::Cast { data_type, expr } => ColumnInfo {
                name: format!("CAST({expr} AS {data_type})"),
                data_type: data_type.clone(),
                nullable: true,
            },
            _ => ColumnInfo {
                name: expr.to_string(),
                data_type: DataType::Null,
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
            Some(ast::Expr::Literal(ScalarValue::Int64(n))) => {
                if *n < 0 {
                    return Err(PlanError::InvalidExpression(
                        "LIMIT/OFFSET must be non-negative".to_string(),
                    ));
                }
                Ok(Some(*n as usize))
            }
            Some(ast::Expr::Literal(ScalarValue::Int32(n))) => {
                if *n < 0 {
                    return Err(PlanError::InvalidExpression(
                        "LIMIT/OFFSET must be non-negative".to_string(),
                    ));
                }
                Ok(Some(*n as usize))
            }
            Some(_) => Err(PlanError::InvalidExpression(
                "LIMIT/OFFSET must be an integer literal".to_string(),
            )),
        }
    }
}

/// Check if a function name is a known aggregate function.
fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use trino_catalog::{CatalogManager, MemoryCatalog, MemorySchema, MemoryTable};
    use trino_common::types::ColumnInfo;

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

    fn plan_sql(sql: &str) -> Result<LogicalPlan, PlanError> {
        let catalog = test_catalog();
        let planner = QueryPlanner::new(&catalog);
        let stmt = trino_sql_parser::parse(sql).expect("parse failed");
        planner.plan_statement(&stmt)
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
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal(ScalarValue::Int64(1))),
        };
        assert_eq!(expr.to_string(), "a > 1");
    }

    #[test]
    fn test_plan_expr_display_function() {
        let expr = PlanExpr::Function {
            name: "COUNT".into(),
            args: vec![PlanExpr::Wildcard],
            distinct: false,
        };
        assert_eq!(expr.to_string(), "COUNT(*)");
    }

    #[test]
    fn test_plan_expr_display_between() {
        let expr = PlanExpr::Between {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "x".into(),
            }),
            negated: false,
            low: Box::new(PlanExpr::Literal(ScalarValue::Int64(1))),
            high: Box::new(PlanExpr::Literal(ScalarValue::Int64(10))),
        };
        assert_eq!(expr.to_string(), "x BETWEEN 1 AND 10");
    }

    #[test]
    fn test_logical_plan_display() {
        let plan = plan_sql("SELECT name FROM users WHERE id > 10").unwrap();
        let display = plan.to_string();
        assert!(display.contains("Projection"));
        assert!(display.contains("Filter"));
        assert!(display.contains("TableScan"));
    }

    // ---------------------------------------------------------------
    // Simple SELECT (task 4.3)
    // ---------------------------------------------------------------

    #[test]
    fn test_simple_select() {
        let plan = plan_sql("SELECT id, name FROM users").unwrap();
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

    #[test]
    fn test_select_with_where() {
        let plan = plan_sql("SELECT name FROM users WHERE id > 10").unwrap();
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

    #[test]
    fn test_select_wildcard() {
        let plan = plan_sql("SELECT * FROM users").unwrap();
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

    #[test]
    fn test_select_with_join() {
        let plan = plan_sql(
            "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id",
        )
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

    #[test]
    fn test_select_with_group_by() {
        let plan = plan_sql("SELECT name, COUNT(*) FROM users GROUP BY name").unwrap();

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

    #[test]
    fn test_select_with_order_by() {
        let plan = plan_sql("SELECT id, name FROM users ORDER BY id DESC").unwrap();
        match &plan {
            LogicalPlan::Sort { order_by, .. } => {
                assert_eq!(order_by.len(), 1);
                assert!(!order_by[0].asc);
            }
            _ => panic!("expected Sort at top"),
        }
    }

    #[test]
    fn test_select_with_limit_offset() {
        let plan = plan_sql("SELECT id FROM users LIMIT 10 OFFSET 5").unwrap();
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

    #[test]
    fn test_explain() {
        let plan = plan_sql("EXPLAIN SELECT id FROM users").unwrap();
        match &plan {
            LogicalPlan::Explain { input } => {
                assert!(matches!(input.as_ref(), LogicalPlan::Projection { .. }));
            }
            _ => panic!("expected Explain"),
        }
    }

    // ---------------------------------------------------------------
    // Error cases (task 4.10)
    // ---------------------------------------------------------------

    #[test]
    fn test_table_not_found() {
        let err = plan_sql("SELECT * FROM nonexistent").unwrap_err();
        match err {
            PlanError::TableNotFound(name) => assert_eq!(name, "nonexistent"),
            _ => panic!("expected TableNotFound, got: {err:?}"),
        }
    }

    #[test]
    fn test_column_not_found() {
        let err = plan_sql("SELECT nonexistent FROM users").unwrap_err();
        match err {
            PlanError::ColumnNotFound(name) => assert_eq!(name, "nonexistent"),
            _ => panic!("expected ColumnNotFound, got: {err:?}"),
        }
    }

    // ---------------------------------------------------------------
    // Aliases, qualified refs, expressions (task 4.11)
    // ---------------------------------------------------------------

    #[test]
    fn test_alias_in_projection() {
        let plan = plan_sql("SELECT name AS user_name FROM users").unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "user_name");
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_qualified_column_reference() {
        let plan = plan_sql("SELECT users.name FROM users").unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "name");
                assert_eq!(schema[0].data_type, DataType::Utf8);
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_expression_in_projection() {
        let plan = plan_sql("SELECT id + 1 FROM users").unwrap();
        match &plan {
            LogicalPlan::Projection { exprs, .. } => {
                assert!(matches!(exprs[0], PlanExpr::BinaryOp { .. }));
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_table_alias() {
        let plan = plan_sql("SELECT u.name FROM users u").unwrap();
        match &plan {
            LogicalPlan::Projection { schema, .. } => {
                assert_eq!(schema[0].name, "name");
            }
            _ => panic!("expected Projection"),
        }
    }

    #[test]
    fn test_cross_join_implicit() {
        let plan = plan_sql("SELECT * FROM users, orders").unwrap();
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

    #[test]
    fn test_qualified_wildcard() {
        let plan =
            plan_sql("SELECT users.* FROM users JOIN orders ON users.id = orders.user_id").unwrap();
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

    #[test]
    fn test_schema_propagation_through_filter() {
        let plan = plan_sql("SELECT * FROM users WHERE age > 18").unwrap();
        let schema = plan.schema();
        assert_eq!(schema.len(), 3);
    }
}
