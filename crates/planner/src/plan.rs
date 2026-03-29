//! Logical plan types for the trino-alt query engine.
//!
//! These types represent relational algebra operations produced by the
//! query planner. They form a tree that the optimizer transforms and
//! the execution engine evaluates.

use std::fmt;

use trino_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
use trino_sql_parser::ast;

/// An expression within a logical plan.
///
/// Unlike AST expressions, column references here are resolved to their
/// position (index) in the input schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PlanExpr {
    /// A column reference resolved to its index in the input schema.
    Column {
        /// Zero-based column index in the input schema.
        index: usize,
        /// Column name (for display purposes).
        name: String,
    },
    /// A literal value.
    Literal(ScalarValue),
    /// A binary operation.
    BinaryOp {
        /// Left operand.
        left: Box<PlanExpr>,
        /// Operator.
        op: ast::BinaryOp,
        /// Right operand.
        right: Box<PlanExpr>,
    },
    /// A unary operation.
    UnaryOp {
        /// Operator.
        op: ast::UnaryOp,
        /// Operand.
        expr: Box<PlanExpr>,
    },
    /// A function call.
    Function {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<PlanExpr>,
        /// Whether DISTINCT was specified.
        distinct: bool,
    },
    /// `expr IS NULL`.
    IsNull(Box<PlanExpr>),
    /// `expr IS NOT NULL`.
    IsNotNull(Box<PlanExpr>),
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        /// The expression being tested.
        expr: Box<PlanExpr>,
        /// Whether this is NOT BETWEEN.
        negated: bool,
        /// Lower bound.
        low: Box<PlanExpr>,
        /// Upper bound.
        high: Box<PlanExpr>,
    },
    /// `expr [NOT] IN (list)`.
    InList {
        /// The expression being tested.
        expr: Box<PlanExpr>,
        /// The list of values.
        list: Vec<PlanExpr>,
        /// Whether this is NOT IN.
        negated: bool,
    },
    /// `CAST(expr AS data_type)`.
    Cast {
        /// The expression to cast.
        expr: Box<PlanExpr>,
        /// The target data type.
        data_type: DataType,
    },
    /// A wildcard (`*`) — only used temporarily before expansion.
    Wildcard,
    /// A scalar subquery expression that returns a single value.
    ScalarSubquery {
        /// The subquery's logical plan.
        subplan: Box<LogicalPlan>,
    },
    /// A CASE expression (both searched and simple forms).
    CaseExpr {
        /// For simple CASE: the operand expression. None for searched CASE.
        operand: Option<Box<PlanExpr>>,
        /// Condition/result pairs evaluated in order.
        when_clauses: Vec<(PlanExpr, PlanExpr)>,
        /// Optional ELSE result.
        else_result: Option<Box<PlanExpr>>,
    },
}

impl PartialEq for PlanExpr {
    fn eq(&self, other: &Self) -> bool {
        // Compare by display string — sufficient for optimizer tests and dedup
        format!("{self}") == format!("{other}")
    }
}

/// An expression in an ORDER BY clause with sort direction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SortExpr {
    /// The expression to sort by.
    pub expr: PlanExpr,
    /// Sort ascending.
    pub asc: bool,
    /// Nulls first.
    pub nulls_first: bool,
}

/// A logical query plan node.
///
/// Each node represents a relational algebra operation and carries
/// enough information to determine its output schema.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum LogicalPlan {
    /// Reads all rows from a table.
    TableScan {
        /// The table reference.
        table: TableReference,
        /// The table's column schema.
        schema: Vec<ColumnInfo>,
        /// Optional alias for this table.
        alias: Option<String>,
    },
    /// Selects/computes columns.
    Projection {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Projected expressions.
        exprs: Vec<PlanExpr>,
        /// Output schema after projection.
        schema: Vec<ColumnInfo>,
    },
    /// Filters rows by a boolean predicate.
    Filter {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Filter predicate.
        predicate: PlanExpr,
    },
    /// Joins two inputs.
    Join {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
        /// Join type.
        join_type: ast::JoinType,
        /// Join condition.
        condition: JoinCondition,
    },
    /// Groups and aggregates rows.
    Aggregate {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Group-by expressions.
        group_by: Vec<PlanExpr>,
        /// Aggregate expressions.
        aggr_exprs: Vec<PlanExpr>,
        /// Output schema.
        schema: Vec<ColumnInfo>,
    },
    /// Orders rows.
    Sort {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Sort expressions.
        order_by: Vec<SortExpr>,
    },
    /// Limits the number of rows.
    Limit {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Maximum number of rows.
        limit: Option<usize>,
        /// Number of rows to skip.
        offset: Option<usize>,
    },
    /// Wraps a plan for EXPLAIN output.
    Explain {
        /// The plan to explain.
        input: Box<LogicalPlan>,
    },
    /// Exchange boundary between distributed fragments.
    ExchangeNode {
        /// The stage that produces this exchange's data.
        stage_id: trino_common::identifiers::StageId,
        /// Output schema.
        schema: Vec<ColumnInfo>,
    },
    /// Partial (map-side) aggregation for distributed execution.
    PartialAggregate {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Group-by expressions.
        group_by: Vec<PlanExpr>,
        /// Aggregate expressions.
        aggr_exprs: Vec<PlanExpr>,
        /// Output schema.
        schema: Vec<ColumnInfo>,
    },
    /// Final (reduce-side) aggregation combining partial results.
    FinalAggregate {
        /// Input plan (typically an ExchangeNode).
        input: Box<LogicalPlan>,
        /// Group-by expressions.
        group_by: Vec<PlanExpr>,
        /// Aggregate expressions.
        aggr_exprs: Vec<PlanExpr>,
        /// Output schema.
        schema: Vec<ColumnInfo>,
    },
    /// Semi-join: returns left rows where at least one match exists in right.
    SemiJoin {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input (subquery plan).
        right: Box<LogicalPlan>,
        /// Left key expression (evaluated against left input).
        left_key: PlanExpr,
        /// Right key expression (evaluated against right input).
        right_key: PlanExpr,
    },
    /// Anti-join: returns left rows where NO match exists in right.
    AntiJoin {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input (subquery plan).
        right: Box<LogicalPlan>,
        /// Left key expression (evaluated against left input).
        left_key: PlanExpr,
        /// Right key expression (evaluated against right input).
        right_key: PlanExpr,
    },
    /// Scalar subquery: executes subplan and returns a single scalar value.
    ScalarSubquery {
        /// The subquery plan (must produce at most 1 row, 1 column).
        subplan: Box<LogicalPlan>,
    },
    /// UNION ALL: concatenate outputs of all inputs.
    UnionAll {
        /// Input plans (all must have compatible schemas).
        inputs: Vec<LogicalPlan>,
    },
    /// Deduplicate rows (used for UNION DISTINCT).
    Distinct {
        /// Input plan.
        input: Box<LogicalPlan>,
    },
    /// INTERSECT: rows in both left and right.
    Intersect {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
    },
    /// EXCEPT: rows in left but not in right.
    Except {
        /// Left input.
        left: Box<LogicalPlan>,
        /// Right input.
        right: Box<LogicalPlan>,
    },
    /// CREATE TABLE statement.
    CreateTable {
        name: TableReference,
        schema: Vec<ColumnInfo>,
    },
    /// DROP TABLE statement.
    DropTable {
        name: TableReference,
        if_exists: bool,
    },
    /// CREATE TABLE AS SELECT.
    CreateTableAsSelect {
        name: TableReference,
        source: Box<LogicalPlan>,
    },
    /// INSERT INTO with a source plan.
    InsertInto {
        table: TableReference,
        source: Box<LogicalPlan>,
    },
    /// DELETE FROM with optional predicate.
    DeleteFrom {
        table: TableReference,
        predicate: Option<String>,
    },
    /// CREATE VIEW.
    CreateView {
        name: TableReference,
        sql: String,
        plan: Box<LogicalPlan>,
    },
    /// DROP VIEW.
    DropView {
        name: TableReference,
        if_exists: bool,
    },
    /// Window function computation.
    Window {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Window function definitions.
        functions: Vec<WindowFunctionDef>,
    },
}

/// A window function definition within a Window plan node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WindowFunctionDef {
    /// Function name (e.g., ROW_NUMBER, SUM).
    pub name: String,
    /// Function arguments (column references for aggregates).
    pub args: Vec<PlanExpr>,
    /// PARTITION BY expressions.
    pub partition_by: Vec<PlanExpr>,
    /// ORDER BY expressions with direction.
    pub order_by: Vec<SortExpr>,
    /// Output column name.
    pub output_name: String,
}

/// A join condition in a logical plan.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum JoinCondition {
    /// ON expression.
    On(PlanExpr),
    /// No condition (for CROSS JOIN).
    None,
}

impl LogicalPlan {
    /// Returns the output schema of this plan node.
    pub fn schema(&self) -> Vec<ColumnInfo> {
        match self {
            LogicalPlan::TableScan { schema, .. } => schema.clone(),
            LogicalPlan::Projection { schema, .. } => schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Join { left, right, .. } => {
                let mut schema = left.schema();
                schema.extend(right.schema());
                schema
            }
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Explain { input } => input.schema(),
            LogicalPlan::ExchangeNode { schema, .. } => schema.clone(),
            LogicalPlan::PartialAggregate { schema, .. } => schema.clone(),
            LogicalPlan::FinalAggregate { schema, .. } => schema.clone(),
            LogicalPlan::SemiJoin { left, .. } => left.schema(),
            LogicalPlan::AntiJoin { left, .. } => left.schema(),
            LogicalPlan::ScalarSubquery { subplan } => {
                let sub_schema = subplan.schema();
                if sub_schema.is_empty() {
                    vec![ColumnInfo {
                        name: "scalar_subquery".to_string(),
                        data_type: trino_common::types::DataType::Utf8,
                        nullable: true,
                    }]
                } else {
                    vec![ColumnInfo {
                        name: sub_schema[0].name.clone(),
                        data_type: sub_schema[0].data_type.clone(),
                        nullable: true,
                    }]
                }
            }
            // DDL/DML plans don't produce tabular output
            LogicalPlan::CreateTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::CreateTableAsSelect { .. }
            | LogicalPlan::InsertInto { .. }
            | LogicalPlan::DeleteFrom { .. }
            | LogicalPlan::CreateView { .. }
            | LogicalPlan::DropView { .. } => vec![],

            LogicalPlan::UnionAll { inputs } => {
                if inputs.is_empty() {
                    vec![]
                } else {
                    inputs[0].schema()
                }
            }
            LogicalPlan::Distinct { input } => input.schema(),
            LogicalPlan::Intersect { left, .. } => left.schema(),
            LogicalPlan::Except { left, .. } => left.schema(),
            LogicalPlan::Window { input, functions } => {
                let mut schema = input.schema();
                for f in functions {
                    schema.push(ColumnInfo {
                        name: f.output_name.clone(),
                        data_type: DataType::Int64, // default; actual type depends on function
                        nullable: true,
                    });
                }
                schema
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Display implementations
// ---------------------------------------------------------------------------

impl fmt::Display for PlanExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanExpr::Column { name, .. } => write!(f, "{name}"),
            PlanExpr::Literal(val) => write!(f, "{val}"),
            PlanExpr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            PlanExpr::UnaryOp { op, expr } => write!(f, "{op} {expr}"),
            PlanExpr::Function {
                name,
                args,
                distinct,
            } => {
                write!(f, "{name}(")?;
                if *distinct {
                    write!(f, "DISTINCT ")?;
                }
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ")")
            }
            PlanExpr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            PlanExpr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            PlanExpr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            PlanExpr::InList {
                expr,
                list,
                negated,
            } => {
                write!(f, "{expr}")?;
                if *negated {
                    write!(f, " NOT")?;
                }
                write!(f, " IN (")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ")")
            }
            PlanExpr::Cast { expr, data_type } => write!(f, "CAST({expr} AS {data_type})"),
            PlanExpr::Wildcard => write!(f, "*"),
            PlanExpr::ScalarSubquery { .. } => write!(f, "(scalar_subquery)"),
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for (cond, result) in when_clauses {
                    write!(f, " WHEN {cond} THEN {result}")?;
                }
                if let Some(el) = else_result {
                    write!(f, " ELSE {el}")?;
                }
                write!(f, " END")
            }
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_plan(self, f, 0)
    }
}

fn fmt_plan(plan: &LogicalPlan, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
    let pad = "  ".repeat(indent);
    match plan {
        LogicalPlan::TableScan {
            table,
            alias,
            schema,
            ..
        } => {
            write!(f, "{pad}TableScan: {table}")?;
            if let Some(a) = alias {
                write!(f, " AS {a}")?;
            }
            write!(
                f,
                " [{}]",
                schema
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        LogicalPlan::Projection { input, exprs, .. } => {
            let expr_strs: Vec<String> = exprs.iter().map(|e| e.to_string()).collect();
            writeln!(f, "{pad}Projection: {}", expr_strs.join(", "))?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Filter { input, predicate } => {
            writeln!(f, "{pad}Filter: {predicate}")?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Join {
            left,
            right,
            join_type,
            condition,
        } => {
            let jt = match join_type {
                ast::JoinType::Inner => "Inner",
                ast::JoinType::Left => "Left",
                ast::JoinType::Right => "Right",
                ast::JoinType::Full => "Full",
                ast::JoinType::Cross => "Cross",
            };
            write!(f, "{pad}Join: {jt}")?;
            if let JoinCondition::On(expr) = condition {
                write!(f, " ON {expr}")?;
            }
            writeln!(f)?;
            fmt_plan(left, f, indent + 1)?;
            writeln!(f)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::Aggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| e.to_string()).collect();
            let agg: Vec<String> = aggr_exprs.iter().map(|e| e.to_string()).collect();
            write!(f, "{pad}Aggregate: group_by=[{}]", gb.join(", "))?;
            if !agg.is_empty() {
                write!(f, ", aggr=[{}]", agg.join(", "))?;
            }
            writeln!(f)?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Sort { input, order_by } => {
            let sorts: Vec<String> = order_by
                .iter()
                .map(|s| {
                    let dir = if s.asc { "ASC" } else { "DESC" };
                    let nulls = if s.nulls_first {
                        " NULLS FIRST"
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", s.expr)
                })
                .collect();
            writeln!(f, "{pad}Sort: {}", sorts.join(", "))?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => {
            write!(f, "{pad}Limit:")?;
            if let Some(l) = limit {
                write!(f, " limit={l}")?;
            }
            if let Some(o) = offset {
                write!(f, " offset={o}")?;
            }
            writeln!(f)?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Explain { input } => {
            writeln!(f, "{pad}Explain:")?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::ExchangeNode { stage_id, schema } => {
            write!(
                f,
                "{pad}Exchange: stage={stage_id} [{}]",
                schema
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        LogicalPlan::PartialAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| e.to_string()).collect();
            let agg: Vec<String> = aggr_exprs.iter().map(|e| e.to_string()).collect();
            write!(f, "{pad}PartialAggregate: group_by=[{}]", gb.join(", "))?;
            if !agg.is_empty() {
                write!(f, ", aggr=[{}]", agg.join(", "))?;
            }
            writeln!(f)?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::FinalAggregate {
            input,
            group_by,
            aggr_exprs,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| e.to_string()).collect();
            let agg: Vec<String> = aggr_exprs.iter().map(|e| e.to_string()).collect();
            write!(f, "{pad}FinalAggregate: group_by=[{}]", gb.join(", "))?;
            if !agg.is_empty() {
                write!(f, ", aggr=[{}]", agg.join(", "))?;
            }
            writeln!(f)?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::SemiJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            writeln!(f, "{pad}SemiJoin: {left_key} = {right_key}")?;
            fmt_plan(left, f, indent + 1)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
        } => {
            writeln!(f, "{pad}AntiJoin: {left_key} = {right_key}")?;
            fmt_plan(left, f, indent + 1)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::ScalarSubquery { subplan } => {
            writeln!(f, "{pad}ScalarSubquery:")?;
            fmt_plan(subplan, f, indent + 1)
        }
        LogicalPlan::CreateTable { name, .. } => writeln!(f, "{pad}CreateTable: {name}"),
        LogicalPlan::DropTable { name, .. } => writeln!(f, "{pad}DropTable: {name}"),
        LogicalPlan::CreateTableAsSelect { name, source } => {
            writeln!(f, "{pad}CreateTableAsSelect: {name}")?;
            fmt_plan(source, f, indent + 1)
        }
        LogicalPlan::InsertInto { table, source } => {
            writeln!(f, "{pad}InsertInto: {table}")?;
            fmt_plan(source, f, indent + 1)
        }
        LogicalPlan::DeleteFrom { table, .. } => writeln!(f, "{pad}DeleteFrom: {table}"),
        LogicalPlan::CreateView { name, .. } => writeln!(f, "{pad}CreateView: {name}"),
        LogicalPlan::DropView { name, .. } => writeln!(f, "{pad}DropView: {name}"),
        LogicalPlan::UnionAll { inputs } => {
            writeln!(f, "{pad}UnionAll:")?;
            for input in inputs {
                fmt_plan(input, f, indent + 1)?;
                writeln!(f)?;
            }
            Ok(())
        }
        LogicalPlan::Distinct { input } => {
            writeln!(f, "{pad}Distinct:")?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::Intersect { left, right } => {
            writeln!(f, "{pad}Intersect:")?;
            fmt_plan(left, f, indent + 1)?;
            writeln!(f)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::Except { left, right } => {
            writeln!(f, "{pad}Except:")?;
            fmt_plan(left, f, indent + 1)?;
            writeln!(f)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::Window { input, functions } => {
            let fns: Vec<String> = functions.iter().map(|f| f.output_name.clone()).collect();
            writeln!(f, "{pad}Window: [{}]", fns.join(", "))?;
            fmt_plan(input, f, indent + 1)
        }
    }
}
