//! AST types for the arneb SQL parser.
//!
//! These types represent the subset of SQL supported by arneb's MVP.
//! They are produced by converting `sqlparser-rs` AST nodes through
//! the conversion layer in [`crate::convert`].
//!
//! Every [`Expr`], [`Statement`], and [`ColumnRef`] carries a
//! [`Span`] pointing at the source range of the node in the original
//! SQL text. Spans flow through to [`crate::PlanExpr`] and error
//! rendering so diagnostics can surface `file:line:col` carets. Nodes
//! synthesized outside the parser (tests, fixtures) should use
//! [`Span::empty()`].

use std::fmt;

use arneb_common::types::{DataType, ScalarValue, TableReference};
use sqlparser::tokenizer::Span;

/// Column definition for CREATE TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub data_type: DataType,
    /// Whether the column is nullable (default true).
    pub nullable: bool,
}

/// Source for INSERT INTO.
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    /// INSERT INTO ... VALUES (...)
    Values(Vec<Vec<Expr>>),
    /// INSERT INTO ... SELECT ...
    Query(Box<Query>),
}

/// A top-level SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A SQL query (SELECT).
    Query {
        /// The query payload.
        query: Box<Query>,
        /// Source span covering the whole statement.
        span: Span,
    },
    /// EXPLAIN followed by a statement.
    Explain {
        /// The statement to explain.
        stmt: Box<Statement>,
        /// Source span covering the whole EXPLAIN statement.
        span: Span,
    },
    /// CREATE TABLE name (columns...) [IF NOT EXISTS]
    CreateTable {
        /// Table name.
        name: TableReference,
        /// Column definitions.
        columns: Vec<ColumnDef>,
        /// IF NOT EXISTS flag.
        if_not_exists: bool,
        /// Source span.
        span: Span,
    },
    /// DROP TABLE [IF EXISTS] name
    DropTable {
        /// Table name.
        name: TableReference,
        /// IF EXISTS flag.
        if_exists: bool,
        /// Source span.
        span: Span,
    },
    /// CREATE TABLE name AS SELECT ...
    CreateTableAsSelect {
        /// Table name.
        name: TableReference,
        /// Source query.
        query: Box<Query>,
        /// Source span.
        span: Span,
    },
    /// INSERT INTO table [(columns)] source
    InsertInto {
        /// Target table.
        table: TableReference,
        /// Optional column list.
        columns: Vec<String>,
        /// Values or subquery source.
        source: InsertSource,
        /// Source span.
        span: Span,
    },
    /// DELETE FROM table [WHERE predicate]
    DeleteFrom {
        /// Target table.
        table: TableReference,
        /// Optional WHERE predicate.
        predicate: Option<Box<Expr>>,
        /// Source span.
        span: Span,
    },
    /// CREATE [OR REPLACE] VIEW name AS query
    CreateView {
        /// View name.
        name: TableReference,
        /// Source query.
        query: Box<Query>,
        /// Whether OR REPLACE was specified.
        or_replace: bool,
        /// Source span.
        span: Span,
    },
    /// DROP VIEW [IF EXISTS] name
    DropView {
        /// View name.
        name: TableReference,
        /// IF EXISTS flag.
        if_exists: bool,
        /// Source span.
        span: Span,
    },
}

impl Statement {
    /// Returns the source span of this statement.
    pub fn span(&self) -> Span {
        match self {
            Statement::Query { span, .. }
            | Statement::Explain { span, .. }
            | Statement::CreateTable { span, .. }
            | Statement::DropTable { span, .. }
            | Statement::CreateTableAsSelect { span, .. }
            | Statement::InsertInto { span, .. }
            | Statement::DeleteFrom { span, .. }
            | Statement::CreateView { span, .. }
            | Statement::DropView { span, .. } => *span,
        }
    }
}

/// A Common Table Expression (CTE) definition.
#[derive(Debug, Clone, PartialEq)]
pub struct CTEDefinition {
    /// The CTE name.
    pub name: String,
    /// Optional column aliases.
    pub column_aliases: Vec<String>,
    /// The CTE subquery.
    pub query: Box<Query>,
}

/// A complete SQL query with optional CTEs, ORDER BY, LIMIT, and OFFSET.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// Common Table Expressions (WITH clause).
    pub ctes: Vec<CTEDefinition>,
    /// The SELECT body of the query (may be a set operation).
    pub body: QueryBody,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT expression.
    pub limit: Option<Box<Expr>>,
    /// OFFSET expression.
    pub offset: Option<Box<Expr>>,
}

/// The body of a query — either a single SELECT or a set operation.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    /// A simple SELECT statement.
    Select(SelectBody),
    /// A set operation (UNION ALL, UNION, INTERSECT, EXCEPT).
    SetOperation {
        /// The set operator.
        op: SetOperator,
        /// Left side query body.
        left: Box<QueryBody>,
        /// Right side query body.
        right: Box<QueryBody>,
    },
}

/// Set operation types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    /// UNION ALL — concatenate without deduplication.
    UnionAll,
    /// UNION — concatenate with deduplication.
    Union,
    /// INTERSECT — rows in both sides.
    Intersect,
    /// EXCEPT — rows in left but not right.
    Except,
}

/// The body of a SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectBody {
    /// Whether SELECT DISTINCT was specified.
    pub distinct: bool,
    /// The projected columns/expressions.
    pub projection: Vec<SelectItem>,
    /// The FROM clause, each entry may include joins.
    pub from: Vec<TableWithJoins>,
    /// The WHERE clause filter.
    pub selection: Option<Box<Expr>>,
    /// GROUP BY expressions.
    pub group_by: Vec<Expr>,
    /// HAVING clause filter (applied after GROUP BY).
    pub having: Option<Box<Expr>>,
}

/// An item in the SELECT projection list.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// An expression without an explicit alias.
    UnnamedExpr(Expr),
    /// An expression with an explicit alias (`expr AS alias`).
    ExprWithAlias {
        /// The expression.
        expr: Expr,
        /// The alias name.
        alias: String,
    },
    /// `SELECT *`.
    Wildcard,
    /// `SELECT table.*`.
    QualifiedWildcard(TableReference),
}

/// A SQL expression.
///
/// Every variant carries a `span` field pointing at its source location.
/// For nodes synthesized outside the parser (unit tests, fixtures), use
/// [`Span::empty()`].
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column reference, optionally qualified by a table name.
    Column {
        /// The column reference.
        col_ref: ColumnRef,
        /// Source span.
        span: Span,
    },
    /// A literal value.
    Literal {
        /// The scalar value.
        value: ScalarValue,
        /// Source span.
        span: Span,
    },
    /// A binary operation (`left op right`).
    BinaryOp {
        /// Left operand.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
        /// Source span covering the whole binary expression.
        span: Span,
    },
    /// A unary operation (`op expr`).
    UnaryOp {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Expr>,
        /// Source span covering the whole unary expression.
        span: Span,
    },
    /// A function call.
    Function {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<FunctionArg>,
        /// Whether DISTINCT was specified (e.g., `COUNT(DISTINCT x)`).
        distinct: bool,
        /// Source span covering the call.
        span: Span,
    },
    /// `expr IS NULL`.
    IsNull {
        /// The expression being tested.
        expr: Box<Expr>,
        /// Source span.
        span: Span,
    },
    /// `expr IS NOT NULL`.
    IsNotNull {
        /// The expression being tested.
        expr: Box<Expr>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        /// The expression being tested.
        expr: Box<Expr>,
        /// Whether this is NOT BETWEEN.
        negated: bool,
        /// Lower bound.
        low: Box<Expr>,
        /// Upper bound.
        high: Box<Expr>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (list)`.
    InList {
        /// The expression being tested.
        expr: Box<Expr>,
        /// The list of values.
        list: Vec<Expr>,
        /// Whether this is NOT IN.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `CAST(expr AS data_type)`.
    Cast {
        /// The expression to cast.
        expr: Box<Expr>,
        /// The target data type.
        data_type: DataType,
        /// Source span.
        span: Span,
    },
    /// A parenthesized sub-expression.
    Nested {
        /// The inner expression.
        expr: Box<Expr>,
        /// Source span (covers the parens as well).
        span: Span,
    },
    /// A subquery expression (scalar subquery).
    Subquery {
        /// The subquery.
        query: Box<Query>,
        /// Source span.
        span: Span,
    },
    /// `expr [NOT] IN (subquery)`.
    InSubquery {
        /// The expression being tested.
        expr: Box<Expr>,
        /// The subquery providing the set.
        subquery: Box<Query>,
        /// Whether this is NOT IN.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// `[NOT] EXISTS (subquery)`.
    Exists {
        /// The subquery.
        subquery: Box<Query>,
        /// Whether this is NOT EXISTS.
        negated: bool,
        /// Source span.
        span: Span,
    },
    /// A window function call: `func(...) OVER (PARTITION BY ... ORDER BY ...)`.
    WindowFunction {
        /// Function name (e.g., ROW_NUMBER, SUM).
        name: String,
        /// Function arguments.
        args: Vec<Expr>,
        /// PARTITION BY expressions.
        partition_by: Vec<Expr>,
        /// ORDER BY expressions.
        order_by: Vec<OrderByExpr>,
        /// Source span.
        span: Span,
    },
    /// A CASE expression (both searched and simple forms).
    Case {
        /// For simple CASE: the operand expression. None for searched CASE.
        operand: Option<Box<Expr>>,
        /// The WHEN condition expressions.
        conditions: Vec<Expr>,
        /// The THEN result expressions (same length as conditions).
        results: Vec<Expr>,
        /// The optional ELSE expression.
        else_result: Option<Box<Expr>>,
        /// Source span.
        span: Span,
    },
    /// An extended-query-protocol parameter placeholder (`$1`, `$2`, …).
    ///
    /// Parameters only appear when the query is submitted via the
    /// pgwire extended-query protocol (Parse → Bind). Simple query
    /// text with `$N` typically passes through unchanged here and is
    /// resolved before the planner runs (see
    /// `crates/protocol/src/handler.rs::bind_parameters`). The
    /// planner's describe-statement path, which runs before Bind, is
    /// the one that sees this variant.
    Parameter {
        /// 1-based placeholder index as it appeared in the SQL text.
        index: usize,
        /// Source span covering the placeholder literal.
        span: Span,
    },
}

impl Expr {
    /// Returns the source span of this expression.
    pub fn span(&self) -> Span {
        match self {
            Expr::Column { span, .. }
            | Expr::Literal { span, .. }
            | Expr::BinaryOp { span, .. }
            | Expr::UnaryOp { span, .. }
            | Expr::Function { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::IsNotNull { span, .. }
            | Expr::Between { span, .. }
            | Expr::InList { span, .. }
            | Expr::Cast { span, .. }
            | Expr::Nested { span, .. }
            | Expr::Subquery { span, .. }
            | Expr::InSubquery { span, .. }
            | Expr::Exists { span, .. }
            | Expr::WindowFunction { span, .. }
            | Expr::Case { span, .. }
            | Expr::Parameter { span, .. } => *span,
        }
    }
}

/// A reference to a column, optionally qualified by a table name.
///
/// The `span` field covers the identifier (including any table qualifier)
/// in the original source. Synthetic column references (inserted by
/// optimizer rewrites, tests, etc.) should use [`Span::empty()`].
#[derive(Debug, Clone)]
pub struct ColumnRef {
    /// The column name.
    pub name: String,
    /// Optional table qualifier.
    pub table: Option<String>,
    /// Source span pointing at the column reference in the SQL text.
    pub span: Span,
}

impl PartialEq for ColumnRef {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.table == other.table
    }
}

impl Eq for ColumnRef {}

impl std::hash::Hash for ColumnRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.table.hash(state);
    }
}

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    // Arithmetic
    /// Addition (`+`).
    Plus,
    /// Subtraction (`-`).
    Minus,
    /// Multiplication (`*`).
    Multiply,
    /// Division (`/`).
    Divide,
    /// Modulo (`%`).
    Modulo,

    // Comparison
    /// Equality (`=`).
    Eq,
    /// Inequality (`!=` or `<>`).
    NotEq,
    /// Less than (`<`).
    Lt,
    /// Less than or equal (`<=`).
    LtEq,
    /// Greater than (`>`).
    Gt,
    /// Greater than or equal (`>=`).
    GtEq,

    // Logical
    /// Logical AND.
    And,
    /// Logical OR.
    Or,

    // String
    /// LIKE pattern match.
    Like,
    /// NOT LIKE pattern match.
    NotLike,
}

/// A unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum UnaryOp {
    /// Logical NOT.
    Not,
    /// Arithmetic negation (`-`).
    Minus,
    /// Unary plus (`+`).
    Plus,
}

/// A function argument.
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    /// An unnamed argument expression.
    Unnamed(Expr),
    /// A wildcard argument (e.g., `COUNT(*)`).
    Wildcard,
}

/// A FROM clause item: a table/subquery with optional joins.
#[derive(Debug, Clone, PartialEq)]
pub struct TableWithJoins {
    /// The base table or subquery.
    pub relation: TableFactor,
    /// Joins applied to the base relation.
    pub joins: Vec<Join>,
}

/// A table reference or subquery in a FROM clause.
#[derive(Debug, Clone, PartialEq)]
pub enum TableFactor {
    /// A named table reference.
    Table {
        /// The table name (possibly multi-part).
        name: TableReference,
        /// Optional alias for the table.
        alias: Option<String>,
    },
    /// A subquery in parentheses.
    Subquery {
        /// The subquery.
        query: Box<Query>,
        /// Required alias for a derived table.
        alias: String,
    },
}

/// A JOIN clause.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// The table/subquery on the right side of the JOIN.
    pub relation: TableFactor,
    /// The type of join.
    pub join_type: JoinType,
    /// The join condition.
    pub condition: JoinCondition,
}

/// The type of a JOIN operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    /// INNER JOIN.
    Inner,
    /// LEFT [OUTER] JOIN.
    Left,
    /// RIGHT [OUTER] JOIN.
    Right,
    /// FULL [OUTER] JOIN.
    Full,
    /// CROSS JOIN.
    Cross,
}

/// A JOIN condition.
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    /// ON expression.
    On(Expr),
    /// USING (column_list).
    Using(Vec<String>),
    /// No condition (for CROSS JOIN).
    None,
}

/// An expression in an ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    /// The expression to sort by.
    pub expr: Expr,
    /// Sort direction: `Some(true)` = ASC, `Some(false)` = DESC, `None` = default.
    pub asc: Option<bool>,
    /// Nulls ordering: `Some(true)` = NULLS FIRST, `Some(false)` = NULLS LAST, `None` = default.
    pub nulls_first: Option<bool>,
}

// --- Display implementations ---

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Query { .. } => write!(f, "SELECT ..."),
            Statement::Explain { stmt, .. } => write!(f, "EXPLAIN {stmt}"),
            Statement::CreateTable { name, .. } => write!(f, "CREATE TABLE {name}"),
            Statement::DropTable { name, .. } => write!(f, "DROP TABLE {name}"),
            Statement::CreateTableAsSelect { name, .. } => {
                write!(f, "CREATE TABLE {name} AS SELECT ...")
            }
            Statement::InsertInto { table, .. } => write!(f, "INSERT INTO {table}"),
            Statement::DeleteFrom { table, .. } => write!(f, "DELETE FROM {table}"),
            Statement::CreateView { name, .. } => write!(f, "CREATE VIEW {name}"),
            Statement::DropView { name, .. } => write!(f, "DROP VIEW {name}"),
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Plus => write!(f, "+"),
            BinaryOp::Minus => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
            BinaryOp::Divide => write!(f, "/"),
            BinaryOp::Modulo => write!(f, "%"),
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "!="),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Like => write!(f, "LIKE"),
            BinaryOp::NotLike => write!(f, "NOT LIKE"),
        }
    }
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Not => write!(f, "NOT"),
            UnaryOp::Minus => write!(f, "-"),
            UnaryOp::Plus => write!(f, "+"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { col_ref, .. } => {
                if let Some(table) = &col_ref.table {
                    write!(f, "{table}.{}", col_ref.name)
                } else {
                    write!(f, "{}", col_ref.name)
                }
            }
            Expr::Literal { value, .. } => write!(f, "{value}"),
            Expr::BinaryOp {
                left, op, right, ..
            } => write!(f, "{left} {op} {right}"),
            Expr::UnaryOp { op, expr, .. } => write!(f, "{op} {expr}"),
            Expr::Function {
                name,
                args,
                distinct,
                ..
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
            Expr::IsNull { expr, .. } => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull { expr, .. } => write!(f, "{expr} IS NOT NULL"),
            Expr::Between {
                expr,
                negated,
                low,
                high,
                ..
            } => {
                if *negated {
                    write!(f, "{expr} NOT BETWEEN {low} AND {high}")
                } else {
                    write!(f, "{expr} BETWEEN {low} AND {high}")
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
                ..
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
            Expr::Cast {
                expr, data_type, ..
            } => write!(f, "CAST({expr} AS {data_type})"),
            Expr::Nested { expr, .. } => write!(f, "({expr})"),
            Expr::Subquery { .. } => write!(f, "(subquery)"),
            Expr::InSubquery { expr, negated, .. } => {
                if *negated {
                    write!(f, "{expr} NOT IN (subquery)")
                } else {
                    write!(f, "{expr} IN (subquery)")
                }
            }
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                ..
            } => {
                write!(f, "{name}(")?;
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{arg}")?;
                }
                write!(f, ") OVER (")?;
                if !partition_by.is_empty() {
                    write!(f, "PARTITION BY ")?;
                    for (i, p) in partition_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{p}")?;
                    }
                }
                if !order_by.is_empty() {
                    if !partition_by.is_empty() {
                        write!(f, " ")?;
                    }
                    write!(f, "ORDER BY ")?;
                    for (i, o) in order_by.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", o.expr)?;
                    }
                }
                write!(f, ")")
            }
            Expr::Exists { negated, .. } => {
                if *negated {
                    write!(f, "NOT EXISTS (subquery)")
                } else {
                    write!(f, "EXISTS (subquery)")
                }
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                write!(f, "CASE")?;
                if let Some(op) = operand {
                    write!(f, " {op}")?;
                }
                for (cond, res) in conditions.iter().zip(results.iter()) {
                    write!(f, " WHEN {cond} THEN {res}")?;
                }
                if let Some(el) = else_result {
                    write!(f, " ELSE {el}")?;
                }
                write!(f, " END")
            }
            Expr::Parameter { index, .. } => write!(f, "${index}"),
        }
    }
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArg::Unnamed(expr) => write!(f, "{expr}"),
            FunctionArg::Wildcard => write!(f, "*"),
        }
    }
}
