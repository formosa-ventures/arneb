//! AST types for the trino-alt SQL parser.
//!
//! These types represent the subset of SQL supported by trino-alt's MVP.
//! They are produced by converting `sqlparser-rs` AST nodes through
//! the conversion layer in [`crate::convert`].

use std::fmt;

use trino_common::types::{DataType, ScalarValue, TableReference};

/// A top-level SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A SQL query (SELECT).
    Query(Box<Query>),
    /// EXPLAIN followed by a statement.
    Explain(Box<Statement>),
}

/// A complete SQL query with optional ORDER BY, LIMIT, and OFFSET.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// The SELECT body of the query.
    pub body: SelectBody,
    /// ORDER BY clauses.
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT expression.
    pub limit: Option<Box<Expr>>,
    /// OFFSET expression.
    pub offset: Option<Box<Expr>>,
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
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column reference, optionally qualified by a table name.
    Column(ColumnRef),
    /// A literal value.
    Literal(ScalarValue),
    /// A binary operation (`left op right`).
    BinaryOp {
        /// Left operand.
        left: Box<Expr>,
        /// Operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// A unary operation (`op expr`).
    UnaryOp {
        /// Operator.
        op: UnaryOp,
        /// Operand.
        expr: Box<Expr>,
    },
    /// A function call.
    Function {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<FunctionArg>,
        /// Whether DISTINCT was specified (e.g., `COUNT(DISTINCT x)`).
        distinct: bool,
    },
    /// `expr IS NULL`.
    IsNull(Box<Expr>),
    /// `expr IS NOT NULL`.
    IsNotNull(Box<Expr>),
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
    },
    /// `expr [NOT] IN (list)`.
    InList {
        /// The expression being tested.
        expr: Box<Expr>,
        /// The list of values.
        list: Vec<Expr>,
        /// Whether this is NOT IN.
        negated: bool,
    },
    /// `CAST(expr AS data_type)`.
    Cast {
        /// The expression to cast.
        expr: Box<Expr>,
        /// The target data type.
        data_type: DataType,
    },
    /// A parenthesized sub-expression.
    Nested(Box<Expr>),
    /// A subquery expression.
    Subquery(Box<Query>),
}

/// A reference to a column, optionally qualified by a table name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// The column name.
    pub name: String,
    /// Optional table qualifier.
    pub table: Option<String>,
}

/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
            Statement::Query(_) => write!(f, "SELECT ..."),
            Statement::Explain(stmt) => write!(f, "EXPLAIN {stmt}"),
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
            Expr::Column(col) => {
                if let Some(table) = &col.table {
                    write!(f, "{table}.{}", col.name)
                } else {
                    write!(f, "{}", col.name)
                }
            }
            Expr::Literal(val) => write!(f, "{val}"),
            Expr::BinaryOp { left, op, right } => write!(f, "{left} {op} {right}"),
            Expr::UnaryOp { op, expr } => write!(f, "{op} {expr}"),
            Expr::Function {
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
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
            Expr::IsNotNull(expr) => write!(f, "{expr} IS NOT NULL"),
            Expr::Between {
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
            Expr::InList {
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
            Expr::Cast { expr, data_type } => write!(f, "CAST({expr} AS {data_type})"),
            Expr::Nested(expr) => write!(f, "({expr})"),
            Expr::Subquery(_) => write!(f, "(subquery)"),
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
