//! Logical plan types for the arneb query engine.
//!
//! These types represent relational algebra operations produced by the
//! query planner. They form a tree that the optimizer transforms and
//! the execution engine evaluates.

use std::fmt;

use arneb_common::types::{ColumnInfo, DataType, ScalarValue, TableReference};
use arneb_common::DynamicFilterId;
use arneb_sql_parser::ast;
use arneb_sql_parser::Span;

/// Plan-time annotation linking a dynamic filter's build-side equi-key
/// column to its matching probe-side column.
///
/// Attached to [`LogicalPlan::Join`] and [`LogicalPlan::SemiJoin`] to
/// declare that this join PRODUCES a dynamic filter from `build_index`
/// (a column position in the join's right child) that the probe-side
/// scan CONSUMES at `probe_index` (a column position in the join's left
/// child, which is downstream of a [`LogicalPlan::TableScan`] that
/// lists this id under `dynamic_filters_consumed`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DynamicFilterProducer {
    /// The id this join produces.
    pub id: DynamicFilterId,
    /// Column index in the join's right (build) child's output schema.
    pub build_index: usize,
    /// Column index in the join's left (probe) child's output schema.
    pub probe_index: usize,
    /// Display name of the column (for `EXPLAIN` / debug).
    pub column_name: String,
}

/// Plan-time annotation declaring that a scan consumes a cross-fragment
/// dynamic filter at runtime.
///
/// Attached to [`LogicalPlan::TableScan`] for each [`DynamicFilterId`]
/// whose build side lives in an upstream fragment.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DynamicFilterConsumer {
    /// The id this scan should wait on.
    pub id: DynamicFilterId,
    /// Column index in this scan's output schema where the filter applies.
    pub column_index: usize,
    /// Display name of the column.
    pub column_name: String,
}

/// An expression within a logical plan.
///
/// Unlike AST expressions, column references here are resolved to their
/// position (index) in the input schema.
///
/// Every variant carries an optional `span` pointing at the SQL source
/// location that produced the node. Expressions synthesized by later
/// analyzer or optimizer passes (inserted casts, rewritten conjuncts,
/// etc.) use `None`. Consumers that need a position for diagnostics
/// should call [`PlanExpr::best_span`] to fall back to the nearest
/// user-visible descendant. The span field is excluded from `serde`
/// serialization so `EXPLAIN (FORMAT JSON)` output stays
/// position-independent.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum PlanExpr {
    /// A column reference resolved to its index in the input schema.
    Column {
        /// Zero-based column index in the input schema.
        index: usize,
        /// Column name (for display purposes).
        name: String,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A literal value.
    Literal {
        /// The scalar value.
        value: ScalarValue,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A binary operation.
    BinaryOp {
        /// Left operand.
        left: Box<PlanExpr>,
        /// Operator.
        op: ast::BinaryOp,
        /// Right operand.
        right: Box<PlanExpr>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A unary operation.
    UnaryOp {
        /// Operator.
        op: ast::UnaryOp,
        /// Operand.
        expr: Box<PlanExpr>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A function call.
    Function {
        /// Function name.
        name: String,
        /// Function arguments.
        args: Vec<PlanExpr>,
        /// Whether DISTINCT was specified.
        distinct: bool,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// `expr IS NULL`.
    IsNull {
        /// Inner expression being tested.
        expr: Box<PlanExpr>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// `expr IS NOT NULL`.
    IsNotNull {
        /// Inner expression being tested.
        expr: Box<PlanExpr>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
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
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// `expr [NOT] IN (list)`.
    InList {
        /// The expression being tested.
        expr: Box<PlanExpr>,
        /// The list of values.
        list: Vec<PlanExpr>,
        /// Whether this is NOT IN.
        negated: bool,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// `CAST(expr AS data_type)`.
    Cast {
        /// The expression to cast.
        expr: Box<PlanExpr>,
        /// The target data type.
        data_type: DataType,
        /// Source span, if derived from user SQL. Casts inserted by
        /// type-coercion rewrites set this to `None`.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A wildcard (`*`) — only used temporarily before expansion.
    Wildcard,
    /// A scalar subquery expression that returns a single value.
    ScalarSubquery {
        /// The subquery's logical plan.
        subplan: Box<LogicalPlan>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// A CASE expression (both searched and simple forms).
    CaseExpr {
        /// For simple CASE: the operand expression. None for searched CASE.
        operand: Option<Box<PlanExpr>>,
        /// Condition/result pairs evaluated in order.
        when_clauses: Vec<(PlanExpr, PlanExpr)>,
        /// Optional ELSE result.
        else_result: Option<Box<PlanExpr>>,
        /// Source span, if derived from user SQL.
        #[serde(skip)]
        span: Option<Span>,
    },
    /// Extended-query-protocol placeholder (`$1`, `$2`, …).
    ///
    /// The analyzer's type-coercion pass walks surrounding operators
    /// and records an inferred [`DataType`] in
    /// [`crate::AnalyzerContext::param_types`]; when a sibling's
    /// type is known, the `type_hint` field is also populated so the
    /// same parameter node carries its type through subsequent passes
    /// (e.g., downstream function-signature check). Placeholders
    /// whose type cannot be inferred default to
    /// [`DataType::Utf8`] at the end of analysis.
    Parameter {
        /// 1-based placeholder index as it appeared in the SQL text.
        index: usize,
        /// Inferred data type, once analyzed.
        type_hint: Option<DataType>,
        /// Source span.
        #[serde(skip)]
        span: Option<Span>,
    },
}

impl PlanExpr {
    /// Returns the source span attached directly to this node, or `None`
    /// if it was synthesized by an analyzer/optimizer pass or is a
    /// sentinel like `Wildcard`.
    pub fn span(&self) -> Option<Span> {
        match self {
            PlanExpr::Column { span, .. }
            | PlanExpr::Literal { span, .. }
            | PlanExpr::BinaryOp { span, .. }
            | PlanExpr::UnaryOp { span, .. }
            | PlanExpr::Function { span, .. }
            | PlanExpr::IsNull { span, .. }
            | PlanExpr::IsNotNull { span, .. }
            | PlanExpr::Between { span, .. }
            | PlanExpr::InList { span, .. }
            | PlanExpr::Cast { span, .. }
            | PlanExpr::ScalarSubquery { span, .. }
            | PlanExpr::CaseExpr { span, .. }
            | PlanExpr::Parameter { span, .. } => *span,
            PlanExpr::Wildcard => None,
        }
    }

    /// Returns this node's own span if present, otherwise walks the
    /// children to find the nearest descendant span. This lets error
    /// reporters point at the nearest user-visible construct even when
    /// the erroring node is synthetic (e.g., a `Cast` inserted by type
    /// coercion).
    pub fn best_span(&self) -> Option<Span> {
        if let Some(s) = self.span() {
            return Some(s);
        }
        match self {
            PlanExpr::BinaryOp { left, right, .. } => {
                left.best_span().or_else(|| right.best_span())
            }
            PlanExpr::UnaryOp { expr, .. }
            | PlanExpr::IsNull { expr, .. }
            | PlanExpr::IsNotNull { expr, .. }
            | PlanExpr::Cast { expr, .. } => expr.best_span(),
            PlanExpr::Function { args, .. } => args.iter().find_map(|a| a.best_span()),
            PlanExpr::Between {
                expr, low, high, ..
            } => expr
                .best_span()
                .or_else(|| low.best_span())
                .or_else(|| high.best_span()),
            PlanExpr::InList { expr, list, .. } => expr
                .best_span()
                .or_else(|| list.iter().find_map(|e| e.best_span())),
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
            } => operand
                .as_deref()
                .and_then(|o| o.best_span())
                .or_else(|| {
                    when_clauses
                        .iter()
                        .find_map(|(c, r)| c.best_span().or_else(|| r.best_span()))
                })
                .or_else(|| else_result.as_deref().and_then(|e| e.best_span())),
            PlanExpr::Column { .. }
            | PlanExpr::Literal { .. }
            | PlanExpr::ScalarSubquery { .. }
            | PlanExpr::Parameter { .. }
            | PlanExpr::Wildcard => None,
        }
    }
}

impl PartialEq for PlanExpr {
    fn eq(&self, other: &Self) -> bool {
        // Compare by display string — sufficient for optimizer tests and dedup.
        // Spans are explicitly excluded so equality is position-independent
        // (two parses of whitespace-different SQL compare equal).
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
        /// Connector-specific properties from the table provider.
        #[serde(default)]
        properties: std::collections::HashMap<String, String>,
        /// Cross-fragment dynamic filters this scan should await before
        /// reading rows. Populated by `assign_dynamic_filter_ids` after
        /// `JoinReorder`; empty when the feature is off or no producing
        /// join exists for this scan.
        #[serde(default)]
        dynamic_filters_consumed: Vec<DynamicFilterConsumer>,
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
        /// Cross-fragment dynamic filters this join produces from its
        /// build (right) side, to be consumed by an upstream probe-side
        /// scan. Empty for non-INNER/RIGHT joins or when the feature is
        /// off. Populated by `assign_dynamic_filter_ids`.
        #[serde(default)]
        dynamic_filter_ids: Vec<DynamicFilterProducer>,
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
    /// Wraps a plan for EXPLAIN [ANALYZE] output.
    Explain {
        /// The plan to explain.
        input: Box<LogicalPlan>,
        /// `EXPLAIN ANALYZE` — when `true`, the inner plan is executed
        /// at render time and actual root-stream row count is emitted
        /// alongside the static plan tree.
        analyze: bool,
    },
    /// Exchange boundary between distributed fragments.
    ExchangeNode {
        /// The stage that produces this exchange's data.
        stage_id: arneb_common::identifiers::StageId,
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
        /// Optional residual predicate evaluated on a joined
        /// (left, right) row pair when an equi-key match is found.
        /// Column indices reference the concatenated joined layout —
        /// left columns first (0..left_width), then right columns
        /// (left_width..left_width+right_width). The match counts as
        /// a semi-match if at least one paired inner row passes this
        /// residual. Used for correlated EXISTS with mixed equi +
        /// non-equi correlation (e.g. TPC-H Q21's `l2.l_suppkey
        /// <> l1.l_suppkey`).
        residual: Option<PlanExpr>,
        /// Cross-fragment dynamic filters this semi-join produces from
        /// its build (right) side. Same semantics as `Join::dynamic_filter_ids`.
        #[serde(default)]
        dynamic_filter_ids: Vec<DynamicFilterProducer>,
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
        /// Optional residual predicate; see [`LogicalPlan::SemiJoin::residual`].
        residual: Option<PlanExpr>,
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
    /// Appends a per-row monotonically increasing Int64 column to the
    /// input. Used by `CorrelatedExistsToLeftJoin` (F-Perf11) to give
    /// each outer-side row a stable identity so we can recover its
    /// per-row EXISTS result after a LEFT JOIN duplicates it.
    /// Mirrors Trino's `AssignUniqueIdOperator`, but the counter is a
    /// single coordinator-wide `AtomicI64` rather than Trino's
    /// `(stageId<<54 | partitionId<<40 | rowId)` encoding.
    AssignUniqueId {
        /// Input plan.
        input: Box<LogicalPlan>,
        /// Name of the appended Int64 column (typically `__rowid`).
        id_column: String,
    },
    /// Produces a single empty (zero-column) row. Used as the synthetic
    /// FROM source for `SELECT <expr>, ...` queries without a FROM
    /// clause (e.g. `SELECT 1`, `SELECT 1 + 1`, health checks). The
    /// surrounding `Projection` evaluates literal/constant expressions
    /// against this one-row batch to produce the actual output.
    OneRow,
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

fn window_function_output_type(func: &WindowFunctionDef, input_schema: &[ColumnInfo]) -> DataType {
    match func.name.to_ascii_uppercase().as_str() {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "COUNT" => DataType::Int64,
        "SUM" | "AVG" => DataType::Float64,
        "MIN" | "MAX" => func
            .args
            .first()
            .and_then(|arg| match arg {
                PlanExpr::Column { index, .. } => input_schema.get(*index),
                _ => None,
            })
            .map(|c| c.data_type.clone())
            .unwrap_or(DataType::Float64),
        _ => DataType::Int64,
    }
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
    /// q21/q02 SF30 silent-truncation fix (2026-06-12): true if this operator
    /// may legitimately stop reading its input *before* EOF, so a mid-stream
    /// dropped receiver on its producer is a real early-stop (not a silent
    /// truncation). The coordinator calls this on a consumer fragment's root
    /// to set [`arneb_rpc::TaskDescriptor::must_drain`] on its producers: a
    /// non-early-stopper that loses its receiver mid-stream was SILENTLY
    /// truncated by an upstream stall/reset (q21), so it must fail loud
    /// instead of returning `Ok` with missing rows.
    ///
    /// Only a **finite `Limit`** short-circuits — and ONLY when no blocking
    /// operator sits between it and the source. This must be *structural*, not
    /// just "is it a `Limit` node" (2026-06-12 hardening): the planner rewrites
    /// `Limit(Sort)` (finite k, no offset) into a `TopKExec` that COLLECTS its
    /// whole input (`operator.rs`), and a `Limit` over any blocking operator
    /// (Sort, aggregate, join, distinct, window) drains the producer to EOF
    /// before the limit ever sees a row. An offset-only / unbounded `Limit`
    /// also reads to EOF. In all those cases the producer must_drain. (A
    /// physical `ExecutionPlan::may_drop_input_early()` would be the ideal home
    /// for this, but `must_drain` is computed at the coordinator on the logical
    /// fragment root; the recursion below faithfully mirrors the physical TopK
    /// rewrite + blocking-operator draining without building the physical plan.)
    pub fn may_stop_input_early(&self) -> bool {
        match self {
            // Unbounded / offset-only Limit reads to EOF — not an early-stopper.
            LogicalPlan::Limit { limit: None, .. } => false,
            // A finite Limit short-circuits its producer UNLESS a blocking
            // operator below it drains the input first.
            LogicalPlan::Limit { input, .. } => !input.drains_input_to_eof(),
            _ => false,
        }
    }

    /// True if executing this plan reads its leaf input stream(s) to EOF
    /// regardless of a downstream `Limit` short-circuit — i.e. a
    /// pipeline-breaking (blocking) operator lies on the chain from this node
    /// to the source. Used by [`Self::may_stop_input_early`] to tell a genuine
    /// `LIMIT` early-stop (`must_drain=false`) from a `Limit` whose input is
    /// fully consumed anyway (`must_drain=true`). See the q21/q02 SF30
    /// silent-truncation fix.
    fn drains_input_to_eof(&self) -> bool {
        match self {
            // Blocking: consume the whole input before/while emitting, so a
            // downstream LIMIT cannot stop the input early. (`Limit(Sort)`
            // becomes a TopKExec that likewise collects its whole input.)
            LogicalPlan::Sort { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::PartialAggregate { .. }
            | LogicalPlan::FinalAggregate { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::SemiJoin { .. }
            | LogicalPlan::AntiJoin { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::Intersect { .. }
            | LogicalPlan::Except { .. }
            | LogicalPlan::Window { .. } => true,
            // Pass-through: forward batches lazily — recurse to the real input
            // so a non-blocking chain (Projection / Filter / nested Limit /
            // AssignUniqueId / Explain) stays short-circuitable by an outer
            // LIMIT.
            LogicalPlan::Projection { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::AssignUniqueId { input, .. }
            | LogicalPlan::Explain { input, .. } => input.drains_input_to_eof(),
            // Leaves, lazy sources (TableScan / ExchangeNode / OneRow), and
            // lazy multi-input (UnionAll): do not drain on their own — an outer
            // LIMIT short-circuits the stream.
            _ => false,
        }
    }

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
            LogicalPlan::Explain { input, .. } => input.schema(),
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
                        data_type: arneb_common::types::DataType::Utf8,
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
                    let data_type = window_function_output_type(f, &schema);
                    schema.push(ColumnInfo {
                        name: f.output_name.clone(),
                        data_type,
                        nullable: true,
                    });
                }
                schema
            }
            LogicalPlan::AssignUniqueId { input, id_column } => {
                let mut schema = input.schema();
                schema.push(ColumnInfo {
                    name: id_column.clone(),
                    data_type: DataType::Int64,
                    nullable: false,
                });
                schema
            }
            LogicalPlan::OneRow => vec![],
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
            PlanExpr::Literal { value, .. } => write!(f, "{value}"),
            PlanExpr::BinaryOp {
                left, op, right, ..
            } => write!(f, "{left} {op} {right}"),
            PlanExpr::UnaryOp { op, expr, .. } => write!(f, "{op} {expr}"),
            PlanExpr::Function {
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
            PlanExpr::IsNull { expr, .. } => write!(f, "{expr} IS NULL"),
            PlanExpr::IsNotNull { expr, .. } => write!(f, "{expr} IS NOT NULL"),
            PlanExpr::Between {
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
            PlanExpr::InList {
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
            PlanExpr::Cast {
                expr, data_type, ..
            } => write!(f, "CAST({expr} AS {data_type})"),
            PlanExpr::Wildcard => write!(f, "*"),
            PlanExpr::ScalarSubquery { .. } => write!(f, "(scalar_subquery)"),
            PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                ..
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
            PlanExpr::Parameter {
                index, type_hint, ..
            } => match type_hint {
                Some(t) => write!(f, "${index}::{t}"),
                None => write!(f, "${index}"),
            },
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
            ..
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
        LogicalPlan::Explain { input, analyze } => {
            if *analyze {
                writeln!(f, "{pad}Explain ANALYZE:")?;
            } else {
                writeln!(f, "{pad}Explain:")?;
            }
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
            residual,
            ..
        } => {
            write!(f, "{pad}SemiJoin: {left_key} = {right_key}")?;
            if let Some(r) = residual {
                write!(f, " AND {r}")?;
            }
            writeln!(f)?;
            fmt_plan(left, f, indent + 1)?;
            fmt_plan(right, f, indent + 1)
        }
        LogicalPlan::AntiJoin {
            left,
            right,
            left_key,
            right_key,
            residual,
        } => {
            write!(f, "{pad}AntiJoin: {left_key} = {right_key}")?;
            if let Some(r) = residual {
                write!(f, " AND {r}")?;
            }
            writeln!(f)?;
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
        LogicalPlan::AssignUniqueId { input, id_column } => {
            writeln!(f, "{pad}AssignUniqueId: {id_column}")?;
            fmt_plan(input, f, indent + 1)
        }
        LogicalPlan::OneRow => writeln!(f, "{pad}OneRow"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_limit_may_stop_input_early() {
        // Limit (LIMIT n / OFFSET) is the ONE operator that legitimately stops
        // reading its input before EOF — its producers tolerate a mid-stream
        // consumer drop (a real LIMIT short-circuit).
        let limit = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::OneRow),
            limit: Some(10),
            offset: None,
        };
        assert!(limit.may_stop_input_early());

        // Must-drain consumers: a mid-stream dropped receiver here means the
        // partition was SILENTLY truncated by an upstream stall (q21), so they
        // must NOT be treated as early-stoppers (the producer must fail loud).
        let sort = LogicalPlan::Sort {
            input: Box::new(LogicalPlan::OneRow),
            order_by: vec![],
        };
        let projection = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::OneRow),
            exprs: vec![],
            schema: vec![],
        };
        assert!(!sort.may_stop_input_early());
        assert!(!projection.may_stop_input_early());
        assert!(!LogicalPlan::OneRow.may_stop_input_early());
    }

    #[test]
    fn limit_early_stop_is_structural_not_just_the_limit_node() {
        // q21/q02 SF30 hardening (2026-06-12): a `Limit` only legitimately
        // stops its producer early when NO blocking operator below it drains
        // the input first. `Limit(Sort)` is rewritten into a draining TopKExec,
        // and `Limit` over any blocking op (aggregate / join / distinct /
        // window) likewise reads the producer to EOF — so those producers
        // must_drain (a mid-stream drop = silent truncation, not early-stop).
        use arneb_common::identifiers::StageId;

        let exchange = || LogicalPlan::ExchangeNode {
            stage_id: StageId(7),
            schema: vec![],
        };
        let finite_limit = |input: LogicalPlan| LogicalPlan::Limit {
            input: Box::new(input),
            limit: Some(10),
            offset: None,
        };

        // Finite LIMIT directly over a lazy exchange — a real short-circuit.
        assert!(finite_limit(exchange()).may_stop_input_early());

        // Through pass-through operators (Projection / Filter) it still
        // short-circuits the producer.
        let projection = LogicalPlan::Projection {
            input: Box::new(exchange()),
            exprs: vec![],
            schema: vec![],
        };
        assert!(finite_limit(projection).may_stop_input_early());
        let filter = LogicalPlan::Filter {
            input: Box::new(exchange()),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            },
        };
        assert!(finite_limit(filter).may_stop_input_early());

        // Over a BLOCKING operator the producer is drained to EOF regardless of
        // the limit → NOT an early-stopper (must_drain).
        let sort = LogicalPlan::Sort {
            input: Box::new(exchange()),
            order_by: vec![],
        };
        assert!(!finite_limit(sort).may_stop_input_early());
        let aggregate = LogicalPlan::Aggregate {
            input: Box::new(exchange()),
            group_by: vec![],
            aggr_exprs: vec![],
            schema: vec![],
        };
        assert!(!finite_limit(aggregate).may_stop_input_early());
        let distinct = LogicalPlan::Distinct {
            input: Box::new(exchange()),
        };
        assert!(!finite_limit(distinct).may_stop_input_early());

        // A blocking operator below a pass-through chain still drains.
        let proj_over_sort = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(exchange()),
                order_by: vec![],
            }),
            exprs: vec![],
            schema: vec![],
        };
        assert!(!finite_limit(proj_over_sort).may_stop_input_early());

        // Offset-only / unbounded LIMIT reads to EOF — not an early-stopper.
        let offset_only = LogicalPlan::Limit {
            input: Box::new(exchange()),
            limit: None,
            offset: Some(5),
        };
        assert!(!offset_only.may_stop_input_early());
    }
}
