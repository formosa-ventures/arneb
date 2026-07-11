//! Analyzer phase: pluggable semantic-analysis passes that run between
//! [`crate::QueryPlanner`] and [`crate::LogicalOptimizer`].
//!
//! ```text
//! SQL ──► Parser ──► AST ──► QueryPlanner ──► LogicalPlan ──► Analyzer ──► LogicalPlan ──► LogicalOptimizer ──► physical
//!                                (raw)                       (semantic)     (aligned)       (rewrites for perf)
//! ```
//!
//! Analysis differs from optimization in one contract detail: analyzer
//! passes MAY return [`PlanError`] because they verify semantic
//! correctness (e.g., type coercion detecting incompatible operand
//! types). Optimizer rules, by contract, preserve semantics and never
//! introduce new errors.
//!
//! The pipeline is ordered; each pass sees the previous pass's output.
//! An error from any pass short-circuits the rest — subsequent passes
//! are not invoked. This matches Trino's `IterativeAnalyzer` / Spark
//! Catalyst's `RuleExecutor` contract.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_sql_parser::ast;

use crate::cost::CatalogStats;
use crate::plan::{LogicalPlan, PlanExpr};

pub mod assign_dynamic_filter_ids;
pub mod cast_scalar;
pub mod coercion_matrix;
pub mod correlated_exists_to_leftjoin;
pub mod decorrelate_exists;
pub mod eager_aggregation;
pub mod join_reorder;
pub mod minimal_join_carry;
pub mod narrow_decimals;
pub mod narrow_keys;
pub mod predicate_pushdown;
pub mod pullup_semi_anti;
pub mod semi_join_dedup;
pub mod semi_join_to_inner;
pub mod type_coercion;

pub use assign_dynamic_filter_ids::AssignDynamicFilterIds;
pub use correlated_exists_to_leftjoin::CorrelatedExistsToLeftJoin;
pub use decorrelate_exists::DecorrelateExists;
pub use eager_aggregation::EagerAggregation;
pub use join_reorder::{JoinReorder, ReorderAnnotation, ReorderConfig};
pub use minimal_join_carry::{minimal_join_carry_enabled, MinimalJoinCarry};
pub use narrow_decimals::{narrow_decimals_enabled, NarrowDecimals};
pub use narrow_keys::{narrow_keys_enabled, NarrowKeys};
pub use predicate_pushdown::PredicatePushdown;
pub use pullup_semi_anti::PullupSemiAnti;
pub use semi_join_dedup::SemiJoinDedupBuild;
pub use semi_join_to_inner::SemiJoinToInnerJoin;
pub use type_coercion::TypeCoercion;

#[cfg(test)]
mod hint_tests {
    use super::{parse_hints, Hint};

    #[test]
    fn no_hint_block_returns_empty() {
        let hints = parse_hints("SELECT * FROM t");
        assert!(hints.is_empty());
    }

    #[test]
    fn recognises_no_reorder() {
        let hints = parse_hints("/*+ NO_REORDER */ SELECT * FROM t");
        assert!(hints.contains(Hint::NoReorder));
        assert_eq!(hints.len(), 1);
    }

    #[test]
    fn case_insensitive_match() {
        let hints = parse_hints("/*+ no_reorder */ SELECT * FROM t");
        assert!(hints.contains(Hint::NoReorder));
    }

    #[test]
    fn comma_separated_tokens() {
        let hints = parse_hints("/*+ NO_REORDER, FUTURE_HINT */ SELECT *");
        assert!(hints.contains(Hint::NoReorder));
    }

    #[test]
    fn skips_unrecognised_tokens() {
        let hints = parse_hints("/*+ FAKE_HINT */ SELECT *");
        assert!(!hints.contains(Hint::NoReorder));
        assert!(hints.is_empty());
    }

    #[test]
    fn unclosed_block_does_not_panic() {
        let hints = parse_hints("/*+ NO_REORDER SELECT");
        // Unclosed: parsing bails without recognising the token.
        assert!(hints.is_empty());
    }

    #[test]
    fn ignores_block_after_keyword() {
        let hints = parse_hints("SELECT /*+ NO_REORDER */ * FROM t");
        // Hint must be at the start of the statement to be recognised.
        assert!(hints.is_empty());
    }

    #[test]
    fn skips_leading_whitespace() {
        let hints = parse_hints("   \n  /*+ NO_REORDER */ SELECT");
        assert!(hints.contains(Hint::NoReorder));
    }

    #[test]
    fn skips_leading_line_comment() {
        let hints = parse_hints("-- comment\n/*+ NO_REORDER */ SELECT");
        assert!(hints.contains(Hint::NoReorder));
    }

    #[test]
    fn skips_leading_non_hint_block_comment() {
        let hints = parse_hints("/* not a hint */ /*+ NO_REORDER */ SELECT");
        assert!(hints.contains(Hint::NoReorder));
    }
}

/// Type-check helper: returns the output `DataType` of a plan
/// expression given the input schema, or `None` when the type cannot
/// be determined locally (e.g., function calls without a known
/// signature, wildcard placeholders). Callers use `None` as a signal
/// to skip coercion at that site rather than as a failure.
///
/// This is a planner-level approximation that mirrors the function
/// output-type rules in `QueryPlanner::expr_to_column_info`. It is
/// intentionally detached from the execution `FunctionRegistry` to
/// avoid a reverse dependency from `arneb-planner` onto
/// `arneb-execution`.
pub fn plan_expr_type(expr: &PlanExpr, schema: &[ColumnInfo]) -> Option<DataType> {
    match expr {
        PlanExpr::Column { index, .. } => schema.get(*index).map(|c| c.data_type.clone()),
        PlanExpr::Literal { value, .. } => Some(value.data_type()),
        PlanExpr::BinaryOp {
            left, op, right, ..
        } => {
            if is_boolean_result_op(op) {
                Some(DataType::Boolean)
            } else {
                // Arithmetic: widen to the common supertype.
                let lt = plan_expr_type(left, schema)?;
                let rt = plan_expr_type(right, schema)?;
                coercion_matrix::common_supertype(
                    &lt,
                    &rt,
                    coercion_matrix::CoercionSite::Binary {
                        left_is_literal: is_literal_like(left),
                        right_is_literal: is_literal_like(right),
                    },
                )
            }
        }
        PlanExpr::UnaryOp {
            expr: inner, op, ..
        } => match op {
            ast::UnaryOp::Not => Some(DataType::Boolean),
            _ => plan_expr_type(inner, schema),
        },
        PlanExpr::IsNull { .. } | PlanExpr::IsNotNull { .. } => Some(DataType::Boolean),
        PlanExpr::Between { .. } | PlanExpr::InList { .. } => Some(DataType::Boolean),
        PlanExpr::Cast { data_type, .. } => Some(data_type.clone()),
        PlanExpr::Function { name, args, .. } => function_return_type(name, args, schema),
        PlanExpr::CaseExpr {
            when_clauses,
            else_result,
            ..
        } => {
            // Unify all THEN + ELSE arms.
            let mut acc: Option<DataType> = None;
            for (_, result) in when_clauses {
                let rt = plan_expr_type(result, schema)?;
                acc = Some(match acc {
                    None => rt,
                    Some(a) => coercion_matrix::common_supertype(
                        &a,
                        &rt,
                        coercion_matrix::CoercionSite::CaseBranch {
                            left_is_literal: false,
                            right_is_literal: is_literal_like(result),
                        },
                    )?,
                });
            }
            if let Some(er) = else_result {
                let rt = plan_expr_type(er, schema)?;
                acc = Some(match acc {
                    None => rt,
                    Some(a) => coercion_matrix::common_supertype(
                        &a,
                        &rt,
                        coercion_matrix::CoercionSite::CaseBranch {
                            left_is_literal: false,
                            right_is_literal: is_literal_like(er),
                        },
                    )?,
                });
            }
            acc
        }
        PlanExpr::Parameter { type_hint, .. } => type_hint.clone(),
        PlanExpr::ScalarSubquery { subplan, .. } => {
            subplan.schema().first().map(|col| col.data_type.clone())
        }
        PlanExpr::Wildcard => None,
    }
}

/// True if `op` produces a Boolean result regardless of operand types.
fn is_boolean_result_op(op: &ast::BinaryOp) -> bool {
    matches!(
        op,
        ast::BinaryOp::Eq
            | ast::BinaryOp::NotEq
            | ast::BinaryOp::Lt
            | ast::BinaryOp::LtEq
            | ast::BinaryOp::Gt
            | ast::BinaryOp::GtEq
            | ast::BinaryOp::And
            | ast::BinaryOp::Or
            | ast::BinaryOp::Like
            | ast::BinaryOp::NotLike
    )
}

/// Returns true if `expr` is (or folds to) a literal. Used to gate
/// `Safety::LiteralOnly` matrix rules.
///
/// Today this recognises:
/// - [`PlanExpr::Literal`]
/// - [`PlanExpr::Cast`] whose inner expression is itself literal-like
///   (this covers typed-string literals such as `DATE '1998-12-01'`
///   before [`super::optimizer::ConstantFolding`] reduces them).
///
/// Unary minus over a literal is not recognised as a literal today —
/// add it here if a test case demands it (currently not needed).
pub fn is_literal_like(expr: &PlanExpr) -> bool {
    match expr {
        PlanExpr::Literal { .. } => true,
        PlanExpr::Cast { expr: inner, .. } => is_literal_like(inner),
        _ => false,
    }
}

/// Minimal function return-type rules, mirroring
/// `QueryPlanner::expr_to_column_info`. Only covers aggregates whose
/// return type differs from the argument; scalar built-ins either
/// return the argument type (handled via `arg_types[0]`) or need
/// execution-time resolution (returns `None`).
fn function_return_type(name: &str, args: &[PlanExpr], schema: &[ColumnInfo]) -> Option<DataType> {
    match name.to_uppercase().as_str() {
        "COUNT" => Some(DataType::Int64),
        "SUM" | "AVG" => args
            .first()
            .and_then(|a| plan_expr_type(a, schema))
            .map(|t| match t {
                DataType::Int32 | DataType::Int64 => DataType::Int64,
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::Decimal128 { precision, scale } => {
                    DataType::Decimal128 { precision, scale }
                }
                DataType::Null => DataType::Float64,
                other => other,
            })
            .or(Some(DataType::Float64)),
        "MIN" | "MAX" => args.first().and_then(|a| plan_expr_type(a, schema)),
        "BOOL_OR" => Some(DataType::Boolean),

        // Scalar built-ins. Return types here MUST match the
        // corresponding `ScalarFunction::return_type` impls in
        // `crates/execution/src/functions/*.rs`. The analyzer needs
        // them to type-check expressions like
        // `GROUP BY EXTRACT(YEAR FROM l_shipdate)` where the
        // post-aggregate schema is built before the executor's
        // registry is consulted.
        //
        // Date / time
        "EXTRACT" => Some(DataType::Int64),
        "DATE_TRUNC" => Some(DataType::Date32),
        "CURRENT_DATE" => Some(DataType::Date32),

        // String — Utf8 in, Utf8 out (except length/position which
        // return integer counts).
        "UPPER" | "LOWER" | "SUBSTRING" | "SUBSTR" | "TRIM" | "LTRIM" | "RTRIM" | "CONCAT"
        | "REPLACE" => Some(DataType::Utf8),
        "LENGTH" | "POSITION" => Some(DataType::Int64),

        // Math — preserve integer-ness when the input is integral.
        "ROUND" | "CEIL" | "FLOOR" | "POWER" | "ABS" => args
            .first()
            .and_then(|a| plan_expr_type(a, schema))
            .map(|t| match t {
                DataType::Int32 | DataType::Int64 => DataType::Int64,
                _ => DataType::Float64,
            })
            .or(Some(DataType::Float64)),
        "MOD" => args
            .first()
            .and_then(|a| plan_expr_type(a, schema))
            .or(Some(DataType::Int64)),

        _ => None, // unknown / scalar built-in — defer.
    }
}

/// Per-query mutable state that analysis passes share. Created once per
/// planner invocation and discarded when analysis finishes.
///
/// Carries:
/// - inferred parameter types (`$1`, `$2`, …) populated by `TypeCoercion`,
/// - per-query statistics snapshot consumed by cost-driven passes
///   (`JoinReorder` and the cost model),
/// - parsed query-level hints (`NO_REORDER` etc.) so passes can opt out.
#[derive(Debug, Default)]
pub struct AnalyzerContext {
    /// Inferred types for extended-query protocol placeholders (`$1`,
    /// `$2`, …), keyed by their 1-based index as they appeared in the
    /// source SQL.
    pub param_types: HashMap<usize, DataType>,
    /// Statistics snapshot threaded through the analyzer for cost-based
    /// passes (notably `JoinReorder`). Defaults to empty.
    pub catalog_stats: Arc<CatalogStats>,
    /// Parsed query-level hints — `JoinReorder` checks for `NoReorder`.
    pub hints: HintSet,
}

impl AnalyzerContext {
    /// Construct an empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a context pre-populated with a stats snapshot.
    pub fn with_stats(stats: Arc<CatalogStats>) -> Self {
        Self {
            catalog_stats: stats,
            ..Self::default()
        }
    }
}

/// Query-level hint tags parsed from leading `/*+ ... */` SQL comments.
/// Today only `NoReorder` is recognised.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Hint {
    /// Disable the `JoinReorder` analyzer pass for this statement.
    NoReorder,
}

/// Bag of `Hint` values attached to one statement's `AnalyzerContext`.
#[derive(Debug, Default, Clone)]
pub struct HintSet {
    set: HashSet<Hint>,
}

impl HintSet {
    /// True iff `hint` was attached to this statement.
    pub fn contains(&self, hint: Hint) -> bool {
        self.set.contains(&hint)
    }

    /// Marks `hint` as present.
    pub fn insert(&mut self, hint: Hint) {
        self.set.insert(hint);
    }

    /// Number of hints recorded.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether no hints are recorded.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

/// Parses leading `/*+ ... */` block comments out of `sql` into a
/// [`HintSet`]. Trino-compatible syntax: hints appear at the start of
/// the statement, the `/*+` opener is followed by a comma- or
/// whitespace-separated list of hint tokens, then `*/`.
///
/// Recognised hint tokens (case-insensitive):
/// - `NO_REORDER` → [`Hint::NoReorder`]
///
/// Unrecognised tokens are silently ignored — future hints can be
/// added without breaking older binaries. Anything past the first
/// non-hint, non-whitespace token (e.g. `SELECT`) is not scanned.
pub fn parse_hints(sql: &str) -> HintSet {
    let mut hints = HintSet::default();
    // Skip leading whitespace + arbitrary `--` line comments / `/* */`
    // non-hint block comments. Only `/*+ ... */` (the leading `+`) is a
    // hint block.
    let mut cursor = sql.trim_start();
    loop {
        cursor = cursor.trim_start();
        if let Some(rest) = cursor.strip_prefix("/*+") {
            // Find the matching `*/`. Hint blocks don't nest.
            let end = match rest.find("*/") {
                Some(e) => e,
                None => break, // unclosed — bail
            };
            let body = &rest[..end];
            for token in body.split(|c: char| c.is_whitespace() || c == ',') {
                if token.is_empty() {
                    continue;
                }
                if token.eq_ignore_ascii_case("NO_REORDER") {
                    hints.insert(Hint::NoReorder);
                }
            }
            cursor = &rest[end + 2..];
            continue;
        }
        if let Some(rest) = cursor.strip_prefix("/*") {
            // A non-hint block comment — skip it but keep scanning so a
            // `/*+ ... */` following a normal comment is still found.
            let end = match rest.find("*/") {
                Some(e) => e,
                None => break,
            };
            cursor = &rest[end + 2..];
            continue;
        }
        if let Some(rest) = cursor.strip_prefix("--") {
            // Line comment — skip to next newline.
            match rest.find('\n') {
                Some(n) => cursor = &rest[n + 1..],
                None => break,
            }
            continue;
        }
        // First real token (keyword) — stop scanning for hints.
        break;
    }
    hints
}

/// A single semantic-analysis pass over a [`LogicalPlan`].
///
/// Implementors SHOULD be pure functions: given the same input plan and
/// context, `analyze` returns the same output. Passes MUST NOT mutate
/// observable state outside `ctx`.
pub trait AnalysisPass: Send + Sync {
    /// A short, stable identifier used in diagnostic output and tests.
    fn name(&self) -> &'static str;

    /// Consume a plan and either return a rewritten plan or an error
    /// describing a semantic defect detected during analysis.
    fn analyze(
        &self,
        plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError>;
}

/// Ordered sequence of [`AnalysisPass`] implementations.
///
/// Use [`Analyzer::default_pipeline`] to get the production pipeline
/// used by [`crate::QueryPlanner`]. Use [`Analyzer::new`] to assemble a
/// custom pipeline from unit tests or benchmarks.
pub struct Analyzer {
    passes: Vec<Box<dyn AnalysisPass>>,
}

impl Analyzer {
    /// Build an analyzer with an explicit list of passes. Passes run in
    /// the order provided.
    pub fn new(passes: Vec<Box<dyn AnalysisPass>>) -> Self {
        Self { passes }
    }

    /// Build the empty analyzer — useful as a rollback switch if a
    /// pass is found misbehaving in production. An empty pipeline
    /// preserves the plan unchanged and the context untouched.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Build the production pipeline. Runs [`TypeCoercion`] →
    /// [`PredicatePushdown`] → [`JoinReorder`]. Pushdown precedes
    /// JoinReorder so the cost-based DP sees post-pushdown leaf
    /// cardinalities (e.g., `WHERE r_name = 'AMERICA'` collapses
    /// `region` from 5 rows to 1 before the reorder picks an order).
    pub fn default_pipeline() -> Self {
        // SemiJoinDedupBuild was prototyped here but bench (Q21:
        // 5.5s → 7.1s) showed the Aggregate cost over 6M lineitem
        // rows exceeded the dedup savings (the dedup ratio is ~1.5×,
        // not the 200× we hoped for). The pass is kept in the
        // codebase under `analyzer::semi_join_dedup` for future use
        // when we have cardinality-aware gating; not in the default
        // pipeline. See bench `arneb_20260518_023446.json`.
        // Pipeline ordering:
        //   1. TypeCoercion              — align operand types
        //   2. PredicatePushdown         — push filters down so
        //                                   JoinReorder sees post-pushdown
        //                                   leaf cardinalities
        //   3. SemiJoinToInnerJoin       — when right side is unique on
        //                                   right_key (Q18 GROUP BY),
        //                                   collapse SemiJoin → InnerJoin
        //                                   so the new Inner can join the
        //                                   reorder chain in step 4
        //   4. JoinReorder               — cost-based Selinger DP on the
        //                                   (now possibly enlarged) inner-
        //                                   join chain
        //
        // CorrelatedExistsToLeftJoin (Q21) is implemented + has a
        // matching `StreamingHashAggregateExec` fast path
        // (zero-hash fold-aggregation via Arrow `take`), but the
        // rewrite is still NET LOSS on Q21: the SemiJoinExec path
        // touches ~10K outer rows × residual scan = 6s end-to-end;
        // the LEFT JOIN rewrite emits 18M intermediate rows that even
        // a perfect streaming aggregate spends 24s on. Re-enable
        // either after a Q21-specific cardinality gate (only fire the
        // rewrite when left × right is small) or after the LEFT JOIN
        // residual probe gets a separate optimisation.
        let mut passes: Vec<Box<dyn AnalysisPass>> = vec![
            Box::new(TypeCoercion::new()),
            Box::new(PredicatePushdown::new()),
            Box::new(EagerAggregation::new()),
            Box::new(SemiJoinToInnerJoin::new()),
            Box::new(JoinReorder::new()),
            Box::new(PullupSemiAnti::new()),
            Box::new(DecorrelateExists::new()),
            Box::new(MinimalJoinCarry::new()),
            // Annotates eligible joins + probe-side scans with cross-
            // fragment dynamic filter IDs. Runtime is currently a no-op
            // (consumers wired in A1.4, producers in A1.5, default ON
            // in A1.6); presence of annotations alone must not affect
            // results — trino-diff verifies this.
            Box::new(AssignDynamicFilterIds::new()),
        ];
        if narrow_keys_enabled() {
            passes.insert(1, Box::new(NarrowKeys::new()));
            if narrow_decimals_enabled() {
                passes.insert(2, Box::new(NarrowDecimals::new()));
            }
        } else if narrow_decimals_enabled() {
            passes.insert(1, Box::new(NarrowDecimals::new()));
        }
        Self::new(passes)
    }

    /// Run every pass in order, threading `ctx` through each. The first
    /// pass to return an error short-circuits the pipeline; the
    /// remaining passes are not invoked.
    pub fn run(
        &self,
        mut plan: LogicalPlan,
        ctx: &mut AnalyzerContext,
    ) -> Result<LogicalPlan, PlanError> {
        for pass in &self.passes {
            plan = pass.analyze(plan, ctx)?;
        }
        Ok(plan)
    }
}

impl Default for Analyzer {
    fn default() -> Self {
        Self::default_pipeline()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, TableReference};

    fn stub_plan() -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("t"),
            schema: vec![ColumnInfo {
                name: "a".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
            dynamic_filters_consumed: Vec::new(),
        }
    }

    /// Records the order each pass runs so tests can assert ordering.
    struct TrackingPass {
        name: &'static str,
        log: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl AnalysisPass for TrackingPass {
        fn name(&self) -> &'static str {
            self.name
        }
        fn analyze(
            &self,
            plan: LogicalPlan,
            _ctx: &mut AnalyzerContext,
        ) -> Result<LogicalPlan, PlanError> {
            self.log.lock().unwrap().push(self.name);
            Ok(plan)
        }
    }

    /// Always fails — used to verify error short-circuits the pipeline.
    struct FailingPass;

    impl AnalysisPass for FailingPass {
        fn name(&self) -> &'static str {
            "failing"
        }
        fn analyze(
            &self,
            _plan: LogicalPlan,
            _ctx: &mut AnalyzerContext,
        ) -> Result<LogicalPlan, PlanError> {
            Err(PlanError::InternalError(
                "failing pass injected an error".to_string(),
            ))
        }
    }

    #[test]
    fn empty_pipeline_returns_plan_unchanged() {
        let before = stub_plan();
        let before_str = before.to_string();
        let mut ctx = AnalyzerContext::new();
        let after = Analyzer::empty().run(before, &mut ctx).unwrap();
        assert_eq!(after.to_string(), before_str);
        assert!(ctx.param_types.is_empty());
    }

    #[test]
    fn passes_run_in_declared_order() {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let analyzer = Analyzer::new(vec![
            Box::new(TrackingPass {
                name: "alpha",
                log: log.clone(),
            }),
            Box::new(TrackingPass {
                name: "beta",
                log: log.clone(),
            }),
            Box::new(TrackingPass {
                name: "gamma",
                log: log.clone(),
            }),
        ]);
        let mut ctx = AnalyzerContext::new();
        analyzer.run(stub_plan(), &mut ctx).unwrap();
        assert_eq!(*log.lock().unwrap(), vec!["alpha", "beta", "gamma"]);
    }

    #[test]
    fn pipeline_short_circuits_on_error() {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let analyzer = Analyzer::new(vec![
            Box::new(TrackingPass {
                name: "alpha",
                log: log.clone(),
            }),
            Box::new(FailingPass),
            // Should NOT run:
            Box::new(TrackingPass {
                name: "gamma",
                log: log.clone(),
            }),
        ]);
        let mut ctx = AnalyzerContext::new();
        let err = analyzer.run(stub_plan(), &mut ctx).unwrap_err();
        assert!(matches!(err, PlanError::InternalError(_)));
        assert_eq!(*log.lock().unwrap(), vec!["alpha"]);
    }

    #[test]
    fn default_pipeline_is_callable() {
        // Until phases 3/5 land, the default pipeline is empty — this
        // test asserts the public surface is callable so plan_statement
        // can invoke it unconditionally.
        let mut ctx = AnalyzerContext::new();
        let plan = Analyzer::default_pipeline()
            .run(stub_plan(), &mut ctx)
            .unwrap();
        assert!(matches!(plan, LogicalPlan::TableScan { .. }));
    }
}
