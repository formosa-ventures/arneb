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

use std::collections::HashMap;

use arneb_common::error::PlanError;
use arneb_common::types::{ColumnInfo, DataType};
use arneb_sql_parser::ast;

use crate::plan::{LogicalPlan, PlanExpr};

pub mod cast_scalar;
pub mod coercion_matrix;
pub mod type_coercion;

pub use type_coercion::TypeCoercion;

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
        PlanExpr::ScalarSubquery { .. } | PlanExpr::Wildcard => None,
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
                other => other,
            }),
        "MIN" | "MAX" => args.first().and_then(|a| plan_expr_type(a, schema)),
        _ => None, // unknown / scalar built-in — defer.
    }
}

/// Per-query mutable state that analysis passes share. Created once per
/// planner invocation and discarded when analysis finishes.
///
/// Today it carries inferred parameter types only; future passes
/// (function-overload resolution, subquery-decorrelation) will extend
/// this struct with their own state.
#[derive(Debug, Default)]
pub struct AnalyzerContext {
    /// Inferred types for extended-query protocol placeholders (`$1`,
    /// `$2`, …), keyed by their 1-based index as they appeared in the
    /// source SQL.
    pub param_types: HashMap<usize, DataType>,
}

impl AnalyzerContext {
    /// Construct an empty context.
    pub fn new() -> Self {
        Self::default()
    }
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

    /// Build the production pipeline. Runs [`TypeCoercion`] followed
    /// by any future analysis passes.
    pub fn default_pipeline() -> Self {
        Self::new(vec![Box::new(TypeCoercion::new())])
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
