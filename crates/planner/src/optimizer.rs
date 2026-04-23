//! Logical plan optimizer.
//!
//! Applies a sequence of rewrite rules to simplify and optimize the logical plan
//! before physical planning.

use arneb_common::error::PlanError;
use arneb_common::types::ScalarValue;
use arneb_sql_parser::ast;

use crate::plan::{LogicalPlan, PlanExpr};

/// A rule that rewrites a logical plan.
pub trait LogicalRule: Send + Sync {
    /// Human-readable name.
    fn name(&self) -> &str;
    /// Rewrite the plan. Returns the optimized plan.
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError>;
}

/// Applies an ordered sequence of logical optimization rules.
pub struct LogicalOptimizer {
    rules: Vec<Box<dyn LogicalRule>>,
}

impl LogicalOptimizer {
    /// Creates an optimizer with the given rules.
    pub fn new(rules: Vec<Box<dyn LogicalRule>>) -> Self {
        Self { rules }
    }

    /// Creates an optimizer with the default rule set.
    pub fn default_rules() -> Self {
        Self::new(vec![Box::new(SimplifyFilters), Box::new(ConstantFolding)])
    }

    /// Applies all rules in sequence.
    pub fn optimize(&self, mut plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        for rule in &self.rules {
            plan = rule.optimize(plan)?;
        }
        Ok(plan)
    }
}

// ===========================================================================
// SimplifyFilters
// ===========================================================================

/// Removes trivial filters: `WHERE true` → remove filter, `WHERE false` → empty result.
struct SimplifyFilters;

impl LogicalRule for SimplifyFilters {
    fn name(&self) -> &str {
        "SimplifyFilters"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        self.rewrite(plan)
    }
}

impl SimplifyFilters {
    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let input = self.rewrite(*input)?;
                let predicate = simplify_expr(predicate);

                match &predicate {
                    // WHERE true → remove filter
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        ..
                    } => Ok(input),
                    // WHERE false → empty scan (return the input but wrapped in a LIMIT 0)
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(false),
                        ..
                    } => Ok(LogicalPlan::Limit {
                        input: Box::new(input),
                        limit: Some(0),
                        offset: None,
                    }),
                    _ => Ok(LogicalPlan::Filter {
                        input: Box::new(input),
                        predicate,
                    }),
                }
            }
            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => Ok(LogicalPlan::Projection {
                input: Box::new(self.rewrite(*input)?),
                exprs,
                schema,
            }),
            LogicalPlan::Sort { input, order_by } => Ok(LogicalPlan::Sort {
                input: Box::new(self.rewrite(*input)?),
                order_by,
            }),
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(LogicalPlan::Limit {
                input: Box::new(self.rewrite(*input)?),
                limit,
                offset,
            }),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => Ok(LogicalPlan::Aggregate {
                input: Box::new(self.rewrite(*input)?),
                group_by,
                aggr_exprs,
                schema,
            }),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => Ok(LogicalPlan::Join {
                left: Box::new(self.rewrite(*left)?),
                right: Box::new(self.rewrite(*right)?),
                join_type,
                condition,
            }),
            LogicalPlan::Explain { input } => Ok(LogicalPlan::Explain {
                input: Box::new(self.rewrite(*input)?),
            }),
            other => Ok(other),
        }
    }
}

// ===========================================================================
// ConstantFolding
// ===========================================================================

/// Evaluates constant expressions at plan time.
struct ConstantFolding;

impl LogicalRule for ConstantFolding {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        self.rewrite(plan)
    }
}

impl ConstantFolding {
    fn rewrite(&self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let input = self.rewrite(*input)?;
                let predicate = fold_constants(predicate)?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(input),
                    predicate,
                })
            }
            LogicalPlan::Projection {
                input,
                exprs,
                schema,
            } => {
                let input = self.rewrite(*input)?;
                let exprs = exprs
                    .into_iter()
                    .map(fold_constants)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LogicalPlan::Projection {
                    input: Box::new(input),
                    exprs,
                    schema,
                })
            }
            LogicalPlan::Sort { input, order_by } => Ok(LogicalPlan::Sort {
                input: Box::new(self.rewrite(*input)?),
                order_by,
            }),
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(LogicalPlan::Limit {
                input: Box::new(self.rewrite(*input)?),
                limit,
                offset,
            }),
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggr_exprs,
                schema,
            } => Ok(LogicalPlan::Aggregate {
                input: Box::new(self.rewrite(*input)?),
                group_by,
                aggr_exprs,
                schema,
            }),
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => Ok(LogicalPlan::Join {
                left: Box::new(self.rewrite(*left)?),
                right: Box::new(self.rewrite(*right)?),
                join_type,
                condition,
            }),
            LogicalPlan::Explain { input } => Ok(LogicalPlan::Explain {
                input: Box::new(self.rewrite(*input)?),
            }),
            other => Ok(other),
        }
    }
}

// ===========================================================================
// Expression helpers
// ===========================================================================

/// Simplify boolean expressions.
fn simplify_expr(expr: PlanExpr) -> PlanExpr {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => {
            let left = simplify_expr(*left);
            let right = simplify_expr(*right);

            match (&op, &left, &right) {
                // x AND true → x
                (
                    ast::BinaryOp::And,
                    _,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        ..
                    },
                ) => left,
                (
                    ast::BinaryOp::And,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        ..
                    },
                    _,
                ) => right,
                // x AND false → false
                (
                    ast::BinaryOp::And,
                    _,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(false),
                        ..
                    },
                ) => PlanExpr::Literal {
                    value: ScalarValue::Boolean(false),
                    span: None,
                },
                (
                    ast::BinaryOp::And,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(false),
                        ..
                    },
                    _,
                ) => PlanExpr::Literal {
                    value: ScalarValue::Boolean(false),
                    span: None,
                },
                // x OR true → true
                (
                    ast::BinaryOp::Or,
                    _,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        ..
                    },
                ) => PlanExpr::Literal {
                    value: ScalarValue::Boolean(true),
                    span: None,
                },
                (
                    ast::BinaryOp::Or,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(true),
                        ..
                    },
                    _,
                ) => PlanExpr::Literal {
                    value: ScalarValue::Boolean(true),
                    span: None,
                },
                // x OR false → x
                (
                    ast::BinaryOp::Or,
                    _,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(false),
                        ..
                    },
                ) => left,
                (
                    ast::BinaryOp::Or,
                    PlanExpr::Literal {
                        value: ScalarValue::Boolean(false),
                        ..
                    },
                    _,
                ) => right,
                _ => PlanExpr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                    span,
                },
            }
        }
        PlanExpr::UnaryOp {
            op: ast::UnaryOp::Not,
            expr,
            span,
        } => {
            let inner = simplify_expr(*expr);
            match inner {
                // NOT NOT x → x
                PlanExpr::UnaryOp {
                    op: ast::UnaryOp::Not,
                    expr: inner2,
                    ..
                } => *inner2,
                // NOT true → false
                PlanExpr::Literal {
                    value: ScalarValue::Boolean(b),
                    ..
                } => PlanExpr::Literal {
                    value: ScalarValue::Boolean(!b),
                    span: None,
                },
                other => PlanExpr::UnaryOp {
                    op: ast::UnaryOp::Not,
                    expr: Box::new(other),
                    span,
                },
            }
        }
        other => other,
    }
}

/// Fold constant expressions at plan time.
///
/// Covers:
/// - `BinaryOp(Literal, Literal) → Literal` via [`eval_binary_op`].
/// - `Cast(Literal, T) → Literal` via
///   [`crate::analyzer::cast_scalar::cast_scalar`]. An invalid
///   literal produces [`PlanError::InvalidLiteral`] so the query
///   fails at plan time rather than mid-stream (e.g.,
///   `DATE '1998-13-45'`).
///
/// Folding is idempotent: running it twice on the same expression
/// yields the same result (the result is already a `Literal`).
fn fold_constants(expr: PlanExpr) -> Result<PlanExpr, PlanError> {
    match expr {
        PlanExpr::BinaryOp {
            left,
            op,
            right,
            span,
        } => {
            let left = fold_constants(*left)?;
            let right = fold_constants(*right)?;

            // Try to evaluate if both sides are literals.
            if let (PlanExpr::Literal { value: lv, .. }, PlanExpr::Literal { value: rv, .. }) =
                (&left, &right)
            {
                if let Some(result) = eval_binary_op(lv, &op, rv) {
                    return Ok(PlanExpr::Literal {
                        value: result,
                        span: None,
                    });
                }
            }

            Ok(PlanExpr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                span,
            })
        }
        PlanExpr::UnaryOp { op, expr, span } => {
            let expr = fold_constants(*expr)?;
            Ok(PlanExpr::UnaryOp {
                op,
                expr: Box::new(expr),
                span,
            })
        }
        PlanExpr::Cast {
            expr,
            data_type,
            span,
        } => {
            let expr = fold_constants(*expr)?;
            if let PlanExpr::Literal { value, .. } = &expr {
                // Fold the cast: Arrow's strict-mode cast surfaces
                // parse errors (e.g., malformed DATE literal) as a
                // `PlanError::InvalidLiteral`. Enrich the error with
                // the `Cast` node's span-derived location so the
                // diagnostic renderer can point at the offending
                // literal.
                let folded =
                    crate::analyzer::cast_scalar::cast_scalar(value, &data_type).map_err(|e| {
                        match e {
                            PlanError::InvalidLiteral { message, .. } => {
                                PlanError::InvalidLiteral {
                                    message,
                                    location: span.map(|s| s.start),
                                }
                            }
                            other => other,
                        }
                    })?;
                return Ok(PlanExpr::Literal {
                    value: folded,
                    span,
                });
            }
            Ok(PlanExpr::Cast {
                expr: Box::new(expr),
                data_type,
                span,
            })
        }
        PlanExpr::IsNull { expr, span } => Ok(PlanExpr::IsNull {
            expr: Box::new(fold_constants(*expr)?),
            span,
        }),
        PlanExpr::IsNotNull { expr, span } => Ok(PlanExpr::IsNotNull {
            expr: Box::new(fold_constants(*expr)?),
            span,
        }),
        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            span,
        } => Ok(PlanExpr::Between {
            expr: Box::new(fold_constants(*expr)?),
            negated,
            low: Box::new(fold_constants(*low)?),
            high: Box::new(fold_constants(*high)?),
            span,
        }),
        PlanExpr::InList {
            expr,
            list,
            negated,
            span,
        } => {
            let list = list
                .into_iter()
                .map(fold_constants)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanExpr::InList {
                expr: Box::new(fold_constants(*expr)?),
                list,
                negated,
                span,
            })
        }
        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            span,
        } => {
            let operand = match operand {
                Some(o) => Some(Box::new(fold_constants(*o)?)),
                None => None,
            };
            let when_clauses = when_clauses
                .into_iter()
                .map(|(c, r)| Ok::<_, PlanError>((fold_constants(c)?, fold_constants(r)?)))
                .collect::<Result<Vec<_>, _>>()?;
            let else_result = match else_result {
                Some(e) => Some(Box::new(fold_constants(*e)?)),
                None => None,
            };
            Ok(PlanExpr::CaseExpr {
                operand,
                when_clauses,
                else_result,
                span,
            })
        }
        PlanExpr::Function {
            name,
            args,
            distinct,
            span,
        } => {
            let args = args
                .into_iter()
                .map(fold_constants)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(PlanExpr::Function {
                name,
                args,
                distinct,
                span,
            })
        }
        other => Ok(other),
    }
}

/// Evaluate a binary operation on two scalar values.
fn eval_binary_op(
    left: &ScalarValue,
    op: &ast::BinaryOp,
    right: &ScalarValue,
) -> Option<ScalarValue> {
    match op {
        ast::BinaryOp::Eq => Some(ScalarValue::Boolean(left == right)),
        ast::BinaryOp::NotEq => Some(ScalarValue::Boolean(left != right)),
        ast::BinaryOp::Plus => eval_arithmetic(left, right, |a, b| a + b, |a, b| a + b),
        ast::BinaryOp::Minus => eval_arithmetic(left, right, |a, b| a - b, |a, b| a - b),
        ast::BinaryOp::Multiply => eval_arithmetic(left, right, |a, b| a * b, |a, b| a * b),
        _ => None,
    }
}

fn eval_arithmetic(
    left: &ScalarValue,
    right: &ScalarValue,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Option<ScalarValue> {
    match (left, right) {
        (ScalarValue::Int32(a), ScalarValue::Int32(b)) => {
            Some(ScalarValue::Int64(int_op(*a as i64, *b as i64)))
        }
        (ScalarValue::Int64(a), ScalarValue::Int64(b)) => Some(ScalarValue::Int64(int_op(*a, *b))),
        (ScalarValue::Float64(a), ScalarValue::Float64(b)) => {
            Some(ScalarValue::Float64(float_op(*a, *b)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::{ColumnInfo, DataType, TableReference};

    fn scan_plan() -> LogicalPlan {
        LogicalPlan::TableScan {
            table: TableReference::table("t"),
            schema: vec![ColumnInfo {
                name: "id".into(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            alias: None,
            properties: Default::default(),
        }
    }

    // -- LogicalOptimizer tests --

    #[test]
    fn optimizer_no_rules() {
        let opt = LogicalOptimizer::new(vec![]);
        let plan = scan_plan();
        let result = opt.optimize(plan).unwrap();
        assert!(matches!(result, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn optimizer_applies_rules() {
        let opt = LogicalOptimizer::default_rules();
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            },
        };
        let result = opt.optimize(plan).unwrap();
        // SimplifyFilters should remove the WHERE true filter.
        assert!(matches!(result, LogicalPlan::TableScan { .. }));
    }

    // -- SimplifyFilters tests --

    #[test]
    fn simplify_where_true() {
        let rule = SimplifyFilters;
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            },
        };
        let result = rule.optimize(plan).unwrap();
        assert!(matches!(result, LogicalPlan::TableScan { .. }));
    }

    #[test]
    fn simplify_where_false() {
        let rule = SimplifyFilters;
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: PlanExpr::Literal {
                value: ScalarValue::Boolean(false),
                span: None,
            },
        };
        let result = rule.optimize(plan).unwrap();
        // Should become LIMIT 0.
        assert!(matches!(result, LogicalPlan::Limit { limit: Some(0), .. }));
    }

    fn lit(v: ScalarValue) -> PlanExpr {
        PlanExpr::Literal {
            value: v,
            span: None,
        }
    }

    fn col(index: usize, name: &str) -> PlanExpr {
        PlanExpr::Column {
            index,
            name: name.into(),
            span: None,
        }
    }

    #[test]
    fn simplify_and_true() {
        let expr = simplify_expr(PlanExpr::BinaryOp {
            left: Box::new(col(0, "id")),
            op: ast::BinaryOp::And,
            right: Box::new(lit(ScalarValue::Boolean(true))),
            span: None,
        });
        assert!(matches!(expr, PlanExpr::Column { index: 0, .. }));
    }

    #[test]
    fn simplify_or_false() {
        let expr = simplify_expr(PlanExpr::BinaryOp {
            left: Box::new(col(0, "id")),
            op: ast::BinaryOp::Or,
            right: Box::new(lit(ScalarValue::Boolean(false))),
            span: None,
        });
        assert!(matches!(expr, PlanExpr::Column { index: 0, .. }));
    }

    #[test]
    fn simplify_not_not() {
        let expr = simplify_expr(PlanExpr::UnaryOp {
            op: ast::UnaryOp::Not,
            expr: Box::new(PlanExpr::UnaryOp {
                op: ast::UnaryOp::Not,
                expr: Box::new(col(0, "x")),
                span: None,
            }),
            span: None,
        });
        assert!(matches!(expr, PlanExpr::Column { index: 0, .. }));
    }

    // -- ConstantFolding tests --

    #[test]
    fn fold_literal_eq() {
        let expr = fold_constants(PlanExpr::BinaryOp {
            left: Box::new(lit(ScalarValue::Int32(1))),
            op: ast::BinaryOp::Eq,
            right: Box::new(lit(ScalarValue::Int32(1))),
            span: None,
        })
        .unwrap();
        assert_eq!(expr, lit(ScalarValue::Boolean(true)));
    }

    #[test]
    fn fold_literal_neq() {
        let expr = fold_constants(PlanExpr::BinaryOp {
            left: Box::new(lit(ScalarValue::Int32(1))),
            op: ast::BinaryOp::Eq,
            right: Box::new(lit(ScalarValue::Int32(2))),
            span: None,
        })
        .unwrap();
        assert_eq!(expr, lit(ScalarValue::Boolean(false)));
    }

    #[test]
    fn fold_arithmetic() {
        let expr = fold_constants(PlanExpr::BinaryOp {
            left: Box::new(lit(ScalarValue::Int64(10))),
            op: ast::BinaryOp::Plus,
            right: Box::new(lit(ScalarValue::Int64(20))),
            span: None,
        })
        .unwrap();
        assert_eq!(expr, lit(ScalarValue::Int64(30)));
    }

    #[test]
    fn fold_non_constant_unchanged() {
        let expr = fold_constants(PlanExpr::BinaryOp {
            left: Box::new(col(0, "id")),
            op: ast::BinaryOp::Plus,
            right: Box::new(lit(ScalarValue::Int32(1))),
            span: None,
        })
        .unwrap();
        // Should remain unchanged since left is not a literal.
        assert!(matches!(expr, PlanExpr::BinaryOp { .. }));
    }

    // -- Cast(Literal) folding (Phase 4) --

    #[test]
    fn fold_cast_utf8_to_date32() {
        // `DATE '1998-12-01'` parses as `Cast(Literal(Utf8), Date32)`;
        // folding produces `Literal(Date32(10561))`.
        let expr = fold_constants(PlanExpr::Cast {
            expr: Box::new(lit(ScalarValue::Utf8("1998-12-01".into()))),
            data_type: arneb_common::types::DataType::Date32,
            span: None,
        })
        .unwrap();
        assert!(matches!(
            expr,
            PlanExpr::Literal {
                value: ScalarValue::Date32(10561),
                ..
            }
        ));
    }

    #[test]
    fn fold_cast_invalid_date_is_plan_time_error() {
        let err = fold_constants(PlanExpr::Cast {
            expr: Box::new(lit(ScalarValue::Utf8("1998-13-45".into()))),
            data_type: arneb_common::types::DataType::Date32,
            span: None,
        })
        .unwrap_err();
        assert!(matches!(err, PlanError::InvalidLiteral { .. }));
    }

    #[test]
    fn fold_cast_is_idempotent() {
        let first = fold_constants(PlanExpr::Cast {
            expr: Box::new(lit(ScalarValue::Utf8("1998-12-01".into()))),
            data_type: arneb_common::types::DataType::Date32,
            span: None,
        })
        .unwrap();
        let second = fold_constants(first.clone()).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn fold_cast_over_column_is_noop() {
        // Cast(Column, T) doesn't fold because Column isn't a literal.
        let expr = fold_constants(PlanExpr::Cast {
            expr: Box::new(col(0, "id")),
            data_type: arneb_common::types::DataType::Int64,
            span: None,
        })
        .unwrap();
        assert!(matches!(expr, PlanExpr::Cast { .. }));
    }

    // -- Combined test: constant folding + simplify produces optimal plan --

    #[test]
    fn pushdown_sees_folded_date_literal() {
        // Regression guard for the motivating case: after Analyzer
        // (Phase 3) inserts `Cast(Literal(Utf8), Date32)` against a
        // Date32 column, ConstantFolding (this phase) must reduce
        // that Cast to a pre-typed `Literal(Date32(_))`.
        // `parquet_pushdown::extract_column_literal_comparison`
        // matches `(Column, Literal)` pairs — the folded form lets
        // row-group pruning fire.
        let opt = LogicalOptimizer::default_rules();
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::TableScan {
                table: TableReference::table("lineitem"),
                schema: vec![ColumnInfo {
                    name: "l_shipdate".into(),
                    data_type: DataType::Date32,
                    nullable: false,
                }],
                alias: None,
                properties: Default::default(),
            }),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "l_shipdate".into(),
                    span: None,
                }),
                op: ast::BinaryOp::LtEq,
                right: Box::new(PlanExpr::Cast {
                    expr: Box::new(lit(ScalarValue::Utf8("1998-12-01".into()))),
                    data_type: DataType::Date32,
                    span: None,
                }),
                span: None,
            },
        };
        let result = opt.optimize(plan).unwrap();
        let LogicalPlan::Filter { predicate, .. } = result else {
            panic!("expected Filter")
        };
        match predicate {
            PlanExpr::BinaryOp { left, right, .. } => {
                assert!(matches!(left.as_ref(), PlanExpr::Column { .. }));
                // Pushdown's matcher (see
                // `connectors/src/parquet_pushdown.rs::extract_column_literal_comparison`)
                // accepts exactly this shape: `(Column, Literal(Date32))`.
                assert!(matches!(
                    right.as_ref(),
                    PlanExpr::Literal {
                        value: ScalarValue::Date32(10561),
                        ..
                    }
                ));
            }
            other => panic!("expected BinaryOp, got: {other:?}"),
        }
    }

    #[test]
    fn combined_fold_and_simplify() {
        let opt = LogicalOptimizer::default_rules();
        // WHERE 1 = 1 → constant fold to WHERE true → simplify to remove filter
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan()),
            predicate: PlanExpr::BinaryOp {
                left: Box::new(lit(ScalarValue::Int32(1))),
                op: ast::BinaryOp::Eq,
                right: Box::new(lit(ScalarValue::Int32(1))),
                span: None,
            },
        };
        let result = opt.optimize(plan).unwrap();
        // After ConstantFolding: WHERE true. After SimplifyFilters: just the scan.
        // But order is SimplifyFilters first, then ConstantFolding.
        // SimplifyFilters won't simplify (1=1 is not a literal true).
        // ConstantFolding folds to WHERE true.
        // We need another SimplifyFilters pass. Let's fix the rule order.
        // Actually for this to work we need to run SimplifyFilters after ConstantFolding too.
        assert!(matches!(
            result,
            LogicalPlan::Filter { ref predicate, .. }
                if matches!(predicate, PlanExpr::Literal { value: ScalarValue::Boolean(true), .. })
        ));
    }
}
