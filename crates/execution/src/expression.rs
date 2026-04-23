//! Expression evaluator for the execution engine.
//!
//! Evaluates [`PlanExpr`] nodes against an Arrow [`RecordBatch`], producing
//! an [`ArrayRef`] result. Uses Arrow compute kernels for all operations.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::ScalarValue;
use arneb_planner::PlanExpr;
use arneb_sql_parser::ast;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use arrow::compute::kernels;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;

use crate::functions::FunctionRegistry;

/// Evaluates a plan expression against a record batch, producing a columnar result.
///
/// When `registry` is provided, scalar function calls are resolved through it.
/// Aggregate functions are still handled by the aggregate operator, not here.
pub(crate) fn evaluate(
    expr: &PlanExpr,
    batch: &RecordBatch,
    registry: Option<&FunctionRegistry>,
) -> Result<ArrayRef, ExecutionError> {
    match expr {
        PlanExpr::Column { index, .. } => {
            if *index >= batch.num_columns() {
                return Err(ExecutionError::InvalidOperation(format!(
                    "column index {} out of bounds (batch has {} columns)",
                    index,
                    batch.num_columns()
                )));
            }
            Ok(batch.column(*index).clone())
        }

        PlanExpr::Literal { value, .. } => scalar_to_array(value, batch.num_rows()),

        PlanExpr::BinaryOp {
            left, op, right, ..
        } => {
            let left_arr = evaluate(left, batch, registry)?;
            let right_arr = evaluate(right, batch, registry)?;
            evaluate_binary_op(&left_arr, op, &right_arr)
        }

        PlanExpr::UnaryOp { op, expr, .. } => {
            let arr = evaluate(expr, batch, registry)?;
            evaluate_unary_op(op, &arr)
        }

        PlanExpr::IsNull { expr, .. } => {
            let arr = evaluate(expr, batch, registry)?;
            let result = kernels::boolean::is_null(&arr)?;
            Ok(Arc::new(result))
        }

        PlanExpr::IsNotNull { expr, .. } => {
            let arr = evaluate(expr, batch, registry)?;
            let result = kernels::boolean::is_not_null(&arr)?;
            Ok(Arc::new(result))
        }

        PlanExpr::Cast {
            expr, data_type, ..
        } => {
            let arr = evaluate(expr, batch, registry)?;
            let arrow_type: ArrowDataType = data_type.clone().into();
            let result = arrow::compute::cast(&arr, &arrow_type)?;
            Ok(result)
        }

        PlanExpr::Between {
            expr,
            negated,
            low,
            high,
            ..
        } => {
            // expr BETWEEN low AND high  ≡  expr >= low AND expr <= high
            let val = evaluate(expr, batch, registry)?;
            let low_val = evaluate(low, batch, registry)?;
            let high_val = evaluate(high, batch, registry)?;

            let ge_low = compare_op(&val, &low_val, CompareOp::GtEq)?;
            let le_high = compare_op(&val, &high_val, CompareOp::LtEq)?;
            let result = kernels::boolean::and(&ge_low, &le_high)?;

            if *negated {
                let negated_result = kernels::boolean::not(&result)?;
                Ok(Arc::new(negated_result))
            } else {
                Ok(Arc::new(result))
            }
        }

        PlanExpr::InList {
            expr,
            list,
            negated,
            ..
        } => {
            let val = evaluate(expr, batch, registry)?;
            // OR together equality checks for each list item
            let mut result: Option<BooleanArray> = None;
            for item in list {
                let item_val = evaluate(item, batch, registry)?;
                let eq = compare_op(&val, &item_val, CompareOp::Eq)?;
                result = Some(match result {
                    Some(prev) => kernels::boolean::or(&prev, &eq)?,
                    None => eq,
                });
            }

            let final_result =
                result.unwrap_or_else(|| BooleanArray::from(vec![false; batch.num_rows()]));

            if *negated {
                let negated_result = kernels::boolean::not(&final_result)?;
                Ok(Arc::new(negated_result))
            } else {
                Ok(Arc::new(final_result))
            }
        }

        PlanExpr::Function {
            name,
            args,
            distinct: _,
            ..
        } => {
            // Try scalar function registry first
            if let Some(reg) = registry {
                if let Some(func) = reg.get(name) {
                    let evaluated_args: Vec<ArrayRef> = args
                        .iter()
                        .map(|a| evaluate(a, batch, registry))
                        .collect::<Result<Vec<_>, _>>()?;
                    return func.evaluate(&evaluated_args);
                }
            }
            // Not a known scalar function — aggregate functions are handled by
            // HashAggregateExec, not here.
            Err(ExecutionError::InvalidOperation(format!(
                "unknown scalar function: {name}; aggregate functions are handled by the aggregate operator"
            )))
        }

        PlanExpr::ScalarSubquery { .. } => {
            // Execute the subquery plan and return a scalar value repeated for all rows
            // This requires an ExecutionContext which we don't have here.
            // For now, return an error — scalar subqueries in expressions need
            // to be pre-evaluated at the operator level.
            Err(ExecutionError::InvalidOperation(
                "scalar subquery in expression requires pre-evaluation at operator level"
                    .to_string(),
            ))
        }

        PlanExpr::CaseExpr {
            operand,
            when_clauses,
            else_result,
            ..
        } => evaluate_case(
            operand.as_deref(),
            when_clauses,
            else_result.as_deref(),
            batch,
            registry,
        ),

        PlanExpr::Wildcard => Err(ExecutionError::InvalidOperation(
            "wildcard should have been expanded during planning".to_string(),
        )),

        // Parameters (`$1`, `$2`, …) must be substituted with
        // literals before the plan reaches the evaluator. The
        // extended-query handler in `crates/protocol` does this via
        // `bind_parameters` before execution; a `Parameter` node
        // reaching this point is an internal invariant violation.
        PlanExpr::Parameter { index, .. } => Err(ExecutionError::InvalidOperation(format!(
            "unbound parameter ${index}; extended-query protocol must Bind all parameters before Execute"
        ))),
    }
}

/// Converts a [`ScalarValue`] to an Arrow array of the given length.
pub(crate) fn scalar_to_array(
    value: &ScalarValue,
    num_rows: usize,
) -> Result<ArrayRef, ExecutionError> {
    match value {
        ScalarValue::Null => Ok(Arc::new(NullArray::new(num_rows))),
        ScalarValue::Boolean(v) => Ok(Arc::new(BooleanArray::from(vec![*v; num_rows]))),
        ScalarValue::Int32(v) => Ok(Arc::new(Int32Array::from(vec![*v; num_rows]))),
        ScalarValue::Int64(v) => Ok(Arc::new(Int64Array::from(vec![*v; num_rows]))),
        ScalarValue::Float32(v) => Ok(Arc::new(Float32Array::from(vec![*v; num_rows]))),
        ScalarValue::Float64(v) => Ok(Arc::new(Float64Array::from(vec![*v; num_rows]))),
        ScalarValue::Utf8(v) => Ok(Arc::new(StringArray::from(vec![v.as_str(); num_rows]))),
        ScalarValue::Binary(v) => Ok(Arc::new(arrow::array::BinaryArray::from(vec![
            v.as_slice();
            num_rows
        ]))),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let arr = arrow::array::Decimal128Array::from(vec![*value; num_rows])
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| {
                    ExecutionError::InvalidOperation(format!(
                        "invalid decimal precision/scale: {e}"
                    ))
                })?;
            Ok(Arc::new(arr))
        }
        ScalarValue::Date32(v) => Ok(Arc::new(arrow::array::Date32Array::from(vec![
            *v;
            num_rows
        ]))),
        ScalarValue::Timestamp {
            value,
            unit,
            timezone,
        } => {
            let arrow_unit: arrow::datatypes::TimeUnit = (*unit).into();
            let tz: Option<Arc<str>> = timezone.as_ref().map(|s| Arc::from(s.as_str()));
            let arr = match arrow_unit {
                arrow::datatypes::TimeUnit::Second => Arc::new(
                    arrow::array::TimestampSecondArray::from(vec![*value; num_rows])
                        .with_timezone_opt(tz),
                ) as ArrayRef,
                arrow::datatypes::TimeUnit::Millisecond => Arc::new(
                    arrow::array::TimestampMillisecondArray::from(vec![*value; num_rows])
                        .with_timezone_opt(tz),
                ) as ArrayRef,
                arrow::datatypes::TimeUnit::Microsecond => Arc::new(
                    arrow::array::TimestampMicrosecondArray::from(vec![*value; num_rows])
                        .with_timezone_opt(tz),
                ) as ArrayRef,
                arrow::datatypes::TimeUnit::Nanosecond => Arc::new(
                    arrow::array::TimestampNanosecondArray::from(vec![*value; num_rows])
                        .with_timezone_opt(tz),
                ) as ArrayRef,
            };
            Ok(arr)
        }
        _ => Err(ExecutionError::InvalidOperation(
            "unsupported scalar type for array conversion".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum CompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Type-dispatch comparison producing a BooleanArray.
///
/// Operands MUST have the same Arrow type — the planner's
/// [`arneb_planner::analyzer::TypeCoercion`] pass inserts `Cast`
/// nodes so execution only ever sees pre-aligned inputs. A
/// mismatched call here indicates the analyzer was bypassed or
/// regressed; we fail loudly with a clear error rather than silently
/// coercing (historical behavior).
fn compare_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: CompareOp,
) -> Result<BooleanArray, ExecutionError> {
    use arrow::array::AsArray;
    use arrow::datatypes::*;

    if left.data_type() != right.data_type() {
        return Err(ExecutionError::InvalidOperation(format!(
            "internal: compare_op received mismatched types {lt:?} vs {rt:?}; analyzer should have inserted Cast",
            lt = left.data_type(),
            rt = right.data_type()
        )));
    }
    let left = left.clone();
    let right = right.clone();

    match left.data_type() {
        ArrowDataType::Int32 => {
            let l = left.as_primitive::<Int32Type>();
            let r = right.as_primitive::<Int32Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Int64 => {
            let l = left.as_primitive::<Int64Type>();
            let r = right.as_primitive::<Int64Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Float32 => {
            let l = left.as_primitive::<Float32Type>();
            let r = right.as_primitive::<Float32Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Float64 => {
            let l = left.as_primitive::<Float64Type>();
            let r = right.as_primitive::<Float64Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Utf8 => {
            let l = left.as_string::<i32>();
            let r = right.as_string::<i32>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Boolean => {
            let l = left.as_boolean();
            let r = right.as_boolean();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Date32 => {
            let l = left.as_primitive::<Date32Type>();
            let r = right.as_primitive::<Date32Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Decimal128(_, _) => {
            let l = left.as_primitive::<Decimal128Type>();
            let r = right.as_primitive::<Decimal128Type>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Timestamp(TimeUnit::Second, _) => {
            let l = left.as_primitive::<TimestampSecondType>();
            let r = right.as_primitive::<TimestampSecondType>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            let l = left.as_primitive::<TimestampMillisecondType>();
            let r = right.as_primitive::<TimestampMillisecondType>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            let l = left.as_primitive::<TimestampMicrosecondType>();
            let r = right.as_primitive::<TimestampMicrosecondType>();
            typed_cmp(l, r, op)
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let l = left.as_primitive::<TimestampNanosecondType>();
            let r = right.as_primitive::<TimestampNanosecondType>();
            typed_cmp(l, r, op)
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "comparison not supported for type {dt:?}"
        ))),
    }
}

fn typed_cmp<T: arrow::array::Datum>(
    left: &T,
    right: &T,
    op: CompareOp,
) -> Result<BooleanArray, ExecutionError> {
    let result = match op {
        CompareOp::Eq => kernels::cmp::eq(left, right)?,
        CompareOp::NotEq => kernels::cmp::neq(left, right)?,
        CompareOp::Lt => kernels::cmp::lt(left, right)?,
        CompareOp::LtEq => kernels::cmp::lt_eq(left, right)?,
        CompareOp::Gt => kernels::cmp::gt(left, right)?,
        CompareOp::GtEq => kernels::cmp::gt_eq(left, right)?,
    };
    Ok(result)
}

/// Evaluate a binary operation (arithmetic, comparison, logical, string).
fn evaluate_binary_op(
    left: &ArrayRef,
    op: &ast::BinaryOp,
    right: &ArrayRef,
) -> Result<ArrayRef, ExecutionError> {
    match op {
        // Arithmetic
        ast::BinaryOp::Plus => arithmetic_op(left, right, ArithOp::Add),
        ast::BinaryOp::Minus => arithmetic_op(left, right, ArithOp::Sub),
        ast::BinaryOp::Multiply => arithmetic_op(left, right, ArithOp::Mul),
        ast::BinaryOp::Divide => arithmetic_op(left, right, ArithOp::Div),
        ast::BinaryOp::Modulo => arithmetic_op(left, right, ArithOp::Rem),

        // Comparison
        ast::BinaryOp::Eq => Ok(Arc::new(compare_op(left, right, CompareOp::Eq)?)),
        ast::BinaryOp::NotEq => Ok(Arc::new(compare_op(left, right, CompareOp::NotEq)?)),
        ast::BinaryOp::Lt => Ok(Arc::new(compare_op(left, right, CompareOp::Lt)?)),
        ast::BinaryOp::LtEq => Ok(Arc::new(compare_op(left, right, CompareOp::LtEq)?)),
        ast::BinaryOp::Gt => Ok(Arc::new(compare_op(left, right, CompareOp::Gt)?)),
        ast::BinaryOp::GtEq => Ok(Arc::new(compare_op(left, right, CompareOp::GtEq)?)),

        // Logical
        ast::BinaryOp::And => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation("AND requires boolean operands".to_string())
                })?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation("AND requires boolean operands".to_string())
                })?;
            Ok(Arc::new(kernels::boolean::and(l, r)?))
        }
        ast::BinaryOp::Or => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation("OR requires boolean operands".to_string())
                })?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation("OR requires boolean operands".to_string())
                })?;
            Ok(Arc::new(kernels::boolean::or(l, r)?))
        }

        // String pattern matching
        ast::BinaryOp::Like => {
            let result = kernels::comparison::like(
                left.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ExecutionError::InvalidOperation("LIKE requires string operands".to_string())
                })?,
                right
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation("LIKE requires string pattern".to_string())
                    })?,
            )?;
            Ok(Arc::new(result))
        }
        ast::BinaryOp::NotLike => {
            let result = kernels::comparison::nlike(
                left.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "NOT LIKE requires string operands".to_string(),
                    )
                })?,
                right
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ExecutionError::InvalidOperation(
                            "NOT LIKE requires string pattern".to_string(),
                        )
                    })?,
            )?;
            Ok(Arc::new(result))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

/// Arithmetic on two Arrow arrays. Like [`compare_op`], operands
/// MUST have the same Arrow type — the analyzer inserts `Cast`
/// nodes to guarantee this.
fn arithmetic_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: ArithOp,
) -> Result<ArrayRef, ExecutionError> {
    if left.data_type() != right.data_type() {
        return Err(ExecutionError::InvalidOperation(format!(
            "internal: arithmetic_op received mismatched types {lt:?} vs {rt:?}; analyzer should have inserted Cast",
            lt = left.data_type(),
            rt = right.data_type()
        )));
    }
    let result: ArrayRef = match op {
        ArithOp::Add => kernels::numeric::add(left, right)?,
        ArithOp::Sub => kernels::numeric::sub(left, right)?,
        ArithOp::Mul => kernels::numeric::mul(left, right)?,
        ArithOp::Div => kernels::numeric::div(left, right)?,
        ArithOp::Rem => kernels::numeric::rem(left, right)?,
    };
    Ok(result)
}

/// Evaluates a unary operation.
fn evaluate_unary_op(op: &ast::UnaryOp, arr: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    match op {
        ast::UnaryOp::Not => {
            let bool_arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ExecutionError::InvalidOperation("NOT requires boolean operand".to_string())
            })?;
            Ok(Arc::new(kernels::boolean::not(bool_arr)?))
        }
        ast::UnaryOp::Minus => Ok(kernels::numeric::neg(arr)?),
        ast::UnaryOp::Plus => Ok(arr.clone()),
    }
}

// The runtime coercion helpers `coerce_numeric_pair` and
// `wider_numeric_type` have been deleted. Implicit type alignment is
// now a planner concern — `arneb_planner::analyzer::TypeCoercion`
// inserts `Cast` nodes so that every `compare_op` / `arithmetic_op`
// call receives pre-aligned operands. See the `planner-type-coercion`
// OpenSpec change for rationale and gated deletion plan.

/// Evaluate a CASE expression.
///
/// For searched CASE (operand is None): evaluate each when_clause condition as a boolean,
/// pick the first matching result.
/// For simple CASE (operand is Some): compare operand to each when_clause condition using equality.
fn evaluate_case(
    operand: Option<&PlanExpr>,
    when_clauses: &[(PlanExpr, PlanExpr)],
    else_result: Option<&PlanExpr>,
    batch: &RecordBatch,
    registry: Option<&FunctionRegistry>,
) -> Result<ArrayRef, ExecutionError> {
    let num_rows = batch.num_rows();

    // Evaluate the operand once if this is a simple CASE
    let operand_arr = match operand {
        Some(op) => Some(evaluate(op, batch, registry)?),
        None => None,
    };

    // Track which rows have already been assigned a result
    let mut assigned = vec![false; num_rows];
    // Collect (condition_mask, result_array) for each WHEN clause
    let mut branches: Vec<(BooleanArray, ArrayRef)> = Vec::new();

    for (cond, result) in when_clauses {
        let cond_bool = if let Some(ref op_arr) = operand_arr {
            // Simple CASE: compare operand to condition value
            let cond_arr = evaluate(cond, batch, registry)?;
            compare_op(op_arr, &cond_arr, CompareOp::Eq)?
        } else {
            // Searched CASE: condition should evaluate to boolean
            let cond_arr = evaluate(cond, batch, registry)?;
            cond_arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ExecutionError::InvalidOperation(
                        "CASE WHEN condition must be boolean".to_string(),
                    )
                })?
                .clone()
        };

        // Mask: only consider rows not yet assigned
        // A NULL condition is treated as not-matched
        let effective_mask: BooleanArray = (0..num_rows)
            .map(|i| {
                if assigned[i] || cond_bool.is_null(i) {
                    Some(false)
                } else {
                    Some(cond_bool.value(i))
                }
            })
            .collect();

        // Mark newly assigned rows
        for (i, flag) in assigned.iter_mut().enumerate().take(num_rows) {
            if !effective_mask.is_null(i) && effective_mask.value(i) {
                *flag = true;
            }
        }

        let result_arr = evaluate(result, batch, registry)?;
        branches.push((effective_mask, result_arr));
    }

    // Evaluate ELSE if provided
    let else_arr = match else_result {
        Some(el) => Some(evaluate(el, batch, registry)?),
        None => None,
    };

    // Determine output type from the first non-null branch result, or else, or default to Utf8
    let output_type = branches
        .iter()
        .map(|(_, arr)| arr.data_type().clone())
        .chain(else_arr.iter().map(|a| a.data_type().clone()))
        .find(|dt| *dt != ArrowDataType::Null)
        .unwrap_or(ArrowDataType::Utf8);

    // Build the result array row by row using arrow's MutableArrayData
    // Strategy: build nullable arrays for each branch, then select per-row
    // Simpler approach: build result using take/interleave patterns
    // For simplicity, we build the result by iterating rows

    // Cast all branch results and else to the output type
    let cast_branches: Vec<(BooleanArray, ArrayRef)> = branches
        .into_iter()
        .map(|(mask, arr)| {
            let casted = if arr.data_type() == &output_type {
                arr
            } else if *arr.data_type() == ArrowDataType::Null {
                // Create a null array of the target type
                new_null_array(&output_type, num_rows)
            } else {
                arrow::compute::cast(&arr, &output_type).map_err(|e| {
                    ExecutionError::InvalidOperation(format!("CASE branch type cast failed: {e}"))
                })?
            };
            Ok((mask, casted))
        })
        .collect::<Result<Vec<_>, ExecutionError>>()?;

    let else_casted = match else_arr {
        Some(arr) => {
            if arr.data_type() == &output_type {
                Some(arr)
            } else if *arr.data_type() == ArrowDataType::Null {
                Some(new_null_array(&output_type, num_rows))
            } else {
                Some(arrow::compute::cast(&arr, &output_type).map_err(|e| {
                    ExecutionError::InvalidOperation(format!("CASE ELSE type cast failed: {e}"))
                })?)
            }
        }
        None => None,
    };

    // Build result using arrow interleave: for each row, pick from the right source
    // We'll use indices into sources array
    let mut sources: Vec<&ArrayRef> = cast_branches.iter().map(|(_, arr)| arr).collect();
    let null_arr;
    if let Some(ref ea) = else_casted {
        sources.push(ea);
    } else {
        null_arr = new_null_array(&output_type, num_rows);
        sources.push(&null_arr);
    }
    let else_idx = sources.len() - 1;

    let indices: Vec<(usize, usize)> = (0..num_rows)
        .map(|row| {
            for (branch_idx, (mask, _)) in cast_branches.iter().enumerate() {
                if !mask.is_null(row) && mask.value(row) {
                    return (branch_idx, row);
                }
            }
            (else_idx, row)
        })
        .collect();

    let source_refs: Vec<&dyn Array> = sources.iter().map(|a| a.as_ref()).collect();
    let result = arrow::compute::interleave(&source_refs, &indices)?;

    Ok(result)
}

/// Create a null array of the given type and length.
fn new_null_array(data_type: &ArrowDataType, len: usize) -> ArrayRef {
    arrow::array::new_null_array(data_type, len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Int32, false),
            Field::new("b", ArrowDataType::Int64, false),
            Field::new("c", ArrowDataType::Utf8, false),
            Field::new("d", ArrowDataType::Boolean, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["hello", "world", "foo"])),
                Arc::new(BooleanArray::from(vec![true, false, true])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn eval_column() {
        let batch = make_batch();
        let expr = PlanExpr::Column {
            index: 0,
            name: "a".to_string(),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
    }

    #[test]
    fn eval_literal_int() {
        let batch = make_batch();
        let expr = PlanExpr::Literal {
            value: ScalarValue::Int64(42),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn eval_literal_string() {
        let batch = make_batch();
        let expr = PlanExpr::Literal {
            value: ScalarValue::Utf8("test".to_string()),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "test");
    }

    #[test]
    fn eval_add() {
        let batch = make_batch();
        // a + 1 (Int32 + Int32)
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(1),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values(), &[2, 3, 4]);
    }

    #[test]
    fn eval_add_mixed_types() {
        let batch = make_batch();
        // Int32 + Int64 with NO planner Cast inserted: the
        // evaluator now treats this as an internal invariant
        // violation (the analyzer must have inserted the Cast). This
        // is Task 54 of the `planner-type-coercion` change: execution
        // no longer silently coerces mismatched types.
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "b".to_string(),
                span: None,
            }),
            span: None,
        };
        let err = evaluate(&expr, &batch, None).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mismatched types") && msg.contains("analyzer should have inserted Cast"),
            "got: {msg}"
        );

        // With an explicit Cast (as the analyzer would insert), the
        // expression succeeds as before.
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Cast {
                expr: Box::new(PlanExpr::Column {
                    index: 0,
                    name: "a".to_string(),
                    span: None,
                }),
                data_type: arneb_common::types::DataType::Int64,
                span: None,
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "b".to_string(),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values(), &[11, 22, 33]);
    }

    #[test]
    fn eval_comparison() {
        let batch = make_batch();
        // a > 1
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(1),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!arr.value(0));
        assert!(arr.value(1));
        assert!(arr.value(2));
    }

    #[test]
    fn eval_and_or() {
        let batch = make_batch();
        // d AND true
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 3,
                name: "d".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::And,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Boolean(true),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(arr.value(2));
    }

    #[test]
    fn eval_not() {
        let batch = make_batch();
        let expr = PlanExpr::UnaryOp {
            op: ast::UnaryOp::Not,
            expr: Box::new(PlanExpr::Column {
                index: 3,
                name: "d".to_string(),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!arr.value(0));
        assert!(arr.value(1));
        assert!(!arr.value(2));
    }

    #[test]
    fn eval_negate() {
        let batch = make_batch();
        let expr = PlanExpr::UnaryOp {
            op: ast::UnaryOp::Minus,
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values(), &[-1, -2, -3]);
    }

    #[test]
    fn eval_is_null() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            ArrowDataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();

        let expr = PlanExpr::IsNull {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "x".to_string(),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!arr.value(0));
        assert!(arr.value(1));
        assert!(!arr.value(2));
    }

    #[test]
    fn eval_between() {
        let batch = make_batch();
        // a BETWEEN 1 AND 2
        let expr = PlanExpr::Between {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            negated: false,
            low: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(1),
                span: None,
            }),
            high: Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(2),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0)); // 1 in [1,2]
        assert!(arr.value(1)); // 2 in [1,2]
        assert!(!arr.value(2)); // 3 not in [1,2]
    }

    #[test]
    fn eval_in_list() {
        let batch = make_batch();
        // a IN (1, 3)
        let expr = PlanExpr::InList {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            list: vec![
                PlanExpr::Literal {
                    value: ScalarValue::Int32(1),
                    span: None,
                },
                PlanExpr::Literal {
                    value: ScalarValue::Int32(3),
                    span: None,
                },
            ],
            negated: false,
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0)); // 1 in list
        assert!(!arr.value(1)); // 2 not in list
        assert!(arr.value(2)); // 3 in list
    }

    #[test]
    fn eval_like() {
        let batch = make_batch();
        // c LIKE 'he%'
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 2,
                name: "c".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Like,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Utf8("he%".to_string()),
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0)); // "hello" matches
        assert!(!arr.value(1)); // "world" doesn't
        assert!(!arr.value(2)); // "foo" doesn't
    }

    #[test]
    fn eval_cast() {
        let batch = make_batch();
        // CAST(a AS BIGINT)
        let expr = PlanExpr::Cast {
            expr: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
                span: None,
            }),
            data_type: arneb_common::types::DataType::Int64,
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
    }

    #[test]
    fn eval_decimal128_literal() {
        let batch = make_batch();
        let expr = PlanExpr::Literal {
            value: ScalarValue::Decimal128 {
                value: 12345,
                precision: 10,
                scale: 2,
            },
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(arr.value(0), 12345);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn eval_decimal128_comparison() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            ArrowDataType::Decimal128(10, 2),
            false,
        )]));
        let arr = arrow::array::Decimal128Array::from(vec![1000, 2000, 3000])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        // price > 20.00 (2000 in scale=2)
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "price".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Decimal128 {
                    value: 2000,
                    precision: 10,
                    scale: 2,
                },
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bool_arr.value(0)); // 10.00 not > 20.00
        assert!(!bool_arr.value(1)); // 20.00 not > 20.00
        assert!(bool_arr.value(2)); // 30.00 > 20.00
    }

    #[test]
    fn eval_decimal128_arithmetic() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            ArrowDataType::Decimal128(10, 2),
            false,
        )]));
        let arr = arrow::array::Decimal128Array::from(vec![1000, 2000, 3000])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

        // price + 5.00
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "price".to_string(),
                span: None,
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Literal {
                value: ScalarValue::Decimal128 {
                    value: 500,
                    precision: 10,
                    scale: 2,
                },
                span: None,
            }),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let dec_arr = result
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert_eq!(dec_arr.value(0), 1500); // 10.00 + 5.00 = 15.00
        assert_eq!(dec_arr.value(1), 2500);
        assert_eq!(dec_arr.value(2), 3500);
    }

    #[test]
    fn eval_timestamp_literal() {
        let batch = make_batch();
        let expr = PlanExpr::Literal {
            value: ScalarValue::Timestamp {
                value: 1000000,
                unit: arneb_common::types::TimeUnit::Microsecond,
                timezone: None,
            },
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        assert_eq!(
            *result.data_type(),
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn eval_binary_literal() {
        let batch = make_batch();
        let expr = PlanExpr::Literal {
            value: ScalarValue::Binary(vec![0x01, 0x02, 0x03]),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(arr.value(0), &[0x01, 0x02, 0x03]);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn eval_column_out_of_bounds() {
        let batch = make_batch();
        let expr = PlanExpr::Column {
            index: 99,
            name: "z".to_string(),
            span: None,
        };
        assert!(evaluate(&expr, &batch, None).is_err());
    }

    // -- CASE expression tests --

    #[test]
    fn eval_searched_case() {
        let batch = make_batch(); // a = [1, 2, 3]
                                  // CASE WHEN a > 2 THEN 'big' WHEN a > 1 THEN 'medium' ELSE 'small' END
        let expr = PlanExpr::CaseExpr {
            operand: None,
            when_clauses: vec![
                (
                    PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::Column {
                            index: 0,
                            name: "a".into(),
                            span: None,
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal {
                            value: ScalarValue::Int32(2),
                            span: None,
                        }),
                        span: None,
                    },
                    PlanExpr::Literal {
                        value: ScalarValue::Utf8("big".into()),
                        span: None,
                    },
                ),
                (
                    PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::Column {
                            index: 0,
                            name: "a".into(),
                            span: None,
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal {
                            value: ScalarValue::Int32(1),
                            span: None,
                        }),
                        span: None,
                    },
                    PlanExpr::Literal {
                        value: ScalarValue::Utf8("medium".into()),
                        span: None,
                    },
                ),
            ],
            else_result: Some(Box::new(PlanExpr::Literal {
                value: ScalarValue::Utf8("small".into()),
                span: None,
            })),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "small"); // a=1
        assert_eq!(arr.value(1), "medium"); // a=2
        assert_eq!(arr.value(2), "big"); // a=3
    }

    #[test]
    fn eval_simple_case() {
        let batch = make_batch(); // a = [1, 2, 3]
                                  // CASE a WHEN 1 THEN 'one' WHEN 3 THEN 'three' ELSE 'other' END
        let expr = PlanExpr::CaseExpr {
            operand: Some(Box::new(PlanExpr::Column {
                index: 0,
                name: "a".into(),
                span: None,
            })),
            when_clauses: vec![
                (
                    PlanExpr::Literal {
                        value: ScalarValue::Int32(1),
                        span: None,
                    },
                    PlanExpr::Literal {
                        value: ScalarValue::Utf8("one".into()),
                        span: None,
                    },
                ),
                (
                    PlanExpr::Literal {
                        value: ScalarValue::Int32(3),
                        span: None,
                    },
                    PlanExpr::Literal {
                        value: ScalarValue::Utf8("three".into()),
                        span: None,
                    },
                ),
            ],
            else_result: Some(Box::new(PlanExpr::Literal {
                value: ScalarValue::Utf8("other".into()),
                span: None,
            })),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "one");
        assert_eq!(arr.value(1), "other");
        assert_eq!(arr.value(2), "three");
    }

    #[test]
    fn eval_case_no_else_returns_null() {
        let batch = make_batch(); // a = [1, 2, 3]
                                  // CASE WHEN a > 10 THEN 'big' END → all NULL
        let expr = PlanExpr::CaseExpr {
            operand: None,
            when_clauses: vec![(
                PlanExpr::BinaryOp {
                    left: Box::new(PlanExpr::Column {
                        index: 0,
                        name: "a".into(),
                        span: None,
                    }),
                    op: ast::BinaryOp::Gt,
                    right: Box::new(PlanExpr::Literal {
                        value: ScalarValue::Int32(10),
                        span: None,
                    }),
                    span: None,
                },
                PlanExpr::Literal {
                    value: ScalarValue::Utf8("big".into()),
                    span: None,
                },
            )],
            else_result: None,
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn eval_coalesce_desugared() {
        // Simulate COALESCE(x, 0) where x = [NULL, 2, NULL]
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            ArrowDataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![None, Some(2), None]))],
        )
        .unwrap();

        // CASE WHEN x IS NOT NULL THEN x ELSE 0 END
        let expr = PlanExpr::CaseExpr {
            operand: None,
            when_clauses: vec![(
                PlanExpr::IsNotNull {
                    expr: Box::new(PlanExpr::Column {
                        index: 0,
                        name: "x".into(),
                        span: None,
                    }),
                    span: None,
                },
                PlanExpr::Column {
                    index: 0,
                    name: "x".into(),
                    span: None,
                },
            )],
            else_result: Some(Box::new(PlanExpr::Literal {
                value: ScalarValue::Int32(0),
                span: None,
            })),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 0);
        assert_eq!(arr.value(1), 2);
        assert_eq!(arr.value(2), 0);
    }

    #[test]
    fn eval_nullif_desugared() {
        let batch = make_batch(); // a = [1, 2, 3]
                                  // NULLIF(a, 2) → CASE WHEN a = 2 THEN NULL ELSE a END
        let expr = PlanExpr::CaseExpr {
            operand: None,
            when_clauses: vec![(
                PlanExpr::BinaryOp {
                    left: Box::new(PlanExpr::Column {
                        index: 0,
                        name: "a".into(),
                        span: None,
                    }),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(PlanExpr::Literal {
                        value: ScalarValue::Int32(2),
                        span: None,
                    }),
                    span: None,
                },
                PlanExpr::Literal {
                    value: ScalarValue::Null,
                    span: None,
                },
            )],
            else_result: Some(Box::new(PlanExpr::Column {
                index: 0,
                name: "a".into(),
                span: None,
            })),
            span: None,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert!(arr.is_null(1)); // NULLIF(2, 2) = NULL
        assert_eq!(arr.value(2), 3);
    }
}
