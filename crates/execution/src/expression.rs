//! Expression evaluator for the execution engine.
//!
//! Evaluates [`PlanExpr`] nodes against an Arrow [`RecordBatch`], producing
//! an [`ArrayRef`] result. Uses Arrow compute kernels for all operations.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, NullArray,
    StringArray,
};
use arrow::compute::kernels;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;
use trino_common::error::ExecutionError;
use trino_common::types::ScalarValue;
use trino_planner::PlanExpr;
use trino_sql_parser::ast;

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

        PlanExpr::Literal(value) => scalar_to_array(value, batch.num_rows()),

        PlanExpr::BinaryOp { left, op, right } => {
            let left_arr = evaluate(left, batch, registry)?;
            let right_arr = evaluate(right, batch, registry)?;
            evaluate_binary_op(&left_arr, op, &right_arr)
        }

        PlanExpr::UnaryOp { op, expr } => {
            let arr = evaluate(expr, batch, registry)?;
            evaluate_unary_op(op, &arr)
        }

        PlanExpr::IsNull(expr) => {
            let arr = evaluate(expr, batch, registry)?;
            let result = kernels::boolean::is_null(&arr)?;
            Ok(Arc::new(result))
        }

        PlanExpr::IsNotNull(expr) => {
            let arr = evaluate(expr, batch, registry)?;
            let result = kernels::boolean::is_not_null(&arr)?;
            Ok(Arc::new(result))
        }

        PlanExpr::Cast { expr, data_type } => {
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
        ScalarValue::Binary(_) => Err(ExecutionError::InvalidOperation(
            "binary literal arrays not yet supported".to_string(),
        )),
        ScalarValue::Decimal128 { .. } => Err(ExecutionError::InvalidOperation(
            "decimal literal arrays not yet supported".to_string(),
        )),
        ScalarValue::Date32(v) => Ok(Arc::new(arrow::array::Date32Array::from(vec![
            *v;
            num_rows
        ]))),
        ScalarValue::Timestamp { .. } => Err(ExecutionError::InvalidOperation(
            "timestamp literal arrays not yet supported".to_string(),
        )),
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
fn compare_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: CompareOp,
) -> Result<BooleanArray, ExecutionError> {
    use arrow::array::AsArray;
    use arrow::datatypes::*;

    // Widen both sides to a common numeric type if they differ.
    let (left, right) = coerce_numeric_pair(left, right)?;

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

fn arithmetic_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: ArithOp,
) -> Result<ArrayRef, ExecutionError> {
    // Coerce to common numeric type before operating.
    let (left, right) = coerce_numeric_pair(left, right)?;

    let result: ArrayRef = match op {
        ArithOp::Add => kernels::numeric::add(&left, &right)?,
        ArithOp::Sub => kernels::numeric::sub(&left, &right)?,
        ArithOp::Mul => kernels::numeric::mul(&left, &right)?,
        ArithOp::Div => kernels::numeric::div(&left, &right)?,
        ArithOp::Rem => kernels::numeric::rem(&left, &right)?,
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

/// Coerce a pair of arrays to a common numeric type for arithmetic/comparison.
///
/// Rules (widening only):
/// - Int32 + Int64 → both cast to Int64
/// - Int32/Int64 + Float32 → both cast to Float64
/// - Int32/Int64 + Float64 → both cast to Float64
/// - Float32 + Float64 → both cast to Float64
/// - Same type → no change
fn coerce_numeric_pair(
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<(ArrayRef, ArrayRef), ExecutionError> {
    let lt = left.data_type();
    let rt = right.data_type();

    if lt == rt {
        return Ok((left.clone(), right.clone()));
    }

    let target = wider_numeric_type(lt, rt)?;

    let l = if lt != &target {
        arrow::compute::cast(left, &target)?
    } else {
        left.clone()
    };
    let r = if rt != &target {
        arrow::compute::cast(right, &target)?
    } else {
        right.clone()
    };

    Ok((l, r))
}

fn wider_numeric_type(
    a: &ArrowDataType,
    b: &ArrowDataType,
) -> Result<ArrowDataType, ExecutionError> {
    use ArrowDataType::*;
    match (a, b) {
        // Same type — no widening needed.
        _ if a == b => Ok(a.clone()),

        // Int32 ↔ Int64 → Int64
        (Int32, Int64) | (Int64, Int32) => Ok(Int64),

        // Any int + float → Float64
        (Int32 | Int64, Float32 | Float64) | (Float32 | Float64, Int32 | Int64) => Ok(Float64),

        // Float32 + Float64 → Float64
        (Float32, Float64) | (Float64, Float32) => Ok(Float64),

        _ => Err(ExecutionError::InvalidOperation(format!(
            "cannot coerce {a:?} and {b:?} to a common type"
        ))),
    }
}

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
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
    }

    #[test]
    fn eval_literal_int() {
        let batch = make_batch();
        let expr = PlanExpr::Literal(ScalarValue::Int64(42));
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn eval_literal_string() {
        let batch = make_batch();
        let expr = PlanExpr::Literal(ScalarValue::Utf8("test".to_string()));
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
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Literal(ScalarValue::Int32(1))),
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values(), &[2, 3, 4]);
    }

    #[test]
    fn eval_add_mixed_types() {
        let batch = make_batch();
        // a + b (Int32 + Int64 → Int64)
        let expr = PlanExpr::BinaryOp {
            left: Box::new(PlanExpr::Column {
                index: 0,
                name: "a".to_string(),
            }),
            op: ast::BinaryOp::Plus,
            right: Box::new(PlanExpr::Column {
                index: 1,
                name: "b".to_string(),
            }),
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
            }),
            op: ast::BinaryOp::Gt,
            right: Box::new(PlanExpr::Literal(ScalarValue::Int32(1))),
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
            }),
            op: ast::BinaryOp::And,
            right: Box::new(PlanExpr::Literal(ScalarValue::Boolean(true))),
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
            }),
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
            }),
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

        let expr = PlanExpr::IsNull(Box::new(PlanExpr::Column {
            index: 0,
            name: "x".to_string(),
        }));
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
            }),
            negated: false,
            low: Box::new(PlanExpr::Literal(ScalarValue::Int32(1))),
            high: Box::new(PlanExpr::Literal(ScalarValue::Int32(2))),
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
            }),
            list: vec![
                PlanExpr::Literal(ScalarValue::Int32(1)),
                PlanExpr::Literal(ScalarValue::Int32(3)),
            ],
            negated: false,
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
            }),
            op: ast::BinaryOp::Like,
            right: Box::new(PlanExpr::Literal(ScalarValue::Utf8("he%".to_string()))),
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
            }),
            data_type: trino_common::types::DataType::Int64,
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
    }

    #[test]
    fn eval_column_out_of_bounds() {
        let batch = make_batch();
        let expr = PlanExpr::Column {
            index: 99,
            name: "z".to_string(),
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
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal(ScalarValue::Int32(2))),
                    },
                    PlanExpr::Literal(ScalarValue::Utf8("big".into())),
                ),
                (
                    PlanExpr::BinaryOp {
                        left: Box::new(PlanExpr::Column {
                            index: 0,
                            name: "a".into(),
                        }),
                        op: ast::BinaryOp::Gt,
                        right: Box::new(PlanExpr::Literal(ScalarValue::Int32(1))),
                    },
                    PlanExpr::Literal(ScalarValue::Utf8("medium".into())),
                ),
            ],
            else_result: Some(Box::new(PlanExpr::Literal(ScalarValue::Utf8(
                "small".into(),
            )))),
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
            })),
            when_clauses: vec![
                (
                    PlanExpr::Literal(ScalarValue::Int32(1)),
                    PlanExpr::Literal(ScalarValue::Utf8("one".into())),
                ),
                (
                    PlanExpr::Literal(ScalarValue::Int32(3)),
                    PlanExpr::Literal(ScalarValue::Utf8("three".into())),
                ),
            ],
            else_result: Some(Box::new(PlanExpr::Literal(ScalarValue::Utf8(
                "other".into(),
            )))),
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
                    }),
                    op: ast::BinaryOp::Gt,
                    right: Box::new(PlanExpr::Literal(ScalarValue::Int32(10))),
                },
                PlanExpr::Literal(ScalarValue::Utf8("big".into())),
            )],
            else_result: None,
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
                PlanExpr::IsNotNull(Box::new(PlanExpr::Column {
                    index: 0,
                    name: "x".into(),
                })),
                PlanExpr::Column {
                    index: 0,
                    name: "x".into(),
                },
            )],
            else_result: Some(Box::new(PlanExpr::Literal(ScalarValue::Int32(0)))),
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
                    }),
                    op: ast::BinaryOp::Eq,
                    right: Box::new(PlanExpr::Literal(ScalarValue::Int32(2))),
                },
                PlanExpr::Literal(ScalarValue::Null),
            )],
            else_result: Some(Box::new(PlanExpr::Column {
                index: 0,
                name: "a".into(),
            })),
        };
        let result = evaluate(&expr, &batch, None).unwrap();
        let arr = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.value(0), 1);
        assert!(arr.is_null(1)); // NULLIF(2, 2) = NULL
        assert_eq!(arr.value(2), 3);
    }
}
