//! Conditional scalar functions: `GREATEST`, `LEAST`.
//!
//! `IF(cond, a [, b])` is desugared to `CASE` by the SQL parser and
//! `TRY(expr)` is handled directly by the expression evaluator (it has
//! to observe evaluation errors of its argument), so neither lives in
//! the registry.

use std::sync::Arc;

use arneb_common::error::ExecutionError;
use arneb_common::types::DataType;
use arneb_planner::analyzer::variadic_supertype;
use arrow::array::{make_array, Array, ArrayRef, NullArray};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::{cmp, zip::zip};

use super::args::{check_arity, invalid};
use super::registry::ScalarFunction;

/// Return all built-in conditional functions.
pub(crate) fn all_conditional_functions() -> Vec<Arc<dyn ScalarFunction>> {
    vec![
        Arc::new(ExtremumFunction { greatest: true }),
        Arc::new(ExtremumFunction { greatest: false }),
    ]
}

/// `GREATEST(v1, ..., vn)` / `LEAST(v1, ..., vn)`.
///
/// Trino semantics:
/// - All arguments are coerced to their common supertype, which is
///   also the return type (e.g. `greatest(int, bigint) -> bigint`,
///   `greatest(bigint, double) -> double`).
/// - The result is NULL if **any** argument is NULL.
///
/// NaN ordering follows Arrow's IEEE-754 total order (NaN sorts above
/// +Infinity), so `greatest(1.0, nan())` is NaN and `least(1.0, nan())`
/// is 1.0. Trino's NaN handling for these functions has changed across
/// releases; this is documented as a known edge-case difference.
#[derive(Debug)]
struct ExtremumFunction {
    greatest: bool,
}

impl ExtremumFunction {
    fn fn_name(&self) -> &'static str {
        if self.greatest {
            "GREATEST"
        } else {
            "LEAST"
        }
    }
}

impl ScalarFunction for ExtremumFunction {
    fn name(&self) -> &str {
        self.fn_name()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, ExecutionError> {
        if arg_types.is_empty() {
            return Err(invalid(format!(
                "{} requires at least 1 argument",
                self.fn_name()
            )));
        }
        variadic_supertype(arg_types).ok_or_else(|| {
            invalid(format!(
                "{}: arguments have no common type: {arg_types:?}",
                self.fn_name()
            ))
        })
    }

    fn evaluate(&self, args: &[ArrayRef]) -> Result<ArrayRef, ExecutionError> {
        check_arity(self.fn_name(), args, 1, usize::MAX)?;
        let len = args[0].len();
        let arg_types = args
            .iter()
            .map(|a| DataType::try_from(a.data_type().clone()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| invalid(format!("{}: {e}", self.fn_name())))?;
        let target = self.return_type(&arg_types)?;
        if target == DataType::Null {
            return Ok(Arc::new(NullArray::new(len)));
        }
        let target: arrow::datatypes::DataType = target.into();
        let casted = args
            .iter()
            .map(|a| {
                if a.data_type() == &target {
                    Ok(a.clone())
                } else {
                    arrow::compute::cast(a, &target).map_err(ExecutionError::from)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut acc = casted[0].clone();
        for next in &casted[1..] {
            // Pick `next` where it beats the accumulator; NULL comparisons
            // are irrelevant because the NULL mask is applied below.
            let take_next = if self.greatest {
                cmp::gt(next, &acc)?
            } else {
                cmp::lt(next, &acc)?
            };
            acc = zip(&take_next, next, &acc)?;
        }

        // NULL if any argument is NULL.
        let nulls = casted.iter().fold(None::<NullBuffer>, |acc, a| {
            NullBuffer::union(acc.as_ref(), a.logical_nulls().as_ref())
        });
        let data = acc.to_data().into_builder().nulls(nulls).build()?;
        Ok(make_array(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};

    fn greatest() -> ExtremumFunction {
        ExtremumFunction { greatest: true }
    }
    fn least() -> ExtremumFunction {
        ExtremumFunction { greatest: false }
    }

    #[test]
    fn greatest_ints_with_null_propagation() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(5), None]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), Some(2), Some(9)]));
        let out = greatest().evaluate(&[a, b]).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 3);
        assert_eq!(out.value(1), 5);
        assert!(out.is_null(2), "any NULL argument yields NULL");
    }

    #[test]
    fn least_widens_mixed_numeric_types() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 7]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![0.5, 9.0]));
        let f = least();
        assert_eq!(
            f.return_type(&[DataType::Int32, DataType::Float64])
                .unwrap(),
            DataType::Float64
        );
        let out = f.evaluate(&[a, b]).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(out.value(0), 0.5);
        assert_eq!(out.value(1), 7.0);
    }

    #[test]
    fn greatest_strings_and_three_args() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["apple", "kiwi"]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["banana", "fig"]));
        let c: ArrayRef = Arc::new(StringArray::from(vec!["cherry", "date"]));
        let out = greatest().evaluate(&[a, b, c]).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "cherry");
        assert_eq!(out.value(1), "kiwi");
    }

    #[test]
    fn null_literal_argument_yields_null() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(NullArray::new(2));
        let out = greatest().evaluate(&[a, b]).unwrap();
        assert_eq!(out.null_count(), 2);
    }

    #[test]
    fn single_argument_is_identity() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![4, 2]));
        let out = least().evaluate(&[a]).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.values(), &[4, 2]);
    }
}
