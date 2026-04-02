//! Accumulator trait and built-in aggregate function implementations.
//!
//! Each accumulator processes batches of values and produces a single
//! scalar result. Used by [`super::operator::HashAggregateExec`].

use arneb_common::error::ExecutionError;
use arneb_common::types::ScalarValue;
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes;

/// An accumulator that consumes array batches and produces a single scalar.
pub trait Accumulator: Send + Sync {
    /// Incorporates a batch of values into the running aggregate.
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError>;

    /// Returns the final aggregate value.
    fn evaluate(&self) -> Result<ScalarValue, ExecutionError>;

    /// Resets the accumulator to its initial state.
    fn reset(&mut self);
}

// ---------------------------------------------------------------------------
// COUNT
// ---------------------------------------------------------------------------

/// Counts non-null values (or all rows for `COUNT(*)`).
#[derive(Debug, Default)]
pub struct CountAccumulator {
    count: i64,
    count_star: bool,
}

impl CountAccumulator {
    /// Creates a `COUNT(expr)` accumulator (counts non-null values).
    pub fn new() -> Self {
        Self {
            count: 0,
            count_star: false,
        }
    }

    /// Creates a `COUNT(*)` accumulator (counts all rows).
    pub fn count_star() -> Self {
        Self {
            count: 0,
            count_star: true,
        }
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        if self.count_star {
            self.count += values.len() as i64;
        } else {
            // Count non-null values.
            self.count += (values.len() - values.null_count()) as i64;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        Ok(ScalarValue::Int64(self.count))
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

// ---------------------------------------------------------------------------
// SUM
// ---------------------------------------------------------------------------

/// Sums numeric values.
#[derive(Debug, Default)]
pub struct SumAccumulator {
    sum_i64: i64,
    sum_f64: f64,
    is_float: bool,
    has_values: bool,
}

impl SumAccumulator {
    /// Creates a new sum accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;

        match values.data_type() {
            Int32 => {
                let arr = values.as_primitive::<datatypes::Int32Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_i64 += arr.value(i) as i64;
                        self.has_values = true;
                    }
                }
            }
            Int64 => {
                let arr = values.as_primitive::<datatypes::Int64Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_i64 += arr.value(i);
                        self.has_values = true;
                    }
                }
            }
            Float32 => {
                self.is_float = true;
                let arr = values.as_primitive::<datatypes::Float32Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_f64 += arr.value(i) as f64;
                        self.has_values = true;
                    }
                }
            }
            Float64 => {
                self.is_float = true;
                let arr = values.as_primitive::<datatypes::Float64Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_f64 += arr.value(i);
                        self.has_values = true;
                    }
                }
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "SUM not supported for type {dt:?}"
                )));
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        if !self.has_values {
            return Ok(ScalarValue::Null);
        }
        if self.is_float {
            Ok(ScalarValue::Float64(self.sum_f64 + self.sum_i64 as f64))
        } else {
            Ok(ScalarValue::Int64(self.sum_i64))
        }
    }

    fn reset(&mut self) {
        self.sum_i64 = 0;
        self.sum_f64 = 0.0;
        self.is_float = false;
        self.has_values = false;
    }
}

// ---------------------------------------------------------------------------
// AVG
// ---------------------------------------------------------------------------

/// Computes the average of numeric values.
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl AvgAccumulator {
    /// Creates a new average accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;

        match values.data_type() {
            Int32 => {
                let arr = values.as_primitive::<datatypes::Int32Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum += arr.value(i) as f64;
                        self.count += 1;
                    }
                }
            }
            Int64 => {
                let arr = values.as_primitive::<datatypes::Int64Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum += arr.value(i) as f64;
                        self.count += 1;
                    }
                }
            }
            Float32 => {
                let arr = values.as_primitive::<datatypes::Float32Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum += arr.value(i) as f64;
                        self.count += 1;
                    }
                }
            }
            Float64 => {
                let arr = values.as_primitive::<datatypes::Float64Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum += arr.value(i);
                        self.count += 1;
                    }
                }
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "AVG not supported for type {dt:?}"
                )));
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        if self.count == 0 {
            Ok(ScalarValue::Null)
        } else {
            Ok(ScalarValue::Float64(self.sum / self.count as f64))
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

// ---------------------------------------------------------------------------
// MIN
// ---------------------------------------------------------------------------

/// Tracks the minimum value.
#[derive(Debug, Default)]
pub struct MinAccumulator {
    min: Option<OrdScalar>,
}

impl MinAccumulator {
    /// Creates a new min accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for MinAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let val = extract_ordscalar(values, i)?;
            self.min = Some(match self.min.take() {
                Some(current) if val < current => val,
                Some(current) => current,
                None => val,
            });
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        match &self.min {
            Some(v) => Ok(v.to_scalar()),
            None => Ok(ScalarValue::Null),
        }
    }

    fn reset(&mut self) {
        self.min = None;
    }
}

// ---------------------------------------------------------------------------
// MAX
// ---------------------------------------------------------------------------

/// Tracks the maximum value.
#[derive(Debug, Default)]
pub struct MaxAccumulator {
    max: Option<OrdScalar>,
}

impl MaxAccumulator {
    /// Creates a new max accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let val = extract_ordscalar(values, i)?;
            self.max = Some(match self.max.take() {
                Some(current) if val > current => val,
                Some(current) => current,
                None => val,
            });
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        match &self.max {
            Some(v) => Ok(v.to_scalar()),
            None => Ok(ScalarValue::Null),
        }
    }

    fn reset(&mut self) {
        self.max = None;
    }
}

// ---------------------------------------------------------------------------
// Comparable scalar helper
// ---------------------------------------------------------------------------

/// A scalar value that supports total ordering for min/max.
#[derive(Debug, Clone)]
enum OrdScalar {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
}

impl OrdScalar {
    fn to_scalar(&self) -> ScalarValue {
        match self {
            OrdScalar::Int32(v) => ScalarValue::Int32(*v),
            OrdScalar::Int64(v) => ScalarValue::Int64(*v),
            OrdScalar::Float32(v) => ScalarValue::Float32(*v),
            OrdScalar::Float64(v) => ScalarValue::Float64(*v),
            OrdScalar::Utf8(v) => ScalarValue::Utf8(v.clone()),
        }
    }
}

impl PartialEq for OrdScalar {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for OrdScalar {}

impl PartialOrd for OrdScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdScalar {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (OrdScalar::Int32(a), OrdScalar::Int32(b)) => a.cmp(b),
            (OrdScalar::Int64(a), OrdScalar::Int64(b)) => a.cmp(b),
            (OrdScalar::Float32(a), OrdScalar::Float32(b)) => a.total_cmp(b),
            (OrdScalar::Float64(a), OrdScalar::Float64(b)) => a.total_cmp(b),
            (OrdScalar::Utf8(a), OrdScalar::Utf8(b)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal, // mismatched types — shouldn't happen
        }
    }
}

fn extract_ordscalar(arr: &ArrayRef, index: usize) -> Result<OrdScalar, ExecutionError> {
    use arrow::datatypes::DataType::*;
    match arr.data_type() {
        Int32 => {
            let a = arr.as_primitive::<datatypes::Int32Type>();
            Ok(OrdScalar::Int32(a.value(index)))
        }
        Int64 => {
            let a = arr.as_primitive::<datatypes::Int64Type>();
            Ok(OrdScalar::Int64(a.value(index)))
        }
        Float32 => {
            let a = arr.as_primitive::<datatypes::Float32Type>();
            Ok(OrdScalar::Float32(a.value(index)))
        }
        Float64 => {
            let a = arr.as_primitive::<datatypes::Float64Type>();
            Ok(OrdScalar::Float64(a.value(index)))
        }
        Utf8 => {
            let a = arr.as_string::<i32>();
            Ok(OrdScalar::Utf8(a.value(index).to_string()))
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "MIN/MAX not supported for type {dt:?}"
        ))),
    }
}

/// Creates an accumulator for the given aggregate function name.
pub(crate) fn create_accumulator(
    func_name: &str,
    is_count_star: bool,
) -> Result<Box<dyn Accumulator>, ExecutionError> {
    match func_name.to_uppercase().as_str() {
        "COUNT" => {
            if is_count_star {
                Ok(Box::new(CountAccumulator::count_star()))
            } else {
                Ok(Box::new(CountAccumulator::new()))
            }
        }
        "SUM" => Ok(Box::new(SumAccumulator::new())),
        "AVG" => Ok(Box::new(AvgAccumulator::new())),
        "MIN" => Ok(Box::new(MinAccumulator::new())),
        "MAX" => Ok(Box::new(MaxAccumulator::new())),
        other => Err(ExecutionError::InvalidOperation(format!(
            "unknown aggregate function: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn count_non_null() {
        let mut acc = CountAccumulator::new();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(2));
    }

    #[test]
    fn count_star() {
        let mut acc = CountAccumulator::count_star();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(3));
    }

    #[test]
    fn sum_int() {
        let mut acc = SumAccumulator::new();
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(60));
    }

    #[test]
    fn sum_float() {
        let mut acc = SumAccumulator::new();
        let arr: ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.0]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(7.0));
    }

    #[test]
    fn sum_empty() {
        let acc = SumAccumulator::new();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Null);
    }

    #[test]
    fn avg_int() {
        let mut acc = AvgAccumulator::new();
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(20.0));
    }

    #[test]
    fn avg_empty() {
        let acc = AvgAccumulator::new();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Null);
    }

    #[test]
    fn min_int() {
        let mut acc = MinAccumulator::new();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![3, 1, 2]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int32(1));
    }

    #[test]
    fn max_string() {
        let mut acc = MaxAccumulator::new();
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["banana", "apple", "cherry"]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Utf8("cherry".to_string())
        );
    }

    #[test]
    fn min_empty() {
        let acc = MinAccumulator::new();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Null);
    }

    #[test]
    fn accumulator_reset() {
        let mut acc = CountAccumulator::new();
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(3));
        acc.reset();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(0));
    }
}
