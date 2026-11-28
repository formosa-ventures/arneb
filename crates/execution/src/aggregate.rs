//! Accumulator trait and built-in aggregate function implementations.
//!
//! Each accumulator processes batches of values and produces a single
//! scalar result. Used by [`super::operator::HashAggregateExec`].

use arneb_common::error::ExecutionError;
use arneb_common::types::{ScalarValue, TimeUnit};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use arrow::datatypes;

use crate::fast_hash::FastHashSet;
use crate::group_key::GroupKey;

// Vec::resize uses amortized growth when the requested length exceeds capacity.
fn vec_bytes<T>(values: &Vec<T>, groups: usize) -> usize {
    let size = size_of::<T>();
    let capacity = values.capacity();
    if groups <= capacity {
        capacity * size
    } else {
        let minimum = if size == 1 {
            8
        } else if size <= 1024 {
            4
        } else {
            1
        };
        groups.max(capacity.saturating_mul(2)).max(minimum) * size
    }
}

pub(crate) fn scalar_heap_bytes(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Utf8(s) => s.capacity(),
        ScalarValue::Binary(b) => b.capacity(),
        ScalarValue::Timestamp {
            timezone: Some(s), ..
        } => s.capacity(),
        ScalarValue::List { element, values } => {
            size_of_val(element.as_ref())
                + vec_bytes(values, 0)
                + values.iter().map(scalar_heap_bytes).sum::<usize>()
        }
        _ => 0,
    }
}

/// A batch-aware aggregate accumulator that owns the per-group state for
/// **one** aggregate function across **all** groups.
///
/// Compare with [`Accumulator`]: that trait expects one instance per
/// group, which forces the caller to dispatch per-row through a
/// `Box<dyn Accumulator>` and slice the input array down to a single
/// row. `GroupedAccumulator` lets the operator pass a whole batch in a
/// single call, with a pre-computed `group_ids: &[u32]` (one id per
/// row). The inner position loop lives inside the concrete impl, which
/// can use direct typed-array access — no per-row allocation, no
/// per-row dyn dispatch.
///
/// Used by `HashAggregateExec` for non-DISTINCT aggregates. DISTINCT
/// still uses the legacy `Accumulator` path in v1.
pub trait GroupedAccumulator: Send + Sync {
    /// Heap bytes after ensuring capacity for this many groups (zero measures current state).
    fn heap_bytes(&self, groups: usize) -> usize;

    /// Project state after this batch, including payloads retained by add_input.
    fn batch_heap_bytes(
        &self,
        groups: usize,
        _ids: &[u32],
        _values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        Ok(self.heap_bytes(groups))
    }

    /// Grow internal per-group state to at least `num_groups` slots.
    ///
    /// `HashAggregateExec` calls this once per batch with the current
    /// `GroupByHash::num_groups()` before invoking [`add_input`]. Calls
    /// MUST be monotonic — `num_groups` only ever grows.
    fn ensure_capacity(&mut self, num_groups: usize);

    /// For every row `i` in `values`, fold `values[i]` into the
    /// accumulator slot indexed by `group_ids[i]`. Rows where
    /// `values.is_null(i)` are skipped per the standard SQL aggregate
    /// null-semantics (`COUNT(*)` is the exception — it counts every
    /// position regardless of the value column).
    ///
    /// `group_ids.len()` MUST equal `values.len()`.
    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError>;

    /// Materialise the aggregate for a single group.
    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError>;

    /// Number of groups for which this accumulator has reserved state.
    fn num_groups(&self) -> usize;

    /// Merge `other`'s state into `self`, mapping each `g` in
    /// `0..other.num_groups()` through `group_remap` (so
    /// `self.state[group_remap[g]] ⊕= other.state[g]`).
    ///
    /// Used by the parallel partial-merge step in
    /// `HashAggregateExec::execute_parallel`. Default impl returns
    /// `Err` to make missing impls obvious.
    fn merge_from(
        &mut self,
        _other: &dyn GroupedAccumulator,
        _group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        Err(ExecutionError::InvalidOperation(
            "this GroupedAccumulator does not support merge_from".to_string(),
        ))
    }

    /// Up-cast for `merge_from` to downcast `other` back to the
    /// concrete type. Each concrete impl returns `&self`.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// An accumulator that consumes array batches and produces a single scalar.
pub trait Accumulator: Send + Sync {
    /// Incorporates a batch of values into the running aggregate.
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError>;

    /// Returns the final aggregate value.
    fn evaluate(&self) -> Result<ScalarValue, ExecutionError>;

    /// Resets the accumulator to its initial state.
    fn reset(&mut self);

    /// Up-cast for partial-state merge via downcasting. Each concrete
    /// accumulator type returns `&self` here.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Merge another accumulator's partial state into `self`. Used by
    /// parallel hash aggregate to combine per-partition sub-maps.
    /// Returns `Err` when the implementor does not support merge (the
    /// caller falls back to a single-partition aggregate path).
    fn merge(&mut self, _other: &dyn Accumulator) -> Result<(), ExecutionError> {
        Err(ExecutionError::InvalidOperation(
            "this accumulator does not support parallel merge".to_string(),
        ))
    }

    /// Approximate heap bytes the accumulator's state holds, for memory
    /// accounting against the query's pool. An accumulator whose state
    /// does not grow with its input keeps the default 0; one that keeps
    /// input values (`DISTINCT`) reports them. Must be cheap: the
    /// aggregate operators call it after every update.
    fn heap_bytes(&self) -> usize {
        0
    }
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<CountAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "CountAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        self.count += other.count;
        Ok(())
    }

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

fn checked_decimal_add(a: i128, b: i128) -> Result<i128, ExecutionError> {
    a.checked_add(b)
        .ok_or_else(|| ExecutionError::InvalidOperation("Decimal overflow".to_string()))
}

/// Largest unscaled value a DECIMAL(38, _) can hold.
const MAX_DECIMAL38: i128 = 10i128.pow(38) - 1;

/// [`checked_decimal_add`] that also rejects results outside DECIMAL(38):
/// SUM's DECIMAL(38, s) output can't hold a value between 10^38 - 1 and
/// `i128::MAX`. AVG keeps the plain i128 check, since its result is divided
/// back into range.
fn checked_decimal38_add(a: i128, b: i128) -> Result<i128, ExecutionError> {
    let sum = checked_decimal_add(a, b)?;
    if sum.unsigned_abs() > MAX_DECIMAL38 as u128 {
        return Err(ExecutionError::InvalidOperation(
            "Decimal overflow".to_string(),
        ));
    }
    Ok(sum)
}

/// Widens TINYINT/SMALLINT input to BIGINT so SUM/AVG reuse the Int64 path
/// (Trino types them BIGINT/DOUBLE; see `function_return_type`).
fn widen_small_int(values: &ArrayRef) -> Result<ArrayRef, ExecutionError> {
    use arrow::datatypes::DataType::{Int16, Int64, Int8};
    match values.data_type() {
        Int8 | Int16 => Ok(arrow::compute::cast(values, &Int64)?),
        _ => Ok(std::sync::Arc::clone(values)),
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
    sum_decimal: i128,
    decimal_precision: u8,
    decimal_scale: i8,
    is_float: bool,
    is_decimal: bool,
    has_values: bool,
}

impl SumAccumulator {
    /// Creates a new sum accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for SumAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("SumAccumulator::merge: type mismatch".to_string())
            })?;
        self.sum_i64 += other.sum_i64;
        self.sum_f64 += other.sum_f64;
        self.sum_decimal = checked_decimal38_add(self.sum_decimal, other.sum_decimal)?;
        self.has_values |= other.has_values;
        self.is_float |= other.is_float;
        if other.is_decimal {
            self.is_decimal = true;
            self.decimal_precision = self.decimal_precision.max(other.decimal_precision);
            // Both sides must share scale by SQL invariant.
            self.decimal_scale = other.decimal_scale;
        }
        Ok(())
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;
        let values = &widen_small_int(values)?;

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
            Decimal128(p, s) => {
                self.is_decimal = true;
                self.decimal_precision = *p;
                self.decimal_scale = *s;
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_decimal = checked_decimal38_add(self.sum_decimal, arr.value(i))?;
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
        if self.is_decimal {
            Ok(ScalarValue::Decimal128 {
                value: self.sum_decimal,
                precision: 38, // widen to max precision for SUM
                scale: self.decimal_scale,
            })
        } else if self.is_float {
            Ok(ScalarValue::Float64(self.sum_f64 + self.sum_i64 as f64))
        } else {
            Ok(ScalarValue::Int64(self.sum_i64))
        }
    }

    fn reset(&mut self) {
        self.sum_i64 = 0;
        self.sum_f64 = 0.0;
        self.sum_decimal = 0;
        self.is_float = false;
        self.is_decimal = false;
        self.has_values = false;
    }
}

// ---------------------------------------------------------------------------
// AVG
// ---------------------------------------------------------------------------

/// Computes the average of numeric values: DOUBLE for integer/float input,
/// `DECIMAL(p, s)` (exact, rounded half-up) for `DECIMAL(p, s)` input.
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: f64,
    sum_decimal: i128,
    /// `(precision, scale)` of the input once a Decimal128 batch is seen.
    decimal: Option<(u8, i8)>,
    count: i64,
}

impl AvgAccumulator {
    /// Creates a new average accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for AvgAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<AvgAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("AvgAccumulator::merge: type mismatch".to_string())
            })?;
        self.sum += other.sum;
        self.sum_decimal = checked_decimal_add(self.sum_decimal, other.sum_decimal)?;
        self.decimal = self.decimal.or(other.decimal);
        self.count += other.count;
        Ok(())
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;
        let values = &widen_small_int(values)?;

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
            Decimal128(p, s) => {
                self.decimal = Some((*p, *s));
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum_decimal = checked_decimal_add(self.sum_decimal, arr.value(i))?;
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
        Ok(avg_value(
            self.count,
            self.sum,
            self.sum_decimal,
            self.decimal,
        ))
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Final AVG value shared by the single and grouped accumulators.
fn avg_value(count: i64, sum: f64, sum_decimal: i128, decimal: Option<(u8, i8)>) -> ScalarValue {
    if count == 0 {
        return ScalarValue::Null;
    }
    match decimal {
        Some((precision, scale)) => ScalarValue::Decimal128 {
            value: div_round_half_up(sum_decimal, count as i128),
            precision,
            scale,
        },
        None => ScalarValue::Float64(sum / count as f64),
    }
}

/// `n / d` rounded half away from zero (Trino's decimal AVG rounding).
fn div_round_half_up(n: i128, d: i128) -> i128 {
    let (q, r) = (n / d, n % d);
    if 2 * r.abs() >= d {
        q + n.signum()
    } else {
        q
    }
}

// ---------------------------------------------------------------------------
// Variance and standard deviation
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
struct VarianceState {
    count: u64,
    mean: f64,
    m2: f64,
}

impl VarianceState {
    fn add(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (value - self.mean);
    }

    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }
        let total = self.count + other.count;
        let delta = other.mean - self.mean;
        self.m2 += other.m2 + delta * delta * self.count as f64 * other.count as f64 / total as f64;
        self.mean += delta * other.count as f64 / total as f64;
        self.count = total;
    }

    fn evaluate(&self, sample: bool, stddev: bool) -> ScalarValue {
        if self.count == 0 || (sample && self.count < 2) {
            return ScalarValue::Null;
        }
        let variance = (self.m2 / (self.count - u64::from(sample)) as f64).max(0.0);
        ScalarValue::Float64(if stddev { variance.sqrt() } else { variance })
    }
}

fn variance_input(values: &ArrayRef, index: usize) -> Result<Option<f64>, ExecutionError> {
    use arrow::datatypes::DataType::*;
    if values.is_null(index) {
        return Ok(None);
    }
    let value = match values.data_type() {
        Int8 => values.as_primitive::<datatypes::Int8Type>().value(index) as f64,
        Int16 => values.as_primitive::<datatypes::Int16Type>().value(index) as f64,
        Int32 => values.as_primitive::<datatypes::Int32Type>().value(index) as f64,
        Int64 => values.as_primitive::<datatypes::Int64Type>().value(index) as f64,
        Float32 => values.as_primitive::<datatypes::Float32Type>().value(index) as f64,
        Float64 => values.as_primitive::<datatypes::Float64Type>().value(index),
        Decimal128(_, scale) => {
            values
                .as_primitive::<datatypes::Decimal128Type>()
                .value(index) as f64
                / 10_f64.powi(i32::from(*scale))
        }
        dt => {
            return Err(ExecutionError::InvalidOperation(format!(
                "variance not supported for type {dt:?}"
            )))
        }
    };
    Ok(Some(value))
}

#[derive(Debug)]
struct VarianceAccumulator {
    state: VarianceState,
    sample: bool,
    stddev: bool,
}

impl Accumulator for VarianceAccumulator {
    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        for i in 0..values.len() {
            if let Some(value) = variance_input(values, i)? {
                self.state.add(value);
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        Ok(self.state.evaluate(self.sample, self.stddev))
    }

    fn reset(&mut self) {
        self.state = VarianceState::default();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ExecutionError::InvalidOperation("variance accumulator type mismatch".to_string())
        })?;
        self.state.merge(other.state);
        Ok(())
    }
}

#[derive(Debug)]
struct GroupedVarianceAccumulator {
    states: Vec<VarianceState>,
    sample: bool,
    stddev: bool,
}

impl GroupedAccumulator for GroupedVarianceAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.states, groups)
    }

    fn ensure_capacity(&mut self, n: usize) {
        self.states.resize(n, VarianceState::default());
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        for (i, &group) in group_ids.iter().enumerate() {
            if let Some(value) = variance_input(values, i)? {
                self.states[group as usize].add(value);
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        Ok(self.states[group_id as usize].evaluate(self.sample, self.stddev))
    }

    fn num_groups(&self) -> usize {
        self.states.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ExecutionError::InvalidOperation(
                "grouped variance accumulator type mismatch".to_string(),
            )
        })?;
        for (i, &group) in group_remap.iter().enumerate().take(other.states.len()) {
            self.states[group as usize].merge(other.states[i]);
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn variance_mode(name: &str) -> Option<(bool, bool)> {
    match name {
        "STDDEV_SAMP" | "STDDEV" => Some((true, true)),
        "STDDEV_POP" => Some((false, true)),
        "VAR_SAMP" | "VARIANCE" => Some((true, false)),
        "VAR_POP" => Some((false, false)),
        _ => None,
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<MinAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("MinAccumulator::merge: type mismatch".to_string())
            })?;
        if let Some(o) = &other.min {
            self.min = Some(match self.min.take() {
                Some(current) if o < &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
        }
        Ok(())
    }

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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<MaxAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("MaxAccumulator::merge: type mismatch".to_string())
            })?;
        if let Some(o) = &other.max {
            self.max = Some(match self.max.take() {
                Some(current) if o > &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
        }
        Ok(())
    }

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
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Date32(i32),
    Decimal128(i128, u8, i8),
    Timestamp(i64, arneb_common::types::TimeUnit, Option<String>),
}

impl OrdScalar {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Utf8(s) | Self::Timestamp(_, _, Some(s)) => s.capacity(),
            _ => 0,
        }
    }

    fn to_scalar(&self) -> ScalarValue {
        match self {
            // `ScalarValue` has no 8/16-bit integer; `scalars_to_array`
            // narrows these back to the column's declared type.
            OrdScalar::Int8(v) => ScalarValue::Int32(i32::from(*v)),
            OrdScalar::Int16(v) => ScalarValue::Int32(i32::from(*v)),
            OrdScalar::Int32(v) => ScalarValue::Int32(*v),
            OrdScalar::Int64(v) => ScalarValue::Int64(*v),
            OrdScalar::Float32(v) => ScalarValue::Float32(*v),
            OrdScalar::Float64(v) => ScalarValue::Float64(*v),
            OrdScalar::Utf8(v) => ScalarValue::Utf8(v.clone()),
            OrdScalar::Date32(v) => ScalarValue::Date32(*v),
            OrdScalar::Decimal128(v, p, s) => ScalarValue::Decimal128 {
                value: *v,
                precision: *p,
                scale: *s,
            },
            OrdScalar::Timestamp(v, u, tz) => ScalarValue::Timestamp {
                value: *v,
                unit: *u,
                timezone: tz.clone(),
            },
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
            (OrdScalar::Int8(a), OrdScalar::Int8(b)) => a.cmp(b),
            (OrdScalar::Int16(a), OrdScalar::Int16(b)) => a.cmp(b),
            (OrdScalar::Int32(a), OrdScalar::Int32(b)) => a.cmp(b),
            (OrdScalar::Int64(a), OrdScalar::Int64(b)) => a.cmp(b),
            (OrdScalar::Float32(a), OrdScalar::Float32(b)) => a.total_cmp(b),
            (OrdScalar::Float64(a), OrdScalar::Float64(b)) => a.total_cmp(b),
            (OrdScalar::Utf8(a), OrdScalar::Utf8(b)) => a.cmp(b),
            (OrdScalar::Date32(a), OrdScalar::Date32(b)) => a.cmp(b),
            (OrdScalar::Decimal128(a, _, _), OrdScalar::Decimal128(b, _, _)) => a.cmp(b),
            (OrdScalar::Timestamp(a, _, _), OrdScalar::Timestamp(b, _, _)) => a.cmp(b),
            _ => std::cmp::Ordering::Equal, // mismatched types — shouldn't happen
        }
    }
}

fn ord_input_payload_bytes(values: &ArrayRef) -> usize {
    match values.data_type() {
        datatypes::DataType::Utf8 => values
            .as_string::<i32>()
            .iter()
            .flatten()
            .map(str::len)
            .sum(),
        datatypes::DataType::Timestamp(_, Some(zone)) => {
            zone.len() * (values.len() - values.null_count())
        }
        _ => 0,
    }
}

fn extract_ordscalar(arr: &ArrayRef, index: usize) -> Result<OrdScalar, ExecutionError> {
    use arrow::datatypes::DataType::*;
    match arr.data_type() {
        Int8 => {
            let a = arr.as_primitive::<datatypes::Int8Type>();
            Ok(OrdScalar::Int8(a.value(index)))
        }
        Int16 => {
            let a = arr.as_primitive::<datatypes::Int16Type>();
            Ok(OrdScalar::Int16(a.value(index)))
        }
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
        Date32 => {
            let a = arr.as_primitive::<datatypes::Date32Type>();
            Ok(OrdScalar::Date32(a.value(index)))
        }
        Decimal128(p, s) => {
            let a = arr.as_primitive::<datatypes::Decimal128Type>();
            Ok(OrdScalar::Decimal128(a.value(index), *p, *s))
        }
        Timestamp(unit, tz) => {
            let tu: arneb_common::types::TimeUnit = (*unit).into();
            let tz_str = tz.as_ref().map(|s| s.to_string());
            let val = match unit {
                datatypes::TimeUnit::Second => arr
                    .as_primitive::<datatypes::TimestampSecondType>()
                    .value(index),
                datatypes::TimeUnit::Millisecond => arr
                    .as_primitive::<datatypes::TimestampMillisecondType>()
                    .value(index),
                datatypes::TimeUnit::Microsecond => arr
                    .as_primitive::<datatypes::TimestampMicrosecondType>()
                    .value(index),
                datatypes::TimeUnit::Nanosecond => arr
                    .as_primitive::<datatypes::TimestampNanosecondType>()
                    .value(index),
            };
            Ok(OrdScalar::Timestamp(val, tu, tz_str))
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "MIN/MAX not supported for type {dt:?}"
        ))),
    }
}

/// Creates an accumulator for the given aggregate function name.
///
/// When `distinct` is true, the returned accumulator deduplicates its
/// inputs (by value) before feeding them to the underlying aggregate.
/// `DISTINCT` is a no-op on `MIN`/`MAX` and on `COUNT(*)`, so we skip
/// the wrapper in those cases.
pub(crate) fn create_accumulator(
    func_name: &str,
    is_count_star: bool,
    distinct: bool,
) -> Result<Box<dyn Accumulator>, ExecutionError> {
    let upper = func_name.to_uppercase();
    if distinct && variance_mode(&upper).is_some() {
        return Err(ExecutionError::InvalidOperation(format!(
            "{upper}(DISTINCT) is not supported"
        )));
    }
    let inner: Box<dyn Accumulator> = match upper.as_str() {
        "COUNT" => {
            if is_count_star {
                Box::new(CountAccumulator::count_star())
            } else {
                Box::new(CountAccumulator::new())
            }
        }
        "SUM" => Box::new(SumAccumulator::new()),
        "AVG" => Box::new(AvgAccumulator::new()),
        "MIN" => Box::new(MinAccumulator::new()),
        "MAX" => Box::new(MaxAccumulator::new()),
        "BOOL_OR" => Box::new(BoolOrAccumulator::new()),
        "BOOL_AND" => Box::new(BoolAndAccumulator::new()),
        "ARRAY_AGG" if distinct => {
            return Err(ExecutionError::InvalidOperation(
                "ARRAY_AGG(DISTINCT) is not supported".to_string(),
            ))
        }
        "ARRAY_AGG" => Box::new(ArrayAggAccumulator::default()),
        "PERCENTILE_CONT" | "PERCENTILE_DISC" if distinct => {
            return Err(ExecutionError::InvalidOperation(format!(
                "{upper}(DISTINCT) is not supported"
            )))
        }
        "PERCENTILE_CONT" => Box::new(PercentileAccumulator::new(true)),
        "PERCENTILE_DISC" => Box::new(PercentileAccumulator::new(false)),
        name if variance_mode(name).is_some() => {
            let (sample, stddev) = variance_mode(name).unwrap();
            Box::new(VarianceAccumulator {
                state: VarianceState::default(),
                sample,
                stddev,
            })
        }
        other => {
            return Err(ExecutionError::InvalidOperation(format!(
                "unknown aggregate function: {other}"
            )))
        }
    };

    if distinct && !is_count_star && upper != "MIN" && upper != "MAX" {
        Ok(Box::new(DistinctAccumulator::new(inner)))
    } else {
        Ok(inner)
    }
}

// ---------------------------------------------------------------------------
// BOOL_OR (tri-state, mirrors Trino's BooleanOrAggregation +
// TriStateBooleanState — NULL=unknown, TRUE=found a true,
// FALSE=found at least one false but no true)
// ---------------------------------------------------------------------------

/// `bool_or(col)` over a Boolean column. Skips NULL inputs.
/// Returns NULL only when zero non-null values were ever seen — used
/// by `CorrelatedExistsToLeftJoin` to distinguish "matched but
/// residual failed" (FALSE) from "no match at all" (NULL).
#[derive(Debug, Default)]
pub(crate) struct BoolOrAccumulator {
    state: Option<bool>,
}

impl BoolOrAccumulator {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for BoolOrAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<BoolOrAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "BoolOrAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        // Tri-state combine: TRUE is sticky, FALSE only when NULL.
        self.state = match (self.state, other.state) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (None, x) => x,
            (Some(false), _) => Some(false),
        };
        Ok(())
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        let arr = values
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "BoolOrAccumulator: expected BooleanArray".to_string(),
                )
            })?;
        // Short-circuit if state is already TRUE — nothing can change it.
        if self.state == Some(true) {
            return Ok(());
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            if arr.value(i) {
                self.state = Some(true);
                return Ok(());
            }
            // FALSE seen → state becomes Some(false) if it was None.
            if self.state.is_none() {
                self.state = Some(false);
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        match self.state {
            Some(v) => Ok(ScalarValue::Boolean(v)),
            None => Ok(ScalarValue::Null),
        }
    }

    fn reset(&mut self) {
        self.state = None;
    }
}

// ---------------------------------------------------------------------------
// BOOL_AND (tri-state, the dual of BOOL_OR)
// ---------------------------------------------------------------------------

/// Folds `v` into a `bool_and` state: NULL inputs are skipped, FALSE is
/// sticky, and the state is NULL only when no non-NULL value was seen.
fn bool_and_fold(state: Option<bool>, v: bool) -> Option<bool> {
    Some(state.unwrap_or(true) && v)
}

fn bool_and_combine(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x && y),
        (x, None) | (None, x) => x,
    }
}

/// `bool_and(col)` over a Boolean column. Skips NULL inputs; NULL when no
/// non-NULL value was seen.
#[derive(Debug, Default)]
pub(crate) struct BoolAndAccumulator {
    state: Option<bool>,
}

impl BoolAndAccumulator {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Accumulator for BoolAndAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<BoolAndAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "BoolAndAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        self.state = bool_and_combine(self.state, other.state);
        Ok(())
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        let arr = values
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "BoolAndAccumulator: expected BooleanArray".to_string(),
                )
            })?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                self.state = bool_and_fold(self.state, arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        Ok(self.state.map_or(ScalarValue::Null, ScalarValue::Boolean))
    }

    fn reset(&mut self) {
        self.state = None;
    }
}

/// Per-group `bool_and`.
#[derive(Debug, Default)]
pub(crate) struct GroupedBoolAndAccumulator {
    state: Vec<Option<bool>>,
}

impl GroupedAccumulator for GroupedBoolAndAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.state, groups)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.state.len() < n {
            self.state.resize(n, None);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        let arr = values
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("BOOL_AND expects a Boolean argument".to_string())
            })?;
        for (i, &g) in group_ids.iter().enumerate() {
            if !arr.is_null(i) {
                let slot = &mut self.state[g as usize];
                *slot = bool_and_fold(*slot, arr.value(i));
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        Ok(self
            .state
            .get(group_id as usize)
            .copied()
            .flatten()
            .map_or(ScalarValue::Null, ScalarValue::Boolean))
    }

    fn num_groups(&self) -> usize {
        self.state.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedBoolAndAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedBoolAndAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, &o_slot) in other.state.iter().enumerate() {
            let dest = group_remap[g] as usize;
            if self.state.len() <= dest {
                self.state.resize(dest + 1, None);
            }
            self.state[dest] = bool_and_combine(self.state[dest], o_slot);
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ---------------------------------------------------------------------------
// ARRAY_AGG [ORDER BY ...]
// ---------------------------------------------------------------------------
//
// The operator hands `ARRAY_AGG` one `StructArray` per batch (see
// `pack_array_agg_input`): field 0 `v` is the value, fields 1.. are the
// ORDER BY keys. Each key field's name carries its direction
// (`k<i>_<a|d><f|l>`: ascending/descending, nulls first/last), which keeps
// the `Accumulator`/`GroupedAccumulator` signatures and factories unchanged.

/// Packs the value and ORDER BY keys of an `ARRAY_AGG` into one struct.
/// `keys` holds `(array, ascending, nulls_first)`.
pub(crate) fn pack_array_agg_input(
    value: ArrayRef,
    keys: Vec<(ArrayRef, bool, bool)>,
) -> Result<ArrayRef, ExecutionError> {
    use arrow::datatypes::Field;
    let materialize = |a: ArrayRef| crate::operator::materialize_dictionary_array(&a);
    let mut fields = vec![Field::new("v", value.data_type().clone(), true)];
    let mut arrays = vec![materialize(value)?];
    fields[0] = Field::new("v", arrays[0].data_type().clone(), true);
    for (i, (key, asc, nulls_first)) in keys.into_iter().enumerate() {
        let key = materialize(key)?;
        let name = format!(
            "k{i}_{}{}",
            if asc { 'a' } else { 'd' },
            if nulls_first { 'f' } else { 'l' }
        );
        fields.push(Field::new(name, key.data_type().clone(), true));
        arrays.push(key);
    }
    let packed = arrow::array::StructArray::try_new(fields.into(), arrays, None)?;
    Ok(Arc::new(packed))
}

/// One buffered `ARRAY_AGG` input row.
#[derive(Debug, Clone)]
struct ArrayAggRow {
    keys: Vec<ScalarValue>,
    value: ScalarValue,
}

/// Rows of one group plus what the packed input says about them.
#[derive(Debug, Default)]
struct ArrayAggBuffer {
    rows: Vec<ArrayAggRow>,
    /// Approximate heap bytes `rows` holds, for memory accounting.
    bytes: usize,
}

impl ArrayAggRow {
    /// Approximate bytes this row holds once buffered.
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.keys.len() * std::mem::size_of::<ScalarValue>()
            + crate::group_key::scalar_heap_bytes(&self.value)
            + self
                .keys
                .iter()
                .map(crate::group_key::scalar_heap_bytes)
                .sum::<usize>()
    }
}

/// What a packed batch declares: element type and key directions.
#[derive(Debug, Clone)]
struct ArrayAggShape {
    element: arneb_common::types::DataType,
    /// `(ascending, nulls_first)` per key.
    dirs: Vec<(bool, bool)>,
}

/// Splits a packed batch into its shape and per-column arrays.
fn unpack_array_agg(
    values: &ArrayRef,
) -> Result<(ArrayAggShape, &arrow::array::StructArray), ExecutionError> {
    let packed = values.as_struct_opt().ok_or_else(|| {
        ExecutionError::InvalidOperation("ARRAY_AGG: expected a packed struct input".to_string())
    })?;
    let fields = packed.fields();
    let element = arneb_common::types::DataType::try_from(fields[0].data_type().clone())
        .map_err(|e| ExecutionError::InvalidOperation(format!("ARRAY_AGG: {e}")))?;
    let dirs = fields[1..]
        .iter()
        .map(|f| {
            let n = f.name();
            (n.contains("_a"), n.ends_with('f'))
        })
        .collect();
    Ok((ArrayAggShape { element, dirs }, packed))
}

impl ArrayAggBuffer {
    fn push(&mut self, row: ArrayAggRow) {
        self.bytes += row.bytes();
        self.rows.push(row);
    }

    /// Appends copies of `other`'s rows; returns the bytes added.
    fn extend_from(&mut self, other: &ArrayAggBuffer) -> usize {
        self.rows.extend(other.rows.iter().cloned());
        self.bytes += other.bytes;
        other.bytes
    }

    fn push_batch(&mut self, packed: &arrow::array::StructArray) -> Result<(), ExecutionError> {
        let cols = packed.columns();
        for i in 0..packed.len() {
            let scalar = |c: &ArrayRef| crate::operator::extract_scalar(c, i);
            self.push(ArrayAggRow {
                value: scalar(&cols[0])?,
                keys: cols[1..].iter().map(scalar).collect::<Result<_, _>>()?,
            });
        }
        Ok(())
    }

    /// The group's array, or NULL when it has no rows.
    fn finish(&self, shape: Option<&ArrayAggShape>) -> ScalarValue {
        let Some(shape) = shape.filter(|_| !self.rows.is_empty()) else {
            return ScalarValue::Null;
        };
        let mut rows: Vec<&ArrayAggRow> = self.rows.iter().collect();
        if !shape.dirs.is_empty() {
            rows.sort_by(|a, b| cmp_array_agg_keys(&a.keys, &b.keys, &shape.dirs));
        }
        ScalarValue::List {
            element: Box::new(shape.element.clone()),
            values: rows.into_iter().map(|r| r.value.clone()).collect(),
        }
    }
}

fn cmp_array_agg_keys(
    a: &[ScalarValue],
    b: &[ScalarValue],
    dirs: &[(bool, bool)],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for ((x, y), &(asc, nulls_first)) in a.iter().zip(b).zip(dirs) {
        let ord = match (
            matches!(x, ScalarValue::Null),
            matches!(y, ScalarValue::Null),
        ) {
            (true, true) => Ordering::Equal,
            // NULLS FIRST/LAST is absolute, independent of ASC/DESC.
            (true, false) => {
                if nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (false, true) => {
                if nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (false, false) => {
                let o = cmp_scalar(x, y);
                if asc {
                    o
                } else {
                    o.reverse()
                }
            }
        };
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Total order over two non-NULL scalars of the same type.
fn cmp_scalar(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use ScalarValue as S;
    match (a, b) {
        (S::Boolean(x), S::Boolean(y)) => x.cmp(y),
        (S::Int32(x), S::Int32(y)) | (S::Date32(x), S::Date32(y)) => x.cmp(y),
        (S::Int64(x), S::Int64(y)) => x.cmp(y),
        (S::Float32(x), S::Float32(y)) => x.total_cmp(y),
        (S::Float64(x), S::Float64(y)) => x.total_cmp(y),
        (S::Utf8(x), S::Utf8(y)) => x.cmp(y),
        (S::Binary(x), S::Binary(y)) => x.cmp(y),
        (S::Decimal128 { value: x, .. }, S::Decimal128 { value: y, .. }) => x.cmp(y),
        (S::Timestamp { value: x, .. }, S::Timestamp { value: y, .. }) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

/// `array_agg(value [ORDER BY keys])` for one group.
#[derive(Debug, Default)]
pub(crate) struct ArrayAggAccumulator {
    shape: Option<ArrayAggShape>,
    buffer: ArrayAggBuffer,
}

impl Accumulator for ArrayAggAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        let (shape, packed) = unpack_array_agg(values)?;
        self.shape.get_or_insert(shape);
        self.buffer.push_batch(packed)
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<ArrayAggAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "ArrayAggAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        if self.shape.is_none() {
            self.shape = other.shape.clone();
        }
        self.buffer.extend_from(&other.buffer);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        Ok(self.buffer.finish(self.shape.as_ref()))
    }

    fn reset(&mut self) {
        self.buffer.rows.clear();
        self.buffer.bytes = 0;
    }

    fn heap_bytes(&self) -> usize {
        self.buffer.bytes
    }
}

/// Per-group `array_agg`.
#[derive(Debug, Default)]
pub(crate) struct GroupedArrayAggAccumulator {
    payload_bytes: usize,
    shape: Option<ArrayAggShape>,
    groups: Vec<ArrayAggBuffer>,
}

impl GroupedArrayAggAccumulator {
    fn push_row(&mut self, group: usize, row: ArrayAggRow) {
        let rows = &mut self.groups[group].rows;
        self.payload_bytes += vec_bytes(&row.keys, 0)
            + row.keys.iter().map(scalar_heap_bytes).sum::<usize>()
            + scalar_heap_bytes(&row.value);
        let before = rows.capacity();
        rows.push(row);
        self.payload_bytes += (rows.capacity() - before) * size_of::<ArrayAggRow>();
    }
}

impl GroupedAccumulator for GroupedArrayAggAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.groups, groups) + self.payload_bytes
    }

    fn batch_heap_bytes(
        &self,
        groups: usize,
        ids: &[u32],
        values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        let (_, packed) = unpack_array_agg(values)?;
        let mut counts = std::collections::HashMap::<u32, usize>::new();
        let mut bytes = self.heap_bytes(groups);
        for (i, &id) in ids.iter().enumerate() {
            *counts.entry(id).or_default() += 1;
            bytes += (packed.num_columns() - 1) * size_of::<ScalarValue>();
            for col in packed.columns() {
                bytes += scalar_heap_bytes(&crate::operator::extract_scalar(col, i)?);
            }
        }
        for (id, count) in counts {
            let rows = self.groups.get(id as usize).map(|g| &g.rows);
            let cap = rows.map_or(0, Vec::capacity);
            let needed = rows.map_or(0, Vec::len) + count;
            let mut projected = cap;
            while projected < needed {
                projected = (projected * 2).max(4);
            }
            bytes += (projected - cap) * size_of::<ArrayAggRow>();
        }
        Ok(bytes)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.groups.len() < n {
            self.groups.resize_with(n, ArrayAggBuffer::default);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        let (shape, packed) = unpack_array_agg(values)?;
        self.shape.get_or_insert(shape);
        let cols = packed.columns();
        for (i, &g) in group_ids.iter().enumerate() {
            let scalar = |c: &ArrayRef| crate::operator::extract_scalar(c, i);
            self.push_row(
                g as usize,
                ArrayAggRow {
                    value: scalar(&cols[0])?,
                    keys: cols[1..].iter().map(scalar).collect::<Result<_, _>>()?,
                },
            );
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        Ok(self
            .groups
            .get(group_id as usize)
            .map_or(ScalarValue::Null, |g| g.finish(self.shape.as_ref())))
    }

    fn num_groups(&self) -> usize {
        self.groups.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedArrayAggAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedArrayAggAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        if self.shape.is_none() {
            self.shape = other.shape.clone();
        }
        for (g, buf) in other.groups.iter().enumerate() {
            let dest = group_remap[g] as usize;
            if self.groups.len() <= dest {
                self.groups.resize_with(dest + 1, ArrayAggBuffer::default);
            }
            for row in &buf.rows {
                self.push_row(dest, row.clone());
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ---------------------------------------------------------------------------
// PERCENTILE_CONT / PERCENTILE_DISC (ordered-set aggregates)
// ---------------------------------------------------------------------------
//
// Like `ARRAY_AGG ORDER BY`, the values are buffered per group and the
// aggregate is single-phase. The operator packs each batch (see
// `pack_percentile_input`) as field 0 `v_a`/`v_d`, the WITHIN GROUP value
// with its direction in the name, and field 1 `f`, the constant fraction.

/// Packs the fraction and the WITHIN GROUP value of a percentile.
pub(crate) fn pack_percentile_input(
    fraction: ArrayRef,
    value: ArrayRef,
    ascending: bool,
) -> Result<ArrayRef, ExecutionError> {
    use arrow::datatypes::{DataType as ArrowType, Field};
    let value = crate::operator::materialize_dictionary_array(&value)?;
    let fraction = arrow::compute::cast(&fraction, &ArrowType::Float64)?;
    let name = if ascending { "v_a" } else { "v_d" };
    let fields = vec![
        Field::new(name, value.data_type().clone(), true),
        Field::new("f", ArrowType::Float64, true),
    ];
    let packed = arrow::array::StructArray::try_new(fields.into(), vec![value, fraction], None)?;
    Ok(Arc::new(packed))
}

/// What a packed percentile batch declares.
#[derive(Debug, Clone)]
struct PercentileShape {
    ascending: bool,
    fraction: f64,
}

fn unpack_percentile(
    values: &ArrayRef,
) -> Result<(Option<PercentileShape>, &arrow::array::StructArray), ExecutionError> {
    let packed = values.as_struct_opt().ok_or_else(|| {
        ExecutionError::InvalidOperation("PERCENTILE: expected a packed struct input".to_string())
    })?;
    let fraction = packed.column(1).as_primitive::<datatypes::Float64Type>();
    let shape = (!packed.is_empty() && fraction.is_valid(0)).then(|| PercentileShape {
        ascending: packed.fields()[0].name() == "v_a",
        fraction: fraction.value(0),
    });
    Ok((shape, packed))
}

/// Approximate bytes one buffered percentile value holds.
fn percentile_value_bytes(value: &ScalarValue) -> usize {
    std::mem::size_of::<ScalarValue>() + crate::group_key::scalar_heap_bytes(value)
}

fn percentile_value(
    packed: &arrow::array::StructArray,
    row: usize,
) -> Result<Option<ScalarValue>, ExecutionError> {
    let col = packed.column(0);
    if col.is_valid(row) {
        // `ScalarValue` has no TINYINT/SMALLINT: keep them as INTEGER (a
        // `percentile_disc` result narrows back in `scalars_to_array`).
        let value = match col.data_type() {
            arrow::datatypes::DataType::Int8 => ScalarValue::Int32(i32::from(
                col.as_primitive::<datatypes::Int8Type>().value(row),
            )),
            arrow::datatypes::DataType::Int16 => ScalarValue::Int32(i32::from(
                col.as_primitive::<datatypes::Int16Type>().value(row),
            )),
            _ => crate::operator::extract_scalar(col, row)?,
        };
        Ok(Some(value))
    } else {
        Ok(None)
    }
}

fn scalar_to_f64(v: &ScalarValue) -> Result<f64, ExecutionError> {
    use ScalarValue as S;
    Ok(match v {
        S::Int32(x) => f64::from(*x),
        S::Int64(x) => *x as f64,
        S::Float32(x) => f64::from(*x),
        S::Float64(x) => *x,
        S::Decimal128 { value, scale, .. } => *value as f64 / 10f64.powi(i32::from(*scale)),
        other => {
            return Err(ExecutionError::InvalidOperation(format!(
                "PERCENTILE_CONT: unsupported value {other:?}"
            )))
        }
    })
}

/// PostgreSQL's rule over one group's non-NULL values: sort them by the
/// key's direction, then `percentile_disc` takes row `ceil(f*N)` (1-based,
/// at least 1) and `percentile_cont` interpolates at `f*(N-1)`.
fn finish_percentile(
    values: &[ScalarValue],
    shape: Option<&PercentileShape>,
    continuous: bool,
) -> Result<ScalarValue, ExecutionError> {
    let Some(shape) = shape.filter(|_| !values.is_empty()) else {
        return Ok(ScalarValue::Null);
    };
    let mut sorted: Vec<&ScalarValue> = values.iter().collect();
    sorted.sort_by(|a, b| {
        let o = cmp_scalar(a, b);
        if shape.ascending {
            o
        } else {
            o.reverse()
        }
    });
    let n = sorted.len();
    let f = shape.fraction;
    if !continuous {
        let row = ((f * n as f64).ceil() as usize).clamp(1, n);
        return Ok(sorted[row - 1].clone());
    }
    let pos = f * (n - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    let lo_v = scalar_to_f64(sorted[lo])?;
    if lo == hi {
        return Ok(ScalarValue::Float64(lo_v));
    }
    let hi_v = scalar_to_f64(sorted[hi])?;
    Ok(ScalarValue::Float64(
        lo_v + (hi_v - lo_v) * (pos - lo as f64),
    ))
}

/// `percentile_cont` / `percentile_disc` for one group.
#[derive(Debug)]
pub(crate) struct PercentileAccumulator {
    continuous: bool,
    shape: Option<PercentileShape>,
    values: Vec<ScalarValue>,
    /// Approximate heap bytes `values` holds, for memory accounting.
    bytes: usize,
}

impl PercentileAccumulator {
    fn new(continuous: bool) -> Self {
        Self {
            continuous,
            shape: None,
            values: Vec::new(),
            bytes: 0,
        }
    }
}

impl Accumulator for PercentileAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        let (shape, packed) = unpack_percentile(values)?;
        if self.shape.is_none() {
            self.shape = shape;
        }
        for row in 0..packed.len() {
            if let Some(value) = percentile_value(packed, row)? {
                self.bytes += percentile_value_bytes(&value);
                self.values.push(value);
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<PercentileAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "PercentileAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        if self.shape.is_none() {
            self.shape = other.shape.clone();
        }
        self.values.extend(other.values.iter().cloned());
        self.bytes += other.bytes;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        finish_percentile(&self.values, self.shape.as_ref(), self.continuous)
    }

    fn reset(&mut self) {
        self.values.clear();
        self.bytes = 0;
    }

    fn heap_bytes(&self) -> usize {
        self.bytes
    }
}

/// Per-group `percentile_cont` / `percentile_disc`.
#[derive(Debug)]
pub(crate) struct GroupedPercentileAccumulator {
    payload_bytes: usize,
    continuous: bool,
    shape: Option<PercentileShape>,
    groups: Vec<Vec<ScalarValue>>,
}

impl GroupedPercentileAccumulator {
    fn new(continuous: bool) -> Self {
        Self {
            payload_bytes: 0,
            continuous,
            shape: None,
            groups: Vec::new(),
        }
    }

    fn push_value(&mut self, group: usize, value: ScalarValue) {
        let values = &mut self.groups[group];
        let before = values.capacity();
        self.payload_bytes += scalar_heap_bytes(&value);
        values.push(value);
        self.payload_bytes += (values.capacity() - before) * size_of::<ScalarValue>();
    }
}

impl GroupedAccumulator for GroupedPercentileAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.groups, groups) + self.payload_bytes
    }

    fn batch_heap_bytes(
        &self,
        groups: usize,
        ids: &[u32],
        values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        let (_, packed) = unpack_percentile(values)?;
        let mut counts = std::collections::HashMap::<u32, usize>::new();
        let mut bytes = self.heap_bytes(groups);
        for (row, &id) in ids.iter().enumerate() {
            if let Some(value) = percentile_value(packed, row)? {
                *counts.entry(id).or_default() += 1;
                bytes += scalar_heap_bytes(&value);
            }
        }
        for (id, count) in counts {
            let values = self.groups.get(id as usize);
            let capacity = values.map_or(0, Vec::capacity);
            let needed = values.map_or(0, Vec::len) + count;
            let mut projected = capacity;
            while projected < needed {
                projected = (projected * 2).max(4);
            }
            bytes += (projected - capacity) * size_of::<ScalarValue>();
        }
        Ok(bytes)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.groups.len() < n {
            self.groups.resize_with(n, Vec::new);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        let (shape, packed) = unpack_percentile(values)?;
        if self.shape.is_none() {
            self.shape = shape;
        }
        for (row, &g) in group_ids.iter().enumerate() {
            if let Some(value) = percentile_value(packed, row)? {
                self.push_value(g as usize, value);
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        match self.groups.get(group_id as usize) {
            Some(values) => finish_percentile(values, self.shape.as_ref(), self.continuous),
            None => Ok(ScalarValue::Null),
        }
    }

    fn num_groups(&self) -> usize {
        self.groups.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedPercentileAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedPercentileAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        if self.shape.is_none() {
            self.shape = other.shape.clone();
        }
        for (g, values) in other.groups.iter().enumerate() {
            let dest = group_remap[g] as usize;
            if self.groups.len() <= dest {
                self.groups.resize_with(dest + 1, Vec::new);
            }
            for value in values {
                self.push_value(dest, value.clone());
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// ===========================================================================
// Batch-aware GroupedAccumulator impls (one instance covers all groups,
// state indexed by group_id: u32). See `GroupedAccumulator` trait above.
// ===========================================================================

/// COUNT(*) / COUNT(col), with per-group state.
#[derive(Debug, Default)]
pub struct GroupedCountAccumulator {
    counts: Vec<i64>,
    count_star: bool,
}

impl GroupedCountAccumulator {
    /// `COUNT(col)` — skips nulls.
    pub fn new() -> Self {
        Self {
            counts: Vec::new(),
            count_star: false,
        }
    }

    /// `COUNT(*)` — counts every row regardless of the value column.
    pub fn count_star() -> Self {
        Self {
            counts: Vec::new(),
            count_star: true,
        }
    }
}

impl GroupedAccumulator for GroupedCountAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.counts, groups)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.counts.len() < n {
            self.counts.resize(n, 0);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        debug_assert_eq!(group_ids.len(), values.len());
        if self.count_star {
            for &g in group_ids {
                self.counts[g as usize] += 1;
            }
        } else {
            // Skip nulls. Optimised: when the column has no nulls at
            // all, fall through to the count_star branch.
            if values.null_count() == 0 {
                for &g in group_ids {
                    self.counts[g as usize] += 1;
                }
            } else {
                for (i, &g) in group_ids.iter().enumerate() {
                    if !values.is_null(i) {
                        self.counts[g as usize] += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        let count = self.counts.get(group_id as usize).copied().unwrap_or(0);
        Ok(ScalarValue::Int64(count))
    }

    fn num_groups(&self) -> usize {
        self.counts.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedCountAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedCountAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, &count) in other.counts.iter().enumerate() {
            let dest = group_remap[g] as usize;
            if self.counts.len() <= dest {
                self.counts.resize(dest + 1, 0);
            }
            self.counts[dest] += count;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// SUM(col), with per-group state. Supports `Int8/16/32/64`, `Float32/64`,
/// `Decimal128(p, s)` — matches the single-instance `SumAccumulator`.
#[derive(Debug, Default)]
pub struct GroupedSumAccumulator {
    sums_i64: Vec<i64>,
    sums_f64: Vec<f64>,
    sums_decimal: Vec<i128>,
    has_values: Vec<bool>,
    is_float: bool,
    is_decimal: bool,
    decimal_precision: u8,
    decimal_scale: i8,
}

impl GroupedSumAccumulator {
    /// Creates a new per-group sum accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedSumAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.sums_i64, groups)
            + vec_bytes(&self.sums_f64, groups)
            + vec_bytes(&self.sums_decimal, groups)
            + vec_bytes(&self.has_values, groups)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.sums_i64.len() < n {
            self.sums_i64.resize(n, 0);
            self.sums_f64.resize(n, 0.0);
            self.sums_decimal.resize(n, 0);
            self.has_values.resize(n, false);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;
        debug_assert_eq!(group_ids.len(), values.len());
        let values = &widen_small_int(values)?;

        match values.data_type() {
            Int32 => {
                let arr = values.as_primitive::<datatypes::Int32Type>();
                for (i, &g) in group_ids.iter().enumerate() {
                    if !arr.is_null(i) {
                        let g = g as usize;
                        self.sums_i64[g] += arr.value(i) as i64;
                        self.has_values[g] = true;
                    }
                }
            }
            Int64 => {
                let arr = values.as_primitive::<datatypes::Int64Type>();
                // Fast path: no null bitmap (typical for TPC-H lineitem
                // keys/measures). Eliminates per-row `is_null` branch
                // and lets LLVM auto-vectorize the scatter-add inner
                // loop. Empirically halves Q01's aggregate cost.
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums_i64[g] += *v;
                        self.has_values[g] = true;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums_i64[g] += arr.value(i);
                            self.has_values[g] = true;
                        }
                    }
                }
            }
            Float32 => {
                self.is_float = true;
                let arr = values.as_primitive::<datatypes::Float32Type>();
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums_f64[g] += *v as f64;
                        self.has_values[g] = true;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums_f64[g] += arr.value(i) as f64;
                            self.has_values[g] = true;
                        }
                    }
                }
            }
            Float64 => {
                self.is_float = true;
                let arr = values.as_primitive::<datatypes::Float64Type>();
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums_f64[g] += *v;
                        self.has_values[g] = true;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums_f64[g] += arr.value(i);
                            self.has_values[g] = true;
                        }
                    }
                }
            }
            Decimal128(p, s) => {
                self.is_decimal = true;
                self.decimal_precision = *p;
                self.decimal_scale = *s;
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for (i, &g) in group_ids.iter().enumerate() {
                    if !arr.is_null(i) {
                        let g = g as usize;
                        self.sums_decimal[g] =
                            checked_decimal38_add(self.sums_decimal[g], arr.value(i))?;
                        self.has_values[g] = true;
                    }
                }
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "SUM not supported for type {dt:?}"
                )))
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        let g = group_id as usize;
        if g >= self.has_values.len() || !self.has_values[g] {
            return Ok(ScalarValue::Null);
        }
        if self.is_decimal {
            Ok(ScalarValue::Decimal128 {
                value: self.sums_decimal[g],
                precision: 38,
                scale: self.decimal_scale,
            })
        } else if self.is_float {
            Ok(ScalarValue::Float64(
                self.sums_f64[g] + self.sums_i64[g] as f64,
            ))
        } else {
            Ok(ScalarValue::Int64(self.sums_i64[g]))
        }
    }

    fn num_groups(&self) -> usize {
        self.has_values.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedSumAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedSumAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        self.is_float |= other.is_float;
        if other.is_decimal {
            self.is_decimal = true;
            self.decimal_precision = self.decimal_precision.max(other.decimal_precision);
            self.decimal_scale = other.decimal_scale;
        }
        for (g, &mapped) in group_remap.iter().enumerate().take(other.has_values.len()) {
            if !other.has_values[g] {
                continue;
            }
            let dest = mapped as usize;
            if self.has_values.len() <= dest {
                self.sums_i64.resize(dest + 1, 0);
                self.sums_f64.resize(dest + 1, 0.0);
                self.sums_decimal.resize(dest + 1, 0);
                self.has_values.resize(dest + 1, false);
            }
            self.sums_i64[dest] += other.sums_i64[g];
            self.sums_f64[dest] += other.sums_f64[g];
            self.sums_decimal[dest] =
                checked_decimal38_add(self.sums_decimal[dest], other.sums_decimal[g])?;
            self.has_values[dest] = true;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// AVG(col), with per-group `(sum_f64, count_i64)` state, plus an exact
/// `i128` sum per group for `DECIMAL(p, s)` input (see [`AvgAccumulator`]).
#[derive(Debug, Default)]
pub struct GroupedAvgAccumulator {
    sums: Vec<f64>,
    decimal_sums: Vec<i128>,
    decimal: Option<(u8, i8)>,
    counts: Vec<i64>,
}

impl GroupedAvgAccumulator {
    /// Creates a new per-group average accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedAvgAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.sums, groups)
            + vec_bytes(&self.counts, groups)
            + vec_bytes(
                &self.decimal_sums,
                if self.decimal.is_some() { groups } else { 0 },
            )
    }

    fn batch_heap_bytes(
        &self,
        groups: usize,
        _ids: &[u32],
        values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        let mut bytes = self.heap_bytes(groups);
        if self.decimal.is_none()
            && matches!(values.data_type(), datatypes::DataType::Decimal128(..))
        {
            bytes += vec_bytes(&self.decimal_sums, groups);
        }
        Ok(bytes)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.sums.len() < n {
            self.sums.resize(n, 0.0);
            self.counts.resize(n, 0);
            if self.decimal.is_some() {
                self.decimal_sums.resize(n, 0);
            }
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;
        debug_assert_eq!(group_ids.len(), values.len());
        let values = &widen_small_int(values)?;

        match values.data_type() {
            Int32 => {
                let arr = values.as_primitive::<datatypes::Int32Type>();
                for (i, &g) in group_ids.iter().enumerate() {
                    if !arr.is_null(i) {
                        let g = g as usize;
                        self.sums[g] += arr.value(i) as f64;
                        self.counts[g] += 1;
                    }
                }
            }
            Int64 => {
                let arr = values.as_primitive::<datatypes::Int64Type>();
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums[g] += *v as f64;
                        self.counts[g] += 1;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums[g] += arr.value(i) as f64;
                            self.counts[g] += 1;
                        }
                    }
                }
            }
            Float32 => {
                let arr = values.as_primitive::<datatypes::Float32Type>();
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums[g] += *v as f64;
                        self.counts[g] += 1;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums[g] += arr.value(i) as f64;
                            self.counts[g] += 1;
                        }
                    }
                }
            }
            Float64 => {
                let arr = values.as_primitive::<datatypes::Float64Type>();
                if arr.null_count() == 0 {
                    let vs = arr.values();
                    for (g, v) in group_ids.iter().zip(vs.iter()) {
                        let g = *g as usize;
                        self.sums[g] += *v;
                        self.counts[g] += 1;
                    }
                } else {
                    for (i, &g) in group_ids.iter().enumerate() {
                        if !arr.is_null(i) {
                            let g = g as usize;
                            self.sums[g] += arr.value(i);
                            self.counts[g] += 1;
                        }
                    }
                }
            }
            Decimal128(p, s) => {
                self.decimal = Some((*p, *s));
                self.decimal_sums.resize(self.counts.len(), 0);
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for (i, &g) in group_ids.iter().enumerate() {
                    if !arr.is_null(i) {
                        let g = g as usize;
                        self.decimal_sums[g] =
                            checked_decimal_add(self.decimal_sums[g], arr.value(i))?;
                        self.counts[g] += 1;
                    }
                }
            }
            dt => {
                return Err(ExecutionError::InvalidOperation(format!(
                    "AVG not supported for type {dt:?}"
                )))
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        let g = group_id as usize;
        if g >= self.counts.len() {
            return Ok(ScalarValue::Null);
        }
        let decimal_sum = self.decimal_sums.get(g).copied().unwrap_or(0);
        Ok(avg_value(
            self.counts[g],
            self.sums[g],
            decimal_sum,
            self.decimal,
        ))
    }

    fn num_groups(&self) -> usize {
        self.counts.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedAvgAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedAvgAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, &mapped) in group_remap.iter().enumerate().take(other.counts.len()) {
            if other.counts[g] == 0 {
                continue;
            }
            let dest = mapped as usize;
            if self.counts.len() <= dest {
                self.sums.resize(dest + 1, 0.0);
                self.counts.resize(dest + 1, 0);
            }
            self.sums[dest] += other.sums[g];
            self.counts[dest] += other.counts[g];
            if other.decimal.is_some() {
                self.decimal = other.decimal;
                self.decimal_sums.resize(self.counts.len(), 0);
                self.decimal_sums[dest] =
                    checked_decimal_add(self.decimal_sums[dest], other.decimal_sums[g])?;
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MIN(col), per-group state using the existing `OrdScalar` enum.
#[derive(Debug, Default)]
pub struct GroupedMinAccumulator {
    payload_bytes: usize,
    state: Vec<Option<OrdScalar>>,
}

impl GroupedMinAccumulator {
    /// Creates a new per-group min accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedMinAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.state, groups) + self.payload_bytes
    }

    fn batch_heap_bytes(
        &self,
        groups: usize,
        _ids: &[u32],
        values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        Ok(self.heap_bytes(groups) + ord_input_payload_bytes(values))
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.state.len() < n {
            self.state.resize(n, None);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        debug_assert_eq!(group_ids.len(), values.len());
        for (i, &g) in group_ids.iter().enumerate() {
            if values.is_null(i) {
                continue;
            }
            let val = extract_ordscalar(values, i)?;
            let g = g as usize;
            self.payload_bytes -= self.state[g].as_ref().map_or(0, OrdScalar::heap_bytes);
            self.state[g] = Some(match self.state[g].take() {
                Some(current) if val < current => val,
                Some(current) => current,
                None => val,
            });
            self.payload_bytes += self.state[g].as_ref().map_or(0, OrdScalar::heap_bytes);
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        match self.state.get(group_id as usize).and_then(|o| o.as_ref()) {
            Some(v) => Ok(v.to_scalar()),
            None => Ok(ScalarValue::Null),
        }
    }

    fn num_groups(&self) -> usize {
        self.state.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedMinAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedMinAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, slot) in other.state.iter().enumerate() {
            let Some(o) = slot else { continue };
            let dest = group_remap[g] as usize;
            if self.state.len() <= dest {
                self.state.resize(dest + 1, None);
            }
            self.payload_bytes -= self.state[dest].as_ref().map_or(0, OrdScalar::heap_bytes);
            self.state[dest] = Some(match self.state[dest].take() {
                Some(current) if o < &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
            self.payload_bytes += self.state[dest].as_ref().map_or(0, OrdScalar::heap_bytes);
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MAX(col), per-group state.
#[derive(Debug, Default)]
pub struct GroupedMaxAccumulator {
    payload_bytes: usize,
    state: Vec<Option<OrdScalar>>,
}

impl GroupedMaxAccumulator {
    /// Creates a new per-group max accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedMaxAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.state, groups) + self.payload_bytes
    }

    fn batch_heap_bytes(
        &self,
        groups: usize,
        _ids: &[u32],
        values: &ArrayRef,
    ) -> Result<usize, ExecutionError> {
        Ok(self.heap_bytes(groups) + ord_input_payload_bytes(values))
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.state.len() < n {
            self.state.resize(n, None);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        debug_assert_eq!(group_ids.len(), values.len());
        for (i, &g) in group_ids.iter().enumerate() {
            if values.is_null(i) {
                continue;
            }
            let val = extract_ordscalar(values, i)?;
            let g = g as usize;
            self.payload_bytes -= self.state[g].as_ref().map_or(0, OrdScalar::heap_bytes);
            self.state[g] = Some(match self.state[g].take() {
                Some(current) if val > current => val,
                Some(current) => current,
                None => val,
            });
            self.payload_bytes += self.state[g].as_ref().map_or(0, OrdScalar::heap_bytes);
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        match self.state.get(group_id as usize).and_then(|o| o.as_ref()) {
            Some(v) => Ok(v.to_scalar()),
            None => Ok(ScalarValue::Null),
        }
    }

    fn num_groups(&self) -> usize {
        self.state.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedMaxAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedMaxAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, slot) in other.state.iter().enumerate() {
            let Some(o) = slot else { continue };
            let dest = group_remap[g] as usize;
            if self.state.len() <= dest {
                self.state.resize(dest + 1, None);
            }
            self.payload_bytes -= self.state[dest].as_ref().map_or(0, OrdScalar::heap_bytes);
            self.state[dest] = Some(match self.state[dest].take() {
                Some(current) if o > &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
            self.payload_bytes += self.state[dest].as_ref().map_or(0, OrdScalar::heap_bytes);
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// `BOOL_OR(col)` — per-group tri-state aggregator. Required for the
/// Q21-style correlated EXISTS rewrite (`LeftJoin + AssignUniqueId +
/// bool_or` mirrors Trino's `TransformExistsApplyToCorrelatedJoin`
/// pipeline; the `bool_or` step folds matched-vs-unmatched outer
/// rows back to a TRUE/FALSE/NULL boolean per outer row).
///
/// State encoding mirrors Trino's `TriStateBooleanState` (NULL=0,
/// TRUE=1, FALSE=-1), stored here as `Option<bool>`:
/// - `None` → group has seen only NULLs (or no rows yet)
/// - `Some(false)` → group has seen at least one FALSE, no TRUEs
/// - `Some(true)` → group has seen at least one TRUE
#[derive(Debug, Default)]
pub(crate) struct GroupedBoolOrAccumulator {
    state: Vec<Option<bool>>,
}

impl GroupedBoolOrAccumulator {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedBoolOrAccumulator {
    fn heap_bytes(&self, groups: usize) -> usize {
        vec_bytes(&self.state, groups)
    }

    fn ensure_capacity(&mut self, n: usize) {
        if self.state.len() < n {
            self.state.resize(n, None);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        debug_assert_eq!(group_ids.len(), values.len());
        let arr = values
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation("BOOL_OR expects a Boolean argument".to_string())
            })?;
        for (i, &g) in group_ids.iter().enumerate() {
            if arr.is_null(i) {
                // Skip NULL inputs — matches Trino's
                // `@SqlNullable` semantics (NULL inputs don't
                // transition the tri-state).
                continue;
            }
            let v = arr.value(i);
            let slot = &mut self.state[g as usize];
            match (*slot, v) {
                (Some(true), _) => {}                 // already TRUE — sticky
                (_, true) => *slot = Some(true),      // first TRUE
                (None, false) => *slot = Some(false), // first FALSE
                (Some(false), false) => {}            // still FALSE
            }
        }
        Ok(())
    }

    fn evaluate(&self, group_id: u32) -> Result<ScalarValue, ExecutionError> {
        match self.state.get(group_id as usize).and_then(|s| *s) {
            Some(b) => Ok(ScalarValue::Boolean(b)),
            None => Ok(ScalarValue::Null),
        }
    }

    fn num_groups(&self) -> usize {
        self.state.len()
    }

    fn merge_from(
        &mut self,
        other: &dyn GroupedAccumulator,
        group_remap: &[u32],
    ) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<GroupedBoolOrAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "GroupedBoolOrAccumulator::merge_from: type mismatch".to_string(),
                )
            })?;
        for (g, &o_slot) in other.state.iter().enumerate() {
            let dest = group_remap[g] as usize;
            if self.state.len() <= dest {
                self.state.resize(dest + 1, None);
            }
            // Combine tri-state: TRUE is sticky; FALSE only sets
            // when current is NULL; otherwise unchanged. Mirrors
            // `TriStateBooleanState.combine` in Trino.
            let slot = &mut self.state[dest];
            *slot = match (*slot, o_slot) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (None, x) => x,
                (Some(false), _) => Some(false),
            };
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Factory mirroring `create_accumulator` but for the batch-aware API.
/// Returns `Err` for DISTINCT — callers MUST detect this and fall back
/// to the legacy per-row path (`create_accumulator`).
pub(crate) fn create_grouped_accumulator(
    func_name: &str,
    is_count_star: bool,
    distinct: bool,
) -> Result<Box<dyn GroupedAccumulator>, ExecutionError> {
    if distinct {
        return Err(ExecutionError::InvalidOperation(
            "DISTINCT aggregates are not supported by GroupedAccumulator; use the legacy path"
                .to_string(),
        ));
    }
    let upper = func_name.to_uppercase();
    let acc: Box<dyn GroupedAccumulator> = match upper.as_str() {
        "COUNT" => {
            if is_count_star {
                Box::new(GroupedCountAccumulator::count_star())
            } else {
                Box::new(GroupedCountAccumulator::new())
            }
        }
        "SUM" => Box::new(GroupedSumAccumulator::new()),
        "AVG" => Box::new(GroupedAvgAccumulator::new()),
        "MIN" => Box::new(GroupedMinAccumulator::new()),
        "MAX" => Box::new(GroupedMaxAccumulator::new()),
        "BOOL_OR" => Box::new(GroupedBoolOrAccumulator::new()),
        "BOOL_AND" => Box::new(GroupedBoolAndAccumulator::default()),
        "ARRAY_AGG" => Box::new(GroupedArrayAggAccumulator::default()),
        "PERCENTILE_CONT" => Box::new(GroupedPercentileAccumulator::new(true)),
        "PERCENTILE_DISC" => Box::new(GroupedPercentileAccumulator::new(false)),
        name if variance_mode(name).is_some() => {
            let (sample, stddev) = variance_mode(name).unwrap();
            Box::new(GroupedVarianceAccumulator {
                states: Vec::new(),
                sample,
                stddev,
            })
        }
        other => {
            return Err(ExecutionError::InvalidOperation(format!(
                "unknown aggregate function: {other}"
            )))
        }
    };
    Ok(acc)
}

// ---------------------------------------------------------------------------
// DISTINCT wrapper
// ---------------------------------------------------------------------------

/// Wraps another accumulator to deduplicate inputs before forwarding
/// them. Used to implement `SUM/AVG/COUNT(DISTINCT expr)`.
///
/// Dedup keys are stringified (type-prefixed to avoid cross-type
/// collisions such as `1_i64` vs `"1"`). NULL values are skipped so
/// that `COUNT(DISTINCT x)` matches SQL semantics (NULLs never count).
pub(crate) struct DistinctAccumulator {
    inner: Box<dyn Accumulator>,
    seen: FastHashSet<GroupKey>,
    /// Running [`GroupKey::entry_bytes`] total of `seen`.
    seen_bytes: usize,
}

impl DistinctAccumulator {
    pub(crate) fn new(inner: Box<dyn Accumulator>) -> Self {
        Self {
            inner,
            seen: FastHashSet::default(),
            seen_bytes: 0,
        }
    }

    /// Records `key` as seen; true when it is new.
    fn insert(&mut self, key: GroupKey) -> bool {
        let bytes = key.entry_bytes();
        let new = self.seen.insert(key);
        if new {
            self.seen_bytes += bytes;
        }
        new
    }
}

impl Accumulator for DistinctAccumulator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecutionError> {
        let other = other
            .as_any()
            .downcast_ref::<DistinctAccumulator>()
            .ok_or_else(|| {
                ExecutionError::InvalidOperation(
                    "DistinctAccumulator::merge: type mismatch".to_string(),
                )
            })?;
        // Union the dedup sets; re-feed only the keys that this side
        // had not yet seen so the inner accumulator gets each unique
        // value exactly once across all partitions.
        for k in &other.seen {
            if self.insert(k.clone()) {
                // Inner ingestion happens by way of the GroupKey
                // scalar values; we re-materialise to a single-row
                // array per scalar so the inner Accumulator's
                // update_batch sees the same shape as the original
                // value path.
                let arr = crate::expression::scalar_to_array(&k.0[0], 1)?;
                self.inner.update_batch(&arr)?;
            }
        }
        Ok(())
    }

    fn update_batch(&mut self, values: &ArrayRef) -> Result<(), ExecutionError> {
        let mut new_indices: Vec<u32> = Vec::new();
        for i in 0..values.len() {
            if values.is_null(i) {
                continue;
            }
            let scalar = scalar_from_array(values, i)?;
            let key = GroupKey::single(scalar);
            if self.insert(key) {
                new_indices.push(i as u32);
            }
        }

        if new_indices.is_empty() {
            return Ok(());
        }

        let indices = UInt32Array::from(new_indices);
        let filtered = arrow::compute::take(values.as_ref(), &indices, None)
            .map_err(|e| ExecutionError::InvalidOperation(format!("distinct take: {e}")))?;
        self.inner.update_batch(&filtered)
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutionError> {
        self.inner.evaluate()
    }

    fn reset(&mut self) {
        self.inner.reset();
        self.seen.clear();
        self.seen_bytes = 0;
    }

    fn heap_bytes(&self) -> usize {
        self.seen_bytes + self.inner.heap_bytes()
    }
}

/// Extract a single `ScalarValue` at `index` from an Arrow array.
/// Caller is responsible for null-checking; this function does not
/// special-case NULL — pass a non-null index. Returns
/// `InvalidOperation` for unsupported Arrow types so DISTINCT cannot
/// silently accept rows it cannot deduplicate. Also backs
/// `operator::extract_scalar` (group keys).
pub(crate) fn scalar_from_array(
    arr: &ArrayRef,
    index: usize,
) -> Result<ScalarValue, ExecutionError> {
    use arrow::datatypes::DataType::*;
    match arr.data_type() {
        // `ScalarValue` has no 8/16-bit integer; TINYINT and SMALLINT
        // values travel as Int32.
        Int8 => Ok(ScalarValue::Int32(i32::from(
            arr.as_primitive::<datatypes::Int8Type>().value(index),
        ))),
        Int16 => Ok(ScalarValue::Int32(i32::from(
            arr.as_primitive::<datatypes::Int16Type>().value(index),
        ))),
        Int32 => Ok(ScalarValue::Int32(
            arr.as_primitive::<datatypes::Int32Type>().value(index),
        )),
        Int64 => Ok(ScalarValue::Int64(
            arr.as_primitive::<datatypes::Int64Type>().value(index),
        )),
        Float32 => Ok(ScalarValue::Float32(
            arr.as_primitive::<datatypes::Float32Type>().value(index),
        )),
        Float64 => Ok(ScalarValue::Float64(
            arr.as_primitive::<datatypes::Float64Type>().value(index),
        )),
        Utf8 => Ok(ScalarValue::Utf8(
            arr.as_string::<i32>().value(index).to_string(),
        )),
        LargeUtf8 => Ok(ScalarValue::Utf8(
            arr.as_string::<i64>().value(index).to_string(),
        )),
        Boolean => Ok(ScalarValue::Boolean(arr.as_boolean().value(index))),
        Date32 => Ok(ScalarValue::Date32(
            arr.as_primitive::<datatypes::Date32Type>().value(index),
        )),
        // Date64 currently has no ScalarValue variant; degrade to a
        // distinct GroupKey via the Timestamp/Date32 fallthrough is
        // not safe. Reject for now — TPC-H doesn't exercise Date64
        // under DISTINCT.
        Decimal128(p, s) => Ok(ScalarValue::Decimal128 {
            value: arr.as_primitive::<datatypes::Decimal128Type>().value(index),
            precision: *p,
            scale: *s,
        }),
        Timestamp(unit, tz) => {
            let value = match unit {
                arrow::datatypes::TimeUnit::Second => arr
                    .as_primitive::<datatypes::TimestampSecondType>()
                    .value(index),
                arrow::datatypes::TimeUnit::Millisecond => arr
                    .as_primitive::<datatypes::TimestampMillisecondType>()
                    .value(index),
                arrow::datatypes::TimeUnit::Microsecond => arr
                    .as_primitive::<datatypes::TimestampMicrosecondType>()
                    .value(index),
                arrow::datatypes::TimeUnit::Nanosecond => arr
                    .as_primitive::<datatypes::TimestampNanosecondType>()
                    .value(index),
            };
            let common_unit = match unit {
                arrow::datatypes::TimeUnit::Second => TimeUnit::Second,
                arrow::datatypes::TimeUnit::Millisecond => TimeUnit::Millisecond,
                arrow::datatypes::TimeUnit::Microsecond => TimeUnit::Microsecond,
                arrow::datatypes::TimeUnit::Nanosecond => TimeUnit::Nanosecond,
            };
            Ok(ScalarValue::Timestamp {
                value,
                unit: common_unit,
                timezone: tz.as_ref().map(|s| s.to_string()),
            })
        }
        List(_) | LargeList(_) => {
            if arr.is_null(index) {
                return Ok(ScalarValue::Null);
            }
            let (child, element) = match arr.data_type() {
                List(field) => (arr.as_list::<i32>().value(index), field.data_type().clone()),
                LargeList(field) => (arr.as_list::<i64>().value(index), field.data_type().clone()),
                other => unreachable!("matched a list type, got {other:?}"),
            };
            let element = arneb_common::types::DataType::try_from(element)
                .map_err(|e| ExecutionError::InvalidOperation(e.to_string()))?;
            let values = (0..child.len())
                .map(|i| crate::operator::extract_scalar(&child, i))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ScalarValue::List {
                element: Box::new(element),
                values,
            })
        }
        dt => Err(ExecutionError::InvalidOperation(format!(
            "cannot extract scalar from type {dt:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `array_agg` keeps every input value, so it reports them for memory
    /// accounting, in both its single-group and grouped forms.
    #[test]
    fn array_agg_reports_the_values_it_buffers() {
        use arrow::array::{StringArray, StructArray};
        use arrow::datatypes::{DataType as ArrowDataType, Field};
        let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(100); 50]));
        let packed: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("v", ArrowDataType::Utf8, true)),
            values,
        )]));
        let mut single = ArrayAggAccumulator::default();
        assert_eq!(Accumulator::heap_bytes(&single), 0);
        single.update_batch(&packed).unwrap();
        assert!(Accumulator::heap_bytes(&single) >= 50 * 100);

        let mut grouped = GroupedArrayAggAccumulator::default();
        grouped.ensure_capacity(2);
        let ids: Vec<u32> = (0..50).map(|i| i % 2).collect();
        grouped.add_input(&ids, &packed).unwrap();
        let after_input = GroupedAccumulator::heap_bytes(&grouped, 0);
        assert!(after_input >= 50 * 100);
        let mut merged = GroupedArrayAggAccumulator::default();
        merged.ensure_capacity(1);
        merged.merge_from(&grouped, &[0, 0]).unwrap();
        assert_eq!(GroupedAccumulator::heap_bytes(&merged, 0), after_input);
        single.reset();
        assert_eq!(Accumulator::heap_bytes(&single), 0);
    }
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn percentiles_report_the_values_they_buffer() {
        let values: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(100); 50]));
        let fraction: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 50]));
        let packed = pack_percentile_input(fraction, values, true).unwrap();
        let mut single = PercentileAccumulator::new(false);
        assert_eq!(Accumulator::heap_bytes(&single), 0);
        single.update_batch(&packed).unwrap();
        assert!(Accumulator::heap_bytes(&single) >= 50 * 100);
        let mut other = PercentileAccumulator::new(false);
        other.update_batch(&packed).unwrap();
        let one = Accumulator::heap_bytes(&single);
        single.merge(&other).unwrap();
        assert_eq!(Accumulator::heap_bytes(&single), 2 * one);
        single.reset();
        assert_eq!(Accumulator::heap_bytes(&single), 0);

        let mut grouped = GroupedPercentileAccumulator::new(true);
        grouped.ensure_capacity(2);
        let ids: Vec<u32> = (0..50).map(|i| i % 2).collect();
        grouped.add_input(&ids, &packed).unwrap();
        // The grouped form counts allocated slots, so it reports at least
        // what the values themselves take.
        assert!(GroupedAccumulator::heap_bytes(&grouped, 2) >= one);
        let mut merged = GroupedPercentileAccumulator::new(true);
        merged.ensure_capacity(1);
        merged.merge_from(&grouped, &[0, 0]).unwrap();
        assert!(GroupedAccumulator::heap_bytes(&merged, 1) >= one);
    }

    #[test]
    fn percentile_nulls_add_no_bytes() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1), None]));
        let fraction: ArrayRef = Arc::new(Float64Array::from(vec![0.5; 3]));
        let packed = pack_percentile_input(fraction, values, true).unwrap();
        let mut single = PercentileAccumulator::new(true);
        single.update_batch(&packed).unwrap();
        assert_eq!(
            Accumulator::heap_bytes(&single),
            std::mem::size_of::<ScalarValue>()
        );
    }

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
    fn sum_decimal128() {
        let mut acc = SumAccumulator::new();
        let arr: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![1000, 2000, 3000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Decimal128 {
                value: 6000,
                precision: 38,
                scale: 2,
            }
        );
    }

    /// AVG(DECIMAL(p, s)) is an exact DECIMAL(p, s), rounded half away
    /// from zero (Trino): avg(1.00, 1.01) = 1.005 -> 1.01; negated -> -1.01.
    #[test]
    fn avg_decimal128_rounds_half_up() {
        let dec = |v: Vec<i128>| -> ArrayRef {
            Arc::new(
                arrow::array::Decimal128Array::from(v)
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            )
        };
        let expected = |value| ScalarValue::Decimal128 {
            value,
            precision: 10,
            scale: 2,
        };
        let mut acc = AvgAccumulator::new();
        acc.update_batch(&dec(vec![100, 101])).unwrap();
        assert_eq!(acc.evaluate().unwrap(), expected(101));

        let mut grouped = GroupedAvgAccumulator::new();
        grouped.ensure_capacity(2);
        grouped
            .add_input(&[0, 0, 1, 1], &dec(vec![100, 101, -100, -101]))
            .unwrap();
        assert_eq!(grouped.evaluate(0).unwrap(), expected(101));
        assert_eq!(grouped.evaluate(1).unwrap(), expected(-101));
    }

    /// The planner's declared SUM/AVG type (`function_return_type`), the
    /// Trino result type, and what both accumulator flavours produce must
    /// all agree — `AggregateExec` rejects any mismatch at runtime.
    #[test]
    fn array_agg_and_bool_and_types_agree_and_merge() {
        use arneb_common::types::{ColumnInfo, DataType};
        use arneb_planner::analyzer::plan_expr_type;
        use arneb_planner::PlanExpr;

        let schema = [ColumnInfo {
            name: "x".into(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        let call = |name: &str| PlanExpr::Function {
            order_by: None,
            name: name.into(),
            args: vec![PlanExpr::Column {
                index: 0,
                name: "x".into(),
                span: None,
            }],
            distinct: false,
            span: None,
        };
        assert_eq!(
            plan_expr_type(&call("ARRAY_AGG"), &schema),
            Some(DataType::List(Box::new(DataType::Int64)))
        );
        assert_eq!(
            plan_expr_type(&call("BOOL_AND"), &schema),
            Some(DataType::Boolean)
        );

        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(1)]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let packed = pack_array_agg_input(values, vec![(keys, false, false)]).unwrap();
        let want = ScalarValue::List {
            element: Box::new(DataType::Int64),
            values: vec![
                ScalarValue::Int64(1),
                ScalarValue::Null,
                ScalarValue::Int64(3),
            ],
        };
        let mut single = create_accumulator("ARRAY_AGG", false, false).unwrap();
        single.update_batch(&packed).unwrap();
        assert_eq!(single.evaluate().unwrap(), want);
        // Merging two halves sorts across both.
        let mut a = create_accumulator("ARRAY_AGG", false, false).unwrap();
        a.update_batch(&packed.slice(0, 1)).unwrap();
        let mut b = create_accumulator("ARRAY_AGG", false, false).unwrap();
        b.update_batch(&packed.slice(1, 2)).unwrap();
        a.merge(b.as_ref()).unwrap();
        assert_eq!(a.evaluate().unwrap(), want);
        let mut grouped = create_grouped_accumulator("ARRAY_AGG", false, false).unwrap();
        grouped.ensure_capacity(2);
        grouped.add_input(&[1, 1, 1], &packed).unwrap();
        assert_eq!(grouped.evaluate(1).unwrap(), want);
        assert_eq!(grouped.evaluate(0).unwrap(), ScalarValue::Null);
        assert!(create_accumulator("ARRAY_AGG", false, true).is_err());
    }

    /// Grouped percentiles keep every input value until the end, so they
    /// report those values for memory accounting, and the projection made
    /// before a batch is added covers what the batch then keeps.
    #[test]
    fn grouped_percentiles_report_the_values_they_keep() {
        for name in ["PERCENTILE_CONT", "PERCENTILE_DISC"] {
            let n = 10_000usize;
            let values: ArrayRef = Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>()));
            let fraction: ArrayRef = Arc::new(Float64Array::from(vec![0.5; n]));
            let packed = pack_percentile_input(fraction, values, true).unwrap();
            let ids: Vec<u32> = (0..n as u32).map(|i| i % 4).collect();
            let kept = n * std::mem::size_of::<ScalarValue>();

            let mut grouped = create_grouped_accumulator(name, false, false).unwrap();
            grouped.ensure_capacity(4);
            let empty = grouped.heap_bytes(0);
            let projected = grouped.batch_heap_bytes(4, &ids, &packed).unwrap();
            grouped.add_input(&ids, &packed).unwrap();
            let after = grouped.heap_bytes(0);
            assert!(after >= empty + kept, "{name}: {after} B for {kept} B kept");
            assert!(
                projected >= after,
                "{name}: projected {projected} B, kept {after} B"
            );

            // Merged state carries the values with it.
            let mut merged = create_grouped_accumulator(name, false, false).unwrap();
            merged.ensure_capacity(1);
            merged.merge_from(grouped.as_ref(), &[0, 0, 0, 0]).unwrap();
            assert!(merged.heap_bytes(0) >= kept, "{name}: merged");

            // The same batch delivered again is kept again.
            let projected = grouped.batch_heap_bytes(4, &ids, &packed).unwrap();
            grouped.add_input(&ids, &packed).unwrap();
            let twice = grouped.heap_bytes(0);
            assert!(
                twice >= empty + 2 * kept,
                "{name}: {twice} B after two batches"
            );
            assert!(
                projected >= twice,
                "{name}: projected {projected} B, kept {twice} B"
            );
        }
    }

    /// `percentile_cont` / `percentile_disc` follow PostgreSQL: NULLs are
    /// skipped, DESC sorts descending (not `1 - f`), an empty group is
    /// NULL, and the planner's types match what the accumulators produce.
    #[test]
    fn percentile_accumulators_follow_postgresql() {
        use arneb_common::types::{ColumnInfo, DataType};
        use arneb_planner::analyzer::plan_expr_type;
        use arneb_planner::PlanExpr;

        let schema = [ColumnInfo {
            name: "x".into(),
            data_type: DataType::Int64,
            nullable: true,
        }];
        let call = |name: &str| PlanExpr::Function {
            order_by: None,
            name: name.into(),
            args: vec![
                PlanExpr::Literal {
                    value: ScalarValue::Float64(0.5),
                    span: None,
                },
                PlanExpr::Column {
                    index: 0,
                    name: "x".into(),
                    span: None,
                },
            ],
            distinct: false,
            span: None,
        };
        assert_eq!(
            plan_expr_type(&call("PERCENTILE_CONT"), &schema),
            Some(DataType::Float64)
        );
        assert_eq!(
            plan_expr_type(&call("PERCENTILE_DISC"), &schema),
            Some(DataType::Int64)
        );

        let run = |name: &str, f: f64, asc: bool, xs: Vec<Option<i64>>| {
            let n = xs.len();
            let values: ArrayRef = Arc::new(Int64Array::from(xs));
            let fraction: ArrayRef = Arc::new(Float64Array::from(vec![f; n]));
            let packed = pack_percentile_input(fraction, values, asc).unwrap();
            let mut single = create_accumulator(name, false, false).unwrap();
            single.update_batch(&packed).unwrap();
            // Merging two halves, and the grouped flavour, agree.
            let mut a = create_accumulator(name, false, false).unwrap();
            a.update_batch(&packed.slice(0, n / 2)).unwrap();
            let mut b = create_accumulator(name, false, false).unwrap();
            b.update_batch(&packed.slice(n / 2, n - n / 2)).unwrap();
            a.merge(b.as_ref()).unwrap();
            let mut grouped = create_grouped_accumulator(name, false, false).unwrap();
            grouped.ensure_capacity(2);
            grouped.add_input(&vec![1; n], &packed).unwrap();
            let got = single.evaluate().unwrap();
            assert_eq!(a.evaluate().unwrap(), got);
            assert_eq!(grouped.evaluate(1).unwrap(), got);
            assert_eq!(grouped.evaluate(0).unwrap(), ScalarValue::Null);
            got
        };
        let f64v = ScalarValue::Float64;
        let ints = || vec![Some(3), None, Some(1), Some(2)];
        assert_eq!(run("PERCENTILE_CONT", 0.5, true, ints()), f64v(2.0));
        assert_eq!(run("PERCENTILE_CONT", 0.25, true, ints()), f64v(1.5));
        assert_eq!(run("PERCENTILE_CONT", 0.25, false, ints()), f64v(2.5));
        assert_eq!(run("PERCENTILE_CONT", 0.0, true, ints()), f64v(1.0));
        assert_eq!(run("PERCENTILE_CONT", 1.0, true, ints()), f64v(3.0));
        assert_eq!(
            run("PERCENTILE_DISC", 0.5, true, ints()),
            ScalarValue::Int64(2)
        );
        assert_eq!(
            run("PERCENTILE_DISC", 0.0, true, ints()),
            ScalarValue::Int64(1)
        );
        // DESC is not `1 - f`: over 1..4 the ascending median is 2, the
        // descending one 3.
        let four = || vec![Some(1), Some(2), Some(3), Some(4)];
        assert_eq!(
            run("PERCENTILE_DISC", 0.5, true, four()),
            ScalarValue::Int64(2)
        );
        assert_eq!(
            run("PERCENTILE_DISC", 0.5, false, four()),
            ScalarValue::Int64(3)
        );
        assert_eq!(run("PERCENTILE_CONT", 0.5, true, four()), f64v(2.5));
        assert_eq!(
            run("PERCENTILE_CONT", 0.5, true, vec![None, None]),
            ScalarValue::Null
        );
        assert_eq!(run("PERCENTILE_DISC", 0.5, true, vec![]), ScalarValue::Null);
        // DISTINCT is refused.
        assert!(create_accumulator("PERCENTILE_CONT", false, true).is_err());
    }

    #[test]
    fn planner_and_accumulator_types_agree() {
        use arneb_common::types::{ColumnInfo, DataType};
        use arneb_planner::analyzer::plan_expr_type;
        use arneb_planner::PlanExpr;

        let dec = DataType::Decimal128 {
            precision: 15,
            scale: 2,
        };
        let sum_dec = DataType::Decimal128 {
            precision: 38,
            scale: 2,
        };
        let (int, dbl) = (DataType::Int64, DataType::Float64);
        let cases = [
            ("SUM", DataType::Int8, int.clone()),
            ("SUM", DataType::Int16, int.clone()),
            ("SUM", DataType::Int32, int.clone()),
            ("SUM", DataType::Int64, int.clone()),
            ("SUM", dbl.clone(), dbl.clone()),
            ("SUM", dec.clone(), sum_dec),
            ("AVG", DataType::Int8, dbl.clone()),
            ("AVG", DataType::Int16, dbl.clone()),
            ("AVG", DataType::Int32, dbl.clone()),
            ("AVG", DataType::Int64, dbl.clone()),
            ("AVG", dbl.clone(), dbl.clone()),
            ("AVG", dec.clone(), dec.clone()),
        ];
        for (func, input, expected) in cases {
            let schema = [ColumnInfo {
                name: "x".into(),
                data_type: input.clone(),
                nullable: true,
            }];
            let call = PlanExpr::Function {
                order_by: None,
                name: func.into(),
                args: vec![PlanExpr::Column {
                    index: 0,
                    name: "x".into(),
                    span: None,
                }],
                distinct: false,
                span: None,
            };
            let planned = plan_expr_type(&call, &schema);
            assert_eq!(
                planned.as_ref(),
                Some(&expected),
                "planner {func}({input:?})"
            );

            let three: ArrayRef = Arc::new(Int64Array::from(vec![3]));
            let values = arrow::compute::cast(&three, &input.clone().into()).unwrap();
            let mut single = create_accumulator(func, false, false).unwrap();
            single.update_batch(&values).unwrap();
            let mut grouped = create_grouped_accumulator(func, false, false).unwrap();
            grouped.ensure_capacity(1);
            grouped.add_input(&[0], &values).unwrap();
            for got in [single.evaluate().unwrap(), grouped.evaluate(0).unwrap()] {
                assert_eq!(got.data_type(), expected, "executor {func}({input:?})");
            }
        }
    }

    /// TINYINT/SMALLINT SUM widens to i64, so totals beyond the input
    /// type's range are exact (issue #93).
    #[test]
    fn sum_avg_small_ints_widen() {
        let tiny: ArrayRef = Arc::new(arrow::array::Int8Array::from(vec![
            Some(127),
            None,
            Some(127),
        ]));
        let small: ArrayRef = Arc::new(arrow::array::Int16Array::from(vec![32767, 32767, -1]));
        for (values, sum, avg) in [(tiny, 254, 127.0), (small, 65533, 65533.0 / 3.0)] {
            let mut s = SumAccumulator::new();
            s.update_batch(&values).unwrap();
            assert_eq!(s.evaluate().unwrap(), ScalarValue::Int64(sum));
            let mut a = AvgAccumulator::new();
            a.update_batch(&values).unwrap();
            assert_eq!(a.evaluate().unwrap(), ScalarValue::Float64(avg));

            let mut gs = GroupedSumAccumulator::new();
            gs.ensure_capacity(1);
            gs.add_input(&[0, 0, 0], &values).unwrap();
            assert_eq!(gs.evaluate(0).unwrap(), ScalarValue::Int64(sum));
            let mut ga = GroupedAvgAccumulator::new();
            ga.ensure_capacity(1);
            ga.add_input(&[0, 0, 0], &values).unwrap();
            assert_eq!(ga.evaluate(0).unwrap(), ScalarValue::Float64(avg));
        }
    }

    #[test]
    fn min_decimal128() {
        let mut acc = MinAccumulator::new();
        let arr: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![3000, 1000, 2000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Decimal128 {
                value: 1000,
                precision: 10,
                scale: 2,
            }
        );
    }

    #[test]
    fn max_decimal128() {
        let mut acc = MaxAccumulator::new();
        let arr: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![1000, 3000, 2000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Decimal128 {
                value: 3000,
                precision: 10,
                scale: 2,
            }
        );
    }

    #[test]
    fn min_timestamp() {
        use arneb_common::types::TimeUnit;
        let mut acc = MinAccumulator::new();
        let arr: ArrayRef = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
            3000000, 1000000, 2000000,
        ]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Timestamp {
                value: 1000000,
                unit: TimeUnit::Microsecond,
                timezone: None,
            }
        );
    }

    #[test]
    fn max_timestamp() {
        use arneb_common::types::TimeUnit;
        let mut acc = MaxAccumulator::new();
        let arr: ArrayRef = Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
            1000000, 3000000, 2000000,
        ]));
        acc.update_batch(&arr).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Timestamp {
                value: 3000000,
                unit: TimeUnit::Microsecond,
                timezone: None,
            }
        );
    }

    // -------------------------------------------------------------------
    // PB-003: COUNT(DISTINCT ...) deduplication
    // -------------------------------------------------------------------

    /// COUNT(DISTINCT x) must count each distinct non-null value once.
    /// Historically the execution layer ignored the `distinct` flag and
    /// degraded to plain COUNT(x), over-counting whenever a group saw
    /// the same value more than once.
    #[test]
    fn count_distinct_over_arrays_drops_duplicates_and_nulls() {
        let mut acc = DistinctAccumulator::new(Box::new(CountAccumulator::new()));

        // First batch: 1, 1, 2, NULL, 3.
        let a: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            None,
            Some(3),
        ]));
        acc.update_batch(&a).unwrap();

        // Second batch: 2 (dup), 4 (new), NULL.
        let b: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(4), None]));
        acc.update_batch(&b).unwrap();

        // Distinct non-null values: {1, 2, 3, 4} → 4.
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(4));
    }

    #[test]
    fn count_distinct_resets_between_groups() {
        let mut acc = DistinctAccumulator::new(Box::new(CountAccumulator::new()));
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
        acc.update_batch(&a).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(2));
        acc.reset();
        let b: ArrayRef = Arc::new(Int64Array::from(vec![5, 5]));
        acc.update_batch(&b).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(1));
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

    // -------------------------------------------------------------------
    // Batch-aware GroupedAccumulator tests
    // -------------------------------------------------------------------

    #[test]
    fn grouped_count_star_counts_every_row() {
        let mut acc = GroupedCountAccumulator::count_star();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Int64(2));
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Int64(1));
    }

    #[test]
    fn grouped_count_col_skips_nulls() {
        let mut acc = GroupedCountAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Int64(1));
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Int64(1));
    }

    #[test]
    fn grouped_count_unused_group_returns_zero() {
        let mut acc = GroupedCountAccumulator::new();
        acc.ensure_capacity(3);
        assert_eq!(acc.evaluate(2).unwrap(), ScalarValue::Int64(0));
    }

    #[test]
    fn grouped_sum_int64_two_groups() {
        let mut acc = GroupedSumAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 1, 0, 1];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Int64(40));
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Int64(60));
    }

    #[test]
    fn grouped_sum_decimal_widens_to_38() {
        let mut acc = GroupedSumAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1];
        let values: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![1000, 2000, 3000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(
            acc.evaluate(0).unwrap(),
            ScalarValue::Decimal128 {
                value: 3000,
                precision: 38,
                scale: 2,
            }
        );
        assert_eq!(
            acc.evaluate(1).unwrap(),
            ScalarValue::Decimal128 {
                value: 3000,
                precision: 38,
                scale: 2,
            }
        );
    }

    fn max_decimal38(n: usize) -> ArrayRef {
        Arc::new(
            arrow::array::Decimal128Array::from(vec![10i128.pow(38) - 1; n])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        )
    }

    /// `n` DECIMAL(38,0) values of 5·10^37: two or more sum past
    /// DECIMAL(38)'s 10^38 - 1 while still fitting in i128.
    fn half_max_decimal38(n: usize) -> ArrayRef {
        Arc::new(
            arrow::array::Decimal128Array::from(vec![5 * 10i128.pow(37); n])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        )
    }

    #[test]
    fn sum_decimal_beyond_precision_38_errors() {
        let mut acc = SumAccumulator::new();
        let err = acc.update_batch(&half_max_decimal38(3)).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        // 5·10^37 + 5·10^37 = 10^38: each partial is in range, the merge isn't.
        let mut a = SumAccumulator::new();
        a.update_batch(&half_max_decimal38(1)).unwrap();
        let mut b = SumAccumulator::new();
        b.update_batch(&half_max_decimal38(1)).unwrap();
        let err = a.merge(&b).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut acc = GroupedSumAccumulator::new();
        acc.ensure_capacity(1);
        let err = acc
            .add_input(&[0, 0, 0], &half_max_decimal38(3))
            .unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut a = GroupedSumAccumulator::new();
        a.ensure_capacity(1);
        a.add_input(&[0], &half_max_decimal38(1)).unwrap();
        let mut b = GroupedSumAccumulator::new();
        b.ensure_capacity(1);
        b.add_input(&[0], &half_max_decimal38(1)).unwrap();
        let err = a.merge_from(&b, &[0]).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        // Exactly 10^38 - 1 is still in range.
        let mut acc = SumAccumulator::new();
        acc.update_batch(&max_decimal38(1)).unwrap();
        assert!(acc.evaluate().is_ok());
    }

    #[test]
    fn sum_decimal_overflow_errors() {
        let mut acc = SumAccumulator::new();
        let err = acc.update_batch(&max_decimal38(2)).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut a = SumAccumulator::new();
        a.update_batch(&max_decimal38(1)).unwrap();
        let mut b = SumAccumulator::new();
        b.update_batch(&max_decimal38(1)).unwrap();
        let err = a.merge(&b).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");
    }

    #[test]
    fn avg_decimal_overflow_errors() {
        let mut acc = AvgAccumulator::new();
        let err = acc.update_batch(&max_decimal38(2)).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut a = AvgAccumulator::new();
        a.update_batch(&max_decimal38(1)).unwrap();
        let mut b = AvgAccumulator::new();
        b.update_batch(&max_decimal38(1)).unwrap();
        let err = a.merge(&b).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut acc = GroupedAvgAccumulator::new();
        acc.ensure_capacity(1);
        let err = acc.add_input(&[0, 0], &max_decimal38(2)).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut a = GroupedAvgAccumulator::new();
        a.ensure_capacity(1);
        a.add_input(&[0], &max_decimal38(1)).unwrap();
        let mut b = GroupedAvgAccumulator::new();
        b.ensure_capacity(1);
        b.add_input(&[0], &max_decimal38(1)).unwrap();
        let err = a.merge_from(&b, &[0]).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");
    }

    #[test]
    fn grouped_sum_decimal_overflow_errors() {
        let mut acc = GroupedSumAccumulator::new();
        acc.ensure_capacity(1);
        let err = acc.add_input(&[0, 0], &max_decimal38(2)).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");

        let mut a = GroupedSumAccumulator::new();
        a.ensure_capacity(1);
        a.add_input(&[0], &max_decimal38(1)).unwrap();
        let mut b = GroupedSumAccumulator::new();
        b.ensure_capacity(1);
        b.add_input(&[0], &max_decimal38(1)).unwrap();
        let err = a.merge_from(&b, &[0]).unwrap_err();
        assert!(err.to_string().contains("Decimal overflow"), "{err}");
    }

    #[test]
    fn grouped_sum_empty_group_returns_null() {
        let mut acc = GroupedSumAccumulator::new();
        acc.ensure_capacity(2);
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Null);
    }

    #[test]
    fn grouped_avg_int_two_groups() {
        let mut acc = GroupedAvgAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1, 1];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 50]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Float64(15.0));
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Float64(40.0));
    }

    #[test]
    fn grouped_avg_empty_group_returns_null() {
        let mut acc = GroupedAvgAccumulator::new();
        acc.ensure_capacity(2);
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Null);
    }

    #[test]
    fn grouped_min_int_two_groups() {
        let mut acc = GroupedMinAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1, 1];
        let values: ArrayRef = Arc::new(Int32Array::from(vec![3, 1, 5, 2]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(acc.evaluate(0).unwrap(), ScalarValue::Int32(1));
        assert_eq!(acc.evaluate(1).unwrap(), ScalarValue::Int32(2));
    }

    #[test]
    fn grouped_max_string_two_groups() {
        let mut acc = GroupedMaxAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1];
        let values: ArrayRef = Arc::new(StringArray::from(vec!["banana", "apple", "cherry"]));
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(
            acc.evaluate(0).unwrap(),
            ScalarValue::Utf8("banana".to_string())
        );
        assert_eq!(
            acc.evaluate(1).unwrap(),
            ScalarValue::Utf8("cherry".to_string())
        );
    }

    #[test]
    fn grouped_min_decimal128() {
        let mut acc = GroupedMinAccumulator::new();
        acc.ensure_capacity(2);
        let group_ids: Vec<u32> = vec![0, 0, 1];
        let values: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![3000, 1000, 5000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.add_input(&group_ids, &values).unwrap();
        assert_eq!(
            acc.evaluate(0).unwrap(),
            ScalarValue::Decimal128 {
                value: 1000,
                precision: 10,
                scale: 2,
            }
        );
    }

    #[test]
    fn grouped_merge_sum_with_remap() {
        // Partial A has groups {0=>10, 1=>20}; remap A → final ids [0,1].
        let mut a = GroupedSumAccumulator::new();
        a.ensure_capacity(2);
        a.add_input(
            &[0u32, 1u32],
            &(Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
        )
        .unwrap();
        // Partial B has groups {0=>30, 1=>40}; B's group 0 maps to final 1,
        // B's group 1 maps to final 0.
        let mut b = GroupedSumAccumulator::new();
        b.ensure_capacity(2);
        b.add_input(
            &[0u32, 1u32],
            &(Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef),
        )
        .unwrap();
        let mut final_acc = GroupedSumAccumulator::new();
        final_acc.ensure_capacity(2);
        final_acc.merge_from(&a, &[0u32, 1u32]).unwrap();
        final_acc.merge_from(&b, &[1u32, 0u32]).unwrap();
        // Final group 0 ⇐ A.0 (10) + B.1 (40) = 50.
        // Final group 1 ⇐ A.1 (20) + B.0 (30) = 50.
        assert_eq!(final_acc.evaluate(0).unwrap(), ScalarValue::Int64(50));
        assert_eq!(final_acc.evaluate(1).unwrap(), ScalarValue::Int64(50));
    }

    #[test]
    fn grouped_merge_type_mismatch_errors() {
        let final_acc = GroupedSumAccumulator::new();
        let other = GroupedCountAccumulator::new();
        let mut final_acc = final_acc;
        let err = final_acc.merge_from(&other, &[]).unwrap_err();
        assert!(matches!(err, ExecutionError::InvalidOperation(_)));
    }

    #[test]
    fn create_grouped_accumulator_distinct_returns_err() {
        let res = create_grouped_accumulator("SUM", false, true);
        assert!(matches!(
            res.as_ref().err(),
            Some(ExecutionError::InvalidOperation(_))
        ));
    }

    #[test]
    fn create_grouped_accumulator_dispatches() {
        let acc = create_grouped_accumulator("SUM", false, false).unwrap();
        assert!(acc.as_any().is::<GroupedSumAccumulator>());
        let acc = create_grouped_accumulator("COUNT", true, false).unwrap();
        assert!(acc.as_any().is::<GroupedCountAccumulator>());
        let acc = create_grouped_accumulator("AVG", false, false).unwrap();
        assert!(acc.as_any().is::<GroupedAvgAccumulator>());
        let acc = create_grouped_accumulator("MIN", false, false).unwrap();
        assert!(acc.as_any().is::<GroupedMinAccumulator>());
        let acc = create_grouped_accumulator("MAX", false, false).unwrap();
        assert!(acc.as_any().is::<GroupedMaxAccumulator>());
    }
}
