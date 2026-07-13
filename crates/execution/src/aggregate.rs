//! Accumulator trait and built-in aggregate function implementations.
//!
//! Each accumulator processes batches of values and produces a single
//! scalar result. Used by [`super::operator::HashAggregateExec`].

use arneb_common::error::ExecutionError;
use arneb_common::types::{ScalarValue, TimeUnit};
use arrow::array::{Array, ArrayRef, AsArray, UInt32Array};
use arrow::datatypes;

use crate::fast_hash::FastHashSet;
use crate::group_key::GroupKey;

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
        self.sum_decimal += other.sum_decimal;
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
                        self.sum_decimal += arr.value(i);
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
        self.count += other.count;
        Ok(())
    }

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
            Decimal128(_, _) => {
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.sum += arr.value(i) as f64;
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
    fn to_scalar(&self) -> ScalarValue {
        match self {
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

/// SUM(col), with per-group state. Supports `Int32/64`, `Float32/64`,
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
                        self.sums_decimal[g] += arr.value(i);
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
            self.sums_decimal[dest] += other.sums_decimal[g];
            self.has_values[dest] = true;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// AVG(col), with per-group `(sum_f64, count_i64)` state.
#[derive(Debug, Default)]
pub struct GroupedAvgAccumulator {
    sums: Vec<f64>,
    counts: Vec<i64>,
}

impl GroupedAvgAccumulator {
    /// Creates a new per-group average accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedAvgAccumulator {
    fn ensure_capacity(&mut self, n: usize) {
        if self.sums.len() < n {
            self.sums.resize(n, 0.0);
            self.counts.resize(n, 0);
        }
    }

    fn add_input(&mut self, group_ids: &[u32], values: &ArrayRef) -> Result<(), ExecutionError> {
        use arrow::datatypes::DataType::*;
        debug_assert_eq!(group_ids.len(), values.len());

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
            Decimal128(_, _) => {
                let arr = values.as_primitive::<datatypes::Decimal128Type>();
                for (i, &g) in group_ids.iter().enumerate() {
                    if !arr.is_null(i) {
                        let g = g as usize;
                        self.sums[g] += arr.value(i) as f64;
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
        if g >= self.counts.len() || self.counts[g] == 0 {
            return Ok(ScalarValue::Null);
        }
        Ok(ScalarValue::Float64(self.sums[g] / self.counts[g] as f64))
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
    state: Vec<Option<OrdScalar>>,
}

impl GroupedMinAccumulator {
    /// Creates a new per-group min accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedMinAccumulator {
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
            self.state[g] = Some(match self.state[g].take() {
                Some(current) if val < current => val,
                Some(current) => current,
                None => val,
            });
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
            self.state[dest] = Some(match self.state[dest].take() {
                Some(current) if o < &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
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
    state: Vec<Option<OrdScalar>>,
}

impl GroupedMaxAccumulator {
    /// Creates a new per-group max accumulator.
    pub fn new() -> Self {
        Self::default()
    }
}

impl GroupedAccumulator for GroupedMaxAccumulator {
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
            self.state[g] = Some(match self.state[g].take() {
                Some(current) if val > current => val,
                Some(current) => current,
                None => val,
            });
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
            self.state[dest] = Some(match self.state[dest].take() {
                Some(current) if o > &current => o.clone(),
                Some(current) => current,
                None => o.clone(),
            });
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
}

impl DistinctAccumulator {
    pub(crate) fn new(inner: Box<dyn Accumulator>) -> Self {
        Self {
            inner,
            seen: FastHashSet::default(),
        }
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
            if self.seen.insert(k.clone()) {
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
            if self.seen.insert(key) {
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
    }
}

/// Extract a single `ScalarValue` at `index` from an Arrow array.
/// Caller is responsible for null-checking; this function does not
/// special-case NULL — pass a non-null index. Returns
/// `InvalidOperation` for unsupported Arrow types so DISTINCT cannot
/// silently accept rows it cannot deduplicate.
fn scalar_from_array(arr: &ArrayRef, index: usize) -> Result<ScalarValue, ExecutionError> {
    use arrow::datatypes::DataType::*;
    match arr.data_type() {
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
        dt => Err(ExecutionError::InvalidOperation(format!(
            "DISTINCT not supported for type {dt:?}"
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

    #[test]
    fn avg_decimal128() {
        let mut acc = AvgAccumulator::new();
        let arr: ArrayRef = Arc::new(
            arrow::array::Decimal128Array::from(vec![1000, 2000, 3000])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        acc.update_batch(&arr).unwrap();
        // AVG returns f64: (1000 + 2000 + 3000) / 3 = 2000.0
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Float64(2000.0));
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
