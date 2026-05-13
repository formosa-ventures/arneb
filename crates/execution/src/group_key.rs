//! Typed hash key for `HashAggregateExec` and `DistinctAccumulator`.
//!
//! `GroupKey` wraps `Vec<ScalarValue>` and implements `Hash + Eq` with
//! bit-pattern equality on floats. This avoids per-row `format!()` /
//! `String` allocation in the aggregate hot path while preserving the
//! existing NaN-deduplicates-to-itself semantics: two `NaN` values with
//! the same bit pattern collapse, two with different bit patterns stay
//! distinct (matching the prior `format!("f64:{}", value.to_bits())`
//! approach).
//!
//! `ScalarValue` itself derives only `PartialEq` (IEEE 754 semantics:
//! `NaN != NaN`), so we deliberately localize the bit-equality rule to
//! this wrapper rather than changing the type's contract.
//!
//! Tracked under `openspec/changes/exec-typed-hash-keys/`.

use std::hash::{Hash, Hasher};

use arneb_common::types::{ScalarValue, TimeUnit};

/// Typed hash key. Cheap to clone for short Vecs (typical group-by has
/// 1–4 columns); for Utf8 the String inside still costs one
/// allocation, but only on insertion — not on every probing row.
#[derive(Debug, Clone)]
pub(crate) struct GroupKey(pub(crate) Vec<ScalarValue>);

impl GroupKey {
    /// Construct a single-element key. Used by `DistinctAccumulator`.
    pub(crate) fn single(value: ScalarValue) -> Self {
        GroupKey(vec![value])
    }
}

// Type tags. Each variant of ScalarValue gets a distinct one-byte tag
// written before the payload so that cross-variant collisions are
// impossible — `Int32(1)`, `Int64(1)`, `Utf8("1")`, `Float64(1.0)` all
// hash to different slots.
const TAG_NULL: u8 = 0;
const TAG_BOOLEAN: u8 = 1;
const TAG_INT32: u8 = 2;
const TAG_INT64: u8 = 3;
const TAG_FLOAT32: u8 = 4;
const TAG_FLOAT64: u8 = 5;
const TAG_UTF8: u8 = 6;
const TAG_BINARY: u8 = 7;
const TAG_DECIMAL128: u8 = 8;
const TAG_DATE32: u8 = 9;
const TAG_TIMESTAMP: u8 = 10;
const TAG_UNKNOWN: u8 = 255;

fn hash_scalar<H: Hasher>(s: &ScalarValue, state: &mut H) {
    match s {
        ScalarValue::Null => state.write_u8(TAG_NULL),
        ScalarValue::Boolean(b) => {
            state.write_u8(TAG_BOOLEAN);
            state.write_u8(*b as u8);
        }
        ScalarValue::Int32(v) => {
            state.write_u8(TAG_INT32);
            state.write_i32(*v);
        }
        ScalarValue::Int64(v) => {
            state.write_u8(TAG_INT64);
            state.write_i64(*v);
        }
        ScalarValue::Float32(v) => {
            // Bit-equality: `NaN(0x7fc00000)` and `NaN(0x7fc00001)`
            // hash differently; same bits hash the same.
            state.write_u8(TAG_FLOAT32);
            state.write_u32(v.to_bits());
        }
        ScalarValue::Float64(v) => {
            state.write_u8(TAG_FLOAT64);
            state.write_u64(v.to_bits());
        }
        ScalarValue::Utf8(s) => {
            state.write_u8(TAG_UTF8);
            state.write_usize(s.len());
            state.write(s.as_bytes());
        }
        ScalarValue::Binary(b) => {
            state.write_u8(TAG_BINARY);
            state.write_usize(b.len());
            state.write(b);
        }
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            state.write_u8(TAG_DECIMAL128);
            state.write_u8(*precision);
            state.write_i8(*scale);
            state.write_i128(*value);
        }
        ScalarValue::Date32(d) => {
            state.write_u8(TAG_DATE32);
            state.write_i32(*d);
        }
        ScalarValue::Timestamp {
            value,
            unit,
            timezone,
        } => {
            state.write_u8(TAG_TIMESTAMP);
            // Encode unit as its discriminant.
            let unit_byte = match unit {
                TimeUnit::Second => 0u8,
                TimeUnit::Millisecond => 1u8,
                TimeUnit::Microsecond => 2u8,
                TimeUnit::Nanosecond => 3u8,
            };
            state.write_u8(unit_byte);
            state.write_i64(*value);
            match timezone {
                None => state.write_u8(0),
                Some(tz) => {
                    state.write_u8(1);
                    state.write_usize(tz.len());
                    state.write(tz.as_bytes());
                }
            }
        }
        // ScalarValue is #[non_exhaustive]. Any future variant collides
        // under TAG_UNKNOWN until this match is extended; surface as a
        // compile-time issue in test builds with `unreachable!()` once
        // we know it's safe.
        _ => state.write_u8(TAG_UNKNOWN),
    }
}

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    use ScalarValue::*;
    match (a, b) {
        (Null, Null) => true,
        (Null, _) | (_, Null) => false,
        (Boolean(x), Boolean(y)) => x == y,
        (Int32(x), Int32(y)) => x == y,
        (Int64(x), Int64(y)) => x == y,
        // Bit-equality on floats: NaN with same payload collapses;
        // NaN with different payload stays distinct.
        (Float32(x), Float32(y)) => x.to_bits() == y.to_bits(),
        (Float64(x), Float64(y)) => x.to_bits() == y.to_bits(),
        (Utf8(x), Utf8(y)) => x == y,
        (Binary(x), Binary(y)) => x == y,
        (
            Decimal128 {
                value: vx,
                precision: px,
                scale: sx,
            },
            Decimal128 {
                value: vy,
                precision: py,
                scale: sy,
            },
        ) => vx == vy && px == py && sx == sy,
        (Date32(x), Date32(y)) => x == y,
        (
            Timestamp {
                value: vx,
                unit: ux,
                timezone: tx,
            },
            Timestamp {
                value: vy,
                unit: uy,
                timezone: ty,
            },
        ) => vx == vy && ux == uy && tx == ty,
        // Cross-variant: never equal (caught by the type tag above
        // during hashing; explicit match here for completeness).
        _ => false,
    }
}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.0.len());
        for s in &self.0 {
            hash_scalar(s, state);
        }
    }
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(other.0.iter())
            .all(|(a, b)| scalar_eq(a, b))
    }
}

impl Eq for GroupKey {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_one(k: &GroupKey) -> u64 {
        let mut h = DefaultHasher::new();
        k.hash(&mut h);
        h.finish()
    }

    #[test]
    fn int_keys_hash_eq_consistent() {
        let a = GroupKey(vec![ScalarValue::Int64(1)]);
        let b = GroupKey(vec![ScalarValue::Int64(1)]);
        assert_eq!(a, b);
        assert_eq!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn cross_type_distinct() {
        let i32_one = GroupKey(vec![ScalarValue::Int32(1)]);
        let i64_one = GroupKey(vec![ScalarValue::Int64(1)]);
        let str_one = GroupKey(vec![ScalarValue::Utf8("1".to_string())]);
        let f64_one = GroupKey(vec![ScalarValue::Float64(1.0)]);
        assert_ne!(i32_one, i64_one);
        assert_ne!(i32_one, str_one);
        assert_ne!(i64_one, f64_one);
        assert_ne!(hash_one(&i32_one), hash_one(&i64_one));
    }

    #[test]
    fn float_nan_same_bits_collapses() {
        let nan = f64::NAN;
        let a = GroupKey(vec![ScalarValue::Float64(nan)]);
        let b = GroupKey(vec![ScalarValue::Float64(nan)]);
        assert_eq!(a, b);
        assert_eq!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn float_nan_different_bits_distinct() {
        // Two NaN payloads that differ in the low bits.
        let n1 = f64::from_bits(0x7ff8_0000_0000_0000);
        let n2 = f64::from_bits(0x7ff8_0000_0000_0001);
        assert!(n1.is_nan() && n2.is_nan());
        let a = GroupKey(vec![ScalarValue::Float64(n1)]);
        let b = GroupKey(vec![ScalarValue::Float64(n2)]);
        assert_ne!(a, b);
        // Hashes very likely differ; not strictly required for
        // correctness (equality is the contract), but a useful smoke
        // check on the hash function distinguishing them.
        assert_ne!(hash_one(&a), hash_one(&b));
    }

    #[test]
    fn null_handling() {
        let na = GroupKey(vec![ScalarValue::Null]);
        let nb = GroupKey(vec![ScalarValue::Null]);
        let z = GroupKey(vec![ScalarValue::Int32(0)]);
        assert_eq!(na, nb);
        assert_ne!(na, z);
    }

    #[test]
    fn multi_column_order_matters() {
        let ab = GroupKey(vec![ScalarValue::Int32(1), ScalarValue::Int32(2)]);
        let ba = GroupKey(vec![ScalarValue::Int32(2), ScalarValue::Int32(1)]);
        assert_ne!(ab, ba);
    }
}
