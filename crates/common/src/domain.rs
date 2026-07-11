//! Per-column dynamic-filter value sets.
//!
//! A [`Domain`] describes the set of possible values for a single column on the
//! build side of a join. It is the payload of a cross-fragment dynamic filter:
//! the join's build phase produces one Domain per equi-key per partition, the
//! coordinator unions partition Domains into a per-join Domain, and the probe
//! side's scan applies the result as an additional predicate.
//!
//! The tiered representation keeps exact distinct values under the cap, uses
//! a fixed bloom filter for over-cap discrete keys, and keeps range/all
//! fallbacks for domains that are already range-shaped or unfilterable.

use crate::types::ScalarValue;
#[cfg(not(test))]
use std::sync::OnceLock;

/// Default maximum number of distinct values to retain before degrading to a
/// min/max range. Mirrors Trino's `partitioned_max_distinct_values_per_driver`
/// (DynamicFilterConfig.java:145).
pub const DEFAULT_MAX_DISTINCT_VALUES: usize = 20_000;

/// Fixed bloom-filter bit count used for dynamic filters (8 MiB).
pub const BLOOM_FILTER_NUM_BITS: u64 = 1 << 26;

/// Fixed bloom-filter hash count used for dynamic filters.
pub const BLOOM_FILTER_K: u32 = 6;

/// Fixed bloom-filter seed used so partition-local blooms can be unioned.
pub const BLOOM_FILTER_SEED: u64 = 0;

/// Returns whether bloom dynamic filters are enabled.
#[cfg(not(test))]
pub fn bloom_dynamic_filter_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(read_and_log_bloom_dynamic_filter_enabled)
}

/// Returns whether bloom dynamic filters are enabled.
#[cfg(test)]
pub fn bloom_dynamic_filter_enabled() -> bool {
    read_and_log_bloom_dynamic_filter_enabled()
}

fn read_and_log_bloom_dynamic_filter_enabled() -> bool {
    let enabled = std::env::var("ARNEB_BLOOM_DF")
        .map(|v| v == "1")
        .unwrap_or(false);
    tracing::info!(
        target: "arneb::config",
        bloom_dynamic_filter = enabled,
        "ARNEB_BLOOM_DF effective value (default off; =1 to enable)"
    );
    enabled
}

/// Bloom filter used as the over-cap dynamic-filter representation.
///
/// All production instances use the fixed parameters above so partition
/// blooms from the same dynamic-filter id are union-compatible.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    /// Packed bloom bits.
    pub bits: Vec<u64>,
    /// Number of usable bits.
    pub num_bits: u64,
    /// Number of hash probes.
    pub k: u32,
    /// Hash seed.
    pub seed: u64,
}

impl BloomFilter {
    /// Creates an empty bloom filter with arneb's fixed DF parameters.
    pub fn with_fixed_params() -> Self {
        let words = BLOOM_FILTER_NUM_BITS.div_ceil(64) as usize;
        Self {
            bits: vec![0; words],
            num_bits: BLOOM_FILTER_NUM_BITS,
            k: BLOOM_FILTER_K,
            seed: BLOOM_FILTER_SEED,
        }
    }

    /// Inserts one scalar value.
    pub fn insert(&mut self, value: &ScalarValue) {
        let bytes = scalar_canonical_bytes(value);
        for bit in bloom_positions(&bytes, self.num_bits, self.k, self.seed) {
            let word = (bit / 64) as usize;
            let offset = bit % 64;
            self.bits[word] |= 1u64 << offset;
        }
    }

    /// Returns true when the scalar may have been inserted.
    pub fn contains(&self, value: &ScalarValue) -> bool {
        let bytes = scalar_canonical_bytes(value);
        let contains = bloom_positions(&bytes, self.num_bits, self.k, self.seed).all(|bit| {
            let word = (bit / 64) as usize;
            let offset = bit % 64;
            self.bits
                .get(word)
                .map(|w| (w & (1u64 << offset)) != 0)
                .unwrap_or(false)
        });
        contains
    }

    fn union_with(&mut self, other: &BloomFilter) {
        assert_eq!(self.num_bits, other.num_bits);
        assert_eq!(self.k, other.k);
        assert_eq!(self.seed, other.seed);
        for (dst, src) in self.bits.iter_mut().zip(&other.bits) {
            *dst |= *src;
        }
    }
}

/// Per-column dynamic-filter value set.
///
/// `Eq` and `Hash` are intentionally NOT implemented because `ScalarValue`
/// contains float variants; comparison happens through explicit helpers.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Domain {
    /// A small set of discrete values. Empty means "matches nothing" — the
    /// probe side can skip the entire scan.
    DistinctValues(Vec<ScalarValue>),
    /// A min/max range (inclusive on both ends). Used when the distinct count
    /// exceeded the cap. `nullable` tracks whether NULL is permitted.
    Range {
        /// Inclusive lower bound.
        min: ScalarValue,
        /// Inclusive upper bound.
        max: ScalarValue,
        /// Whether the underlying column may contain NULL.
        nullable: bool,
    },
    /// Fixed-size bloom filter for over-cap discrete build-side keys.
    /// May admit false positives, but never false negatives.
    Bloom(BloomFilter),
    /// No filter — all values pass. Used when both distinct-values and
    /// min/max representations would be larger than worthwhile, when the
    /// build side is empty enough that the filter wouldn't help, or when
    /// any incoming comparison returns `None` (e.g. a float NaN).
    All,
}

impl Domain {
    /// Returns a Domain that filters out all rows (empty distinct values).
    pub fn none() -> Self {
        Domain::DistinctValues(Vec::new())
    }

    /// Returns a Domain that admits all rows.
    pub fn all() -> Self {
        Domain::All
    }

    /// Returns true if this Domain is the no-op filter.
    pub fn is_all(&self) -> bool {
        matches!(self, Domain::All)
    }

    /// Returns true if this Domain represents the empty set
    /// (DistinctValues with no entries). A probe-side scan that sees an
    /// empty Domain can skip all rows entirely.
    pub fn is_empty(&self) -> bool {
        matches!(self, Domain::DistinctValues(v) if v.is_empty())
    }

    /// Merges two Domains by union — the result admits any value admitted
    /// by either input. This is the per-partition merge applied on the
    /// coordinator when N tasks of one stage have each reported their
    /// partition's slice of the build-side key set.
    ///
    /// Cap policy: if the union of `DistinctValues` exceeds
    /// `max_distinct_values`, degrade to a fixed bloom filter. If a `Range`
    /// computation needs a comparison that returns `None` (e.g. NaN), degrade
    /// further to `All`.
    pub fn union(self, other: Domain, max_distinct_values: usize) -> Domain {
        use Domain::*;
        match (self, other) {
            (All, _) | (_, All) => All,
            (Bloom(mut a), Bloom(b)) => {
                a.union_with(&b);
                Bloom(a)
            }
            (Bloom(mut bloom), DistinctValues(values))
            | (DistinctValues(values), Bloom(mut bloom)) => {
                for value in values {
                    bloom.insert(&value);
                }
                Bloom(bloom)
            }
            (Bloom(_), Range { .. }) | (Range { .. }, Bloom(_)) => All,
            (DistinctValues(a), DistinctValues(b)) => {
                let mut merged = a;
                merged.extend(b);
                dedupe_in_place(&mut merged);
                if merged.len() <= max_distinct_values {
                    DistinctValues(merged)
                } else if bloom_dynamic_filter_enabled() {
                    let mut bloom = BloomFilter::with_fixed_params();
                    for value in &merged {
                        bloom.insert(value);
                    }
                    Bloom(bloom)
                } else {
                    distinct_to_range(merged).unwrap_or(All)
                }
            }
            (DistinctValues(values), Range { min, max, nullable })
            | (Range { min, max, nullable }, DistinctValues(values)) => {
                let mut new_min = min;
                let mut new_max = max;
                for v in values {
                    match scalar_min(&new_min, &v) {
                        Some(m) => new_min = m,
                        None => return All,
                    }
                    match scalar_max(&new_max, &v) {
                        Some(m) => new_max = m,
                        None => return All,
                    }
                }
                Range {
                    min: new_min,
                    max: new_max,
                    nullable,
                }
            }
            (
                Range {
                    min: amin,
                    max: amax,
                    nullable: an,
                },
                Range {
                    min: bmin,
                    max: bmax,
                    nullable: bn,
                },
            ) => {
                let new_min = match scalar_min(&amin, &bmin) {
                    Some(m) => m,
                    None => return All,
                };
                let new_max = match scalar_max(&amax, &bmax) {
                    Some(m) => m,
                    None => return All,
                };
                Range {
                    min: new_min,
                    max: new_max,
                    nullable: an || bn,
                }
            }
        }
    }

    /// Returns true if this domain admits `value`.
    pub fn contains(&self, value: &ScalarValue) -> bool {
        match self {
            Domain::DistinctValues(values) => values.iter().any(|v| v == value),
            Domain::Range { min, max, nullable } => {
                if matches!(value, ScalarValue::Null) {
                    return *nullable;
                }
                matches!(
                    (
                        scalar_partial_cmp(value, min),
                        scalar_partial_cmp(value, max)
                    ),
                    (
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    )
                )
            }
            Domain::Bloom(bloom) => bloom.contains(value),
            Domain::All => true,
        }
    }
}

fn bloom_positions<'a>(
    bytes: &'a [u8],
    num_bits: u64,
    k: u32,
    seed: u64,
) -> impl Iterator<Item = u64> + 'a {
    let h1 = stable_hash(bytes, seed);
    let h2 = stable_hash(bytes, seed ^ 0x9e37_79b9_7f4a_7c15) | 1;
    (0..k).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits)
}

fn stable_hash(bytes: &[u8], seed: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64 ^ seed;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn scalar_canonical_bytes(value: &ScalarValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        ScalarValue::Null => out.push(0),
        ScalarValue::Boolean(v) => {
            out.push(1);
            out.push(u8::from(*v));
        }
        ScalarValue::Int32(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Int64(v) => {
            out.push(3);
            out.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Float32(v) => {
            out.push(4);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        ScalarValue::Float64(v) => {
            out.push(5);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        ScalarValue::Utf8(v) => {
            out.push(6);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            out.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Binary(v) => {
            out.push(7);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            out.extend_from_slice(v);
        }
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            out.push(8);
            out.extend_from_slice(&value.to_le_bytes());
            out.push(*precision);
            out.extend_from_slice(&scale.to_le_bytes());
        }
        ScalarValue::Date32(v) => {
            out.push(9);
            out.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Timestamp {
            value,
            unit,
            timezone,
        } => {
            out.push(10);
            out.extend_from_slice(&value.to_le_bytes());
            out.push(match unit {
                crate::types::TimeUnit::Second => 0,
                crate::types::TimeUnit::Millisecond => 1,
                crate::types::TimeUnit::Microsecond => 2,
                crate::types::TimeUnit::Nanosecond => 3,
            });
            match timezone {
                Some(tz) => {
                    out.push(1);
                    out.extend_from_slice(&(tz.len() as u64).to_le_bytes());
                    out.extend_from_slice(tz.as_bytes());
                }
                None => out.push(0),
            }
        }
    }
    out
}

/// Dedupes a Vec<ScalarValue> in place, preserving the first occurrence.
///
/// O(n²) for tiny n; acceptable here because per-partition DF inputs are
/// bounded by `DEFAULT_MAX_DISTINCT_VALUES` (20K) and the cost is dwarfed
/// by the build phase itself.
fn dedupe_in_place(v: &mut Vec<ScalarValue>) {
    let mut i = 0;
    while i < v.len() {
        let mut j = i + 1;
        while j < v.len() {
            if v[i] == v[j] {
                v.swap_remove(j);
            } else {
                j += 1;
            }
        }
        i += 1;
    }
}

/// Returns the smaller of two ScalarValues using `partial_cmp`. Returns
/// `None` if comparison is undefined (e.g. NaN float, type mismatch).
fn scalar_min(a: &ScalarValue, b: &ScalarValue) -> Option<ScalarValue> {
    match scalar_partial_cmp(a, b)? {
        std::cmp::Ordering::Greater => Some(b.clone()),
        _ => Some(a.clone()),
    }
}

/// Returns the larger of two ScalarValues using `partial_cmp`. Returns
/// `None` if comparison is undefined.
fn scalar_max(a: &ScalarValue, b: &ScalarValue) -> Option<ScalarValue> {
    match scalar_partial_cmp(a, b)? {
        std::cmp::Ordering::Less => Some(b.clone()),
        _ => Some(a.clone()),
    }
}

/// Defines a partial ordering across `ScalarValue` variants. Only same-typed
/// pairs are comparable; cross-type comparison returns `None`. NaN floats
/// also return `None`.
fn scalar_partial_cmp(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    use ScalarValue::*;
    match (a, b) {
        (Boolean(x), Boolean(y)) => Some(x.cmp(y)),
        (Int32(x), Int32(y)) => Some(x.cmp(y)),
        (Int64(x), Int64(y)) => Some(x.cmp(y)),
        (Float32(x), Float32(y)) => x.partial_cmp(y),
        (Float64(x), Float64(y)) => x.partial_cmp(y),
        (Utf8(x), Utf8(y)) => Some(x.cmp(y)),
        (Date32(x), Date32(y)) => Some(x.cmp(y)),
        (
            Decimal128 {
                value: xv,
                precision: xp,
                scale: xs,
            },
            Decimal128 {
                value: yv,
                precision: yp,
                scale: ys,
            },
        ) if xp == yp && xs == ys => Some(xv.cmp(yv)),
        _ => None,
    }
}

/// Computes a Range Domain summarising a Vec<ScalarValue>. Returns `None`
/// if any element comparison yields `None` (e.g. mixed types or NaN).
fn distinct_to_range(values: Vec<ScalarValue>) -> Option<Domain> {
    let mut iter = values.into_iter();
    let first = iter.next()?;
    let mut min = first.clone();
    let mut max = first;
    for v in iter {
        min = scalar_min(&min, &v)?;
        max = scalar_max(&max, &v)?;
    }
    Some(Domain::Range {
        min,
        max,
        nullable: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_none_is_empty() {
        let d = Domain::none();
        assert!(d.is_empty());
        assert!(!d.is_all());
    }

    #[test]
    fn domain_all_is_all() {
        let d = Domain::all();
        assert!(d.is_all());
        assert!(!d.is_empty());
    }

    #[test]
    fn union_distinct_distinct_below_cap() {
        let a = Domain::DistinctValues(vec![ScalarValue::Int64(1), ScalarValue::Int64(2)]);
        let b = Domain::DistinctValues(vec![ScalarValue::Int64(2), ScalarValue::Int64(3)]);
        let merged = a.union(b, 10);
        match merged {
            Domain::DistinctValues(mut v) => {
                v.sort_by(|x, y| scalar_partial_cmp(x, y).unwrap());
                assert_eq!(
                    v,
                    vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Int64(3),
                    ]
                );
            }
            other => panic!("expected DistinctValues, got {other:?}"),
        }
    }

    #[test]
    fn union_distinct_overflow_degrades_to_bloom() {
        std::env::set_var("ARNEB_BLOOM_DF", "1");
        let a = Domain::DistinctValues((0..30i64).map(ScalarValue::Int64).collect());
        let b = Domain::DistinctValues((30..70i64).map(ScalarValue::Int64).collect());
        // cap = 50, union has 70 unique entries → must degrade
        let merged = a.union(b, 50);
        match merged {
            Domain::Bloom(bloom) => {
                for v in 0..70i64 {
                    assert!(bloom.contains(&ScalarValue::Int64(v)));
                }
            }
            other => panic!("expected Bloom, got {other:?}"),
        }
        std::env::remove_var("ARNEB_BLOOM_DF");
    }

    #[test]
    fn bloom_filter_insert_contains_has_no_false_negatives() {
        let mut bloom = BloomFilter::with_fixed_params();
        for v in 0..1000i64 {
            bloom.insert(&ScalarValue::Int64(v));
        }
        for v in 0..1000i64 {
            assert!(bloom.contains(&ScalarValue::Int64(v)));
        }
    }

    #[test]
    fn union_bloom_bloom_bitwise_or_finds_inserted_keys() {
        let mut a = BloomFilter::with_fixed_params();
        let mut b = BloomFilter::with_fixed_params();
        a.insert(&ScalarValue::Int64(11));
        b.insert(&ScalarValue::Int64(22));

        let merged = Domain::Bloom(a).union(Domain::Bloom(b), 10);
        match merged {
            Domain::Bloom(bloom) => {
                assert!(bloom.contains(&ScalarValue::Int64(11)));
                assert!(bloom.contains(&ScalarValue::Int64(22)));
            }
            other => panic!("expected Bloom, got {other:?}"),
        }
    }

    #[test]
    fn union_bloom_distinct_values_inserts_distinct_values() {
        let mut bloom = BloomFilter::with_fixed_params();
        bloom.insert(&ScalarValue::Int64(11));
        let merged = Domain::Bloom(bloom).union(
            Domain::DistinctValues(vec![ScalarValue::Int64(33), ScalarValue::Int64(44)]),
            10,
        );
        match merged {
            Domain::Bloom(bloom) => {
                assert!(bloom.contains(&ScalarValue::Int64(11)));
                assert!(bloom.contains(&ScalarValue::Int64(33)));
                assert!(bloom.contains(&ScalarValue::Int64(44)));
            }
            other => panic!("expected Bloom, got {other:?}"),
        }
    }

    #[test]
    fn domain_contains_bloom_positive_and_negative() {
        let mut bloom = BloomFilter::with_fixed_params();
        bloom.insert(&ScalarValue::Int64(123));
        let domain = Domain::Bloom(bloom);
        assert!(domain.contains(&ScalarValue::Int64(123)));
        assert!(!domain.contains(&ScalarValue::Int64(9_876_543_210)));
    }

    #[test]
    fn union_with_all_is_all() {
        let a = Domain::DistinctValues(vec![ScalarValue::Int64(1)]);
        assert!(a.union(Domain::All, 10).is_all());
        let b = Domain::DistinctValues(vec![ScalarValue::Int64(1)]);
        assert!(Domain::All.union(b, 10).is_all());
    }

    #[test]
    fn union_range_range() {
        let a = Domain::Range {
            min: ScalarValue::Int64(5),
            max: ScalarValue::Int64(10),
            nullable: false,
        };
        let b = Domain::Range {
            min: ScalarValue::Int64(7),
            max: ScalarValue::Int64(15),
            nullable: true,
        };
        match a.union(b, 10) {
            Domain::Range { min, max, nullable } => {
                assert_eq!(min, ScalarValue::Int64(5));
                assert_eq!(max, ScalarValue::Int64(15));
                assert!(nullable);
            }
            other => panic!("expected Range, got {other:?}"),
        }
    }

    #[test]
    fn union_nan_degrades_to_all() {
        let a = Domain::Range {
            min: ScalarValue::Float64(0.0),
            max: ScalarValue::Float64(1.0),
            nullable: false,
        };
        let b = Domain::Range {
            min: ScalarValue::Float64(f64::NAN),
            max: ScalarValue::Float64(2.0),
            nullable: false,
        };
        assert!(a.union(b, 10).is_all());
    }

    #[test]
    fn dedupe_basic() {
        let mut v = vec![
            ScalarValue::Int64(1),
            ScalarValue::Int64(2),
            ScalarValue::Int64(1),
            ScalarValue::Int64(3),
            ScalarValue::Int64(2),
        ];
        dedupe_in_place(&mut v);
        v.sort_by(|x, y| scalar_partial_cmp(x, y).unwrap());
        assert_eq!(
            v,
            vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Int64(3),
            ]
        );
    }
}
