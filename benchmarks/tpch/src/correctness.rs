//! Cross-engine correctness check via canonical row hashing.

use sha2::{Digest, Sha256};

use crate::canonical::{write_row, CanonicalValue};

/// Canonicalize a result set: format every value, sort rows lexicographically,
/// and join with `\n`. Two engines that agree on a query produce identical
/// canonical strings up to the documented float-rounding tolerance.
pub fn canonicalize(rows: &[Vec<CanonicalValue>]) -> String {
    let mut row_strings: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut s = String::new();
            write_row(row, &mut s);
            s
        })
        .collect();
    row_strings.sort();
    row_strings.join("\n")
}

/// SHA-256 of the canonicalized result, lowercase hex.
pub fn hash(rows: &[Vec<CanonicalValue>]) -> String {
    let canonical = canonicalize(rows);
    let digest = Sha256::digest(canonical.as_bytes());
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_with_different_order_hash_identically() {
        let a = vec![
            vec![CanonicalValue::Int(1), CanonicalValue::Str("a".into())],
            vec![CanonicalValue::Int(2), CanonicalValue::Str("b".into())],
        ];
        let b = vec![
            vec![CanonicalValue::Int(2), CanonicalValue::Str("b".into())],
            vec![CanonicalValue::Int(1), CanonicalValue::Str("a".into())],
        ];
        assert_eq!(hash(&a), hash(&b));
    }

    #[test]
    fn null_renders_as_backslash_n() {
        let rows = vec![vec![CanonicalValue::Null]];
        assert_eq!(canonicalize(&rows), "\\N");
    }

    #[test]
    fn floats_differing_beyond_twelve_significant_digits_still_diverge() {
        // A real computation error is far larger than float noise and must not
        // be absorbed by the canonical form.
        let a = vec![vec![CanonicalValue::Float(1.123456789)]];
        let b = vec![vec![CanonicalValue::Float(1.123456111)]];
        assert_ne!(canonicalize(&a), canonicalize(&b));
        assert_ne!(hash(&a), hash(&b));
    }

    #[test]
    fn one_ulp_of_summation_noise_does_not_count_as_divergence() {
        // Real values observed running TPC-H q01's SUM(l_extendedprice) on two
        // engines: identical except for the final ULP, because the two summed
        // the column in a different order. Comparing at fixed decimals reported
        // these as disagreeing; at 12 significant digits they agree, which is
        // the honest reading — f64 cannot distinguish them.
        let trino = vec![vec![CanonicalValue::Float(56586554400.72966)]];
        let arneb = vec![vec![CanonicalValue::Float(56586554400.72965)]];
        assert_eq!(canonicalize(&trino), canonicalize(&arneb));
        assert_eq!(hash(&trino), hash(&arneb));
    }

    #[test]
    fn canonical_form_is_scale_invariant() {
        // The same relative error must be judged the same way at any magnitude.
        // Fixed-decimal formatting fails this: it is strict on large values and
        // lax on small ones.
        let small_a = vec![vec![CanonicalValue::Float(1.000000000001)]];
        let small_b = vec![vec![CanonicalValue::Float(1.000000000002)]];
        let big_a = vec![vec![CanonicalValue::Float(1.000000000001e10)]];
        let big_b = vec![vec![CanonicalValue::Float(1.000000000002e10)]];
        assert_eq!(
            canonicalize(&small_a) == canonicalize(&small_b),
            canonicalize(&big_a) == canonicalize(&big_b),
            "the same relative difference was judged differently at two magnitudes"
        );
    }
}
