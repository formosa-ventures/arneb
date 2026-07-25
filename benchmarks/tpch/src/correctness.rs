//! Cross-engine correctness check via canonical row hashing.

use sha2::{Digest, Sha256};

use crate::canonical::{write_row, CanonicalValue};

/// Precision the primary hash is computed at. Well inside f64's 15-17
/// significant digits, so it absorbs summation-order noise between engines that
/// partition an aggregate differently.
pub const STRICT_SIG_DIGITS: usize = 12;

/// Precision used to adjudicate a strict-hash mismatch.
///
/// Any comparison built on "round, then compare the text" has rounding
/// boundaries, and two values straddling one are reported as different however
/// close they are: TPC-H q09 produced 309901366.4294996 against
/// 309901366.4295008 — agreeing to 15 significant digits, 3.9e-15 apart — yet
/// the 12th digit rounds to ...429 on one side and ...430 on the other.
/// Lowering the precision only moves the boundary, so a second, coarser hash
/// adjudicates instead. Two values straddling a boundary at both precisions at
/// once is not a case worth engineering around.
pub const RELAXED_SIG_DIGITS: usize = 9;

/// Canonicalize a result set: format every value, sort rows lexicographically,
/// and join with `\n`.
pub fn canonicalize_with(rows: &[Vec<CanonicalValue>], sig_digits: usize) -> String {
    let mut row_strings: Vec<String> = rows
        .iter()
        .map(|row| {
            let mut s = String::new();
            write_row(row, &mut s, sig_digits);
            s
        })
        .collect();
    row_strings.sort();
    row_strings.join("\n")
}

/// Canonicalize at the strict precision.
pub fn canonicalize(rows: &[Vec<CanonicalValue>]) -> String {
    canonicalize_with(rows, STRICT_SIG_DIGITS)
}

fn digest(canonical: &str) -> String {
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

/// SHA-256 of the canonicalized result at the strict precision, lowercase hex.
pub fn hash(rows: &[Vec<CanonicalValue>]) -> String {
    digest(&canonicalize(rows))
}

/// SHA-256 at the relaxed precision, used only to adjudicate a strict mismatch.
pub fn hash_relaxed(rows: &[Vec<CanonicalValue>]) -> String {
    digest(&canonicalize_with(rows, RELAXED_SIG_DIGITS))
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

#[cfg(test)]
mod boundary_tests {
    use super::*;
    use crate::canonical::CanonicalValue;

    /// Real values from TPC-H q09 on arneb and Trino: 3.9e-15 apart — agreeing
    /// to 15 significant digits — but straddling the rounding boundary in the
    /// 12th, so the strict hashes differ. The relaxed hash is what stops that
    /// from being published as a correctness divergence.
    #[test]
    fn rounding_boundary_splits_strict_hash_but_not_relaxed() {
        let a = vec![vec![CanonicalValue::Float(309901366.4294996)]];
        let b = vec![vec![CanonicalValue::Float(309901366.4295008)]];
        assert_ne!(
            hash(&a),
            hash(&b),
            "expected the strict hashes to straddle the boundary"
        );
        assert_eq!(
            hash_relaxed(&a),
            hash_relaxed(&b),
            "the relaxed hash must adjudicate a boundary artifact as agreement"
        );
    }

    /// The relaxed hash must not wave through a real difference.
    #[test]
    fn relaxed_hash_still_separates_a_genuine_difference() {
        let a = vec![vec![CanonicalValue::Float(309901366.42)]];
        let b = vec![vec![CanonicalValue::Float(309911366.42)]];
        assert_ne!(hash(&a), hash(&b));
        assert_ne!(hash_relaxed(&a), hash_relaxed(&b));
    }
}
