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
    fn floats_round_to_six_decimals() {
        let a = vec![vec![CanonicalValue::Float(1.123456789)]];
        let b = vec![vec![CanonicalValue::Float(1.123456111)]];
        assert_eq!(canonicalize(&a), "1.123457");
        assert_eq!(canonicalize(&b), "1.123456");
        assert_ne!(hash(&a), hash(&b));
    }

    #[test]
    fn float_within_six_decimals_hashes_same() {
        let a = vec![vec![CanonicalValue::Float(1.1234567)]];
        let b = vec![vec![CanonicalValue::Float(1.1234568)]];
        // 1.1234567 → 1.123457 ; 1.1234568 → 1.123457 (banker's rounding may differ;
        // this test asserts the property at a tolerance-friendly pair).
        assert_eq!(canonicalize(&a), canonicalize(&b));
    }
}
