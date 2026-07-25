//! Canonical row representation shared by all engine adapters.
//!
//! Each adapter converts engine-native row data into `Vec<Vec<CanonicalValue>>`
//! so the runner can hash result sets uniformly without engine-specific logic.

use std::fmt::{self, Write};

#[derive(Debug, Clone, PartialEq)]
pub enum CanonicalValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// Pre-formatted timestamp/date in RFC 3339 UTC form.
    Timestamp(String),
}

impl CanonicalValue {
    /// Render this value to its canonical text form. NULL renders as `\N`,
    /// floats are rendered with 6 fractional digits, timestamps are passed
    /// through verbatim (the adapter is responsible for formatting them
    /// as RFC 3339 UTC before constructing the variant).
    pub fn write_canonical(&self, out: &mut String, sig_digits: usize) {
        match self {
            CanonicalValue::Null => out.push_str("\\N"),
            CanonicalValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            CanonicalValue::Int(i) => {
                let _ = write!(out, "{i}");
            }
            CanonicalValue::Float(f) => {
                if f.is_nan() {
                    out.push_str("NaN");
                } else if f.is_infinite() {
                    out.push_str(if *f > 0.0 { "Infinity" } else { "-Infinity" });
                } else {
                    // Significant digits, not fixed decimals. Fixed decimals are
                    // not scale-invariant: `{:.6}` on a value near 5.7e10 asks
                    // for 17 significant digits, past what f64 can represent, so
                    // the last bit of float noise becomes a "divergence". Two
                    // engines summing the same column in a different order —
                    // which any difference in partitioning guarantees — differ by
                    // one ULP and would be reported as disagreeing.
                    //
                    // 12 significant digits sits comfortably inside f64's ~15-17
                    // and absorbs summation-order noise, while a genuine
                    // computation error is far larger than one part in 1e12.
                    let _ = write!(out, "{f:.*e}", sig_digits.saturating_sub(1));
                }
            }
            CanonicalValue::Str(s) => out.push_str(s),
            CanonicalValue::Timestamp(s) => out.push_str(s),
        }
    }
}

impl fmt::Display for CanonicalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        // Display is for diagnostics only; hashing always goes through
        // `correctness`, which passes an explicit precision.
        self.write_canonical(&mut s, crate::correctness::STRICT_SIG_DIGITS);
        f.write_str(&s)
    }
}

/// Render a single row's values joined by `\t` into the canonical row form.
pub fn write_row(row: &[CanonicalValue], out: &mut String, sig_digits: usize) {
    for (i, v) in row.iter().enumerate() {
        if i > 0 {
            out.push('\t');
        }
        v.write_canonical(out, sig_digits);
    }
}
