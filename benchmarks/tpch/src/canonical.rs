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
    pub fn write_canonical(&self, out: &mut String) {
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
                    let _ = write!(out, "{f:.6}");
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
        self.write_canonical(&mut s);
        f.write_str(&s)
    }
}

/// Render a single row's values joined by `\t` into the canonical row form.
pub fn write_row(row: &[CanonicalValue], out: &mut String) {
    for (i, v) in row.iter().enumerate() {
        if i > 0 {
            out.push('\t');
        }
        v.write_canonical(out);
    }
}
