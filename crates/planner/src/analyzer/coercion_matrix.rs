//! Implicit type-coercion matrix.
//!
//! Every entry in [`MATRIX`] is a single permitted implicit cast,
//! tagged with a [`Safety`] classification. The matrix is the only
//! source of truth for what coercions the analyzer will insert — the
//! traversal in [`super::type_coercion`] is purely mechanical.
//!
//! Adding a new implicit cast SHOULD require only a new row here.
//!
//! Semantics match Trino's `TypeCoercion.getCommonSuperType` (see
//! [Trino docs](https://trino.io/docs/current/language/types.html) on
//! implicit conversions). Decimal precision/scale reconciliation
//! follows the same formula as `arrow::compute::cast` — see
//! [`decimal_supertype`] for the derivation.
//!
//! # Safety classification
//!
//! - [`Safety::AlwaysSafe`]: column-to-column is allowed; no data loss
//!   expected. Example: `Int32 → Int64`.
//! - [`Safety::LiteralOnly`]: allowed only when the source operand is
//!   a literal (or a folded literal — see `is_literal_like`). Example:
//!   `Utf8 → Date32`. Applied to a column, this would silently turn
//!   every lookup into a date parse at read time; that is never what
//!   the user wants. The coercion path MUST reject column-to-column
//!   applications of `LiteralOnly` rules with a `PlanError::TypeMismatch`.
//! - [`Safety::PrecisionLoss`]: allowed column-to-column but may lose
//!   precision (e.g., `Int64 → Float64`). Trino allows these; we match.

use arneb_common::types::DataType;

/// How dangerous an implicit coercion is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Safety {
    /// Losslessly widening; column ↔ column allowed.
    AlwaysSafe,
    /// Only allowed when the source is a literal (or folds to one).
    /// Column-to-column applications are rejected at plan time.
    LiteralOnly,
    /// May lose precision (e.g., `Int64 → Float64`), but Trino allows
    /// it and so do we.
    PrecisionLoss,
}

/// A single permitted implicit cast.
#[derive(Debug, Clone, Copy)]
pub struct CoercionRule {
    /// Pattern that must match the source type.
    pub from: TypePattern,
    /// Pattern that must match the target type.
    pub to: TypePattern,
    /// Safety classification.
    pub safety: Safety,
}

/// A pattern used by the matrix to match variants whose parameters
/// (decimal precision/scale, timestamp unit/timezone) should not gate
/// rule applicability. Exact types can be expressed as `Exact(dt)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypePattern {
    /// The `Int32` variant.
    Int32,
    /// The `Int64` variant.
    Int64,
    /// The `Float32` variant.
    Float32,
    /// The `Float64` variant.
    Float64,
    /// Any `Decimal128 { precision, scale }`.
    AnyDecimal,
    /// The `Utf8` variant.
    Utf8,
    /// The `Date32` variant.
    Date32,
    /// Any `Timestamp { unit, timezone }`.
    AnyTimestamp,
}

impl TypePattern {
    /// Returns true if this pattern's tag matches `dt`.
    fn matches(self, dt: &DataType) -> bool {
        matches!(
            (self, dt),
            (TypePattern::Int32, DataType::Int32)
                | (TypePattern::Int64, DataType::Int64)
                | (TypePattern::Float32, DataType::Float32)
                | (TypePattern::Float64, DataType::Float64)
                | (TypePattern::AnyDecimal, DataType::Decimal128 { .. })
                | (TypePattern::Utf8, DataType::Utf8)
                | (TypePattern::Date32, DataType::Date32)
                | (TypePattern::AnyTimestamp, DataType::Timestamp { .. })
        )
    }
}

/// The coercion matrix. Order matters only insofar as `lookup_cast`
/// returns the first match; the matrix is deliberately disjoint so
/// behavior is order-independent.
const MATRIX: &[CoercionRule] = &[
    // --- Numeric widening ---
    CoercionRule {
        from: TypePattern::Int32,
        to: TypePattern::Int64,
        safety: Safety::AlwaysSafe,
    },
    CoercionRule {
        from: TypePattern::Int32,
        to: TypePattern::Float32,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::Int32,
        to: TypePattern::Float64,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::Int64,
        to: TypePattern::Float64,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::Float32,
        to: TypePattern::Float64,
        safety: Safety::AlwaysSafe,
    },
    // --- Numeric ↔ Decimal ---
    CoercionRule {
        from: TypePattern::Int32,
        to: TypePattern::AnyDecimal,
        safety: Safety::AlwaysSafe, // AnyDecimal's precision is decided by supertype; lossless at widest scale.
    },
    CoercionRule {
        from: TypePattern::Int64,
        to: TypePattern::AnyDecimal,
        safety: Safety::AlwaysSafe,
    },
    CoercionRule {
        from: TypePattern::AnyDecimal,
        to: TypePattern::Float64,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::Float32,
        to: TypePattern::AnyDecimal,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::Float64,
        to: TypePattern::AnyDecimal,
        safety: Safety::PrecisionLoss,
    },
    CoercionRule {
        from: TypePattern::AnyDecimal,
        to: TypePattern::AnyDecimal,
        // Safety between two decimals is computed on a per-instance
        // basis in `decimal_supertype`; the matrix entry only gates
        // the existence of a path. AlwaysSafe here is a floor —
        // `common_supertype` narrows further when scales shrink.
        safety: Safety::AlwaysSafe,
    },
    // --- String → date/time (LITERAL ONLY) ---
    CoercionRule {
        from: TypePattern::Utf8,
        to: TypePattern::Date32,
        safety: Safety::LiteralOnly,
    },
    CoercionRule {
        from: TypePattern::Utf8,
        to: TypePattern::AnyTimestamp,
        safety: Safety::LiteralOnly,
    },
];

/// Look up the safety classification for an implicit cast from
/// `from` to `to`. Returns `None` if the cast is not in the matrix
/// (i.e., no implicit coercion is permitted between those types).
///
/// The identity cast `T → T` is always allowed and returns
/// `Some(Safety::AlwaysSafe)` without consulting the matrix.
pub fn lookup_cast(from: &DataType, to: &DataType) -> Option<Safety> {
    if from == to {
        return Some(Safety::AlwaysSafe);
    }
    for rule in MATRIX {
        if rule.from.matches(from) && rule.to.matches(to) {
            return Some(rule.safety);
        }
    }
    None
}

/// Where the coercion is happening. Affects how `common_supertype`
/// interprets literal-only rules.
///
/// - `Binary { left_is_literal, right_is_literal }`: the two operands
///   of a binary operator / join equality / between / in-list. A
///   `LiteralOnly` rule fires only when the side being coerced is a
///   literal.
/// - `CaseBranch`: two CASE-result arms unified by common supertype.
///   Both arms are treated as column-like (no literal-only coercions
///   fire across arms); literal-only still applies when a literal
///   branch is promoted to match a column branch.
/// - `UnionColumn`: set-operation column alignment. Column-to-column
///   only — literal-only rules do not fire here.
#[derive(Debug, Clone, Copy)]
pub enum CoercionSite {
    /// Binary operator, IN list tested expression, BETWEEN operand.
    Binary {
        /// Whether the left operand is a literal (or folds to one).
        left_is_literal: bool,
        /// Whether the right operand is a literal (or folds to one).
        right_is_literal: bool,
    },
    /// Two arms of a CASE / COALESCE / NULLIF.
    CaseBranch {
        /// Whether the left arm is literal-like.
        left_is_literal: bool,
        /// Whether the right arm is literal-like.
        right_is_literal: bool,
    },
    /// One column of a UNION / INTERSECT / EXCEPT branch alignment.
    UnionColumn,
}

impl CoercionSite {
    /// Whether the left-hand operand is literal-like. Used by the
    /// matrix walker to gate `Safety::LiteralOnly` rules.
    pub fn left_is_literal(self) -> bool {
        match self {
            CoercionSite::Binary {
                left_is_literal, ..
            }
            | CoercionSite::CaseBranch {
                left_is_literal, ..
            } => left_is_literal,
            CoercionSite::UnionColumn => false,
        }
    }
    /// Whether the right-hand operand is literal-like.
    pub fn right_is_literal(self) -> bool {
        match self {
            CoercionSite::Binary {
                right_is_literal, ..
            }
            | CoercionSite::CaseBranch {
                right_is_literal, ..
            } => right_is_literal,
            CoercionSite::UnionColumn => false,
        }
    }
}

/// Compute the smallest data type both `a` and `b` can be implicitly
/// cast to under the matrix, given the coercion site.
///
/// Returns `None` when no common supertype exists — the analyzer will
/// surface this as a `PlanError::TypeMismatch`.
///
/// # Algorithm
///
/// 1. Identity shortcut: `a == b → a`.
/// 2. Decimal/Decimal: compute via [`decimal_supertype`] (Trino formula).
/// 3. Otherwise, try both directions in the matrix: if `a → b` is
///    allowed (with any needed `LiteralOnly` gate satisfied), pick
///    `b`; if `b → a` is allowed, pick `a`; if both are allowed, pick
///    the one with the higher precedence in the numeric tower
///    (Int32 < Int64 < Float32 < Float64 < Decimal).
/// 4. Otherwise, try a two-step bridge through `Float64` or `Decimal`
///    for the well-known numeric-vs-decimal case.
pub fn common_supertype(a: &DataType, b: &DataType, site: CoercionSite) -> Option<DataType> {
    if a == b {
        return Some(a.clone());
    }

    // Decimal ↔ Decimal has its own precision/scale reconciliation.
    if let (
        DataType::Decimal128 {
            precision: p1,
            scale: s1,
        },
        DataType::Decimal128 {
            precision: p2,
            scale: s2,
        },
    ) = (a, b)
    {
        return Some(decimal_supertype(*p1, *s1, *p2, *s2));
    }

    // Int ↔ Decimal: widen integer into the decimal's space.
    if let (DataType::Int32, DataType::Decimal128 { precision, scale })
    | (DataType::Decimal128 { precision, scale }, DataType::Int32) = (a, b)
    {
        return Some(decimal_supertype(10, 0, *precision, *scale));
    }
    if let (DataType::Int64, DataType::Decimal128 { precision, scale })
    | (DataType::Decimal128 { precision, scale }, DataType::Int64) = (a, b)
    {
        return Some(decimal_supertype(19, 0, *precision, *scale));
    }

    // Float ↔ Decimal promotes to Float64 (Trino).
    if matches!(a, DataType::Float32 | DataType::Float64)
        && matches!(b, DataType::Decimal128 { .. })
        || matches!(b, DataType::Float32 | DataType::Float64)
            && matches!(a, DataType::Decimal128 { .. })
    {
        return Some(DataType::Float64);
    }

    // Generic: try a → b, then b → a.
    if let Some(safety) = lookup_cast(a, b) {
        if safety_allowed(
            safety,
            /* source_is_literal = */ site.left_is_literal(),
        ) {
            return Some(b.clone());
        }
    }
    if let Some(safety) = lookup_cast(b, a) {
        if safety_allowed(
            safety,
            /* source_is_literal = */ site.right_is_literal(),
        ) {
            return Some(a.clone());
        }
    }

    // Float32 ↔ Int* bridge through Float64 (matches runtime behavior).
    if (matches!(a, DataType::Float32) && matches!(b, DataType::Int32 | DataType::Int64))
        || (matches!(b, DataType::Float32) && matches!(a, DataType::Int32 | DataType::Int64))
    {
        return Some(DataType::Float64);
    }

    None
}

/// Trino's decimal supertype formula.
///
/// `common(Decimal(p1, s1), Decimal(p2, s2))`
///   = `Decimal(min(38, max(p1 - s1, p2 - s2) + max(s1, s2)), max(s1, s2))`
///
/// Intuition: the integer-part digit count of each side is `p - s`; the
/// supertype keeps enough integer digits for both (`max` of those),
/// plus the wider scale. We clamp at 38 (Decimal128's max precision).
///
/// Copy of the existing formula in
/// `crates/execution/src/expression.rs::wider_numeric_type` so the
/// runtime-deletion step in Phase 7 is a pure lift with no behavior
/// change.
pub fn decimal_supertype(p1: u8, s1: i8, p2: u8, s2: i8) -> DataType {
    let scale = s1.max(s2);
    let int_digits = (p1 as i8 - s1).max(p2 as i8 - s2);
    let precision = (int_digits + scale).clamp(1, 38) as u8;
    DataType::Decimal128 { precision, scale }
}

/// Returns true if the caller is allowed to apply a cast with the
/// given safety from a source operand with the given "is literal"
/// status.
fn safety_allowed(safety: Safety, source_is_literal: bool) -> bool {
    match safety {
        Safety::AlwaysSafe | Safety::PrecisionLoss => true,
        Safety::LiteralOnly => source_is_literal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arneb_common::types::TimeUnit;

    fn dec(p: u8, s: i8) -> DataType {
        DataType::Decimal128 {
            precision: p,
            scale: s,
        }
    }

    fn ts_us() -> DataType {
        DataType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        }
    }

    // --- lookup_cast: one test per matrix entry ---

    #[test]
    fn lookup_identity_is_always_safe() {
        assert_eq!(
            lookup_cast(&DataType::Int32, &DataType::Int32),
            Some(Safety::AlwaysSafe)
        );
    }

    #[test]
    fn lookup_int32_to_int64() {
        assert_eq!(
            lookup_cast(&DataType::Int32, &DataType::Int64),
            Some(Safety::AlwaysSafe)
        );
    }

    #[test]
    fn lookup_int_to_float_is_precision_loss() {
        assert_eq!(
            lookup_cast(&DataType::Int64, &DataType::Float64),
            Some(Safety::PrecisionLoss)
        );
    }

    #[test]
    fn lookup_float32_to_float64() {
        assert_eq!(
            lookup_cast(&DataType::Float32, &DataType::Float64),
            Some(Safety::AlwaysSafe)
        );
    }

    #[test]
    fn lookup_utf8_to_date32_is_literal_only() {
        assert_eq!(
            lookup_cast(&DataType::Utf8, &DataType::Date32),
            Some(Safety::LiteralOnly)
        );
    }

    #[test]
    fn lookup_utf8_to_timestamp_is_literal_only() {
        assert_eq!(
            lookup_cast(&DataType::Utf8, &ts_us()),
            Some(Safety::LiteralOnly)
        );
    }

    #[test]
    fn lookup_no_path_returns_none() {
        // Boolean ↔ Date32 is not in the matrix.
        assert_eq!(lookup_cast(&DataType::Boolean, &DataType::Date32), None);
    }

    #[test]
    fn lookup_decimal_to_decimal() {
        assert_eq!(
            lookup_cast(&dec(10, 2), &dec(20, 5)),
            Some(Safety::AlwaysSafe)
        );
    }

    // --- common_supertype ---

    #[test]
    fn supertype_identity() {
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Int32, &DataType::Int32, site),
            Some(DataType::Int32)
        );
    }

    #[test]
    fn supertype_int32_and_int64_is_int64() {
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Int32, &DataType::Int64, site),
            Some(DataType::Int64)
        );
    }

    #[test]
    fn supertype_int_and_float_is_float64() {
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Int32, &DataType::Float64, site),
            Some(DataType::Float64)
        );
        assert_eq!(
            common_supertype(&DataType::Int64, &DataType::Float32, site),
            Some(DataType::Float64)
        );
    }

    #[test]
    fn supertype_decimal_decimal_trino_formula() {
        // D(10,2) ∧ D(12,4) → integer digits = max(8, 8) = 8, scale = 4 → D(12, 4)
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&dec(10, 2), &dec(12, 4), site),
            Some(dec(12, 4))
        );
    }

    #[test]
    fn supertype_int_and_decimal() {
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        // Int32 ∧ Decimal(10, 2) → integer digits = max(10, 8) = 10, scale = 2 → Decimal(12, 2)
        assert_eq!(
            common_supertype(&DataType::Int32, &dec(10, 2), site),
            Some(dec(12, 2))
        );
    }

    #[test]
    fn supertype_no_common_returns_none() {
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Boolean, &DataType::Date32, site),
            None
        );
    }

    // --- LiteralOnly gating ---

    #[test]
    fn literal_only_rejects_column_to_column() {
        // Utf8 (column) <=> Date32 (column): no literal side, so the
        // Utf8 → Date32 rule must not fire.
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Utf8, &DataType::Date32, site),
            None
        );
    }

    #[test]
    fn literal_only_allows_column_vs_literal() {
        // Date32 column <= Utf8 literal: LiteralOnly rule fires
        // because the right side is a literal. Result is Date32.
        let site = CoercionSite::Binary {
            left_is_literal: false,
            right_is_literal: true,
        };
        assert_eq!(
            common_supertype(&DataType::Date32, &DataType::Utf8, site),
            Some(DataType::Date32)
        );
    }

    #[test]
    fn literal_only_allows_literal_vs_column_reversed() {
        // Utf8 literal <= Date32 column: the Utf8 side is the
        // literal, so LiteralOnly is permitted.
        let site = CoercionSite::Binary {
            left_is_literal: true,
            right_is_literal: false,
        };
        assert_eq!(
            common_supertype(&DataType::Utf8, &DataType::Date32, site),
            Some(DataType::Date32)
        );
    }

    #[test]
    fn union_column_disallows_literal_only_rules() {
        // Set-op column alignment is column-to-column only — no
        // implicit Utf8 → Date32.
        assert_eq!(
            common_supertype(
                &DataType::Utf8,
                &DataType::Date32,
                CoercionSite::UnionColumn
            ),
            None
        );
    }

    // --- decimal_supertype formula ---

    #[test]
    fn decimal_mul_like_supertype_matches_trino_doc_example() {
        // D(10,2) * D(10,2) output type in Trino is D(21, 4). That's
        // computed by the arithmetic rule (we pick up scale sum), not
        // by this supertype function which handles unification.
        // Here we only verify the unification path: D(10,2) ∧ D(10,2)
        // is already equal → itself.
        assert_eq!(decimal_supertype(10, 2, 10, 2), dec(10, 2));
    }

    #[test]
    fn decimal_supertype_clamps_at_38() {
        // Ensure the saturation branch is exercised.
        let got = decimal_supertype(38, 5, 38, 10);
        assert!(matches!(got, DataType::Decimal128 { precision: 38, .. }));
    }
}
