## ADDED Requirements

### Requirement: Selectivity is a fraction in `[0.0, 1.0]`

The system SHALL provide a function `selectivity(predicate: &PlanExpr, stats: &TableStatistics) -> f64` that returns the estimated fraction of input rows passing the predicate. The result SHALL be clamped to `[0.0, 1.0]`.

#### Scenario: Selectivity is bounded

- **WHEN** `selectivity` is called for any predicate
- **THEN** the result is `>= 0.0` and `<= 1.0`

### Requirement: Per-predicate selectivity rules

The system SHALL estimate selectivity for each predicate shape according to the table below. Defaults err on the **selective** side so unknown predicates do not fool the reorderer into choosing a giant build side.

| Predicate                  | Selectivity                                          | Default when stats missing |
|----------------------------|------------------------------------------------------|----------------------------|
| `col = literal`            | `1.0 / max(ndv, 1)`                                  | `0.1`                      |
| `col != literal`           | `1.0 - selectivity(col = literal)`                   | `0.9`                      |
| `col < literal`            | `(literal - min) / (max - min)`                      | `0.33`                     |
| `col <= literal`           | `(literal - min) / (max - min)`                      | `0.33`                     |
| `col > literal`            | `(max - literal) / (max - min)`                      | `0.33`                     |
| `col >= literal`           | `(max - literal) / (max - min)`                      | `0.33`                     |
| `col BETWEEN a AND b`      | `(b - a) / (max - min)`                              | `0.25`                     |
| `col IN (k items)`         | `min(k / ndv, 1.0)`                                  | `0.1 * k` (capped at 1.0)  |
| `col LIKE 'prefix%'`       | `0.1`                                                | `0.1`                      |
| `col LIKE '%suffix'`       | `0.1`                                                | `0.1`                      |
| `col LIKE '%infix%'`       | `0.1`                                                | `0.1`                      |
| `col IS NULL`              | `null_fraction`                                      | `0.05`                     |
| `col IS NOT NULL`          | `1.0 - null_fraction`                                | `0.95`                     |
| `A AND B`                  | `selectivity(A) * selectivity(B)` (independence)     | n/a                        |
| `A OR B`                   | `selectivity(A) + selectivity(B) - selectivity(A)*selectivity(B)` | n/a       |
| `NOT A`                    | `1.0 - selectivity(A)`                               | n/a                        |
| any other expression       | `0.5`                                                | `0.5`                      |

For ordered-type comparisons (`<`, `<=`, `>`, `>=`, `BETWEEN`), when either `min_value` or `max_value` is missing OR when `min_value == max_value`, the system SHALL fall back to the default value in the table.

#### Scenario: Equality with NDV

- **GIVEN** a column `c_nationkey` with `ColumnStatistics { ndv: Some(25), .. }`
- **WHEN** `selectivity(c_nationkey = 5)` is called
- **THEN** the result is `1.0 / 25 = 0.04`

#### Scenario: Equality without NDV defaults to 0.1

- **GIVEN** a column with `ColumnStatistics { ndv: None, .. }`
- **WHEN** `selectivity(col = 'X')` is called
- **THEN** the result is `0.1`

#### Scenario: Range with min/max

- **GIVEN** a column `l_quantity` with `min = 1`, `max = 50`
- **WHEN** `selectivity(l_quantity < 25)` is called
- **THEN** the result is `(25 - 1) / (50 - 1) ≈ 0.49`

#### Scenario: Range without min/max defaults to 0.33

- **GIVEN** a column with `min = None`
- **WHEN** `selectivity(col < 100)` is called
- **THEN** the result is `0.33`

#### Scenario: IN list

- **GIVEN** a column with `ndv = Some(50)`
- **WHEN** `selectivity(col IN (1, 2, 3, 4, 5))` is called
- **THEN** the result is `min(5 / 50, 1.0) = 0.1`

#### Scenario: AND combination

- **GIVEN** predicates `A` with `selectivity = 0.1` and `B` with `selectivity = 0.5`
- **WHEN** `selectivity(A AND B)` is called
- **THEN** the result is `0.05`

#### Scenario: OR combination

- **GIVEN** predicates `A` with `selectivity = 0.1` and `B` with `selectivity = 0.2`
- **WHEN** `selectivity(A OR B)` is called
- **THEN** the result is `0.1 + 0.2 - 0.02 = 0.28`

#### Scenario: IS NULL with null_fraction

- **GIVEN** a column with `null_fraction = Some(0.15)`
- **WHEN** `selectivity(col IS NULL)` is called
- **THEN** the result is `0.15`

#### Scenario: Unknown expression shape

- **WHEN** `selectivity` encounters a predicate shape not covered above (e.g. `udf(col)`)
- **THEN** the result is `0.5`

### Requirement: Conservative defaults documented and centralized

The selectivity defaults SHALL be exposed as named constants in `crates/planner/src/selectivity.rs` so tuning happens in one place.

```rust
pub const DEFAULT_EQ_SELECTIVITY: f64 = 0.1;
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
pub const DEFAULT_BETWEEN_SELECTIVITY: f64 = 0.25;
pub const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;
pub const DEFAULT_NULL_SELECTIVITY: f64 = 0.05;
pub const DEFAULT_UNKNOWN_SELECTIVITY: f64 = 0.5;
```

#### Scenario: Constants are accessible from tests

- **WHEN** a unit test references `selectivity::DEFAULT_EQ_SELECTIVITY`
- **THEN** the symbol resolves to `0.1`
