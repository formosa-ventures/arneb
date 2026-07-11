## ADDED Requirements

### Requirement: Cost is expected output cardinality

The system SHALL represent a logical plan node's cost as a single `f64` equal to the expected number of output rows. Cost SHALL be defined recursively over `LogicalPlan` variants; CPU cost is not modeled in v1.

```rust
pub type Cost = f64;

pub fn estimated_cardinality(plan: &LogicalPlan, stats: &CatalogStats) -> Cost;
```

`CatalogStats` SHALL be a snapshot of all relevant table statistics gathered once at the start of planning.

#### Scenario: Cost is row-count units

- **WHEN** `estimated_cardinality(plan, stats)` is invoked on any `LogicalPlan`
- **THEN** the returned `f64` is the estimated row count, not a CPU-time or IO-byte estimate

### Requirement: Cardinality propagation per node type

The system SHALL propagate cardinality through each `LogicalPlan` variant according to the following rules.

| Node           | Output cardinality                                                |
|----------------|-------------------------------------------------------------------|
| `TableScan`    | `stats.row_count.unwrap_or(default_table_size = 10_000)`          |
| `Filter`       | `child * selectivity(predicate, stats)`                           |
| `Projection`   | `child`                                                           |
| `Limit n`      | `min(child, n)`                                                   |
| `Sort`         | `child`                                                           |
| `InnerJoin`    | `(left * right) / max(ndv_l, ndv_r, 1)`                           |
| `LeftJoin`     | `max(left, inner_estimate)`                                       |
| `RightJoin`    | `max(right, inner_estimate)`                                      |
| `FullJoin`     | `left + right + inner_estimate`                                   |
| `Aggregate`    | `min(child, product(group_ndv))`                                  |
| `Distinct`     | `min(child, product(col_ndv))`                                    |
| `Union`        | `sum(branches)`                                                   |
| `Intersect`    | `min(branches)`                                                   |
| `Except`       | `left`                                                            |

Estimates SHALL be clamped to `[1.0, f64::MAX]` to avoid division-by-zero in derived join cardinality calculations.

#### Scenario: TableScan with row_count

- **GIVEN** a `TableScan` whose underlying table reports `stats.row_count = Some(6_000_000)`
- **WHEN** `estimated_cardinality` is called
- **THEN** the result is `6_000_000.0`

#### Scenario: TableScan without row_count uses default

- **GIVEN** a `TableScan` whose `TableProvider::statistics()` returns `None`
- **WHEN** `estimated_cardinality` is called with `default_table_size = 10_000`
- **THEN** the result is `10_000.0`

#### Scenario: Filter selectivity

- **GIVEN** a `Filter` over a `TableScan` of `6_000_000` rows with predicate `col = 'X'` and `stats.col.ndv = Some(100)`
- **WHEN** `estimated_cardinality` is called
- **THEN** the result is `6_000_000.0 * (1.0 / 100.0) = 60_000.0`

#### Scenario: Inner join cardinality

- **GIVEN** an `InnerJoin` of `lineitem` (6M rows) and `orders` (1.5M rows) on `l_orderkey = o_orderkey` where `orders.o_orderkey.ndv = 1_500_000`
- **WHEN** `estimated_cardinality` is called
- **THEN** the result is `(6_000_000 * 1_500_000) / max(6_000_000, 1_500_000) = 1_500_000`

#### Scenario: Aggregate cardinality

- **GIVEN** an `Aggregate` over a child of `10_000_000` rows grouping by `(col_a, col_b)` where `col_a.ndv = 25` and `col_b.ndv = 100`
- **WHEN** `estimated_cardinality` is called
- **THEN** the result is `min(10_000_000, 25 * 100) = 2_500`

#### Scenario: Limit cardinality

- **GIVEN** a `Limit 100` over a child of `1_000_000` rows
- **WHEN** `estimated_cardinality` is called
- **THEN** the result is `100`

### Requirement: Cost model is total over LogicalPlan

The system's `estimated_cardinality` SHALL produce a finite, non-negative `f64` for every `LogicalPlan` variant. It SHALL never panic, return `NaN`, or return a negative number.

#### Scenario: Cost is finite for any plan

- **WHEN** `estimated_cardinality` is called on any `LogicalPlan` constructible by the planner
- **THEN** the result is `f64::is_finite()` and `>= 0.0`

#### Scenario: Missing stats degrade gracefully

- **GIVEN** a `Filter` whose predicate references a column with no `ColumnStatistics`
- **WHEN** `estimated_cardinality` is called
- **THEN** the result uses the conservative default selectivity (0.1 for equality, 0.33 for range, etc.) and is still finite and non-negative

### Requirement: CatalogStats container

The system SHALL provide a `CatalogStats` map keyed by qualified table reference, populated once per planning invocation, and threaded into `estimated_cardinality` calls.

```rust
pub struct CatalogStats {
    tables: HashMap<TableReference, Arc<TableStatistics>>,
}

impl CatalogStats {
    pub fn get(&self, reference: &TableReference) -> Option<&TableStatistics>;
    pub fn insert(&mut self, reference: TableReference, stats: TableStatistics);
}
```

#### Scenario: Lookup by qualified reference

- **GIVEN** a `CatalogStats` populated with `("datalake", "tpch", "lineitem") → stats`
- **WHEN** `get(&TableReference::Full { catalog, schema, table })` is called for the same path
- **THEN** it returns `Some(&stats)`

#### Scenario: Missing entry returns None

- **GIVEN** an empty `CatalogStats`
- **WHEN** `get(&any_reference)` is called
- **THEN** it returns `None`
- **AND** the cost model treats the table size as `default_table_size`
