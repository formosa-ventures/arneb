## ADDED Requirements

### Requirement: TableStatistics struct

The system SHALL provide a `TableStatistics` struct in `crates/catalog/src/lib.rs` carrying optional per-table statistics used by the cost model and join reorderer.

```rust
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub columns: HashMap<String, ColumnStatistics>,
}
```

Every field SHALL be nullable. Connectors with no stats SHALL return `TableStatistics::default()` (all `None`). The cost model and selectivity estimator SHALL degrade to conservative defaults when any field is `None`.

#### Scenario: Default stats are all None

- **WHEN** `TableStatistics::default()` is constructed
- **THEN** `row_count`, `size_bytes` are `None` and `columns` is empty

#### Scenario: Stats are clonable and debug-printable

- **GIVEN** a populated `TableStatistics`
- **WHEN** the struct is cloned or formatted with `Debug`
- **THEN** the clone equals the original and the debug string contains all populated fields

### Requirement: ColumnStatistics struct

The system SHALL provide a `ColumnStatistics` struct carrying per-column distributional summaries.

```rust
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub ndv: Option<u64>,
    pub null_fraction: Option<f64>,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
}
```

`null_fraction` SHALL be in the closed interval `[0.0, 1.0]` when present. `min_value`/`max_value` SHALL be `ScalarValue` instances whose type matches the column's declared `DataType`.

#### Scenario: NDV may be approximate

- **WHEN** a connector populates `ndv = Some(N)` from HMS column stats or HyperLogLog
- **THEN** consumers (cost model, selectivity estimator) treat `N` as an estimate, not an exact distinct count

#### Scenario: Missing fields fall back to defaults

- **WHEN** a `ColumnStatistics` has `ndv = None`
- **THEN** the selectivity estimator uses the configured default (`1/10 = 0.1` for equality)

### Requirement: TableStatistics is Send + Sync

`TableStatistics` and `ColumnStatistics` SHALL be `Send + Sync` so that they can be shared across async tasks during planning.

#### Scenario: Stats traverse async boundaries

- **GIVEN** a `TableStatistics` cloned into an `Arc`
- **WHEN** the `Arc` is sent across `tokio::spawn`
- **THEN** it compiles and can be read on the spawned task
