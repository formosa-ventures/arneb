## MODIFIED Requirements

### Requirement: TableProvider trait

The system SHALL define a `TableProvider` trait that exposes table schema metadata and optional statistics. It SHALL provide a `schema()` method returning a list of `ColumnInfo` describing all columns in the table, and a `statistics()` method returning `Option<TableStatistics>` describing row count, size, and per-column distributional summaries when available.

```rust
pub trait TableProvider: Send + Sync + Debug {
    fn schema(&self) -> Vec<ColumnInfo>;

    /// Per-table statistics for cost-based planning.
    /// Connectors that cannot provide stats SHALL return `None`.
    fn statistics(&self) -> Option<TableStatistics> { None }

    // ... existing methods unchanged
}
```

The default implementation of `statistics()` SHALL return `None`, so existing connectors compile unchanged and the planner falls back to conservative defaults.

#### Scenario: Getting table schema

- **WHEN** `table.schema()` is called on a table with columns (id: Int64, name: Utf8)
- **THEN** it returns a `Vec<ColumnInfo>` with two entries matching those column definitions

#### Scenario: TableProvider without stats returns None

- **WHEN** `table.statistics()` is called on a connector that has not overridden the method
- **THEN** it returns `None`
- **AND** the cost model treats the table's `row_count` as the configured `default_table_size`

#### Scenario: TableProvider with stats returns Some

- **GIVEN** an HMS-backed `TableProvider` whose underlying `Table.parameters.numRows = 6000000`
- **WHEN** `table.statistics()` is called
- **THEN** it returns `Some(TableStatistics { row_count: Some(6000000), .. })`
