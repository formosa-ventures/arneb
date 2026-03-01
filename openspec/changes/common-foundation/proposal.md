## Why

trino-alt is a Rust distributed SQL query engine built from scratch. All subsequent crates (sql-parser, planner, optimizer, execution, connectors, catalog, protocol, server) require a shared set of type definitions, error handling mechanisms, and configuration management. The `common` crate is the foundation layer of the entire project and must be established before any other development can proceed.

## What Changes

- Set up the Cargo workspace with `crates/common/` as the first crate
- Define a unified error type hierarchy (using `thiserror`) covering error categories for SQL parsing, planning, execution, connectors, and other stages
- Define shared data types: `DataType` (SQL type system), `TableReference` (catalog.schema.table identification), `ColumnInfo` (column metadata), `ScalarValue` (constant value representation)
- Build a configuration system supporting loading server settings from files and environment variables
- Integrate `tracing` as the unified logging and instrumentation framework

## Capabilities

### New Capabilities

- `error-types`: Unified error type hierarchy using `thiserror` to define composable error types for each module (ParseError, PlanError, ExecutionError, ConnectorError, etc.), with support for error chaining and context propagation
- `common-data-types`: Shared data type definitions including the SQL type system (DataType), table and column identification (TableReference, ColumnInfo), scalar values (ScalarValue), and corresponding Arrow type conversions
- `server-config`: Server configuration management supporting TOML config files and environment variable overrides, covering bind address, thread count, memory limits, and other parameters

### Modified Capabilities

(No existing capabilities; all newly created)

## Impact

- **New crate**: `crates/common/`
- **Cargo workspace**: New `Cargo.toml` workspace configuration at project root
- **Key dependencies**: `thiserror`, `serde`/`toml` (config), `tracing`, `arrow` (type mapping)
- **Affects all subsequent crates**: All other crates will `depend on common`; the API design here directly impacts the ergonomics of the entire project
