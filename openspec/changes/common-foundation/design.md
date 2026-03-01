## Context

trino-alt is currently an empty project with only CLAUDE.md defining the architectural blueprint. The `common` crate is the first crate to be built, and all subsequent 8+ crates will depend on it. This means the API design here must be stable and forward-looking — the cost of changes will amplify as more downstream crates are added.

Project conventions: use `thiserror` (not `anyhow`) for library errors, `tracing` (not `log`) for logging, Arrow as the data format.

## Goals / Non-Goals

**Goals:**

- Establish the Cargo workspace structure so subsequent crates can be added directly
- Provide unified error types that each crate can compose while preserving the full error chain
- Define bidirectional mapping between the SQL type system and Arrow types
- Provide shared types for table/column identification
- Build a configuration system that loads from TOML files + environment variables
- Comprehensive unit tests for every module

**Non-Goals:**

- No SQL parsing, query planning, or execution logic
- No distributed-related types (Phase 2 scope)
- No RPC/network communication types
- No logging/tracing subscriber setup (handled by the server binary)

## Decisions

### D1: Module structure — Single crate with multiple modules vs Multiple micro-crates

**Choice**: A single `trino-common` crate containing three modules: `error`, `types`, and `config`.

**Rationale**: These three modules are highly coupled (error types reference data types, config is used for global settings). Splitting them into multiple crates would increase workspace management overhead and risk circular dependencies with no real benefit.

**Alternative**: Split into `trino-error`, `trino-types`, `trino-config` as three independent crates. Rejected because it increases management complexity with no independent publishing requirements.

### D2: Error type design — Single enum vs Layered enums

**Choice**: Layered design. Each domain has its own error enum (`ParseError`, `PlanError`, `ExecutionError`, `ConnectorError`, `CatalogError`, `ConfigError`), composed by a top-level `TrinoError` enum via `#[from]`.

**Rationale**: Each crate only needs to depend on its own domain error type, without being forced to import unrelated error variants. The top-level `TrinoError` is used by the server binary for unified error handling.

**Alternative**: A single large `Error` enum containing all variants. Rejected because it violates Single Responsibility and would become bloated as the project grows.

### D3: DataType definition — Custom vs Using Arrow DataType directly

**Choice**: Custom `DataType` enum with `impl From<DataType> for arrow::datatypes::DataType` and reverse conversion.

**Rationale**: The SQL type system and Arrow type system have semantic differences (e.g., SQL's `VARCHAR(255)` vs Arrow's `Utf8`, SQL's `DECIMAL(p,s)` precision information). A custom type preserves SQL semantics while the conversion layer handles the mapping.

**Alternative**: Use `arrow::datatypes::DataType` directly. Rejected because it would lose SQL-specific type information (length limits, precision, etc.).

### D4: ScalarValue representation — Custom vs Using DataFusion's ScalarValue

**Choice**: Custom `ScalarValue` enum, with MVP supporting only basic types (Null, Boolean, Int32/64, Float32/64, Utf8, Binary, Decimal128, Date32, Timestamp).

**Rationale**: DataFusion's ScalarValue brings in too many dependencies and includes variants we don't need. A custom version stays lean and can be extended as needed.

**Alternative**: Depend on `datafusion-common::ScalarValue`. Rejected because it would pull in the entire DataFusion dependency tree, which is too heavy.

### D5: TableReference structure — Three-part identification

**Choice**: `TableReference` with `catalog: Option<String>`, `schema: Option<String>`, `table: String`. Supports `table`, `schema.table`, and `catalog.schema.table` formats.

**Rationale**: Consistent with Trino's naming conventions and supports multi-catalog federated query scenarios.

### D6: Configuration system — serde + toml vs Dedicated config library

**Choice**: Use `serde` + `toml` crate for deserialization, with custom environment variable override logic.

**Rationale**: The requirements are simple (one TOML file + env vars) and don't need the multi-layer merge capabilities of libraries like `config-rs`. Fewer dependencies.

**Alternative**: Use `config-rs`. Rejected because our requirements are simple enough that a direct implementation is more transparent.

### D7: Crate naming — `common` vs `trino-common`

**Choice**: Crate name is `trino-common` (in Cargo.toml's `[package] name`), directory remains `crates/common/`.

**Rationale**: Avoids conflicts with `common` on crates.io and clearly identifies project ownership. The directory uses the short name to keep paths concise.

## Risks / Trade-offs

**[Premature API freeze]** → Since all crates depend on common, modifying the public API has a large blast radius. **Mitigation**: Keep types lean during the MVP stage, only expose what is definitely needed. Use `#[non_exhaustive]` on enums that may be extended.

**[Arrow version coupling]** → The Arrow type mapping in `common` locks to a specific Arrow version; upgrades require synchronizing all crates. **Mitigation**: Manage the Arrow version centrally in the workspace `Cargo.toml`.

**[Pre-defined error types]** → Defining error types for each crate before actually implementing them may not match real-world needs. **Mitigation**: Define only the most basic variants first, and extend when each crate is implemented. Use `#[non_exhaustive]` to preserve extensibility.

**[Config over-engineering]** → The MVP stage may not need a full config system. **Mitigation**: Define only the most basic parameters (bind address, port), add more as needed. Provide reasonable default values.
