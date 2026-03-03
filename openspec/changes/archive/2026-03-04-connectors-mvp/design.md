## Context

trino-alt has a complete single-node query pipeline: SQL parsing → logical planning → physical execution. The execution engine defines a `DataSource` trait (`schema()` + `scan()`) and includes `InMemoryDataSource` for testing. The catalog system provides metadata-only traits (`CatalogProvider`, `SchemaProvider`, `TableProvider`). Currently there is no way to query data from files or dynamically register data-bearing tables — the gap between catalog metadata and execution data access must be bridged.

Project conventions: `Arc<dyn Trait>` for polymorphism, `thiserror` for errors, Arrow columnar format, trait-based extensibility.

## Goals / Non-Goals

**Goals:**

- Implement a `ConnectorFactory` trait and `ConnectorRegistry` for dynamic connector dispatch
- Implement an in-memory connector that integrates catalog metadata with data access (wrapping `InMemoryDataSource`)
- Implement CSV file reading via Arrow's CSV reader, producing `Vec<RecordBatch>`
- Implement Parquet file reading via the `parquet` crate's Arrow integration
- Provide catalog implementations (CatalogProvider/SchemaProvider/TableProvider) for each connector type
- Comprehensive unit tests for each connector

**Non-Goals:**

- No write/insert support (read-only connectors for MVP)
- No schema inference from CSV headers at query time (schema must be provided at registration)
- No predicate pushdown into file connectors (full scan only; optimizer handles filtering)
- No directory/glob scanning (single file per table for MVP)
- No remote file access (S3, HTTP) — local filesystem only; Phase 2 adds object stores
- No connection pooling or caching for file reads

## Decisions

### D1: Connector crate depends on execution, not the other way around

**Choice**: `crates/connectors` depends on `trino-execution` (for `DataSource` trait) and `trino-catalog` (for catalog traits). The execution crate does not depend on connectors.

**Rationale**: Connectors are leaf implementations — they implement traits defined by execution and catalog. This keeps the dependency graph clean: `common → catalog → execution ← connectors`. The server crate wires them together.

**Alternative**: Move `DataSource` into a shared crate. Rejected because `DataSource` is fundamentally an execution concept (produces RecordBatches).

### D2: ConnectorFactory trait — factory pattern for data source creation

**Choice**: Define `ConnectorFactory` trait with a method that creates `Arc<dyn DataSource>` from a table reference and options. A `ConnectorRegistry` maps string names to factories.

**Rationale**: The server needs to create DataSource instances at query time from catalog metadata. A factory pattern decouples connector creation from query execution. New connector types are added by registering a factory.

**Alternative**: Have `TableProvider` directly return a `DataSource`. Rejected because it would require catalog to depend on execution.

### D3: Unified MemoryTable — catalog metadata + data in one struct

**Choice**: The memory connector provides `MemoryTable` that implements both `TableProvider` (for catalog metadata) and stores `Vec<RecordBatch>` for data. A `MemoryConnectorFactory` creates `InMemoryDataSource` from these tables.

**Rationale**: For in-memory tables, metadata and data naturally live together. This simplifies the server integration — register a `MemoryTable` once and it works for both planning (catalog) and execution (connector).

### D4: CSV reader — Arrow's built-in CSV reader with explicit schema

**Choice**: Use `arrow::csv::ReaderBuilder` with an explicitly provided Arrow schema. No schema inference from headers for MVP.

**Rationale**: Schema inference adds complexity (type guessing, header parsing edge cases). For MVP, the user provides the schema when registering a CSV table. Arrow's CSV reader handles the actual parsing efficiently.

**Alternative**: Infer schema from CSV headers. Deferred — can be added as an enhancement without changing the trait interface.

### D5: Parquet reader — Arrow's native Parquet integration

**Choice**: Use the `parquet` crate's `ArrowReaderBuilder` to read Parquet files. Schema is derived from Parquet file metadata (self-describing format).

**Rationale**: Parquet files contain their own schema, so no explicit schema is needed. Arrow's Parquet integration produces RecordBatches directly with zero-copy where possible.

### D6: File connector catalog — schema-per-directory, table-per-file

**Choice**: A `FileCatalog` with a configurable root directory. Each registered file becomes a table. Schema is either provided explicitly or read from file metadata (Parquet).

**Rationale**: Simple mapping: one file = one table. Sufficient for MVP. Multi-file tables (partitioned datasets) are a Phase 2 feature.

## Risks / Trade-offs

**[Full file scan on every query]** → No caching of file contents; each `scan()` re-reads the file. **Mitigation**: Acceptable for MVP with small files. File caching can be added later without changing the DataSource trait.

**[No predicate pushdown to files]** → CSV and Parquet connectors read all rows, then the execution engine filters. **Mitigation**: Parquet's row group filtering could be added later. For MVP, full scan + in-engine filter is correct and simple.

**[Schema must be provided for CSV]** → Users must know their CSV schema upfront. **Mitigation**: A future enhancement can add `infer_schema()` that samples the file. Parquet doesn't have this limitation (self-describing).

**[Single file per table]** → No partitioned datasets or directory scanning. **Mitigation**: Sufficient for MVP demonstrations. Partitioned reading is a natural Phase 2 extension.
