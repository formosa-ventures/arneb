# Architecture Overview

Arneb is organized as a Rust workspace with 13 crates, each responsible for a distinct layer of the query engine.

## Crate Map

```
crates/
├── common/          Shared types, error hierarchy, identifiers
├── sql-parser/      SQL text → AST via sqlparser-rs
├── catalog/         Catalog/Schema/Table provider traits and in-memory impls
├── planner/         AST → LogicalPlan, optimizer, plan fragmenter
├── execution/       Physical operators, scalar functions, DataSource trait
├── connectors/      ConnectorFactory/Registry, file + object store connectors
├── hive/            Hive Metastore catalog provider and HiveDataSource
├── iceberg/         Read-only Iceberg reader (Hive catalogs redirect Iceberg tables here)
├── hive-metastore/  Auto-generated Thrift bindings for HMS 4.x
├── protocol/        PostgreSQL wire protocol (pgwire), pg_catalog, type encoding,
│                    Trino client REST protocol
├── scheduler/       Query state machine, node registry, resource groups
├── rpc/             Arrow Flight RPC server/client, heartbeat, output buffers
└── server/          Main binary, CLI, config loading, Web UI, shutdown
```

## Query Data Flow

```
SQL String
  → Parser (sql-parser)        → AST (Statement)
  → Planner (planner)          → LogicalPlan
  → Optimizer (planner)        → Optimized LogicalPlan
  → Metadata interception      → pg_catalog, information_schema, SET/SHOW
  → ExecutionContext (execution)→ PhysicalPlan (Arc<dyn ExecutionPlan>)
  → execute()                  → SendableRecordBatchStream (async)
  → Protocol (protocol)        → PostgreSQL wire or Trino JSON response
```

### Stage Details

1. **Parsing**: The `sql-parser` crate uses [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs) to parse SQL text into an AST. Supports SELECT, DDL/DML, CASE, CTEs, set operations, window functions, and subqueries.

2. **Planning**: The `planner` crate converts the AST into a `LogicalPlan` tree. The `QueryPlanner` resolves table references through the `CatalogManager` (3-part name resolution: `catalog.schema.table`).

3. **Optimization**: The `LogicalOptimizer` applies rule-based transformations to the logical plan (filter pushdown, projection pruning, etc.).

4. **Metadata Interception**: Queries against `pg_catalog` and `information_schema` are intercepted in the protocol layer and handled directly, before reaching the parser.

5. **Physical Planning**: The `ExecutionContext` converts the optimized `LogicalPlan` into a tree of physical operators (`Arc<dyn ExecutionPlan>`).

6. **Execution**: Physical operators return `SendableRecordBatchStream` — an async stream of Arrow `RecordBatch` values. Execution is pipelined; operators pull data from their children on demand.

7. **Wire Protocol**: The `protocol` crate encodes Arrow batches into PostgreSQL wire format (v3), or into Trino's JSON for clients on the Trino REST protocol, and sends them to the client.

### Distributed Execution

In distributed mode, additional stages are inserted:

- The `PlanFragmenter` splits the optimized logical plan into fragments suitable for distributed execution
- The `NodeScheduler` assigns fragments to available workers based on the `NodeRegistry`
- Workers execute fragments and return results via Arrow Flight RPC
- The coordinator assembles partial results into the final output

Fragments exchange data in a few shapes. A large fact-table scan is split
across all workers. A join whose build side fits under the broadcast cap
broadcasts it; otherwise both sides are hash-partitioned on the join key, and
semi/anti joins and final aggregates are partitioned the same way. When the
build side of a partitioned semi/anti join is much larger than its probe side,
each join task reads the probe side first and publishes its key set to the
build-side scans, which drop non-matching rows before they are shuffled. Each
of these is on by default and can be switched off for comparison; see
[Query Engine Levers](/guide/configuration#query-engine-levers).

## Design Principles

### Arrow-Native

All intermediate data is represented in Apache Arrow columnar format. This enables:
- Vectorized computation over column batches
- Zero-copy data sharing between operators
- Efficient serialization for distributed execution via Flight RPC

### Async Streaming

Operators return `SendableRecordBatchStream` for async, pipelined execution. No operator materializes the full result in memory before passing it downstream (except explicit materializations like sort and hash build).

### Trait-Based Connectors

All data access goes through the `DataSource` trait. Adding a new connector means implementing `ConnectorFactory` + `DataSource`. The engine doesn't know or care where data comes from.

### Pushdown

Filters, projections, and limits are pushed into connectors via the `ScanContext`. Connectors apply what they can; the engine applies the rest. Correctness never depends on pushdown.

### PostgreSQL Compatible

Arneb speaks the PostgreSQL wire protocol v3, including both Simple Query and Extended Query flows. This means standard clients (psql, DBeaver, JDBC, psycopg2) work without modification.

## Key Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| `arrow` / `parquet` | v60 | Apache Arrow columnar format, Parquet reader |
| `sqlparser` | v0.63 | SQL parsing |
| `tokio` | v1 | Async runtime |
| `pgwire` | v0.41 | PostgreSQL wire protocol |
| `axum` | v0.8 | HTTP framework (Web UI, Trino REST protocol) |
| `object_store` | v0.14 | S3/GCS/Azure abstraction |
| `volo-thrift` | v0.12 | Async Thrift runtime (HMS) |
| `clap` | v4 | CLI argument parsing |
| `tracing` | — | Structured logging |
| `thiserror` | v2 | Error types (library crates) |
