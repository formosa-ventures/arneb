# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**trino-alt** — A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

**Status**: Phase 1 (single-node) and Phase 2 (distribution) complete. 18 OpenSpec changes implemented. TPC-H 16/22 queries passing.

## Build & Development Commands

```bash
# Build
cargo build
cargo build --release

# Run tests
cargo test
cargo test -- --nocapture                # with stdout
cargo test <test_name>                   # single test

# Lint & format
cargo fmt -- --check                     # check formatting
cargo fmt                                # auto-format
cargo clippy -- -D warnings              # lint with warnings as errors

# Run the server (standalone — single process, default)
cargo run --bin trino-alt
cargo run --bin trino-alt -- --config path/to/config.toml

# Run as coordinator + worker (distributed mode)
cargo run --bin trino-alt -- --config trino-alt.toml --port 5432 --role coordinator
cargo run --bin trino-alt -- --config worker.toml --role worker

# Run TPC-H benchmark
cd benchmarks/tpch && cargo run --release -- --engine trino-alt --port 5432
```

### Server Configuration

The server loads config from `trino-alt.toml` (if present), env vars, and CLI args. Precedence: CLI > env > file > defaults.

```toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"
```

**Ports**:
- Coordinator/Standalone: pgwire (configured port), Web UI (port + 1000), Flight RPC (9090)
- Worker: Flight RPC only (no pgwire, no Web UI)

**Roles**:
- `standalone` (default) — single process, all-in-one
- `coordinator` — accepts SQL, plans queries, dispatches tasks to workers via Flight RPC
- `worker` — receives tasks from coordinator, executes plan fragments, serves data via Flight RPC

**Worker config** (no `port` needed — worker has no pgwire):
```toml
bind_address = "127.0.0.1"

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[cluster]
rpc_port = 9091
coordinator_address = "127.0.0.1:9090"
worker_id = "worker-1"
```

Connect with any PostgreSQL client: `psql -h 127.0.0.1 -p 5432`

## Architecture

### Core Crates (workspace layout)

```
crates/
├── common/        # Shared types (DataType, ScalarValue, ColumnInfo, TableReference),
│                  # error hierarchy (TrinoError), identifiers (QueryId, StageId, TaskId)
├── sql-parser/    # SQL → AST via sqlparser-rs. Supports SELECT, DDL/DML, CASE,
│                  # CTEs, set operations, window functions, subqueries
├── catalog/       # CatalogProvider/SchemaProvider/TableProvider traits,
│                  # in-memory impls, CatalogManager (3-part table resolution)
├── planner/       # AST → LogicalPlan, QueryPlanner, LogicalOptimizer,
│                  # PlanFragmenter for distributed execution
├── execution/     # Physical operators (scan, filter, project, join, aggregate,
│                  # sort, limit, semi-join, set ops, window, explain),
│                  # ScalarFunction trait + 19 built-in functions,
│                  # DataSource trait, ExecutionContext
├── connectors/    # ConnectorFactory/ConnectorRegistry/DDLProvider traits,
│                  # memory module (read + write), file module (CSV + Parquet)
├── protocol/      # PostgreSQL wire protocol v3 (Simple + Extended Query) via pgwire,
│                  # pg_catalog/information_schema metadata handler,
│                  # type encoding (Arrow → PG), error mapping, SET/SHOW handling
├── scheduler/     # QueryTracker (state machine), NodeRegistry (worker heartbeat),
│                  # ResourceGroupManager, NodeScheduler
├── rpc/           # Arrow Flight RPC server/client for distributed task execution,
│                  # heartbeat protocol, output buffer management
└── server/        # Main binary (trino-alt), CLI (clap), config loading,
                   # catalog/connector wiring, Web UI (axum + rust-embed),
                   # graceful shutdown, coordinator/worker startup
```

### Key Data Flow

```
SQL String
  → Parser (sql-parser) → AST (Statement)
  → Planner (planner) → LogicalPlan
  → Optimizer → Optimized LogicalPlan
  → Metadata interception (pg_catalog, information_schema, SET/SHOW)
  → ExecutionContext (execution) → PhysicalPlan (Arc<dyn ExecutionPlan>)
  → execute() → SendableRecordBatchStream (async)
  → Protocol (protocol) → PostgreSQL wire format response
```

### Key Dependencies

- **Apache Arrow** (`arrow` v54): Columnar memory format for all intermediate data.
- **sqlparser-rs** (`sqlparser` v0.61): SQL dialect parsing into AST.
- **tokio** (v1): Async runtime for the protocol server.
- **pgwire** (v0.25): PostgreSQL wire protocol v3 implementation.
- **axum** (v0.8): HTTP framework for Web UI.
- **rust-embed** (v8): Embeds frontend assets into binary.
- **clap** (v4): CLI argument parsing for the server binary.
- **tracing** / **tracing-subscriber**: Structured logging throughout.
- **thiserror** / **anyhow**: Error handling (thiserror for libraries, anyhow for the binary).

### Design Principles

- **Arrow-native**: All intermediate data in Arrow columnar format. No row-by-row processing.
- **Async streaming**: Operators return `SendableRecordBatchStream` for async execution.
- **Trait-based connectors**: `DataSource` trait abstracts all data access. Adding a new connector = implementing `ConnectorFactory` + `DataSource`.
- **Pushdown**: Filters, projections, and limits are pushed into connectors when supported.
- **PostgreSQL compatible**: Full Simple and Extended Query protocol. DBeaver, JDBC, psycopg2 all work out of the box.

## SQL Support

### Expressions
CASE WHEN, COALESCE, NULLIF, CAST, BETWEEN, IN, LIKE, IS NULL/NOT NULL, arithmetic, comparison, logical operators, subqueries (IN/EXISTS/scalar)

### Statements
SELECT, EXPLAIN, CREATE TABLE, DROP TABLE, CREATE TABLE AS SELECT, INSERT INTO, DELETE FROM, CREATE VIEW, DROP VIEW

### Advanced DQL
CTEs (WITH), UNION ALL/UNION/INTERSECT/EXCEPT, window functions (ROW_NUMBER, RANK, DENSE_RANK, SUM/AVG/COUNT/MIN/MAX OVER), GROUP BY with HAVING, ORDER BY on aggregates/aliases

### Scalar Functions (19)
String: UPPER, LOWER, SUBSTRING, TRIM, LTRIM, RTRIM, CONCAT, LENGTH, REPLACE, POSITION
Math: ABS, ROUND, CEIL, FLOOR, MOD, POWER
Date: EXTRACT, CURRENT_DATE, DATE_TRUNC

## Phase Roadmap

### Phase 1 (single-node) — complete
8 changes: common-foundation, sql-parsing, catalog-system, query-planning, execution-engine, connectors-mvp, pg-wire-protocol, server-integration

### Phase 2 (distribution + advanced SQL) — complete
10 changes: async-streaming-execution, connector-pushdown, hash-join-operator, logical-plan-optimizer, plan-fragmentation, query-state-machine, flight-rpc-layer, coordinator-worker-split, distributed-operators, tpch-benchmark

### Phase 2.5 (SQL completeness + client compat) — complete
8 changes: ansi-expressions, scalar-functions, subquery-support, advanced-dql, ddl-dml-connector-delegated, coordinator-web-ui, extended-query-protocol, pg-catalog-metadata

### OpenSpec Structure

```
openspec/
├── config.yaml              # OpenSpec configuration
├── specs/                   # Consolidated capability specs
└── changes/                 # Change artifacts (proposal, design, specs, tasks)
    ├── archive/             # Phase 1 archived changes
    └── <name>/              # Active/completed changes
```

## Conventions

- Use `thiserror` for library error types, `anyhow` only in the server binary.
- Prefer `Arc<dyn Trait>` for polymorphic plan nodes and operators.
- All public APIs get doc comments. Internal functions don't need them.
- Tests live in `#[cfg(test)] mod tests` within source files for unit tests; `tests/` directory for integration tests.
- Use `tracing` (not `log`) for instrumentation.
- Config: `serde` + `toml` for deserialization, `TRINO_*` env vars for overrides.
- Metadata queries (pg_catalog, information_schema) are intercepted in the protocol layer before the SQL parser.
- Quoted identifiers are stripped during AST conversion (use `Ident.value`, not `to_string()`).
