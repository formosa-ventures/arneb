# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Arneb** — A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

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
cargo run --bin arneb
cargo run --bin arneb -- --config path/to/config.toml

# Run as coordinator + worker (distributed mode)
cargo run --bin arneb -- --config arneb.toml --port 5432 --role coordinator
cargo run --bin arneb -- --config worker.toml --role worker

# Run TPC-H benchmark
cd benchmarks/tpch && cargo run --release -- --engine arneb --port 5432

# Local Hive + S3 environment (HMS 4.2.0 + MinIO + Trino via docker-compose)
docker compose up -d                                        # start HMS + MinIO + Trino
docker compose run --rm tpch-seed                           # seed TPC-H SF1 data
cargo run --bin arneb -- --config benchmarks/tpch/tpch-hive.toml  # start Arneb with hive catalog
psql -h 127.0.0.1 -p 5432 -c "SELECT COUNT(*) FROM datalake.tpch.nation;"
docker compose down                                         # tear down
```

### Server Configuration

The server loads config from `arneb.toml` (if present), env vars, and CLI args. Precedence: CLI > env > file > defaults.

```toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

# Remote tables (S3, GCS, Azure)
[[tables]]
name = "events"
path = "s3://my-bucket/data/events.parquet"
format = "parquet"

# Global storage configuration (optional)
[storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"  # MinIO/LocalStack; omit for real AWS
allow_http = true
# access_key_id and secret_access_key are optional — if omitted,
# AmazonS3Builder::from_env() picks up AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
# Precedence: config file > env var > IAM role / instance profile.
# access_key_id = "minioadmin"
# secret_access_key = "minioadmin"

# [storage.gcs]
# service_account_path = "/path/to/sa.json"

# Hive Metastore catalog — HMS 4.x supported via crates/hive-metastore
[[catalogs]]
name = "datalake"
type = "hive"
metastore_uri = "127.0.0.1:9083"   # host:port, no scheme
default_schema = "default"

# Per-catalog storage override (merges with global [storage])
[catalogs.storage.s3]
region = "us-east-1"
endpoint = "http://localhost:9000"
allow_http = true
```

See `benchmarks/tpch/tpch-hive.toml` for a Hive-backed benchmark config.

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
│                  # error hierarchy (ArnebError), identifiers (QueryId, StageId, TaskId)
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
│                  # memory + file (CSV/Parquet) connectors,
│                  # StorageRegistry (S3/GCS/Azure/local object store abstraction)
├── hive/          # Hive Metastore catalog provider + HiveDataSource,
│                  # HMS Thrift client wrapper (HMS 4.x via _req API),
│                  # HiveConnectorFactory wired through StorageRegistry
├── hive-metastore/# Auto-generated Thrift bindings from Hive 4.2.0 IDL via volo-build.
│                  # Rebuild with `cargo run -p hive-metastore-thrift-build` after
│                  # editing `thrift_idl/hive_metastore.thrift`.
├── protocol/      # PostgreSQL wire protocol v3 (Simple + Extended Query) via pgwire,
│                  # pg_catalog/information_schema metadata handler,
│                  # type encoding (Arrow → PG), error mapping, SET/SHOW handling
├── scheduler/     # QueryTracker (state machine), NodeRegistry (worker heartbeat),
│                  # ResourceGroupManager, NodeScheduler
├── rpc/           # Arrow Flight RPC server/client for distributed task execution,
│                  # heartbeat protocol, output buffer management
└── server/        # Main binary (arneb), CLI (clap), config loading,
                   # catalog/connector wiring, Web UI (axum + rust-embed),
                   # graceful shutdown, coordinator/worker startup.
                   # Hive/MinIO seeding handled by docker compose seed services.
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
- **object_store** (v0.11, `aws` feature): Unified S3/GCS/Azure/local filesystem abstraction.
- **parquet** (v58): Parquet file reader/writer. Supported compression codecs: Snappy, Gzip, Zstd, LZ4, Brotli, and uncompressed.
- **volo-thrift** (v0.10) / **pilota** (v0.11): Async Thrift runtime backing the `hive-metastore` crate. Note: the generated client uses `DefaultMakeCodec::buffered()` (plain TBinaryProtocol) to talk to real HMS servers, not the default TT-Header framing.

### Design Principles

- **Arrow-native**: All intermediate data in Arrow columnar format. No row-by-row processing.
- **Async streaming**: Operators return `SendableRecordBatchStream` for async execution.
- **Trait-based connectors**: `DataSource` trait abstracts all data access. Adding a new connector = implementing `ConnectorFactory` + `DataSource`.
- **Pushdown**: Filters, projections, and limits are pushed into connectors when supported. Parquet connectors support row group pruning (min/max statistics) and predicate pushdown (ArrowPredicate) for simple column comparisons.
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
- Config: `serde` + `toml` for deserialization, `ARNEB_*` env vars for overrides.
- Metadata queries (pg_catalog, information_schema) are intercepted in the protocol layer before the SQL parser.
- Quoted identifiers are stripped during AST conversion (use `Ident.value`, not `to_string()`).
