# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**trino-alt** — A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

**Status**: MVP Phase 1 complete (single-node). All 8 crates implemented and working end-to-end.

## Build & Development Commands

```bash
# Build
cargo build
cargo build --release

# Run tests
cargo test
cargo test -- --nocapture                # with stdout
cargo test <test_name>                   # single test
cargo test -p <crate_name>              # single crate

# Lint & format
cargo fmt -- --check                     # check formatting
cargo fmt                                # auto-format
cargo clippy -- -D warnings              # lint with warnings as errors

# Run the server
cargo run --bin trino-alt
cargo run --bin trino-alt -- --config path/to/config.toml
cargo run --bin trino-alt -- --port 5433 --bind 0.0.0.0
```

### Server Configuration

The server loads config from `trino-alt.toml` (if present), env vars (`TRINO_BIND_ADDRESS`, `TRINO_PORT`, etc.), and CLI args. Precedence: CLI > env > file > defaults.

```toml
# trino-alt.toml
bind_address = "127.0.0.1"
port = 5432

[[tables]]
name = "lineitem"
path = "/data/lineitem.parquet"
format = "parquet"

[[tables]]
name = "orders"
path = "/data/orders.csv"
format = "csv"
schema = [
    { name = "id", type = "int32" },
    { name = "customer", type = "utf8" },
    { name = "total", type = "float64" },
]
```

Connect with any PostgreSQL client: `psql -h 127.0.0.1 -p 5432`

## Architecture

### MVP Scope

Phase 1 (single-node) — **complete**:
- SQL parsing and validation (via `sqlparser-rs`)
- Logical query planning
- Vectorized execution engine (Arrow-based)
- Connectors: in-memory, CSV/Parquet files
- Wire protocol: PostgreSQL wire protocol (via `pgwire`)
- Server binary with config-driven table registration

Phase 2 (distribution) — not started:
- Coordinator/Worker separation
- Distributed query planning and task scheduling
- Shuffle/exchange operators
- Query optimization (predicate pushdown, projection pruning)
- Connector: object stores (S3-compatible)

### Core Crates (workspace layout)

```
crates/
├── common/            # Shared types (DataType, ScalarValue, ColumnInfo, TableReference),
│                      # error hierarchy (TrinoError), ServerConfig
├── sql-parser/        # SQL parsing → AST via sqlparser-rs
├── catalog/           # CatalogProvider/SchemaProvider/TableProvider traits,
│                      # in-memory impls, CatalogManager (3-part table resolution)
├── planner/           # AST → LogicalPlan, QueryPlanner with CatalogManager
├── execution/         # Physical operators (scan, filter, project, join, aggregate,
│                      # sort, limit, explain), DataSource trait, ExecutionContext
├── connectors/        # ConnectorFactory/ConnectorRegistry traits,
│                      # memory module (MemoryTable/Schema/Catalog),
│                      # file module (CSV + Parquet via FileConnectorFactory)
├── protocol/          # PostgreSQL wire protocol v3 (Simple Query) via pgwire,
│                      # type encoding (Arrow → PG), error mapping (TrinoError → SQLSTATE)
└── server/            # Main binary (trino-alt), CLI (clap), config loading,
                       # catalog/connector wiring, graceful shutdown
```

### Key Data Flow

```
SQL String
  → Parser (sql-parser) → AST (Statement)
  → Planner (planner) → LogicalPlan
  → ExecutionContext (execution) → PhysicalPlan (Arc<dyn ExecutionPlan>)
  → execute() → Vec<RecordBatch>
  → Protocol (protocol) → PostgreSQL wire format response
```

Note: Execution is currently synchronous (`execute() → Result<Vec<RecordBatch>>`). The protocol layer bridges to async via `tokio::task::spawn_blocking`.

### Key Dependencies

- **Apache Arrow** (`arrow` v54): Columnar memory format for all intermediate data.
- **sqlparser-rs** (`sqlparser` v0.61): SQL dialect parsing into AST.
- **tokio** (v1): Async runtime for the protocol server.
- **pgwire** (v0.25): PostgreSQL wire protocol v3 implementation.
- **clap** (v4): CLI argument parsing for the server binary.
- **tracing** / **tracing-subscriber**: Structured logging throughout.
- **thiserror** / **anyhow**: Error handling (thiserror for libraries, anyhow for the binary).

### Design Principles

- **Arrow-native**: All intermediate data in Arrow columnar format. No row-by-row processing.
- **Synchronous execution**: Operators return `Vec<RecordBatch>` synchronously. Async streaming is a Phase 2 goal.
- **Trait-based connectors**: `DataSource` trait abstracts all data access. Adding a new connector = implementing `ConnectorFactory` + `DataSource`.
- **Pushdown-first**: Push filters, projections, and limits into connectors whenever possible (not yet implemented — Phase 2).

## MVP Phase 1 Roadmap

Eight changes implemented sequentially via OpenSpec. All complete and archived.

| # | Change | Crate | Description |
|---|--------|-------|-------------|
| 1 | `common-foundation` | `common` | Shared types (`DataType`, `ScalarValue`, `TableReference`, `ColumnInfo`), error hierarchy (`TrinoError`), `ServerConfig` |
| 2 | `sql-parsing` | `sql-parser` | SQL → AST via `sqlparser-rs`. Custom AST types (`Statement`, `Query`, `Expr`, `SelectItem`, `JoinType`, etc.) |
| 3 | `catalog-system` | `catalog` | Catalog traits (`CatalogProvider`, `SchemaProvider`, `TableProvider`), in-memory impls, `CatalogManager` |
| 4 | `query-planning` | `planner` | AST → `LogicalPlan` tree, `PlanExpr` (index-based column refs), `QueryPlanner` |
| 5 | `execution-engine` | `execution` | Physical operators, `DataSource` trait, expression evaluator, `ExecutionContext` |
| 6 | `connectors-mvp` | `connectors` | `ConnectorFactory`/`ConnectorRegistry`, memory connector, file connector (CSV + Parquet) |
| 7 | `pg-wire-protocol` | `protocol` | PostgreSQL wire protocol v3 (Simple Query), type encoding, error mapping |
| 8 | `server-integration` | `server` | Main binary, CLI, config loading, catalog/connector wiring, graceful shutdown |

### OpenSpec Structure

```
openspec/
├── config.yaml              # OpenSpec configuration
├── specs/                   # 24 consolidated capability specs (synced from all changes)
│   ├── common-data-types/   # DataType, ScalarValue, ColumnInfo specs
│   ├── sql-ast/             # AST node type specs
│   ├── catalog-traits/      # CatalogProvider/SchemaProvider/TableProvider specs
│   ├── execution-operators/ # Physical operator specs
│   ├── pg-encoding/         # Arrow → PostgreSQL type mapping specs
│   ├── server-startup/      # Server lifecycle specs
│   └── ...                  # (24 total)
└── changes/
    └── archive/             # 8 completed changes (proposal, design, specs, tasks each)
```

## Conventions

- Use `thiserror` for library error types, `anyhow` only in the server binary.
- Prefer `Arc<dyn Trait>` for polymorphic plan nodes and operators.
- All public APIs get doc comments. Internal functions don't need them.
- Tests live in `#[cfg(test)] mod tests` within source files for unit tests; `tests/` directory for integration tests.
- Use `tracing` (not `log`) for instrumentation.
- Config: `serde` + `toml` for deserialization, `TRINO_*` env vars for overrides.
