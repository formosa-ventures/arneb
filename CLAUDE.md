# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**trino-alt** — A Trino alternative built in Rust. Distributed SQL query engine for federated queries across heterogeneous data sources. MVP stage.

Trino (formerly PrestoSQL) lets users query data where it lives — across object stores, databases, and other systems — using standard SQL. This project aims to achieve similar goals with Rust's performance and safety guarantees.

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

# Run benchmarks
cargo bench
```

## Architecture

### MVP Scope

Phase 1 (single-node):
- SQL parsing and validation
- Logical query planning
- Basic query optimization (predicate pushdown, projection pruning)
- Vectorized execution engine (Arrow-based)
- Connectors: in-memory, CSV/Parquet files
- Wire protocol: PostgreSQL wire protocol (for client compatibility)

Phase 2 (distribution):
- Coordinator/Worker separation
- Distributed query planning and task scheduling
- Shuffle/exchange operators
- Connector: object stores (S3-compatible)

### Core Crates (workspace layout)

```
crates/
├── sql-parser/        # SQL parsing → AST (consider sqlparser-rs)
├── planner/           # AST → Logical Plan → Physical Plan
├── optimizer/         # Rule-based and cost-based optimizations
├── execution/         # Vectorized execution engine on Arrow RecordBatches
├── connectors/        # DataSource trait + connector implementations
│   ├── memory/
│   ├── file/          # CSV, Parquet
│   └── postgres/      # (future)
├── protocol/          # PostgreSQL wire protocol handler
├── catalog/           # Schema/table metadata management
├── common/            # Shared types, errors, config
└── server/            # Main binary, service orchestration
```

### Key Data Flow

```
SQL String
  → Parser (sql-parser) → AST
  → Planner (planner) → LogicalPlan
  → Optimizer (optimizer) → optimized LogicalPlan → PhysicalPlan
  → Execution (execution) → stream of Arrow RecordBatches
  → Protocol (protocol) → wire format response to client
```

### Key Dependencies

- **Apache Arrow / DataFusion**: Arrow for columnar memory format. Evaluate DataFusion for reusable components (expression evaluation, physical operators) rather than reimplementing everything.
- **sqlparser-rs**: SQL dialect parsing.
- **tokio**: Async runtime.
- **tonic/gRPC or custom**: Inter-node communication (Phase 2).
- **pgwire**: PostgreSQL wire protocol compatibility.

### Design Principles

- **Arrow-native**: All intermediate data in Arrow columnar format. No row-by-row processing.
- **Streaming execution**: Operators produce `Stream<RecordBatch>`, never materialize full result sets in memory.
- **Trait-based connectors**: `DataSource` trait abstracts all data access. Adding a new connector = implementing the trait.
- **Pushdown-first**: Push filters, projections, and limits into connectors whenever possible.

## MVP Phase 1 Roadmap

Eight changes implemented sequentially via OpenSpec (`openspec/changes/`):

| # | Change | Crate | Status | Description |
|---|--------|-------|--------|-------------|
| 1 | `common-foundation` | `crates/common` | Done | Shared types (`DataType`, `ScalarValue`, `TableReference`, `ColumnInfo`), error hierarchy (`TrinoError`, `PlanError`, `CatalogError`, etc.), config types |
| 2 | `sql-parsing` | `crates/sql-parser` | Done | SQL string → AST via `sqlparser-rs`. Custom AST types (`Statement`, `Query`, `Expr`, `SelectItem`, `JoinType`, etc.) |
| 3 | `catalog-system` | `crates/catalog` | Done | Catalog traits (`CatalogProvider`, `SchemaProvider`, `TableProvider`), in-memory impls, `CatalogManager` with 3-part table resolution |
| 4 | `query-planning` | `crates/planner` | Done | AST → `LogicalPlan` tree. `PlanExpr` (index-based column refs), `QueryPlanner` using `CatalogManager` for table/column resolution |
| 5 | `execution-engine` | `crates/execution` | Done | `ExecutionPlan` operators (scan, filter, project, join, aggregate, sort, limit, explain), `DataSource` trait, expression evaluator, `ExecutionContext` physical planner |
| 6 | `connectors-mvp` | `crates/connectors` | Not Started | `DataSource` trait, in-memory connector, CSV/Parquet file connectors |
| 7 | `pg-wire-protocol` | `crates/protocol` | Not Started | PostgreSQL wire protocol handler for client compatibility |
| 8 | `server-integration` | `crates/server` | Not Started | Main binary, service orchestration, end-to-end query pipeline |

### Dependency Chain

```
common-foundation
  → sql-parsing
  → catalog-system
    → query-planning
      → execution-engine
        → connectors-mvp
          → pg-wire-protocol
            → server-integration
```

## Conventions

- Use `thiserror` for library error types, `anyhow` only in binaries/tests.
- Prefer `Arc<dyn Trait>` for polymorphic plan nodes and operators.
- All public APIs get doc comments. Internal functions don't need them.
- Tests live in `#[cfg(test)] mod tests` within source files for unit tests; `tests/` directory for integration tests.
- Use `tracing` (not `log`) for instrumentation.
