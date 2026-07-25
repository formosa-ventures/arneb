# Changelog

All notable changes to Arneb are documented here. Change-by-change detail lives
in `openspec/changes/archive/`.

## 1.0.0

First official release. Arneb is a distributed SQL query engine in Rust that
speaks the PostgreSQL wire protocol and runs federated queries across files,
object stores, and Hive Metastore catalogs.

### Phase 1 — single-node engine

The foundations: shared types and error hierarchy, SQL parsing into an AST via
sqlparser-rs, a catalog system with three-part name resolution, logical planning
and optimization, and an Arrow-native execution engine whose operators stream
asynchronously rather than materializing intermediate results. Connectors arrived
for in-memory tables and CSV/Parquet files behind a `DataSource` trait, so adding
a source is a matter of implementing two traits. PostgreSQL wire protocol v3
support made the engine reachable from psql, DBeaver, JDBC, and psycopg2 from the
start.

### Phase 2 — distribution and query performance

Coordinator and worker roles, plan fragmentation, and Arrow Flight RPC for
inter-node data exchange, with a query state machine and node registry tracking
work across the cluster. On the execution side: hash joins, parallel aggregation,
repartitioning, exchange backpressure, and filter/projection/limit pushdown into
connectors — including Parquet row-group pruning from min/max statistics. The
TPC-H benchmark harness dates from this phase.

### Phase 2.5 — SQL surface and client compatibility

CTEs, set operations, window functions, subqueries in `IN`/`EXISTS`/scalar
position, `CASE`/`COALESCE`/`NULLIF`/`CAST`, 19 scalar functions, and DDL/DML
delegated to connectors. For clients: the Extended Query protocol (prepared
statements), synthetic `pg_catalog` and `information_schema` tables so schema
browsers work, and a web UI for query and cluster monitoring. Object store support
(S3/GCS/Azure) and a Hive Metastore catalog against HMS 4.x landed here, along
with source spans and a type-coercion analyzer for better diagnostics.

### Benchmarking

The TPC-H comparison harness runs Arneb, Trino, and Apache DataFusion over
identical Parquet in MinIO, with per-query statistics, cross-engine correctness
hashing, and a Markdown comparison report. Everything executes in containers —
including the harness itself, because its DataFusion adapter runs in-process and
would otherwise be the only engine measured natively. See
[`benchmarks/tpch/TUTORIAL.md`](benchmarks/tpch/TUTORIAL.md).

### Breaking changes

- **`benchmarks/tpch/scripts/report.py` is removed.** Use `tpch-bench report`,
  which accepts the same result JSONs, additionally handles DataFusion, and
  removes the harness's Python dependency. `run_benchmark.sh` calls it directly;
  scripts invoking `report.py` need updating.
- **`run_benchmark.sh --skip-trino` is replaced by `--engines=`**, which forwards
  to the runner's own engine selector — for example `--engines=arneb,datafusion`.

### Notes

- `SHOW server_version` reports `14.0`. That is the PostgreSQL compatibility level
  advertised to clients, not Arneb's version, and it does not track releases —
  drivers branch on it to pick catalog queries and protocol features. Arneb's own
  version comes from `SELECT version()`, `arneb --version`, and the web UI.
