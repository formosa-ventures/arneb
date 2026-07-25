# tpch-benchmark-engines Specification

## Purpose
Define the engine-adapter contract that lets the TPC-H harness execute the same SQL
against structurally different query engines — Arneb over the PostgreSQL wire
protocol, Trino over its REST statement API, and Apache DataFusion in-process —
without engine-specific branching leaking into the runner, the statistics layer,
or the report. Adapters normalize away transport and type-system differences so
every engine returns comparable rows and a comparable wall-clock duration.

## Requirements

### Requirement: Engine adapter contract

The harness SHALL define a single async engine-adapter contract that abstracts how a SQL string is executed against a query engine. Every supported engine MUST implement this contract; the runner MUST consume engines only through the contract and MUST NOT contain engine-specific branching outside the adapter implementations.

The contract MUST expose, at minimum:
- a stable engine name (used as the `engine` field in result JSON and in report headers),
- an async `connect` step that prepares any state needed for queries (TCP connection, in-process session context, etc.),
- an async `execute(sql)` operation that returns the result rows in a canonicalized form suitable for hashing, plus the wall-clock duration of the call,
- a typed error path that distinguishes connection failures from query failures.

#### Scenario: A new engine is added by implementing only the contract

- **WHEN** a contributor implements the adapter contract for a new engine
- **THEN** the runner, statistics, correctness check, and report all work for that engine without further code changes outside the adapter module

#### Scenario: A query failure does not abort the whole run

- **WHEN** an adapter's `execute` returns a query error for one query
- **THEN** the runner records that query as failed with the error message and continues to the next query for the same engine

### Requirement: Arneb adapter

The harness SHALL provide an Arneb adapter that connects to a running Arneb server over the PostgreSQL wire protocol and executes each query as a simple statement. The adapter MUST collect every returned row and report the row count truthfully (i.e., not capped or paginated away).

#### Scenario: Arneb adapter executes a TPC-H query

- **WHEN** the runner invokes the Arneb adapter with the SQL of `q01`
- **THEN** the adapter returns a non-empty row set and a wall-clock duration measured from just before send to just after the last row is read

#### Scenario: Arneb is unreachable

- **WHEN** the Arneb host is not listening on the configured port
- **THEN** `connect` returns a connection error and the runner records every query for that engine as failed with that connection error, without trying to execute any SQL

### Requirement: Trino adapter

The harness SHALL provide a Trino adapter that submits queries to Trino's `/v1/statement` endpoint, follows the `nextUri` pagination chain to completion, accumulates all rows, and uses `X-Trino-User`, `X-Trino-Catalog`, and `X-Trino-Schema` headers configured by CLI flags.

#### Scenario: Trino adapter follows pagination to the end

- **WHEN** Trino returns a result split across multiple `nextUri` pages
- **THEN** the adapter walks every page until `nextUri` is absent and returns the union of all `data` rows

### Requirement: DataFusion adapter

The harness SHALL provide a DataFusion adapter that runs in-process using the `datafusion` crate. The adapter MUST register all eight TPC-H tables (lineitem, orders, customer, part, partsupp, supplier, nation, region) as Parquet listing tables backed by the same MinIO bucket Arneb and Trino read from, so that all three engines see identical Parquet bytes.

The adapter MUST configure DataFusion's S3 object store with the MinIO endpoint, region, and credentials supplied by CLI flags or environment variables, and MUST set `allow_http=true` for the local-stack case.

#### Scenario: DataFusion reads the same Parquet as Arneb and Trino

- **WHEN** the harness runs `q06` (a single-table aggregate over `lineitem`) on DataFusion and on Arneb back-to-back
- **THEN** both engines return result sets whose canonical hash matches (per the runner's correctness rules)

#### Scenario: DataFusion cannot reach MinIO

- **WHEN** the configured S3 endpoint is unreachable
- **THEN** the adapter's `connect` returns a clear error naming the endpoint, and no DataFusion queries are attempted

### Requirement: Engine selection from the CLI

The runner SHALL accept an `--engines` flag whose value is a comma-separated subset of `{arneb, trino, datafusion}`. The legacy `--engine <name>` flag MUST continue to work as a single-engine alias. With no engine flag specified, the harness MUST run all three engines.

#### Scenario: Default invocation runs all three engines

- **WHEN** the user runs `tpch-bench` with no `--engine` or `--engines` flag
- **THEN** the harness executes the query suite against Arneb, Trino, and DataFusion in turn

#### Scenario: Legacy flag still selects a single engine

- **WHEN** the user passes `--engine arneb`
- **THEN** the harness executes only the Arneb adapter, and the resulting JSON's `engine` field is `arneb`
