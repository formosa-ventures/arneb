## ADDED Requirements

### Requirement: PostgreSQL TPC-H seed
The local environment SHALL provide a `pg-tpch-seed` compose profile. It SHALL
load `nation`, `region`, `customer`, `supplier`, `part` and `partsupp` into the
`postgres` service under the Hive seed's schema name plus a `_mixed` suffix
(`tpch_mixed`, or `tpch_decimal_mixed` when `TPCH_DECIMAL=1`; the postgres
fixture already owns `tpch`), at the scale factor given by `TPCH_SF` (default
`sf1`). It SHALL use the same column definitions as the Hive seed, read
from a script that both seeds share. It SHALL add each table's TPC-H primary key
and run `ANALYZE` on each table. Re-running the seed SHALL replace the tables.

#### Scenario: Seeded data matches the Hive seed
- **WHEN** both seeds run at the same `TPCH_SF` and `TPCH_DECIMAL`
- **THEN** for every dimension table, Trino reports the same row count and the same column names and types from `postgresql.<schema>_mixed` as from `hive.<schema>`

#### Scenario: Primary keys enable splits
- **WHEN** Arneb scans `postgresql.tpch.customer` at SF10 (single-column integer primary key, 1,500,000 rows; default `split_min_rows` 500,000)
- **THEN** the scan is read as more than one primary-key range split

### Requirement: Mixed-layout query set
`benchmarks/tpch/queries-mixed/` SHALL hold the 22 TPC-H queries with every
table reference qualified: dimensions as `postgresql.tpch_mixed.<table>`,
facts (`orders`, `lineitem`) as `hive.tpch.<table>`. A generator script SHALL
produce the set from `benchmarks/tpch/queries/`, and it SHALL fail if any of
the eight table names remains unqualified in a `FROM` or `JOIN` position.
`benchmarks/tpch/tpch-mixed.toml` SHALL mount the catalogs as `hive` and
`postgresql`, the same names as the bundled Trino's, so the same SQL text
runs on both engines.

#### Scenario: Generator is reproducible
- **WHEN** the generator runs on an unchanged `queries/` directory
- **THEN** `queries-mixed/` is unchanged

#### Scenario: Same SQL on both engines
- **WHEN** a mixed query is run on Trino with no default catalog or schema, and on Arneb started with `tpch-mixed.toml`
- **THEN** both engines execute it without a resolution error

### Requirement: Mixed-layout correctness gate
The correctness gate (`blast_radius_oracle.py`) SHALL accept `--layout mixed`,
which runs the mixed query set against Arneb and against Trino with no default
catalog, comparing the results with the gate's existing tolerance-aware
multiset diff. The gate SHALL support an Arneb running on the host. Phase 3 M2
F4 SHALL be accepted only when all 22 queries are cell-identical to Trino at
one scale factor of SF1 or larger, seeded identically in both stores, both in
standalone mode and in distributed mode (a coordinator and two workers).

#### Scenario: 22/22 standalone
- **WHEN** the gate runs `--layout mixed` against a standalone Arneb
- **THEN** every query reports cell-identical to Trino

#### Scenario: 22/22 distributed
- **WHEN** the gate runs `--layout mixed` against a coordinator with two workers
- **THEN** every query reports cell-identical to Trino

### Requirement: Pushdown evidence
The local `postgres` service SHALL log every statement with the client's
`application_name`. A benchmark helper SHALL capture, for each mixed query, the
statements PostgreSQL received from Arneb. It SHALL do so by bracketing the
query with marker statements, not by time window, and SHALL report each
statement's `WHERE` clause.

#### Scenario: Filters and projections are visible remotely
- **WHEN** mixed q05 runs on Arneb with pushdown evidence capture
- **THEN** the captured `region` statement contains `"r_name" = 'ASIA'` and selects only the referenced columns

#### Scenario: A dynamic filter is visible remotely
- **WHEN** a mixed query whose PostgreSQL scan is the probe side of a same-fragment hash join runs on Arneb
- **THEN** the captured statement for that scan contains the dynamic filter, as an `IN` list or as a compacted range

### Requirement: Mixed-layout SF10 report
The change SHALL record mixed-layout SF10 latency and peak memory for all 22
queries on Arneb and on Trino, labeled with the validation tier and the host. For
each query, the report SHALL state whether a dynamic filter reached
PostgreSQL. No latency threshold SHALL gate the change.

#### Scenario: Report is produced
- **WHEN** the SF10 mixed run completes
- **THEN** `openspec/changes/cross-source-join/measurements/` contains the latency/memory table and the per-query dynamic-filter column
