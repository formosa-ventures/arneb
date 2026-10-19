# PostgreSQL

Arneb mounts a PostgreSQL database as a **read-only** catalog, so one SQL
statement can join PostgreSQL tables with Hive, Iceberg or file tables:

```sql
SELECT n.n_name, count(*) AS customers
FROM datalake.tpch.customer c
JOIN pg.tpch.nation n ON c.c_nationkey = n.n_nationkey
WHERE n.n_regionkey = 3
GROUP BY n.n_name;
```

Filters, projections and limits are pushed into PostgreSQL, and large tables are
read in parallel as primary-key ranges, across workers in distributed mode.

## Configuration

```toml
[[catalogs]]
name = "pg"
type = "postgresql"
connection_url = "postgresql://db.internal:5432/sales"
user = "arneb"
password = "..."            # or ARNEB_CATALOG_PG_PASSWORD
# default_schema = "public"
# max_connections = 8
# split_min_rows = 500000
# max_splits = 16
# unconstrained_numeric_scale = 10
# connect_timeout_secs = 10
# pool_wait_timeout_secs = 60
```

| Field | Default | Description |
|---|---|---|
| `connection_url` | required | `postgresql://host:port/database`. It may carry the user and password, but the separate fields are preferred. |
| `user`, `password` | from the URL | Credentials. The password is never logged. |
| `default_schema` | `public` | The schema used for unqualified names when this is the default (first) catalog. |
| `max_connections` | `8` | Connection pool size, per catalog **per node**. |
| `split_min_rows` | `500000` | A table is split once its estimated row count reaches this value. |
| `max_splits` | `16` | The maximum number of key-range splits per table. |
| `unconstrained_numeric_scale` | `10` | The scale used for `numeric` columns that have no declared precision. |
| `connect_timeout_secs` | `10` | How long opening one connection may take before it fails. A `connect_timeout` in `connection_url` takes precedence for the TCP connect. |
| `pool_wait_timeout_secs` | `60` | How long a scan or a metadata lookup waits for a free connection when all `max_connections` are busy, before the query fails. |

`ARNEB_CATALOG_<NAME>_PASSWORD` overrides `password`. `<NAME>` is the catalog
name in upper case, with every non-alphanumeric character replaced by `_`. For
example, the catalog `pg-prod` reads `ARNEB_CATALOG_PG_PROD_PASSWORD`.

The catalog is registered on the coordinator **and on every worker**, so every
node needs the same `[[catalogs]]` entry and network access to the database. If
the database cannot be reached at startup, Arneb logs a warning, skips the
catalog and keeps serving the rest. An unreachable Hive Metastore is handled the
same way. Each mounted catalog logs its effective settings, without the
password, at startup.

## Tables and types

The catalog lists every schema except `pg_catalog`, `information_schema` and the
`pg_toast` / `pg_temp` schemas. Ordinary tables, views, materialized views,
foreign tables and partitioned tables all appear. An exact name match is tried
first, then a unique case-insensitive one, so `pg.public.mixedcase` finds
`"MixedCase"`.

| PostgreSQL | Arneb |
|---|---|
| `boolean` | BOOLEAN |
| `smallint` / `integer` / `bigint` | SMALLINT / INTEGER / BIGINT |
| `real` / `double precision` | REAL / DOUBLE |
| `numeric(p,s)`, p ≤ 38 | DECIMAL(p,s) |
| `numeric` (no precision) | DECIMAL(38, `unconstrained_numeric_scale`) |
| `char(n)` / `varchar(n)` / `text` | VARCHAR (`char(n)` keeps its padding) |
| `date` | DATE |
| `timestamp` | TIMESTAMP(6) |
| `timestamptz` | TIMESTAMP(6) WITH TIME ZONE, in UTC |
| `uuid` | VARCHAR, canonical lower-case form |
| `bytea` | VARBINARY |

Values are never rounded or truncated silently. The query fails with an error
naming the column when a value does not fit its mapped type: a `numeric` with
more fractional digits than the scale, `NaN` or `±Infinity` in `numeric`, or
`±infinity` dates and timestamps.

A column of any other type, such as `jsonb`, arrays, enums or domains, still
appears in the table schema. A query that **reads** it fails with
`column 'x' has unsupported PostgreSQL type 'jsonb'`. A query that selects only
other columns works.

## Pushdown

Each scan partition runs one `COPY (SELECT …) TO STDOUT (FORMAT binary)`, and
the result is decoded straight into Arrow.

- **Projection**: only the referenced columns are selected.
- **Filters**: every `AND` conjunct that PostgreSQL is guaranteed to evaluate
  exactly as Arneb does is added to the remote `WHERE`. That covers comparisons
  between a column and a literal (or two columns), `IN`, `BETWEEN`, `LIKE`,
  `IS [NOT] NULL`, and `AND` / `OR` / `NOT` over those. Lossless widening casts
  that the analyzer adds, such as `integer` → `bigint`, are looked through.
- **Kept in Arneb**: function calls and arithmetic, comparisons with a NULL
  literal, range comparisons on text (PostgreSQL collation order can differ
  from Arneb's), anything on `real` / `double precision` columns (PostgreSQL
  sorts `NaN` above every number), anything on `char(n)` columns
  (PostgreSQL ignores their trailing spaces), and anything on `uuid` columns
  (PostgreSQL compares them as uuids, not as the text Arneb sees). Such a conjunct is evaluated by
  Arneb after the scan. It costs transfer, never correctness.
- **Limit**: `SELECT … FROM pg.t LIMIT n` sends `LIMIT n`, but only when
  every filter was pushed.

To see the SQL each partition sends, set `RUST_LOG=arneb::rdbms=debug`.

## Parallel reads

On the coordinator, while planning, Arneb reads the row estimate
(`pg_class.reltuples`) and primary key of each referenced table. A table qualifies
for splitting when it has a single-column `smallint` / `integer` / `bigint`
primary key and at least `split_min_rows` estimated rows. Such a table is cut into
`min(max_splits, ceil(rows / split_min_rows))` equal-width key ranges. The first
and last ranges are open-ended, so rows inserted after planning are still read
exactly once. The ranges travel with the plan, so every worker reads the same
ones.

A table that was never analyzed reports no row estimate and is read as a single
partition. Run `ANALYZE` to enable splitting. Skewed keys are read correctly,
but the partitions are uneven.

## Limitations

- **Read-only**: `INSERT`, `DELETE` and DDL against a PostgreSQL catalog fail,
  and nothing is sent to the database.
- **No TLS to PostgreSQL yet**: connections use plaintext. Use a trusted network
  or an SSH/TLS tunnel until `sslmode` support lands.
- No aggregate, TopN or join pushdown yet. Joins between two PostgreSQL tables
  run in Arneb.
- Metadata is read on each query that references a table, with no cache.

## Local environment

`docker compose up -d postgres` starts PostgreSQL 17 on `127.0.0.1:5433`,
seeded from `docker/postgres/init` with TPC-H `nation`/`region` in schema `tpch`
and type-coverage and split fixtures in schema `arneb_test`. The bundled
Trino has a `postgresql` catalog over the same database for comparisons.
After adding the catalog file, restart Trino once so it picks it up.

```toml
[[catalogs]]
name = "pg"
type = "postgresql"
connection_url = "postgresql://127.0.0.1:5433/arneb"
user = "arneb"
password = "arneb"
```

The connector's integration tests run against this database:

```bash
ARNEB_TEST_POSTGRES_URL=postgresql://arneb:arneb@127.0.0.1:5433/arneb \
  cargo test -p arneb-postgresql --test live
```
