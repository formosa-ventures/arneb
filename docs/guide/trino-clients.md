# Trino Client Compatibility

Arneb speaks the [Trino client REST protocol (v1)](https://trino.io/docs/current/develop/client-protocol.html)
in addition to the PostgreSQL wire protocol. Tools that already talk to Trino
can point at an Arneb coordinator (or standalone node) without code changes:

- the `trino` CLI
- the Trino JDBC driver (DBeaver, DataGrip, Metabase's Trino driver, …)
- [`trino-python-client`](https://github.com/trinodb/trino-python-client), including its SQLAlchemy dialect (Superset)
- dbt-trino

Verified end to end with `trino-python-client` 0.340.0 (DB-API and the
SQLAlchemy dialect); the protocol test suite drives the same HTTP exchange a
client performs.

Both protocols share one engine: a statement sent over HTTP is parsed, planned,
optimized and executed by the same pipeline as one sent over pgwire, including
distributed execution on a coordinator with workers.

## Enabling and ports

The Trino listener is **on by default** for the `standalone` and `coordinator`
roles, on port **8080** (Trino's default, so clients need no extra settings).
Workers never serve it.

| Setting | Config file | Env var | CLI |
|---|---|---|---|
| Enable/disable | `[trino] enabled = true` | `ARNEB_TRINO_ENABLED=false` | `--no-trino` |
| Port | `[trino] port = 8080` | `ARNEB_TRINO_PORT=8081` | `--trino-port 8081` |

It binds the same `bind_address` as pgwire. Precedence is CLI > env > file >
default, and the server logs the effective values at startup (target
`arneb::config`).

```toml
[trino]
enabled = true
port = 8080
```

If the port is already taken — for example by the Trino container in this
repository's `docker-compose.yml`, which also publishes 8080 — Arneb logs an
error and keeps serving pgwire and the Web UI without the Trino listener. Pick
another port with `--trino-port` in that case.

## Connecting

```bash
# trino CLI
trino --server http://127.0.0.1:8080 --catalog datalake --schema tpch

# Python
python -c "
import trino
conn = trino.dbapi.connect(host='127.0.0.1', port=8080, user='me',
                           catalog='datalake', schema='tpch')
cur = conn.cursor()
cur.execute('SELECT count(*) FROM nation')
print(cur.fetchall())
"

# SQLAlchemy / Superset
#   trino://me@127.0.0.1:8080/datalake/tpch

# JDBC
#   jdbc:trino://127.0.0.1:8080/datalake/tpch
```

Catalog names are the ones Arneb registers: `memory`, `file` (tables from
`[[tables]]`), and each `[[catalogs]]` entry (e.g. `datalake`). A virtual
`system` catalog provides `system.jdbc.*` and `system.metadata.*` for drivers.

## Supported protocol surface

### Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/statement` | Submit SQL (request body). Returns the first `QueryResults` (state `QUEUED`) with a `nextUri`. |
| `GET /v1/statement/executing/{id}/{slug}/{token}` | Follow `nextUri`: long-polls up to 1 s while the query runs, then pages through results. Re-requesting the current token returns the same page; an older token returns `410 Gone`. |
| `DELETE /v1/statement/executing/{id}/{slug}/{token}` | Cancel the query (`204`). |
| `GET /v1/query/{id}` (`infoUri`), `DELETE /v1/query/{id}` | Basic query info / kill. |
| `GET /v1/query` | List queries the Trino listener currently holds. |
| `GET /v1/info`, `GET /v1/info/state` | Health checks (`starting: false`, `"ACTIVE"`). The reported version is `476-arneb-<version>`: the leading number is the Trino release whose client protocol Arneb mirrors, so clients that parse a numeric major version keep working. |

Result pages are bounded (about 1 MiB of rows, at most 10 000 rows per page).
Queries that the client stops polling are canceled after 5 minutes; finished
queries are kept for 2 minutes after the last request so a retried request for
the final page still succeeds. Trino queries appear in the Web UI query list.

### Request headers

| Header | Behavior |
|---|---|
| `X-Trino-User` | Recorded with the query (no authentication, see below). |
| `X-Trino-Catalog`, `X-Trino-Schema` | Session defaults for unqualified table names. |
| `X-Trino-Source`, `X-Trino-Client-Info` | Recorded, shown in `/v1/query/{id}`. |
| `X-Trino-Session` | Accepted and listed by `SHOW SESSION`; properties are not interpreted. |
| `X-Trino-Prepared-Statement` | Prepared statements the client holds, used by `EXECUTE`. |

Legacy `X-Presto-*` headers are accepted too; responses to such clients use
`X-Presto-*` response headers.

### Session statements

These are answered by the protocol layer and update the client session through
response headers, as in Trino:

| Statement | Response header |
|---|---|
| `USE catalog.schema` / `USE schema` | `X-Trino-Set-Catalog`, `X-Trino-Set-Schema` |
| `SET SESSION name = value` | `X-Trino-Set-Session` |
| `RESET SESSION name` | `X-Trino-Clear-Session` |
| `PREPARE name FROM sql` | `X-Trino-Added-Prepare` |
| `DEALLOCATE PREPARE name` | `X-Trino-Deallocated-Prepare` |
| `EXECUTE name USING …`, `EXECUTE IMMEDIATE '…' USING …` | — (runs the statement with `?` bound to the given literals) |
| `START TRANSACTION`, `COMMIT`, `ROLLBACK`, `SET TIME ZONE`, `SET ROLE`, `SET PATH` | Accepted as no-ops. |

`trino-python-client` sends parameterized queries as `EXECUTE IMMEDIATE`, and
the JDBC driver uses `PREPARE`/`EXECUTE`; both forms are supported.

### Metadata

| Statement / table | Notes |
|---|---|
| `SHOW CATALOGS [LIKE …]` | Registered catalogs plus `system`. |
| `SHOW SCHEMAS [FROM catalog] [LIKE …]` | Includes `information_schema`. |
| `SHOW TABLES [FROM [catalog.]schema] [LIKE …]` | |
| `SHOW COLUMNS FROM t`, `DESCRIBE t` | Columns `Column`, `Type`, `Extra`, `Comment`. |
| `SHOW SESSION` | Echoes the `X-Trino-Session` properties. |
| `<catalog>.information_schema.{schemata,tables,columns,views}` | Trino column layout; `data_type` holds Trino type names. |
| `system.jdbc.{catalogs,schemas,tables,columns,table_types,types}` | Layout the Trino JDBC `DatabaseMetaData` queries expect. |
| `system.metadata.table_comments` | Used by the SQLAlchemy dialect; comments are `NULL`. |

Queries over these tables run through the regular engine against a snapshot of
the catalog taken when the query starts, so `WHERE`, `ORDER BY`, `LIMIT`,
functions and so on work normally. Each such query lists the catalogs it
references; `columns` tables also load every table's schema, which can be slow
on a very large Hive Metastore.

### Type encoding

| Arneb / Arrow type | Trino type | JSON value |
|---|---|---|
| Int8 / Int16 / Int32 / Int64 | `tinyint` / `smallint` / `integer` / `bigint` | number |
| Float32 / Float64 | `real` / `double` | number; `"NaN"`, `"Infinity"`, `"-Infinity"` |
| Decimal(p, s) | `decimal(p,s)` | string, e.g. `"123.45"` |
| Utf8 | `varchar` | string |
| Binary | `varbinary` | base64 string |
| Boolean | `boolean` | `true` / `false` |
| Date32 | `date` | `"YYYY-MM-DD"` |
| Timestamp (s/ms/µs/ns) | `timestamp(0/3/6/9)` | `"YYYY-MM-DD HH:MM:SS[.fff]"` |
| Timestamp with time zone | `timestamp(p) with time zone` | `"YYYY-MM-DD HH:MM:SS[.fff] UTC"` |
| Time | `time(p)` | `"HH:MM:SS[.fff]"` |
| List / Map / Struct | `array(T)` / `map(K,V)` / `row(…)` | JSON array / object / array |
| NULL | — | `null` |

Every column carries both the `type` string and a `typeSignature`
(`rawType` + `arguments`) for JDBC.

### Errors

Failures come back as a Trino `QueryError` (`message`, `errorCode`,
`errorName`, `errorType`, `failureInfo`), with the query in state `FAILED`.
Common classifications: `SYNTAX_ERROR`, `TABLE_NOT_FOUND`, `COLUMN_NOT_FOUND`,
`CATALOG_NOT_FOUND`, `SCHEMA_NOT_FOUND`, `FUNCTION_NOT_FOUND`, `NOT_SUPPORTED`,
`USER_CANCELED`, `EXCEEDED_LOCAL_MEMORY_LIMIT`, `GENERIC_INTERNAL_ERROR`.

## Known limitations

- **No authentication or TLS.** `X-Trino-User` is trusted as-is and the
  listener speaks plain HTTP. Bind it to a trusted network, or put it behind a
  TLS-terminating proxy that authenticates users (the server honors
  `X-Forwarded-Proto` / `X-Forwarded-Host` when building `nextUri`).
- **Results are materialized before the first page.** The query runs to
  completion, then pages are served from memory, as with pgwire. There is no
  streaming of the first rows while the query is still running.
- **Session properties are not interpreted.** `SET SESSION` round-trips to the
  client correctly but does not change engine behavior.
- **`DESCRIBE INPUT` / `DESCRIBE OUTPUT`** are not supported, so JDBC
  `PreparedStatement.getMetaData()` before execution fails.
- **Transactions** are accepted as no-ops; every statement autocommits.
- **Progress stats** in `stats` are minimal (state, elapsed time, rows
  returned); split counts and CPU time are reported as `0`.
- **`LIKE … ESCAPE`** in metadata queries ignores the escape character.
- Cancelling a distributed query drops the coordinator side immediately;
  worker tasks that are already running may continue until they finish.
- On a coordinator, a distributed query appears twice in the Web UI: once as
  the submitted SQL (Trino listener) and once as the plan the coordinator
  schedules.
