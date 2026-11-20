# G6 stage 2b-1: SQLAlchemy reflection over pgwire — design

Status: draft, 2026-11-21. Part of the production-ready v0.2.x gates in
`2026-10-16-phase3-roadmap-design.md` ("G6 stage 2b"). Stage 2b-2 (a
Metabase run) is a separate change and is out of scope here.

## Intent

A team replacing Trino points its BI and ELT tools at Arneb. Over the Trino
protocol, SQLAlchemy reflection already works. Over pgwire it does not:
the postgresql dialect cannot read a table's columns, so
`Table(..., autoload_with=engine)`, `Inspector.get_columns` and every tool
built on them (Superset's postgres path, pandas `read_sql_table`, Alembic
autogenerate, many notebooks) fail. This change makes the SQLAlchemy 2.1
postgresql dialect's reflection work end to end over pgwire, with both
psycopg2 and psycopg 3. It does so by adding the engine features that the
dialect's queries need, not by special-casing those queries.

### Why engine features, not shortcuts

The decision was "two steps, engine-level". A cheaper option was rejected:
extending the `pg_catalog` fallback so that these queries get a correctly
shaped empty or canned answer. It would leave arrays, aggregate `ORDER BY`
and correlated subqueries in `SELECT` missing for user queries too. It
would also keep the fallback, which #189 wants to retire, as a load-bearing
part of reflection.

## Evidence: what fails today

Measured on 2026-11-21 against `main` (20c04fc), standalone, TPC-H tables,
SQLAlchemy 2.1.6:

| Inspector call | psycopg2 / psycopg 3 | Root cause (first error) |
|---|---|---|
| `get_schema_names`, `get_table_names`, `get_table_comment` | OK | — |
| `get_columns`, `autoload_with` | FAIL | missing `pg_attribute.atthasdef` (and further: correlated scalar subqueries inside `CASE`, `json_build_object`, `pg_sequence`, `pg_collation`, `pg_type.typcollation`, runtime `regclass`) |
| `get_pk_constraint`, `get_unique_constraints` | FAIL | missing `pg_index`; `unnest`, `generate_subscripts`, `array_agg(... ORDER BY ...)`, `bool_and` |
| `get_foreign_keys`, `get_check_constraints` | FAIL | unknown function `pg_get_constraintdef` |
| `get_indexes` | FAIL | missing `pg_opclass` (and `pg_index`, two `unnest`s in one `SELECT`, `pg_get_indexdef`, array `= ANY`) |

**The probe** is `scripts/client-smoke/pg_reflect_probe.py`. It runs the ten
single-table calls above, plus `Table(..., autoload_with=engine)`, against
schema `default`, table `nation`. Those come from a standalone Arneb whose
config lists `nation`, `region` and `orders` as `[[tables]]` Parquet files
written by `scripts/k8s-smoke/gen_tpch.py <dir> 0.01`. That is the
`file`-connector layout of the client smoke cluster, not Hive.

**The captured SQL** from that run is in
`docs/superpowers/specs/2026-11-21-g6-stage-2b1/captured-fallback-sql.txt`.
It contains each failing statement once, with the error that sent it to the
fallback. It is the reference input for the Tier 1 tests.
- In SQLAlchemy 2.1, single-table calls go through the batched
  `get_multi_*` queries.
- Both drivers sent the names and oids as **inlined literals**
  (`relname IN ('nation')`, `conrelid IN (1778988401)`), not as array
  parameters. Bound array parameters are covered by the `= ANY($1)`
  goal on its own, not by the probe.
- `get_indexes` failed before reaching the fallback (`pg_opclass` is not a
  known table), so its SQL is not in the file. The source is
  `PGDialect._index_query` in SQLAlchemy 2.1.6
  `dialects/postgresql/base.py`. Re-capture it once `pg_opclass` exists.

Arneb has no primary keys, unique constraints, indexes, column defaults,
identity columns, sequences or non-default collations. So every query
except the column query reads only empty tables. The constraint queries
must still plan and run so that they return zero rows with the right
columns.

## Goals

1. All ten probe calls succeed with both drivers. That means
   `get_columns`, `get_pk_constraint`, `get_foreign_keys`, `get_indexes`,
   `get_unique_constraints`, `get_check_constraints`, `get_table_comment`,
   schemas, tables and `Table(..., autoload_with=engine)`. They succeed
   without any statement going through the `pg_catalog` fallback: the
   fallback counter stays at 0 for the whole run.
2. Correct results.
   - `get_columns` returns every column with the right name, type (from
     `format_type`), nullability and a `None` default.
   - A reflected `Table` can be queried with `GROUP BY`, `ORDER BY` and
     `LIMIT`.
   - The constraint and index calls return empty results.
3. An engine-level array minimum that user queries can use as well:
   - a `DataType::List` value end to end;
   - `ARRAY[...]` literals;
   - `array_agg`: `ORDER BY` supported; `DISTINCT`, `FILTER` and
     `WITHIN GROUP` refused with an error;
   - `unnest` and `generate_subscripts` in a `SELECT` list;
   - arrays in pgwire results;
   - `= ANY (<array>)` with a bind parameter.
4. Correlated scalar subqueries anywhere in a `SELECT` list expression,
   including `CASE` conditions and branches. They become joins. They are
   not a nested-loop evaluator.
5. The client smoke suite (`scripts/client-smoke/clients.py`) asserts the
   above, and `docs/guide/clients.md` drops the "reflecting columns is not
   supported" caveats.

## Non-goals

- Metabase (stage 2b-2).
- Real constraints, indexes, defaults or sequences: these tables stay
  empty.
- A JSON type. `json_build_object` returns text containing JSON, typed
  as `json` over pgwire if that is cheap, otherwise `text`.
- Array functions beyond those listed. Out of scope: `array_length`,
  `cardinality`, subscripts `a[i]`, `ANY(column)`, `unnest` in `FROM`,
  `LATERAL`, multi-dimensional arrays, and array columns read from
  Parquet/Hive/Iceberg/PostgreSQL connectors. Connector array columns
  may already surface as `List`; this change must not break them, but
  it does not test them beyond the pgwire encoding.
- Distributed partial aggregation for `array_agg`. It runs single-phase,
  as other non-decomposable aggregates do today.
- Retiring `metadata::fixed_pg_catalog`. It stays as a safety net, and
  #189 tracks its removal.
- `ORDER BY` inside aggregates other than `array_agg`.

## Anti-goals (failure even if the tests pass)

- Matching the SQLAlchemy SQL text, or any client's query shape, in the
  protocol layer.
- An aggregate `ORDER BY` that is still silently dropped. Today
  `convert_function` ignores `arg_list.clauses`, `func.filter` and
  `func.within_group`. After this change, `ORDER BY` is accepted only in
  `array_agg`. `FILTER` and `WITHIN GROUP` are refused on every function,
  `array_agg` included. Every refusal comes with a clear error; nothing
  is ignored.
- A correlated subquery that silently returns one of several rows. More
  than one row is an error, as in PostgreSQL.
- Any regression in TPC-H/TPC-DS plans or results. This change only adds
  plan shapes. Queries that planned before must plan identically.

## Design

### 1. `pg_catalog` surface (crates/protocol/src/pg_catalog.rs)

Add these columns, always populated with PostgreSQL's value for "nothing
special":

- `pg_attribute`:
  - `atthasdef` false;
  - `attidentity` `''`;
  - `attgenerated` `''`;
  - `attcollation` 0. Use PostgreSQL's real collation oid for text columns
    only if that is easy; otherwise 0 everywhere, so that the collation
    branch is never taken.
- `pg_type`: `typcollation` 0.
- `pg_class`: `relam` 0, and `reloptions` as a `text[]` NULL.

Add these tables, all empty, with PostgreSQL's column names and types for
the columns the dialect reads:

- `pg_index`:
  - scalar columns `indexrelid`, `indrelid`, `indisprimary`, `indisunique`,
    `indisvalid`, `indnkeyatts`, `indnullsnotdistinct`, `indpred`;
  - array columns `indkey`, `indclass`, `indoption`. PostgreSQL types
    these as int2vector or oidvector; here they are `int2[]`, `oid[]` and
    `int2[]` arrays.
- `pg_sequence`: `seqrelid`, `seqstart`, `seqincrement`, `seqmin`,
  `seqmax`, `seqcache`, `seqcycle`.
- `pg_collation`: `oid`, `collname`, `collnamespace`.
- `pg_opclass`: `oid`, `opcname`, `opcdefault`.

The `TABLES` column-spec mini-language gains an array marker, for example
`indkey[]#` for an integer array; the exact syntax is the builder's call.
New tables route automatically through `references_pg_catalog`.

Scalar functions go in crates/execution/src/functions/pg_catalog.rs and
are registered and typed in the usual places. Since nothing exists for
them to describe, they return NULL:

- `pg_get_constraintdef(oid[, bool])`
- `pg_get_expr(text, oid[, bool])`
- `pg_get_indexdef(oid, int, bool)`
- `pg_get_serial_sequence(text, text)`

`pg_collation_is_visible(oid)` returns true.

`json_build_object(k1, v1, …)` is variadic with an even number of
arguments and returns a JSON object as text.
- Keys are text. Values are encoded by type: numbers bare, booleans
  `true`/`false`, NULL as `null`, strings JSON-escaped.
- An odd number of arguments is a planning error.

Runtime `regclass`:
- `CAST(<integer expr> AS REGCLASS)` keeps the oid.
- `CAST(<regclass> AS TEXT)` yields the relation name, or the number when
  the oid is unknown, as PostgreSQL does.
- `CAST(<text expr> AS REGCLASS)` resolves the name, NULL in gives NULL
  out, and an unknown name is an error.

Today only a string literal works, folded at plan time. This needs the
session's catalog view at execution. #189 sketched a runtime function
that takes session defaults the way `session_literal` does. The builder
picks the mechanism.

### 2. Arrays in the engine

- **Value model.**
  - `DataType::List(Box<DataType>)` already maps to and from Arrow
    `List`.
  - Add a list variant to `ScalarValue` so that accumulators, literal
    folding and `scalars_to_array` can carry arrays.
- **Literals.**
  - `ARRAY[e1, …]` becomes an AST node and a plan expression producing a
    `ListArray`, one list per input row.
  - The element type is the common supertype of the elements.
  - `ARRAY[]` without a cast is an error, as in PostgreSQL.
  - The existing `= ANY (ARRAY[literals])` → `IN` desugar stays, because
    it is cheaper.
- **`= ANY (<array expr>)` / `<> ALL`.**
  - Generalise the desugar. When the right side is not a literal
    `ARRAY[...]`, plan an array-membership expression. NULL semantics
    match PostgreSQL's three-valued `ANY`.
  - Bind parameters are text-substituted before parsing. So
    `param_literal` must turn an array parameter (text format `{a,b}`,
    and binary for the array OIDs of the types already decoded) into an
    `ARRAY[...]` literal with the element type from the parameter OID.
  - Describe should report the array parameter type where it can be
    inferred.
- **`array_agg(x [ORDER BY k1 [ASC|DESC] [NULLS …], …])`.**
  - The AST and plan `Function` nodes gain an `order_by` list. These are
    serde types, so the worker wire format changes; coordinator and
    workers of this version must agree.
  - The aggregate needs the sort-key columns as input. Either give the
    accumulator traits multiple input columns, or pack value and keys
    into one struct column; the builder chooses.
  - It returns `List(T)`, and NULL elements are kept.
  - It is non-decomposable for the fragmenter, so it runs single-phase.
  - Over an empty input with no `GROUP BY` it gives NULL.
  - It works in both the row-at-a-time and grouped paths.
- **`bool_and`.** Add it alongside `bool_or`; the index query uses it.
- **Set-returning functions in `SELECT`.**
  - Add a logical and physical node (in the style of `ProjectSet`) below
    the final projection, mirroring how `plan_windows` places a Window
    node.
  - `unnest(array)` yields one row per element.
  - `generate_subscripts(array, 1)` yields 1..n.
  - Several SRFs in one `SELECT` list zip by position and pad the shorter
    with NULL (PostgreSQL 10+ semantics).
  - A NULL or empty array yields no rows for that input row.
  - It streams per batch and runs in whatever fragment holds its input.
    Every exhaustive `LogicalPlan` match must handle it: schema, Display,
    fragmenter, cost, optimizer passes.
  - SRFs nested inside other expressions, or used in `WHERE`, are refused.
- **pgwire output.**
  - `datatype_to_pg_type` maps `List(T)` to T's array OID (from
    `pg_oid::PG_TYPES.array_oid`), falling back to `text[]`.
  - Text encoding is PostgreSQL's array literal `{…}`:
    - quote elements containing `,`, `"`, `\`, `{`, `}`, or whitespace,
      and the empty string;
    - render NULL elements as unquoted `NULL`;
    - support nested lists.
  - Results are text format today, so binary array output is not needed
    unless a client asks for it. psycopg 3 may ask; check.
  - Also cover `arrow_type_to_pg` and `arneb_type_to_pg_param_type`.
- **Trino protocol:** already encodes `ListArray` as `array(T)`. No change
  is needed, but one test should check that `array_agg` comes back over
  Trino correctly.

### 3. Correlated scalar subqueries in `SELECT`

Today a `SELECT`-list subquery is planned with no outer scope, so an outer
reference is "column not found". Only `WHERE` comparison operands get
decorrelated, and only for aggregate subqueries. The new behaviour:

- **Where.** A pre-pass over the `SELECT` items, in both
  `plan_projection` and the aggregate projection path, finds `Subquery`
  nodes at any depth: inside `CASE` conditions and branches, function
  arguments, comparisons, casts and arithmetic.
- **Which subqueries.** A subquery whose `WHERE` is a conjunction of:
  - **correlation equalities** `inner_expr = outer_expr`. Either side may
    be an expression, for example
    `seqrelid = CAST(CAST(pg_get_serial_sequence(CAST(CAST(attrelid AS REGCLASS) AS TEXT), attname) AS REGCLASS) AS OID)`.
    Expression join keys landed in #253;
  - **inner-only predicates**, which stay in the subquery;
  - **outer-only predicates**, for example `pg_attribute.atthasdef`.
    These are lifted out as a guard: the subquery's value is NULL for
    outer rows where the guard is false or NULL.

  The subquery's `FROM` may contain joins, as with
  `pg_collation JOIN pg_namespace`. It needs at least one correlation
  equality.
- **Rewrite.**
  - The subquery is grouped by its correlation keys and LEFT JOINed to
    the outer input.
  - A non-aggregate subquery yields its single value per key. A key
    group with more than one row raises PostgreSQL's error ("more than
    one row returned by a subquery used as an expression"), but only for
    outer rows that actually reference that group. For an aggregate
    subquery, the existing `LeftJoinSpec` / `empty_value` handling
    (COUNT → 0) is reused.
  - If the subquery sits in a `CASE` branch that is not taken for a row,
    that row must not raise the multi-row error. Whether that comes from
    lazy branch evaluation or from folding the guard into the check is
    the builder's call; a test pins it.
- **Unsupported shapes** keep failing with a clear "correlated subquery
  … not supported" error, never "column not found". These are:
  - correlation only through non-equality;
  - correlation in `GROUP BY`, `HAVING`, `LIMIT` or `ORDER BY` of the
    subquery;
  - nested correlation more than one level deep;
  - `LATERAL`.
- **Uncorrelated** subqueries keep today's execute-once-and-fold path.

### 4. Tests and the probe

- **Tier 1 (`cargo test`).**
  - Unit and integration tests per feature, through the protocol test
    harnesses (`crates/protocol/tests/`).
  - Results are compared with what PostgreSQL returns, using the
    expectations written in the test.
  - For each SQLAlchemy query family (columns, pk/unique, fk/check,
    indexes), one integration test runs the captured SQL verbatim, with
    its parameters, against the pgwire handler. It asserts the result
    shape and rows, and that no fallback happened.
- **Tier 2 (local).**
  - Run the probe script: the ten inspector calls with both drivers,
    against a standalone and against the 1+2 cluster from
    `scripts/client-smoke/cluster.sh`.
  - Then extend `scripts/client-smoke/clients.py` with those calls,
    `autoload_with` plus a query over the reflected table, and
    `arneb_pg_catalog_fallbacks_total == 0` read from `/metrics` after
    the pgwire SQLAlchemy section.
- **Distributed correctness is part of acceptance.**
  - Reflection SQL runs only on the coordinator: `pg_catalog::run` never
    fragments. The new features, though, are open to user queries, which
    do get distributed.
  - The cluster section of `clients.py` therefore also runs, over the
    TPC-H tables:
    - an `array_agg(... ORDER BY ...)` grouped query;
    - an `unnest` / `generate_subscripts` query over an `array_agg`
      subquery;
    - an `ARRAY[...]` literal column;
    - a correlated `SELECT`-list subquery (aggregate and non-aggregate,
      one inside `CASE`).
  - Each is checked against DuckDB the way the TPC-H queries already are.
  - These exercise `List` columns over Flight IPC and the plan serde for
    the new nodes and expressions.
  - Mixed versions need no extra care: workers whose `CARGO_PKG_VERSION`
    differs are already rejected.
- No Tier 3; nothing here is scale-sensitive.

### 5. Docs

- `docs/guide/clients.md`: update the pgwire SQLAlchemy row and the
  "Not supported yet" section.
- `docs/sql/`: document `ARRAY[...]`, `array_agg`, `unnest`,
  `generate_subscripts`, `= ANY(array)`, `json_build_object`, the new
  catalog functions and correlated `SELECT`-list subqueries.
- The roadmap status line.

## Delivery

One OpenSpec change (`pg-reflection-arrays`) and one PR, with commits
grouped by the four parts above, so review can go part by part. If the
array part alone grows past a reviewable size, the builder may split it
into a first PR (sections 2 and 4 for arrays) and a second
(sections 1 and 3), each green on its own.

## Left to the builder

- The `ScalarValue` list variant's exact shape.
- The `TABLES` array-column syntax.
- Multi-column aggregate input versus struct packing.
- The SRF node's name and placement details.
- The runtime `regclass` mechanism.
- How a not-taken `CASE` branch avoids the multi-row error.
- Whether `json_build_object` is typed `json` or `text` over pgwire.
