## Context

The pgwire metadata intercept (`crates/protocol/src/metadata.rs`) answers
any statement that contains `pg_catalog.pg_class` and similar names with a
fixed table. The Trino protocol already solves the same problem for
`information_schema` and `system.jdbc`: it builds per-statement memory
catalogs and runs the client's SQL through the engine
(`trino/metadata.rs::build_virtual_catalogs`). The client queries this
change targets were recorded from PostgreSQL 17 with `log_statement=all`:
- dbt-postgres 1.11: `debug`, `test`, `show` and `docs generate`.
- SQLAlchemy 2.1: with psycopg2 and psycopg 3, inspector calls and
  `Table(autoload_with=…)`.

## Decisions

### D1: Run pg_catalog statements in the engine over a per-statement schema

A statement that names one of the provided tables runs against a
`CatalogManager` that holds one catalog, named after the session's catalog.
That catalog has a `pg_catalog` schema of memory tables built for the
statement, and `pg_catalog` is the default schema. As a result,
`pg_catalog.pg_class`, `pg_class` and `"file".pg_catalog.pg_class` all
resolve.

The statement runs through the intercept, so Simple Query, Extended Query
Execute and Describe all take the same path, the way the fixed handlers did.
It runs on the coordinator only, outside admission and the query tracker,
like every other metadata statement.

A statement that mixes pg_catalog tables with user tables does not resolve
the user tables. No recorded client does this.

### D2: A PostgreSQL database is the session's catalog

`pg_namespace` lists the session catalog's schemas, plus `pg_catalog` and
`information_schema`. `pg_class` and `pg_attribute` list that catalog's
tables, plus the provided pg_catalog tables, so that
`get_table_names(schema='pg_catalog')` works. `pg_database` lists every
visible catalog.

The old handlers merged the schemas of all catalogs, so two catalogs with a
`default` schema collapsed into one namespace. Every listing goes through
the session's access view (`visible_*`), so hidden objects are absent.

### D3: Stable oids from one scheme, shared by the tables and `regclass`

`arneb_common::pg_oid` holds the scheme:
- `pg_catalog` is namespace oid 11, as in PostgreSQL.
- System relations use their PostgreSQL oids (`pg_class` 1259,
  `pg_rewrite` 2618, `pg_constraint` 2606, …), including relations Arneb
  does not provide, so `'pg_rewrite'::regclass` resolves.
- Every other schema and table gets an FNV-1a hash of its qualified name,
  folded into `16384..2^31`. That range is where PostgreSQL user oids live,
  so the hash cannot collide with a type oid.
- The owner role is oid 10, named `arneb`.

The old handlers used `DefaultHasher`, whose output is not guaranteed
stable across Rust releases. They move to the same scheme.

### D4: `regclass` is resolved at plan time, from a literal only

The parser turns `x::regclass` / `CAST(x AS REGCLASS)` into a `regclass(x)`
call. The planner replaces `regclass('<name>')` with the oid literal:
- An unqualified name is a system relation when it is one, and otherwise a
  table in the session's default schema.
- Quoted parts keep their case; unquoted parts are lowercased.

A non-literal argument is refused as unsupported. That is SQLAlchemy's
`attrelid::regclass::text`, part of stage 2b. Resolving at plan time needs
no catalog access at execution and keeps workers out of it.

`regtype` follows the same path:
- A `'int4'::regtype` literal is the type's oid.
- A non-literal `x::regtype` plans as `format_type(x, NULL)`, which is how
  PostgreSQL prints a `regtype`. That covers psycopg 3's
  `oid::regtype::text`.
- `to_regtype(name)` returns the oid, or NULL for an unknown name.

`oid` is an alias for `BIGINT`. Every oid column is `BIGINT`, and over the
wire it is `int8`, which psycopg, JDBC and dbt read as an integer.

### D5: `= ANY (ARRAY[…])` is an `IN` list in the parser

These forms are rewritten:
- `x = ANY (ARRAY[a, b])` and `x = SOME (…)` become `x IN (a, b)`.
- `x <> ALL (ARRAY[a, b])` becomes `x NOT IN (a, b)`.

Other operators and non-literal arrays are still refused. This covers the
recorded SQLAlchemy listing queries without an array type, which is
stage 2b.

### D6: Catalog functions are ordinary scalar functions

`format_type`, `pg_get_userbyid`, `pg_table_is_visible`,
`pg_is_other_temp_schema`, `obj_description` and `col_description` are
registered like every built-in, with planner return types. They are
visible on the Trino protocol too, where no client calls them.

The parser drops a `pg_catalog.` qualifier on a function name, as
PostgreSQL's search path does.

`format_type` covers the types Arneb maps to PostgreSQL. It reads numeric
precision and scale from the typmod, and prints `???` for an unknown oid,
as PostgreSQL does.

### D7: The fixed handlers stay as the fallback

When the engine fails on a pg_catalog statement, the old fixed result is
returned if one matches. Engine failures here are mostly unsupported SQL,
such as the stage 2b array queries. Because the fixed result may not match
the query, each fallback is logged at warn level on `arneb::protocol` with
its SQL and counted in `arneb_pg_catalog_fallbacks_total`, so the gaps can
be seen and the fallback retired.

The fallback keeps today's behavior for clients this change does not
target, such as DBeaver's and JDBC's catalog queries. Removing it is part
of stage 2b, once those clients are recorded and covered.

## Risks / Trade-offs

- **A statement that worked by accident can now fail.** A fixed result
  ignored the query, so it worked even when its rows were wrong. If the
  engine fails, the fallback still answers, so only statements the engine
  runs change. Those now return the rows they ask for.
- **Every pg_catalog statement builds the tables.** The tables are small,
  with one row per visible table and column. Building them costs the same
  catalog walk the old handlers made.
- **Oid width.** Oids are `int8` rather than `oid` on the wire. The
  recorded clients compare and print them, and no recorded query binds an
  oid parameter by type.
