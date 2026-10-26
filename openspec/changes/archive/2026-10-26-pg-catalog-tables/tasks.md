## 1. Shared pieces

- [x] 1.1 `arneb_common::pg_oid`: system relation oids, hashed oids, owner role
- [x] 1.2 Parser: `regclass` / `oid` casts, `ANY` / `ALL` over array literals, `pg_catalog.` function prefix; tests
- [x] 1.3 Planner: `regclass('<name>')` to an oid literal; return types for the catalog functions
- [x] 1.4 Execution: `format_type`, `pg_get_userbyid`, `pg_table_is_visible`, `pg_is_other_temp_schema`, `obj_description`, `col_description`; registry agreement test

## 2. pgwire

- [x] 2.1 `crates/protocol/src/pg_catalog.rs`: per-statement tables from the session view
- [x] 2.2 Intercept: run pg_catalog statements in the engine, fixed result as fallback; old handlers on the shared oids; Describe plans over the catalog tables for parameter types
- [x] 2.4 psycopg 3: `regtype`, `to_regtype`, `SAVEPOINT` / `RELEASE`; `LIKE ... ESCAPE`
- [x] 2.3 Tests: the recorded dbt and SQLAlchemy statements end to end in `client_compat.rs`; access rules

## 3. Smoke and docs

- [x] 3.1 `run.sh`: dbt-postgres `test`, `show`, `docs generate`; `clients.py`: SQLAlchemy postgresql listing
- [x] 3.2 Run `run.sh` locally on TPC-H SF1 (Tier 2): all steps pass, including the new dbt-postgres and SQLAlchemy postgresql checks
- [x] 3.3 `docs/guide/clients.md`, `docs/sql/functions.md`
