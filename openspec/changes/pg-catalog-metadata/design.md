## Context

DBeaver (and most PostgreSQL GUI clients) sends metadata queries immediately after connecting: `pg_catalog.pg_type`, `pg_catalog.pg_namespace`, `pg_catalog.pg_class`, `pg_catalog.pg_attribute`, `information_schema.tables`, `information_schema.columns`, and `SELECT version()`. These all fail with "table not found" because trino-alt has no system catalog tables.

CatalogManager already has the metadata needed: `catalog_names()`, `schema_names()`, `table_names()`, `table().schema()` → `Vec<ColumnInfo>`. The gap is exposing this metadata through PostgreSQL system table queries.

## Goals / Non-Goals

**Goals:**

- DBeaver schema browser shows all registered tables and their columns
- `SELECT version()` returns a server identity string
- Core pg_catalog tables return sensible data: pg_type, pg_namespace, pg_class, pg_attribute
- information_schema.tables and information_schema.columns work
- Queries with WHERE filters on these tables work (e.g., `WHERE table_schema = 'default'`)

**Non-Goals:**

- Full PostgreSQL system catalog fidelity (hundreds of columns we don't have data for)
- pg_catalog.pg_index, pg_constraint, pg_proc (indexes, constraints, functions)
- pg_catalog.pg_settings, pg_stat_* (configuration, statistics)
- Writing to system tables

## Decisions

### D1: Early interception in protocol handler

**Choice**: Detect metadata queries in `execute_query()` before the SQL parser. Match by checking if the SQL references `pg_catalog.`, `information_schema.`, or calls `version()`. If matched, delegate to a `MetadataHandler` that builds result sets from CatalogManager.

**Rationale**: Avoids modifying the parser, planner, or catalog system. The system catalog is a protocol-level concern — PostgreSQL-specific, not part of the query engine's core.

**Alternative**: Register synthetic schemas in CatalogManager. Rejected because it couples PostgreSQL-specific metadata to the engine core, and the query planner would need to handle these special tables differently.

### D2: Minimal column sets

**Choice**: Return only the columns that DBeaver/JDBC drivers actually use, not all 30+ columns per pg_catalog table. Fill unused columns with NULL or sensible defaults.

**Rationale**: DBeaver only reads a subset of columns. Implementing all columns wastes effort and complicates maintenance. We can add more columns later if other clients need them.

### D3: Synthetic OIDs

**Choice**: Generate stable OIDs by hashing names. Tables get OID = hash(catalog.schema.table), types get fixed OIDs matching PostgreSQL's well-known type OIDs (e.g., int8=20, text=25, bool=16).

**Rationale**: DBeaver uses OIDs to cross-reference pg_class, pg_attribute, and pg_type. They need to be consistent within a session but don't need to match real PostgreSQL OIDs (except for built-in types which drivers recognize by OID).

### D4: Parse metadata SQL with regex, not the SQL parser

**Choice**: Use simple string matching / regex to detect and route metadata queries, rather than parsing them through the full SQL pipeline.

**Rationale**: The full parser doesn't understand multi-part names like `pg_catalog.pg_type` as a catalog.table reference. Regex detection is simpler and sufficient for the limited set of metadata queries we need to support.

## Risks / Trade-offs

**[Regex matching is fragile]** → Complex metadata queries with JOINs or subqueries won't be intercepted. **Mitigation**: DBeaver's metadata queries are predictable and well-known. We match the patterns it uses. Unknown metadata queries still get a "table not found" error, which is better than crashing.

**[Incomplete column coverage]** → Some columns in pg_catalog tables will be NULL. **Mitigation**: DBeaver handles NULLs gracefully for optional columns. The critical columns (oid, name, type) are populated.
