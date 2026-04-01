## Why

DBeaver and other PostgreSQL GUI clients query `pg_catalog.*` and `information_schema.*` system tables immediately after connecting to discover schemas, tables, and columns for the schema browser. arneb currently has no system catalog tables — these queries fail with "table not found", making DBeaver's schema browser unusable despite the connection itself working.

## What Changes

- Intercept queries to `pg_catalog` and `information_schema` tables in the protocol handler before they reach the regular query planner
- Synthesize result sets from CatalogManager metadata (catalog names, schema names, table names, column info)
- Support `version()` function to report server identity
- Support the core system tables that DBeaver requires: `pg_type`, `pg_namespace`, `pg_class`, `pg_attribute`, `information_schema.tables`, `information_schema.columns`

## Capabilities

### New Capabilities

- `pg-system-catalog`: Synthetic PostgreSQL system catalog tables (`pg_catalog.pg_type`, `pg_catalog.pg_namespace`, `pg_catalog.pg_class`, `pg_catalog.pg_attribute`) populated from CatalogManager metadata. Metadata query interception in the protocol handler.
- `pg-information-schema`: Synthetic `information_schema.tables` and `information_schema.columns` views populated from CatalogManager metadata.

### Modified Capabilities

- `pg-connection`: The query handler intercepts metadata queries before the regular planner, returning synthetic results for system catalog and information_schema queries.

## Impact

- **Crates**: `protocol` (query interception + result synthesis), `catalog` (metadata enumeration helpers)
- **Dependencies**: None new
- **Unlocks**: DBeaver schema browser, DataGrip metadata panel, any JDBC/ODBC tool that introspects PostgreSQL system tables
