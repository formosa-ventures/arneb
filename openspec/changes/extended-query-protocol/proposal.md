## Why

arneb currently only implements the PostgreSQL Simple Query protocol. Most SQL clients (DBeaver, JDBC drivers, language-specific PostgreSQL libraries like `node-postgres`, `psycopg2`, Go's `pgx`) default to the Extended Query protocol with prepared statements. This means they either fail to connect or require non-standard configuration (`preferQueryMode=simple`). Supporting Extended Query is essential for real-world client compatibility.

## What Changes

- Implement PostgreSQL Extended Query protocol (Parse/Bind/Describe/Execute/Sync message flow)
- Add a prepared statement store per connection (named + unnamed statements)
- Support parameter placeholders (`$1`, `$2`) with type inference
- Support the portal lifecycle (Bind creates a portal, Execute consumes it)
- Handle Describe messages for both statements and portals (return column metadata + parameter types)
- Keep the existing Simple Query path unchanged

## Capabilities

### New Capabilities

- `pg-extended-query`: PostgreSQL Extended Query protocol implementation — Parse, Bind, Describe, Execute, Sync, Close message handlers. Prepared statement storage, parameter binding, and portal management.

### Modified Capabilities

- `pg-connection`: Connection handler factory must now wire the new ExtendedQueryHandler instead of PlaceholderExtendedQueryHandler.

## Impact

- **Crates**: `protocol` (new ExtendedQueryHandler implementation, modified HandlerFactory)
- **Dependencies**: No new external crate dependencies (pgwire already exposes the `ExtendedQueryHandler` trait)
- **Unlocks**: DBeaver, JDBC, psycopg2, node-postgres, pgx, and all other standard PostgreSQL clients can connect without workarounds
