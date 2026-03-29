## MODIFIED Requirements

### Requirement: Connection handler intercepts metadata queries
The query handler SHALL detect queries referencing `pg_catalog.*`, `information_schema.*`, or the `version()` function BEFORE passing them to the regular query planner. Matched queries SHALL be routed to a MetadataHandler that builds result sets from CatalogManager metadata. Unmatched queries SHALL proceed through the normal parse-plan-execute pipeline unchanged.

#### Scenario: pg_catalog query intercepted
- **WHEN** client sends `SELECT * FROM pg_catalog.pg_type`
- **THEN** the handler returns a synthetic result set without invoking the SQL parser or query planner

#### Scenario: Regular query not intercepted
- **WHEN** client sends `SELECT * FROM lineitem LIMIT 10`
- **THEN** the handler processes it through the normal query pipeline (no interception)

#### Scenario: Mixed metadata and regular queries in session
- **WHEN** client sends `SELECT * FROM pg_catalog.pg_namespace` then `SELECT COUNT(*) FROM orders`
- **THEN** the first query returns synthetic metadata, the second runs through normal pipeline

### Requirement: Extended Query Describe intercepts metadata queries
The Extended Query handler's `do_describe_statement` and `do_describe_portal` SHALL also intercept metadata queries and return correct FieldInfo (column metadata). This is required because JDBC drivers send Parse → Describe → Bind → Execute, and the Describe step needs matching column structure before Execute sends row data.

#### Scenario: Describe on pg_catalog query via Extended Query
- **WHEN** JDBC driver sends Parse("SELECT * FROM pg_catalog.pg_type") then Describe
- **THEN** the server returns RowDescription with columns matching the pg_type synthetic result (oid, typname, etc.)

#### Scenario: Describe on SET command
- **WHEN** JDBC driver sends Parse("SET extra_float_digits = 3") then Describe
- **THEN** the server returns NoData (no columns, since SET is a command not a query)
