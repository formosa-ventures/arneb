## ADDED Requirements

### Requirement: Trino-compatible rules file
When a rules file is configured, the server SHALL read it as JSON in Trino's
file-based access control format, supporting the `catalogs`, `schemas`,
`tables` and `queries` sections with the fields `user`, `catalog`,
`schema`, `table`, `allow`, `owner`, `privileges` and `queryOwner`.
Patterns SHALL be regular expressions matched against the whole name and
SHALL default to `.*`. Within a section the first rule whose patterns all
match SHALL decide, and no matching rule SHALL deny. A section that is
absent SHALL allow everything it governs. Any other section or field SHALL
stop startup with an error naming it.

#### Scenario: First match wins
- **WHEN** the `tables` rules are `[{"user": "alice", "table": "secret", "privileges": []}, {"privileges": ["SELECT"]}]`
- **THEN** alice cannot read a table named `secret`, and can read every other table

#### Scenario: Unsupported rule
- **WHEN** a `tables` rule has a `filter` field
- **THEN** the server refuses to start with an error naming `filter`

#### Scenario: No rules file
- **WHEN** no rules file is configured
- **THEN** every user can read every table, as before

### Requirement: Table reads are authorized
A query SHALL read a table only when the session user's catalog access for
its catalog is not `none` and the matching `tables` rule grants `SELECT` or
`OWNERSHIP`. Otherwise the query SHALL fail before execution with
`Access Denied: Cannot select from table <catalog>.<schema>.<table>`, the
same whether or not the table exists, as SQLSTATE `42501` on pgwire and
error `PERMISSION_DENIED` on the Trino protocol.

#### Scenario: Denied catalog
- **WHEN** a `catalogs` rule gives bob `none` on `pg` and bob runs `SELECT * FROM pg.public.accounts`
- **THEN** the query fails with `Access Denied: Cannot select from table pg.public.accounts`

#### Scenario: Unqualified name
- **WHEN** the session's default catalog and schema are `hive.tpch` and alice may not read `hive.tpch.lineitem`
- **THEN** `SELECT * FROM lineitem` fails naming `hive.tpch.lineitem`

### Requirement: Listings show only what the user can use
Catalog, schema and table listings SHALL hide a catalog with access
`none`, a schema the user neither owns nor holds a table privilege in, and
a table the user holds no privilege on. This covers pgwire (`pg_catalog`,
`information_schema`) and the Trino protocol (`SHOW CATALOGS`,
`SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`, `information_schema`,
`system.jdbc`). The `system` catalog and `information_schema` schemas SHALL
stay listed.

#### Scenario: Hidden schema
- **WHEN** the only `schemas` rule is `{"owner": false}` and alice holds privileges only on tables in `hive.sales`
- **THEN** `SHOW SCHEMAS FROM hive` lists `sales` and `information_schema` and nothing else

#### Scenario: No schemas section
- **WHEN** the rules file has `tables` rules but no `schemas` section
- **THEN** every schema of a visible catalog is listed, as in Trino, where everyone owns every schema then

### Requirement: Query rules
When the rules file has a `queries` section, a user SHALL run a statement
only when the first rule without `queryOwner` that matches them allows
`execute`; otherwise the statement SHALL fail with
`Access Denied: Cannot execute query`. `system.runtime.queries` SHALL list
a user's own queries, and another user's query only when the first
`queries` rule matching the viewer and the query's owner allows `view`.
Without a `queries` section every user SHALL run statements and see every
query. A rule with `queryOwner` that allows `execute` SHALL be rejected.

#### Scenario: No execute rule
- **WHEN** the `queries` rules are `[{"user": "admin", "allow": ["execute", "view"]}]` and carol runs `SELECT 1`
- **THEN** the statement fails with `Access Denied: Cannot execute query`

#### Scenario: Restricted view
- **WHEN** the `queries` rules are `[{"user": "admin", "allow": ["execute", "view"]}, {"allow": ["execute"]}]`
- **THEN** admin sees every query in `system.runtime.queries`, and alice sees only her own

### Requirement: The policy is logged at startup
Startup SHALL log the rules file and the number of rules in each section on
the `arneb::config` target, and SHALL log a warning when a rules file is
set while `[auth]` is `none`.

#### Scenario: Rules without authentication
- **WHEN** a rules file is configured and `[auth] type = "none"`
- **THEN** startup logs a warning that user names are not verified
