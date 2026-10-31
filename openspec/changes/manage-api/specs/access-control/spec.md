## MODIFIED Requirements

### Requirement: Trino-compatible rules file
When a rules file is configured, the server SHALL read it as JSON in Trino's
file-based access control format, supporting the `catalogs`, `schemas`,
`tables`, `queries` and `system_information` sections with the fields `user`, `catalog`,
`schema`, `table`, `allow`, `owner`, `privileges` and `queryOwner`. A
`system_information` rule SHALL list `read` and/or `write` in `allow`, and
SHALL refuse `group` and `role`.
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

#### Scenario: system_information rule
- **WHEN** the rules file has `system_information` rules `[{"user": "ops", "allow": ["read", "write"]}, {"allow": ["read"]}]`
- **THEN** ops may observe and manage, and every other user may only observe

#### Scenario: Unsupported system_information field
- **WHEN** a `system_information` rule has a `group` field
- **THEN** the server refuses to start with an error naming `group`

### Requirement: Query rules
When the rules file has a `queries` section, a user SHALL run a statement
only when the first rule without `queryOwner` that matches them allows
`execute`; otherwise the statement SHALL fail with
`Access Denied: Cannot execute query`. `system.runtime.queries` SHALL list
a user's own queries, and another user's query only when the first
`queries` rule matching the viewer and the query's owner allows `view`.
Without a `queries` section every user SHALL run statements and see every
query. A rule with `queryOwner` that allows `execute` SHALL be rejected.
Cancelling a query SHALL be allowed to its owner, and to another user when
the first rule matching the user and the owner allows `kill` (or there is no
`queries` section) under password auth, or when the user has manage.

#### Scenario: No execute rule
- **WHEN** the `queries` rules are `[{"user": "admin", "allow": ["execute", "view"]}]` and carol runs `SELECT 1`
- **THEN** the statement fails with `Access Denied: Cannot execute query`

#### Scenario: Restricted view
- **WHEN** the `queries` rules are `[{"user": "admin", "allow": ["execute", "view"]}, {"allow": ["execute"]}]`
- **THEN** admin sees every query in `system.runtime.queries`, and alice sees only her own

#### Scenario: Kill rule
- **WHEN** the `queries` rules are `[{"user": "ops", "queryOwner": "alice", "allow": ["kill"]}, {"allow": ["execute"]}]` and ops cancels alice's query
- **THEN** the cancel is allowed, and bob cancelling alice's query is `forbidden`

## ADDED Requirements

### Requirement: Table writes are authorized
A statement that writes SHALL be authorized after planning against the target table. `INSERT` SHALL need `INSERT`, `DELETE` SHALL need `DELETE`, and `CREATE TABLE`, `CREATE TABLE AS SELECT`, `DROP TABLE`, `CREATE VIEW` and `DROP VIEW` SHALL need `OWNERSHIP`. `OWNERSHIP` SHALL imply every privilege. A catalog with access `read-only` SHALL refuse every write and `none` SHALL refuse everything. `EXPLAIN` of a write SHALL be checked like the write. A refusal SHALL read `Access Denied: Cannot <action> <catalog>.<schema>.<name> (requires <PRIVILEGE>)`, as SQLSTATE `42501` on pgwire, `PERMISSION_DENIED` on the Trino protocol and `forbidden` in an agent or MCP statement. `UPDATE` SHALL be accepted in rules files and grant nothing.

#### Scenario: Insert without INSERT
- **WHEN** alice holds only `SELECT` on `lake.s.t` and runs `INSERT INTO lake.s.t VALUES (1)`
- **THEN** the statement fails with `Access Denied: Cannot insert into table lake.s.t (requires INSERT)`

#### Scenario: Ownership implies insert
- **WHEN** alice holds `OWNERSHIP` on `lake.s.t` and runs the same insert
- **THEN** the privilege check passes
