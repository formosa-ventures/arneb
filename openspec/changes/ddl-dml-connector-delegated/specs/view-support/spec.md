# Spec: View Support

## ADDED Requirements

### Requirement: Parse CREATE VIEW statement
The SQL parser SHALL recognize `CREATE VIEW name AS SELECT ...` and `CREATE OR REPLACE VIEW name AS SELECT ...` syntax.

#### Scenario: Simple CREATE VIEW
- **WHEN** the engine receives `CREATE VIEW active_users AS SELECT * FROM users WHERE active = true`
- **THEN** the parser produces a CreateView AST node with name "active_users" and the subquery.

#### Scenario: CREATE OR REPLACE VIEW
- **WHEN** the engine receives `CREATE OR REPLACE VIEW v AS SELECT ...`
- **THEN** the parser produces a CreateView AST node with the `or_replace` flag set to true.

### Requirement: Parse DROP VIEW statement
The SQL parser SHALL recognize `DROP VIEW name` and `DROP VIEW IF EXISTS name` syntax.

#### Scenario: DROP VIEW
- **WHEN** the engine receives `DROP VIEW active_users`
- **THEN** the parser produces a DropView AST node with name "active_users".

#### Scenario: DROP VIEW IF EXISTS
- **WHEN** the engine receives `DROP VIEW IF EXISTS active_users`
- **THEN** the parser produces a DropView AST node with `if_exists` flag set to true.

### Requirement: Register view in catalog
The execution engine SHALL store the view definition (name, SQL, and pre-planned logical plan) in the catalog's view registry.

#### Scenario: View created successfully
- **WHEN** `CREATE VIEW v AS SELECT id, name FROM users` is executed
- **THEN** the view "v" is registered in the catalog and a "CREATE VIEW" confirmation is returned.

#### Scenario: View already exists
- **WHEN** `CREATE VIEW v AS SELECT ...` is executed and "v" already exists
- **THEN** an error "View already exists" SHALL be returned.

#### Scenario: View replaced
- **WHEN** `CREATE OR REPLACE VIEW v AS SELECT ...` is executed and "v" already exists
- **THEN** the existing view definition is replaced.

### Requirement: Resolve view references during planning
The query planner SHALL recognize view names in FROM clauses and substitute the view's logical plan.

#### Scenario: Query a view
- **WHEN** `SELECT * FROM active_users` is executed and "active_users" is a registered view
- **THEN** the planner substitutes the view's logical plan in place of a table scan.

#### Scenario: View combined with other tables
- **WHEN** `SELECT * FROM active_users JOIN orders ON ...` is executed
- **THEN** the planner substitutes the view for "active_users" and resolves "orders" normally.

#### Scenario: Nested views
- **WHEN** view "b" is defined as `SELECT * FROM a` where "a" is also a view
- **THEN** the planner recursively expands both views.

### Requirement: Drop view from catalog
The execution engine SHALL remove the view definition from the catalog's view registry.

#### Scenario: Drop existing view
- **WHEN** `DROP VIEW v` is executed and "v" exists
- **THEN** the view is removed and a "DROP VIEW" confirmation is returned.

#### Scenario: Drop non-existent view
- **WHEN** `DROP VIEW v` is executed and "v" does not exist
- **THEN** an error "View not found" SHALL be returned.

#### Scenario: Drop non-existent view with IF EXISTS
- **WHEN** `DROP VIEW IF EXISTS v` is executed and "v" does not exist
- **THEN** no error is raised and a "DROP VIEW" confirmation is returned.

### Requirement: View is non-materialized
Views SHALL be expanded inline at planning time on every query. They do not store data.

#### Scenario: Underlying data changes
- **WHEN** data in the underlying table changes after view creation
- **THEN** subsequent queries against the view reflect the updated data.
