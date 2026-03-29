## ADDED Requirements

### Requirement: QueryParser parses SQL with parameter placeholders
The system SHALL implement `QueryParser` that accepts SQL strings containing `$1`, `$2`, ... placeholders and stores them as `String` statements.

#### Scenario: Parse simple SELECT with parameters
- **WHEN** client sends Parse with SQL `SELECT * FROM t WHERE id = $1`
- **THEN** server stores the statement and responds with ParseComplete

#### Scenario: Parse SQL without parameters
- **WHEN** client sends Parse with SQL `SELECT 1`
- **THEN** server stores the statement and responds with ParseComplete

### Requirement: Parameter binding substitutes placeholders
The system SHALL substitute `$N` placeholders with parameter values from the Bind message before executing the query. Parameters SHALL be formatted as text literals.

#### Scenario: Bind integer parameter
- **WHEN** client sends Bind with parameter values `["42"]` for statement `SELECT $1`
- **THEN** server creates a portal with bound SQL `SELECT 42`

#### Scenario: Bind string parameter
- **WHEN** client sends Bind with parameter values `["hello"]` for statement `SELECT $1`
- **THEN** server creates a portal with bound SQL `SELECT 'hello'`

#### Scenario: Bind NULL parameter
- **WHEN** client sends Bind with NULL parameter for statement `SELECT $1`
- **THEN** server creates a portal with bound SQL `SELECT NULL`

### Requirement: Execute runs the query and returns results
The system SHALL execute the bound SQL through the standard parse-plan-optimize-execute pipeline and return results as DataRow messages.

#### Scenario: Execute SELECT returning rows
- **WHEN** client sends Execute for a portal containing `SELECT 1 AS val`
- **THEN** server returns DataRow with value `1` and CommandComplete

#### Scenario: Execute query with error
- **WHEN** client sends Execute for a portal with invalid SQL
- **THEN** server returns ErrorResponse with appropriate error message

### Requirement: Describe returns column metadata
The system SHALL respond to Describe messages with RowDescription (column names and types) for both statements and portals.

#### Scenario: Describe statement
- **WHEN** client sends Describe for a parsed statement `SELECT id, name FROM users`
- **THEN** server returns ParameterDescription and RowDescription with columns `id` and `name`

#### Scenario: Describe portal
- **WHEN** client sends Describe for a bound portal
- **THEN** server returns RowDescription with column metadata from the planned query

### Requirement: Close deallocates statements and portals
The system SHALL respond to Close messages by removing the named statement or portal from the connection's store.

#### Scenario: Close named statement
- **WHEN** client sends Close for statement `stmt1`
- **THEN** server removes `stmt1` and responds with CloseComplete

### Requirement: Full Extended Query flow works end-to-end
The system SHALL handle the standard Parse → Bind → Describe → Execute → Sync message sequence.

#### Scenario: DBeaver-style query flow
- **WHEN** client sends Parse("SELECT $1"), Bind(params=["42"]), Execute, Sync
- **THEN** server returns ParseComplete, BindComplete, DataRow("42"), CommandComplete, ReadyForQuery

#### Scenario: Multiple statements in sequence
- **WHEN** client sends Parse("SELECT 1"), Execute, Parse("SELECT 2"), Execute, Sync
- **THEN** server executes both queries in order and returns results for each
