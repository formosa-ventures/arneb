## ADDED Requirements

### Requirement: Statement submission
The system SHALL accept `POST /v1/statement` with the SQL text as the request body and respond with a Trino `QueryResults` JSON document containing `id`, `infoUri`, `nextUri`, `stats` (with `state` = `QUEUED`) and `warnings`. An empty body SHALL be rejected with HTTP 400.

#### Scenario: Submitting a query
- **WHEN** a client posts `SELECT 1` with header `X-Trino-User: alice`
- **THEN** the server responds 200 with a document whose `stats.state` is `QUEUED` and whose `nextUri` points at `/v1/statement/executing/{id}/{slug}/1`

#### Scenario: Empty statement
- **WHEN** a client posts an empty or whitespace-only body
- **THEN** the server responds with HTTP 400

### Requirement: Result paging through nextUri
The system SHALL execute the statement asynchronously through the same pipeline as the PostgreSQL protocol and serve its results through successive `nextUri` documents. While the query runs, a `GET` SHALL wait up to `max_wait` and then return state `RUNNING` with the same token. Once finished, each `GET` SHALL return `columns` and a bounded `data` page, and include a `nextUri` with the next token only while rows remain. Re-requesting the most recent token SHALL return the same document; a token older than that SHALL return HTTP 410.

#### Scenario: Multi-page results
- **WHEN** a query returns 25 rows and the page limit is 10 rows
- **THEN** the client receives pages of 10, 10 and 5 rows in order, and the final document has no `nextUri` and state `FINISHED`

#### Scenario: Idempotent re-fetch
- **WHEN** a client requests the same token twice
- **THEN** both responses carry identical `data`

#### Scenario: Stale token
- **WHEN** a client requests a token older than the last served one
- **THEN** the server responds with HTTP 410

### Requirement: Cancellation and expiry
The system SHALL cancel a query on `DELETE` of its `nextUri` (HTTP 204), stop its execution, and forget it. Queries not polled for `client_timeout` SHALL be canceled as abandoned; finished queries SHALL be removed `retention` after the client's last request.

#### Scenario: Cancel a running query
- **WHEN** a client sends `DELETE` on the `nextUri` of a running query
- **THEN** the server responds 204, the execution future is dropped, the tracked query is marked `CANCELLED`, and a later `GET` returns 404

#### Scenario: Abandoned query
- **WHEN** a client stops polling a running query for longer than `client_timeout`
- **THEN** the query is canceled and removed

### Requirement: Trino type encoding
The system SHALL describe each output column with `name`, a Trino `type` string and a `typeSignature`, and SHALL encode values as Trino does: integers and floating point as JSON numbers (non-finite as `"NaN"`/`"Infinity"`/`"-Infinity"`), decimals as strings, varchar as strings, varbinary as base64, booleans as JSON booleans, dates as `YYYY-MM-DD`, timestamps as `YYYY-MM-DD HH:MM:SS[.f…]` with the precision of the type, arrays/rows as JSON arrays, maps as JSON objects, and NULL as `null`.

#### Scenario: Typed row
- **WHEN** a query returns a bigint `1`, decimal(12,2) `123.45`, date `1995-01-01` and timestamp(3) `2020-01-02 03:04:05.123`
- **THEN** the row is encoded as `[1, "123.45", "1995-01-01", "2020-01-02 03:04:05.123"]` and the column types are `bigint`, `decimal(12,2)`, `date`, `timestamp(3)`

### Requirement: Error reporting
The system SHALL report failures as a Trino `QueryError` object (`message`, `errorCode`, `errorName`, `errorType`, `failureInfo`) with query state `FAILED`, classifying at least syntax errors, missing tables/columns/catalogs/schemas, unsupported features, cancellation, memory exhaustion and internal errors.

#### Scenario: Missing table
- **WHEN** a client runs `SELECT * FROM no_such_table`
- **THEN** the final document has `error.errorName` = `TABLE_NOT_FOUND`, `error.errorCode` = 46, `error.errorType` = `USER_ERROR`

### Requirement: Client session headers
The system SHALL read the session from `X-Trino-User`, `X-Trino-Source`, `X-Trino-Client-Info`, `X-Trino-Catalog`, `X-Trino-Schema`, `X-Trino-Session` and `X-Trino-Prepared-Statement`, and SHALL accept the legacy `X-Presto-*` equivalents, answering such clients with `X-Presto-*` response headers. Unqualified table names SHALL resolve against the session catalog and schema, including under distributed execution.

#### Scenario: Session catalog and schema
- **WHEN** a client sends `X-Trino-Catalog: lake` and `X-Trino-Schema: sales` and runs `SELECT order_id FROM orders`
- **THEN** the table `lake.sales.orders` is read

### Requirement: Session and prepared-statement control
The system SHALL handle `USE`, `SET SESSION`, `RESET SESSION`, `PREPARE`, `DEALLOCATE PREPARE`, `EXECUTE … USING`, `EXECUTE IMMEDIATE … USING` and transaction/`SET TIME ZONE|ROLE|PATH` statements in the protocol layer, returning the Trino response headers (`X-Trino-Set-Catalog`, `X-Trino-Set-Schema`, `X-Trino-Set-Session`, `X-Trino-Clear-Session`, `X-Trino-Added-Prepare`, `X-Trino-Deallocated-Prepare`) and an `updateType`.

#### Scenario: USE
- **WHEN** a client runs `USE lake.sales`
- **THEN** the response carries `X-Trino-Set-Catalog: lake`, `X-Trino-Set-Schema: sales` and `updateType` = `USE`

#### Scenario: Prepared statement round trip
- **WHEN** a client runs `PREPARE q1 FROM SELECT n FROM numbers WHERE n > ? AND n < ?`, then `EXECUTE q1 USING 20, 23` sending the returned statement in `X-Trino-Prepared-Statement`
- **THEN** the execute returns rows 21 and 22

### Requirement: Metadata browsing
The system SHALL answer `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`, `SHOW COLUMNS`/`DESCRIBE` and `SHOW SESSION` with Trino's column names, and SHALL serve `information_schema.{schemata,tables,columns,views}` per catalog, `system.jdbc.{catalogs,schemas,tables,columns,table_types,types}` and `system.metadata.table_comments` with Trino's column layouts, executing the client's SQL over them through the engine.

#### Scenario: information_schema with filters and ordering
- **WHEN** a client runs `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'default' AND table_name = 'typed' ORDER BY ordinal_position`
- **THEN** the columns are returned in ordinal order with Trino type names

#### Scenario: JDBC getTables
- **WHEN** a client runs `SELECT TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE FROM system.jdbc.tables WHERE TABLE_CAT = 'lake'`
- **THEN** each table in catalog `lake` is returned with type `TABLE`

### Requirement: Server info
The system SHALL answer `GET /v1/info` with `nodeVersion.version`, `coordinator`, `starting` = false, `environment` and `uptime`, and `GET /v1/info/state` with `"ACTIVE"`.

#### Scenario: Health check
- **WHEN** a client requests `/v1/info`
- **THEN** it receives `starting: false` and a version string of the form `<trino-compat>-arneb-<version>`

### Requirement: Web UI visibility
The system SHALL register each Trino-protocol query with the query tracker and transition it through its lifecycle (finished, failed or cancelled), so it appears in the Web UI.

#### Scenario: Query listed in Web UI
- **WHEN** a Trino client runs a query on a standalone node
- **THEN** `GET /api/v1/queries` on the Web UI lists it with its SQL and final state
