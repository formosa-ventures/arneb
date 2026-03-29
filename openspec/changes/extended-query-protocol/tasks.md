## 1. QueryParser Implementation

- [x] 1.1 Create `TrinoQueryParser` struct implementing pgwire's `QueryParser` trait with `Statement = String` — `parse_sql()` stores the SQL string as-is
- [x] 1.2 Write unit test: parse SQL with and without `$N` placeholders returns the SQL string

## 2. Parameter Binding

- [x] 2.1 Implement `bind_parameters(sql: &str, params: &[Option<String>]) -> String` that replaces `$1`, `$2`, ... with literal values (quoted strings, unquoted numbers, NULL)
- [x] 2.2 Write unit tests: integer param, string param (with quoting), NULL param, multiple params, no params

## 3. ExtendedQueryHandler Implementation

- [x] 3.1 Create `TrinoExtendedQueryHandler` struct holding `Arc<CatalogManager>` and `Arc<ConnectorRegistry>` (same state as `ConnectionHandler`)
- [x] 3.2 Implement `do_query()`: extract SQL from portal, bind parameters, run parse→plan→optimize→execute pipeline, return `Response::Query` with encoded results
- [x] 3.3 Implement `do_describe_statement()`: parse and plan SQL (with dummy params if needed), return `DescribeStatementResponse` with column metadata
- [x] 3.4 Implement `do_describe_portal()`: parse and plan bound SQL, return `DescribePortalResponse` with column metadata
- [x] 3.5 Refactor shared execution logic: extract the parse→plan→optimize→execute path into a reusable function shared by SimpleQueryHandler and ExtendedQueryHandler

## 4. Wire into HandlerFactory

- [x] 4.1 Replace `PlaceholderExtendedQueryHandler` with `TrinoExtendedQueryHandler` in `HandlerFactory`
- [x] 4.2 Update `PgWireHandlerFactory` type alias for `ExtendedQueryHandler`

## 5. Integration Tests

- [x] 5.1 Test: connect with `tokio-postgres` using default mode (extended query), execute `SELECT 1`, verify result
- [x] 5.2 Test: execute parameterized query `SELECT $1::int + $2::int` with params `[3, 4]`, verify result is `7`
- [x] 5.3 Test: execute query against actual table with parameter in WHERE clause
- [x] 5.4 Test: Simple Query mode still works (no regression)

## 6. Quality

- [x] 6.1 `cargo build` compiles without warnings
- [x] 6.2 `cargo test` — all tests pass
- [x] 6.3 `cargo clippy -- -D warnings` — clean
- [x] 6.4 `cargo fmt -- --check` — clean
