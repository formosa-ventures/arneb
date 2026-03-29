## MODIFIED Requirements

### Requirement: Connection handler wiring
Each spawned connection handler SHALL receive clones of `Arc<CatalogManager>` and `Arc<ConnectorRegistry>` from the ProtocolServer. The handler SHALL construct per-connection resources (QueryPlanner, ExecutionContext) using these shared dependencies. Query execution SHALL be performed by awaiting `plan.execute()` directly in the async context, without using `tokio::task::spawn_blocking`.

#### Scenario: Handler executes queries asynchronously
- **WHEN** a client sends a SQL query via the Simple Query flow
- **THEN** the handler parses, plans, and executes the query using async execution
- **AND** no `spawn_blocking` is used for query execution

#### Scenario: Handler streams results to client
- **WHEN** query execution produces a `SendableRecordBatchStream`
- **THEN** the handler consumes batches from the stream and encodes each as PostgreSQL DataRow messages
- **AND** results are sent to the client as batches become available

#### Scenario: Handler receives shared state
- **WHEN** a new connection is accepted
- **THEN** the connection handler has access to the server's CatalogManager and ConnectorRegistry
- **AND** can use them to resolve tables and create data sources for query execution
