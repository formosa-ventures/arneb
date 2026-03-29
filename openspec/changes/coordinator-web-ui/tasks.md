## 1. HTTP Server Setup

- [x] 1.1 Add axum, tower-http, serde_json, rust-embed to server crate dependencies
- [x] 1.2 Create axum Router with API and static file routes
- [x] 1.3 Start HTTP server on configurable port (default 8080) alongside pgwire and Flight
- [x] 1.4 Wire into coordinator startup (only in coordinator/standalone mode)

## 2. REST API — Queries

- [x] 2.1 GET /api/v1/queries — list queries with optional state filter
- [x] 2.2 GET /api/v1/queries/{id} — query detail (state, SQL, plan, stages, tasks, timing)
- [x] 2.3 DELETE /api/v1/queries/{id} — cancel query
- [x] 2.4 Define JSON response types with serde Serialize

## 3. REST API — Cluster

- [x] 3.1 GET /api/v1/cluster — cluster overview (coordinator info, worker count)
- [x] 3.2 GET /api/v1/cluster/workers — worker list (id, address, status, active tasks, last heartbeat)
- [x] 3.3 GET /api/v1/info — server version, uptime, role

## 4. Frontend — Dashboard

- [x] 4.1 Create index.html with navigation (Dashboard, Queries, Cluster)
- [x] 4.2 Create dashboard view: running/completed/failed query counts, cluster health summary
- [x] 4.3 Implement auto-refresh (fetch /api/v1/queries every 2 seconds)
- [x] 4.4 Add CSS styling for clean, readable layout

## 5. Frontend — Query Views

- [x] 5.1 Create query list view: table with columns (ID, SQL preview, state, duration, started)
- [x] 5.2 Create query detail view: SQL text, execution plan, stage progress bars, task table
- [x] 5.3 Add cancel button calling DELETE /api/v1/queries/{id}

## 6. Frontend — Cluster View

- [x] 6.1 Create cluster view: worker cards showing status, capacity, active tasks
- [x] 6.2 Show coordinator info (version, uptime, port)

## 7. Static Asset Embedding

- [x] 7.1 Set up rust-embed for frontend files
- [x] 7.2 Configure axum to serve embedded assets for non-API routes
- [x] 7.3 Verify single binary includes all frontend assets

## 8. Integration Tests

- [x] 8.1 Test API endpoints return correct JSON for running queries
- [x] 8.2 Test query cancellation via API
- [x] 8.3 Test cluster endpoint shows registered workers
- [x] 8.4 Verify all existing tests pass

## 9. Quality

- [x] 9.1 `cargo build` compiles without warnings
- [x] 9.2 `cargo test` — all tests pass
- [x] 9.3 `cargo clippy -- -D warnings` — clean
- [x] 9.4 `cargo fmt -- --check` — clean
