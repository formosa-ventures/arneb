## Why

Operating a distributed query engine requires visibility into system state. Without a Web UI, operators must rely on logs and CLI tools to understand query progress, identify slow queries, and monitor cluster health. A built-in Web UI on the coordinator provides real-time monitoring — similar to Trino's web interface.

## What Changes

- Add HTTP server (axum) to coordinator alongside pgwire and Flight
- Implement REST API endpoints for queries, cluster status, and server info
- Build embedded SPA frontend (vanilla HTML/CSS/JS) for dashboard, query list, query detail, and cluster views
- Auto-refresh for live query monitoring

## Capabilities

### New Capabilities

- `web-ui-dashboard`: Main dashboard with query summary and cluster health
- `query-detail-view`: Detailed view for individual query (plan, stages, tasks, progress)
- `cluster-overview`: Worker list with health, capacity, active tasks
- `query-history-api`: REST API endpoints serving query and cluster data

### Modified Capabilities

- `server-startup`: Coordinator starts HTTP server for Web UI

## Impact

- **Crates**: server (extend)
- **Dependencies**: axum, tower-http, serde_json, rust-embed
