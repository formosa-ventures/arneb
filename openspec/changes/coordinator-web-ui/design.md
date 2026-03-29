## Context

The coordinator already manages queries via QueryTracker and knows cluster state via NodeRegistry. The Web UI exposes this data through a REST API and HTML frontend.

## Goals / Non-Goals

**Goals:**

- REST API for programmatic and UI access to query/cluster state
- Embedded SPA frontend (no separate build step, no Node.js)
- Live auto-refresh on dashboard
- Query cancellation from UI

**Non-Goals:**

- Query editor / SQL submission — use psql or other SQL clients
- Historical analytics / charts — simple tables and status indicators only
- Authentication — open access for Phase 2

## Decisions

### D1: axum for HTTP

**Choice**: Use axum as the HTTP framework. Already used for Flight endpoints (via tonic). axum routes merged with existing server.

**Rationale**: Avoids adding a second HTTP framework. axum is lightweight, async-native, and integrates well with tokio and tonic.

### D2: Embedded frontend

**Choice**: Use rust-embed to bundle HTML/CSS/JS files at compile time. No runtime file serving. Single binary deployment.

**Rationale**: Simplifies deployment — no need to ship static files alongside the binary. The SPA is small (100-500KB) so binary size impact is negligible.

### D3: REST API format

**Choice**: JSON responses. Endpoints under `/api/v1/`. UI under `/`.

**Rationale**: JSON is universally supported by HTTP clients and JavaScript `fetch()`. Versioned API path allows future evolution.

### D4: API endpoints

**Choice**:
- `GET /api/v1/queries?state=RUNNING` — list queries
- `GET /api/v1/queries/{id}` — query detail
- `DELETE /api/v1/queries/{id}` — cancel query
- `GET /api/v1/cluster` — cluster overview
- `GET /api/v1/cluster/workers` — worker list
- `GET /api/v1/info` — server version, uptime

**Rationale**: RESTful resource-based design. DELETE for cancellation follows HTTP semantics (removing a running resource).

### D5: Frontend tech

**Choice**: Vanilla HTML + CSS + `fetch()`. No framework. Simple, maintainable. Dashboard polls every 2 seconds.

**Rationale**: No build toolchain needed. The UI is simple enough that a framework adds complexity without benefit. Polling at 2s provides near-real-time updates without excessive load.

**Alternative**: WebSocket for push updates. Rejected because polling is simpler and adequate for dashboard use cases.

## Risks / Trade-offs

**[No auth]** → Anyone with network access can view queries and cancel them. **Mitigation**: Acceptable for Phase 2. Authentication will be added in a dedicated change.

**[Embedded assets increase binary size]** → Typically 100-500KB for simple SPA. **Mitigation**: Negligible compared to the rest of the binary.

**[Polling overhead]** → 2-second polling from multiple browser tabs could add load. **Mitigation**: API responses are small JSON payloads. The coordinator can handle hundreds of requests per second easily.
