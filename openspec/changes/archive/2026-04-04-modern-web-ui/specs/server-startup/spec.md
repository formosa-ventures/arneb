## MODIFIED Requirements

### Requirement: Startup banner
The system SHALL log a startup banner at `info` level after successful initialization, before entering the accept loop. The banner SHALL include: the server name (`arneb`), the listening address and port, the number of registered catalogs, the number of registered tables, and the Web UI URL when running in coordinator or standalone role. The Web UI URL SHALL be formatted as `http://{bind_address}:{port + 1000}`.

#### Scenario: Banner with tables
- **WHEN** the server starts with 2 configured tables in standalone mode on port 5432
- **THEN** the log output includes a banner showing the listening address, `"2 tables registered"`, and `"Web UI: http://127.0.0.1:6432"`

#### Scenario: Banner with no tables
- **WHEN** the server starts with no configured tables
- **THEN** the log output includes a banner showing the listening address and `"0 tables registered"`

#### Scenario: Worker role banner
- **WHEN** the server starts in worker role
- **THEN** the banner does not include a Web UI URL since workers do not serve the Web UI

## ADDED Requirements

### Requirement: Frontend build integration in CI
The system SHALL require a frontend build step before `cargo build` in the CI pipeline. The CI configuration SHALL install Node.js, run `pnpm install --frozen-lockfile` in the `web/` directory, and run `pnpm build` to output assets to `crates/server/frontend/`. The `cargo build` step SHALL run after the frontend build so that `rust-embed` includes the latest assets.

#### Scenario: CI builds frontend then backend
- **WHEN** CI runs the build pipeline
- **THEN** the pipeline executes `pnpm install --frozen-lockfile && pnpm build` in `web/` before running `cargo build`

#### Scenario: Frontend build failure blocks cargo build
- **WHEN** `pnpm build` fails due to a TypeScript error
- **THEN** the CI pipeline fails before reaching `cargo build`

### Requirement: Development workflow without frontend build
The system SHALL allow Rust development without rebuilding the frontend. When `crates/server/frontend/` contains stale or placeholder assets, `cargo build` SHALL succeed and the binary SHALL serve whatever assets are embedded. Developers working only on the backend SHALL not need Node.js installed.

#### Scenario: Backend-only development
- **WHEN** a developer runs `cargo build` without running `pnpm build` first
- **THEN** the build succeeds using whatever frontend assets currently exist in `crates/server/frontend/`

#### Scenario: Frontend developer workflow
- **WHEN** a frontend developer runs `pnpm dev` in the `web/` directory
- **THEN** Vite dev server starts with hot module replacement, proxying API calls to a running Arneb instance
