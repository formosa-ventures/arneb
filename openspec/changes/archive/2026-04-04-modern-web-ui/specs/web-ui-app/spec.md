## ADDED Requirements

### Requirement: Vite 8 React TypeScript project scaffold
The system SHALL provide a frontend application at `web/` in the project root, built with Vite 8, React 19, and TypeScript. The project SHALL use `pnpm` as the package manager. The `tsconfig.json` SHALL enable `strict` mode. The entry point SHALL be `web/src/main.tsx` which mounts the React app to a root DOM element.

#### Scenario: Fresh project setup
- **WHEN** a developer clones the repository and runs `pnpm install` in the `web/` directory
- **THEN** all dependencies are installed and `pnpm dev` starts a Vite dev server with hot module replacement

#### Scenario: TypeScript strict mode
- **WHEN** a developer writes code with implicit `any` types or null-unsafe access
- **THEN** the TypeScript compiler reports errors and the build fails

### Requirement: Client-side routing with React Router
The system SHALL use React Router v7 for client-side navigation between pages. The routes SHALL be: `/` (Dashboard), `/queries` (Queries list), `/queries/:id` (Query detail), and `/cluster` (Cluster). The application SHALL render a persistent layout shell (sidebar + header) with page content swapped per route.

#### Scenario: Navigate between pages
- **WHEN** the user clicks a navigation link in the sidebar
- **THEN** the URL updates and the corresponding page component renders without a full page reload

#### Scenario: Direct URL access
- **WHEN** a user navigates directly to `/queries` in the browser
- **THEN** the server returns `index.html` (SPA fallback) and React Router renders the Queries page

#### Scenario: Unknown route
- **WHEN** a user navigates to an undefined route like `/settings`
- **THEN** the application renders a 404 "Page Not Found" view with a link back to the dashboard

### Requirement: Application layout shell
The system SHALL render a persistent layout with a collapsible sidebar navigation and a top header bar. The sidebar SHALL contain navigation links for Dashboard, Queries, and Cluster pages. The sidebar SHALL highlight the currently active route. The header SHALL display the Arneb logo/name and a theme toggle button.

#### Scenario: Sidebar navigation rendering
- **WHEN** the application loads
- **THEN** the sidebar displays navigation links for Dashboard, Queries, and Cluster with icons

#### Scenario: Active route highlighting
- **WHEN** the user is on the Queries page
- **THEN** the Queries link in the sidebar has a visually distinct active state (background highlight)

#### Scenario: Sidebar collapse on mobile
- **WHEN** the viewport width is less than 768px
- **THEN** the sidebar collapses to an icon-only mode or a hamburger menu overlay

### Requirement: API client module
The system SHALL provide a centralized API client module at `web/src/lib/api.ts` that wraps all `/api/v1/*` endpoint calls. Each endpoint SHALL have a typed function returning a Promise of the response type. The client SHALL use the native `fetch` API. The base URL SHALL be derived from `window.location.origin` to support any deployment host.

#### Scenario: Fetch queries list
- **WHEN** the application calls `getQueries()`
- **THEN** the function sends `GET /api/v1/queries` and returns a typed `QueriesResponse` object

#### Scenario: Fetch queries filtered by state
- **WHEN** the application calls `getQueries("RUNNING")`
- **THEN** the function sends `GET /api/v1/queries?state=RUNNING` and returns only running queries

#### Scenario: Cancel a query
- **WHEN** the application calls `cancelQuery(queryId)`
- **THEN** the function sends `DELETE /api/v1/queries/{queryId}` and returns void on success

#### Scenario: API error handling
- **WHEN** an API call returns a non-2xx status code
- **THEN** the client throws an error with the HTTP status and response body for the caller to handle

### Requirement: Production build outputs to rust-embed directory
The system SHALL configure Vite's build output to `crates/server/frontend/` so that `cargo build` embeds the latest frontend assets via `rust-embed`. The build command SHALL be `pnpm build` executed from the `web/` directory. The output SHALL include a single `index.html`, hashed JS bundles, and hashed CSS bundles.

#### Scenario: Production build
- **WHEN** a developer runs `pnpm build` from the `web/` directory
- **THEN** the build output is written to `crates/server/frontend/` containing `index.html`, JS, and CSS files

#### Scenario: Cargo build serves new frontend
- **WHEN** the frontend is built and `cargo build` is run
- **THEN** the compiled Arneb binary serves the new React frontend at the web UI port

#### Scenario: Build output is clean
- **WHEN** `pnpm build` runs
- **THEN** the previous contents of `crates/server/frontend/` are removed before new output is written

### Requirement: Auto-refresh data fetching
The system SHALL provide a custom React hook `useAutoRefresh` that polls an API endpoint at a configurable interval (default 2 seconds). The hook SHALL return the latest data, loading state, error state, and a manual refresh function. Polling SHALL pause when the browser tab is not visible (using the Page Visibility API) and resume when the tab becomes visible again.

#### Scenario: Dashboard auto-refresh
- **WHEN** the Dashboard page is mounted
- **THEN** query and cluster data refresh automatically every 2 seconds

#### Scenario: Tab hidden pauses refresh
- **WHEN** the user switches to a different browser tab
- **THEN** polling stops until the user returns to the Arneb tab

#### Scenario: Manual refresh
- **WHEN** the user clicks a refresh button
- **THEN** data is fetched immediately regardless of the polling interval
