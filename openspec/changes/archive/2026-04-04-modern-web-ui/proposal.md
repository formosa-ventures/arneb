## Why

The current Arneb Web UI is a vanilla HTML/JS/CSS application (~3 files) with no component framework, no type safety, and limited extensibility. As the query engine matures (TPC-H 16/22 passing, distributed mode complete), users need a richer monitoring and management interface. Rebuilding with Vite 8, Tailwind CSS, and shadcn/ui provides a modern, maintainable foundation with accessible components, dark mode, and a professional look—matching the quality of the backend.

## What Changes

- **BREAKING**: Replace the existing `crates/server/frontend/` vanilla HTML/JS/CSS with a new Vite 8 + React + TypeScript SPA
- Add Tailwind CSS v4 for utility-first styling with design tokens
- Integrate shadcn/ui component library for accessible, consistent UI primitives
- Restructure frontend as a standalone project at `web/` (project root) with a build step that outputs to `crates/server/frontend/` for rust-embed
- Preserve all existing pages: Dashboard, Queries, Cluster
- Enhance the UI with:
  - Responsive layout with sidebar navigation
  - Dark/light mode toggle
  - Query detail view with SQL syntax highlighting
  - Real-time auto-refresh with visual indicators
  - Improved worker status cards with health indicators
- Backend API (`/api/v1/*`) and rust-embed serving remain unchanged

## Capabilities

### New Capabilities
- `web-ui-app`: Vite 8 + React + TypeScript application scaffold, routing, build pipeline, and rust-embed integration
- `web-ui-dashboard`: Dashboard page with stats cards, recent queries table, and cluster overview
- `web-ui-queries`: Query list with filtering, query detail view with SQL display, and cancel action
- `web-ui-cluster`: Cluster info, worker list with health status, and standalone mode handling
- `web-ui-theme`: Tailwind CSS + shadcn/ui theming, dark/light mode, and design tokens

### Modified Capabilities
- `server-startup`: Build process must run `pnpm build` (or embed pre-built assets) before `cargo build`; the `frontend/` embed path may change

## Impact

- **Frontend**: Complete replacement of `crates/server/frontend/` (3 files → full React app build output)
- **New directory**: `web/` at project root containing the Vite + React source
- **Dependencies**: Node.js / pnpm required for frontend development (not for running the binary)
- **Build pipeline**: CI must install Node.js and build frontend before `cargo build`
- **Backend**: No Rust code changes to API endpoints; `rust-embed` path in `frontend.rs` may need updating if output directory changes
- **Binary size**: Slightly larger due to bundled JS/CSS assets (estimated +200-500KB gzipped)
