## 1. Project Scaffold & Build Pipeline

- [x] 1.1 Initialize Vite 8 + React + TypeScript project in `web/` with `pnpm create vite@latest`
- [x] 1.2 Install core dependencies: `react-router-dom`, `highlight.js`, `clsx`, `tailwind-merge`
- [x] 1.3 Configure `vite.config.ts`: set `build.outDir` to `../../crates/server/frontend/`, `build.emptyOutDir` to true, and API proxy to `http://localhost:6432`
- [x] 1.4 Configure `tsconfig.json` with strict mode and `@/` path alias pointing to `src/`
- [x] 1.5 Add `web/node_modules/` to root `.gitignore`

## 2. Tailwind CSS v4 & Theme Setup

- [x] 2.1 Install `@tailwindcss/vite` and `tailwindcss` v4, add Vite plugin to config
- [x] 2.2 Create `src/styles/globals.css` with Tailwind imports and semantic color tokens (background, foreground, card, primary, secondary, muted, accent, destructive, border) as CSS custom properties for both light and dark modes
- [x] 2.3 Create `src/theme/theme-provider.tsx` with ThemeContext that reads `localStorage('arneb-theme')`, falls back to `prefers-color-scheme`, and toggles `dark` class on `<html>`
- [x] 2.4 Create `src/components/ThemeToggle.tsx` button component using sun/moon icons

## 3. shadcn/ui Components

- [x] 3.1 Initialize shadcn/ui with `npx shadcn@latest init` — configure `src/components/ui/` path, CSS variables mode, and Tailwind v4 support
- [x] 3.2 Install minimum component set: `npx shadcn@latest add button card table badge select dropdown-menu tooltip separator sheet`
- [x] 3.3 Verify all components render correctly in both light and dark modes

## 4. Layout Shell & Routing

- [x] 4.1 Create `src/lib/utils.ts` with `cn()` helper (clsx + tailwind-merge) and `formatDuration()` utility
- [x] 4.2 Create `src/components/layout/Sidebar.tsx` with nav links (Dashboard, Queries, Cluster) using icons, active route highlighting via `useLocation`, and responsive collapse (full on desktop, icon-only on tablet, Sheet overlay on mobile)
- [x] 4.3 Create `src/components/layout/Header.tsx` with Arneb logo/name and ThemeToggle
- [x] 4.4 Create `src/components/layout/Layout.tsx` combining Sidebar + Header + `<Outlet />`
- [x] 4.5 Configure React Router v7 in `src/App.tsx` with routes: `/` (Dashboard), `/queries` (Queries), `/queries/:id` (QueryDetail), `/cluster` (Cluster), and `*` (NotFound)
- [x] 4.6 Update `src/main.tsx` to mount App with BrowserRouter and ThemeProvider

## 5. API Client & Shared Hooks

- [x] 5.1 Create `src/lib/types.ts` with TypeScript interfaces: `QueryResponse`, `QueriesResponse`, `ClusterResponse`, `WorkerResponse`, `InfoResponse`
- [x] 5.2 Create `src/lib/api.ts` with typed fetch wrappers: `getQueries(state?)`, `getQuery(id)`, `cancelQuery(id)`, `getCluster()`, `getWorkers()`, `getInfo()`
- [x] 5.3 Create `src/hooks/useAutoRefresh.ts` hook that polls at configurable interval (default 2s), pauses on tab hidden via Page Visibility API, and returns `{ data, isLoading, error, refresh }`
- [x] 5.4 Create `src/components/QueryStateBadge.tsx` shared badge component with color mapping (Running=blue, Finished=green, Failed=red, Queued=yellow, Cancelled=gray)

## 6. Dashboard Page

- [x] 6.1 Create `src/pages/Dashboard.tsx` with auto-refreshing data from queries and cluster endpoints
- [x] 6.2 Implement four stat cards (Running, Completed, Failed, Workers) using shadcn Card component with distinct icons/colors, showing "Standalone" for workers card when role is standalone
- [x] 6.3 Implement recent queries table showing 10 most recent queries with columns: ID (truncated 8 chars), SQL (truncated 80 chars), State (QueryStateBadge), Duration — rows clickable to navigate to `/queries/:id`
- [x] 6.4 Implement cluster overview summary section showing server role and worker health count

## 7. Queries Page & Detail View

- [x] 7.1 Create `src/pages/Queries.tsx` with auto-refreshing query list table and state filter dropdown (All, Running, Queued, Finished, Failed, Cancelled) using shadcn Select
- [x] 7.2 Implement Cancel button on running/queued query rows that calls `cancelQuery()` and refreshes the list
- [x] 7.3 Create `src/pages/QueryDetail.tsx` with full query ID, state badge, error panel (if failed), and cancel action (if running)
- [x] 7.4 Integrate highlight.js with SQL language pack for syntax-highlighted SQL display on the query detail page
- [x] 7.5 Add copy-to-clipboard button for the SQL text with brief "Copied!" toast/confirmation

## 8. Cluster Page

- [x] 8.1 Create `src/pages/Cluster.tsx` with auto-refreshing data from info and workers endpoints
- [x] 8.2 Implement server info section showing version, formatted uptime, and role badge
- [x] 8.3 Implement cluster health summary bar showing total/healthy/unhealthy worker counts and total split capacity
- [x] 8.4 Implement worker cards grid with worker ID, RPC address, alive/dead indicator (green/red dot), max splits, and last heartbeat time
- [x] 8.5 Implement standalone mode handling: show informational panel instead of empty worker list

## 9. Not Found & Polish

- [x] 9.1 Create `src/pages/NotFound.tsx` with 404 message and link back to dashboard
- [x] 9.2 Add empty state messages to all list views (no queries, no workers)
- [x] 9.3 Verify responsive layout at desktop (1280px), tablet (768px), and mobile (375px) breakpoints
- [x] 9.4 Verify dark/light mode renders correctly on all pages

## 10. Build Integration & CI

- [x] 10.1 Run `pnpm build` and verify output in `crates/server/frontend/` includes `index.html`, hashed JS and CSS bundles
- [x] 10.2 Run `cargo build` and verify the binary serves the new React frontend at the web UI port
- [x] 10.3 Test SPA fallback: direct navigation to `/queries` and `/cluster` routes returns the React app
- [x] 10.4 Update CI workflow to install Node.js and run `pnpm install --frozen-lockfile && pnpm build` in `web/` before `cargo build`
- [x] 10.5 Verify backend-only `cargo build` succeeds without Node.js (using existing/placeholder assets)
