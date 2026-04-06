## Context

The Arneb Web UI is currently a vanilla HTML/JS/CSS application consisting of three files (`index.html`, `app.js`, `style.css`) embedded via `rust-embed` from `crates/server/frontend/`. It provides real-time monitoring across three views (Dashboard, Queries, Cluster) with 2-second auto-refresh, communicating with 6 REST API endpoints under `/api/v1/*`.

As the query engine matures (TPC-H 16/22 passing, distributed mode complete), the frontend needs a proper component framework for maintainability, type safety, and a professional user experience. The backend API layer and `rust-embed` serving infrastructure remain unchanged — this is a frontend-only replacement.

Key constraints:
- The Arneb binary must remain self-contained (no external static file server)
- Backend Rust code changes should be minimal to none (API endpoints stay the same)
- Developers working only on the Rust backend should not need Node.js
- The existing 6 API endpoints provide all data the UI needs

## Goals / Non-Goals

**Goals:**
- Replace vanilla JS with a typed, component-based React application
- Provide dark/light mode with a polished, accessible UI via shadcn/ui
- Maintain all existing functionality (Dashboard, Queries, Cluster pages)
- Add query detail view with SQL syntax highlighting
- Responsive design supporting desktop, tablet, and mobile
- Clean separation: frontend source in `web/`, build output in `crates/server/frontend/`

**Non-Goals:**
- New backend API endpoints — the existing 6 endpoints are sufficient
- Server-side rendering — this is a client-rendered SPA
- Authentication or authorization — Arneb has no auth layer
- Real-time WebSocket connections — polling at 2s intervals is adequate
- Internationalization (i18n) — English only
- E2E testing framework — unit/component tests only for this change

## Decisions

### D1: Project location — `web/` at project root

**Decision**: Place the frontend source in `web/` at the repository root, not inside `crates/server/`.

**Rationale**: Separating the frontend from the Rust workspace keeps concerns clean. The `web/` directory has its own `package.json`, `tsconfig.json`, and `vite.config.ts`. Build output goes to `crates/server/frontend/` for `rust-embed`. This avoids polluting the Cargo workspace with Node.js tooling artifacts.

**Alternatives considered**:
- `crates/server/frontend-src/` — too deeply nested, couples frontend to one Rust crate
- Separate repository — overkill for a UI that's tightly coupled to Arneb's API

### D2: Vite 8 as build tool

**Decision**: Use Vite 8 with the React plugin for development and production builds.

**Rationale**: Vite provides near-instant HMR during development and optimized production builds. Vite 8 is the latest stable version with first-class React support via `@vitejs/plugin-react`. The build output is configured via `build.outDir` to write directly to `crates/server/frontend/`.

**Alternatives considered**:
- Webpack — slower dev server, more configuration complexity
- Rspack — newer, less ecosystem support for plugins
- Turbopack — still maturing, Next.js-focused

### D3: Tailwind CSS v4 via Vite plugin

**Decision**: Use Tailwind CSS v4 integrated via `@tailwindcss/vite`, not the PostCSS plugin.

**Rationale**: Tailwind v4 introduces a CSS-first configuration model using `@theme` directives directly in CSS files, eliminating the need for `tailwind.config.js`. The Vite plugin provides the fastest development experience. Design tokens (colors, spacing, fonts) are defined in `web/src/styles/globals.css` using CSS custom properties that adapt to light/dark mode.

**Alternatives considered**:
- Tailwind v3 + PostCSS — older model, requires JS config file
- CSS Modules — less utility-first, more boilerplate
- Styled-components — runtime overhead, not needed with Tailwind

### D4: shadcn/ui as component library

**Decision**: Use shadcn/ui installed via CLI into `web/src/components/ui/`.

**Rationale**: shadcn/ui provides accessible, well-designed components built on Radix UI primitives. Components are source-owned (copied into the project), allowing full customization without forking a library. This fits Arneb's self-contained philosophy — no runtime dependency on an external component library CDN.

**Minimum component set**: Button, Card, Table, Badge, Select, DropdownMenu, Tooltip, Separator, Sheet (mobile sidebar).

**Alternatives considered**:
- Radix UI directly — more work to style, shadcn/ui already does this
- Headless UI — less comprehensive component coverage
- Ant Design / MUI — heavy bundles, opinionated styling that conflicts with Tailwind

### D5: React Router v7 for client-side routing

**Decision**: Use React Router v7 in SPA mode with `BrowserRouter`.

**Rationale**: React Router is the de facto standard for React SPAs. The existing `rust-embed` fallback handler already returns `index.html` for unknown routes, which is exactly the SPA fallback needed. Routes: `/` (Dashboard), `/queries` (list), `/queries/:id` (detail), `/cluster`.

**Alternatives considered**:
- TanStack Router — type-safe but smaller ecosystem
- Wouter — minimalist but lacks nested layout support

### D6: SQL syntax highlighting with a lightweight library

**Decision**: Use `highlight.js` with only the SQL language pack, or a minimal regex-based highlighter.

**Rationale**: The query detail page needs SQL syntax highlighting. A full code editor (Monaco, CodeMirror) is overkill for read-only display. `highlight.js` with the SQL language pack adds ~15KB gzipped. Alternatively, a simple regex-based highlighter (~2KB) that colors keywords, strings, and numbers would suffice for the limited SQL display use case.

**Implementation**: Start with `highlight.js/sql` for correctness. If bundle size becomes a concern, replace with a custom regex highlighter.

**Alternatives considered**:
- Monaco Editor — ~2MB, extreme overkill for read-only display
- CodeMirror 6 — ~100KB, still too heavy for display-only
- Prism.js — similar size to highlight.js, less maintained

### D7: Dark/light mode via CSS class strategy

**Decision**: Use the `class` strategy — toggle `dark` class on `<html>` element, with preference stored in `localStorage` under key `arneb-theme`.

**Rationale**: Tailwind v4 supports class-based dark mode natively. shadcn/ui's theming is built around CSS custom properties that switch based on the `.dark` class. The `localStorage` approach persists across sessions, with `prefers-color-scheme` media query as the initial default for first-time visitors.

### D8: API client using native fetch

**Decision**: Use a thin typed wrapper around the native `fetch` API in `web/src/lib/api.ts`.

**Rationale**: The Arneb API has 6 endpoints with simple request/response shapes. A full HTTP client library (axios, ky) adds unnecessary bundle size. The typed wrapper provides autocompletion and type checking while keeping the dependency footprint minimal.

### D9: Auto-refresh via custom hook with Page Visibility API

**Decision**: Implement a `useAutoRefresh(fetchFn, intervalMs)` hook that pauses when the tab is hidden.

**Rationale**: The current vanilla JS refreshes every 2 seconds unconditionally. The React version improves on this by pausing polling when the tab is not visible (via `document.visibilityState`), reducing unnecessary network requests. The hook returns `{ data, isLoading, error, refresh }` for clean consumption by page components.

## Project Structure

```
web/
├── package.json
├── tsconfig.json
├── vite.config.ts
├── index.html                    # Vite entry HTML
├── src/
│   ├── main.tsx                  # React mount point
│   ├── App.tsx                   # Router + layout shell
│   ├── styles/
│   │   └── globals.css           # Tailwind imports + CSS custom properties
│   ├── components/
│   │   ├── ui/                   # shadcn/ui components (Button, Card, etc.)
│   │   ├── layout/
│   │   │   ├── Sidebar.tsx       # Navigation sidebar
│   │   │   ├── Header.tsx        # Top header with theme toggle
│   │   │   └── Layout.tsx        # Shell combining sidebar + header + outlet
│   │   ├── QueryStateBadge.tsx   # Reusable query state badge
│   │   └── ThemeToggle.tsx       # Dark/light mode toggle button
│   ├── pages/
│   │   ├── Dashboard.tsx
│   │   ├── Queries.tsx
│   │   ├── QueryDetail.tsx
│   │   ├── Cluster.tsx
│   │   └── NotFound.tsx
│   ├── hooks/
│   │   └── useAutoRefresh.ts     # Polling hook with visibility pause
│   ├── lib/
│   │   ├── api.ts                # Typed API client
│   │   ├── types.ts              # API response types
│   │   └── utils.ts              # formatDuration, cn() helper
│   └── theme/
│       └── theme-provider.tsx    # Theme context + localStorage persistence
```

## Build Pipeline

```
Developer workflow:
  cd web && pnpm dev       → Vite dev server (port 5173) with HMR
                                 Proxy /api/* → localhost:6432

Production build:
  cd web && pnpm build     → Output to ../../crates/server/frontend/
  cargo build                 → rust-embed includes new assets

CI pipeline:
  1. cd web && pnpm install --frozen-lockfile && pnpm build
  2. cargo build --release
  3. cargo test
  4. cargo clippy -- -D warnings
```

The Vite dev server proxies API requests to a running Arneb instance, allowing frontend development with live data. The proxy is configured in `vite.config.ts`:

```ts
server: {
  proxy: {
    '/api': 'http://localhost:6432'
  }
}
```

## Risks / Trade-offs

**[Node.js build dependency]** → The frontend build requires Node.js, adding a tool dependency. Mitigated by: (1) pre-built assets are committed or CI-built, so backend-only developers never need Node.js; (2) CI handles the build automatically.

**[Bundle size increase]** → React + Tailwind + shadcn/ui will produce larger assets than the current 3 vanilla files (~9KB total). Estimated output: ~150-300KB gzipped. Mitigated by: Vite's tree-shaking, Tailwind's purge, and code-splitting per route. The binary size impact is modest compared to Arneb's total binary size.

**[Build output in git]** → The `crates/server/frontend/` directory contains build output that `rust-embed` needs at compile time. Two options: (1) commit build output so `cargo build` works without Node.js, or (2) require frontend build before `cargo build`. Decision: commit a minimal placeholder set of files. CI always rebuilds fresh. Add `web/node_modules/` to `.gitignore`.

**[Vite version churn]** → Vite 8 is current but frontend tooling evolves rapidly. Mitigated by: minimal Vite config surface, no custom plugins, standard React setup that ports easily to future versions.

**[Two dev servers during development]** → Frontend developers need both Vite dev server and Arneb running. Mitigated by: Vite proxy makes this transparent — only one browser tab needed, API calls route automatically.

## Open Questions

- **Commit build output?** Should `crates/server/frontend/` contain committed build artifacts for zero-Node.js `cargo build`, or should it require `pnpm build` first? Recommend: commit a minimal placeholder, CI always rebuilds.
- **SQL highlighter choice**: Start with `highlight.js/sql` or go with a custom regex highlighter from day one? Recommend: start with highlight.js for correctness, optimize later if needed.
