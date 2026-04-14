## Context

Arneb is a Rust-based distributed SQL query engine. The project already has a `web/` directory containing a React + Vite Web UI that gets embedded into the server binary via `rust-embed`. There is no documentation site. CI uses pnpm 10 and Node 24. The `web/` directory has its own `pnpm-workspace.yaml`; there is no root-level pnpm workspace. The existing CI builds the frontend in `web/` and uploads the output as an artifact consumed by subsequent Rust build jobs.

The docs site must be fully independent from the `web/` UI — different toolchain (VitePress vs Vite+React), different output target (GitHub Pages vs embedded binary), different content lifecycle.

## Goals / Non-Goals

**Goals:**
- Provide a browsable, searchable documentation site for Arneb covering setup, SQL reference, configuration, architecture, and connectors.
- Use VitePress with minimal custom theme work — leverage the default theme with brand colors and logo.
- Enable local authoring with hot reload (`pnpm docs:dev`).
- Automate deployment to GitHub Pages on every push to `main`.
- Keep the docs site completely decoupled from the `web/` React UI and Rust build.

**Non-Goals:**
- API reference auto-generated from Rust doc comments (`cargo doc` serves that purpose separately).
- Internationalization / multi-language support.
- Blog or changelog section (release-please handles release notes).
- Custom VitePress plugins or Vue components beyond basic theme configuration.
- Search backend — VitePress ships with local MiniSearch; no external search service.

## Decisions

### 1. Directory: `docs/` at project root

Place the VitePress project in `docs/` at the repository root with its own `package.json`.

**Why over nesting under `web/`**: The `web/` directory is a React SPA that gets compiled and embedded into the Arneb binary. The docs site has a completely different lifecycle (static site → GitHub Pages). Mixing them would couple unrelated build pipelines. A top-level `docs/` directory is the VitePress convention and keeps concerns separate.

**Why not a separate repository**: The docs should version-lock with the code. Keeping them in-repo means PRs can update docs alongside code changes.

### 2. Standalone pnpm project (no shared workspace)

`docs/` will have its own `package.json` and `pnpm-lock.yaml`, independent of `web/`. No root-level pnpm workspace will be created.

**Why over a monorepo workspace**: The `web/` and `docs/` projects share zero dependencies or build steps. A pnpm workspace would add configuration complexity for no benefit. The CI workflow for docs will `cd docs && pnpm install && pnpm build` independently, mirroring how `web/` is handled today.

### 3. VitePress default theme with brand customization

Use the VitePress default theme. Customize only: site title, logo, hero section, brand colors (Arneb's palette), nav bar links, and sidebar structure.

**Why over a custom theme**: The default theme covers all needs — sidebar navigation, search, dark mode, mobile responsive. Custom themes require ongoing maintenance. VitePress's default theme is well-tested and accessible.

### 4. Content structure mirroring user journeys

```
docs/
├── .vitepress/
│   └── config.ts          # Site config, nav, sidebar
├── index.md               # Landing / hero page
├── guide/
│   ├── introduction.md    # What is Arneb, feature overview
│   ├── quickstart.md      # Install, run, first query in <5 min
│   ├── configuration.md   # arneb.toml reference, env vars, CLI args
│   └── distributed.md     # Coordinator/worker setup, Flight RPC
├── sql/
│   ├── overview.md        # SQL dialect overview, supported statements
│   ├── expressions.md     # CASE, COALESCE, CAST, operators, subqueries
│   ├── functions.md       # 19 scalar functions reference
│   └── advanced.md        # CTEs, window functions, set operations
├── connectors/
│   ├── overview.md        # Connector model, DataSource trait
│   ├── file.md            # CSV, Parquet, local filesystem
│   ├── object-store.md    # S3, GCS, Azure configuration
│   └── hive.md            # Hive Metastore catalog, HMS 4.x setup
├── architecture/
│   └── overview.md        # Crate map, data flow, design principles
├── contributing.md         # Dev setup, testing, PR workflow
├── package.json
└── pnpm-lock.yaml
```

**Why this grouping**: Matches how users approach the project — "get started" → "write queries" → "connect data" → "understand internals" → "contribute". Each top-level section becomes a sidebar group.

### 5. GitHub Pages deployment via dedicated workflow

Add `.github/workflows/docs.yml` triggered on pushes to `main` that touch `docs/**`. Uses `actions/configure-pages`, builds VitePress, and deploys with `actions/deploy-pages`.

**Why a separate workflow (not extending CI)**: The docs build is independent of the Rust build. It doesn't need frontend artifacts or Cargo. A dedicated workflow keeps CI fast and avoids unnecessary coupling. Path filtering (`docs/**`) means Rust-only changes don't trigger a docs rebuild.

**Why GitHub Pages over Vercel/Netlify**: Zero configuration for public GitHub repos, no external account needed, consistent with the project's existing GitHub-centric tooling (Actions, release-please).

### 6. Base URL configuration

Configure VitePress `base` to `/<repo-name>/` for GitHub Pages project site hosting. This will be parameterized so it can be overridden for custom domain setups later.

## Risks / Trade-offs

- **Content freshness**: Docs can drift from code. → Mitigation: keep docs in the same repo so PRs can update both. Add a note in `CONTRIBUTING.md` to update docs when changing user-facing behavior.
- **VitePress version churn**: VitePress is actively developed and may introduce breaking changes. → Mitigation: pin to a specific major version in `package.json`. pnpm lockfile ensures reproducible builds.
- **GitHub Pages limits**: 1 GB storage, 100 GB/month bandwidth, 10 builds/hour. → Mitigation: static docs site will be well under these limits. Path-filtered triggers prevent excessive builds.
- **No preview deploys for PRs**: GitHub Pages only deploys from `main`. → Mitigation: contributors can preview locally with `pnpm docs:dev`. PR preview deploys can be added later if needed.

## Open Questions

- **Custom domain**: Should the docs be served from a custom domain (e.g., `arneb.dev`) or the default `<user>.github.io/<repo>/`? This affects the VitePress `base` config. Can be changed post-deploy.
- **Logo/branding assets**: Does Arneb have a logo to use in the docs hero and nav bar, or should a placeholder be used initially?
