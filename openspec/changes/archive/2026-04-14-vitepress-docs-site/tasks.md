## 1. Project Scaffold

- [x] 1.1 Create `docs/` directory with `package.json` (VitePress as dev dependency, pnpm scripts: `docs:dev`, `docs:build`, `docs:preview`)
- [x] 1.2 Create `docs/.vitepress/config.ts` with site title, description, base URL, local search, and empty nav/sidebar placeholders
- [x] 1.3 Create `docs/index.md` hero page with project name, tagline, "Get Started" and "GitHub" action buttons
- [x] 1.4 Create `docs/.gitignore` to exclude `.vitepress/cache/` and `.vitepress/dist/`
- [x] 1.5 Run `pnpm install` and verify `pnpm docs:dev` starts successfully

## 2. Navigation and Theme

- [x] 2.1 Configure nav bar in `config.ts` with links: Guide, SQL Reference, Connectors, Architecture, and GitHub repo link
- [x] 2.2 Configure sidebar groups in `config.ts`: guide (4 pages), sql (4 pages), connectors (4 pages), architecture (1 page)
- [x] 2.3 Add brand color customization to the theme config (hero gradient, accent colors)

## 3. Guide Content

- [x] 3.1 Write `docs/guide/introduction.md` — what is Arneb, features, status, supported data sources
- [x] 3.2 Write `docs/guide/quickstart.md` — prerequisites, build, start server, connect with psql, first query
- [x] 3.3 Write `docs/guide/configuration.md` — arneb.toml reference, all fields with types/defaults, env vars, CLI args, precedence, example configs
- [x] 3.4 Write `docs/guide/distributed.md` — coordinator/worker roles, cluster config, Flight RPC, port mapping, multi-node example

## 4. SQL Reference Content

- [x] 4.1 Write `docs/sql/overview.md` — supported statements list with descriptions and links
- [x] 4.2 Write `docs/sql/expressions.md` — CASE, COALESCE, NULLIF, CAST, BETWEEN, IN, LIKE, IS NULL, operators, subqueries with syntax and examples
- [x] 4.3 Write `docs/sql/functions.md` — all 19 scalar functions grouped by category (string, math, date) with signatures, return types, and examples
- [x] 4.4 Write `docs/sql/advanced.md` — CTEs, window functions, set operations, GROUP BY with HAVING, with syntax and examples

## 5. Connectors Content

- [x] 5.1 Write `docs/connectors/overview.md` — connector model, DataSource trait, pushdown capabilities
- [x] 5.2 Write `docs/connectors/file.md` — CSV and Parquet config, `[[tables]]` examples
- [x] 5.3 Write `docs/connectors/object-store.md` — S3/GCS/Azure config, credential precedence, MinIO/LocalStack setup
- [x] 5.4 Write `docs/connectors/hive.md` — `[[catalogs]]` config, HMS setup, per-catalog storage overrides, docker compose demo walkthrough

## 6. Architecture and Contributing

- [x] 6.1 Write `docs/architecture/overview.md` — crate map, query data flow pipeline, design principles, key dependencies
- [x] 6.2 Write `docs/contributing.md` — dev prerequisites, build/test/lint commands, TPC-H benchmark, PR workflow, docs update reminder

## 7. GitHub Pages Deployment

- [x] 7.1 Create `.github/workflows/docs.yml` with path filter (`docs/**`), pnpm 10 + Node 24 setup, `pnpm install --frozen-lockfile`, `pnpm docs:build`
- [x] 7.2 Add `actions/configure-pages`, `actions/upload-pages-artifact`, and `actions/deploy-pages` steps with `pages: write` and `id-token: write` permissions
- [x] 7.3 Add concurrency group (`pages`, `cancel-in-progress: false`)

## 8. Verification

- [x] 8.1 Run `pnpm docs:build` and verify all pages build without errors or broken links
- [x] 8.2 Run `pnpm docs:preview` and manually verify navigation, sidebar, search, hero page, and all content pages render correctly
