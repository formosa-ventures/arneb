## ADDED Requirements

### Requirement: GitHub Actions docs workflow
The file `.github/workflows/docs.yml` SHALL define a GitHub Actions workflow that builds the VitePress site and deploys it to GitHub Pages. The workflow SHALL trigger on pushes to `main` that modify files under `docs/`.

#### Scenario: Docs change triggers deployment
- **WHEN** a commit is pushed to `main` that modifies any file under `docs/`
- **THEN** the GitHub Actions workflow runs and deploys the updated site to GitHub Pages

#### Scenario: Non-docs change skips deployment
- **WHEN** a commit is pushed to `main` that only modifies Rust source files (no `docs/` changes)
- **THEN** the docs deployment workflow does not run

### Requirement: Workflow build steps
The workflow SHALL: checkout the repository, set up pnpm and Node.js (matching the versions used in CI: pnpm 10, Node 24), install dependencies with `pnpm install --frozen-lockfile`, and build the site with `pnpm docs:build`.

#### Scenario: Reproducible build
- **WHEN** the workflow runs
- **THEN** it uses `--frozen-lockfile` to ensure the build uses the exact dependency versions from the lockfile

#### Scenario: Build output is valid
- **WHEN** `pnpm docs:build` completes in the workflow
- **THEN** the `docs/.vitepress/dist/` directory contains `index.html` and all generated assets

### Requirement: GitHub Pages deployment
The workflow SHALL use `actions/configure-pages`, `actions/upload-pages-artifact`, and `actions/deploy-pages` to deploy the built static site. The workflow SHALL request `pages: write` and `id-token: write` permissions.

#### Scenario: Successful deployment
- **WHEN** the build step succeeds
- **THEN** the workflow uploads `docs/.vitepress/dist/` as a Pages artifact and deploys it, making the site accessible at the GitHub Pages URL

#### Scenario: Build failure prevents deployment
- **WHEN** the VitePress build fails (e.g., broken Markdown, config error)
- **THEN** the deployment step does not execute and the workflow reports failure

### Requirement: Deployment concurrency control
The workflow SHALL use a concurrency group (e.g., `pages`) with `cancel-in-progress: false` to prevent overlapping deployments. Only one deployment SHALL run at a time.

#### Scenario: Concurrent pushes
- **WHEN** two commits are pushed to `main` in quick succession, both modifying `docs/`
- **THEN** the second workflow run waits for the first to complete rather than cancelling it
