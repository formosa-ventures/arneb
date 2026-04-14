## Why

Arneb has no public-facing documentation site. All project knowledge lives in `CLAUDE.md`, inline code comments, and scattered `README.md` files. Users, contributors, and evaluators need a browsable, searchable documentation site to understand Arneb's capabilities, get started, and reference SQL syntax and configuration. VitePress provides a static-site generator optimized for technical documentation with minimal configuration, fast build times, and Markdown-first authoring — fitting Arneb's developer-oriented audience.

## What Changes

- Add a new `docs/` directory at the project root containing VitePress configuration, theme customization, and Markdown content pages.
- Add documentation content covering: project introduction, getting started guide, SQL reference, configuration reference, architecture overview, connector guide (file, Hive, object store), distributed deployment, and contributing guide.
- Add `pnpm` scripts and VitePress dev/build tooling in `docs/package.json`.
- Add a GitHub Actions workflow to build and deploy the docs site to GitHub Pages on pushes to `main`.
- Update the root-level project structure to include the `docs/` workspace.

## Capabilities

### New Capabilities
- `docs-site-scaffold`: VitePress project setup, directory structure, theme configuration, navigation, and local dev/build scripts.
- `docs-content`: Documentation pages — introduction, quickstart, SQL reference, configuration reference, architecture overview, connectors guide, distributed mode guide, and contributing guide.
- `docs-deployment`: GitHub Actions workflow for building the VitePress site and deploying to GitHub Pages.

### Modified Capabilities
_(none — this change introduces a standalone docs site with no impact on existing specs)_

## Impact

- **New directory**: `docs/` at project root with VitePress config, Markdown pages, and `package.json`.
- **Dependencies**: VitePress (npm) added as a dev dependency in `docs/package.json`. No Rust dependency changes.
- **CI**: New GitHub Actions workflow (`.github/workflows/docs.yml`) for automated deployment.
- **Existing code**: No changes to any Rust crate or the `web/` Web UI. The docs site is fully independent.
- **Hosting**: GitHub Pages (static), zero runtime cost.
