## ADDED Requirements

### Requirement: VitePress project initialization
The `docs/` directory at the project root SHALL contain a valid VitePress project with `package.json`, VitePress as a dev dependency, and pnpm as the package manager. The project SHALL be independent from the `web/` directory (no shared pnpm workspace).

#### Scenario: Fresh clone and install
- **WHEN** a developer clones the repository and runs `cd docs && pnpm install`
- **THEN** all dependencies are installed successfully and `pnpm docs:dev` starts the VitePress dev server

#### Scenario: Independent from web UI
- **WHEN** the `web/` directory is deleted or its dependencies are not installed
- **THEN** `cd docs && pnpm install && pnpm docs:dev` still succeeds without errors

### Requirement: VitePress configuration
The file `docs/.vitepress/config.ts` SHALL define the site configuration including: site title ("Arneb"), site description, base URL (configurable for GitHub Pages), theme configuration with nav bar and sidebar, and search enabled via the built-in local search.

#### Scenario: Site metadata is set
- **WHEN** the VitePress site is built
- **THEN** the HTML output contains `<title>` with "Arneb" and a meta description referencing a distributed SQL query engine

#### Scenario: Base URL for GitHub Pages
- **WHEN** VitePress is configured with `base: '/arneb/'`
- **THEN** all generated asset and link paths are prefixed with `/arneb/`

### Requirement: Navigation bar
The VitePress config SHALL define a top-level nav bar with links to: Guide, SQL Reference, Connectors, Architecture, and a link to the GitHub repository.

#### Scenario: Nav bar renders all sections
- **WHEN** a user visits any page on the docs site
- **THEN** the nav bar displays links labeled "Guide", "SQL Reference", "Connectors", "Architecture", and a GitHub icon/link

### Requirement: Sidebar navigation
The VitePress config SHALL define sidebar groups that map to content sections. Each sidebar group SHALL list its child pages in logical reading order.

#### Scenario: Guide sidebar
- **WHEN** a user navigates to any page under `/guide/`
- **THEN** the sidebar displays links to: Introduction, Quickstart, Configuration, and Distributed Mode

#### Scenario: SQL sidebar
- **WHEN** a user navigates to any page under `/sql/`
- **THEN** the sidebar displays links to: Overview, Expressions, Functions, and Advanced

#### Scenario: Connectors sidebar
- **WHEN** a user navigates to any page under `/connectors/`
- **THEN** the sidebar displays links to: Overview, File Connector, Object Store, and Hive

### Requirement: Theme customization
The VitePress theme SHALL be customized with Arneb brand colors. The hero page SHALL display a project tagline and call-to-action buttons linking to the quickstart guide and GitHub repository.

#### Scenario: Hero page renders
- **WHEN** a user visits the docs site root (`/`)
- **THEN** the page displays the project name "Arneb", a tagline describing it as a distributed SQL query engine, a "Get Started" button linking to `/guide/quickstart`, and a "GitHub" button linking to the repository

### Requirement: Local development scripts
The `docs/package.json` SHALL define scripts: `docs:dev` (start VitePress dev server with hot reload), `docs:build` (produce static site output), and `docs:preview` (serve the built site locally for verification).

#### Scenario: Dev server with hot reload
- **WHEN** a developer runs `pnpm docs:dev` and edits a Markdown file
- **THEN** the browser automatically reflects the change without a manual refresh

#### Scenario: Production build
- **WHEN** a developer runs `pnpm docs:build`
- **THEN** VitePress outputs a static site to `docs/.vitepress/dist/` with no errors

#### Scenario: Preview built site
- **WHEN** a developer runs `pnpm docs:preview` after a successful build
- **THEN** a local HTTP server starts serving the built static site
