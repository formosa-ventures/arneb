## MODIFIED Requirements

### Requirement: Theme customization
The VitePress theme SHALL be customized with Arneb brand colors. The hero page SHALL display a project tagline and call-to-action buttons linking to the quickstart guide and GitHub repository.

The homepage SHALL additionally present the official comparison figures for the current release, below the hero and features. The comparison content MUST include:
- a suite-level comparison against Trino drawn from the published official run,
- the scale factor and run date the figures came from,
- a link to the document that reproduces them.

The comparison content MUST be authored as Markdown within `docs/index.md` following the home-layout frontmatter, so that no custom theme entry point or component is required to render it.

#### Scenario: Hero page renders
- **WHEN** a user visits the docs site root (`/`)
- **THEN** the page displays the project name "Arneb", a tagline describing it as a distributed SQL query engine, a "Get Started" button linking to `/guide/quickstart`, and a "GitHub" button linking to the repository

#### Scenario: Homepage shows sourced comparison figures
- **WHEN** a user scrolls past the hero and features on the docs site root
- **THEN** they see the current release's comparison figures against Trino, labelled with the scale factor and run date, and a link to the reproduction document

#### Scenario: Comparison renders without a custom theme
- **WHEN** the docs site is built with `pnpm docs:build`
- **THEN** the comparison content appears on the rendered homepage, and `docs/.vitepress/` contains no theme entry point or component file added for it
