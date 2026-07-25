## ADDED Requirements

### Requirement: Single declared release version

The project's release version SHALL be declared in `[workspace.package].version` in the root `Cargo.toml`. Every crate in the workspace MUST inherit it via `version.workspace = true` rather than declaring a literal version.

The `benchmarks/tpch` crate is excluded from the workspace (`exclude = ["benchmarks/tpch"]` in the root `Cargo.toml`) and therefore cannot inherit. Its `version` field MUST be kept equal to the workspace version, and any procedure that bumps the release version MUST update both locations.

#### Scenario: Bumping the release version

- **WHEN** the release version is changed in `[workspace.package].version`
- **THEN** `benchmarks/tpch/Cargo.toml` is updated to the same version in the same commit, and no crate under `crates/` declares a literal version that could drift from it

#### Scenario: Version consistency is checkable

- **WHEN** the version string is read from `[workspace.package].version` and from `benchmarks/tpch/Cargo.toml`
- **THEN** the two values are identical

### Requirement: Release version is observable at runtime

A running Arneb server SHALL report its release version through every interface that exposes version information, and each MUST derive it from the compiled-in package version rather than a separately maintained constant.

The interfaces are:
- the CLI `--version` flag,
- the SQL `version()` function,
- the Web UI server-info endpoint.

#### Scenario: CLI reports the release version

- **WHEN** a user runs `arneb --version`
- **THEN** the output contains the version declared in `[workspace.package].version`

#### Scenario: SQL version() reports the release version

- **WHEN** a client executes `SELECT version()` over the PostgreSQL wire protocol
- **THEN** the returned string names Arneb and the version declared in `[workspace.package].version`

#### Scenario: Web UI reports the release version

- **WHEN** a client requests the Web UI server-info endpoint
- **THEN** the response's version field equals the version declared in `[workspace.package].version`

### Requirement: PostgreSQL compatibility level is distinct from the release version

`SHOW server_version` SHALL report the PostgreSQL wire-protocol compatibility level that Arneb advertises to clients, NOT Arneb's own release version. Bumping the release version MUST NOT change this value.

Clients such as JDBC drivers, psycopg2, and DBeaver branch on `server_version` to select catalog queries and protocol features; reporting an Arneb version here would be interpreted as a PostgreSQL version and break client capability detection.

#### Scenario: server_version is unaffected by a release bump

- **WHEN** the release version is bumped from one value to another
- **THEN** `SHOW server_version` returns the same PostgreSQL compatibility level as before the bump

#### Scenario: Release version and compatibility level are separately reported

- **WHEN** a client executes both `SELECT version()` and `SHOW server_version` against the same server
- **THEN** `version()` returns Arneb's release version and `SHOW server_version` returns the PostgreSQL compatibility level, and the two values differ

### Requirement: Changelog records each release

The repository SHALL contain a `CHANGELOG.md` at its root with one section per released version, newest first. Each section MUST state the version, and MUST call out any change that breaks an existing user-facing interface, script, or file path under an explicitly labelled heading.

#### Scenario: A release adds a changelog section

- **WHEN** a version is released
- **THEN** `CHANGELOG.md` contains a section for that version describing what it delivers

#### Scenario: A breaking change is disclosed

- **WHEN** a release removes or renames a user-facing script, command, or file path
- **THEN** that release's changelog section labels it as breaking and names the replacement, if one exists

### Requirement: Releases are tagged in git

Each release SHALL be marked by a git tag of the form `v<version>` on the commit that carries the corresponding version declarations and changelog section.

#### Scenario: Tag matches the declared version

- **WHEN** a release tag `vX.Y.Z` is checked out
- **THEN** `[workspace.package].version` at that commit equals `X.Y.Z` and `CHANGELOG.md` contains a section for `X.Y.Z`
