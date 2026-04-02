## ADDED Requirements

### Requirement: Automated release PR creation
release-please SHALL create a release PR on pushes to main that contain releasable commits (feat, fix, or breaking changes).

#### Scenario: Feature commit triggers release PR
- **WHEN** a `feat:` commit is pushed to main
- **THEN** release-please creates or updates a release PR with a MINOR version bump

#### Scenario: Fix commit triggers release PR
- **WHEN** a `fix:` commit is pushed to main
- **THEN** release-please creates or updates a release PR with a PATCH version bump

#### Scenario: Breaking change triggers major bump
- **WHEN** a commit with `!` suffix or `BREAKING CHANGE:` footer is pushed to main
- **THEN** release-please creates or updates a release PR with a MAJOR version bump

### Requirement: Automated changelog generation
release-please SHALL auto-generate CHANGELOG.md entries from conventional commit messages.

#### Scenario: Changelog reflects commits
- **WHEN** a release PR is created
- **THEN** CHANGELOG.md includes categorized entries (Features, Bug Fixes, Breaking Changes) from commit messages since the last release

### Requirement: GitHub release creation
release-please SHALL create a GitHub release with a git tag when the release PR is merged.

#### Scenario: Release PR merged
- **WHEN** a release PR is merged to main
- **THEN** release-please creates a GitHub release with the version tag and changelog as the release body

### Requirement: Rust manifest version sync
release-please SHALL update version fields in workspace Cargo.toml files when creating release PRs.

#### Scenario: Cargo.toml version updated
- **WHEN** a release PR is created
- **THEN** the `version` field in the root Cargo.toml is updated to the new version
