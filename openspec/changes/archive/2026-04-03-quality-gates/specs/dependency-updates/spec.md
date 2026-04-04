## ADDED Requirements

### Requirement: Automated dependency update PRs
Dependabot SHALL be configured to create pull requests for Cargo dependency updates on a weekly schedule.

#### Scenario: Weekly dependency check
- **WHEN** the weekly Dependabot schedule triggers
- **THEN** Dependabot creates PRs for any outdated dependencies in the Cargo workspace

### Requirement: GitHub Actions version updates
Dependabot SHALL also monitor GitHub Actions workflow dependencies for version updates.

#### Scenario: Action version update available
- **WHEN** a GitHub Action used in workflows has a newer version
- **THEN** Dependabot creates a PR to update the action version

### Requirement: Update grouping
Dependabot SHALL group minor and patch updates into a single PR to reduce noise.

#### Scenario: Grouped minor/patch updates
- **WHEN** multiple dependencies have minor or patch updates available
- **THEN** Dependabot groups them into a single PR instead of creating one PR per dependency
