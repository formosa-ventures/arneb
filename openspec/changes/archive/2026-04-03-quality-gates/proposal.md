## Why

Arneb has no automated CI/CD or quality control — all checks (fmt, clippy, test) are manual. As a distributed SQL engine preparing for open-source release, the project needs professional quality gates to ensure code quality, supply-chain security, and a welcoming contributor experience.

## What Changes

- Add GitHub Actions CI pipeline (fmt, clippy, test, build) with matrix support (stable + MSRV x Linux + macOS)
- Add `cargo-deny` configuration for license audit, vulnerability scanning, and duplicate dependency detection
- Add code coverage tracking with `cargo-llvm-cov` + Codecov integration
- Add Dependabot configuration for automated dependency updates
- Add release automation via `release-please`
- Add project governance files: CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md, CHANGELOG.md
- Add GitHub issue/PR templates for standardized collaboration
- Document branch protection rules (manual GitHub settings)

## Capabilities

### New Capabilities
- `ci-pipeline`: GitHub Actions workflows for automated quality checks (fmt, clippy, test, build) with MSRV matrix
- `dependency-audit`: cargo-deny configuration for license, advisory, ban, and source auditing
- `code-coverage`: cargo-llvm-cov + Codecov integration for coverage tracking on PRs
- `dependency-updates`: Dependabot configuration for automated dependency update PRs
- `release-automation`: release-please integration for automated versioning, tagging, and changelog generation
- `project-governance`: CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md, and GitHub issue/PR templates

### Modified Capabilities

(none — these are all new infrastructure additions, no existing spec behavior changes)

## Impact

- **New files**: `.github/workflows/`, `.github/dependabot.yml`, `.github/ISSUE_TEMPLATE/`, `.github/pull_request_template.md`, `deny.toml`, `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `CHANGELOG.md`
- **Modified files**: root `Cargo.toml` (add `rust-version` field for MSRV policy)
- **Dependencies**: `cargo-deny` and `cargo-llvm-cov` as CI-only tools (not project dependencies)
- **External services**: Codecov (free for open-source), GitHub Actions, release-please GitHub App
- **Branch protection**: Requires manual configuration in GitHub repository settings
