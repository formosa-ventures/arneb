## 1. CI Pipeline

- [x] 1.1 Create `.github/workflows/ci.yml` with fmt, clippy, test, and release build jobs using matrix {stable, 1.75} x {ubuntu-latest, macos-latest} with Swatinem/rust-cache
- [x] 1.2 Add `rust-version = "1.75"` to workspace root `Cargo.toml` for MSRV declaration

## 2. Dependency Audit

- [x] 2.1 Create `deny.toml` with license allowlist (Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, Unicode-3.0), advisory checks, and duplicate ban warnings
- [x] 2.2 Create `.github/workflows/deny.yml` to run `cargo-deny check` on push/PR and weekly schedule

## 3. Code Coverage

- [x] 3.1 Create `.github/workflows/coverage.yml` to run `cargo-llvm-cov` and upload LCOV to Codecov
- [x] 3.2 Create `codecov.yml` with informational mode for project and patch coverage

## 4. Dependency Updates

- [x] 4.1 Create `.github/dependabot.yml` with weekly Cargo and GitHub Actions update schedules, grouped minor/patch updates

## 5. Release Automation

- [x] 5.1 Create `.github/workflows/release.yml` with release-please action for Rust workspace
- [x] 5.2 Create `release-please-config.json` and `.release-please-manifest.json` for Rust crate configuration
- [x] 5.3 Create initial `CHANGELOG.md`

## 6. Project Governance

- [x] 6.1 Create `CONTRIBUTING.md` with dev setup, build/test commands, coding conventions, commit format, and PR process
- [x] 6.2 Create `SECURITY.md` with vulnerability reporting policy (GitHub Security Advisories)
- [x] 6.3 Create `CODE_OF_CONDUCT.md` based on Contributor Covenant v2.1
- [x] 6.4 Create `.github/ISSUE_TEMPLATE/bug_report.yml` with structured fields (description, steps, expected, actual, environment)
- [x] 6.5 Create `.github/ISSUE_TEMPLATE/feature_request.yml` with structured fields (problem, solution, alternatives)
- [x] 6.6 Create `.github/pull_request_template.md` with summary, change type, testing checklist, related issues
