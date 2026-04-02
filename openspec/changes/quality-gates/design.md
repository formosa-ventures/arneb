## Context

Arneb is a Rust-based distributed SQL engine with 10 workspace crates, 33+ unit tests, 3 integration test suites, and a TPC-H benchmark. All quality checks are currently manual (`cargo fmt`, `cargo clippy`, `cargo test`). The project uses Apache-2.0 license and conventional commits. No `.github/` directory exists.

## Goals / Non-Goals

**Goals:**
- Automate all quality checks (format, lint, test, build) on every push and PR
- Enforce supply-chain security via dependency auditing
- Track code coverage trends and prevent coverage regression on PRs
- Automate dependency updates to catch vulnerabilities promptly
- Automate releases with semantic versioning and changelog generation
- Provide professional open-source governance files for contributor onboarding

**Non-Goals:**
- Benchmark regression tracking in CI (TPC-H queries take too long for per-PR runs)
- Miri testing (no unsafe code in the codebase)
- Windows cross-compilation support
- Pre-commit hooks (CI enforcement is sufficient)
- Docker image publishing (separate concern)

## Decisions

### 1. CI matrix: stable + MSRV on Linux + macOS

**Decision**: Run CI on `{stable, 1.75}` x `{ubuntu-latest, macos-latest}`.

**Rationale**: MSRV 1.75 follows the common N-2 policy for Rust projects. macOS is included because the primary developer works on macOS and it catches platform-specific issues early. Windows is excluded per non-goals.

**Alternative considered**: Only run on `ubuntu-latest` — rejected because macOS-specific issues would go undetected.

### 2. cargo-deny over cargo-audit

**Decision**: Use `cargo-deny` with a `deny.toml` configuration.

**Rationale**: cargo-deny covers four audit categories (licenses, advisories, bans, sources) while cargo-audit only covers advisories. For an Apache-2.0 project, license compliance is critical — we need to ensure no GPL-only dependencies leak in.

**Alternative considered**: cargo-audit — simpler but only covers security advisories, not license compliance.

### 3. cargo-llvm-cov over cargo-tarpaulin

**Decision**: Use `cargo-llvm-cov` for code coverage instrumentation.

**Rationale**: llvm-cov uses LLVM's native instrumentation, producing more accurate results. cargo-tarpaulin has known issues with async code and macros, both of which are prevalent in Arneb (tokio, arrow macros).

**Alternative considered**: cargo-tarpaulin — simpler setup but less accurate for async/macro-heavy Rust code.

### 4. Codecov for coverage reporting

**Decision**: Upload coverage to Codecov with PR status checks.

**Rationale**: Free for open-source, integrates well with GitHub, provides PR diff coverage comments, and supports coverage thresholds. The `informational = true` setting initially avoids blocking PRs until baseline is established.

### 5. release-please for release automation

**Decision**: Use Google's release-please GitHub Action.

**Rationale**: Integrates naturally with conventional commits (already in use per CLAUDE.md). Automatically generates CHANGELOG.md entries, creates release PRs with version bumps, and creates GitHub releases with tags.

**Alternative considered**: cargo-release — requires manual invocation, doesn't auto-generate changelogs from commits.

### 6. Workflow separation

**Decision**: Separate workflows into distinct files by concern:
- `ci.yml` — fmt, clippy, test, build (on push/PR)
- `coverage.yml` — llvm-cov + Codecov upload (on push/PR to main)
- `deny.yml` — cargo-deny check (on push/PR + weekly schedule)
- `release.yml` — release-please (on push to main)

**Rationale**: Separate workflows allow independent re-runs, clearer failure messages, and different trigger conditions. A single monolithic workflow would delay feedback and make it harder to identify which gate failed.

### 7. MSRV enforcement via rust-version field

**Decision**: Set `rust-version = "1.75"` in the workspace root `Cargo.toml` and use `toolchain` in CI matrix.

**Rationale**: The `rust-version` field in Cargo.toml is the standard way to declare MSRV. Combined with CI matrix testing, this ensures compatibility is both documented and enforced.

## Risks / Trade-offs

- **CI time**: Full matrix (4 combinations) may take 10-15 minutes. → Mitigation: Use `cargo build` caching with `Swatinem/rust-cache` action.
- **Codecov token**: Public repos can use tokenless uploads, but it may be unreliable. → Mitigation: Configure a Codecov token as a GitHub secret as fallback.
- **MSRV breakage**: New dependencies may require newer Rust versions. → Mitigation: CI catches this immediately; update MSRV deliberately with a dedicated commit.
- **release-please learning curve**: Requires strict conventional commit format. → Mitigation: Already documented in project conventions (CLAUDE.md / bcy-GIT.md).
- **cargo-deny false positives**: Some transitive dependencies may have license issues. → Mitigation: Use `exceptions` list in deny.toml for known-safe exceptions.
