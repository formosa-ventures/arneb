## ADDED Requirements

### Requirement: Automated format checking
The CI pipeline SHALL run `cargo fmt -- --check` on every push and pull request to ensure consistent code formatting across the workspace.

#### Scenario: Format check passes
- **WHEN** a push or PR contains properly formatted code
- **THEN** the format check job succeeds

#### Scenario: Format check fails
- **WHEN** a push or PR contains improperly formatted code
- **THEN** the format check job fails and blocks merging

### Requirement: Automated lint checking
The CI pipeline SHALL run `cargo clippy -- -D warnings` on every push and pull request to catch common mistakes and enforce idiomatic Rust.

#### Scenario: Clippy passes with clean code
- **WHEN** a push or PR contains code with no clippy warnings
- **THEN** the clippy job succeeds

#### Scenario: Clippy fails on warnings
- **WHEN** a push or PR contains code that triggers clippy warnings
- **THEN** the clippy job fails and blocks merging

### Requirement: Automated test execution
The CI pipeline SHALL run `cargo test` on every push and pull request to verify all unit and integration tests pass.

#### Scenario: All tests pass
- **WHEN** a push or PR does not break any existing tests
- **THEN** the test job succeeds

#### Scenario: Test failure blocks merge
- **WHEN** a push or PR causes one or more tests to fail
- **THEN** the test job fails and blocks merging

### Requirement: Release build verification
The CI pipeline SHALL run `cargo build --release` on every push and pull request to ensure the project compiles in release mode without errors.

#### Scenario: Release build succeeds
- **WHEN** a push or PR compiles cleanly in release mode
- **THEN** the build job succeeds

### Requirement: MSRV matrix testing
The CI pipeline SHALL test against both stable Rust and the declared MSRV (1.75) on both Linux and macOS runners.

#### Scenario: Matrix covers all combinations
- **WHEN** CI is triggered
- **THEN** jobs run on {stable, 1.75} x {ubuntu-latest, macos-latest} (4 combinations)

#### Scenario: MSRV breakage detected
- **WHEN** a change introduces code or dependencies requiring a Rust version newer than 1.75
- **THEN** the MSRV matrix job fails

### Requirement: Build caching
The CI pipeline SHALL use Rust build caching (Swatinem/rust-cache) to reduce CI execution time.

#### Scenario: Cached build is faster
- **WHEN** CI runs on a branch with no dependency changes since the last run
- **THEN** the build uses cached artifacts and completes faster than a cold build
