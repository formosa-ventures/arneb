## ADDED Requirements

### Requirement: Coverage report generation
The CI SHALL generate code coverage reports using `cargo-llvm-cov` on pushes and PRs to the main branch.

#### Scenario: Coverage report produced
- **WHEN** CI runs on a push or PR targeting main
- **THEN** an LCOV coverage report is generated for the entire workspace

### Requirement: Coverage upload to Codecov
The CI SHALL upload coverage reports to Codecov for tracking and PR comments.

#### Scenario: Coverage uploaded successfully
- **WHEN** coverage report is generated
- **THEN** the report is uploaded to Codecov and a coverage summary appears as a PR comment

### Requirement: Coverage threshold (informational)
The coverage check SHALL initially run in informational mode (`informational = true`) to establish a baseline without blocking PRs.

#### Scenario: Coverage drop does not block initially
- **WHEN** a PR reduces overall coverage while informational mode is active
- **THEN** Codecov reports the drop as a comment but does NOT fail the status check

#### Scenario: Coverage threshold enforcement (future)
- **WHEN** informational mode is disabled after baseline is established
- **THEN** PRs that reduce coverage below the threshold SHALL fail the Codecov status check
