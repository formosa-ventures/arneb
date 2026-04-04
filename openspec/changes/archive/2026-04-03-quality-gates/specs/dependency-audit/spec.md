## ADDED Requirements

### Requirement: License compliance audit
The dependency audit SHALL verify that all direct and transitive dependencies use licenses compatible with Apache-2.0. The `deny.toml` configuration SHALL explicitly list allowed licenses.

#### Scenario: Compatible license passes
- **WHEN** all dependencies use Apache-2.0, MIT, BSD-2-Clause, BSD-3-Clause, ISC, or Unicode-3.0 licenses
- **THEN** the license check passes

#### Scenario: Incompatible license detected
- **WHEN** a dependency uses a GPL-only or other incompatible license
- **THEN** the license check fails and the CI job blocks merging

### Requirement: Security advisory scanning
The dependency audit SHALL check all dependencies against the RustSec advisory database for known vulnerabilities.

#### Scenario: No known vulnerabilities
- **WHEN** no dependencies have active security advisories
- **THEN** the advisory check passes

#### Scenario: Vulnerability detected
- **WHEN** a dependency has an active security advisory in the RustSec database
- **THEN** the advisory check fails and reports the affected crate and advisory ID

### Requirement: Duplicate dependency detection
The dependency audit SHALL warn on duplicate versions of the same crate in the dependency tree to prevent unnecessary bloat.

#### Scenario: No duplicates
- **WHEN** the dependency tree contains at most one version of each crate
- **THEN** the ban check passes

#### Scenario: Duplicate detected
- **WHEN** the dependency tree contains multiple versions of the same crate
- **THEN** the ban check warns (configurable to deny)

### Requirement: Scheduled audit runs
The dependency audit SHALL run on a weekly schedule in addition to push/PR triggers, to catch newly disclosed vulnerabilities.

#### Scenario: Weekly scan catches new advisory
- **WHEN** a weekly scheduled run detects a new advisory for an existing dependency
- **THEN** the CI job fails and notifies via GitHub Actions notification
