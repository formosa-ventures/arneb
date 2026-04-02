## ADDED Requirements

### Requirement: Contribution guidelines
The project SHALL provide a CONTRIBUTING.md with instructions for setting up the development environment, running tests, coding conventions, commit message format, and PR submission process.

#### Scenario: New contributor reads CONTRIBUTING.md
- **WHEN** a potential contributor visits the repository
- **THEN** CONTRIBUTING.md provides clear step-by-step instructions to build, test, and submit changes

### Requirement: Security policy
The project SHALL provide a SECURITY.md that describes how to report security vulnerabilities responsibly.

#### Scenario: Security researcher finds vulnerability
- **WHEN** a security researcher discovers a vulnerability
- **THEN** SECURITY.md provides a private reporting mechanism (email or GitHub Security Advisories)

### Requirement: Code of conduct
The project SHALL provide a CODE_OF_CONDUCT.md based on the Contributor Covenant to set community behavior expectations.

#### Scenario: Community member reads code of conduct
- **WHEN** a participant joins the project
- **THEN** CODE_OF_CONDUCT.md clearly defines expected behavior, unacceptable behavior, and enforcement procedures

### Requirement: Issue templates
The project SHALL provide GitHub issue templates for bug reports and feature requests with structured fields.

#### Scenario: User files a bug report
- **WHEN** a user creates a new issue and selects "Bug Report"
- **THEN** the template prompts for: description, steps to reproduce, expected behavior, actual behavior, environment info

#### Scenario: User requests a feature
- **WHEN** a user creates a new issue and selects "Feature Request"
- **THEN** the template prompts for: problem description, proposed solution, alternatives considered

### Requirement: Pull request template
The project SHALL provide a pull request template that prompts for summary, type of change, testing checklist, and related issues.

#### Scenario: Contributor opens a PR
- **WHEN** a contributor creates a new pull request
- **THEN** the PR body is pre-filled with the template sections
