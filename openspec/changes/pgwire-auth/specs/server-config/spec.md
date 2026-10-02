## ADDED Requirements

### Requirement: [auth] config section
The system SHALL accept an optional `[auth]` TOML section with `type` (`"none"` by default, or `"password"`) and a `[[auth.users]]` list of `{ name, password_hash }` entries. `password_hash` SHALL be a PostgreSQL-format SCRAM-SHA-256 verifier. Plaintext passwords SHALL NOT be accepted: unknown keys, including `password`, are rejected. The effective auth mode and user count SHALL be logged at startup on the `arneb::config` target for the coordinator and standalone roles. Debug output SHALL redact stored verifiers.

#### Scenario: Section omitted
- **WHEN** the config has no `[auth]` section
- **THEN** the server starts in `none` mode

#### Scenario: Password mode with users
- **WHEN** `type = "password"` and two valid users are configured
- **THEN** the server starts and logs `auth="password (scram-sha-256)" users=2`

#### Scenario: Invalid auth config
- **WHEN** `type = "password"` has no users, a user has a malformed `password_hash`, a user name is duplicated or empty, `type` is unknown, or a user entry uses a `password` key
- **THEN** startup fails with a configuration error naming the problem

#### Scenario: Users configured while auth is none
- **WHEN** `type = "none"` and `[[auth.users]]` entries are present
- **THEN** the server starts in `none` mode and logs a warning that the users are ignored

### Requirement: hash-password subcommand
The `arneb` binary SHALL provide a `hash-password` subcommand. It reads one password line from stdin and prints a SCRAM-SHA-256 verifier to stdout, using a random 16-byte salt and 4096 iterations. The output is suitable for `password_hash`.

#### Scenario: Generate a verifier
- **WHEN** the user runs `echo -n 'pw' | arneb hash-password`
- **THEN** stdout contains a single `SCRAM-SHA-256$4096:...` line that authenticates password `pw`
