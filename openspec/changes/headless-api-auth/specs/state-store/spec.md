## ADDED Requirements

### Requirement: Durable coordinator state
The coordinator and standalone server SHALL keep users and API tokens in one versioned JSON state file at `[state] path` (`ARNEB_STATE_PATH`, env over file). Every write SHALL be atomic (write a temporary file, fsync, rename) and the file SHALL be created with mode `0600`. Workers SHALL NOT open the state store. The state store SHALL be used only under `[auth] type = "password"`.

#### Scenario: State survives a restart
- **WHEN** a token is issued, the coordinator restarts, and the same `[state] path` is configured
- **THEN** the token still authenticates

#### Scenario: Crash during a write
- **WHEN** a write is interrupted before the rename
- **THEN** the previous state file is intact and loads on the next start

#### Scenario: Workers
- **WHEN** a worker starts with `[state] path` set
- **THEN** it does not open the file

### Requirement: The config file seeds users
`[[auth.users]]` SHALL seed the state on first start only. On a later start whose config users differ from the state, the server SHALL log a WARN on `arneb::config` naming the user and SHALL keep the state. `--reseed-state` SHALL overwrite the config-seeded users with the config.

#### Scenario: Divergence
- **WHEN** a user's `password_hash` in the config differs from the state
- **THEN** startup logs a WARN naming the user and the state's verifier stays in force

#### Scenario: Reseed
- **WHEN** the server starts with `--reseed-state`
- **THEN** the state's config-seeded users match the config

### Requirement: In-memory fallback
With no `[state] path`, the server SHALL keep the state in memory and SHALL log a WARN `no [state] path: API tokens are lost on restart`.

#### Scenario: No path
- **WHEN** password auth is on and no path is configured
- **THEN** startup logs the WARN and issuing a token returns `persistent: false` with a hint
