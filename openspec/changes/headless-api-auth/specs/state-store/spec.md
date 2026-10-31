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
On every start the server SHALL apply `[[auth.users]]` to the users seeded from the config: it SHALL add new users, SHALL replace the verifier of a user whose `password_hash` changed and revoke all of that user's tokens, and SHALL delete a config-seeded user that is no longer in the config together with their tokens. Each change SHALL be logged on `arneb::config` naming the user and never a secret. Users created through the state store SHALL NOT be changed by seeding. When a config user's name collides with a state-created user, the server SHALL keep the state user and log a WARN, unless started with `--reseed-state`, which SHALL overwrite it with the config user and revoke its tokens.

#### Scenario: Changed password
- **WHEN** a user's `password_hash` in the config differs from the state
- **THEN** after startup the new password works, the old one fails, and the user's previous tokens are rejected

#### Scenario: User removed from the config
- **WHEN** a config-seeded user is no longer in `[[auth.users]]`
- **THEN** after startup the user and all of their tokens are gone

#### Scenario: Unchanged config
- **WHEN** the config's users match the state
- **THEN** startup changes nothing and existing tokens keep working

#### Scenario: State-created user
- **WHEN** a user was created through the state store
- **THEN** seeding never changes or deletes it; a config user of the same name logs a WARN and `--reseed-state` overwrites it

### Requirement: In-memory fallback
With no `[state] path`, the server SHALL keep the state in memory and SHALL log a WARN `no [state] path: API tokens are lost on restart`.

#### Scenario: No path
- **WHEN** password auth is on and no path is configured
- **THEN** startup logs the WARN and issuing a token returns `persistent: false` with a hint
