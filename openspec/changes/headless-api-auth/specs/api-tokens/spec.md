## ADDED Requirements

### Requirement: API token lifecycle
An authenticated user SHALL be able to issue API tokens of the form `arneb_<16 hex>_<43 base64url>`. The plaintext SHALL be returned once and the server SHALL store only a verifier. A token SHALL have an optional name (at most 128 characters) and an optional expiry of 1 to 3650 days. A user SHALL have at most 100 live tokens. A token minted with another token SHALL NOT outlive it: its expiry is clamped to the parent's. Revoking a token SHALL NOT revoke tokens minted with it. Users SHALL only list and revoke their own tokens.

#### Scenario: Issue and list
- **WHEN** alice issues a token named `ci`
- **THEN** the response carries the plaintext token once, and `GET /api/v2/tokens` lists its id, name and times but not the secret

#### Scenario: Clamped expiry
- **WHEN** a token expiring in 1 day mints a token with `expires_in_days = 30`
- **THEN** the new token expires when the parent does, and the response says so in `hint`

#### Scenario: Too many tokens
- **WHEN** a user already holds 100 live tokens and issues another
- **THEN** the response is `invalid_argument` with a hint to revoke unused tokens

#### Scenario: Another user's token
- **WHEN** bob revokes alice's token id
- **THEN** the response is `not_found`

### Requirement: Tokens authenticate on every protocol
A live token SHALL be accepted as the user's password on pgwire (SCRAM) and on Trino HTTP Basic. A revoked or expired token SHALL be refused on the next connection or request on every protocol, and Trino SHALL NOT serve a revoked token from its verification cache. Session tokens (`arneb_s_…`) SHALL NOT be accepted on pgwire or Trino.

#### Scenario: pgwire
- **WHEN** `psql "user=alice password=<token>"` runs `select count(*) from lineitem`
- **THEN** it returns the correct count

#### Scenario: Trino
- **WHEN** a client sends Basic `alice:<token>` to `/v1/statement`
- **THEN** the query runs as alice

#### Scenario: Revocation
- **WHEN** the token is revoked
- **THEN** pgwire, Trino and `GET /api/v2/whoami` all refuse it

#### Scenario: Token-shaped config password
- **WHEN** a config password looks like a token
- **THEN** it works on pgwire but not on Trino Basic (documented limitation)
