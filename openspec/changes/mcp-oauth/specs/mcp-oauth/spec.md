## ADDED Requirements

### Requirement: Configuration
An `[oauth]` section SHALL enable OAuth on `/mcp` when `issuer` is set, with `resource` (the public URL of `/mcp`, ending in `/mcp`) required, `audience` defaulting to `resource`, `user_claim` defaulting to `sub`, and optional `jwks_uri`, `required_scopes`, `leeway_secs` (0 to 300, default 60), `jwks_cache_secs` (at least 60, default 3600) and `jwks_min_refresh_secs` (1 to `jwks_cache_secs`, default 30). Each key SHALL have an `ARNEB_OAUTH_*` override that wins over the file, and the effective values SHALL be logged on `arneb::config`. `issuer`, `resource` and `jwks_uri` SHALL be `https`, or `http` only for a loopback host. Startup SHALL fail with a message naming the setting when a value is invalid, or when `[oauth]` is used without `[auth] type = "password"` or with `[mcp] enabled = false`. A `user_claim` other than `sub` SHALL log a WARN at startup. Without `issuer` the server SHALL behave exactly as before.

#### Scenario: Missing resource
- **WHEN** the config sets `issuer` and no `resource`
- **THEN** startup fails naming `[oauth] resource`

#### Scenario: Password auth required
- **WHEN** the config sets `[oauth] issuer` under `[auth] type = "none"`
- **THEN** startup fails naming `[auth] type = "password"`

#### Scenario: Off by default
- **WHEN** the config has no `[oauth]` section
- **THEN** every response, `WWW-Authenticate` included, is unchanged

### Requirement: Protected-resource metadata
With `[oauth]` enabled, `GET /.well-known/oauth-protected-resource` and `GET /.well-known/oauth-protected-resource/mcp` SHALL answer without credentials with a JSON document holding `resource`, `authorization_servers` (the issuer), `bearer_methods_supported` (`["header"]`) and, when `required_scopes` is set, `scopes_supported`. Without `[oauth]` the routes SHALL answer `404`. A `401` from `/mcp` SHALL carry `WWW-Authenticate: Bearer realm="arneb", resource_metadata="<url>"` naming that document, and its body SHALL be the same `unauthenticated` envelope as every other `401`.

#### Scenario: A client discovers the IdP
- **WHEN** an unauthenticated client posts to `/mcp`
- **THEN** it gets `401` with `resource_metadata`, and fetching that URL returns the issuer as the authorization server

### Requirement: Token validation
A bearer on `/mcp` that has three dot-separated non-empty parts and does not start with `arneb_` SHALL be validated as a JWT access token: the signature against a key of the issuer's JWKS (selected by `kid`), an algorithm in RS256, RS384, RS512, PS256, ES256 or ES384 (never `none` or `HS*`), `iss` equal to the issuer, `aud` containing the audience, `exp` and `nbf` within `leeway_secs`, and every required scope in `scope` or `scp`. The JWKS SHALL be found through the issuer's discovery document unless `jwks_uri` is set, cached for `jwks_cache_secs`, and refetched at most once per `jwks_min_refresh_secs`, with one fetch in flight, when a token names an unknown `kid`. A token over 16 KiB SHALL be refused unparsed. A refusal SHALL answer `401` with the standard body, except a missing scope, which SHALL answer `403` with `error="insufficient_scope"`. No token SHALL be logged or audited; a refusal MAY log its reason code on `arneb::oauth` at DEBUG.

#### Scenario: Wrong audience
- **WHEN** a token signed by the issuer carries no `aud` containing the resource
- **THEN** `/mcp` answers `401` with the standard body

#### Scenario: Algorithm confusion
- **WHEN** a token is signed with `HS256` using the public key as the secret
- **THEN** `/mcp` answers `401`

### Requirement: Identity mapping
The value of `user_claim` SHALL be the Arneb user, who must exist in the state store. The request SHALL run with that user's access rules and privileges, `whoami` SHALL report `via: "oauth"`, and audit entries SHALL record `via: "oauth"`. A valid token whose user does not exist SHALL answer `403 forbidden`. Deleting the user SHALL make their still-valid tokens answer `403` from the next request. A JWT on any route other than `/mcp` SHALL be an invalid bearer (`401`). API tokens, sessions and Basic credentials SHALL keep working on `/mcp`.

#### Scenario: Unknown user
- **WHEN** a valid token names `bob`, who has no Arneb user
- **THEN** `/mcp` answers `403 forbidden`

#### Scenario: JWT outside MCP
- **WHEN** a valid token is sent to `GET /api/v2/whoami`
- **THEN** the answer is `401`

#### Scenario: Audited as oauth
- **WHEN** alice creates a user through the MCP `create_user` tool with a token
- **THEN** `GET /api/v2/audit` has a `user.create` entry by alice with `via: "oauth"`

### Requirement: Identity provider unavailable
When a JWT needs a key that is not cached and the JWKS or discovery document cannot be fetched, `/mcp` SHALL answer `503` with code `auth_provider_unavailable` and `Retry-After: 5`, never `401`. Requests with other credentials SHALL be unaffected.

#### Scenario: IdP down
- **WHEN** the IdP is unreachable and a token arrives with an uncached `kid`
- **THEN** the answer is `503 auth_provider_unavailable`, and an API token on `/mcp` still works
