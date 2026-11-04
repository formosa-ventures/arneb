## ADDED Requirements

### Requirement: Configuration
An `[oauth]` section SHALL enable OAuth on `/mcp` when `issuer` is set, with `resource` (the public URL of `/mcp`, ending in `/mcp`) required, `audience` defaulting to `resource`, `user_claim` defaulting to `sub`, and optional `jwks_uri`, `required_scopes` (RFC 6749 scope tokens), `leeway_secs` (0 to 300, default 60), `jwks_cache_secs` (at least 60, default 3600), `jwks_min_refresh_secs` (1 to `jwks_cache_secs`, default 30), `jwks_max_stale_secs` (at least `jwks_cache_secs`, default 86400) and `require_access_token_typ` (default false). Each key SHALL have an `ARNEB_OAUTH_*` override that wins over the file, and the effective values SHALL be logged on `arneb::config`. `issuer`, `resource` and `jwks_uri` SHALL be `https`, or `http` only for a loopback host. Startup SHALL fail with a message naming the setting when a value is invalid, when `audience` differs from `resource` while `required_scopes` is empty, or when `[oauth]` is used without `[auth] type = "password"` or with `[mcp] enabled = false`. A `user_claim` other than `sub` SHALL log a WARN at startup. Without `issuer` the server SHALL behave exactly as before.

#### Scenario: Missing resource
- **WHEN** the config sets `issuer` and no `resource`
- **THEN** startup fails naming `[oauth] resource`

#### Scenario: Client-id audience without a scope
- **WHEN** the config sets `audience` to a client id and no `required_scopes`
- **THEN** startup fails naming `required_scopes`

#### Scenario: Password auth required
- **WHEN** the config sets `[oauth] issuer` under `[auth] type = "none"`
- **THEN** startup fails naming `[auth] type = "password"`

#### Scenario: Off by default
- **WHEN** the config has no `[oauth]` section
- **THEN** every response, `WWW-Authenticate` included, is unchanged, except that `/.well-known/oauth-protected-resource[/...]` answers `404` instead of the single-page app

### Requirement: Protected-resource metadata
With `[oauth]` enabled, `GET` of the well-known path `/.well-known/oauth-protected-resource` followed by the path of `resource` (for example `/.well-known/oauth-protected-resource/mcp`) SHALL answer without credentials with a JSON document holding `resource`, `authorization_servers` (the issuer), `bearer_methods_supported` (`["header"]`) and, when `required_scopes` is set, `scopes_supported`. The bare `/.well-known/oauth-protected-resource` SHALL answer the same document (`/mcp` is the one protected resource). Any other path under `/.well-known/oauth-protected-resource`, and every such path without `[oauth]`, SHALL answer a plain `404`. A `401` from `/mcp` SHALL carry `WWW-Authenticate: Bearer realm="arneb", resource_metadata="<url>"` naming that document, adding `error="invalid_token"` (and `scope="<required_scopes>"` when set) when a bearer credential was presented and refused, and its body SHALL be the same `unauthenticated` envelope as every other `401`.

#### Scenario: A client discovers the IdP
- **WHEN** an unauthenticated client posts to `/mcp`
- **THEN** it gets `401` with `resource_metadata`, and fetching that URL returns the issuer as the authorization server

#### Scenario: Another metadata path
- **WHEN** a client fetches `/.well-known/oauth-protected-resource/foo` while `resource` ends in `/mcp`
- **THEN** the answer is `404`

#### Scenario: A client probes the root
- **WHEN** a client fetches the bare `/.well-known/oauth-protected-resource`
- **THEN** it gets the same document as at the derived path

#### Scenario: An opaque or revoked bearer
- **WHEN** a bearer that is not a JWT (an opaque IdP token, an expired or revoked Arneb token) is refused on `/mcp`
- **THEN** the `401` challenge carries `error="invalid_token"`

### Requirement: Token validation
A bearer on `/mcp` that has three dot-separated non-empty parts and does not start with `arneb_` SHALL be validated as a JWT access token: the signature against a key of the issuer's JWKS (selected by `kid`), an algorithm in RS256, RS384, RS512, PS256, ES256 or ES384 (never `none` or `HS*`), `iss` equal to the issuer, `aud` containing the audience, `exp` and `nbf` within `leeway_secs`, that it is not sender-constrained, that it is an access token, and every required scope in `scope` or `scp`. A token with a `cnf` claim, or a payload or header `typ` of `DPoP` or `dpop+jwt` (case-insensitive), SHALL be refused whatever its other markers, since no proof of possession is verified. Every JWKS key under the token's `kid` whose type and algorithm fit SHALL be tried. A header `typ` of `at+jwt` or `application/at+jwt`, or else a payload `typ` of `Bearer` (case-insensitive), SHALL mark an access token; otherwise a payload `typ` of `ID`, `Refresh`, `Offline` or `Logout` (case-insensitive) SHALL be refused, and a token carrying `nonce` or `at_hash`, or neither `scope` nor `scp`, SHALL be refused when `required_scopes` is empty (with required scopes, the scope check decides). With `require_access_token_typ` every token without the `at+jwt` `typ` SHALL be refused. The JWKS SHALL be found through the issuer's discovery document unless `jwks_uri` is set (a discovered `jwks_uri` SHALL be `https` when the issuer is not on loopback). It SHALL be fetched by a task that a dropped request cannot cancel (a task that ends without a result, by panic or shutdown, SHALL count as a failed attempt), with one fetch in flight and at most one started per `jwks_min_refresh_secs`; refetched in the background from 80% of `jwks_cache_secs`; refetched when a token names an unknown `kid`; and while refetches fail, the last good keys SHALL keep deciding until they are `jwks_max_stale_secs` old. A token over 16 KiB SHALL be refused unparsed. A refusal SHALL answer `401` with the standard body and an `error="invalid_token"` challenge, except a missing scope, which SHALL answer `403` with `error="insufficient_scope"`. No token SHALL be logged or audited; a refusal MAY log its reason code on `arneb::oauth` at DEBUG.

#### Scenario: Wrong audience
- **WHEN** a token signed by the issuer carries no `aud` containing the resource
- **THEN** `/mcp` answers `401` with the standard body

#### Scenario: ID token
- **WHEN** an OIDC ID token of the issuer (header `typ: JWT`, with `nonce` or `at_hash`, or from a refresh grant with neither and no scope) is sent to `/mcp` and `required_scopes` is empty
- **THEN** the answer is `401` with `error="invalid_token"`

#### Scenario: DPoP-bound access token
- **WHEN** a Keycloak access token with payload `typ: DPoP` and `cnf.jkt` (with or without a `nonce`, with a scope) is sent to `/mcp` as a bearer
- **THEN** the answer is `401` with `error="invalid_token"`

#### Scenario: Keycloak access token with a nonce
- **WHEN** an access token with payload `typ: Bearer` and a `nonce` (Keycloak before 24) is sent to `/mcp`
- **THEN** it is accepted as an access token

#### Scenario: Client disconnects mid-fetch
- **WHEN** the request that started a JWKS fetch is dropped before the fetch completes
- **THEN** the fetch completes and the next request with a valid token succeeds without another fetch

#### Scenario: IdP down at cache expiry
- **WHEN** the JWKS cannot be fetched once the cached keys are older than `jwks_cache_secs` but younger than `jwks_max_stale_secs`
- **THEN** a token signed by a cached key is still accepted

#### Scenario: Algorithm confusion
- **WHEN** a token is signed with `HS256` using the public key as the secret
- **THEN** `/mcp` answers `401`

### Requirement: Identity mapping
The value of `user_claim` SHALL be the Arneb user, who must exist in the state store. With `user_claim = "email"`, a token whose `email_verified` is not `true` (or `"true"`) SHALL answer `403 forbidden`. With any other claim except `sub` whose value contains `@`, the token's `email` SHALL equal the value (ASCII case-insensitive) and `email_verified` SHALL be `true`; a different `email` or a non-true `email_verified` SHALL answer `403 forbidden`, and a token without `email` or `email_verified` SHALL be accepted only with `allow_missing_email_verified`. `sub` SHALL NOT be checked. The request SHALL run with that user's access rules and privileges, `whoami` SHALL report `via: "oauth"`, and audit entries SHALL record `via: "oauth"`. A valid token whose user does not exist, or whose `user_claim` value is not a valid Arneb user name, SHALL answer `403 forbidden` (with distinct messages). API tokens minted by an OAuth identity SHALL expire no later than the access token's `exp`. Deleting the user SHALL make their still-valid tokens answer `403` from the next request. A JWT on any route other than `/mcp` SHALL be an invalid bearer (`401`). API tokens, sessions and Basic credentials SHALL keep working on `/mcp`.

#### Scenario: Unknown user
- **WHEN** a valid token names `bob`, who has no Arneb user
- **THEN** `/mcp` answers `403 forbidden`

#### Scenario: Unverified email
- **WHEN** `user_claim = "email"` and a valid token has `email_verified: false`
- **THEN** `/mcp` answers `403 forbidden`, even if a user with that name exists

#### Scenario: Unverified address under another claim
- **WHEN** `user_claim = "preferred_username"` and a valid token carries `alice@corp.example` without `email_verified: true`
- **THEN** `/mcp` answers `403 forbidden`

#### Scenario: A username that is someone else's address
- **WHEN** `user_claim = "preferred_username"` and a valid token carries `preferred_username: admin@corp.example`, `email: attacker@evil.example`, `email_verified: true`
- **THEN** `/mcp` answers `403 forbidden`, even if the Arneb user `admin@corp.example` exists

#### Scenario: JWT outside MCP
- **WHEN** a valid token is sent to `GET /api/v2/whoami`
- **THEN** the answer is `401`

#### Scenario: Audited as oauth
- **WHEN** alice, a manager, changes a setting through the MCP `update_settings` tool with a token
- **THEN** `GET /api/v2/audit` has a `settings.update` entry by alice with `via: "oauth"`

### Requirement: Identity provider unavailable
When a JWT needs a key that is not cached and the JWKS or discovery document cannot be fetched, `/mcp` SHALL answer `503` with code `auth_provider_unavailable` and `Retry-After: 5`, never `401`. Requests with other credentials SHALL be unaffected.

#### Scenario: IdP down
- **WHEN** the IdP is unreachable and a token arrives with an uncached `kid`
- **THEN** the answer is `503 auth_provider_unavailable`, and an API token on `/mcp` still works
