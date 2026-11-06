## Why

Stage H6 of the headless agent interface (`docs/superpowers/specs/2026-10-30-headless-agent-interface-design.md`). H3 gave agents an MCP endpoint at `/mcp`, authenticated with Arneb API tokens, sessions or Basic credentials. Clients that insist on an OAuth flow, such as claude.ai custom connectors, cannot send those. H6 lets `/mcp` accept OAuth 2.0 access tokens from an external OpenID Connect provider, with Arneb as the resource server only.

## What Changes

- **`[oauth]` config section** (startup-only, `ARNEB_OAUTH_*` overrides): `issuer`, `resource`, `audience`, `user_claim`, `jwks_uri`, `required_scopes`, `leeway_secs`, `jwks_cache_secs`, `jwks_min_refresh_secs`. Setting `issuer` turns it on; it needs `[auth] type = "password"` and `[mcp] enabled`.
- **JWT validation** against the issuer's JWKS (discovery document, cached, single-flight refetch on an unknown `kid`): signature, asymmetric algorithms only, `iss`, `aud`, `exp` / `nbf`, scopes.
- **Protected-resource metadata** (RFC 9728) at `/.well-known/oauth-protected-resource[/mcp]`, and a `WWW-Authenticate` challenge with `resource_metadata` on `/mcp` `401`s.
- **Identity mapping**: the `user_claim` names an existing Arneb user; `whoami` and the audit log report `via: "oauth"`.
- **Error code** `auth_provider_unavailable` (HTTP 503) when no cached key can decide.
- **Tier 2 tooling**: a Keycloak dev realm behind the `oauth` compose profile and `scripts/mcp-oauth-smoke.sh`.

## Capabilities

### New Capabilities

- `mcp-oauth`: accepting and validating OAuth access tokens on `/mcp`, the metadata document, the user mapping and its failure modes.

### Modified Capabilities

None. Behavior without `[oauth]` is unchanged.

## Impact

- New: `crates/server/src/oauth.rs` (validator, JWKS cache), `crates/server/src/web/oauth_metadata.rs`; `OAuthConfig` in `config.rs`.
- Changed: `web/auth.rs` (JWT branch on `/mcp`, `AuthVia::OAuth`), `web/error.rs`, `main.rs`, the `whoami` doc strings.
- New dependency: `jsonwebtoken` (aws-lc-rs backend); no second crypto stack.
- Docs: `mcp`, `configuration` (`[oauth]`, upgrade notes), `http-api`, `kubernetes` guides; `docker/keycloak/arneb-realm.json`, `scripts/mcp-oauth-smoke.sh`.
- Upgrade notes: `docs/guide/configuration.md#upgrading-to-v020-mcp-oauth`.
