## 1. Config

- [x] 1.1 `[oauth]` section, `ARNEB_OAUTH_*` overrides, validation and cross-checks (`[auth] type = "password"`, `[mcp] enabled`), effective values logged

## 2. Validator

- [x] 2.1 JWT validation against a cached JWKS (discovery, single-flight refetch on an unknown `kid`), asymmetric algorithms only, scopes, rejection reason codes at DEBUG

## 3. Metadata and challenge

- [x] 3.1 `/.well-known/oauth-protected-resource[/mcp]` and the `resource_metadata` challenge on `/mcp` `401`s

## 4. Acceptance on /mcp

- [x] 4.1 JWT branch in `authenticate` on `/mcp` only, `via: "oauth"`, `403` for an unknown user, `503 auth_provider_unavailable`

## 5. Tier 2 and docs

- [x] 5.1 Keycloak dev realm (compose profile `oauth`), `scripts/mcp-oauth-smoke.sh`, Tier 2 run against a coordinator and two workers
- [x] 5.2 Guides: `mcp`, `configuration` (`[oauth]`, upgrade notes), `http-api`, `kubernetes`

## 6. Review round 2

- [x] 6.1 JWKS fetch in a detached task (single-flight, throttled), background refetch from 80% of `jwks_cache_secs`, last good keys until `jwks_max_stale_secs`
- [x] 6.2 Refuse ID tokens (`nonce` / `at_hash` / payload `typ` `ID`/`Refresh`/`Offline`/`Logout` without `at+jwt`), opt-in `require_access_token_typ`, client-id audience needs `required_scopes`
- [x] 6.3 `email_verified` for `user_claim = "email"`; https for a discovered `jwks_uri` of a remote issuer; every key under a `kid` tried
- [x] 6.4 `error="invalid_token"` on refused JWTs, metadata only at the derived path, challenges computed once
