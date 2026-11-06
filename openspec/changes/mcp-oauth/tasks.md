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

## 7. Review round 4

- [x] 7.1 Token kind by marker: header `at+jwt`, then payload `typ` `Bearer` (access, even with `nonce`) or `ID`/`Refresh`/`Offline`/`Logout` (refused); an unmarked token with `nonce`/`at_hash`/no scope needs `required_scopes`
- [x] 7.2 `email_verified` for any email-address user name, opt-in `allow_missing_email_verified`
- [x] 7.3 Client-id guard only for an `audience` that is not a URL or URI
- [x] 7.4 Fetch guard disarmed with `take` (no leaked `Arc`); key-cache lock recovers from poisoning everywhere
- [x] 7.5 `invalid_token` challenge carries `scope`, and is sent for every refused bearer on `/mcp`
- [x] 7.6 Metadata also at the bare well-known path
- [x] 7.7 An email-address user under a claim other than `email`/`sub` must equal the token's verified `email`; `sub` exempt; an empty bearer gets no error code; app-only tokens documented

## 8. Review round 5

- [x] 8.1 Refuse sender-constrained tokens (`cnf`, payload or header `typ` DPoP) with `401 invalid_token`
- [x] 8.2 `sub` no longer exempt from the email check
- [x] 8.3 With `allow_missing_email_verified`, an unverified `email` that differs is ignored; mismatch only for a verified `email`
- [x] 8.4 Client-id guard: only the `resource` URL itself is exempt from `required_scopes`
- [x] 8.5 An unmarked ID-token-like token without every required scope is `401 invalid_token` (before scope and email `403`s); one parsed scope list; scope checked before email
- [x] 8.6 Bare well-known path only when the resource path is exactly `/mcp`; shared-ingress risk documented
- [x] 8.7 Entra app-only remedies corrected (Arneb API token; no `scp`, no `at+jwt`)

## 9. Review round 6

- [x] 9.1 `scope` parsed like `scp` (string or string array); a malformed claim (`null`, number, boolean, object, array with a non-string element) grants nothing and counts as no scope
