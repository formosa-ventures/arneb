## 1. State store

- [x] 1.1 `crates/state` (`arneb-state`): versioned JSON file, atomic write, mode `0600`, in-memory fallback
- [x] 1.2 Apply `[[auth.users]]` on every start (changed hash / removed user revokes tokens; state-created users untouched); `--reseed-state` for name collisions
- [x] 1.3 Issue, list and revoke API tokens; 100-token cap; optional expiry; hashed storage
- [x] 1.4 Open the store at startup on coordinator and standalone only; `[state] path` / `ARNEB_STATE_PATH`; log the effective settings

## 2. Tokens on pgwire and Trino

- [x] 2.1 `is_token_shaped` and token-aware `CredentialSource` in `arneb-protocol`
- [x] 2.2 Accept tokens as the SCRAM password on pgwire
- [x] 2.3 Accept tokens as Trino Basic passwords, bypassing the verification cache

## 3. Web API

- [x] 3.1 Auth middleware with one public-route list; `[web] metrics_auth` / `ARNEB_WEB_METRICS_AUTH`
- [x] 3.2 Apply the `queries` view and kill rules to `/api/v1/queries*`
- [x] 3.3 Session store and `/api/v2` routes: health, sessions, whoami, tokens, queries, cluster, workers, info
- [x] 3.4 Error envelope and stable error codes
- [x] 3.5 Harden token minting: expiry clamping, issuance limits, bounded PBKDF2 concurrency
- [x] 3.6 Web UI sign-in page

## 4. Deployment

- [x] 4.1 Helm: `coordinator.state.persistence.*`, PVC, mount at `/var/lib/arneb`

## 5. Docs and validation

- [x] 5.1 `docs/guide/http-api.md`; update configuration, access control and Kubernetes guides; vitepress sidebar
- [x] 5.2 Tier 1: `cargo fmt`, `cargo clippy --workspace --all-targets`, `cargo test --workspace`
- [x] 5.3 Tier 2: coordinator plus two workers with password auth and a state path: session, token, pgwire, Trino, restart, revoke
