## 1. Errors

- [x] 1.1 SQL position and nearest-column candidates on parse and plan errors; Trino JSON `errorLocation`

## 2. Limits

- [x] 2.1 `[agent_api]` limits with `ARNEB_AGENT_*` overrides, validation and the effective-values log line

## 3. Statements

- [x] 3.1 Statement lifecycle over the Trino engine: submit, poll with `wait_ms`, cancel, per-user visibility, session statements refused
- [x] 3.2 Bounded registry (32 per user, 1024 total) and the idle reaper
- [x] 3.3 Bounded results: `max_rows` retention with `total_rows`, byte-bounded pages with a cursor, cell truncation

## 4. Discovery and explain

- [x] 4.1 Catalog, schema and table listings with `prefix`/`limit`; describe with statistics; sample
- [x] 4.2 Explain with per-scan row and byte estimates

## 5. HTTP

- [x] 5.1 `/api/v2` agent routes, envelope `location`/`candidates`, coordinator and standalone only

## 6. Docs and validation

- [x] 6.1 "Querying" section in `docs/guide/http-api.md`; `[agent_api]` in the configuration guide
- [x] 6.2 Tier 1: `cargo fmt`, `cargo clippy --workspace --all-targets`, `cargo test --workspace`
- [x] 6.3 `scripts/agent-api-smoke.sh` (curl and jq; psql for the Q6 cross-check)
- [x] 6.4 Tier 2: coordinator plus two workers, password auth, TLS, hive TPC-H; the smoke script prints 9 `PASS` lines
