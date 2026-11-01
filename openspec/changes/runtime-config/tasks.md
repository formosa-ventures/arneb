## 1. Access rules

- [x] 1.1 Live `RulesHandle` shared by every protocol and the `Authorizer`; rules in the state store, seeded once; `GET|PUT|DELETE /api/v2/access-rules`; lockout guard; `conflict` error code

## 2. Users and their tokens

- [x] 2.1 `GET|POST /api/v2/users`, `PATCH|DELETE /api/v2/users/{name}`, tombstones, Basic-cache drop and session end; per-user token routes

## 3. Catalog definitions

- [x] 3.1 `CatalogConfig` serializable, catalogs in the state store (seeded once, divergence WARN), `LiveCatalogs`, redaction; coordinator builds catalogs after the state opens; standalone under password reads the state

## 4. Catalog sync to workers

- [x] 4.1 JSON heartbeat reply with generation and epoch, `sync_catalogs` / `get_catalogs`, partial-failure retry, pull-once on a task miss; workers ignore `[[catalogs]]`

## 5. Catalog API

- [x] 5.1 `/api/v2/catalog-configs` with probe-first, push to workers, redacted round-trip, resend-secret rule for a new destination

## 6. Settings and re-seed

- [x] 6.1 `GET|PATCH /api/v2/settings`, stored threshold wins at startup; `POST /api/v2/state/reseed` and `--reseed-state` for every object

## 7. MCP

- [x] 7.1 18 tools (44 in total), each calling its route's `*_for`

## 8. Docs and validation

- [x] 8.1 OpenSpec change; upgrade notes in `docs/guide/configuration.md`; `access-control`, `http-api` and `mcp` guides
- [x] 8.2 Tier 1: `cargo fmt`, `cargo +1.99.0 clippy --workspace --all-targets`, `cargo test --workspace`
- [x] 8.3 Tier 2: `scripts/runtime-config-smoke.sh` steps 1–7 and `--after-restart` (coordinator restart, then a worker restart) against a coordinator plus 2 workers with password auth, hive SF10 and PostgreSQL; `scripts/manage-smoke.sh` steps 1–10 on the same stack
