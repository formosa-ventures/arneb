## 1. Privilege layers

- [x] 1.1 `Authorizer` with observe/manage layers, `admin_users`, `system_information` rules, `check_kill`, `whoami.privileges`, new error codes and hints

## 2. Write privileges

- [x] 2.1 `INSERT`, `DELETE`, `OWNERSHIP` checks after planning on pgwire, Trino, the agent API and MCP; `read-only` catalogs; `EXPLAIN` of a write

## 3. Audit log

- [x] 3.1 `AuditLog`: durable append, retention and compaction, torn-line recovery, `GET /api/v2/audit`, `audit_unavailable`

## 4. Cancel any query

- [x] 4.1 Per-query cancellers on pgwire, Trino and agent statements; `DELETE /api/v2/queries/{id}` with the manage envelope and dry run; `/api/v1` kept

## 5. Workers

- [x] 5.1 Cordon, drain and uncordon with persisted state, `scheduling`, bounded `GET /api/v2/workers`

## 6. Admission limits

- [x] 6.1 Always-built controller, runtime `GET|PUT /api/v2/admission`, persisted limits and startup precedence

## 7. MCP

- [x] 7.1 Seven manage tools, widened `cancel_query`, bounded `list_workers`, 25-tool contract test

## 8. Docs and validation

- [x] 8.1 OpenSpec change, upgrade notes in `docs/guide/configuration.md`
- [x] 8.2 Tier 1: `cargo fmt`, `cargo clippy --workspace --all-targets` (pinned and 1.99.0), `cargo test --workspace`
- [x] 8.3 Tier 2: `scripts/manage-smoke.sh` against a coordinator plus 2 workers with password auth, before and after a coordinator restart (run by the controller at aaf9751: steps 1–9, 9b and `--after-restart` 10 passed)
