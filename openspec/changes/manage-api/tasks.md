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

## 9. Review round 2

- [x] 9.1 Kill: no `queries` rules grant only the owner or manage
- [x] 9.2 Audit reads: managers see all entries, others only their own (`scope`)
- [x] 9.3 Audit intent entry before apply, completion after, `interrupted` at startup
- [x] 9.4 Audit reads never wait for an fsync or a compaction (split writer and memory locks)
- [x] 9.5 Audit log named after the state file (`<stem>.audit.jsonl`)
- [x] 9.6 A kill before canceller registration: the statement cannot report rows afterwards
- [x] 9.7 `run_manage` takes the change lock itself (`ManageAction::change_mutex`); shared `count` / `persist_failed`
- [x] 9.8 Lock-free admission while no cap is set
- [x] 9.9 `DELETE /api/v2/admission` and MCP `reset_admission_limits` (26 tools)

## 10. Review round 3

- [x] 10.1 Kill: cancellers registered before the work starts, through a `KillSwitch`; a kill after the work completed is not applied and the statement keeps its real result
- [x] 10.2 Audit: a failed completion is kept and appended when the log reopens; a panicking action is completed as `internal`
- [x] 10.3 Audit: the refused closing entry only after an attempted write
- [x] 10.4 Audit: the `token.issue` intent names the token id (`TokenId`, `issue_token_as`)
- [x] 10.5 Audit log named after the full state file name (`prod.json.audit.jsonl`); WARN on a legacy `audit.jsonl`
- [x] 10.6 Audit reads examine at most 10,000 entries; a filtered page that runs out ends early, truncated, with a cursor
- [x] 10.7 Lock-free admission checks the cap flag and takes its slot in one atomic step (no spurious `QueueFull`)
- [x] 10.8 Kill: `kill_query` documented as blocking, the v1 cancel runs it in `spawn_blocking`; Trino and agent client cancels and reapers go through `KillSwitch::try_kill`; docs state the mid-work behavior precisely
- [x] 10.9 Audit: a completion written twice (fsync failed, then replayed) loads once

## 11. Review round 4

- [x] 11.1 Kill: a kill during a poll is pending and settled by the guard when the poll ends; `KillSwitch::kill`, `Canceller` and `kill_query` are async (no lock held across a poll, no `spawn_blocking` for the v1 cancel); every protocol cancel goes through it with no abort fallback; a not-applied kill reports `NotApplied` with the real state and concurrent kills share the first kill's outcome; docs narrow the between-polls claim
- [x] 11.2 Audit: a panicking action is completed as `interrupted`, not `internal`; a failed `token.issue` completion has no target
- [x] 11.3 Audit: a background tick retries an unwritable log (and its kept completions) every 5 s; graceful shutdown flushes once more
- [x] 11.4 Kill (fix review): Trino decides a paging cancel before the kill, so a cancel resuming after `complete` keeps the result; a kill that ends without an outcome releases its claim (`internal` / `500`); a query that leaves the history mid-kill is `not_found`; the agent reaper touches before spawning a cancel
- [x] 11.5 Audit (fix review): the exit flush also runs on fatal listener errors and logs a failed flush task

## 12. Review round 5

- [x] 12.1 Kill: the settle task is spawned after the tracker lock is released (a runtime shutting down drops it inline, and the claim's drop takes the lock); an interrupted kill hands its canceller, or its pending call, back to the entry, so a retry asks the same kill switch instead of assuming the kill applied
- [x] 12.2 Trino: only a read with remaining pages is paging; a finished write is never relabelled; paging is read when the cancel arrives; the reaper spawns one task per cancel
- [x] 12.3 Audit: a kill without an outcome is audited `interrupted`; a query that left the history reports the last state the kill knew, never an invented `cancelled`
- [x] 12.4 Audit: the shutdown flush waits (at most 10 s) for audited actions still running
- [x] 12.5 Agent: one reaper cancel in flight per statement; a cancel whose task failed keeps the statement instead of releasing it

## 13. Review round 6

- [x] 13.1 Shutdown: the Web API stops gracefully (bounded) before the audit flush; once the flush starts, new audited actions (one queued on the blocking pool included) are refused before their intent is written
- [x] 13.2 Kill: a not-applied kill of a query that then left the history reports `state` `ended`, not the stale `running`
