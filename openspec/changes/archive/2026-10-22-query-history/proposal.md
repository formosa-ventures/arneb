## Why

Phase 3 M3 / P5 asks for "bounded query history, shown in the Web UI and
exposed as `system.runtime.queries` over both pgwire and the Trino protocol"
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`). The metrics
half landed separately (`observability-metrics`). The `QueryTracker` behind
the Web UI's query list could not provide a history:

- pgwire statements never entered it. Only the Trino protocol recorded
  queries.
- The distributed coordinator added an entry per query (`format!("{plan:?}")`)
  that never left `Queued`.
- Nothing was ever removed, so the map grew for the life of the process.

## What Changes

- **QueryTracker** records each query's source (`pgwire` / `trino`), user,
  created / started / ended wall-clock times, and returned rows. It keeps every
  active query plus the most recent `ARNEB_QUERY_HISTORY_SIZE` finished ones
  (default 100, logged at startup) and evicts the oldest finished entry when
  a new one finishes. New helpers `create_query_from`, `advance_to` and
  `finish_query` replace hand-walked state transitions.
- **pgwire** records every statement that reaches the engine: `QUEUED` at
  receipt, `RUNNING` once admitted, then `FINISHED` with its row count or
  `FAILED` with its error (statement timeouts and admission refusals
  included). A connection dropped mid-statement records `CANCELLED`.
  Intercepted metadata and `SET` / `SHOW` are not recorded.
- **Trino protocol** records source `trino`, the user and the row count.
- The coordinator's stray entry is removed.
- **`system.runtime.queries`** (pgwire and Trino): `query_id`, `state`,
  `user`, `source`, `query`, `queued_time_ms`, `elapsed_time_ms`, `rows`,
  `created`, `started`, `end`, `error_message`. Like the Trino virtual metadata
  tables, it is a snapshot materialized per statement, so filters, ordering and
  `LIMIT` work.
- **Web UI API**: `/api/v1/queries` entries gain `source`, `user`,
  `created_ms`, `elapsed_ms` and `rows`.

## Capabilities

### New Capabilities

- `query-history`: what is recorded per query, by which protocol and in which
  states, retention, `system.runtime.queries`, and the Web UI fields.

## Impact

- `crates/scheduler/src/{tracker,state}.rs`
- `crates/protocol/src/{handler,server,runtime_queries}.rs` and
  `trino/{mod,engine}.rs`
- `crates/server/src/{main,coordinator}.rs` and `web/api.rs`
- `docs/guide/configuration.md`
