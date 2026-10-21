## Context

`QueryTracker` (`crates/scheduler`) held a `QueryStateMachine` per query in an
unbounded map. Only the Trino protocol fed it, and the Web UI listed it.
pgwire, the primary protocol, never recorded anything.

## Decisions

### D1 — The protocols record; the coordinator does not

Only the protocol layer knows the SQL text, the user and when a statement is
admitted. The distributed coordinator's entry was a debug-printed plan and was
removed. Its `QueryId` still keys the distributed tasks.

### D2 — pgwire records around admission and the timeout

`ConnectionHandler::run_statement` wraps the shared admit → execute path for
both the Simple and the Extended protocols. A `TrackedStatement` guard creates
the entry, moves it to `RUNNING` once admitted, and finishes or fails it. Its
`Drop` cancels it, which covers a client that disconnects mid-statement.
Statement-timeout and admission errors are recorded as `FAILED`, with the
message the client sees, as Trino reports time limits.

### D3 — Retention by count of finished queries

A `VecDeque` of terminal ids gives O(1) eviction. Active queries are never
evicted, so a long-running query is always visible. The count is a runtime
knob (`[observability] query_history_size`, `ARNEB_QUERY_HISTORY_SIZE`),
resolved and validated in `config.rs` like the other sections. Trino's
equivalent `query.max-history` defaults to 100 too.

### D5 — The tracker feeds the query metrics

The tracker already owns each statement's lifecycle, so it reports to
`arneb_common::metrics` on the transitions: `query_started` when a query
reaches `Running`, `query_ended` (outcome, duration, slow-query log) when it
turns terminal. This replaces the separate `QueryObservation` guard from the
metrics change, which tracked the same lifecycle at nearly the same call
site. The tracker is therefore required in both protocol servers; a server
built without one gets a private tracker.

### D4 — A per-statement snapshot table

`system.runtime.queries` reuses the approach of the Trino virtual metadata
tables: the tracker is snapshotted into an in-memory catalog and the client's
SQL runs over it locally, never distributed, so the full SQL surface works.
The snapshot is taken after admission, so the reader sees itself as running.
Joining it with real tables is not supported, because the statement runs
against the snapshot catalog only.

## Risks / Trade-offs

- **Memory:** each entry holds its SQL text. 100 entries of typical TPC-H size
  are well under 1 MB. Very large SQL counts toward that, and the size is
  configurable.
- **Tracker write lock per transition:** a few map operations per statement,
  which is negligible next to planning.
