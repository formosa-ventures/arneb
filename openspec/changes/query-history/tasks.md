## 1. Tracker

- [x] 1.1 Source, user, wall-clock times and rows per query; `create_query_from`, `advance_to`, `finish_query`
- [x] 1.2 Bounded history (`[observability] query_history_size` / `ARNEB_QUERY_HISTORY_SIZE`, default 100, positive, logged); newest-first listing; tests
- [x] 1.3 Tracker feeds `/metrics` on Running and terminal transitions, replacing `QueryObservation` (review)

## 2. Protocols

- [x] 2.1 pgwire `run_statement` + `TrackedStatement` (queued → running → finished / failed, drop = cancelled), Simple and Extended
- [x] 2.2 Trino: source, user, rows
- [x] 2.3 Remove the coordinator's stray entry

## 3. system.runtime.queries

- [x] 3.1 Snapshot catalog + routing in pgwire and Trino; unit test (filters, ordering) and an end-to-end test over both protocols

## 4. Web UI

- [x] 4.1 `/api/v1/queries` fields: source, user, created_ms, elapsed_ms, rows

## 5. Validation and docs

- [x] 5.1 Tier 2 on the bench cluster (`ARNEB_QUERY_HISTORY_SIZE=5`): eviction, both protocols, single entry per distributed query, reader sees itself RUNNING, Web UI API
- [x] 5.2 Docs: configuration guide
