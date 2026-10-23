## Why

Issue #139: under ~20 concurrent SF10 queries, a worker-to-worker Flight
exchange stream occasionally breaks with an h2 `BrokenPipe` /
`ConnectionReset`. This is the "Path to production" gap G1
(`docs/superpowers/specs/2026-10-16-phase3-roadmap-design.md`), and scenario 1
(replacing an internal Trino cluster) is exactly this workload.

A reproduction on v0.1.5 (2026-10-22, 460 queries across Docker and native
clusters, admission cap 8 and uncapped) found no failed query. One broadcast
stream broke on the native cluster and #147's resume recovered it, so the
break still happens (about once per 300 queries natively) but is now hidden
for broadcast output. A partitioned stream cannot be resumed, so the same
break there would fail the query.

The cause is still unknown. Reading h2 0.4 leaves one explanation: the
producer's connection closed mid-stream without a GOAWAY. Arneb runs one
tokio runtime, so a consumer dropping its own connection was ruled out. Full
h2 debug logging changes the timing enough that the break stops happening,
so finding the cause needs cheap, always-on instrumentation.

G2 (#156 item 3) lands here too: a `cancel_query` that loses the `try_lock`
race on a partitioned buffer leaves it unflagged, which weakens P2's "a
timed-out query releases its memory and tasks".

## What Changes

- **Producer-side stream accounting.** Every `do_get` response stream
  records its ticket, buffer kind (partitioned / broadcast), peer address,
  messages and bytes sent. If the stream is dropped before its end (the
  connection went away, or the consumer stopped reading), one line is logged
  on `arneb::exchange` with those fields and the stream's age. With the
  consumer's existing "exchange stream broke" warning, this shows which side
  ended a broken stream and how far it had got.
- **Exchange counters** in `/metrics`: `arneb_exchange_stream_breaks_total`
  (`outcome` = `resumed` / `failed`) for consumer-side transport breaks, and
  `arneb_exchange_streams_dropped_total` for producer-side early drops. They
  are process-wide, so a node serving `/metrics` (coordinator, standalone)
  reports its own. Workers log the same events.
- **Concurrency soak test** checked into the repo
  (`benchmarks/tpch/scripts/concurrency_soak.sh`): N rounds of 20 concurrent
  TPC-H queries against a running cluster, reporting failures and exchange
  breaks found in the node logs. It is the Tier 2 acceptance check for this
  change and for P4 stage 2.
- **`cancel_query` race (G2).** Cancelling a query flags every partitioned
  buffer it owns even when another task holds the buffer's lock.
- **Root cause.** With the instrumentation in place, the soak test is rerun
  on the native cluster until a break is captured, and the cause is fixed
  here if it is in Arneb. If it is not found within the change, #139 stays
  open with the captured evidence.

## Capabilities

### New Capabilities

- `exchange-reliability`: producer-side stream accounting, the
  concurrency soak test as an acceptance check, and cancellation reaching
  every partitioned buffer.

### Modified Capabilities

- `observability-metrics`: the exchange counters.

## Impact

- `crates/rpc/src/{flight_service,exchange_client,output_buffer}.rs`
- `crates/common/src/metrics.rs`
- `crates/server/src/task_manager.rs` (cancellation)
- `benchmarks/tpch/scripts/concurrency_soak.sh`, `docs/guide/` (soak test
  and the new metrics)
