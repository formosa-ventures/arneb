## Context

The consumer side of an exchange (`ExchangeClient`) already logs a
transport break on `arneb::exchange` ("exchange stream broke, reopening")
and resumes broadcast output (#147). Nothing on the producer side says how
far a broken stream got or whether the producer saw the connection fail, and
full h2 debug logging hides the break (it changes the timing). Each
`fetch_partition` opens its own connection, with no h2 keepalive configured
on either side.

## Goals / Non-Goals

**Goals:**

- Always-on evidence that tells which side ended a broken stream, cheap
  enough for production.
- A repeatable soak test for concurrent distributed queries.
- `cancel_query` flags every buffer it cancels.

**Non-Goals:**

- Resuming partitioned output. It is consumed once; making it re-readable
  needs spooling and belongs with P4 stage 2.
- Exposing worker metrics. Workers serve no HTTP; their exchange events go to
  the log. A worker `/metrics` endpoint is a separate change.

## Decisions

- **Wrap the `do_get` response stream (`ServedStream`) rather than
  instrumenting the encoders.** One place covers both buffer kinds. It counts
  FlightData messages and bytes, and marks the stream ended on a clean end or
  on the producer's own error frame. `Drop` without an end logs once at
  `info` (an early drop is also how a consumer that stopped reading looks,
  so it is not a warning) and bumps a counter. Rows aren't counted because
  FlightData doesn't carry them without decoding.
- **Connection-level errors come from tonic's own debug events**
  (`tonic::transport::server` "failed serving connection",
  `tonic::transport::channel::service::connection` "connection task error"),
  enabled per target. Each fires once per failed connection, unlike h2's
  per-frame debug output, so they don't perturb the timing.
- **Counters live with the query counters in `arneb_common::metrics`**, so
  the rpc crate reports into them without a registry.
- **`FlightState` keeps each buffer's failure handle in a side map at
  registration.** The handle is an `Arc` created with the buffer and never
  replaced, so `fail_output` sets it without the buffer's tokio mutex.
  Spawning a task to wait for the lock would only narrow the race.

## Risks / Trade-offs

- The early-drop line also fires for a consumer that stops reading on
  purpose (a cancelled query). That's rare and short, and the soak runs
  logged none.
- If the cause turns out to be outside Arneb (h2/hyper, the OS), the fix is
  limited to reporting it upstream and relying on resume + P4 stage 2.
