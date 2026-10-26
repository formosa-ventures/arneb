# exchange-reliability Specification

## Purpose
TBD - created by archiving change exchange-concurrency-reliability. Update Purpose after archive.
## Requirements
### Requirement: Producer-side exchange stream accounting
A node serving an exchange `do_get` SHALL track, per response stream, its
ticket, buffer kind (`partitioned` / `broadcast`), peer address, and the
FlightData messages and bytes sent. When the stream is dropped before its end
(a clean end, or the producer's own error frame), the node SHALL log one
event on the `arneb::exchange` target with those fields and the stream's age.

#### Scenario: Stream read to its end
- **WHEN** a consumer reads a partitioned response to its end
- **THEN** no early-drop event is logged

#### Scenario: Stream dropped part-way
- **WHEN** a response stream is dropped after sending some of its messages
- **THEN** one early-drop event is logged with the messages and bytes sent so far

### Requirement: Cancellation flags every output buffer
Cancelling a query on a worker SHALL mark every output buffer of the query
failed, so consumers end on an error rather than a clean end of stream, even
when a `do_get` holds the buffer's lock at that moment.

#### Scenario: Cancel races a do_get
- **WHEN** `cancel_query` runs while a `do_get` holds a partitioned buffer's lock
- **THEN** the buffer's failure is set to "query cancelled"

### Requirement: Concurrency soak test
The repository SHALL provide a soak test that runs rounds of 20 concurrent
TPC-H queries against a running cluster, exits non-zero if any query fails,
and counts the exchange events in the nodes' logs.

#### Scenario: Acceptance run
- **WHEN** the soak test runs 10 rounds on a local uncapped cluster at SF10
- **THEN** no query fails

