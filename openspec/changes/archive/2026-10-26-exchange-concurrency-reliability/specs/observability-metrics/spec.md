## ADDED Requirements

### Requirement: Exchange counters
`GET /metrics` SHALL include `arneb_exchange_stream_breaks_total` with an
`outcome` label (`resumed`, `failed`), counting exchange streams the node
read that broke mid-stream on a transport error, and
`arneb_exchange_streams_dropped_total`, counting exchange response streams
the node served that were dropped before their end. Both are process-wide
counts for the node serving `/metrics`.

#### Scenario: A broadcast break is resumed
- **WHEN** a broadcast exchange stream the coordinator reads breaks and the resume succeeds
- **THEN** `arneb_exchange_stream_breaks_total{outcome="resumed"}` increases by one
