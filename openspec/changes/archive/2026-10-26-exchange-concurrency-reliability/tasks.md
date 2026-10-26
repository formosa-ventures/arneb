## 1. Instrumentation

- [x] 1.1 `arneb_common::metrics`: exchange break (`resumed` / `failed`) and early-drop counters, rendered in `/metrics`; test
- [x] 1.2 `ExchangeClient::resumable` counts each mid-stream break by outcome
- [x] 1.3 `ServedStream` wraps every `do_get` response: messages, bytes, peer, age; early drop logs on `arneb::exchange` and counts; test

## 2. Cancellation (G2, #156 item 3)

- [x] 2.1 `FlightState` keeps each buffer's failure handle; `fail_output` sets it without the buffer lock; `remove_buffer` drops it
- [x] 2.2 `cancel_query` uses `fail_output`; test with the partitioned lock held

## 3. Soak test

- [x] 3.1 `benchmarks/tpch/scripts/concurrency_soak.sh`: rounds of 20 concurrent queries, non-zero exit on failure, exchange-event counts from `LOG_DIR`

## 4. Root cause (#139)

- [x] 4.1 Native uncapped cluster with the instrumentation and the tonic connection-error targets: 20 warm rounds + 10 cold restarts × 2 rounds (800 queries, 2026-10-23): 0 failures, 0 exchange events of any kind — not captured
- [ ] 4.2 Read producer and consumer events for the break; identify which side closed the connection and why — deferred: no break captured; the instrumentation stays on to catch the next one
- [x] 4.3 Fix in Arneb if the cause is there; otherwise post the evidence to #139 and keep it open

## 5. Tier 2 + docs

- [x] 5.1 Soak test on the Docker bench cluster (cap 8: 100 queries, 0 failures, 0 exchange events) and native uncapped (above); `/metrics` shows the exchange counters, and a worker OOM-killed during an uncapped Docker run moved `breaks_total{failed}` and logged producer early drops. Uncapped on the 16 GB Docker VM OOM-kills a worker or RustFS within 3–4 rounds (unbounded memory pool), so it is not a valid soak there
- [x] 5.2 `docs/guide/observability.md`: exchange counters and events; soak test in `docs/guide/distributed.md`
