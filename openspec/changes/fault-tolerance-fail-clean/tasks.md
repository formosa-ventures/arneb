## 1. Registry

- [x] 1.1 `NodeRegistry::lost_worker(&[ids])`, `mark_dead(id, reason)`, re-registration logging; unit tests

## 2. Coordinator

- [x] 2.1 `watch_workers` raced against dispatch + collect in `DistributedExecutor::execute`; unit test with a registry whose worker goes silent
- [x] 2.2 Submission transport failure → `mark_dead` + error naming the worker
- [x] 2.3 Exchange read errors carry the remote address; a transport error on the root collect marks that worker dead

## 3. Config

- [x] 3.1 `[cluster] heartbeat_interval_secs` / `heartbeat_timeout_secs` + env overrides, validation, startup log; tests
- [x] 3.2 Worker heartbeat loop uses the interval; coordinator registry uses the timeout

## 4. Tier 2

- [x] 4.1 `benchmarks/tpch/scripts/fault_injection.sh` (kill + pause during SF10 q09, next query, rejoin)
- [x] 4.2 Run it on the local bench cluster; record the output in `measurements/`
- [x] 4.3 Oracle 22/22 unchanged; latency spot-check (heartbeat 5× more frequent)

## 5. Docs

- [x] 5.1 `docs/guide/configuration.md` (`[cluster]` heartbeat knobs) and the distributed deployment guide (failure behavior)
