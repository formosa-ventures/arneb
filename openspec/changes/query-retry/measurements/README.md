# query-retry — measurements (2026-10-26)

Local bench cluster (Docker Desktop; coordinator + 2 workers), SF10, Tier 2,
image built from this branch.

| Run | Result |
|---|---|
| `fault_injection.sh` (retry off) | all 15 checks passed, unchanged from stage 1: kill fails in 1.2 s, pause in 8.7 s, both naming `worker-2` ([fault-injection-sf10.txt](fault-injection-sf10.txt)) |
| `RETRY=1 fault_injection.sh` | all 13 checks passed: q09 completes after the fault in 8.2 s (kill) and 16.0 s (pause), same rows as the run without a fault ([fault-injection-retry-sf10.txt](fault-injection-retry-sf10.txt)) |

- The coordinator logged `query retry effective value … retry_policy="query" query_retry_attempts=4`
  at startup, then one `retrying query after worker loss retry=1 … worker_id=worker-2 delay_ms=1000`
  and one `query succeeded after retry retries=1` per fault.
- `/metrics` on the coordinator read `arneb_query_retries_total 2` afterwards.
- The first `RETRY=1` run compared q09 byte for byte and failed both "same rows" checks. Two
  fault-free runs on 2 workers differ too: q09's float sums vary in the last digits with the
  summation order (max relative difference 1.6e-14 between two 2-worker runs, 2.6e-14 against
  a 1-worker run). The script now compares numbers within a relative 1e-12, which still
  rejects a changed digit or a missing row.
