# fault-tolerance-fail-clean — measurements (2026-10-20)

Local bench cluster (Apple M1 Pro, Docker Desktop; coordinator + 2 workers,
2.7 CPUs each), SF10, Tier 2.

| Check | Before (main `fd1bc61`) | After |
|---|---|---|
| `docker kill` worker-2 during q09 | fails in 4 s with a bare `h2 … BrokenPipe`; next queries fail ~30 s | fails in **0.8 s**, `worker 'worker-2' (http://arneb-worker-2:9090) failed: …`; next query OK first try |
| `docker pause` worker-2 during q09 | **never returns** (client gave up at 150 s) | fails in **8.7 s**, `worker 'worker-2' (…) lost: no heartbeat for 10 s`; next query OK first try |
| Surviving worker's tasks | cancelled | cancelled |
| Restarted / resumed worker | rejoins after the heartbeat timeout | rejoins on its next heartbeat (`worker re-registered`), runs tasks again |

- `fault_injection.sh`: all 15 checks passed. See [fault-injection-sf10.txt](fault-injection-sf10.txt).
- Oracle (`blast_radius_oracle.py --runs 2`): 22/22 deterministic and cell-identical to Trino.
- Latency with 2 s heartbeats (vs 10 s), cold, Arneb only, against main `fd1bc61`: geomean 1.008× (the latency
  A/B for #169 measured 1.004× between two main-equivalent builds, so this is within noise), sim_peak 0.977×.
