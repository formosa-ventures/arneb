# Verification status

Every requirement in the three delta specs, what verifies it, and what has
actually been run. Written at the point where all work that does not require a
full benchmark run is complete.

## Verified

| Requirement | Spec | Verified by | Evidence |
|---|---|---|---|
| Scale scenarios are declared | `tpch-scale-scenarios` | Tasks 1.1–1.4 | `--scenario=sf10` fixes scale/run plan/CPUs from the fragment; `--scenario=sf42` aborts listing `sf1, sf10`; precedence CLI > env > scenario > default confirmed by direct invocation |
| Host preconditions verified before any data is seeded | `tpch-scale-scenarios` | Tasks 2.1–2.5 | Run against a runtime reporting 1 GiB: both unmet floors reported (not just the first), disk reported indeterminate rather than passed, exit 1, and **zero** `docker` calls other than `info` — nothing started, nothing seeded. `SKIP_PREFLIGHT=1` proceeds and reaches the runner with `--preflight-bypassed` |
| Reused data of the wrong scale is refused | `tpch-scale-scenarios` | Implemented + fake-runtime test | `SKIP_SEED=1 --scenario=sf10` against a resident 6,001,215-row lineitem aborts naming both counts; the runner is never invoked |
| The run's scale is an explicit input | `tpch-benchmark-runner` | Tasks 3.1, 3.4 + unit tests | No code path derives scale from the output path; an absent `--scale-factor` records `None` |
| Result persistence (scale, scenario, bypass recorded; optional on read) | `tpch-benchmark-runner` | Tasks 3.2, 3.3, 3.6, 4.1–4.4, 10.2 | 21 tests pass. The **published** `official/v1.0.0/sf1/*.json` render through the new report as `**Scale factor:** unrecorded` with no error |
| A measured scale factor that is withheld is disclosed as withheld | `release-baseline-numbers` | Task 9.1 | `README.md` and `docs/index.md` now state SF10 was measured, its figure, why it is withheld, and the commit it is readable at |
| Republication gated on landing, not on the result | `release-baseline-numbers` | Tasks 9.2, 9.3 | Rule written into `RELEASE_CHECKLIST.md`, including the explicit rejection of a threshold-conditioned gate |
| Every regression in a withheld run is attributed | `release-baseline-numbers` | Tasks 8.1–8.5 | `baseline/attribution.md`: all 15 sub-1.00x queries have a row; 8 are `unowned` |

## Not verified — requires a full benchmark run

| Requirement | Spec | Blocking tasks | Why not done |
|---|---|---|---|
| A scenario runs end to end on one local Docker host | `tpch-scale-scenarios` | 6.1, 6.3 | Needs a full SF10 cycle: ~10 GB seed plus three engines × 22 queries × 8 runs. Hours of wall clock on a machine that is then unusable for other work |
| An interrupted scenario leaves no partially seeded dataset | `tpch-scale-scenarios` | 6.4 | `docker/tpch-seed/seed.sh` opens with `DROP SCHEMA IF EXISTS hive.tpch CASCADE`, so it is structurally idempotent — but that is an argument, not an observation |
| Scenario repeatability is measured, not asserted | `tpch-scale-scenarios` | 7.1–7.3 | Three SF1 runs plus at least two SF10 runs. This is the largest single cost in the change |

## Consequences of the gap

The `sf10` scenario's declared floors — 15 GiB memory, 6 CPUs, 40 GB disk — rest
on **what one host had**, from the withheld run's provenance, not on what the run
consumed. They are marked provisional in `benchmarks/tpch/scenarios/sf10.sh` and
task 6.2 is their correction.

Until 7.1–7.3 run, there is no measured run-to-run spread, so the queries closest
to parity in `baseline/attribution.md` — q07 at 0.96x, q04 at 0.91x, q22 at 0.89x
— are not established as being outside noise. The attribution says so.

Nothing in this change publishes a performance claim, so the unverified items
block confirming the scenario's reproducibility, not the correctness of anything
already stated.
