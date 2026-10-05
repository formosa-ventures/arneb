## Why

At SF1 Arneb leads Trino by **1.76x** geomean; at SF10 it *trails* by **0.76x** (1.32x slower).
The sign of the release's headline claim flips between two scale factors, so `c6c2dea` withdrew
the SF10 results and scoped every published figure to SF1. The withdrawn run (`b288bd6`) shows the
loss is not uniform — scans and simple aggregation still win (q06 1.56x, q14 1.47x, q10 1.35x,
q01 1.26x) while multi-table joins and correlated subqueries collapse (q02 0.15x, q11 0.44x,
q21 0.46x, q18 0.50x, q20 0.54x, q05 0.55x, q09 0.56x, q08 0.60x, q17 0.67x).

Several changes are already in flight against exactly those shapes
(`planner-build-side-selection`, `dynamic-filter-provenance-targeting`, `dist-mxn-nested-joins`,
`planner-join-reorder`, `dist-adaptive-partition`). What is missing is the **measurement ground**
they are validated on. The SF10 number exists as a single historical run on one host; there is no
named, re-runnable scenario that a contributor can execute to reproduce it, no captured baseline
in the change to diff against, and no stated condition under which the withheld `sf10/` results
return to `official/`. Without that, "the join work landed" is unfalsifiable, and per the repo's
tiered validation workflow an expensive tier gets burned discovering harness problems instead of
engine problems.

This change builds that ground: SF1 and SF10 as first-class scenarios, both reproducible **entirely
on a local Docker host**, a captured baseline, a per-query attribution routing every SF10 regression
to the change that owns it, and an explicit republish gate.

## What Changes

- **Named scale scenarios.** SF1 and SF10 become declared scenarios of the benchmark harness rather
  than a `TPCH_SF` env default. Each scenario names its scale factor, engine topology, seeding
  source, run plan (warmup/measurement counts), and the host resource floor it requires.
- **Local-Docker reproducibility for both scenarios.** Each scenario runs end-to-end via
  `docker compose` on a single developer host — seed, engine rotation, measure, report, tear down —
  with no dependency on any remote benchmark host. Host preconditions (container memory ceiling,
  CPU allocation, free disk for the SF10 dataset and the image build) are checked and reported **before** seeding
  rather than surfacing as an OOM mid-run.
- **Preflight and teardown are part of the scenario.** A scenario that cannot be satisfied on the
  current host fails fast with the unmet precondition named, and leaves no half-seeded MinIO state.
- **Captured baseline.** The withdrawn SF10 run and the current SF1 run are captured under the
  change's `baseline/` directory as the diff target for the join work, distinct from
  `official/v1.0.0/` which stays reserved for published claims.
- **Per-query regression attribution.** Every SF10 query below 1.00x is attributed to a plan shape
  and routed to the in-flight change that owns it, so the remaining unowned gap is visible.
- **SF10 republish gate.** A stated, checkable condition for returning `sf10/` to
  `official/v1.0.0/` — re-measured on a declared scenario, with the honest number published
  whatever it is, not held back a second time.
- Scope boundary: this change does **not** implement join or subquery optimizations. Those belong
  to the changes named above. This change is the scenario, the baseline, the attribution, and the
  gate they are measured against.

## Capabilities

### New Capabilities

- `tpch-scale-scenarios`: Declared, re-runnable benchmark scenarios (`sf1`, `sf10`) that execute
  entirely on a local Docker host — scenario definition, host precondition preflight, deterministic
  seeding, engine rotation, and teardown — such that two contributors on comparable hosts produce
  comparable numbers from one command.

### Modified Capabilities

- `tpch-benchmark-runner`: scale factor becomes a recorded, first-class parameter of the run plan
  and its result document, rather than an environment default that leaves the produced numbers
  unlabelled as to scale.
- `release-baseline-numbers`: adds the withheld-scale-factor rule — what a release must state when
  it declines to publish a scale factor it measured, and the condition under which that scale
  factor is republished.

## Impact

- `docker-compose.yml`, `docker/tpch-bench/docker-compose.official.yml`, `docker/tpch-seed/seed.sh`
  — scenario wiring and SF10 resource shape.
- `benchmarks/tpch/scripts/run_benchmark.sh` — scenario selection, preflight, teardown.
- `benchmarks/tpch/src/` — scale factor recorded in the result document and comparison report.
- `benchmarks/tpch/RELEASE_CHECKLIST.md`, `benchmarks/tpch/README.md`, `docs/` — scenario
  invocation and the republish rule.
- `openspec/changes/sf10-join-performance/baseline/` — captured SF1 and SF10 baselines and the
  per-query attribution.
- No engine crate (`crates/execution`, `crates/planner`) behavior changes here; those land in the
  join changes this scenario measures.
