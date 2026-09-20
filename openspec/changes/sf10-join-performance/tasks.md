# Tasks

> **Status reset, 2026-09-20.** Every box below was previously ticked, but the
> work behind sections 1.3–4.4 and 6–10 existed in no branch — not on `main`,
> not on `release/v1.0.0`. The ticks recorded intent, not implementation.
>
> Two things changed on that date. First, `release/v1.0.0` — which carried a
> parallel benchmark harness (`runner.rs`, `report.rs`, `engines/`,
> `official/v1.0.0/sf1/`) that was never merged — was deleted, locally and on
> `origin`, by decision of the maintainer; its tip was `c6c2dea`. The scenario
> mechanism was therefore rebuilt from scratch against the harness `main`
> actually has: `run_memory_bench.sh` for measurement, `bench_report.py` for the
> report, and `benchmarks/tpch/src/main.rs` for the JSON runner.
>
> Second, the SF10 regressions this change was created to attribute **no longer
> reproduce**. The 2026-09-20 run has arneb ahead of Trino on all 22 queries on
> both latency and peak memory, cell-identical to Trino on all 22. Sections 8
> and 5 are superseded by that result rather than completed.
>
> A box is ticked below only where the thing it describes exists and was
> verified on 2026-09-20. Where the task named a file that only ever existed on
> the deleted branch, it is marked ADAPTED with what replaced it, or DEFERRED
> with why.

## 1. Scenario mechanism

- [x] 1.1 Create `benchmarks/tpch/scenarios/` and add `sf1.sh` and `sf10.sh` as assignment-only shell fragments declaring the scale factor, run plan, per-node CPU allocation, and host floors. ADAPTED: every key carries a `SCENARIO_` prefix (`SCENARIO_TPCH_SF`, `SCENARIO_FLOOR_MEM_GIB`, …) so sourcing a fragment cannot clobber a variable the operator exported — which is what makes the precedence in 1.3 implementable rather than aspirational
- [x] 1.2 Set the floors. `sf1.sh` floors are DERIVED and say so in the file. `sf10.sh` declares `FLOOR_MEM_GIB=15`, now resting on an observed 2026-09-20 run at 15.66 GiB that completed all 22 queries twice (cold and warm) rather than on the withdrawn run's provenance; `FLOOR_DISK_GB=40` is sized by the image build, with the measured 2.0 GiB Parquet figure recorded in the file
- [x] 1.3 Add `--scenario=<name>` to `run_benchmark.sh`, sourcing the matching fragment before argument defaults are applied. Precedence is explicit CLI flag > environment variable > scenario > built-in default, implemented by capturing operator-set environment values before the source
- [x] 1.4 Abort with the requested name and the list of declared scenarios when `--scenario` names a fragment that does not exist, before any `docker compose` invocation — verified: `--scenario=sf99` exits 1 listing `sf1`, `sf10`, with no container touched
- [x] 1.5 Keep the bare `TPCH_SF` path working. ADAPTED: `TUTORIAL.md` and `RELEASE_CHECKLIST.md` existed only on the deleted branch; the documented forms that remain are in the root `README.md` and `benchmarks/tpch/README.md`, both updated and invoked

## 2. Host precondition preflight

- [x] 2.1 Preflight function in `run_benchmark.sh` reading container-runtime memory and CPU count from `docker info --format`, and free disk. NOTE: on macOS the Docker data root is inside a VM, so free space is read from the host filesystem that backs it — the comment in the script says so rather than implying an exact reading
- [x] 2.2 Abort on any unmet floor naming the precondition, the declared floor, and the observed value — one line per unmet floor, all reported. Verified with a scenario declaring impossible floors: all three were listed, not just the first
- [x] 2.3 Preflight runs before Step 1, so no container starts and nothing is seeded when a floor is unmet — verified
- [x] 2.4 `SKIP_PREFLIGHT=1` bypasses, prints that the run is bypassed at the point of bypass, and sets the marker that reaches the result document — verified
- [x] 2.5 Verified by running a scenario with deliberately raised floors and confirming the abort happens before MinIO comes up

## 3. Runner records the run's scale

- [x] 3.1 `--scenario <name>`, `--scale-factor <sf>`, and `--preflight-bypassed` added to the runner CLI in `benchmarks/tpch/src/main.rs`
- [x] 3.2 `scale_factor: Option<String>`, `scenario: Option<String>` added to `BenchmarkResult`, using `#[serde(default, skip_serializing_if = "Option::is_none")]` so documents written before this change still parse. ADAPTED: on `main` the struct lives in `src/main.rs`; `src/runner.rs` existed only on the deleted branch
- [x] 3.3 `preflight_bypassed: Option<bool>` added and populated from the CLI marker
- [x] 3.4 No code path infers scale from the output directory or file name; an absent `--scale-factor` records `None`
- [ ] 3.5 DEFERRED — pass `--scenario`/`--scale-factor` from `run_benchmark.sh` into the containerized runner. `run_benchmark.sh` no longer invokes the Rust runner: it delegates measurement to `run_memory_bench.sh`, the container-isolated dual-axis path, because that is what removes the native-vs-container bias the old script had. The scale travels with the measurement by the equivalent mechanism in 4.3 — a provenance sidecar written next to the CSV. The runner's own flags (3.1–3.4) remain for the `cargo run -p tpch-bench` path
- [x] 3.6 Unit test deserializing a fixture document that lacks all three new fields, asserting each parses as `None`; plus a round-trip test asserting the fields are omitted from the serialized form when absent

## 4. Report and provenance carry the scale

- [x] 4.1 Scale factor and scenario render in the comparison report header, printing `unrecorded` when absent rather than assuming a scale. ADAPTED: rendered by `bench_report.py`, the report `main` actually has; `report.rs` existed only on the deleted branch
- [x] 4.2 `preflight_bypassed` is surfaced in the report header when true, as a blockquote, so a bypassed run is visibly bypassed
- [x] 4.3 The harness is the source of `scale_factor`: `run_benchmark.sh` writes `<csv-stem>.provenance.json` from the run it just performed — scenario, scale, warm-up state, bypass marker, observed `lineitem` row count, timestamp. Nothing is hand-written
- [ ] 4.4 DEFERRED — extend the `report.rs` test fixture. `report.rs` does not exist on `main`. `bench_report.py` has no test suite to extend; both rendering paths (sidecar present, sidecar absent) were verified by invocation instead. Adding a Python test suite for the report is worth doing and is not in this change's scope

## 5. Capture the baselines

- [ ] 5.1 SUPERSEDED — `baseline/sf1/published-reference/` was copied from `official/v1.0.0/sf1/` on the deleted branch and cites commits that are no longer reachable from any ref
- [ ] 5.2 SUPERSEDED — `baseline/sf10/` holds the withdrawn run recovered from `b288bd6`, likewise unreachable now
- [ ] 5.3 SUPERSEDED — the diff target this change needed is now the 2026-09-20 run in `benchmarks/tpch/results/`, which reverses the regression the baselines were captured to measure. The `baseline/` directory should be deleted or re-sourced; left in place pending that decision

## 6. Confirm the SF10 floors on a local host

- [x] 6.1 SF10 measured end to end on one local Docker host: two full 22-query suites (cold and warm) through `run_memory_bench.sh`, both engines in rotation, plus a 2-query suite through the `--scenario=sf10` entry point exercising preflight → infrastructure → seed guard → measurement → provenance → report. PARTIAL in one respect, stated rather than glossed: the scenario entry point has not itself driven all 22 in one invocation — the 22-query evidence comes from the measurement script it delegates to
- [x] 6.2 Memory, CPU, and disk actually consumed recorded, and `sf10.sh`'s floors corrected to rest on that observation; the provisional marker is removed and replaced with what is and is not measured
- [x] 6.3 The report of that run carries scale factor and scenario in its header
- [ ] 6.4 DEFERRED — interrupt a seed partway and confirm the re-run recovers without manual MinIO/metastore cleanup. Not run: the only resident dataset is the SF10 data both 2026-09-20 measurements used, and the test destroys it. Worth doing on a host that can afford the re-seed
- [x] 6.5 `SKIP_SEED=1` against a resident dataset of a different scale aborts rather than measuring one scale under another's label — the guard compares the resident `lineitem` count against the scenario's declared count and refuses on mismatch

## 7. Repeatability spread

- [ ] 7.1 DEFERRED — three SF1 repeats on one unchanged host. Not run; SF1 would require re-seeding over the resident SF10 dataset
- [ ] 7.2 DEFERRED — depends on 7.1
- [ ] 7.3 PARTIAL — two full SF10 suites were run on 2026-09-20 (cold and warm-up variants). They are not a spread: they differ by a deliberate parameter change, so they measure that parameter, not run-to-run variance. Two repeats under identical settings are still owed before any difference below the unmeasured noise floor is called a result

## 8. Per-query attribution

- [ ] 8.1 SUPERSEDED — no SF10 query is below 1.00x. The attribution table this section specifies has no rows to hold
- [ ] 8.2 SUPERSEDED — the multi-table join regressions (q05, q08, q09, q18, q21) are all above 1.00x in the 2026-09-20 run
- [ ] 8.3 SUPERSEDED — likewise the correlated-subquery regressions (q02, q11, q17, q20, q22)
- [ ] 8.4 SUPERSEDED — see 8.1
- [x] 8.5 The note on correcting a falsified attribution rather than deleting it is what this status block does: the attribution is not deleted, it is recorded as overtaken by a later measurement, with the measurement named

## 9. Disclosure and the republish gate

- [x] 9.1 ADAPTED — the "measured and withheld" wording had nothing to attach to: `README.md:161` and `docs/index.md:45` as cited were lines on the deleted branch. The root `README.md` now states the scale, the date, the host, and the warm/cold caveat of the run it reports
- [ ] 9.2 DEFERRED — the republish gate belongs in `benchmarks/tpch/RELEASE_CHECKLIST.md`, which existed only on the deleted branch. A replacement checklist is worth writing and is not in this change's scope
- [ ] 9.3 DEFERRED — depends on 9.2
- [x] 9.4 The scenario invocation is documented in `benchmarks/tpch/README.md` and the root `README.md`, and `run_benchmark.sh` is the single entry point
- [x] 9.5 No reader-facing surface claims a scale factor the published run does not contain; the README states SF10 and names the measurement date

## 10. Verification

- [x] 10.1 `cargo build` and `cargo test` pass in `benchmarks/tpch/`, including the backward-compatibility tests from 3.6
- [x] 10.2 A result document lacking the new fields renders with scale `unrecorded` and no error — verified against `benchmarks/tpch/results/memory_total_20260920_112332.csv`, which has no sidecar
- [x] 10.3 `shellcheck` clean on `run_benchmark.sh` and both scenario fragments
- [ ] 10.4 DEFERRED — `openspec validate sf10-join-performance`. The change's own delta specs still describe the deleted branch's file layout and would need rewriting first
- [x] 10.5 Deferred items are listed above with their reasons rather than silently dropped
