## Context

The benchmark harness today is a bash orchestrator (`benchmarks/tpch/scripts/run_benchmark.sh`)
over two compose files, driving a containerized Rust runner. It already does the hard parts
correctly: engine rotation so an idle JVM does not hold heap against its rival, `--no-deps` so
compose cannot resurrect the engine it just stopped, a Trino seed that brings up workers because a
lone coordinator with `node-scheduler.include-coordinator=false` cannot execute its own healthcheck.

What it does not have is a notion of **scale as a declared thing**. Scale enters as
`TPCH_SF="${TPCH_SF:-sf1}"` — one environment variable, defaulted, never recorded. The consequences
are concrete:

- A result document is scale-anonymous. The only carrier of "this was SF10" is the directory a human
  filed it in. That is a weak barrier between a SF10 number and a SF1 claim.
- SF10's resource shape is undeclared. Three Trino JVMs declare `-Xmx8G` each;
  `docker/trino/etc/config.properties` already carries a comment about being OOM-killed under
  Docker Desktop's 3.88 GB default. The withdrawn SF10 run (`b288bd6` provenance) needed 15.67 GiB
  of container-runtime memory. A contributor on a stock Docker Desktop allocation finds this out
  roughly forty minutes in, after seeding the dataset. (The withheld run's
  provenance calls that dataset "~10 GB"; measured after the 2026-07-26 repeat it
  is 2.0 GiB of Parquet — the provenance figure is the raw TPC-H text size.)
- There is no captured baseline inside the change and no attribution of the loss, so "the join work
  landed" has nothing to be checked against.

Everything here stays local, cheap, and repeatable — no remote benchmark host is involved at any
point. Constraint from the harness itself: the official path is
Rust-only, deliberately — Step 4's comment notes the report is rendered by the same Rust binary so
the harness has no Python dependency. Nothing added here may reintroduce one.

## Goals / Non-Goals

**Goals:**

- SF1 and SF10 are declared scenarios, each runnable end-to-end on one local Docker host from a
  single command.
- A scenario that the current host cannot satisfy fails **before** seeding, naming the unmet
  precondition and the observed value.
- Scale factor and scenario name travel inside the result document, the comparison report, and the
  provenance record — not only in the path.
- The SF1 and withdrawn SF10 baselines are captured in the change as the diff target.
- Every SF10 query below 1.00x is attributed to a plan shape and to the change that owns it, with
  "unowned" a permitted and visible answer.
- A stated, checkable condition returns `sf10/` to `official/`.

**Non-Goals:**

- No join, subquery, or planner optimization lands here. Those are `planner-build-side-selection`,
  `dynamic-filter-provenance-targeting`, `dist-mxn-nested-joins`, `planner-join-reorder`,
  `dist-adaptive-partition`.
- No scenario beyond `sf1` and `sf10`, and no remote-host scenario. The mechanism is general enough
  to declare more later; this change declares two, both local.
- No change to engine rotation, seeding strategy, or the statistics the runner computes.
- No claim that numbers from two different hosts are comparable. Scenarios make a run *repeatable*,
  not hardware-independent.

## Decisions

### D1 — A scenario is a sourced shell fragment, not a TOML file

`benchmarks/tpch/scenarios/<name>.sh`, containing assignments only, sourced by
`run_benchmark.sh`. Declares scale factor, run plan (`NUM_RUNS`, `WARM_UP`), per-node CPU
allocation, and the host floors (memory, CPU, free disk).

*Alternatives considered.* **TOML parsed by the Rust runner** — rejected because preflight has to
run on the host *before* compose comes up, and the runner binary only exists inside an image built
by that same compose. Bootstrapping it on the host means a host Rust toolchain and a build outside
the measured isolation, to read a file with six keys in it. **YAML + `yq`** — rejected: a new host
dependency for the same six keys. **Compose profiles alone** — rejected: profiles can select
services but cannot express a run plan or a host floor.

*Consequence.* The file is executable, so discipline is required: assignments only, no logic. A
scenario that needs logic is a signal the mechanism is wrong, not an invitation to script in it.

### D2 — Preflight runs on the host, in bash, before any seeding

`run_benchmark.sh` reads `docker info` (`.MemTotal`, `.NCPU`) and free space on the Docker data
root, compares them to the scenario's floors, and aborts naming the unmet precondition, its floor,
and the observed value.

*Alternative considered.* A warning rather than an abort — rejected. A warning that scrolls past at
minute two of a forty-minute run is not a control; the failure it predicts arrives as an OOM kill
with no obvious cause.

*Escape hatch.* `SKIP_PREFLIGHT=1` proceeds, and records `preflight_bypassed: true` into the result
document. A bypassed run must not be indistinguishable from a clean one — that is the whole point of
having the flag rather than telling people to comment out the check.

### D3 — Floors are declared per scenario and provisional until confirmed

SF1's floors come from the published run that already works. SF10's start from the only
configuration known to have completed: **15.67 GiB** container-runtime memory (`b288bd6`
provenance), so the floor is declared at 15 GiB rather than guessed lower. CPU floor covers 3 nodes ×
`BENCH_NODE_CPUS` plus the runner.

**Corrected by the 2026-07-26 repeat.** The disk floor was originally sized against the provenance's
"~10 GB of Parquet"; the dataset actually written is **2.0 GiB**, and the 40 GB floor turns out to be
sized by the image build (~13.5 GB of images plus ~18 GB of BuildKit cache), not by the data. The
memory floor is now observed working on two runs — and is still not a measured minimum, because
neither run instrumented peak consumption and no smaller host has been tried.

Declaring an observed-working number and correcting it is honest; declaring an optimistic guess and
letting contributors discover it is not. The same applies to leaving "observed working" labelled as
such rather than promoting it to "required".

### D4 — Scale factor is recorded in the result document

The runner gains `--scenario <name>` and `--scale-factor <sf>`, persisted into each engine's JSON,
echoed in the comparison report header, and carried into provenance (which already has a
`scale_factor` field — this makes the runner the source of it rather than a hand-written value).

*Backward compatibility.* `tpch-benchmark-reporting`'s stated purpose includes consuming documents
written by earlier versions, so the field is optional on read and renders as "unrecorded" when
absent. Existing `official/v1.0.0/sf1/` documents stay valid and are not rewritten.

### D5 — The baseline stores the comparison and provenance, not the raw per-engine documents

`baseline/sf1/` and `baseline/sf10/` hold `comparison.md` + `provenance.json`.

*Rationale.* The per-engine result documents are ~1,500 lines each; three engines × two scales is
~9,000 lines of JSON that is **already tracked** — SF1's under `official/v1.0.0/sf1/`, SF10's
recoverable from `b288bd6`. The baseline exists to be read by a human and cited by tasks. Copying
tracked JSON into a directory that will be archived is cost with no reader. Each baseline records
the commit its documents come from, so recovery is one `git show` away.

### D6 — Attribution is a table in the change, keyed by query, with "unowned" allowed

`baseline/attribution.md`: query, SF10 `arneb→trino` ratio, plan shape, owning change, evidence,
status. Every query below 1.00x gets a row.

The unowned rows are the deliverable, not an embarrassment — they are the honest statement of what
the current join push does not reach. Each row cites its evidence (a profile log, an `EXPLAIN`, or
the owning change's own measurement) so a claim of ownership is checkable rather than asserted.

### D7 — The republish gate is landing-based, not number-based

`sf10/` returns to `official/v1.0.0/` when (a) every owning change is archived or explicitly
deferred with that deferral recorded, and (b) a fresh run on the declared `sf10` scenario exists.
The resulting number is published **whatever it says**.

*Alternative considered and rejected.* Gating republication on `arneb→trino ≥ 1.00x`. That makes
publication conditional on the result — publication bias by construction — and would convert a
one-time withholding into a standing one. The engine is allowed to be slower at SF10; a reader is
not allowed to be unable to find that out.

*Corollary on disclosure.* While a measured scale factor is withheld, the disclosure must say a run
exists, why it is withheld, and where it can be read. Today `README.md:161` and `docs/index.md:45`
say larger scale factors "are not published yet", which a reader parses as *not measured*. The run
exists and is recoverable from `b288bd6`; saying so costs nothing and is the difference between
scoping a claim and hiding a result.

### D8 — `TPCH_SF` keeps working

The existing environment variable continues to select scale when no scenario is named, so every
documented invocation in `README.md`, `TUTORIAL.md`, and `RELEASE_CHECKLIST.md` keeps working
unchanged. Scenarios are the recommended path and the only path that gets preflight and recording.

## Risks / Trade-offs

- **SF10 needs ~15 GiB and a stock Docker Desktop gives less** → preflight names the exact setting
  and observed value before seeding; SF1 stays the default so the common case is untouched; the
  scenario docs state the Docker memory allocation to change.
- **Local SF10 numbers vary by host more than SF1 does** → provenance already records CPUs and
  memory; the scenario adds the declared floor, and the report states the figures are host-relative.
  Repeatability is claimed; cross-host comparability is not.
- **The 15 GiB floor may be an artifact of one host** → marked provisional; the first re-run
  confirms or corrects it, and that correction is an expected task rather than a defect.
- **Attribution can be wrong or self-serving** → every row cites evidence, and "unowned" is a
  permitted value. A row whose owning change lands without moving its query is a falsified
  attribution and gets corrected, not quietly dropped.
- **Adding fields to the result document could break older reports** → optional on read, rendered as
  "unrecorded"; no existing published document is rewritten.
- **A full local SF10 cycle is expensive (seed + 3 engines × 8 runs × 22 queries)** → `--queries`
  subsetting and `SKIP_SEED=1` cover iteration; only the republish run must be the full suite.
- **`sf10/` may be republished with a number that contradicts the headline** → that is the intended
  behavior of D7, and the README already scopes its claim to SF1, so the two coexist without
  either being retracted.

## Migration Plan

Additive. Scenario files, preflight, and the recorded fields land alongside the existing
`TPCH_SF` path (D8), so nothing that works today stops working. Rollback is deleting
`benchmarks/tpch/scenarios/` and the preflight block; no data or published result is migrated or
rewritten.

Order: scenario mechanism + preflight → capture baselines → confirm the SF10 floors with one local
re-run → attribution table → disclosure and republish-gate wording. The re-run comes before
attribution so attribution is written against a run this change can reproduce, not only against
`b288bd6`.

## Open Questions

- Is 15 GiB the true SF10 floor, or specific to the host that produced `b288bd6`? The first local
  re-run answers this and may move the floor in either direction.
- Does the withheld-scale disclosure belong in `docs/` as well as `README.md` and
  `RELEASE_CHECKLIST.md`, or is the docs site's SF1 scoping sufficient?
- Does the attribution table survive archival as a change artifact, or should its conclusions move
  into a spec once the owning changes land?
