# Source of these documents

Recovered from commit `b288bd6`, the commit that published the SF10 run. The
next commit, `c6c2dea`, withdrew it — these files no longer exist on `main`.

- `comparison.md` — the withdrawn SF10 comparison report, unedited.
- `provenance.json` — the withdrawn SF10 provenance record.

## What is deliberately not here

The per-engine result documents (`arneb.json`, `trino.json`, `datafusion.json`,
1,484 lines each) are **not copied**. They remain recoverable:

```bash
git show b288bd6:benchmarks/tpch/official/v1.0.0/sf10/arneb.json
git show b288bd6:benchmarks/tpch/official/v1.0.0/sf10/trino.json
git show b288bd6:benchmarks/tpch/official/v1.0.0/sf10/datafusion.json
```

Copying ~4,500 lines of JSON that git already holds into a directory that will be
archived would add no reader and no recoverability.

## What this run showed

Geomean `arneb→trino` **0.76x** — Arneb 1.32x slower — reversing the SF1 result
of 1.76x. The loss is not uniform:

| Still winning | Losing |
|---|---|
| q06 1.56x, q14 1.47x, q10 1.35x, q01 1.26x, q15 1.20x, q19 1.14x, q03 1.09x | q02 0.15x, q11 0.44x, q21 0.46x, q18 0.50x, q20 0.54x, q05 0.55x, q09 0.56x, q08 0.60x, q17 0.67x |

Scans and simple aggregation hold; multi-table joins and correlated subqueries
collapse. Per-query attribution is in `../attribution.md`.

## Why it was withdrawn

`c6c2dea`: rather than publish a scale factor the engine had not been tuned for,
the results and the prose describing them were withdrawn until that work is done.
The measurement improvements SF10 forced — engine rotation, `--no-deps`, seeding
that brings up Trino's workers, a healthcheck that allows for JVM startup —
stayed, because they improved SF1's numbers too.

This baseline is the diff target for that work, and the condition for
republishing is stated in `benchmarks/tpch/RELEASE_CHECKLIST.md`.

## Host it ran on

Apple M1 Pro, 10 cores, 32 GB, macOS 26.5.2, arm64, OrbStack with **15.67 GiB**
allocated to the container runtime. That figure is the only evidence for the
`sf10` scenario's declared memory floor, and it is an observed-working value —
not a measured minimum. See `benchmarks/tpch/scenarios/sf10.sh`.
