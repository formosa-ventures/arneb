# SF10 regression attribution

Every query in the withheld SF10 run (`sf10/comparison.md`, recovered from
`b288bd6`) that trailed Trino, with the plan shape responsible and the change
that owns it. 15 of 22 queries are below 1.00x.

**`unowned` is a real answer here, not a gap in the analysis.** It means no
in-flight change addresses that shape, and it is the honest statement of what
this release's join push does not reach.

## Standing of this table

Every attribution below rests on the **owning change's own analysis**, produced
at SF1 or SF30. None of it has been measured at SF10 against this baseline. The
attributions are hypotheses about which lever moves which query, and a lever that
lands without moving its query falsifies its row — see "When an attribution is
wrong" at the bottom.

Ratios are `arneb→trino` p50: below 1.00x means Arneb is that fraction of Trino's
speed.

**Two measured caveats, from `sf1/SPREAD.md`:**

- The SF10 suite aggregate spans **6.8%** across three runs of an unchanged system
  (`sf10/SPREAD.md`); SF1 spans 2.7%. **q07 (0.95–0.96x) and q04 (0.91–0.98x) are
  inside that band and are not regressions.** q22 (0.85–0.89x) and q16 (0.86–0.88x)
  sit within roughly two noise-widths on one measurement each — treat them as weak.
  The losses at 0.61x and below are far outside any measured spread and are real.
- **q02's ratio varies by 52.8% across identical repeats** (2.62x–4.40x at SF1).
  Its 0.15x at SF10 is a single measurement of the noisiest query in the suite.
  The sign of that regression is not in doubt; its magnitude is not established,
  and the "worst query in the suite" framing rests on one sample.

## Multi-table joins

| Query | Ratio | Plan shape | Owning change | Evidence |
|---|---:|---|---|---|
| q02 | 0.15x ⚠️ | 6-way join whose derived table repeats `partsupp ⋈ supplier ⋈ nation ⋈ region` inside a `GROUP BY`, then joins the result back on `(p_partkey, ps_supplycost)`. **Not** the standard correlated form — this repo rewrote it as a join (see the query's own header comment) | `planner-join-reorder` | The planner builds join trees in SQL text order with no cardinality estimation, cost model, or statistics (`planner-join-reorder` proposal; `crates/planner/src/planner.rs::plan_from`). ⚠️ **Magnitude unreliable**: q02 is the noisiest query measured, 52.8% spread across identical SF1 repeats (`sf1/SPREAD.md`) |
| q05 | 0.55x | 6-way join, `region → nation → supplier/customer → orders → lineitem` | `planner-join-reorder` | Same: no cost-based ordering, so the tree follows the FROM clause |
| q08 | 0.60x | 8-way join with a `nation n1 / nation n2` self-join and a CASE aggregate | ~~`planner-build-side-selection`~~ → **unowned** | **FALSIFIED 2026-07-26.** The lever is already applied and q08 did not move — see "Falsified attributions" below |
| q09 | 0.56x | 6-way join with no selective filter on lineitem | `planner-join-reorder` | Named in the SF30 root-cause map as a probe-throughput bottleneck, distinct from q08's build-side defect |
| q21 | 0.46x | `lineitem ⋈ orders ⋈ supplier ⋈ nation` with `EXISTS` + `NOT EXISTS` over lineitem — semi-join and anti-join over the largest table | `planner-join-reorder` | Named in the SF30 root-cause map as an anti-join build bottleneck |
| q07 | 0.96x | 4-way join with a `nation n1 / nation n2` self-join | **inside noise** | Within the measured 6.8% band (`sf10/SPREAD.md`) — not a regression. The build-side lever is applied here too |
| q18 | 0.50x | 3-way join plus `o_orderkey IN (SELECT ... GROUP BY ... HAVING)` — a semi-join against a full aggregate over lineitem | ~~`planner-build-side-selection`~~ → **unowned** | **FALSIFIED 2026-07-26** — same reason as q08 |

### Falsified attributions — q08, q18, q07 (2026-07-26)

These three were attributed to `planner-build-side-selection` on the strength of
that change's SF30 analysis. **The lever is already in the tree and already
applied, and the queries did not move.** Evidence:

- `JoinReorder::new()` is in the default analyzer pipeline unconditionally
  (`crates/planner/src/analyzer/mod.rs:531`) — no gate, no env flag.
- The duplicate-name bail is gone: `has_duplicate_leaf_column_names` no longer
  appears in `join_reorder.rs`. Leaf-origin index tracking replaced it.
- `reorders_self_join_chain_puts_fact_on_probe_spine` — the RED test written to
  prove the q08 shape reorders — is **green** (336 planner tests, 0 failures;
  the recorded baseline was 241 pass / 1 fail).
- HMS carries real statistics for the seeded data (`SHOW STATS`: lineitem
  59,986,052 rows, nation 25, part 2,000,000), so the cost model is not falling
  back to `default_table_size`.
- `EXPLAIN` on the real q08 against SF10 Hive data puts **`TableScan: lineitem`
  at the base of the probe spine**, with the filtered `part` as the build side —
  the exact shape the change set out to produce. The `n1`/`n2` self-join did not
  block it.
- q07, q08, q18 and q02 are **cell-identical to Trino and DataFusion at SF10**
  (canonical hashes agree across all three engines, both runs) — so the
  corruption that caused the earlier F-Perf-RN revert is not present either.

And q08 is still 0.60–0.61x, q18 still 0.50–0.53x.

**What this does not say.** The change's stated payoff was SF30 **peak memory**
(q08 3.4× → ~1× Trino), not SF10 latency. Nothing here refutes that; it is
unverified, and verifying it needs the remote SF30 tier (that change's tasks
4.1–4.3). What is refuted is the claim that this lever explains q08's and q18's
**SF10 latency** gap. It does not, so those rows are `unowned` until something
else does.

`dynamic-filter-provenance-targeting` owns no query in this table directly. It is
a **correctness prerequisite**: `planner-build-side-selection` removes the
self-join reorder bail, which exposes a latent dynamic-filter bug that produced a
~5× wrong q08 result when tested. Its code is also already in the tree, and the
cross-engine hash agreement above is the evidence that it works.

`dist-mxn-nested-joins` and `dist-adaptive-partition` own nothing here either.
Their defect appears at hash fan-out N > 2; the benchmark topology is one
coordinator and two workers, so the SF10 run is unlikely to have exercised it.
Listing them as owners of these regressions would be attribution by association.
They matter for scaling past this topology, which is not what this baseline
measures.

## Correlated subqueries and other shapes — no owner

| Query | Ratio | Plan shape | Owning change | Evidence |
|---|---:|---|---|---|
| q17 | 0.67x | Correlated scalar subquery: `l_quantity < (SELECT 0.2*AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)` | **unowned** | `subquery-support` (complete, 2026-04-02) implements correlated subqueries by **nested-loop evaluation**, per its own proposal. Nested-loop cost scales with outer cardinality, which is exactly what changes from SF1 to SF10. No in-flight change decorrelates |
| q20 | 0.54x | Three nested `IN` subqueries, the innermost correlated on `ps_availqty > (SELECT 0.5*SUM(l_quantity) ...)` | **unowned** | Same nested-loop evaluation, three levels deep |
| q22 | 0.89x | Correlated scalar subquery on `AVG(c_acctbal)` plus `NOT EXISTS` over orders | **unowned** | Same |
| q11 | 0.44x | Scalar subquery in `HAVING`: `SUM(...) > (SELECT SUM(...) * fraction FROM ...)` | **unowned** | Same. The subquery is uncorrelated and could in principle be evaluated once; whether it is, is unverified |
| q04 | 0.91x | `EXISTS` over lineitem — semi-join | **unowned** | Possibly inside run-to-run noise; not yet established |
| q12 | 0.76x | 2-way join `orders ⋈ lineitem` with a CASE-based aggregate | **unowned** | No join-ordering choice exists in a 2-way join, so `planner-join-reorder` does not plausibly own it. Cause unidentified |
| q13 | 0.85x | `LEFT OUTER JOIN customer→orders`, then group-by-count-of-count | **unowned** | Outer-join build side is not covered by the build-side change, which addresses inner-join reorder. Cause unidentified |
| q16 | 0.86x | 2-way join plus `COUNT(DISTINCT ps_suppkey)` grouped aggregate. The file's header comment claiming `NOT IN` is **stale** — the query contains no subquery | **unowned** | Distinct-aggregate cost, not a join shape. Cause unidentified |

## What this says

**Revised 2026-07-26, after measurement.** Of 15 queries below 1.00x:

- **2 are not regressions** — q07 and q04 are inside the measured 6.8% band.
- **4 have an owner**: q02, q05, q09, q21, all `planner-join-reorder` (no cost
  model, no cardinality estimation — the broadest remaining lever).
- **9 are unowned**, including q08 and q18, whose attribution was falsified above
  when the lever they were assigned to turned out to be already applied.

The largest coherent unowned group is the **correlated-subquery cluster** — q17,
q20, q22, q11 — whose likely cause is named in a change that is already complete:
`subquery-support` evaluates correlated subqueries by nested loop, and
nested-loop cost scales with outer cardinality, which is precisely what changes
between SF1 and SF10. No in-flight change decorrelates.

The rest — q12, q13, q16 — match no identified cause at all. They are small
individually and easy to leave out of a summary; leaving them out is how a
partial explanation becomes a claimed one.

If every owning change lands and every unowned query stays where it is, the
suite geomean improves but does not necessarily reach 1.00x. That outcome is
publishable under the republish gate in `benchmarks/tpch/RELEASE_CHECKLIST.md`,
which is deliberately not conditioned on the number.

## When an attribution is wrong

An owning change lands, is confirmed to have taken effect, and its query does not
move. That row is then **falsified**, and it is corrected in place — the owning
change becomes `unowned` again with a note recording that the lever was tried and
did not explain the regression.

It is not deleted. A regression that was mis-attributed once is more likely to be
mis-attributed again, and the record of the failed explanation is what prevents
that.
