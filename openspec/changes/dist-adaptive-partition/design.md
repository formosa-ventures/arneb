## Context

The distributed fragmenter inserts hash-repartition exchanges and currently
hard-codes their fan-out: `crates/planner/src/fragment.rs:615`
(`let partition_count = 2usize;`). The downstream machinery is already
partition-count-parametric:

- `crates/server/src/coordinator.rs:312-325` reads `*partition_count` from the
  exchange node to drive α/β (producer/consumer) task scheduling (M×N).
- The W3-Hash partitioned-probe path executes an N-way fan-out with a
  deterministic hash seed (landed previously; distributed 14/16 at the time).

So the `2` is a conservative default, not a structural limit. The SF30 profile
shows the cost: `orders ⋈ lineitem` produces ~90M rows / 5.2 GB, split two ways
→ ~45M rows per partition → per-partition memory pressure that OOM-kills q09/q18.

This is the lowest-risk first step of the "distributed brain" program: the brain
is the cost-based decision layer (elide / broadcast / co-locate). Before any of
that, simply choosing a *better N* shrinks the per-partition working set with no
new exchange mechanism.

## Goals / Non-Goals

**Goals:**
- Replace the fixed `2` with an adaptive count = f(worker_count, estimated_cardinality).
- Expose a runtime `ARNEB_*` knob (default in source; env override; log effective value).
- Preserve correctness exactly: N-way fan-out (N ≥ 2) cell-identical to 2-way.
- Reduce per-partition peak RSS on q09/q18 at SF30 without regressing the SF10 winning set.

**Non-Goals:**
- No exchange elision / property derivation (`dist-ensure-requirements`).
- No broadcast join (`dist-broadcast-join`).
- No memory accounting / spill work (`exec-memory-accounting`).
- No change to the `Partitioning` enum, RepartitionExec, or coordinator scheduling
  shape — only the *value* of N flowing into them.

## Decisions

### D1 — The adaptive rule

`partition_count = clamp(candidate, min = 2, max = ARNEB_MAX_HASH_PARTITIONS)`
where `candidate = max(worker_count, ceil(estimated_rows / target_rows_per_partition))`.

- **Why worker_count as a floor**: every worker should get at least one
  partition of work, otherwise the cluster is under-utilised. (The previous `2`
  starved clusters with > 2 workers.)
- **Why the cardinality term**: it is what actually bounds the per-partition
  working set — the SF30 OOM is driven by rows-per-partition, not worker count.
- **target_rows_per_partition** is the primary tuning lever (a knob), e.g. a few
  million rows so a partition's intermediate stays well under the spill budget.
- _Alternative considered_: fixed `= worker_count`. Rejected — ignores data
  volume, so a 2-worker SF30 run stays at 2 (the current problem).
- _Alternative considered_: a pure data-size rule ignoring worker count.
  Rejected — can produce N < worker_count and idle workers.

### D2 — Where the estimate comes from

Reuse the planner's existing cardinality estimation (the cost model already
produces `estimated_cardinality` for nodes; the join-reorder cost model consumes
it). The fragmenter reads the child's estimate at the point it inserts the
exchange. If the estimate is unavailable, fall back to the worker-count-only
default (D1 with the cardinality term dropped). _No new statistics
infrastructure_ — if per-column NDV stats are absent, the existing default
selectivities already feed the estimate (that gap is owned by join-reorder /
later `dist-ensure-requirements`, not here).

### D3 — The knob(s)

A single primary runtime knob `ARNEB_HASH_PARTITION_TARGET_ROWS`
(target_rows_per_partition) plus a guardrail `ARNEB_MAX_HASH_PARTITIONS`.
Resolved in `config.rs` like the existing `resolve_*` helpers (env → config →
default), and the effective value logged — mirroring `resolve_budget()` /
`resolve_query_cap()`. Defaults chosen so that small/SF1 plans are unaffected
(they already produce ≤ 2 partitions' worth of data) and SF30 intermediates fan
out wider.

### D4 — Determinism

The hash assignment must remain deterministic across N (the W3-Hash deterministic
seed already guarantees this). The adaptive count itself must be a pure function
of (worker_count, estimate, knobs) so the same plan + same cluster always yields
the same N — important for reproducible benches and the cell-parity gate.

## Risks / Trade-offs

- **[N > 2 path under-exercised]** The partitioned-probe + M×N exchange has been
  run mostly at N = 2 in routine benches. → Mitigation: the spec's cell-parity +
  no-loss/no-dup scenarios; validate at SF1 (trino-diff) AND SF30 cell-diff vs
  Trino before declaring done. This is exactly the class of change that has
  reverted before (A.4, broadcast v1), so correctness gating is non-optional.
- **[Partition skew]** A skewed hash key (e.g. many rows on one key) means more
  partitions does not evenly reduce the hot partition. → Mitigation: out of
  scope to *fix* here; but measure per-partition peak RSS at SF30 to confirm the
  even-case win and document any residual skew for a later change.
- **[Too-high N adds exchange/scheduling overhead]** Many tiny partitions add
  per-partition fixed costs and more Flight streams. → Mitigation: the
  `target_rows_per_partition` knob + `ARNEB_MAX_HASH_PARTITIONS` cap; tune on the
  SF30 latency measurement, not just memory.
- **[Estimate quality]** A bad cardinality estimate picks a poor N. → Mitigation:
  the worker-count floor bounds the downside; unknown-estimate fallback is
  deterministic; the result is still *correct*, only sub-optimal.

## Migration Plan

Pure internal planner change; no wire/API/schema change, no data migration.
Rollback = set `ARNEB_HASH_PARTITION_TARGET_ROWS` high enough (or
`ARNEB_MAX_HASH_PARTITIONS=2`) to reproduce the old fixed-2 behaviour without a
revert. Land behind the cell-parity gate; commit only when SF30 cell-diff is
green.

## Open Questions

- Default `target_rows_per_partition`: pick by measuring SF30 q09/q18 per-partition
  peak RSS vs latency across a small sweep (e.g. 1M / 4M / 16M rows-per-partition)
  on the remote host. The default is an empirical choice, recorded in tasks.
- Should the cardinality estimate be the child's *output* rows or the *build/probe*
  side specifically? Resolve against `fragment.rs`'s exchange-insertion site —
  the estimate must reflect what actually flows through the exchange.
