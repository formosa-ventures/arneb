## RESOLUTION (2026-06-12/13) — solved via a SHORTER path than this plan anticipated

The measure-first §1 work (EXCHTRACE, committed `7b5e3ae`) did its job and
SUPERSEDED §3's exchange-reliability lever: the trace showed the first
receiver-drop was `RECV_DROP_MIDSTREAM` with **zero** `RECV_ERR` — an operator
ABANDONING the probe stream, NOT an h2/tonic idle timeout or flow-control reset.
The abandon was `execute_grace_single`'s empty-build arm dropping its un-drained
remote probe `rest`. So the fix was a one-arm operator change (drain `rest` to
EOF before returning empty, `bba7107`), not the D2 "spill the probe during the
FIXED build" lever this change was scoped around. q02 has the same shape and was
fixed by the same commit (no separate D4 OOM work needed — q02 is cell-correct
and within `mem_limit` in the SF30 bench).

A Codex review then hardened the surrounding safety net (this branch, commits
`9b00047` / `4332cee` / `179da3a` / `59a665f` / `b9456fd`): structural
`may_stop_input_early` (TopK/blocking-child drains → must_drain), the sibling
empty-build drain in `execute_single_finish_streaming_multi`, the spill-drain
`must_drain` hole, the q10 oracle tiebreak + rel-tol reconcile, and the loud
`ARNEB_MUST_DRAIN=0` log + oracle refusal.

Outcome (validated): SF30 `blast_radius_oracle.py` all-22 ×2 = 22/22 🟢
(deterministic + cell-identical to Trino); memory bench 22/22 correctness, no
regression. Tasks below reflect what was actually achieved.

## 1. Measure-first: pin the exact idle-connection-drop trigger (GATING — no lever before this)

- [x] 1.1 `[EXCHTRACE]` per-ticket SERVE/RECV/RECV_ERR/RECV_DROP_MIDSTREAM probe committed (`7b5e3ae`), gated by `ARNEB_TRACE_FRAGMENTS`.
- [x] 1.2 Connection-lifecycle tracing added (producer + consumer side) under `ARNEB_TRACE_FRAGMENTS`.
- [x] 1.3 Ran q21/q02 on the loaded SF30 stack with tracing: FIRST drop = `RECV_DROP_MIDSTREAM`, zero `RECV_ERR` → classified (d) lazy operator abandon (the empty-build arm dropping `rest`), NOT (a) idle timeout / (b) flow-control / (c) admission.
- [x] 1.4 Trigger recorded; lever chosen = DRAIN `rest` (the operator-local fix), which made the D2 exchange-spill lever unnecessary.

## 2. Reproduce + oracle baseline

- [x] 2.1 Baseline established: `blast_radius_oracle.py` all-22 ×2 SF30 N=2 → q21 + q02 ERROR (Layer-1 `must_drain` firing), other 20 clean (committed gate `d78214b`).
- [x] 2.2 Fast unit repro added in place of a synthetic harness: `grace_single_empty_build_overflow_drains_rest` (forces the empty-build + probe-overflow arm via a tiny `ARNEB_PROBE_COLLECT_MAX_BYTES`) reproduces the drop deterministically without a full SF30 run.

## 3. Implement the chosen lever (per §1.4)

- [x] 3.1 (TDD) Failing test capturing the invariant added: `grace_single_empty_build_overflow_drains_rest` (verified discriminating — pre-fix it fails left:1 right:4) + the sibling `grace_single_multi_empty_build_drains_probe`.
- [x] 3.2 Lever implemented = the measured alternative, NOT D2: drain the un-collected probe `rest`/`left_stream` to EOF on the empty INNER build before returning empty (`bba7107` for `execute_grace_single`; `4332cee` for `execute_single_finish_streaming_multi`).
- [x] 3.3 Change is operator-local (INNER + no-residual grace single-build arm); single-node and non-FIXED paths untouched.
- [x] 3.4 `cargo fmt` + `cargo clippy -D warnings` + `cargo test`/nextest green (553 touched-crate tests).

## 4. Resolve q02's co-occurring SF30 OOM (D4)

- [x] 4.1 No separate OOM materialised: q02 shares the empty-build shape and was fixed by the same drain-`rest` commit; it is cell-correct and within `mem_limit` in the SF30 bench.
- [x] 4.2 N/A — the fix did not trade the stall for an OOM (q02 memory within cap, no aggregate spill change needed).

## 5. Validate (oracle-gated, MANDATORY after every §3/§4 step)

- [x] 5.1 SF30 `blast_radius_oracle.py` all-22 ×2: q21 AND q02 flipped ERROR → 🟢 (deterministic + cell-identical to Trino); the other 20 stayed clean. Re-validated after each hardening commit (combined-tree 21/22 🟢 + q10 LIMIT-tie, then 22/22 🟢 with the q10 oracle fix).
- [x] 5.2 Determinism: q21 + q02 identical result sets across runs (the tie-immune signal; ×2 per gate run, multiple gate runs this session).
- [x] 5.3 SF30 memory bench (`run_memory_bench.sh` + `verify_memory.py`): 22/22 correctness, no OOM, no regression vs baseline.
- [x] 5.4 Layer-1 `must_drain` guard confirmed still firing (unit: `drain_spill_must_drain_fails_loud_when_consumer_closes`; and the pre-fix SF30 baseline showed it firing as q21/q02 ERROR).

## 6. Wrap up

- [x] 6.1 `[EXCHTRACE]` probe committed as permanent diagnostic infra (`7b5e3ae`), gated by `ARNEB_TRACE_FRAGMENTS` (kept, not reverted).
- [x] 6.2 Committed (Taiwan-window backdated); memory updated with the measured trigger, the chosen lever, and the validated result.
- [ ] 6.3 Restore the remote bench stack to a clean default; remove temp trace files. (Deferred: the loaded SF30 stack is left UP for the next session per the bench-host convention; no temp overlays/trace files were added this session.)
