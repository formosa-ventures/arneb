# cross-source-join — measurements (2026-10-20)

Mixed TPC-H layout at **SF10**: `nation`, `region`, `customer`, `supplier`,
`part` and `partsupp` in PostgreSQL 17.6 (`postgresql.tpch_mixed`, seeded by
`pg-tpch-seed`), `orders` and `lineitem` in Hive on RustFS (`hive.tpch`).
DOUBLE money columns (`TPCH_DECIMAL=0`). Queries: `benchmarks/tpch/queries-mixed`.

**Host:** Apple M1 Pro, 10 cores, 32 GiB, Docker Desktop. **Tier:** 2 (local).
Base commit `fd1bc61` plus this change. Not a remote SF30 number.

## Seed parity

Every dimension table matches `hive.tpch` in Trino: same column names, same
types, same row counts (region 5, nation 25, supplier 100,000, customer
1,500,000, part 2,000,000, partsupp 8,000,000). The SF10 seed took 1 min 39 s.

## Correctness gate — 22/22 both modes

`blast_radius_oracle.py --layout mixed --runs 2` (determinism run-to-run plus a
tolerance-aware cell diff against Trino on the same layout):

| Mode | Result | Log |
|---|---|---|
| Standalone (host release binary, `tpch-mixed.toml`) | **22/22 🟢** | [gate-sf10-standalone.txt](gate-sf10-standalone.txt) |
| Distributed (bench cluster: coordinator + 2 workers, `docker-compose.mixed.yml`) | **22/22 🟢** | [gate-sf10-distributed.txt](gate-sf10-distributed.txt) |

The first standalone run failed q11. That was a pre-existing standalone bug in
scalar-subquery handling, not a federation bug (design.md D8): the all-Hive q11
fails the same way on a standalone server. It is fixed in this change, and the
run above includes the fix.

## Pushdown evidence (PostgreSQL's own statement log)

`pg_pushdown_evidence.py` brackets each query with marker statements and reads
what PostgreSQL logged for `application_name=arneb`:
[standalone](pg-pushdown-evidence-sf10-standalone.md),
[distributed](pg-pushdown-evidence-sf10-distributed.md).

- **Projection** is pushed on every scan. For example, q05 reads only
  `"c_custkey"` from `customer`.
- **Static filters** are pushed. Examples: q05 `("r_name" = 'ASIA')`, q02
  `("p_size" = 15) AND ("p_type" LIKE '%BRASS')`, q07
  `(("n_name" = 'FRANCE') OR ("n_name" = 'GERMANY'))`, q11 `("n_name" = 'GERMANY')`.
- **Dynamic filters** reached PostgreSQL in **standalone** mode for:

  | Query | Scan | Remote predicate from a dynamic filter |
  |---|---|---|
  | q09 | supplier | `("s_suppkey" BETWEEN 1 AND 100000)` (compacted) + `("s_nationkey" IN (…25 values))` |
  | q20 | partsupp | `("ps_partkey" BETWEEN 5 AND 1999994)` (compacted) |
  | q20 | supplier | `("s_suppkey" BETWEEN 2 AND 100000)` (compacted) |

  In **distributed** mode, none of the 22 queries put a dynamic filter into the
  PostgreSQL SQL. Their PostgreSQL scans are probe sides of partitioned joins
  in another fragment, and cross-fragment dynamic filtering is off by default
  (design.md Non-Goals, "Risks").

**What this says about the range compaction:** every compacted range above
spans almost the whole key space (`s_suppkey` 1..100000 of 100000). It is
correct and bounded, but it prunes next to nothing on TPC-H's scattered keys.
That is the risk D2 named. A follow-up (a bound `= ANY($1)` array, or
cross-fragment filters) is where the transfer savings would come from.

## SF10 latency and peak memory — Arneb vs Trino, same layout

`EXTRA_COMPOSE=docker/arneb-bench/docker-compose.mixed.yml QDIR=benchmarks/tpch/queries-mixed run_memory_bench.sh`:
cold runs (cluster restarted and storage flushed before each query), 3 nodes
per engine at 2.7 CPUs each, memory = simultaneous cluster peak RSS. Raw:
[memory_total_sf10_mixed.csv](memory_total_sf10_mixed.csv).

| Query | arneb (ms) | Trino (ms) | Speedup | arneb (MB) | Trino (MB) | Mem vs Trino |
|-------|-----------:|-----------:|--------:|-----------:|-----------:|-------------:|
| q01   |       1855 |       6313 |    3.4x |        385 |       5042 |        0.08x |
| q02   |       2570 |      11837 |    4.6x |        197 |       3627 |        0.05x |
| q03   |       2333 |       8393 |    3.6x |       1810 |       5430 |        0.33x |
| q04   |       1690 |       7604 |    4.5x |        534 |       6424 |        0.08x |
| q05   |       2398 |       8956 |    3.7x |        934 |       5492 |        0.17x |
| q06   |        753 |       4558 |    6.1x |        274 |       4659 |        0.06x |
| q07   |       2199 |       7854 |    3.6x |       1073 |       5239 |        0.20x |
| q08   |       2986 |       9097 |    3.0x |       1527 |       6476 |        0.24x |
| q09   |       5469 |      21452 |    3.9x |       1482 |      13797 |        0.11x |
| q10   |       2204 |      11987 |    5.4x |        894 |       5563 |        0.16x |
| q11   |       3822 |       3953 |    1.0x |        146 |       3210 |        0.05x |
| q12   |       1412 |       6702 |    4.7x |        304 |       4467 |        0.07x |
| q13   |       1605 |       7528 |    4.7x |        368 |       5916 |        0.06x |
| q14   |       1332 |       7501 |    5.6x |        792 |       5043 |        0.16x |
| q15   |       1102 |       8765 |    8.0x |        328 |       6299 |        0.05x |
| q16   |       1460 |       8056 |    5.5x |        269 |       3901 |        0.07x |
| q17   |       3074 |      10180 |    3.3x |       1445 |       9248 |        0.16x |
| q18   |       3929 |      10326 |    2.6x |       2078 |      10074 |        0.21x |
| q19   |       1539 |       8417 |    5.5x |        770 |       5249 |        0.15x |
| q20   |       2643 |      11947 |    4.5x |        558 |       6029 |        0.09x |
| q21   |       3767 |      14787 |    3.9x |        921 |       7511 |        0.12x |
| q22   |       1254 |       7403 |    5.9x |        276 |       5337 |        0.05x |

Geomean speedup: 4.1x   ·   Mean peak-mem vs Trino: 0.12x

No threshold gates this change (M2 gates on correctness and pushdown).
PostgreSQL's own memory and CPU are not included for either engine, and both
engines read the same PostgreSQL instance.
