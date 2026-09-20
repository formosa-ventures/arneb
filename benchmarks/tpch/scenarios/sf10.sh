# shellcheck shell=bash
# shellcheck disable=SC2034  # every value here is read by the sourcing script
#
# Scenario: sf10 — the scale the README's arneb-vs-Trino claim is measured at.
#
# Sourced by benchmarks/tpch/scripts/run_benchmark.sh. Assignments only: no
# conditionals, no command substitution, no logic. A scenario that needs logic is
# a sign the mechanism is wrong, not an invitation to script inside it.
#
# Precedence when the harness runs: an explicit CLI flag beats an environment
# variable, which beats these values, which beat the script's built-in defaults.

SCENARIO_TPCH_SF=sf10
SCENARIO_NUM_RUNS=8
SCENARIO_WARM_UP=3
SCENARIO_NODE_CPUS=2
SCENARIO_RUNNER_CPUS=6

# Row count of the largest table at this scale, used to check that data left
# over from a previous run is this scale's data before SKIP_SEED=1 measures it.
# Observed directly on the seeded dataset, 2026-09-20.
SCENARIO_LINEITEM_ROWS=59986052

# ---------------------------------------------------------------------------
# Host floors
# ---------------------------------------------------------------------------
# MEMORY IS AN OBSERVED-WORKING VALUE, NOT A MEASURED MINIMUM. Full 22-query
# suites have completed at 15.66 GiB of container-runtime memory (most recently
# 2026-09-20: all 22 queries, both engines, cold and warm variants, no OOM).
# No smaller host has been tried, so 15 is what is known to work rather than a
# floor derived from what the run needs. It is declared at 15 rather than 15.66
# so the host that produced those runs is not refused by integer rounding.
#
# Why sf10 is memory-shaped at all: three Trino JVMs declare -Xmx8G each. Engine
# rotation (only the measured engine resident, the rest stopped rather than idle)
# is what keeps the combined ceiling under what the runtime has; without it sf10
# does not fit on this class of host. The measured peaks bear this out — Trino's
# cluster peaks at ~9.5 GB on q18 while arneb peaks at ~4.5 GB on the same query.
#
# CPUs: never tested below the 10 the reference host has. 6 is the arithmetic
# requirement — 3 nodes at SCENARIO_NODE_CPUS — not an observed limit.
#
# DISK, measured:
#   - the seeded dataset is 2.0 GiB of Parquet. It is NOT the "~10 GB" that
#     raw-TPC-H-text figures suggest; Snappy Parquet is roughly a fifth of it.
#   - engine and runner images: ~13.5 GB
#   - BuildKit cache after building them from source: ~18 GB
# 40 GB therefore sizes a FIRST run, which builds. A re-run against cached
# images needs a small fraction of it. Sized by the build, not by the data.
SCENARIO_FLOOR_MEM_GIB=15
SCENARIO_FLOOR_CPUS=6
SCENARIO_FLOOR_DISK_GB=40
