# shellcheck shell=bash
# shellcheck disable=SC2034  # every value here is read by the sourcing script
#
# Scenario: sf1 — the small scale, runnable on a modest host.
#
# Sourced by benchmarks/tpch/scripts/run_benchmark.sh. Assignments only: no
# conditionals, no command substitution, no logic. A scenario that needs logic is
# a sign the mechanism is wrong, not an invitation to script inside it.
#
# Precedence when the harness runs: an explicit CLI flag beats an environment
# variable, which beats these values, which beat the script's built-in defaults.

SCENARIO_TPCH_SF=sf1
SCENARIO_NUM_RUNS=8
SCENARIO_WARM_UP=3
SCENARIO_NODE_CPUS=2
SCENARIO_RUNNER_CPUS=6

# Row count of the largest table at this scale, used to check that data left
# over from a previous run is this scale's data before SKIP_SEED=1 measures it.
# TPC-H SF1 lineitem cardinality, fixed by the specification and reproduced by
# the Trino CTAS seed.
SCENARIO_LINEITEM_ROWS=6001215

# ---------------------------------------------------------------------------
# Host floors — DERIVED, not observed.
# ---------------------------------------------------------------------------
# No SF1 run has been made on a host small enough to find the true floor, so
# these are reasoned rather than measured:
#
#   - Memory: with engine rotation only one engine is resident at a time, and
#     Trino's own limits (docker/trino/etc/*.properties) declare
#     query.max-memory=8GB with max-memory-per-node=4GB. The one hard data point
#     is a recorded OOM-kill under Docker Desktop's 3.88 GB default, noted in
#     docker/trino/etc/config.properties.
#   - CPUs: 6 is the arithmetic requirement — 3 nodes at SCENARIO_NODE_CPUS.
#   - Disk: sized by the image build, not by the data; SF1 Parquet is a few
#     hundred MB, while building the engine images from source is ~18 GB of
#     BuildKit cache on a first run.
#
# Erring high is deliberate. A floor set too high refuses the run with a legible
# message and an explicit bypass (SKIP_PREFLIGHT=1); a floor set too low lets the
# run die of an out-of-memory kill forty minutes in, with no obvious cause.
#
# Replace these with observed values once an SF1 run is made on a host that is
# actually constrained, and delete this note when they do.
SCENARIO_FLOOR_MEM_GIB=8
SCENARIO_FLOOR_CPUS=6
SCENARIO_FLOOR_DISK_GB=20
