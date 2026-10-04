#!/usr/bin/env bash
set -euo pipefail

# TPC-H Benchmark: arneb vs Trino — the single scenario-driven entry point.
# ========================================================================
#
# One command takes a declared scenario end to end on one local Docker host:
# preflight the host, bring up the infrastructure, seed the declared scale,
# measure both engines under identical container isolation, and render the
# comparison with the scale it was measured at printed in the header.
#
#   ./benchmarks/tpch/scripts/run_benchmark.sh --scenario=sf10
#
# WHY THIS DELEGATES THE MEASUREMENT
#
# The measurement itself is `run_memory_bench.sh`, not code duplicated here.
# That script already does the parts that make the comparison fair, and doing
# them twice in two places is how the two copies drift apart:
#
#   - engine rotation: only the measured engine is resident, the other is
#     stopped, so Trino's ~3 GB of committed JVM heap is not held against
#     arneb's measurement (and vice versa);
#   - a cluster restart per query, so each measurement starts from a clean
#     memory baseline rather than its predecessor's high-water mark;
#   - both engines in containers with the same per-node CPU cap, which is what
#     removes the native-vs-container bias the old version of this script had
#     (it ran arneb natively from `cargo run` against a containerized Trino).
#
# WHAT A SCENARIO DECLARES
#
# `benchmarks/tpch/scenarios/<name>.sh` — assignments only, no logic. It
# declares the scale factor, the run plan, the per-node CPU allocation, and the
# host floors below which the run is refused. Precedence, highest first:
#
#   explicit CLI flag  >  environment variable  >  scenario  >  built-in default
#
# Usage:
#   ./scripts/run_benchmark.sh --scenario=sf10
#   ./scripts/run_benchmark.sh --scenario=sf1 --queries="q01 q06"
#   ./scripts/run_benchmark.sh --scenario=sf10 --skip-trino
#   TPCH_SF=sf1 ./scripts/run_benchmark.sh          # bare path, no scenario
#
# Environment:
#   SKIP_PREFLIGHT=1  proceed although a host floor is unmet; the run is
#                     recorded as bypassed so it is not mistaken for a clean one
#   SKIP_SEED=1       measure the resident dataset instead of re-seeding; the
#                     run is refused if that dataset is not this scale's data
#   BENCH_WARMUP=1    one untimed warm-up run per query per engine before the
#                     timed one (applied identically to both engines)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/../.." && pwd)"
SCENARIO_DIR="$BENCH_DIR/scenarios"
RESULTS_DIR="$BENCH_DIR/results"

die() { echo "ERROR: $*" >&2; exit 1; }

# ------------------------------------------------------------------
# Values the operator supplied through the environment, captured BEFORE the
# scenario fragment is sourced. A scenario supplies a default; it must not
# silently overrule an operator who exported the variable themselves.
# ------------------------------------------------------------------
ENV_TPCH_SF="${TPCH_SF:-}"
ENV_NUM_RUNS="${NUM_RUNS:-}"
ENV_WARM_UP="${WARM_UP:-}"
ENV_NODE_CPUS="${BENCH_NODE_CPUS:-}"

CLI_SCENARIO=""
CLI_TPCH_SF=""
CLI_NUM_RUNS=""
CLI_QUERIES=""
SKIP_TRINO="${SKIP_TRINO:-0}"

for arg in "$@"; do
    case $arg in
        --scenario=*)  CLI_SCENARIO="${arg#*=}" ;;
        --sf=*)        CLI_TPCH_SF="${arg#*=}" ;;
        --runs=*)      CLI_NUM_RUNS="${arg#*=}" ;;
        --queries=*)   CLI_QUERIES="${arg#*=}" ;;
        --skip-trino)  SKIP_TRINO=1 ;;
        -h|--help)     sed -n '4,47p' "$0"; exit 0 ;;
        *)             die "unknown argument: $arg" ;;
    esac
done

# ------------------------------------------------------------------
# Load the scenario, if one was named. An unknown name aborts here — before
# any container starts and before anything is seeded — naming what was asked
# for and what is actually declared.
# ------------------------------------------------------------------
SCENARIO_NAME=""
if [[ -n "$CLI_SCENARIO" ]]; then
    scenario_file="$SCENARIO_DIR/${CLI_SCENARIO}.sh"
    if [[ ! -f "$scenario_file" ]]; then
        {
            echo "ERROR: no such scenario: $CLI_SCENARIO"
            echo "Declared scenarios in $SCENARIO_DIR:"
            for f in "$SCENARIO_DIR"/*.sh; do
                [[ -f "$f" ]] || continue
                echo "  $(basename "$f" .sh)"
            done
        } >&2
        exit 1
    fi
    # shellcheck source=/dev/null
    source "$scenario_file"
    SCENARIO_NAME="$CLI_SCENARIO"
fi

TPCH_SF="${CLI_TPCH_SF:-${ENV_TPCH_SF:-${SCENARIO_TPCH_SF:-sf1}}}"
NUM_RUNS="${CLI_NUM_RUNS:-${ENV_NUM_RUNS:-${SCENARIO_NUM_RUNS:-5}}}"
WARM_UP="${ENV_WARM_UP:-${SCENARIO_WARM_UP:-2}}"
LINEITEM_ROWS="${SCENARIO_LINEITEM_ROWS:-}"
FLOOR_MEM_GIB="${SCENARIO_FLOOR_MEM_GIB:-0}"
FLOOR_CPUS="${SCENARIO_FLOOR_CPUS:-0}"
FLOOR_DISK_GB="${SCENARIO_FLOOR_DISK_GB:-0}"

export TPCH_SF
[[ -n "${ENV_NODE_CPUS:-}" ]] && export BENCH_NODE_CPUS="$ENV_NODE_CPUS"

COMPOSE=(docker compose
         -f "$PROJECT_DIR/docker-compose.yml"
         -f "$PROJECT_DIR/docker/arneb-bench/docker-compose.bench.yml")

# ------------------------------------------------------------------
# Step 0: Host preflight — BEFORE any container starts, so an unmet floor
# costs seconds rather than being discovered as an out-of-memory kill forty
# minutes into a seeded run.
# ------------------------------------------------------------------
PREFLIGHT_BYPASSED=false

preflight() {
    [[ -n "$SCENARIO_NAME" ]] || return 0  # nothing declared, nothing to check

    local unmet=()

    local mem_bytes cpus mem_gib
    mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    cpus=$(docker info --format '{{.NCPU}}' 2>/dev/null || echo 0)
    mem_gib=$(( mem_bytes / 1024 / 1024 / 1024 ))

    (( mem_gib < FLOOR_MEM_GIB )) && unmet+=(
        "container-runtime memory: scenario '$SCENARIO_NAME' declares ${FLOOR_MEM_GIB} GiB, host has ${mem_gib} GiB")
    (( cpus < FLOOR_CPUS )) && unmet+=(
        "container-runtime CPUs: scenario '$SCENARIO_NAME' declares ${FLOOR_CPUS}, host has ${cpus}")

    # Free disk. On Linux the Docker data root is a host path, so `df` on it is
    # exact. On macOS it lives inside a VM whose disk is a sparse file on the
    # host filesystem, so host free space is the binding constraint — fall back
    # to the home filesystem, which is where that file lives.
    local root disk_gb
    root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo "")
    [[ -d "$root" ]] || root="$HOME"
    disk_gb=$(df -Pk "$root" 2>/dev/null | awk 'NR==2 {printf "%d", $4 / 1024 / 1024}')
    disk_gb=${disk_gb:-0}
    (( disk_gb < FLOOR_DISK_GB )) && unmet+=(
        "free disk on ${root}: scenario '$SCENARIO_NAME' declares ${FLOOR_DISK_GB} GB, host has ${disk_gb} GB")

    if (( ${#unmet[@]} > 0 )); then
        # Every unmet floor, not just the first — an operator raising the VM
        # allocation should learn all of what to raise in one pass.
        echo "Host does not meet the preconditions of scenario '$SCENARIO_NAME':" >&2
        printf '  - %s\n' "${unmet[@]}" >&2
        if [[ "${SKIP_PREFLIGHT:-0}" == "1" ]]; then
            echo "SKIP_PREFLIGHT=1 — proceeding anyway; this run is recorded as bypassed." >&2
            PREFLIGHT_BYPASSED=true
            return 0
        fi
        echo "Raise the allocation, or set SKIP_PREFLIGHT=1 to proceed anyway." >&2
        exit 1
    fi

    echo "Preflight OK: ${mem_gib} GiB / ${cpus} CPUs / ${disk_gb} GB free"
    echo "  (floors: ${FLOOR_MEM_GIB} GiB / ${FLOOR_CPUS} CPUs / ${FLOOR_DISK_GB} GB)"
}

echo "============================================"
echo "TPC-H Benchmark: arneb vs Trino"
echo "============================================"
echo "Scenario:  ${SCENARIO_NAME:-none (bare TPCH_SF path)}"
echo "Scale:     $TPCH_SF"
echo "Warm-up:   ${BENCH_WARMUP:-0}"
echo ""

preflight
echo ""

# ------------------------------------------------------------------
# Step 1: Infrastructure — MinIO, Hive Metastore, Trino.
# ------------------------------------------------------------------
echo ">>> Step 1: Bringing up MinIO + Hive Metastore + Trino..."
cd "$PROJECT_DIR"
"${COMPOSE[@]}" up -d --wait --wait-timeout "${TRINO_WAIT_TIMEOUT:-300}" \
    minio hive-metastore trino trino-worker-1 trino-worker-2 \
    || die "infrastructure did not come up"
echo ""

# ------------------------------------------------------------------
# Step 2: Dataset — seed this scale, or verify the resident one IS this scale.
#
# The guard exists because the failure it prevents is silent: measuring a
# dataset of one scale and filing the numbers under another scale's name
# produces a plausible-looking result that is simply not what it claims.
# ------------------------------------------------------------------
echo ">>> Step 2: Dataset ($TPCH_SF)..."

resident_rows() {
    docker exec arneb-trino-1 trino --execute \
        "SELECT COUNT(*) FROM hive.tpch.lineitem" 2>/dev/null \
        | tr -d '"' | tr -d '[:space:]' || echo 0
}

rows=$(resident_rows)
rows=${rows:-0}
echo "Resident lineitem rows: ${rows:-0}"

if [[ "${SKIP_SEED:-0}" == "1" ]]; then
    if [[ -z "$LINEITEM_ROWS" ]]; then
        echo "SKIP_SEED=1 and the scenario declares no row count — measuring the resident dataset unverified."
    elif [[ "$rows" != "$LINEITEM_ROWS" ]]; then
        die "SKIP_SEED=1 but the resident dataset is not $TPCH_SF data:
  scenario '$SCENARIO_NAME' declares lineitem = $LINEITEM_ROWS rows
  the resident dataset has        lineitem = $rows rows
Re-run without SKIP_SEED=1 to seed this scale, or name the scenario whose data is resident."
    else
        echo "SKIP_SEED=1 — resident dataset matches $TPCH_SF ($rows rows); not re-seeding."
    fi
elif [[ -n "$LINEITEM_ROWS" && "$rows" == "$LINEITEM_ROWS" ]]; then
    echo "Resident dataset already matches $TPCH_SF ($rows rows); not re-seeding."
else
    echo "Seeding $TPCH_SF via Trino CTAS (this takes a while)..."
    "${COMPOSE[@]}" run --rm tpch-seed || die "seed failed"
    rows=$(resident_rows)
    echo "Seeded. Resident lineitem rows: $rows"
    if [[ -n "$LINEITEM_ROWS" && "$rows" != "$LINEITEM_ROWS" ]]; then
        die "seed completed but lineitem = $rows rows, scenario declares $LINEITEM_ROWS"
    fi
fi
echo ""

# ------------------------------------------------------------------
# Step 3: Measure — both engines, container-isolated, one at a time.
# ------------------------------------------------------------------
echo ">>> Step 3: Measuring both engines (latency + peak memory)..."

# The measurement names its own output; this finds it afterwards. The names are
# memory_total_YYYYMMDD_HHMMSS.csv, whose lexical order IS their chronological
# order — so a glob and `sort` give the newest without parsing `ls` output.
latest_csv() {
    local files=("$RESULTS_DIR"/memory_total_*.csv)
    [[ -e ${files[0]} ]] || return 0   # unmatched glob stays literal
    printf '%s\n' "${files[@]}" | sort | tail -1
}

before=$(latest_csv)

measure_env=(SKIP_TRINO="$SKIP_TRINO")
[[ -n "${BENCH_WARMUP:-}" ]] && measure_env+=(BENCH_WARMUP="$BENCH_WARMUP")
[[ -n "${BENCH_NODE_CPUS:-}" ]] && measure_env+=(BENCH_NODE_CPUS="$BENCH_NODE_CPUS")

if [[ -n "$CLI_QUERIES" ]]; then
    # shellcheck disable=SC2086  # deliberate word splitting: a query list
    env "${measure_env[@]}" "$SCRIPT_DIR/run_memory_bench.sh" $CLI_QUERIES
else
    env "${measure_env[@]}" "$SCRIPT_DIR/run_memory_bench.sh"
fi
echo ""

CSV=$(latest_csv)
[[ -n "$CSV" && "$CSV" != "$before" ]] || die "measurement produced no new CSV"

# ------------------------------------------------------------------
# Step 4: Provenance — the scale travels WITH the measurement, not only in
# the path a human filed it under.
# ------------------------------------------------------------------
PROV="${CSV%.csv}.provenance.json"
cat >"$PROV" <<JSON
{
  "scenario": $( [[ -n "$SCENARIO_NAME" ]] && echo "\"$SCENARIO_NAME\"" || echo null ),
  "scale_factor": "$TPCH_SF",
  "num_runs": $NUM_RUNS,
  "warm_up": $WARM_UP,
  "bench_warmup": ${BENCH_WARMUP:-0},
  "preflight_bypassed": $PREFLIGHT_BYPASSED,
  "node_cpus": "${BENCH_NODE_CPUS:-auto}",
  "lineitem_rows": ${rows:-0},
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
JSON
echo ">>> Step 4: Provenance written to $PROV"
echo ""

# ------------------------------------------------------------------
# Step 5: Report.
# ------------------------------------------------------------------
echo ">>> Step 5: Comparison report"
echo ""
python3 "$SCRIPT_DIR/bench_report.py" "$CSV" | tee "$RESULTS_DIR/comparison.md"

echo ""
echo "============================================"
echo "Benchmark complete — scenario ${SCENARIO_NAME:-none}, scale $TPCH_SF"
echo "  raw CSV:    $CSV"
echo "  provenance: $PROV"
echo "  report:     $RESULTS_DIR/comparison.md"
echo "============================================"
