#!/usr/bin/env bash
# Per-query latency + TOTAL cluster peak memory bench for distributed
# arneb vs Trino.
#
# For each query:
#   1. Restart the engine's entire cluster (coord + 2 workers) for a
#      clean baseline.
#   2. Wait for engine healthy.
#   3. Read baseline `memory.current` summed across coord + 2 workers.
#   4. Run query; record wall-clock latency.
#   5. Sleep 1s; read `memory.peak` summed across coord + 2 workers.
#   6. Emit CSV row with both baseline and peak; the bench reports
#      `peak_kib` as the headline memory metric — total cluster peak RSS.
#
# Output: benchmarks/tpch/results/memory_total_YYYYMMDD_HHMMSS.csv
#
# Why total peak (was delta = peak - baseline through 2026-05-25):
#   Trino's JVM `-Xmx` reserves ~2 GB committed at idle BEFORE any query
#   work — a real production-deployment cost the operator cannot ignore.
#   Comparing only the per-query delta hid this 2 GB fixed cost and made
#   arneb (which lazy-allocates) look 2.4× worse on memory while in
#   total-cluster terms it uses HALF Trino's peak. Total peak treats
#   every byte of committed memory as a real resource cost regardless of
#   whether it's actively in use, which is what `kubectl top pods` or
#   any container memory limit reports.
#
# For each query and engine this records latency and peak resident memory
# (sim_peak) so the two engines can be compared directly. See
# `bench_report.py` for the arneb-vs-Trino comparison table.
#
# CORRECTNESS: this bench checks ROW COUNT only (verify_memory.py criterion 2).
#   That is NOT a cell-correctness guarantee — q21 @ SF30 matches row count yet
#   silently drops rows. The SF30 cell-correctness gate is the sibling
#   blast_radius_oracle.py (determinism + cell-diff vs Trino); run it to verify
#   results, not just that the row counts line up.
#
# Usage:
#   ./benchmarks/tpch/scripts/run_memory_bench.sh                  # all 16q
#   ./benchmarks/tpch/scripts/run_memory_bench.sh q05 q09          # subset
#   QUERIES="q01 q02" ./benchmarks/tpch/scripts/run_memory_bench.sh
#   SKIP_TRINO=1 ./benchmarks/tpch/scripts/run_memory_bench.sh     # arneb only
#   SKIP_ARNEB=1 ./benchmarks/tpch/scripts/run_memory_bench.sh     # trino only

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_DIR="$(cd "$BENCH_DIR/../.." && pwd)"
QDIR="$BENCH_DIR/queries"
RESULTS_DIR="$BENCH_DIR/results"
mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV="$RESULTS_DIR/memory_total_${TIMESTAMP}.csv"
LOG_DIR="$RESULTS_DIR/memory_total_${TIMESTAMP}_logs"
mkdir -p "$LOG_DIR"

# All 22 TPC-H queries. Q15 uses a `>= MAX - 0.01` rewrite (within the
# TPC-H spec's 0.01 monetary tolerance) instead of the spec-literal
# `= MAX(...)` to avoid parallel-SUM FP instability across engines —
# see comment header in queries/q15.sql.
DEFAULT_QUERIES=(q01 q02 q03 q04 q05 q06 q07 q08 q09 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22)
if [[ $# -gt 0 ]]; then
    QUERIES=("$@")
elif [[ -n "${QUERIES:-}" ]]; then
    read -ra QUERIES <<<"$QUERIES"
else
    QUERIES=("${DEFAULT_QUERIES[@]}")
fi

SKIP_TRINO=${SKIP_TRINO:-0}
SKIP_ARNEB=${SKIP_ARNEB:-0}

# Include coordinator in the cluster list. The coordinator does real
# query work (final aggregation, sort, root fragment in arneb; query
# planning and scheduling in Trino) and its JVM heap / Rust footprint
# is a real production-deployment cost. Measuring only workers hid
# Trino's ~2 GB coordinator JVM baseline.
ARNEB_CLUSTER=(arneb-arneb-1 arneb-worker-1 arneb-worker-2)
TRINO_CLUSTER=(arneb-trino-1 arneb-trino-worker-1-1 arneb-trino-worker-2-1)

COMPOSE_FILES=(-f "$PROJECT_DIR/docker-compose.yml" -f "$PROJECT_DIR/docker/arneb-bench/docker-compose.bench.yml")

# Optional overlay compose file(s) appended after the bench file — e.g.
# the cache-fit env overlay for an A/B run, or the profile overlay.
# Space-separated paths relative to $PROJECT_DIR. Lets a run flip an
# env-gated feature on without editing the committed bench compose.

# Per-node CPU cap for the compose `cpus:` limits. The bench runs ONE engine
# at a time as a 3-node cluster (coord + 2 workers); cap each node so the
# active engine + the OS/co-tenants fit the host without over-subscribing
# (which thrashed an 8-core box to load 28). Auto-detected from nproc, so the
# bench is portable across hosts — each engine then cgroup-auto-sizes its
# own thread pools (Trino JVM, arneb tokio) to the limited cores.
export BENCH_NODE_CPUS=$(nproc | awk "{c=(\$1-2)/3; printf \"%.1f\", (c<1?1:c)}")
echo "BENCH_NODE_CPUS=$BENCH_NODE_CPUS per node (host $(nproc) cores)"

if [[ -n "${EXTRA_COMPOSE:-}" ]]; then
    for f in $EXTRA_COMPOSE; do
        COMPOSE_FILES+=(-f "$PROJECT_DIR/$f")
    done
fi

# --- cgroup readers --------------------------------------------------

read_cgroup_field() {
    # $1 = container, $2 = field (current|peak)
    docker exec "$1" cat "/sys/fs/cgroup/memory.$2" 2>/dev/null || echo 0
}

sum_field() {
    # $1 = field, $2..N = containers
    local field=$1; shift
    local total=0
    for c in "$@"; do
        local v
        v=$(read_cgroup_field "$c" "$field")
        # Strip non-numeric (cgroup files can return 'max')
        v=${v//[^0-9]/}
        v=${v:-0}
        total=$((total + v))
    done
    echo "$total"
}

# --- simultaneous-peak sampler ---------------------------------------
#
# `sum_field peak` (the headline `peak_kib`) sums each node's LIFETIME
# memory.peak — but the nodes peak at DIFFERENT times, so the sum reports
# a cluster footprint that never simultaneously existed, and it includes
# reclaimable spill-file page cache. This sampler runs DURING the query
# and records, over time, the max of the cluster SUM of:
#   - memory.current  → `sim_peak`  (true simultaneous total resident RSS)
#   - memory.stat anon → `sim_anon` (simultaneous UN-reclaimable; excludes
#                                    evictable spill cache — a diagnostic,
#                                    NOT the default gate)
# Both are read identically for arneb and Trino. The gate metric is
# selectable in verify_memory.py (default sim_peak — the pure-correctness
# fix: it removes the non-simultaneity artifact without the cache-exclusion
# judgement call).
SAMPLE_TMP=$(mktemp -d)
SAMPLER_PID=""
SIM_CUR=0
SIM_ANON=0

start_sampler() {
    # $@ = containers
    rm -f "$SAMPLE_TMP/stop" "$SAMPLE_TMP/out"
    local nodes=("$@")
    (
        local mc=0 ma=0
        while [[ ! -f "$SAMPLE_TMP/stop" ]]; do
            local cur=0 anon=0
            for c in "${nodes[@]}"; do
                local out v a
                # One exec per node: cat current + stat together, parse host-side.
                out=$(docker exec "$c" cat /sys/fs/cgroup/memory.current \
                                            /sys/fs/cgroup/memory.stat 2>/dev/null)
                v=$(printf '%s\n' "$out" | head -1); v=${v//[^0-9]/}
                a=$(printf '%s\n' "$out" | awk '/^anon /{print $2; exit}'); a=${a//[^0-9]/}
                cur=$(( cur + ${v:-0} )); anon=$(( anon + ${a:-0} ))
            done
            (( cur  > mc )) && mc=$cur
            (( anon > ma )) && ma=$anon
            sleep 0.2
        done
        echo "$mc $ma" > "$SAMPLE_TMP/out"
    ) &
    SAMPLER_PID=$!
}

stop_sampler() {
    touch "$SAMPLE_TMP/stop"
    [[ -n "$SAMPLER_PID" ]] && wait "$SAMPLER_PID" 2>/dev/null
    SIM_CUR=0; SIM_ANON=0
    [[ -f "$SAMPLE_TMP/out" ]] && read -r SIM_CUR SIM_ANON < "$SAMPLE_TMP/out"
    SIM_CUR=${SIM_CUR:-0}; SIM_ANON=${SIM_ANON:-0}
}

# --- engine lifecycle -------------------------------------------------
#
# Phase M.5 (2026-05-22): each engine gets a SOLO run with full VM
# resources. Before measuring arneb we stop Trino containers (frees
# the ~3 GB committed JVM heap) and vice versa. The Q09 Phase M.2c +
# Z.1 + 8 GB OrbStack experiment showed: arneb peak 3.34 GB + Trino
# baseline 3 GB + 0.5 GB services > 8 GB VM → kernel SIGKILL on arneb.
# Solo runs verify whether arneb's own peak fits the VM independent
# of cohabiting Trino.

stop_trino() {
    docker compose "${COMPOSE_FILES[@]}" stop trino trino-worker-1 trino-worker-2 >/dev/null 2>&1
}

stop_arneb() {
    docker compose "${COMPOSE_FILES[@]}" stop arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
}

restart_arneb() {
    stop_trino  # Free Trino JVM heap before arneb measurement.
    docker compose "${COMPOSE_FILES[@]}" stop arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
    docker compose "${COMPOSE_FILES[@]}" rm -f arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
    docker compose "${COMPOSE_FILES[@]}" up -d arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
    # Readiness gate: poll pgwire until the coordinator responds. D1's
    # liveness check handles worker-registration races, so a simple
    # SELECT 1 suffices — no need for a heavy distributed query that
    # would inflate cgroup memory.peak before the actual measurement.
    local tries=0
    until psql -h 127.0.0.1 -p 5432 -t -A -q \
            -c "SELECT 1" >/dev/null 2>&1; do
        sleep 2
        tries=$((tries + 1))
        if [[ $tries -gt 90 ]]; then
            echo "  [arneb] not query-ready after 180s" >&2
            return 1
        fi
    done
    return 0
}

restart_trino() {
    stop_arneb  # Free arneb footprint before Trino measurement.
    docker compose "${COMPOSE_FILES[@]}" stop trino trino-worker-1 trino-worker-2 >/dev/null 2>&1
    docker compose "${COMPOSE_FILES[@]}" rm -f trino trino-worker-1 trino-worker-2 >/dev/null 2>&1
    # --wait blocks until the compose healthcheck passes. Trino coord
    # healthcheck runs `trino --execute "SELECT 1"`, which only succeeds
    # once it is past "still initializing" and a worker is registered —
    # so this replaces the old ad-hoc /v1/info + sleep race.
    # Cold Trino at SF30 needs > 180s to register a worker and go healthy;
    # bump via TRINO_WAIT_TIMEOUT (default 300) so the per-query recreate
    # doesn't RESTART_FAIL on the bench host.
    local twait="${TRINO_WAIT_TIMEOUT:-300}"
    if ! docker compose "${COMPOSE_FILES[@]}" up -d --wait --wait-timeout "$twait" trino trino-worker-1 trino-worker-2 >/dev/null 2>&1; then
        echo "  [trino] failed to become healthy after ${twait}s" >&2
        return 1
    fi
    return 0
}

# --- query execution --------------------------------------------------

run_arneb_query() {
    # $1 = query id (e.g., q05)
    local q=$1
    local sql_file="$QDIR/$q.sql"
    local out="$LOG_DIR/arneb_${q}.csv"
    local err="$LOG_DIR/arneb_${q}.err"
    local start end elapsed_ms

    # BENCH_WARMUP=1: one untimed warm-up run BEFORE the timed run so the
    # measured latency reflects warm data (OS page cache populated) + warm JIT,
    # not a cold re-read+gzip-redecompress of lineitem from MinIO. Default off
    # (unchanged metric). The warm-up's peak ~= the timed run's peak (same query)
    # so memory.peak is unaffected; baseline stays clean from the cluster restart.
    # Applied identically to Trino below (fair to both engines).
    if [[ "${BENCH_WARMUP:-0}" == "1" ]]; then
        psql -h 127.0.0.1 -p 5432 -t -A -F',' -q \
            -c "SET search_path TO datalake.tpch" -f "$sql_file" >/dev/null 2>&1
    fi
    start=$(date +%s.%N)
    psql -h 127.0.0.1 -p 5432 -t -A -F',' -q \
        -c "SET search_path TO datalake.tpch" \
        -f "$sql_file" 2>"$err" | grep -v '^SET$' >"$out"
    local rc=${PIPESTATUS[0]}
    end=$(date +%s.%N)
    elapsed_ms=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.1f", (e - s) * 1000 }')

    local rows=$(wc -l <"$out" | tr -d ' ')
    if [[ $rc -ne 0 ]] || [[ -s "$err" ]]; then
        echo "ERR:$elapsed_ms:$rows"
    else
        echo "OK:$elapsed_ms:$rows"
    fi
}

run_trino_query() {
    local q=$1
    local sql_file="$QDIR/$q.sql"
    local out="$LOG_DIR/trino_${q}.csv"
    local err="$LOG_DIR/trino_${q}.err"
    local start end elapsed_ms

    # BENCH_WARMUP=1: untimed warm-up before the timed run (see run_arneb_query).
    # Warms Trino's JVM JIT + the OS page cache equally → fair, stable latency.
    if [[ "${BENCH_WARMUP:-0}" == "1" ]]; then
        docker exec arneb-trino-1 trino --server localhost:8080 --catalog hive --schema tpch \
            --execute "$(cat "$sql_file")" --output-format CSV_UNQUOTED >/dev/null 2>&1
    fi
    start=$(date +%s.%N)
    docker exec arneb-trino-1 trino --server localhost:8080 --catalog hive --schema tpch \
        --execute "$(cat "$sql_file")" --output-format CSV_UNQUOTED \
        >"$out" 2>"$err"
    local rc=$?
    end=$(date +%s.%N)
    elapsed_ms=$(awk -v s="$start" -v e="$end" 'BEGIN { printf "%.1f", (e - s) * 1000 }')

    local rows=$(wc -l <"$out" | tr -d ' ')
    if [[ $rc -ne 0 ]]; then
        echo "ERR:$elapsed_ms:$rows"
    else
        echo "OK:$elapsed_ms:$rows"
    fi
}

# --- main loop --------------------------------------------------------

echo "query,engine,status,latency_ms,baseline_kib,peak_kib,delta_kib,rows,node0_peak_kib,node1_peak_kib,node2_peak_kib,sim_peak_kib,sim_anon_kib" >"$CSV"
echo "Total-cluster-memory bench started: $TIMESTAMP"
echo "CSV: $CSV"
echo "Queries: ${QUERIES[*]}"
echo

printf "%-5s %-7s %-8s %-12s %-6s %-12s\n" "query" "engine" "lat(ms)" "peak(MB)" "rows" "status"
echo "---------------------------------------------------------------"

for q in "${QUERIES[@]}"; do
    sql_file="$QDIR/$q.sql"
    [[ -f "$sql_file" ]] || { echo "$q: skip (no SQL)"; continue; }

    # ---- arneb ----
    if [[ "$SKIP_ARNEB" != "1" ]]; then
        if ! restart_arneb; then
            printf "%-5s %-7s %-8s %-12s %-6s %-12s\n" "$q" "arneb" "-" "-" "-" "RESTART_FAIL"
            echo "$q,arneb,restart_fail,0,0,0,0,0" >>"$CSV"
        else
            sleep 1  # post-health quiescence
            baseline=$(sum_field current "${ARNEB_CLUSTER[@]}")
            start_sampler "${ARNEB_CLUSTER[@]}"
            res=$(run_arneb_query "$q")
            stop_sampler  # → SIM_CUR (sim_peak), SIM_ANON (sim_anon)
            sleep 1  # GC / decay settle
            peak=$(sum_field peak "${ARNEB_CLUSTER[@]}")
            node_peaks=()
            for c in "${ARNEB_CLUSTER[@]}"; do
                node_peaks+=("$(( $(read_cgroup_field "$c" peak) / 1024 ))")
            done
            status=${res%%:*}
            rest=${res#*:}
            latency=${rest%%:*}
            rows=${rest#*:}
            delta=$(( peak - baseline ))
            (( delta < 0 )) && delta=0
            baseline_kib=$(( baseline / 1024 ))
            peak_kib=$(( peak / 1024 ))
            delta_kib=$(( delta / 1024 ))
            sim_peak_kib=$(( SIM_CUR / 1024 ))
            sim_anon_kib=$(( SIM_ANON / 1024 ))
            peak_mb=$(( peak_kib / 1024 ))
            printf "%-5s %-7s %-8s %-12s %-6s %-12s\n" "$q" "arneb" "$latency" "$peak_mb" "$rows" "$status"
            echo "$q,arneb,$status,$latency,$baseline_kib,$peak_kib,$delta_kib,$rows,${node_peaks[0]},${node_peaks[1]},${node_peaks[2]},$sim_peak_kib,$sim_anon_kib" >>"$CSV"
        fi
    fi

    # ---- trino ----
    if [[ "$SKIP_TRINO" != "1" ]]; then
        if ! restart_trino; then
            printf "%-5s %-7s %-8s %-12s %-6s %-12s\n" "$q" "trino" "-" "-" "-" "RESTART_FAIL"
            echo "$q,trino,restart_fail,0,0,0,0,0" >>"$CSV"
        else
            sleep 1
            baseline=$(sum_field current "${TRINO_CLUSTER[@]}")
            start_sampler "${TRINO_CLUSTER[@]}"
            res=$(run_trino_query "$q")
            stop_sampler  # → SIM_CUR (sim_peak), SIM_ANON (sim_anon)
            sleep 1
            peak=$(sum_field peak "${TRINO_CLUSTER[@]}")
            node_peaks=()
            for c in "${TRINO_CLUSTER[@]}"; do
                node_peaks+=("$(( $(read_cgroup_field "$c" peak) / 1024 ))")
            done
            status=${res%%:*}
            rest=${res#*:}
            latency=${rest%%:*}
            rows=${rest#*:}
            delta=$(( peak - baseline ))
            (( delta < 0 )) && delta=0
            baseline_kib=$(( baseline / 1024 ))
            peak_kib=$(( peak / 1024 ))
            delta_kib=$(( delta / 1024 ))
            sim_peak_kib=$(( SIM_CUR / 1024 ))
            sim_anon_kib=$(( SIM_ANON / 1024 ))
            peak_mb=$(( peak_kib / 1024 ))
            printf "%-5s %-7s %-8s %-12s %-6s %-12s\n" "$q" "trino" "$latency" "$peak_mb" "$rows" "$status"
            echo "$q,trino,$status,$latency,$baseline_kib,$peak_kib,$delta_kib,$rows,${node_peaks[0]},${node_peaks[1]},${node_peaks[2]},$sim_peak_kib,$sim_anon_kib" >>"$CSV"
        fi
    fi
done

echo
echo "CSV written to: $CSV"
echo "Logs in: $LOG_DIR"
echo
echo "Comparison report:"
echo "  python3 $BENCH_DIR/scripts/bench_report.py $CSV"
