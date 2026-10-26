#!/usr/bin/env bash
# Worker-failure handling on the bench cluster (Phase 3 P4, Tier 2).
#
# Kills, then pauses, arneb-worker-2 while SF10 q09 runs, and checks:
#   1. the query fails within FAIL_WITHIN_SECS of the fault, naming the worker
#      (RETRY=1: the coordinator runs with retry_policy = query, and the
#      query instead completes on the remaining worker with the same rows
#      as a run without a fault, after a logged retry)
#   2. (kill) the surviving worker cancels that query's tasks
#   3. the next query succeeds on the remaining worker, first try
#   4. the restarted / resumed worker re-registers and runs tasks again
#
# Prerequisites: the bench cluster with SF10 seeded (hive.tpch), e.g.
#   docker compose -f docker-compose.yml -f docker/arneb-bench/docker-compose.bench.yml \
#     up -d --build arneb arneb-worker-1 arneb-worker-2
# psql on PATH (Homebrew: export PATH="$(brew --prefix libpq)/bin:$PATH").
#
# Usage: ./benchmarks/tpch/scripts/fault_injection.sh     (exit 0 = all checks passed)
#        RETRY=1 ./benchmarks/tpch/scripts/fault_injection.sh
set -u

PROJECT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
cd "$PROJECT_DIR" || exit 2
COMPOSE=(-f docker-compose.yml -f docker/arneb-bench/docker-compose.bench.yml)
COORD=arneb-arneb-1
VICTIM=arneb-worker-2
VICTIM_ID=worker-2
SURVIVOR=arneb-worker-1
FAIL_WITHIN_SECS=${FAIL_WITHIN_SECS:-30}
FAULT_AFTER_SECS=${FAULT_AFTER_SECS:-1.5}
RETRY=${RETRY:-0}
OUT=$(mktemp -d)
FAILED=0

psql_q() { # $1 = sql; prints rows, returns psql's status
    psql -h 127.0.0.1 -p 5432 -U arneb -d arneb -tAq -c "SET search_path TO datalake.tpch" -c "$1"
}
q09=$(sed '/^--/d' benchmarks/tpch/queries/q09.sql)

check() { # $1 = name, $2 = 0 for pass
    if [[ "$2" == 0 ]]; then echo "  PASS  $1"; else echo "  FAIL  $1"; FAILED=1; fi
}

# Same rows, numbers within a relative 1e-12: q09's float sums differ in the
# last digits from run to run (summation order; ~3e-14 measured), with or
# without a fault.
same_rows() { # $1, $2 = psql -tA outputs
    python3 -I - "$1" "$2" <<'PY'
import math, sys
a, b = (open(p).read().splitlines() for p in sys.argv[1:3])
def same(x, y):
    if x == y:
        return True
    try:
        return math.isclose(float(x), float(y), rel_tol=1e-12)
    except ValueError:
        return False
ok = len(a) == len(b) and all(
    len(r.split("|")) == len(s.split("|")) and all(map(same, r.split("|"), s.split("|")))
    for r, s in zip(a, b))
sys.exit(0 if ok else 1)
PY
}

# Fractional epoch seconds (macOS `date` has no %N); `docker logs --since`
# takes the same form, so log windows start exactly at a step.
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }

wait_ready() { # every worker registered and a trivial query runs
    for _ in $(seq 1 60); do
        psql_q "SELECT count(*) FROM nation" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

wait_rejoin() { # $1 = coordinator log cursor (epoch seconds, taken before the restart)
    for _ in $(seq 1 30); do
        docker logs --since "$1" "$COORD" 2>&1 | grep -q "worker re-registered.*$VICTIM_ID\|worker_id=$VICTIM_ID.*re-registered" && return 0
        sleep 1
    done
    return 1
}

# Runs q09, injects `$1` (kill|pause) after FAULT_AFTER_SECS, returns via globals.
run_with_fault() {
    local fault=$1
    (psql_q "$q09" >"$OUT/q.out" 2>"$OUT/q.err"; echo $? >"$OUT/q.rc") &
    local client=$!
    sleep "$FAULT_AFTER_SECS"
    FAULT_AT=$(now)
    docker "$fault" "$VICTIM" >/dev/null
    wait "$client"
    ELAPSED=$(since "$FAULT_AT")
    RC=$(cat "$OUT/q.rc")
    ERR=$(cat "$OUT/q.err")
}

if [[ $RETRY == 1 ]]; then export ARNEB_RETRY_POLICY=query; else export ARNEB_RETRY_POLICY=none; fi
echo "== recreate the bench cluster (retry_policy=$ARNEB_RETRY_POLICY)"
docker compose "${COMPOSE[@]}" up -d --force-recreate arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
wait_ready || { echo "cluster not ready"; exit 2; }
sleep 3 # let both workers' first heartbeats land
if [[ $RETRY == 1 ]]; then
    psql_q "$q09" >"$OUT/expected.out" 2>&1 || { echo "baseline q09 failed"; exit 2; }
    echo "     baseline q09: $(wc -l <"$OUT/expected.out" | tr -d ' ') rows"
fi

for fault in kill pause; do
    echo "== $fault $VICTIM during q09"
    cursor=$(now)
    run_with_fault "$fault"
    echo "     query rc=$RC after ${ELAPSED}s: ${ERR:0:200}"
    if [[ $RETRY == 1 ]]; then
        check "$fault: q09 completes with retry" "$RC"
        check "$fault: same rows as without a fault" "$(same_rows "$OUT/q.out" "$OUT/expected.out"; echo $?)"
        docker logs --since "$cursor" "$COORD" >"$OUT/coord.log" 2>&1
        check "$fault: coordinator logs the retry after losing '$VICTIM_ID'" \
            "$(grep 'retrying query after worker loss' "$OUT/coord.log" | grep -q "$VICTIM_ID"; echo $?)"
    else
        check "$fault: q09 fails" "$([[ $RC != 0 ]]; echo $?)"
        check "$fault: fails within ${FAIL_WITHIN_SECS}s of the fault (${ELAPSED}s)" \
            "$(awk -v e="$ELAPSED" -v l="$FAIL_WITHIN_SECS" 'BEGIN { exit !(e <= l) }'; echo $?)"
        check "$fault: error names worker '$VICTIM_ID'" "$(grep -q "worker '$VICTIM_ID'" <<<"$ERR"; echo $?)"
    fi
    if [[ $fault == kill ]]; then
        sleep 1
        check "kill: $SURVIVOR cancels the query's tasks" \
            "$(docker logs --since "$cursor" "$SURVIVOR" 2>&1 | grep -q 'query cancelled on worker'; echo $?)"
    fi

    start=$(now)
    psql_q "SELECT count(*) FROM lineitem WHERE l_quantity > 49" >"$OUT/next.out" 2>&1
    rc=$?
    echo "     next query rc=$rc after $(since "$start")s: $(head -c 120 "$OUT/next.out")"
    check "$fault: next query succeeds on the remaining worker" "$rc"

    rejoin_from=$(now)
    if [[ $fault == kill ]]; then docker start "$VICTIM" >/dev/null; else docker unpause "$VICTIM" >/dev/null; fi
    wait_rejoin "$rejoin_from"
    check "$fault: $VICTIM re-registers" "$?"
    psql_q "SELECT count(*) FROM lineitem WHERE l_quantity > 49" >/dev/null 2>&1
    check "$fault: $VICTIM runs tasks again" \
        "$(docker logs --since "$rejoin_from" "$VICTIM" 2>&1 | grep -q 'task completed'; echo $?)"
done

rm -rf "$OUT"
if [[ $FAILED == 0 ]]; then echo "ALL CHECKS PASSED"; else echo "SOME CHECKS FAILED"; fi
exit "$FAILED"
