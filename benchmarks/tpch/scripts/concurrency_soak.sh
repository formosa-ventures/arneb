#!/usr/bin/env bash
# Concurrency soak (Tier 2): ROUNDS rounds of 20 TPC-H queries started at
# once (two copies of a 10-query mix) against a running Arneb, over pgwire.
# Exits non-zero if any query fails. With LOG_DIR set, also counts the
# exchange events (#139) the nodes logged to LOG_DIR/*.log:
#   - "exchange stream broke"            consumer: transport break mid-stream
#   - "broke before the response"        consumer: transport break before it
#   - "dropped before its end"           producer: response dropped early
#   - "failed serving connection"        producer: h2 connection error
#   - "connection task error"            consumer: h2 connection error
# The last two need RUST_LOG to include
#   tonic::transport::server=debug,tonic::transport::channel::service::connection=debug
# (one line per broken connection, so cheap enough to leave on for a soak).
#
# Prerequisites: a cluster with TPC-H seeded and unqualified table names
# resolving to it (default_schema = "tpch"), e.g. the bench cluster
#   docker compose -f docker-compose.yml -f docker/arneb-bench/docker-compose.bench.yml \
#     up -d arneb arneb-worker-1 arneb-worker-2
# For real concurrency, lift the admission cap (ARNEB_MAX_RUNNING_QUERIES=0);
# with an unbounded memory pool that OOMs a 16 GB Docker Desktop VM at SF10,
# so do it on a native cluster or with a memory budget.
# psql on PATH (Homebrew: export PATH="$(brew --prefix libpq)/bin:$PATH").
#
# Usage: ./benchmarks/tpch/scripts/concurrency_soak.sh [rounds]   (default 5)
#   env: PGHOST (127.0.0.1) PGPORT (5432) PGUSER (test) PGDATABASE (test)
#        MIX ("q01 q03 q05 q06 q09 q10 q12 q14 q18 q21")  OUT (mktemp -d)
#        LOG_DIR (unset = skip the log scan)
set -u

PROJECT_DIR="$(cd "$(dirname "$0")/../../.." && pwd)"
QUERIES="$PROJECT_DIR/benchmarks/tpch/queries"
ROUNDS=${1:-5}
MIX=${MIX:-"q01 q03 q05 q06 q09 q10 q12 q14 q18 q21"}
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT" || exit 2
command -v psql > /dev/null || { echo "psql not on PATH" >&2; exit 2; }
export PGHOST=${PGHOST:-127.0.0.1} PGPORT=${PGPORT:-5432}
export PGUSER=${PGUSER:-test} PGDATABASE=${PGDATABASE:-test}
TOTAL_FAILED=0

for r in $(seq 1 "$ROUNDS"); do
  start=$(date +%s)
  pids=()
  for copy in a b; do
    for q in $MIX; do
      f="$OUT/r${r}_${q}${copy}"
      ( if psql -At -v ON_ERROR_STOP=1 -f "$QUERIES/$q.sql" > "$f.out" 2> "$f.err"
        then echo OK > "$f.status"; else echo FAIL > "$f.status"; fi ) &
      pids+=($!)
    done
  done
  wait "${pids[@]}"
  failed=$(grep -l FAIL "$OUT"/r"${r}"_*.status 2>/dev/null | wc -l | tr -d ' ')
  TOTAL_FAILED=$((TOTAL_FAILED + failed))
  echo "round $r: $(( $(date +%s) - start ))s, failed=$failed"
  for s in $(grep -l FAIL "$OUT"/r"${r}"_*.status 2>/dev/null); do
    b=${s%.status}
    echo "  $(basename "$b"): $(head -c 300 "$b.err" | tr '\n' ' ')"
  done
done

queries=$((ROUNDS * 2 * $(echo "$MIX" | wc -w)))
echo "total: $queries queries, $TOTAL_FAILED failed (outputs in $OUT)"

if [ -n "${LOG_DIR:-}" ]; then
  for f in "$LOG_DIR"/*.log; do
    printf '%s:' "$(basename "$f")"
    for pat in 'exchange stream broke' 'broke before the response' \
               'dropped before its end' 'failed serving connection' \
               'connection task error'; do
      printf ' [%s]=%s' "$pat" "$(grep -c "$pat" "$f")"
    done
    echo
  done
fi

[ "$TOTAL_FAILED" -eq 0 ]
