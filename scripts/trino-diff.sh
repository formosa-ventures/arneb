#!/usr/bin/env bash
#
# Cell-diff a query suite between Arneb and Trino on the local Hive stack.
#
#   scripts/trino-diff.sh [--suite tpch|tpcds] [--tol 1e-9] [--port 15432]
#                         [--timeout SECS] [--max-rss-mb MB] [--out DIR]
#                         [--query-dir DIR] [QUERY...]
#
# Every query in benchmarks/<suite>/queries/ (or the ones named) runs through
# both engines; the two CSVs are compared cell by cell with a float tolerance.
# Arneb is started from ${CARGO_TARGET_DIR:-target}/release/arneb with the suite's Hive config and
# stopped on exit. Trino is the docker compose one.
#
# Per-query outcome:
#   ok        both ran, every cell matches
#   mismatch  both ran, cells differ (first difference in the summary)
#   err:<cls> Arneb failed; cls is parse, plan, exec or memory from the SQLSTATE
#             and message, or timeout
#   trino-err Trino failed (the query is not a fair comparison)
#   err:crash Arneb exited while running the query (it is restarted, the
#             panic is in arneb.log, and the run continues)
#   A query whose Arneb RSS passes --max-rss-mb (default 4096) or whose time
#   passes --timeout is killed together with Arneb and counted as err:memory
#   or err:timeout; Arneb is restarted and the run continues.
#
# Exit code is the number of queries that are not ok, capped at 125.

set -uo pipefail

SUITE=tpch
TOL=1e-9
PORT=15432
TIMEOUT=600
MAX_RSS_MB=4096
OUT=""
QDIR=""
QUERIES=()
while [ $# -gt 0 ]; do
    case "$1" in
        --suite) SUITE=$2; shift 2 ;;
        --tol) TOL=$2; shift 2 ;;
        --port) PORT=$2; shift 2 ;;
        --timeout) TIMEOUT=$2; shift 2 ;;
        --max-rss-mb) MAX_RSS_MB=$2; shift 2 ;;
        --out) OUT=$2; shift 2 ;;
        --query-dir) QDIR=$2; shift 2 ;;
        -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
        *) QUERIES+=("$1"); shift ;;
    esac
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
QUERY_DIR="${QDIR:-benchmarks/$SUITE/queries}"
CONFIG="benchmarks/$SUITE/$SUITE-hive.toml"
COMPARE="scripts/trino-diff-compare.py"
BIN="${CARGO_TARGET_DIR:-target}/release/arneb"
case "$SUITE" in
    tpch) SEED_CHECK="SELECT COUNT(*) FROM hive.tpch.nation"; SEED_EXPECT=25; SEED_CMD="docker compose run --rm tpch-seed" ;;
    tpcds) SEED_CHECK="SELECT COUNT(*) FROM hive.tpcds.date_dim"; SEED_EXPECT=73049; SEED_CMD="docker compose run --rm tpcds-seed" ;;
    *) echo "unknown suite: $SUITE (tpch|tpcds)" >&2; exit 2 ;;
esac
[ -n "$OUT" ] || OUT=$(mktemp -d "${TMPDIR:-/tmp}/trino-diff-$SUITE.XXXXXX")
mkdir -p "$OUT"

trino() {
    docker compose exec -T trino trino --server http://localhost:8080 \
        --catalog hive --schema "$SUITE" --output-format CSV_UNQUOTED "$@"
}

# ---- pre-flight -----------------------------------------------------------
fail() { echo "✗ $1"; echo "  fix: $2"; exit 2; }
running=$(docker compose ps --services --filter status=running 2>/dev/null)
for s in s3 hive-metastore trino; do
    grep -qx "$s" <<<"$running" || fail "docker stack: $s is not running" "docker compose up -d && wait for the healthchecks"
done
echo "✓ docker stack up (s3, hive-metastore, trino)"
count=$(trino --execute "$SEED_CHECK" 2>/dev/null | tail -1 | tr -d '"\r')
[ "$count" = "$SEED_EXPECT" ] || fail "$SUITE SF1 not seeded (got '${count:-nothing}', want $SEED_EXPECT)" "$SEED_CMD"
echo "✓ $SUITE SF1 seeded"
[ -x "$BIN" ] || fail "$BIN missing" "cargo build --release"
echo "✓ $BIN"
command -v psql >/dev/null || fail "psql missing" "brew install libpq"
echo "✓ psql"
[ -d "$QUERY_DIR" ] || fail "$QUERY_DIR missing" "check --suite"

# ---- arneb ----------------------------------------------------------------
ARNEB_PID=""
start_arneb() {
    "$BIN" --config "$CONFIG" --port "$PORT" --no-trino >>"$OUT/arneb.log" 2>&1 &
    ARNEB_PID=$!
    for _ in $(seq 1 30); do
        psql -h 127.0.0.1 -p "$PORT" -U test -d test -c "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}
trap 'kill $ARNEB_PID 2>/dev/null' EXIT
start_arneb || { tail -20 "$OUT/arneb.log"; fail "arneb did not come up on :$PORT" "see $OUT/arneb.log"; }
echo "✓ arneb up on :$PORT ($CONFIG)"
echo

# ---- queries ---------------------------------------------------------------
if [ ${#QUERIES[@]} -eq 0 ]; then
    for f in "$QUERY_DIR"/q*.sql; do QUERIES+=("$(basename "$f" .sql)"); done
fi
total=${#QUERIES[@]}
n=0 ok=0
declare -a BAD
RESULTS="$OUT/results.tsv"
printf 'query\toutcome\tarneb_s\ttrino_s\tarneb_peak_mb\tdetail\n' >"$RESULTS"

# Classify an Arneb psql failure from its verbose error output.
classify() {
    local err=$1
    if grep -qi 'timeout' <<<"$err"; then echo timeout
    elif grep -qE 'ERROR:  (53200|53400|out of memory)|memory limit' <<<"$err"; then echo memory
    elif grep -qE 'ERROR:  (42601|invalid syntax|unsupported feature)' <<<"$err"; then echo parse
    elif grep -qE 'ERROR:  (42703|42P01|42883|42804|0A000|planner internal|unsupported expression|invalid expression|column not found|table not found|function not found|type mismatch|ambiguous|not yet supported|not supported|unsupported)' <<<"$err"; then echo plan
    else echo exec
    fi
}

for q in "${QUERIES[@]}"; do
    n=$((n + 1))
    sql="$QUERY_DIR/$q.sql"
    [ -f "$sql" ] || { printf '[%3d/%d] %-5s missing file\n' "$n" "$total" "$q"; BAD+=("$q"); continue; }

    t0=$(date +%s.%N)
    psql -h 127.0.0.1 -p "$PORT" -U test -d test -A -F , -t -q -v ON_ERROR_STOP=1 \
        -c '\set VERBOSITY verbose' -f "$sql" \
        >"$OUT/arneb_$q.csv" 2>"$OUT/arneb_$q.err" &
    PSQL_PID=$!
    # Watchdog: a query that outgrows the engine's own accounting (e.g. a
    # cross product) is killed here, with Arneb, before it takes the host.
    blown=0 crashed=0 timed_out=0 peak_mb=0
    while kill -0 "$PSQL_PID" 2>/dev/null; do
        rss_mb=$(ps -o rss= -p "$ARNEB_PID" 2>/dev/null | awk '{printf "%d", $1/1024}')
        [ -z "$rss_mb" ] && { crashed=1; break; }
        [ "$rss_mb" -gt "$peak_mb" ] && peak_mb=$rss_mb
        if [ "$rss_mb" -gt "$MAX_RSS_MB" ]; then blown=1; kill -9 "$ARNEB_PID"; break; fi
        if [ "$(echo "$(date +%s.%N) - $t0 > $TIMEOUT" | bc)" = 1 ]; then timed_out=1; kill -9 "$ARNEB_PID"; break; fi
        sleep 0.5
    done
    wait "$PSQL_PID" 2>/dev/null; arc=$?
    ta=$(printf '%.1f' "$(echo "$(date +%s.%N) - $t0" | bc)")
    [ $timed_out -eq 1 ] && echo "ERROR:  timeout after ${TIMEOUT}s (arneb killed)" >>"$OUT/arneb_$q.err"
    [ $blown -eq 1 ] && echo "ERROR:  out of memory: arneb RSS ${peak_mb} MB exceeded ${MAX_RSS_MB} MB (arneb killed)" >>"$OUT/arneb_$q.err"
    if [ $crashed -eq 1 ]; then
        echo "ERROR:  arneb exited during $q (see arneb.log)" >>"$OUT/arneb_$q.err"
    fi
    if ! kill -0 "$ARNEB_PID" 2>/dev/null; then
        wait "$ARNEB_PID" 2>/dev/null
        echo "===== arneb restarted after $q =====" >>"$OUT/arneb.log"
        start_arneb || { fail "arneb could not be restarted after $q" "see $OUT/arneb.log"; }
    fi

    t0=$(date +%s.%N)
    trino --execute "$(cat "$sql")" >"$OUT/trino_$q.csv" 2>"$OUT/trino_$q.err"
    trc=$?
    tt=$(printf '%.1f' "$(echo "$(date +%s.%N) - $t0" | bc)")

    if [ $crashed -eq 1 ]; then
        outcome="err:crash"; detail="peak RSS ${peak_mb} MB; $(grep -o 'panicked at.*' "$OUT/arneb.log" | tail -1 | cut -c1-90)"
    elif [ $arc -ne 0 ] || grep -q 'ERROR:' "$OUT/arneb_$q.err"; then
        cls=$(classify "$(cat "$OUT/arneb_$q.err")")
        msg=$(grep -m1 -o 'ERROR:.*' "$OUT/arneb_$q.err" | cut -c1-110)
        outcome="err:$cls"; detail=$msg
    elif [ $trc -ne 0 ]; then
        outcome="trino-err"; detail=$(grep -m1 -i 'error\|exception' "$OUT/trino_$q.err" | cut -c1-110)
    elif diffout=$(python3 "$COMPARE" "$OUT/arneb_$q.csv" "$OUT/trino_$q.csv" "$TOL"); then
        outcome=ok; detail="rows=$(wc -l <"$OUT/trino_$q.csv" | tr -d ' ')"
    else
        outcome=mismatch; detail="$(wc -l <<<"$diffout" | tr -d ' ') diff(s); first: $(head -1 <<<"$diffout")"
    fi
    case "$outcome" in ok) ok=$((ok + 1)); mark='✓' ;; mismatch) mark='✗'; BAD+=("$q") ;; *) mark='ERR'; BAD+=("$q") ;; esac
    printf '[%3d/%d] %-5s %-3s a=%6ss t=%6ss  %s%s\n' "$n" "$total" "$q" "$mark" "$ta" "$tt" \
        "$([ "$outcome" = ok ] || printf '%s ' "$outcome")" "$detail"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$q" "$outcome" "$ta" "$tt" "$peak_mb" "$detail" >>"$RESULTS"
done

echo
echo "Summary ($SUITE, tol=$TOL): $ok/$total identical"
if [ ${#BAD[@]} -gt 0 ]; then
    echo "Not ok: ${BAD[*]}"
    echo "By outcome:"
    cut -f2 "$RESULTS" | tail -n +2 | sort | uniq -c | sort -rn | sed 's/^/  /'
fi
echo "Artifacts: $OUT (results.tsv, arneb_<q>.csv|err, trino_<q>.csv|err, arneb.log)"
bad=${#BAD[@]}
exit $(( bad > 125 ? 125 : bad ))
