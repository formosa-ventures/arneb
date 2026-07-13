#!/usr/bin/env bash
# Warm-loop bench: run a query N times against a FIXED (never-recreated)
# warm cluster and report the steady-state distribution. Unlike
# run_memory_bench.sh (which force-recreates the cluster per query and thus
# always samples a cold/transient run), this keeps the cluster warm so the
# reported latency reflects a real long-lived deployment.
#
# Q09 is BISTABLE+STICKY (~7.3s fast vs ~17.5s slow, each mode persists
# several consecutive runs then flips), so a single "steady-state median"
# is misleading. This harness classifies each run into fast/slow modes via
# a gap in the sorted latencies and reports BOTH modes + the fast fraction.
#
# Usage:
#   ./benchmarks/tpch/scripts/warm_loop_bench.sh                 # q09, 15 runs, discard 2
#   QUERY=q05 RUNS=20 DISCARD=3 ./benchmarks/tpch/scripts/warm_loop_bench.sh
# Assumes the cluster is already up + warm (run it once first if cold).
# Solo: stop trino before running for clean numbers.
set -uo pipefail
cd "$(dirname "$0")/../../.."

QUERY=${QUERY:-q09}
RUNS=${RUNS:-15}
DISCARD=${DISCARD:-2}   # warm-up runs excluded from stats
SQL="benchmarks/tpch/queries/${QUERY}.sql"
[[ -f "$SQL" ]] || { echo "no SQL: $SQL"; exit 1; }

run_q() {
  local out err start end; out=$(mktemp); err=$(mktemp)
  start=$(date +%s.%N)
  psql -h 127.0.0.1 -p 5432 -t -A -F',' -q \
    -c "SET search_path TO datalake.tpch" -f "$SQL" 2>"$err" | grep -v '^SET$' >"$out"
  local rc=${PIPESTATUS[0]}; end=$(date +%s.%N)
  local ms=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.0f",(e-s)*1000}')
  rm -f "$out" "$err"
  [[ $rc -ne 0 ]] && { echo "ERR"; return; }
  echo "$ms"
}

echo "warm-loop bench: $QUERY x$RUNS (discard first $DISCARD), fixed warm cluster"
vals=()
for r in $(seq 1 "$RUNS"); do
  v=$(run_q)
  tag=""; [[ $r -le $DISCARD ]] && tag=" (warm-up, excluded)"
  printf "  run%-2d: %sms%s\n" "$r" "$v" "$tag"
  [[ $r -gt $DISCARD ]] && vals+=("$v")
done

echo "----"
printf '%s\n' "${vals[@]}" | grep -E '^[0-9]+$' | sort -n | awk -v T="${TRINO_MS:-12933}" '
  { a[++n]=$1 }
  function med(arr, c,   m){ return (c%2)? arr[int((c+1)/2)] : int((arr[int(c/2)]+arr[int(c/2)+1])/2) }
  END {
    if (n==0){ print "no numeric results"; exit }
    omed = med(a, n)
    # largest ratio gap between consecutive sorted values -> bimodal split
    best=1; bidx=0
    for (i=1;i<n;i++){ r=a[i+1]/a[i]; if(r>best){best=r; bidx=i} }
    cut = (best>1.4)? (a[bidx]+a[bidx+1])/2 : -1
    printf "n=%d  overall median=%.0fms (%.2fx Trino)  min=%d  max=%d\n", n, omed, omed/T, a[1], a[n]
    if (cut>0){
      fc=0; sc=0
      for(i=1;i<=n;i++){ if(a[i]<cut){f[++fc]=a[i]} else {s[++sc]=a[i]} }
      printf "BIMODAL (split ~%.0fms):\n", cut
      printf "  FAST: %d/%d (%.0f%%)  median %.0fms (%.2fx Trino)\n", fc,n, fc*100/n, med(f,fc), med(f,fc)/T
      printf "  SLOW: %d/%d (%.0f%%)  median %.0fms (%.2fx Trino)\n", sc,n, sc*100/n, med(s,sc), med(s,sc)/T
    } else {
      printf "UNIMODAL (no >1.4x gap): median %.0fms (%.2fx Trino)\n", omed, omed/T
    }
  }'
