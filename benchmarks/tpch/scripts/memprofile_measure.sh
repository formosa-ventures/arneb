#!/usr/bin/env bash
# Measure-first: run each query alone with ARNEB_MEM_PROFILE=1 and capture the
# per-task arneb::memprofile lines (pool_peak / jemalloc_resident_peak /
# untracked_estimate / top_consumers) from worker logs → quantify where q05/q21's
# peak resident memory lives (tracked pool vs untracked). Recreates arneb only.
set -uo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:$PATH
cd /extdrive/formosa-ventures/repos/arneb || exit 2
CF=(-f docker-compose.yml -f docker/arneb-bench/docker-compose.bench.yml -f docker/arneb-bench/docker-compose.eageron.yml -f docker/arneb-bench/docker-compose.memprofile.yml)
PSQL="psql -h 127.0.0.1 -p 5432 -t -A -q"
OUT=/tmp/memprofile.out
: >"$OUT"

for q in q05 q21; do
  echo "==================== $q ====================" >>"$OUT"
  docker compose "${CF[@]}" up -d --force-recreate arneb arneb-worker-1 arneb-worker-2 >/dev/null 2>&1
  for _ in $(seq 1 60); do $PSQL -c "SELECT 1" >/dev/null 2>&1 && break; sleep 3; done
  printf '[%s] running %s\n' "$(date +%H:%M:%S)" "$q" >>"$OUT"
  $PSQL -f "benchmarks/tpch/queries/$q.sql" >/dev/null 2>&1
  sleep 5
  for w in arneb-arneb-1 arneb-worker-1 arneb-worker-2; do
    echo "--- $w memprofile lines ---" >>"$OUT"
    docker logs "$w" 2>&1 | grep "arneb::memprofile\|memprofile\|pool_peak_bytes" | tail -25 >>"$OUT"
  done
  # peak resident across all workers for this query
  echo "--- $q PEAK resident_bytes across workers ---" >>"$OUT"
  for w in arneb-worker-1 arneb-worker-2; do
    docker logs "$w" 2>&1 | grep -oE "jemalloc_resident_peak_bytes=[0-9]+" | sort -t= -k2 -n | tail -1 | sed "s/^/$w /" >>"$OUT"
  done
done
echo "MEMPROFILE_DONE" >>"$OUT"
echo "===== RESULTS ====="; cat "$OUT"
