# Planner backlog

This file used to track three pre-existing planner / execution defects
(PB-001, PB-002, PB-003) that surfaced during the TPC-H value-equivalence
diff against Trino. All three were resolved in OpenSpec change
`tpch-correctness-fixes` (the working copy that produced this file).

After the fix, the values-match rate is **16/16** at relative tolerance
`1e-9` on SF1. The `Discovery workflow` block below is preserved as the
reference recipe for re-running the diff against future changes.

---

## Discovery workflow (for reproducing the value diff)

Use this when you suspect a planner / execution change might have
broken parity with Trino. The `trino-diff` skill at
`.claude/skills/trino-diff/SKILL.md` wraps the same pipeline with
pre-flight checks, parallel CSV capture, and a one-shot summary.

```bash
# Bring up the shared stack + SF1 data.
docker compose up -d
docker compose run --rm tpch-seed

# Start arneb against Hive.
./target/release/arneb --config benchmarks/tpch/tpch-hive.toml &
sleep 3

# Per query, capture CSV from both engines.
for q in q01 q02 q03 q04 q05 q06 q07 q08 q09 q10 q11 q12 q13 q14 q16 q19; do
  psql -h 127.0.0.1 -p 5432 -U test -d test -A -F "," -t \
    -f benchmarks/tpch/queries/$q.sql > /tmp/tpch-diff/arneb_$q.csv
  docker exec arneb-trino-1 trino \
    --server http://localhost:8080 --catalog hive --schema tpch \
    --output-format CSV_UNQUOTED \
    --execute "$(cat benchmarks/tpch/queries/$q.sql)" \
    > /tmp/tpch-diff/trino_$q.csv
done

# Compare with relative-tolerance float diff.
for q in q01 ... q19; do
  python3 /tmp/tpch-diff/compare.py \
    /tmp/tpch-diff/arneb_$q.csv /tmp/tpch-diff/trino_$q.csv 1e-9
done

# Cleanup.
kill $(pgrep -f target/release/arneb); docker compose down
```

`compare.py` parses each CSV cell-by-cell, tolerates
`|a - b| <= tol × max(|a|, |b|, 1)` on float pairs, and reports
per-cell mismatches. Use `1e-9` for strict identity; relax to `1e-3`
if you want to filter out real float-order-of-operations drift on
aggregates.
